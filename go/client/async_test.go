// Async send tests: the fair semaphores, the back-pressure gate, the bounded
// executors and the per-attempt retry chain — all against the same in-process
// mock cluster the sync send tests use.
package client

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/zhaohai666/rocketmq-client-remoting/go/common"
	"github.com/zhaohai666/rocketmq-client-remoting/go/remoting"
)

// deadBrokerAddr is a syntactically valid address with nothing listening, so a
// connection attempt fails fast with ECONNREFUSED — which the transport types as
// KindConnect, the Go spelling of Java's RemotingConnectException.
const deadBrokerAddr = "127.0.0.1:1"

// ---------------------------------------------------------------- fixtures

// captureCallback records what the async chain delivers. `calls` is what makes
// the "fires exactly once" contract testable: the second delivery of a buggy
// chain would bump it (and close of a channel twice would panic).
type captureCallback struct {
	calls  atomic.Int64
	fired  atomic.Bool
	panics bool

	mu  sync.Mutex
	ok  *SendResult
	err error
}

func newCaptureCallback() *captureCallback { return &captureCallback{} }

func (c *captureCallback) OnSuccess(result *SendResult) { c.record(result, nil) }
func (c *captureCallback) OnException(err error)        { c.record(nil, err) }

func (c *captureCallback) record(result *SendResult, err error) {
	c.calls.Add(1)
	c.mu.Lock()
	c.ok, c.err = result, err
	c.mu.Unlock()
	c.fired.Store(true)
	if c.panics {
		panic("callback boom")
	}
}

func (c *captureCallback) failure() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.err
}

func (c *captureCallback) result() *SendResult {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.ok
}

// waitFired blocks until the callback ran, then gives a duplicate delivery a
// chance to land so calls can be asserted exactly.
func (c *captureCallback) waitFired(t *testing.T) {
	t.Helper()
	waitFor(t, "async send callback", c.fired.Load)
	time.Sleep(80 * time.Millisecond)
	if n := c.calls.Load(); n != 1 {
		t.Fatalf("callback fired %d times, want exactly 1", n)
	}
}

// newStartedProducer starts a producer against nsAddr. tune runs before Start,
// so knobs that the pools read once (queue capacity, callback threads) take
// effect.
func newStartedProducer(t *testing.T, nsAddr, group string, tune func(*DefaultMQProducer)) *DefaultMQProducer {
	t.Helper()
	p := MustNewDefaultMQProducer(group)
	p.SetNameServerAddresses([]string{nsAddr})
	p.SetInstanceName(uniqueClientID(t))
	p.SetSendMsgTimeout(3000)
	if tune != nil {
		tune(p)
	}
	if err := p.Start(); err != nil {
		t.Fatalf("producer start: %v", err)
	}
	t.Cleanup(p.Shutdown)
	return p
}

// startAsyncFixture wires one mock broker (answering SEND with sendCode, 0 for
// success) plus a nameserver whose route points at it.
func startAsyncFixture(t *testing.T, topic, group string, sendCode int32,
	tune func(*DefaultMQProducer)) (*DefaultMQProducer, *mockServer, *sendBrokerOnReq) {

	t.Helper()
	handler := &sendBrokerOnReq{offset: 7, sendCode: sendCode}
	broker := startMockServer(t, handler.handle)
	ns := startMockServer(t, nameserverOnReq(map[string]string{
		topic: routeBodyFor(broker.addr, ""),
	}))
	return newStartedProducer(t, ns.addr, group, tune), broker, handler
}

// ---------------------------------------------------------------- semaphore

func TestFairSemaphoreGrantsOnlyTheHead(t *testing.T) {
	s := newFairSemaphore(1)
	if !s.tryAcquire(1, 100) {
		t.Fatal("the first acquire must succeed")
	}
	if got := s.availablePermits(); got != 0 {
		t.Fatalf("available = %d, want 0", got)
	}

	var wg sync.WaitGroup
	granted := make([]atomic.Bool, 2)
	// Queue them ONE AT A TIME. Whichever goroutine reaches tryAcquire first is
	// the head, so starting both at once would make "granted[0] must win" a coin
	// flip about the scheduler rather than a statement about the semaphore.
	wg.Add(1)
	go func() {
		defer wg.Done()
		// Short timeouts: the loser is expected to fail, and it must not cost
		// the suite a two-second sleep to find that out.
		granted[0].Store(s.tryAcquire(1, 400))
	}()
	waitFor(t, "the head to queue", func() bool { return s.waitingCount() == 1 })

	wg.Add(1)
	go func() {
		defer wg.Done()
		granted[1].Store(s.tryAcquire(1, 400))
	}()
	waitFor(t, "the second waiter to queue", func() bool { return s.waitingCount() == 2 })

	s.release(1)
	wg.Wait()

	// FIFO: the goroutine that queued first is the only one that may take it.
	if !granted[0].Load() {
		t.Error("the head of the queue must be granted")
	}
	if granted[1].Load() {
		t.Error("a later waiter cut in front of the head — the semaphore is not fair")
	}
	if got := s.availablePermits(); got != 0 {
		t.Errorf("available = %d, want 0 (one permit was granted)", got)
	}
}

// TestFairSemaphoreHeadGivingUpWakesTheNext is the case the Python port's
// docstring calls out: the head asks for more than can ever fit, times out, and
// its REMOVAL is what makes the request behind it admissible. Without the wake-up
// on that exit the second request would sleep out its whole timeout even though a
// permit was free the entire time.
func TestFairSemaphoreHeadGivingUpWakesTheNext(t *testing.T) {
	s := newFairSemaphore(2)
	if !s.tryAcquire(1, 100) {
		t.Fatal("prime acquire failed")
	}
	// free == 1: a request for 2 can never be satisfied, a request for 1 can.
	headDone := make(chan bool, 1)
	go func() { headDone <- s.tryAcquire(2, 120) }()
	waitFor(t, "the oversized head queued", func() bool { return s.waitingCount() == 1 })

	started := time.Now()
	second := make(chan bool, 1)
	go func() { second <- s.tryAcquire(1, 3_000) }()

	select {
	case ok := <-headDone:
		if ok {
			t.Fatal("a 2-permit request must not be satisfied by 1 free permit")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the oversized head never timed out")
	}
	select {
	case ok := <-second:
		if !ok {
			t.Fatal("the second request must be granted once the head gave up")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the giving-up exit did not wake the next waiter")
	}
	if elapsed := time.Since(started); elapsed > 1500*time.Millisecond {
		t.Errorf("the second request waited %v; the head's exit should have woken it", elapsed)
	}
}

func TestFairSemaphoreSetTotalPermitsPreservesInFlight(t *testing.T) {
	s := newFairSemaphore(10)
	if !s.tryAcquire(3, 100) {
		t.Fatal("acquire failed")
	}
	// Java DefaultMQProducerTest:593-595 asserts exactly this sum.
	if got := s.availablePermits(); got != 7 {
		t.Fatalf("available = %d, want 7", got)
	}
	s.setTotalPermits(20)
	if got := s.availablePermits(); got != 17 {
		t.Errorf("after growing to 20, available = %d, want 17 (3 in flight)", got)
	}
	if got := s.totalPermits(); got != 20 {
		t.Errorf("total = %d, want 20", got)
	}
	for i := 0; i < 3; i++ {
		s.release(1)
	}
	if got := s.availablePermits(); got != 20 {
		t.Errorf("after returning the 3, available = %d, want 20", got)
	}
}

func TestFairSemaphoreShrinkMayGoNegative(t *testing.T) {
	s := newFairSemaphore(10)
	if !s.tryAcquire(10, 100) {
		t.Fatal("acquire failed")
	}
	// Shrinking below what is in flight: Java's `new Semaphore(negative)` accepts
	// this too, and the outstanding releases pull it back positive.
	s.setTotalPermits(4)
	if got := s.availablePermits(); got != -6 {
		t.Fatalf("available = %d, want -6", got)
	}
	for i := 0; i < 10; i++ {
		s.release(1)
	}
	if got := s.availablePermits(); got != 4 {
		t.Errorf("available = %d, want 4", got)
	}
}

func TestFairSemaphoreExpandWakesAQueuedWaiter(t *testing.T) {
	s := newFairSemaphore(1)
	if !s.tryAcquire(1, 100) {
		t.Fatal("prime acquire failed")
	}
	done := make(chan bool, 1)
	go func() { done <- s.tryAcquire(1, 3_000) }()
	waitFor(t, "waiter queued", func() bool { return s.waitingCount() == 1 })

	// Growing the total is what frees a permit here — no release() is involved,
	// so the wake-up has to come from setTotalPermits.
	s.setTotalPermits(2)
	select {
	case ok := <-done:
		if !ok {
			t.Fatal("the waiter must be granted after the total grew")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("setTotalPermits did not wake the queued waiter")
	}
}

// ---------------------------------------------------------------- config

func TestAsyncConfigDefaultsAndFloors(t *testing.T) {
	p := MustNewDefaultMQProducer("GID_async_config")

	if p.IsEnableBackpressureForAsyncMode() {
		t.Error("Java's enableBackpressureForAsyncMode default is false")
	}
	if got := p.BackPressureForAsyncSendNum(); got != 1024 {
		t.Errorf("backPressureForAsyncSendNum = %d, want 1024", got)
	}
	if got := p.BackPressureForAsyncSendSize(); got != 100*1024*1024 {
		t.Errorf("backPressureForAsyncSendSize = %d, want 100M", got)
	}
	if got := p.RetryTimesWhenSendAsyncFailed(); got != 2 {
		t.Errorf("retryTimesWhenSendAsyncFailed = %d, want 2", got)
	}
	if got := p.AsyncSenderQueueCapacity(); got != 50000 {
		t.Errorf("asyncSenderQueueCapacity = %d, want Java's 50000", got)
	}
	if got := p.SemaphoreAsyncSendNumAvailablePermits(); got != 1024 {
		t.Errorf("num semaphore permits = %d, want 1024", got)
	}
	if got := p.SemaphoreAsyncSendSizeAvailablePermits(); got != 100*1024*1024 {
		t.Errorf("size semaphore permits = %d, want 100M", got)
	}

	// The floors. Java writes the branch as `if (cfg > floor)`, so a value EQUAL
	// to the floor also lands on the floor.
	p.SetBackPressureForAsyncSendNum(3)
	if got := p.BackPressureForAsyncSendNum(); got != MinAsyncSendNum {
		t.Errorf("num floor: got %d, want %d", got, MinAsyncSendNum)
	}
	p.SetBackPressureForAsyncSendNum(MinAsyncSendNum)
	if got := p.SemaphoreAsyncSendNumAvailablePermits(); got != MinAsyncSendNum {
		t.Errorf("num floor permits: got %d, want %d", got, MinAsyncSendNum)
	}
	p.SetBackPressureForAsyncSendSize(1)
	if got := p.BackPressureForAsyncSendSize(); got != MinAsyncSendSize {
		t.Errorf("size floor: got %d, want %d", got, MinAsyncSendSize)
	}

	// A runtime resize must move the semaphore, not just the field.
	p.SetBackPressureForAsyncSendNum(64)
	if got := p.SemaphoreAsyncSendNumAvailablePermits(); got != 64 {
		t.Errorf("resize did not reach the semaphore: %d permits, want 64", got)
	}
}

func TestAsyncConfigResizeKeepsInFlightCount(t *testing.T) {
	p := MustNewDefaultMQProducer("GID_async_resize")
	p.SetBackPressureForAsyncSendNum(10)
	// Take the permits the way the gate does, so the semaphore has 4 in flight.
	for i := 0; i < 4; i++ {
		if !p.semaphoreAsyncSendNum.tryAcquire(1, 10) {
			t.Fatal("could not take a permit")
		}
	}
	p.SetBackPressureForAsyncSendNum(10)
	if got := p.SemaphoreAsyncSendNumAvailablePermits(); got != 6 {
		t.Fatalf("available = %d, want 6", got)
	}
	// Java's formula makes this the interesting case: acquired = 10 - 6 = 4, so
	// the new semaphore is `new Semaphore(12 - 4)` = 8 free.
	p.SetBackPressureForAsyncSendNum(12)
	if got := p.SemaphoreAsyncSendNumAvailablePermits(); got != 8 {
		t.Errorf("available = %d, want 8 (4 still in flight)", got)
	}
}

// ---------------------------------------------------------------- happy path

func TestAsyncSendDeliversResultOffTheCallerGoroutine(t *testing.T) {
	const topic = "AsyncHappyTopic"
	p, broker, _ := startAsyncFixture(t, topic, "GID_async_happy", 0, nil)

	cb := newCaptureCallback()
	if err := p.SendAsync(common.NewMessage(topic, []byte("async")), cb); err != nil {
		t.Fatalf("SendAsync submit: %v", err)
	}
	// SendAsync only submits, so returning before the callback runs is both "it
	// does not block" and "the callback is not run on the caller's goroutine".
	if cb.fired.Load() {
		t.Fatal("the callback ran before SendAsync returned")
	}
	cb.waitFired(t)

	if err := cb.failure(); err != nil {
		t.Fatalf("async send failed: %v", err)
	}
	result := cb.result()
	if result == nil || result.SendStatus != SendOK {
		t.Fatalf("result = %+v, want SEND_OK", result)
	}
	if got := len(broker.requests(remoting.ReqSendMessageV2)); got != 1 {
		t.Errorf("broker saw %d SEND_MESSAGE_V2, want 1", got)
	}
}

// TestAsyncSendHookPairFiresOnceWithTheRealResult pins down a deliberate
// divergence: Java runs SendMessageHook.after TWICE for an async send — once with
// a null SendResult straight after dispatch (sendKernelImpl:1087-1090, the ASYNC
// branch never assigns sendResult) and once with the real outcome. This port, like
// the Python/C++/C# ones, fires it once with the real outcome.
func TestAsyncSendHookPairFiresOnceWithTheRealResult(t *testing.T) {
	const topic = "AsyncHookTopic"
	p, _, _ := startAsyncFixture(t, topic, "GID_async_hook", 0, nil)

	hook := &recordingSendHook{}
	p.RegisterSendMessageHook(hook)

	cb := newCaptureCallback()
	if err := p.SendAsync(common.NewMessage(topic, []byte("async-hook")), cb); err != nil {
		t.Fatalf("SendAsync submit: %v", err)
	}
	cb.waitFired(t)
	time.Sleep(80 * time.Millisecond)

	before, after := hook.counts()
	if before != 1 {
		t.Fatalf("sendMessageBefore ran %d times, want 1", before)
	}
	if after != 1 {
		t.Fatalf("sendMessageAfter ran %d times, want exactly 1", after)
	}
	last := hook.lastAfter()
	if last == nil {
		t.Fatal("no after context recorded")
	}
	if last.CommunicationMode != CommunicationModeAsync {
		t.Errorf("CommunicationMode = %q, want %q", last.CommunicationMode, CommunicationModeAsync)
	}
	if last.SendResult == nil {
		t.Error("the after hook must see the real SendResult, not nil")
	} else if last.SendResult.SendStatus != SendOK {
		t.Errorf("after-hook SendResult = %s, want SEND_OK", last.SendResult.SendStatus)
	}
	if last.Exception != nil {
		t.Errorf("after-hook Exception = %v, want nil", last.Exception)
	}
	if last.BrokerAddr == "" {
		t.Error("after-hook BrokerAddr must be filled in")
	}
}

// ---------------------------------------------------------------- retry chain

// TestAsyncSendRetriesOnAnotherBroker also proves the request is built ONCE:
// after a failed attempt the retry goes to b2, yet the header still carries the
// queueId of the b1 queue the FIRST attempt picked. Rebuilding the request per
// attempt would make that queueId 0 (b2's first queue).
func TestAsyncSendRetriesOnAnotherBroker(t *testing.T) {
	const topic = "AsyncRetryTopic"
	handler := &sendBrokerOnReq{offset: 9}
	live := startMockServer(t, handler.handle)
	ns := startMockServer(t, nameserverOnReq(map[string]string{
		topic: twoBrokerRouteBody(deadBrokerAddr, live.addr),
	}))
	p := newStartedProducer(t, ns.addr, "GID_async_retry", func(p *DefaultMQProducer) {
		p.SetRetryTimesWhenSendAsyncFailed(2)
	})

	// Advance the ring cursor so the first selection is b1's queue 3: the retry
	// then lands on b2 queue 0, and any queueId other than 0 in the request proves
	// the request survived the broker switch untouched.
	info, err := p.topicPublishInfo(topic)
	if err != nil {
		t.Fatalf("route lookup: %v", err)
	}
	for i := 0; i < 3; i++ {
		if _, ok, err := info.SelectOneMessageQueue(nil); err != nil || !ok {
			t.Fatalf("cursor prime %d failed (ok=%v err=%v)", i, ok, err)
		}
	}

	cb := newCaptureCallback()
	if err := p.SendAsync(common.NewMessage(topic, []byte("retry-me")), cb); err != nil {
		t.Fatalf("SendAsync submit: %v", err)
	}
	cb.waitFired(t)

	if err := cb.failure(); err != nil {
		t.Fatalf("the retry should have succeeded on b2: %v", err)
	}
	if result := cb.result(); result == nil || result.SendStatus != SendOK {
		t.Fatalf("result = %+v, want SEND_OK from b2", result)
	}
	reqs := live.requests(remoting.ReqSendMessageV2)
	if len(reqs) != 1 {
		t.Fatalf("b2 saw %d sends, want exactly 1", len(reqs))
	}
	// The mock echoes the request's queueId back, so the SendResult names the
	// queue the headers asked for.
	if got := headerOf(t, reqs[0]).QueueID; got == nil || *got != 3 {
		t.Errorf("retry header queueId = %v, want 3 (the b1 queue the request was built for)", got)
	}
}

// TestAsyncSendRetryBudgetIsHonoured counts attempts against a broker that
// accepts the request and then closes the connection without answering — the
// transport failure that makes needRetry true.
func TestAsyncSendRetryBudgetIsHonoured(t *testing.T) {
	for _, tc := range []struct {
		retryTimes int
		wantSends  int
	}{
		{retryTimes: 0, wantSends: 1},
		{retryTimes: 1, wantSends: 2},
		{retryTimes: 2, wantSends: 3},
	} {
		t.Run(fmt.Sprintf("retry=%d", tc.retryTimes), func(t *testing.T) {
			const topic = "AsyncBudgetTopic"
			p, broker, _ := startAsyncFixture(t, topic, "GID_async_budget", 0,
				func(p *DefaultMQProducer) {
					p.SetRetryTimesWhenSendAsyncFailed(tc.retryTimes)
					p.SetSendMsgTimeout(2000)
				})
			// Record the request, then close the connection without answering: the
			// client sees "connection closed" (RemotingSendRequestException).
			broker.setDrop(true)

			cb := newCaptureCallback()
			if err := p.SendAsync(common.NewMessage(topic, []byte("budget")), cb); err != nil {
				t.Fatalf("SendAsync submit: %v", err)
			}
			cb.waitFired(t)

			if cb.failure() == nil {
				t.Fatal("a dropped connection must be reported to the callback")
			}
			if got := len(broker.requests(remoting.ReqSendMessageV2)); got != tc.wantSends {
				t.Errorf("attempts = %d, want %d (1 + %d retries)",
					got, tc.wantSends, tc.retryTimes)
			}
		})
	}
}

// TestAsyncSendReportsSendRequestFailed checks the error the transport failure is
// reported AS: Java wraps operationFail's RemotingSendRequestException into
// MQClientException("send request failed") and the original stays reachable.
func TestAsyncSendReportsSendRequestFailed(t *testing.T) {
	const topic = "AsyncFailTextTopic"
	p, broker, _ := startAsyncFixture(t, topic, "GID_async_failtext", 0,
		func(p *DefaultMQProducer) {
			p.SetRetryTimesWhenSendAsyncFailed(0)
			p.SetSendMsgTimeout(2000)
		})
	broker.setDrop(true)

	cb := newCaptureCallback()
	if err := p.SendAsync(common.NewMessage(topic, []byte("x")), cb); err != nil {
		t.Fatalf("SendAsync submit: %v", err)
	}
	cb.waitFired(t)

	err := cb.failure()
	if err == nil {
		t.Fatal("expected a failure")
	}
	if !strings.Contains(err.Error(), "send request failed") {
		t.Errorf("error = %q, want Java's %q wording", err.Error(), "send request failed")
	}
	if !common.IsKind(errors.Unwrap(err), common.KindSendRequest) {
		t.Errorf("the transport error must stay reachable through Unwrap: %v", errors.Unwrap(err))
	}
}

// TestAsyncSendBrokerErrorIsNotRetried pins the divergence from the sync path:
// an explicit broker error is FINAL for an async send, which never consults
// retryResponseCodes. The sync path would be trying another broker here.
func TestAsyncSendBrokerErrorIsNotRetried(t *testing.T) {
	const topic = "AsyncBrokerErrTopic"
	handler := &sendBrokerOnReq{sendCode: remoting.RespSystemBusy}
	broker := startMockServer(t, handler.handle)
	ns := startMockServer(t, nameserverOnReq(map[string]string{
		topic: twoBrokerRouteBody(broker.addr, broker.addr),
	}))
	p := newStartedProducer(t, ns.addr, "GID_async_brokererr", func(p *DefaultMQProducer) {
		p.SetRetryTimesWhenSendAsyncFailed(2)
	})

	cb := newCaptureCallback()
	if err := p.SendAsync(common.NewMessage(topic, []byte("x")), cb); err != nil {
		t.Fatalf("SendAsync submit: %v", err)
	}
	cb.waitFired(t)

	err := cb.failure()
	if err == nil {
		t.Fatal("SYSTEM_BUSY must surface as a failure")
	}
	if !common.IsKind(err, common.KindBroker) {
		t.Fatalf("error kind = %v, want the broker error passed through unwrapped: %v", err, err)
	}
	if code, ok := responseCodeOf(err); !ok || code != remoting.RespSystemBusy {
		t.Errorf("response code = %d (ok=%v), want %d", code, ok, remoting.RespSystemBusy)
	}
	if got := len(broker.requests(remoting.ReqSendMessageV2)); got != 1 {
		t.Errorf("attempts = %d, want exactly 1 — a broker error is not retried", got)
	}
}

// ---------------------------------------------------------------- back pressure

func TestAsyncSendBackPressureNumGateRejectsAndSendsNothing(t *testing.T) {
	const topic = "AsyncBpNumTopic"
	p, broker, _ := startAsyncFixture(t, topic, "GID_async_bp_num", 0,
		func(p *DefaultMQProducer) {
			p.SetEnableBackpressureForAsyncMode(true)
			p.SetSendMsgTimeout(200) // the gate waits out the budget; keep it short
		})
	// Hold every count permit, the way maxed-out in-flight sends would.
	for i := 0; i < p.BackPressureForAsyncSendNum(); i++ {
		if !p.semaphoreAsyncSendNum.tryAcquire(1, 10) {
			t.Fatalf("could not take permit %d", i)
		}
	}

	cb := newCaptureCallback()
	if err := p.SendAsync(common.NewMessage(topic, []byte("x")), cb); err != nil {
		t.Fatalf("SendAsync submit: %v", err)
	}
	cb.waitFired(t)

	err := cb.failure()
	if err == nil {
		t.Fatal("the count gate must fail the send")
	}
	if !common.IsKind(err, common.KindTooMuchRequest) {
		t.Fatalf("error kind = %v, want RemotingTooMuchRequestException: %v", err, err)
	}
	if !strings.Contains(err.Error(), "send message tryAcquire semaphoreAsyncNum timeout") {
		t.Errorf("error = %q, want Java's semaphoreAsyncNum wording", err.Error())
	}
	if got := len(broker.requests(remoting.ReqSendMessageV2)); got != 0 {
		t.Errorf("a gated send reached the broker %d times, want 0", got)
	}
}

// TestAsyncSendBackPressureSizeGateReturnsTheCountPermit is the regression that
// matters: the byte permit is requested AFTER the count permit, so a byte-gate
// timeout has to hand the count permit back. Otherwise one engagement with the
// gate permanently eats a slot.
func TestAsyncSendBackPressureSizeGateReturnsTheCountPermit(t *testing.T) {
	const topic = "AsyncBpSizeTopic"
	p, broker, _ := startAsyncFixture(t, topic, "GID_async_bp_size", 0,
		func(p *DefaultMQProducer) {
			p.SetEnableBackpressureForAsyncMode(true)
			p.SetBackPressureForAsyncSendSize(MinAsyncSendSize)
			p.SetSendMsgTimeout(200)
		})
	// Drain the whole BYTE budget, leaving the count budget untouched.
	if !p.semaphoreAsyncSendSize.tryAcquire(MinAsyncSendSize, 10) {
		t.Fatal("could not drain the byte budget")
	}
	numBefore := p.SemaphoreAsyncSendNumAvailablePermits()

	cb := newCaptureCallback()
	if err := p.SendAsync(common.NewMessage(topic, []byte("x")), cb); err != nil {
		t.Fatalf("SendAsync submit: %v", err)
	}
	cb.waitFired(t)

	err := cb.failure()
	if err == nil {
		t.Fatal("the byte gate must fail the send")
	}
	if !strings.Contains(err.Error(), "send message tryAcquire semaphoreAsyncSize timeout") {
		t.Errorf("error = %q, want Java's semaphoreAsyncSize wording", err.Error())
	}
	if got := p.SemaphoreAsyncSendNumAvailablePermits(); got != numBefore {
		t.Errorf("count permits = %d, want %d — the byte-gate failure leaked a permit",
			got, numBefore)
	}
	if got := p.SemaphoreAsyncSendSizeAvailablePermits(); got != 0 {
		t.Errorf("byte permits = %d, want 0 (the byte gate never granted anything)", got)
	}
	if got := len(broker.requests(remoting.ReqSendMessageV2)); got != 0 {
		t.Errorf("a gated send reached the broker %d times, want 0", got)
	}
}

func TestAsyncSendReturnsPermitsAfterSuccessAndFailure(t *testing.T) {
	for _, tc := range []struct {
		name     string
		sendCode int32
	}{
		{name: "success", sendCode: 0},
		{name: "broker_error", sendCode: remoting.RespSystemBusy},
	} {
		t.Run(tc.name, func(t *testing.T) {
			const topic = "AsyncPermitsTopic"
			p, _, _ := startAsyncFixture(t, topic, "GID_async_permits", tc.sendCode,
				func(p *DefaultMQProducer) { p.SetEnableBackpressureForAsyncMode(true) })

			numBefore := p.SemaphoreAsyncSendNumAvailablePermits()
			sizeBefore := p.SemaphoreAsyncSendSizeAvailablePermits()

			body := []byte("permit-accounting")
			cb := newCaptureCallback()
			if err := p.SendAsync(common.NewMessage(topic, body), cb); err != nil {
				t.Fatalf("SendAsync submit: %v", err)
			}
			cb.waitFired(t)

			if got := p.SemaphoreAsyncSendNumAvailablePermits(); got != numBefore {
				t.Errorf("count permits = %d, want %d back", got, numBefore)
			}
			if got := p.SemaphoreAsyncSendSizeAvailablePermits(); got != sizeBefore {
				t.Errorf("byte permits = %d, want %d back", got, sizeBefore)
			}
		})
	}
}

// TestAsyncSendChargesThePreCompressionBodyLength: Java computes msgLen on the
// CALLER's thread, before the worker compresses, so the permits charged must be
// the ORIGINAL body length. The test is discriminating rather than tautological:
// the byte budget is left with 7 permits, which cannot cover a 4 KiB body but
// easily covers its compressed form — charging the post-compression length would
// let the send through.
func TestAsyncSendChargesThePreCompressionBodyLength(t *testing.T) {
	const topic = "AsyncPermitLenTopic"
	p, broker, _ := startAsyncFixture(t, topic, "GID_async_permit_len", 0,
		func(p *DefaultMQProducer) {
			p.SetEnableBackpressureForAsyncMode(true)
			p.SetBackPressureForAsyncSendSize(MinAsyncSendSize)
			p.SetCompressMsgBodyOverHowmuch(16) // compress anything >= 16 bytes
			p.SetSendMsgTimeout(200)
		})

	body := make([]byte, 4096)
	for i := range body {
		body[i] = byte('a' + i%8) // highly compressible
	}
	if !p.semaphoreAsyncSendSize.tryAcquire(MinAsyncSendSize-7, 10) {
		t.Fatal("could not leave a 7-permit byte budget")
	}

	cb := newCaptureCallback()
	if err := p.SendAsync(common.NewMessage(topic, body), cb); err != nil {
		t.Fatalf("SendAsync submit: %v", err)
	}
	cb.waitFired(t)

	err := cb.failure()
	if err == nil {
		t.Fatal("a 4096-byte body must not fit in a 7-permit byte budget")
	}
	if !strings.Contains(err.Error(), "semaphoreAsyncSize timeout") {
		t.Errorf("error = %q, want the byte gate to be the one that refused", err.Error())
	}
	if got := len(broker.requests(remoting.ReqSendMessageV2)); got != 0 {
		t.Errorf("a gated send reached the broker %d times, want 0", got)
	}
}

// TestAsyncSendCompressesOnTheWorkerAndRestoresTheCallerMessage mirrors Java's
// sendKernelImpl: compression happens on the sender goroutine (:945) and its
// finally (:1093-1096) puts the caller's body back once the request is on its
// way. The wire body must be the compressed one while the caller's message ends
// up byte-identical to what it was handed over as.
func TestAsyncSendCompressesOnTheWorkerAndRestoresTheCallerMessage(t *testing.T) {
	const topic = "AsyncRestoreTopic"
	p, broker, _ := startAsyncFixture(t, topic, "GID_async_restore", 0,
		func(p *DefaultMQProducer) {
			p.SetCompressMsgBodyOverHowmuch(16)
			p.SetNamespace("")
		})

	body := make([]byte, 1024)
	for i := range body {
		body[i] = byte('a' + i%8)
	}
	msg := common.NewMessage(topic, body)
	cb := newCaptureCallback()
	if err := p.SendAsync(msg, cb); err != nil {
		t.Fatalf("SendAsync submit: %v", err)
	}
	cb.waitFired(t)
	if err := cb.failure(); err != nil {
		t.Fatalf("async send failed: %v", err)
	}

	reqs := broker.requests(remoting.ReqSendMessageV2)
	if len(reqs) != 1 {
		t.Fatalf("broker saw %d sends, want 1", len(reqs))
	}
	if got := len(reqs[0].Body); got >= len(body) {
		t.Errorf("wire body = %d bytes, want the compressed form (< %d)", got, len(body))
	}
	if len(msg.Body) != len(body) || msg.Body[0] != body[0] {
		t.Errorf("the caller's body was not restored: %d bytes", len(msg.Body))
	}
	if msg.Topic != topic {
		t.Errorf("topic = %q, want the caller's %q back", msg.Topic, topic)
	}
}

// ---------------------------------------------------------------- executors

// fillSenderQueue replaces the sender pool with a one-worker/one-slot pool and
// blocks that worker, so every further Submit is rejected. The returned func
// unblocks it (and must run before Shutdown drains the pool).
func fillSenderQueue(t *testing.T, p *DefaultMQProducer) (release func()) {
	t.Helper()
	pool := newBoundedPool("test-async-sender", 1, 1)
	p.asyncSenderPool.Store(pool)
	unblock := make(chan struct{})
	var once sync.Once
	release = func() { once.Do(func() { close(unblock) }) }
	waitFor(t, "the sender queue to fill", func() bool {
		return pool.Submit(func() { <-unblock }) != nil
	})
	return release
}

// TestAsyncSendExecutorRejectedWhenQueueFull: without back pressure a full sender
// queue is reported to the CALLER, not through the callback — Java's
// `MQClientException("executor rejected")` out of executeAsyncMessageSend.
func TestAsyncSendExecutorRejectedWhenQueueFull(t *testing.T) {
	const topic = "AsyncRejectTopic"
	p, _, _ := startAsyncFixture(t, topic, "GID_async_reject", 0, nil)
	release := fillSenderQueue(t, p)
	defer release()

	cb := newCaptureCallback()
	err := p.SendAsync(common.NewMessage(topic, []byte("x")), cb)
	if !errors.Is(err, errPoolRejected) {
		t.Fatalf("SendAsync = %v, want the executor rejection", err)
	}
	if !strings.Contains(err.Error(), "executor rejected") {
		t.Errorf("error = %q, want %q", err.Error(), "executor rejected")
	}
	if cb.fired.Load() {
		t.Error("a rejected submission must not fire the callback")
	}
}

// TestAsyncSendRunsInlineWhenBackpressureIsOn covers Java :675-681: with the gate
// armed the permits are already deducted, so a full queue makes the send run on
// the CALLER's goroutine (blocking it — that is the point) instead of failing.
func TestAsyncSendRunsInlineWhenBackpressureIsOn(t *testing.T) {
	const topic = "AsyncInlineTopic"
	p, broker, _ := startAsyncFixture(t, topic, "GID_async_inline", 0,
		func(p *DefaultMQProducer) { p.SetEnableBackpressureForAsyncMode(true) })
	release := fillSenderQueue(t, p)
	defer release()

	numBefore := p.SemaphoreAsyncSendNumAvailablePermits()
	cb := newCaptureCallback()
	if err := p.SendAsync(common.NewMessage(topic, []byte("inline")), cb); err != nil {
		t.Fatalf("SendAsync submit: %v", err)
	}
	cb.waitFired(t)

	if err := cb.failure(); err != nil {
		t.Fatalf("the inline send should have succeeded: %v", err)
	}
	// The inline branch really sent it (the sender pool was unusable).
	if got := len(broker.requests(remoting.ReqSendMessageV2)); got != 1 {
		t.Errorf("broker saw %d sends, want 1 from the inline path", got)
	}
	if got := p.SemaphoreAsyncSendNumAvailablePermits(); got != numBefore {
		t.Errorf("count permits = %d, want %d back after the inline run", got, numBefore)
	}
}

// TestAsyncSendCallbackPanicLeavesThePoolUsable: the pool must swallow a panicking
// task and keep its workers alive (ThreadPoolExecutor semantics).
func TestAsyncSendCallbackPanicLeavesThePoolUsable(t *testing.T) {
	const topic = "AsyncPanicTopic"
	p, _, _ := startAsyncFixture(t, topic, "GID_async_panic", 0, nil)

	boom := newCaptureCallback()
	boom.panics = true
	if err := p.SendAsync(common.NewMessage(topic, []byte("boom")), boom); err != nil {
		t.Fatalf("SendAsync submit: %v", err)
	}
	boom.waitFired(t)

	// The callback pool must still be servicing callbacks.
	after := newCaptureCallback()
	if err := p.SendAsync(common.NewMessage(topic, []byte("after")), after); err != nil {
		t.Fatalf("SendAsync submit: %v", err)
	}
	after.waitFired(t)
	if err := after.failure(); err != nil {
		t.Fatalf("the callback pool stopped working after a panicking callback: %v", err)
	}
}

// ---------------------------------------------------------------- API surface

func TestAsyncSendRejectsUnusableArguments(t *testing.T) {
	p := MustNewDefaultMQProducer("GID_async_args")
	msg := common.NewMessage("SomeTopic", []byte("x"))
	cb := newCaptureCallback()

	if err := p.SendAsync(msg, cb); err == nil {
		t.Error("sending on a producer that was never started must fail on the caller")
	}
	if err := p.SendAsync(msg, nil); err == nil {
		t.Error("a nil callback must be rejected, not silently dropped")
	}
	if err := p.SendAsync(nil, cb); err == nil {
		t.Error("a nil message must be rejected on the caller")
	}
	if err := p.SendAsyncBatch(nil, cb); err == nil {
		t.Error("an empty batch must be rejected")
	}
}

func TestAsyncSendPinnedQueueUsesTheAsyncWording(t *testing.T) {
	const topic = "AsyncPinnedTopic"
	p, _, _ := startAsyncFixture(t, topic, "GID_async_pinned", 0, nil)

	// A queue whose topic differs from the message's: Java guards this with
	// pinnedTopicMismatchAsync on the async kernel, pinnedTopicMismatchSync on the
	// sync one, and the two texts differ on purpose.
	mq := common.NewMessageQueue("AnotherTopic", "b1", 0)
	cb := newCaptureCallback()
	if err := p.SendAsyncToQueue(common.NewMessage(topic, []byte("x")), mq, cb); err != nil {
		t.Fatalf("SendAsyncToQueue submit: %v", err)
	}
	cb.waitFired(t)

	err := cb.failure()
	if err == nil {
		t.Fatal("a topic/queue mismatch must fail")
	}
	if !strings.Contains(err.Error(), pinnedTopicMismatchAsync) {
		t.Errorf("error = %q, want the async wording %q", err.Error(), pinnedTopicMismatchAsync)
	}
	if strings.Contains(err.Error(), pinnedTopicMismatchSync) {
		t.Errorf("error = %q used the SYNC wording on the async path", err.Error())
	}
}

func TestAsyncSendToQueuePinsTheQueue(t *testing.T) {
	const topic = "AsyncPinnedOkTopic"
	p, broker, _ := startAsyncFixture(t, topic, "GID_async_pinned_ok", 0, nil)

	// b1 has four queues; pin queue 2 and check the header says so.
	mq := common.NewMessageQueue(topic, "b1", 2)
	cb := newCaptureCallback()
	if err := p.SendAsyncToQueue(common.NewMessage(topic, []byte("pinned")), mq, cb); err != nil {
		t.Fatalf("SendAsyncToQueue submit: %v", err)
	}
	cb.waitFired(t)
	if err := cb.failure(); err != nil {
		t.Fatalf("pinned async send failed: %v", err)
	}
	reqs := broker.requests(remoting.ReqSendMessageV2)
	if len(reqs) != 1 {
		t.Fatalf("broker saw %d sends, want 1", len(reqs))
	}
	if got := headerOf(t, reqs[0]).QueueID; got == nil || *got != 2 {
		t.Errorf("header queueId = %v, want the pinned 2", got)
	}
}

func TestAsyncSendBatchDeliversTheBatchResult(t *testing.T) {
	const topic = "AsyncBatchTopic"
	p, broker, _ := startAsyncFixture(t, topic, "GID_async_batch", 0, nil)

	batch := []*common.Message{
		common.NewMessage(topic, []byte("one")),
		common.NewMessage(topic, []byte("two")),
	}
	cb := newCaptureCallback()
	if err := p.SendAsyncBatch(batch, cb); err != nil {
		t.Fatalf("SendAsyncBatch submit: %v", err)
	}
	cb.waitFired(t)

	if err := cb.failure(); err != nil {
		t.Fatalf("batch async send failed: %v", err)
	}
	if result := cb.result(); result == nil || result.SendStatus != SendOK {
		t.Fatalf("result = %+v, want SEND_OK", result)
	}
	if got := len(broker.requests(remoting.ReqSendBatchMessage)); got != 1 {
		t.Errorf("broker saw %d SEND_BATCH_MESSAGE, want 1", got)
	}
}

func TestAsyncSendBySelectorUsesTheSelectedQueue(t *testing.T) {
	const topic = "AsyncSelectorTopic"
	p, broker, _ := startAsyncFixture(t, topic, "GID_async_selector", 0, nil)

	hash := common.JavaStringHash("shard-key")
	wantQueue := int32(hash % 4)
	if wantQueue < 0 {
		wantQueue += 4
	}
	cb := newCaptureCallback()
	err := p.SendAsyncBySelector(common.NewMessage(topic, []byte("sel")),
		SelectMessageQueueByHash{}, "shard-key", cb)
	if err != nil {
		t.Fatalf("SendAsyncBySelector submit: %v", err)
	}
	cb.waitFired(t)
	if ferr := cb.failure(); ferr != nil {
		t.Fatalf("selector async send failed: %v", ferr)
	}
	reqs := broker.requests(remoting.ReqSendMessageV2)
	if len(reqs) != 1 {
		t.Fatalf("broker saw %d sends, want 1", len(reqs))
	}
	if got := headerOf(t, reqs[0]).QueueID; got == nil || *got != wantQueue {
		t.Errorf("header queueId = %v, want the selected %d", got, wantQueue)
	}
}

// emptySelector mirrors a selector that cannot decide; Java turns that into
// "select message queue return null.".
type emptySelector struct{}

func (emptySelector) Select(_ []common.MessageQueue, _ *common.Message, _ any) (common.MessageQueue, error) {
	return common.MessageQueue{}, nil
}

// throwingSelector mirrors a selector that blows up; Java wraps it as
// "select message queue threw exception." rather than leaking the type.
type throwingSelector struct{}

func (throwingSelector) Select(_ []common.MessageQueue, _ *common.Message, _ any) (common.MessageQueue, error) {
	panic("selector exploded")
}

// TestAsyncSendBySelectorSurfacesSelectorFailuresToTheCallback checks that a
// selector which cannot decide, or which throws, is reported through the callback
// with Java's wording rather than leaking its own error type.
func TestAsyncSendBySelectorSurfacesSelectorFailuresToTheCallback(t *testing.T) {
	const topic = "AsyncSelectorErrTopic"
	p, _, _ := startAsyncFixture(t, topic, "GID_async_selector_err", 0, nil)

	for _, tc := range []struct {
		name     string
		selector MessageQueueSelector
		want     string
	}{
		{name: "null_queue", selector: emptySelector{}, want: "select message queue return null."},
		{name: "throwing", selector: throwingSelector{}, want: "select message queue threw exception."},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cb := newCaptureCallback()
			err := p.SendAsyncBySelector(common.NewMessage(topic, []byte("x")), tc.selector, nil, cb)
			if err != nil {
				t.Fatalf("submit: %v", err)
			}
			cb.waitFired(t)
			if ferr := cb.failure(); ferr == nil || !strings.Contains(ferr.Error(), tc.want) {
				t.Errorf("error = %v, want %q", ferr, tc.want)
			}
		})
	}
}
