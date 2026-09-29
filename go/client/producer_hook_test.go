// Send-hook chain tests: CheckForbiddenHook (fires per ATTEMPT, error
// propagates), SendMessageHook before/after (panics swallowed, one pair per
// attempt, sees either SendResult or Exception) and EndTransactionHook (fires
// on both the caller and the check-back path).
//
// These run against the same in-process mock cluster as producer_test.go.
package client

import (
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/zhaohai666/rocketmq-client-remoting/go/common"
	"github.com/zhaohai666/rocketmq-client-remoting/go/remoting"
)

// ---------------------------------------------------------------- test hooks

// recordingSendHook captures every context it is handed. The contexts are
// collected under a mutex because the async path can call the after-hook from
// another goroutine.
type recordingSendHook struct {
	name string

	mu       sync.Mutex
	befores  []*SendMessageContext
	afters   []*SendMessageContext
	panicsOn bool
	onBefore func(*SendMessageContext)
}

func (h *recordingSendHook) HookName() string { return h.name }

func (h *recordingSendHook) SendMessageBefore(ctx *SendMessageContext) {
	h.mu.Lock()
	h.befores = append(h.befores, ctx)
	before := len(h.befores)
	onBefore := h.onBefore
	panicOn := h.panicsOn
	h.mu.Unlock()
	if onBefore != nil {
		onBefore(ctx)
	}
	if panicOn && before == 1 {
		panic("send hook exploded")
	}
}

func (h *recordingSendHook) SendMessageAfter(ctx *SendMessageContext) {
	h.mu.Lock()
	h.afters = append(h.afters, ctx)
	h.mu.Unlock()
}

func (h *recordingSendHook) counts() (int, int) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.befores), len(h.afters)
}

func (h *recordingSendHook) lastAfter() *SendMessageContext {
	h.mu.Lock()
	defer h.mu.Unlock()
	if len(h.afters) == 0 {
		return nil
	}
	return h.afters[len(h.afters)-1]
}

// recordingForbiddenHook records the context and rejects on demand.
type recordingForbiddenHook struct {
	mu       sync.Mutex
	contexts []*CheckForbiddenContext
	reject   string
}

func (h *recordingForbiddenHook) HookName() string { return "recordingForbidden" }

func (h *recordingForbiddenHook) CheckForbidden(ctx *CheckForbiddenContext) error {
	h.mu.Lock()
	h.contexts = append(h.contexts, ctx)
	reject := h.reject
	h.mu.Unlock()
	if reject != "" {
		return common.ClientError(reject)
	}
	return nil
}

func (h *recordingForbiddenHook) count() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.contexts)
}

func (h *recordingForbiddenHook) last() *CheckForbiddenContext {
	h.mu.Lock()
	defer h.mu.Unlock()
	if len(h.contexts) == 0 {
		return nil
	}
	return h.contexts[len(h.contexts)-1]
}

// recordingEndTxHook captures endTransaction contexts.
type recordingEndTxHook struct {
	mu       sync.Mutex
	contexts []*EndTransactionContext
	panics   bool
}

func (h *recordingEndTxHook) HookName() string { return "recordingEndTx" }

func (h *recordingEndTxHook) EndTransaction(ctx *EndTransactionContext) {
	if h.panics {
		panic("end transaction hook exploded")
	}
	h.mu.Lock()
	h.contexts = append(h.contexts, ctx)
	h.mu.Unlock()
}

func (h *recordingEndTxHook) snapshot() []*EndTransactionContext {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]*EndTransactionContext(nil), h.contexts...)
}

// ------------------------------------------------------------ send hooks

func TestSendHookBeforeAndAfterOnSuccess(t *testing.T) {
	const topic = "HookSendTopic"
	p, broker, _ := startProducerFixture(t, topic, "GID_hook_send")
	hook := &recordingSendHook{name: "rec1"}
	p.RegisterSendMessageHook(hook)

	msg := common.NewMessage(topic, []byte("hooked"))
	if _, err := p.Send(msg); err != nil {
		t.Fatalf("send: %v", err)
	}

	befores, afters := hook.counts()
	if befores != 1 || afters != 1 {
		t.Fatalf("before=%d after=%d, want 1/1", befores, afters)
	}
	after := hook.lastAfter()
	if after.Exception != nil {
		t.Errorf("after context carries exception %v", after.Exception)
	}
	if after.SendResult == nil || after.SendResult.MsgID == "" {
		t.Errorf("after context must carry the SendResult, got %v", after.SendResult)
	}
	if after.CommunicationMode != CommunicationModeSync {
		t.Errorf("mode = %q, want SYNC", after.CommunicationMode)
	}
	if after.BrokerAddr != broker.addr {
		t.Errorf("brokerAddr = %q, want %q", after.BrokerAddr, broker.addr)
	}
	if after.ProducerGroup != "GID_hook_send" || after.Producer != p {
		t.Errorf("producer identity wrong: group=%q producer=%v", after.ProducerGroup, after.Producer)
	}
	if after.MQ.Topic != topic || after.MQ.QueueID != 0 {
		t.Errorf("mq = %+v", after.MQ)
	}
	if after.Message != msg {
		t.Error("the context must carry the very message being sent")
	}
	if after.MsgType != common.NormalMsg {
		t.Errorf("msgType = %v, want Normal", after.MsgType)
	}
	if after.BornHost == "" {
		t.Error("bornHost must be populated from the local address")
	}
}

func TestSendHookAfterSeesNonRetryableBrokerError(t *testing.T) {
	const topic = "HookSendErrTopic"
	handler := &sendBrokerOnReq{offset: 1, sendCode: remoting.RespMessageIllegal}
	broker := startMockServer(t, handler.handle)
	ns := startMockServer(t, nameserverOnReq(map[string]string{
		topic: routeBodyFor(broker.addr, ""),
	}))
	p := MustNewDefaultMQProducer("GID_hook_send_err")
	p.SetNameServerAddresses([]string{ns.addr})
	p.SetInstanceName(uniqueClientID(t))
	if err := p.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	t.Cleanup(p.Shutdown)

	hook := &recordingSendHook{name: "rec-err"}
	p.RegisterSendMessageHook(hook)

	// MESSAGE_ILLEGAL(13) is not in RetryResponseCodes, so the send stops after
	// the first attempt and the error surfaces with a real code.
	_, err := p.Send(common.NewMessage(topic, []byte("nope")))
	if err == nil {
		t.Fatal("expected the broker error to surface")
	}
	befores, afters := hook.counts()
	if befores != 1 || afters != 1 {
		t.Fatalf("before=%d after=%d, want 1/1 (non-retryable code must not retry)", befores, afters)
	}
	after := hook.lastAfter()
	if after.Exception == nil {
		t.Fatal("after context must carry the failure")
	}
	if after.SendResult != nil {
		t.Errorf("a failed attempt must not report a SendResult, got %v", after.SendResult)
	}
	if len(broker.requests(remoting.ReqSendMessageV2)) != 1 {
		t.Error("exactly one attempt should have reached the broker")
	}
}

func TestSendHookPanicIsSwallowedAndNextHookStillRuns(t *testing.T) {
	const topic = "HookPanicTopic"
	p, _, _ := startProducerFixture(t, topic, "GID_hook_panic")

	bad := &recordingSendHook{name: "panicky", panicsOn: true}
	good := &recordingSendHook{name: "second"}
	p.RegisterSendMessageHook(bad)
	p.RegisterSendMessageHook(good)

	if _, err := p.Send(common.NewMessage(topic, []byte("still sends"))); err != nil {
		t.Fatalf("a panicking hook must not fail the send: %v", err)
	}
	if _, afters := bad.counts(); afters != 1 {
		t.Errorf("the panicking hook's after must still run once, got %d", afters)
	}
	if befores, afters := good.counts(); befores != 1 || afters != 1 {
		t.Errorf("a later hook must still run: before=%d after=%d", befores, afters)
	}
}

func TestSendHookRunsOncePerAttempt(t *testing.T) {
	const topic = "HookRetryTopic"
	// SYSTEM_ERROR(1) IS retryable, so with the default 2 retries the client
	// makes three attempts — and each one gets its own before/after pair.
	handler := &sendBrokerOnReq{offset: 1, sendCode: remoting.RespSystemError}
	broker := startMockServer(t, handler.handle)
	ns := startMockServer(t, nameserverOnReq(map[string]string{
		topic: routeBodyFor(broker.addr, ""),
	}))
	p := MustNewDefaultMQProducer("GID_hook_retry")
	p.SetNameServerAddresses([]string{ns.addr})
	p.SetInstanceName(uniqueClientID(t))
	if err := p.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	t.Cleanup(p.Shutdown)

	hook := &recordingSendHook{name: "retry-rec"}
	p.RegisterSendMessageHook(hook)

	if _, err := p.Send(common.NewMessage(topic, []byte("retry me"))); err == nil {
		t.Fatal("expected the send to fail after exhausting retries")
	}
	befores, afters := hook.counts()
	if befores != 3 || afters != 3 {
		t.Errorf("before=%d after=%d, want 3/3 (1 + RetryTimesWhenSendFailed=2)", befores, afters)
	}
	if got := len(broker.requests(remoting.ReqSendMessageV2)); got != 3 {
		t.Errorf("broker saw %d attempts, want 3", got)
	}
}

func TestCheckForbiddenHookBlocksEveryAttempt(t *testing.T) {
	const topic = "HookForbidTopic"
	p, broker, _ := startProducerFixture(t, topic, "GID_hook_forbid")
	hook := &recordingForbiddenHook{reject: "forbidden by policy"}
	p.RegisterCheckForbiddenHook(hook)

	_, err := p.Send(common.NewMessage(topic, []byte("blocked")))
	if err == nil {
		t.Fatal("the interceptor must fail the send")
	}
	if !strings.Contains(err.Error(), "forbidden by policy") {
		t.Errorf("the interceptor's own error must survive to the caller, got %v", err)
	}
	if got := hook.count(); got != 3 {
		t.Errorf("interceptor ran %d times, want 3 (once per attempt)", got)
	}
	if got := len(broker.requests(remoting.ReqSendMessageV2)); got != 0 {
		t.Errorf("no SEND request may reach the broker, got %d", got)
	}
}

func TestCheckForbiddenHookContextFields(t *testing.T) {
	const topic = "HookForbidCtxTopic"
	// The interceptor runs AFTER the namespace has been applied, so the route
	// has to exist under the namespaced name too.
	handler := &sendBrokerOnReq{offset: 3}
	broker := startMockServer(t, handler.handle)
	ns := startMockServer(t, nameserverOnReq(map[string]string{
		topic:              routeBodyFor(broker.addr, ""),
		"ns-hook%" + topic: routeBodyFor(broker.addr, ""),
	}))
	p := MustNewDefaultMQProducer("GID_hook_forbid_ctx")
	p.SetNameServerAddresses([]string{ns.addr})
	p.SetInstanceName(uniqueClientID(t))
	p.SetUnitMode(true)
	p.SetNamespace("ns-hook")
	if err := p.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	t.Cleanup(p.Shutdown)

	hook := &recordingForbiddenHook{}
	p.RegisterCheckForbiddenHook(hook)

	msg := common.NewMessage(topic, []byte("payload"))
	if _, err := p.Send(msg); err != nil {
		t.Fatalf("send: %v", err)
	}
	if hook.count() != 1 {
		t.Fatalf("interceptor ran %d times, want 1", hook.count())
	}
	ctx := hook.last()
	// The group is namespaced by Start() — Java DefaultMQProducer.start:375
	// does setProducerGroup(withNamespace(producerGroup)) — so the interceptor
	// sees the prefixed name, not the one the caller passed to the constructor.
	if ctx.ProducerGroup != "ns-hook%GID_hook_forbid_ctx" {
		t.Errorf("group = %q, want the namespaced group", ctx.ProducerGroup)
	}
	if ctx.NameServerAddr != p.NamesrvAddr() {
		t.Errorf("nameServerAddr = %q, want %q", ctx.NameServerAddr, p.NamesrvAddr())
	}
	if ctx.BrokerAddr != broker.addr {
		t.Errorf("brokerAddr = %q, want %q", ctx.BrokerAddr, broker.addr)
	}
	if ctx.CommunicationMode != CommunicationModeSync {
		t.Errorf("mode = %q", ctx.CommunicationMode)
	}
	if !ctx.UnitMode {
		t.Error("unitMode must be forwarded")
	}
	if ctx.Message != msg {
		t.Error("the context must carry the message being sent")
	}
	// The topic has been namespaced in place by the time the interceptor runs
	// (Java compresses and namespaces first, then calls the hook).
	if ctx.MQ.Topic != "ns-hook%"+topic {
		t.Errorf("mq topic = %q, want the namespaced topic", ctx.MQ.Topic)
	}
	// Java never populates arg on this context; our port mirrors that.
	if ctx.Arg != nil {
		t.Errorf("arg = %v, want nil (Java sends none)", ctx.Arg)
	}
}

func TestCheckForbiddenHookBlocksOneway(t *testing.T) {
	const topic = "HookForbidOnewayTopic"
	p, broker, _ := startProducerFixture(t, topic, "GID_hook_forbid_oneway")
	hook := &recordingForbiddenHook{reject: "oneway blocked"}
	p.RegisterCheckForbiddenHook(hook)

	err := p.SendOneway(common.NewMessage(topic, []byte("nope")), nil)
	if err == nil || !strings.Contains(err.Error(), "oneway blocked") {
		t.Fatalf("oneway must be blocked too, got %v", err)
	}
	if got := len(broker.requests(remoting.ReqSendMessageV2)); got != 0 {
		t.Errorf("no oneway request may reach the broker, got %d", got)
	}
	if ctx := hook.last(); ctx == nil || ctx.CommunicationMode != CommunicationModeOneway {
		t.Errorf("mode = %v, want ONEWAY", ctx)
	}
}

func TestSendHookContextMsgTypeClassification(t *testing.T) {
	p := MustNewDefaultMQProducer("GID_msgtype")
	mq := common.NewMessageQueue("T", "b1", 0)

	cases := []struct {
		name  string
		apply func(*common.Message)
		want  common.MessageType
	}{
		{"plain", func(*common.Message) {}, common.NormalMsg},
		{"tran prepared", func(m *common.Message) {
			m.PutProperty(common.PropertyTransactionPrepared, "true")
		}, common.TransMsgHalf},
		{"delay level", func(m *common.Message) {
			m.SetDelayTimeLevel(3)
		}, common.DelayMsg},
		{"start deliver time", func(m *common.Message) {
			m.PutProperty(common.PropertyStartDeliverTime, "1700000000000")
		}, common.DelayMsg},
		{"timer deliver ms", func(m *common.Message) {
			m.PutProperty(common.PropertyTimerDeliverMs, "1000")
		}, common.DelayMsg},
		{"timer delay sec", func(m *common.Message) {
			m.PutProperty(common.PropertyTimerDelaySec, "1")
		}, common.DelayMsg},
		{"timer delay ms", func(m *common.Message) {
			m.PutProperty(common.PropertyTimerDelayMs, "1000")
		}, common.DelayMsg},
		// The delay branch is a second `if`, not an else-if: a transactional
		// message that also carries a delay property classifies as Delay.
		{"tran beats delay", func(m *common.Message) {
			m.PutProperty(common.PropertyTransactionPrepared, "true")
			m.PutProperty(common.PropertyTimerDelayMs, "1000")
		}, common.DelayMsg},
		// TRAN_MSG must be the string "true": Boolean.parseBoolean of anything
		// else is false, so the message stays Normal.
		{"tran not true", func(m *common.Message) {
			m.PutProperty(common.PropertyTransactionPrepared, "TRUE")
		}, common.NormalMsg},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			msg := common.NewMessage("T", []byte("x"))
			tc.apply(msg)
			ctx := p.buildSendContext(msg, mq, "127.0.0.1:10911", CommunicationModeSync)
			if ctx.MsgType != tc.want {
				t.Errorf("msgType = %v (%s), want %v (%s)",
					ctx.MsgType, ctx.MsgType.ShortName(), tc.want, tc.want.ShortName())
			}
		})
	}
}

// ------------------------------------------------------- end transaction

func TestEndTransactionHookOnCallerPath(t *testing.T) {
	const topic = "HookEndTxTopic"
	const group = "GID_hook_endtx"
	p, broker, _ := startProducerFixture(t, topic, group)

	hook := &recordingEndTxHook{}
	p.RegisterEndTransactionHook(hook)

	listener := &recordingTransactionListener{executeState: CommitMessage, checkState: Unknow}
	msg := common.NewMessage(topic, []byte("tx"))
	result, err := p.SendMessageInTransaction(msg, listener, nil)
	if err != nil {
		t.Fatalf("transaction send: %v", err)
	}
	ctxs := hook.snapshot()
	if len(ctxs) != 1 {
		t.Fatalf("end-transaction hook fired %d times, want 1", len(ctxs))
	}
	ctx := ctxs[0]
	if ctx.FromTransactionCheck {
		t.Error("fromTransactionCheck must be false on the caller path")
	}
	if ctx.TransactionState != CommitMessage {
		t.Errorf("state = %s, want COMMIT", ctx.TransactionState)
	}
	if ctx.MsgID != result.SendResult.MsgID {
		t.Errorf("msgId = %q, want the send result's %q", ctx.MsgID, result.SendResult.MsgID)
	}
	if ctx.BrokerAddr != broker.addr {
		t.Errorf("brokerAddr = %q, want %q", ctx.BrokerAddr, broker.addr)
	}
	if ctx.ProducerGroup != group {
		t.Errorf("producerGroup = %q", ctx.ProducerGroup)
	}
	// Java's doExecuteEndTransactionHook takes transactionId from the MESSAGE
	// (msg.getTransactionId()), not from the send result — and
	// sendMessageInTransaction has just overwritten that field with the
	// message's UNIQ_KEY. So the context sees the uniq id, while the broker
	// response's own transactionId survives only as the __transactionId__
	// property. Asserting against SendResult.TransactionID would be wrong.
	if ctx.TransactionID == "" || ctx.TransactionID != msg.TransactionID {
		t.Errorf("transactionId = %q, want the message's %q", ctx.TransactionID, msg.TransactionID)
	}
	if uniq := msg.TransactionID; uniq == result.SendResult.TransactionID {
		t.Errorf("the message's transactionId should be its UNIQ_KEY, not the response's %q", uniq)
	}
}

func TestEndTransactionHookOnCheckBackPath(t *testing.T) {
	const topic = "HookEndTxCheckTopic"
	const group = "GID_hook_endtx_check"
	p, broker, _ := startProducerFixture(t, topic, group)

	hook := &recordingEndTxHook{}
	p.RegisterEndTransactionHook(hook)

	listener := &recordingTransactionListener{executeState: Unknow, checkState: RollbackMessage}
	if _, err := p.SendMessageInTransaction(common.NewMessage(topic, []byte("tx")), listener, nil); err != nil {
		t.Fatalf("transaction send: %v", err)
	}

	ext := common.ExtFromMessage(common.NewMessage(topic, []byte("tx")))
	ext.MsgID = "0A0B0C0D0000000000000000000000AA"
	ext.PutProperty(common.PropertyProducerGroup, group)
	ext.PutProperty(common.PropertyUniqKey, "UNIQ-HOOK-1")
	body, err := common.EncodeMessageExt(ext, false)
	if err != nil {
		t.Fatalf("encode check-back body: %v", err)
	}
	checkHeader := &remoting.CheckTransactionStateRequestHeader{
		Topic:                strPtr(topic),
		CommitLogOffset:      i64Ptr(4242),
		TranStateTableOffset: i64Ptr(11),
		TransactionID:        strPtr("TID-CHECK"),
		Bname:                strPtr("b1"),
	}
	cmd := remoting.CreateRequestCommand(remoting.ReqCheckTransactionState, checkHeader)
	cmd.SetBody(body)
	cmd.MarkOnewayRPC()
	if err := broker.pushOneway(cmd); err != nil {
		t.Fatalf("push check-back: %v", err)
	}

	waitFor(t, "the end-transaction hook to fire from the check-back", func() bool {
		return len(hook.snapshot()) >= 2
	})
	ctxs := hook.snapshot()
	check := ctxs[1]
	if !check.FromTransactionCheck {
		t.Error("fromTransactionCheck must be true on the check-back path")
	}
	if check.TransactionState != RollbackMessage {
		t.Errorf("state = %s, want ROLLBACK", check.TransactionState)
	}
	// Java uses the message's UNIQ_KEY here, not its msgId.
	if check.MsgID != "UNIQ-HOOK-1" {
		t.Errorf("msgId = %q, want the UNIQ_KEY", check.MsgID)
	}
	// The context's transactionId comes from the decoded MESSAGE, and neither
	// Java's MessageDecoder nor this port encodes a transactionId into the
	// message body — so on the check-back path it is legitimately empty. The
	// transaction id only exists in the check request's HEADER (and in
	// check.MsgID above, which is the closest equivalent). Asserting the
	// header's value here would be a bug, not a fix.
	if check.TransactionID != "" {
		t.Errorf("transactionId = %q, want empty (the body carries none)", check.TransactionID)
	}
}

func TestEndTransactionHookPanicIsSwallowed(t *testing.T) {
	const topic = "HookEndTxPanicTopic"
	p, broker, _ := startProducerFixture(t, topic, "GID_hook_endtx_panic")

	hook := &recordingEndTxHook{panics: true}
	p.RegisterEndTransactionHook(hook)

	listener := &recordingTransactionListener{executeState: CommitMessage, checkState: Unknow}
	if _, err := p.SendMessageInTransaction(common.NewMessage(topic, []byte("tx")), listener, nil); err != nil {
		t.Fatalf("a panicking end-transaction hook must not fail the send: %v", err)
	}
	waitFor(t, "END_TRANSACTION despite the panicking hook", func() bool {
		return len(broker.requests(remoting.ReqEndTransaction)) >= 1
	})
}

// ---------------------------------------------------------------- pool

func TestBoundedPoolRejectsWhenQueueFull(t *testing.T) {
	release := make(chan struct{})
	started := make(chan struct{})

	pool := newBoundedPool("test", 1, 1)
	t.Cleanup(pool.Shutdown)

	// Occupies the single worker.
	if err := pool.Submit(func() {
		close(started)
		<-release
	}); err != nil {
		t.Fatalf("first submit: %v", err)
	}
	<-started
	// Fills the single queue slot.
	if err := pool.Submit(func() {}); err != nil {
		t.Fatalf("second submit must still fit: %v", err)
	}
	// Queue full: rejected, and NOT by blocking.
	if err := pool.Submit(func() {}); err == nil {
		t.Fatal("third submit must be rejected")
	} else if !strings.Contains(err.Error(), "executor rejected") {
		t.Errorf("rejection text = %q", err.Error())
	}
	close(release)
}

func TestBoundedPoolKeepsWorkerAliveAfterTaskPanic(t *testing.T) {
	pool := newBoundedPool("test-panic", 1, 4)
	t.Cleanup(pool.Shutdown)

	if err := pool.Submit(func() { panic("boom") }); err != nil {
		t.Fatalf("submit: %v", err)
	}
	ran := make(chan struct{})
	if err := pool.Submit(func() { close(ran) }); err != nil {
		t.Fatalf("submit: %v", err)
	}
	select {
	case <-ran:
	case <-time.After(2 * time.Second):
		t.Fatal("the worker died with the panicking task")
	}
}

func TestBoundedPoolSubmitOrRunInlineWhenFull(t *testing.T) {
	release := make(chan struct{})
	started := make(chan struct{})
	pool := newBoundedPool("test-inline", 1, 1)
	t.Cleanup(pool.Shutdown)

	if err := pool.Submit(func() {
		close(started)
		<-release
	}); err != nil {
		t.Fatalf("submit: %v", err)
	}
	<-started
	if err := pool.Submit(func() {}); err != nil {
		t.Fatalf("fill the queue: %v", err)
	}
	var ran atomic.Bool
	pool.SubmitOrRunInline(func() { ran.Store(true) })
	if !ran.Load() {
		t.Fatal("a rejected task must run inline instead of being dropped")
	}
	close(release)
}
