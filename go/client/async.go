// Async send: the real retry chain, the two back-pressure semaphores and the
// two bounded executors.
//
// Scope of this file: everything that ONLY the ASYNC send path uses. The six
// synchronous send paths live in producer.go; the transport is shared.
//
// Why the chain is a separate implementation rather than a call into the sync
// one: the two differ in where the send hook fires and in WHO retries. The sync
// path runs the whole before/after hook pair per ATTEMPT inside its own retry
// loop and consults retryResponseCodes; async builds one request, fires the
// hook pair ONCE at completion, and retries inside onExceptionImpl without ever
// looking at retryResponseCodes.
package client

import (
	"fmt"
	"runtime"
	"time"

	"github.com/zhaohai666/rocketmq-client-remoting/go/common"
	"github.com/zhaohai666/rocketmq-client-remoting/go/remoting"
)

// Defaults for the async path, the two back-pressure floors and the two pool
// sizes.
const (
	// DefaultRetryTimesWhenSendAsyncFailed is the async retry budget
	// (retryTimesWhenSendAsyncFailed).
	// Deliberately a SEPARATE knob from RetryTimesWhenSendFailed: the async chain
	// never reads the sync one.
	DefaultRetryTimesWhenSendAsyncFailed = 2
	// DefaultBackPressureForAsyncSendNum bounds in-flight async messages.
	DefaultBackPressureForAsyncSendNum = 1024
	// DefaultBackPressureForAsyncSendSize bounds in-flight async bytes (100M).
	DefaultBackPressureForAsyncSendSize = 100 * 1024 * 1024
	// DefaultAsyncSenderQueueCapacity is the hardcoded sender queue depth (50000)
	// of the classic async producer.
	DefaultAsyncSenderQueueCapacity = 50000

	// MinAsyncSendNum / MinAsyncSendSize are the floors applied when the
	// semaphores are built. The branch is written as `if (cfg > 10) … else …`,
	// so a value EQUAL to the floor also takes the floor branch.
	MinAsyncSendNum  = 10
	MinAsyncSendSize = 1024 * 1024

	// defaultCallbackExecutorThreads is the floor applied when
	// callbackExecutorThreads is configured as <= 0. The DEFAULT value
	// of that config is availableProcessors, so the two constants say different
	// things: unset means NumCPU, an explicit 0 means 4.
	defaultCallbackExecutorThreads = 4

	// callbackPoolQueueCapacity stands in for the classic callback pool's queue,
	// which is UNBOUNDED (newFixedThreadPool) and therefore never rejects. A Go
	// channel must be sized, so this is "very large" plus the inline fallback in
	// executeOnCallbackThread. Copying the unbounded original would be a memory
	// leak under sustained overload, not a behaviour.
	callbackPoolQueueCapacity = 1 << 16
)

func floorAsyncSendNum(n int) int {
	if n < MinAsyncSendNum {
		return MinAsyncSendNum
	}
	return n
}

func floorAsyncSendSize(n int) int {
	if n < MinAsyncSendSize {
		return MinAsyncSendSize
	}
	return n
}

// initAsyncConfig seeds the async knobs and builds the two semaphores. Called
// from NewDefaultMQProducer: the semaphores belong to construction, seeded
// from the producer's already-configured values.
func (p *DefaultMQProducer) initAsyncConfig() {
	p.enableBackpressureForAsyncMode.Store(false)
	p.backPressureForAsyncSendNum.Store(DefaultBackPressureForAsyncSendNum)
	p.backPressureForAsyncSendSize.Store(DefaultBackPressureForAsyncSendSize)
	p.retryTimesWhenSendAsyncFailed.Store(DefaultRetryTimesWhenSendAsyncFailed)
	p.asyncSenderQueueCapacity.Store(DefaultAsyncSenderQueueCapacity)
	p.callbackExecutorThreads.Store(int64(runtime.NumCPU()))
	p.semaphoreAsyncSendNum = newFairSemaphore(floorAsyncSendNum(DefaultBackPressureForAsyncSendNum))
	p.semaphoreAsyncSendSize = newFairSemaphore(floorAsyncSendSize(DefaultBackPressureForAsyncSendSize))
}

// ---------------------------------------------------------------- configuration

// SetEnableBackpressureForAsyncMode arms the async back-pressure gate
// (default false).
func (p *DefaultMQProducer) SetEnableBackpressureForAsyncMode(enable bool) {
	p.enableBackpressureForAsyncMode.Store(enable)
}

func (p *DefaultMQProducer) IsEnableBackpressureForAsyncMode() bool {
	return p.enableBackpressureForAsyncMode.Load()
}

// SetBackPressureForAsyncSendNum retunes the in-flight MESSAGE-COUNT budget.
// It floors at MinAsyncSendNum and re-derives the semaphore total as
// `newTotal - acquired`, where
// acquired = oldTotal - availablePermits. Shifting the total in place yields
// exactly the same free count, so the documented assertions
// (in-flight preserved, free = total - acquired)
// still hold here.
func (p *DefaultMQProducer) SetBackPressureForAsyncSendNum(n int) {
	n = floorAsyncSendNum(n)
	p.backPressureForAsyncSendNum.Store(int64(n))
	p.semaphoreAsyncSendNum.setTotalPermits(n)
}

func (p *DefaultMQProducer) BackPressureForAsyncSendNum() int {
	return int(p.backPressureForAsyncSendNum.Load())
}

// SetBackPressureForAsyncSendSize retunes the in-flight BYTE budget.
func (p *DefaultMQProducer) SetBackPressureForAsyncSendSize(n int) {
	n = floorAsyncSendSize(n)
	p.backPressureForAsyncSendSize.Store(int64(n))
	p.semaphoreAsyncSendSize.setTotalPermits(n)
}

func (p *DefaultMQProducer) BackPressureForAsyncSendSize() int {
	return int(p.backPressureForAsyncSendSize.Load())
}

// SetRetryTimesWhenSendAsyncFailed sets the ASYNC retry budget (the first
// attempt is not counted). Only the async chain reads it; the sync chain uses
// SetRetryTimesWhenSendFailed.
func (p *DefaultMQProducer) SetRetryTimesWhenSendAsyncFailed(n int) {
	p.retryTimesWhenSendAsyncFailed.Store(int64(n))
}

func (p *DefaultMQProducer) RetryTimesWhenSendAsyncFailed() int {
	return int(p.retryTimesWhenSendAsyncFailed.Load())
}

// SetAsyncSenderQueueCapacity sizes the sender queue. Like the classic producer
// (which hardcodes 50000 at construction) it is read once, when the pool
// is built in Start.
func (p *DefaultMQProducer) SetAsyncSenderQueueCapacity(n int) {
	p.asyncSenderQueueCapacity.Store(int64(n))
}

func (p *DefaultMQProducer) AsyncSenderQueueCapacity() int {
	return int(p.asyncSenderQueueCapacity.Load())
}

// SetCallbackExecutorThreads sizes the pool the user callback and the after
// hooks run on. <= 0 means the
// floor of 4; the default is NumCPU.
func (p *DefaultMQProducer) SetCallbackExecutorThreads(n int) {
	p.callbackExecutorThreads.Store(int64(n))
}

func (p *DefaultMQProducer) CallbackExecutorThreads() int {
	return int(p.callbackExecutorThreads.Load())
}

// SemaphoreAsyncSendNumAvailablePermits reports the count gate's free permits.
// The count may be negative after a
// shrink (a semaphore initialised with a negative count is legal too).
func (p *DefaultMQProducer) SemaphoreAsyncSendNumAvailablePermits() int {
	return p.semaphoreAsyncSendNum.availablePermits()
}

func (p *DefaultMQProducer) SemaphoreAsyncSendSizeAvailablePermits() int {
	return p.semaphoreAsyncSendSize.availablePermits()
}

// ---------------------------------------------------------------- executors

// initAsyncExecutors builds the two executors the async chain runs on:
//
//   - AsyncSenderExecutor_N: moves the async prologue (route lookup, queue
//     selection, request building, the retry chain) off the caller's goroutine.
//     Its queue is BOUNDED — a full queue is the "executor rejected" signal.
//   - NettyClientPublicExecutor_N: where the user callback and the after hook
//     run, so business code never occupies the transport's read or timeout
//     goroutines.
//
// The classic implementation builds both executors at construction time.
// Building them in Start and tearing them down in Shutdown is a
// deliberate divergence: a shut-down producer should have nothing of its own
// still running, and it makes Shutdown-then-Start usable.
func (p *DefaultMQProducer) initAsyncExecutors() {
	workers := runtime.NumCPU()
	if workers < 1 {
		workers = 1
	}
	p.asyncSenderPool.Store(newBoundedPool("AsyncSenderExecutor", workers,
		int(p.asyncSenderQueueCapacity.Load())))

	callbackWorkers := int(p.callbackExecutorThreads.Load())
	if callbackWorkers <= 0 {
		callbackWorkers = defaultCallbackExecutorThreads
	}
	p.callbackPool.Store(newBoundedPool("NettyClientPublicExecutor", callbackWorkers,
		callbackPoolQueueCapacity))
}

// destroyAsyncExecutors drains both pools (ExecutorService.shutdown semantics:
// stop accepting, let the queued tasks finish). Order matters — the sender pool
// first, so no new attempt can be dispatched, then the callback pool, which is
// where the terminal handler and therefore every retry runs.
//
// ⚠ Must be called WITHOUT p.mu held. Draining waits for in-flight tasks, and
// those take p.mu themselves (requireClient, hookSnapshot), so holding it here
// would deadlock against them.
func (p *DefaultMQProducer) destroyAsyncExecutors() {
	if pool := p.asyncSenderPool.Swap(nil); pool != nil {
		pool.Shutdown()
	}
	if pool := p.callbackPool.Swap(nil); pool != nil {
		pool.Shutdown()
	}
}

// ---------------------------------------------------------------- entry points

// SendAsync sends without blocking: the whole chain runs in the background and
// `callback` fires EXACTLY once, on the callback executor.
//
// The returned error is the caller-visible failure set — the three things
// reported to the CALLER rather than through the callback: a producer that is not running,
// a full sender queue (the "executor rejected " client error),
// and a nil message. Every other failure —
// local validation, no route, a broker error, the retry budget running out —
// goes to callback.OnException, exactly as the reference chain routes it from
// inside the runnable's catch.
func (p *DefaultMQProducer) SendAsync(msg *common.Message, callback SendCallback) error {
	return p.SendAsyncWithTimeout(msg, callback, p.sendMsgTimeout)
}

// SendAsyncWithTimeout is SendAsync with an explicit budget. The budget covers
// the WHOLE chain — queueing, every retry attempt and the callback — not each
// attempt separately: one Stopwatch starts at `beginStartTime`
// and every retry spends from that same remainder.
func (p *DefaultMQProducer) SendAsyncWithTimeout(msg *common.Message, callback SendCallback, timeoutMillis int64) error {
	return p.dispatchAsync(&asyncRequest{msg: msg, callback: callback, timeout: timeoutMillis})
}

// SendAsyncToQueue sends asynchronously to a PINNED queue.
func (p *DefaultMQProducer) SendAsyncToQueue(msg *common.Message, mq common.MessageQueue, callback SendCallback) error {
	return p.SendAsyncToQueueWithTimeout(msg, mq, callback, p.sendMsgTimeout)
}

func (p *DefaultMQProducer) SendAsyncToQueueWithTimeout(msg *common.Message, mq common.MessageQueue, callback SendCallback, timeoutMillis int64) error {
	return p.dispatchAsync(&asyncRequest{msg: msg, mq: &mq, callback: callback, timeout: timeoutMillis})
}

// SendAsyncBySelector sends asynchronously to the queue a selector picks.
//
// The two classic selector-send variants are NOT the same
// implementation: the non-deprecated 4-arg form runs the route lookup and the
// selection on the CALLER's thread, while the 5-arg timeout form does it inside
// the runnable. This one follows the latter — a cold route would otherwise make
// an "async" send block on a nameserver RPC. The kernel is entered with a nil
// publish info either way, so a failed attempt can only be
// retried on the SAME broker: there is no route to pick another one from.
func (p *DefaultMQProducer) SendAsyncBySelector(msg *common.Message, selector MessageQueueSelector, arg any, callback SendCallback) error {
	return p.SendAsyncBySelectorWithTimeout(msg, selector, arg, callback, p.sendMsgTimeout)
}

func (p *DefaultMQProducer) SendAsyncBySelectorWithTimeout(msg *common.Message, selector MessageQueueSelector, arg any, callback SendCallback, timeoutMillis int64) error {
	return p.dispatchAsync(&asyncRequest{
		msg: msg, selector: selector, selectorArg: arg, callback: callback, timeout: timeoutMillis,
	})
}

// SendAsyncBatch sends a batch asynchronously.
func (p *DefaultMQProducer) SendAsyncBatch(messages []*common.Message, callback SendCallback) error {
	return p.SendAsyncBatchWithTimeout(messages, callback, p.sendMsgTimeout)
}

func (p *DefaultMQProducer) SendAsyncBatchWithTimeout(messages []*common.Message, callback SendCallback, timeoutMillis int64) error {
	return p.dispatchAsync(&asyncRequest{batch: messages, callback: callback, timeout: timeoutMillis})
}

// SendAsyncBatchToQueue sends a batch asynchronously to a pinned queue.
func (p *DefaultMQProducer) SendAsyncBatchToQueue(messages []*common.Message, mq common.MessageQueue, callback SendCallback) error {
	return p.SendAsyncBatchToQueueWithTimeout(messages, mq, callback, p.sendMsgTimeout)
}

func (p *DefaultMQProducer) SendAsyncBatchToQueueWithTimeout(messages []*common.Message, mq common.MessageQueue, callback SendCallback, timeoutMillis int64) error {
	return p.dispatchAsync(&asyncRequest{
		batch: messages, mq: &mq, callback: callback, timeout: timeoutMillis,
	})
}

// ---------------------------------------------------------------- chain state

// asyncRequest is one async send request. Exactly one of msg / batch is set, and
// mq / selector are alternatives for the pinned-queue and selector flavours.
type asyncRequest struct {
	msg         *common.Message
	batch       []*common.Message
	mq          *common.MessageQueue
	selector    MessageQueueSelector
	selectorArg any
	callback    SendCallback
	timeout     int64
}

// asyncChain is the state of one in-flight async send, threaded as separate
// parameters through the classic kernel's success and failure handlers.
// Grouping it keeps the Go signatures readable;
// `times` counts FAILED attempts, so the retry
// budget is checked as `times <= retryTimesWhenSendAsyncFailed`.
type asyncChain struct {
	inst     *Instance
	msg      *common.Message
	mq       common.MessageQueue
	request  *remoting.RemotingCommand
	publish  *TopicPublishInfo
	isBatch  bool
	sysFlag  int32
	hooks    []SendMessageHook
	ctx      *SendMessageContext
	callback SendCallback

	// timeout is the REMAINING budget, shared by every attempt.
	timeout int64
	times   int

	// Permit bookkeeping for the back-pressure gates.
	// msgLen is computed on the CALLER's goroutine from the PRE-compression
	// body (the classic chain computes it before the runnable
	// compresses). Written on the caller and read on the worker — the pool
	// channel is the happens-before edge.
	msgLen       int
	numAcquired  bool
	sizeAcquired bool
	permitsFreed bool

	// prevBody / prevTopic keep the caller's message as it was handed over, so
	// the restore can happen at exactly ONE point: before the first attempt is
	// dispatched. Only ever touched from the sender goroutine.
	prevBody  []byte
	prevTopic string
	restored  bool
}

// restoreCallerMessage puts the caller's message back the way it was handed over
// — the classic ASYNC branch clones the
// message for the wire precisely so that it can restore the caller's copy
// immediately.
//
// ⚠ It must run BEFORE the first attempt is dispatched, and never after. The
// sync path's equivalent (sendKernelImpl's finally) runs after the send because a
// sync send is over by then; on the async path the completion handler reads
// chain.msg from another goroutine as soon as the request is in flight, so a
// late write here would be a genuine data race rather than a cosmetic difference.
func (p *DefaultMQProducer) restoreCallerMessage(chain *asyncChain) {
	if chain.restored {
		return
	}
	chain.restored = true
	chain.msg.Body = chain.prevBody
	chain.msg.Topic = common.WithoutNamespace(chain.msg.Topic, p.namespace)
}

// dispatchAsync is the shared ASYNC prologue for every entry point above. It:
//
//  1. checks the things that are thrown to the CALLER;
//  2. builds the runnable the sender executor will run;
//  3. hands it to executeAsyncMessageSend, which applies the back-pressure gate
//     on the CALLER's goroutine and then submits.
func (p *DefaultMQProducer) dispatchAsync(req *asyncRequest) error {
	if req.callback == nil {
		return common.ClientError("the send callback is null")
	}
	if req.msg == nil && len(req.batch) == 0 {
		return common.ClientError("the message is null")
	}
	// Fail on the caller's goroutine
	// rather than silently inside the runnable.
	if _, err := p.requireClient(); err != nil {
		return err
	}
	begin := time.Now()

	chain := &asyncChain{
		msg:      req.msg,
		callback: req.callback,
		timeout:  req.timeout,
	}
	if len(req.batch) > 0 {
		chain.isBatch = true
		chain.msgLen = batchBackPressureMsgLen(req.batch)
	} else {
		chain.msgLen = backPressureMsgLen(req.msg)
	}

	run := func() {
		// The cost is measured AFTER dequeueing: a budget eaten by
		// queueing means the request is never built at all.
		cost := time.Since(begin).Milliseconds()
		if req.timeout <= cost {
			p.completeAsync(chain, nil, common.TooMuchRequestError("DEFAULT ASYNC send call timeout"))
			return
		}
		chain.timeout = req.timeout - cost
		if chain.isBatch {
			p.sendBatchAsyncInner(chain, req)
			return
		}
		p.sendAsyncInner(chain, req)
	}
	return p.executeAsyncMessageSend(chain, run, req.timeout, begin)
}

// executeAsyncMessageSend applies the back-pressure gate, then the submission.
//
// Both permits are requested SEQUENTIALLY and both wait on the budget measured
// from `begin`, so the first gate alone can exhaust it. Whichever one fails
// reports its own fixed wording.
func (p *DefaultMQProducer) executeAsyncMessageSend(chain *asyncChain, run func(),
	timeoutMillis int64, begin time.Time) error {

	pool := p.asyncSenderPool.Load()
	if pool == nil {
		return common.ClientError("producer already shutdown")
	}
	backPressure := p.enableBackpressureForAsyncMode.Load()
	if backPressure {
		numBudget := timeoutMillis - time.Since(begin).Milliseconds()
		chain.numAcquired = numBudget > 0 &&
			p.semaphoreAsyncSendNum.tryAcquire(1, numBudget)
		if !chain.numAcquired {
			p.completeAsync(chain, nil,
				common.TooMuchRequestError("send message tryAcquire semaphoreAsyncNum timeout"))
			return nil
		}
		sizeBudget := timeoutMillis - time.Since(begin).Milliseconds()
		chain.sizeAcquired = sizeBudget > 0 &&
			p.semaphoreAsyncSendSize.tryAcquire(chain.msgLen, sizeBudget)
		if !chain.sizeAcquired {
			p.completeAsync(chain, nil,
				common.TooMuchRequestError("send message tryAcquire semaphoreAsyncSize timeout"))
			return nil
		}
	}
	if err := pool.Submit(run); err != nil {
		if backPressure {
			// The permits are ALREADY deducted, so run this one
			// inline — blocking the caller — to let the callback hand them back.
			// Rejecting outright would strand the capacity until the timeout,
			// and the caller is exactly who should be slowed down.
			run()
			return nil
		}
		// An executor rejection surfaces to the caller as the
		// "executor rejected" client error.
		return errPoolRejected
	}
	return nil
}

// releaseBackPressure returns the acquired permits — bytes first, then the
// count.
//
// Only what was ACTUALLY acquired is returned: when the byte gate times out
// while the count gate had already been taken, that count must go back or one
// single back-pressure engagement permanently eats a slot. The reference
// implementation achieves this with two boolean flags; the once-only guard is a
// hardening this port adds (the reference releases unconditionally, so any
// second traversal of the chain terminal would inflate the capacity).
func (p *DefaultMQProducer) releaseBackPressure(chain *asyncChain) {
	if chain.permitsFreed {
		return
	}
	chain.permitsFreed = true
	if chain.sizeAcquired {
		p.semaphoreAsyncSendSize.release(chain.msgLen)
	}
	if chain.numAcquired {
		p.semaphoreAsyncSendNum.release(1)
	}
}

// executeOnCallbackThread runs a completion handler on the callback executor,
// falling back to the CALLING goroutine when the pool is gone or its queue is
// full — the same inline fallback the classic callback dispatch has.
func (p *DefaultMQProducer) executeOnCallbackThread(task func()) {
	pool := p.callbackPool.Load()
	if pool == nil {
		task()
		return
	}
	pool.SubmitOrRunInline(task)
}

// completeAsync is the ONE terminal step of the async chain. Every path — the
// budget gate, a local validation failure, a broker error, an exhausted retry
// budget — funnels through here, which is what makes "the callback fires exactly
// once" true by construction.
//
// Order, fixed by the chain: SendMessageHook.after → return the permits → user
// callback. The permits must go back BEFORE the user callback, or a callback
// that fires another async send would consume one extra slot.
//
// Deliberate divergence: the reference chain fires SendMessageHook.after TWICE
// for an async send — once right after the request is dispatched with
// `sendResult == null` (the ASYNC branch of the switch there never
// assigns sendResult, so the unguarded block runs with null), and once here with
// the real outcome. The trace hook happens to survive that only because
// its implementation returns early on a null result. Firing it once
// with the real outcome is what a hook can actually act on.
func (p *DefaultMQProducer) completeAsync(chain *asyncChain, result *SendResult, err error) {
	if chain.ctx != nil {
		if err != nil {
			chain.ctx.Exception = err
		} else {
			chain.ctx.SendResult = result
		}
		executeSendMessageHooksAfter(chain.hooks, chain.ctx)
	}
	p.releaseBackPressure(chain)

	if chain.callback == nil {
		return
	}
	// Wrap both callback calls in a recover: a panic out of user
	// code must not take a callback-pool worker down with it.
	defer func() {
		if r := recover(); r != nil {
			common.LogWarnf("async send callback panicked: %v", r)
		}
	}()
	if err != nil {
		chain.callback.OnException(err)
		return
	}
	chain.callback.OnSuccess(result)
}

// ---------------------------------------------------------------- async kernel

// sendAsyncInner is the async counterpart of sendDefaultImpl for ASYNC,
// including the pinned-queue prologue.
//
// Note what is NOT here that the sync version has: the retry loop. The chain
// fixes timesTotal at 1 when the mode is ASYNC, so the body runs exactly once
// and every broker switch happens in the failure handler instead.
//
// It is also the only place the caller's message is mutated, and the restore is
// guaranteed to have happened by the time prepareAsyncSend dispatches — see
// restoreCallerMessage for why that ordering is load-bearing.
func (p *DefaultMQProducer) sendAsyncInner(chain *asyncChain, req *asyncRequest) {
	chain.prevBody = chain.msg.Body
	chain.prevTopic = chain.msg.Topic
	chain.msg.Topic = p.withNamespace(chain.msg.Topic)
	if err := p.prepareAsyncSend(chain, req); err != nil {
		p.restoreCallerMessage(chain)
		p.completeAsync(chain, nil, err)
	}
}

// prepareAsyncSend is everything between "the caller's message may be mutated" and
// "the first attempt is in flight": validation, queue selection, compression, the
// kernel, and finally the dispatch. It returns an error for every outcome that
// never reached the transport, so the single caller can restore and complete.
func (p *DefaultMQProducer) prepareAsyncSend(chain *asyncChain, req *asyncRequest) error {
	msg := chain.msg
	if err := p.checkMessage(msg); err != nil {
		return err
	}

	if req.selector != nil {
		selected, err := p.invokeMessageQueueSelector(msg, req.selector, req.selectorArg, chain.timeout)
		if err != nil {
			return err
		}
		chain.mq = selected
	} else if req.mq != nil {
		// The ASYNC branch guards the pinned queue with its own
		// wording, deliberately different from the sync one.
		if err := p.checkPinnedTopic(msg.Topic, *req.mq, pinnedTopicMismatchAsync); err != nil {
			return err
		}
		chain.mq = common.NewMessageQueue(req.mq.Topic, req.mq.BrokerName, req.mq.QueueID)
	} else {
		publish, err := p.topicPublishInfo(msg.Topic)
		if err != nil {
			// Same as the sync path: an error that already carries a code (10004
			// "no nameserver") passes through unchanged; everything else is typed
			// NOT_FOUND_TOPIC_EXCEPTION. Rewriting 10004 to 10005 would erase the
			// distinction, and the async and sync chains must not diverge here.
			code := common.NotFoundTopicException
			if existing, ok := responseCodeOf(err); ok {
				code = existing
			}
			return common.ClientErrorCode(code, err.Error())
		}
		selected, ok, selErr := p.faultStrategy.selectOneMessageQueue(publish, "", false)
		if selErr != nil {
			return selErr
		}
		if !ok {
			return common.ClientError(fmt.Sprintf(
				"Send [0] times, still failed, Topic: %s, BrokersSent: []", msg.Topic))
		}
		chain.publish = publish
		chain.mq = common.NewMessageQueue(msg.Topic, selected.BrokerName, selected.QueueID)
	}

	// Compression happens ONCE, outside the retry chain: compressing per attempt
	// would feed an already-compressed stream back in (zlib(zlib(x))).
	chain.sysFlag = p.tryToCompressMessage(msg, false)
	return p.sendKernelAsync(chain)
}

// invokeMessageQueueSelector applies the caller's selector: the
// selector sees the message WITHOUT the namespace, and the queue it returns gets
// the namespace applied. A selector that throws becomes "select message queue
// threw exception." rather than leaking its own error type.
func (p *DefaultMQProducer) invokeMessageQueueSelector(msg *common.Message, selector MessageQueueSelector,
	arg any, timeoutMillis int64) (common.MessageQueue, error) {

	begin := time.Now()
	publish, err := p.topicPublishInfo(msg.Topic)
	if err != nil {
		code := common.NotFoundTopicException
		if existing, ok := responseCodeOf(err); ok {
			code = existing
		}
		return common.MessageQueue{}, common.ClientErrorCode(code, err.Error())
	}
	var selected common.MessageQueue
	func() {
		defer func() {
			if r := recover(); r != nil {
				err = common.ClientError(fmt.Sprintf("select message queue threw exception. %v", r))
			}
		}()
		selected, err = selector.Select(publish.MsgQueueList(), msg, arg)
	}()
	if err != nil {
		return common.MessageQueue{}, err
	}
	// The budget rule: `if (timeout < costTime) throw
	// RemotingTooMuchRequestException("sendSelectImpl call timeout")` — the route
	// lookup and the selector are on the send budget too.
	if timeoutMillis < time.Since(begin).Milliseconds() {
		return common.MessageQueue{}, common.TooMuchRequestError("sendSelectImpl call timeout")
	}
	if selected.Topic == "" {
		return common.MessageQueue{}, common.ClientError("select message queue return null.")
	}
	return selected, nil
}

// sendBatchAsyncInner sends a batch on the sender executor: the classic batch
// send builds a MessageBatch and runs it through DEFAULT ASYNC.
//
// This port's batch kernel is synchronous only (SEND_BATCH_MESSAGE over
// InvokeSync), so "async" here means "run the synchronous batch kernel on the
// sender executor and deliver only the RESULT through the callback".
// The caller-visible contract is
// unchanged — non-blocking, callback on the callback executor — but note the
// consequence: the batch inherits the SYNC retry rules (retryTimesWhenSendFailed
// and retryResponseCodes) rather than the per-attempt async chain.
func (p *DefaultMQProducer) sendBatchAsyncInner(chain *asyncChain, req *asyncRequest) {
	var pinned *common.MessageQueue
	if req.mq != nil {
		pinned = req.mq
	}
	// The async pinned wording, not the sync one — the batch is on the async
	// path.
	result, err := p.sendBatch(req.batch, pinned, chain.timeout, pinnedTopicMismatchAsync)
	p.executeOnCallbackThread(func() { p.completeAsync(chain, result, err) })
}

// sendKernelAsync is the send kernel restricted to ASYNC: resolve
// the publish address, run the forbidden hooks, build the hook context, build
// the request ONCE, re-check the budget, restore the caller's message, dispatch.
//
// It returns an error for every outcome that never reached the transport; the
// single caller (sendAsyncInner) is the one that restores and completes, so the
// ordering rule documented on restoreCallerMessage holds for all of them.
func (p *DefaultMQProducer) sendKernelAsync(chain *asyncChain) error {
	inst, err := p.requireClient()
	if err != nil {
		return err
	}
	chain.inst = inst
	// The publish address table (master only), then
	// "The broker[X] not exist". For a pinned send this is the ONLY route source
	// — sendDefaultImpl never fetched publish info for it — so skipping it would
	// make the first pinned send resolve an empty address.
	addr, err := inst.PublishAddrFor(chain.mq.BrokerName, chain.mq.Topic)
	if err != nil {
		return err
	}
	// The kernel takes a FRESH beginStartTime, so this gate
	// measures only the kernel's own work (hooks, compression, request build),
	// not the chain's. That is why the check compares the incoming budget against
	// an almost-empty elapsed time and, unlike the dequeue gate, practically
	// never fires.
	beginKernel := time.Now()

	sendHooks, forbiddenHooks, _ := p.hookSnapshot()
	chain.hooks = sendHooks
	if err := p.checkForbidden(forbiddenHooks, chain.msg, chain.mq, addr, CommunicationModeAsync); err != nil {
		return err
	}
	// W3C traceparent passthrough (opt-in), same position as the sync path.
	// ⚠ This runs on chain.msg, which restoreCallerMessage has not yet touched,
	// and the property it writes survives every retry — the request is built
	// once below and reused.
	if p.enableTraceContext {
		InjectTraceContext(chain.msg)
	}
	if len(sendHooks) > 0 {
		chain.ctx = p.buildSendContext(chain.msg, chain.mq, addr, CommunicationModeAsync)
		executeSendMessageHooksBefore(sendHooks, chain.ctx)
	}
	// The request is built ONCE and reused by every retry: the failure handler
	// only swaps the opaque. The header therefore keeps the FIRST
	// attempt's queueId across retries — that is intended behaviour, not a bug
	// to tidy up.
	chain.request = p.buildSendRequest(chain.msg, chain.isBatch, chain.mq, chain.sysFlag)

	costAsync := time.Since(beginKernel).Milliseconds()
	if chain.timeout < costAsync {
		return common.TooMuchRequestError("sendKernelImpl call timeout")
	}
	chain.timeout -= costAsync

	// ⚠ The ONE restore point. It has to sit after buildSendRequest — which has
	// already aliased the compressed body into the request, so putting the
	// caller's slice back cannot affect the bytes on the wire — and before the
	// dispatch, because from the dispatch onwards chain.msg is read by the
	// completion handler on another goroutine.
	p.restoreCallerMessage(chain)
	p.sendMessageAsyncOnce(chain, addr, chain.mq.BrokerName)
	return nil
}

// sendMessageAsyncOnce is ONE in-flight attempt. The retry, if any, is decided
// in onSendExceptionAsync from the completion handler.
func (p *DefaultMQProducer) sendMessageAsyncOnce(chain *asyncChain, addr, brokerName string) {
	began := time.Now()
	chain.inst.Remoting().InvokeAsync(p.sendAddr(addr), chain.request,
		func(response *remoting.RemotingCommand, err error) {
			// The whole completion — hook.after, the fault record and
			// any retry — belongs on the callback executor rather than the
			// transport's callback path. Route it there too, so user code never
			// runs on a transport goroutine.
			p.executeOnCallbackThread(func() {
				cost := time.Since(began).Milliseconds()
				if err != nil {
					// Go's transport has no synchronous throw: InvokeAsync runs
					// the same code as InvokeSync on its own goroutine and always
					// reports through the callback. The reference chain
					// therefore has TWO failure paths with different fault
					// records — an outer catch (reachable=false, error passed
					// through UNWRAPPED, retry allowed) and the operation-fail
					// callback (reachable=true, classified per type). The thing
					// that separates them is whether a request could be written at
					// all, which is exactly "the channel could not be created" —
					// KindConnect here.
					if common.IsKind(err, common.KindConnect) {
						p.updateFaultItem(brokerName, began, true, false)
						p.onSendExceptionAsync(chain, brokerName, chain.timeout-cost, err, true)
						return
					}
					p.updateFaultItem(brokerName, began, true, true)
					wrapped, needRetry := classifyAsyncFailure(err, cost)
					common.LogWarnf("async send to broker %s failed: %v (reported as %v)",
						brokerName, err, wrapped)
					p.onSendExceptionAsync(chain, brokerName, chain.timeout-cost, wrapped, needRetry)
					return
				}
				result, perr := processSendResponse(response, chain.msg, chain.mq)
				if perr != nil {
					// The response-arrived catch: the broker DID answer,
					// but the response is unusable. needRetry stays false and the
					// error is passed through unwrapped — so unlike the sync
					// path, an async send never consults retryResponseCodes: an
					// explicit broker error is final.
					p.updateFaultItem(brokerName, began, true, true)
					p.completeAsync(chain, nil, perr)
					return
				}
				p.updateFaultItem(brokerName, began, false, true)
				p.completeAsync(chain, result, nil)
			})
		}, chain.timeout)
}

// classifyAsyncFailure mirrors the branches of the reference chain's
// operation-fail callback: the message text and whether another broker is worth
// trying, decided by error TYPE.
//
// ⚠ Only errors coming back from the TRANSPORT take this route. An error carried
// by a response that DID arrive (processSendResponse → MQBrokerException) does
// not: the response-arrived path sets needRetry=false and rethrows it
// as-is. So an async send ignores retryResponseCodes entirely — a broker that
// answered with an explicit error code is never retried elsewhere. Do not
// conflate this with the sync chain.
func classifyAsyncFailure(err error, costMillis int64) (error, bool) {
	switch {
	case common.IsKind(err, common.KindSendRequest):
		return asyncWrapError("send request failed", err), true
	case common.IsKind(err, common.KindTimeout):
		return asyncWrapError(fmt.Sprintf("wait response timeout, cost=%d", costMillis), err), true
	}
	// The fallback branch covers every other error the fail callback can be
	// handed, i.e. the remaining transport failures. A non-transport error cannot
	// legitimately reach here (a failure carried by a response is completed
	// before classification), so it is passed through unwrapped rather than
	// guessed at.
	if common.IsKind(err, common.KindConnect) || common.IsKind(err, common.KindRemotingCommand) ||
		common.IsKind(err, common.KindTooMuchRequest) || common.IsKind(err, common.KindIO) {
		return asyncWrapError("unknown reason", err), !common.IsKind(err, common.KindTooMuchRequest)
	}
	return err, false
}

// asyncWrapError builds the wrapped client error the caller sees: the text
// is what a user sees, and the original transport error stays reachable through
// errors.Unwrap (common.Error.Cause).
func asyncWrapError(message string, cause error) error {
	e := common.ClientError(message)
	e.Cause = cause
	return e
}

// onSendExceptionAsync decides the async retry: switch to
// another broker's queue while the budget lasts, otherwise terminate.
func (p *DefaultMQProducer) onSendExceptionAsync(chain *asyncChain, brokerName string,
	timeoutMillis int64, err error, needRetry bool) {

	chain.times++
	if needRetry && chain.times <= p.RetryTimesWhenSendAsyncFailed() && timeoutMillis > 0 {
		retryBroker := brokerName
		if chain.publish != nil {
			// selectOneMessageQueue(publish, brokerName, false).
			// The third argument being false is load-bearing: the
			// lastBrokerName filter steers away from the broker that just failed,
			// but the round-robin cursor is NOT reset — that is the SYNC loop's
			// resetIndex = times > 0 rule, and borrowing it here would change
			// which queue the retry lands on.
			selected, ok, selErr := p.faultStrategy.selectOneMessageQueue(chain.publish, brokerName, false)
			if selErr == nil && ok {
				retryBroker = selected.BrokerName
			}
		}
		addr, ok := chain.inst.FindBrokerAddressInPublish(retryBroker)
		if !ok || addr == "" {
			// Only the publish address table (master only) is consulted and
			// the route is NOT refreshed. A miss there is fed into the async
			// invoke as an empty address, which fails and burns the remaining
			// retry budget on
			// the same dead address over and over. Terminating with the actual
			// reason is more useful and cannot loop.
			p.completeAsync(chain, nil,
				common.ClientError(fmt.Sprintf("The broker[%s] not exist", retryBroker)))
			return
		}
		common.LogWarnf("async send msg by retry %d times. topic=%s, brokerAddr=%s, brokerName=%s: %v",
			chain.times, chain.msg.Topic, addr, retryBroker, err)
		chain.timeout = timeoutMillis
		// Fresh opaque: the previous request is still sitting in the response
		// table waiting for its own timeout, so reusing its opaque would cross
		// the two attempts' answers.
		chain.request.Opaque = remoting.NextOpaque()
		p.sendMessageAsyncOnce(chain, addr, retryBroker)
		return
	}
	p.completeAsync(chain, nil, err)
}

// backPressureMsgLen is how many BYTE permits this send consumes: the body
// length, with a floor of 1.
//
// The reference formula (`body == null ? 1 : body.length`) charges 1 for a NULL
// body but 0 for an EMPTY one, which makes the byte gate free for a stream of
// empty messages; this port always charges at least 1.
func backPressureMsgLen(msg *common.Message) int {
	if len(msg.Body) == 0 {
		return 1
	}
	return len(msg.Body)
}

// batchBackPressureMsgLen charges one permit per sub-message: the batch is a
// single MessageBatch on the wire so there is no per-message reference formula,
// and charging the batch a single permit would make the byte gate meaningless
// for batches.
func batchBackPressureMsgLen(messages []*common.Message) int {
	if len(messages) == 0 {
		return 1
	}
	total := 0
	for _, m := range messages {
		total += backPressureMsgLen(m)
	}
	return total
}
