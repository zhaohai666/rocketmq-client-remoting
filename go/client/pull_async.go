// Async pull: PullAsync runs one Pull off the caller's goroutine and delivers
// the outcome through a PullCallback — exactly once.
//
// The two-pool split mirrors the async send path (async.go) and the Java
// client's arrangement (pullMessageService runs the RPC, the public executor
// runs user code):
//
//   - pullAsyncPool runs the RPC (the pullKernel round trip), so the caller
//     never blocks on the wire;
//   - pullCallbackPool delivers the user callback, so business code never
//     occupies the transport's read goroutines or the pull workers.
//
// Caller-visible refusals (RETURNED, never delivered through the callback): a
// nil callback, an empty topic / unparsable subscription, a consumer that is
// not started, and a torn-down or full executor ("executor rejected"). Every
// other outcome — argument validation inside the pull, a broker error, a
// timeout — arrives through OnSuccess/OnException: exactly one of them,
// exactly once.
//
// Shutdown DRAINS both pools (ExecutorService.shutdown semantics), so a
// submit accepted before Shutdown always delivers its callback before
// Shutdown returns — the exactly-once promise holds across the shutdown
// boundary too.
package client

import (
	"github.com/zhaohai666/rocketmq-client-remoting/go/common"
	"github.com/zhaohai666/rocketmq-client-remoting/go/remoting"
)

// PullCallback receives the outcome of an asynchronous pull — the port of
// org.apache.rocketmq.client.consumer.PullCallback. OnSuccess and OnException
// are mutually exclusive and each fires exactly once.
type PullCallback interface {
	OnSuccess(result *PullResult)
	OnException(err error)
}

// PullCallbackFunc adapts two functions to PullCallback.
type PullCallbackFunc struct {
	SuccessFn func(*PullResult)
	ExceptFn  func(error)
}

func (c PullCallbackFunc) OnSuccess(result *PullResult) {
	if c.SuccessFn != nil {
		c.SuccessFn(result)
	}
}

func (c PullCallbackFunc) OnException(err error) {
	if c.ExceptFn != nil {
		c.ExceptFn(err)
	}
}

// PullAsync is the async flavour of Pull: a SHORT poll (the suspend bit stays
// clear) at an explicit offset, answered through the callback. The RPC budget
// is consumerPullTimeoutMillis, the same one the sync Pull uses.
func (c *DefaultMQPullConsumer) PullAsync(mq common.MessageQueue, subExpression string,
	offset int64, maxNums int32, callback PullCallback) error {

	c.mu.Lock()
	timeout := c.consumerPullTimeoutMillis
	c.mu.Unlock()
	return c.PullAsyncWithTimeout(mq, subExpression, offset, maxNums, callback, timeout)
}

// PullAsyncWithTimeout is PullAsync with an explicit RPC budget.
func (c *DefaultMQPullConsumer) PullAsyncWithTimeout(mq common.MessageQueue, subExpression string,
	offset int64, maxNums int32, callback PullCallback, timeoutMillis int64) error {

	sub, err := buildPullSubscription(mq, subExpression)
	if err != nil {
		return err
	}
	return c.dispatchPullAsync(mq, sub, offset, maxNums, false, callback, timeoutMillis)
}

// PullAsyncBySelector is the MessageSelector flavour of PullAsync: a SQL92
// expression travels with its own expressionType, exactly as on the sync path.
func (c *DefaultMQPullConsumer) PullAsyncBySelector(mq common.MessageQueue, selector MessageSelector,
	offset int64, maxNums int32, callback PullCallback) error {

	c.mu.Lock()
	timeout := c.consumerPullTimeoutMillis
	c.mu.Unlock()
	return c.PullAsyncBySelectorWithTimeout(mq, selector, offset, maxNums, callback, timeout)
}

func (c *DefaultMQPullConsumer) PullAsyncBySelectorWithTimeout(mq common.MessageQueue, selector MessageSelector,
	offset int64, maxNums int32, callback PullCallback, timeoutMillis int64) error {

	sub, err := buildPullSelectorSubscription(mq, selector)
	if err != nil {
		return err
	}
	return c.dispatchPullAsync(mq, sub, offset, maxNums, false, callback, timeoutMillis)
}

// PullAsyncBlockIfNotFound is the async LONG poll (suspend=true). The budget
// switches to consumerTimeoutMillisWhenSuspend, the same one the sync long
// poll uses — the client must outwait the broker's hold.
func (c *DefaultMQPullConsumer) PullAsyncBlockIfNotFound(mq common.MessageQueue, subExpression string,
	offset int64, maxNums int32, callback PullCallback) error {

	sub, err := buildPullSubscription(mq, subExpression)
	if err != nil {
		return err
	}
	c.mu.Lock()
	timeout := c.consumerTimeoutMillisWhenSuspend
	c.mu.Unlock()
	return c.dispatchPullAsync(mq, sub, offset, maxNums, true, callback, timeout)
}

// dispatchPullAsync is the shared prologue: the caller-side refusals, then the
// submit. The whole pull (the same pullSyncWithMaxBytes the sync entry points
// run) executes on the pullAsyncPool worker, and its outcome — success,
// broker error, timeout — is routed through deliverPullCallback.
func (c *DefaultMQPullConsumer) dispatchPullAsync(mq common.MessageQueue, sub *remoting.SubscriptionData,
	offset int64, maxNums int32, block bool, callback PullCallback, timeoutMillis int64) error {

	if callback == nil {
		return common.ClientError("the pull callback is null")
	}
	// Fail on the caller's goroutine rather than silently inside the worker.
	if _, err := c.requireClient(); err != nil {
		return err
	}
	pool := c.pullAsyncPool.Load()
	if pool == nil {
		// Shutdown already swapped the pool out: refuse synchronously instead
		// of queueing onto a dead consumer.
		return common.ClientError("consumer already shutdown")
	}
	run := func() {
		result, err := c.pullSyncWithMaxBytes(mq, sub, offset, maxNums, -1, block, timeoutMillis)
		c.deliverPullCallback(callback, result, err)
	}
	// A rejected submit — a full queue, or the pool closing mid-race with
	// Shutdown — is the async send's "executor rejected" contract: it is
	// reported to the CALLER and the callback never fires for it. An ACCEPTED
	// submit always fires the callback, exactly once.
	return pool.Submit(run)
}

// deliverPullCallback is the ONE terminal step of an async pull. Every
// accepted path funnels through here, which is what makes "the callback fires
// exactly once" true by construction, and it runs on the callback pool so user
// code never occupies a pull worker. A panic out of the callback is recovered
// and logged — it must not take a pool worker down with it.
func (c *DefaultMQPullConsumer) deliverPullCallback(callback PullCallback, result *PullResult, err error) {
	task := func() {
		defer func() {
			if r := recover(); r != nil {
				common.LogWarnf("async pull callback panicked: %v", r)
			}
		}()
		if err != nil {
			callback.OnException(err)
			return
		}
		callback.OnSuccess(result)
	}
	pool := c.pullCallbackPool.Load()
	if pool == nil {
		task()
		return
	}
	// The inline fallback keeps the exactly-once promise even when the
	// callback pool is gone or its queue is full — the same fallback the
	// async send's executeOnCallbackThread uses.
	pool.SubmitOrRunInline(task)
}
