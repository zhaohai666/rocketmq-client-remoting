// The consumer's runtime loops: one pull goroutine per assigned queue, one
// dispatch goroutine, and the two sweeps (clean-expire, orderly lock).
package client

import (
	"time"

	"github.com/zhaohai666/rocketmq-client-remoting/go/common"
	"github.com/zhaohai666/rocketmq-client-remoting/go/remoting"
)

// queuePullLoop pulls one queue until it is retired or the consumer stops.
func (c *DefaultMQPushConsumer) queuePullLoop(mq common.MessageQueue, stop chan struct{}) {
	defer func() {
		if r := recover(); r != nil {
			common.LogErrorf("pull loop panicked for %v: %v", mq, r)
		}
	}()
	orderly := c.isOrderly()
	for {
		if c.stopRequested(stop) {
			return
		}
		c.mu.Lock()
		pq, stillMine := c.processQueueTable[mq]
		c.mu.Unlock()
		if !stillMine || pq.IsDropped() {
			return
		}
		pq.TouchPull()

		if c.IsPaused() {
			if c.sleepOrStop(stop, pullTimeDelayMillsWhenSuspend) {
				return
			}
			continue
		}

		sub, hasSub := c.subscriptionFor(mq.Topic)
		if !hasSub {
			return
		}
		if orderly && !pq.IsLocked() {
			// Orderly in CLUSTERING: do not pull before LOCK_BATCH_MQ granted
			// this queue, or we would consume what another instance is about to
			// lock.
			if c.sleepOrStop(stop, 200*time.Millisecond) {
				return
			}
			continue
		}
		if c.flowControlHit(mq, pq) {
			if c.sleepOrStop(stop, pullTimeDelayMillsWhenFlowControl) {
				return
			}
			continue
		}
		offset, ok := c.pullCursor(mq)
		if !ok {
			if c.sleepOrStop(stop, time.Second) {
				return
			}
			continue
		}

		var expr *string
		if c.postSubscriptionWhenPull && !sub.ClassFilterMode {
			expr = &sub.SubString
		}
		sysFlag := common.BuildSysFlag(false, true, expr != nil, false, false)

		c.mu.Lock()
		timeout := c.pullTimeoutMillis
		suspend := c.pullSuspendTimeoutMillis
		batchNums := c.pullBatchSize
		batchBytes := c.pullBatchSizeInBytes
		c.mu.Unlock()

		begin := common.CurrentTimeMillis()
		result, err := c.pullAPI.pullKernel(c.consumerGroup, mq, offset, sub, sysFlag, 0,
			batchNums, batchBytes, suspend, timeout)
		if err != nil {
			if isTimeout(err) {
				// A long poll timing out while suspended is NORMAL: the broker
				// clamps the suspend time to its own brokerSuspendMaxTimeMillis
				// (~15s) and ignores what we sent, so an idle queue times out
				// periodically. Debug level, so the run log keeps ERROR=0.
				common.LogDebugf("pull long-poll timeout for %v (benign, will retry): %v", mq, err)
			} else {
				common.LogDebugf("pull error for %v: %v", mq, err)
				if c.sleepOrStop(stop, pullRetryDelay(err)) {
					return
				}
			}
			continue
		}
		result = c.pullAPI.processPullResult(mq, result, sub)

		c.mu.Lock()
		if !c.started {
			c.mu.Unlock()
			return
		}
		current, stillMine := c.processQueueTable[mq]
		if !stillMine || current != pq || pq.IsDropped() {
			// Revoked mid-pull: discard the batch — do not consume it and do not
			// advance the offset. The new owner redelivers it from the last
			// offset we persisted.
			c.mu.Unlock()
			common.LogDebugf("queue %v revoked during pull, discard %d fetched messages", mq, len(result.MsgFoundList))
			return
		}
		if result.PullStatus == PullFound && len(result.MsgFoundList) > 0 {
			pq.PutMessage(result.MsgFoundList)
			c.msgAccCnt[mq] = pq.MsgAccCnt()
		}
		c.offsetTable[mq] = result.NextBeginOffset
		illegal := false
		if result.PullStatus == PullOffsetIllegal {
			c.frozenOffsets[mq] = true
			if c.offsetStore != nil {
				c.offsetStore.UpdateOffset(mq, result.NextBeginOffset, false)
			}
			illegal = true
		} else {
			c.correctTagsOffsetLocked(mq, result.PullStatus, result.NextBeginOffset)
		}
		c.mu.Unlock()
		if illegal {
			c.offsetIllegalRecover(mq)
			return
		}
		if result.PullStatus == PullFound {
			c.incPullRT(mq.Topic, common.CurrentTimeMillis()-begin)
			if len(result.MsgFoundList) > 0 {
				c.incPullTPS(mq.Topic, int64(len(result.MsgFoundList)))
			}
		}
		interval := c.pullIntervalValue()
		if interval > 0 && c.sleepOrStop(stop, time.Duration(interval)*time.Millisecond) {
			return
		}
	}
}

// pullCursor reads (or resolves) the queue's next pull offset.
func (c *DefaultMQPushConsumer) pullCursor(mq common.MessageQueue) (int64, bool) {
	c.mu.Lock()
	offset, ok := c.offsetTable[mq]
	c.mu.Unlock()
	if ok {
		return offset, true
	}
	sub, hasSub := c.subscriptionFor(mq.Topic)
	if !hasSub {
		return 0, false
	}
	_ = sub
	next, err := c.computePullFromWhere(mq)
	if err != nil {
		common.LogDebugf("resolve initial offset failed for %v: %v", mq, err)
		return 0, false
	}
	if next < 0 {
		return 0, false
	}
	c.mu.Lock()
	c.offsetTable[mq] = next
	c.mu.Unlock()
	return next, true
}

// The cache gates all compare **strictly greater than** the threshold as it stands: no floor of 1 is
// clamped under it and no "0 means disabled" escape is honored, so a queue that has reached the
// threshold exactly is still allowed to pull and a threshold reads as "how many may be cached".
// Keeping the legal range of each knob is the job of the start-up config check, not of the gate.
//   - pullThresholdForQueue (1000): how many messages this queue HOLDS;
//   - pullThresholdSizeForQueue (100): how many **whole MiB of message body** it holds, so a
//     1 MiB budget tolerates 1.9 MiB and a partial MiB never trips;
//   - consumeConcurrentlyMaxSpan (2000): the offset SPAN of the held messages, so one message that
//     keeps failing cannot let the offset run away. This term is the ordered path's one exception: it
//     does not run AT ALL for orderly consumption, because an ordered pull cursor cannot move past the
//     batch still in the listener's hands, so there is no run-ahead to guard against.
//
// Everything the gates judge is what this queue still holds: the pulled-but-undispatched buffer plus
// the batches already in the listener's hands and not settled yet.
//
// Only these three per-queue gates run at pull time; nothing here ever sums the
// sibling queues of a topic. The two topic-level knobs are turned into per-queue
// ones when the assignment changes (see divideTopicThresholds), so by the time a
// pull is judged they already describe THIS queue.
func (c *DefaultMQPushConsumer) flowControlHit(mq common.MessageQueue, pq *processQueue) bool {
	reason := c.cacheGateReason(pq)
	if reason == "" {
		return false
	}
	c.mu.Lock()
	c.flowControlTriggered++
	times := c.flowControlTriggered
	c.mu.Unlock()
	common.LogDebugf("flow control: queue %v %s, pause pull", mq, reason)
	if isFlowControlWarnTrip(times) {
		common.LogWarnf("flow control: queue %v %s exceeds its cache threshold, pause pull for %s (flowControlTimes=%d)",
			mq, reason, pullTimeDelayMillsWhenFlowControl, times)
	}
	return true
}

// cacheGateReason names the per-queue gate this queue's held messages are over, or "" when it may
// pull. It judges ONE queue and reads no sibling queue: a topic-level budget reaches it
// only through the division done at rebalance (see divideTopicThresholds).
func (c *DefaultMQPushConsumer) cacheGateReason(pq *processQueue) string {
	count, bodyBytes, span, judgeSpan := pq.CacheStats()
	sizeMiB := bodyBytes / (1024 * 1024)

	c.mu.Lock()
	maxCount := c.pullThresholdForQueue
	maxSize := c.pullThresholdSizeForQueue
	maxSpan := c.consumeConcurrentlyMaxSpan
	c.mu.Unlock()

	switch {
	case int64(count) > maxCount:
		return "count"
	case sizeMiB > maxSize:
		return "size"
	case judgeSpan && span > maxSpan:
		return "span"
	}
	return ""
}

// flowControlWarnEvery is the trip count between two WARN lines: a throttled queue trips a gate on
// every pull round, so warning each round buries the signal in noise.
const flowControlWarnEvery = 1000

// isFlowControlWarnTrip reports whether the n-th (1-based) flow-control trip logs a WARN.
func isFlowControlWarnTrip(times int64) bool {
	return (times-1)%flowControlWarnEvery == 0
}

// ---------------------------------------------------------------- dispatch

// dispatchLoop takes batches out of the buffers and hands them to the listener.
func (c *DefaultMQPushConsumer) dispatchLoop() {
	for {
		select {
		case <-c.stopCh:
			return
		default:
		}
		progressed := false
		c.mu.Lock()
		mqs := make([]common.MessageQueue, 0, len(c.processQueueTable))
		for mq := range c.processQueueTable {
			mqs = append(mqs, mq)
		}
		batchMax := c.consumeMessageBatchMaxSize
		c.mu.Unlock()

		for _, mq := range mqs {
			if c.stopRequested(nil) {
				return
			}
			c.mu.Lock()
			pq, ok := c.processQueueTable[mq]
			if !ok {
				c.mu.Unlock()
				continue
			}
			batch := pq.TakeBatch(maxInt(1, batchMax))
			epoch := c.queueEpoch[mq]
			c.mu.Unlock()
			if len(batch) == 0 {
				continue
			}
			progressed = true
			if !c.beginInFlight() {
				// Shutdown froze new work: hand the batch back instead of
				// consuming it. Its offset was never advanced, so the broker
				// redelivers it on the next start.
				if pq := c.processQueueOf(mq); pq != nil {
					pq.RequeueBatch(batch)
				}
				return
			}
			if c.isOrderly() {
				func() {
					defer c.inFlight.Done()
					c.consumeBatch(mq, batch, epoch)
				}()
				continue
			}
			select {
			case c.dispatchSem <- struct{}{}:
			case <-c.stopCh:
				c.inFlight.Done()
				return
			}
			go func(mq common.MessageQueue, batch []*common.MessageExt, epoch uint64) {
				defer func() {
					<-c.dispatchSem
					if r := recover(); r != nil {
						common.LogErrorf("dispatch batch panicked for %v (requeued): %v", mq, r)
						if pq := c.processQueueOf(mq); pq != nil {
							pq.RequeueBatch(batch)
						}
					}
					c.inFlight.Done()
				}()
				c.consumeBatch(mq, batch, epoch)
			}(mq, batch, epoch)
		}
		if !progressed {
			select {
			case <-c.stopCh:
				return
			case <-time.After(50 * time.Millisecond):
			}
		}
	}
}

// ---------------------------------------------------------------- sweeps

func (c *DefaultMQPushConsumer) cleanExpireLoop() {
	period := time.Duration(maxInt64(1, c.consumeTimeoutValue())) * time.Minute
	if c.sleepOrStop(nil, period) {
		return
	}
	for {
		if !c.IsStarted() {
			return
		}
		if !c.beginInFlight() {
			// Shutdown froze new work; a sweep started now would run its
			// send-backs after the drain window.
			return
		}
		func() {
			defer c.inFlight.Done()
			defer func() {
				if r := recover(); r != nil {
					common.LogErrorf("scheduleAtFixedRate cleanExpireMsg exception: %v", r)
				}
			}()
			c.cleanExpiredMsgOnce()
		}()
		if c.sleepOrStop(nil, period) {
			return
		}
	}
}

func (c *DefaultMQPushConsumer) lockLoop() {
	for {
		func() {
			defer func() {
				if r := recover(); r != nil {
					common.LogDebugf("lock mq error: %v", r)
				}
			}()
			c.lockMQOnce()
		}()
		if c.sleepOrStop(nil, 20*time.Second) {
			return
		}
	}
}

// ---------------------------------------------------------------- producer

func (c *DefaultMQPushConsumer) innerProducer() (*DefaultMQProducer, error) {
	c.innerProducerMu.Lock()
	defer c.innerProducerMu.Unlock()
	if c.producer != nil {
		return c.producer, nil
	}
	c.mu.Lock()
	addrs := append([]string(nil), c.nameServerAddrs...)
	unitName := c.unitName
	unitMode := c.unitMode
	stream := c.streamRequest
	tls := c.tlsEnable
	c.mu.Unlock()
	producer, err := NewDefaultMQProducer(common.ClientInnerProducerGroup)
	if err != nil {
		return nil, err
	}
	producer.SetNameServerAddresses(addrs)
	producer.SetUnitName(unitName)
	producer.SetUnitMode(unitMode)
	producer.SetEnableStreamRequestType(stream)
	if tls != nil {
		producer.SetTLSEnable(*tls)
	}
	if err := producer.Start(); err != nil {
		return nil, err
	}
	c.producer = producer
	return producer, nil
}

// ---------------------------------------------------------------- helpers

func (c *DefaultMQPushConsumer) processQueueOf(mq common.MessageQueue) *processQueue {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.processQueueTable[mq]
}

func (c *DefaultMQPushConsumer) queueEpochOf(mq common.MessageQueue) uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.queueEpoch[mq]
}

func (c *DefaultMQPushConsumer) pullIntervalValue() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.pullInterval
}

func (c *DefaultMQPushConsumer) consumeTimeoutValue() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.consumeTimeout
}

// stopRequested reports whether the consumer (or this queue) has been stopped.
func (c *DefaultMQPushConsumer) stopRequested(queueStop chan struct{}) bool {
	select {
	case <-c.stopCh:
		return true
	default:
	}
	if queueStop == nil {
		return false
	}
	select {
	case <-queueStop:
		return true
	default:
		return false
	}
}

// sleepOrStop waits, and reports whether it was interrupted.
func (c *DefaultMQPushConsumer) sleepOrStop(queueStop chan struct{}, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	if queueStop == nil {
		select {
		case <-c.stopCh:
			return true
		case <-timer.C:
			return false
		}
	}
	select {
	case <-c.stopCh:
		return true
	case <-queueStop:
		return true
	case <-timer.C:
		return false
	}
}

// isTimeout reports whether the error is a transport timeout. Both the remoting
// timeout wrapper and the raw deadline error count.
func isTimeout(err error) bool {
	if err == nil {
		return false
	}
	if e, ok := err.(*common.Error); ok && e.Kind == common.KindTimeout {
		return true
	}
	return false
}

// pullRetryDelay is how long the pull loop waits before its next attempt after a
// failed pull. Two failures look alike in the log but mean opposite things and
// must not share one wait:
//
//   - the BROKER refused the pull because it is flow-controlling its own queues
//     (response code 215): the request already round-tripped, nothing landed in
//     the local buffer, and the queue is quiet again as soon as the broker's
//     pressure drops — so a short backoff, and no rebalance or threshold rescan;
//   - anything else (a system error, a busy/dispatch answer, a dead connection):
//     the longer generic wait, because retrying hot at a broker that is
//     unhealthy adds pressure to whatever is already wrong.
//
// This client's own backlog threshold is not here: flowControlHit stops the pull
// BEFORE the request is sent and sleeps
// pullTimeDelayMillsWhenFlowControl, so the two waits never meet.
//
// One loop serves both the concurrent and the orderly consumer (orderly is just
// a flag on it), so both paths get the same discrimination.
func pullRetryDelay(err error) time.Duration {
	if isBrokerFlowControlError(err) {
		return pullTimeDelayMillsWhenBrokerFlow
	}
	return pullTimeDelayMillsWhenPullFailed
}

// isBrokerFlowControlError recognises the one failure answer that means "the
// BROKER is throttling its own queues" (response code 215), whichever error shape
// carried it. Pull paths share this single recognition so a 215 can never map to
// two different waits.
func isBrokerFlowControlError(err error) bool {
	if e, ok := err.(*common.Error); ok {
		if code, has := e.ResponseCode(); has && code == remoting.RespFlowControl {
			return true
		}
	}
	return false
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func maxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
