// The consumer's runtime loops: one pull goroutine per assigned queue, one
// dispatch goroutine, and the two sweeps (clean-expire, orderly lock).
package client

import (
	"fmt"
	"math"
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
	// The lock gate is a CLUSTERING-only rule: a BROADCASTING orderly consumer
	// owns every queue outright, so no broker lock is (or can be) granted and
	// waiting for one would stall every queue forever.
	orderlyClustering := c.isOrderly() && c.MessageModel() == MessageModelClustering
	// The generic failed-round back-off is captured once when this loop is set
	// up, so changing the knob after start never alters a loop already running.
	failedDelay := c.failedRoundDelayValue()
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
		if orderlyClustering && !pq.IsLocked() {
			// Orderly in CLUSTERING: do not pull before LOCK_BATCH_MQ granted
			// this queue, or we would consume what another instance is about to
			// lock.
			if c.sleepOrStop(stop, 200*time.Millisecond) {
				return
			}
			continue
		}
		if orderlyClustering && pq.IsLockExpired() {
			// The last grant is stale: the broker may have handed this queue to
			// another instance. Back off briefly and let the periodic lock sweep
			// re-acquire it.
			common.LogWarnf("the message queue lock expired, so consume later, %v", mq)
			if c.sleepOrStop(stop, 10*time.Millisecond) {
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
			// The pull cursor could not be resolved: this round failed for a
			// reason other than the broker's own flow control (the initial offset
			// resolution errored, or the subscription this queue belongs to is
			// gone), so it sleeps the generic failed-round back-off like any other
			// non-flow-control failure.
			if c.sleepOrStop(stop, failedDelay) {
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
		// The cursor THIS round asked to pull from. The round's own offset check at
		// the bottom compares against it, and the cursor itself moves on below, so the
		// value has to be held for the whole round.
		prevRequestOffset := offset
		result, err := c.pullAPI.pullKernel(c.consumerGroup, mq, offset, sub, sysFlag, 0,
			batchNums, batchBytes, suspend, timeout)
		if err != nil {
			if isTimeout(err) {
				// An idle queue's long poll answers on its own schedule, so our
				// request timeout can expire before the reply does. That is the
				// expected shape of an idle queue, not a failure: Debug level, so
				// the run log keeps ERROR=0.
				common.LogDebugf("pull long-poll timeout for %v (benign, will retry): %v", mq, err)
			} else {
				common.LogDebugf("pull error for %v: %v", mq, err)
				if c.sleepOrStop(stop, pullRetryDelay(err, failedDelay)) {
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
		// Offset sanity warning: the cursor has moved to nextBeginOffset and the batch
		// is already with the consume side, so all a round can still be asked is whether
		// its own numbers hang together. It sits ahead of the stats and the cadence wait
		// below because that wait can end the round on a stop request, and the rounds
		// worth warning about are the delivered ones. Purely diagnostic: no offset is
		// rewritten, no batch dropped, no wait added.
		if warn := pullOffsetsSanityWarning(result, mq, prevRequestOffset); warn != "" {
			common.LogWarnf("%s", warn)
		}
		if result.PullStatus == PullFound {
			c.incPullRT(mq.Topic, common.CurrentTimeMillis()-begin)
			if len(result.MsgFoundList) > 0 {
				c.incPullTPS(mq.Topic, int64(len(result.MsgFoundList)))
				// Only a round that DELIVERED rows is paced by pullInterval, and
				// only when it is positive. This is the same list the PULL_TPS
				// counter above just recorded — what survived the tag pass and the
				// filter hooks, i.e. what this queue was actually handed — not the
				// rows the broker put on the wire.
				if interval := c.pullIntervalValue(); interval > 0 {
					if c.sleepOrStop(stop, time.Duration(interval)*time.Millisecond) {
						return
					}
				}
			}
			// A FOUND round whose batch came back empty, or whose every row was
			// dropped by the client's own filtering, delivered nothing, so it falls
			// through and re-pulls at once like the idle statuses below.
		}
		// NO_NEW_MSG and NO_MATCHED_MSG (and a FOUND that delivered nothing) reach
		// here and start the next round immediately: the cadence paces rounds that
		// delivered, so an extra local wait on an idle round would only delay the
		// next fetch. Failures are not here — each failure arm sleeps its own wait
		// before continuing, and OFFSET_ILLEGAL leaves the loop.
	}
}

// pullOffsetsLookWrong is the arithmetic behind a FOUND round's sanity warning: the
// round asked to pull from prevRequestOffset, so a reply that puts the next cursor
// — or its own first row — below that point describes a range the round did not ask
// for. These two comparisons are the whole check; neither has any teeth.
func pullOffsetsLookWrong(nextBeginOffset, firstMsgOffset, prevRequestOffset int64) bool {
	return nextBeginOffset < prevRequestOffset || firstMsgOffset < prevRequestOffset
}

// firstPullMsgOffset is the queue offset of the first row of the list a round hands
// to the consume side — read after this port's tag pass and filter hooks, so it is
// the offset this queue was actually given. An empty list reports the largest int64:
// a round that handed no row over has no row offset to judge.
func firstPullMsgOffset(msgs []*common.MessageExt) int64 {
	if len(msgs) == 0 {
		return math.MaxInt64
	}
	return msgs[0].QueueOffset
}

// pullOffsetsSanityWarning is one round's offset check as a decision: "" when there
// is nothing to report — every status but FOUND hands no rows over and moves no
// cursor on its own reply, so only a FOUND round is judged — and otherwise the one
// line that names all three offsets, so the log alone shows which comparison tripped.
func pullOffsetsSanityWarning(result *PullResult, mq common.MessageQueue,
	prevRequestOffset int64) string {
	if result.PullStatus != PullFound {
		return ""
	}
	firstMsgOffset := firstPullMsgOffset(result.MsgFoundList)
	if !pullOffsetsLookWrong(result.NextBeginOffset, firstMsgOffset, prevRequestOffset) {
		return ""
	}
	return fmt.Sprintf("pull offsets look wrong for %v: requested at %d, "+
		"nextBeginOffset=%d, firstMsgOffset=%d",
		mq, prevRequestOffset, result.NextBeginOffset, firstMsgOffset)
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
//     the generic failed-round back-off the caller captured at loop setup,
//     because retrying hot at a broker that is unhealthy adds pressure to
//     whatever is already wrong.
//
// This client's own backlog threshold is not here: flowControlHit stops the pull
// BEFORE the request is sent and sleeps
// pullTimeDelayMillsWhenFlowControl, so the two waits never meet.
//
// One loop serves both the concurrent and the orderly consumer (orderly is just
// a flag on it), so both paths get the same discrimination.
func pullRetryDelay(err error, failedDelay time.Duration) time.Duration {
	if isBrokerFlowControlError(err) {
		return pullTimeDelayMillsWhenBrokerFlow
	}
	return failedDelay
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
