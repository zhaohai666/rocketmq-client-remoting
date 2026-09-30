// The consumer's runtime loops: one pull goroutine per assigned queue, one
// dispatch goroutine, and the two sweeps (clean-expire, orderly lock).
//
// Why one goroutine PER QUEUE rather than Java's shared PullMessageService
// thread: the push consumer long-polls (suspend=true), so an idle queue parks
// its request for up to ~15s. A shared thread would serialise those parks and
// starve every other queue. The Python/C++/.NET ports made the same call.
package client

import (
	"time"

	"github.com/zhaohai666/rocketmq-client-remoting/go/common"
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
		// Java DefaultMQPushConsumerImpl.pullMessage:253 — the timestamp is
		// stamped when a pull is STARTED, before the flow-control/lock
		// decisions: the stall detector asks "is this loop alive", not "did
		// this round make a network call".
		pq.TouchPull()

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

		// Java pullMessage:458-468 — the expression goes on the wire only when
		// postSubscriptionWhenPull is on AND the subscription is not a class
		// filter. Off by default: the broker then does not filter and the
		// client's second-stage tag check covers it.
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
				if c.sleepOrStop(stop, 500*time.Millisecond) {
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
			// Java DefaultMQPushConsumerImpl:402-427 — freeze the corrected
			// offset here (equivalent to updateAndFreezeOffset inside the
			// callback) and do the revoke/persist outside the lock.
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

// flowControlHit is Java ProcessQueue.putMessage / pullMessage's threshold
// checks. Any one of them pauses THIS queue's pulls:
//
//   - pullThresholdForQueue (1000): buffered message COUNT;
//   - pullThresholdSizeForQueue (100 MB): buffered bytes;
//   - consumeConcurrentlyMaxSpan (2000): the offset SPAN of the buffer, so one
//     message that keeps failing cannot let the offset run away;
//   - the topic-level count/size variants (-1 disables them).
func (c *DefaultMQPushConsumer) flowControlHit(mq common.MessageQueue, pq *processQueue) bool {
	count, sizeMB, span := pq.PendingStats()

	c.mu.Lock()
	maxCount := c.pullThresholdForQueue
	maxSize := c.pullThresholdSizeForQueue
	maxSpan := c.consumeConcurrentlyMaxSpan
	topicMaxCount := c.pullThresholdForTopic
	topicMaxSize := c.pullThresholdSizeForTopic
	c.mu.Unlock()

	reason := ""
	switch {
	case int64(count) >= maxInt64(1, maxCount):
		reason = "count"
	case maxSize > 0 && sizeMB >= float64(maxSize):
		reason = "size"
	case maxSpan > 0 && span > maxSpan:
		reason = "span"
	case topicMaxCount > 0 || topicMaxSize > 0:
		topicCount, topicSizeMB := c.topicPendingStats(mq.Topic)
		if topicMaxCount > 0 && int64(topicCount) >= topicMaxCount {
			reason = "topicCount"
		} else if topicMaxSize > 0 && topicSizeMB >= float64(topicMaxSize) {
			reason = "topicSize"
		}
	}
	if reason == "" {
		return false
	}
	c.mu.Lock()
	c.flowControlTriggered++
	c.mu.Unlock()
	common.LogDebugf("flow control: queue %v %s, pause pull", mq, reason)
	return true
}

func (c *DefaultMQPushConsumer) topicPendingStats(topic string) (int, float64) {
	c.mu.Lock()
	pqs := make([]*processQueue, 0, len(c.processQueueTable))
	for mq, pq := range c.processQueueTable {
		if mq.Topic == topic {
			pqs = append(pqs, pq)
		}
	}
	c.mu.Unlock()
	total, size := 0, 0.0
	for _, pq := range pqs {
		n, mb, _ := pq.PendingStats()
		total += n
		size += mb
	}
	return total, size
}

// ---------------------------------------------------------------- dispatch

// dispatchLoop takes batches out of the buffers and hands them to the listener.
//
// Concurrent mode: a goroutine per batch, bounded by the core pool size (the
// Java ThreadPoolExecutor with an unbounded queue, whose real concurrency is
// therefore core). Orderly mode: consumed INLINE, so a queue's ordering is
// preserved without any extra lock.
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

// cleanExpireLoop is Java's scheduleAtFixedRate(cleanExpireMsg,
// consumeTimeout, consumeTimeout, MINUTES): initialDelay and period are the SAME,
// so the first sweep also waits a full period. Sweeping immediately would walk
// messages that are still well within their timeout.
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
			// Java's scheduler shell catches Throwable (:77-81): one bad round
			// must not kill the schedule.
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

// lockLoop is Java ConsumeMessageOrderlyService#lockMQ: lock every assigned
// queue every 20s, with one attempt immediately at startup so the first 20s is
// not wasted.
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

// innerProducer lazily builds the CLIENT_INNER_PRODUCER used by the orderly
// send-back path. Java constructs it inside ConsumeMessageOrderlyService and
// calls resetClientConfig, which is why unitMode is copied across.
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
