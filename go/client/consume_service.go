// Consume services (Java
// org.apache.rocketmq.client.impl.consumer.ConsumeMessageConcurrentlyService /
// ConsumeMessageOrderlyService), plus the two periodic sweeps they own.
//
// The two paths are NOT variants of one another — four differences must be kept:
//
//  1. maxReconsumeTimes -1 means "unlimited" (MaxInt) for ORDERLY but 16 for
//     CONCURRENT (that 16 is the broker's default retryMaxTimes, and every
//     concurrent round-trip goes through the broker). Merging the two either
//     invents a dead-letter for orderly or lets concurrent messages retry
//     forever.
//  2. Orderly redelivery is a PLAIN SEND to %RETRY%<group>, not
//     CONSUMER_SEND_MSG_BACK(36).
//  3. Orderly has no "exception means ack" path: a panic suspends and retries
//     in place.
//  4. Orderly rolls the batch back into the buffer; concurrent removes acked
//     entries and sends the unacked tail back to the broker.
package client

import (
	"fmt"
	"math"
	"time"

	"github.com/zhaohai666/rocketmq-client-remoting/go/common"
	"github.com/zhaohai666/rocketmq-client-remoting/go/remoting"
)

// javaIntMax is Java Integer.MAX_VALUE — the orderly "unlimited" sentinel.
const javaIntMax = math.MaxInt32

// consumeBatch dispatches one batch and handles redelivery/suspension. Returns
// whether the consumed offset advanced.
//
// epoch is the process-queue generation captured when the batch was taken. If
// the queue was dropped meanwhile (rebalance / OFFSET_ILLEGAL / stall recovery)
// the batch must be neither consumed nor acked.
func (c *DefaultMQPushConsumer) consumeBatch(mq common.MessageQueue, batch []*common.MessageExt, epoch uint64) bool {
	if len(batch) == 0 {
		return false
	}
	if epoch != c.queueEpochOf(mq) {
		common.LogWarnf("the message queue not be able to consume, because it's dropped. group=%s mq=%v msgs=%d epoch=%d->%d",
			c.consumerGroup, mq, len(batch), epoch, c.queueEpochOf(mq))
		return false
	}
	c.resetRetryTopicAndNamespace(batch)
	if c.isOrderly() {
		return c.consumeOrderlyBatch(mq, batch, epoch)
	}
	return c.consumeConcurrentlyBatch(mq, batch, epoch)
}

// resetRetryTopicAndNamespace is Java
// DefaultMQPushConsumerImpl#resetRetryAndNamespace, called BEFORE the listener.
//
// A retried message physically lives under %RETRY%<group>; the broker writes the
// business topic into the RETRY_TOPIC property. Restoring it here is what lets
// the listener branch on the topic it subscribed to.
func (c *DefaultMQPushConsumer) resetRetryTopicAndNamespace(msgs []*common.MessageExt) {
	groupTopic := common.GetRetryTopic(c.consumerGroup)
	for _, msg := range msgs {
		if retryTopic, ok := msg.GetProperty(common.PropertyRetryTopic); ok && retryTopic != "" && msg.Topic == groupTopic {
			msg.Topic = retryTopic
		}
		if c.namespace != "" {
			msg.Topic = common.WithoutNamespace(msg.Topic, c.namespace)
		}
	}
}

// ------------------------------------------------------------ concurrent

func (c *DefaultMQPushConsumer) consumeConcurrentlyBatch(mq common.MessageQueue, batch []*common.MessageExt, epoch uint64) bool {
	listener, ok := c.listener.(MessageListenerConcurrently)
	if !ok {
		common.LogErrorf("listener is not a MessageListenerConcurrently for group %s", c.consumerGroup)
		return false
	}
	ctx := NewConsumeConcurrentlyContext(mq)

	var hookCtx *ConsumeMessageContext
	hooks := c.consumeMessageHooks()
	if len(hooks) > 0 {
		hookCtx = c.buildConsumeHookContext(batch, mq)
		executeConsumeHookBefore(hooks, hookCtx)
	}
	begin := common.CurrentTimeMillis()
	// Java ConsumeMessageConcurrentlyService:366-370 stamps CONSUME_START_TIME on
	// every delivery (retries included) BEFORE the listener; cleanExpiredMsg's
	// escape hatch reads it.
	for _, msg := range batch {
		msg.PutProperty(common.PropertyConsumeStartTime, i64Text(common.CurrentTimeMillis()))
	}

	status, panicked := callConcurrently(listener, batch, ctx)
	if panicked {
		common.LogDebugf("listener error, treat as RECONSUME_LATER: mq=%v", mq)
		status = ReconsumeLater
	}
	// Java:380 — the RT is taken ONCE, immediately after the listener returns,
	// and then feeds both the hook's returnType and the CONSUME_RT statistic.
	consumeRT := common.CurrentTimeMillis() - begin
	rawStatus := status
	if status != ConsumeSuccess && status != ReconsumeLater {
		// Java:399-405 — a null return behaves as RECONSUME_LATER. A Go listener
		// cannot return null, so an out-of-range value takes the same branch
		// instead of silently falling into SUCCESS and acking an unhandled batch.
		common.LogWarnf("consumeMessage return unknown status, Group: %s Msgs: %d MQ: %v", c.consumerGroup, len(batch), mq)
		status = ReconsumeLater
	}

	ackIndex := ctx.AckIndex
	if status == ConsumeSuccess {
		if ackIndex >= len(batch) {
			ackIndex = len(batch) - 1
		}
	} else {
		ackIndex = -1
	}
	c.finishConsumeHook(hooks, hookCtx, rawStatus, panicked, consumeRT, status, ackIndex, len(batch))
	// Java:414-418 — CONSUME_RT is bumped after the hook epilogue and BEFORE
	// processConsumeResult's OK/Failed accounting; the accounting itself is the
	// top of processConsumeResult (:212-229).
	c.incConsumeRT(mq.Topic, consumeRT)
	if status == ConsumeSuccess {
		c.recordConsumeSuccessTPS(mq.Topic, ackIndex, len(batch))
	} else {
		c.incConsumeFailedTPS(mq.Topic, int64(len(batch)))
	}

	if c.messageModel == MessageModelBroadcasting {
		// Java:232-237 — broadcasting never sends back: the unacked tail is
		// dropped with a warning and the whole batch advances.
		if dropped := len(batch) - ackIndex - 1; dropped > 0 {
			common.LogWarnf("BROADCASTING, the message consume failed, drop it: %d msgs in %v", dropped, mq)
		}
		c.commitConcurrently(mq, batch, epoch)
		return true
	}
	if ackIndex+1 >= len(batch) {
		c.commitConcurrently(mq, batch, epoch)
		return true
	}
	// Cluster mode: redeliver the unacked tail one by one to %RETRY%<group>
	// (delay 3+reconsumeTimes; the broker moves it to %DLQ% once
	// maxReconsumeTimes is exceeded).
	failed := c.sendBackBatch(mq, batch[ackIndex+1:], ctx)
	if len(failed) > 0 {
		// Java:256-260 — the ones whose send-back failed are re-submitted
		// later; here they go back to the front of the buffer.
		if pq := c.processQueueOf(mq); pq != nil {
			pq.RequeueBatch(failed)
		}
		select {
		case <-c.stopCh:
		case <-time.After(200 * time.Millisecond):
		}
	}
	acked := make([]*common.MessageExt, 0, len(batch))
	failedOffsets := make(map[int64]struct{}, len(failed))
	for _, msg := range failed {
		failedOffsets[msg.QueueOffset] = struct{}{}
	}
	for _, msg := range batch {
		if _, bad := failedOffsets[msg.QueueOffset]; !bad {
			acked = append(acked, msg)
		}
	}
	// Java:266-269 — the commit is what removeMessage(msgs) answers, and `msgs`
	// is the ACKED list (consumeRequest.getMsgs() minus msgBackFailed).
	c.commitConcurrently(mq, acked, epoch)
	return len(failed) == 0
}

// commitConcurrently is Java ProcessQueue#removeMessage + updateOffset for the
// concurrent paths (both the clean ack and the partly-failed one).
//
// The target is the queue's high-water mark + 1 — NOT the end of this batch.
// Batches of one queue are consumed in parallel, so the batch that finishes
// second is not necessarily the one with the higher offsets: when the higher
// batch finishes FIRST the buffer still holds the lower one, and Java answers
// the lower batch's head; when the lower one finishes second the buffer drains
// and Java answers the queue's end. Using the batch's own end as the drained
// value would leave the cursor one batch short in the second case, and using
// the FAILED set as the floor would make the first case step over the very
// entries that were never handed to the broker.
func (c *DefaultMQPushConsumer) commitConcurrently(mq common.MessageQueue, acked []*common.MessageExt, epoch uint64) {
	drained := batchEnd(acked)
	floor := int64(-1)
	if pq := c.processQueueOf(mq); pq != nil {
		drained = pq.QueueOffsetMax() + 1
		floor = pq.MinRemainingExcept(acked)
	}
	c.advanceConsumeOffset(mq, acked, drained, floor, epoch)
}

// batchEnd is "the largest offset in the batch + 1" — the orderly path's commit
// target (Java ProcessQueue#commit is lastKey + 1 over the batch it consumed
// inline), and the concurrent path's fallback when there is no buffer to ask.
func batchEnd(batch []*common.MessageExt) int64 {
	next := int64(-1)
	for _, msg := range batch {
		if msg.QueueOffset > next {
			next = msg.QueueOffset
		}
	}
	return next + 1
}

// callConcurrently runs the listener with the Java exception rule: a panic is
// RECONSUME_LATER, never a crash.
func callConcurrently(listener MessageListenerConcurrently, batch []*common.MessageExt, ctx *ConsumeConcurrentlyContext) (status ConsumeConcurrentlyStatus, panicked bool) {
	defer func() {
		if r := recover(); r != nil {
			panicked = true
			status = ReconsumeLater
			common.LogErrorf("concurrently listener panic: %v", r)
		}
	}()
	return listener.ConsumeMessage(batch, ctx), false
}

// sendBackBatch redelivers the unacked entries one by one and returns the ones
// that FAILED.
//
// As in Java (:251), a failed entry gets reconsumeTimes+1 locally — the broker
// never recorded the attempt, and without the local bump the message can never
// reach the dead-letter queue.
func (c *DefaultMQPushConsumer) sendBackBatch(mq common.MessageQueue, batch []*common.MessageExt, ctx *ConsumeConcurrentlyContext) []*common.MessageExt {
	var failed []*common.MessageExt
	pq := c.processQueueOf(mq)
	for _, msg := range batch {
		if pq != nil && !pq.Contains(msg.QueueOffset) {
			// Java:243-248 — an entry already swept (or whose queue was revoked)
			// is skipped: it is on its way back to the broker, and sending it
			// again would duplicate it.
			common.LogInfof("Message is not found in its process queue; skip send-back-procedure, topic=%s, brokerName=%s, queueId=%d, queueOffset=%d",
				msg.Topic, msg.BrokerName, msg.QueueID, msg.QueueOffset)
			continue
		}
		delayLevel := ctx.DelayLevelWhenNextConsume
		if delayLevel == 0 {
			delayLevel = 3 + msg.ReconsumeTimes
		}
		if err := c.sendMessageBack(msg, delayLevel); err != nil {
			common.LogDebugf("send message back failed for msg %s: %v", msg.MsgID, err)
			msg.ReconsumeTimes++
			failed = append(failed, msg)
		}
	}
	return failed
}

// sendMessageBack is DefaultMQPushConsumerImpl#sendMessageBack: broker-side
// redelivery via CONSUMER_SEND_MSG_BACK(36).
func (c *DefaultMQPushConsumer) sendMessageBack(msg *common.MessageExt, delayLevel int32) error {
	inst := c.instance
	if inst == nil {
		return common.ClientError("consumer not started")
	}
	maxReconsume := int32(16)
	if c.maxReconsumeTimes != -1 {
		maxReconsume = c.maxReconsumeTimes
	}
	return inst.sendMessageBack(c.consumerGroup, msg, delayLevel, maxReconsume, c.unitMode, 5000)
}

// ---------------------------------------------------------------- orderly

func (c *DefaultMQPushConsumer) consumeOrderlyBatch(mq common.MessageQueue, batch []*common.MessageExt, epoch uint64) bool {
	listener, ok := c.listener.(MessageListenerOrderly)
	if !ok {
		common.LogErrorf("listener is not a MessageListenerOrderly for group %s", c.consumerGroup)
		return false
	}
	ctx := NewConsumeOrderlyContext(mq)

	var hookCtx *ConsumeMessageContext
	hooks := c.consumeMessageHooks()
	if len(hooks) > 0 {
		hookCtx = c.buildConsumeHookContext(batch, mq)
		executeConsumeHookBefore(hooks, hookCtx)
	}
	begin := common.CurrentTimeMillis()

	status, panicked := callOrderly(listener, batch, ctx)
	if panicked {
		common.LogDebugf("orderly listener error (retry in place): mq=%v", mq)
	}
	if status == OrderlyRollback || status == OrderlySuspendCurrentQueueAMoment {
		common.LogWarnf("consumeMessage Orderly return not OK, Group: %s Msgs: %d MQ: %v", c.consumerGroup, len(batch), mq)
	}
	// Java:483 — same single measurement as the concurrent path.
	consumeRT := common.CurrentTimeMillis() - begin
	rawStatus := status
	if status != OrderlySuccess && status != OrderlyRollback && status != OrderlyCommit &&
		status != OrderlySuspendCurrentQueueAMoment {
		// Java:502-504 — a null status is normalised to "suspend" BEFORE the
		// hook. Without this a stray value would fall into the SUCCESS branch
		// and silently ack messages that were never consumed.
		status = OrderlySuspendCurrentQueueAMoment
	}
	c.finishOrderlyHook(hooks, hookCtx, rawStatus, panicked, consumeRT, status)
	// Java:514-517 — CONSUME_RT first, then processConsumeResult's OK/Failed
	// accounting.
	c.incConsumeRT(mq.Topic, consumeRT)

	if ctx.AutoCommit {
		if status == OrderlyCommit || status == OrderlyRollback {
			// Java:246-250 — with autoCommit on, COMMIT/ROLLBACK are illegal
			// (they belong to the binlog consumer). Java warns and does NOT
			// break, i.e. falls through to SUCCESS: the messages are acked.
			common.LogWarnf("the message queue consume result is illegal, we think you want to ack these message %v", mq)
			status = OrderlySuccess
		}
		if status == OrderlySuspendCurrentQueueAMoment {
			// Java:254-255 — SUSPEND counts the WHOLE batch as failed, before
			// the retry decision (a batch handed to the broker still failed).
			c.incConsumeFailedTPS(mq.Topic, int64(len(batch)))
			// Java:256-266 — checkReconsumeTimes runs first: only "still within
			// the retry budget, or the send-back failed" suspends in place;
			// once the message has been handed to the broker the offset moves
			// on, otherwise a poison message parks the queue forever.
			if c.checkOrderlyReconsumeTimes(batch) {
				if pq := c.processQueueOf(mq); pq != nil {
					pq.RequeueBatch(batch)
				}
				c.sleepOrderly(ctx)
				return false
			}
		} else {
			// Java:246-252 — SUCCESS, and with autoCommit on COMMIT/ROLLBACK
			// fall through into it, count the whole batch as OK.
			c.incConsumeOKTPS(mq.Topic, int64(len(batch)))
		}
		c.advanceConsumeOffset(mq, batch, batchEnd(batch), -1, epoch)
		return true
	}
	// autoCommit == false (Java:270-300, the binlog path).
	switch status {
	case OrderlyCommit:
		c.advanceConsumeOffset(mq, batch, batchEnd(batch), -1, epoch)
		return true
	case OrderlyRollback:
		if pq := c.processQueueOf(mq); pq != nil {
			pq.RequeueBatch(batch)
		}
		c.sleepOrderly(ctx)
		return false
	case OrderlySuspendCurrentQueueAMoment:
		// Java:287 — the whole batch counts as failed. COMMIT and ROLLBACK
		// record nothing at all (:275-284), which is why they have no line here.
		c.incConsumeFailedTPS(mq.Topic, int64(len(batch)))
		if c.checkOrderlyReconsumeTimes(batch) {
			if pq := c.processQueueOf(mq); pq != nil {
				pq.RequeueBatch(batch)
			}
			c.sleepOrderly(ctx)
		}
		// Java:288-296 — unlike the autoCommit branch the offset is NOT
		// committed; whether to advance is the binlog consumer's call.
		return false
	default:
		// Java:272-274 — SUCCESS with autoCommit off counts as OK.
		c.incConsumeOKTPS(mq.Topic, int64(len(batch)))
		// SUCCESS with autoCommit off: Java leaves the messages in
		// consumingMsgOrderlyTreeMap waiting for an explicit commit(), and none
		// of the four ports exposes that handle to a listener. Doing nothing
		// would let the dispatch loop swallow the batch without moving the
		// offset, so the equivalent is "requeue and wait one suspend period":
		// the offset stays put, nothing is lost, and the loop does not spin.
		if pq := c.processQueueOf(mq); pq != nil {
			pq.RequeueBatch(batch)
		}
		c.sleepOrderly(ctx)
		return false
	}
}

func callOrderly(listener MessageListenerOrderly, batch []*common.MessageExt, ctx *ConsumeOrderlyContext) (status ConsumeOrderlyStatus, panicked bool) {
	defer func() {
		if r := recover(); r != nil {
			panicked = true
			status = OrderlySuspendCurrentQueueAMoment
			common.LogErrorf("orderly listener panic: %v", r)
		}
	}()
	return listener.ConsumeMessage(batch, ctx), false
}

// sleepOrderlyMillis is Java
// ConsumeMessageOrderlyService#submitConsumeRequestLater:211-234. -1 means
// "not set" and falls back to the consumer's configuration, then the result is
// clamped to [10, 30000].
func (c *DefaultMQPushConsumer) sleepOrderlyMillis(ctx *ConsumeOrderlyContext) int64 {
	ms := ctx.SuspendCurrentQueueTimeMillis
	if ms == -1 {
		ms = c.suspendCurrentQueueTimeMs
	}
	if ms < 10 {
		return 10
	}
	if ms > 30000 {
		return 30000
	}
	return ms
}

func (c *DefaultMQPushConsumer) sleepOrderly(ctx *ConsumeOrderlyContext) {
	select {
	case <-c.stopCh:
	case <-time.After(time.Duration(c.sleepOrderlyMillis(ctx)) * time.Millisecond):
	}
}

// orderlyMaxReconsumeTimes is Java
// ConsumeMessageOrderlyService#getMaxReconsumeTimes:313-320 — -1 is
// Integer.MAX_VALUE here (orderly retries in place, the broker never counts).
func (c *DefaultMQPushConsumer) orderlyMaxReconsumeTimes() int32 {
	if c.maxReconsumeTimes == -1 {
		return javaIntMax
	}
	return c.maxReconsumeTimes
}

// checkOrderlyReconsumeTimes is Java
// ConsumeMessageOrderlyService#checkReconsumeTimes:322-336. Returns whether the
// batch still has to be suspended in place.
func (c *DefaultMQPushConsumer) checkOrderlyReconsumeTimes(msgs []*common.MessageExt) bool {
	suspend := false
	maxTimes := c.orderlyMaxReconsumeTimes()
	for _, msg := range msgs {
		if msg.ReconsumeTimes >= maxTimes {
			// The count is written to the RETRY message's RECONSUME_TIME
			// property so the send side hoists it into the request header.
			msg.PutProperty(common.PropertyReconsumeTime, i32Text(msg.ReconsumeTimes))
			if !c.orderlySendMessageBack(msg) {
				suspend = true
				msg.ReconsumeTimes++
			}
		} else {
			suspend = true
			msg.ReconsumeTimes++
		}
	}
	return suspend
}

// orderlySendMessageBack is Java
// ConsumeMessageOrderlyService#sendMessageBack:338-360 — a PLAIN send to
// %RETRY%<group>, not a CONSUMER_SEND_MSG_BACK.
//
// The broker's handleRetryAndDLQ sees that this group still holds unexpired
// queue locks (the orderly signature) and moves the message straight to
// %DLQ%<group>.
func (c *DefaultMQPushConsumer) orderlySendMessageBack(msg *common.MessageExt) bool {
	defer func() {
		if r := recover(); r != nil {
			common.LogDebugf("orderly send message back panicked, group=%s msg=%s: %v", c.consumerGroup, msg.MsgID, r)
		}
	}()
	if err := c.doOrderlySendMessageBack(msg); err != nil {
		common.LogDebugf("orderly send message back failed, group=%s msg=%s: %v", c.consumerGroup, msg.MsgID, err)
		return false
	}
	return true
}

func (c *DefaultMQPushConsumer) doOrderlySendMessageBack(msg *common.MessageExt) error {
	producer, err := c.innerProducer()
	if err != nil {
		return err
	}
	retryTopic := common.GetRetryTopic(c.consumerGroup)
	newMsg := common.NewMessage(retryTopic, append([]byte(nil), msg.GetBody()...))
	if msg.Properties != nil {
		newMsg.SetProperties(msg.Properties.Clone())
	}
	newMsg.Flag = msg.Flag
	originMsgID := msg.MsgID
	if v, ok := msg.GetProperty(common.PropertyOriginMessageID); ok && v != "" {
		originMsgID = v
	}
	if originMsgID != "" {
		newMsg.PutProperty(common.PropertyOriginMessageID, originMsgID)
	}
	newMsg.PutProperty(common.PropertyRetryTopic, msg.Topic)
	newMsg.PutProperty(common.PropertyReconsumeTime, i32Text(msg.ReconsumeTimes+1))
	newMsg.PutProperty(common.PropertyMaxReconsumeTimes, i32Text(c.orderlyMaxReconsumeTimes()))
	// The half-message marker must go, else the broker treats it as a
	// transaction check-back message all over again.
	newMsg.RemoveProperty(common.PropertyTransactionPrepared)
	newMsg.SetDelayTimeLevel(3 + msg.ReconsumeTimes)

	info, err := c.instance.GetTopicPublishInfo(retryTopic, true)
	if err != nil {
		return err
	}
	mq, ok, err := info.SelectOneMessageQueue(nil)
	if err != nil || !ok {
		return common.ClientError(fmt.Sprintf("no writable queue for retry topic %s", retryTopic))
	}
	_, err = producer.SendToQueue(newMsg, mq)
	return err
}

// ------------------------------------------------------------------ offsets

// advanceConsumeOffset is Java's updateOffset: it writes `drained` — the offset
// to commit once the buffer holds nothing else — and then lowers it to `floor`,
// the smallest offset that WILL still be buffered, whenever that is lower.
//
// `drained` differs per path because Java's two services derive it from
// different places: ConsumeMessageConcurrentlyService goes through
// removeMessage, whose fallback is queueOffsetMax + 1 (see commitConcurrently),
// while ConsumeMessageOrderlyService uses ProcessQueue#commit, i.e. lastKey + 1
// over the batch it took inline. Feeding the concurrent value to the orderly
// path (or the other way round) either skips messages or parks the queue.
//
// `batch` is the set being dropped from the buffer (Java's
// consumeRequest.getMsgs() minus msgBackFailed). epoch is the process-queue
// generation captured with the batch; a mismatch means the queue was revoked or
// rebuilt (Java's `!processQueue.isDropped()`) and the ack is void. A frozen
// offset (OFFSET_ILLEGAL recovery) must not move either.
//
// The two guards and the write share ONE critical section: revoke/reset bumps
// the epoch under the same lock, so an in-flight ack cannot slip in between a
// lock-free epoch read and a later write and push the corrected offset back.
func (c *DefaultMQPushConsumer) advanceConsumeOffset(mq common.MessageQueue, batch []*common.MessageExt, drained int64, floor int64, epoch uint64) {
	if len(batch) == 0 {
		return
	}
	nextOffset := drained
	if floor >= 0 && floor < nextOffset {
		nextOffset = floor
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if epoch != c.queueEpoch[mq] {
		common.LogDebugf("drop ack for %v: process queue was dropped (epoch %d -> %d)", mq, epoch, c.queueEpoch[mq])
		return
	}
	if c.frozenOffsets[mq] {
		return
	}
	if c.offsetStore != nil {
		c.offsetStore.UpdateOffset(mq, nextOffset, true)
	}
	if pq := c.processQueueTable[mq]; pq != nil {
		pq.CompleteBatch(batch)
	}
}

// correctTagsOffset is Java DefaultMQPushConsumerImpl#correctTagsOffset
// (:713-717).
//
// When a pull answers NO_NEW_MSG (nothing there) or NO_MATCHED_MSG (the broker
// ran a full pass and matched nothing) the CONSUMED offset must follow the pull
// cursor, otherwise it parks forever: nobody will ever ack a message that the
// broker filtered out or that the client's second-stage tag filter dropped.
//
// Java gates it on `0L == processQueue.getMsgCount()`. msgCount counts what is
// still in the ProcessQueue, and a concurrent in-flight batch is still in there
// until the listener returns — raising the offset before that would silently
// skip the batch on a crash. Caller must hold c.mu.
func (c *DefaultMQPushConsumer) correctTagsOffsetLocked(mq common.MessageQueue, status PullStatus, nextOffset int64) {
	if status != PullNoNewMsg && status != PullNoMatchedMsg {
		return
	}
	if c.frozenOffsets[mq] {
		return
	}
	pq := c.processQueueTable[mq]
	if pq != nil && pq.MsgCount() != 0 {
		return
	}
	if c.offsetStore == nil {
		return
	}
	cur, err := c.offsetStore.ReadOffset(mq, ReadFromMemory)
	if err == nil && cur >= nextOffset {
		return
	}
	c.offsetStore.UpdateOffset(mq, nextOffset, true)
}

// offsetIllegalRecover is Java DefaultMQPushConsumerImpl's OFFSET_ILLEGAL branch
// (:402-427).
//
// The broker answered PULL_OFFSET_MOVED (queue truncated, commitlog expired, or
// a server-side reset) and put the corrected offset in nextBeginOffset. Every
// message already fetched on that queue is void: it belongs to the skipped
// range, and acking it would push the offset back to an illegal value. So the
// queue is rebuilt from the corrected offset.
//
// Java's four steps: setNextOffset -> ProcessQueue.setDropped(true) ->
// {updateAndFreezeOffset; persist; removeProcessQueue} -> rebalanceImmediately.
//
// MUST be called without c.mu held (it does RPCs).
func (c *DefaultMQPushConsumer) offsetIllegalRecover(mq common.MessageQueue) {
	common.LogWarnf("the pull request offset illegal, fix it, queue=%v", mq)
	q := c.retireQueue(mq)
	c.persistRevoked([]revokedQueue{q})
	c.RebalanceImmediately()
}

// ------------------------------------------------------------ clean expire

// cleanExpiredMsgOnce is Java ConsumeMessageConcurrentlyService#cleanExpireMsg
// (:192-200): walk the queues currently held and sweep each.
func (c *DefaultMQPushConsumer) cleanExpiredMsgOnce() {
	for _, mq := range c.assignedQueues() {
		c.cleanExpiredQueue(mq)
	}
}

// cleanExpiredQueue is Java ProcessQueue#cleanExpiredMsg (:80-127), line by
// line. Three rules: only ever look at the HEAD (smallest offset), the message
// must be STRICTLY older than consumeTimeout, and at most 16 per round.
func (c *DefaultMQPushConsumer) cleanExpiredQueue(mq common.MessageQueue) {
	if c.isOrderly() {
		// Java:76-78 — orderly has no such path (messages retry in place;
		// sending them back would break the ordering).
		return
	}
	pq := c.processQueueOf(mq)
	if pq == nil {
		return
	}
	timeoutMillis := c.consumeTimeout * 60 * 1000
	for i := 0; i < 16; i++ {
		head, ok := pq.FirstMessage()
		if !ok {
			break
		}
		stamp, hasStamp := head.GetProperty(common.PropertyConsumeStartTime)
		if !hasStamp || stamp == "" {
			// Java:87-90 — a message that was never handed to the listener has
			// no stamp and counts as not expired.
			break
		}
		began, err := parseInt64(stamp)
		if err != nil {
			break
		}
		if common.CurrentTimeMillis()-began <= timeoutMillis {
			break
		}
		if err := c.sendMessageBack(head, 3); err != nil {
			// Java:122-125 — a failed send-back is logged only: the message
			// stays where it is and the next round retries. Removing it would
			// lose it for good.
			common.LogErrorf("send expired msg exception: %v", err)
			continue
		}
		common.LogInfof("send expire msg back. topic=%s, msgId=%s, storeHost=%s, queueId=%d, queueOffset=%d",
			head.Topic, head.MsgID, head.StoreHostString(), head.QueueID, head.QueueOffset)
		// Java:106-115 — remove only if it is STILL the head: a racing normal
		// completion wins and we must not steal its message.
		if nowHead, ok := pq.FirstMessage(); ok && nowHead.QueueOffset == head.QueueOffset {
			pq.RemoveMessage([]*common.MessageExt{head})
		}
	}
}

// ------------------------------------------------------------- lock sweep

// lockMQOnce is Java ConsumeMessageOrderlyService#lockMQ: LOCK_BATCH_MQ(41) for
// every assigned queue.
func (c *DefaultMQPushConsumer) lockMQOnce() {
	if c.instance == nil {
		return
	}
	mqs := c.assignedQueues()
	if len(mqs) == 0 {
		return
	}
	locked := c.instance.LockBatchMQ(c.consumerGroup, c.clientID, mqs, 1000)
	lockedSet := make(map[common.MessageQueue]struct{}, len(locked))
	for _, mq := range locked {
		lockedSet[mq] = struct{}{}
	}
	c.mu.Lock()
	for _, entry := range c.processQueueTable {
		_ = entry
	}
	for mq, pq := range c.processQueueTable {
		_, ok := lockedSet[mq]
		pq.SetLocked(ok)
	}
	c.mu.Unlock()
	common.LogDebugf("lock_batch_mq: %d/%d queues locked", len(lockedSet), len(mqs))
}

// unlockAssigned is Java RebalanceImpl#unlockAll, used on shutdown.
func (c *DefaultMQPushConsumer) unlockAssigned() {
	if c.instance == nil {
		return
	}
	mqs := c.assignedQueues()
	if len(mqs) == 0 {
		return
	}
	c.instance.UnlockBatchMQ(c.consumerGroup, c.clientID, mqs, 1000)
}

// ---------------------------------------------------------------- hooks

func (c *DefaultMQPushConsumer) consumeMessageHooks() []ConsumeMessageHook {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]ConsumeMessageHook(nil), c.consumeMessageHookList...)
}

func (c *DefaultMQPushConsumer) filterMessageHooks() []FilterMessageHook {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]FilterMessageHook(nil), c.filterMessageHookList...)
}

// buildConsumeHookContext matches Java's initial values: success=false, empty
// props.
func (c *DefaultMQPushConsumer) buildConsumeHookContext(msgs []*common.MessageExt, mq common.MessageQueue) *ConsumeMessageContext {
	return &ConsumeMessageContext{
		ConsumerGroup: c.consumerGroup,
		MsgList:       msgs,
		MQ:            mq,
		Success:       false,
		Props:         map[string]string{},
		AccessChannel: AccessChannelLocal,
	}
}

// finishConsumeHook is Java's hook epilogue (ConsumeMessageConcurrentlyService
// :395-412). consumeRT is the value the caller already measured and handed to
// the statistics, so the trace's contextCode and CONSUME_RT cannot disagree.
func (c *DefaultMQPushConsumer) finishConsumeHook(hooks []ConsumeMessageHook, ctx *ConsumeMessageContext,
	rawStatus ConsumeConcurrentlyStatus, panicked bool, consumeRT int64,
	status ConsumeConcurrentlyStatus, ackIndex, msgCount int) {

	if ctx == nil {
		return
	}
	unknown := rawStatus != ConsumeSuccess && rawStatus != ReconsumeLater
	ctx.Props[common.ConsumeContextType] = c.consumeReturnType(unknown, panicked,
		status == ReconsumeLater, consumeRT).String()
	ctx.Success = status == ConsumeSuccess
	ctx.Status = status.String()
	executeConsumeHookAfter(hooks, ctx)
}

// finishOrderlyHook is the orderly flavour (ConsumeMessageOrderlyService:498-512),
// again sharing the caller's single consumeRT measurement.
func (c *DefaultMQPushConsumer) finishOrderlyHook(hooks []ConsumeMessageHook, ctx *ConsumeMessageContext,
	rawStatus ConsumeOrderlyStatus, panicked bool, consumeRT int64, status ConsumeOrderlyStatus) {

	if ctx == nil {
		return
	}
	unknown := rawStatus != OrderlySuccess && rawStatus != OrderlyRollback &&
		rawStatus != OrderlyCommit && rawStatus != OrderlySuspendCurrentQueueAMoment
	ctx.Props[common.ConsumeContextType] = c.consumeReturnType(unknown, panicked,
		status == OrderlySuspendCurrentQueueAMoment, consumeRT).String()
	// Java:483-511 — the hook sees the NORMALISED status, while the returnType
	// is derived from the raw one; success is SUCCESS||COMMIT.
	ctx.Success = status == OrderlySuccess || status == OrderlyCommit
	ctx.Status = status.String()
	executeConsumeHookAfter(hooks, ctx)
}

// consumeReturnType is Python _consume_return_type (Rust consume_return_type) —
// the value lands in the trace's SubAfter contextCode.
//
// unknown stands for Java's null listener return: a Go listener cannot return
// null, so the out-of-range value the callers already normalise the same way
// takes its place. Order of the checks is Java's (concurrent :381-393,
// orderly :483-496): null, then the RT ceiling, then the outcome.
func (c *DefaultMQPushConsumer) consumeReturnType(unknown, panicked, failed bool, rtMs int64) ConsumeReturnType {
	if unknown {
		if panicked {
			return ConsumeReturnException
		}
		return ConsumeReturnNull
	}
	if rtMs >= c.consumeTimeout*60*1000 {
		return ConsumeReturnTimeout
	}
	if failed {
		return ConsumeReturnFailed
	}
	return ConsumeReturnSuccess
}

// AccessChannelLocal is Java AccessChannel.LOCAL.
const AccessChannelLocal = "LOCAL"

// ---------------------------------------------------------------- misc

func i32Text(v int32) string { return fmt.Sprintf("%d", v) }

// subscriptionFor looks one topic's subscription up.
func (c *DefaultMQPushConsumer) subscriptionFor(topic string) (*remoting.SubscriptionData, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	sub, ok := c.subscription[topic]
	return sub, ok
}
