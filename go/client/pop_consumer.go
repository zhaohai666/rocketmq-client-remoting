// POP differs from pull in ways that make a "just reuse the pull loop" port
// wrong at every step:
package client

import (
	"sort"
	"time"

	"github.com/zhaohai666/rocketmq-client-remoting/go/common"
	"github.com/zhaohai666/rocketmq-client-remoting/go/remoting"
)

const (
	// popBrokerSuspendMaxTimeMillis is BROKER_SUSPEND_MAX_TIME_MILLIS: the
	// long-poll budget sent as pollTime.
	popBrokerSuspendMaxTimeMillis int64 = 1000 * 15
	// popAsyncTimeout is ASYNC_TIMEOUT, used for ACK and CHANGE_INVISIBLETIME.
	popAsyncTimeoutMillis        int64 = 3000
	popRequestExtraNetworkMillis int64 = 10 * 1000

	popMinInvisibleTime int64 = 5000
	popMaxInvisibleTime int64 = 300000

	popDelayWhenCacheFlowControl = 50
	popDelayWhenBrokerFlow       = 20
	popDelayWhenSuspend          = 1000
	popDelayWhenException        = 3000

	popDefaultInvisibleTime int64 = 60000
)

// queuePopLoop pops one queue until it is retired or the consumer stops.
func (c *DefaultMQPushConsumer) queuePopLoop(mq common.MessageQueue, stop chan struct{}) {
	defer func() {
		if r := recover(); r != nil {
			common.LogErrorf("pop loop panicked for %v: %v", mq, r)
		}
	}()

	for {
		if c.stopRequested(stop) {
			return
		}
		pq := c.popProcessQueueOf(mq)
		if pq == nil || pq.IsDropped() {
			return
		}
		pq.SetLastPopTimestamp(common.CurrentTimeMillis())

		if _, hasSub := c.subscriptionFor(mq.Topic); !hasSub {
			return
		}
		if c.isPausedForPop() {
			if c.sleepOrStop(stop, popDelayWhenSuspend*time.Millisecond) {
				return
			}
			continue
		}
		if c.popFlowControlHit(mq, pq) {
			if c.sleepOrStop(stop, popDelayWhenCacheFlowControl*time.Millisecond) {
				return
			}
			continue
		}

		sub, _ := c.subscriptionFor(mq.Topic)
		begin := common.CurrentTimeMillis()
		result, err := c.popOnce(mq, sub)
		if err != nil {
			if e, ok := err.(*common.Error); ok && e.Kind == common.KindServer && e.Code == remoting.RespFlowControl {
				// Broker-side flow control: back off briefly, do not rescan.
				if c.sleepOrStop(stop, popDelayWhenBrokerFlow*time.Millisecond) {
					return
				}
				continue
			}
			// A long poll timing out while idle is normal (the broker clamps
			// the suspend time); keep it at debug so the run log stays clean.
			if isTimeout(err) {
				common.LogDebugf("pop long-poll timeout for %v (benign, will retry): %v", mq, err)
			} else {
				common.LogDebugf("pop error for %v: %v", mq, err)
				if c.sleepOrStop(stop, popDelayWhenException*time.Millisecond) {
					return
				}
			}
			continue
		}

		if c.stopRequested(stop) {
			return
		}
		switch result.PopStatus {
		case PopFound:
			c.incPullRT(mq.Topic, common.CurrentTimeMillis()-begin)
			if len(result.MsgFoundList) == 0 {
				continue
			}
			c.incPullTPS(mq.Topic, int64(len(result.MsgFoundList)))
			pq.IncFoundMsg(len(result.MsgFoundList))
			c.submitPopConsume(mq, result.MsgFoundList, pq)
			if interval := c.pullIntervalValue(); interval > 0 {
				if c.sleepOrStop(stop, time.Duration(interval)*time.Millisecond) {
					return
				}
			}
		case PopNoNewMsg, PopPollingNotFound:
			// Retry immediately: the broker already held the poll open.
			continue
		default:
			// POLLING_FULL and anything else: back off.
			if c.sleepOrStop(stop, popDelayWhenException*time.Millisecond) {
				return
			}
		}
	}
}

// popFlowControlHit reports whether this queue's outstanding-ACK debt is over the
// POP cache threshold, and records the trip on the SAME counter the pull path's
// cache gate bumps. One client-wide flowControlTimes number therefore spans both
// consumption modes: an operator reading the diagnostics sees the whole throttling
// picture of this client instead of half of it, and the WARN cadence (first trip,
// then one per flowControlWarnEvery trips) keeps its meaning across both paths.
//
// Only THIS queue's own debt is judged — popThresholdForQueue is a plain per-queue
// knob and is never divided at rebalance (see divideTopicThresholds). Like the
// pull gates the comparison is strictly greater than, so a queue sitting exactly on
// the threshold keeps popping; the wait itself stays with the loop, so a shutdown
// interrupts it instead of running the pause out.
func (c *DefaultMQPushConsumer) popFlowControlHit(mq common.MessageQueue, pq *popProcessQueue) bool {
	if pq == nil {
		return false
	}
	waitAck := pq.WaitAckMsgCount()
	threshold := c.popThresholdForQueueValue()
	if waitAck <= threshold {
		return false
	}

	c.mu.Lock()
	c.flowControlTriggered++
	times := c.flowControlTriggered
	c.mu.Unlock()
	common.LogDebugf("pop flow control: queue %v waiting-ack=%d exceeds pop threshold %d, pause pop",
		mq, waitAck, threshold)
	if isFlowControlWarnTrip(times) {
		common.LogWarnf("pop flow control: queue %v waiting-ack=%d exceeds pop threshold %d, pause pop for %dms (flowControlTimes=%d)",
			mq, waitAck, threshold, popDelayWhenCacheFlowControl, times)
	}
	return true
}

// popOnce issues one POP_MESSAGE and returns the processed result.
func (c *DefaultMQPushConsumer) popOnce(mq common.MessageQueue, sub *remoting.SubscriptionData) (*PopResult, error) {
	c.mu.Lock()
	invisibleTime := c.popInvisibleTime
	batchNums := c.popBatchNums
	namespace := c.namespace
	inst := c.instance
	c.mu.Unlock()
	if inst == nil {
		return nil, common.ClientError("consumer not started")
	}
	if invisibleTime < popMinInvisibleTime || invisibleTime > popMaxInvisibleTime {
		invisibleTime = popDefaultInvisibleTime
	}

	addr, _, found := inst.FindBrokerAddressInSubscribe(mq.BrokerName, int64(common.MasterID), true)
	if !found {
		if _, err := inst.UpdateTopicRouteInfoFromNameServer(mq.Topic, 0, false); err != nil {
			return nil, err
		}
		addr, _, found = inst.FindBrokerAddressInSubscribe(mq.BrokerName, int64(common.MasterID), true)
		if !found {
			return nil, common.ClientError("The broker[" + mq.BrokerName + "] not exist")
		}
	}

	born := common.CurrentTimeMillis()
	header := &remoting.PopMessageRequestHeader{
		Bname:         remoting.StrPtr(mq.BrokerName),
		ConsumerGroup: remoting.StrPtr(c.consumerGroup),
		Topic:         remoting.StrPtr(mq.Topic),
		QueueID:       remoting.I32Ptr(mq.QueueID),
		MaxMsgNums:    remoting.I32Ptr(batchNums),
		InvisibleTime: remoting.I64Ptr(invisibleTime),
		PollTime:      remoting.I64Ptr(popBrokerSuspendMaxTimeMillis),
		BornTime:      remoting.I64Ptr(born),
		InitMode:      remoting.I32Ptr(0),
		// order stays false: this port does not do orderly POP.
		Order: remoting.BoolPtr(false),
	}
	if sub != nil {
		header.ExpType = remoting.StrPtr(sub.ExpressionType)
		header.Exp = remoting.StrPtr(sub.SubString)
	}
	timeout := popBrokerSuspendMaxTimeMillis + popRequestExtraNetworkMillis
	return inst.PopMessage(mq.BrokerName, addr, header, namespace, timeout)
}

// submitPopConsume splits one pop batch into consumeMessageBatchMaxSize chunks
// and hands each chunk to the consume service
// in order, so a batch is never consumed out of sequence.
func (c *DefaultMQPushConsumer) submitPopConsume(mq common.MessageQueue, msgs []*common.MessageExt, pq *popProcessQueue) {
	batchSize := maxInt(1, c.consumeMessageBatchMaxSizeValue())
	for total := 0; total < len(msgs); {
		end := total + batchSize
		if end > len(msgs) {
			end = len(msgs)
		}
		chunk := msgs[total:end]
		total = end
		if !c.beginInFlight() {
			// Shutdown froze new work: hand the debt back so nothing is left
			// waiting for an ACK that will never come.
			pq.DecFoundMsg(-len(chunk))
			return
		}
		select {
		case c.dispatchSem <- struct{}{}:
		case <-c.stopCh:
			c.inFlight.Done()
			pq.DecFoundMsg(-len(chunk))
			return
		}
		go func(chunk []*common.MessageExt) {
			defer func() {
				<-c.dispatchSem
				if r := recover(); r != nil {
					common.LogErrorf("pop consume panicked for %v: %v", mq, r)
					pq.DecFoundMsg(-len(chunk))
				}
				c.inFlight.Done()
			}()
			c.consumePopBatch(mq, chunk, pq)
		}(chunk)
	}
}

func (c *DefaultMQPushConsumer) consumePopBatch(mq common.MessageQueue, batch []*common.MessageExt, pq *popProcessQueue) {
	if len(batch) == 0 {
		return
	}
	if pq.IsDropped() {
		common.LogDebugf("pop batch dropped for %v (queue retired)", mq)
		return
	}
	if popBatchTimedOut(batch) {
		common.LogDebugf("the pop message time out so abort consume, mq=%v", mq)
		pq.DecFoundMsg(-len(batch))
		return
	}

	listener, ok := c.listener.(MessageListenerConcurrently)
	if !ok {
		common.LogErrorf("listener is not a MessageListenerConcurrently for group %s", c.consumerGroup)
		pq.DecFoundMsg(-len(batch))
		return
	}
	ctx := NewConsumeConcurrentlyContext(mq)
	c.resetRetryTopicAndNamespace(batch)

	var hookCtx *ConsumeMessageContext
	hooks := c.consumeMessageHooks()
	if len(hooks) > 0 {
		hookCtx = c.buildConsumeHookContext(batch, mq)
		executeConsumeHookBefore(hooks, hookCtx)
	}

	begin := common.CurrentTimeMillis()
	// Every message of the batch is stamped with CONSUME_START_TIME, the
	// same stamping the pull path applies.
	for _, msg := range batch {
		msg.PutProperty(common.PropertyConsumeStartTime, i64Text(common.CurrentTimeMillis()))
	}

	status, panicked := callConcurrently(listener, batch, ctx)
	if panicked {
		common.LogDebugf("pop listener error, treat as RECONSUME_LATER: mq=%v", mq)
		status = ReconsumeLater
	}
	consumeRT := common.CurrentTimeMillis() - begin
	rawStatus := status
	if status != ConsumeSuccess && status != ReconsumeLater {
		common.LogWarnf("consumeMessage return unknown status, Group: %s Msgs: %d MQ: %v", c.consumerGroup, len(batch), mq)
		status = ReconsumeLater
	}
	invisibleTime := popInvisibleTimeOf(batch)
	c.finishPopConsumeHook(hooks, hookCtx, rawStatus, panicked, status, consumeRT, invisibleTime)
	c.incConsumeRT(mq.Topic, consumeRT)

	if pq.IsDropped() || popBatchTimedOut(batch) {
		common.LogWarnf("processQueue invalid or popTimeout, mq=%v", mq)
		pq.DecFoundMsg(-len(batch))
		return
	}
	c.processPopConsumeResult(mq, batch, pq, status, ctx)
}

// finishPopConsumeHook is the POP flavour of the consume-hook epilogue.
//
// It cannot reuse finishConsumeHook: the pull path declares TIME_OUT when the
// listener took longer than consumeTimeout MINUTES, while the POP path declares
// it when the listener took longer than the batch's invisibility window — which
// is seconds, not minutes. Sharing the helper would make a POP trace report
// SUCCESS for a listener that overran its window.
func (c *DefaultMQPushConsumer) finishPopConsumeHook(hooks []ConsumeMessageHook, ctx *ConsumeMessageContext,
	rawStatus ConsumeConcurrentlyStatus, panicked bool, status ConsumeConcurrentlyStatus,
	consumeRT, invisibleTime int64) {

	if ctx == nil {
		return
	}
	unknown := rawStatus != ConsumeSuccess && rawStatus != ReconsumeLater
	returnType := ConsumeReturnSuccess
	switch {
	case unknown:
		if panicked {
			returnType = ConsumeReturnException
		} else {
			returnType = ConsumeReturnNull
		}
	case consumeRT >= invisibleTime*1000:
		returnType = ConsumeReturnTimeout
	case status == ReconsumeLater:
		returnType = ConsumeReturnFailed
	}
	ctx.Props[common.ConsumeContextType] = returnType.String()
	ctx.Success = status == ConsumeSuccess
	ctx.Status = status.String()
	executeConsumeHookAfter(hooks, ctx)
}

// Two loops, and EVERY message decrements the debt exactly once, so the counter
// returns to zero whatever the outcome:
//
//   - [0, ackIndex] are ACKed;
//   - (ackIndex, size) are re-hidden: either with a longer invisibility window
//     (changePopInvisibleTime) or — once the message has exhausted its retry
//     budget — by checkNeedAckOrDelay, which gives up and ACKs when the message
//     is older than twice the longest delay.
func (c *DefaultMQPushConsumer) processPopConsumeResult(mq common.MessageQueue, batch []*common.MessageExt,
	pq *popProcessQueue, status ConsumeConcurrentlyStatus, ctx *ConsumeConcurrentlyContext) {

	ackIndex := -1
	if status == ConsumeSuccess {
		ackIndex = ctx.AckIndex
		if ackIndex >= len(batch) {
			ackIndex = len(batch) - 1
		}
	}
	if status == ConsumeSuccess {
		c.recordConsumeSuccessTPS(mq.Topic, ackIndex, len(batch))
	} else {
		c.incConsumeFailedTPS(mq.Topic, int64(len(batch)))
	}

	for i := 0; i < len(batch); i++ {
		msg := batch[i]
		if i <= ackIndex {
			c.ackMessagePop(msg)
		} else if msg.ReconsumeTimes >= c.popMaxReconsumeTimes() {
			// Retry budget exhausted. checkNeedAckOrDelay either ACKs the
			// message for good or extends its invisibility by the next notch.
			c.checkNeedAckOrDelay(msg)
		} else {
			delayLevel := int(ctx.DelayLevelWhenNextConsume)
			c.changePopInvisibleTime(msg, delayLevel)
		}
		pq.Ack()
	}
}

func ackIndexFor(status ConsumeConcurrentlyStatus, ctxAckIndex, size int) int {
	if status != ConsumeSuccess {
		return -1
	}
	if ctxAckIndex >= size {
		return size - 1
	}
	return ctxAckIndex
}

func (c *DefaultMQPushConsumer) popMaxReconsumeTimes() int32 {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.maxReconsumeTimes == -1 {
		return 16
	}
	return c.maxReconsumeTimes
}

// The request fields are read back out of the checkpoint, with two exceptions:
// the topic is re-derived through getRealTopic (so a retried message ACKs on
// the pop-retry topic) and the offset is segment 7 — the message's own queue
// offset, never the batch start.
func (c *DefaultMQPushConsumer) ackMessagePop(msg *common.MessageExt) {
	extraInfo := propertyOf(msg, common.PropertyPopCk)
	if extraInfo == "" {
		common.LogWarnf("ack skipped: message %s carries no POP_CK", msg.MsgID)
		return
	}
	parts := common.SplitExtraInfo(extraInfo)
	brokerName, err := common.GetBrokerName(parts)
	if err != nil {
		common.LogWarnf("ack failed to parse POP_CK %q: %v", extraInfo, err)
		return
	}
	queueID, err := common.GetQueueId(parts)
	if err != nil {
		common.LogWarnf("ack failed to parse POP_CK %q: %v", extraInfo, err)
		return
	}
	queueOffset, err := common.GetQueueOffset(parts)
	if err != nil {
		common.LogWarnf("ack failed to parse POP_CK %q: %v", extraInfo, err)
		return
	}
	topic := common.GetRealTopic(parts, msg.Topic, c.consumerGroup)

	addr, err := c.popAckAddress(brokerName, msg.Topic, int32(queueID))
	if err != nil {
		common.LogWarnf("ack failed, broker %s not resolvable: %v", brokerName, err)
		return
	}
	header := &remoting.AckMessageRequestHeader{
		Bname:         remoting.StrPtr(brokerName),
		ConsumerGroup: remoting.StrPtr(c.consumerGroup),
		Topic:         remoting.StrPtr(topic),
		QueueID:       remoting.I32Ptr(int32(queueID)),
		ExtraInfo:     remoting.StrPtr(extraInfo),
		Offset:        remoting.I64Ptr(queueOffset),
	}
	if err := c.instance.AckMessage(addr, header, popAsyncTimeoutMillis); err != nil {
		common.LogWarnf("Ack message fail. extraInfo: %s error: %v", extraInfo, err)
	}
}

func (c *DefaultMQPushConsumer) changePopInvisibleTime(msg *common.MessageExt, delayLevel int) {
	if delayLevel == 0 {
		delayLevel = int(msg.ReconsumeTimes)
	}
	c.changePopInvisibleTimeAtLevel(msg, delayLevel)
}

// changePopInvisibleTimeAtLevel is the shared body with the level ALREADY
// resolved: the "0 means unset" rule must not be applied a second time here, or
// a deliberate level-0 (the 10s back-off) would be re-read as "use the
// reconsume count" and every give-up path would silently wait much longer.
func (c *DefaultMQPushConsumer) changePopInvisibleTimeAtLevel(msg *common.MessageExt, delayLevel int) {
	delaySecond := popDelayLevelSeconds(delayLevel)
	extraInfo := propertyOf(msg, common.PropertyPopCk)
	if extraInfo == "" {
		common.LogWarnf("changePopInvisibleTime skipped: message %s carries no POP_CK", msg.MsgID)
		return
	}
	parts := common.SplitExtraInfo(extraInfo)
	brokerName, err := common.GetBrokerName(parts)
	if err != nil {
		common.LogWarnf("changePopInvisibleTime failed to parse POP_CK %q: %v", extraInfo, err)
		return
	}
	queueID, err := common.GetQueueId(parts)
	if err != nil {
		common.LogWarnf("changePopInvisibleTime failed to parse POP_CK %q: %v", extraInfo, err)
		return
	}
	queueOffset, err := common.GetQueueOffset(parts)
	if err != nil {
		common.LogWarnf("changePopInvisibleTime failed to parse POP_CK %q: %v", extraInfo, err)
		return
	}
	topic := common.GetRealTopic(parts, msg.Topic, c.consumerGroup)
	addr, err := c.popAckAddress(brokerName, msg.Topic, int32(queueID))
	if err != nil {
		common.LogWarnf("changePopInvisibleTime failed, broker %s not resolvable: %v", brokerName, err)
		return
	}
	header := &remoting.ChangeInvisibleTimeRequestHeader{
		Bname:         remoting.StrPtr(brokerName),
		ConsumerGroup: remoting.StrPtr(c.consumerGroup),
		Topic:         remoting.StrPtr(topic),
		QueueID:       remoting.I32Ptr(int32(queueID)),
		ExtraInfo:     remoting.StrPtr(extraInfo),
		Offset:        remoting.I64Ptr(queueOffset),
		InvisibleTime: remoting.I64Ptr(delaySecond * 1000),
		Suspend:       remoting.BoolPtr(false),
	}
	if _, err := c.instance.ChangeInvisibleTime(addr, header, popAsyncTimeoutMillis); err != nil {
		common.LogErrorf("changePopInvisibleTimeAsync fail, group:%s msg:%s error:%v",
			c.consumerGroup, msg.MsgID, err)
	}
}

// Called once the retry budget is spent. If the message has been bouncing for
// longer than twice the longest back-off there is no point extending again — ACK
// it and let it go. Otherwise extend by the next notch above the elapsed time.
func (c *DefaultMQPushConsumer) checkNeedAckOrDelay(msg *common.MessageExt) {
	last := popDelayLevel[len(popDelayLevel)-1]
	elapsed := common.CurrentTimeMillis() - msg.BornTimestamp
	if elapsed > last*1000*2 {
		common.LogWarnf("Consume too many times, ack message async. message %s", msg)
		c.ackMessagePop(msg)
		return
	}
	delayLevel := popDelayLevelForElapsed(elapsed)
	if delayLevel < 0 {
		delayLevel = 0
	}
	c.changePopInvisibleTimeAtLevel(msg, delayLevel)
	common.LogWarnf("Consume too many times, but delay time %d not enough. changePopInvisibleTime to delayLevel %d . message key:%s",
		elapsed, delayLevel, msgKeysOf(msg))
}

func popBatchTimedOut(batch []*common.MessageExt) bool {
	if len(batch) == 0 {
		return true
	}
	parts := common.SplitExtraInfo(propertyOf(batch[0], common.PropertyPopCk))
	popTime, err := common.GetPopTime(parts)
	if err != nil {
		return true
	}
	invisible, err := common.GetInvisibleTime(parts)
	if err != nil {
		return true
	}
	if popTime <= 0 || invisible <= 0 {
		return true
	}
	return common.CurrentTimeMillis()-popTime >= invisible
}

// popInvisibleTimeOf reads the batch's invisibility window (0 when unknown),
// used for the consume-hook timing decision.
func popInvisibleTimeOf(batch []*common.MessageExt) int64 {
	if len(batch) == 0 {
		return 0
	}
	parts := common.SplitExtraInfo(propertyOf(batch[0], common.PropertyPopCk))
	v, err := common.GetInvisibleTime(parts)
	if err != nil {
		return 0
	}
	return v
}

func msgKeysOf(msg *common.MessageExt) string {
	if msg.Properties == nil {
		return ""
	}
	v, _ := msg.GetProperty(common.PropertyKeys)
	return v
}

// ---------------------------------------------------------------- tables

// syncPopLoops is syncPullLoops' POP twin: retire the queues we no longer own,
// then start a pop goroutine for each newly assigned one.
//
// Order matters for the same reason as the pull path (retire before add), and
// there is no offset to settle: POP keeps no client cursor, so retiring only has
// to stop the loop and mark the queue dropped so an in-flight batch aborts
// instead of ACKing a batch the broker has taken back.
func (c *DefaultMQPushConsumer) syncPopLoops() {
	current := map[common.MessageQueue]struct{}{}
	for _, mq := range c.assignedQueues() {
		current[mq] = struct{}{}
	}

	c.mu.Lock()
	changed := 0
	for mq := range c.popQueueTable {
		if _, ok := current[mq]; !ok {
			c.retirePopQueueLocked(mq)
			changed++
		}
	}
	for mq := range current {
		if _, ok := c.popQueueTable[mq]; !ok {
			changed++
		}
	}
	c.mu.Unlock()

	c.mu.Lock()
	for mq := range current {
		if _, ok := c.popQueueTable[mq]; ok {
			continue
		}
		c.popQueueTable[mq] = newPopProcessQueue()
		stop := make(chan struct{})
		c.queueStop[mq] = stop
		go c.queuePopLoop(mq, stop)
	}
	c.mu.Unlock()
	if changed > 0 {
		common.LogDebugf("pop queues synced, group=%s changed=%d", c.consumerGroup, changed)
	}
	// Like the pull path, this spends no broker beat of its own: the caller's
	// afterAssignmentChange beats once the topics that moved are known, so a round
	// that only rebuilt a loop for queues it still owns does not reach the broker.
}

// retirePopQueueLocked drops one queue's POP state. Caller must hold c.mu.
func (c *DefaultMQPushConsumer) retirePopQueueLocked(mq common.MessageQueue) {
	if stop, ok := c.queueStop[mq]; ok {
		close(stop)
		delete(c.queueStop, mq)
	}
	if pq := c.popQueueTable[mq]; pq != nil {
		pq.SetDropped(true)
	}
	delete(c.popQueueTable, mq)
}

// ---------------------------------------------------------------- broker mode

// SetMessageRequestModeOnBroker sends SET_MESSAGE_REQUEST_MODE(401) for every
// subscribed business topic, telling each broker to serve this
// (group, topic) pair in POP mode.
//
// The %RETRY% topics are skipped: the POP retry topic is
// %RETRY%<group>_<topic>, created by the broker, and its request mode follows
// the business topic.
func (c *DefaultMQPushConsumer) SetMessageRequestModeOnBroker(timeoutMillis int64) error {
	inst := c.instance
	if inst == nil {
		return common.ClientError("consumer not started")
	}
	c.mu.Lock()
	topics := make([]string, 0, len(c.subscription))
	for topic := range c.subscription {
		if common.IsRetryTopic(topic) || common.IsDLQTopic(topic) {
			continue
		}
		topics = append(topics, topic)
	}
	group := c.consumerGroup
	share := c.popShareQueueNum
	c.mu.Unlock()
	sort.Strings(topics)

	var firstErr error
	for _, topic := range topics {
		addr, err := inst.AddrFor(topic, "")
		if err != nil {
			common.LogWarnf("set message request mode POP: no broker for %s: %v", topic, err)
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		if err := inst.SetMessageRequestMode(addr, topic, group, remoting.MessageRequestModePop, share, timeoutMillis); err != nil {
			common.LogWarnf("set message request mode POP failed for %s/%s: %v", group, topic, err)
			if firstErr == nil {
				firstErr = err
			}
		}
	}
	return firstErr
}

func (c *DefaultMQPushConsumer) popProcessQueueOf(mq common.MessageQueue) *popProcessQueue {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.popQueueTable[mq]
}

// PopProcessQueueCount is the number of queues currently popping (diagnostics /
// live assertions).
func (c *DefaultMQPushConsumer) PopProcessQueueCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.popQueueTable)
}

// PopWaitAckCount totals the outstanding ACK debt across every queue.
func (c *DefaultMQPushConsumer) PopWaitAckCount() int {
	c.mu.Lock()
	tables := make([]*popProcessQueue, 0, len(c.popQueueTable))
	for _, pq := range c.popQueueTable {
		tables = append(tables, pq)
	}
	c.mu.Unlock()
	total := 0
	for _, pq := range tables {
		total += pq.WaitAckMsgCount()
	}
	return total
}

func (c *DefaultMQPushConsumer) isPausedForPop() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.paused
}

func (c *DefaultMQPushConsumer) popThresholdForQueueValue() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.popThresholdForQueue
}

func (c *DefaultMQPushConsumer) consumeMessageBatchMaxSizeValue() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.consumeMessageBatchMaxSize
}
