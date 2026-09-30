// CONSUME_MESSAGE_DIRECTLY(309) — the broker hands ONE message to this client
// and the listener must really consume it once.
//
// This is the admin "consume message directly" path (madmin
// consumeMessageDirectly). The verdict travels back in a
// ConsumeMessageDirectlyResult, and the two consume modes differ in ways the
// broker reads:
//
//	concurrent : order=false, autoCommit=true — both set up front and NEVER
//	             changed. There is no autoCommit for a concurrent listener to
//	             report.
//	orderly    : order=true, and autoCommit is read from the context AFTER the
//	             listener ran (Java sets it right before returning). A binlog
//	             listener that turns autoCommit off must be reported honestly;
//	             reading the initial value would always send `true` back.
//
// One Java arm is unreachable here: a null listener return maps to
// CR_RETURN_NULL, but a Go listener returns a status VALUE, so `null` cannot be
// expressed. The constant stays declared for wire completeness.
package client

import (
	"fmt"

	"github.com/zhaohai666/rocketmq-client-remoting/go/common"
	"github.com/zhaohai666/rocketmq-client-remoting/go/remoting"
)

// consumeMessageDirectly mirrors Java
// ConsumeMessageConcurrentlyService#consumeMessageDirectly and
// ConsumeMessageOrderlyService#consumeMessageDirectly.
//
// Note what Java does NOT do here: no consume-message hooks run, no offset is
// committed, nothing is written to the process queue. It is a one-shot probe of
// the listener, nothing more.
func (c *DefaultMQPushConsumer) consumeMessageDirectly(msg *common.MessageExt, brokerName string) *remoting.ConsumeMessageDirectlyResult {
	result := remoting.NewConsumeMessageDirectlyResult()
	orderly := c.isOrderly()
	result.Order = orderly

	if msg != nil {
		msg.BrokerName = brokerName
	}
	msgs := []*common.MessageExt{msg}
	mq := common.NewMessageQueue(msg.Topic, brokerName, msg.QueueID)

	// Java resetRetryAndNamespace: a message the broker resent from
	// %RETRY%<group> is presented to the listener under its ORIGINAL topic.
	c.resetRetryTopicAndNamespace(msgs)

	begin := common.CurrentTimeMillis()
	common.LogInfof("consumeMessageDirectly receive new message: %s", msg)

	if orderly {
		ctx := NewConsumeOrderlyContext(mq)
		if listener, ok := c.listener.(MessageListenerOrderly); ok {
			status, panicVal, panicked := callOrderlyDirectly(listener, msgs, ctx)
			if panicked {
				setThrowException(result, panicVal)
			} else {
				result.ConsumeResult = orderlyResultFor(status)
			}
		}
		result.AutoCommit = ctx.AutoCommit
	} else {
		ctx := NewConsumeConcurrentlyContext(mq)
		if listener, ok := c.listener.(MessageListenerConcurrently); ok {
			status, panicVal, panicked := callConcurrentlyDirectly(listener, msgs, ctx)
			if panicked {
				setThrowException(result, panicVal)
			} else {
				result.ConsumeResult = concurrentResultFor(status)
			}
		}
		// Java leaves autoCommit at its constructor default (true) on this path.
	}

	result.SpentTimeMills = common.CurrentTimeMillis() - begin
	common.LogInfof("consumeMessageDirectly result: %s", consumeDirectlyResultText(result))
	return result
}

// orderlyResultFor is Java's orderly switch. Note COMMIT and ROLLBACK are the
// two members the concurrent switch lacks — the concurrent path returns them
// only by falling into `default` (i.e. never).
func orderlyResultFor(status ConsumeOrderlyStatus) *remoting.CMResult {
	switch status {
	case OrderlyCommit:
		return cmResultPtr(remoting.CMResultCommit)
	case OrderlyRollback:
		return cmResultPtr(remoting.CMResultRollback)
	case OrderlySuccess:
		return cmResultPtr(remoting.CMResultSuccess)
	case OrderlySuspendCurrentQueueAMoment:
		return cmResultPtr(remoting.CMResultLater)
	}
	return nil
}

func concurrentResultFor(status ConsumeConcurrentlyStatus) *remoting.CMResult {
	switch status {
	case ConsumeSuccess:
		return cmResultPtr(remoting.CMResultSuccess)
	case ReconsumeLater:
		return cmResultPtr(remoting.CMResultLater)
	}
	return nil
}

func cmResultPtr(r remoting.CMResult) *remoting.CMResult { return &r }

// setThrowException fills the CR_THROW_EXCEPTION verdict plus the remark.
//
// Java's remark is UtilAll.exceptionSimpleDesc(e) — "class: message" followed by
// the first stack frame. Go tolerates the same shape through %T/%v (the type is
// the analogue of the Java class name); the stack frame is dropped, matching the
// reduced form the other ports use.
func setThrowException(result *remoting.ConsumeMessageDirectlyResult, panicVal any) {
	result.ConsumeResult = cmResultPtr(remoting.CMResultThrowException)
	remark := fmt.Sprintf("panic: %v", panicVal)
	if err, ok := panicVal.(error); ok {
		remark = fmt.Sprintf("%T: %v", err, err)
	}
	result.Remark = &remark
	common.LogWarnf("consumeMessageDirectly exception: %s", remark)
}

// callConcurrentlyDirectly invokes the listener and reports a panic instead of
// flattening it into a retry status.
//
// callConcurrently (the normal consume path) deliberately turns a panic into
// ReconsumeLater, which is right there — the batch must come back. Here Java
// catches Throwable and answers CR_THROW_EXCEPTION, so the two must not share a
// helper.
func callConcurrentlyDirectly(listener MessageListenerConcurrently, batch []*common.MessageExt,
	ctx *ConsumeConcurrentlyContext) (status ConsumeConcurrentlyStatus, panicVal any, panicked bool) {
	defer func() {
		if r := recover(); r != nil {
			panicked = true
			panicVal = r
		}
	}()
	return listener.ConsumeMessage(batch, ctx), nil, false
}

func callOrderlyDirectly(listener MessageListenerOrderly, batch []*common.MessageExt,
	ctx *ConsumeOrderlyContext) (status ConsumeOrderlyStatus, panicVal any, panicked bool) {
	defer func() {
		if r := recover(); r != nil {
			panicked = true
			panicVal = r
		}
	}()
	return listener.ConsumeMessage(batch, ctx), nil, false
}

// consumeDirectlyResultText is Java's ConsumeMessageDirectlyResult#toString.
func consumeDirectlyResultText(r *remoting.ConsumeMessageDirectlyResult) string {
	result := ""
	if r.ConsumeResult != nil {
		result = string(*r.ConsumeResult)
	}
	remark := ""
	if r.Remark != nil {
		remark = *r.Remark
	}
	return fmt.Sprintf("ConsumeMessageDirectlyResult [order=%t, autoCommit=%t, consumeResult=%s, remark=%s, spentTimeMills=%d]",
		r.Order, r.AutoCommit, result, remark, r.SpentTimeMills)
}
