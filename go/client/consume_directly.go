// CONSUME_MESSAGE_DIRECTLY(309) — the broker hands ONE message to this client
// and the listener must really consume it once.
//
// This is the admin "consume message directly" path (madmin
// consumeMessageDirectly). The verdict travels back in a
// ConsumeMessageDirectlyResult, and the two consume modes differ in ways the
// broker reads:
package client

import (
	"fmt"

	"github.com/zhaohai666/rocketmq-client-remoting/go/common"
	"github.com/zhaohai666/rocketmq-client-remoting/go/remoting"
)

func (c *DefaultMQPushConsumer) consumeMessageDirectly(msg *common.MessageExt, brokerName string) *remoting.ConsumeMessageDirectlyResult {
	result := remoting.NewConsumeMessageDirectlyResult()
	orderly := c.isOrderly()
	result.Order = orderly

	if msg != nil {
		msg.BrokerName = brokerName
	}
	msgs := []*common.MessageExt{msg}
	mq := common.NewMessageQueue(msg.Topic, brokerName, msg.QueueID)

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
	}

	result.SpentTimeMills = common.CurrentTimeMillis() - begin
	common.LogInfof("consumeMessageDirectly result: %s", consumeDirectlyResultText(result))
	return result
}

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
