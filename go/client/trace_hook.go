// Client-side trace hooks (Java org.apache.rocketmq.client.trace.hook.*,
// Python trace_hook.py).
//
//	SendMessageTraceHook        <- SendMessageTraceHookImpl
//	ConsumeMessageTraceHook     <- ConsumeMessageTraceHookImpl
//	EndTransactionTraceHook     <- EndTransactionTraceHookImpl
//
// Two rules every hook here obeys:
//
//  1. a trace message is never traced again — both halves check whether the
//     topic starts with the trace topic name and return early (anti-recursion
//     guard 2; guard 1 is the internal producer's own enableTrace=false);
//  2. whether a record is emitted at all is the BROKER's call — the send side
//     reads SendResult.RegionID/TraceOn, the consume side the TRACE_ON message
//     property — so a broker with trace disabled produces no trace traffic.
package client

import (
	"strings"

	"github.com/zhaohai666/rocketmq-client-remoting/go/common"
)

// SendMessageTraceHook turns one send into a TraceType.Pub record.
type SendMessageTraceHook struct{ dispatcher *AsyncTraceDispatcher }

// NewSendMessageTraceHook wires the hook to its dispatcher.
func NewSendMessageTraceHook(dispatcher *AsyncTraceDispatcher) *SendMessageTraceHook {
	return &SendMessageTraceHook{dispatcher: dispatcher}
}

// HookName is Java SendMessageTraceHookImpl#hookName.
func (h *SendMessageTraceHook) HookName() string { return "SendMessageTraceHook" }

// SendMessageBefore builds the Pub context. It runs per send ATTEMPT (the
// retry chain rebuilds the context each pass), so a retried send can produce
// several Pub records — Java behaves the same way.
func (h *SendMessageTraceHook) SendMessageBefore(ctx *SendMessageContext) {
	if ctx == nil || ctx.Message == nil {
		return
	}
	topic := ctx.Message.Topic
	if strings.HasPrefix(topic, h.dispatcher.TraceTopicName()) {
		return
	}
	traceContext := NewTraceContext()
	ctx.MQTraceContext = traceContext
	traceContext.TraceType = TracePub
	traceContext.GroupName = common.WithoutNamespace(ctx.ProducerGroup, ctx.Namespace)
	bean := newTraceBean()
	bean.Topic = common.WithoutNamespace(topic, ctx.Namespace)
	bean.Tags, _ = ctx.Message.GetTags()
	bean.Keys, _ = ctx.Message.GetKeys()
	bean.StoreHost = ctx.BrokerAddr
	bean.BodyLength = len(ctx.Message.GetBody())
	bean.MsgType = ctx.MsgType
	traceContext.TraceBeans = []*TraceBean{bean}
}

// SendMessageAfter completes the Pub record and appends it to the dispatcher.
//
// A missing result means the attempt never got an answer (or a oneway send):
// nothing is appended. Both the no-region and TRACE_ON=false cases are the
// broker saying "do not track this", and both are dropped.
func (h *SendMessageTraceHook) SendMessageAfter(ctx *SendMessageContext) {
	if ctx == nil || ctx.Message == nil {
		return
	}
	topic := ctx.Message.Topic
	if strings.HasPrefix(topic, h.dispatcher.TraceTopicName()) {
		return
	}
	traceContext, _ := ctx.MQTraceContext.(*TraceContext)
	if traceContext == nil || len(traceContext.TraceBeans) == 0 {
		return
	}
	result := ctx.SendResult
	if result == nil {
		return
	}
	if result.RegionID == "" || !result.TraceOn {
		return
	}
	bean := traceContext.TraceBeans[0]
	traceContext.CostTime = common.CurrentTimeMillis() - traceContext.TimeStamp
	traceContext.IsSuccess = result.SendStatus == SendOK
	traceContext.RegionID = result.RegionID
	bean.MsgID = result.MsgID
	// MsgID is the client UNIQ_KEY, OffsetMsgID the broker's id: the console
	// joins a publish trace with a consume trace on the former and looks the
	// message up on the latter.
	bean.OffsetMsgID = result.OffsetMsgID
	bean.StoreTime = traceContext.TimeStamp + traceContext.CostTime/2
	h.dispatcher.Append(traceContext)
}

// ConsumeMessageTraceHook emits the SubBefore/SubAfter pair of one consume.
// Both halves share the requestId minted in NewTraceContext, which is what lets
// the console pair them up.
type ConsumeMessageTraceHook struct{ dispatcher *AsyncTraceDispatcher }

// NewConsumeMessageTraceHook wires the hook to its dispatcher.
func NewConsumeMessageTraceHook(dispatcher *AsyncTraceDispatcher) *ConsumeMessageTraceHook {
	return &ConsumeMessageTraceHook{dispatcher: dispatcher}
}

// HookName is Java ConsumeMessageTraceHookImpl#hookName.
func (h *ConsumeMessageTraceHook) HookName() string { return "ConsumeMessageTraceHook" }

// ConsumeMessageBefore emits one SubBefore record covering the whole batch
// (Java sends one record per message inside a single TraceContext).
func (h *ConsumeMessageTraceHook) ConsumeMessageBefore(ctx *ConsumeMessageContext) {
	if ctx == nil || len(ctx.MsgList) == 0 {
		return
	}
	traceContext := NewTraceContext()
	ctx.MQTraceContext = traceContext
	traceContext.TraceType = TraceSubBefore
	traceContext.GroupName = common.WithoutNamespace(ctx.ConsumerGroup, ctx.Namespace)
	var beans []*TraceBean
	for _, msg := range ctx.MsgList {
		if msg == nil {
			continue
		}
		if v, ok := msg.GetProperty(common.PropertyTraceSwitch); ok && v == "false" {
			continue
		}
		bean := newTraceBean()
		bean.Topic = common.WithoutNamespace(msg.Topic, ctx.Namespace)
		bean.MsgID = msg.MsgID
		bean.Tags, _ = msg.GetTags()
		bean.Keys, _ = msg.GetKeys()
		bean.StoreTime = msg.StoreTimestamp
		bean.BodyLength = int(msg.StoreSize)
		bean.RetryTimes = msg.ReconsumeTimes
		if region, ok := msg.GetProperty(common.PropertyMsgRegion); ok {
			traceContext.RegionID = region
		}
		beans = append(beans, bean)
	}
	if len(beans) > 0 {
		traceContext.TraceBeans = beans
		traceContext.TimeStamp = common.CurrentTimeMillis()
		h.dispatcher.Append(traceContext)
	}
}

// ConsumeMessageAfter emits the SubAfter half. It deliberately appends even
// when the consume failed — that is what the contextCode and isSuccess fields
// are for; a before that produced no beans (every message had TRACE_ON=false)
// produces no after either.
func (h *ConsumeMessageTraceHook) ConsumeMessageAfter(ctx *ConsumeMessageContext) {
	if ctx == nil || len(ctx.MsgList) == 0 {
		return
	}
	subBefore, _ := ctx.MQTraceContext.(*TraceContext)
	if subBefore == nil || len(subBefore.TraceBeans) == 0 {
		return
	}
	subAfter := NewTraceContext()
	subAfter.TraceType = TraceSubAfter
	subAfter.RegionID = subBefore.RegionID
	subAfter.GroupName = common.WithoutNamespace(subBefore.GroupName, ctx.Namespace)
	subAfter.RequestID = subBefore.RequestID
	subAfter.AccessChannel = ctx.AccessChannel
	subAfter.IsSuccess = ctx.Success
	subAfter.CostTime = (common.CurrentTimeMillis() - subBefore.TimeStamp) / int64(len(ctx.MsgList))
	subAfter.TraceBeans = subBefore.TraceBeans
	if ctx.Props != nil {
		if name, ok := ctx.Props[common.ConsumeContextType]; ok {
			if ret, known := ConsumeReturnTypeByName(name); known {
				subAfter.ContextCode = int32(ret)
			}
		}
	}
	h.dispatcher.Append(subAfter)
}

// EndTransactionTraceHook records one transaction ending — from the caller's
// commit/rollback AND from the broker's check-back; FromTransactionCheck is the
// only field that tells the two apart.
type EndTransactionTraceHook struct{ dispatcher *AsyncTraceDispatcher }

// NewEndTransactionTraceHook wires the hook to its dispatcher.
func NewEndTransactionTraceHook(dispatcher *AsyncTraceDispatcher) *EndTransactionTraceHook {
	return &EndTransactionTraceHook{dispatcher: dispatcher}
}

// HookName is Java EndTransactionTraceHookImpl#hookName.
func (h *EndTransactionTraceHook) HookName() string { return "EndTransactionTraceHook" }

// EndTransaction emits the EndTransaction record.
func (h *EndTransactionTraceHook) EndTransaction(ctx *EndTransactionContext) {
	if ctx == nil || ctx.Message == nil {
		return
	}
	topic := ctx.Message.Topic
	if strings.HasPrefix(topic, h.dispatcher.TraceTopicName()) {
		return
	}
	msg := ctx.Message
	traceContext := NewTraceContext()
	traceContext.TraceType = TraceEndTransaction
	traceContext.GroupName = common.WithoutNamespace(ctx.ProducerGroup, ctx.Namespace)
	bean := newTraceBean()
	bean.Topic = common.WithoutNamespace(topic, ctx.Namespace)
	bean.Tags, _ = msg.GetTags()
	bean.Keys, _ = msg.GetKeys()
	bean.StoreHost = ctx.BrokerAddr
	bean.MsgType = common.TransMsgCommit
	// The record carries the HOST client's id, not the internal trace
	// producer's — the console shows where the transaction was decided.
	bean.ClientHost = h.dispatcher.clientID()
	bean.MsgID = ctx.MsgID
	bean.TransactionState = ctx.TransactionState.String()
	bean.TransactionID = ctx.TransactionID
	bean.FromTransactionCheck = ctx.FromTransactionCheck
	region, ok := msg.GetProperty(common.PropertyMsgRegion)
	if !ok || region == "" {
		region = common.DefaultTraceRegionID
	}
	traceContext.RegionID = region
	traceContext.TraceBeans = []*TraceBean{bean}
	traceContext.TimeStamp = common.CurrentTimeMillis()
	h.dispatcher.Append(traceContext)
}
