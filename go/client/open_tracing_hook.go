// OpenTracing-style hook trio for the message lifecycle.
//
// Port of the Java client's SendMessageOpenTracingHookImpl /
// ConsumeMessageOpenTracingHookImpl / EndTransactionOpenTracingHookImpl (the
// same trio the PHP port carries as OpenTracingHook.php), on top of a
// duck-typed Tracer SPI with zero third-party deps. The tracer is duck-typed
// on purpose: any io.opentracing-shaped tracer (or a test fake) plugs in.
//
// The span rides in the context's MQTraceContext slot, exactly like the Java
// original (context.setMqTraceContext(span)): before opens the span and
// stashes it, after reads it back and closes it. Registering both the trace
// dispatcher hooks and these hooks on the same producer is not supported
// (same restriction as Java).
//
// Cross-process propagation rides on the message properties themselves: the
// upstream context is extracted from the property StringMap and the
// just-started span's context is injected back into it, so whichever keys the
// tracer's injector writes travel in-band with the message.
package client

import (
	"github.com/zhaohai666/rocketmq-client-remoting/go/common"
)

// OpenTracing standard tags (io.opentracing.tag.Tags).
const (
	OTSpanKind              = "span.kind"
	OTSpanKindProducer      = "producer"
	OTSpanKindConsumer      = "consumer"
	OTPeerService           = "peer.service"
	OTMessageBusDestination = "message_bus.destination"
)

// The rocketmq.* tag keys, mirroring the Java TraceConstants / the PHP
// Trace.php block (the mgs_id/mgs_type spellings are the Java protocol
// literals -- the typo is load-bearing, do not "fix" it).
const (
	OTRocketmqService                = "rocketmq"
	OTRocketmqSuccess                = "rocketmq.success"
	OTRocketmqTags                   = "rocketmq.tags"
	OTRocketmqKeys                   = "rocketmq.keys"
	OTRocketmqStoreHost              = "rocketmq.store_host"
	OTRocketmqBodyLength             = "rocketmq.body_length"
	OTRocketmqMsgID                  = "rocketmq.mgs_id"
	OTRocketmqMsgType                = "rocketmq.mgs_type"
	OTRocketmqRegionID               = "rocketmq.region_id"
	OTRocketmqTransactionID          = "rocketmq.transaction_id"
	OTRocketmqTransactionState       = "rocketmq.transaction_state"
	OTRocketmqIsFromTransactionCheck = "rocketmq.is_from_transaction_check"
	OTRocketmqRetryTimers            = "rocketmq.retry_times"
)

const (
	otToPrefix       = "To_"
	otFromPrefix     = "From_"
	otEndTransaction = "EndTransaction"
	// The transaction leg reports the commit state only.
	otTransMsgCommit = "Trans_msg_Commit"
)

// OTSpan is the duck-typed OpenTracing span: enough surface for the hook trio
// to record and close a span without importing io.opentracing.
type OTSpan interface {
	SetTag(key string, value any)
	Finish()
}

// OTTracer is the duck-typed OpenTracing tracer.
//
//   - StartSpan opens a span; refs carries the upstream SpanContext (from
//     Extract) when there is one.
//   - Extract reads the upstream context from the message property map; a nil
//     return means "no upstream".
//   - Inject writes the propagation keys of the just-started span into the
//     property StringMap, which then travels in-band with the message.
type OTTracer interface {
	StartSpan(operation string, tags map[string]any, refs []any) OTSpan
	Extract(carrier *common.StringMap) any
	Inject(spanContext any, carrier *common.StringMap)
}

// otMsgTypeName maps the Go MessageType ordinal to the Java enum *name*
// (NORMAL_MSG...), which is what the tag reports.
func otMsgTypeName(t common.MessageType) string {
	switch t {
	case common.TransMsgHalf:
		return "TRANS_MSG_HALF"
	case common.TransMsgCommit:
		return "TRANS_MSG_COMMIT"
	case common.DelayMsg:
		return "DELAY_MSG"
	case common.OrderMsg:
		return "ORDER_MSG"
	default:
		return "NORMAL_MSG"
	}
}

func otString(v string, ok bool) string {
	if ok {
		return v
	}
	return ""
}

func otExtractUpstream(tracer OTTracer, props *common.StringMap) any {
	if props == nil {
		return nil
	}
	return tracer.Extract(props)
}

// ---------------------------------------------------------------- send leg

// SendMessageOpenTracingHook is the send leg: span "To_<topic>", kind
// producer, with the kind+service+destination+tags+keys+store_host+msg_type+
// body_length tags at open; success / msg_id / region_id at close.
type SendMessageOpenTracingHook struct {
	Tracer OTTracer
}

func (h *SendMessageOpenTracingHook) HookName() string {
	return "SendMessageOpenTracingHook"
}

func (h *SendMessageOpenTracingHook) SendMessageBefore(ctx *SendMessageContext) {
	if ctx == nil || ctx.Message == nil {
		return
	}
	msg := ctx.Message
	upstream := otExtractUpstream(h.Tracer, msg.Properties)
	tags := map[string]any{OTSpanKind: OTSpanKindProducer}
	var refs []any
	if upstream != nil {
		refs = []any{upstream}
	}
	span := h.Tracer.StartSpan(otToPrefix+msg.Topic, tags, refs)
	span.SetTag(OTPeerService, OTRocketmqService)
	span.SetTag(OTMessageBusDestination, msg.Topic)
	span.SetTag(OTRocketmqTags, otString(msg.GetTags()))
	span.SetTag(OTRocketmqKeys, otString(msg.GetKeys()))
	span.SetTag(OTRocketmqStoreHost, ctx.BrokerAddr)
	span.SetTag(OTRocketmqMsgType, otMsgTypeName(ctx.MsgType))
	body := msg.GetBody()
	span.SetTag(OTRocketmqBodyLength, len(body))
	// The just-started span's context is what travels: the downstream leg must
	// parent onto THIS send, not onto whatever upstream produced the context.
	if msg.Properties != nil {
		h.Tracer.Inject(span, msg.Properties)
	}
	ctx.MQTraceContext = span
}

func (h *SendMessageOpenTracingHook) SendMessageAfter(ctx *SendMessageContext) {
	if ctx == nil || ctx.MQTraceContext == nil {
		return
	}
	span, ok := ctx.MQTraceContext.(OTSpan)
	if !ok {
		return
	}
	if ctx.SendResult == nil || ctx.SendResult.RegionID == "" {
		return // nothing to report; the span stays open
	}
	span.SetTag(OTRocketmqSuccess, ctx.SendResult.SendStatus == SendOK)
	span.SetTag(OTRocketmqMsgID, ctx.SendResult.MsgID)
	span.SetTag(OTRocketmqRegionID, ctx.SendResult.RegionID)
	span.Finish()
}

// ---------------------------------------------------------------- consume leg

// ConsumeMessageOpenTracingHook is the consume leg: one span per message,
// "From_<topic>", kind consumer; msg_id / tags / keys / body_length (store
// size) / retry_times / region_id at open; success at close.
type ConsumeMessageOpenTracingHook struct {
	Tracer OTTracer
}

func (h *ConsumeMessageOpenTracingHook) HookName() string {
	return "ConsumeMessageOpenTracingHook"
}

func (h *ConsumeMessageOpenTracingHook) ConsumeMessageBefore(ctx *ConsumeMessageContext) {
	if ctx == nil || len(ctx.MsgList) == 0 {
		return
	}
	spans := make([]OTSpan, 0, len(ctx.MsgList))
	for _, msg := range ctx.MsgList {
		if msg == nil {
			continue
		}
		upstream := otExtractUpstream(h.Tracer, msg.Properties)
		tags := map[string]any{OTSpanKind: OTSpanKindConsumer}
		var refs []any
		if upstream != nil {
			refs = []any{upstream}
		}
		span := h.Tracer.StartSpan(otFromPrefix+msg.Topic, tags, refs)
		span.SetTag(OTPeerService, OTRocketmqService)
		span.SetTag(OTMessageBusDestination, common.WithoutNamespace(msg.Topic, ""))
		span.SetTag(OTRocketmqMsgID, msg.MsgID)
		span.SetTag(OTRocketmqTags, otString(msg.GetTags()))
		span.SetTag(OTRocketmqKeys, otString(msg.GetKeys()))
		span.SetTag(OTRocketmqBodyLength, int(msg.StoreSize))
		span.SetTag(OTRocketmqRetryTimers, int(msg.ReconsumeTimes))
		if region, ok := msg.GetProperty(common.PropertyMsgRegion); ok {
			span.SetTag(OTRocketmqRegionID, region)
		} else {
			span.SetTag(OTRocketmqRegionID, "")
		}
		spans = append(spans, span)
	}
	ctx.MQTraceContext = spans
}

func (h *ConsumeMessageOpenTracingHook) ConsumeMessageAfter(ctx *ConsumeMessageContext) {
	if ctx == nil || len(ctx.MsgList) == 0 {
		return
	}
	spans, ok := ctx.MQTraceContext.([]OTSpan)
	if !ok {
		return
	}
	for _, span := range spans {
		span.SetTag(OTRocketmqSuccess, ctx.Success)
		span.Finish()
	}
}

// ------------------------------------------------------------ transaction leg

// EndTransactionOpenTracingHook is the transaction leg: span "EndTransaction"
// with the transaction identity keys, closed immediately. The message is left
// untouched on this leg (the send side already put the context on the
// properties).
type EndTransactionOpenTracingHook struct {
	Tracer OTTracer
}

func (h *EndTransactionOpenTracingHook) HookName() string {
	return "EndTransactionOpenTracingHook"
}

func (h *EndTransactionOpenTracingHook) EndTransaction(ctx *EndTransactionContext) {
	if ctx == nil {
		return
	}
	msg := ctx.Message
	var props *common.StringMap
	if msg != nil {
		props = msg.Properties
	}
	upstream := otExtractUpstream(h.Tracer, props)
	tags := map[string]any{OTSpanKind: OTSpanKindProducer}
	var refs []any
	if upstream != nil {
		refs = []any{upstream}
	}
	span := h.Tracer.StartSpan(otEndTransaction, tags, refs)
	span.SetTag(OTPeerService, OTRocketmqService)
	span.SetTag(OTRocketmqTransactionID, ctx.TransactionID)
	span.SetTag(OTRocketmqTransactionState, ctx.TransactionState.String())
	span.SetTag(OTRocketmqIsFromTransactionCheck, ctx.FromTransactionCheck)
	span.SetTag(OTRocketmqStoreHost, ctx.BrokerAddr)
	span.SetTag(OTRocketmqMsgID, ctx.MsgID)
	span.SetTag(OTRocketmqMsgType, otTransMsgCommit)
	if msg != nil {
		span.SetTag(OTMessageBusDestination, msg.Topic)
		span.SetTag(OTRocketmqTags, otString(msg.GetTags()))
		span.SetTag(OTRocketmqKeys, otString(msg.GetKeys()))
	} else {
		span.SetTag(OTMessageBusDestination, "")
		span.SetTag(OTRocketmqTags, "")
		span.SetTag(OTRocketmqKeys, "")
	}
	span.Finish()
}
