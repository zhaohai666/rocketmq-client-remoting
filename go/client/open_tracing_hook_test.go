package client

import (
	"fmt"
	"testing"

	"github.com/zhaohai666/rocketmq-client-remoting/go/common"
)

// ---------------------------------------------------------------- fake tracer

type fakeOTSpan struct {
	tracer    *fakeOTTracer
	ops       []string
	operation string
	tags      map[string]any
	refs      []any
	finished  bool
}

func (s *fakeOTSpan) SetTag(key string, value any) {
	s.ops = append(s.ops, "tag:"+key)
	s.tags[key] = value
}

func (s *fakeOTSpan) Finish() {
	s.finished = true
}

type fakeOTTracer struct {
	spans    []*fakeOTSpan
	upstream any
	injected []any
}

func (t *fakeOTTracer) StartSpan(operation string, tags map[string]any, refs []any) OTSpan {
	s := &fakeOTSpan{tracer: t, operation: operation, tags: map[string]any{}, refs: refs}
	for k, v := range tags {
		s.tags[k] = v
	}
	t.spans = append(t.spans, s)
	return s
}

func (t *fakeOTTracer) Extract(carrier *common.StringMap) any {
	if t.upstream != nil && carrier != nil {
		return t.upstream
	}
	return nil
}

func (t *fakeOTTracer) Inject(spanContext any, carrier *common.StringMap) {
	carrier.Put("traceparent", "fake-trace-context")
	t.injected = append(t.injected, spanContext)
}

// ---------------------------------------------------------------- helpers

func otSendMsg(t *testing.T) *common.Message {
	msg := common.NewMessage("TopicTest", []byte("hello-world"))
	msg.SetTags("TagA")
	msg.SetKeys("KeyA")
	return msg
}

func otSendCtx(msg *common.Message) *SendMessageContext {
	return &SendMessageContext{
		Message:    msg,
		BrokerAddr: "127.0.0.1:10911",
		MsgType:    common.NormalMsg,
	}
}

func otExt(storeSize int32, retry int32) *common.MessageExt {
	msg := &common.MessageExt{}
	msg.Topic = "TopicTest"
	msg.MsgID = "EXT-MSG-ID"
	msg.StoreSize = storeSize
	msg.ReconsumeTimes = retry
	msg.PutProperty(common.PropertyMsgRegion, "DefaultRegion")
	msg.PutProperty(common.PropertyTags, "TagC")
	msg.PutProperty(common.PropertyKeys, "KeyC")
	return msg
}

func otTagValue(t *testing.T, span OTSpan, key string) any {
	t.Helper()
	return span.(*fakeOTSpan).tags[key]
}

func otRequireTag(t *testing.T, span OTSpan, key string, want any) {
	t.Helper()
	got := otTagValue(t, span, key)
	if got != want {
		t.Fatalf("tag %s = %v, want %v", key, got, want)
	}
}

// ---------------------------------------------------------------- send leg

func TestOTSendBeforeOpensProducerSpanWithDocumentedTags(t *testing.T) {
	tracer := &fakeOTTracer{}
	hook := &SendMessageOpenTracingHook{Tracer: tracer}
	ctx := otSendCtx(otSendMsg(t))
	hook.SendMessageBefore(ctx)

	if len(tracer.spans) != 1 {
		t.Fatalf("spans = %d, want 1", len(tracer.spans))
	}
	span := tracer.spans[0]
	if span.operation != "To_TopicTest" {
		t.Fatalf("operation = %q, want To_TopicTest", span.operation)
	}
	otRequireTag(t, span, OTSpanKind, OTSpanKindProducer)
	otRequireTag(t, span, OTPeerService, "rocketmq")
	otRequireTag(t, span, OTMessageBusDestination, "TopicTest")
	otRequireTag(t, span, OTRocketmqTags, "TagA")
	otRequireTag(t, span, OTRocketmqKeys, "KeyA")
	otRequireTag(t, span, OTRocketmqStoreHost, "127.0.0.1:10911")
	otRequireTag(t, span, OTRocketmqMsgType, "NORMAL_MSG")
	otRequireTag(t, span, OTRocketmqBodyLength, len("hello-world"))
	if span.finished {
		t.Fatal("span finished too early")
	}
	// the span rides in the MQTraceContext slot (Java setMqTraceContext)
	if ctx.MQTraceContext != OTSpan(span) {
		t.Fatal("span not stashed in MQTraceContext")
	}
}

func TestOTSendBeforeInjectsSpanContextIntoProperties(t *testing.T) {
	tracer := &fakeOTTracer{}
	hook := &SendMessageOpenTracingHook{Tracer: tracer}
	msg := otSendMsg(t)
	ctx := otSendCtx(msg)
	hook.SendMessageBefore(ctx)

	v, ok := msg.GetProperty("traceparent")
	if !ok || v != "fake-trace-context" {
		t.Fatalf("propagation key missing: %q ok=%v", v, ok)
	}
	if len(tracer.injected) != 1 || tracer.injected[0] != OTSpan(tracer.spans[0]) {
		t.Fatal("inject did not receive the just-started span")
	}
}

func TestOTSendBeforeUsesUpstreamContextAsRef(t *testing.T) {
	upstream := "upstream-context"
	tracer := &fakeOTTracer{upstream: upstream}
	hook := &SendMessageOpenTracingHook{Tracer: tracer}
	hook.SendMessageBefore(otSendCtx(otSendMsg(t)))

	if len(tracer.spans[0].refs) != 1 || tracer.spans[0].refs[0] != upstream {
		t.Fatalf("refs = %v, want [upstream]", tracer.spans[0].refs)
	}
}

func TestOTSendMsgTypeTagUsesJavaEnumName(t *testing.T) {
	tracer := &fakeOTTracer{}
	hook := &SendMessageOpenTracingHook{Tracer: tracer}
	ctx := otSendCtx(otSendMsg(t))
	ctx.MsgType = common.TransMsgHalf
	hook.SendMessageBefore(ctx)
	otRequireTag(t, tracer.spans[0], OTRocketmqMsgType, "TRANS_MSG_HALF")

	for tc, want := range map[common.MessageType]string{
		common.NormalMsg:      "NORMAL_MSG",
		common.TransMsgCommit: "TRANS_MSG_COMMIT",
		common.DelayMsg:       "DELAY_MSG",
		common.OrderMsg:       "ORDER_MSG",
	} {
		if got := otMsgTypeName(tc); got != want {
			t.Fatalf("otMsgTypeName(%v) = %q, want %q", tc, got, want)
		}
	}
}

func TestOTSendAfterClosesWithSuccessMsgIDRegionID(t *testing.T) {
	tracer := &fakeOTTracer{}
	hook := &SendMessageOpenTracingHook{Tracer: tracer}
	ctx := otSendCtx(otSendMsg(t))
	hook.SendMessageBefore(ctx)
	ctx.SendResult = &SendResult{SendStatus: SendOK, MsgID: "MSG-ID-1", RegionID: "DefaultRegion"}
	hook.SendMessageAfter(ctx)

	span := tracer.spans[0]
	otRequireTag(t, span, OTRocketmqSuccess, true)
	otRequireTag(t, span, OTRocketmqMsgID, "MSG-ID-1")
	otRequireTag(t, span, OTRocketmqRegionID, "DefaultRegion")
	if !span.finished {
		t.Fatal("span not finished")
	}
}

func TestOTSendAfterFailureReportsSuccessFalse(t *testing.T) {
	tracer := &fakeOTTracer{}
	hook := &SendMessageOpenTracingHook{Tracer: tracer}
	ctx := otSendCtx(otSendMsg(t))
	hook.SendMessageBefore(ctx)
	ctx.SendResult = &SendResult{SendStatus: FlushDiskTimeout, MsgID: "M", RegionID: "R"}
	hook.SendMessageAfter(ctx)
	otRequireTag(t, tracer.spans[0], OTRocketmqSuccess, false)
	if !tracer.spans[0].finished {
		t.Fatal("span not finished")
	}
}

func TestOTSendAfterNoResultLeavesSpanOpen(t *testing.T) {
	tracer := &fakeOTTracer{}
	hook := &SendMessageOpenTracingHook{Tracer: tracer}
	ctx := otSendCtx(otSendMsg(t))
	hook.SendMessageBefore(ctx)
	hook.SendMessageAfter(ctx)
	if tracer.spans[0].finished {
		t.Fatal("span closed without a send result")
	}
	ctx.SendResult = &SendResult{SendStatus: SendOK, MsgID: "M"} // RegionID empty
	hook.SendMessageAfter(ctx)
	if tracer.spans[0].finished {
		t.Fatal("span closed without a region id")
	}
}

func TestOTSendAfterWithoutBeforeIsNoop(t *testing.T) {
	tracer := &fakeOTTracer{}
	hook := &SendMessageOpenTracingHook{Tracer: tracer}
	ctx := otSendCtx(otSendMsg(t))
	ctx.SendResult = &SendResult{SendStatus: SendOK, MsgID: "M", RegionID: "R"}
	hook.SendMessageAfter(ctx)
	if len(tracer.spans) != 0 {
		t.Fatal("after without before must not open a span")
	}
}

// ---------------------------------------------------------------- consume leg

func TestOTConsumeBeforeOpensOneSpanPerMessage(t *testing.T) {
	tracer := &fakeOTTracer{}
	hook := &ConsumeMessageOpenTracingHook{Tracer: tracer}
	ctx := &ConsumeMessageContext{ConsumerGroup: "CID_test", MsgList: []*common.MessageExt{otExt(42, 2), otExt(42, 2)}}
	hook.ConsumeMessageBefore(ctx)

	if len(tracer.spans) != 2 {
		t.Fatalf("spans = %d, want 2", len(tracer.spans))
	}
	for _, span := range tracer.spans {
		if span.operation != "From_TopicTest" {
			t.Fatalf("operation = %q", span.operation)
		}
		otRequireTag(t, span, OTSpanKind, OTSpanKindConsumer)
		otRequireTag(t, span, OTPeerService, "rocketmq")
		otRequireTag(t, span, OTMessageBusDestination, "TopicTest")
		otRequireTag(t, span, OTRocketmqMsgID, "EXT-MSG-ID")
		otRequireTag(t, span, OTRocketmqTags, "TagC")
		otRequireTag(t, span, OTRocketmqKeys, "KeyC")
		otRequireTag(t, span, OTRocketmqBodyLength, 42)
		otRequireTag(t, span, OTRocketmqRetryTimers, 2)
		otRequireTag(t, span, OTRocketmqRegionID, "DefaultRegion")
		if span.finished {
			t.Fatal("span finished too early")
		}
	}
	stored, ok := ctx.MQTraceContext.([]OTSpan)
	if !ok || len(stored) != 2 {
		t.Fatalf("MQTraceContext not []OTSpan of 2: %T", ctx.MQTraceContext)
	}
}

func TestOTConsumeAfterTagsSuccessAndFinishesAll(t *testing.T) {
	tracer := &fakeOTTracer{}
	hook := &ConsumeMessageOpenTracingHook{Tracer: tracer}
	ctx := &ConsumeMessageContext{ConsumerGroup: "CID_test", MsgList: []*common.MessageExt{otExt(1, 0)}}
	hook.ConsumeMessageBefore(ctx)
	ctx.Success = false
	hook.ConsumeMessageAfter(ctx)
	otRequireTag(t, tracer.spans[0], OTRocketmqSuccess, false)
	if !tracer.spans[0].finished {
		t.Fatal("span not finished")
	}
}

func TestOTConsumeEmptyMsgListIsNoop(t *testing.T) {
	tracer := &fakeOTTracer{}
	hook := &ConsumeMessageOpenTracingHook{Tracer: tracer}
	ctx := &ConsumeMessageContext{ConsumerGroup: "CID_test"}
	hook.ConsumeMessageBefore(ctx)
	hook.ConsumeMessageAfter(ctx)
	if len(tracer.spans) != 0 || ctx.MQTraceContext != nil {
		t.Fatalf("empty list must be a noop, got %v / %v", tracer.spans, ctx.MQTraceContext)
	}
}

// ---------------------------------------------------------------- transaction leg

func TestOTEndTransactionOpensAndClosesImmediately(t *testing.T) {
	tracer := &fakeOTTracer{}
	hook := &EndTransactionOpenTracingHook{Tracer: tracer}
	msg := common.NewMessage("TopicTest", []byte("tx"))
	msg.SetTags("TagT")
	msg.SetKeys("KeyT")
	ctx := &EndTransactionContext{
		Message:              msg,
		BrokerAddr:           "127.0.0.1:10911",
		MsgID:                "TX-MSG-ID",
		TransactionID:        "TX-ID-9",
		TransactionState:     CommitMessage,
		FromTransactionCheck: true,
	}
	hook.EndTransaction(ctx)

	if len(tracer.spans) != 1 {
		t.Fatalf("spans = %d", len(tracer.spans))
	}
	span := tracer.spans[0]
	if span.operation != "EndTransaction" {
		t.Fatalf("operation = %q", span.operation)
	}
	otRequireTag(t, span, OTSpanKind, OTSpanKindProducer)
	otRequireTag(t, span, OTPeerService, "rocketmq")
	otRequireTag(t, span, OTMessageBusDestination, "TopicTest")
	otRequireTag(t, span, OTRocketmqTags, "TagT")
	otRequireTag(t, span, OTRocketmqKeys, "KeyT")
	otRequireTag(t, span, OTRocketmqStoreHost, "127.0.0.1:10911")
	otRequireTag(t, span, OTRocketmqMsgID, "TX-MSG-ID")
	otRequireTag(t, span, OTRocketmqMsgType, "Trans_msg_Commit")
	otRequireTag(t, span, OTRocketmqTransactionID, "TX-ID-9")
	otRequireTag(t, span, OTRocketmqTransactionState, "COMMIT_MESSAGE")
	otRequireTag(t, span, OTRocketmqIsFromTransactionCheck, true)
	if !span.finished {
		t.Fatal("span not finished")
	}
}

func TestOTEndTransactionWithoutMessageStillOpensSpan(t *testing.T) {
	tracer := &fakeOTTracer{}
	hook := &EndTransactionOpenTracingHook{Tracer: tracer}
	hook.EndTransaction(&EndTransactionContext{MsgID: "TX", TransactionState: RollbackMessage})
	span := tracer.spans[0]
	otRequireTag(t, span, OTMessageBusDestination, "")
	otRequireTag(t, span, OTRocketmqTransactionState, "ROLLBACK_MESSAGE")
	if !span.finished {
		t.Fatal("span not finished")
	}
}

func TestOTHookNamesMatchJavaImpls(t *testing.T) {
	if (&SendMessageOpenTracingHook{}).HookName() != "SendMessageOpenTracingHook" {
		t.Fatal("send hook name drifted")
	}
	if (&ConsumeMessageOpenTracingHook{}).HookName() != "ConsumeMessageOpenTracingHook" {
		t.Fatal("consume hook name drifted")
	}
	if (&EndTransactionOpenTracingHook{}).HookName() != "EndTransactionOpenTracingHook" {
		t.Fatal("end transaction hook name drifted")
	}
}

// Compile-time interface checks: the hook trio plugs into the ordinary hook lists.
var (
	_ SendMessageHook    = (*SendMessageOpenTracingHook)(nil)
	_ ConsumeMessageHook = (*ConsumeMessageOpenTracingHook)(nil)
	_ EndTransactionHook = (*EndTransactionOpenTracingHook)(nil)
	_ fmt.Stringer       = CommitMessage
)
