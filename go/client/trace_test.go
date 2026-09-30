// Message-trace tests. The first group is the one that matters most: every
// EXPECTED_* string below was printed by the OFFICIAL JAVA implementation
// (TraceDataEncoder in Apache RocketMQ 5.5.1, run by the parity probe recorded
// in python/tests/test_trace.py) with SOH/STX made readable. As long as the Go
// encoder reproduces them byte for byte, this client and the RocketMQ console
// (and the Java client) understand each other's traces.
//
// The second group is an in-process end-to-end pass: a real producer and a real
// push consumer, both with tracing on, against the mock cluster from
// instance_test.go — the trace topic is routed to the mock broker and the
// records it receives are decoded back and checked.
package client

import (
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/zhaohai666/rocketmq-client-remoting/go/common"
	"github.com/zhaohai666/rocketmq-client-remoting/go/remoting"
)

const (
	traceSOH = "\u0001"
	traceSTX = "\u0002"
)

var (
	javaMsgID1      = "AC1400A1F0A018B4AAC2A1B2C3D4E5F6"
	javaMsgID2      = "AC1400A1F0A018B4AAC2A1B2C3D4E5F7"
	javaOffsetMsgID = "AC1400A1000027100000000000000001"
	javaBody        = []byte(strings.Repeat("x", 42))

	expectedPub = strings.Join([]string{"Pub", "1700000000000", "DefaultRegion", "GID_test",
		"TopicTest", javaMsgID1, "TagA", "KeyA KeyB", "127.0.0.1:10911", "42", "7", "0",
		javaOffsetMsgID, "true"}, traceSOH) + traceSTX

	expectedSubBefore = strings.Join([]string{"SubBefore", "1700000000000", "DefaultRegion",
		"CID_test", "REQ-SUB-001", javaMsgID1, "2", "KeyA KeyB"}, traceSOH) + traceSTX +
		strings.Join([]string{"SubBefore", "1700000000000", "DefaultRegion",
			"CID_test", "REQ-SUB-001", javaMsgID2, "0", "KeyC"}, traceSOH) + traceSTX

	expectedSubAfter = strings.Join([]string{"SubAfter", "REQ-SUB-001", javaMsgID1, "11",
		"false", "KeyA KeyB", "2", "1700000000000", "CID_test"}, traceSOH) + traceSTX

	expectedEndTransaction = strings.Join([]string{"EndTransaction", "1700000000000",
		"DefaultRegion", "GID_test", "TopicTest", javaMsgID1, "TagA", "KeyA KeyB",
		"127.0.0.1:10911", "0", "TRAN-001", "COMMIT_MESSAGE", "false"}, traceSOH) + traceSTX

	expectedRecall = strings.Join([]string{"Recall", "1700000000000", "DefaultRegion",
		"GID_test", "TopicTest", javaMsgID1, "true"}, traceSOH) + traceSTX
)

// javaVectorBean fills every field the Java probe set on TraceBean.
func javaVectorBean(msgID, keys string, retryTimes int32) *TraceBean {
	b := newTraceBean()
	b.Topic = "TopicTest"
	b.MsgID = msgID
	b.OffsetMsgID = javaOffsetMsgID
	b.Tags = "TagA"
	b.Keys = keys
	b.StoreHost = "127.0.0.1:10911"
	b.StoreTime = 1700000000123
	b.RetryTimes = retryTimes
	b.BodyLength = 42
	b.MsgType = common.NormalMsg
	b.TransactionID = "TRAN-001"
	b.TransactionState = "COMMIT_MESSAGE"
	return b
}

func keysOf(set map[string]struct{}) []string {
	out := make([]string, 0, len(set))
	for k := range set {
		out = append(out, k)
	}
	return out
}

// equalStringSet compares two key sets without caring about order.
func equalStringSet(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	seen := make(map[string]int, len(a))
	for _, v := range a {
		seen[v]++
	}
	for _, v := range b {
		seen[v]--
		if seen[v] < 0 {
			return false
		}
	}
	return true
}

// ---------------------------------------------------------------- encoding

func TestTraceEncodePubMatchesJavaVector(t *testing.T) {
	ctx := &TraceContext{
		TraceType: TracePub,
		TimeStamp: 1700000000000,
		RegionID:  "DefaultRegion",
		GroupName: "GID_test",
		CostTime:  7,
		IsSuccess: true,
		RequestID: "REQ-PUB-001",
		TraceBeans: []*TraceBean{
			javaVectorBean(javaMsgID1, "KeyA KeyB", 2),
		},
	}
	tb := EncodeTraceContext(ctx)
	if tb.TransData != expectedPub {
		t.Errorf("encoded Pub differs from the Java vector:\n got %q\nwant %q", tb.TransData, expectedPub)
	}
	if got := keysOf(tb.TransKey); !equalStringSet(got, []string{javaMsgID1, "KeyA", "KeyB"}) {
		t.Errorf("trans keys = %v", got)
	}
}

func TestTraceEncodeSubBeforeMatchesJavaVector(t *testing.T) {
	ctx := &TraceContext{
		TraceType: TraceSubBefore,
		TimeStamp: 1700000000000,
		RegionID:  "DefaultRegion",
		GroupName: "CID_test",
		RequestID: "REQ-SUB-001",
		TraceBeans: []*TraceBean{
			javaVectorBean(javaMsgID1, "KeyA KeyB", 2),
			javaVectorBean(javaMsgID2, "KeyC", 0),
		},
	}
	tb := EncodeTraceContext(ctx)
	if tb.TransData != expectedSubBefore {
		t.Errorf("encoded SubBefore differs:\n got %q\nwant %q", tb.TransData, expectedSubBefore)
	}
	if got := keysOf(tb.TransKey); !equalStringSet(got,
		[]string{javaMsgID1, javaMsgID2, "KeyA", "KeyB", "KeyC"}) {
		t.Errorf("trans keys = %v", got)
	}
}

func TestTraceEncodeSubAfterMatchesJavaVector(t *testing.T) {
	ctx := &TraceContext{
		TraceType:     TraceSubAfter,
		TimeStamp:     1700000000000,
		GroupName:     "CID_test",
		RequestID:     "REQ-SUB-001",
		CostTime:      11,
		IsSuccess:     false,
		ContextCode:   2,
		AccessChannel: AccessChannelLocal,
		TraceBeans:    []*TraceBean{javaVectorBean(javaMsgID1, "KeyA KeyB", 2)},
	}
	if got := EncodeTraceContext(ctx).TransData; got != expectedSubAfter {
		t.Errorf("encoded SubAfter differs:\n got %q\nwant %q", got, expectedSubAfter)
	}
}

// A CLOUD dispatch leaves off the trailing timestamp/groupName pair: the
// Aliyun trace backend takes those from its own envelope.
func TestTraceEncodeSubAfterCloudDropsTimestampAndGroup(t *testing.T) {
	ctx := &TraceContext{
		TraceType:     TraceSubAfter,
		TimeStamp:     1700000000000,
		GroupName:     "CID_test",
		RequestID:     "REQ-SUB-001",
		CostTime:      11,
		IsSuccess:     false,
		ContextCode:   2,
		AccessChannel: AccessChannelCloud,
		TraceBeans:    []*TraceBean{javaVectorBean(javaMsgID1, "KeyA KeyB", 2)},
	}
	want := strings.Join([]string{"SubAfter", "REQ-SUB-001", javaMsgID1, "11", "false",
		"KeyA KeyB", "2"}, traceSOH) + traceSTX
	if got := EncodeTraceContext(ctx).TransData; got != want {
		t.Errorf("CLOUD SubAfter got %q, want %q", got, want)
	}
}

func TestTraceEncodeEndTransactionAndRecallMatchJavaVectors(t *testing.T) {
	endTx := &TraceContext{
		TraceType:  TraceEndTransaction,
		TimeStamp:  1700000000000,
		RegionID:   "DefaultRegion",
		GroupName:  "GID_test",
		TraceBeans: []*TraceBean{javaVectorBean(javaMsgID1, "KeyA KeyB", 2)},
	}
	if got := EncodeTraceContext(endTx).TransData; got != expectedEndTransaction {
		t.Errorf("encoded EndTransaction differs:\n got %q\nwant %q", got, expectedEndTransaction)
	}

	recall := &TraceContext{
		TraceType:  TraceRecall,
		TimeStamp:  1700000000000,
		RegionID:   "DefaultRegion",
		GroupName:  "GID_test",
		IsSuccess:  true,
		TraceBeans: []*TraceBean{javaVectorBean(javaMsgID1, "KeyA KeyB", 2)},
	}
	if got := EncodeTraceContext(recall).TransData; got != expectedRecall {
		t.Errorf("encoded Recall differs:\n got %q\nwant %q", got, expectedRecall)
	}
}

func TestTraceEncodeNilContextIsNil(t *testing.T) {
	if tb := EncodeTraceContext(nil); tb != nil {
		t.Errorf("encoding nil must yield nil, got %+v", tb)
	}
}

func TestTraceConstantsMatchJava(t *testing.T) {
	if TraceContentSplitor != "\u0001" || TraceFieldSplitor != "\u0002" {
		t.Errorf("splitors = %q/%q", TraceContentSplitor, TraceFieldSplitor)
	}
	if TraceGroupNamePrefix != "_INNER_TRACE_PRODUCER" {
		t.Errorf("group prefix = %q", TraceGroupNamePrefix)
	}
	if TraceInstanceName != "PID_CLIENT_INNER_TRACE_PRODUCER" {
		t.Errorf("instance name = %q", TraceInstanceName)
	}
	if TraceTopicPrefix != "rmq_sys_TRACE_DATA_" {
		t.Errorf("trace topic prefix = %q", TraceTopicPrefix)
	}
	if common.TraceTopic != "RMQ_SYS_TRACE_TOPIC" {
		t.Errorf("trace topic = %q", common.TraceTopic)
	}
	if common.DefaultTraceRegionID != "DefaultRegion" {
		t.Errorf("default region = %q", common.DefaultTraceRegionID)
	}
	if common.KeySeparator != " " || common.PropertyTraceSwitch != "TRACE_ON" {
		t.Errorf("key separator / trace switch = %q/%q", common.KeySeparator, common.PropertyTraceSwitch)
	}
	if got := []common.MessageType{common.NormalMsg, common.TransMsgHalf, common.TransMsgCommit,
		common.DelayMsg, common.OrderMsg}; len(got) != 5 || got[0] != 0 || got[4] != 4 {
		t.Errorf("message type ordinals are a wire format: %v", got)
	}
}

// ---------------------------------------------------------------- decoding

func TestTraceDecodePubRoundTrip(t *testing.T) {
	ctx := &TraceContext{
		TraceType: TracePub,
		TimeStamp: 1700000000000,
		RegionID:  "DefaultRegion",
		GroupName: "GID_test",
		CostTime:  7,
		IsSuccess: true,
		TraceBeans: []*TraceBean{
			javaVectorBean(javaMsgID1, "KeyA KeyB", 2),
		},
	}
	records := DecodeTraceDataString(EncodeTraceContext(ctx).TransData)
	if len(records) != 1 {
		t.Fatalf("decoded %d records, want 1", len(records))
	}
	got := records[0]
	if got.TraceType != TracePub || got.TimeStamp != 1700000000000 ||
		got.RegionID != "DefaultRegion" || got.GroupName != "GID_test" {
		t.Errorf("context fields = %+v", got)
	}
	if got.CostTime != 7 || !got.IsSuccess {
		t.Errorf("costTime/isSuccess = %d/%v", got.CostTime, got.IsSuccess)
	}
	bean := got.TraceBeans[0]
	if bean.Topic != "TopicTest" || bean.MsgID != javaMsgID1 || bean.Tags != "TagA" ||
		bean.Keys != "KeyA KeyB" || bean.StoreHost != "127.0.0.1:10911" {
		t.Errorf("bean = %+v", bean)
	}
	if bean.BodyLength != 42 || bean.OffsetMsgID != javaOffsetMsgID || bean.MsgType != common.NormalMsg {
		t.Errorf("bean = %+v", bean)
	}
	// Java's encoder never writes clientHost; the decoder falls back to the
	// local address.
	if bean.ClientHost != traceLocalAddress {
		t.Errorf("clientHost = %q, want the LOCAL_ADDRESS default %q", bean.ClientHost, traceLocalAddress)
	}
}

func TestTraceDecodeSubBeforeKeepsRequestIDAndRetryTimes(t *testing.T) {
	ctx := &TraceContext{
		TraceType: TraceSubBefore,
		TimeStamp: 1700000000000,
		RegionID:  "DefaultRegion",
		GroupName: "CID_test",
		RequestID: "REQ-SUB-001",
		TraceBeans: []*TraceBean{
			javaVectorBean(javaMsgID1, "KeyA KeyB", 2),
			javaVectorBean(javaMsgID2, "KeyC", 0),
		},
	}
	records := DecodeTraceDataString(EncodeTraceContext(ctx).TransData)
	if len(records) != 2 {
		t.Fatalf("one record per bean, got %d", len(records))
	}
	if records[0].TraceBeans[0].MsgID != javaMsgID1 || records[1].TraceBeans[0].MsgID != javaMsgID2 {
		t.Errorf("msgIds = %q/%q", records[0].TraceBeans[0].MsgID, records[1].TraceBeans[0].MsgID)
	}
	if records[0].TraceBeans[0].RetryTimes != 2 || records[1].TraceBeans[0].RetryTimes != 0 {
		t.Errorf("retryTimes = %d/%d", records[0].TraceBeans[0].RetryTimes, records[1].TraceBeans[0].RetryTimes)
	}
	for _, r := range records {
		if r.RequestID != "REQ-SUB-001" || r.GroupName != "CID_test" {
			t.Errorf("shared requestId/group lost: %+v", r)
		}
	}
}

func TestTraceDecodeSubAfterContextCodeAndLegacyBranch(t *testing.T) {
	ctx := &TraceContext{
		TraceType:     TraceSubAfter,
		TimeStamp:     1700000000000,
		GroupName:     "CID_test",
		RequestID:     "REQ-SUB-001",
		CostTime:      11,
		IsSuccess:     false,
		ContextCode:   4,
		AccessChannel: AccessChannelLocal,
		TraceBeans:    []*TraceBean{javaVectorBean(javaMsgID1, "KeyA KeyB", 2)},
	}
	got := DecodeTraceDataString(EncodeTraceContext(ctx).TransData)[0]
	if got.ContextCode != 4 || got.IsSuccess || got.GroupName != "CID_test" || got.TimeStamp != 1700000000000 {
		t.Errorf("decoded SubAfter = %+v", got)
	}
	// The pre-5.x layout has only 7 segments; timestamp and group must fall back
	// to "now" and "" rather than shifting the other fields.
	legacy := strings.Join([]string{"SubAfter", "REQ", javaMsgID1, "5", "true", "KeyA", "0"}, traceSOH)
	old := DecodeTraceDataString(legacy)[0]
	if old.ContextCode != 0 || old.GroupName != "" || old.TimeStamp < 1700000000000 {
		t.Errorf("legacy SubAfter = %+v", old)
	}
}

func TestTraceDecodeEndTransactionFields(t *testing.T) {
	ctx := &TraceContext{
		TraceType:  TraceEndTransaction,
		TimeStamp:  1700000000000,
		RegionID:   "DefaultRegion",
		GroupName:  "GID_test",
		TraceBeans: []*TraceBean{javaVectorBean(javaMsgID1, "KeyA KeyB", 2)},
	}
	got := DecodeTraceDataString(EncodeTraceContext(ctx).TransData)[0]
	if got.TraceType != TraceEndTransaction {
		t.Fatalf("type = %v", got.TraceType)
	}
	bean := got.TraceBeans[0]
	if bean.TransactionID != "TRAN-001" || bean.TransactionState != "COMMIT_MESSAGE" {
		t.Errorf("transaction fields = %+v", bean)
	}
	if bean.FromTransactionCheck || bean.MsgType != common.NormalMsg {
		t.Errorf("check flag / msgType = %v/%v", bean.FromTransactionCheck, bean.MsgType)
	}
}

// ------------------------------------------------------- decoding robustness

// A SubBefore record of a message WITHOUT keys loses its last segment to Java's
// trailing-empty split rule. Java then throws AIOOBE and drops the whole trace
// message; this decoder reads a missing segment as "".
func TestTraceDecodeSubBeforeWithoutKeys(t *testing.T) {
	raw := strings.Join([]string{"SubBefore", "1700000000000", "DefaultRegion", "GID_trace_live",
		"REQ-001", javaMsgID1, "0", ""}, traceSOH) + traceSTX
	records := DecodeTraceDataString(raw)
	if len(records) != 1 {
		t.Fatalf("decoded %d records, want 1", len(records))
	}
	bean := records[0].TraceBeans[0]
	if bean.Keys != "" || bean.RetryTimes != 0 || bean.MsgID != javaMsgID1 {
		t.Errorf("bean = %+v", bean)
	}
}

func TestTraceDecodeOneBadRecordDoesNotDropTheBatch(t *testing.T) {
	broken := strings.Join([]string{"SubAfter", "REQ-9", javaMsgID2}, traceSOH) + traceSTX
	goodSub := strings.Join([]string{"SubBefore", "1700000000000", "R", "G", "REQ-1",
		javaMsgID1, "0", "K"}, traceSOH) + traceSTX

	records := DecodeTraceDataString(expectedPub + broken + goodSub)
	if len(records) != 2 || records[0].TraceType != TracePub || records[1].TraceType != TraceSubBefore {
		t.Errorf("types = %v", []TraceType{records[0].TraceType, records[1].TraceType})
	}
}

func TestTraceDecodeIgnoresUnknownRecordKind(t *testing.T) {
	raw := strings.Join([]string{"SomethingNew", "1", "2"}, traceSOH) + traceSTX + expectedRecall
	records := DecodeTraceDataString(raw)
	if len(records) != 1 || records[0].TraceType != TraceRecall {
		t.Errorf("records = %+v", records)
	}
}

func TestTraceDecodeEmptyInput(t *testing.T) {
	for _, in := range []string{"", traceSTX + traceSTX} {
		if got := DecodeTraceDataString(in); len(got) != 0 {
			t.Errorf("DecodeTraceDataString(%q) = %+v", in, got)
		}
	}
}

// ------------------------------------------------------------- traceparent

func TestTraceparentGenerateAndValidate(t *testing.T) {
	tp := GenerateTraceparent()
	if !IsValidTraceparent(tp) {
		t.Fatalf("generated traceparent %q must be valid", tp)
	}
	if len(tp) != 55 || tp[2] != '-' || tp[35] != '-' || tp[52] != '-' {
		t.Errorf("traceparent shape = %q", tp)
	}
	valid := []string{"00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
		"00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-00"}
	for _, v := range valid {
		if !IsValidTraceparent(v) {
			t.Errorf("%q must be accepted", v)
		}
	}
	invalid := []string{
		"",
		"00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7",    // three parts
		"ff-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01", // version ff is reserved
		"00-4bf92f3577b34da6a3ce929d0e0e473-00f067aa0ba902b7-01",  // trace-id too short
		"00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b-01",  // parent-id too short
		"00-00000000000000000000000000000000-00f067aa0ba902b7-01", // all-zero trace-id
		"00-4bf92f3577b34da6a3ce929d0e0e4736-0000000000000000-01", // all-zero parent-id
	}
	for _, v := range invalid {
		if IsValidTraceparent(v) {
			t.Errorf("%q must be rejected", v)
		}
	}
}

func TestTraceparentInjectIsIdempotentAndChildKeepsTraceID(t *testing.T) {
	msg := common.NewMessage("TopicTest", []byte("x"))
	first := InjectTraceContext(msg)
	if got := InjectTraceContext(msg); got != first {
		t.Errorf("a caller-propagated traceparent must win: %q -> %q", first, got)
	}
	child, ok := ChildTraceparent(first)
	if !ok || !IsValidTraceparent(child) {
		t.Fatalf("child = %q ok=%v", child, ok)
	}
	if child[3:35] != first[3:35] {
		t.Errorf("child lost the trace-id: %q vs %q", child, first)
	}
	if child == first {
		t.Error("child must get a fresh parent-id")
	}
	if _, ok := ChildTraceparent("garbage"); ok {
		t.Error("a malformed parent must be rejected")
	}

	ext := common.NewMessageExt()
	ext.PutProperty(TraceContextProperty, first)
	if got, ok := ExtractTraceparent(ext); !ok || got != first {
		t.Errorf("extract = %q ok=%v", got, ok)
	}
	if _, ok := ExtractTraceparent(common.NewMessageExt()); ok {
		t.Error("a message without a traceparent must report ok=false")
	}
}

// --------------------------------------------------------------- dispatcher

func newTestDispatcher(group string, kind TraceDispatcherType, batch int, topic string) *AsyncTraceDispatcher {
	return NewAsyncTraceDispatcher(group, kind, batch, topic, nil)
}

func TestTraceDispatcherDefaultsAndGroupName(t *testing.T) {
	d := newTestDispatcher("GID_test", TraceDispatcherProduce, 10, "")
	if d.TraceTopicName() != "RMQ_SYS_TRACE_TOPIC" {
		t.Errorf("trace topic = %q", d.TraceTopicName())
	}
	if d.batchNum != 10 || d.maxMsgSize != 128000 {
		t.Errorf("batchNum/maxMsgSize = %d/%d", d.batchNum, d.maxMsgSize)
	}
	// Java caps batchNum at 20 (the extra records would overflow the broker's
	// per-message limit anyway).
	if got := newTestDispatcher("GID_test", TraceDispatcherProduce, 50, "").batchNum; got != 20 {
		t.Errorf("batchNum cap = %d, want 20", got)
	}
	if got := newTestDispatcher("GID_test", TraceDispatcherProduce, 0, "").batchNum; got != 1 {
		t.Errorf("batchNum floor = %d, want 1", got)
	}
	if got := newTestDispatcher("GID_test", TraceDispatcherProduce, 10, "MyTraceTopic").
		TraceTopicName(); got != "MyTraceTopic" {
		t.Errorf("custom trace topic = %q", got)
	}
	// The internal producer's group tells the broker which side of the client a
	// batch came from: _INNER_TRACE_PRODUCER-<group>-<type>-<n>.
	produce := newTestDispatcher("GID_test", TraceDispatcherProduce, 10, "").genGroupNameForTrace()
	if !strings.Contains(produce, "_INNER_TRACE_PRODUCER-") || !strings.Contains(produce, "-PRODUCE-") {
		t.Errorf("produce group = %q", produce)
	}
	consume := newTestDispatcher("CID_test", TraceDispatcherConsume, 10, "").genGroupNameForTrace()
	if !strings.Contains(consume, "-CONSUME-") {
		t.Errorf("consume group = %q", consume)
	}
}

func TestTraceDispatcherFullQueueDiscardsAndCounts(t *testing.T) {
	d := newTestDispatcher("GID_test", TraceDispatcherProduce, 10, "")
	for i := 0; i < traceQueueCapacity; i++ {
		if !d.Append(&TraceContext{}) {
			t.Fatalf("append %d must fit (capacity %d)", i, traceQueueCapacity)
		}
	}
	if d.Append(&TraceContext{}) {
		t.Fatal("a full queue must discard, not block the business thread")
	}
	if got := d.DiscardCount(); got != 1 {
		t.Errorf("discard count = %d, want 1", got)
	}
}

// ------------------------------------------------------------- send hooks

func TestSendTraceHookPublishesPubRecord(t *testing.T) {
	d := newTestDispatcher("GID_test", TraceDispatcherProduce, 1, "")
	hook := NewSendMessageTraceHook(d)
	if hook.HookName() != "SendMessageTraceHook" {
		t.Errorf("hook name = %q", hook.HookName())
	}
	msg := common.NewMessage("TopicTest", javaBody)
	msg.SetTags("TagA")
	msg.SetKeys("KeyA KeyB")
	ctx := &SendMessageContext{
		ProducerGroup: "GID_test",
		Message:       msg,
		BrokerAddr:    "127.0.0.1:10911",
		SendResult: &SendResult{
			SendStatus:  SendOK,
			MsgID:       javaMsgID1,
			OffsetMsgID: javaOffsetMsgID,
			RegionID:    "DefaultRegion",
			TraceOn:     true,
		},
		MsgType: common.NormalMsg,
	}
	hook.SendMessageBefore(ctx)
	traceCtx, ok := ctx.MQTraceContext.(*TraceContext)
	if !ok || traceCtx.TraceType != TracePub || len(traceCtx.TraceBeans) != 1 {
		t.Fatalf("before hook did not build a Pub context: %+v", ctx.MQTraceContext)
	}
	if traceCtx.GroupName != "GID_test" || traceCtx.TraceBeans[0].Topic != "TopicTest" ||
		traceCtx.TraceBeans[0].BodyLength != len(javaBody) {
		t.Errorf("before context = %+v", traceCtx)
	}
	hook.SendMessageAfter(ctx)
	select {
	case got := <-d.traceQueue:
		if got.TraceType != TracePub || got.RegionID != "DefaultRegion" || !got.IsSuccess {
			t.Errorf("appended context = %+v", got)
		}
		if got.TraceBeans[0].MsgID != javaMsgID1 || got.TraceBeans[0].OffsetMsgID != javaOffsetMsgID {
			t.Errorf("bean = %+v", got.TraceBeans[0])
		}
		// storeTime is the midpoint of the attempt (Java: timestamp + cost/2), so
		// it can never precede the context timestamp.
		if got.TraceBeans[0].StoreTime < got.TimeStamp {
			t.Errorf("storeTime %d < timestamp %d", got.TraceBeans[0].StoreTime, got.TimeStamp)
		}
	default:
		t.Fatal("after hook appended nothing")
	}
}

// A trace message must never be traced again — that is anti-recursion guard 2
// (guard 1 is the internal producer's own enableTrace=false).
func TestSendTraceHookSkipsTheTraceTopicItself(t *testing.T) {
	d := newTestDispatcher("GID_test", TraceDispatcherProduce, 1, "")
	hook := NewSendMessageTraceHook(d)
	ctx := &SendMessageContext{
		ProducerGroup: "GID_test",
		Message:       common.NewMessage(common.TraceTopic, javaBody),
		SendResult:    &SendResult{SendStatus: SendOK, RegionID: "DefaultRegion", TraceOn: true},
	}
	hook.SendMessageBefore(ctx)
	if ctx.MQTraceContext != nil {
		t.Error("a trace-topic message must not get a trace context")
	}
	hook.SendMessageAfter(ctx)
	if len(d.traceQueue) != 0 {
		t.Error("a trace-topic message must not be appended")
	}
}

func TestSendTraceHookSkipsTraceOffAndMissingResult(t *testing.T) {
	d := newTestDispatcher("GID_test", TraceDispatcherProduce, 1, "")
	hook := NewSendMessageTraceHook(d)

	// TRACE_ON=false from the broker: not tracked.
	off := &SendMessageContext{
		Message:    common.NewMessage("TopicTest", javaBody),
		SendResult: &SendResult{SendStatus: SendOK, RegionID: "DefaultRegion", TraceOn: false},
	}
	hook.SendMessageBefore(off)
	hook.SendMessageAfter(off)

	// No result at all (a failed attempt or a oneway send): not tracked.
	failed := &SendMessageContext{Message: common.NewMessage("TopicTest", javaBody)}
	hook.SendMessageBefore(failed)
	hook.SendMessageAfter(failed)

	// After without Before: not tracked.
	bare := &SendMessageContext{
		Message:    common.NewMessage("TopicTest", javaBody),
		SendResult: &SendResult{SendStatus: SendOK, RegionID: "DefaultRegion", TraceOn: true},
	}
	hook.SendMessageAfter(bare)

	if len(d.traceQueue) != 0 {
		t.Errorf("nothing should have been appended, queue=%d", len(d.traceQueue))
	}
}

func TestSendTraceHookMarksFailureStatus(t *testing.T) {
	d := newTestDispatcher("GID_test", TraceDispatcherProduce, 1, "")
	hook := NewSendMessageTraceHook(d)
	ctx := &SendMessageContext{
		Message:    common.NewMessage("TopicTest", javaBody),
		SendResult: &SendResult{SendStatus: FlushDiskTimeout, RegionID: "DefaultRegion", TraceOn: true},
	}
	hook.SendMessageBefore(ctx)
	hook.SendMessageAfter(ctx)
	select {
	case got := <-d.traceQueue:
		if got.IsSuccess {
			t.Error("a non-SEND_OK status must record isSuccess=false")
		}
	default:
		t.Fatal("nothing appended")
	}
}

func TestEndTransactionTraceHookRecordsTheVerdict(t *testing.T) {
	d := newTestDispatcher("GID_test", TraceDispatcherProduce, 1, "")
	hook := NewEndTransactionTraceHook(d)
	if hook.HookName() != "EndTransactionTraceHook" {
		t.Errorf("hook name = %q", hook.HookName())
	}
	msg := common.NewMessage("TopicTest", javaBody)
	msg.SetKeys("KeyA")
	hook.EndTransaction(&EndTransactionContext{
		ProducerGroup:        "GID_test",
		Message:              msg,
		BrokerAddr:           "127.0.0.1:10911",
		MsgID:                javaMsgID1,
		TransactionID:        "TRAN-001",
		TransactionState:     CommitMessage,
		FromTransactionCheck: false,
	})
	select {
	case got := <-d.traceQueue:
		if got.TraceType != TraceEndTransaction || got.RegionID != common.DefaultTraceRegionID {
			t.Errorf("context = %+v", got)
		}
		bean := got.TraceBeans[0]
		if bean.TransactionState != "COMMIT_MESSAGE" || bean.MsgType != common.TransMsgCommit ||
			bean.MsgID != javaMsgID1 {
			t.Errorf("bean = %+v", bean)
		}
	default:
		t.Fatal("nothing appended")
	}
}

// ------------------------------------------------------------ consume hooks

func TestConsumeTraceHookSharesRequestIDAndMapsContextCode(t *testing.T) {
	d := newTestDispatcher("CID_test", TraceDispatcherConsume, 1, "")
	hook := NewConsumeMessageTraceHook(d)
	if hook.HookName() != "ConsumeMessageTraceHook" {
		t.Errorf("hook name = %q", hook.HookName())
	}
	msg := common.NewMessageExt()
	msg.Topic = "TopicTest"
	msg.MsgID = javaMsgID1
	msg.Body = javaBody
	msg.StoreTimestamp = 1700000000000
	msg.StoreSize = 42
	msg.ReconsumeTimes = 1
	msg.PutProperty(common.PropertyMsgRegion, "DefaultRegion")
	ctx := &ConsumeMessageContext{
		ConsumerGroup: "CID_test",
		MsgList:       []*common.MessageExt{msg},
		Props:         map[string]string{common.ConsumeContextType: "SUCCESS"},
		AccessChannel: AccessChannelLocal,
		Success:       true,
	}
	hook.ConsumeMessageBefore(ctx)
	select {
	case got := <-d.traceQueue:
		if got.TraceType != TraceSubBefore || got.GroupName != "CID_test" || got.RegionID != "DefaultRegion" {
			t.Fatalf("SubBefore = %+v", got)
		}
		if got.TraceBeans[0].MsgID != javaMsgID1 || got.TraceBeans[0].RetryTimes != 1 {
			t.Errorf("bean = %+v", got.TraceBeans[0])
		}
	default:
		t.Fatal("before hook appended nothing")
	}
	hook.ConsumeMessageAfter(ctx)
	select {
	case got := <-d.traceQueue:
		before := ctx.MQTraceContext.(*TraceContext)
		if got.TraceType != TraceSubAfter {
			t.Fatalf("SubAfter = %+v", got)
		}
		if got.RequestID != before.RequestID {
			t.Errorf("requestId %q != SubBefore %q — the console pairs on it", got.RequestID, before.RequestID)
		}
		// SUCCESS is ordinal 0 in ConsumeReturnType.
		if got.ContextCode != 0 || !got.IsSuccess {
			t.Errorf("contextCode/isSuccess = %d/%v", got.ContextCode, got.IsSuccess)
		}
	default:
		t.Fatal("after hook appended nothing")
	}
}

func TestConsumeTraceHookContextCodeFromFailedStatus(t *testing.T) {
	d := newTestDispatcher("CID_test", TraceDispatcherConsume, 1, "")
	hook := NewConsumeMessageTraceHook(d)
	msg := common.NewMessageExt()
	msg.Topic = "TopicTest"
	msg.MsgID = javaMsgID1
	msg.PutProperty(common.PropertyMsgRegion, "DefaultRegion")
	ctx := &ConsumeMessageContext{
		ConsumerGroup: "CID_test",
		MsgList:       []*common.MessageExt{msg},
		Props:         map[string]string{common.ConsumeContextType: "FAILED"},
		Success:       false,
	}
	hook.ConsumeMessageBefore(ctx)
	<-d.traceQueue
	hook.ConsumeMessageAfter(ctx)
	if got := <-d.traceQueue; got.ContextCode != 4 || got.IsSuccess {
		t.Errorf("FAILED must map to contextCode 4, got %d (success=%v)", got.ContextCode, got.IsSuccess)
	}
}

func TestConsumeTraceHookSkipsTraceOffAndBeforeLessAfter(t *testing.T) {
	d := newTestDispatcher("CID_test", TraceDispatcherConsume, 1, "")
	hook := NewConsumeMessageTraceHook(d)
	off := common.NewMessageExt()
	off.Topic = "TopicTest"
	off.MsgID = javaMsgID1
	off.PutProperty(common.PropertyTraceSwitch, "false")
	ctx := &ConsumeMessageContext{ConsumerGroup: "CID_test", MsgList: []*common.MessageExt{off}}
	hook.ConsumeMessageBefore(ctx)
	if len(d.traceQueue) != 0 {
		t.Error("TRACE_ON=false must produce no SubBefore")
	}
	hook.ConsumeMessageAfter(ctx)
	if len(d.traceQueue) != 0 {
		t.Error("after without a before must not append")
	}
}

// consumeReturnType decides the SubAfter contextCode; its Java shape is
// null -> EXCEPTION/RETURNNULL, then the RT ceiling, then the outcome.
func TestConsumeReturnTypeBranches(t *testing.T) {
	c := MustNewDefaultMQPushConsumer("CID_test")
	c.SetConsumeTimeout(15)
	const rt = int64(5)
	if got := c.consumeReturnType(true, true, false, rt); got != ConsumeReturnException {
		t.Errorf("panicked = %v, want EXCEPTION", got)
	}
	if got := c.consumeReturnType(true, false, false, rt); got != ConsumeReturnNull {
		t.Errorf("stray status = %v, want RETURNNULL", got)
	}
	if got := c.consumeReturnType(false, false, true, 15*60*1000); got != ConsumeReturnTimeout {
		t.Errorf("RT at the ceiling = %v, want TIME_OUT", got)
	}
	if got := c.consumeReturnType(false, false, true, rt); got != ConsumeReturnFailed {
		t.Errorf("failed = %v, want FAILED", got)
	}
	if got := c.consumeReturnType(false, false, false, rt); got != ConsumeReturnSuccess {
		t.Errorf("success = %v, want SUCCESS", got)
	}
	if got := ConsumeReturnFailed; got.String() != "FAILED" || int(got) != 4 {
		t.Errorf("FAILED must be ordinal 4 (wire format), got %d/%q", int(got), got.String())
	}
}

// --------------------------------------------------------------- end-to-end

// traceSendTap wraps the mock broker's handler and keeps a copy of every plain
// SEND whose topic is the trace topic — everything a dispatcher's internal
// producer put on the wire — while the request is still answered normally.
type traceSendTap struct {
	inner      func(*mockServer, *remoting.RemotingCommand) *remoting.RemotingCommand
	traceTopic string

	mu     sync.Mutex
	bodies []string
	props  []string
}

func (tap *traceSendTap) handle(s *mockServer, req *remoting.RemotingCommand) *remoting.RemotingCommand {
	if req.Code == remoting.ReqSendMessageV2 {
		var header remoting.SendMessageRequestHeaderV2
		header.FromExtFields(req.ExtFields())
		if strings.HasPrefix(deref(header.Topic), tap.traceTopic) {
			tap.mu.Lock()
			tap.bodies = append(tap.bodies, string(req.Body))
			tap.props = append(tap.props, deref(header.Properties))
			tap.mu.Unlock()
		}
	}
	return tap.inner(s, req)
}

func (tap *traceSendTap) count() int {
	tap.mu.Lock()
	defer tap.mu.Unlock()
	return len(tap.bodies)
}

func (tap *traceSendTap) snapshot() (bodies, props []string) {
	tap.mu.Lock()
	defer tap.mu.Unlock()
	return append([]string(nil), tap.bodies...), append([]string(nil), tap.props...)
}

// decodeTraceBodies decodes every trace message a dispatcher sent into the
// records it carried (one message may hold several).
func decodeTraceBodies(bodies []string) []*TraceContext {
	var out []*TraceContext
	for _, body := range bodies {
		out = append(out, DecodeTraceDataString(body)...)
	}
	return out
}

// A producer with tracing on sends its Pub record to the trace topic through
// the dispatcher's internal producer — and STOPS there. Both anti-recursion
// guards are checked: the trace message must not produce a trace of its own
// (guard 1: the internal producer's enableTrace is false; guard 2: the hook
// skips trace topics), so the trace-topic send count must stay at one.
func TestProducerTraceE2EWritesPubToTraceTopic(t *testing.T) {
	topic := uniqueTopic("GoProducerTraceE2E", t)
	const group = "GID_go_trace_producer_e2e"

	handler := &sendBrokerOnReq{offset: 7}
	tap := &traceSendTap{inner: handler.handle, traceTopic: common.TraceTopic}
	broker := startMockServer(t, tap.handle)
	ns := startMockServer(t, nameserverOnReq(map[string]string{
		topic:             routeBodyFor(broker.addr, ""),
		common.TraceTopic: routeBodyFor(broker.addr, ""),
	}))

	p := MustNewDefaultMQProducer(group)
	p.SetNameServerAddresses([]string{ns.addr})
	p.SetInstanceName(uniqueClientID(t))
	p.SetSendMsgTimeout(3000)
	p.SetEnableTrace(true)
	// One record per flush: the worker picks it up on its next 5ms tick instead
	// of holding it until the 5s interval or a full batch.
	p.SetTraceMsgBatchNum(1)
	requireNoError(t, "producer start", p.Start())
	t.Cleanup(p.Shutdown)

	msg := common.NewMessage(topic, []byte("traced"))
	msg.SetKeys("biz-key-1")
	result, err := p.Send(msg)
	requireNoError(t, "send", err)

	waitFor(t, "the Pub record on "+common.TraceTopic, func() bool { return tap.count() > 0 })
	// Quiet window: a recursive trace would be flushed within a tick or two of
	// the first one (batchNum is 1), so a late second send is the signal.
	time.Sleep(250 * time.Millisecond)
	if got := tap.count(); got != 1 {
		t.Fatalf("%d trace sends for one business send — tracing is recursing", got)
	}

	bodies, props := tap.snapshot()
	records := decodeTraceBodies(bodies)
	if len(records) != 1 {
		t.Fatalf("decoded %d records from %q", len(records), bodies[0])
	}
	rec := records[0]
	if rec.TraceType != TracePub {
		t.Errorf("traceType = %v, want Pub", rec.TraceType)
	}
	if rec.GroupName != group {
		t.Errorf("group = %q, want %q", rec.GroupName, group)
	}
	if rec.RegionID != common.DefaultTraceRegionID {
		t.Errorf("region = %q (the send response carried none, so the default applies)", rec.RegionID)
	}
	if !rec.IsSuccess {
		t.Error("a SEND_OK send must be recorded as success")
	}
	if len(rec.TraceBeans) != 1 {
		t.Fatalf("beans = %d, want 1", len(rec.TraceBeans))
	}
	bean := rec.TraceBeans[0]
	if bean.Topic != topic {
		t.Errorf("bean topic = %q, want the business topic %q", bean.Topic, topic)
	}
	if bean.MsgID != result.MsgID {
		t.Errorf("bean msgId = %q, want the UNIQ_KEY %q", bean.MsgID, result.MsgID)
	}
	if bean.OffsetMsgID != result.OffsetMsgID {
		t.Errorf("bean offsetMsgId = %q, want the broker id %q", bean.OffsetMsgID, result.OffsetMsgID)
	}
	if bean.Keys != "biz-key-1" || bean.BodyLength != len("traced") {
		t.Errorf("bean keys/bodyLength = %q/%d", bean.Keys, bean.BodyLength)
	}
	// The console finds a message's trace by the trace message's KEYS property,
	// which the dispatcher fills with the records' TransKeys (here: the msgId).
	keys, _ := common.String2MessageProperties(props[0]).Get(common.PropertyKeys)
	if !strings.Contains(keys, result.MsgID) {
		t.Errorf("trace message KEYS = %q, must contain the traced msgId %q", keys, result.MsgID)
	}
}

// The consume side against the mock cluster: a push consumer with tracing on
// emits the SubBefore/SubAfter pair of one consume to the trace topic, and the
// pair shares the requestId the console joins it by.
func TestConsumerTraceE2EWritesSubBeforeAndSubAfter(t *testing.T) {
	topic := uniqueTopic("GoConsumerTraceE2E", t)
	const group = "GID_go_trace_consumer_e2e"

	// The trace topic needs a route on the nameserver, or the dispatcher's
	// internal producer has nowhere to send the records.
	f := newClusterFixture(t, map[string]int{topic: 1, common.TraceTopic: 1})
	for _, m := range f.broker.add(topic, 0, "traced-consume") {
		// The real broker stamps MSG_REGION on every stored message; without it
		// the dispatcher drops the record (Java: no region, nothing to trace).
		m.PutProperty(common.PropertyMsgRegion, common.DefaultTraceRegionID)
	}

	listener := newRecordingListener()
	c := f.newConsumer(t, group, listener, withConsumeFromWhere(ConsumeFromWhereFirstOffset))
	c.SetEnableTrace(true)
	c.SetTraceMsgBatchNum(1)
	startConsumer(t, c, topic, "*")

	waitFor(t, "the consumed message", func() bool { return listener.receivedAll(1) })
	var before, after *TraceContext
	waitFor(t, "the SubBefore/SubAfter pair on "+common.TraceTopic, func() bool {
		bodies := make([]string, 0, 2)
		for _, s := range f.broker.traceSends(common.TraceTopic) {
			bodies = append(bodies, s.body)
		}
		before, after = nil, nil
		for _, rec := range decodeTraceBodies(bodies) {
			switch rec.TraceType {
			case TraceSubBefore:
				before = rec
			case TraceSubAfter:
				after = rec
			}
		}
		return before != nil && after != nil
	})

	if before.GroupName != group || after.GroupName != group {
		t.Errorf("groups = %q/%q, want %q", before.GroupName, after.GroupName, group)
	}
	if before.RegionID != common.DefaultTraceRegionID {
		t.Errorf("SubBefore region = %q (comes from the message's MSG_REGION)", before.RegionID)
	}
	if len(before.TraceBeans) != 1 || len(after.TraceBeans) != 1 {
		t.Fatalf("beans = %d/%d, want 1/1", len(before.TraceBeans), len(after.TraceBeans))
	}
	// Neither record carries the topic on the wire (Java's encoder does not
	// write it for the Sub pair); the msgId is what joins a trace to its
	// message, so that is what must have survived the round trip.
	if before.TraceBeans[0].MsgID == "" || before.TraceBeans[0].MsgID != after.TraceBeans[0].MsgID {
		t.Errorf("bean msgIds = %q/%q, must be the same non-empty message id",
			before.TraceBeans[0].MsgID, after.TraceBeans[0].MsgID)
	}
	// Consume-side msgId is the broker's store-address id (Java MessageDecoder
	// builds it from storeHost + commitLogOffset), here the mock broker's
	// 127.0.0.1:10911 and the injected commitLogOffset 1000.
	if _, _, offset, err := common.DecodeMessageID(before.TraceBeans[0].MsgID); err != nil || offset != 1000 {
		t.Errorf("bean msgId %q does not decode to commitLogOffset 1000 (err=%v)",
			before.TraceBeans[0].MsgID, err)
	}
	// The pair is joined on the requestId; a fresh one per record would leave
	// the console with two unrelated half-traces.
	if before.RequestID == "" || after.RequestID != before.RequestID {
		t.Errorf("requestIds = %q/%q, must be equal", before.RequestID, after.RequestID)
	}
	if !after.IsSuccess || after.ContextCode != int32(ConsumeReturnSuccess) {
		t.Errorf("after isSuccess/contextCode = %v/%d, want true/SUCCESS(0)",
			after.IsSuccess, after.ContextCode)
	}
	if after.CostTime < 0 {
		t.Errorf("costTime = %d", after.CostTime)
	}
	// The trace topic is the only thing the consumer ever sends.
	for _, s := range f.broker.sendSnapshot() {
		if s.topic != common.TraceTopic {
			t.Errorf("consumer sent to %q — a trace consumer must not publish business traffic", s.topic)
		}
	}
}
