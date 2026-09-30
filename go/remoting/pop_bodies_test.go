package remoting

import (
	"testing"

	"github.com/zhaohai666/rocketmq-client-remoting/go/common"
)

// POP wire guards. The extFields key spellings here are the contract with the
// broker's fastjson2-free reflection decoder: a renamed key is silently dropped,
// which for POP means "every ack is a no-op" rather than a loud error.

func TestPopMessageRequestHeaderExtFieldKeys(t *testing.T) {
	ext := common.NewStringMap()
	(&PopMessageRequestHeader{
		Bname:         StrPtr("broker-a"),
		ConsumerGroup: StrPtr("G"),
		Topic:         StrPtr("T"),
		QueueID:       I32Ptr(2),
		MaxMsgNums:    I32Ptr(32),
		InvisibleTime: I64Ptr(60000),
		PollTime:      I64Ptr(15000),
		BornTime:      I64Ptr(1700000000000),
		InitMode:      I32Ptr(0),
		ExpType:       StrPtr("TAG"),
		Exp:           StrPtr("TagA"),
		Order:         BoolPtr(false),
		AttemptID:     StrPtr("attempt-1"),
	}).ToExtFields(ext)

	for _, key := range []string{
		"bname", "consumerGroup", "topic", "queueId", "maxMsgNums",
		"invisibleTime", "pollTime", "bornTime", "initMode", "expType",
		"exp", "order", "attemptId",
	} {
		if !ext.ContainsKey(key) {
			t.Fatalf("missing extFields key %q (all keys: %v)", key, extKeys(ext))
		}
	}
	// The inherited broker-name field must be `bname`, never `brokerName`.
	if ext.ContainsKey("brokerName") {
		t.Fatal("POP header must use bname, not brokerName")
	}
	if v, _ := ext.Get("order"); v != "false" {
		t.Fatalf("order = %q; a Java Boolean.FALSE is emitted as \"false\"", v)
	}

	// Round-trip.
	back := &PopMessageRequestHeader{}
	back.FromExtFields(ext)
	if *back.Bname != "broker-a" || *back.QueueID != 2 || *back.InvisibleTime != 60000 ||
		*back.Order || back.AttemptID == nil || *back.AttemptID != "attempt-1" {
		t.Fatalf("round-trip mismatch: %+v", back)
	}
}

// A nil `order` means false — not true. The Java getter is isOrder() and the
// field defaults to FALSE.
func TestPopHeaderOrderNeverDefaultsTrue(t *testing.T) {
	if (&PopMessageRequestHeader{}).IsOrder() {
		t.Fatal("nil order must not read as true")
	}
	if !(&PopMessageRequestHeader{Order: BoolPtr(true)}).IsOrder() {
		t.Fatal("explicit true must read as true")
	}
}

func TestAckMessageRequestHeaderExtFieldKeys(t *testing.T) {
	ext := common.NewStringMap()
	(&AckMessageRequestHeader{
		Bname:         StrPtr("broker-a"),
		ConsumerGroup: StrPtr("G"),
		Topic:         StrPtr("%RETRY%G_T"),
		QueueID:       I32Ptr(1),
		ExtraInfo:     StrPtr("0 1 60000 0 1 broker-a 1 5"),
		Offset:        I64Ptr(5),
	}).ToExtFields(ext)
	for _, key := range []string{"bname", "consumerGroup", "topic", "queueId", "extraInfo", "offset"} {
		if !ext.ContainsKey(key) {
			t.Fatalf("missing extFields key %q", key)
		}
	}
	// liteTopic is null in the classic path and therefore absent.
	if ext.ContainsKey("liteTopic") {
		t.Fatal("nil liteTopic must not be written")
	}
}

func TestChangeInvisibleTimeHeadersExtFieldKeys(t *testing.T) {
	ext := common.NewStringMap()
	(&ChangeInvisibleTimeRequestHeader{
		Bname:         StrPtr("broker-a"),
		ConsumerGroup: StrPtr("G"),
		Topic:         StrPtr("T"),
		QueueID:       I32Ptr(0),
		ExtraInfo:     StrPtr("0 1 60000 0 0 broker-a 0 3"),
		Offset:        I64Ptr(3),
		InvisibleTime: I64Ptr(120000),
		Suspend:       BoolPtr(false),
	}).ToExtFields(ext)
	for _, key := range []string{
		"bname", "consumerGroup", "topic", "queueId", "extraInfo", "offset",
		"invisibleTime", "suspend",
	} {
		if !ext.ContainsKey(key) {
			t.Fatalf("missing extFields key %q", key)
		}
	}
	// Java's `suspend` is a primitive boolean defaulting to false, so it always
	// reaches the wire.
	if v, _ := ext.Get("suspend"); v != "false" {
		t.Fatalf("suspend = %q", v)
	}

	resp := common.NewStringMap()
	(&ChangeInvisibleTimeResponseHeader{PopTime: I64Ptr(9), InvisibleTime: I64Ptr(120000), ReviveQid: I32Ptr(4)}).ToExtFields(resp)
	for _, key := range []string{"popTime", "invisibleTime", "reviveQid"} {
		if !resp.ContainsKey(key) {
			t.Fatalf("missing response key %q", key)
		}
	}
}

func TestPopMessageResponseHeaderExtFieldKeys(t *testing.T) {
	ext := common.NewStringMap()
	(&PopMessageResponseHeader{
		PopTime:         I64Ptr(1700000000000),
		InvisibleTime:   I64Ptr(60000),
		ReviveQid:       I32Ptr(3),
		RestNum:         I64Ptr(7),
		StartOffsetInfo: StrPtr("0 0 10"),
		MsgOffsetInfo:   StrPtr("0 0 10,11,12"),
		OrderCountInfo:  StrPtr("0 0 3"),
	}).ToExtFields(ext)
	for _, key := range []string{
		"popTime", "invisibleTime", "reviveQid", "restNum",
		"startOffsetInfo", "msgOffsetInfo", "orderCountInfo",
	} {
		if !ext.ContainsKey(key) {
			t.Fatalf("missing response key %q", key)
		}
	}
}

// ---------------------------------------------------------------- request mode

func TestSetMessageRequestModeBody(t *testing.T) {
	body := &SetMessageRequestModeRequestBody{
		Topic:            "T",
		ConsumerGroup:    "G",
		Mode:             MessageRequestModePop,
		PopShareQueueNum: 2,
	}
	raw := string(body.Encode())
	for _, want := range []string{`"topic":"T"`, `"consumerGroup":"G"`, `"mode":"POP"`, `"popShareQueueNum":2`} {
		if !contains(raw, want) {
			t.Fatalf("body %s missing %s", raw, want)
		}
	}

	// Java's field initialiser is PULL, so an unset mode must not silently
	// become POP on the wire.
	empty := &SetMessageRequestModeRequestBody{Topic: "T", ConsumerGroup: "G"}
	if !contains(string(empty.Encode()), `"mode":"PULL"`) {
		t.Fatalf("default mode body: %s", string(empty.Encode()))
	}

	decoded, err := DecodeSetMessageRequestModeRequestBody(body.Encode())
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if decoded.Mode != MessageRequestModePop || decoded.PopShareQueueNum != 2 {
		t.Fatalf("decoded %+v", decoded)
	}
	// An empty body is a valid default (Java's default PULL), not an error.
	if b, err := DecodeSetMessageRequestModeRequestBody(nil); err != nil || b.Mode != MessageRequestModePull {
		t.Fatalf("empty body: %+v %v", b, err)
	}
}

// ---------------------------------------------------------------- bitset

func TestBitSetLittleEndianAndTrailingTrim(t *testing.T) {
	var bs BitSet
	bs.Set(0)
	bs.Set(2)
	// bits 0 and 2 -> 0b101 = 5
	if got := bs.Bytes(); len(got) != 1 || got[0] != 5 {
		t.Fatalf("bytes %v", got)
	}
	if !bs.Get(0) || bs.Get(1) || !bs.Get(2) {
		t.Fatal("bit membership wrong")
	}
	// Out-of-range reads are false, like java.util.BitSet#get.
	if bs.Get(64) {
		t.Fatal("out-of-range bit must be false")
	}

	// Bit 8 lands in the second byte; Java trims trailing zero bytes so bit 8
	// alone encodes as [0, 1] -> "AAE=".
	var high BitSet
	high.Set(8)
	if got := high.Bytes(); len(got) != 2 || got[0] != 0 || got[1] != 1 {
		t.Fatalf("bit 8 bytes %v", got)
	}
	if high.EncodeBase64() != "AAE=" {
		t.Fatalf("bit 8 base64 = %q", high.EncodeBase64())
	}
	// A zero-length byte array encodes to "" (Base64 of nothing).
	if (&BitSet{}).EncodeBase64() != "" {
		t.Fatalf("empty set base64 = %q", (&BitSet{}).EncodeBase64())
	}
}

func TestBitSetRoundTripBase64(t *testing.T) {
	var bs BitSet
	for _, i := range []int{0, 1, 7, 8, 33} {
		bs.Set(i)
	}
	var back BitSet
	if err := back.DecodeBase64(bs.EncodeBase64()); err != nil {
		t.Fatalf("decode: %v", err)
	}
	for _, i := range []int{0, 1, 7, 8, 33} {
		if !back.Get(i) {
			t.Fatalf("bit %d lost in round-trip", i)
		}
	}
	if back.Get(2) || back.Get(9) {
		t.Fatal("spurious bit set")
	}
	if back.PopCount() != 5 {
		t.Fatalf("PopCount = %d", back.PopCount())
	}
}

// ---------------------------------------------------------------- batch ack

// The bit index is (msgQueueOffset - ckQueueOffset). Getting this wrong acks
// the wrong message and leaves the intended one to be redelivered.
func TestBuildBatchAckGroupsAndIndexesByBit(t *testing.T) {
	// Two messages on the same queue/batch (ckQueueOffset 10, offsets 10 and 12)
	// plus one on another queue, and one on a retry topic.
	c1 := common.BuildExtraInfoWithMsgQueueOffset(10, 1700000000000, 60000, 3, "T", "broker-a", 0, 10)
	c2 := common.BuildExtraInfoWithMsgQueueOffset(10, 1700000000000, 60000, 3, "T", "broker-a", 0, 12)
	c3 := common.BuildExtraInfoWithMsgQueueOffset(20, 1700000000000, 60000, 3, "T", "broker-a", 0, 20)
	c4 := common.BuildExtraInfoWithMsgQueueOffset(0, 1700000000000, 60000, 3, "%RETRY%G_T", "broker-a", 0, 1)

	body, err := BuildBatchAckMessageRequestBody("T", "G", []string{c1, c2, c3, c4})
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	if body.BrokerName != "broker-a" {
		t.Fatalf("brokerName = %q", body.BrokerName)
	}
	if len(body.Acks) != 3 {
		t.Fatalf("want 3 merged acks, got %d", len(body.Acks))
	}
	first := body.Acks[0]
	if first.StartOffset != 10 || !first.BitSet.Get(0) || first.BitSet.Get(1) || !first.BitSet.Get(2) {
		t.Fatalf("first ack bitset covering offsets 10 and 12: %+v", first)
	}
	if first.BitSet.PopCount() != 2 {
		t.Fatalf("first ack PopCount = %d", first.BitSet.PopCount())
	}
	if first.QueueID != 0 || first.ReviveQueueID != 3 || first.InvisibleTime != 60000 {
		t.Fatalf("first ack fields: %+v", first)
	}
	// The retry-topic checkpoint must keep retry="1" so the broker acks the
	// %RETRY% queue — not the business queue with the same offset.
	last := body.Acks[2]
	if last.Retry != "1" {
		t.Fatalf("retry marker = %q", last.Retry)
	}
}

func TestBatchAckBodyUsesShortJSONNames(t *testing.T) {
	cases := []string{
		common.BuildExtraInfoWithMsgQueueOffset(10, 1700000000000, 60000, 3, "T", "broker-a", 0, 10),
	}
	body, err := BuildBatchAckMessageRequestBody("T", "G", cases)
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	raw := string(body.Encode())
	// fastjson2 writes @JSONField(name=...) — the SHORT names — not the Java
	// property names.
	for _, want := range []string{`"brokerName":"broker-a"`, `"c":"G"`, `"t":"T"`, `"r":"0"`, `"so":10`, `"q":0`, `"rq":3`, `"it":60000`} {
		if !contains(raw, want) {
			t.Fatalf("body %s missing %s", raw, want)
		}
	}
	if contains(raw, `"consumerGroup"`) || contains(raw, `"startOffset"`) {
		t.Fatalf("body used long names: %s", raw)
	}

	// The decoder must still accept the long alternate names.
	long := `{"brokerName":"broker-a","acks":[{"consumerGroup":"G","topic":"T","retry":"1","startOffset":7,"queueId":1,"reviveQueueId":2,"popTime":5,"invisibleTime":60000,"bitSet":"BQ=="}]}`
	decoded, err := DecodeBatchAckMessageRequestBody([]byte(long))
	if err != nil {
		t.Fatalf("decode long names: %v", err)
	}
	if len(decoded.Acks) != 1 || decoded.Acks[0].Retry != "1" || decoded.Acks[0].StartOffset != 7 {
		t.Fatalf("decoded %+v", decoded.Acks)
	}
	if !decoded.Acks[0].BitSet.Get(0) || !decoded.Acks[0].BitSet.Get(2) {
		t.Fatal("alternate-name bitset lost")
	}
}

func TestBatchAckRejectsShortCheckpoint(t *testing.T) {
	// A 7-segment checkpoint cannot address a message: the ack offset segment is
	// missing. Fail loudly instead of acking offset 0.
	short := common.BuildExtraInfo(10, 1700000000000, 60000, 3, "T", "broker-a", 0)
	if _, err := BuildBatchAckMessageRequestBody("T", "G", []string{short}); err == nil {
		t.Fatal("short checkpoint must be rejected")
	}
}

// ---------------------------------------------------------------- assignment

func TestMessageQueueAssignmentMode(t *testing.T) {
	pop := `{"messageQueue":{"brokerName":"broker-a","queueId":3,"topic":"T"},"mode":"POP"}`
	var a MessageQueueAssignment
	if err := a.FromJSONValue(mustJSON(t, pop)); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if a.Mode != MessageRequestModePop || a.MessageQueue.QueueID != 3 || a.MessageQueue.Topic != "T" {
		t.Fatalf("decoded %+v", a)
	}

	// Absent mode defaults to PULL (the Java field initialiser) — this decides
	// pop-vs-pull for the queue, so the default must not be invented.
	var b MessageQueueAssignment
	if err := b.FromJSONValue(mustJSON(t, `{"messageQueue":{"brokerName":"b","queueId":0,"topic":"T"}}`)); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if b.Mode != MessageRequestModePull {
		t.Fatalf("absent mode = %q", b.Mode)
	}
}

// ---------------------------------------------------------------- helpers

func extKeys(ext *common.StringMap) []string {
	var keys []string
	ext.Range(func(k, _ string) { keys = append(keys, k) })
	return keys
}

func contains(haystack, needle string) bool {
	return len(needle) == 0 || indexOf(haystack, needle) >= 0
}

func indexOf(haystack, needle string) int {
	for i := 0; i+len(needle) <= len(haystack); i++ {
		if haystack[i:i+len(needle)] == needle {
			return i
		}
	}
	return -1
}

func mustJSON(t *testing.T, text string) any {
	t.Helper()
	v, err := DecodeJSON([]byte(text))
	if err != nil {
		t.Fatalf("DecodeJSON(%s): %v", text, err)
	}
	return v
}
