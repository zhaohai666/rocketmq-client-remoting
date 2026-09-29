package common

import (
	"strings"
	"testing"
)

func TestMessageQueueHashCodeVectors(t *testing.T) {
	// java.lang.String#hashCode + MessageQueue#hashCode，32 位回绕
	cases := []struct {
		topic  string
		broker string
		queue  int32
		want   int32
	}{
		{"TopicTest", "BrokerA", 0, -1229861880},
		{"TopicTest", "BrokerA", 3, -1229861787},
		{"", "", 0, 29791},
		{"%RETRY%GroupA", "BrokerB", 1, 566955243},
		{"A", "B", 0, 93282},
	}
	for _, c := range cases {
		q := NewMessageQueue(c.topic, c.broker, c.queue)
		if got := q.HashCode(); got != c.want {
			t.Fatalf("HashCode(%s,%s,%d) = %d, want %d", c.topic, c.broker, c.queue, got, c.want)
		}
	}
}

func TestMessageQueueJavaStringIsTheHashTarget(t *testing.T) {
	q := NewMessageQueue("TopicTest", "BrokerA", 3)
	want := "MessageQueue [topic=TopicTest, brokerName=BrokerA, queueId=3]"
	if q.JavaString() != want {
		t.Fatalf("JavaString = %q", q.JavaString())
	}
	// String() 是日志形态，故意与 JavaString 不同：一致哈希路由吃的是 JavaString
	if q.String() == q.JavaString() {
		t.Fatal("String and JavaString must stay different")
	}
}

func TestMessageQueueCompareTo(t *testing.T) {
	a := NewMessageQueue("TopicA", "BrokerB", 1)
	if a.CompareTo(NewMessageQueue("TopicA", "BrokerB", 1)) != 0 {
		t.Fatal("equal queues")
	}
	if a.CompareTo(NewMessageQueue("TopicB", "BrokerB", 1)) >= 0 {
		t.Fatal("topic dominates")
	}
	if a.CompareTo(NewMessageQueue("TopicA", "BrokerC", 1)) >= 0 {
		t.Fatal("broker dominates queue id")
	}
	if a.CompareTo(NewMessageQueue("TopicA", "BrokerB", 2)) >= 0 {
		t.Fatal("queue id last")
	}
	// 可比较值类型：直接当 map key 用（rebalance/offset 表的前提）
	seen := map[MessageQueue]int{a: 1}
	if seen[NewMessageQueue("TopicA", "BrokerB", 1)] != 1 {
		t.Fatal("identical queue must hit the same map slot")
	}
}

func TestWaitStoreMsgOKRule(t *testing.T) {
	// absent == true；只有大小写不敏感的 "true" 是 true（"1"/""/yes 都是 false）
	cases := []struct {
		value   string
		present bool
		want    bool
	}{
		{"", false, true},
		{"true", true, true},
		{"True", true, true},
		{"false", true, false},
		{"1", true, false},
		{"", true, false},
	}
	for _, c := range cases {
		if got := WaitStoreMsgOKOf(c.value, c.present); got != c.want {
			t.Fatalf("WaitStoreMsgOKOf(%q,%v) = %v", c.value, c.present, got)
		}
	}
}

func TestMessageWaitAndDelayDefaults(t *testing.T) {
	m := NewMessage("T", []byte("b"))
	// Message 构造从来不预写 WAIT —— 缺席是常态，必须当 true
	if !m.IsWaitStoreMsgOK() {
		t.Fatal("absent WAIT must default true")
	}
	if _, ok := m.GetWaitStoreMsgOK(); ok {
		t.Fatal("WAIT must be absent by default")
	}
	m.SetWaitStoreMsgOK(false)
	if m.IsWaitStoreMsgOK() {
		t.Fatal("WAIT=false must be respected")
	}
	if m.DelayTimeLevel() != 0 {
		t.Fatal("no delay level by default")
	}
	m.SetDelayTimeLevel(3)
	if v, ok := m.GetDelayTimeLevel(); !ok || v != "3" {
		t.Fatalf("delay level property %q", v)
	}
	if m.DelayTimeLevel() != 3 {
		t.Fatal("delay level parse")
	}
	m.PutProperty(PropertyDelayTimeLevel, "abc")
	if m.DelayTimeLevel() != 0 {
		t.Fatal("illegal delay level must read 0")
	}
}

func TestMessageBodyNilVsZeroLength(t *testing.T) {
	var zero Message
	if zero.Body != nil {
		t.Fatal("zero Message keeps nil body so validators can tell null from empty")
	}
	if len(zero.GetBody()) != 0 {
		t.Fatal("GetBody never returns nil to callers")
	}
	// NewMessage 把 nil 归一成空（Python 同语义）
	if m := NewMessage("T", nil); m.Body == nil {
		t.Fatal("NewMessage normalizes nil body")
	}
	m2 := NewMessage("T", []byte("x"))
	if string(m2.GetBody()) != "x" {
		t.Fatal("body preserved")
	}
	m2.SetBody(nil)
	if m2.Body == nil {
		t.Fatal("SetBody normalizes nil")
	}
}

func TestMessagePropertiesAccessors(t *testing.T) {
	m := &Message{} // Properties 为 nil，所有访问都必须安全
	if _, ok := m.GetProperty("TAGS"); ok {
		t.Fatal("nil props")
	}
	m.SetTags("TagA")
	m.SetKeys("k1 k2")
	if v, _ := m.GetTags(); v != "TagA" {
		t.Fatal("tags")
	}
	if v, _ := m.GetKeys(); v != "k1 k2" {
		t.Fatal("keys")
	}
	m.RemoveProperty(PropertyTags)
	if _, ok := m.GetTags(); ok {
		t.Fatal("tags removed")
	}
	m.ClearProperty()
	if m.Properties.Len() != 0 {
		t.Fatal("cleared")
	}
	// NewMessageWithTags：空 tags/keys 不写属性
	if bare := NewMessageWithTags("T", []byte("b"), "", "", 0); bare.Properties != nil {
		t.Fatal("empty tags must not create properties")
	}
	if tagged := NewMessageWithTags("T", []byte("b"), "TagA", "", 5); tagged.Flag != 5 {
		t.Fatal("flag set")
	}
}

func TestMessageCloneIsDeep(t *testing.T) {
	m := NewMessage("T", []byte("body"))
	m.SetTags("TagA")
	clone := m.Clone()
	clone.Body[0] = 'B'
	clone.SetTags("TagB")
	if string(m.GetBody()) != "body" {
		t.Fatal("body was shallow copied")
	}
	if v, _ := m.GetTags(); v != "TagA" {
		t.Fatalf("properties were shallow copied: %q", v)
	}
	if clone.TransactionID != m.TransactionID {
		t.Fatal("transaction id must be copied")
	}
}

func TestMessageExtHostStringsAndDerivations(t *testing.T) {
	e := NewMessageExt()
	if e.BornHostString() != "" || e.StoreHostString() != "" {
		t.Fatal("empty host renders empty")
	}
	e.BornHost = "10.0.0.1"
	if e.BornHostString() != "10.0.0.1" {
		t.Fatal("port 0 renders the bare host")
	}
	e.BornHostPort = 54321
	if e.BornHostString() != "10.0.0.1:54321" {
		t.Fatalf("got %q", e.BornHostString())
	}
	// ExtFromMessage -> ToMessage 往返：属性是克隆，互不影响
	m := NewMessage("T", []byte("b"))
	m.SetTags("TagA")
	e = ExtFromMessage(m)
	e.SetTags("TagB")
	if v, _ := m.GetTags(); v != "TagA" {
		t.Fatal("ExtFromMessage must clone properties")
	}
	back := e.ToMessage()
	if v, _ := back.GetTags(); v != "TagB" {
		t.Fatal("ToMessage keeps the ext properties")
	}
	if back.Topic != "T" || string(back.GetBody()) != "b" {
		t.Fatal("ToMessage copies topic/body")
	}
}

func TestBatchGenerateFromListDefaults(t *testing.T) {
	m1 := NewMessage("batch-topic", []byte("m1"))
	m2 := NewMessage("batch-topic", []byte("m2"))
	m2.SetTags("TagA")
	batch, err := GenerateFromList([]*Message{m1, m2})
	if err != nil {
		t.Fatal(err)
	}
	if batch.Message.Topic != "batch-topic" {
		t.Fatalf("topic %q", batch.Message.Topic)
	}
	// Java generateFromList：batch.setWaitStoreMsgOK(first.isWaitStoreMsgOK())，
	// 缺席按 true —— 普通消息绝不能被降级成 WAIT=false
	if v, ok := batch.Message.GetWaitStoreMsgOK(); !ok || v != "true" {
		t.Fatalf("batch WAIT = %q (present=%v)", v, ok)
	}
	if !batch.Message.IsWaitStoreMsgOK() {
		t.Fatal("batch WAIT must parse true")
	}
	if len(batch.Messages) != 2 {
		t.Fatalf("messages %d", len(batch.Messages))
	}
}

func TestBatchGenerateFromListRejects(t *testing.T) {
	assertReject := func(t *testing.T, msgs []*Message, want string) {
		t.Helper()
		_, err := GenerateFromList(msgs)
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Fatalf("got %v, want %q", err, want)
		}
	}
	assertReject(t, nil, "messages must not be null or empty")
	assertReject(t, []*Message{}, "messages must not be null or empty")

	mixed := []*Message{NewMessage("T1", []byte("a")), NewMessage("T2", []byte("b"))}
	assertReject(t, mixed, "The topic of the messages in one batch should be the same")

	delayed := NewMessage("T", []byte("a"))
	delayed.SetDelayTimeLevel(3)
	assertReject(t, []*Message{delayed}, "Delayed messages are not supported for batching")

	retry := NewMessage("%RETRY%G", []byte("a"))
	assertReject(t, []*Message{retry}, "Retry Group is not supported for batching")

	wait := []*Message{NewMessage("T", []byte("a")), NewMessage("T", []byte("b"))}
	wait[0].SetWaitStoreMsgOK(true)
	wait[1].SetWaitStoreMsgOK(false)
	assertReject(t, wait, "The waitStoreMsgOK of the messages in one batch should be the same")

	// delay level 0 不是延迟消息，放行
	zero := NewMessage("T", []byte("a"))
	zero.PutProperty(PropertyDelayTimeLevel, "0")
	if _, err := GenerateFromList([]*Message{zero}); err != nil {
		t.Fatalf("delay level 0 must be allowed: %v", err)
	}
}

// python3 生成的 6 段轻量帧（Java MessageDecoder#encodeMessage 同字节）：
// 单条：flag=7，body="body"，TAGS=TagA，KEYS=k1
const batchOneHex = "0000002C00000000000000000000000700000004626F64790012544147530154616741024B455953016B3102"

// 两条：m1 无属性；m2 带 TAGS=TagA
const batchTwoHex = "00000018000000000000000000000000000000026D310000" +
	"00000022000000000000000000000000000000026D32000A54414753015461674102"

func TestEncodeMessageMatchesFixture(t *testing.T) {
	m := NewMessage("", []byte("body"))
	m.Flag = 7
	m.SetTags("TagA")
	m.SetKeys("k1")
	got := EncodeMessage(m)
	if bytes2HexString(got) != batchOneHex {
		t.Fatalf("got  %s\nwant %s", bytes2HexString(got), batchOneHex)
	}
	if len(got) != 44 {
		t.Fatalf("len %d", len(got))
	}
}

func TestEncodeMessagesMatchesFixture(t *testing.T) {
	m1 := NewMessage("", []byte("m1"))
	m2 := NewMessage("", []byte("m2"))
	m2.SetTags("TagA")
	got := EncodeMessages([]*Message{m1, m2})
	if bytes2HexString(got) != batchTwoHex {
		t.Fatalf("got  %s\nwant %s", bytes2HexString(got), batchTwoHex)
	}
	if len(got) != 58 {
		t.Fatalf("len %d", len(got))
	}
}

func TestBatchEncodeDecodeRoundTrip(t *testing.T) {
	m1 := NewMessage("", []byte("m1"))
	m2 := NewMessage("", []byte("m2"))
	m2.SetTags("TagA")
	body := EncodeMessages([]*Message{m1, m2})

	msgs := DecodeBatchMessages(body)
	if len(msgs) != 2 {
		t.Fatalf("decoded %d", len(msgs))
	}
	if string(msgs[0].GetBody()) != "m1" || msgs[0].Flag != 0 || msgs[0].Properties.Len() != 0 {
		t.Fatalf("m1: %v flag %d props %d", msgs[0].GetBody(), msgs[0].Flag, msgs[0].Properties.Len())
	}
	if string(msgs[1].GetBody()) != "m2" || msgs[1].Flag != 0 {
		t.Fatalf("m2 body/flag mismatch")
	}
	if v, ok := msgs[1].GetProperty(PropertyTags); !ok || v != "TagA" {
		t.Fatalf("m2 tags %q", v)
	}
	// 单条帧：flag 与 body、属性一起回来
	one := NewMessage("", []byte("body"))
	one.Flag = 7
	one.SetTags("TagA")
	one.SetKeys("k1")
	msg, err := DecodeBatchMessage(EncodeMessage(one))
	if err != nil {
		t.Fatal(err)
	}
	if msg.Flag != 7 || string(msg.GetBody()) != "body" {
		t.Fatalf("flag %d body %v", msg.Flag, msg.GetBody())
	}
	if v, _ := msg.GetKeys(); v != "k1" {
		t.Fatalf("keys %q", v)
	}
	if CountInnerMsgNum(body) != 2 {
		t.Fatalf("inner count %d", CountInnerMsgNum(body))
	}
	// 帧被截断时列表停在最后一条完整消息
	if got := DecodeBatchMessages(body[:30]); len(got) != 1 {
		t.Fatalf("truncated decode %d", len(got))
	}
}

// 批量 body 的编码里 MagicCode 固定 0、BODYCRC 固定 0（Java encodeMessage 同）
func TestBatchFrameConstantsAreZero(t *testing.T) {
	m := NewMessage("", []byte("x"))
	raw := EncodeMessage(m)
	magic := int32(uint32(raw[4])<<24 | uint32(raw[5])<<16 | uint32(raw[6])<<8 | uint32(raw[7]))
	crc := int32(uint32(raw[8])<<24 | uint32(raw[9])<<16 | uint32(raw[10])<<8 | uint32(raw[11]))
	if magic != 0 || crc != 0 {
		t.Fatalf("magic %d crc %d", magic, crc)
	}
}

func bytes2HexString(b []byte) string {
	const digits = "0123456789ABCDEF"
	out := make([]byte, 0, len(b)*2)
	for _, v := range b {
		out = append(out, digits[v>>4], digits[v&0xF])
	}
	return string(out)
}
