package remoting

import (
	"strings"
	"testing"

	"github.com/zhaohai666/rocketmq-client-remoting/go/common"
)

// Guards for the fastjson2 inline-object-key writer. The expected byte strings
// were produced by feeding the real Go output to the 5.5.1 jars — for CRI, TST
// and CS fastjson2's own re-encoding of the parsed object is byte-identical to
// what the Go encoder emits, which is the strongest available check.

// The safety net: encoding/json VALIDATES Marshaler output, so a MessageQueue-
// keyed table routed through EncodeJSON must fail LOUDLY rather than silently
// serialising as an array (which is what a slice without MarshalJSON would do).
func TestMQKeyedJSONThroughEncodeJSONFailsLoudly(t *testing.T) {
	body := map[string]any{"offsetTable": EncodeMQOffsetTable(MQOffsetTable{
		{Queue: common.NewMessageQueue("T", "b", 0), Offset: 1},
	})}
	if got := EncodeJSON(body); got != nil {
		t.Fatalf("EncodeJSON returned %s, want nil so the mistake is not silent", got)
	}
	// And the reason must be the dedicated error, not a generic marshal failure.
	if _, err := (&mqKeyedJSON{}).MarshalJSON(); err == nil {
		t.Fatal("mqKeyedJSON.MarshalJSON must fail so encoding/json cannot be used by accident")
	} else if !strings.Contains(err.Error(), "EncodeFastJSON") {
		t.Errorf("error = %v, want it to name EncodeFastJSON", err)
	}
}

// The full ConsumerRunningInfo shape: raw inline-object keys in both tables,
// PROP_CONSUMEORDERLY without underscores, and `0.0` doubles in statusTable.
func TestEncodeFastJSONConsumerRunningInfoShape(t *testing.T) {
	cri := NewConsumerRunningInfo()
	cri.MQTable[common.NewMessageQueue("TopicA", "broker-a", 3)] =
		(&ProcessQueueInfo{CommitOffset: 7}).ToJSONValue()
	cri.StatusTable["TopicA"] = (&ConsumeStatus{}).ToJSONValue()
	cri.Properties[PropConsumeOrderly] = "false"

	got := string(cri.Encode())

	// Raw key — the unquoted `{` immediately after `:` is the whole point.
	wantTable := `"mqTable":{{"brokerName":"broker-a","queueId":3,"topic":"TopicA"}:{`
	if !strings.Contains(got, wantTable) {
		t.Errorf("body = %s\n want substring %s", got, wantTable)
	}
	// The escaped alternative is what fastjson2 rejects.
	if strings.Contains(got, `\"brokerName\"`) {
		t.Errorf("body = %s, key is escaped; fastjson2 rejects that spelling", got)
	}
	if !strings.Contains(got, `"PROP_CONSUMEORDERLY":"false"`) {
		t.Errorf("body = %s, want the underscore-free property VALUE", got)
	}
	if !strings.Contains(got, `"pullRT":0.0`) {
		t.Errorf("body = %s, want Java double spelling in statusTable", got)
	}
	// The empty tables stay objects, not nulls.
	if !strings.Contains(got, `"mqPopTable":{}`) || !strings.Contains(got, `"userConsumerInfo":{}`) {
		t.Errorf("body = %s, want empty tables as {}", got)
	}
	if !strings.Contains(got, `"subscriptionSet":[]`) {
		t.Errorf("body = %s, want subscriptionSet as []", got)
	}
}

// Java iterates a TreeMap<MessageQueue, X>, so the order is
// MessageQueue.compareTo: topic, then brokerName, then queueId. Inserting in the
// reverse order must not change the output.
func TestMQKeyedEntriesFollowMessageQueueCompareTo(t *testing.T) {
	queues := []common.MessageQueue{
		common.NewMessageQueue("TopicB", "broker-a", 0), // topic wins
		common.NewMessageQueue("TopicA", "broker-b", 0), // brokerName next
		common.NewMessageQueue("TopicA", "broker-a", 2), // queueId last
		common.NewMessageQueue("TopicA", "broker-a", 1),
	}
	build := func(order []int) string {
		cri := NewConsumerRunningInfo()
		for _, i := range order {
			cri.MQTable[queues[i]] = (&ProcessQueueInfo{CommitOffset: int64(i)}).ToJSONValue()
		}
		return string(cri.Encode())
	}
	forward := build([]int{0, 1, 2, 3})
	reverse := build([]int{3, 2, 1, 0})
	if forward != reverse {
		t.Errorf("order changed the output\n forward %s\n reverse %s", forward, reverse)
	}
	// The first table entry must be the (TopicA, broker-a, 1) queue.
	want := `"mqTable":{{"brokerName":"broker-a","queueId":1,"topic":"TopicA"}`
	if !strings.Contains(forward, want) {
		t.Errorf("body = %s\n want the first entry to be %s", forward, want)
	}
	// And they must appear in compareTo order.
	idx := func(sub string) int { return strings.Index(forward, sub) }
	keys := []string{
		`"queueId":1,"topic":"TopicA"`,
		`"queueId":2,"topic":"TopicA"`,
		`"brokerName":"broker-b"`,
		`"topic":"TopicB"`,
	}
	for i := 1; i < len(keys); i++ {
		if idx(keys[i-1]) > idx(keys[i]) {
			t.Errorf("entry %d (%s) sorts after %d (%s) in %s",
				i-1, keys[i-1], i, keys[i], forward)
		}
	}
}

// The change is CONTAINED: a body without a MessageQueue-keyed table must
// encode to the exact same bytes whichever writer is used.
func TestEncodeFastJSONMatchesEncodeJSONWhenNoMQTableIsPresent(t *testing.T) {
	bodies := []struct {
		name string
		v    any
	}{
		{"ConsumeMessageDirectlyResult", NewConsumeMessageDirectlyResult().ToJSONValue()},
		{"PopProcessQueueInfo", (&PopProcessQueueInfo{WaitAckCount: 3, Droped: true}).ToJSONValue()},
		{"ProcessQueueInfo", (&ProcessQueueInfo{CommitOffset: 9, CachedMsgSizeInMiB: 2}).ToJSONValue()},
		{"nested", map[string]any{
			"a": []any{1, "x", true, nil},
			"b": map[string]any{"c": 2.5},
			"d": "has <angle> & amp",
		}},
	}
	for _, b := range bodies {
		std := EncodeJSON(b.v)
		fast := EncodeFastJSON(b.v)
		if std == nil {
			t.Fatalf("%s: EncodeJSON failed", b.name)
		}
		if string(std) != string(fast) {
			t.Errorf("%s: writers disagree\n stdlib %s\n fast   %s", b.name, std, fast)
		}
	}
}

// A body whose table is empty still has to be a valid object (Java writes `{}`).
func TestEncodeFastJSONEmptyTables(t *testing.T) {
	if got := string((&ResetOffsetBody{}).Encode()); got != `{"offsetTable":{}}` {
		t.Errorf("empty ResetOffsetBody = %s, want {\"offsetTable\":{}}", got)
	}
	if got := string(NewConsumerRunningInfo().Encode()); !strings.Contains(got, `"mqTable":{}`) {
		t.Errorf("empty ConsumerRunningInfo = %s, want empty mqTable object", got)
	}
}

// Strings must be escaped the same way in both writers: no HTML escaping, but
// quotes/backslashes/control characters are still escaped.
func TestEncodeFastJSONStringEscaping(t *testing.T) {
	v := map[string]any{"s": "a\"b\\c\nd<e>f&g"}
	if std, fast := EncodeJSON(v), EncodeFastJSON(v); string(std) != string(fast) {
		t.Errorf("escaping differs\n stdlib %s\n fast   %s", std, fast)
	}
	if got := string(EncodeFastJSON(v)); strings.Contains(got, `\u003c`) {
		t.Errorf("got %s, want no HTML escaping (Java does not escape < > &)", got)
	}
}

// Go must be able to read the fastjson2 form it now emits (the admin side of
// the same wire): the tolerant parser keeps the object key as raw text and
// DecodeMapKey re-parses it.
func TestFastJSONMQKeyedBodyRoundTripsThroughTheTolerantParser(t *testing.T) {
	cri := NewConsumerRunningInfo()
	cri.MQTable[common.NewMessageQueue("TopicA", "broker-a", 3)] =
		(&ProcessQueueInfo{CommitOffset: 7}).ToJSONValue()
	cri.MQPopTable[common.NewMessageQueue("TopicA", "broker-a", 5)] =
		(&PopProcessQueueInfo{WaitAckCount: 2}).ToJSONValue()

	back, err := DecodeConsumerRunningInfo(cri.Encode())
	if err != nil {
		t.Fatalf("DecodeConsumerRunningInfo: %v", err)
	}
	if len(back.MQTable) != 1 || len(back.MQPopTable) != 1 {
		t.Fatalf("mqTable=%d mqPopTable=%d, want 1/1", len(back.MQTable), len(back.MQPopTable))
	}
	for q, info := range back.MQTable {
		if q.Topic != "TopicA" || q.BrokerName != "broker-a" || q.QueueID != 3 {
			t.Errorf("decoded mqTable key = %+v", q)
		}
		if got := info["commitOffset"]; got == nil {
			t.Errorf("decoded mqTable value lost commitOffset: %+v", info)
		}
	}
	// And the typed reader must see the value, not a default.
	pqi := &ProcessQueueInfo{}
	if err := pqi.FromJSONValue(back.MQTable[common.NewMessageQueue("TopicA", "broker-a", 3)]); err != nil {
		t.Fatalf("ProcessQueueInfo.FromJSONValue: %v", err)
	}
	if pqi.CommitOffset != 7 {
		t.Errorf("CommitOffset = %d, want 7", pqi.CommitOffset)
	}
}

// Repeat encodes must be byte-identical (map iteration is not deterministic, so
// the writer sorts).
func TestEncodeFastJSONIsDeterministic(t *testing.T) {
	cri := NewConsumerRunningInfo()
	for i := int32(0); i < 12; i++ {
		cri.MQTable[common.NewMessageQueue("T", "b", i)] =
			(&ProcessQueueInfo{CommitOffset: int64(i)}).ToJSONValue()
	}
	want := string(cri.Encode())
	for i := 0; i < 20; i++ {
		if got := string(cri.Encode()); got != want {
			t.Fatalf("encode #%d differs\n got %s\nwant %s", i, got, want)
		}
	}
}

// A MessageQueue-keyed offset body from the admin side (220) must use the raw
// key form too — this one travels to the broker and on to a Java consumer.
//
// MQOffsetTable is a SLICE, so its order is the caller's and is preserved
// as-is; Java declares the field as an unordered HashMap, so order is not part
// of this particular contract.
func TestEncodeFastJSONResetOffsetBodyShape(t *testing.T) {
	body := &ResetOffsetBody{OffsetTable: MQOffsetTable{
		{Queue: common.NewMessageQueue("T", "b", 1), Offset: 11},
		{Queue: common.NewMessageQueue("T", "b", 0), Offset: 10},
	}}
	got := string(body.Encode())
	want := `{"offsetTable":{{"brokerName":"b","queueId":1,"topic":"T"}:11,` +
		`{"brokerName":"b","queueId":0,"topic":"T"}:10}}`
	if got != want {
		t.Errorf("ResetOffsetBody\n got %s\nwant %s", got, want)
	}
	if strings.Contains(got, `\"brokerName\"`) {
		t.Errorf("body = %s, key is escaped; fastjson2 rejects that spelling", got)
	}
}
