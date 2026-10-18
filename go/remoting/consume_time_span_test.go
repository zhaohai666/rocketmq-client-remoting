// QUERY_CONSUME_TIME_SPAN(303) body tests.
//
// Wire facts pinned here:
//   - the body is a JSON object holding `consumeTimeSpanSet`, one entry per
//     queue this broker owns; the entry's `messageQueue` is a nested object with
//     `topic` / `brokerName` / `queueId` (fastjson2 field names, matched byte for
//     byte);
//   - a broker that owns no queue of the topic answers with an EMPTY BODY, which
//     is a legitimate answer, not a decode failure;
//   - `delayTime` is broker-computed (the age of the oldest unconsumed message,
//     0 once the group is drained) and must survive a round trip untouched.
package remoting

import (
	"strings"
	"testing"

	"github.com/zhaohai666/rocketmq-client-remoting/go/common"
)

// brokerBodyOf is the shape a 5.x broker puts on the wire for one queue.
const brokerTimeSpanBody = `{"consumeTimeSpanSet":[` +
	`{"messageQueue":{"topic":"topicName","brokerName":"brokerName","queueId":1},` +
	`"minTimeStamp":1550825710000,"maxTimeStamp":1550825790000,` +
	`"consumeTimeStamp":1550825760000,"delayTime":5000}]}`

func TestDecodeQueryConsumeTimeSpanBodyBrokerShape(t *testing.T) {
	body, err := DecodeQueryConsumeTimeSpanBody([]byte(brokerTimeSpanBody))
	if err != nil {
		t.Fatal(err)
	}
	if len(body.ConsumeTimeSpanSet) != 1 {
		t.Fatalf("got %d windows, want 1", len(body.ConsumeTimeSpanSet))
	}
	span := body.ConsumeTimeSpanSet[0]
	want := common.NewMessageQueue("topicName", "brokerName", 1)
	if span.MessageQueue != want {
		t.Errorf("messageQueue = %+v, want %+v", span.MessageQueue, want)
	}
	if span.MinTimeStamp != 1550825710000 || span.MaxTimeStamp != 1550825790000 ||
		span.ConsumeTimeStamp != 1550825760000 || span.DelayTime != 5000 {
		t.Errorf("timestamps lost or renamed: %+v", span)
	}
	if got := span.String(); !strings.Contains(got, "messageQueue=") ||
		!strings.Contains(got, "delayTime=5000") {
		t.Errorf("String() rendering: %q", got)
	}
}

func TestDecodeQueryConsumeTimeSpanBodyEmptyIsNoWindow(t *testing.T) {
	// A broker that owns no queue of the topic reports no window at all.
	for _, data := range [][]byte{nil, {}, []byte("")} {
		body, err := DecodeQueryConsumeTimeSpanBody(data)
		if err != nil {
			t.Fatalf("empty body %q must not be an error: %v", data, err)
		}
		if body == nil || len(body.ConsumeTimeSpanSet) != 0 {
			t.Errorf("empty body %q gave %+v", data, body)
		}
	}
	// An explicit empty set and a missing key read the same way.
	for _, text := range []string{`{}`, `{"consumeTimeSpanSet":[]}`, `{"consumeTimeSpanSet":null}`} {
		body, err := DecodeQueryConsumeTimeSpanBody([]byte(text))
		if err != nil {
			t.Fatalf("%s: %v", text, err)
		}
		if len(body.ConsumeTimeSpanSet) != 0 {
			t.Errorf("%s gave %d windows, want 0", text, len(body.ConsumeTimeSpanSet))
		}
	}
}

func TestDecodeQueryConsumeTimeSpanBodyErrors(t *testing.T) {
	for _, text := range []string{`not json`, `[{}]`, `"a string"`} {
		if _, err := DecodeQueryConsumeTimeSpanBody([]byte(text)); err == nil {
			t.Errorf("%q must not decode as a QueryConsumeTimeSpanBody", text)
		}
	}
	// A broken entry inside an otherwise valid set is an error, not a drop: a
	// silently missing window would look like a drained queue.
	if _, err := DecodeQueryConsumeTimeSpanBody(
		[]byte(`{"consumeTimeSpanSet":[1]}`)); err == nil {
		t.Error("a non-object window must be an error")
	}
}

func TestQueueTimeSpanEncodeKeepsWireNames(t *testing.T) {
	span := &QueueTimeSpan{
		MessageQueue:     common.NewMessageQueue("topicName", "brokerName", 1),
		MinTimeStamp:     1550825710000,
		MaxTimeStamp:     1550825790000,
		ConsumeTimeStamp: 1550825760000,
		DelayTime:        5000,
	}
	text := string(span.Encode())
	for _, want := range []string{
		`"messageQueue":{"brokerName":"brokerName","queueId":1,"topic":"topicName"}`,
		`"minTimeStamp":1550825710000`,
		`"maxTimeStamp":1550825790000`,
		`"consumeTimeStamp":1550825760000`,
		`"delayTime":5000`,
	} {
		if !strings.Contains(text, want) {
			t.Errorf("encoded window missing %s: %s", want, text)
		}
	}
	// The reference's `*TimeStampStr` accessors are derived renderings, never
	// wire fields: a broker that rejects unknown keys would reject them here.
	for _, banned := range []string{"minTimeStampStr", "maxTimeStampStr", "consumeTimeStampStr"} {
		if strings.Contains(text, banned) {
			t.Errorf("%s must not be encoded: %s", banned, text)
		}
	}
}

func TestQueryConsumeTimeSpanBodyRoundTrip(t *testing.T) {
	body := NewQueryConsumeTimeSpanBody()
	body.ConsumeTimeSpanSet = []*QueueTimeSpan{
		{
			MessageQueue: common.NewMessageQueue("Tt", "b1", 0),
			MinTimeStamp: 1, MaxTimeStamp: 9, ConsumeTimeStamp: 4, DelayTime: 5,
		},
		{
			MessageQueue: common.NewMessageQueue("Tt", "b1", 1),
		},
	}
	back, err := DecodeQueryConsumeTimeSpanBody(body.Encode())
	if err != nil {
		t.Fatal(err)
	}
	if len(back.ConsumeTimeSpanSet) != 2 {
		t.Fatalf("round trip kept %d windows", len(back.ConsumeTimeSpanSet))
	}
	if *back.ConsumeTimeSpanSet[0] != *body.ConsumeTimeSpanSet[0] {
		t.Errorf("round trip mismatch: %+v vs %+v", *back.ConsumeTimeSpanSet[0],
			*body.ConsumeTimeSpanSet[0])
	}
	if back.ConsumeTimeSpanSet[1].MessageQueue.QueueID != 1 {
		t.Errorf("second window lost its queue: %+v", back.ConsumeTimeSpanSet[1])
	}

	// Decoding twice must not append to the previous answer.
	if err := back.FromJSONValue(mustDecode(t, body.Encode())); err != nil {
		t.Fatal(err)
	}
	if len(back.ConsumeTimeSpanSet) != 2 {
		t.Errorf("FromJSONValue accumulated: %d windows", len(back.ConsumeTimeSpanSet))
	}
}

func TestQueueTimeSpanFromJSONValueToleratesNumbersAsText(t *testing.T) {
	// fastjson2 renders a long as a bare number, but a hand-written or proxied
	// answer can quote it; the port's numeric reader takes both.
	span := &QueueTimeSpan{}
	err := span.FromJSONValue(mustDecode(t, []byte(
		`{"messageQueue":{"topic":"Tt","brokerName":"b1","queueId":"2"},`+
			`"minTimeStamp":"10","maxTimeStamp":20,"consumeTimeStamp":15,"delayTime":"5"}`)))
	if err != nil {
		t.Fatal(err)
	}
	if span.MessageQueue.QueueID != 2 {
		t.Errorf("queueId = %d, want 2", span.MessageQueue.QueueID)
	}
	if span.MinTimeStamp != 10 || span.MaxTimeStamp != 20 || span.ConsumeTimeStamp != 15 ||
		span.DelayTime != 5 {
		t.Errorf("timestamps = %+v", span)
	}
}

func TestQueueTimeSpanFromJSONValueRejectsNonObject(t *testing.T) {
	if err := (&QueueTimeSpan{}).FromJSONValue([]any{}); err == nil {
		t.Error("a non-object window must be an error")
	}
	// A window without the nested messageQueue still decodes: only the keys the
	// broker sends are read, and a missing queue stays the zero value.
	span := &QueueTimeSpan{}
	if err := span.FromJSONValue(mustDecode(t, []byte(`{"maxTimeStamp":7}`))); err != nil {
		t.Fatal(err)
	}
	if span.MaxTimeStamp != 7 || span.MessageQueue.Topic != "" {
		t.Errorf("partial window = %+v", span)
	}
}
