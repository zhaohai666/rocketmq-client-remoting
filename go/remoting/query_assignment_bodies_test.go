package remoting

import (
	"strings"
	"testing"

	"github.com/zhaohai666/rocketmq-client-remoting/go/common"
)

func TestQueryAssignmentRequestBodyRoundTrip(t *testing.T) {
	body := &QueryAssignmentRequestBody{
		Topic:         "TopicA",
		ConsumerGroup: "G1",
		ClientID:      "cid-1",
		StrategyName:  "AVG",
		MessageModel:  MessageModelClustering,
	}
	decoded, err := DecodeQueryAssignmentRequestBody(body.Encode())
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if *decoded != *body {
		t.Fatalf("round trip\n got=%+v\nwant=%+v", decoded, body)
	}
	wire := string(body.Encode())
	for _, key := range []string{
		`"topic"`, `"consumerGroup"`, `"clientId"`, `"strategyName"`, `"messageModel"`,
	} {
		if !strings.Contains(wire, key) {
			t.Fatalf("wire body %s lacks key %s", wire, key)
		}
	}
}

func TestQueryAssignmentResponseBodyRoundTrip(t *testing.T) {
	body := &QueryAssignmentResponseBody{
		MessageQueueAssignments: []*MessageQueueAssignment{
			{
				MessageQueue: common.NewMessageQueue("TopicA", "b1", 0),
				Mode:         MessageRequestModePop,
			},
			{
				MessageQueue: common.NewMessageQueue("TopicA", "b1", 1),
				Mode:         MessageRequestModePull,
				Attachments:  func() *common.StringMap { m := common.NewStringMap(); m.Put("k", "v"); return m }(),
			},
		},
	}
	decoded, err := DecodeQueryAssignmentResponseBody(body.Encode())
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(decoded) != 2 {
		t.Fatalf("decoded %d assignments: %+v", len(decoded), decoded)
	}
	if decoded[0].MessageQueue != (common.NewMessageQueue("TopicA", "b1", 0)) {
		t.Fatalf("assignment 0 queue = %+v", decoded[0].MessageQueue)
	}
	if decoded[0].Mode != MessageRequestModePop {
		t.Fatalf("assignment 0 mode = %q", decoded[0].Mode)
	}
	if decoded[1].Mode != MessageRequestModePull {
		t.Fatalf("assignment 1 mode = %q, want the PULL default", decoded[1].Mode)
	}
	if decoded[1].Attachments == nil || decoded[1].Attachments.Len() != 1 {
		t.Fatalf("assignment 1 attachments = %v", decoded[1].Attachments)
	}
}

func TestQueryAssignmentResponseBodyNullVsEmpty(t *testing.T) {
	got, err := DecodeQueryAssignmentResponseBody(nil)
	if err != nil || got != nil {
		t.Fatalf("empty body: got=%v err=%v, want nil/nil", got, err)
	}
	// Object without the key: same.
	got, err = DecodeQueryAssignmentResponseBody(EncodeJSON(map[string]any{}))
	if err != nil || got != nil {
		t.Fatalf("missing key: got=%v err=%v, want nil/nil", got, err)
	}
	// Explicit EMPTY array: "clear everything" — must be non-nil.
	got, err = DecodeQueryAssignmentResponseBody(
		(&QueryAssignmentResponseBody{MessageQueueAssignments: []*MessageQueueAssignment{}}).Encode())
	if err != nil {
		t.Fatalf("empty set decode: %v", err)
	}
	if got == nil || len(got) != 0 {
		t.Fatalf("empty set: got=%#v, want non-nil empty slice", got)
	}
	// Malformed payload is an error, never a silent nil.
	if _, err := DecodeQueryAssignmentResponseBody([]byte("[1,2,3]")); err == nil {
		t.Fatal("malformed body must error")
	}
}
