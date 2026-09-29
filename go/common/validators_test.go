package common

import (
	"strings"
	"testing"
)

func repeatByte(b byte, n int) string { return strings.Repeat(string([]byte{b}), n) }

// clientErrOf unwraps an *Error and returns (message, code, hasCode) so the
// tests can assert Java's "pure client error has no code" rule explicitly.
func clientErrOf(t *testing.T, err error) (string, int32, bool) {
	t.Helper()
	if err == nil {
		t.Fatal("expected an error, got nil")
	}
	e, ok := err.(*Error)
	if !ok {
		t.Fatalf("expected *common.Error, got %T", err)
	}
	if e.Kind != KindClient {
		t.Fatalf("expected KindClient, got kind %d (%v)", e.Kind, err)
	}
	return e.Message, e.Code, e.HasCode
}

func TestCheckGroupRulesAndTextsMatchJava(t *testing.T) {
	for _, blank := range []string{"", "   ", "\t "} {
		text, code, hasCode := clientErrOf(t, CheckGroup(blank))
		if text != "the specified group is blank" {
			t.Fatalf("blank group %q: message %q", blank, text)
		}
		// Pure client error: no broker code (Java's -1).
		if hasCode || code != 0 {
			t.Fatalf("blank group %q must carry no code, got (%d,%v)", blank, code, hasCode)
		}
	}
	if err := CheckGroup(repeatByte('g', 120)); err != nil {
		t.Fatalf("120 chars must pass: %v", err)
	}
	tooLong := repeatByte('g', 121)
	text, _, _ := clientErrOf(t, CheckGroup(tooLong))
	want := "the specified group[" + tooLong + "] is longer than group max length: 120."
	if text != want {
		t.Fatalf("group too long: got %q want %q", text, want)
	}
	text, _, _ = clientErrOf(t, CheckGroup("CID 001"))
	want = "the specified group[CID 001] contains illegal characters, allowing only " + ValidCharPattern
	if text != want {
		t.Fatalf("group charset: got %q want %q", text, want)
	}
	if err := CheckGroup("CID_ok-1|2%3"); err != nil {
		t.Fatalf("legal group rejected: %v", err)
	}
}

func TestCheckTopicRulesAndTextsMatchJava(t *testing.T) {
	text, _, _ := clientErrOf(t, CheckTopic(""))
	if text != "The specified topic is blank" {
		t.Fatalf("blank topic: %q", text)
	}
	if err := CheckTopic(repeatByte('a', 127)); err != nil {
		t.Fatalf("127 chars must pass: %v", err)
	}
	text, _, _ = clientErrOf(t, CheckTopic(repeatByte('a', 128)))
	if text != "The specified topic is longer than topic max length 127." {
		t.Fatalf("topic too long: %q", text)
	}
	text, _, _ = clientErrOf(t, CheckTopic("bad.topic"))
	want := "The specified topic[bad.topic] contains illegal characters, allowing only " + ValidCharPattern
	if text != want {
		t.Fatalf("topic charset: got %q want %q", text, want)
	}
}

func TestSystemAndForbiddenTopicsAreCodeless(t *testing.T) {
	text, code, hasCode := clientErrOf(t, ValidateSystemTopic("rmq_sys_watermark"))
	if text != "The topic[rmq_sys_watermark] is conflict with system topic." {
		t.Fatalf("system topic text: %q", text)
	}
	if hasCode || code != 0 {
		t.Fatalf("system topic must be codeless, got (%d,%v)", code, hasCode)
	}
	if err := ValidateSystemTopic("MyTopic"); err != nil {
		t.Fatalf("ordinary topic rejected: %v", err)
	}

	text, code, hasCode = clientErrOf(t, ValidateNotAllowedSendTopic(RmqSysScheduleTopic))
	if text != "Sending message to topic[SCHEDULE_TOPIC_XXXX] is forbidden." {
		t.Fatalf("forbidden topic text: %q", text)
	}
	// Java's isNotAllowedSendTopic is also (String, null) => no broker code,
	// deliberately NOT MESSAGE_ILLEGAL.
	if hasCode || code != 0 {
		t.Fatalf("forbidden topic must be codeless, got (%d,%v)", code, hasCode)
	}
}

func TestCheckMessageBodyBranchesCarryCode13(t *testing.T) {
	// nil body vs zero-length body must stay distinguishable.
	text, code, hasCode := clientErrOf(t, CheckMessage(&Message{Topic: "T1"}, 4096))
	if text != "the message body is null" {
		t.Fatalf("nil body: %q", text)
	}
	if !hasCode || code != ResponseCodeMessageIllegal {
		t.Fatalf("nil body must carry 13, got (%d,%v)", code, hasCode)
	}

	text, code, _ = clientErrOf(t, CheckMessage(&Message{Topic: "T1", Body: []byte{}}, 4096))
	if text != "the message body length is zero" {
		t.Fatalf("empty body: %q", text)
	}
	if code != ResponseCodeMessageIllegal {
		t.Fatalf("empty body must carry 13, got %d", code)
	}

	text, code, _ = clientErrOf(t, CheckMessage(&Message{Topic: "T1", Body: make([]byte, 4097)}, 4096))
	if text != "the message body size over max value, MAX: 4096" {
		t.Fatalf("oversize body: %q", text)
	}
	if code != ResponseCodeMessageIllegal {
		t.Fatalf("oversize body must carry 13, got %d", code)
	}

	// Exactly at the threshold passes: Java uses >, not >=.
	if err := CheckMessage(&Message{Topic: "T1", Body: make([]byte, 4096)}, 4096); err != nil {
		t.Fatalf("body at the threshold must pass: %v", err)
	}
}

func TestCheckMessageValidatesTopicBeforeBody(t *testing.T) {
	// Illegal topic + empty body reports the topic problem: order is Java's.
	text, code, hasCode := clientErrOf(t, CheckMessage(&Message{Topic: "bad.topic"}, 4096))
	if !strings.HasPrefix(text, "The specified topic[bad.topic]") {
		t.Fatalf("topic must be checked first, got %q", text)
	}
	if hasCode || code != 0 {
		t.Fatalf("topic failure must be codeless, got (%d,%v)", code, hasCode)
	}
	// nil message keeps MESSAGE_ILLEGAL so the branch stays debuggable.
	text, code, _ = clientErrOf(t, CheckMessage(nil, 4096))
	if text != "the message is null" || code != ResponseCodeMessageIllegal {
		t.Fatalf("nil message: (%q,%d)", text, code)
	}
}

func TestCheckMessageAllowsRetryTopicAndRejectsLmqSeparator(t *testing.T) {
	// %RETRY%group is sendMessageBack's normal target and must never be banned.
	if err := CheckMessage(&Message{Topic: "%RETRY%myGroup", Body: []byte{1}}, 4096); err != nil {
		t.Fatalf("%%RETRY%% must pass: %v", err)
	}
	if err := CheckMessage(&Message{Topic: "%DLQ%myGroup", Body: []byte{1}}, 4096); err != nil {
		t.Fatalf("%%DLQ%% must pass: %v", err)
	}

	msg := &Message{Topic: "T1", Body: []byte{1}}
	msg.PutProperty(PropertyInnerMultiDispatch, "queueA/extra")
	text, code, _ := clientErrOf(t, CheckMessage(msg, 4096))
	want := "INNER_MULTI_DISPATCH queueA/extra can not contains / character"
	if text != want {
		t.Fatalf("lmq separator: got %q want %q", text, want)
	}
	if code != ResponseCodeMessageIllegal {
		t.Fatalf("lmq separator must carry 13, got %d", code)
	}

	// A normal comma-separated LMQ path passes.
	ok := &Message{Topic: "T1", Body: []byte{1}}
	ok.PutProperty(PropertyInnerMultiDispatch, "queueA,queueB")
	if err := CheckMessage(ok, 4096); err != nil {
		t.Fatalf("comma-separated LMQ path must pass: %v", err)
	}
}
