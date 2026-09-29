package common

import (
	"strings"
	"testing"
)

func TestRetryDLQAndReplyTopics(t *testing.T) {
	if got := GetRetryTopic("GroupA"); got != "%RETRY%GroupA" {
		t.Fatalf("retry topic %q", got)
	}
	if !IsRetryTopic("%RETRY%GroupA") || IsRetryTopic("GroupA") {
		t.Fatal("IsRetryTopic prefix check")
	}
	if got := GetDLQTopic("GroupA"); got != "%DLQ%GroupA" {
		t.Fatalf("dlq topic %q", got)
	}
	if ResetRetryAndDLQTopic("%RETRY%GroupA") != "GroupA" ||
		ResetRetryAndDLQTopic("%DLQ%GroupA") != "GroupA" ||
		ResetRetryAndDLQTopic("Plain") != "Plain" {
		t.Fatal("ResetRetryAndDLQTopic mismatch")
	}
	if got := GetReplyTopic("DefaultCluster"); got != "DefaultCluster_REPLY_TOPIC" {
		t.Fatalf("reply topic %q", got)
	}
}

func TestCompareAndIncreaseNamespace(t *testing.T) {
	if got := CompareAndIncreaseNamespace("inst", ""); got != "inst" {
		t.Fatalf("empty namespace is a no-op, got %q", got)
	}
	if got := CompareAndIncreaseNamespace("inst", "ns"); got != "%ns%%inst" {
		t.Fatalf("got %q", got)
	}
	// 幂等：已带命名空间的实例名不再叠加
	if got := CompareAndIncreaseNamespace("%ns%%inst", "ns"); got != "%ns%%inst" {
		t.Fatalf("idempotency broken: %q", got)
	}
}

func TestBrokerVIPChannel(t *testing.T) {
	if got := BrokerVIPChannel(false, "127.0.0.1:10911"); got != "127.0.0.1:10911" {
		t.Fatalf("unchanged expected, got %q", got)
	}
	if got := BrokerVIPChannel(true, "127.0.0.1:10911"); got != "127.0.0.1:10909" {
		t.Fatalf("vip port = %q", got)
	}
	if got := BrokerVIPChannel(true, "no-port"); got != "no-port" {
		t.Fatalf("unparseable must pass through, got %q", got)
	}
}

func TestBuildMQClientID(t *testing.T) {
	if got := BuildMQClientID("10.0.0.1", "inst", "", false); got != "10.0.0.1@inst" {
		t.Fatalf("got %q", got)
	}
	if got := BuildMQClientID("10.0.0.1", "inst", "unit-a", false); got != "10.0.0.1@inst@unit-a" {
		t.Fatalf("unit segment: %q", got)
	}
	if got := BuildMQClientID("10.0.0.1", "inst", " ", false); got != "10.0.0.1@inst" {
		t.Fatal("blank unit must be skipped")
	}
	if got := BuildMQClientID("10.0.0.1", "inst", "", true); got != "10.0.0.1@inst@STREAM" {
		t.Fatalf("stream segment: %q", got)
	}
}

func TestChangeInstanceNameToPIDAndClientIDFor(t *testing.T) {
	pidName := ChangeInstanceNameToPID(DefaultInstanceName)
	if !strings.HasPrefix(pidName, itoa(CachedPID())+"#") {
		t.Fatalf("instance name %q must start with pid#", pidName)
	}
	if again := ChangeInstanceNameToPID(pidName); again != pidName {
		t.Fatal("rewrite must be idempotent")
	}
	if kept := ChangeInstanceNameToPID("custom"); kept != "custom" {
		t.Fatalf("non-default name stays, got %q", kept)
	}
	if InstanceNameForModel("DEFAULT", false) != "DEFAULT" {
		t.Fatal("broadcasting keeps DEFAULT")
	}
	if InstanceNameForModel("DEFAULT", true) == "DEFAULT" {
		t.Fatal("clustering rewrites DEFAULT")
	}
	a := ClientIDFor(DefaultInstanceName, "", false)
	b := ClientIDFor(DefaultInstanceName, "", false)
	if a == b {
		t.Fatal("default client ids must differ per call (pid#nanoTime)")
	}
	if !strings.HasPrefix(a, CachedIPStr()+"@") {
		t.Fatalf("client id %q must be IP-first", a)
	}
}

func itoa(v int) string {
	if v == 0 {
		return "0"
	}
	digits := ""
	for v > 0 {
		digits = string(rune('0'+v%10)) + digits
		v /= 10
	}
	return digits
}

func TestMessageQueueStringRoundTrip(t *testing.T) {
	q := NewMessageQueue("TopicTest", "BrokerA", 3)
	back, err := StringToMessageQueue(MessageQueueToString(q), " ")
	if err != nil || back != q {
		t.Fatalf("round trip: %v %v", back, err)
	}
	if _, err := StringToMessageQueue("only-two", " "); err == nil {
		t.Fatal("fewer than 3 parts must fail")
	}
	queues := StringToMessageQueues("TopicTest BrokerA 0\n\nTopicTest BrokerA 1\nbroken-line")
	if len(queues) != 2 || queues[1].QueueID != 1 {
		t.Fatalf("parsed %v", queues)
	}
}

func TestPropertiesToString(t *testing.T) {
	m := NewStringMap()
	m.Put("B", "2")
	m.Put("A", "1")
	if got := PropertiesToString(m, false); got != "B=2\nA=1\n" {
		t.Fatalf("unsorted %q", got)
	}
	if got := PropertiesToString(m, true); got != "A=1\nB=2\n" {
		t.Fatalf("sorted %q", got)
	}
	if got := PropertiesToString(nil, true); got != "" {
		t.Fatalf("nil map %q", got)
	}
}

func TestCreateUniqNameShape(t *testing.T) {
	a := CreateUniqName("prefix")
	b := CreateUniqName("prefix")
	if !strings.HasPrefix(a, "prefix") {
		t.Fatalf("prefix missing: %q", a)
	}
	if a == b {
		t.Fatal("consecutive names must differ")
	}
}
