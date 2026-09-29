package common

import "testing"

func TestLegalTopicAndGroupNames(t *testing.T) {
	legal := []string{
		"order-topic",
		"%RETRY%myGroup",
		"%DLQ%myGroup",
		"CID_ONS-HTTP-PROXY",
		"Topic|With|Pipe",
		"TBW102",
		"a-b_c%|d",
	}
	for _, name := range legal {
		if IsTopicOrGroupIllegal(name) {
			t.Fatalf("%q must be legal", name)
		}
	}
}

func TestIllegalTopicAndGroupNames(t *testing.T) {
	illegal := []string{
		"topic.name",
		"topic 1",
		"中文topic",
		"topic~",
		"topic!",
		"\x7f",
		"\x80",
	}
	for _, name := range illegal {
		if !IsTopicOrGroupIllegal(name) {
			t.Fatalf("%q must be illegal", name)
		}
	}
	// 空串在这里放行：blank 由上层 checkTopic 的 is_blank 档先拦（Java 同顺序）
	if IsTopicOrGroupIllegal("") {
		t.Fatal("empty is checked by the blank step, not the charset")
	}
}

func TestSystemTopicSet(t *testing.T) {
	if len(systemTopicSet) != 12 {
		t.Fatalf("system topic set has %d entries, want 12", len(systemTopicSet))
	}
	for name := range systemTopicSet {
		if !IsSystemTopic(name) {
			t.Fatalf("%q must be a system topic", name)
		}
	}
	if !IsSystemTopic("rmq_sys_whatever") {
		t.Fatal("the rmq_sys_ prefix marks system topics")
	}
	if IsSystemTopic("MyTopic") || IsSystemTopic("") {
		t.Fatal("ordinary names are not system topics")
	}
}

func TestNotAllowedSendTopicSet(t *testing.T) {
	if len(notAllowedSendTopicSet) != 8 {
		t.Fatalf("not-allowed-send set has %d entries, want 8", len(notAllowedSendTopicSet))
	}
	if !IsNotAllowedSendTopic(RmqSysScheduleTopic) {
		t.Fatal("SCHEDULE_TOPIC_XXXX must be forbidden")
	}
	if !IsNotAllowedSendTopic(RmqSysTransHalfTopic) {
		t.Fatal("RMQ_SYS_TRANS_HALF_TOPIC must be forbidden")
	}
	// TBW102 是系统 topic 但允许发送；%RETRY% 是 sendMessageBack 的正常目标
	if IsNotAllowedSendTopic(AutoCreateTopicKeyTopic) {
		t.Fatal("TBW102 must stay sendable")
	}
	if IsNotAllowedSendTopic("%RETRY%myGroup") {
		t.Fatal("%RETRY% must stay sendable")
	}
}
