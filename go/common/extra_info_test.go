package common

import (
	"strings"
	"testing"
)

// The 8-segment layout is the contract with the broker. These cases are
// transcribed from the Java call sites (PullAPIWrapper.popAsync ->
// MQClientAPIImpl.processPopResponse -> DefaultMQPushConsumerImpl.ackAsync) so a
// reordering of the segments fails here instead of at "every ack silently does
// nothing" on a real cluster.

func TestExtraInfoBuildAndParseRoundTrip(t *testing.T) {
	ck := BuildExtraInfoWithMsgQueueOffset(100, 1700000000000, 60000, 3, "TopicA", "broker-a", 2, 107)

	parts := SplitExtraInfo(ck)
	if len(parts) != ExtraInfoSegmentCount {
		t.Fatalf("want %d segments, got %d: %q", ExtraInfoSegmentCount, len(parts), ck)
	}
	want := "100 1700000000000 60000 3 0 broker-a 2 107"
	if ck != want {
		t.Fatalf("checkpoint shape\n got=%q\nwant=%q", ck, want)
	}

	if v, err := GetCkQueueOffset(parts); err != nil || v != 100 {
		t.Fatalf("GetCkQueueOffset = %d, %v", v, err)
	}
	if v, err := GetPopTime(parts); err != nil || v != 1700000000000 {
		t.Fatalf("GetPopTime = %d, %v", v, err)
	}
	if v, err := GetInvisibleTime(parts); err != nil || v != 60000 {
		t.Fatalf("GetInvisibleTime = %d, %v", v, err)
	}
	if v, err := GetReviveQid(parts); err != nil || v != 3 {
		t.Fatalf("GetReviveQid = %d, %v", v, err)
	}
	if v, err := GetBrokerName(parts); err != nil || v != "broker-a" {
		t.Fatalf("GetBrokerName = %q, %v", v, err)
	}
	if v, err := GetQueueId(parts); err != nil || v != 2 {
		t.Fatalf("GetQueueId = %d, %v", v, err)
	}
	// Segment 7 is the ACK offset; it is NOT the ckQueueOffset (segment 0).
	if v, err := GetQueueOffset(parts); err != nil || v != 107 {
		t.Fatalf("GetQueueOffset = %d, %v", v, err)
	}
	if v, _ := GetQueueOffset(parts); v == func() int64 { x, _ := GetCkQueueOffset(parts); return x }() {
		t.Fatal("segment 7 must not be segment 0")
	}
}

// The 7-argument overload stops at segment 6; Java uses it only as a prefix and
// then appends the offset. Using it as an ACK checkpoint would send an address
// with no message offset.
func TestExtraInfoSevenArgOverloadHasNoOffsetSegment(t *testing.T) {
	ck := BuildExtraInfo(100, 1700000000000, 60000, 3, "TopicA", "broker-a", 2)
	parts := SplitExtraInfo(ck)
	if len(parts) != 7 {
		t.Fatalf("want 7 segments, got %d: %q", len(parts), ck)
	}
	if _, err := GetQueueOffset(parts); err == nil {
		t.Fatal("a 7-segment checkpoint must not yield an ack offset")
	}
}

func TestExtraInfoRetryMarkerPerTopic(t *testing.T) {
	cases := []struct {
		topic string
		want  string
	}{
		{"TopicA", "0"},
		{"%RETRY%GroupA_TopicA", "1"},
		{"%RETRY%GroupA+TopicA", "2"},
		// A plain %RETRY% group topic (no separator) still counts as "1": Java
		// only checks the prefix.
		{"%RETRY%GroupA", "1"},
	}
	for _, c := range cases {
		if got := RetryOfTopic(c.topic); got != c.want {
			t.Fatalf("RetryOfTopic(%q) = %q, want %q", c.topic, got, c.want)
		}
	}

	if !IsPopRetryTopicV2("%RETRY%G+T") || IsPopRetryTopicV2("%RETRY%G_T") {
		t.Fatal("IsPopRetryTopicV2 must key off '+' only")
	}
	if !IsPopRetryTopicV1("%RETRY%G_T") || IsPopRetryTopicV1("%RETRY%G+T") {
		t.Fatal("IsPopRetryTopicV1 must exclude V2")
	}
	if got := BuildPopRetryTopicV1("TopicA", "GroupA"); got != "%RETRY%GroupA_TopicA" {
		t.Fatalf("V1 retry topic %q", got)
	}
	if got := BuildPopRetryTopicV2("TopicA", "GroupA"); got != "%RETRY%GroupA+TopicA" {
		t.Fatalf("V2 retry topic %q", got)
	}
}

// getRealTopic is how the client resolves the physical topic an ACK belongs to:
// a retried message must be acked on the retry topic, not on the business topic,
// or the broker cannot find the checkpoint.
func TestExtraInfoGetRealTopic(t *testing.T) {
	ck := BuildExtraInfoWithMsgQueueOffset(0, 1, 60000, 0, "%RETRY%GroupA_TopicA", "broker-a", 0, 5)
	parts := SplitExtraInfo(ck)
	if got := GetRealTopic(parts, "TopicA", "GroupA"); got != "%RETRY%GroupA_TopicA" {
		t.Fatalf("V1 real topic %q", got)
	}

	ckV2 := BuildExtraInfoWithMsgQueueOffset(0, 1, 60000, 0, "%RETRY%GroupA+TopicA", "broker-a", 0, 5)
	if got := GetRealTopic(SplitExtraInfo(ckV2), "TopicA", "GroupA"); got != "%RETRY%GroupA+TopicA" {
		t.Fatalf("V2 real topic %q", got)
	}

	plain := BuildExtraInfoWithMsgQueueOffset(0, 1, 60000, 0, "TopicA", "broker-a", 0, 5)
	if got := GetRealTopic(SplitExtraInfo(plain), "TopicA", "GroupA"); got != "TopicA" {
		t.Fatalf("normal real topic %q", got)
	}

	// The by-marker form rejects an unknown marker instead of defaulting.
	if _, err := GetRealTopicByRetry("TopicA", "GroupA", "9"); err == nil {
		t.Fatal("unknown retry marker must be rejected")
	}
}

func TestExtraInfoIsOrderUsesReviveQid(t *testing.T) {
	normal := SplitExtraInfo(BuildExtraInfoWithMsgQueueOffset(0, 1, 60000, 0, "T", "b", 0, 1))
	if IsOrder(normal) {
		t.Fatal("reviveQid 0 is not an orderly pop")
	}
	orderly := SplitExtraInfo(BuildExtraInfoWithMsgQueueOffset(0, 1, 60000, PopOrderReviveQueue, "T", "b", 0, 1))
	if !IsOrder(orderly) {
		t.Fatal("reviveQid 999 is an orderly pop")
	}
	if PopOrderReviveQueue != 999 {
		t.Fatalf("POP_ORDER_REVIVE_QUEUE = %d", PopOrderReviveQueue)
	}
}

// Java drops trailing empty segments; Go's strings.Split would keep them. That
// difference flips the length guards, so pin it down.
func TestExtraInfoSplitMatchesJavaTrailingTrim(t *testing.T) {
	if got := SplitExtraInfo("1 2"); len(got) != 2 {
		t.Fatalf("want 2 segments, got %v", got)
	}
	if got := SplitExtraInfo("1 2 "); len(got) != 2 {
		t.Fatalf("trailing separator must be trimmed, got %v", got)
	}
	// A leading separator is a real empty segment in Java.
	if got := SplitExtraInfo(" 2"); len(got) != 2 || got[0] != "" {
		t.Fatalf("leading empty segment must be kept, got %q", got)
	}
	// "".split(" ") is [""] in Java, not an empty array.
	if got := SplitExtraInfo(""); len(got) != 1 {
		t.Fatalf(`SplitExtraInfo("") = %v`, got)
	}
}

func TestExtraInfoShortCheckpointIsRejectedNotDefaulted(t *testing.T) {
	short := SplitExtraInfo("1 2")
	if _, err := GetQueueOffset(short); err == nil {
		t.Fatal("short checkpoint must not yield an offset")
	}
	if _, err := GetReviveQid(short); err == nil {
		t.Fatal("short checkpoint must not yield a reviveQid")
	}
	if _, err := GetBrokerName(short); err == nil {
		t.Fatal("short checkpoint must not yield a broker name")
	}
}

func TestExtraInfoParseBatchInfo(t *testing.T) {
	var sb strings.Builder
	BuildStartOffsetInfo(&sb, "TopicA", 0, 10)
	BuildStartOffsetInfo(&sb, "TopicA", 1, 20)
	starts, err := ParseStartOffsetInfo(sb.String())
	if err != nil {
		t.Fatalf("ParseStartOffsetInfo: %v", err)
	}
	// 多队列切片必须按 (retry, queueId) 分开，否则第一条会覆盖第二条
	if starts["0@0"] != 10 || starts["0@1"] != 20 {
		t.Fatalf("start offsets %v", starts)
	}

	var mb strings.Builder
	BuildMsgOffsetInfo(&mb, "TopicA", 0, []int64{10, 11, 12})
	offsets, err := ParseMsgOffsetInfo(mb.String())
	if err != nil {
		t.Fatalf("ParseMsgOffsetInfo: %v", err)
	}
	got := offsets["0@0"]
	if len(got) != 3 || got[0] != 10 || got[2] != 12 {
		t.Fatalf("msg offsets %v", got)
	}

	// single entry without ';'
	if starts, err := ParseStartOffsetInfo("0 3 7"); err != nil || starts["0@3"] != 7 {
		t.Fatalf("single-entry parse: %v %v", starts, err)
	}
	// duplicate key must be loud
	if _, err := ParseStartOffsetInfo("0 3 7;0 3 8"); err == nil {
		t.Fatal("duplicate key must be an error")
	}
	if v, err := ParseStartOffsetInfo(""); v != nil || err != nil {
		t.Fatalf("empty info must be nil,nil: %v %v", v, err)
	}
}

func TestExtraInfoMapKeys(t *testing.T) {
	if got := GetQueueOffsetKeyValueKey(2, 107); got != "qo2%107" {
		t.Fatalf("GetQueueOffsetKeyValueKey = %q", got)
	}
	if got := GetStartOffsetInfoMapKey("TopicA", 3); got != "0@3" {
		t.Fatalf("GetStartOffsetInfoMapKey = %q", got)
	}
	if got := GetQueueOffsetMapKey("TopicA", 2, 107); got != "0@qo2%107" {
		t.Fatalf("GetQueueOffsetMapKey = %q", got)
	}
	// The retry marker comes out of the checkpoint when there is one, so a
	// retried message keys into the same slot the broker produced.
	ck := BuildExtraInfoWithMsgQueueOffset(0, 1, 60000, 0, "%RETRY%G_T", "b", 0, 1)
	if got := GetStartOffsetInfoMapKeyWithTopic("TopicA", ck, 3); got != "1@3" {
		t.Fatalf("GetStartOffsetInfoMapKeyWithTopic = %q", got)
	}
	if got := RetryOfPopCk("", "%RETRY%G_T"); got != "1" {
		t.Fatalf("RetryOfPopCk fallback = %q", got)
	}
}

func TestPopAckConstants(t *testing.T) {
	if ReviveGroup != "CID_RMQ_SYS_REVIVE_GROUP" {
		t.Fatalf("ReviveGroup = %q", ReviveGroup)
	}
	if ReviveTopic != "rmq_sys_REVIVE_LOG_" {
		t.Fatalf("ReviveTopic = %q", ReviveTopic)
	}
	if got := BuildClusterReviveTopic("DefaultCluster"); got != "rmq_sys_REVIVE_LOG_DefaultCluster" {
		t.Fatalf("BuildClusterReviveTopic = %q", got)
	}
	if !IsStartWithRevivePrefix("rmq_sys_REVIVE_LOG_DefaultCluster") ||
		IsStartWithRevivePrefix("TopicA") {
		t.Fatal("IsStartWithRevivePrefix")
	}
	if PopBatchAckTag != "bAck" || PopAckTag != "ack" || PopCkTag != "ck" {
		t.Fatal("ack tag literals")
	}
}
