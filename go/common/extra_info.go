package common

import (
	"fmt"
	"strconv"
	"strings"
)

// ExtraInfoUtil — a faithful port of
// org.apache.rocketmq.remoting.protocol.header.ExtraInfoUtil.
//
// This is the POP checkpoint ("POP_CK"). The broker hands the client a
// checkpoint string in the POP response headers; the client rebuilds it per
// message and sends it back verbatim on ACK / CHANGE_INVISIBLETIME. Any
// disagreement about segment count or ordering makes every ACK a no-op: the
// broker cannot find the checkpoint, the message stays invisible, and it is
// redelivered after popInvisibleTime. That is the failure mode this file
// exists to prevent.
//
// Segment layout (MessageConst.KEY_SEPARATOR, a single space):
//
//	0 ckQueueOffset   queue offset of the first message of the pop batch
//	1 popTime         broker clock at pop time
//	2 invisibleTime   how long the batch stays invisible
//	3 reviveQid       revive queue id (999 == orderly pop)
//	4 t               retry marker: "0" normal, "1" %RETRY%, "2" %RETRY% V2
//	5 brokerName
//	6 queueId
//	7 msgQueueOffset  THIS message's offset inside the queue  <-- ack uses this
//
// Segment 7 is the one people get wrong: the ACK offset is msgQueueOffset, not
// ckQueueOffset (segment 0) and not the consume-queue offset. The 7-argument
// buildExtraInfo overload stops at segment 6; only the 8-argument overload
// produces a usable ACK checkpoint.
const (
	// ExtraNormalTopic / ExtraRetryTopic / ExtraRetryTopicV2 are the segment-4
	// markers.
	ExtraNormalTopic   = "0"
	ExtraRetryTopic    = "1"
	ExtraRetryTopicV2  = "2"
	extraQueueOffsetID = "qo"

	// ExtraInfoSegmentCount is the number of segments a usable ACK checkpoint
	// has. Anything shorter cannot address a message.
	ExtraInfoSegmentCount = 8
)

// SplitExtraInfo mirrors Java ExtraInfoUtil.split(String).
//
// Faithful to java.lang.String#split(regex) as used there: a single-space
// separator, trailing empty segments dropped, leading/interior empties kept.
// (Go's strings.Split keeps trailing empties too, which would turn
// "a b " into 3 segments where Java yields 2 — that difference matters for the
// length guards below, so split it by hand.)
func SplitExtraInfo(extraInfo string) []string {
	parts := strings.Split(extraInfo, KeySeparator)
	// Java drops trailing empty strings. "".split(" ") is [""] in Java, not [],
	// so a wholly empty input keeps its single empty segment.
	for len(parts) > 1 && parts[len(parts)-1] == "" {
		parts = parts[:len(parts)-1]
	}
	return parts
}

// extraSegment fetches segment index, mirroring the Java length guards: every
// accessor independently requires the array to be long enough and throws
// IllegalArgumentException otherwise. Never silently default — a short
// checkpoint means the caller is acking something it cannot address.
func extraSegment(extraInfoStrs []string, index int, what string) (string, error) {
	if len(extraInfoStrs) <= index {
		return "", fmt.Errorf("get%s fail, extraInfoStrs length %d", what, len(extraInfoStrs))
	}
	return extraInfoStrs[index], nil
}

// GetCkQueueOffset mirrors ExtraInfoUtil.getCkQueueOffset (segment 0).
func GetCkQueueOffset(extraInfoStrs []string) (int64, error) {
	raw, err := extraSegment(extraInfoStrs, 0, "CkQueueOffset")
	if err != nil {
		return 0, err
	}
	v, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("getCkQueueOffset fail, %q is not a long", raw)
	}
	return v, nil
}

// GetPopTime mirrors ExtraInfoUtil.getPopTime (segment 1).
func GetPopTime(extraInfoStrs []string) (int64, error) {
	raw, err := extraSegment(extraInfoStrs, 1, "PopTime")
	if err != nil {
		return 0, err
	}
	v, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("getPopTime fail, %q is not a long", raw)
	}
	return v, nil
}

// GetInvisibleTime mirrors ExtraInfoUtil.getInvisibleTime (segment 2).
func GetInvisibleTime(extraInfoStrs []string) (int64, error) {
	raw, err := extraSegment(extraInfoStrs, 2, "InvisibleTime")
	if err != nil {
		return 0, err
	}
	v, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("getInvisibleTime fail, %q is not a long", raw)
	}
	return v, nil
}

// GetReviveQid mirrors ExtraInfoUtil.getReviveQid (segment 3).
func GetReviveQid(extraInfoStrs []string) (int, error) {
	raw, err := extraSegment(extraInfoStrs, 3, "ReviveQid")
	if err != nil {
		return 0, err
	}
	v, err := strconv.ParseInt(raw, 10, 32)
	if err != nil {
		return 0, fmt.Errorf("getReviveQid fail, %q is not an int", raw)
	}
	return int(v), nil
}

// GetRetry mirrors ExtraInfoUtil.getRetry (segment 4).
func GetRetry(extraInfoStrs []string) (string, error) {
	return extraSegment(extraInfoStrs, 4, "Retry")
}

// GetBrokerName mirrors ExtraInfoUtil.getBrokerName (segment 5).
func GetBrokerName(extraInfoStrs []string) (string, error) {
	return extraSegment(extraInfoStrs, 5, "BrokerName")
}

// GetQueueId mirrors ExtraInfoUtil.getQueueId (segment 6).
func GetQueueId(extraInfoStrs []string) (int, error) {
	raw, err := extraSegment(extraInfoStrs, 6, "QueueId")
	if err != nil {
		return 0, err
	}
	v, err := strconv.ParseInt(raw, 10, 32)
	if err != nil {
		return 0, fmt.Errorf("getQueueId fail, %q is not an int", raw)
	}
	return int(v), nil
}

// GetQueueOffset mirrors ExtraInfoUtil.getQueueOffset (segment 7) — the offset
// an ACK must carry. Requires the full 8-segment checkpoint.
func GetQueueOffset(extraInfoStrs []string) (int64, error) {
	raw, err := extraSegment(extraInfoStrs, 7, "QueueOffset")
	if err != nil {
		return 0, err
	}
	v, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("getQueueOffset fail, %q is not a long", raw)
	}
	return v, nil
}

// GetRealTopic mirrors ExtraInfoUtil.getRealTopic(extraInfoStrs, topic, cid):
// resolve segment 4 back to the physical topic. "1" -> V1 retry topic,
// "2" -> V2 retry topic, anything else -> the topic as handed in.
func GetRealTopic(extraInfoStrs []string, topic, consumerGroup string) string {
	retry, err := GetRetry(extraInfoStrs)
	if err != nil {
		return topic
	}
	switch retry {
	case ExtraRetryTopic:
		return BuildPopRetryTopicV1(topic, consumerGroup)
	case ExtraRetryTopicV2:
		return BuildPopRetryTopicV2(topic, consumerGroup)
	}
	return topic
}

// GetRealTopicByRetry mirrors ExtraInfoUtil.getRealTopic(topic, cid, retry).
// Unlike the array form this one rejects an unknown marker, because it is the
// broker-side direction where a bad marker means a corrupt request.
func GetRealTopicByRetry(topic, consumerGroup, retry string) (string, error) {
	switch retry {
	case ExtraNormalTopic:
		return topic, nil
	case ExtraRetryTopic:
		return BuildPopRetryTopicV1(topic, consumerGroup), nil
	case ExtraRetryTopicV2:
		return BuildPopRetryTopicV2(topic, consumerGroup), nil
	}
	return "", fmt.Errorf("getRetry fail, format is wrong (retry=%q)", retry)
}

// RetryOfTopic mirrors the private ExtraInfoUtil.getRetry(topic): the segment-4
// marker that a topic would produce.
func RetryOfTopic(topic string) string {
	if IsPopRetryTopicV2(topic) {
		return ExtraRetryTopicV2
	}
	if strings.HasPrefix(topic, RetryGroupTopicPrefix) {
		return ExtraRetryTopic
	}
	return ExtraNormalTopic
}

// BuildExtraInfo mirrors the 7-argument ExtraInfoUtil.buildExtraInfo. The
// result has no msgQueueOffset segment and is therefore NOT ackable — Java uses
// it as the checkpoint *prefix* only.
func BuildExtraInfo(ckQueueOffset, popTime, invisibleTime int64, reviveQid int,
	topic, brokerName string, queueID int) string {
	return strings.Join([]string{
		strconv.FormatInt(ckQueueOffset, 10),
		strconv.FormatInt(popTime, 10),
		strconv.FormatInt(invisibleTime, 10),
		strconv.Itoa(reviveQid),
		RetryOfTopic(topic),
		brokerName,
		strconv.Itoa(queueID),
	}, KeySeparator)
}

// BuildExtraInfoWithMsgQueueOffset mirrors the 8-argument
// ExtraInfoUtil.buildExtraInfo: the ackable form. msgQueueOffset is this
// message's own queue offset (segment 7), which is exactly what the ACK
// request must carry as its offset field.
func BuildExtraInfoWithMsgQueueOffset(ckQueueOffset, popTime, invisibleTime int64, reviveQid int,
	topic, brokerName string, queueID int, msgQueueOffset int64) string {
	return BuildExtraInfo(ckQueueOffset, popTime, invisibleTime, reviveQid, topic, brokerName, queueID) +
		KeySeparator + strconv.FormatInt(msgQueueOffset, 10)
}

// IsOrder mirrors ExtraInfoUtil.isOrder: an orderly POP is identified by the
// revive queue id, not by any explicit flag.
func IsOrder(extraInfo []string) bool {
	qid, err := GetReviveQid(extraInfo)
	return err == nil && qid == PopOrderReviveQueue
}

// BuildStartOffsetInfo mirrors ExtraInfoUtil.buildStartOffsetInfo; it appends to
// stringBuilder, inserting ';' between entries.
func BuildStartOffsetInfo(stringBuilder *strings.Builder, topic string, queueID int, startOffset int64) {
	if stringBuilder == nil {
		stringBuilder = &strings.Builder{}
	}
	if stringBuilder.Len() > 0 {
		stringBuilder.WriteByte(';')
	}
	stringBuilder.WriteString(RetryOfTopic(topic))
	stringBuilder.WriteString(KeySeparator)
	stringBuilder.WriteString(strconv.Itoa(queueID))
	stringBuilder.WriteString(KeySeparator)
	stringBuilder.WriteString(strconv.FormatInt(startOffset, 10))
}

// BuildMsgOffsetInfo mirrors ExtraInfoUtil.buildMsgOffsetInfo: the trailing
// list is comma-joined inside the third segment.
func BuildMsgOffsetInfo(stringBuilder *strings.Builder, topic string, queueID int, msgOffsets []int64) {
	if stringBuilder == nil {
		stringBuilder = &strings.Builder{}
	}
	if stringBuilder.Len() > 0 {
		stringBuilder.WriteByte(';')
	}
	stringBuilder.WriteString(RetryOfTopic(topic))
	stringBuilder.WriteString(KeySeparator)
	stringBuilder.WriteString(strconv.Itoa(queueID))
	stringBuilder.WriteString(KeySeparator)
	for i, offset := range msgOffsets {
		if i > 0 {
			stringBuilder.WriteByte(',')
		}
		stringBuilder.WriteString(strconv.FormatInt(offset, 10))
	}
}

// BuildQueueIdOrderCountInfo mirrors ExtraInfoUtil.buildQueueIdOrderCountInfo.
func BuildQueueIdOrderCountInfo(stringBuilder *strings.Builder, topic string, queueID, orderCount int) {
	if stringBuilder == nil {
		stringBuilder = &strings.Builder{}
	}
	if stringBuilder.Len() > 0 {
		stringBuilder.WriteByte(';')
	}
	stringBuilder.WriteString(RetryOfTopic(topic))
	stringBuilder.WriteString(KeySeparator)
	stringBuilder.WriteString(strconv.Itoa(queueID))
	stringBuilder.WriteString(KeySeparator)
	stringBuilder.WriteString(strconv.Itoa(orderCount))
}

// GetQueueOffsetKeyValueKey mirrors ExtraInfoUtil.getQueueOffsetKeyValueKey:
// "qo<queueId>%<queueOffset>".
func GetQueueOffsetKeyValueKey(queueID, queueOffset int64) string {
	return extraQueueOffsetID + strconv.FormatInt(queueID, 10) + "%" + strconv.FormatInt(queueOffset, 10)
}

// GetStartOffsetInfoMapKey mirrors ExtraInfoUtil.getStartOffsetInfoMapKey(
// topic, key): "<retry>@<key>". The two-argument overload resolves a topic into
// its retry marker first.
func GetStartOffsetInfoMapKey(topic string, key int64) string {
	return RetryOfTopic(topic) + "@" + strconv.FormatInt(key, 10)
}

// GetStartOffsetInfoMapKeyWithTopic mirrors the three-argument overload, which
// prefers the marker carried by a checkpoint over the topic's own.
func GetStartOffsetInfoMapKeyWithTopic(topic, popCk string, key int64) string {
	retry := RetryOfPopCk(popCk, topic)
	return retry + "@" + strconv.FormatInt(key, 10)
}

// GetQueueOffsetMapKey mirrors ExtraInfoUtil.getQueueOffsetMapKey:
// "<retry>@qo<queueId>%<queueOffset>".
func GetQueueOffsetMapKey(topic string, queueID, queueOffset int64) string {
	return RetryOfTopic(topic) + "@" + GetQueueOffsetKeyValueKey(queueID, queueOffset)
}

// RetryOfPopCk mirrors the private ExtraInfoUtil.getRetry(topic, popCk): take
// the marker out of the checkpoint when there is one, else out of the topic.
func RetryOfPopCk(popCk, topic string) string {
	if popCk != "" {
		if parts := SplitExtraInfo(popCk); len(parts) > 4 {
			return parts[4]
		}
	}
	return RetryOfTopic(topic)
}

// ParseMsgOffsetInfo mirrors ExtraInfoUtil.parseMsgOffsetInfo. A nil map means
// "no info"; duplicate keys are an error, as in Java.
func ParseMsgOffsetInfo(msgOffsetInfo string) (map[string][]int64, error) {
	if msgOffsetInfo == "" {
		return nil, nil
	}
	out := make(map[string][]int64, 4)
	for _, one := range splitInfoEntries(msgOffsetInfo) {
		split := strings.Split(one, KeySeparator)
		if len(split) != 3 {
			return nil, fmt.Errorf("parse msgOffsetMap error, %q", msgOffsetInfo)
		}
		key := split[0] + "@" + split[1]
		if _, dup := out[key]; dup {
			return nil, fmt.Errorf("parse msgOffsetMap error, duplicate, %q", msgOffsetInfo)
		}
		offsets := make([]int64, 0, 8)
		if split[2] != "" {
			for _, raw := range strings.Split(split[2], ",") {
				v, err := strconv.ParseInt(raw, 10, 64)
				if err != nil {
					return nil, fmt.Errorf("parse msgOffsetMap error, %q is not a long", raw)
				}
				offsets = append(offsets, v)
			}
		}
		out[key] = offsets
	}
	return out, nil
}

// ParseStartOffsetInfo mirrors ExtraInfoUtil.parseStartOffsetInfo.
func ParseStartOffsetInfo(startOffsetInfo string) (map[string]int64, error) {
	if startOffsetInfo == "" {
		return nil, nil
	}
	out := make(map[string]int64, 4)
	for _, one := range splitInfoEntries(startOffsetInfo) {
		split := strings.Split(one, KeySeparator)
		if len(split) != 3 {
			return nil, fmt.Errorf("parse startOffsetInfo error, %q", startOffsetInfo)
		}
		key := split[0] + "@" + split[1]
		if _, dup := out[key]; dup {
			return nil, fmt.Errorf("parse startOffsetInfo error, duplicate, %q", startOffsetInfo)
		}
		v, err := strconv.ParseInt(split[2], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("parse startOffsetInfo error, %q is not a long", split[2])
		}
		out[key] = v
	}
	return out, nil
}

// ParseOrderCountInfo mirrors ExtraInfoUtil.parseOrderCountInfo.
func ParseOrderCountInfo(orderCountInfo string) (map[string]int, error) {
	if orderCountInfo == "" {
		return nil, nil
	}
	out := make(map[string]int, 4)
	for _, one := range splitInfoEntries(orderCountInfo) {
		split := strings.Split(one, KeySeparator)
		if len(split) != 3 {
			return nil, fmt.Errorf("parse orderCountInfo error, %q", orderCountInfo)
		}
		key := split[0] + "@" + split[1]
		if _, dup := out[key]; dup {
			return nil, fmt.Errorf("parse orderCountInfo error, duplicate, %q", orderCountInfo)
		}
		v, err := strconv.Atoi(split[2])
		if err != nil {
			return nil, fmt.Errorf("parse orderCountInfo error, %q is not an int", split[2])
		}
		out[key] = v
	}
	return out, nil
}

// splitInfoEntries mirrors the Java "split on ';' unless there is none" dance:
// a payload with no ';' is one entry, and Java would otherwise return a
// single-element array anyway.
func splitInfoEntries(info string) []string {
	if !strings.Contains(info, ";") {
		return []string{info}
	}
	return strings.Split(info, ";")
}
