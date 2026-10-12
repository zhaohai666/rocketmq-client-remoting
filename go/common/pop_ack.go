package common

import "strings"

// POP / acknowledgement constants and the pop retry-topic naming rules —
// the broker stores and derives retry topics with exactly these names.
//
// Getting the retry topic wrong is silent: the client keeps popping the main
// topic, the broker never sees a retry, and "it works" until a message is
// supposed to come back.

const (
	// PopRetryQueueNum: the broker sizes %RETRY%<group>_<topic> with this
	// queue count.
	PopRetryQueueNum = 1

	// PopAckSeconds: ms per second, used by the ack timing math below.
	PopAckSeconds int64 = 1000

	// PopRetrySeparatorV1 / V2: the two on-wire retry-topic separators.
	PopRetrySeparatorV1 = '_'
	PopRetrySeparatorV2 = '+'

	// PopOrderReviveQueue: an orderly POP carries this reviveQid in the
	// checkpoint, and it is how the extra-info parser recognises an order-mode
	// POP.
	PopOrderReviveQueue = 999
)

// Revive-topic / ack-tag constants. REVIVE_TOPIC is a
// *prefix*: the real topic is REVIVE_TOPIC + clusterName.
const (
	ReviveGroup      = SystemConsumerGroupPrefix + "REVIVE_GROUP"
	ReviveTopic      = SystemTopicPrefix + "REVIVE_LOG_"
	PopCkTag         = "ck"
	PopAckTag        = "ack"
	PopBatchAckTag   = "bAck"
	PopAckSplit      = "@"
	PopAckLocalHost  = "127.0.0.1"
	PopAckLockTime   = 5000
	PopAckTimeSecond = 1000
)

// BuildPopRetryTopicV1: `%RETRY%<group>_<topic>`.
//
// Note the argument order — (topic, consumerGroup) — which is the reverse of
// how the resulting string reads; every builder here keeps it that way.
func BuildPopRetryTopicV1(topic, consumerGroup string) string {
	return RetryGroupTopicPrefix + consumerGroup + string(PopRetrySeparatorV1) + topic
}

// BuildPopRetryTopicV2: `%RETRY%<group>+<topic>`.
func BuildPopRetryTopicV2(topic, consumerGroup string) string {
	return RetryGroupTopicPrefix + consumerGroup + string(PopRetrySeparatorV2) + topic
}

// IsPopRetryTopicV2: a %RETRY% topic that
// contains '+'. Deliberately NOT "starts with %RETRY%<group>+" — the no-group
// form is the real test, and the extra group check would reject retry topics
// the broker built for a namespaced group.
func IsPopRetryTopicV2(retryTopic string) bool {
	return strings.HasPrefix(retryTopic, RetryGroupTopicPrefix) &&
		strings.ContainsRune(retryTopic, PopRetrySeparatorV2)
}

// IsPopRetryTopicV1: a
// `%RETRY%` topic that is not V2 (i.e. carries the '_' separator).
func IsPopRetryTopicV1(retryTopic string) bool {
	return strings.HasPrefix(retryTopic, RetryGroupTopicPrefix) && !IsPopRetryTopicV2(retryTopic)
}

// BuildClusterReviveTopic appends the cluster name to the REVIVE_TOPIC prefix.
func BuildClusterReviveTopic(clusterName string) string {
	return ReviveTopic + clusterName
}

// IsStartWithRevivePrefix tests the REVIVE_TOPIC prefix.
func IsStartWithRevivePrefix(topicName string) bool {
	return strings.HasPrefix(topicName, ReviveTopic)
}
