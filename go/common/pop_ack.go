package common

import "strings"

// POP / acknowledgement constants and the pop retry-topic naming rules.
// Mirrors Java org.apache.rocketmq.common.PopAckConstants and the
// buildPopRetryTopic* family in org.apache.rocketmq.common.KeyBuilder.
//
// Getting the retry topic wrong is silent: the client keeps popping the main
// topic, the broker never sees a retry, and "it works" until a message is
// supposed to come back.

const (
	// PopRetryQueueNum mirrors PopAckConstants.retryQueueNum (the broker sizes
	// %RETRY%<group>_<topic> with it).
	PopRetryQueueNum = 1

	// PopAckSeconds mirrors PopAckConstants.SECOND.
	PopAckSeconds int64 = 1000

	// PopRetrySeparatorV1 / V2 mirror KeyBuilder.POP_RETRY_SEPARATOR_V1/V2.
	PopRetrySeparatorV1 = '_'
	PopRetrySeparatorV2 = '+'

	// PopOrderReviveQueue mirrors KeyBuilder.POP_ORDER_REVIVE_QUEUE: an orderly
	// POP carries this reviveQid in the checkpoint, and is how ExtraInfoUtil
	// recognises an order-mode POP.
	PopOrderReviveQueue = 999
)

// Revive-topic / ack-tag constants (PopAckConstants). REVIVE_TOPIC is a
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

// BuildPopRetryTopicV1 mirrors KeyBuilder.buildPopRetryTopicV1:
// `%RETRY%<group>_<topic>`.
//
// Note the argument order — (topic, consumerGroup) — which is the reverse of
// how the resulting string reads. Java keeps it that way in every overload.
func BuildPopRetryTopicV1(topic, consumerGroup string) string {
	return RetryGroupTopicPrefix + consumerGroup + string(PopRetrySeparatorV1) + topic
}

// BuildPopRetryTopicV2 mirrors KeyBuilder.buildPopRetryTopicV2:
// `%RETRY%<group>+<topic>`.
func BuildPopRetryTopicV2(topic, consumerGroup string) string {
	return RetryGroupTopicPrefix + consumerGroup + string(PopRetrySeparatorV2) + topic
}

// IsPopRetryTopicV2 mirrors KeyBuilder.isPopRetryTopicV2: a %RETRY% topic that
// contains '+'. Deliberately NOT "starts with %RETRY%<group>+" — the no-group
// form is what Java checks, and the extra group check would reject retry topics
// the broker built for a namespaced group.
func IsPopRetryTopicV2(retryTopic string) bool {
	return strings.HasPrefix(retryTopic, RetryGroupTopicPrefix) &&
		strings.ContainsRune(retryTopic, PopRetrySeparatorV2)
}

// IsPopRetryTopicV1 mirrors the negation used by KeyBuilder.getRealTopic: a
// `%RETRY%` topic that is not V2 (i.e. carries the '_' separator).
func IsPopRetryTopicV1(retryTopic string) bool {
	return strings.HasPrefix(retryTopic, RetryGroupTopicPrefix) && !IsPopRetryTopicV2(retryTopic)
}

// BuildClusterReviveTopic mirrors PopAckConstants.buildClusterReviveTopic.
func BuildClusterReviveTopic(clusterName string) string {
	return ReviveTopic + clusterName
}

// IsStartWithRevivePrefix mirrors PopAckConstants.isStartWithRevivePrefix.
func IsStartWithRevivePrefix(topicName string) bool {
	return strings.HasPrefix(topicName, ReviveTopic)
}
