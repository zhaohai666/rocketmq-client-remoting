package common

import (
	"fmt"
	"unicode/utf8"
)

// Name validation on the send / subscribe entry points (Java
// org.apache.rocketmq.client.Validators). Texts and decision order follow
// python/rocketmq/client/validators.py, which Rust and C# mirror.
//
// WHY validate locally at all: an illegal topic (or group) is rejected by the
// broker too, but only after the request is on the wire — and the reply is
// TOPIC_NOT_EXIST, which sits in the send retry code set. So every doomed
// message burns the full retry budget and a timeout before reporting the same
// reason. Local validation fails on the first line of send().
//
// Code discipline copied from Java, do not "tidy" it up:
//   - CheckTopic / CheckGroup / IsSystemTopic / IsNotAllowedSendTopic all map
//     to Java's MQClientException(String, null), i.e. responseCode -1 ("pure
//     client error", not a broker code). In this port that is ClientError
//     (HasCode == false), matching Python's default and Rust's
//     Error::client.
//   - ONLY CheckMessage's message/body/LMQ branches carry
//     MESSAGE_ILLEGAL(13). Hence "send into SCHEDULE_TOPIC_XXXX" reports a
//     codeless error rather than 13 — awkward, but top-level code that
//     branches on the response code has to know it, and all five ports agree.

// ResponseCodeMessageIllegal mirrors Java ResponseCode.MESSAGE_ILLEGAL. It is
// declared here instead of reusing remoting.RespMessageIllegal because the
// remoting package imports common; the reverse import would be a cycle.
// client_test asserts the two stay equal (see TestValidatorCodesMatchRemoting).
const ResponseCodeMessageIllegal int32 = 13

// CharacterMaxLength mirrors Java Validators.CHARACTER_MAX_LENGTH and already
// lives in message_const.go (constant kept so this file diffs against Java).

// FsSeparator is Java's File.separator / Python's os.sep: LMQ paths are
// filesystem paths, so the illegal character depends on the platform.
const FsSeparator = '/'

// CheckGroup mirrors Validators.checkGroup: blank -> length (120) -> character
// table, in Java's order.
//
// The length step counts Unicode code points, the same unit as Python's
// len(str) and Rust's chars().count() (Java counts UTF-16 units). The three
// only disagree for non-BMP names, and those die on the character step that
// follows anyway.
func CheckGroup(group string) error {
	if IsBlank(group) {
		return ClientError("the specified group is blank")
	}
	if utf8.RuneCountInString(group) > GroupMaxLength {
		return ClientError(fmt.Sprintf(
			"the specified group[%s] is longer than group max length: %d.", group, GroupMaxLength))
	}
	if IsTopicOrGroupIllegal(group) {
		return ClientError(fmt.Sprintf(
			"the specified group[%s] contains illegal characters, allowing only %s", group, ValidCharPattern))
	}
	return nil
}

// CheckTopic mirrors Validators.checkTopic: blank -> length (127) -> character
// table.
func CheckTopic(topic string) error {
	if IsBlank(topic) {
		return ClientError("The specified topic is blank")
	}
	if utf8.RuneCountInString(topic) > TopicMaxLength {
		return ClientError(fmt.Sprintf(
			"The specified topic is longer than topic max length %d.", TopicMaxLength))
	}
	if IsTopicOrGroupIllegal(topic) {
		return ClientError(fmt.Sprintf(
			"The specified topic[%s] contains illegal characters, allowing only %s", topic, ValidCharPattern))
	}
	return nil
}

// ValidateSystemTopic mirrors Validators.isSystemTopic: squatting on a broker
// internal topic silently rewrites its bookkeeping, so it is refused rather
// than allowed to fail later.
func ValidateSystemTopic(topic string) error {
	if IsSystemTopic(topic) {
		return ClientError(fmt.Sprintf("The topic[%s] is conflict with system topic.", topic))
	}
	return nil
}

// ValidateNotAllowedSendTopic mirrors Validators.isNotAllowedSendTopic.
func ValidateNotAllowedSendTopic(topic string) error {
	if IsNotAllowedSendTopic(topic) {
		return ClientError(fmt.Sprintf("Sending message to topic[%s] is forbidden.", topic))
	}
	return nil
}

// CheckMessage mirrors Validators.checkMessage(msg, producer). Java reads
// maxMessageSize off the producer; the numeric value is passed in so this
// package need not know about producer types.
//
// Java's leading `null == msg` branch has no counterpart: callers hold a
// *Message, never a nil reference to a valid message — but a nil pointer IS
// possible here, and Java's MESSAGE_ILLEGAL is preserved for it so the
// behaviour stays debuggable.
//
// Order is Java's and matters: topic, then forbidden-topic, then body. An
// illegal topic with an empty body must report the topic problem.
func CheckMessage(msg *Message, maxMessageSize int) error {
	if msg == nil {
		return ClientErrorCode(ResponseCodeMessageIllegal, "the message is null")
	}
	if err := CheckTopic(msg.Topic); err != nil {
		return err
	}
	if err := ValidateNotAllowedSendTopic(msg.Topic); err != nil {
		return err
	}
	bodyIllegal := func(text string) error {
		return ClientErrorCode(ResponseCodeMessageIllegal, text)
	}
	if msg.Body == nil {
		return bodyIllegal("the message body is null")
	}
	if len(msg.Body) == 0 {
		return bodyIllegal("the message body length is zero")
	}
	if len(msg.Body) > maxMessageSize {
		return bodyIllegal(fmt.Sprintf("the message body size over max value, MAX: %d", maxMessageSize))
	}
	// A filesystem separator inside an LMQ path makes the broker build an
	// out-of-tree queue path when it creates the queue.
	if lmqPath, ok := msg.GetProperty(PropertyInnerMultiDispatch); ok {
		if lmqPath != "" && containsRune(lmqPath, FsSeparator) {
			return bodyIllegal(fmt.Sprintf(
				"INNER_MULTI_DISPATCH %s can not contains %c character", lmqPath, FsSeparator))
		}
	}
	return nil
}

func containsRune(s string, r rune) bool {
	for _, c := range s {
		if c == r {
			return true
		}
	}
	return false
}
