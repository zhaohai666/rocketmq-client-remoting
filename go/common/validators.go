package common

import (
	"fmt"
	"unicode/utf8"
)

// Name validation on the send / subscribe entry points.
//
// WHY validate locally at all: an illegal topic (or group) is rejected by the
// broker too, but only after the request is on the wire — and the reply is
// TOPIC_NOT_EXIST, which sits in the send retry code set. So every doomed
// message burns the full retry budget and a timeout before reporting the same
// reason. Local validation fails on the first line of send().
//
// Error-code discipline, do not "tidy" it up:
//   - CheckTopic / CheckGroup / IsSystemTopic / IsNotAllowedSendTopic all map
//     to a pure client error: not a broker code (the historical wire value -1
//     never travels). In this port that is ClientError (HasCode == false).
//   - ONLY CheckMessage's message/body/LMQ branches carry
//     MESSAGE_ILLEGAL(13). Hence "send into SCHEDULE_TOPIC_XXXX" reports a
//     codeless error rather than 13 — awkward, but top-level code that
//     branches on the response code has to know it.

// ResponseCodeMessageIllegal is the protocol's MESSAGE_ILLEGAL (13). It is
// declared here instead of reusing remoting.RespMessageIllegal because the
// remoting package imports common; the reverse import would be a cycle.
// client_test asserts the two stay equal (see TestValidatorCodesMatchRemoting).
const ResponseCodeMessageIllegal int32 = 13

// FsSeparator is the filesystem path separator: LMQ paths are filesystem
// paths, so the illegal character depends on the platform.
const FsSeparator = '/'

// CheckGroup validates a consumer/producer group: blank -> length (120) ->
// character table, in that order.
//
// The length step counts Unicode code points; counting UTF-16 units instead
// would only matter for non-BMP names, and those die on the character step
// that follows anyway.
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

// CheckTopic validates a topic: blank -> length (127) -> character table.
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

// ValidateSystemTopic: squatting on a broker internal topic silently rewrites
// its bookkeeping, so it is refused rather than allowed to fail later.
func ValidateSystemTopic(topic string) error {
	if IsSystemTopic(topic) {
		return ClientError(fmt.Sprintf("The topic[%s] is conflict with system topic.", topic))
	}
	return nil
}

// ValidateNotAllowedSendTopic reports whether sending to the topic is forbidden.
func ValidateNotAllowedSendTopic(topic string) error {
	if IsNotAllowedSendTopic(topic) {
		return ClientError(fmt.Sprintf("Sending message to topic[%s] is forbidden.", topic))
	}
	return nil
}

// CheckMessage validates a message before send. maxMessageSize is passed in
// rather than read off a producer so this package need not know about
// producer types.
//
// A nil *Message is possible here and reports MESSAGE_ILLEGAL(13) so the
// behaviour stays debuggable.
//
// Order matters: topic, then forbidden-topic, then body. An illegal topic
// with an empty body must report the topic problem.
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
