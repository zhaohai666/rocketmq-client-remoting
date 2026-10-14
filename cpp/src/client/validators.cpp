// Validators implementation: the validation error texts and the check order are a contract matched by callers and documentation, so they stay stable.
#include "rocketmq/client/validators.h"

#include <string>

#include "rocketmq/client/exception.h"
#include "rocketmq/common/message_const.h"
#include "rocketmq/common/topic_validator.h"
#include "rocketmq/common/util_all.h"
#include "rocketmq/remoting/protocol/codes.h"

namespace rocketmq {

void Validators::checkGroup(const std::string& group) {
    if (UtilAll::isBlank(group)) {
        throw MQClientException("the specified group is blank");
    }
    if (static_cast<int32_t>(group.size()) > TopicValidator::GROUP_MAX_LENGTH) {
        throw MQClientException("the specified group[" + group +
                                "] is longer than group max length: " +
                                std::to_string(TopicValidator::GROUP_MAX_LENGTH) + ".");
    }
    if (TopicValidator::isTopicOrGroupIllegal(group)) {
        throw MQClientException("the specified group[" + group +
                                "] contains illegal characters, allowing only " +
                                TopicValidator::VALID_CHAR_PATTERN);
    }
}

void Validators::checkTopic(const std::string& topic) {
    if (UtilAll::isBlank(topic)) {
        throw MQClientException("The specified topic is blank");
    }
    if (static_cast<int32_t>(topic.size()) > TopicValidator::TOPIC_MAX_LENGTH) {
        throw MQClientException("The specified topic is longer than topic max length " +
                                std::to_string(TopicValidator::TOPIC_MAX_LENGTH) + ".");
    }
    if (TopicValidator::isTopicOrGroupIllegal(topic)) {
        throw MQClientException("The specified topic[" + topic +
                                "] contains illegal characters, allowing only " +
                                TopicValidator::VALID_CHAR_PATTERN);
    }
}

void Validators::isSystemTopic(const std::string& topic) {
    if (TopicValidator::isSystemTopic(topic)) {
        throw MQClientException("The topic[" + topic + "] is conflict with system topic.");
    }
}

void Validators::isNotAllowedSendTopic(const std::string& topic) {
    if (TopicValidator::isNotAllowedSendTopic(topic)) {
        throw MQClientException("Sending message to topic[" + topic + "] is forbidden.");
    }
}

void Validators::checkMessage(const Message& msg, int32_t maxMessageSize) {
    // The first branch, "msg is null", has no C++ counterpart: Message is a value type,
    // so callers (the producer.cpp send entry points) always receive an object, never null, and this branch is skipped.

    // topic: checkTopic first, then block publishing to forbidden topics
    checkTopic(msg.topic);
    isNotAllowedSendTopic(msg.topic);

    // body: three checks -- null / zero length / over limit; C++ expresses "null body" with the hasBody flag
    // (Bytes is std::string and has no null state; see the hasBody comment in message.h).
    if (!msg.hasBody) {
        throw MQClientException("the message body is null", ResponseCode::MESSAGE_ILLEGAL);
    }
    if (msg.body.empty()) {
        throw MQClientException("the message body length is zero", ResponseCode::MESSAGE_ILLEGAL);
    }
    if (static_cast<int32_t>(msg.body.size()) > maxMessageSize) {
        throw MQClientException("the message body size over max value, MAX: " +
                                    std::to_string(maxMessageSize),
                                ResponseCode::MESSAGE_ILLEGAL);
    }

    // A filesystem separator in a multi-queue dispatch (LMQ) path would make the broker build an out-of-bounds queue path
    const std::string lmqPath = msg.getUserProperty(MessageConst::PROPERTY_INNER_MULTI_DISPATCH);
    if (!lmqPath.empty() && lmqPath.find(kFileSeparator) != std::string::npos) {
        throw MQClientException(std::string("INNER_MULTI_DISPATCH ") + lmqPath +
                                    " can not contains " + kFileSeparator + " character",
                                ResponseCode::MESSAGE_ILLEGAL);
    }
}

}  // namespace rocketmq
