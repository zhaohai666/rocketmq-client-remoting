// Validators 实现（对应 Java org.apache.rocketmq.client.Validators
// 与 Python client/validators.py，文案与判定顺序以后者为准）。
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
    // Java 的第一分支 `null == msg` 在 C++ 没有对应物：Message 是值类型，
    // 调用方（producer.cpp 各发送入口）拿到的永远是实体而不是 null，故此分支跳过。

    // topic：先 checkTopic，再挡禁发 topic（顺序照抄 Java/Python）
    checkTopic(msg.topic);
    isNotAllowedSendTopic(msg.topic);

    // body：Java 判 null / 零长 / 超限；C++ 用 hasBody 标志位表达"null body"
    // （Bytes 即 std::string，本身无 null 态，见 message.h 的 hasBody 注释）。
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

    // 多队列分发（LMQ）的路径里带文件系统分隔符会让 broker 侧建队列时拼出越界路径
    const std::string lmqPath = msg.getUserProperty(MessageConst::PROPERTY_INNER_MULTI_DISPATCH);
    if (!lmqPath.empty() && lmqPath.find(kFileSeparator) != std::string::npos) {
        throw MQClientException(std::string("INNER_MULTI_DISPATCH ") + lmqPath +
                                    " can not contains " + kFileSeparator + " character",
                                ResponseCode::MESSAGE_ILLEGAL);
    }
}

}  // namespace rocketmq
