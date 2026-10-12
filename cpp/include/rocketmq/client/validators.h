// **为什么要在客户端就拦下来**：topic/group 名字非法时 broker 也会拒，但要等到请求
// 真的打出去才拿到 TOPIC_NOT_EXIST / ILLEGAL_TOPIC，而 TOPIC_NOT_EXIST 在发送重试的
// 可重试码集合里——于是每条必然失败的消息都会把重试次数和超时预算空转一遍，最后报的
// 还是同一个原因。本地校验让这类输入在 send() 的第一行就失败。
#ifndef ROCKETMQ_CLIENT_VALIDATORS_H
#define ROCKETMQ_CLIENT_VALIDATORS_H

#include <cstdint>
#include <string>

#include "rocketmq/common/message.h"

namespace rocketmq {

#ifdef _WIN32
inline constexpr const char* kFileSeparator = "\\";
#else
inline constexpr const char* kFileSeparator = "/";
#endif

struct Validators {
    static constexpr int32_t CHARACTER_MAX_LENGTH = 255;
    static constexpr int32_t TOPIC_MAX_LENGTH = 127;
    static constexpr int32_t GROUP_MAX_LENGTH = 120;

    // 对应 Validators.checkGroup：blank → 长度 → 字符表。
    static void checkGroup(const std::string& group);

    // 对应 Validators.checkTopic：blank → 长度 → 字符表。
    static void checkTopic(const std::string& topic);

    // 对应 Validators.isSystemTopic / isNotAllowedSendTopic：命中即抛，正常返回。
    static void isSystemTopic(const std::string& topic);
    static void isNotAllowedSendTopic(const std::string& topic);

    static void checkMessage(const Message& msg, int32_t maxMessageSize);
};

}  // namespace rocketmq

#endif  // ROCKETMQ_CLIENT_VALIDATORS_H
