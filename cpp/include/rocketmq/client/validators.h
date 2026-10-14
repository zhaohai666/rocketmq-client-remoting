// **Why the client rejects up front**: an invalid topic/group name is also rejected by the broker, but only
// after the request actually goes out, as TOPIC_NOT_EXIST / ILLEGAL_TOPIC, and TOPIC_NOT_EXIST is in the
// retryable code set of send retries — so every message bound to fail burns through the retry count and the
// timeout budget, only to report the same cause. Local validation makes such input fail on the first line of send().
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

    // Checks in order: blank → length → allowed character set.
    static void checkGroup(const std::string& group);

    // Checks in order: blank → length → allowed character set.
    static void checkTopic(const std::string& topic);

    // Throws when the topic is a system topic or not allowed for sending; returns normally otherwise.
    static void isSystemTopic(const std::string& topic);
    static void isNotAllowedSendTopic(const std::string& topic);

    static void checkMessage(const Message& msg, int32_t maxMessageSize);
};

}  // namespace rocketmq

#endif  // ROCKETMQ_CLIENT_VALIDATORS_H
