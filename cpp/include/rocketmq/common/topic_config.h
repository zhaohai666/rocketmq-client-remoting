#ifndef ROCKETMQ_COMMON_TOPIC_CONFIG_H
#define ROCKETMQ_COMMON_TOPIC_CONFIG_H

#include <cstdint>
#include <string>

#include "rocketmq/common/byte_buffer.h"
#include "rocketmq/common/types.h"
#include "rocketmq/remoting/protocol/json.h"

namespace rocketmq {

struct TopicFilterType {
    static constexpr const char* SINGLE_TAG = "SINGLE_TAG";
    static constexpr const char* MULTI_TAG = "MULTI_TAG";
};

struct TopicConfig {
    // TopicConfig.defaultReadQueueNums / defaultWriteQueueNums
    static constexpr int32_t DEFAULT_READ_QUEUE_NUMS = 16;
    static constexpr int32_t DEFAULT_WRITE_QUEUE_NUMS = 16;
    // PermName.PERM_READ | PermName.PERM_WRITE
    static constexpr int32_t DEFAULT_PERM = 6;

    std::string topicName;
    int32_t readQueueNums = DEFAULT_READ_QUEUE_NUMS;
    int32_t writeQueueNums = DEFAULT_WRITE_QUEUE_NUMS;
    int32_t perm = DEFAULT_PERM;
    std::string topicFilterType = TopicFilterType::SINGLE_TAG;
    int32_t topicSysFlag = 0;
    bool order = false;
    PropertyMap attributes;

    TopicConfig() = default;
    explicit TopicConfig(const std::string& name) : topicName(name) {}

    std::string permString() const;

    JsonValue toJson() const;
    static TopicConfig fromJson(const JsonValue& v);

    Bytes encode() const;
    // 解析失败返回 false（不抛异常）
    static bool decode(const Bytes& data, TopicConfig& out);

    std::string toString() const;
};

}  // namespace rocketmq

#endif  // ROCKETMQ_COMMON_TOPIC_CONFIG_H
