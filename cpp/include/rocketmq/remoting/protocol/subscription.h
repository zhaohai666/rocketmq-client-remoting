// Field names and defaults were measured against the broker-side serialization probe:
//   JSON.toJSONString(new SubscriptionGroupConfig("MyGroup")) ->
//   {"attributes":{},"brokerId":0,"consumeBroadcastEnable":true,"consumeEnable":true,
//    "consumeFromMinEnable":true,"consumeMessageOrderly":false,"consumeTimeoutMinute":15,
//    "groupName":"MyGroup","groupRetryPolicy":{"type":"CUSTOMIZED"},"groupSysFlag":0,
//    "notifyConsumerIdsChangedEnable":true,"retryMaxTimes":16,"retryQueueNums":1,
//    "whichBrokerWhenConsumeSlowly":1}
// fastjson2 **skips null fields** (when subscriptionDataSet is null the whole key is absent).
#ifndef ROCKETMQ_REMOTING_PROTOCOL_SUBSCRIPTION_H
#define ROCKETMQ_REMOTING_PROTOCOL_SUBSCRIPTION_H

#include <cstdint>
#include <map>
#include <string>
#include <vector>

#include "rocketmq/common/byte_buffer.h"
#include "rocketmq/common/mix_all.h"
#include "rocketmq/common/types.h"
#include "rocketmq/remoting/protocol/json.h"

namespace rocketmq {

struct GroupRetryPolicyType {
    static constexpr const char* EXPONENTIAL = "EXPONENTIAL";
    static constexpr const char* CUSTOMIZED = "CUSTOMIZED";
};

// Mirrors the GroupRetryPolicy wire structure. The two sub-policies default to freshly created instances in the
// protocol, but the broker-side fastjson probe output only shows {"type":"CUSTOMIZED"} — they are skipped when serialized.
// So this side also emits them only when set explicitly (passed through as JsonValue to avoid over-modeling).
struct GroupRetryPolicy {
    std::string type = GroupRetryPolicyType::CUSTOMIZED;
    JsonValue exponentialRetryPolicy;  // isNull() means "do not emit"
    JsonValue customizedRetryPolicy;   // isNull() means "do not emit"

    JsonValue toJson() const;
    static GroupRetryPolicy fromJson(const JsonValue& v);
};

// Mirrors the SimpleSubscriptionData wire structure
struct SimpleSubscriptionData {
    std::string topic;
    std::string expressionType = "TAG";
    std::string expression = "*";
    int64_t version = 0;

    JsonValue toJson() const;
    static SimpleSubscriptionData fromJson(const JsonValue& v);
};

// Mirrors the SubscriptionGroupConfig wire structure
struct SubscriptionGroupConfig {
    // MixAll.MASTER_ID
    static constexpr int32_t MASTER_ID = 0;

    std::string groupName;
    bool consumeEnable = true;
    bool consumeFromMinEnable = true;
    bool consumeBroadcastEnable = true;
    bool consumeMessageOrderly = false;
    int32_t retryQueueNums = 1;
    int32_t retryMaxTimes = 16;
    GroupRetryPolicy groupRetryPolicy;
    int32_t brokerId = MASTER_ID;
    int32_t whichBrokerWhenConsumeSlowly = 1;
    bool notifyConsumerIdsChangedEnable = true;
    int32_t groupSysFlag = 0;
    int32_t consumeTimeoutMinute = 15;
    PropertyMap attributes;
    // When this field is null the broker-side fastjson omits the whole key
    std::vector<SimpleSubscriptionData> subscriptionDataSet;
    bool hasSubscriptionDataSet = false;

    SubscriptionGroupConfig() = default;
    explicit SubscriptionGroupConfig(const std::string& name) : groupName(name) {}

    JsonValue toJson() const;
    static SubscriptionGroupConfig fromJson(const JsonValue& v);

    Bytes encode() const;
    static bool decode(const Bytes& data, SubscriptionGroupConfig& out);

    std::string toString() const;
};

struct SubscriptionGroupWrapper {
    std::map<std::string, SubscriptionGroupConfig> subscriptionGroupTable;
    JsonValue forbiddenTable;  // Passed through (this implementation does not interpret its content)
    JsonValue dataVersion;     // Pass-through: a paging request must return it verbatim, avoiding reordering or lost precision

    JsonValue toJson() const;
    static SubscriptionGroupWrapper fromJson(const JsonValue& v);

    Bytes encode() const;
    static bool decode(const Bytes& data, SubscriptionGroupWrapper& out);

    // Merge another page (used to accumulate getAllSubscriptionGroup pages)
    void mergeFrom(const SubscriptionGroupWrapper& other);
};

}  // namespace rocketmq

#endif  // ROCKETMQ_REMOTING_PROTOCOL_SUBSCRIPTION_H
