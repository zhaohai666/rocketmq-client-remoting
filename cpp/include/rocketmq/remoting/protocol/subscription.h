// 订阅组模型（对应 org.apache.rocketmq.remoting.protocol.subscription 包）：
// SubscriptionGroupConfig / GroupRetryPolicy / SimpleSubscriptionData /
// SubscriptionGroupWrapper。
//
// 字段名与默认值以 Java 5.x 探针实测为准：
//   JSON.toJSONString(new SubscriptionGroupConfig("MyGroup")) ->
//   {"attributes":{},"brokerId":0,"consumeBroadcastEnable":true,"consumeEnable":true,
//    "consumeFromMinEnable":true,"consumeMessageOrderly":false,"consumeTimeoutMinute":15,
//    "groupName":"MyGroup","groupRetryPolicy":{"type":"CUSTOMIZED"},"groupSysFlag":0,
//    "notifyConsumerIdsChangedEnable":true,"retryMaxTimes":16,"retryQueueNums":1,
//    "whichBrokerWhenConsumeSlowly":1}
// fastjson2 **跳过 null 字段**（subscriptionDataSet 为 null 时整个键不出现）。
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

// org.apache.rocketmq.remoting.protocol.subscription.GroupRetryPolicyType
struct GroupRetryPolicyType {
    static constexpr const char* EXPONENTIAL = "EXPONENTIAL";
    static constexpr const char* CUSTOMIZED = "CUSTOMIZED";
};

// 对应 GroupRetryPolicy。两个子策略在 Java 里默认是 new 出来的实例，但 fastjson2
// 的探针输出里只有 {"type":"CUSTOMIZED"} —— 说明它们实际序列化时被跳过。
// 因此这里同样只在显式设置时输出（用 JsonValue 透传，避免过度建模）。
struct GroupRetryPolicy {
    std::string type = GroupRetryPolicyType::CUSTOMIZED;
    JsonValue exponentialRetryPolicy;  // isNull() 表示不输出
    JsonValue customizedRetryPolicy;   // isNull() 表示不输出

    JsonValue toJson() const;
    static GroupRetryPolicy fromJson(const JsonValue& v);
};

// 对应 SimpleSubscriptionData
struct SimpleSubscriptionData {
    std::string topic;
    std::string expressionType = "TAG";
    std::string expression = "*";
    int64_t version = 0;

    JsonValue toJson() const;
    static SimpleSubscriptionData fromJson(const JsonValue& v);
};

// 对应 SubscriptionGroupConfig
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
    // Java 为 null 时 fastjson2 整个键不输出
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

// 对应 org.apache.rocketmq.remoting.protocol.body.SubscriptionGroupWrapper
// 探针输出：{"dataVersion":{...},"forbiddenTable":{},"subscriptionGroupTable":{...}}
struct SubscriptionGroupWrapper {
    std::map<std::string, SubscriptionGroupConfig> subscriptionGroupTable;
    JsonValue forbiddenTable;  // 透传（本实现不解释其内容）
    JsonValue dataVersion;     // 透传：翻页请求要原样回传，避免重排/丢精度

    JsonValue toJson() const;
    static SubscriptionGroupWrapper fromJson(const JsonValue& v);

    Bytes encode() const;
    static bool decode(const Bytes& data, SubscriptionGroupWrapper& out);

    // 合并另一页（getAllSubscriptionGroup 分页累积用）
    void mergeFrom(const SubscriptionGroupWrapper& other);
};

}  // namespace rocketmq

#endif  // ROCKETMQ_REMOTING_PROTOCOL_SUBSCRIPTION_H
