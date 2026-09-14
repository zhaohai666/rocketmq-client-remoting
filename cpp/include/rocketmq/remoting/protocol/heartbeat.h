// 心跳数据（对应 org.apache.rocketmq.remoting.protocol.heartbeat.*）。
//
// 用途：生产者/消费者按固定间隔向**所有** broker 发 HEART_BEAT，body 即
// HeartbeatData 的 JSON。broker 据此注册/刷新 client channel、消费组订阅与
// 生产者组，并创建重试 topic。
//
// JSON 字段（与 fastjson2 序列化结果一致，键按字母序）：
//   ProducerData  : groupName
//   ConsumerData  : consumeFromWhere | consumeType | groupName | messageModel
//                   | subscriptionDataSet | unitMode
//   HeartbeatData : clientID | consumerDataSet | heartbeatFingerprint
//                   | producerDataSet | withoutSub
//
// 注意两点与旧版 4.x 的差异（本仓库为 5.x 源码，已实测 fastjson2 输出确认）：
//   1. ConsumerData **没有** consumeTimestamp / maxReconsumeTimes 字段；
//   2. HeartbeatData 多了 heartbeatFingerprint / withoutSub 两个字段。
//
// heartbeatFingerprint 的语义（见 broker ClientManageProcessor.heartBeat）：
//   - fingerprint != 0 -> 走 heartBeatV2 路径（按指纹判断订阅是否变化，可配合
//     withoutSub 跳过订阅上报）；
//   - fingerprint == 0 -> 走 V1 路径，用完整的 subscriptionDataSet 注册消费者。
// 因此**保持默认 0** 是最稳妥的选择：无需实现 computeHeartbeatFingerprint() 那套
// 依赖 fastjson2 字段序的指纹算法，也能被 broker 正确注册。
#ifndef ROCKETMQ_REMOTING_PROTOCOL_HEARTBEAT_H
#define ROCKETMQ_REMOTING_PROTOCOL_HEARTBEAT_H

#include <cstdint>
#include <string>
#include <vector>

#include "rocketmq/common/byte_buffer.h"
#include "rocketmq/common/subscription_data.h"
#include "rocketmq/remoting/protocol/json.h"

namespace rocketmq {

// org.apache.rocketmq.remoting.protocol.heartbeat.ConsumeType
struct ConsumeType {
    static const char* const CONSUME_ACTIVELY;   // "CONSUME_ACTIVELY"
    static const char* const CONSUME_PASSIVELY;  // "CONSUME_PASSIVELY"
    static const char* const CONSUME_POP;        // "CONSUME_POP"
};

// org.apache.rocketmq.remoting.protocol.heartbeat.MessageModel
struct MessageModel {
    static const char* const BROADCASTING;     // "BROADCASTING"
    static const char* const CLUSTERING;       // "CLUSTERING"
    static const char* const LITE_SELECTIVE;   // "LITE_SELECTIVE"
};

// org.apache.rocketmq.common.consumer.ConsumeFromWhere
struct ConsumeFromWhere {
    static const char* const CONSUME_FROM_LAST_OFFSET;  // "CONSUME_FROM_LAST_OFFSET"
    static const char* const CONSUME_FROM_FIRST_OFFSET;  // "CONSUME_FROM_FIRST_OFFSET"
    static const char* const CONSUME_FROM_TIMESTAMP;     // "CONSUME_FROM_TIMESTAMP"
};

// ---------------------------------------------------------------- ProducerData
class ProducerData {
public:
    std::string groupName;

    ProducerData() = default;
    explicit ProducerData(const std::string& groupName) : groupName(groupName) {}

    const std::string& getGroupName() const { return groupName; }
    void setGroupName(const std::string& g) { groupName = g; }

    JsonValue toJson() const;
    static ProducerData fromJson(const JsonValue& v);

    bool operator==(const ProducerData& o) const { return groupName == o.groupName; }
    bool operator!=(const ProducerData& o) const { return !(*this == o); }

    std::string toString() const { return "ProducerData [groupName=" + groupName + "]"; }
};

// ---------------------------------------------------------------- ConsumerData
class ConsumerData {
public:
    std::string groupName;
    std::string consumeType = ConsumeType::CONSUME_PASSIVELY;
    std::string messageModel = MessageModel::CLUSTERING;
    std::string consumeFromWhere = ConsumeFromWhere::CONSUME_FROM_LAST_OFFSET;
    std::vector<SubscriptionData> subscriptionDataSet;
    bool unitMode = false;

    ConsumerData() = default;
    ConsumerData(const std::string& groupName, const std::string& consumeType,
                 const std::string& messageModel, const std::string& consumeFromWhere)
        : groupName(groupName), consumeType(consumeType), messageModel(messageModel),
          consumeFromWhere(consumeFromWhere) {}

    const std::string& getGroupName() const { return groupName; }
    void setGroupName(const std::string& g) { groupName = g; }
    const std::string& getConsumeType() const { return consumeType; }
    void setConsumeType(const std::string& t) { consumeType = t; }
    const std::string& getMessageModel() const { return messageModel; }
    void setMessageModel(const std::string& m) { messageModel = m; }
    const std::string& getConsumeFromWhere() const { return consumeFromWhere; }
    void setConsumeFromWhere(const std::string& w) { consumeFromWhere = w; }
    const std::vector<SubscriptionData>& getSubscriptionDataSet() const {
        return subscriptionDataSet;
    }
    void setSubscriptionDataSet(const std::vector<SubscriptionData>& s) {
        subscriptionDataSet = s;
    }
    // 对应 Java subscriptionDataSet.add(...)
    void addSubscriptionData(const SubscriptionData& sd) { subscriptionDataSet.push_back(sd); }
    bool isUnitMode() const { return unitMode; }
    void setUnitMode(bool m) { unitMode = m; }

    JsonValue toJson() const;
    static ConsumerData fromJson(const JsonValue& v);

    bool operator==(const ConsumerData& o) const;
    bool operator!=(const ConsumerData& o) const { return !(*this == o); }

    std::string toString() const;
};

// ---------------------------------------------------------------- HeartbeatData
class HeartbeatData {
public:
    std::string clientID;
    std::vector<ProducerData> producerDataSet;
    std::vector<ConsumerData> consumerDataSet;
    // 0 = 走 broker 的 V1 注册路径（推荐）；非 0 才会触发 heartBeatV2 指纹优化
    int32_t heartbeatFingerprint = 0;
    // 仅在 fingerprint != 0（V2 路径）时被 broker 读取
    bool withoutSub = false;

    HeartbeatData() = default;
    explicit HeartbeatData(const std::string& clientID) : clientID(clientID) {}

    const std::string& getClientID() const { return clientID; }
    void setClientID(const std::string& id) { clientID = id; }
    const std::vector<ProducerData>& getProducerDataSet() const { return producerDataSet; }
    void setProducerDataSet(const std::vector<ProducerData>& p) { producerDataSet = p; }
    const std::vector<ConsumerData>& getConsumerDataSet() const { return consumerDataSet; }
    void setConsumerDataSet(const std::vector<ConsumerData>& c) { consumerDataSet = c; }
    void addProducerData(const ProducerData& p) { producerDataSet.push_back(p); }
    void addConsumerData(const ConsumerData& c) { consumerDataSet.push_back(c); }
    int32_t getHeartbeatFingerprint() const { return heartbeatFingerprint; }
    void setHeartbeatFingerprint(int32_t f) { heartbeatFingerprint = f; }
    bool isWithoutSub() const { return withoutSub; }
    void setWithoutSub(bool b) { withoutSub = b; }

    JsonValue toJson() const;
    static HeartbeatData fromJson(const JsonValue& v);

    Bytes encode() const;
    static bool decode(const Bytes& data, HeartbeatData& out);

    std::string toString() const;
};

}  // namespace rocketmq

#endif  // ROCKETMQ_REMOTING_PROTOCOL_HEARTBEAT_H
