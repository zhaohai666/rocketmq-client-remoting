// Purpose: producers/consumers send HEART_BEAT to **every** broker at a fixed interval, the body being the
// JSON of HeartbeatData. The broker uses it to register/refresh the client channel, consumer group
// subscriptions and producer groups, and to create retry topics.
//
// JSON fields (identical to the fastjson2 serialization result, keys in alphabetical order):
//   ProducerData  : groupName
//   ConsumerData  : consumeFromWhere | consumeType | groupName | messageModel
//                   | subscriptionDataSet | unitMode
//   HeartbeatData : clientID | consumerDataSet | heartbeatFingerprint
//                   | producerDataSet | withoutSub
//
// Two differences from the older 4.x protocol (this repository tracks 5.x sources; confirmed by measured fastjson2 output):
//   1. ConsumerData has **no** consumeTimestamp / maxReconsumeTimes fields;
//   2. HeartbeatData has two extra fields, heartbeatFingerprint / withoutSub.
//
// Semantics of heartbeatFingerprint (see the broker's ClientManageProcessor.heartBeat):
//   - fingerprint != 0 -> the heartBeatV2 path (the fingerprint decides whether the subscription
//     changed, and withoutSub can skip reporting subscriptions);
//   - fingerprint == 0 -> the V1 path, registering the consumer with the full subscriptionDataSet.
// So **keeping the default 0** is the safest choice: it needs no computeHeartbeatFingerprint()
// fingerprint algorithm depending on fastjson2 field order, and the broker still registers the client correctly.
#ifndef ROCKETMQ_REMOTING_PROTOCOL_HEARTBEAT_H
#define ROCKETMQ_REMOTING_PROTOCOL_HEARTBEAT_H

#include <cstdint>
#include <string>
#include <vector>

#include "rocketmq/common/byte_buffer.h"
#include "rocketmq/common/subscription_data.h"
#include "rocketmq/remoting/protocol/json.h"

namespace rocketmq {

struct ConsumeType {
    static const char* const CONSUME_ACTIVELY;   // "CONSUME_ACTIVELY"
    static const char* const CONSUME_PASSIVELY;  // "CONSUME_PASSIVELY"
    static const char* const CONSUME_POP;        // "CONSUME_POP"
};

struct MessageModel {
    static const char* const BROADCASTING;     // "BROADCASTING"
    static const char* const CLUSTERING;       // "CLUSTERING"
    static const char* const LITE_SELECTIVE;   // "LITE_SELECTIVE"
};

struct ConsumeFromWhere {
    static const char* const CONSUME_FROM_LAST_OFFSET;  // "CONSUME_FROM_LAST_OFFSET"
    // @Deprecated: same branch as CONSUME_FROM_LAST_OFFSET
    static const char* const CONSUME_FROM_LAST_OFFSET_AND_FROM_MIN_WHEN_BOOT_FIRST;  // "CONSUME_FROM_LAST_OFFSET_AND_FROM_MIN_WHEN_BOOT_FIRST"
    // @Deprecated: same branch as CONSUME_FROM_LAST_OFFSET
    static const char* const CONSUME_FROM_MIN_OFFSET;  // "CONSUME_FROM_MIN_OFFSET"
    // @Deprecated: same branch as CONSUME_FROM_LAST_OFFSET
    static const char* const CONSUME_FROM_MAX_OFFSET;  // "CONSUME_FROM_MAX_OFFSET"
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
    // Append one subscription data
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
    // 0 = the broker's V1 registration path (recommended); only a non-zero value triggers the heartBeatV2 fingerprint optimization
    int32_t heartbeatFingerprint = 0;
    // Read by the broker only when fingerprint != 0 (the V2 path)
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
