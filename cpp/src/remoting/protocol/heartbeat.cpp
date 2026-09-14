// ProducerData / ConsumerData / HeartbeatData 的实现。
#include "rocketmq/remoting/protocol/heartbeat.h"

#include <string>
#include <vector>

#include "rocketmq/common/byte_buffer.h"
#include "rocketmq/remoting/protocol/serialize.h"

namespace rocketmq {

const char* const ConsumeType::CONSUME_ACTIVELY = "CONSUME_ACTIVELY";
const char* const ConsumeType::CONSUME_PASSIVELY = "CONSUME_PASSIVELY";
const char* const ConsumeType::CONSUME_POP = "CONSUME_POP";

const char* const MessageModel::BROADCASTING = "BROADCASTING";
const char* const MessageModel::CLUSTERING = "CLUSTERING";
const char* const MessageModel::LITE_SELECTIVE = "LITE_SELECTIVE";

const char* const ConsumeFromWhere::CONSUME_FROM_LAST_OFFSET = "CONSUME_FROM_LAST_OFFSET";
const char* const ConsumeFromWhere::CONSUME_FROM_FIRST_OFFSET = "CONSUME_FROM_FIRST_OFFSET";
const char* const ConsumeFromWhere::CONSUME_FROM_TIMESTAMP = "CONSUME_FROM_TIMESTAMP";

// ---------------------------------------------------------------- ProducerData
JsonValue ProducerData::toJson() const {
    JsonValue o = JsonValue::makeObject();
    o.set("groupName", JsonValue::makeString(groupName));
    return o;
}

ProducerData ProducerData::fromJson(const JsonValue& v) {
    ProducerData p;
    std::string s;
    if (v.isObject() && v.tryGetString("groupName", s)) {
        p.groupName = s;
    }
    return p;
}

// ---------------------------------------------------------------- ConsumerData
JsonValue ConsumerData::toJson() const {
    JsonValue o = JsonValue::makeObject();
    o.set("consumeFromWhere", JsonValue::makeString(consumeFromWhere));
    o.set("consumeType", JsonValue::makeString(consumeType));
    o.set("groupName", JsonValue::makeString(groupName));
    o.set("messageModel", JsonValue::makeString(messageModel));
    JsonValue subs = JsonValue::makeArray();
    for (const SubscriptionData& sd : subscriptionDataSet) {
        subs.pushArray(sd.toJson());
    }
    o.set("subscriptionDataSet", subs);
    o.set("unitMode", JsonValue::makeBool(unitMode));
    return o;
}

ConsumerData ConsumerData::fromJson(const JsonValue& v) {
    ConsumerData c;
    if (!v.isObject()) {
        return c;
    }
    std::string s;
    if (v.tryGetString("consumeFromWhere", s)) c.consumeFromWhere = s;
    if (v.tryGetString("consumeType", s)) c.consumeType = s;
    if (v.tryGetString("groupName", s)) c.groupName = s;
    if (v.tryGetString("messageModel", s)) c.messageModel = s;
    bool b = false;
    if (v.tryGetBool("unitMode", b)) c.unitMode = b;
    const JsonValue* subs = v.find("subscriptionDataSet");
    if (subs != nullptr && subs->isArray()) {
        for (size_t i = 0; i < subs->size(); ++i) {
            c.subscriptionDataSet.push_back(SubscriptionData::fromJson(subs->at(i)));
        }
    }
    return c;
}

bool ConsumerData::operator==(const ConsumerData& o) const {
    return groupName == o.groupName && consumeType == o.consumeType
        && messageModel == o.messageModel && consumeFromWhere == o.consumeFromWhere
        && unitMode == o.unitMode && subscriptionDataSet == o.subscriptionDataSet;
}

std::string ConsumerData::toString() const {
    std::string subs = "[";
    for (size_t i = 0; i < subscriptionDataSet.size(); ++i) {
        if (i) subs += ", ";
        subs += subscriptionDataSet[i].toString();
    }
    subs += "]";
    return "ConsumerData [groupName=" + groupName + ", consumeType=" + consumeType
         + ", messageModel=" + messageModel + ", consumeFromWhere=" + consumeFromWhere
         + ", unitMode=" + (unitMode ? "true" : "false") + ", subscriptionDataSet=" + subs + "]";
}

// ---------------------------------------------------------------- HeartbeatData
JsonValue HeartbeatData::toJson() const {
    JsonValue o = JsonValue::makeObject();
    o.set("clientID", JsonValue::makeString(clientID));

    JsonValue consumers = JsonValue::makeArray();
    for (const ConsumerData& c : consumerDataSet) {
        consumers.pushArray(c.toJson());
    }
    o.set("consumerDataSet", consumers);

    o.set("heartbeatFingerprint", JsonValue::makeInt(heartbeatFingerprint));

    JsonValue producers = JsonValue::makeArray();
    for (const ProducerData& p : producerDataSet) {
        producers.pushArray(p.toJson());
    }
    o.set("producerDataSet", producers);

    o.set("withoutSub", JsonValue::makeBool(withoutSub));
    return o;
}

HeartbeatData HeartbeatData::fromJson(const JsonValue& v) {
    HeartbeatData hb;
    if (!v.isObject()) {
        return hb;
    }
    std::string s;
    if (v.tryGetString("clientID", s)) {
        hb.clientID = s;
    }
    int64_t n = 0;
    if (v.tryGetInt("heartbeatFingerprint", n)) {
        hb.heartbeatFingerprint = static_cast<int32_t>(n);
    }
    bool b = false;
    // 同时兼容 withoutSub 与 Java 字段名 isWithoutSub 两种写法
    if (v.tryGetBool("withoutSub", b)) {
        hb.withoutSub = b;
    } else if (v.tryGetBool("isWithoutSub", b)) {
        hb.withoutSub = b;
    }
    const JsonValue* producers = v.find("producerDataSet");
    if (producers != nullptr && producers->isArray()) {
        for (size_t i = 0; i < producers->size(); ++i) {
            hb.producerDataSet.push_back(ProducerData::fromJson(producers->at(i)));
        }
    }
    const JsonValue* consumers = v.find("consumerDataSet");
    if (consumers != nullptr && consumers->isArray()) {
        for (size_t i = 0; i < consumers->size(); ++i) {
            hb.consumerDataSet.push_back(ConsumerData::fromJson(consumers->at(i)));
        }
    }
    return hb;
}

Bytes HeartbeatData::encode() const {
    return RemotingSerializable::encode(toJson());
}

bool HeartbeatData::decode(const Bytes& data, HeartbeatData& out) {
    JsonValue v;
    if (!RemotingSerializable::decode(data, v)) {
        return false;
    }
    out = HeartbeatData::fromJson(v);
    return true;
}

std::string HeartbeatData::toString() const {
    return "HeartbeatData [clientID=" + clientID
         + ", producerDataSet=" + std::to_string(producerDataSet.size())
         + ", consumerDataSet=" + std::to_string(consumerDataSet.size()) + "]";
}

}  // namespace rocketmq
