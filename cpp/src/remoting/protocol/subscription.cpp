// 订阅组模型实现（对应 Python remoting/protocol/subscription.py）。
#include "rocketmq/remoting/protocol/subscription.h"

#include <string>

#include "rocketmq/remoting/protocol/serialize.h"

namespace rocketmq {

// ---------------------------------------------------------------- 小工具
namespace {

// 取 bool 字段，缺失时用默认值（fastjson2 会跳过 null/未设置字段）
bool getBool(const JsonValue& v, const char* key, bool def) {
    const JsonValue* p = v.find(key);
    if (p == nullptr) return def;
    if (p->isBool()) return p->boolValue();
    if (p->isNumber()) return p->intValue() != 0;
    return def;
}

int64_t getInt(const JsonValue& v, const char* key, int64_t def) {
    const JsonValue* p = v.find(key);
    return (p != nullptr && p->isNumber()) ? p->intValue() : def;
}

std::string getStr(const JsonValue& v, const char* key, const std::string& def) {
    std::string s;
    return v.tryGetString(key, s) ? s : def;
}

PropertyMap getStringMap(const JsonValue& v, const char* key) {
    PropertyMap out;
    const JsonValue* p = v.find(key);
    if (p != nullptr && p->isObject()) {
        for (const auto& kv : p->objectItems()) {
            if (kv.second.isString()) out[kv.first] = kv.second.stringValue();
        }
    }
    return out;
}

JsonValue makeStringMap(const PropertyMap& m) {
    JsonValue o = JsonValue::makeObject();
    for (const auto& kv : m) {
        o.set(kv.first, JsonValue::makeString(kv.second));
    }
    return o;
}

// 取原始子对象（isNull() 表示不存在）
JsonValue rawSub(const JsonValue& v, const char* key) {
    const JsonValue* p = v.find(key);
    return p != nullptr ? *p : JsonValue::makeNull();
}

}  // namespace

// ---------------------------------------------------------------- GroupRetryPolicy
JsonValue GroupRetryPolicy::toJson() const {
    JsonValue v = JsonValue::makeObject();
    v.set("type", JsonValue::makeString(type));
    if (!exponentialRetryPolicy.isNull()) v.set("exponentialRetryPolicy", exponentialRetryPolicy);
    if (!customizedRetryPolicy.isNull()) v.set("customizedRetryPolicy", customizedRetryPolicy);
    return v;
}

GroupRetryPolicy GroupRetryPolicy::fromJson(const JsonValue& v) {
    GroupRetryPolicy p;
    if (!v.isObject()) return p;
    p.type = getStr(v, "type", GroupRetryPolicyType::CUSTOMIZED);
    p.exponentialRetryPolicy = rawSub(v, "exponentialRetryPolicy");
    p.customizedRetryPolicy = rawSub(v, "customizedRetryPolicy");
    return p;
}

// ---------------------------------------------------------------- SimpleSubscriptionData
JsonValue SimpleSubscriptionData::toJson() const {
    JsonValue v = JsonValue::makeObject();
    v.set("topic", JsonValue::makeString(topic));
    v.set("expressionType", JsonValue::makeString(expressionType));
    v.set("expression", JsonValue::makeString(expression));
    v.set("version", JsonValue::makeInt(version));
    return v;
}

SimpleSubscriptionData SimpleSubscriptionData::fromJson(const JsonValue& v) {
    SimpleSubscriptionData s;
    s.topic = getStr(v, "topic", "");
    s.expressionType = getStr(v, "expressionType", "TAG");
    s.expression = getStr(v, "expression", "*");
    s.version = getInt(v, "version", 0);
    return s;
}

// ---------------------------------------------------------------- SubscriptionGroupConfig
JsonValue SubscriptionGroupConfig::toJson() const {
    JsonValue v = JsonValue::makeObject();
    v.set("groupName", JsonValue::makeString(groupName));
    v.set("consumeEnable", JsonValue::makeBool(consumeEnable));
    v.set("consumeFromMinEnable", JsonValue::makeBool(consumeFromMinEnable));
    v.set("consumeBroadcastEnable", JsonValue::makeBool(consumeBroadcastEnable));
    v.set("consumeMessageOrderly", JsonValue::makeBool(consumeMessageOrderly));
    v.set("retryQueueNums", JsonValue::makeInt(retryQueueNums));
    v.set("retryMaxTimes", JsonValue::makeInt(retryMaxTimes));
    v.set("groupRetryPolicy", groupRetryPolicy.toJson());
    v.set("brokerId", JsonValue::makeInt(brokerId));
    v.set("whichBrokerWhenConsumeSlowly", JsonValue::makeInt(whichBrokerWhenConsumeSlowly));
    v.set("notifyConsumerIdsChangedEnable", JsonValue::makeBool(notifyConsumerIdsChangedEnable));
    v.set("groupSysFlag", JsonValue::makeInt(groupSysFlag));
    v.set("consumeTimeoutMinute", JsonValue::makeInt(consumeTimeoutMinute));
    v.set("attributes", makeStringMap(attributes));
    // fastjson2 默认跳过 null：只有显式设置过才输出该键
    if (hasSubscriptionDataSet) {
        JsonValue arr = JsonValue::makeArray();
        for (const auto& s : subscriptionDataSet) arr.pushArray(s.toJson());
        v.set("subscriptionDataSet", arr);
    }
    return v;
}

SubscriptionGroupConfig SubscriptionGroupConfig::fromJson(const JsonValue& v) {
    SubscriptionGroupConfig c;
    c.groupName = getStr(v, "groupName", "");
    c.consumeEnable = getBool(v, "consumeEnable", true);
    c.consumeFromMinEnable = getBool(v, "consumeFromMinEnable", true);
    c.consumeBroadcastEnable = getBool(v, "consumeBroadcastEnable", true);
    c.consumeMessageOrderly = getBool(v, "consumeMessageOrderly", false);
    c.retryQueueNums = static_cast<int32_t>(getInt(v, "retryQueueNums", 1));
    c.retryMaxTimes = static_cast<int32_t>(getInt(v, "retryMaxTimes", 16));
    const JsonValue* policy = v.find("groupRetryPolicy");
    if (policy != nullptr) c.groupRetryPolicy = GroupRetryPolicy::fromJson(*policy);
    c.brokerId = static_cast<int32_t>(getInt(v, "brokerId", MASTER_ID));
    c.whichBrokerWhenConsumeSlowly =
        static_cast<int32_t>(getInt(v, "whichBrokerWhenConsumeSlowly", 1));
    c.notifyConsumerIdsChangedEnable = getBool(v, "notifyConsumerIdsChangedEnable", true);
    c.groupSysFlag = static_cast<int32_t>(getInt(v, "groupSysFlag", 0));
    c.consumeTimeoutMinute = static_cast<int32_t>(getInt(v, "consumeTimeoutMinute", 15));
    c.attributes = getStringMap(v, "attributes");
    const JsonValue* sub = v.find("subscriptionDataSet");
    if (sub != nullptr && sub->isArray() && sub->size() > 0) {
        c.hasSubscriptionDataSet = true;
        c.subscriptionDataSet.clear();
        for (size_t i = 0; i < sub->size(); ++i) {
            c.subscriptionDataSet.push_back(SimpleSubscriptionData::fromJson(sub->at(i)));
        }
    }
    return c;
}

Bytes SubscriptionGroupConfig::encode() const { return RemotingSerializable::encode(toJson()); }

bool SubscriptionGroupConfig::decode(const Bytes& data, SubscriptionGroupConfig& out) {
    JsonValue v;
    if (!RemotingSerializable::decode(data, v)) return false;
    out = fromJson(v);
    return true;
}

std::string SubscriptionGroupConfig::toString() const {
    return "SubscriptionGroupConfig [groupName=" + groupName
         + ", consumeEnable=" + (consumeEnable ? "true" : "false")
         + ", consumeFromMinEnable=" + (consumeFromMinEnable ? "true" : "false")
         + ", consumeBroadcastEnable=" + (consumeBroadcastEnable ? "true" : "false")
         + ", consumeMessageOrderly=" + (consumeMessageOrderly ? "true" : "false")
         + ", retryQueueNums=" + std::to_string(retryQueueNums)
         + ", retryMaxTimes=" + std::to_string(retryMaxTimes)
         + ", brokerId=" + std::to_string(brokerId)
         + ", whichBrokerWhenConsumeSlowly=" + std::to_string(whichBrokerWhenConsumeSlowly) + "]";
}

// ---------------------------------------------------------------- SubscriptionGroupWrapper
JsonValue SubscriptionGroupWrapper::toJson() const {
    JsonValue v = JsonValue::makeObject();
    v.set("dataVersion", dataVersion.isNull() ? JsonValue::makeObject() : dataVersion);
    v.set("forbiddenTable", forbiddenTable.isNull() ? JsonValue::makeObject() : forbiddenTable);
    JsonValue table = JsonValue::makeObject();
    for (const auto& kv : subscriptionGroupTable) {
        table.set(kv.first, kv.second.toJson());
    }
    v.set("subscriptionGroupTable", table);
    return v;
}

SubscriptionGroupWrapper SubscriptionGroupWrapper::fromJson(const JsonValue& v) {
    SubscriptionGroupWrapper w;
    const JsonValue* table = v.find("subscriptionGroupTable");
    if (table != nullptr && table->isObject()) {
        for (const auto& kv : table->objectItems()) {
            w.subscriptionGroupTable[kv.first] = SubscriptionGroupConfig::fromJson(kv.second);
        }
    }
    w.forbiddenTable = rawSub(v, "forbiddenTable");
    w.dataVersion = rawSub(v, "dataVersion");
    return w;
}

Bytes SubscriptionGroupWrapper::encode() const { return RemotingSerializable::encode(toJson()); }

bool SubscriptionGroupWrapper::decode(const Bytes& data, SubscriptionGroupWrapper& out) {
    JsonValue v;
    if (!RemotingSerializable::decode(data, v)) return false;
    out = fromJson(v);
    return true;
}

void SubscriptionGroupWrapper::mergeFrom(const SubscriptionGroupWrapper& other) {
    for (const auto& kv : other.subscriptionGroupTable) {
        subscriptionGroupTable[kv.first] = kv.second;
    }
    if (other.forbiddenTable.isObject()) {
        if (!forbiddenTable.isObject()) forbiddenTable = JsonValue::makeObject();
        for (const auto& kv : other.forbiddenTable.objectItems()) {
            forbiddenTable.set(kv.first, kv.second);
        }
    }
    if (!other.dataVersion.isNull()) dataVersion = other.dataVersion;
}

}  // namespace rocketmq
