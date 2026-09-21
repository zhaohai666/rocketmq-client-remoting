// 公共 body 实现（对应 Python remoting/protocol/body.py）。
#include "rocketmq/remoting/protocol/body.h"

#include <algorithm>
#include <string>

#include "rocketmq/remoting/protocol/serialize.h"

namespace rocketmq {

namespace {

JsonValue rawSub(const JsonValue& v, const char* key) {
    const JsonValue* p = v.find(key);
    return p != nullptr ? *p : JsonValue::makeNull();
}

std::string firstString(const JsonValue& v, const char* key) {
    std::string s;
    v.tryGetString(key, s);
    return s;
}

int64_t firstInt(const JsonValue& v, const char* key) {
    const JsonValue* p = v.find(key);
    return (p != nullptr && p->isNumber()) ? p->intValue() : 0;
}

JsonValue makeStringMap(const PropertyMap& m) {
    JsonValue o = JsonValue::makeObject();
    for (const auto& kv : m) o.set(kv.first, JsonValue::makeString(kv.second));
    return o;
}

PropertyMap readStringMap(const JsonValue& v) {
    PropertyMap out;
    if (v.isObject()) {
        for (const auto& kv : v.objectItems()) {
            if (kv.second.isString()) out[kv.first] = kv.second.stringValue();
        }
    }
    return out;
}

}  // namespace

// ---------------------------------------------------------------- KVTable
JsonValue KVTable::toJson() const {
    JsonValue v = JsonValue::makeObject();
    v.set("table", makeStringMap(table));
    return v;
}

KVTable KVTable::fromJson(const JsonValue& v) {
    KVTable kv;
    kv.table = readStringMap(v.get("table"));
    return kv;
}

Bytes KVTable::encode() const { return RemotingSerializable::encode(toJson()); }

bool KVTable::decode(const Bytes& data, KVTable& out) {
    JsonValue v;
    if (!RemotingSerializable::decode(data, v)) return false;
    out = fromJson(v);
    return true;
}

// ---------------------------------------------------------------- TopicList
JsonValue TopicList::toJson() const {
    JsonValue v = JsonValue::makeObject();
    JsonValue arr = JsonValue::makeArray();
    for (const auto& t : topicList) arr.pushArray(JsonValue::makeString(t));
    v.set("topicList", arr);
    if (hasBrokerAddr) v.set("brokerAddr", JsonValue::makeString(brokerAddr));
    return v;
}

TopicList TopicList::fromJson(const JsonValue& v) {
    TopicList tl;
    const JsonValue* arr = v.find("topicList");
    if (arr != nullptr && arr->isArray()) {
        for (size_t i = 0; i < arr->size(); ++i) {
            if (arr->at(i).isString()) tl.topicList.push_back(arr->at(i).stringValue());
        }
    }
    std::string s;
    if (v.tryGetString("brokerAddr", s)) {
        tl.brokerAddr = s;
        tl.hasBrokerAddr = true;
    }
    return tl;
}

Bytes TopicList::encode() const { return RemotingSerializable::encode(toJson()); }

bool TopicList::decode(const Bytes& data, TopicList& out) {
    JsonValue v;
    if (!RemotingSerializable::decode(data, v)) return false;
    out = fromJson(v);
    return true;
}

bool TopicList::contains(const std::string& topic) const {
    return std::find(topicList.begin(), topicList.end(), topic) != topicList.end();
}

// ---------------------------------------------------------------- GetConsumerListByGroupResponseBody
JsonValue GetConsumerListByGroupResponseBody::toJson() const {
    JsonValue v = JsonValue::makeObject();
    JsonValue arr = JsonValue::makeArray();
    for (const auto& id : consumerIdList) arr.pushArray(JsonValue::makeString(id));
    v.set("consumerIdList", arr);
    return v;
}

GetConsumerListByGroupResponseBody GetConsumerListByGroupResponseBody::fromJson(
    const JsonValue& v) {
    GetConsumerListByGroupResponseBody b;
    const JsonValue* arr = v.find("consumerIdList");
    if (arr != nullptr && arr->isArray()) {
        for (size_t i = 0; i < arr->size(); ++i) {
            if (arr->at(i).isString()) b.consumerIdList.push_back(arr->at(i).stringValue());
        }
    }
    return b;
}

Bytes GetConsumerListByGroupResponseBody::encode() const {
    return RemotingSerializable::encode(toJson());
}

bool GetConsumerListByGroupResponseBody::decode(const Bytes& data,
                                               GetConsumerListByGroupResponseBody& out) {
    JsonValue v;
    if (!RemotingSerializable::decode(data, v)) return false;
    out = fromJson(v);
    return true;
}

// ---------------------------------------------------------------- CheckClientRequestBody
JsonValue CheckClientRequestBody::toJson() const {
    JsonValue v = JsonValue::makeObject();
    v.set("clientId", JsonValue::makeString(clientId));
    v.set("group", JsonValue::makeString(group));
    v.set("subscriptionData", subscriptionData.toJson());
    // Java 的 namespace 为 null 时 fastjson 不写这个键，这里用 hasNamespace 表达同一语义
    if (hasNamespace) v.set("namespace", JsonValue::makeString(clientNamespace));
    return v;
}

CheckClientRequestBody CheckClientRequestBody::fromJson(const JsonValue& v) {
    CheckClientRequestBody b;
    std::string s;
    if (v.tryGetString("clientId", s)) b.clientId = s;
    if (v.tryGetString("group", s)) b.group = s;
    if (v.tryGetString("namespace", s)) {
        b.clientNamespace = s;
        b.hasNamespace = true;
    }
    const JsonValue* sd = v.find("subscriptionData");
    if (sd != nullptr && sd->isObject()) b.subscriptionData = SubscriptionData::fromJson(*sd);
    return b;
}

Bytes CheckClientRequestBody::encode() const { return RemotingSerializable::encode(toJson()); }

bool CheckClientRequestBody::decode(const Bytes& data, CheckClientRequestBody& out) {
    JsonValue v;
    if (!RemotingSerializable::decode(data, v)) return false;
    out = fromJson(v);
    return true;
}

// ---------------------------------------------------------------- ClusterInfo
std::vector<std::string> ClusterInfo::getBrokerAddrs() const {
    // brokerAddrTable 是 std::map（已按 brokerName 排序），内层 brokerAddrs 也按 brokerId 排序
    std::vector<std::string> addrs;
    for (const auto& kv : brokerAddrTable) {
        for (const auto& addrKv : kv.second.brokerAddrs) {
            const std::string& a = addrKv.second;
            if (!a.empty() && std::find(addrs.begin(), addrs.end(), a) == addrs.end()) {
                addrs.push_back(a);
            }
        }
    }
    return addrs;
}

std::vector<std::string> ClusterInfo::getBrokerAddrsOfCluster(
    const std::string& clusterName) const {
    if (clusterName.empty()) return getBrokerAddrs();
    auto it = clusterAddrTable.find(clusterName);
    if (it == clusterAddrTable.end()) return std::vector<std::string>();
    std::vector<std::string> addrs;
    for (const std::string& brokerName : it->second) {
        auto bit = brokerAddrTable.find(brokerName);
        if (bit == brokerAddrTable.end()) continue;
        for (const auto& addrKv : bit->second.brokerAddrs) {
            const std::string& a = addrKv.second;
            if (!a.empty() && std::find(addrs.begin(), addrs.end(), a) == addrs.end()) {
                addrs.push_back(a);
            }
        }
    }
    return addrs;
}

JsonValue ClusterInfo::toJson() const {
    JsonValue v = JsonValue::makeObject();
    JsonValue brokers = JsonValue::makeObject();
    for (const auto& kv : brokerAddrTable) brokers.set(kv.first, kv.second.toJson());
    v.set("brokerAddrTable", brokers);

    JsonValue clusters = JsonValue::makeObject();
    for (const auto& kv : clusterAddrTable) {
        JsonValue arr = JsonValue::makeArray();
        for (const auto& n : kv.second) arr.pushArray(JsonValue::makeString(n));
        clusters.set(kv.first, arr);
    }
    v.set("clusterAddrTable", clusters);
    return v;
}

ClusterInfo ClusterInfo::fromJson(const JsonValue& v) {
    ClusterInfo ci;
    // 真实 RocketMQ 的 brokerAddrTable[name] 是 BrokerData 对象，
    // 真正的 {brokerId: addr} 映射在其中的 brokerAddrs 字段下。
    const JsonValue* brokers = v.find("brokerAddrTable");
    if (brokers != nullptr && brokers->isObject()) {
        for (const auto& kv : brokers->objectItems()) {
            ci.brokerAddrTable[kv.first] = BrokerData::fromJson(kv.second);
        }
    }
    const JsonValue* clusters = v.find("clusterAddrTable");
    if (clusters != nullptr && clusters->isObject()) {
        for (const auto& kv : clusters->objectItems()) {
            // 兼容两种写法：数组形式（标准）与字符串形式（个别老版本）
            if (kv.second.isArray()) {
                std::vector<std::string> names;
                for (size_t i = 0; i < kv.second.size(); ++i) {
                    if (kv.second.at(i).isString()) names.push_back(kv.second.at(i).stringValue());
                }
                ci.clusterAddrTable[kv.first] = names;
            } else if (kv.second.isString()) {
                ci.clusterAddrTable[kv.first].push_back(kv.second.stringValue());
            }
        }
    }
    return ci;
}

Bytes ClusterInfo::encode() const { return RemotingSerializable::encode(toJson()); }

bool ClusterInfo::decode(const Bytes& data, ClusterInfo& out) {
    JsonValue v;
    if (!RemotingSerializable::decode(data, v)) return false;
    out = fromJson(v);
    return true;
}

// ---------------------------------------------------------------- Connection
JsonValue Connection::toJson() const {
    JsonValue v = JsonValue::makeObject();
    v.set("clientId", JsonValue::makeString(clientId));
    v.set("clientAddr", JsonValue::makeString(clientAddr));
    v.set("language", JsonValue::makeString(language));
    v.set("version", JsonValue::makeInt(version));
    return v;
}

Connection Connection::fromJson(const JsonValue& v) {
    Connection c;
    c.clientId = firstString(v, "clientId");
    c.clientAddr = firstString(v, "clientAddr");
    c.language = firstString(v, "language");
    c.version = static_cast<int32_t>(firstInt(v, "version"));
    return c;
}

// ---------------------------------------------------------------- ConsumerConnection
JsonValue ConsumerConnection::toJson() const {
    JsonValue v = JsonValue::makeObject();
    JsonValue arr = JsonValue::makeArray();
    for (const auto& c : connectionSet) arr.pushArray(c.toJson());
    v.set("connectionSet", arr);
    v.set("subscriptionTable", subscriptionTable.isNull() ? JsonValue::makeObject()
                                                          : subscriptionTable);
    v.set("consumeType", JsonValue::makeString(consumeType));
    v.set("messageModel", JsonValue::makeString(messageModel));
    v.set("consumeFromWhere", JsonValue::makeString(consumeFromWhere));
    return v;
}

ConsumerConnection ConsumerConnection::fromJson(const JsonValue& v) {
    ConsumerConnection cc;
    const JsonValue* arr = v.find("connectionSet");
    if (arr != nullptr && arr->isArray()) {
        for (size_t i = 0; i < arr->size(); ++i) {
            cc.connectionSet.push_back(Connection::fromJson(arr->at(i)));
        }
    }
    cc.subscriptionTable = rawSub(v, "subscriptionTable");
    cc.consumeType = firstString(v, "consumeType");
    cc.messageModel = firstString(v, "messageModel");
    cc.consumeFromWhere = firstString(v, "consumeFromWhere");
    return cc;
}

Bytes ConsumerConnection::encode() const { return RemotingSerializable::encode(toJson()); }

bool ConsumerConnection::decode(const Bytes& data, ConsumerConnection& out) {
    JsonValue v;
    if (!RemotingSerializable::decode(data, v)) return false;
    out = fromJson(v);
    return true;
}

// ---------------------------------------------------------------- ProducerConnection
JsonValue ProducerConnection::toJson() const {
    JsonValue v = JsonValue::makeObject();
    JsonValue arr = JsonValue::makeArray();
    for (const auto& c : connectionSet) arr.pushArray(c.toJson());
    v.set("connectionSet", arr);
    return v;
}

ProducerConnection ProducerConnection::fromJson(const JsonValue& v) {
    ProducerConnection pc;
    const JsonValue* arr = v.find("connectionSet");
    if (arr != nullptr && arr->isArray()) {
        for (size_t i = 0; i < arr->size(); ++i) {
            pc.connectionSet.push_back(Connection::fromJson(arr->at(i)));
        }
    }
    return pc;
}

Bytes ProducerConnection::encode() const { return RemotingSerializable::encode(toJson()); }

bool ProducerConnection::decode(const Bytes& data, ProducerConnection& out) {
    JsonValue v;
    if (!RemotingSerializable::decode(data, v)) return false;
    out = fromJson(v);
    return true;
}

// ---------------------------------------------------------------- ConsumerRunningInfo
JsonValue ConsumeStatus::toJson() const {
    JsonValue v = JsonValue::makeObject();
    v.set("pullRT", JsonValue::makeDouble(pullRT));
    v.set("pullTPS", JsonValue::makeDouble(pullTPS));
    v.set("consumeRT", JsonValue::makeDouble(consumeRT));
    v.set("consumeOKTPS", JsonValue::makeDouble(consumeOKTPS));
    v.set("consumeFailedTPS", JsonValue::makeDouble(consumeFailedTPS));
    v.set("consumeFailedMsgs", JsonValue::makeInt(consumeFailedMsgs));
    return v;
}

ConsumeStatus ConsumeStatus::fromJson(const JsonValue& v) {
    ConsumeStatus cs;
    cs.pullRT = v.get("pullRT").doubleValue();
    cs.pullTPS = v.get("pullTPS").doubleValue();
    cs.consumeRT = v.get("consumeRT").doubleValue();
    cs.consumeOKTPS = v.get("consumeOKTPS").doubleValue();
    cs.consumeFailedTPS = v.get("consumeFailedTPS").doubleValue();
    cs.consumeFailedMsgs = v.get("consumeFailedMsgs").intValue();
    return cs;
}

JsonValue ConsumerRunningInfo::toJson() const {
    JsonValue v = JsonValue::makeObject();
    v.set("properties", makeStringMap(properties));
    v.set("subscriptionSet", subscriptionSet.isNull() ? JsonValue::makeArray() : subscriptionSet);
    v.set("mqTable", mqTable.isNull() ? JsonValue::makeObject() : mqTable);
    // Python/Java 侧 307 应答总是带 mqPopTable/statusTable/userConsumerInfo；
    // 缺省时输出空对象，保持三语言报文形状一致（fastjson2 会忽略空 map 吗？不会，
    // Java 的字段非 null 就序列化，空 TreeMap 输出 {}）。
    v.set("mqPopTable", mqPopTable.isNull() ? JsonValue::makeObject() : mqPopTable);
    v.set("statusTable", statusTable.isNull() ? JsonValue::makeObject() : statusTable);
    v.set("userConsumerInfo", userConsumerInfo.isNull() ? JsonValue::makeObject() : userConsumerInfo);
    if (hasJstack) v.set("jstack", JsonValue::makeString(jstack));
    return v;
}

ConsumerRunningInfo ConsumerRunningInfo::fromJson(const JsonValue& v) {
    ConsumerRunningInfo ri;
    ri.properties = readStringMap(v.get("properties"));
    ri.subscriptionSet = rawSub(v, "subscriptionSet");
    ri.mqTable = rawSub(v, "mqTable");
    ri.mqPopTable = rawSub(v, "mqPopTable");
    ri.statusTable = rawSub(v, "statusTable");
    ri.userConsumerInfo = rawSub(v, "userConsumerInfo");
    std::string s;
    if (v.tryGetString("jstack", s)) {
        ri.jstack = s;
        ri.hasJstack = true;
    }
    return ri;
}

Bytes ConsumerRunningInfo::encode() const { return RemotingSerializable::encode(toJson()); }

bool ConsumerRunningInfo::decode(const Bytes& data, ConsumerRunningInfo& out) {
    JsonValue v;
    if (!RemotingSerializable::decode(data, v)) return false;
    out = fromJson(v);
    return true;
}

// ---------------------------------------------------------------- ConsumeStatsList
JsonValue ConsumeStatsList::toJson() const {
    JsonValue v = JsonValue::makeObject();
    v.set("consumeStatsList", statsList.isNull() ? JsonValue::makeArray() : statsList);
    if (hasBrokerAddr) v.set("brokerAddr", JsonValue::makeString(brokerAddr));
    v.set("totalDiff", JsonValue::makeInt(totalDiff));
    v.set("totalInflightDiff", JsonValue::makeInt(totalInflightDiff));
    return v;
}

ConsumeStatsList ConsumeStatsList::fromJson(const JsonValue& v) {
    ConsumeStatsList sl;
    sl.statsList = rawSub(v, "consumeStatsList");
    std::string s;
    if (v.tryGetString("brokerAddr", s)) {
        sl.brokerAddr = s;
        sl.hasBrokerAddr = true;
    }
    v.tryGetInt("totalDiff", sl.totalDiff);
    v.tryGetInt("totalInflightDiff", sl.totalInflightDiff);
    return sl;
}

Bytes ConsumeStatsList::encode() const { return RemotingSerializable::encode(toJson()); }

bool ConsumeStatsList::decode(const Bytes& data, ConsumeStatsList& out) {
    JsonValue v;
    if (!RemotingSerializable::decode(data, v)) return false;
    out = fromJson(v);
    return true;
}

// ---------------------------------------------------------------- ResetOffsetBody
JsonValue ResetOffsetBody::toJson() const {
    JsonValue v = JsonValue::makeObject();
    JsonValue table = JsonValue::makeObject();
    for (const auto& kv : offsetTable) {
        table.set(messageQueueKey(kv.first), JsonValue::makeInt(kv.second));
    }
    v.set("offsetTable", table);
    return v;
}

ResetOffsetBody ResetOffsetBody::fromJson(const JsonValue& v) {
    ResetOffsetBody b;
    for (const auto& kv : decodeMessageQueueMap(rawSub(v, "offsetTable"))) {
        if (kv.second.isNumber()) {
            b.offsetTable[kv.first] = kv.second.intValue();
        }
    }
    return b;
}

Bytes ResetOffsetBody::encode() const { return RemotingSerializable::encode(toJson()); }

bool ResetOffsetBody::decode(const Bytes& data, ResetOffsetBody& out) {
    JsonValue v;
    if (!RemotingSerializable::decode(data, v)) return false;
    out = fromJson(v);
    return true;
}

JsonValue GetConsumerStatusBody::toJson() const {
    JsonValue v = JsonValue::makeObject();
    JsonValue table = JsonValue::makeObject();
    for (const auto& kv : messageQueueTable) {
        table.set(messageQueueKey(kv.first), JsonValue::makeInt(kv.second));
    }
    v.set("messageQueueTable", table);
    // Java 保留的废弃字段 consumerTable（clientId -> 位点表）：始终带空对象，
    // 与 Python/Java 序列化形状一致。
    v.set("consumerTable", JsonValue::makeObject());
    return v;
}

Bytes GetConsumerStatusBody::encode() const { return RemotingSerializable::encode(toJson()); }

JsonValue ConsumeMessageDirectlyResult::toJson() const {
    JsonValue v = JsonValue::makeObject();
    v.set("order", JsonValue::makeBool(order));
    v.set("autoCommit", JsonValue::makeBool(autoCommit));
    v.set("consumeResult", consumeResult.empty() ? JsonValue::makeNull()
                                                 : JsonValue::makeString(consumeResult));
    v.set("remark", remark.empty() ? JsonValue::makeNull() : JsonValue::makeString(remark));
    v.set("spentTimeMills", JsonValue::makeInt(spentTimeMills));
    return v;
}

ConsumeMessageDirectlyResult ConsumeMessageDirectlyResult::fromJson(const JsonValue& v) {
    ConsumeMessageDirectlyResult r;
    r.order = rawSub(v, "order").boolValue();
    r.autoCommit = rawSub(v, "autoCommit").boolValue();
    JsonValue cr = rawSub(v, "consumeResult");
    if (cr.isString()) r.consumeResult = cr.stringValue();
    JsonValue rk = rawSub(v, "remark");
    if (rk.isString()) r.remark = rk.stringValue();
    r.spentTimeMills = rawSub(v, "spentTimeMills").intValue();
    return r;
}

Bytes ConsumeMessageDirectlyResult::encode() const { return RemotingSerializable::encode(toJson()); }

bool ConsumeMessageDirectlyResult::decode(const Bytes& data, ConsumeMessageDirectlyResult& out) {
    JsonValue v;
    if (!RemotingSerializable::decode(data, v)) return false;
    out = fromJson(v);
    return true;
}

}  // namespace rocketmq
