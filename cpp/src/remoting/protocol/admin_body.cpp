// Admin response body implementations.
#include "rocketmq/remoting/protocol/admin_body.h"

#include <string>

#include "rocketmq/remoting/protocol/serialize.h"

namespace rocketmq {

namespace {

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

double getDouble(const JsonValue& v, const char* key, double def) {
    const JsonValue* p = v.find(key);
    return (p != nullptr && p->isNumber()) ? p->doubleValue() : def;
}

JsonValue rawSub(const JsonValue& v, const char* key) {
    const JsonValue* p = v.find(key);
    return p != nullptr ? *p : JsonValue::makeNull();
}

// Encode a <MessageQueue, T> map as the fastjson2-style offsetTable object
template <typename T>
JsonValue encodeMqKeyedMap(const std::map<MessageQueue, T>& m) {
    JsonValue table = JsonValue::makeObject();
    for (const auto& kv : m) {
        table.set(messageQueueKey(kv.first), kv.second.toJson());
    }
    return table;
}

}  // namespace

// ---------------------------------------------------------------- MessageQueue as a map key
std::string messageQueueKey(const MessageQueue& mq) {
    // Keys in alphabetical order (fastjson2 behaviour): brokerName, queueId, topic
    JsonValue v = JsonValue::makeObject();
    v.set("brokerName", JsonValue::makeString(mq.brokerName));
    v.set("queueId", JsonValue::makeInt(mq.queueId));
    v.set("topic", JsonValue::makeString(mq.topic));
    return v.dump();
}

bool parseMessageQueueKey(const std::string& key, MessageQueue& out) {
    // Check for an inline object key (starting with '{') first; a plain string key is not accepted here
    size_t b = 0;
    while (b < key.size() && (key[b] == ' ' || key[b] == '\t')) ++b;
    if (b >= key.size() || key[b] != '{') return false;

    JsonValue v;
    if (!jsonParse(key, v, nullptr) || !v.isObject()) return false;
    out.topic = getStr(v, "topic", "");
    out.brokerName = getStr(v, "brokerName", "");
    out.queueId = static_cast<int32_t>(getInt(v, "queueId", 0));
    return true;
}

std::map<MessageQueue, JsonValue> decodeMessageQueueMap(const JsonValue& raw) {
    std::map<MessageQueue, JsonValue> result;
    if (!raw.isObject()) return result;
    for (const auto& kv : raw.objectItems()) {
        MessageQueue mq;
        if (parseMessageQueueKey(kv.first, mq)) {
            result[mq] = kv.second;
        }
    }
    return result;
}

// ---------------------------------------------------------------- TopicOffset
JsonValue TopicOffset::toJson() const {
    JsonValue v = JsonValue::makeObject();
    v.set("minOffset", JsonValue::makeInt(minOffset));
    v.set("maxOffset", JsonValue::makeInt(maxOffset));
    v.set("lastUpdateTimestamp", JsonValue::makeInt(lastUpdateTimestamp));
    return v;
}

TopicOffset TopicOffset::fromJson(const JsonValue& v) {
    TopicOffset o;
    o.minOffset = getInt(v, "minOffset", 0);
    o.maxOffset = getInt(v, "maxOffset", 0);
    o.lastUpdateTimestamp = getInt(v, "lastUpdateTimestamp", 0);
    return o;
}

// ---------------------------------------------------------------- TopicStatsTable
int64_t TopicStatsTable::totalMaxOffset() const {
    int64_t sum = 0;
    for (const auto& kv : offsetTable) sum += kv.second.maxOffset;
    return sum;
}

JsonValue TopicStatsTable::toJson() const {
    JsonValue v = JsonValue::makeObject();
    v.set("offsetTable", encodeMqKeyedMap(offsetTable));
    v.set("topicPutTps", JsonValue::makeDouble(topicPutTps));
    return v;
}

TopicStatsTable TopicStatsTable::fromJson(const JsonValue& v) {
    TopicStatsTable t;
    for (const auto& kv : decodeMessageQueueMap(rawSub(v, "offsetTable"))) {
        t.offsetTable[kv.first] = TopicOffset::fromJson(kv.second);
    }
    t.topicPutTps = getDouble(v, "topicPutTps", 0.0);
    return t;
}

Bytes TopicStatsTable::encode() const { return RemotingSerializable::encode(toJson()); }

bool TopicStatsTable::decode(const Bytes& data, TopicStatsTable& out) {
    JsonValue v;
    if (!RemotingSerializable::decode(data, v)) return false;
    out = fromJson(v);
    return true;
}

// ---------------------------------------------------------------- OffsetWrapper
JsonValue OffsetWrapper::toJson() const {
    JsonValue v = JsonValue::makeObject();
    v.set("brokerOffset", JsonValue::makeInt(brokerOffset));
    v.set("consumerOffset", JsonValue::makeInt(consumerOffset));
    v.set("lastTimestamp", JsonValue::makeInt(lastTimestamp));
    v.set("pullOffset", JsonValue::makeInt(pullOffset));
    return v;
}

OffsetWrapper OffsetWrapper::fromJson(const JsonValue& v) {
    OffsetWrapper o;
    o.brokerOffset = getInt(v, "brokerOffset", 0);
    o.consumerOffset = getInt(v, "consumerOffset", 0);
    o.lastTimestamp = getInt(v, "lastTimestamp", 0);
    o.pullOffset = getInt(v, "pullOffset", 0);
    return o;
}

// ---------------------------------------------------------------- ConsumeStats
int64_t ConsumeStats::totalLag() const {
    int64_t sum = 0;
    for (const auto& kv : offsetTable) sum += kv.second.lag();
    return sum;
}

JsonValue ConsumeStats::toJson() const {
    JsonValue v = JsonValue::makeObject();
    v.set("offsetTable", encodeMqKeyedMap(offsetTable));
    v.set("consumeTps", JsonValue::makeDouble(consumeTps));
    return v;
}

ConsumeStats ConsumeStats::fromJson(const JsonValue& v) {
    ConsumeStats c;
    for (const auto& kv : decodeMessageQueueMap(rawSub(v, "offsetTable"))) {
        c.offsetTable[kv.first] = OffsetWrapper::fromJson(kv.second);
    }
    c.consumeTps = getDouble(v, "consumeTps", 0.0);
    return c;
}

Bytes ConsumeStats::encode() const { return RemotingSerializable::encode(toJson()); }

bool ConsumeStats::decode(const Bytes& data, ConsumeStats& out) {
    JsonValue v;
    if (!RemotingSerializable::decode(data, v)) return false;
    out = fromJson(v);
    return true;
}

// ---------------------------------------------------------------- TopicConfigSerializeWrapper
JsonValue TopicConfigSerializeWrapper::toJson() const {
    JsonValue v = JsonValue::makeObject();
    v.set("dataVersion", dataVersion.isNull() ? JsonValue::makeObject() : dataVersion);
    JsonValue table = JsonValue::makeObject();
    for (const auto& kv : topicConfigTable) {
        table.set(kv.first, kv.second.toJson());
    }
    v.set("topicConfigTable", table);
    return v;
}

TopicConfigSerializeWrapper TopicConfigSerializeWrapper::fromJson(const JsonValue& v) {
    TopicConfigSerializeWrapper w;
    const JsonValue* table = v.find("topicConfigTable");
    if (table != nullptr && table->isObject()) {
        for (const auto& kv : table->objectItems()) {
            w.topicConfigTable[kv.first] = TopicConfig::fromJson(kv.second);
        }
    }
    w.dataVersion = rawSub(v, "dataVersion");
    return w;
}

Bytes TopicConfigSerializeWrapper::encode() const {
    return RemotingSerializable::encode(toJson());
}

bool TopicConfigSerializeWrapper::decode(const Bytes& data, TopicConfigSerializeWrapper& out) {
    JsonValue v;
    if (!RemotingSerializable::decode(data, v)) return false;
    out = fromJson(v);
    return true;
}

// ---------------------------------------------------------------- ConsumeQueueData
JsonValue ConsumeQueueData::toJson() const {
    JsonValue v = JsonValue::makeObject();
    v.set("physicOffset", JsonValue::makeInt(physicOffset));
    v.set("physicSize", JsonValue::makeInt(physicSize));
    v.set("tagsCode", JsonValue::makeInt(tagsCode));
    v.set("eval", JsonValue::makeBool(eval));
    if (hasBitMap) v.set("bitMap", JsonValue::makeString(bitMap));
    else v.set("bitMap", JsonValue::makeNull());
    // Broker convention: null fields are not serialized (the has* flags express null explicitly here)
    if (hasExtendDataJson) v.set("extendDataJson", JsonValue::makeString(extendDataJson));
    if (hasMsg) v.set("msg", JsonValue::makeString(msg));
    return v;
}

ConsumeQueueData ConsumeQueueData::fromJson(const JsonValue& v) {
    ConsumeQueueData d;
    d.physicOffset = getInt(v, "physicOffset", 0);
    d.physicSize = getInt(v, "physicSize", 0);
    d.tagsCode = getInt(v, "tagsCode", 0);
    d.eval = getBool(v, "eval", false);
    std::string tmp;
    if (v.tryGetString("extendDataJson", tmp)) {
        d.extendDataJson = tmp;
        d.hasExtendDataJson = true;
    }
    if (v.tryGetString("bitMap", tmp)) {
        d.bitMap = tmp;
        d.hasBitMap = true;
    }
    if (v.tryGetString("msg", tmp)) {
        d.msg = tmp;
        d.hasMsg = true;
    }
    return d;
}

// ---------------------------------------------------------------- QueryConsumeQueueResponseBody
JsonValue QueryConsumeQueueResponseBody::toJson() const {
    JsonValue v = JsonValue::makeObject();
    v.set("maxQueueIndex", JsonValue::makeInt(maxQueueIndex));
    v.set("minQueueIndex", JsonValue::makeInt(minQueueIndex));
    if (!subscriptionData.isNull()) v.set("subscriptionData", subscriptionData);
    if (hasFilterData) v.set("filterData", JsonValue::makeString(filterData));
    if (hasQueueData) {
        JsonValue arr = JsonValue::makeArray();
        for (const auto& q : queueData) arr.pushArray(q.toJson());
        v.set("queueData", arr);
    }
    return v;
}

QueryConsumeQueueResponseBody QueryConsumeQueueResponseBody::fromJson(const JsonValue& v) {
    QueryConsumeQueueResponseBody b;
    b.subscriptionData = rawSub(v, "subscriptionData");
    std::string tmp;
    if (v.tryGetString("filterData", tmp)) {
        b.filterData = tmp;
        b.hasFilterData = true;
    }
    const JsonValue* raw = v.find("queueData");
    if (raw != nullptr && raw->isArray()) {
        b.hasQueueData = true;
        for (size_t i = 0; i < raw->size(); ++i) {
            b.queueData.push_back(ConsumeQueueData::fromJson(raw->at(i)));
        }
    }
    b.maxQueueIndex = getInt(v, "maxQueueIndex", 0);
    b.minQueueIndex = getInt(v, "minQueueIndex", 0);
    return b;
}

Bytes QueryConsumeQueueResponseBody::encode() const {
    return RemotingSerializable::encode(toJson());
}

bool QueryConsumeQueueResponseBody::decode(const Bytes& data,
                                          QueryConsumeQueueResponseBody& out) {
    JsonValue v;
    if (!RemotingSerializable::decode(data, v)) return false;
    out = fromJson(v);
    return true;
}

// ---------------------------------------------------------------- AUTH user / ACL management
namespace {

// Reads a JSON array of strings; present reports whether the key itself was an array
std::vector<std::string> getStrList(const JsonValue& v, const char* key, bool& present) {
    std::vector<std::string> out;
    const JsonValue* p = v.find(key);
    present = (p != nullptr && p->isArray());
    if (!present) return out;
    for (size_t i = 0; i < p->size(); ++i) {
        if (p->at(i).isString()) out.push_back(p->at(i).stringValue());
    }
    return out;
}

JsonValue encodeStrList(const std::vector<std::string>& items) {
    JsonValue arr = JsonValue::makeArray();
    for (const std::string& s : items) arr.pushArray(JsonValue::makeString(s));
    return arr;
}

}  // namespace

JsonValue UserInfo::toJson() const {
    // Key order is the protocol field order; an unset field contributes no key at all
    JsonValue v = JsonValue::makeObject();
    if (!username.empty()) v.set("username", JsonValue::makeString(username));
    if (hasPassword) v.set("password", JsonValue::makeString(password));
    if (hasUserType) v.set("userType", JsonValue::makeString(userType));
    if (hasUserStatus) v.set("userStatus", JsonValue::makeString(userStatus));
    return v;
}

UserInfo UserInfo::fromJson(const JsonValue& v) {
    UserInfo u;
    u.username = getStr(v, "username", "");
    std::string tmp;
    if (v.tryGetString("password", tmp)) {
        u.password = tmp;
        u.hasPassword = true;
    }
    if (v.tryGetString("userType", tmp)) {
        u.userType = tmp;
        u.hasUserType = true;
    }
    if (v.tryGetString("userStatus", tmp)) {
        u.userStatus = tmp;
        u.hasUserStatus = true;
    }
    return u;
}

std::vector<UserInfo> UserInfo::listFromJson(const JsonValue& v) {
    std::vector<UserInfo> out;
    if (!v.isArray()) return out;
    for (size_t i = 0; i < v.size(); ++i) {
        if (v.at(i).isObject()) out.push_back(UserInfo::fromJson(v.at(i)));
    }
    return out;
}

Bytes UserInfo::encode() const { return RemotingSerializable::encode(toJson()); }

bool UserInfo::decode(const Bytes& data, UserInfo& out) {
    JsonValue v;
    if (!RemotingSerializable::decode(data, v) || !v.isObject()) return false;
    out = fromJson(v);
    return true;
}

JsonValue AclEntry::toJson() const {
    JsonValue v = JsonValue::makeObject();
    if (!resource.empty()) v.set("resource", JsonValue::makeString(resource));
    if (hasActions) v.set("actions", encodeStrList(actions));
    if (hasSourceIps) v.set("sourceIps", encodeStrList(sourceIps));
    if (hasDecision) v.set("decision", JsonValue::makeString(decision));
    return v;
}

AclEntry AclEntry::fromJson(const JsonValue& v) {
    AclEntry e;
    e.resource = getStr(v, "resource", "");
    e.actions = getStrList(v, "actions", e.hasActions);
    e.sourceIps = getStrList(v, "sourceIps", e.hasSourceIps);
    std::string tmp;
    if (v.tryGetString("decision", tmp)) {
        e.decision = tmp;
        e.hasDecision = true;
    }
    return e;
}

JsonValue AclPolicy::toJson() const {
    JsonValue v = JsonValue::makeObject();
    if (hasPolicyType) v.set("policyType", JsonValue::makeString(policyType));
    if (!entries.empty()) {
        JsonValue arr = JsonValue::makeArray();
        for (const AclEntry& e : entries) arr.pushArray(e.toJson());
        v.set("entries", arr);
    }
    return v;
}

AclPolicy AclPolicy::fromJson(const JsonValue& v) {
    AclPolicy p;
    std::string tmp;
    if (v.tryGetString("policyType", tmp)) {
        p.policyType = tmp;
        p.hasPolicyType = true;
    }
    const JsonValue* raw = v.find("entries");
    if (raw != nullptr && raw->isArray()) {
        for (size_t i = 0; i < raw->size(); ++i) {
            if (raw->at(i).isObject()) p.entries.push_back(AclEntry::fromJson(raw->at(i)));
        }
    }
    return p;
}

JsonValue AclInfo::toJson() const {
    JsonValue v = JsonValue::makeObject();
    if (!subject.empty()) v.set("subject", JsonValue::makeString(subject));
    if (!policies.empty()) {
        JsonValue arr = JsonValue::makeArray();
        for (const AclPolicy& p : policies) arr.pushArray(p.toJson());
        v.set("policies", arr);
    }
    return v;
}

AclInfo AclInfo::fromJson(const JsonValue& v) {
    AclInfo a;
    a.subject = getStr(v, "subject", "");
    const JsonValue* raw = v.find("policies");
    if (raw != nullptr && raw->isArray()) {
        for (size_t i = 0; i < raw->size(); ++i) {
            if (raw->at(i).isObject()) a.policies.push_back(AclPolicy::fromJson(raw->at(i)));
        }
    }
    return a;
}

std::vector<AclInfo> AclInfo::listFromJson(const JsonValue& v) {
    std::vector<AclInfo> out;
    if (!v.isArray()) return out;
    for (size_t i = 0; i < v.size(); ++i) {
        if (v.at(i).isObject()) out.push_back(AclInfo::fromJson(v.at(i)));
    }
    return out;
}

Bytes AclInfo::encode() const { return RemotingSerializable::encode(toJson()); }

bool AclInfo::decode(const Bytes& data, AclInfo& out) {
    JsonValue v;
    if (!RemotingSerializable::decode(data, v) || !v.isObject()) return false;
    out = fromJson(v);
    return true;
}

// ---------------------------------------------------------------- ReplicaIdentity
JsonValue ReplicaIdentity::toJson() const {
    JsonValue v = JsonValue::makeObject();
    v.set("brokerName", JsonValue::makeString(brokerName));
    v.set("brokerId", JsonValue::makeInt(brokerId));
    if (hasBrokerAddress) v.set("brokerAddress", JsonValue::makeString(brokerAddress));
    if (hasAlive) v.set("alive", JsonValue::makeBool(alive));
    return v;
}

ReplicaIdentity ReplicaIdentity::fromJson(const JsonValue& v) {
    ReplicaIdentity r;
    r.brokerName = getStr(v, "brokerName", "");
    r.brokerId = getInt(v, "brokerId", 0);
    std::string tmp;
    if (v.tryGetString("brokerAddress", tmp)) {
        r.brokerAddress = tmp;
        r.hasBrokerAddress = true;
    }
    if (v.find("alive") != nullptr && !v.find("alive")->isNull()) {
        r.alive = getBool(v, "alive", false);
        r.hasAlive = true;
    }
    return r;
}

Bytes ReplicaIdentity::encode() const { return RemotingSerializable::encode(toJson()); }

bool ReplicaIdentity::decode(const Bytes& data, ReplicaIdentity& out) {
    JsonValue v;
    if (!RemotingSerializable::decode(data, v) || !v.isObject()) return false;
    out = fromJson(v);
    return true;
}

// ---------------------------------------------------------------- ReplicasInfo
JsonValue ReplicasInfo::toJson() const {
    JsonValue v = JsonValue::makeObject();
    if (hasMasterBrokerId) v.set("masterBrokerId", JsonValue::makeInt(masterBrokerId));
    if (hasMasterAddress) v.set("masterAddress", JsonValue::makeString(masterAddress));
    v.set("masterEpoch", JsonValue::makeInt(masterEpoch));
    v.set("syncStateSetEpoch", JsonValue::makeInt(syncStateSetEpoch));
    JsonValue inSync = JsonValue::makeArray();
    for (const ReplicaIdentity& r : inSyncReplicas) inSync.pushArray(r.toJson());
    v.set("inSyncReplicas", inSync);
    JsonValue notInSync = JsonValue::makeArray();
    for (const ReplicaIdentity& r : notInSyncReplicas) notInSync.pushArray(r.toJson());
    v.set("notInSyncReplicas", notInSync);
    return v;
}

ReplicasInfo ReplicasInfo::fromJson(const JsonValue& v) {
    ReplicasInfo r;
    if (v.find("masterBrokerId") != nullptr && v.find("masterBrokerId")->isNumber()) {
        r.masterBrokerId = getInt(v, "masterBrokerId", 0);
        r.hasMasterBrokerId = true;
    }
    std::string tmp;
    if (v.tryGetString("masterAddress", tmp)) {
        r.masterAddress = tmp;
        r.hasMasterAddress = true;
    }
    r.masterEpoch = static_cast<int32_t>(getInt(v, "masterEpoch", 0));
    r.syncStateSetEpoch = static_cast<int32_t>(getInt(v, "syncStateSetEpoch", 0));
    const JsonValue* inSync = v.find("inSyncReplicas");
    if (inSync != nullptr && inSync->isArray()) {
        for (size_t i = 0; i < inSync->size(); ++i) {
            if (inSync->at(i).isObject()) r.inSyncReplicas.push_back(ReplicaIdentity::fromJson(inSync->at(i)));
        }
    }
    const JsonValue* notInSync = v.find("notInSyncReplicas");
    if (notInSync != nullptr && notInSync->isArray()) {
        for (size_t i = 0; i < notInSync->size(); ++i) {
            if (notInSync->at(i).isObject()) {
                r.notInSyncReplicas.push_back(ReplicaIdentity::fromJson(notInSync->at(i)));
            }
        }
    }
    return r;
}

Bytes ReplicasInfo::encode() const { return RemotingSerializable::encode(toJson()); }

bool ReplicasInfo::decode(const Bytes& data, ReplicasInfo& out) {
    JsonValue v;
    if (!RemotingSerializable::decode(data, v) || !v.isObject()) return false;
    out = fromJson(v);
    return true;
}

// ---------------------------------------------------------------- BrokerReplicasInfo
JsonValue BrokerReplicasInfo::toJson() const {
    JsonValue v = JsonValue::makeObject();
    JsonValue table = JsonValue::makeObject();
    for (const auto& kv : replicasInfoTable) {
        table.set(kv.first, kv.second.toJson());
    }
    v.set("replicasInfoTable", table);
    return v;
}

BrokerReplicasInfo BrokerReplicasInfo::fromJson(const JsonValue& v) {
    BrokerReplicasInfo b;
    const JsonValue* table = v.find("replicasInfoTable");
    if (table == nullptr || !table->isObject()) return b;
    for (const auto& kv : table->objectItems()) {
        if (kv.second.isObject()) {
            b.replicasInfoTable[kv.first] = ReplicasInfo::fromJson(kv.second);
        }
    }
    return b;
}

Bytes BrokerReplicasInfo::encode() const { return RemotingSerializable::encode(toJson()); }

bool BrokerReplicasInfo::decode(const Bytes& data, BrokerReplicasInfo& out) {
    JsonValue v;
    if (!RemotingSerializable::decode(data, v) || !v.isObject()) return false;
    out = fromJson(v);
    return true;
}

// ---------------------------------------------------------------- CheckRocksdbCqWriteResult
JsonValue CheckRocksdbCqWriteResult::toJson() const {
    JsonValue v = JsonValue::makeObject();
    v.set("checkResult", JsonValue::makeString(checkResult));
    v.set("checkStatus", JsonValue::makeInt(checkStatus));
    return v;
}

CheckRocksdbCqWriteResult CheckRocksdbCqWriteResult::fromJson(const JsonValue& v) {
    CheckRocksdbCqWriteResult r;
    r.checkResult = getStr(v, "checkResult", "");
    r.checkStatus = static_cast<int32_t>(getInt(v, "checkStatus", 0));
    return r;
}

Bytes CheckRocksdbCqWriteResult::encode() const { return RemotingSerializable::encode(toJson()); }

bool CheckRocksdbCqWriteResult::decode(const Bytes& data, CheckRocksdbCqWriteResult& out) {
    JsonValue v;
    if (!RemotingSerializable::decode(data, v) || !v.isObject()) return false;
    out = fromJson(v);
    return true;
}

// ---------------------------------------------------------------- BrokerOperatorResult
JsonValue BrokerOperatorResult::toJson() const {
    JsonValue v = JsonValue::makeObject();
    JsonValue success = JsonValue::makeArray();
    for (const std::string& s : successList) success.pushArray(JsonValue::makeString(s));
    v.set("successList", success);
    JsonValue failure = JsonValue::makeArray();
    for (const std::string& s : failureList) failure.pushArray(JsonValue::makeString(s));
    v.set("failureList", failure);
    return v;
}

BrokerOperatorResult BrokerOperatorResult::fromJson(const JsonValue& v) {
    BrokerOperatorResult r;
    const JsonValue* success = v.find("successList");
    if (success != nullptr && success->isArray()) {
        for (size_t i = 0; i < success->size(); ++i) {
            if (success->at(i).isString()) r.successList.push_back(success->at(i).stringValue());
        }
    }
    const JsonValue* failure = v.find("failureList");
    if (failure != nullptr && failure->isArray()) {
        for (size_t i = 0; i < failure->size(); ++i) {
            if (failure->at(i).isString()) r.failureList.push_back(failure->at(i).stringValue());
        }
    }
    return r;
}

Bytes BrokerOperatorResult::encode() const { return RemotingSerializable::encode(toJson()); }

bool BrokerOperatorResult::decode(const Bytes& data, BrokerOperatorResult& out) {
    JsonValue v;
    if (!RemotingSerializable::decode(data, v) || !v.isObject()) return false;
    out = fromJson(v);
    return true;
}

// ---------------------------------------------------------------- #47 ops RPC group

namespace {

int64_t keyToI64(const std::string& key) {
    try {
        return std::stoll(key);
    } catch (...) {
        return 0;
    }
}

}  // namespace

// ---------------------------------------------------------------- QueryCorrectionOffsetBody
JsonValue QueryCorrectionOffsetBody::toJson() const {
    JsonValue table = JsonValue::makeObject();
    for (const auto& kv : correctionOffsets) {
        table.set(std::to_string(kv.first), JsonValue::makeInt(kv.second));
    }
    JsonValue v = JsonValue::makeObject();
    v.set("correctionOffsets", table);
    return v;
}

QueryCorrectionOffsetBody QueryCorrectionOffsetBody::fromJson(const JsonValue& v) {
    QueryCorrectionOffsetBody b;
    const JsonValue* table = v.find("correctionOffsets");
    if (table != nullptr && table->isObject()) {
        for (const auto& kv : table->objectItems()) {
            b.correctionOffsets[static_cast<int32_t>(keyToI64(kv.first))] = kv.second.intValue();
        }
    }
    return b;
}

Bytes QueryCorrectionOffsetBody::encode() const { return RemotingSerializable::encode(toJson()); }

bool QueryCorrectionOffsetBody::decode(const Bytes& data, QueryCorrectionOffsetBody& out) {
    JsonValue v;
    if (!RemotingSerializable::decode(data, v) || !v.isObject()) return false;
    out = fromJson(v);
    return true;
}

// ---------------------------------------------------------------- ProducerInfo / ProducerTableInfo
JsonValue ProducerInfo::toJson() const {
    JsonValue v = JsonValue::makeObject();
    v.set("clientId", JsonValue::makeString(clientId));
    v.set("remoteIP", JsonValue::makeString(remoteIP));
    v.set("language", JsonValue::makeString(language));
    v.set("version", JsonValue::makeInt(version));
    v.set("lastUpdateTimestamp", JsonValue::makeInt(lastUpdateTimestamp));
    return v;
}

ProducerInfo ProducerInfo::fromJson(const JsonValue& v) {
    ProducerInfo p;
    p.clientId = getStr(v, "clientId", "");
    p.remoteIP = getStr(v, "remoteIP", "");
    p.language = getStr(v, "language", "");
    p.version = getInt(v, "version", 0);
    p.lastUpdateTimestamp = getInt(v, "lastUpdateTimestamp", 0);
    return p;
}

JsonValue ProducerTableInfo::toJson() const {
    JsonValue table = JsonValue::makeObject();
    for (const auto& kv : data) {
        JsonValue arr = JsonValue::makeArray();
        for (const ProducerInfo& p : kv.second) arr.pushArray(p.toJson());
        table.set(kv.first, arr);
    }
    JsonValue v = JsonValue::makeObject();
    v.set("data", table);
    return v;
}

ProducerTableInfo ProducerTableInfo::fromJson(const JsonValue& v) {
    ProducerTableInfo t;
    const JsonValue* data = v.find("data");
    if (data != nullptr && data->isObject()) {
        for (const auto& kv : data->objectItems()) {
            std::vector<ProducerInfo> list;
            if (kv.second.isArray()) {
                for (size_t i = 0; i < kv.second.size(); ++i) {
                    if (kv.second.at(i).isObject()) list.push_back(ProducerInfo::fromJson(kv.second.at(i)));
                }
            }
            t.data[kv.first] = std::move(list);
        }
    }
    return t;
}

Bytes ProducerTableInfo::encode() const { return RemotingSerializable::encode(toJson()); }

bool ProducerTableInfo::decode(const Bytes& data, ProducerTableInfo& out) {
    JsonValue v;
    if (!RemotingSerializable::decode(data, v) || !v.isObject()) return false;
    out = fromJson(v);
    return true;
}

// ---------------------------------------------------------------- HA runtime info
JsonValue HAConnectionRuntimeInfo::toJson() const {
    JsonValue v = JsonValue::makeObject();
    v.set("addr", JsonValue::makeString(addr));
    v.set("slaveAckOffset", JsonValue::makeInt(slaveAckOffset));
    v.set("diff", JsonValue::makeInt(diff));
    v.set("inSync", JsonValue::makeBool(inSync));
    v.set("transferredByteInSecond", JsonValue::makeInt(transferredByteInSecond));
    v.set("transferFromWhere", JsonValue::makeInt(transferFromWhere));
    return v;
}

HAConnectionRuntimeInfo HAConnectionRuntimeInfo::fromJson(const JsonValue& v) {
    HAConnectionRuntimeInfo c;
    c.addr = getStr(v, "addr", "");
    c.slaveAckOffset = getInt(v, "slaveAckOffset", 0);
    c.diff = getInt(v, "diff", 0);
    c.inSync = getBool(v, "inSync", false);
    c.transferredByteInSecond = getInt(v, "transferredByteInSecond", 0);
    c.transferFromWhere = getInt(v, "transferFromWhere", 0);
    return c;
}

JsonValue HAClientRuntimeInfo::toJson() const {
    JsonValue v = JsonValue::makeObject();
    v.set("masterAddr", JsonValue::makeString(masterAddr));
    v.set("transferredByteInSecond", JsonValue::makeInt(transferredByteInSecond));
    v.set("maxOffset", JsonValue::makeInt(maxOffset));
    v.set("lastReadTimestamp", JsonValue::makeInt(lastReadTimestamp));
    v.set("lastWriteTimestamp", JsonValue::makeInt(lastWriteTimestamp));
    v.set("masterFlushOffset", JsonValue::makeInt(masterFlushOffset));
    v.set("activated", JsonValue::makeBool(activated));
    return v;
}

HAClientRuntimeInfo HAClientRuntimeInfo::fromJson(const JsonValue& v) {
    HAClientRuntimeInfo c;
    c.masterAddr = getStr(v, "masterAddr", "");
    c.transferredByteInSecond = getInt(v, "transferredByteInSecond", 0);
    c.maxOffset = getInt(v, "maxOffset", 0);
    c.lastReadTimestamp = getInt(v, "lastReadTimestamp", 0);
    c.lastWriteTimestamp = getInt(v, "lastWriteTimestamp", 0);
    c.masterFlushOffset = getInt(v, "masterFlushOffset", 0);
    c.activated = getBool(v, "activated", getBool(v, "isActivated", false));
    return c;
}

JsonValue HARuntimeInfo::toJson() const {
    JsonValue v = JsonValue::makeObject();
    v.set("master", JsonValue::makeBool(master));
    v.set("masterCommitLogMaxOffset", JsonValue::makeInt(masterCommitLogMaxOffset));
    v.set("inSyncSlaveNums", JsonValue::makeInt(inSyncSlaveNums));
    JsonValue arr = JsonValue::makeArray();
    for (const HAConnectionRuntimeInfo& c : haConnectionInfo) arr.pushArray(c.toJson());
    v.set("haConnectionInfo", arr);
    v.set("haClientRuntimeInfo", haClientRuntimeInfo.toJson());
    return v;
}

HARuntimeInfo HARuntimeInfo::fromJson(const JsonValue& v) {
    HARuntimeInfo info;
    info.master = getBool(v, "master", false);
    info.masterCommitLogMaxOffset = getInt(v, "masterCommitLogMaxOffset", 0);
    info.inSyncSlaveNums = static_cast<int32_t>(getInt(v, "inSyncSlaveNums", 0));
    const JsonValue* arr = v.find("haConnectionInfo");
    if (arr != nullptr && arr->isArray()) {
        for (size_t i = 0; i < arr->size(); ++i) {
            if (arr->at(i).isObject()) {
                info.haConnectionInfo.push_back(HAConnectionRuntimeInfo::fromJson(arr->at(i)));
            }
        }
    }
    const JsonValue* client = v.find("haClientRuntimeInfo");
    if (client != nullptr && client->isObject()) {
        info.haClientRuntimeInfo = HAClientRuntimeInfo::fromJson(*client);
    }
    return info;
}

Bytes HARuntimeInfo::encode() const { return RemotingSerializable::encode(toJson()); }

bool HARuntimeInfo::decode(const Bytes& data, HARuntimeInfo& out) {
    JsonValue v;
    if (!RemotingSerializable::decode(data, v) || !v.isObject()) return false;
    out = fromJson(v);
    return true;
}

// ---------------------------------------------------------------- EpochEntry / EpochEntryCache
JsonValue EpochEntry::toJson() const {
    JsonValue v = JsonValue::makeObject();
    v.set("epoch", JsonValue::makeInt(epoch));
    v.set("startOffset", JsonValue::makeInt(startOffset));
    v.set("endOffset", JsonValue::makeInt(endOffset));
    return v;
}

EpochEntry EpochEntry::fromJson(const JsonValue& v) {
    EpochEntry e;
    e.epoch = static_cast<int32_t>(getInt(v, "epoch", 0));
    e.startOffset = getInt(v, "startOffset", 0);
    // A missing endOffset means the epoch is still being written (Java Long.MAX_VALUE)
    e.endOffset = getInt(v, "endOffset", std::numeric_limits<int64_t>::max());
    return e;
}

JsonValue EpochEntryCache::toJson() const {
    JsonValue v = JsonValue::makeObject();
    v.set("clusterName", JsonValue::makeString(clusterName));
    v.set("brokerName", JsonValue::makeString(brokerName));
    v.set("brokerId", JsonValue::makeInt(brokerId));
    JsonValue arr = JsonValue::makeArray();
    for (const EpochEntry& e : epochList) arr.pushArray(e.toJson());
    v.set("epochList", arr);
    v.set("maxOffset", JsonValue::makeInt(maxOffset));
    return v;
}

EpochEntryCache EpochEntryCache::fromJson(const JsonValue& v) {
    EpochEntryCache c;
    c.clusterName = getStr(v, "clusterName", "");
    c.brokerName = getStr(v, "brokerName", "");
    c.brokerId = getInt(v, "brokerId", 0);
    const JsonValue* arr = v.find("epochList");
    if (arr != nullptr && arr->isArray()) {
        for (size_t i = 0; i < arr->size(); ++i) {
            if (arr->at(i).isObject()) c.epochList.push_back(EpochEntry::fromJson(arr->at(i)));
        }
    }
    c.maxOffset = getInt(v, "maxOffset", 0);
    return c;
}

Bytes EpochEntryCache::encode() const { return RemotingSerializable::encode(toJson()); }

bool EpochEntryCache::decode(const Bytes& data, EpochEntryCache& out) {
    JsonValue v;
    if (!RemotingSerializable::decode(data, v) || !v.isObject()) return false;
    out = fromJson(v);
    return true;
}

// ---------------------------------------------------------------- BrokerMemberGroup
JsonValue BrokerMemberGroup::toJson() const {
    JsonValue addrs = JsonValue::makeObject();
    for (const auto& kv : brokerAddrs) {
        addrs.set(std::to_string(kv.first), JsonValue::makeString(kv.second));
    }
    JsonValue v = JsonValue::makeObject();
    v.set("cluster", JsonValue::makeString(cluster));
    v.set("brokerName", JsonValue::makeString(brokerName));
    v.set("brokerAddrs", addrs);
    return v;
}

BrokerMemberGroup BrokerMemberGroup::fromJson(const JsonValue& v) {
    BrokerMemberGroup g;
    g.cluster = getStr(v, "cluster", "");
    g.brokerName = getStr(v, "brokerName", "");
    const JsonValue* addrs = v.find("brokerAddrs");
    if (addrs != nullptr && addrs->isObject()) {
        for (const auto& kv : addrs->objectItems()) {
            g.brokerAddrs[keyToI64(kv.first)] = kv.second.stringValue();
        }
    }
    return g;
}

Bytes BrokerMemberGroup::encode() const { return RemotingSerializable::encode(toJson()); }

bool BrokerMemberGroup::decode(const Bytes& data, BrokerMemberGroup& out) {
    JsonValue v;
    if (!RemotingSerializable::decode(data, v) || !v.isObject()) return false;
    out = fromJson(v);
    return true;
}

}  // namespace rocketmq
