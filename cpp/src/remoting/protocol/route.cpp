// QueueData / BrokerData / TopicRouteData implementations.
#include "rocketmq/remoting/protocol/route.h"

#include <algorithm>
#include <cstdint>
#include <cstdlib>
#include <random>
#include <string>
#include <vector>

#include "rocketmq/common/byte_buffer.h"
#include "rocketmq/common/sysflag.h"
#include "rocketmq/remoting/protocol/serialize.h"

namespace rocketmq {

namespace {

// The broker's brokerAddrs map is keyed by numbers (a Long -> String map).
// We emit string keys ("0"), which is valid JSON, semantically equivalent, and readable by any parser.
JsonValue addrsToJson(const std::map<int64_t, std::string>& addrs) {
    JsonValue o = JsonValue::makeObject();
    for (const auto& kv : addrs) {
        o.set(std::to_string(kv.first), JsonValue::makeString(kv.second));
    }
    return o;
}

std::map<int64_t, std::string> addrsFromJson(const JsonValue& v) {
    std::map<int64_t, std::string> out;
    if (!v.isObject()) {
        return out;
    }
    for (const auto& kv : v.objectItems()) {
        // A key may be "0" (standard JSON) or a bare numeric 0 that the parser surfaces as a string
        int64_t id = 0;
        if (!kv.first.empty()) {
            bool numeric = true;
            for (char c : kv.first) {
                if (c < '0' || c > '9') {
                    numeric = false;
                    break;
                }
            }
            if (numeric) {
                id = std::strtoll(kv.first.c_str(), nullptr, 10);
            } else {
                continue;  // skip non-numeric keys (malformed data)
            }
        }
        out[id] = kv.second.stringValue();
    }
    return out;
}

}  // namespace

// ---------------------------------------------------------------- QueueData
JsonValue QueueData::toJson() const {
    JsonValue o = JsonValue::makeObject();
    o.set("brokerName", JsonValue::makeString(brokerName));
    o.set("perm", JsonValue::makeInt(perm));
    o.set("readQueueNums", JsonValue::makeInt(readQueueNums));
    o.set("topicSysFlag", JsonValue::makeInt(topicSysFlag));
    o.set("writeQueueNums", JsonValue::makeInt(writeQueueNums));
    return o;
}

QueueData QueueData::fromJson(const JsonValue& v) {
    QueueData q;
    if (!v.isObject()) {
        return q;
    }
    std::string s;
    if (v.tryGetString("brokerName", s)) {
        q.brokerName = s;
    }
    int64_t n = 0;
    if (v.tryGetInt("perm", n)) q.perm = static_cast<int32_t>(n);
    if (v.tryGetInt("readQueueNums", n)) q.readQueueNums = static_cast<int32_t>(n);
    if (v.tryGetInt("writeQueueNums", n)) q.writeQueueNums = static_cast<int32_t>(n);
    if (v.tryGetInt("topicSysFlag", n)) q.topicSysFlag = static_cast<int32_t>(n);
    return q;
}

int QueueData::hashCode() const {
    int64_t result = 1;
    result = 31 * result + javaStringHash(brokerName);
    result = 31 * result + perm;
    result = 31 * result + readQueueNums;
    result = 31 * result + writeQueueNums;
    result = 31 * result + topicSysFlag;
    result &= 0xFFFFFFFFL;
    return static_cast<int32_t>(result);
}

std::string QueueData::toString() const {
    return "QueueData [brokerName=" + brokerName + ", readQueueNums=" + std::to_string(readQueueNums)
         + ", writeQueueNums=" + std::to_string(writeQueueNums) + ", perm=" + std::to_string(perm)
         + ", topicSysFlag=" + std::to_string(topicSysFlag) + "]";
}

// ---------------------------------------------------------------- BrokerData
std::string BrokerData::selectBrokerAddr() const {
    if (brokerAddrs.empty()) {
        return std::string();
    }
    auto it = brokerAddrs.find(0);  // MixAll.MASTER_ID
    if (it != brokerAddrs.end()) {
        return it->second;
    }
    // No master: pick a slave at random
    static thread_local std::mt19937_64 rng{std::random_device{}()};
    std::vector<std::string> addrs;
    addrs.reserve(brokerAddrs.size());
    for (const auto& kv : brokerAddrs) {
        addrs.push_back(kv.second);
    }
    std::uniform_int_distribution<size_t> dist(0, addrs.size() - 1);
    return addrs[dist(rng)];
}

JsonValue BrokerData::toJson() const {
    JsonValue o = JsonValue::makeObject();
    o.set("brokerAddrs", addrsToJson(brokerAddrs));
    o.set("brokerName", JsonValue::makeString(brokerName));
    o.set("cluster", JsonValue::makeString(cluster));
    o.set("enableActingMaster", JsonValue::makeBool(enableActingMaster));
    o.set("zoneName", JsonValue::makeString(zoneName));
    return o;
}

BrokerData BrokerData::fromJson(const JsonValue& v) {
    BrokerData b;
    if (!v.isObject()) {
        return b;
    }
    std::string s;
    if (v.tryGetString("brokerName", s)) b.brokerName = s;
    if (v.tryGetString("cluster", s)) b.cluster = s;
    if (v.tryGetString("zoneName", s)) b.zoneName = s;
    bool bv = false;
    if (v.tryGetBool("enableActingMaster", bv)) b.enableActingMaster = bv;
    const JsonValue* addrs = v.find("brokerAddrs");
    if (addrs != nullptr) {
        b.brokerAddrs = addrsFromJson(*addrs);
    }
    return b;
}

int BrokerData::hashCode() const {
    int64_t result = 1;
    result = 31 * result + javaStringHash(cluster);
    result = 31 * result + javaStringHash(brokerName);
    // Map.hashCode = the sum of (keyHash ^ valueHash) over all entries
    int64_t mapHash = 0;
    for (const auto& kv : brokerAddrs) {
        int64_t k = kv.first;                       // Long.hashCode = (int)(v ^ (v>>>32))
        int32_t kh = static_cast<int32_t>(k ^ static_cast<uint64_t>(k) >> 32);
        mapHash += (kh ^ javaStringHash(kv.second));
        mapHash &= 0xFFFFFFFFL;
    }
    result = 31 * result + mapHash;
    result &= 0xFFFFFFFFL;
    return static_cast<int32_t>(result);
}

std::string BrokerData::toString() const {
    std::string addrs = "{";
    bool first = true;
    for (const auto& kv : brokerAddrs) {
        if (!first) addrs += ", ";
        first = false;
        addrs += std::to_string(kv.first) + "=" + kv.second;
    }
    addrs += "}";
    return "BrokerData [brokerName=" + brokerName + ", brokerAddrs=" + addrs + "]";
}

// ---------------------------------------------------------------- TopicRouteData
std::vector<MessageQueue> TopicRouteData::getAllMessageQueue(const std::string& topic) const {
    std::vector<MessageQueue> mqs;
    for (const QueueData& qd : queueDatas) {
        // Only queues with write permission (protocol PermName.isWriteable)
        if (!PermName::checkPerm(qd.perm, PermName::PERM_WRITE)) {
            continue;
        }
        const BrokerData* found = nullptr;
        for (const BrokerData& bd : brokerDatas) {
            if (bd.brokerName == qd.brokerName) {
                found = &bd;
                break;
            }
        }
        if (found == nullptr) {
            continue;
        }
        // Publish view: a broker whose brokerAddrs has no MASTER_ID(0) is skipped entirely.
        // Slaves register in the name server on their own and still carry the write bit in the default config, so once the master is gone
        // the route keeps only brokerId=1 under the same brokerName. Missing that check lets producers send to a slave,
        // and a slave rejects every send request (the broker's SendMessageProcessor answers SYSTEM_BUSY, a retryable code).
        if (found->brokerAddrs.find(0) == found->brokerAddrs.end()) {
            continue;
        }
        for (int32_t i = 0; i < qd.writeQueueNums; ++i) {
            mqs.emplace_back(topic, qd.brokerName, i);
        }
    }
    return mqs;
}

// A separate view from getAllMessageQueue, the two must not be merged: this one looks only at the **read** bit and readQueueNums,
// without consulting brokerDatas or requiring a master (a slave keeps serving pulls while the master is down;
// a read-only topic with perm=4 is consumable yet empty in the publish view).
std::vector<MessageQueue> TopicRouteData::getAllSubscribeMessageQueue(
    const std::string& topic) const {
    std::vector<MessageQueue> mqs;
    for (const QueueData& qd : queueDatas) {
        if (!PermName::checkPerm(qd.perm, PermName::PERM_READ)) {
            continue;
        }
        for (int32_t i = 0; i < qd.readQueueNums; ++i) {
            mqs.emplace_back(topic, qd.brokerName, i);
        }
    }
    return mqs;
}

bool TopicRouteData::topicRouteDataChanged(const TopicRouteData* oldData) const {
    if (oldData == nullptr) {
        return true;
    }
    std::vector<QueueData> nowQ = queueDatas;
    std::vector<QueueData> oldQ = oldData->queueDatas;
    std::vector<BrokerData> nowB = brokerDatas;
    std::vector<BrokerData> oldB = oldData->brokerDatas;
    std::sort(nowQ.begin(), nowQ.end());
    std::sort(oldQ.begin(), oldQ.end());
    std::sort(nowB.begin(), nowB.end());
    std::sort(oldB.begin(), oldB.end());
    return !(nowQ == oldQ && nowB == oldB);
}

JsonValue TopicRouteData::toJson() const {
    JsonValue o = JsonValue::makeObject();
    JsonValue bArr = JsonValue::makeArray();
    for (const BrokerData& b : brokerDatas) {
        bArr.pushArray(b.toJson());
    }
    o.set("brokerDatas", bArr);

    JsonValue fst = JsonValue::makeObject();
    for (const auto& kv : filterServerTable) {
        JsonValue arr = JsonValue::makeArray();
        for (const std::string& s : kv.second) {
            arr.pushArray(JsonValue::makeString(s));
        }
        fst.set(kv.first, arr);
    }
    o.set("filterServerTable", fst);

    o.set("orderTopicConf", JsonValue::makeString(orderTopicConf));

    JsonValue qArr = JsonValue::makeArray();
    for (const QueueData& q : queueDatas) {
        qArr.pushArray(q.toJson());
    }
    o.set("queueDatas", qArr);

    if (!topicQueueMappingByBroker.isNull()) {
        o.set("topicQueueMappingByBroker", topicQueueMappingByBroker);
    }
    return o;
}

TopicRouteData TopicRouteData::fromJson(const JsonValue& v) {
    TopicRouteData t;
    if (!v.isObject()) {
        return t;
    }
    std::string s;
    if (v.tryGetString("orderTopicConf", s)) {
        t.orderTopicConf = s;
    }
    const JsonValue* qs = v.find("queueDatas");
    if (qs != nullptr && qs->isArray()) {
        for (size_t i = 0; i < qs->size(); ++i) {
            t.queueDatas.push_back(QueueData::fromJson(qs->at(i)));
        }
    }
    const JsonValue* bs = v.find("brokerDatas");
    if (bs != nullptr && bs->isArray()) {
        for (size_t i = 0; i < bs->size(); ++i) {
            t.brokerDatas.push_back(BrokerData::fromJson(bs->at(i)));
        }
    }
    const JsonValue* fst = v.find("filterServerTable");
    if (fst != nullptr && fst->isObject()) {
        for (const auto& kv : fst->objectItems()) {
            std::vector<std::string> servers;
            if (kv.second.isArray()) {
                for (size_t i = 0; i < kv.second.size(); ++i) {
                    servers.push_back(kv.second.at(i).stringValue());
                }
            }
            t.filterServerTable[kv.first] = servers;
        }
    }
    const JsonValue* tqm = v.find("topicQueueMappingByBroker");
    if (tqm != nullptr && !tqm->isNull()) {
        t.topicQueueMappingByBroker = *tqm;
    }
    return t;
}

bool TopicRouteData::operator==(const TopicRouteData& o) const {
    return brokerDatas == o.brokerDatas && orderTopicConf == o.orderTopicConf
        && queueDatas == o.queueDatas && filterServerTable == o.filterServerTable
        && topicQueueMappingByBroker.dump() == o.topicQueueMappingByBroker.dump();
}

Bytes TopicRouteData::encode() const {
    return RemotingSerializable::encode(toJson());
}

bool TopicRouteData::decode(const Bytes& data, TopicRouteData& out) {
    JsonValue v;
    if (!RemotingSerializable::decode(data, v)) {
        return false;
    }
    out = TopicRouteData::fromJson(v);
    return true;
}

std::string TopicRouteData::toString() const {
    return "TopicRouteData [orderTopicConf=" + orderTopicConf
         + ", queueDatas=" + std::to_string(queueDatas.size())
         + ", brokerDatas=" + std::to_string(brokerDatas.size()) + "]";
}

}  // namespace rocketmq
