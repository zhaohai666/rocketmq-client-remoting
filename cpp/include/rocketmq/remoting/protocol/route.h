// Purpose: the client sends GET_ROUTEINFO_BY_TOPIC to the name server and the response body is the JSON
// of TopicRouteData. It gives the broker addresses in the cluster (brokerDatas) and the read/write queue
// counts per topic (queueDatas), from which the sendable/pullable MessageQueue lists are assembled.
//
// JSON fields (as fastjson2 emits them, keys in alphabetical order):
//   QueueData      : brokerName | perm | readQueueNums | topicSysFlag | writeQueueNums
//   BrokerData     : brokerAddrs | brokerName | cluster | enableActingMaster | zoneName
//   TopicRouteData : brokerDatas | filterServerTable | orderTopicConf | queueDatas
//                    (+ topicQueueMappingByBroker, emitted only when non-empty)
#ifndef ROCKETMQ_REMOTING_PROTOCOL_ROUTE_H
#define ROCKETMQ_REMOTING_PROTOCOL_ROUTE_H

#include <cstdint>
#include <map>
#include <string>
#include <vector>

#include "rocketmq/common/byte_buffer.h"
#include "rocketmq/common/message.h"
#include "rocketmq/remoting/protocol/json.h"

namespace rocketmq {

// ---------------------------------------------------------------- QueueData
class QueueData {
public:
    std::string brokerName;
    int32_t readQueueNums = 0;
    int32_t writeQueueNums = 0;
    int32_t perm = 0;
    int32_t topicSysFlag = 0;

    QueueData() = default;
    QueueData(const std::string& brokerName, int32_t readQueueNums, int32_t writeQueueNums,
              int32_t perm, int32_t topicSysFlag)
        : brokerName(brokerName), readQueueNums(readQueueNums), writeQueueNums(writeQueueNums),
          perm(perm), topicSysFlag(topicSysFlag) {}

    const std::string& getBrokerName() const { return brokerName; }
    void setBrokerName(const std::string& n) { brokerName = n; }
    int32_t getReadQueueNums() const { return readQueueNums; }
    void setReadQueueNums(int32_t n) { readQueueNums = n; }
    int32_t getWriteQueueNums() const { return writeQueueNums; }
    void setWriteQueueNums(int32_t n) { writeQueueNums = n; }
    int32_t getPerm() const { return perm; }
    void setPerm(int32_t p) { perm = p; }
    int32_t getTopicSysFlag() const { return topicSysFlag; }
    void setTopicSysFlag(int32_t f) { topicSysFlag = f; }

    JsonValue toJson() const;
    static QueueData fromJson(const JsonValue& v);

    bool operator==(const QueueData& o) const {
        return brokerName == o.brokerName && perm == o.perm && readQueueNums == o.readQueueNums
            && writeQueueNums == o.writeQueueNums && topicSysFlag == o.topicSysFlag;
    }
    bool operator!=(const QueueData& o) const { return !(*this == o); }

    // compareTo in protocol terms: by brokerName only
    int compareTo(const QueueData& o) const {
        if (brokerName == o.brokerName) return 0;
        return brokerName < o.brokerName ? -1 : 1;
    }
    bool operator<(const QueueData& o) const { return compareTo(o) < 0; }

    int hashCode() const;
    std::string toString() const;
};

// ---------------------------------------------------------------- BrokerData
class BrokerData {
public:
    std::string cluster;
    std::string brokerName;
    // brokerId -> address. brokerId=0 is the master (MixAll::MASTER_ID)
    std::map<int64_t, std::string> brokerAddrs;
    std::string zoneName;
    bool enableActingMaster = false;

    BrokerData() = default;
    BrokerData(const std::string& cluster, const std::string& brokerName,
               const std::map<int64_t, std::string>& brokerAddrs, const std::string& zoneName = "",
               bool enableActingMaster = false)
        : cluster(cluster), brokerName(brokerName), brokerAddrs(brokerAddrs), zoneName(zoneName),
          enableActingMaster(enableActingMaster) {}

    const std::string& getCluster() const { return cluster; }
    void setCluster(const std::string& c) { cluster = c; }
    const std::string& getBrokerName() const { return brokerName; }
    void setBrokerName(const std::string& n) { brokerName = n; }
    const std::map<int64_t, std::string>& getBrokerAddrs() const { return brokerAddrs; }
    void setBrokerAddrs(const std::map<int64_t, std::string>& a) { brokerAddrs = a; }
    const std::string& getZoneName() const { return zoneName; }
    void setZoneName(const std::string& z) { zoneName = z; }
    bool isEnableActingMaster() const { return enableActingMaster; }
    void setEnableActingMaster(bool b) { enableActingMaster = b; }

    // Prefer brokerId=0 (master); otherwise pick a slave address at random.
    // Returns an empty string when no address is available.
    std::string selectBrokerAddr() const;

    JsonValue toJson() const;
    static BrokerData fromJson(const JsonValue& v);

    // equals in protocol terms: cluster / brokerName / brokerAddrs
    bool operator==(const BrokerData& o) const {
        return cluster == o.cluster && brokerName == o.brokerName && brokerAddrs == o.brokerAddrs;
    }
    bool operator!=(const BrokerData& o) const { return !(*this == o); }

    // compareTo in protocol terms: by brokerName only
    int compareTo(const BrokerData& o) const {
        if (brokerName == o.brokerName) return 0;
        return brokerName < o.brokerName ? -1 : 1;
    }
    bool operator<(const BrokerData& o) const { return compareTo(o) < 0; }

    int hashCode() const;
    std::string toString() const;
};

// ---------------------------------------------------------------- TopicRouteData
class TopicRouteData {
public:
    std::string orderTopicConf;
    std::vector<QueueData> queueDatas;
    std::vector<BrokerData> brokerDatas;
    std::map<std::string, std::vector<std::string>> filterServerTable;
    // Pass-through: TopicQueueMappingInfo is not modelled, the JSON is kept verbatim so no field is lost.
    // isNull() means the field is absent.
    JsonValue topicQueueMappingByBroker;

    TopicRouteData() = default;

    const std::vector<QueueData>& getQueueDatas() const { return queueDatas; }
    void setQueueDatas(const std::vector<QueueData>& q) { queueDatas = q; }
    const std::vector<BrokerData>& getBrokerDatas() const { return brokerDatas; }
    void setBrokerDatas(const std::vector<BrokerData>& b) { brokerDatas = b; }
    const std::string& getOrderTopicConf() const { return orderTopicConf; }
    void setOrderTopicConf(const std::string& c) { orderTopicConf = c; }
    const std::map<std::string, std::vector<std::string>>& getFilterServerTable() const {
        return filterServerTable;
    }
    void setFilterServerTable(const std::map<std::string, std::vector<std::string>>& t) {
        filterServerTable = t;
    }
    const JsonValue& getTopicQueueMappingByBroker() const { return topicQueueMappingByBroker; }
    void setTopicQueueMappingByBroker(const JsonValue& v) { topicQueueMappingByBroker = v; }

    // Assemble every **writable** MessageQueue from queueDatas + brokerDatas
    // (the assembly used when deriving topic publish info in MQClientInstance).
    // The topic is written back into each MessageQueue, otherwise a later route lookup by mq.topic finds nothing.
    std::vector<MessageQueue> getAllMessageQueue(const std::string& topic) const;

    // Assemble every **readable** MessageQueue from queueDatas.
    // It looks only at the read permission and readQueueNums, **never at brokerDatas and it does not require a master**, because:
    // the consume side (rebalance / fetchSubscribeMessageQueues) must use this list and must not substitute publish info.
    // Case 1: a read-only topic with perm=4 is consumable, yet its publish info is an empty table;
    // Case 2: when the master is gone and only slaves remain, the consumable queue set is unchanged (pulling may go to a slave).
    std::vector<MessageQueue> getAllSubscribeMessageQueue(const std::string& topic) const;

    TopicRouteData cloneTopicRouteData() const { return *this; }

    // Whether the route changed: sort by compareTo first, then compare
    bool topicRouteDataChanged(const TopicRouteData* oldData) const;

    JsonValue toJson() const;
    static TopicRouteData fromJson(const JsonValue& v);

    bool operator==(const TopicRouteData& o) const;
    bool operator!=(const TopicRouteData& o) const { return !(*this == o); }

    // The equivalent of RemotingSerializable.encode / decode
    Bytes encode() const;
    static bool decode(const Bytes& data, TopicRouteData& out);

    std::string toString() const;
};

}  // namespace rocketmq

#endif  // ROCKETMQ_REMOTING_PROTOCOL_ROUTE_H
