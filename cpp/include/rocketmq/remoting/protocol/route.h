// Topic 路由数据（对应 org.apache.rocketmq.remoting.protocol.route.*）。
//
// 用途：客户端向 NameServer 发 GET_ROUTEINFO_BY_TOPIC，响应 body 即 TopicRouteData
// 的 JSON。据此得知集群里 broker 的地址（brokerDatas）与每个 topic 的读写队列数
// （queueDatas），进而组装出可发送/可拉取的 MessageQueue 列表。
//
// JSON 字段（与 fastjson2 一致，键按字母序）：
//   QueueData      : brokerName | perm | readQueueNums | topicSysFlag | writeQueueNums
//   BrokerData     : brokerAddrs | brokerName | cluster | enableActingMaster | zoneName
//   TopicRouteData : brokerDatas | filterServerTable | orderTopicConf | queueDatas
//                    （+ topicQueueMappingByBroker，非空时才输出）
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

    // Java compareTo：仅按 brokerName
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
    // brokerId -> 地址。brokerId=0 是 master（MixAll::MASTER_ID）
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

    // 优先返回 brokerId=0（master）；否则随机取一个从节点地址。
    // 无可用地址时返回空串。
    std::string selectBrokerAddr() const;

    JsonValue toJson() const;
    static BrokerData fromJson(const JsonValue& v);

    // Java equals：cluster / brokerName / brokerAddrs
    bool operator==(const BrokerData& o) const {
        return cluster == o.cluster && brokerName == o.brokerName && brokerAddrs == o.brokerAddrs;
    }
    bool operator!=(const BrokerData& o) const { return !(*this == o); }

    // Java compareTo：仅按 brokerName
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
    // 透传：不建模 TopicQueueMappingInfo，原样保留 JSON 以免丢字段。
    // isNull() 表示该字段不存在。
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

    // 按 queueDatas + brokerDatas 组装全部**可写** MessageQueue
    // （对应 MQClientInstance.topicRouteData2TopicPublishInfo 的组装逻辑）。
    // topic 会回填进每个 MessageQueue，否则后续按 mq.topic 回查路由会查不到。
    std::vector<MessageQueue> getAllMessageQueue(const std::string& topic) const;

    TopicRouteData cloneTopicRouteData() const { return *this; }

    // 路由是否变化：先按 compareTo 排序再比较（与 Java topicRouteDataChanged 一致）
    bool topicRouteDataChanged(const TopicRouteData* oldData) const;

    JsonValue toJson() const;
    static TopicRouteData fromJson(const JsonValue& v);

    bool operator==(const TopicRouteData& o) const;
    bool operator!=(const TopicRouteData& o) const { return !(*this == o); }

    // RemotingSerializable.encode / decode 的等价物
    Bytes encode() const;
    static bool decode(const Bytes& data, TopicRouteData& out);

    std::string toString() const;
};

}  // namespace rocketmq

#endif  // ROCKETMQ_REMOTING_PROTOCOL_ROUTE_H
