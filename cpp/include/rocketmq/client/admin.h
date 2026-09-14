// 管理客户端（对应 org.apache.rocketmq.client.admin.DefaultMQAdminExt /
// MQAdminExt 与 org.apache.rocketmq.tools.admin.DefaultMQAdminExtImpl）。
//
// 覆盖：Topic 增删查/配置、Broker 集群信息与运行时信息/配置、NameServer KV 配置、
// 订阅组管理、消费者/生产者连接、消费统计、offset 管理（含真实 broker 位点重置）、
// 消息查询（key / uniqKey / msgId / ConsumeQueue）。
//
// 与 Python 参考实现（python/client/admin.py）逐项对齐。对齐要点（Java 5.x 探针 +
// 源码核对结论，勿凭记忆改）：
//  - GET_BROKER_CONFIG 的 body 是 **properties 文本**（"k=v\n"），不是 JSON。
//  - UPDATE_AND_CREATE_SUBSCRIPTIONGROUP 的 body 是 SubscriptionGroupConfig JSON。
//  - GET_TOPIC_CONFIG 请求头带 topic + lo，响应体是 TopicConfig JSON。
//  - GET_ALL_SUBSCRIPTIONGROUP_CONFIG 是**分页**接口（groupSeq / maxGroupNum / dataVersion）。
//  - ResetOffsetBody.offsetTable 是 Map<MessageQueue, Long>。
//  - KV 配置类请求打到 **NameServer**，且 PUT/DELETE 要广播到**每一个** NameServer。
#ifndef ROCKETMQ_CLIENT_ADMIN_H
#define ROCKETMQ_CLIENT_ADMIN_H

#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <vector>

#include "rocketmq/client/mq_client.h"
#include "rocketmq/common/message.h"
#include "rocketmq/common/topic_config.h"
#include "rocketmq/remoting/protocol/admin_body.h"
#include "rocketmq/remoting/protocol/body.h"
#include "rocketmq/remoting/protocol/route.h"
#include "rocketmq/remoting/protocol/subscription.h"

namespace rocketmq {

class DefaultMQAdminExt {
public:
    // 对应 DefaultMQAdminExt.DEFAULT_TIMEOUT = 5000 * 3
    static constexpr int32_t DEFAULT_TIMEOUT = 5000 * 3;

    explicit DefaultMQAdminExt(const std::string& instanceName = "ADMIN");
    DefaultMQAdminExt(const DefaultMQAdminExt&) = delete;
    DefaultMQAdminExt& operator=(const DefaultMQAdminExt&) = delete;
    ~DefaultMQAdminExt();

    // ---------------- 配置与生命周期 ----------------
    void setNamesrvAddr(const std::string& addr);  // 分号分隔
    void setNameServerAddresses(const std::vector<std::string>& addrs);
    void setInstanceName(const std::string& name) { instanceName_ = name; }
    std::string getNamesrvAddr() const;
    std::vector<std::string> getNameServerAddressList() const { return nameServerAddrs_; }
    void setTimeoutMillis(int32_t millis) { timeoutMillis_ = millis; }
    int32_t getTimeoutMillis() const { return timeoutMillis_; }
    // 删除 topic 时一并清理的 KV namespace（对应 Java kvNamespaceToDeleteList）
    void addKvNamespaceToDeleteList(const std::string& ns) {
        kvNamespaceToDeleteList_.push_back(ns);
    }

    void start();
    void shutdown();
    bool isStarted() const { return started_; }
    MQClientInstance& getMQClientInstance();
    MQClientInstance& client() { return getMQClientInstance(); }

    // ---------------- Topic 管理 ----------------
    void createTopic(const std::string& key, const std::string& newTopic,
                     int32_t queueNum = 4, int32_t topicSysFlag = 0);
    void createAndUpdateTopicConfig(const std::string& addr, const TopicConfig& config);
    void createTopicInBroker(const std::string& brokerAddr, const std::string& topic,
                             int32_t readQueueNums = 4, int32_t writeQueueNums = 4,
                             int32_t perm = 6);
    void deleteTopicInBroker(const std::string& brokerAddr, const std::string& topic);
    void deleteTopicInNameServer(const std::vector<std::string>& addrs,
                                 const std::string& topic);
    void deleteTopicInNamesrv(const std::string& topic);
    void deleteTopic(const std::string& topic, const std::string& clusterName = std::string());
    TopicList fetchAllTopicList();
    std::set<std::string> fetchTopicsByCluster(const std::string& clusterName);
    std::set<std::string> getClusterList(const std::string& topic);
    std::vector<TopicRouteData> fetchAllTopicRoute();
    TopicRouteData examineTopicRoute(const std::string& topic);
    TopicConfig examineTopicConfig(const std::string& addr, const std::string& topic);
    TopicConfigSerializeWrapper getAllTopicConfig(const std::string& brokerAddr,
                                                 int32_t timeoutMillis = -1);
    TopicConfigSerializeWrapper getUserTopicConfig(const std::string& brokerAddr,
                                                   bool specialTopic = false,
                                                   int32_t timeoutMillis = -1);
    TopicList getSystemTopicListFromBroker(const std::string& brokerAddr,
                                          int32_t timeoutMillis = -1);
    TopicStatsTable examineTopicStats(const std::string& topic);
    TopicStatsTable examineTopicStatsByBroker(const std::string& brokerAddr,
                                             const std::string& topic);

    // ---------------- 集群 / Broker ----------------
    ClusterInfo fetchBrokerClusterInfo();
    ClusterInfo examineBrokerClusterInfo() { return fetchBrokerClusterInfo(); }
    KVTable fetchBrokerRuntimeStats(const std::string& brokerAddr, int32_t timeoutMillis = -1);
    KVTable getBrokerRuntimeInfo(const std::string& brokerAddr, int32_t timeoutMillis = -1) {
        return fetchBrokerRuntimeStats(brokerAddr, timeoutMillis);
    }
    // GET_BROKER_CONFIG 的响应体是 **properties 文本**，这里解析成 k=v
    PropertyMap getBrokerConfig(const std::string& brokerAddr, int32_t timeoutMillis = -1);
    void updateBrokerConfig(const std::string& brokerAddr, const PropertyMap& properties,
                            int32_t timeoutMillis = -1);
    int32_t wipeWritePermOfBroker(const std::string& namesrvAddr, const std::string& brokerName);
    int32_t addWritePermOfBroker(const std::string& namesrvAddr, const std::string& brokerName);
    bool cleanUnusedTopic(const std::string& clusterName = std::string(),
                          const std::string& topic = std::string());
    JsonValue viewBrokerStatsData(const std::string& brokerAddr, const std::string& statsName,
                                  const std::string& statsKey);

    // ---------------- NameServer KV 配置 ----------------
    void createAndUpdateKvConfig(const std::string& ns, const std::string& key,
                                 const std::string& value);
    void putKvConfig(const std::string& ns, const std::string& key, const std::string& value) {
        createAndUpdateKvConfig(ns, key, value);
    }
    // 返回 false 表示该 key 不存在
    bool getKvConfig(const std::string& ns, const std::string& key, std::string& outValue);
    void deleteKvConfig(const std::string& ns, const std::string& key);
    KVTable getKvListByNamespace(const std::string& ns);

    // ---------------- 订阅组管理 ----------------
    void createAndUpdateSubscriptionGroupConfig(const std::string& addr,
                                                const SubscriptionGroupConfig& config);
    // 返回 false 表示该订阅组不存在
    bool examineSubscriptionGroupConfig(const std::string& addr, const std::string& group,
                                        SubscriptionGroupConfig& out);
    bool getSubscriptionGroupConfig(const std::string& addr, const std::string& group,
                                    SubscriptionGroupConfig& out);
    SubscriptionGroupWrapper getAllSubscriptionGroup(const std::string& brokerAddr,
                                                     int32_t timeoutMillis = -1);
    SubscriptionGroupWrapper getUserSubscriptionGroup(const std::string& brokerAddr,
                                                      int32_t timeoutMillis = -1);
    void deleteSubscriptionGroup(const std::string& addr, const std::string& groupName,
                                 bool removeOffset = false);

    // ---------------- 消费者 / 生产者连接 ----------------
    ConsumerConnection examineConsumerConnectionInfo(
        const std::string& consumerGroup, const std::string& brokerAddr = std::string());
    ConsumerConnection examineConsumerConnection(
        const std::string& consumerGroup, const std::string& brokerAddr = std::string()) {
        return examineConsumerConnectionInfo(consumerGroup, brokerAddr);
    }
    ProducerConnection examineProducerConnectionInfo(
        const std::string& producerGroup, const std::string& brokerAddr = std::string());
    ConsumerRunningInfo examineConsumerRunningInfo(
        const std::string& consumerGroup, const std::string& clientId, bool jstack = false,
        const std::string& brokerAddr = std::string());
    GetConsumerListByGroupResponseBody getConsumerListByGroup(
        const std::string& consumerGroup, const std::string& brokerAddr = std::string());

    // ---------------- 消费统计 ----------------
    ConsumeStats examineConsumeStats(const std::string& brokerAddr,
                                     const std::string& consumerGroup,
                                     const std::string& topic = std::string(),
                                     const std::vector<std::string>& topicList = {});
    ConsumeStatsList fetchConsumeStatsInBroker(const std::string& brokerAddr,
                                               bool isOrder = false,
                                               int32_t timeoutMillis = -1);
    std::set<std::string> queryTopicConsumeByWho(const std::string& brokerAddr,
                                                 const std::string& topic);
    TopicList queryTopicsByConsumer(const std::string& brokerAddr, const std::string& group);
    JsonValue querySubscription(const std::string& brokerAddr, const std::string& group,
                                const std::string& topic);
    JsonValue getConsumeStatus(const std::string& brokerAddr, const std::string& topic,
                               const std::string& group,
                               const std::string& clientAddr = std::string());
    void cloneGroupOffset(const std::string& brokerAddr, const std::string& srcGroup,
                          const std::string& destGroup, const std::string& topic,
                          bool offline = false);

    // ---------------- Offset 管理 ----------------
    int64_t maxOffset(const MessageQueue& mq);
    int64_t minOffset(const MessageQueue& mq);
    int64_t searchOffset(const MessageQueue& mq, int64_t timestamp);
    int64_t earliestMsgStoreTime(const MessageQueue& mq);
    // 返回 false 表示该消费组在该队列上尚无位点
    bool examineConsumerOffset(const std::string& consumerGroup, const MessageQueue& mq,
                               int64_t& outOffset);
    void updateConsumerOffset(const std::string& consumerGroup, const MessageQueue& mq,
                              int64_t offset);
    void updateConsumerOffsetToBroker(const std::string& brokerAddr,
                                      const std::string& consumerGroup, const MessageQueue& mq,
                                      int64_t offset);
    // 对应 Java resetOffsetByTimestamp：逐 broker 下发 INVOKE_BROKER_TO_RESET_OFFSET
    std::map<MessageQueue, int64_t> resetOffsetByTimestamp(
        const std::string& topic, const std::string& group, int64_t timestamp,
        bool isForce = true, const std::string& clusterName = std::string(), bool isCpp = true);
    void resetOffsetNew(const std::string& consumerGroup, const std::string& topic,
                        int64_t timestamp);
    std::map<MessageQueue, int64_t> resetOffsetByTimestampOld(
        const std::string& consumerGroup, const std::string& topic, int64_t timestamp,
        bool force = true);

    // ---------------- 消息查询 ----------------
    std::vector<MessageExt> queryMessage(const std::string& topic, const std::string& key,
                                         int32_t maxNum, int64_t begin, int64_t end);
    // 返回 false 表示未命中（注意：默认文件索引下 uniqKey 查询需要 RocksDB 索引）
    bool queryMessageByUniqKey(const std::string& topic, const std::string& uniqKey,
                               MessageExt& out);
    std::vector<MessageExt> queryMessageByKey(const std::string& topic, const std::string& key,
                                             int32_t maxNum = 32);
    MessageExt viewMessage(const std::string& topic, const std::string& msgId);
    QueryConsumeQueueResponseBody queryConsumeQueue(const std::string& brokerAddr,
                                                   const std::string& topic, int32_t queueId,
                                                   int64_t index, int32_t count = 32,
                                                   const std::string& consumerGroup = std::string());

private:
    // ---------------- 底层调用助手 ----------------
    MQClientInstance& requireClient();
    static std::string firstBrokerAddr(MQClientInstance& client);
    std::string findFirstBrokerAddr(MQClientInstance& client);
    std::string brokerAddrForMq(MQClientInstance& client, const MessageQueue& mq);
    std::vector<std::string> brokerAddrsOfCluster(MQClientInstance& client,
                                                  const std::string& clusterName);

    std::string instanceName_;
    std::string clientId_;
    std::vector<std::string> nameServerAddrs_;
    std::vector<std::string> kvNamespaceToDeleteList_;
    int32_t timeoutMillis_ = DEFAULT_TIMEOUT;

    std::unique_ptr<MQClientInstance> mqClient_;
    bool started_ = false;
};

}  // namespace rocketmq

#endif  // ROCKETMQ_CLIENT_ADMIN_H
