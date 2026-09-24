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

    // ---------------- unitName / enableStreamRequestType ----------------
    // 对应 Java `ClientConfig` 的这两个开关（必须在 start() 之前设置）。
    // ⚠ 管理端**没有** unitMode 的落点：Java `DefaultMQAdminExtImpl` 全程没读过
    // `isUnitMode()` —— 它既不发普通消息、也不做消息过滤。
    void setUnitName(const std::string& unitName) { unitName_ = unitName; }
    const std::string& unitName() const { return unitName_; }
    // true 时每个请求带扩展字段 `ReqT=0`，且 clientId 末尾多一段 `@STREAM`。
    void setEnableStreamRequestType(bool enable) { enableStreamRequestType_ = enable; }
    bool isEnableStreamRequestType() const { return enableStreamRequestType_; }
    // Java `ClientConfig#pollNameServerInterval`（:58，默认 30000ms）：在用 topic 的
    // 路由刷新周期，start() 时透传给 MQClientInstance。
    void setPollNameServerIntervalMillis(int32_t millis) { pollNameServerIntervalMillis_ = millis; }
    int32_t pollNameServerIntervalMillis() const { return pollNameServerIntervalMillis_; }
    std::string getNamesrvAddr() const;
    std::vector<std::string> getNameServerAddressList() const { return nameServerAddrs_; }
    void setTimeoutMillis(int32_t millis) { timeoutMillis_ = millis; }
    int32_t getTimeoutMillis() const { return timeoutMillis_; }
    // 删除 topic 时一并清理的 KV namespace（对应 Java kvNamespaceToDeleteList）
    void addKvNamespaceToDeleteList(const std::string& ns) {
        kvNamespaceToDeleteList_.push_back(ns);
    }

    // ---------------- ACL 鉴权（对应 Java DefaultMQAdminExt(rpcHook)）----------------
    // 必须在 start() 之前调用。
    void setRPCHook(std::shared_ptr<RPCHook> hook) { rpcHook_ = std::move(hook); }
    void setCredentials(const std::string& accessKey, const std::string& secretKey,
                        const std::string& securityToken = std::string()) {
        rpcHook_ = std::make_shared<AclClientRPCHook>(
            SessionCredentials(accessKey, secretKey, securityToken));
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
    // 对应 Java MQClientAPIImpl#queryTopicsByConsumer:2525（343）的单 broker 原始调用。
    // broker 端走 AdminBrokerProcessor#queryTopicsByConsumer:2421 →
    // ConsumerOffsetManager#whichTopicByConsumer：**从位点表**（topic@group 键）反查该组
    // 消费过哪些 topic，所以组从没提交过位点时回空表，这是预期而不是 bug。
    TopicList queryTopicsByConsumerToBroker(const std::string& brokerAddr,
                                            const std::string& group);
    // 对应 Java DefaultMQAdminExt#queryTopicsByConsumer（DefaultMQAdminExtImpl:1078）：
    // 先按 %RETRY%<group> 查路由，再对路由里每个 broker 下发 343 并合并
    // （Java 的 TopicList.topicList 是 Set<String>，所以这里同样去重）。
    TopicList queryTopicsByConsumer(const std::string& group);
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
    // Java DefaultMQAdminExt:133/:137 的两个方法名，分别固定 LOWER / UPPER 边界。
    int64_t searchLowerBoundaryOffset(const MessageQueue& mq, int64_t timestamp);
    int64_t searchUpperBoundaryOffset(const MessageQueue& mq, int64_t timestamp);
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
    // 对应 Java DefaultMQAdminExt#resetOffsetByQueueId（DefaultMQAdminExtImpl:1827）：**两笔**
    // RPC，缺一不可。先 updateConsumerOffset(25) 直接写 offsetTable，再发一笔带 queueId +
    // offset 的 INVOKE_BROKER_TO_RESET_OFFSET(222)：broker 在 resetOffsetInner 里先按
    // [min, max+1] 校验目标位点，再 assignResetOffset（同时写一次性的 resetOffsetTable
    // 和 offsetTable，并清掉该队列的 POP 在途计数）。Java 返回 void（只打日志），这里把
    // broker 报回的队列表返回，便于调用方核对。
    // 注意实测（5.5.1 真机）这两笔 RPC **不是原子的**：ConsumerOffsetManager#commitOffset
    // 只做覆盖写（offset 变小也只打 [NOTIFYME] warn，不做区间校验），所以第 2 笔被
    // resetOffsetInner 以 "Target offset N not in consume queue range" 拒绝时，第 1 笔已经
    // 把非法位点落库。Java 同样如此，这里不做保护性回滚。
    std::map<MessageQueue, int64_t> resetOffsetByQueueId(
        const std::string& brokerAddr, const std::string& consumerGroup,
        const std::string& topic, int32_t queueId, int64_t resetOffset);

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
    // 一笔 INVOKE_BROKER_TO_RESET_OFFSET(222)。Java 在这里有两个重载：不带 queueId 的按
    // timestamp 重置整个 topic，带 queueId + offset 的只重置单个队列。这里用哨兵值表示
    // 「不带该字段」：queueId < 0 即 Java 的默认 -1，offset < 0 即 Java 的「offset=-1 表示
    // offset 为 null」。
    std::map<MessageQueue, int64_t> invokeBrokerToResetOffset(
        const std::string& brokerAddr, const std::string& topic, const std::string& group,
        int64_t timestamp, bool isForce, bool isCpp, int32_t queueId, int64_t offset);
    static std::string firstBrokerAddr(MQClientInstance& client);
    std::string findFirstBrokerAddr(MQClientInstance& client);
    std::string brokerAddrForMq(MQClientInstance& client, const MessageQueue& mq);
    std::vector<std::string> brokerAddrsOfCluster(MQClientInstance& client,
                                                  const std::string& clusterName);

    std::string instanceName_;
    std::string clientId_;
    std::string unitName_;
    bool enableStreamRequestType_ = false;
    // Java ClientConfig:58，默认 30000ms（路由刷新周期）
    int32_t pollNameServerIntervalMillis_ = 30000;
    std::vector<std::string> nameServerAddrs_;
    std::vector<std::string> kvNamespaceToDeleteList_;
    int32_t timeoutMillis_ = DEFAULT_TIMEOUT;

    std::unique_ptr<MQClientInstance> mqClient_;
    // ACL 钩子，start() 时绑定到 MQClientInstance 的传输层
    std::shared_ptr<RPCHook> rpcHook_;
    bool started_ = false;
};

}  // namespace rocketmq

#endif  // ROCKETMQ_CLIENT_ADMIN_H
