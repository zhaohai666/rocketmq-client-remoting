// 职责：NameServer 地址管理、Topic 路由获取与缓存、Broker 地址解析、
// 消息发送（SEND_MESSAGE_V2）、拉取（PULL_MESSAGE）、offset 查询/更新、心跳、
// 按 Key 查询消息、创建 Topic。
#ifndef ROCKETMQ_CLIENT_MQ_CLIENT_H
#define ROCKETMQ_CLIENT_MQ_CLIENT_H

#include <atomic>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <set>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "rocketmq/client/consumer_stats.h"
#include "rocketmq/client/result.h"
#include "rocketmq/common/byte_buffer.h"
#include "rocketmq/common/message.h"
#include "rocketmq/common/top_addressing.h"
#include "rocketmq/common/topic_config.h"
#include "rocketmq/remoting/protocol/body.h"
#include "rocketmq/remoting/protocol/headers.h"
#include "rocketmq/remoting/protocol/heartbeat.h"
#include "rocketmq/remoting/protocol/remoting_command.h"
#include "rocketmq/remoting/protocol/route.h"
#include "rocketmq/remoting/remoting_client.h"

namespace rocketmq {

std::string changeInstanceNameToPID(const std::string& instanceName);

std::string buildMqClientId(const std::string& clientIp, const std::string& instanceName,
                            const std::string& unitName = std::string(),
                            bool enableStreamRequestType = false);

std::string buildClientId(const std::string& instanceName,
                          const std::string& unitName = std::string(),
                          bool enableStreamRequestType = false);

class TopicPublishInfo {
public:
    bool orderTopic = false;
    std::vector<MessageQueue> msgQueueList;
    TopicRouteData topicRouteData;

    TopicPublishInfo() = default;
    TopicPublishInfo(const TopicPublishInfo&) = delete;
    TopicPublishInfo& operator=(const TopicPublishInfo&) = delete;

    bool ok() const { return !msgQueueList.empty(); }

    MessageQueue selectOneMessageQueue();
    MessageQueue selectOneMessageQueue(const std::string& lastBrokerName);

    // 带过滤器的轮询：游标照常推进，
    // 一轮内全部不匹配返回 nullopt，由调用方退化选择。
    // 全部过滤器都通过才选中。
    std::optional<MessageQueue> selectOneMessageQueue(
        const std::function<bool(const MessageQueue&)>& filter,
        const std::function<bool(const MessageQueue&)>& brokerFilter);
    // 重置轮询游标（故障规避 resetIndex 用）
    void resetIndex() { index_.store(0); }

private:
    std::atomic<uint64_t> index_{0};
};

class MQClientInstance {
public:
    static bool tlsEnabledFromEnv();

    MQClientInstance(const std::string& clientId,
                     const std::vector<std::string>& nameServerAddrs,
                     int32_t connectTimeoutMillis = 3000,
                     int32_t invokeTimeoutMillis = 15000,
                     bool tlsEnable = tlsEnabledFromEnv(),
                     const std::string& unitName = std::string(),
                     int32_t pollNameServerIntervalMillis = 30000);
    ~MQClientInstance();

    MQClientInstance(const MQClientInstance&) = delete;
    MQClientInstance& operator=(const MQClientInstance&) = delete;

    // ---------------- 生命周期 ----------------
    void start();
    void shutdown();

    const std::string& clientId() const { return clientId_; }
    std::vector<std::string> nameServerAddrs() const;
    void updateNameServerAddressList(const std::vector<std::string>& addrs);
    RemotingClient& remotingClient() { return *remotingClient_; }
    // 实例持有的路由刷新周期（离线/真机用例断言门面透传结果用）。
    int32_t pollNameServerIntervalMillis() const { return pollNameServerIntervalMillis_; }

    DefaultTopAddressing& topAddressing() { return topAddressing_; }
    void fetchNameServerAddr();

    ConsumerStatsManager& consumerStats() { return consumerStats_; }

    void registerRebalanceWakeup(const std::string& group, std::function<void()> wakeup);
    void unregisterRebalanceWakeup(const std::string& group);
    void rebalanceImmediately();
    // 收到过多少次 broker 的 40 通知。反向请求只有 broker 发得出来，用例无法注入，
    // 计数是真机断言的唯一落点。
    size_t consumerIdsChangedCount() const { return consumerIdsChangedCount_.load(); }
    void processNotifyConsumerIdsChanged(const RemotingCommand& cmd, const std::string& addr);

    bool registerRPCHook(std::shared_ptr<RPCHook> hook) {
        return remotingClient_->registerRPCHook(std::move(hook));
    }

    // ---- TLS 严格校验选项（四端口径统一：caCert 非空 = 证书链 + 主机名严格校验）----
    // 必须在 start() 之前调用；调用后自动开启 TLS（等价 setTlsEnable(true)）并把选项
    // 转发给底层 RemotingClient，首条连接建立时生效。
    void setTlsOptions(const TlsOptions& options) {
        tlsOptions_ = options;
        tlsEnable_ = true;
        remotingClient_->setTlsOptions(options);
    }

    void setSocksProxy(const SocksProxyConfig& proxy) {
        remotingClient_->setSocksProxy(proxy);
    }

    bool updateTopicRouteInfoFromNameServer(const std::string& topic,
                                            int32_t timeoutMillis = 5000,
                                            bool isDefault = false);
    // 取发布信息（**缓存实例共享**，轮询游标在实例内推进）；
    // 缓存未命中会触发一次路由刷新，仍拿不到则抛 MQClientException。
    // isDefault 透传给 updateTopicRouteInfoFromNameServer（仅生产者发送路径显式传 true）。
    std::shared_ptr<TopicPublishInfo> getTopicPublishInfo(const std::string& topic,
                                                         bool isDefault = false);
    std::vector<MessageQueue> getTopicSubscribeInfo(const std::string& topic);
    void registerTopicInUse(const std::string& topic);
    std::shared_ptr<TopicRouteData> getTopicRouteData(const std::string& topic);
    // 只读缓存命中探测（**不触发网络拉取**）：getTopicRouteData 未命中会立刻拉一次，
    // 真机验证「周期刷新何时把新 topic 带进缓存」时用它才不会被按需拉取掩盖周期本身。
    bool isTopicRouteCached(const std::string& topic) const;

    static std::string findBrokerAddrInRoute(const TopicRouteData& route,
                                             const std::string& brokerName);

    static std::pair<std::string, bool> findBrokerAddressInSubscribe(
        const std::map<int64_t, std::string>& brokerAddrs, int64_t brokerId,
        bool onlyThisBroker = false);

    // ---------------- 消息发送 ----------------
    // sysFlag 由调用方（Producer）算好：压缩标志与压缩类型位都在这里下发，
    // 且 msg.body 应已经是压缩后的字节（见 DefaultMQProducer::prepareForSend）。
    SendResult sendMessage(const std::string& producerGroup, const Message& msg,
                           const MessageQueue& mq, int32_t timeoutMillis = 3000,
                           int32_t sysFlag = 0, bool unitMode = false,
                           const std::optional<std::string>& createTopicKey = std::nullopt,
                           const std::optional<int32_t>& defaultTopicQueueNums = std::nullopt);
    void sendMessageOneway(const std::string& producerGroup, const Message& msg,
                           const MessageQueue& mq, int32_t timeoutMillis = 3000,
                           int32_t sysFlag = 0, bool unitMode = false,
                           const std::optional<std::string>& createTopicKey = std::nullopt,
                           const std::optional<int32_t>& defaultTopicQueueNums = std::nullopt);

    RemotingCommand buildSendRequest(const std::string& producerGroup, const Message& msg,
                                     const MessageQueue& mq, int32_t sysFlag, bool unitMode,
                                     const std::optional<std::string>& createTopicKey = std::nullopt,
                                     const std::optional<int32_t>& defaultTopicQueueNums = std::nullopt);
    // 把应答解析成 SendResult；broker 回了非成功码时抛 MQBrokerException。
    static SendResult parseSendResponse(const RemotingCommand& response, const Message& msg,
                                        const MessageQueue& mq);
    void sendMessageAsync(const std::string& addr, RemotingCommand& request, const Message& msg,
                          const MessageQueue& mq, int32_t timeoutMillis,
                          std::function<void(const SendResult&, const InvokeError&)> onComplete);

    std::string recallMessage(const std::string& addr, const RecallMessageRequestHeader& header,
                              int32_t timeoutMillis = 3000);

    PullResult pullMessage(const std::string& consumerGroup, const MessageQueue& mq,
                           int64_t queueOffset, int32_t maxMsgNums, int32_t sysFlag,
                           int64_t commitOffset, const std::string& subscription,
                           int64_t subVersion, const std::string& expressionType,
                           int32_t timeoutMillis = 30000, int32_t maxMsgBytes = -1,
                           int32_t suspendTimeoutMillis = 15000,
                           const std::string& addr = std::string(),
                           int32_t requestSource = 0,
                           std::optional<int64_t> brokerId = std::nullopt);

    bool queryConsumerOffset(const std::string& consumerGroup, const MessageQueue& mq,
                             int64_t& outOffset, int32_t timeoutMillis = 5000,
                             const std::string& addr = std::string(),
                             bool setZeroIfNotFound = false);
    void updateConsumerOffset(const std::string& consumerGroup, const MessageQueue& mq,
                              int64_t commitOffset, int32_t timeoutMillis = 5000,
                              const std::string& addr = std::string());
    std::vector<MessageQueue> lockBatchMq(const std::string& consumerGroup,
                                          const std::string& clientId,
                                          const std::vector<MessageQueue>& mqs,
                                          int32_t timeoutMillis = 5000);
    void unlockBatchMq(const std::string& consumerGroup, const std::string& clientId,
                       const std::vector<MessageQueue>& mqs, int32_t timeoutMillis = 5000);
    int64_t getMaxOffset(const MessageQueue& mq, int32_t timeoutMillis = 5000,
                         const std::string& addr = std::string());
    int64_t getMinOffset(const MessageQueue& mq, int32_t timeoutMillis = 5000,
                         const std::string& addr = std::string());
    int64_t searchOffsetByTimestamp(const MessageQueue& mq, int64_t timestamp,
                                    int32_t timeoutMillis = 5000,
                                    const std::string& addr = std::string());
    int64_t searchOffsetByBoundary(const MessageQueue& mq, int64_t timestamp,
                                   const std::optional<BoundaryType>& boundaryType,
                                   int32_t timeoutMillis = 5000,
                                   const std::string& addr = std::string());

    // ---------------- POP 模式（5.x 轻量消费） ----------------
    //
    // 与 pull 的语义差别：**不提交位点**，消费完成后用 ackMessage 确认；不 ack 的消息
    // 在 invisibleTime 之后被 broker 复活并重投到 %RETRY%<group>_<topic>（V1），
    // 下次 POP 会再弹回来（至少一次语义）。queueId = -1 表示弹该 topic 的所有队列。
    //
    // 返回的每条消息都已被盖上 POP_CK（客户端反构的 8 段 CK 串）与 1ST_POP_TIME。
    PopResult popMessage(const std::string& consumerGroup, const std::string& topic,
                         int32_t queueId, int32_t maxMsgNums, int64_t invisibleTime,
                         int64_t pollTime, int32_t initMode,
                         const std::string& expression = std::string(),
                         const std::string& expressionType = std::string(),
                         bool order = false, int32_t timeoutMillis = 10000,
                         const std::string& brokerName = std::string(),
                         const std::string& addr = std::string());

    // 确认一条 POP 消息。extraInfo 用消息上的 POP_CK 属性；offset 必须是
    // **consumeQueue offset**（即 CK 串第 8 段），不是 commitlog offset —— 传错
    // broker 会回 NO_MESSAGE(208)。返回 broker 响应码，SUCCESS(0) 即成功。
    int32_t ackMessage(const std::string& consumerGroup, const std::string& topic,
                       int32_t queueId, const std::string& extraInfo, int64_t offset,
                       int32_t timeoutMillis = 3000,
                       const std::string& brokerName = std::string(),
                       const std::string& addr = std::string());

    // 延长 POP 消息的不可见时间。响应返回**新的** popTime/invisibleTime/reviveQid，
    // 客户端据此重建 8 段 extraInfo（结果里的 extraInfo 字段）供后续 ACK 使用。
    ChangeInvisibleTimeResult changeInvisibleTime(const std::string& consumerGroup,
                                                  const std::string& topic, int32_t queueId,
                                                  const std::string& extraInfo, int64_t offset,
                                                  int64_t invisibleTime,
                                                  int32_t timeoutMillis = 3000,
                                                  const std::string& brokerName = std::string(),
                                                  const std::string& addr = std::string());

    // 单独暴露成静态函数是为了**可单测**：这是整个 POP 实现里最容易踩坑、
    // 又最难靠真机定位的一段逻辑（普通 topic 直连 POP 时 broker 不写 POP_CK，
    // 必须由客户端用 startOffsetInfo/msgOffsetInfo 反构，否则 ACK 无从下手）。
    //
    // 注意：调用方必须在**改写消息 topic 之前**调用，因为 retryFlag 是从消息的
    // 原始 topic 推出来的（broker 可能改写 topic）。
    static void stampPopCk(std::vector<MessageExt>& msgs, const std::string& brokerName,
                           const PopMessageResponseHeader& respHeader);

    // ---------------- 心跳 / 注销 ----------------
    void sendHeartbeat(const std::string& addr, const HeartbeatData& heartbeatData,
                       int32_t timeoutMillis = 5000);
    void unregisterClient(const std::string& addr, const std::string& clientId,
                          const std::string& producerGroup, const std::string& consumerGroup,
                          int32_t timeoutMillis = kMqClientApiTimeoutMillis);
    void unregisterClientAllBrokers(const std::string& clientId,
                                    const std::string& producerGroup,
                                    const std::string& consumerGroup,
                                    int32_t timeoutMillis = kMqClientApiTimeoutMillis);

    static constexpr int32_t kMqClientApiTimeoutMillis = 3000;

    std::string findBrokerAddrByTopic(const std::string& topic);

    void checkClientConfig(const std::string& brokerAddr, const std::string& consumerGroup,
                           const std::string& clientId, const SubscriptionData& subscriptionData,
                           int32_t timeoutMillis = kMqClientApiTimeoutMillis);

    void checkSubscriptionsInBroker(const std::string& group,
                                    const std::vector<SubscriptionData>& subs);

    // ---------------- 通用同步调用（管理端复用）----------------
    // 下发任意 requestCode + extFields + body。languageOverride >= 0 时覆盖请求的
    // language 字段：个别 RPC 会按它改变行为（INVOKE_BROKER_TO_RESET_OFFSET 对
    // CPP/PYTHON 才返回可解析的 offsetTable 响应体）。
    //
    // invokeSyncRaw 不做响应码校验，留给调用方自己判断（管理端很多接口的"未找到"
    // 是正常分支，例如 QUERY_NOT_FOUND）；invokeSync 会抛 MQBrokerException。
    RemotingCommand invokeSyncRaw(const std::string& addr, int32_t code,
                                  const PropertyMap& extFields = PropertyMap(),
                                  const Bytes& body = Bytes(), bool hasBody = false,
                                  int32_t timeoutMillis = 3000,
                                  int32_t languageOverride = -1);
    RemotingCommand invokeSync(const std::string& addr, int32_t code,
                               const PropertyMap& extFields = PropertyMap(),
                               const Bytes& body = Bytes(), bool hasBody = false,
                               int32_t timeoutMillis = 3000,
                               int32_t languageOverride = -1);
    // 把响应码非 SUCCESS 转成 MQBrokerException
    static void checkResponseCode(const RemotingCommand& response);

    // ---------------- 管理类 ----------------
    void createTopicInBroker(const std::string& brokerAddr, const std::string& defaultTopic,
                             const std::string& topic, int32_t readQueueNums = 4,
                             int32_t writeQueueNums = 4, int32_t perm = 6,
                             int32_t topicSysFlag = 0,
                             const std::string& topicFilterType = TopicFilterType::SINGLE_TAG,
                             const std::string& attributes = std::string(),
                             bool force = false, int32_t timeoutMillis = 5000,
                             int32_t retryTimes = 5);
    void createTopicInRoute(const std::string& topic, int32_t readQueueNums = 4,
                            int32_t writeQueueNums = 4, int32_t perm = 6,
                            int32_t timeoutMillis = 5000);
    void deleteTopicInBroker(const std::string& brokerAddr, const std::string& topic,
                             int32_t timeoutMillis = 5000);
    void deleteTopicInNamesrv(const std::string& topic, int32_t timeoutMillis = 5000);

    // ---------------- 集群 / Topic / 消费者列表 ----------------
    ClusterInfo getBrokerClusterInfo(int32_t timeoutMillis = 10000);
    TopicList getAllTopicListFromNameServer(int32_t timeoutMillis = 10000);
    GetConsumerListByGroupResponseBody getConsumerListByGroup(
        const std::string& consumerGroup, const std::string& addr,
        int32_t timeoutMillis = 5000);
    std::vector<std::string> getConsumerIdListByGroup(const std::string& topic,
                                                     const std::string& consumerGroup,
                                                     int32_t timeoutMillis = 5000);

    // ---------------- 按 Key / uniqKey 查消息 ----------------
    // indexType 见 MessageConst::INDEX_*_TYPE；uniqKey 为 true 时额外下发
    // MixAll::UNIQUE_MSG_QUERY_FLAG，命中后按 msgId == key 二次校验。
    bool queryMessage(const std::string& topic, const std::string& key, int32_t maxNum,
                      int64_t beginTimestamp, int64_t endTimestamp, Bytes& outBody,
                      int32_t timeoutMillis = 15000, const std::string& addr = std::string(),
                      const std::string& indexType = std::string(), bool uniqKey = false);
    std::vector<MessageExt> queryMessageAllBrokers(const std::string& topic,
                                                   const std::string& key, int32_t maxNum,
                                                   int64_t beginTimestamp, int64_t endTimestamp,
                                                   const std::string& indexType = std::string(),
                                                   bool uniqKey = false,
                                                   int32_t timeoutMillis = 15000);

    // ---------------- 工具 ----------------
    std::string brokerAddrOf(const std::string& brokerName);
    // 解析 mq 对应 broker 地址（公开版；找不到抛 MQClientException）
    std::string brokerAddrForMq(const MessageQueue& mq);

    std::string findBrokerAddressInPublish(const std::string& brokerName);

    std::string getBrokerNameFromMessageQueue(const MessageQueue& mq);

    // 定点发送不会在 sendDefaultImpl 里取发布信息，这里是它唯一的路由来源；
    // 主从切换期间这也是「本端立刻报错」与「把请求打到从节点上白挨一轮
    // SYSTEM_BUSY(2)」的分水岭 —— 从节点不接 SEND_MESSAGE / CONSUMER_SEND_MSG_BACK /
    // CHANGE_INVISIBLE_TIME 这些写请求。
    std::string publishAddrFor(const std::string& brokerName, const std::string& topic);
    // POP 三兄弟（popMessage / ackMessage / changeInvisibleTime）的地址解析。
    //
    //
    // brokerName 传引用：调用方没给时按路由挑第一台的 brokerName 会被就地补上
    // （brokerName 为空时用它去反构 POP_CK，retryFlag 会算错）；addrIn 非空则原样返回。
    std::string resolvePopAddr(const std::string& topic, std::string& brokerName,
                               const std::string& addrIn);
    // POP 的 ack / changeInvisibleTime 地址解析。
    std::string popAckAddrFor(const std::string& topic, const std::string& brokerName,
                              int32_t queueId, const std::string& addrIn);
    std::vector<std::string> getRouteOfAllBrokers();
    // 列出已知路由里所有 broker 地址（用于探活）
    std::vector<std::string> knownBrokerAddrs();
    std::vector<std::string> getAllBrokerAddrs();

private:
    // 解析 mq 对应 broker 地址；找不到抛 MQClientException
    std::string brokerAddr(const MessageQueue& mq);
    std::string publishAddrInAdmin(const MessageQueue& mq);
    std::string consumerOffsetAddr(const MessageQueue& mq);
    RemotingCommand invokeSyncOnAddr(const std::string& addr, RemotingCommand& request,
                                     int32_t timeoutMillis);
    void routeRefreshLoop();

    // 离线单测播种端点表用（离线没有名字服务；产品代码只在路由落库块里写这张表）。
    friend struct MQClientInstanceTestAccess;

    std::string clientId_;
    std::vector<std::string> nameServerAddrs_;
    std::unique_ptr<RemotingClient> remotingClient_;
    int32_t pollNameServerIntervalMillis_ = 30000;

    mutable std::recursive_mutex routeLock_;
    std::map<std::string, TopicRouteData> topicRouteTable_;
    // 最近一次「全部 NS 都不可达」的失败原因（updateTopicRouteInfoFromNameServer
    // 里写入，getTopicPublishInfo 报 "Can not find Message Queue" 时带上 ——
    // TLS 握手 CA 校验失败这类根因不能黑盒成路由缺失）。读多写少，独立小锁。
    std::mutex lastRouteErrorMutex_;
    std::string lastRouteError_;
    std::map<std::string, std::map<int64_t, std::string>> brokerAddrTable_;
    std::map<std::string, std::map<MessageQueue, std::string>> topicEndPointsTable_;
    std::map<std::string, std::shared_ptr<TopicPublishInfo>> topicPublishInfoTable_;
    // 在用 topic（消费者订阅 + 生产者发送过的），由周期任务刷新路由
    std::set<std::string> topicsInUse_;
    bool tlsEnable_ = false;
    TlsOptions tlsOptions_;
    bool started_ = false;
    std::atomic<bool> routeRefreshStop_{false};
    std::thread routeRefreshThread_;
    std::atomic<bool> namesrvRefreshStop_{false};
    std::thread namesrvRefreshThread_;
    void namesrvRefreshLoop();
    DefaultTopAddressing topAddressing_;
    ConsumerStatsManager consumerStats_;
    // group → 消费者的「叫醒」回调（消费者 shutdown 时摘掉，避免悬垂 this）
    std::mutex rebalanceWakeupLock_;
    std::map<std::string, std::function<void()>> rebalanceWakeups_;
    std::atomic<size_t> consumerIdsChangedCount_{0};
};

}  // namespace rocketmq

#endif  // ROCKETMQ_CLIENT_MQ_CLIENT_H
