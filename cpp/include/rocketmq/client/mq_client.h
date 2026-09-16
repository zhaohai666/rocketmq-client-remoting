// MQClientInstance：客户端核心编排（对应 org.apache.rocketmq.client.impl.factory.MQClientInstance
// 与 MQClientAPIImpl 的核心调用面）。
//
// 职责：NameServer 地址管理、Topic 路由获取与缓存、Broker 地址解析、
// 消息发送（SEND_MESSAGE_V2）、拉取（PULL_MESSAGE）、offset 查询/更新、心跳、
// 按 Key 查询消息、创建 Topic。
//
// 与 Python 参考实现（python/client/mq_client.py）逐项对齐。
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
#include <vector>

#include "rocketmq/client/result.h"
#include "rocketmq/common/byte_buffer.h"
#include "rocketmq/common/message.h"
#include "rocketmq/common/topic_config.h"
#include "rocketmq/remoting/protocol/body.h"
#include "rocketmq/remoting/protocol/heartbeat.h"
#include "rocketmq/remoting/protocol/remoting_command.h"
#include "rocketmq/remoting/protocol/route.h"
#include "rocketmq/remoting/remoting_client.h"

namespace rocketmq {

// 生成唯一 clientId：instanceName@yyyymmddhhmmss@pid@seq。
// Java 用 ip@instanceName@unitName(pid 派生)，Python 用 instanceName@时间戳；
// 这里额外带 pid 与进程内序号，保证同一秒内多次启动的客户端（如测试里连续起
// 多个 consumer）不会撞 clientId —— broker 的消费组 channel 表以 clientId 为键。
std::string buildClientId(const std::string& instanceName);

// 对应 org.apache.rocketmq.client.impl.producer.TopicPublishInfo
//
// 注意：本类型的**轮询游标是共享状态**（Java 用 ThreadLocal，Python 用缓存的单例），
// 因此不提供拷贝语义 —— 调用方一律通过 shared_ptr 使用 getTopicPublishInfo() 返回的
// 缓存实例，这样多次发送才能在队列间真正轮转，而不是每次都从 0 号队列开始。
class TopicPublishInfo {
public:
    bool orderTopic = false;
    std::vector<MessageQueue> msgQueueList;
    TopicRouteData topicRouteData;

    TopicPublishInfo() = default;
    TopicPublishInfo(const TopicPublishInfo&) = delete;
    TopicPublishInfo& operator=(const TopicPublishInfo&) = delete;

    bool ok() const { return !msgQueueList.empty(); }

    // 轮询选择（对应 Java selectOneMessageQueue）
    MessageQueue selectOneMessageQueue();
    // 避开上一次失败的 broker（对应 Java selectOneMessageQueue(lastBrokerName)）
    MessageQueue selectOneMessageQueue(const std::string& lastBrokerName);

    // 带过滤器的轮询（对应 Python select_one_message_queue(*filters)）：游标照常推进，
    // 一轮内全部不匹配返回 nullopt，由调用方退化选择。
    // 全部过滤器都通过才选中。
    std::optional<MessageQueue> selectOneMessageQueue(
        const std::function<bool(const MessageQueue&)>& filter,
        const std::function<bool(const MessageQueue&)>& brokerFilter);
    // 重置轮询游标（对应 Python reset_index，故障规避 resetIndex 用）
    void resetIndex() { index_.store(0); }

private:
    std::atomic<uint64_t> index_{0};
};

class MQClientInstance {
public:
    MQClientInstance(const std::string& clientId,
                     const std::vector<std::string>& nameServerAddrs,
                     int32_t connectTimeoutMillis = 3000,
                     int32_t invokeTimeoutMillis = 15000);
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

    // 安装 RPC 钩子（ACL 鉴权）。对应 Java 在 MQClientInstance 构造时绑定 rpcHook。
    // **first-wins**：同一 clientId 的实例被复用，第二个注册者不会覆盖（与 Java 一致），
    // 此时返回 false。故钩子必须在 start() 之前设置。
    bool registerRPCHook(std::shared_ptr<RPCHook> hook) {
        return remotingClient_->registerRPCHook(std::move(hook));
    }

    // ---------------- 路由管理 ----------------
    // 从 NameServer 拉取 topic 路由。未知 topic 会回退到 MixAll::DEFAULT_TOPIC
    // （5.x nameserver 不为未知 topic 合成路由，返回 TOPIC_NOT_EXIST）。
    // isDefault=true 时未知 topic 才会回退到默认 topic（TBW102）来合成发布信息——
    // 这**只有生产者**在真实路由拉不到时才允许（对应 Java DefaultMQProducerImpl
    // 的 tryToFindTopicPublishInfo）。消费者路径必须传 false（默认），否则 %RETRY%group
    // 这类尚未由 broker 创建的主题会被合成出一组假队列，两个实例视图不一致。
    bool updateTopicRouteInfoFromNameServer(const std::string& topic,
                                            int32_t timeoutMillis = 5000,
                                            bool isDefault = false);
    // 取发布信息（**缓存实例共享**，轮询游标在实例内推进）；
    // 缓存未命中会触发一次路由刷新，仍拿不到则抛 MQClientException。
    // isDefault 透传给 updateTopicRouteInfoFromNameServer（仅生产者发送路径显式传 true）。
    std::shared_ptr<TopicPublishInfo> getTopicPublishInfo(const std::string& topic,
                                                         bool isDefault = false);
    // 登记「在用」topic，交给后台周期任务刷新路由（对应 Java 的订阅/发布 topic 列表）。
    // 没有它，路由变化（新 topic 被 broker 创建、队列扩容）只能等下一次 rebalance
    // 或生产者下次发送才被发现。
    void registerTopicInUse(const std::string& topic);
    std::shared_ptr<TopicRouteData> getTopicRouteData(const std::string& topic);

    static std::string findBrokerAddrInRoute(const TopicRouteData& route,
                                            const std::string& brokerName);

    // ---------------- 消息发送 ----------------
    // sysFlag 由调用方（Producer）算好：压缩标志与压缩类型位都在这里下发，
    // 且 msg.body 应已经是压缩后的字节（见 DefaultMQProducer::prepareForSend）。
    SendResult sendMessage(const std::string& producerGroup, const Message& msg,
                           const MessageQueue& mq, int32_t timeoutMillis = 3000,
                           int32_t sysFlag = 0);
    void sendMessageOneway(const std::string& producerGroup, const Message& msg,
                           const MessageQueue& mq, int32_t timeoutMillis = 3000,
                           int32_t sysFlag = 0);

    // ---------------- 消息拉取 ----------------
    PullResult pullMessage(const std::string& consumerGroup, const MessageQueue& mq,
                           int64_t queueOffset, int32_t maxMsgNums, int32_t sysFlag,
                           int64_t commitOffset, const std::string& subscription,
                           int64_t subVersion, const std::string& expressionType,
                           int32_t timeoutMillis = 30000, int32_t maxMsgBytes = -1,
                           int32_t suspendTimeoutMillis = 15000,
                           const std::string& addr = std::string(),
                           int32_t requestSource = 0);

    // ---------------- 消费位点 ----------------
    // 返回 false 表示 broker 回 QUERY_NOT_FOUND（消费组尚无位点）
    bool queryConsumerOffset(const std::string& consumerGroup, const MessageQueue& mq,
                             int64_t& outOffset, int32_t timeoutMillis = 5000,
                             const std::string& addr = std::string(),
                             bool setZeroIfNotFound = true);
    void updateConsumerOffset(const std::string& consumerGroup, const MessageQueue& mq,
                              int64_t commitOffset, int32_t timeoutMillis = 5000,
                              const std::string& addr = std::string());
    // 批量锁/解锁队列（顺序消费，Java MQClientAPIImpl.lockBatchMQ / unlockBatchMQ）。
    // 按 broker 分组发送；lockBatchMq 返回 broker 确认锁定成功的队列集（lockOKMQSet）。
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

    // ---------------- 心跳 / 注销 ----------------
    void sendHeartbeat(const std::string& addr, const HeartbeatData& heartbeatData,
                       int32_t timeoutMillis = 5000);
    void unregisterClient(const std::string& addr, const std::string& clientId,
                          const std::string& producerGroup, const std::string& consumerGroup,
                          int32_t timeoutMillis = 5000);
    // 向所有已知 broker 注销本 clientId（对应 Java MQClientInstance.unregisterClient）：
    // 关闭连接前调用，broker 端立刻摘除，不必等心跳超时（~120s）。
    void unregisterClientAllBrokers(const std::string& clientId,
                                    const std::string& producerGroup,
                                    const std::string& consumerGroup,
                                    int32_t timeoutMillis = 5000);

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
    // 按 topic 路由找到的 master broker 查消费组 clientId 列表（对应 Java
    // MQClientInstance.findConsumerIdList）。查不到/无路由/非 SUCCESS 返回空 vector，
    // 调用方按 Java 语义「保留当前分配」，不要回退成独占全部队列。
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
    // 对应 Java MQAdminImpl.queryMessage：查该 topic 全部 broker 并做客户端侧二次校验
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
    std::vector<std::string> getRouteOfAllBrokers();
    // 列出已知路由里所有 broker 地址（用于探活）
    std::vector<std::string> knownBrokerAddrs();

private:
    // 解析 mq 对应 broker 地址；找不到抛 MQClientException
    std::string brokerAddr(const MessageQueue& mq);
    RemotingCommand invokeSyncOnAddr(const std::string& addr, RemotingCommand& request,
                                     int32_t timeoutMillis);
    // 后台路由刷新循环（对应 Java startScheduledTask 的 updateTopicRouteInfoFromNameServer 周期任务）
    void routeRefreshLoop();

    std::string clientId_;
    std::vector<std::string> nameServerAddrs_;
    std::unique_ptr<RemotingClient> remotingClient_;

    mutable std::recursive_mutex routeLock_;
    std::map<std::string, TopicRouteData> topicRouteTable_;
    std::map<std::string, std::shared_ptr<TopicPublishInfo>> topicPublishInfoTable_;
    // 在用 topic（消费者订阅 + 生产者发送过的），由周期任务刷新路由
    std::set<std::string> topicsInUse_;
    bool started_ = false;
    bool routeRefreshStop_ = false;
    std::thread routeRefreshThread_;
};

}  // namespace rocketmq

#endif  // ROCKETMQ_CLIENT_MQ_CLIENT_H
