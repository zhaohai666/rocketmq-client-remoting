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

// 对应 Java `ClientConfig#changeInstanceNameToPID`：instanceName 还是默认的 "DEFAULT"
// 时换成 `<pid>#<nanoTime>`，其余原样返回。
//
// 这一步是 clientId 唯一性的来源：不换的话同进程里两个客户端会算出同一个 clientId，
// 而 broker 的消费组 channel 表以 clientId 为键。Java 只在生产者（非
// CLIENT_INNER_PRODUCER）和 CLUSTERING 消费者的 start() 里调用它，条件由各 facade 把。
std::string changeInstanceNameToPID(const std::string& instanceName);

// 对应 Java `ClientConfig#buildMQClientId`：
// `ip@instanceName` + （unitName 非空白时）`@unitName` + （enableStreamRequestType 时）
// `@STREAM`。⚠ 末段用的是 `RequestType.STREAM.name()` 字面量，不是它的 code。
std::string buildMqClientId(const std::string& clientIp, const std::string& instanceName,
                            const std::string& unitName = std::string(),
                            bool enableStreamRequestType = false);

// 未显式配置 clientId 时的默认口径：`<本机 IP>@<instanceName>[@unitName][@STREAM]`
// （对应 Java 的 `changeInstanceNameToPID()` + `buildMQClientId()` 连用，改写那步由调用方按条件做）。
// 旧的 `instanceName@时间戳@pid@seq` 已废弃：把唯一性做在 instanceName 里才是 Java 的做法。
std::string buildClientId(const std::string& instanceName,
                          const std::string& unitName = std::string(),
                          bool enableStreamRequestType = false);

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
    // Java 的 tls.enable 是 JVM 全局系统属性；这里等价为 env ROCKETMQ_TLS_ENABLE。
    static bool tlsEnabledFromEnv();

    // `unitName` 对应 Java `MQClientAPIImpl` 构造里传给 `DefaultTopAddressing` 的那个值：
    // 只影响动态取址的 URL（`-<unitName>` 段）。clientId 的 unitName/@STREAM 后缀在
    // 各 facade 里就已经拼好了（Java 同：`ClientConfig#buildMQClientId`）。
    MQClientInstance(const std::string& clientId,
                     const std::vector<std::string>& nameServerAddrs,
                     int32_t connectTimeoutMillis = 3000,
                     int32_t invokeTimeoutMillis = 15000,
                     bool tlsEnable = tlsEnabledFromEnv(),
                     const std::string& unitName = std::string(),
                     // Java `ClientConfig#pollNameServerInterval`（:58，默认 30000ms）：
                     // 在用 topic 的路由刷新周期，门面 start() 时透传一次。
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

    // ---- 动态 name server（对应 Java MQClientAPIImpl.topAddressing + fetchNameServerAddr）----
    // 未配置 ROCKETMQ_NAMESRV_DOMAIN 时 wsAddr 为空 → fetch 是 no-op，行为不变。
    DefaultTopAddressing& topAddressing() { return topAddressing_; }
    // 取一次地址；变化才应用到 nameServerAddrs_（Java 地址变化才 update）。
    void fetchNameServerAddr();

    // ---- 消费统计（Java MQClientFactory.getConsumerStatsManager，实例级共享）----
    ConsumerStatsManager& consumerStats() { return consumerStats_; }

    // ---- broker 主动通知 NOTIFY_CONSUMER_IDS_CHANGED(40)（实例级，对齐 Java）----
    // Java 把 40 注册在 MQClientAPIImpl（实例级），处理器只做 rebalanceImmediately()。
    // remoting 的处理器表是「一个 code 一个处理器」，所以消费者各自注册会互相覆盖
    // —— 消费者改为向实例登记自己的「叫醒」回调，由实例收到后逐个扇出。
    void registerRebalanceWakeup(const std::string& group, std::function<void()> wakeup);
    void unregisterRebalanceWakeup(const std::string& group);
    // 对应 Java MQClientInstance#rebalanceImmediately（一行 rebalanceService.wakeup()）。
    // 这里没有实例级重平衡线程，改成逐个叫醒已注册消费者自己那份循环。
    void rebalanceImmediately();
    // 收到过多少次 broker 的 40 通知。反向请求只有 broker 发得出来，用例无法注入，
    // 计数是真机断言的唯一落点。
    size_t consumerIdsChangedCount() const { return consumerIdsChangedCount_.load(); }
    // 40 的处理器本体：记 Java 的 INFO 文案 + 计数 + rebalanceImmediately()。
    // Java 整段包在 try/catch 且返回 null（不回包），所以这里也不抛、不回。
    // 公开只为让离线用例能驱动这条反向路径（真连接上 broker 推不进来）。
    void processNotifyConsumerIdsChanged(const RemotingCommand& cmd, const std::string& addr);

    // 安装 RPC 钩子（ACL 鉴权 + 可选的 stream 打标）。对应 Java 在 MQClientAPIImpl
    // 构造时绑定 rpcHook。
    // ⚠ 与 Java 的差异：Java 的传输层持 RPCHook **列表**（后注册者追加在后面），本端口
    // 只有一槽且 first-wins —— 第二个注册者被忽略并返回 false。因此各 facade 必须先把
    // stream 钩子与用户钩子**合成一个**再注册（见 composeRequestHooks），否则顺序就丢了。
    // 钩子必须在 start() 之前设置。
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
    // 只读缓存命中探测（**不触发网络拉取**）：getTopicRouteData 未命中会立刻拉一次，
    // 真机验证「周期刷新何时把新 topic 带进缓存」时用它才不会被按需拉取掩盖周期本身。
    bool isTopicRouteCached(const std::string& topic) const;

    static std::string findBrokerAddrInRoute(const TopicRouteData& route,
                                            const std::string& brokerName);

    // ---------------- 消息发送 ----------------
    // sysFlag 由调用方（Producer）算好：压缩标志与压缩类型位都在这里下发，
    // 且 msg.body 应已经是压缩后的字节（见 DefaultMQProducer::prepareForSend）。
    //
    // `unitMode` 对应 Java `sendKernelImpl:1004` 写进发送头的 `tc.isUnitMode()`
    // （V2 头里映射成单字母键 `k`）。必须**逐次传入**而不是存在实例上：Java 的
    // MQClientInstance 按 clientId 共享，同一实例可能被 unitMode 不同的客户端复用。
    // broker 侧后果见 `AbstractSendMessageProcessor:485-497`（自动建 topic 时打 UNIT 位）。
    //
    // `createTopicKey` / `defaultTopicQueueNums` 对位 Java `sendKernelImpl:996-997`：
    // 这两个值取自**生产者配置**（V2 头的单字母键 `c`/`d`），broker 自动建 topic 时按
    // 它们决定队列数。不传才落回 `TBW102` / 4 —— 写死会让 `setCreateTopicKey` /
    // `setDefaultTopicQueueNums` 变成假 setter。
    // brokerName（键 `n`，`sendKernelImpl:1007`）不在参数里：它跟着 `mq` 走。
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

    // 只**构建** SEND_MESSAGE 请求对象、不发送（对应 Java sendKernelImpl 建头 +
    // MQClientAPIImpl#sendMessage 建 command）。异步发送要跨重试复用同一个请求
    // （Java ``onExceptionImpl`` 只换 opaque、不换队列），所以建与发必须能分开。
    RemotingCommand buildSendRequest(const std::string& producerGroup, const Message& msg,
                                     const MessageQueue& mq, int32_t sysFlag, bool unitMode,
                                     const std::optional<std::string>& createTopicKey = std::nullopt,
                                     const std::optional<int32_t>& defaultTopicQueueNums = std::nullopt);
    // 把应答解析成 SendResult；broker 回了非成功码时抛 MQBrokerException。
    static SendResult parseSendResponse(const RemotingCommand& response, const Message& msg,
                                        const MessageQueue& mq);
    // 异步发出一笔**已建好**的请求（对应 Java MQClientAPIImpl#sendMessageAsync）。
    // onComplete(SendResult, InvokeError) 恰好被调一次：error 为空即成功。
    // ⚠ 与同步发送不同，broker 明确回了错误码时**不换 broker 重试**（needRetry=false），
    //    重试策略由调用方按 Java 的分类实现（见 DefaultMQProducer::sendAsync）。
    //    建连/写失败在本函数上就地抛出（Java 也是同步抛给调用方）。
    void sendMessageAsync(const std::string& addr, RemotingCommand& request, const Message& msg,
                          const MessageQueue& mq, int32_t timeoutMillis,
                          std::function<void(const SendResult&, const InvokeError&)> onComplete);

    // ---------------- 定时消息撤回 ----------------
    // RECALL_MESSAGE(370)，对应 Java MQClientAPIImpl#recallMessage(:3749-3767)：
    // SUCCESS 才取响应头 msgId（被撤回消息的 uniqKey），其余码一律抛 MQBrokerException。
    std::string recallMessage(const std::string& addr, const RecallMessageRequestHeader& header,
                              int32_t timeoutMillis = 3000);

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
    // setZeroIfNotFound 默认 false：Java 的 fetchConsumeOffsetFromBroker 从不设置该字段，
    // 新消费组因此拿到 QUERY_NOT_FOUND 而非 0，调用方才会按 consumeFromWhere 计算起点。
    // 默认 true 会把首次启动的消费者钉在队首重放历史消息，并让 LAST_OFFSET / TIMESTAMP 形同虚设。
    bool queryConsumerOffset(const std::string& consumerGroup, const MessageQueue& mq,
                             int64_t& outOffset, int32_t timeoutMillis = 5000,
                             const std::string& addr = std::string(),
                             bool setZeroIfNotFound = false);
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
    // 对应 Java MQClientAPIImpl#searchOffset(addr, mq, ts, timeout)：内部固定 LOWER。
    int64_t searchOffsetByTimestamp(const MessageQueue& mq, int64_t timestamp,
                                    int32_t timeoutMillis = 5000,
                                    const std::string& addr = std::string());
    // 带边界类型的重载（Java MQClientAPIImpl#searchOffset(addr, mq, ts, boundaryType, timeout)：
    // 时间戳落在队尾之后时 LOWER 给 maxOffset、UPPER 给最后一条自身的位点）。
    // boundaryType 为 nullopt 时不写 boundaryType 字段（Java 已废弃的 5 参重载的形状）。
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

    // 给 POP 出来的消息反构 POP_CK 与 1ST_POP_TIME（对应 Java
    // MQClientAPIImpl.processPopResponse 的前半段）。
    //
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
    // 注销走 Java 的 mqClientApiTimeout（3000ms，见下面 kMqClientApiTimeoutMillis 的出处）：
    // Java `MQClientInstance#unregisterClient:1170` 传的就是它，与发送/拉取预算无关。
    void unregisterClient(const std::string& addr, const std::string& clientId,
                          const std::string& producerGroup, const std::string& consumerGroup,
                          int32_t timeoutMillis = kMqClientApiTimeoutMillis);
    // 向所有已知 broker（**主 + 从**，见 getAllBrokerAddrs）注销本 clientId
    // （对应 Java MQClientInstance.unregisterClient）：
    // 关闭连接前调用，broker 端立刻摘除，不必等心跳超时（~120s）。
    void unregisterClientAllBrokers(const std::string& clientId,
                                    const std::string& producerGroup,
                                    const std::string& consumerGroup,
                                    int32_t timeoutMillis = kMqClientApiTimeoutMillis);

    // ---------------- CHECK_CLIENT_CONFIG(46)：订阅表达式向 broker 求证 ----------------
    // Java `ClientConfig#mqClientApiTimeout` 的默认值（`ClientConfig.java:81` = 3 * 1000）：
    // 管理类短 RPC 走的就是它，与发送/拉取的超时预算无关。
    static constexpr int32_t kMqClientApiTimeoutMillis = 3000;

    // 对应 Java MQClientInstance#findBrokerAddrByTopic:1390：**只读缓存**路由，随机挑其中
    // 一个 broker（优先 master 地址）；没有缓存返回空串，由调用方决定跳过（不抛）。
    // 与 getTopicRouteData 的分工照抄 Java：后者缓存空了会补拉一次路由。
    std::string findBrokerAddrByTopic(const std::string& topic);

    // 一笔 CHECK_CLIENT_CONFIG(46)（Java MQClientAPIImpl#checkClientInBroker:3256）：
    // 请求头是 null、body 是 CheckClientRequestBody 的 JSON；broker 非 SUCCESS 时用
    // **响应码**抛 MQClientException（SUBSCRIPTION_PARSE_FAILED=23、
    // 未开 enablePropertyFilter 的 SYSTEM_ERROR=1 都走这里）。
    // Java 的 brokerVIPChannel(vipChannelEnabled=false) 是恒等变换，本端口不实现。
    void checkClientConfig(const std::string& brokerAddr, const std::string& consumerGroup,
                           const std::string& clientId, const SubscriptionData& subscriptionData,
                           int32_t timeoutMillis = kMqClientApiTimeoutMillis);

    // 对应 Java MQClientInstance#checkClientInBroker:534 的内层循环：只把**非 TAG**
    // （SQL92 / CLASS_FILTER）的表达式发给 broker 校验，查不到路由的订阅跳过。
    // 为什么必须发：SQL92 表达式写错时 broker 的 ExpressionMessageFilter 拿不到编译好的
    // 过滤数据会**直接放行全部消息**（返回 true），静默变成「订阅全部消息」、启动也不报错；
    // 这一调用把「写错的表达式」变成启动期一次显式失败。
    // 单独暴露是因为本端口的 MQClientInstance 没有 Java 的 consumerTable（消费者各自持有
    // 实例），由消费者在 start() 里带着自己那份订阅调它 —— 语义与 Java 逐分支一致。
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
    // 路由里出现过的**每一台** broker（主 + 从）。`getRouteOfAllBrokers` 走
    // `selectBrokerAddr()`（主优先、没主才随机），适合「问到一台就行」的心跳；注销(35)
    // 必须用这个 —— Java `MQClientInstance#unregisterClient`:1158-1182 遍历的是
    // `brokerAddrTable` 的每个 brokerId，而 Producer/ConsumerManager 是每台 broker
    // 各自一份状态，漏掉从节点就等于那台的注册要等通道扫描（默认 ~120s）才回收。
    std::vector<std::string> getAllBrokerAddrs();

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
    // Java ClientConfig:58 的实例级副本（构造时定型，运行期改不重排已启动的周期任务）
    int32_t pollNameServerIntervalMillis_ = 30000;

    mutable std::recursive_mutex routeLock_;
    std::map<std::string, TopicRouteData> topicRouteTable_;
    std::map<std::string, std::shared_ptr<TopicPublishInfo>> topicPublishInfoTable_;
    // 在用 topic（消费者订阅 + 生产者发送过的），由周期任务刷新路由
    std::set<std::string> topicsInUse_;
    bool tlsEnable_ = false;
    bool started_ = false;
    std::atomic<bool> routeRefreshStop_{false};
    std::thread routeRefreshThread_;
    // 动态 name server 周期刷新（Java scheduleAtFixedRate(fetchNameServerAddr, 10s, 2min)）
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
