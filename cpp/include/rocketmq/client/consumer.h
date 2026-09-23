// 推模式消费者（对齐 org.apache.rocketmq.client.consumer.DefaultMQPushConsumer）。
//
// 架构（对齐 Java PushConsumer 的三层模型）：
//   1. 拉取层：每个队列一个拉取线程（对应 Java PullMessageService 的并发长轮询——
//      broker 为每个队列挂起长轮询请求、消息到达立即返回），拉到的消息进
//      pending_ 缓冲（对应 ProcessQueue），拉取游标推进到 nextBeginOffset；
//   2. 分发层：单分发线程从缓冲按 consumeMessageBatchMaxSize 取批次交给
//      MessageListener；RECONSUME_LATER/异常批次逐条回投 %RETRY%topic
//      （延迟梯度 3+reconsumeTimes，超 maxReconsumeTimes 由 broker 转 %DLQ%）；
//   3. 位点层：_consume_offsets 记录"已消费位点"，每 5s 用 UPDATE_CONSUMER_OFFSET
//      提交 broker（Java persistAllConsumerOffset），启动先 QUERY_CONSUMER_OFFSET。
//
// 关键工程点（真机验证得出，勿删注释）：
//   - 拉取必须按队列并行：单线程顺序长轮询下，空闲队列的 suspend 会阻塞
//     其余队列投递（曾导致"第一批消息能收到、后续全迟到"）。
//   - broker 会把客户端下发的 suspend 时间钳制到自身 brokerSuspendMaxTimeMillis，
//     故空闲队列仍会周期性客户端超时 —— 这是**良性**的，按 debug 处理不记 ERROR。
#ifndef ROCKETMQ_CLIENT_CONSUMER_H
#define ROCKETMQ_CLIENT_CONSUMER_H

#include <algorithm>
#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include "rocketmq/client/allocate_strategy.h"
#include "rocketmq/client/consume_executor.h"
#include "rocketmq/client/hook.h"
#include "rocketmq/client/mq_client.h"
#include "rocketmq/client/result.h"
#include "rocketmq/client/trace_dispatcher.h"
#include "rocketmq/common/message.h"
#include "rocketmq/common/mix_all.h"
#include "rocketmq/common/namespace_util.h"
#include "rocketmq/common/subscription_data.h"
#include "rocketmq/remoting/protocol/body.h"
#include "rocketmq/remoting/protocol/heartbeat.h"

namespace rocketmq {

// 选择器（对应 Java MessageSelector / Python MessageSelector）
struct MessageSelector {
    std::string type = ExpressionType::TAG;
    std::string expression = "*";

    static MessageSelector byTag(const std::string& tag) {
        return MessageSelector{ExpressionType::TAG, tag};
    }
    static MessageSelector bySql(const std::string& sql) {
        return MessageSelector{ExpressionType::SQL92, sql};
    }
};

// Java DefaultMQPushConsumerImpl.popDelayLevel（单位**秒**）。
// 与 send 的延迟档位（首档 1s）不是同一张表，别混。
inline const std::vector<int32_t>& popDelayLevelTable() {
    static const std::vector<int32_t> kTable = {10, 30, 60, 120, 180, 240, 300, 360,
                                                420, 480, 540, 600, 1200, 1800, 3600, 7200};
    return kTable;
}

// Java DefaultMQPushConsumerImpl.MIN/MAX_POP_INVISIBLE_TIME：超出范围一律回落到 60000
constexpr int64_t kMinPopInvisibleTime = 5000;
constexpr int64_t kMaxPopInvisibleTime = 300000;

// Java ProcessQueue.PULL_MAX_IDLE_TIME（`rocketmq.client.pull.pullMaxIdleTime`，默认
// 120000ms）：仍归本实例的队列如果超过这么久没发起过任何一次拉取/弹出，说明这条循环
// 死了或卡住了，RebalanceImpl.updateProcessQueueTableInRebalance:442 就按这个判据把
// 它撤掉并重建（否则那个队列从此**静默**不再消费，客户端不报任何错）。
constexpr int64_t kPullMaxIdleTime = 120000;

// Java ConsumeInitMode
enum class ConsumeInitMode : int32_t { MIN = 0, MAX = 1 };

// POP 模式的队列状态（对应 org.apache.rocketmq.client.impl.consumer.PopProcessQueue）。
//
// 与 pull 模式的 ProcessQueue 不同，POP **没有"已拉未消费"缓冲**：消息一弹出就交给
// 消费线程，确认靠 ack。这里只跟踪两件事：
//   - waitAckCounter：已弹出但还没 ack / 还没延长不可见时间的条数，用于流控；
//   - dropped：队列是否已被 rebalance 撤走（撤走后本批消息不再消费、也不 ack，
//     交给 invisibleTime 到期后 broker 自动复活重投）。
class PopProcessQueue {
public:
    void incFoundMsg(int32_t n) {
        std::lock_guard<std::mutex> lk(lock_);
        waitAckCounter_ += n;
    }
    // Java 传的是负数（decFoundMsg(-msgs.size())），这里按"减多少"理解。
    void decFoundMsg(int32_t n) {
        std::lock_guard<std::mutex> lk(lock_);
        waitAckCounter_ += n;
    }
    int32_t ack() {
        std::lock_guard<std::mutex> lk(lock_);
        return --waitAckCounter_;
    }
    int32_t waitAckCount() const {
        std::lock_guard<std::mutex> lk(lock_);
        return waitAckCounter_;
    }
    bool isDropped() const { return dropped_.load(); }
    void setDropped(bool v) { dropped_.store(v); }

    // Java PopProcessQueue.lastPopTimestamp：在**发起**弹出时盖章（:508），
    // isPullExpired 读的就是它（:74-76）——POP 模式没有"拉取"动作，靠这个时刻判停摆。
    int64_t lastPopTimestamp() const { return lastPopTimestamp_.load(); }
    void setLastPopTimestamp(int64_t v) { lastPopTimestamp_.store(v); }

private:
    mutable std::mutex lock_;
    int32_t waitAckCounter_ = 0;
    std::atomic<bool> dropped_{false};
    std::atomic<int64_t> lastPopTimestamp_{0};
};

// 从 POP_CK 解出的 ack / 延长不可见时间目标。
struct PopCkTarget {
    std::string topic;        // getRealTopic 按 retryFlag 还原后的真实 topic
    std::string brokerName;   // CK 第 6 段
    int32_t queueId = 0;      // CK 第 7 段
    int64_t offset = 0;       // CK 第 8 段 = consumeQueue offset
    std::string extraInfo;    // 原样回传的 CK 串
};

class DefaultMQPushConsumer {
public:
    explicit DefaultMQPushConsumer(
        const std::string& consumerGroup = MixAll::DEFAULT_CONSUMER_GROUP);
    ~DefaultMQPushConsumer();

    DefaultMQPushConsumer(const DefaultMQPushConsumer&) = delete;
    DefaultMQPushConsumer& operator=(const DefaultMQPushConsumer&) = delete;

    // ---------------- 配置 ----------------
    void setNamesrvAddr(const std::string& addr);
    void setNameServerAddresses(const std::vector<std::string>& addrs);
    void setInstanceName(const std::string& name) { instanceName_ = name; }

    // ---------------- unitName / unitMode / enableStreamRequestType ----------------
    // 对应 Java `ClientConfig` 的三个同名开关。⚠ 必须在 start() 之前设置：
    // unitName / @STREAM 决定 clientId 的形状，stream 决定请求钩子链（`ReqT` 要进 ACL 签名）。
    void setUnitName(const std::string& unitName) { unitName_ = unitName; }
    const std::string& unitName() const { return unitName_; }
    // Java 在消息过滤上下文（`DefaultMQPushConsumerImpl:640`）、心跳里的
    // `ConsumerData.unitMode`（`MQClientInstance:1039`）和回投请求头（`:927/948`）三处读它。
    void setUnitMode(bool unitMode) { unitMode_ = unitMode; }
    bool isUnitMode() const { return unitMode_; }

    // true 时每个请求带扩展字段 `ReqT=0`，且 clientId 末尾多一段 `@STREAM`。
    void setEnableStreamRequestType(bool enable) { enableStreamRequestType_ = enable; }
    bool isEnableStreamRequestType() const { return enableStreamRequestType_; }
    void setMessageModel(const std::string& model) { messageModel_ = model; }
    void setConsumeFromWhere(const std::string& where) { consumeFromWhere_ = where; }
    // 队列分配策略（对应 Java DefaultMQPushConsumer.setAllocateMessageQueueStrategy）。
    // 与 Java 同款：setter 允许传 nullptr，由 start() 的 checkConfig 拒绝
    //（Java DefaultMQPushConsumerImpl.checkConfig:1067 "allocateMessageQueueStrategy is null"）。
    // 默认 AllocateMessageQueueAveragely。
    void setAllocateMessageQueueStrategy(std::shared_ptr<AllocateMessageQueueStrategy> strategy);
    // 对应 Java DefaultMQPushConsumer.getAllocateMessageQueueStrategy（RebalanceImpl 同名字段）。
    std::shared_ptr<AllocateMessageQueueStrategy> allocateMessageQueueStrategy() const;
    void setConsumeThreadNums(int32_t n);
    // ---- 消费线程弹性（对应 Java DefaultMQPushConsumer / AbstractConsumeMessageService）----
    void setConsumeThreadMin(int32_t n);
    void setConsumeThreadMax(int32_t n);
    int32_t getConsumeThreadMin() const { return consumeThreadMin_; }
    int32_t getConsumeThreadMax() const { return consumeThreadMax_; }
    void setAdjustThreadPoolNumsThreshold(int64_t v) { adjustThreadPoolNumsThreshold_ = v; }
    int64_t getAdjustThreadPoolNumsThreshold() const { return adjustThreadPoolNumsThreshold_; }
    // Java AbstractConsumeMessageService.updateCorePoolSize：守卫不满足则**静默忽略**。
    // 返回值只用于单测断言"是否真的生效"（Java 无返回值）。
    bool updateCorePoolSize(int32_t corePoolSize);
    int32_t getCorePoolSize() const;
    // Java DefaultMQPushConsumerImpl.computeAccumulationTotal / adjustThreadPool。
    // ⚠ adjustThreadPool 在 Java 5.5.1 是 no-op（inc/dec 空实现），本实现照抄。
    int64_t computeAccumulationTotal() const;
    void adjustThreadPool();
    // 单队列（key 省略则求和）的 ProcessQueue.msgAccCnt。
    int64_t msgAccCnt(const std::string& key = std::string()) const;
    // 按 Java ProcessQueue.putMessage 的规则更新某队列的 msgAccCnt
    // （= 最后一条消息的 MAX_OFFSET 属性 - 它的 queueOffset，> 0 才更新）。
    void updateMsgAccCnt(const std::string& key, const std::vector<MessageExt>& msgs);
    // 观测：当前 POP 消费执行器的存活线程数 / 排队任务数（未建执行器时为 0）。
    int32_t consumeExecutorWorkers() const;
    int32_t consumeExecutorQueued() const;
    // 对应 Java DefaultMQPushConsumerImpl.consumerRunningInfo（307 的应答体）。
    ConsumerRunningInfo consumerRunningInfo();
    // 对应 Java MQClientInstance.resetOffset（220 的消费者侧逻辑）：
    // 命中本 topic 分配队列的 → 清在途缓冲与拉取游标 → 写新已消费位点 →
    // 撤销该队列（持久化新位点 + 顺序解锁）→ 立即 rebalance 从新位点重拉。
    void resetOffset(const std::string& topic, const std::map<MessageQueue, int64_t>& offsetTable);
    // 对应 Java MQClientInstance.getConsumerStatus（221 的应答数据源）：
    // 返回**已消费位点**表（不是拉取游标），topic 为空则返回全部。
    std::map<MessageQueue, int64_t> getConsumerStatus(const std::string& topic);
    // 对应 Java ConsumeMessageConcurrentlyService.consumeMessageDirectly（309）：
    // 本地真实消费一条消息（还原重投 topic 后交给监听器），把结果回给 admin。
    ConsumeMessageDirectlyResult consumeMessageDirectly(const MessageExt& msg,
                                                        const std::string& brokerName);
    void setMessageListener(std::shared_ptr<MessageListener> listener);
    void setPullBatchSize(int32_t n) { pullBatchSize_ = n; }
    void setPullBatchSizeInBytes(int32_t n) { pullBatchSizeInBytes_ = n; }
    void setConsumeMessageBatchMaxSize(int32_t n) { consumeMessageBatchMaxSize_ = std::max(1, n); }
    void setPullTimeoutMillis(int32_t t) { pullTimeoutMillis_ = t; }
    void setPullSuspendTimeoutMillis(int32_t t) { pullSuspendTimeoutMillis_ = t; }
    void setSuspendCurrentQueueTimeMillis(int32_t t) { suspendCurrentQueueTimeMillis_ = t; }
    void setMaxReconsumeTimes(int32_t n) { maxReconsumeTimes_ = n; }
    void setPullIntervalMillis(int32_t t) { pullIntervalMillis_ = t; }
    // 每队列"已拉未消费"阈值，超过则暂停该队列拉取（Java pullThresholdForQueue，默认 1000）
    void setPullThresholdForQueue(int32_t n) { pullThresholdForQueue_ = n; }
    // 是否在消费循环里周期性发 HEART_BEAT（默认开启；失败仅告警不影响消费）
    // ---- POP 模式（5.x 轻量消费）----
    // 关掉时完全走原来的 pull 长轮询路径，行为与改动前一致。
    void setPopMode(bool b) { popMode_ = b; }
    bool popMode() const { return popMode_; }
    // TLS（对应 Java tls.enable；缺省读 env ROCKETMQ_TLS_ENABLE）
    void setTlsEnable(bool b) { tlsEnable_ = b; }
    // 弹出后对其它实例不可见的时长（Java popInvisibleTime 默认 60000）
    void setPopInvisibleTime(int64_t t) { popInvisibleTime_ = t; }
    int64_t popInvisibleTime() const { return popInvisibleTime_; }
    // 单次 POP 的最大条数（Java popBatchNums 默认 32；broker 侧 >32 会回 INVALID_PARAMETER）
    void setPopBatchNums(int32_t n) { popBatchNums_ = n; }
    int32_t popBatchNums() const { return popBatchNums_; }
    // 本队列"已弹未 ack"计数器上限，超过就暂停 POP（Java popThresholdForQueue 默认 96）
    void setPopThresholdForQueue(int32_t n) { popThresholdForQueue_ = n; }
    int32_t popThresholdForQueue() const { return popThresholdForQueue_; }
    // POP 长轮询挂起时长。0 = 短轮询（broker 立即返回或 NO_NEW_MSG）。
    // ⚠ 非 0 时请求超时必须 > 它，否则客户端先超时。
    void setPopPollTimeMillis(int32_t t) { popPollTimeMillis_ = t; }
    int32_t popPollTimeMillis() const { return popPollTimeMillis_; }
    void setPopTimeoutMillis(int32_t t) { popTimeoutMillis_ = t; }
    int32_t popTimeoutMillis() const { return popTimeoutMillis_; }

    // 以下两个是**纯逻辑**（不发请求），做成 public 是为了离线可测——popCkTarget 是 POP
    // 最容易错的一段（retry topic 还原），必须能单测而不是只能靠真机。
    // 从 POP_CK 还原 ack 目标；CK 缺失或段数不足返回 nullopt（放弃 ack，交给 broker 复活）。
    std::optional<PopCkTarget> popCkTarget(const MessageExt& msg);
    // Java ConsumeRequest.isPopTimeout：解析不出 popTime/invisibleTime 时按超时处理
    static bool isPopTimeout(int64_t popTime, int64_t invisible);

    void setHeartbeatEnabled(bool b) { heartbeatEnabled_ = b; }
    void setHeartbeatIntervalMillis(int32_t t) { heartbeatIntervalMillis_ = t; }

    // 命名空间（对应 Java DefaultMQPushConsumer.setNamespace）：非空时把 topic / group
    // 套上 "ns%" 前缀再与 broker 交互（对齐 Java start() 里对 consumerGroup 的包装）。
    void setNamespace(const std::string& ns) { namespace_ = ns; }

    // ---------------- ACL 鉴权（对应 Java DefaultMQPushConsumer(rpcHook)）----------------
    // 必须在 start() 之前调用：钩子在 start() 里绑定到 MQClientInstance。
    void setRPCHook(std::shared_ptr<RPCHook> hook) { rpcHook_ = std::move(hook); }
    void setCredentials(const std::string& accessKey, const std::string& secretKey,
                        const std::string& securityToken = std::string()) {
        rpcHook_ = std::make_shared<AclClientRPCHook>(
            SessionCredentials(accessKey, secretKey, securityToken));
    }
    const std::shared_ptr<RPCHook>& rpcHook() const { return rpcHook_; }

    // ---------------- 消息轨迹（对应 Java DefaultMQPushConsumer.setEnableMsgTrace 等）----
    // enableMsgTrace=true 时 start() 会建 AsyncTraceDispatcher（Type=CONSUME）并自动注册
    // ConsumeMessageTraceHook；三条消费路径（并发 / 顺序 / POP）都会产出 SubBefore/SubAfter。
    void setEnableMsgTrace(bool enable) { enableMsgTrace_ = enable; }
    bool isEnableMsgTrace() const { return enableMsgTrace_; }
    // setEnableMsgTrace 的别名（对齐 Java 5.x 里的 setEnableTrace 写法）
    void setEnableTrace(bool enable) { enableMsgTrace_ = enable; }
    void setTraceTopic(const std::string& t) { traceTopic_ = t; }
    void setTraceMsgBatchNum(int32_t n) { traceMsgBatchNum_ = n; }
    // SubAfter 是否补 timestamp + groupName 两段由此决定（CLOUD 不补，Java 默认 LOCAL）
    void setAccessChannel(AccessChannel ch) { accessChannel_ = ch; }
    AccessChannel accessChannel() const { return accessChannel_; }
    // 单批消费超时（分钟），用于 ConsumeReturnType.TIME_OUT 判定（Java 默认 15）
    void setConsumeTimeout(int32_t minutes) { consumeTimeoutMinutes_ = minutes; }

    void registerConsumeMessageHook(std::shared_ptr<ConsumeMessageHook> hook) {
        if (hook) consumeMessageHookList_.push_back(std::move(hook));
    }
    bool hasConsumeMessageHook() const { return !consumeMessageHookList_.empty(); }
    size_t consumeMessageHookCount() const { return consumeMessageHookList_.size(); }

    // 投递前过滤钩子（对应 Java registerFilterMessageHook / hasFilterMessageHook）。
    // 钩子摘掉的消息：拉取路径静默跳过（位点照常推进），POP 路径立刻 ack。
    void registerFilterMessageHook(std::shared_ptr<FilterMessageHook> hook) {
        if (hook) filterMessageHookList_.push_back(std::move(hook));
    }
    bool hasFilterMessageHook() const { return !filterMessageHookList_.empty(); }
    size_t filterMessageHookCount() const { return filterMessageHookList_.size(); }
    // 已丢弃的条数（拉取 + POP 合计），供联调脚本与单测观测
    int64_t filteredMessageCount() const { return filteredMessageCount_.load(); }

    // ---- 投递前过滤（对应 Java PullAPIWrapper.processPullResult 的 113-122 与 executeHook）----
    // 以下三个供单测/联调直接驱动（不经过网络）。
    // 依次执行过滤钩子，**异常一律吞掉**（Java PullAPIWrapper.executeHook:171-178 记 error）；
    // 与 CheckForbiddenHook 相反：过滤钩子挂了不能影响消费。
    void executeFilterMessageHook(FilterMessageContext& context);
    // 拉取 / POP 两条路径共用的投递前过滤：
    //   ① 客户端二次 tag 过滤（broker 侧按 codeSet 哈希过滤有碰撞误放）
    //   ② FilterMessageHook（可改写 msgList）
    // 传入的 sub 为 nullptr 时只跑 ②。
    std::vector<MessageExt> filterMessagesForDelivery(const MessageQueue& mq,
                                                      const SubscriptionData* sub,
                                                      const std::vector<MessageExt>& msgs);
    // 求 original \ kept 的差集（POP 路径要给被摘掉的消息补 ack）。
    // Java 用 List.contains 的引用同一性；C++ 拷 vector 后无同一性，改按 msgId 求差（语义等价）。
    static std::vector<MessageExt> droppedMessages(const std::vector<MessageExt>& original,
                                                   const std::vector<MessageExt>& kept);

    std::shared_ptr<AsyncTraceDispatcher> traceDispatcher() const { return traceDispatcher_; }

    const std::string& consumerGroup() const { return consumerGroup_; }
    const std::string& clientId() const { return clientId_; }
    const std::string& messageModel() const { return messageModel_; }
    bool isStarted() const { return started_.load(); }
    int32_t pullTimeoutMillis() const { return pullTimeoutMillis_; }
    int32_t pullSuspendTimeoutMillis() const { return pullSuspendTimeoutMillis_; }
    // 已成功消费的消息总数（用于测试/监控）
    int64_t consumedCount() const { return consumedCount_.load(); }
    // broker 心跳成功次数（用于验证心跳能力）
    int64_t heartbeatCount() const { return heartbeatCount_.load(); }
    // 流控触发次数（用于验证流控能力）
    int64_t flowControlTriggered() const { return flowControlTriggered_.load(); }
    // 当前分给本实例的队列 key 列表（真机验证"同组两实例不重不漏"用）。
    // key 格式与 offsetKey 一致：topic + brokerName + queueId。
    std::vector<std::string> assignedQueueKeys() const;
    // 查消费组在某 topic 上的全部 clientId（对应 Java findConsumerIdList），用于验证多实例注册。
    std::vector<std::string> consumerIdListOfGroup(const std::string& topic) const;

    // ---------------- 订阅 ----------------
    void subscribe(const std::string& topic, const std::string& subExpression = "*");
    void subscribe(const std::string& topic, const MessageSelector& selector);
    void unsubscribe(const std::string& topic);
    std::vector<std::string> subscribedTopics() const;

    // ---------------- 生命周期 ----------------
    void start();
    void shutdown();

    MQClientInstance& client();

    // ---------------- 管理 ----------------
    std::vector<MessageQueue> fetchSubscribeMessageQueues(const std::string& topic);
    // 消息重投（对应 Java sendMessageBack）：内部不抛异常。返回 false 表示
    // CONSUMER_SEND_MSG_BACK 与"普通消息重投"兜底都失败了。
    bool sendMessageBack(const MessageExt& msg, int32_t delayLevel,
                         const std::string& brokerName = std::string());
    // 向所有已知 broker 发一次心跳（对应 Java sendHeartbeatToAllBrokerWithLock）
    int32_t sendHeartbeatToAllBroker();

    // ---------------- 供单测/联调直接驱动消费分发（不经过网络）----------------
    // classic 消费路径的 ackIndex 语义错得很安静（尾巴静默丢失、位点越过未消费完的
    // 消息），必须能离线锁死再上真机，所以这一小段是 public。
    // 消费一个批次，处理回投/挂起；返回消费位点是否前进。key 用 offsetKey(mq)。
    bool consumeBatch(const std::string& key, const MessageQueue& mq,
                      const std::vector<MessageExt>& batch);
    // 队列在缓冲/位点表里的 key（topic + brokerName + queueId）。
    static std::string offsetKey(const MessageQueue& mq);
    // 预置某队列的「已拉未消费」缓冲（Java ProcessQueue），用于断言回投失败后塞回队首的内容
    void setPendingMessages(const std::string& key, const std::vector<MessageExt>& msgs);
    std::vector<MessageExt> pendingMessages(const std::string& key) const;
    // 读回某队列的已消费位点；nullopt = 还没有记录
    std::optional<int64_t> consumeOffset(const std::string& key) const;

    // ---- 停摆自愈（Java isPullExpired）的观测/注入面 ----
    // 停摆判据算错方向是**静默**故障：阈值写太小会把健康队列反复撤走重投（凭空造重复），
    // 写太大或干脆不判，则循环死掉的队列永久不再消费。真机窗口里两种错都看不出差别，
    // 所以阈值算术与时刻表要能在离线单测里直接读写。
    bool pullStalled(const std::string& key) const;
    // 该队列最近一次「发起拉取/弹出」的毫秒时刻；-1 = 还没有记录
    int64_t lastPullAt(const std::string& key) const;
    // 注入时刻（真机故障注入用）：倒拨到阈值之外即等价于"这路卡住了"
    void setLastPullAt(const std::string& key, int64_t millis);
    // 立刻按当前分配集撤/建一轮拉取线程（Java updateProcessQueueTableInRebalance）
    void syncPullThreads();

private:
    // 拉取：每个队列一个线程（对齐 Java PullMessageService 的并发长轮询语义：
    // broker 为每个队列挂起长轮询、消息到达立即返回；若单线程顺序轮询，
    // 一个空队列的 suspend 会阻塞其余队列的投递）。
    void rebalancePullThreads();
    void rebalanceLoop();
    void queuePullLoop(const MessageQueue& mq, uint64_t token);
    // 分发：单线程从各队列缓冲取批次交给监听器
    void dispatchLoop();
    // 从 base 起把 batch[base, size) 逐条回投（Java processConsumeResult → sendMessageBack）：
    // base 是尾巴在整批里的起始下标（部分 ack 时前缀已经认可，不能再回投）。
    // 返回回投**失败**的 (整批下标, 消息)，失败条目的 reconsumeTimes 已 +1
    //（Java :251 —— broker 那边没记上这次数，客户端不补就永远进不了 DLQ）；
    // 调用方据此把尾巴塞回队首并钳住位点。
    std::vector<std::pair<size_t, MessageExt>> sendBackBatch(
        const std::vector<MessageExt>& batch, const ConsumeConcurrentlyContext& ctx, size_t base);
    // 推进位点到 batch 中最大 queueOffset+1；floor 非空时不越过它
    //（对应 Java ProcessQueue.removeMessage：树里还留着未消费完的消息时，
    //  提交位点只能是 firstKey，否则会静默丢掉那条）。空批次直接返回。
    void advanceConsumeOffset(const std::string& key, const std::vector<MessageExt>& batch,
                              const std::optional<int64_t>& floor = std::nullopt);
    // 回投兜底（Java getMaxReconsumeTimes / sendMessageBackAsNormalMessage）
    int32_t maxReconsumeTimesOrDefault() const;
    void sendMessageBackAsNormalMessage(const MessageExt& msg);
    // 位点持久化：每 5s 把"已消费位点"提交 broker（Java persistAllConsumerOffset）
    void offsetPersistLoop();
    void persistOffsetsOnce();
    // 广播模式本地位点文件（Java LocalFileOffsetStore）
    std::string localOffsetPath() const;
    void saveLocalOffsets();
    std::map<std::string, int64_t> loadLocalOffsets() const;
    // 顺序消费 broker 队列锁（Java ConsumeMessageOrderlyService.lockMQ，每 20s）
    void lockLoop();
    bool isOrderly() const;
    void maybeSendHeartbeat();

    std::vector<MessageQueue> assignedQueues();
    int64_t resolveInitialOffset(const MessageQueue& mq, const SubscriptionData& sub);

    // ---- 真实 rebalance（对齐 Java RebalanceImpl.rebalanceByTopic）----
    // 计算本实例应持有的队列集并写入 assignedQueues_，再同步拉取线程；
    // 新分配的队列**立刻**解析初始位点写入 offsetTable_。
    void doRebalance();
    // 当前分配里「topic 的全部队列」（对应 Java RebalanceImpl.topicSubscribeInfoTable）。
    std::vector<MessageQueue> allQueuesOfTopic(const std::string& topic);
    // 本拉取线程是否仍持有该队列（rebalance 撤走或换了拉取线程后即失效）。
    // token 在**起线程之前**就写进 pullOwners_：std::thread 一构造就跑，如果等到
    // 建好再登记，新线程可能先跑到 ownsQueue() 看到「这张表里还没有我」而当场退出，
    // 队列却被登记成「已有拉取线程」⇒ 永远没人再拉它（真机少消费一批的根因）。
    bool ownsQueue(const std::string& key, uint64_t token) const;
    // 每次**发起**拉取/弹出时盖时刻（Java DefaultMQPushConsumerImpl.pullMessage:253 /
    // popMessage:508 的位置：在流控、锁判定之前——判据是"这条循环还在跑"，不是"这轮真打了网络"）。
    void stampPullAt(const std::string& key);
    // 该队列是否已停摆（Java ProcessQueue.isPullExpired）。调用方须持 lock_。
    bool pullStalledLocked(const std::string& key) const;
    // 循环函数返回了、却仍持有该队列（不是被 rebalance 撤走的）：标记为停摆，
    // 让下一趟 rebalance 走撤走+重建。std::thread 死了没法像 Python 那样问 is_alive()
    // （joinable() 仍是 true），所以由线程包装器自己报到。
    void markPullLoopExited(const std::string& key, uint64_t token);
    // 队列被撤走时的收尾（对应 Java removeUnnecessaryMessageQueue）：持久化已消费位点、
    // 丢弃在途缓冲、顺序消费集群模式解锁。revoked 为 (队列, 已消费位点) 列表。
    void onQueuesRevoked(const std::vector<std::pair<MessageQueue, int64_t>>& revoked);
    // broker 通知消费组实例变化 → 立即重算（对齐 Java rebalanceImmediately）。
    // 由 MQClientInstance 的 40 处理器逐个点名调用（实例级注册，见 start()）。
    void wakeRebalanceLoop();
    // 分发前把重投消息的 topic 还原成业务原始 topic（对应 Java resetRetryAndNamespace）。
    void resetRetryTopicAndNamespace(std::vector<MessageExt>& msgs);

    // ---- POP 消费循环（5.x 轻量消费，对应 Java popMessage 回调 + ConsumeMessagePopConcurrentlyService）----
    // 单队列 POP 循环。**不查、不提交消费位点**：进度由 broker 侧 checkpoint 跟踪，确认只靠 ack。
    void queuePopLoop(const MessageQueue& mq, uint64_t token);
    // 按 consumeMessageBatchMaxSize 切批投递
    void submitPopConsumeRequest(std::vector<MessageExt> msgs,
                                 std::shared_ptr<PopProcessQueue> pq, const MessageQueue& mq);
    // 消费一个批次并按结果 ack / 延长不可见时间
    void consumePopBatch(std::vector<MessageExt> msgs,
                         std::shared_ptr<PopProcessQueue> pq, const MessageQueue& mq);
    void processPopConsumeResult(ConsumeConcurrentlyStatus status,
                                 const ConsumeConcurrentlyContext& ctx,
                                 std::vector<MessageExt>& msgs,
                                 const std::shared_ptr<PopProcessQueue>& pq);
    // 重试次数用尽后的兜底（Java checkNeedAckOrDelay）
    void checkNeedAckOrDelay(const MessageExt& msg);
    // 调用方必须已持有 lock_（pull 循环在入队临界区内直接调它）
    void updateMsgAccCntLocked(const std::string& key, const std::vector<MessageExt>& msgs);
    void ackPopMsg(const MessageExt& msg);
    void changePopInvisibleTime(const MessageExt& msg, int32_t delayLevel);

    // ---- 消费钩子 / 轨迹 ----
    // 构造消费钩子上下文（Java 的初始值：success=false、props 空）
    ConsumeMessageContext buildConsumeHookContext(const std::vector<MessageExt>& msgs,
                                                  const MessageQueue& mq) const;
    void executeConsumeHookBefore(ConsumeMessageContext& context);
    void executeConsumeHookAfter(ConsumeMessageContext& context);
    // 消费结果 -> ConsumeReturnType 名字（写进 props，决定轨迹 SubAfter 的 contextCode）。
    // 判定顺序与 Java ConsumeMessageConcurrentlyService:378-393 一致。
    static const char* consumeReturnTypeOf(bool hasException, int64_t consumeRtMs,
                                           bool failed, bool succeeded);
    // 收尾：写 props/status/success 后触发 after 钩子（对应 Java executeHookAfter 那一段）
    void finishConsumeHook(ConsumeMessageContext* hookCtx, bool hasException, int64_t beginMs,
                           bool failed, bool succeeded, const std::string& statusText);
    // 消费侧 RT/TPS 记数（Java ConsumeRequest.run：RT 恒记，OK/FAILED 按结果）。
    // ackCount 只有**并发 classic** 路径传（Java 按 ackIndex+1 拆 ok/failed，
    // 部分 ack 的尾巴既记 FAILED 又会被重投）；POP 与顺序消费传 nullopt = 整批同一状态。
    void recordConsumeStats(const std::string& topic, int64_t msgCount, int64_t beginMs,
                            bool failed, const std::optional<int64_t>& ackCount = std::nullopt);
    // start() 里按 enableMsgTrace 建分发器并注册 ConsumeMessageTraceHook
    void startTraceDispatcher();

    std::string consumerGroup_;
    // 队列分配策略，对应 Java RebalanceImpl.allocateMessageQueueStrategy
    // （DefaultMQPushConsumer 构造时传入）。默认 AllocateMessageQueueAveragely。
    std::shared_ptr<AllocateMessageQueueStrategy> allocateStrategy_;
    std::string namespace_;
    // ACL 钩子，start() 时绑定到 MQClientInstance 的传输层
    std::shared_ptr<RPCHook> rpcHook_;
    std::string instanceName_ = "DEFAULT";
    std::string clientId_;
    std::string unitName_;
    bool unitMode_ = false;
    // Java 的 pull / lite 消费者在**每个构造函数**里置 true（DefaultMQPullConsumer:113/126、
    // DefaultLitePullConsumer:213/228），生产者与推送消费者保持 false。
    bool enableStreamRequestType_ = false;
    std::string messageModel_ = MessageModel::CLUSTERING;
    std::string consumeFromWhere_ = ConsumeFromWhere::CONSUME_FROM_LAST_OFFSET;

    // ---- 消费线程池（对齐 Java DefaultMQPushConsumer 的 consumeThreadMin/Max）----
    // Java 默认 min=20 / max=64。本实现的**拉取**路径是"每队列一个拉取线程 + 单分发线程"，
    // 只有 **POP** 路径用真正的线程池（对应 Java ConsumeMessagePopConcurrentlyService），
    // 因此 corePoolSize 直接决定 POP 的消费并发度（Java 无界队列下 max 实际用不到）。
    int32_t consumeThreadMin_ = 20;
    int32_t consumeThreadMax_ = 64;
    // Java adjustThreadPoolNumsThreshold 默认 100000（自动弹性阈值；上游 inc/dec 是空实现）
    int64_t adjustThreadPoolNumsThreshold_ = 100000;
    // 声明式 core pool size（Java setCorePoolSize 的等价物），默认 = consumeThreadMin
    int32_t corePoolSize_ = 20;
    // key -> ProcessQueue.msgAccCnt（最近一次拉取算出的积压条数）
    mutable std::map<std::string, int64_t> msgAccCntTable_;
    // POP 消费执行器（start 且 popMode_ 时创建；stop 时 shutdown）
    std::shared_ptr<ConsumeExecutor> popConsumeExecutor_;

    int32_t pullBatchSize_ = 32;
    int32_t pullBatchSizeInBytes_ = 256 * 1024;
    int32_t consumeMessageBatchMaxSize_ = 1;
    int32_t pullTimeoutMillis_ = 30000;
    int32_t pullSuspendTimeoutMillis_ = 15000;
    int32_t suspendCurrentQueueTimeMillis_ = 1000;
    int32_t maxReconsumeTimes_ = -1;
    int32_t pullIntervalMillis_ = 0;
    int32_t pullThresholdForQueue_ = 1000;

    // ---- POP 模式（5.x 轻量消费）----
    bool popMode_ = false;
    bool tlsEnable_ = MQClientInstance::tlsEnabledFromEnv();
    int64_t popInvisibleTime_ = 60000;
    int32_t popBatchNums_ = 32;
    int32_t popThresholdForQueue_ = 96;
    int32_t popPollTimeMillis_ = 15000;
    int32_t popTimeoutMillis_ = 25000;
    std::vector<int32_t> popDelayLevel_ = popDelayLevelTable();
    // 队列 key -> PopProcessQueue（已弹未 ack 计数 + 是否已被 rebalance 撤销）
    std::map<std::string, std::shared_ptr<PopProcessQueue>> popQueues_;

    bool heartbeatEnabled_ = true;
    int32_t heartbeatIntervalMillis_ = 30000;

    std::vector<std::string> nameServerAddrs_;
    mutable std::mutex lock_;
    std::map<std::string, SubscriptionData> subscriptionData_;
    std::shared_ptr<MessageListener> messageListener_;
    // 拉取游标（nextBeginOffset）
    std::map<std::string, int64_t> offsetTable_;
    // 已消费位点（周期持久化的对象；Java ProcessQueue.removeMessage 后的 commitOffset）
    std::map<std::string, int64_t> consumeOffsetTable_;
    std::map<std::string, MessageQueue> mqMap_;
    // 已拉未消费缓冲（Java ProcessQueue 的简化版）
    std::map<std::string, std::deque<MessageExt>> pending_;
    // 顺序消费：broker LOCK_BATCH_MQ 确认锁定成功的队列 key 集
    std::set<std::string> lockOk_;

    std::unique_ptr<MQClientInstance> mqClient_;
    std::atomic<bool> started_{false};
    std::atomic<bool> stop_{false};
    std::map<std::string, std::thread> pullThreads_;
    // 每个队列当前拉取线程的「归属凭据」：在**起线程之前**登记，线程每轮自查
    // （见 ownsQueue）。std::thread 一构造就开跑，没法像 Python/.NET 那样「先入表
    // 再 start」，所以归属不能靠线程 id 反查，只能靠这张先写入的表。
    std::map<std::string, uint64_t> pullOwners_;
    uint64_t nextPullToken_ = 0;
    // 每队列最近一次「发起拉取/弹出」的毫秒时刻（Java ProcessQueue.lastPullTimestamp /
    // PopProcessQueue.lastPopTimestamp）。rebalance 用它判 pull 是否停摆（kPullMaxIdleTime）。
    // 0 = 循环自己返回了却仍持有该队列（异常打穿），下一趟按停摆撤走重建。
    std::map<std::string, int64_t> lastPullAt_;
    // 被撤销队列对应的旧拉取线程（已脱离 pullThreads_，等待其自然退出后回收）
    std::vector<std::thread> retiredThreads_;
    std::thread dispatchThread_;
    std::thread persistThread_;
    std::thread lockThread_;
    std::thread rebalanceThread_;
    // 真实 rebalance 计算出的本实例队列集（对应 Java ProcessQueueTable 的键集）。
    // 取代旧实现里「订阅 topic 的全部队列」，避免同组多实例重复消费。
    std::vector<MessageQueue> assignedQueues_;
    std::condition_variable cv_;
    std::mutex waitMutex_;
    // 即时重算信号（broker 发 NOTIFY_CONSUMER_IDS_CHANGED 时置位）
    std::atomic<bool> rebalanceNow_{false};
    std::mutex rebalanceMutex_;
    std::condition_variable rebalanceCv_;
    int64_t startMillis_ = 0;
    std::atomic<int64_t> consumedCount_{0};
    std::atomic<int64_t> heartbeatCount_{0};
    std::atomic<int64_t> lastHeartbeatMs_{0};
    std::atomic<int64_t> flowControlTriggered_{0};

    // ---------------- 消息轨迹 / 消费钩子 ----------------
    bool enableMsgTrace_ = false;
    std::string traceTopic_;                      // 空 => 用 MixAll::TRACE_TOPIC
    int32_t traceMsgBatchNum_ = 10;
    int32_t consumeTimeoutMinutes_ = 15;
    AccessChannel accessChannel_ = AccessChannel::LOCAL;
    std::vector<std::shared_ptr<ConsumeMessageHook>> consumeMessageHookList_;
    std::vector<std::shared_ptr<FilterMessageHook>> filterMessageHookList_;
    std::atomic<int64_t> filteredMessageCount_{0};
    std::shared_ptr<AsyncTraceDispatcher> traceDispatcher_;
};

}  // namespace rocketmq

#endif  // ROCKETMQ_CLIENT_CONSUMER_H
