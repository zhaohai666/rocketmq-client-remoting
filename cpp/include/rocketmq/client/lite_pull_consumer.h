// 轻量拉取消费者（对应 org.apache.rocketmq.client.consumer.DefaultLitePullConsumer
// 与 Python client/consumer.py 的 DefaultLitePullConsumer）。
//
// 与 DefaultMQPullConsumer（cpp/pull_consumer.h）的本质区别：
// - DefaultMQPullConsumer：调用方自己 pull(mq,offset) 逐队列拉、自己管位点；没有本地缓冲、
//   没有后台拉取线程、不做 rebalance。
// - DefaultLitePullConsumer：支持两种模式——
//   * subscribe 模式：登记订阅后自动 rebalance 分配队列（与 push 一致），后台拉取线程把
//     消息灌进**本地缓冲**；poll() 只从本地缓冲取消息，不用调用方管位点；
//   * assign 模式：调用方 assign([mq...]) 显式指定队列，不走 rebalance，同样后台灌本地缓冲。
// 两种模式都用 poll(timeout) 取批量消息；位点默认 autoCommit，提交的是 **poll 已经交出去**
// 的那一格（对位 Java 的 consumeOffset），不是「已经拉进本地缓冲」的那一格。
//
// 设计取舍（与 Python / Java 参考实现一致）：
// - 后台**单个** pull 服务线程顺序遍历所有已分配队列做短轮询（suspend=false），把消息塞进
//   一个线程安全的本地缓冲 _local_buffer；poll() 用条件变量等待并 drain 该缓冲。
//   不按队列起独立线程（与 Java 的 PullTask 不同，但语义等价：本地缓冲 + poll）。
// - subscribe 模式的 rebalance 复用既有 getConsumerIdListByGroup + 队列分配策略
//   （allocate_strategy.h，默认 AllocateMessageQueueAveragely），与 push 消费者同一套分配算法；
//   查询不到消费组列表时按 Java 语义「保留当前分配」，不回退独占。
// - 不做 POP / 推模式；不做 broker 主动请求（309/313）处理（那是 push 消费者的职责）。
#ifndef ROCKETMQ_CLIENT_LITE_PULL_CONSUMER_H
#define ROCKETMQ_CLIENT_LITE_PULL_CONSUMER_H

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include "rocketmq/client/allocate_strategy.h"
#include "rocketmq/client/mq_client.h"
#include "rocketmq/client/result.h"
#include "rocketmq/common/message.h"
#include "rocketmq/common/mix_all.h"
#include "rocketmq/common/namespace_util.h"
#include "rocketmq/common/subscription_data.h"
#include "rocketmq/remoting/protocol/heartbeat.h"
#include "rocketmq/remoting/rpchook.h"

namespace rocketmq {

// 队列分配变更监听器（对应 Java MessageQueueListener / Python MessageQueueListener）。
class LiteMessageQueueListener {
public:
    virtual ~LiteMessageQueueListener() = default;
    // mqAll=当前订阅的全部队列；mqDivided=本次新分配给本实例的队列。
    virtual void messageQueueChanged(const std::vector<MessageQueue>& mqAll,
                                    const std::vector<MessageQueue>& mqDivided) = 0;
};

class DefaultLitePullConsumer {
public:
    explicit DefaultLitePullConsumer(
        const std::string& consumerGroup = MixAll::DEFAULT_CONSUMER_GROUP);
    ~DefaultLitePullConsumer();

    DefaultLitePullConsumer(const DefaultLitePullConsumer&) = delete;
    DefaultLitePullConsumer& operator=(const DefaultLitePullConsumer&) = delete;

    // ---------------- 配置 ----------------
    void setNamesrvAddr(const std::string& addr);
    void setNameServerAddresses(const std::vector<std::string>& addrs);
    void setInstanceName(const std::string& name) { instanceName_ = name; }

    // ---------------- unitName / unitMode / enableStreamRequestType ----------------
    // 对应 Java `ClientConfig` 的三个同名开关。⚠ 必须在 start() 之前设置：
    // unitName / @STREAM 决定 clientId 的形状，stream 决定请求钩子链（`ReqT` 要进 ACL 签名）。
    void setUnitName(const std::string& unitName) { unitName_ = unitName; }
    const std::string& unitName() const { return unitName_; }
    // Java 侧 lite 消费者的 unitMode 传进 PullAPIWrapper（DefaultLitePullConsumerImpl:356-359），
    // 只用于消息过滤上下文（PullAPIWrapper:126）；另外心跳里的 ConsumerData.unitMode
    // 由 MQClientInstance:1039 从本消费者读取，决定 %RETRY% topic 是否带 UNIT_SUB 标记。
    // 本端 lite 消费者没有 filter message hook，所以第一处落地只有心跳这一条。
    void setUnitMode(bool unitMode) { unitMode_ = unitMode; }
    bool isUnitMode() const { return unitMode_; }

    // true 时每个请求带扩展字段 `ReqT=0`，且 clientId 末尾多一段 `@STREAM`。
    void setEnableStreamRequestType(bool enable) { enableStreamRequestType_ = enable; }
    bool isEnableStreamRequestType() const { return enableStreamRequestType_; }
    void setMessageModel(const std::string& model) { messageModel_ = model; }
    void setNamespace(const std::string& ns) { namespace_ = ns; }
    void setRPCHook(std::shared_ptr<RPCHook> hook) { rpcHook_ = std::move(hook); }
    void setCredentials(const std::string& accessKey, const std::string& secretKey,
                        const std::string& securityToken = std::string()) {
        rpcHook_ = std::make_shared<AclClientRPCHook>(
            SessionCredentials(accessKey, secretKey, securityToken));
    }
    const std::shared_ptr<RPCHook>& rpcHook() const { return rpcHook_; }

    void setConsumeFromWhere(const std::string& where) { consumeFromWhere_ = where; }
    void setConsumeTimestamp(const std::string& ts) { consumeTimestamp_ = ts; }
    void setPullBatchSize(int32_t n) { pullBatchSize_ = std::max(1, n); }
    void setPollTimeoutMillis(int32_t ms) { pollTimeoutMillis_ = std::max(0, ms); }
    void setAutoCommit(bool b) { autoCommit_ = b; }
    void setAutoCommitIntervalMillis(int32_t ms) { autoCommitIntervalMillis_ = std::max(0, ms); }
    void setConsumerTimeoutMillisWhenSuspend(int32_t ms) {
        consumerTimeoutMillisWhenSuspend_ = ms;
    }
    void setBrokerSuspendMaxTimeMillis(int32_t ms) { brokerSuspendMaxTimeMillis_ = ms; }
    void setPullIntervalMillis(int32_t ms) { pullIntervalMillis_ = std::max(0, ms); }
    void setMessageQueueListener(std::shared_ptr<LiteMessageQueueListener> listener) {
        messageQueueListener_ = std::move(listener);
    }
    // 队列分配策略（对应 Java DefaultLitePullConsumer.setAllocateMessageQueueStrategy）。
    // 与 Java 同款：setter 允许传 nullptr，由 start() 的 checkConfig 拒绝
    //（Java DefaultLitePullConsumerImpl.checkConfig:435）。默认 AllocateMessageQueueAveragely。
    void setAllocateMessageQueueStrategy(std::shared_ptr<AllocateMessageQueueStrategy> strategy);
    std::shared_ptr<AllocateMessageQueueStrategy> allocateMessageQueueStrategy() const;

    const std::string& consumerGroup() const { return consumerGroup_; }
    const std::string& clientId() const { return clientId_; }
    // 对应 Java DefaultLitePullConsumer.getConsumeTimestamp：默认「now - 30 分钟」的 yyyyMMddHHmmss。
    const std::string& consumeTimestamp() const { return consumeTimestamp_; }
    const std::string& namespaceOf() const { return namespace_; }
    bool isStarted() const { return started_; }
    bool isRunning() const { return running_; }

    // ---------------- 订阅 / 分配 ----------------
    // subscribe 模式：登记 topic 订阅（支持 tag 表达式），自动 rebalance 分配队列。
    void subscribe(const std::string& topic, const std::string& subExpression = "*");
    // assign 模式：给某个 topic 的队列指定 tag 过滤表达式（Java setSubExpressionForAssign）。
    void setSubExpressionForAssign(const std::string& topic, const std::string& subExpression);
    // assign 模式：显式指定队列，不走 rebalance。
    void assign(const std::vector<MessageQueue>& messageQueues);

    // ---------------- 生命周期 ----------------
    void start();
    void shutdown();

    // ---------------- 拉取 / poll ----------------
    // 从本地缓冲批量取消息，阻塞至多 timeoutMillis 毫秒。返回取到的消息（可能为空）。
    std::vector<MessageExt> poll(int32_t timeoutMillis = -1);

    // ---------------- 位点 ----------------
    // 三张游标（对位 Java AssignedMessageQueue.MessageQueueState + OffsetStore 内存位点表）：
    //   nextOffset_    拉取游标：下次从哪拉，只有 pullOne/seek 会推进
    //   consumeOffset_ 已消费游标：只有 poll() 把消息真交到调用方手上才前进
    //   offsetTable_   提交落点：发给 broker 的就是它，commit(map, persist=false) 只写这一格
    // 把拉取游标当提交源（本端口改之前的写法）会静默丢消息：一次 pull 能把整段消息灌进
    // 本地缓冲，位点却已经提交到队尾，调用方在这之后崩掉那段就永远不再投递。
    // 把拉取游标定位到指定 offset（并丢弃缓冲里该队列 offset 之前的消息）。
    void seek(const MessageQueue& mq, int64_t offset);
    void seekToBegin(const MessageQueue& mq);
    void seekToEnd(const MessageQueue& mq);
    // 查询该消费组在该队列的已提交位点。对位 Java
    // ``offsetStore.readOffset(mq, ReadOffsetType.MEMORY_FIRST_THEN_STORE)``：先看内存位点表
    // （persist=false 刚写进去、还没发 broker 的值也算数），表里没有再问 broker 并回填。
    // 查不到返回 -1。
    int64_t committed(const MessageQueue& mq);
    // 对应 Java ``commit()`` → ``commitAll()``：按「已消费游标」提交所有已分配队列。
    void commit();
    // 对应 Java ``commit(Map<MessageQueue, Long>, boolean persist)``：调用方指定位点。
    // ⚠ 只改提交落点，**不动拉取游标**：缓冲里已有的消息照旧交给调用方。
    // persist=false 时只写内存表（本端口没有 Java 那份 5s persist 定时器，见 commit 文档）。
    void commit(const std::map<MessageQueue, int64_t>& offsets, bool persist = true);
    // 对应 Java ``commit(Set<MessageQueue>, boolean persist)``：只提交这几条队列，
    // 位点取它们当下的「已消费游标」。
    void commit(const std::set<MessageQueue>& messageQueues, bool persist = true);
    // 按时间戳取该队列的位点（对应 Java offsetForTimestamp）。
    int64_t offsetForTimestamp(const MessageQueue& mq, int64_t timestamp);

    // 两个游标的观测点：位点契约的判据是「哪张表动了、哪张没动」，只看 broker 侧收到的
    // UPDATE_CONSUMER_OFFSET 分不出「提交的是已消费游标」和「提交的是拉取游标」——
    // 消费跟得上的时候两者数值一样。所以单测与真机脚本都要能直接读到这两格；
    // 该队列没有条目时返回 -1（对位 Java MessageQueueState 的初值）。
    int64_t pullCursorOf(const MessageQueue& mq);
    int64_t consumeCursorOf(const MessageQueue& mq);

    // ---------------- 队列查询 / 控制 ----------------
    // 该 topic 的全部可消费队列（按 broker 路由取，已套命名空间）。
    std::vector<MessageQueue> fetchMessageQueues(const std::string& topic);
    // 当前分配给本实例的队列。
    std::vector<MessageQueue> assignment();
    // 暂停 / 恢复某些队列的拉取。
    void pause(const std::vector<MessageQueue>& messageQueues);
    void resume(const std::vector<MessageQueue>& messageQueues);

private:
    std::string withNamespace(const std::string& topic) const {
        return namespace_.empty() ? topic : NamespaceUtil::wrapNamespace(namespace_, topic);
    }

    // 收集订阅的全部队列（listener 回调用）
    std::vector<MessageQueue> mqAllOfSubscription() const;
    // std::set<MessageQueue> -> vector（listener 回调用）
    std::vector<MessageQueue> newSetAsVector(const std::set<MessageQueue>& s) const;

    void rebalance();
    bool pullOne(const MessageQueue& mq);
    std::string subscriptionFor(const std::string& topic) const;
    bool filterTags(const std::string& topic, std::vector<MessageExt>& msgs, const std::string& sub);
    void enqueue(const std::vector<MessageExt>& msgs);
    // 对位 Java DefaultLitePullConsumerImpl#maybeAutoCommit：到点提交**全部**已分配队列，
    // 并把下一道截止时刻推到 now + autoCommitIntervalMillis_（初值 -1 ⇒ 第一次检查就提交）。
    void maybeAutoCommit();
    // commit() 的三个入口共用的那半段：写内存位点表 + 按需 persistAll。
    void commitOffsets(const std::map<MessageQueue, int64_t>& targets,
                       const std::set<MessageQueue>& scope, bool persist);
    // 对位 Java OffsetStore#persist(mq)：只把这一条队列的内存位点发给 broker，不做清理。
    void persistOffset(const MessageQueue& mq);
    // 对位 Java RemoteBrokerOffsetStore#persistAll(Set)：落在 mqs 上的写给 broker，
    // 不在的从内存表里清掉（Java 日志里的 "remove unused mq"）。
    void persistOffsetTable(const std::set<MessageQueue>& mqs);
    // poll() 交付之后推进「已消费游标」，对位 assignedMessageQueue.updateConsumeOffset。
    void advanceConsumeOffset(const std::vector<MessageExt>& msgs);
    int64_t resolveInitialOffset(const MessageQueue& mq);

    // 心跳（把 tag 订阅注册给 broker）
    HeartbeatData buildHeartbeat() const;
    int32_t sendHeartbeatToAllBroker();
    void heartbeatLoop();
    void pullServiceLoop();

    std::string consumerGroup_;
    // 队列分配策略，对应 Java DefaultLitePullConsumer.allocateMessageQueueStrategy
    // （字段初值 new AllocateMessageQueueAveragely()）。
    std::shared_ptr<AllocateMessageQueueStrategy> allocateStrategy_;
    std::string namespace_;
    std::string instanceName_ = "DEFAULT";
    std::string clientId_;
    std::string unitName_;
    bool unitMode_ = false;
    // Java 的 pull / lite 消费者在**每个构造函数**里置 true（DefaultMQPullConsumer:113/126、
    // DefaultLitePullConsumer:213/228），生产者与推送消费者保持 false。
    bool enableStreamRequestType_ = true;
    std::string messageModel_ = MessageModel::CLUSTERING;
    std::string consumeFromWhere_ = ConsumeFromWhere::CONSUME_FROM_LAST_OFFSET;
    std::string consumeTimestamp_;

    int32_t brokerSuspendMaxTimeMillis_ = 20000;
    int32_t consumerTimeoutMillisWhenSuspend_ = 30000;
    int32_t pollTimeoutMillis_ = 5000;
    int32_t pullBatchSize_ = 32;
    bool autoCommit_ = true;
    int32_t autoCommitIntervalMillis_ = 5000;
    int32_t pullIntervalMillis_ = 50;

    std::vector<std::string> nameServerAddrs_;
    std::shared_ptr<RPCHook> rpcHook_;

    // subscribe 模式的订阅表（topic -> sub_expression，已套命名空间）
    std::map<std::string, std::string> subscription_;
    // 订阅对应的 SubscriptionData（带 tagsSet），用于发给 broker 的心跳做 tag 过滤注册
    std::map<std::string, SubscriptionData> subscriptionData_;
    // assign 模式：topic -> tag 过滤表达式（透传给 pull）
    std::map<std::string, std::string> assignSubExpr_;
    // assign 模式与 subscribe 模式互斥；拉取线程每轮都读它，赋值方是调用方线程，
    // 所以是原子量而不是 stateMutex_ 下的普通 bool（读点很多，不值得为它加锁）。
    std::atomic<bool> assignMode_{false};
    // 分配与位点的全部可变状态（assigned_ / 三张游标表 / 自动提交截止时刻 / 暂停集合）
    // 由这一把 stateMutex_ 保护：拉取线程在推进拉取游标、rebalance 在改分配，
    // 试用 poll/commit 的调用方线程同时在读，无锁就是数据竞争（C++ 里是 UB）。
    // 两条铁律：拿着这把锁**不做网络 I/O**，也**不去拿 bufferMutex_**。
    mutable std::mutex stateMutex_;
    std::set<MessageQueue> assigned_;

    std::shared_ptr<LiteMessageQueueListener> messageQueueListener_;

    std::unique_ptr<MQClientInstance> mqClient_;
    std::atomic<bool> started_{false};
    std::atomic<bool> running_{false};

    std::mutex bufferMutex_;
    std::condition_variable bufferCv_;
    std::deque<MessageExt> localBuffer_;

    // 拉取游标：对位 Java MessageQueueState.pullOffset
    std::map<MessageQueue, int64_t> nextOffset_;
    // 已消费游标：对位 Java MessageQueueState.consumeOffset，只有 poll() 交付出去才前进
    std::map<MessageQueue, int64_t> consumeOffset_;
    // 提交落点：对位 Java OffsetStore 的内存位点表（RemoteBrokerOffsetStore.offsetTable）
    std::map<MessageQueue, int64_t> offsetTable_;
    // 对位 Java MessageQueueState.seekOffset：seek 之后下一次拉取从这里取数
    std::map<MessageQueue, int64_t> seekOffset_;
    std::set<MessageQueue> paused_;
    // Java DefaultLitePullConsumerImpl.nextAutoCommitDeadline：初值 -1 ⇒ 第一次检查就提交
    int64_t nextAutoCommitDeadline_ = -1;

    std::chrono::steady_clock::time_point lastRebalanceTs_;
    std::thread pullThread_;
    std::thread heartbeatThread_;
};

}  // namespace rocketmq

#endif  // ROCKETMQ_CLIENT_LITE_PULL_CONSUMER_H
