// 生产者自动攒批（对应 org.apache.rocketmq.client.producer.ProduceAccumulator，Java 5.5.0）。
// 与 `python/client/produce_accumulator.py`、`csharp/.../ProduceAccumulator.cs`、
// `rust/src/client/produce_accumulator.rs` 同题。
//
// 打开 `autoBatch` 之后，`send(Message)` 不再一条一条直发，而是先按
// `AggregateKey(topic, mq, waitStoreMsgOK, tag)` 归并进 `MessageAccumulation`，攒够
// `holdMs` / `holdSize`（或被守卫线程唤醒）再合成**一个** `MessageBatch` 发出去，最后把
// broker 回的**批量** `SendResult` 拆回每条消息各自的 `SendResult` —— 调用方看到的东西
// 与直发一致。
//
// 契约（逐条对齐 Java，别"顺手优化"）：
//   1. `AggregateKey` 是 topic + mq + waitStoreMsgOK + tag 四元组：tag 不同不合并
//      （一个 `MessageBatch` 只有一个 TAGS 属性）；指定 mq 与不指定 mq 也不合并。
//   2. `tryAddMessage` 是全局字节闸门：`currentlyHoldSize < totalHoldSize` 才放行，放行时
//      把本条 body 长度记进去；**批次真的发完**才扣回。⚠ 上游真实口径是「先记账，再判延时/
//      重试」—— `canBatch` 里因延时消息退回直发的那条，其字节数已被记进 `currentlyHoldSize`
//      且**永不归还**（Java 遗漏，照抄）。
//   3. 批量应答拆条：broker 对批量消息回的 MsgId/OffsetMsgId 是**逗号分隔**的逐条 ID；
//      含逗号才拆，条数对不上抛异常；不含逗号（老 broker / 单条）时**所有**下标指向同一份
//      内容（Java 是同一个实例，这里按值拷贝）。
//   4. **同步 `add` 收集 keys，异步 `add` 不收集**（Java 的不对称行为，照抄）。
//   5. 批级 `KEYS` 是 `String.join(" ", keys)`：分隔符是**空格**
//      （`MessageConst::KEY_SEPARATOR`），且**无条件**写属性 —— 空集合写出 `KEYS=""`。
//   6. 守卫线程每轮 `max(1, holdMs/2)` ms：sync 版对每个批次 `wakeup()`（叫醒正在 `add` 里
//      等阈值的调用方去自查 `readyToSend`），再把 `messagesSize == 0` 的空批次置 closed 并
//      摘表；async 版先 `readyToSend` 就发，再做同样的摘表。
//      ⚠ 发完的批次 `messagesSize` 仍 > 0（`send` 只置 closed，不重置 size），所以它会
//      **留在表里**，直到下一次同键 `send` 拿到它、`add` 返回 -1 才被摘掉重取。
//
// ## 与其它三端的差异
//
// * **sender 的生命周期**（cpp 结构性差异）：Java / Python / C# / Rust 的累加器都**强引用**
//   那个生产者（Java 其实因此泄漏），而 cpp 的生产者通常是栈对象。所以这里持
//   `AccumulatorSender*` 裸指针，并要求生产者在 `~DefaultMQProducer` / `shutdown()` 里调
//   `detachSender()`；`getOrCreateProduceAccumulator` 在发现 sender 已 detach 时会**重新
//   绑定**当前生产者（Java 是"第一次记下的那个用到天荒地老"，在 cpp 会悬垂）。
// * **语言级差异（语义不变）**：Java 用 `synchronized/wait`，这里是
//   `std::mutex + std::condition_variable`；Java 的 `Throwable` 是共享实例，这里回调按
//   `std::exception_ptr` 逐个复制（见 `AccumulationCallback`）。
#ifndef ROCKETMQ_CLIENT_PRODUCE_ACCUMULATOR_H
#define ROCKETMQ_CLIENT_PRODUCE_ACCUMULATOR_H

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include "rocketmq/client/result.h"
#include "rocketmq/common/message.h"

namespace rocketmq {

// Java ProduceAccumulator 的三个默认值
constexpr int64_t PRODUCE_ACCUMULATOR_DEFAULT_TOTAL_HOLD_SIZE = 32 * 1024 * 1024;
constexpr int64_t PRODUCE_ACCUMULATOR_DEFAULT_HOLD_SIZE = 32 * 1024;
constexpr int32_t PRODUCE_ACCUMULATOR_DEFAULT_HOLD_MS = 10;

// 累加器唯一的对外依赖：一次「绕过累加器」的批量发送
// （Java `DefaultMQProducer.sendDirect(MessageBatch, mq, callback)`）。
//
// 抽成接口有两个原因：① 守卫线程是普通 `std::thread`，而生产者的发送入口是同步 / 异步两套，
// 需要一个明确的同步入口；② 单测要能塞一个「只记账不联网」的假生产者（Java 单测的
// `MockMQProducer` 同款）。真实实现是 `DefaultMQProducer` 自己。
class AccumulatorSender {
public:
    virtual ~AccumulatorSender() = default;

    // 同步语义（Java `sendDirect(batch, mq, null)`）：返回该批量的 `SendResult`。
    virtual SendResult sendDirectBlocking(const MessageBatch& batch,
                                          const MessageQueue* mq) = 0;

    // 异步语义（Java `sendDirect(batch, mq, callback)`）：**立刻返回**，结果走 callback。
    virtual void sendDirectAsync(const MessageBatch& batch, const MessageQueue* mq,
                                 std::shared_ptr<SendCallback> callback) = 0;
};

// ---------------------------------------------------------------- 归并键

// 归并键：topic + mq + waitStoreMsgOK + tag（Java `ProduceAccumulator.AggregateKey`）。
struct AggregateKey {
    std::string topic;
    bool hasMq = false;
    MessageQueue mq;
    bool waitStoreMsgOk = true;
    // Java 的 `tag` 可以是 `null`（属性缺失）—— 与 `""` 不是一回事。
    bool hasTag = false;
    std::string tag;

    static AggregateKey ofMessage(const Message& msg);
    static AggregateKey ofMessageWithMq(const Message& msg, const MessageQueue& mq);

    bool operator==(const AggregateKey& o) const;
    bool operator!=(const AggregateKey& o) const { return !(*this == o); }
    // 只给 `std::map` 用（Java 那边用 hashCode/equals 的 ConcurrentHashMap，顺序无所谓）。
    bool operator<(const AggregateKey& o) const;

    std::string toString() const;
};

// ---------------------------------------------------------------- 批次

class ProduceAccumulator;  // 前向声明：批次只持它的裸指针（生命周期由累加器的表保证）

// 一批待归并的消息（Java `ProduceAccumulator.MessageAccumulation`）。
//
// 两类调用方：同步 `add()` 会在批次达到阈值前**阻塞**自己（等到本批真的发出去，自己的那条
// 消息才有 SendResult 可返回）；异步 `add(msg, callback)` 立刻返回，结果走回调。
//
// 非拷贝：`std::shared_ptr` 持有，`owner_` 是裸指针（累加器的表持着它，生命周期更长）。
class MessageAccumulation {
public:
    MessageAccumulation(AggregateKey key, ProduceAccumulator* owner);
    ~MessageAccumulation();

    MessageAccumulation(const MessageAccumulation&) = delete;
    MessageAccumulation& operator=(const MessageAccumulation&) = delete;

    const AggregateKey& aggregateKey() const { return key_; }

    // Java `add(Message)`：入队并阻塞到本批发完（返回本条消息在本批里的下标），
    // `-1` 表示本批已关闭（调用方需重取批次）。
    int64_t add(const Message& msg);

    // Java `add(Message, SendCallback)`：`false` 表示本批已关闭（调用方需重取）。
    bool add(const Message& msg, std::shared_ptr<SendCallback> callback);

    bool readyToSend() const;
    bool closed() const { return closed_.load(); }
    // Java `wakeup()`：`public synchronized`，叫醒一个正在 `add` 里等阈值的调用方。
    void wakeup();
    int64_t count() const;
    int64_t messagesSize() const;
    std::set<std::string> keys() const;
    std::vector<Message> messages() const;
    std::vector<SendResult> sendResults() const;

    // 测试接缝（Java 单测是直接 `batch.send()`，两边都绕开守卫线程）
    void forceSyncSend();
    void forceAsyncSend();

private:
    void markClosed();
    MessageBatch buildBatch() const;
    void splitSendResults(const SendResult& sendResult);
    void sendSync();
    void sendAsyncNow();
    // 把异常逐个交给本批收集到的回调，**不**归还字节额度 —— 对应 Java 异步 `send(cb)` 的
    // 外层 `catch (Exception e) { for (v : sendCallbacks) v.onException(e); }`：那条支路
    // 确实漏掉了 `currentlyHoldSize.addAndGet(-size)`（上游遗漏，照抄；回调路径才归还）。
    void notifyCallbacksError(const std::exception_ptr& error);
    void releaseHold();
    void finish(const SendResult* result, const std::exception_ptr& error);

    AggregateKey key_;
    ProduceAccumulator* owner_;
    std::vector<Message> messages_;
    std::vector<std::shared_ptr<SendCallback>> callbacks_;
    std::set<std::string> keys_;
    std::vector<SendResult> sendResults_;
    mutable std::mutex stateMutex_;
    int64_t count_ = 0;
    int64_t messagesSize_ = 0;
    std::atomic<bool> closed_{false};
    int64_t createTime_ = 0;
    // Java 的 `synchronized (this)`：`wait/notify` 用的那一把。`forceSyncSend` 与
    // `sendSync`（从 `add` 里调）都持有它，避免"发完的唤醒"落在等待者进 `wait()` 之前丢掉。
    mutable std::mutex waitMutex_;
    std::condition_variable waitCv_;

    friend class ProduceAccumulator;
};

// ---------------------------------------------------------------- 累加器

// 对应 Java `ProduceAccumulator`：按 clientId 复用的自动攒批器。
class ProduceAccumulator {
public:
    ProduceAccumulator(std::string instanceName, AccumulatorSender* sender);
    ~ProduceAccumulator();

    ProduceAccumulator(const ProduceAccumulator&) = delete;
    ProduceAccumulator& operator=(const ProduceAccumulator&) = delete;

    const std::string& instanceName() const { return instanceName_; }

    // 幂等且可重复：`start -> shutdown -> start`（生产者重启）会重建两根守卫线程
    // （Java 的 `ServiceThread` 同样可重复 start）。
    void start();
    void shutdown();

    // ---------------- 参数（Java 的校验口径与文案逐字照抄）----------------
    int32_t getBatchMaxDelayMs() const { return holdMs_.load(); }
    void setBatchMaxDelayMs(int32_t holdMs);
    int32_t getBatchMaxBytes() const { return static_cast<int32_t>(holdSize_.load()); }
    void setBatchMaxBytes(int32_t holdSize);
    // Java 这里也返回 `holdSize`（不是 `totalHoldSize`）—— 上游笔误，照抄。
    int32_t getTotalBatchMaxBytes() const { return static_cast<int32_t>(holdSize_.load()); }
    void setTotalBatchMaxBytes(int32_t totalHoldSize);
    int64_t totalHoldSize() const { return totalHoldSize_.load(); }
    int64_t currentlyHoldSize() const;

    // ---------------- 全局字节闸门 ----------------
    // Java `tryAddMessage`：还有额度就记账放行，否则拒绝（调用方退回直发）。
    bool tryAddMessage(const Message& message);
    void releaseHold(int64_t size);

    // ---------------- 归并入口 ----------------
    // Java `send(Message, DefaultMQProducer)`：同步版阻塞到本批发完并返回本条自己的结果。
    SendResult send(const Message& msg);
    SendResult sendWithMq(const Message& msg, const MessageQueue& mq);
    // Java `send(Message, MessageQueue, SendCallback, DefaultMQProducer)` 的异步版。
    void sendAsync(const Message& msg, std::shared_ptr<SendCallback> callback);
    void sendAsyncWithMq(const Message& msg, const MessageQueue& mq,
                         std::shared_ptr<SendCallback> callback);

    // ---------------- sender 生命周期 ----------------
    // 生产者析构 / shutdown 时调用：之后守卫线程不再碰那个 sender。
    void detachSender();
    // 重新绑定（`getOrCreateProduceAccumulator` 在 sender 已 detach 时用）。
    void attachSender(AccumulatorSender* sender);
    bool hasSender() const;

    // ---------------- 取证 / 测试接缝 ----------------
    size_t syncBatchCount() const;
    size_t asyncBatchCount() const;
    std::vector<std::shared_ptr<MessageAccumulation>> syncBatchesSnapshot() const;
    std::vector<std::shared_ptr<MessageAccumulation>> asyncBatchesSnapshot() const;
    // 往同步表里放一个（尚无人 add 的）批次 —— 验证守卫线程对 `messagesSize == 0` 的清理口径。
    std::shared_ptr<MessageAccumulation> putEmptySyncBatch(const AggregateKey& key);
    // 手工跑一轮守卫**主体**（守卫线程每个周期里 `sleep(max(1, holdMs/2))` 之前的那段）。
    void runGuardOnce(bool sync);

private:
    std::shared_ptr<MessageAccumulation> getOrCreateSyncBatch(const AggregateKey& key);
    std::shared_ptr<MessageAccumulation> getOrCreateAsyncBatch(const AggregateKey& key);
    void removeSyncBatch(const AggregateKey& key,
                         const std::shared_ptr<MessageAccumulation>& batch);
    void removeAsyncBatch(const AggregateKey& key,
                          const std::shared_ptr<MessageAccumulation>& batch);
    void guardOnce(bool sync);
    void guardLoop(bool sync, const std::string& name);
    int64_t holdMs() const { return holdMs_.load(); }
    int64_t holdSize() const { return holdSize_.load(); }

    std::string instanceName_;
    // `mutable`：`hasSender()` 是 const，要拿它。
    mutable std::mutex senderMutex_;
    AccumulatorSender* sender_ = nullptr;
    std::atomic<int32_t> holdMs_{PRODUCE_ACCUMULATOR_DEFAULT_HOLD_MS};
    std::atomic<int64_t> holdSize_{PRODUCE_ACCUMULATOR_DEFAULT_HOLD_SIZE};
    std::atomic<int64_t> totalHoldSize_{PRODUCE_ACCUMULATOR_DEFAULT_TOTAL_HOLD_SIZE};
    mutable std::mutex holdMutex_;
    int64_t currentlyHoldSize_ = 0;
    mutable std::mutex syncTableMutex_;
    std::map<AggregateKey, std::shared_ptr<MessageAccumulation>> syncBatches_;
    mutable std::mutex asyncTableMutex_;
    std::map<AggregateKey, std::shared_ptr<MessageAccumulation>> asyncBatches_;
    std::atomic<bool> stopped_{false};
    std::mutex guardMutex_;
    std::thread syncGuard_;
    std::thread asyncGuard_;

    friend class MessageAccumulation;
};

// 进程级复用表：对应 Java `MQClientManager.getOrCreateProduceAccumulator` —— 按 clientId 缓存，
// 所以同进程里两个 clientId 相同的 producer 共享同一个累加器与同一对守卫线程（这也是「阈值先
// 记在 producer 上、`start()` 时再同步下去」的原因）。
//
// ⚠ 与 Java 的一处差异：sender 已 detach（前任生产者已析构）时会重新绑定到本次的 sender。
std::shared_ptr<ProduceAccumulator> getOrCreateProduceAccumulator(const std::string& clientId,
                                                                 AccumulatorSender* sender);

}  // namespace rocketmq

#endif  // ROCKETMQ_CLIENT_PRODUCE_ACCUMULATOR_H
