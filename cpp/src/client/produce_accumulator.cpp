// ProduceAccumulator / MessageAccumulation / AggregateKey 的实现。
// 契约与四端差异见 produce_accumulator.h 的文件头。
#include "rocketmq/client/produce_accumulator.h"

#include <algorithm>
#include <chrono>
#include <exception>
#include <functional>
#include <stdexcept>
#include <utility>

#include "rocketmq/client/exception.h"
#include "rocketmq/common/logging.h"
#include "rocketmq/common/message_const.h"
#include "rocketmq/common/mix_all.h"
#include "rocketmq/common/util_all.h"

namespace rocketmq {
namespace {

// ---------------------------------------------------------------- 小工具

// Java `String.split(sep)`（limit=0）：**丢弃所有尾部空串**，C++ 的朴素 split 会保留。
// 用在批量应答的 MsgId/OffsetMsgId 拆条上 —— 与 Java 同口径才不会把
// `"a,b,"` 拆成 3 段导致「条数对不上」误判。
//
// 唯一与 Java 不同的输入是**全空串**：Java `"".split(" ")` 返回 `[""]`（长度 1），
// 这里返回空表。这条差异只可能经由 `KEYS` 的 join 显形，而且只在「同一批里既有
// `KEYS=""` 又有关键字」时才看得见 —— Java 会拼出一个前导/尾随空格（HashSet 顺序还不定），
// 这里确定性地不拼。两者对 broker 都只是"一对多的 key 索引"，取值取确定的那一个。
std::vector<std::string> split(const std::string& text, char sep) {
    std::vector<std::string> parts;
    size_t start = 0;
    while (true) {
        const size_t pos = text.find(sep, start);
        if (pos == std::string::npos) {
            parts.push_back(text.substr(start));
            break;
        }
        parts.push_back(text.substr(start, pos - start));
        start = pos + 1;
    }
    while (!parts.empty() && parts.back().empty()) {
        parts.pop_back();
    }
    return parts;
}

// Java `String.split(MessageConst.KEY_SEPARATOR)` 的等价物（分隔符是**字符串**" "，不是 char）。
void splitKeysInto(std::set<std::string>& target, const std::string& keys) {
    for (const std::string& p : split(keys, MessageConst::KEY_SEPARATOR[0])) {
        target.insert(p);
    }
}

// 发送前确保消息带有 UNIQ_KEY（32 位十六进制唯一 ID），与 Java
// `MessageClientIDSetter.setUniqID` 对齐：缺失才生成，已存在则保留（幂等）。
// 与 producer.cpp 里的 `ensureUniqId` 同实现 —— 那里是文件静态函数，不进公共头。
//
// ⚠ 这里**只**给批量对象本身补 ID。子消息的 UNIQ_KEY 由 `sendByAccumulator` 逐条写好
// （Java `sendByAccumulator:791`），在**编码之前**；少了它，broker 拆开批量后每条子消息都
// 没有客户端 ID，消费端与轨迹控制台都串不起来。
void ensureUniqId(Message& msg) {
    std::string uniq = msg.getProperty(MessageConst::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX);
    if (uniq.empty()) {
        uniq = InnerIdGenerator::createUniqId();
        msg.putProperty(MessageConst::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX, uniq);
    }
}

// 异步批量发送的「结局折回器」：对应 Java `MessageAccumulation.send(sendCallback)` 里那个
// 匿名 `SendCallback`。把**一次批量发送**的结果（或异常）折回它所属批次的逐条结果，
// 再按序交付给本批收集到的所有用户回调，最后归还全局字节额度。
//
// 为什么不直接把 `MessageAccumulation*` 交给回调：`finish()` 是私有的，而这里只需要
// 「一个终点函数」。用 `std::function` 让 `sendAsyncNow()` 在成员函数里组一个闭包，
// 就不必为了跨类访问再开一条 friend。
//
// 生命周期：闭包按 Java 同款捕获批次裸指针。批次由累加器的表持有，而累加器活在进程级
// 注册表里（见文件尾 `getOrCreateProduceAccumulator`），所以批次不会先于在途回调消失。
class AccumulationCallback : public SendCallback {
public:
    using Reducer = std::function<void(const SendResult*, const std::exception_ptr&)>;

    explicit AccumulationCallback(Reducer reducer) : reducer_(std::move(reducer)) {}

    void onSuccess(const SendResult& sendResult) override { reducer_(&sendResult, nullptr); }
    void onException(const std::exception_ptr& error) override { reducer_(nullptr, error); }

private:
    Reducer reducer_;
};

// 进程级复用表（见头文件 getOrCreateProduceAccumulator 的注释）。
std::mutex& registryMutex() {
    static std::mutex m;
    return m;
}
std::map<std::string, std::shared_ptr<ProduceAccumulator>>& registry() {
    static std::map<std::string, std::shared_ptr<ProduceAccumulator>> table;
    return table;
}

}  // namespace

// ---------------------------------------------------------------- AggregateKey

AggregateKey AggregateKey::ofMessage(const Message& msg) {
    AggregateKey key;
    key.topic = msg.getTopic();
    key.hasMq = false;
    // Java `AggregateKey(message)` 用 `message.isWaitStoreMsgOK()`：**缺省即 true**。
    // 直接比 `getWaitStoreMsgOk() == "true"` 会把普通消息（没设过 WAIT）判成 false，
    // 攒出来的批量就会以 `WAIT=false` 下发（broker 不等刷盘就回 SEND_OK）。
    key.waitStoreMsgOk = msg.isWaitStoreMsgOk();
    const std::string tag = msg.getTags();
    key.hasTag = msg.properties.count(MessageConst::PROPERTY_TAGS) > 0;
    key.tag = key.hasTag ? tag : std::string();
    return key;
}

AggregateKey AggregateKey::ofMessageWithMq(const Message& msg, const MessageQueue& mq) {
    AggregateKey key = ofMessage(msg);
    key.hasMq = true;
    key.mq = mq;
    return key;
}

bool AggregateKey::operator==(const AggregateKey& o) const {
    return waitStoreMsgOk == o.waitStoreMsgOk && topic == o.topic && hasMq == o.hasMq
           && (!hasMq || mq == o.mq) && hasTag == o.hasTag && (!hasTag || tag == o.tag);
}

bool AggregateKey::operator<(const AggregateKey& o) const {
    if (topic != o.topic) return topic < o.topic;
    if (hasMq != o.hasMq) return hasMq < o.hasMq;
    if (hasMq && !(mq == o.mq)) {
        if (mq.brokerName != o.mq.brokerName) return mq.brokerName < o.mq.brokerName;
        if (mq.queueId != o.mq.queueId) return mq.queueId < o.mq.queueId;
        return mq.topic < o.mq.topic;
    }
    if (waitStoreMsgOk != o.waitStoreMsgOk) return waitStoreMsgOk < o.waitStoreMsgOk;
    if (hasTag != o.hasTag) return hasTag < o.hasTag;
    return hasTag && tag < o.tag;
}

std::string AggregateKey::toString() const {
    return "AggregateKey(topic=" + topic + ", mq=" + (hasMq ? mq.toString() : "null")
           + ", waitStoreMsgOK=" + (waitStoreMsgOk ? "true" : "false")
           + ", tag=" + (hasTag ? tag : "null") + ")";
}

// ---------------------------------------------------------------- MessageAccumulation

MessageAccumulation::MessageAccumulation(AggregateKey key, ProduceAccumulator* owner)
    : key_(std::move(key)), owner_(owner), createTime_(UtilAll::currentTimeMillis()) {}

MessageAccumulation::~MessageAccumulation() = default;

int64_t MessageAccumulation::add(const Message& msg) {
    int64_t index = -1;
    {
        std::lock_guard<std::mutex> lk(stateMutex_);
        if (closed_.load()) {
            return -1;
        }
        index = count_++;
        const int64_t bodySize = static_cast<int64_t>(msg.getBody().size());
        if (bodySize > 0) {
            messagesSize_ += bodySize;
        }
        // Java：`String msgKeys = msg.getKeys(); if (msgKeys != null) ...` —— 注意判的是
        // **属性存在**而不是非空串（空串会被 split 出一个空 key，join 回来还是空串）。
        auto it = msg.properties.find(MessageConst::PROPERTY_KEYS);
        if (it != msg.properties.end()) {
            splitKeysInto(keys_, it->second);
        }
        messages_.push_back(msg);
    }
    // Java `synchronized (this) { while (!closed) { if (readyToSend()) { send(); break; } wait(); } }`
    std::unique_lock<std::mutex> lk(waitMutex_);
    while (!closed_.load()) {
        if (readyToSend()) {
            sendSync();
            break;
        }
        waitCv_.wait(lk);
    }
    return index;
}

bool MessageAccumulation::add(const Message& msg, std::shared_ptr<SendCallback> callback) {
    {
        std::lock_guard<std::mutex> lk(stateMutex_);
        if (closed_.load()) {
            return false;
        }
        ++count_;
        const int64_t bodySize = static_cast<int64_t>(msg.getBody().size());
        if (bodySize > 0) {
            messagesSize_ += bodySize;
        }
        // ⚠ 异步 `add` **不**收集 keys（Java 的不对称行为，照抄）：异步批次的 KEYS 恒为空串。
        messages_.push_back(msg);
        callbacks_.push_back(std::move(callback));
    }
    if (readyToSend()) {
        sendAsyncNow();
    }
    return true;
}

bool MessageAccumulation::readyToSend() const {
    return messagesSize() > owner_->holdSize()
           || UtilAll::currentTimeMillis() >= createTime_ + owner_->holdMs();
}

void MessageAccumulation::wakeup() {
    std::lock_guard<std::mutex> lk(waitMutex_);
    if (closed_.load()) {
        return;
    }
    waitCv_.notify_all();
}

void MessageAccumulation::markClosed() { closed_.store(true); }

int64_t MessageAccumulation::count() const {
    std::lock_guard<std::mutex> lk(stateMutex_);
    return count_;
}

int64_t MessageAccumulation::messagesSize() const {
    std::lock_guard<std::mutex> lk(stateMutex_);
    return messagesSize_;
}

std::set<std::string> MessageAccumulation::keys() const {
    std::lock_guard<std::mutex> lk(stateMutex_);
    return keys_;
}

std::vector<Message> MessageAccumulation::messages() const {
    std::lock_guard<std::mutex> lk(stateMutex_);
    return messages_;
}

std::vector<SendResult> MessageAccumulation::sendResults() const {
    std::lock_guard<std::mutex> lk(stateMutex_);
    return sendResults_;
}

MessageBatch MessageAccumulation::buildBatch() const {
    std::lock_guard<std::mutex> lk(stateMutex_);
    MessageBatch batch(messages_);
    batch.topic = key_.topic;
    batch.setWaitStoreMsgOk(key_.waitStoreMsgOk);
    // 无条件写（空集合即 KEYS=""，见头文件第 5 条）。Java 是 `String.join(" ", keys)`，
    // 顺序由 HashSet 的哈希决定（未定义）；这里用 `std::set` 的字典序，顺序确定 ——
    // 属性语义是"空格分隔的集合"，broker 只拿它做索引，顺序无关。
    std::string joined;
    for (auto it = keys_.begin(); it != keys_.end(); ++it) {
        if (it != keys_.begin()) {
            joined += MessageConst::KEY_SEPARATOR;
        }
        joined += *it;
    }
    batch.setKeys(joined);
    if (key_.hasTag) {
        batch.setTags(key_.tag);
    }
    ensureUniqId(batch);
    batch.body = batch.encode();
    batch.hasBody = true;
    batch.isBatch = true;
    return batch;
}

void MessageAccumulation::splitSendResults(const SendResult& sendResult) {
    std::vector<SendResult> results;
    const size_t total = static_cast<size_t>(count());
    // Java `isBatchConsumerQueue = !sendResult.getMsgId().contains(",")`
    if (sendResult.msgId.find(',') != std::string::npos) {
        const std::vector<std::string> ids = split(sendResult.msgId, ',');
        const std::vector<std::string> offsets = split(sendResult.offsetMsgId, ',');
        if (ids.size() != total || offsets.size() != total) {
            throw MQClientException("sendResult is illegal");
        }
        for (size_t i = 0; i < total; ++i) {
            // 逐字段构造（Java 用的是 7 参构造器，**不是**拷贝整个 result）：
            // `traceOn` 落回默认 true、`recallHandle` 落回 null —— 与 Java 同。
            // 那两个字段对累加器的调用方没有意义（延时消息根本进不了累加器，
            // 见 `canBatch`），但"少拷贝一个字段"这种事不该靠推理，照 Java 写死。
            SendResult item;
            item.sendStatus = sendResult.sendStatus;
            item.msgId = ids[i];
            item.messageQueue = sendResult.messageQueue;
            item.queueOffset = sendResult.queueOffset + static_cast<int64_t>(i);
            item.transactionId = sendResult.transactionId;
            item.offsetMsgId = offsets[i];
            item.regionId = sendResult.regionId;
            results.push_back(std::move(item));
        }
    } else {
        // 不含逗号：老 broker / 单条应答，所有下标都是同一份内容（Java 是同一个实例）
        results.assign(total, sendResult);
    }
    std::lock_guard<std::mutex> lk(stateMutex_);
    sendResults_ = std::move(results);
}

void MessageAccumulation::releaseHold() {
    owner_->releaseHold(messagesSize());
}

void MessageAccumulation::sendSync() {
    // 调用方（`add`）已经持有 `waitMutex_`，与 Java 在监视器里调 `send()` 一致。
    {
        std::lock_guard<std::mutex> lk(stateMutex_);
        if (closed_.load()) {
            return;
        }
        closed_.store(true);
    }
    const MessageBatch batch = buildBatch();
    AccumulatorSender* sender = nullptr;
    {
        std::lock_guard<std::mutex> lk(owner_->senderMutex_);
        sender = owner_->sender_;
    }
    try {
        if (sender == nullptr) {
            throw MQClientException("defaultMQProducer is null, can not send message");
        }
        const SendResult result =
            sender->sendDirectBlocking(batch, key_.hasMq ? &key_.mq : nullptr);
        splitSendResults(result);
    } catch (...) {
        // Java：finally 里无条件归还全局字节额度，异常继续向上抛
        releaseHold();
        waitCv_.notify_all();
        throw;
    }
    releaseHold();
    waitCv_.notify_all();
}

void MessageAccumulation::finish(const SendResult* result, const std::exception_ptr& error) {
    std::vector<std::shared_ptr<SendCallback>> callbacks;
    {
        std::lock_guard<std::mutex> lk(stateMutex_);
        callbacks = callbacks_;
    }
    if (error == nullptr && result != nullptr) {
        try {
            splitSendResults(*result);
        } catch (...) {
            // Java：`onSuccess` 里拆条失败就转 `onException`（额度在那一支里归还一次）
            const std::exception_ptr e = std::current_exception();
            releaseHold();
            for (const auto& cb : callbacks) {
                if (cb != nullptr) {
                    cb->onException(e);
                }
            }
            return;
        }
        const std::vector<SendResult> results = sendResults();
        releaseHold();
        for (size_t i = 0; i < callbacks.size(); ++i) {
            if (callbacks[i] == nullptr) {
                continue;
            }
            if (i < results.size()) {
                callbacks[i]->onSuccess(results[i]);
            } else {
                // Java 在 `if (i != count) throw` / `sendResults[i]` 越界处抛
                // `IllegalArgumentException("sendResult is illegal")`，被外层 catch 抓住后
                // 对**所有**回调调 `onException`。`sendResults` 恒为 `count` 长、回调也恒为
                // `count` 个，所以这一支实际不可达，留作护栏。
                callbacks[i]->onException(std::make_exception_ptr(
                    MQClientException("sendResult is illegal")));
            }
        }
        return;
    }
    // Java `splitSendResults(null)` 抛 `IllegalArgumentException("sendResult is null")`；
    // 其余情况是 `onException` 原样把 Throwable 转给每个回调（这里按 `exception_ptr` 逐个
    // 复制 —— 语言级差异，见头文件）。
    const std::exception_ptr e =
        error != nullptr ? error
                         : std::make_exception_ptr(MQClientException("sendResult is null"));
    releaseHold();
    for (const auto& cb : callbacks) {
        if (cb != nullptr) {
            cb->onException(e);
        }
    }
}

void MessageAccumulation::notifyCallbacksError(const std::exception_ptr& error) {
    std::vector<std::shared_ptr<SendCallback>> callbacks;
    {
        std::lock_guard<std::mutex> lk(stateMutex_);
        callbacks = callbacks_;
    }
    for (const auto& cb : callbacks) {
        if (cb != nullptr) {
            cb->onException(error);
        }
    }
}

void MessageAccumulation::sendAsyncNow() {
    {
        std::lock_guard<std::mutex> lk(stateMutex_);
        if (closed_.load()) {
            return;
        }
        closed_.store(true);
    }
    const MessageBatch batch = buildBatch();
    AccumulatorSender* sender = nullptr;
    {
        std::lock_guard<std::mutex> lk(owner_->senderMutex_);
        sender = owner_->sender_;
    }
    if (sender == nullptr) {
        // cpp 结构性差异：Java 的 `defaultMQProducer` 是强引用（永不为 null），这里生产者
        // 可能已经 `detachSender()`（析构）。行为对齐 Java 的 `else throw` 支路：交错误给
        // 回调、**不**归还额度。
        notifyCallbacksError(std::make_exception_ptr(
            MQClientException("defaultMQProducer is null, can not send message")));
        return;
    }
    // 批量发送完成的回执：拆条后按序交付给本批收集到的所有用户回调，并归还字节额度。
    auto reducer = std::make_shared<AccumulationCallback>(
        [this](const SendResult* r, const std::exception_ptr& e) { finish(r, e); });
    try {
        sender->sendDirectAsync(batch, key_.hasMq ? &key_.mq : nullptr, reducer);
    } catch (...) {
        // ⚠ Java 在这里**没有**归还 currentlyHoldSize（只有回调路径会还）—— 即"异步发送在
        // 发起阶段就抛异常"会漏掉一份字节额度。照抄，别修。
        notifyCallbacksError(std::current_exception());
    }
}

void MessageAccumulation::forceSyncSend() {
    std::lock_guard<std::mutex> lk(waitMutex_);
    sendSync();
}

void MessageAccumulation::forceAsyncSend() { sendAsyncNow(); }

// ---------------------------------------------------------------- ProduceAccumulator

ProduceAccumulator::ProduceAccumulator(std::string instanceName, AccumulatorSender* sender)
    : instanceName_(std::move(instanceName)), sender_(sender) {}

ProduceAccumulator::~ProduceAccumulator() {
    try {
        shutdown();
    } catch (...) {
        // 析构不抛
    }
}

// Java 的 `start()` 是 `guardThreadForSyncSend.start(); guardThreadForAsyncSend.start();`
// —— ServiceThread 的 start **可重复**（stopped=false + 起新线程），生产者重启时照样走一遍。
// 这里用"两根线程都 joinable 才当已启动"，语义等价。
void ProduceAccumulator::start() {
    std::lock_guard<std::mutex> lk(guardMutex_);
    if (syncGuard_.joinable() || asyncGuard_.joinable()) {
        return;
    }
    stopped_.store(false);
    // Java 的服务名：`String.format("Client_%s_GuardForSyncSend", clientInstanceName)`
    const std::string syncName = "Client_" + instanceName_ + "_GuardForSyncSend";
    const std::string asyncName = "Client_" + instanceName_ + "_GuardForAsyncSend";
    syncGuard_ = std::thread([this, syncName] { guardLoop(true, syncName); });
    asyncGuard_ = std::thread([this, asyncName] { guardLoop(false, asyncName); });
}

void ProduceAccumulator::shutdown() {
    std::thread syncGuard;
    std::thread asyncGuard;
    {
        std::lock_guard<std::mutex> lk(guardMutex_);
        // 幂等：没起来就直接把标志压成"停"（`start()` 会再翻回来）
        stopped_.store(true);
        syncGuard = std::move(syncGuard_);
        asyncGuard = std::move(asyncGuard_);
    }
    // 在锁外 join（守卫循环本身不碰 guardMutex_，但 join 期间不该占着它）
    if (syncGuard.joinable()) {
        syncGuard.join();
    }
    if (asyncGuard.joinable()) {
        asyncGuard.join();
    }
}

// ---------------- 参数 ----------------
// Java 的 `batchMaxDelayMs / batchMaxBytes / totalBatchMaxBytes` 三个 setter 的校验区间与
// 文案逐字照抄（`String.format("... but get %d!", v)`）。Java 抛 IllegalArgumentException，
// 这里是 `std::invalid_argument` —— cpp 里参数域错误的标准映射。
void ProduceAccumulator::setBatchMaxDelayMs(int32_t holdMs) {
    if (holdMs <= 0 || holdMs > 30 * 1000) {
        throw std::invalid_argument("batchMaxDelayMs expect between 1ms and 30s, but get "
                                   + std::to_string(holdMs) + "!");
    }
    holdMs_.store(holdMs);
}

void ProduceAccumulator::setBatchMaxBytes(int32_t holdSize) {
    if (holdSize <= 0 || holdSize > 2 * 1024 * 1024) {
        throw std::invalid_argument("batchMaxBytes expect between 1B and 2MB, but get "
                                   + std::to_string(holdSize) + "!");
    }
    holdSize_.store(holdSize);
}

void ProduceAccumulator::setTotalBatchMaxBytes(int32_t totalHoldSize) {
    if (totalHoldSize <= 0) {
        throw std::invalid_argument("totalBatchMaxBytes must bigger then 0, but get "
                                   + std::to_string(totalHoldSize) + "!");
    }
    totalHoldSize_.store(totalHoldSize);
}

// ---------------- 全局字节闸门 ----------------
bool ProduceAccumulator::tryAddMessage(const Message& message) {
    std::lock_guard<std::mutex> lk(holdMutex_);
    if (currentlyHoldSize_ >= totalHoldSize_.load()) {
        return false;
    }
    const int64_t bodySize = static_cast<int64_t>(message.getBody().size());
    if (bodySize > 0) {
        currentlyHoldSize_ += bodySize;
    }
    return true;
}

void ProduceAccumulator::releaseHold(int64_t size) {
    std::lock_guard<std::mutex> lk(holdMutex_);
    currentlyHoldSize_ -= size;
}

int64_t ProduceAccumulator::currentlyHoldSize() const {
    std::lock_guard<std::mutex> lk(holdMutex_);
    return currentlyHoldSize_;
}

// ---------------- 表 ----------------
// Java 是 `get` → `putIfAbsent` → 返回 previous。这里一把锁里做完，等价且更强。
std::shared_ptr<MessageAccumulation> ProduceAccumulator::getOrCreateSyncBatch(
    const AggregateKey& key) {
    std::lock_guard<std::mutex> lk(syncTableMutex_);
    auto it = syncBatches_.find(key);
    if (it != syncBatches_.end()) {
        return it->second;
    }
    auto created = std::make_shared<MessageAccumulation>(key, this);
    syncBatches_[key] = created;
    return created;
}

std::shared_ptr<MessageAccumulation> ProduceAccumulator::getOrCreateAsyncBatch(
    const AggregateKey& key) {
    std::lock_guard<std::mutex> lk(asyncTableMutex_);
    auto it = asyncBatches_.find(key);
    if (it != asyncBatches_.end()) {
        return it->second;
    }
    auto created = std::make_shared<MessageAccumulation>(key, this);
    asyncBatches_[key] = created;
    return created;
}

// Java `map.remove(key, value)`：**只有当前映射仍指向这个 value** 才摘 ——
// 否则会把「别人刚换上去的新批次」误摘掉。cpp 的 map 没有条件删除，手写这段判断。
void ProduceAccumulator::removeSyncBatch(const AggregateKey& key,
                                         const std::shared_ptr<MessageAccumulation>& batch) {
    std::lock_guard<std::mutex> lk(syncTableMutex_);
    auto it = syncBatches_.find(key);
    if (it != syncBatches_.end() && it->second == batch) {
        syncBatches_.erase(it);
    }
}

void ProduceAccumulator::removeAsyncBatch(const AggregateKey& key,
                                          const std::shared_ptr<MessageAccumulation>& batch) {
    std::lock_guard<std::mutex> lk(asyncTableMutex_);
    auto it = asyncBatches_.find(key);
    if (it != asyncBatches_.end() && it->second == batch) {
        asyncBatches_.erase(it);
    }
}

// ---------------- 归并入口 ----------------
// Java 的 `while (true)` 重试环：拿到一个**已关闭**的批次（发过头 / 被守卫摘表）时，
// `add` 返回 -1 / false，调用方把它从表里摘掉再重取一个。
SendResult ProduceAccumulator::send(const Message& msg) {
    const AggregateKey partitionKey = AggregateKey::ofMessage(msg);
    while (true) {
        auto batch = getOrCreateSyncBatch(partitionKey);
        const int64_t index = batch->add(msg);
        if (index == -1) {
            removeSyncBatch(partitionKey, batch);
            continue;
        }
        const std::vector<SendResult> results = batch->sendResults();
        if (static_cast<size_t>(index) >= results.size()) {
            // Java 这里是 `batch.sendResults[index]` 裸数组访问 —— 越界抛
            // ArrayIndexOutOfBoundsException。可达性：batch 里只有**空 body** 消息时
            // messagesSize 恒为 0，守卫线程那支 `messagesSize == 0` 会把批次直接置 closed
            // 摘表，被卡在 `wait()` 里的 `add` 于是醒来、跳过循环、返回一个合法下标，而
            // 这一批从来没发过 → sendResults 为空。上游同样会抛（只是异常类型不同）。
            throw MQClientException("sendResult is illegal");
        }
        return results[static_cast<size_t>(index)];
    }
}

SendResult ProduceAccumulator::sendWithMq(const Message& msg, const MessageQueue& mq) {
    const AggregateKey partitionKey = AggregateKey::ofMessageWithMq(msg, mq);
    while (true) {
        auto batch = getOrCreateSyncBatch(partitionKey);
        const int64_t index = batch->add(msg);
        if (index == -1) {
            removeSyncBatch(partitionKey, batch);
            continue;
        }
        const std::vector<SendResult> results = batch->sendResults();
        if (static_cast<size_t>(index) >= results.size()) {
            throw MQClientException("sendResult is illegal");
        }
        return results[static_cast<size_t>(index)];
    }
}

// ⚠ 每次循环传的是 `callback` 的**副本**：`add` 会把它存进本批的回调表，下一轮还要用同一个。
void ProduceAccumulator::sendAsync(const Message& msg, std::shared_ptr<SendCallback> callback) {
    const AggregateKey partitionKey = AggregateKey::ofMessage(msg);
    while (true) {
        auto batch = getOrCreateAsyncBatch(partitionKey);
        if (!batch->add(msg, callback)) {
            removeAsyncBatch(partitionKey, batch);
            continue;
        }
        return;
    }
}

void ProduceAccumulator::sendAsyncWithMq(const Message& msg, const MessageQueue& mq,
                                         std::shared_ptr<SendCallback> callback) {
    const AggregateKey partitionKey = AggregateKey::ofMessageWithMq(msg, mq);
    while (true) {
        auto batch = getOrCreateAsyncBatch(partitionKey);
        if (!batch->add(msg, callback)) {
            removeAsyncBatch(partitionKey, batch);
            continue;
        }
        return;
    }
}

// ---------------- 守卫线程 ----------------
// Java `GuardForSyncSendService.doWork()`：
//   for v in syncSendBatchs.values():
//       v.wakeup()                       // 叫醒正在 add 里等阈值的调用方去自查 readyToSend
//       synchronized(v) { synchronized(v.closed) {
//           if (v.messagesSize == 0) { v.closed.set(true); syncSendBatchs.remove(k, v); }
//           else { v.notify(); } } }
//   Thread.sleep(max(1, holdMs/2))
//
// Java `GuardForAsyncSendService.doWork()`：
//   for v in asyncSendBatchs.values():
//       if (v.readyToSend()) v.send(null)
//       synchronized(v.closed) { if (v.messagesSize == 0) { v.closed.set(true); remove } }
//   Thread.sleep(max(1, holdMs/2))
//
// ⚠ 发完的批次 `messagesSize` 仍 > 0（`send` 只置 closed，不清 size），所以它**留在表里**，
// 直到下一次同键 `send` 拿到它、`add` 返回 -1 / false 才被摘掉重取 —— 不是守卫清的。
void ProduceAccumulator::guardOnce(bool sync) {
    // 先抄一份快照：Java 遍历 ConcurrentHashMap.values() 时允许并发 remove，cpp 的
    // std::map 不行（迭代器失效）。快照里可能包含已经不在表里的批次，那也没关系 ——
    // 下面的"摘表"判断是条件删除（值相等才删），而 wakeup / notify 对已关闭批次是无害的。
    std::vector<std::shared_ptr<MessageAccumulation>> batches;
    if (sync) {
        std::lock_guard<std::mutex> lk(syncTableMutex_);
        batches.reserve(syncBatches_.size());
        for (const auto& kv : syncBatches_) {
            batches.push_back(kv.second);
        }
    } else {
        std::lock_guard<std::mutex> lk(asyncTableMutex_);
        batches.reserve(asyncBatches_.size());
        for (const auto& kv : asyncBatches_) {
            batches.push_back(kv.second);
        }
    }

    for (const auto& batch : batches) {
        if (sync) {
            batch->wakeup();
            // Java 的 `synchronized (v)` —— 就是 add 里 wait/notify 的那把监视器
            std::lock_guard<std::mutex> lk(batch->waitMutex_);
            // Java 的 `synchronized (v.closed)` —— 对应这里的 stateMutex_
            if (batch->messagesSize() == 0) {
                batch->markClosed();
                removeSyncBatch(batch->aggregateKey(), batch);
            } else {
                batch->waitCv_.notify_all();
            }
        } else {
            if (batch->readyToSend()) {
                batch->sendAsyncNow();
            }
            if (batch->messagesSize() == 0) {
                batch->markClosed();
                removeAsyncBatch(batch->aggregateKey(), batch);
            }
        }
    }
}

void ProduceAccumulator::guardLoop(bool sync, const std::string& name) {
    logger_info(name + " service started");
    while (!stopped_.load()) {
        try {
            guardOnce(sync);
        } catch (const std::exception& e) {
            logger_warn(name + " service has exception. " + e.what());
        } catch (...) {
            logger_warn(name + " service has exception.");
        }
        if (stopped_.load()) {
            break;
        }
        // Java `Thread.sleep(Math.max(1, holdMs / 2))`：睡在 doWork 的**末尾**，
        // 所以线程起来后的第一轮是立即跑的。
        const int64_t ms = std::max<int64_t>(1, holdMs() / 2);
        std::this_thread::sleep_for(std::chrono::milliseconds(ms));
    }
    logger_info(name + " service end");
}

// ---------------- sender 生命周期 ----------------
void ProduceAccumulator::detachSender() {
    std::lock_guard<std::mutex> lk(senderMutex_);
    sender_ = nullptr;
}

void ProduceAccumulator::attachSender(AccumulatorSender* sender) {
    std::lock_guard<std::mutex> lk(senderMutex_);
    // 只在"上一任已 detach"时重绑：还活着的那位继续用它（Java 是"第一次记下的那个
    // 用到天荒地老"，同 clientId 的第二个生产者会被忽略）。
    if (sender_ == nullptr) {
        sender_ = sender;
    }
}

bool ProduceAccumulator::hasSender() const {
    std::lock_guard<std::mutex> lk(senderMutex_);
    return sender_ != nullptr;
}

// ---------------- 取证 / 测试接缝 ----------------
size_t ProduceAccumulator::syncBatchCount() const {
    std::lock_guard<std::mutex> lk(syncTableMutex_);
    return syncBatches_.size();
}

size_t ProduceAccumulator::asyncBatchCount() const {
    std::lock_guard<std::mutex> lk(asyncTableMutex_);
    return asyncBatches_.size();
}

std::vector<std::shared_ptr<MessageAccumulation>> ProduceAccumulator::syncBatchesSnapshot() const {
    std::lock_guard<std::mutex> lk(syncTableMutex_);
    std::vector<std::shared_ptr<MessageAccumulation>> out;
    out.reserve(syncBatches_.size());
    for (const auto& kv : syncBatches_) {
        out.push_back(kv.second);
    }
    return out;
}

std::vector<std::shared_ptr<MessageAccumulation>> ProduceAccumulator::asyncBatchesSnapshot() const {
    std::lock_guard<std::mutex> lk(asyncTableMutex_);
    std::vector<std::shared_ptr<MessageAccumulation>> out;
    out.reserve(asyncBatches_.size());
    for (const auto& kv : asyncBatches_) {
        out.push_back(kv.second);
    }
    return out;
}

std::shared_ptr<MessageAccumulation> ProduceAccumulator::putEmptySyncBatch(const AggregateKey& key) {
    std::lock_guard<std::mutex> lk(syncTableMutex_);
    auto created = std::make_shared<MessageAccumulation>(key, this);
    syncBatches_[key] = created;
    return created;
}

void ProduceAccumulator::runGuardOnce(bool sync) { guardOnce(sync); }

// ---------------------------------------------------------------- 进程级注册表

std::shared_ptr<ProduceAccumulator> getOrCreateProduceAccumulator(const std::string& clientId,
                                                                 AccumulatorSender* sender) {
    std::lock_guard<std::mutex> lk(registryMutex());
    auto& table = registry();
    auto it = table.find(clientId);
    if (it == table.end()) {
        auto created = std::make_shared<ProduceAccumulator>(clientId, sender);
        table[clientId] = created;
        return created;
    }
    // 与 Java 的唯一差异（见头文件）：上一任生产者已经 detach（析构）时重绑到本次的生产者。
    // sender 还活着时 `attachSender` 是空操作，与 Java 的"只认第一个"一致。
    it->second->attachSender(sender);
    return it->second;
}

}  // namespace rocketmq
