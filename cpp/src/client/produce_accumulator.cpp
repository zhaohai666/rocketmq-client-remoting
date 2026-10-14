// ProduceAccumulator / MessageAccumulation / AggregateKey implementation.
// The contract and the deliberate behavioural choices are documented in the produce_accumulator.h file header.
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

// ---------------------------------------------------------------- Small utilities

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

void splitKeysInto(std::set<std::string>& target, const std::string& keys) {
    for (const std::string& p : split(keys, MessageConst::KEY_SEPARATOR[0])) {
        target.insert(p);
    }
}

void ensureUniqId(Message& msg) {
    std::string uniq = msg.getProperty(MessageConst::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX);
    if (uniq.empty()) {
        uniq = InnerIdGenerator::createUniqId();
        msg.putProperty(MessageConst::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX, uniq);
    }
}

// Why the callback is not given a `MessageAccumulation*` directly: `finish()` is private and all this needs is
// "a completion function". Using `std::function` lets `sendAsyncNow()` build a closure inside a member
// function, so no extra friend declaration is needed for cross-class access.
class AccumulationCallback : public SendCallback {
public:
    using Reducer = std::function<void(const SendResult*, const std::exception_ptr&)>;

    explicit AccumulationCallback(Reducer reducer) : reducer_(std::move(reducer)) {}

    void onSuccess(const SendResult& sendResult) override { reducer_(&sendResult, nullptr); }
    void onException(const std::exception_ptr& error) override { reducer_(nullptr, error); }

private:
    Reducer reducer_;
};

// Process-wide reuse table (see the comment on getOrCreateProduceAccumulator in the header).
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
        auto it = msg.properties.find(MessageConst::PROPERTY_KEYS);
        if (it != msg.properties.end()) {
            splitKeysInto(keys_, it->second);
        }
        messages_.push_back(msg);
    }
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
    if (sendResult.msgId.find(',') != std::string::npos) {
        const std::vector<std::string> ids = split(sendResult.msgId, ',');
        const std::vector<std::string> offsets = split(sendResult.offsetMsgId, ',');
        if (ids.size() != total || offsets.size() != total) {
            throw MQClientException("sendResult is illegal");
        }
        for (size_t i = 0; i < total; ++i) {
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
        results.assign(total, sendResult);
    }
    std::lock_guard<std::mutex> lk(stateMutex_);
    sendResults_ = std::move(results);
}

void MessageAccumulation::releaseHold() {
    owner_->releaseHold(messagesSize());
}

void MessageAccumulation::sendSync() {
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
                callbacks[i]->onException(std::make_exception_ptr(
                    MQClientException("sendResult is illegal")));
            }
        }
        return;
    }
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
        notifyCallbacksError(std::make_exception_ptr(
            MQClientException("defaultMQProducer is null, can not send message")));
        return;
    }
    // Completion receipt of a batch send: split per message and deliver in order to every user callback collected for the batch, then release the byte quota.
    auto reducer = std::make_shared<AccumulationCallback>(
        [this](const SendResult* r, const std::exception_ptr& e) { finish(r, e); });
    try {
        sender->sendDirectAsync(batch, key_.hasMq ? &key_.mq : nullptr, reducer);
    } catch (...) {
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
        // The destructor must not throw
    }
}

void ProduceAccumulator::start() {
    std::lock_guard<std::mutex> lk(guardMutex_);
    if (syncGuard_.joinable() || asyncGuard_.joinable()) {
        return;
    }
    stopped_.store(false);
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
        // Idempotent: when not started, force the flag to "stopped" (`start()` will flip it back)
        stopped_.store(true);
        syncGuard = std::move(syncGuard_);
        asyncGuard = std::move(asyncGuard_);
    }
    // Join outside the lock (the guard loop never touches guardMutex_, but it should not hold it while joining)
    if (syncGuard.joinable()) {
        syncGuard.join();
    }
    if (asyncGuard.joinable()) {
        asyncGuard.join();
    }
}

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

// ---------------- Global byte gate ----------------
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

// ⚠ Each iteration passes a **copy** of `callback`: `add` stores it in this batch's callback table, and the next round needs the original.
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

// ⚠ A sent batch still has `messagesSize` > 0 (`send` only marks closed, it does not clear size), so it **stays in the table**
// until the next `send` on the same key picks it up and `add` returns -1 / false, at which point it is dropped and re-created -- not by the guard.
void ProduceAccumulator::guardOnce(bool sync) {
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
            std::lock_guard<std::mutex> lk(batch->waitMutex_);
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
        const int64_t ms = std::max<int64_t>(1, holdMs() / 2);
        std::this_thread::sleep_for(std::chrono::milliseconds(ms));
    }
    logger_info(name + " service end");
}

// ---------------- sender lifecycle ----------------
void ProduceAccumulator::detachSender() {
    std::lock_guard<std::mutex> lk(senderMutex_);
    sender_ = nullptr;
}

void ProduceAccumulator::attachSender(AccumulatorSender* sender) {
    std::lock_guard<std::mutex> lk(senderMutex_);
    if (sender_ == nullptr) {
        sender_ = sender;
    }
}

bool ProduceAccumulator::hasSender() const {
    std::lock_guard<std::mutex> lk(senderMutex_);
    return sender_ != nullptr;
}

// ---------------- Diagnostics / test seams ----------------
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

// ---------------------------------------------------------------- Process-wide registry

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
    it->second->attachSender(sender);
    return it->second;
}

}  // namespace rocketmq
