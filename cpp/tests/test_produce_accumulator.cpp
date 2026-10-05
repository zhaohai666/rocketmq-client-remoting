// 生产者自动攒批单测（对应 Java `org.apache.rocketmq.client.producer.ProduceAccumulator`，
// 526 行）—— 不需要集群，全部对着一个「只记账、按剧本回结果」的假生产者跑。
//
// 与 python/tests/test_produce_accumulator.py（18 例）、
// csharp/tests/RocketMQ.Client.Tests/ProduceAccumulatorTests.cs（17 例）、
// rust/src/client/produce_accumulator/tests.rs（16 例）同题。
//
// 覆盖 Java `ProduceAccumulatorTest` 的三个场景（同步 / 异步 / 指定 MessageQueue），另补：
//   * 三档参数校验的边界值与 Java 原文案；
//   * `tryAddMessage` 全局字节闸门（放行即记账 / 归还 / 拒绝后调用方退回直发）；
//   * 批量应答**拆条**（逗号分隔 msgId/offsetMsgId → 每条各自的结果、queueOffset 递增）；
//   * `AggregateKey` 四维分区（topic / mq / waitStoreMsgOK / tag）；
//   * 批级四组属性（KEYS 空格并集、TAGS、WAIT）与「同步收集 keys、异步不收集」的不对称；
//   * 守卫线程对「空批次」的清理、对「已发完但 size 仍 > 0」批次的**保留**；
//   * 同步 / 异步失败路径（异常上抛 + 额度归还；回调各拿一份）；
//   * `start → shutdown → start`（守卫线程可重建）；
//   * sender 生命周期（cpp 结构性差异）：detach 后的同步 / 异步行为与 Java 的两条漏还路径。
//
// ⚠ 与 Java 单测一样，这里**直接调累加器**的 `send` / `sendAsync`，绕过了
// `DefaultMQProducer::sendByAccumulator` 里的 `ensureUniqId` —— 所以子消息都没有 UNIQ_KEY，
// `MessageBatch::encode()` 出来的 body 才与 `referenceBatchBody()` 可比
// （`generateFromList` 本身**不**打 ID）。批级 KEYS / TAGS 不进 body，所以 `buildBatch`
// 多写这两个属性不影响 body 相等。
//
// ⚠ 和 Java 一样，`sendSync` 会把 `currentlyHoldSize` 扣掉，而真实调用链里是
// `canBatch` → `tryAddMessage` 先记的账。这里直接调累加器，所以每条消息都得先自己补一次
// `tryAddMessage`，否则归还后额度会变成负数。
#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include "rocketmq/client/exception.h"
#include "rocketmq/client/produce_accumulator.h"
#include "rocketmq/client/result.h"
#include "rocketmq/common/message.h"
#include "rocketmq/common/message_const.h"

using namespace rocketmq;

namespace {

const char* const TOPIC = "AccumTestTopic";

int fails = 0;
int checks = 0;

void expect(bool ok, const std::string& name, const std::string& detail = "") {
    ++checks;
    if (!ok) {
        ++fails;
        std::printf("FAIL %s %s\n", name.c_str(), detail.c_str());
    }
}

void expectInt(long long actual, long long expected, const std::string& name) {
    ++checks;
    if (actual != expected) {
        ++fails;
        std::printf("FAIL %s (actual=%lld expected=%lld)\n", name.c_str(), actual, expected);
    }
}

void expectStr(const std::string& actual, const std::string& expected, const std::string& name) {
    ++checks;
    if (actual != expected) {
        ++fails;
        std::printf("FAIL %s (actual=\"%s\" expected=\"%s\")\n", name.c_str(), actual.c_str(),
                    expected.c_str());
    }
}

// 把 exception_ptr 变成文案（回调只能拿到 exception_ptr，断言要比内容）
std::string textOf(const std::exception_ptr& e) {
    if (e == nullptr) return "<null>";
    try {
        std::rethrow_exception(e);
    } catch (const std::exception& ex) {
        return ex.what();
    } catch (...) {
        return "<non-std exception>";
    }
}

bool waitUntil(const std::function<bool()>& pred, int64_t timeoutMillis) {
    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::milliseconds(timeoutMillis);
    while (std::chrono::steady_clock::now() < deadline) {
        if (pred()) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    return pred();
}

// ================================================================ 夹具

// 累加器发出去的一批：批量对象 + 目标队列 + 这次是不是异步（带回调）。
struct BatchSlot {
    MessageBatch batch;
    bool hasMq = false;
    MessageQueue mq;
    bool hasCallback = false;
};

// 假的生产者（Java 单测里的 `MockMQProducer` 同款）：只记账 + 按剧本回结果。
class FakeSender : public AccumulatorSender {
public:
    void setResult(const SendResult& result) {
        std::lock_guard<std::mutex> lk(mutex_);
        result_ = result;
        hasResult_ = true;
    }

    // 设了之后：同步入口抛异常、异步入口把异常交给回调（两处都对齐 Java 的失败语义）
    void setError(const std::string& message) {
        std::lock_guard<std::mutex> lk(mutex_);
        error_ = message;
    }

    // 异步入口**就地抛**（Java `send(cb)` 外层 catch 那一支：回调拿到异常但**不还额度**）
    void setThrowOnAsync(bool value) { throwOnAsync_ = value; }

    SendResult sendDirectBlocking(const MessageBatch& batch, const MessageQueue* mq) override {
        record(batch, mq, false);
        const std::string error = readError();
        if (!error.empty()) {
            throw MQClientException(error);
        }
        return resolve(mq);
    }

    void sendDirectAsync(const MessageBatch& batch, const MessageQueue* mq,
                         std::shared_ptr<SendCallback> callback) override {
        record(batch, mq, true);
        if (throwOnAsync_.load()) {
            throw MQClientException("async throw");
        }
        const std::string error = readError();
        if (!error.empty()) {
            callback->onException(std::make_exception_ptr(MQClientException(error)));
            return;
        }
        callback->onSuccess(resolve(mq));
    }

    size_t sentCount() const {
        std::lock_guard<std::mutex> lk(mutex_);
        return sent_.size();
    }

    size_t calls() const {
        std::lock_guard<std::mutex> lk(mutex_);
        return calls_;
    }

    BatchSlot at(size_t index) const {
        std::lock_guard<std::mutex> lk(mutex_);
        return sent_.at(index);
    }

private:
    void record(const MessageBatch& batch, const MessageQueue* mq, bool hasCallback) {
        std::lock_guard<std::mutex> lk(mutex_);
        ++calls_;
        BatchSlot slot;
        slot.batch = batch;
        slot.hasMq = mq != nullptr;
        if (mq != nullptr) slot.mq = *mq;
        slot.hasCallback = hasCallback;
        sent_.push_back(std::move(slot));
    }

    std::string readError() const {
        std::lock_guard<std::mutex> lk(mutex_);
        return error_;
    }

    // 未指定结果时的兜底：`SEND_OK` + `123`（与 Python 的 `FakeProducer` 一致）
    SendResult resolve(const MessageQueue* mq) const {
        std::lock_guard<std::mutex> lk(mutex_);
        if (hasResult_) return result_;
        SendResult result;
        result.sendStatus = SendStatus::SEND_OK;
        result.msgId = "123";
        if (mq != nullptr) result.messageQueue = *mq;
        return result;
    }

    mutable std::mutex mutex_;
    std::vector<BatchSlot> sent_;
    SendResult result_;
    bool hasResult_ = false;
    std::string error_;
    size_t calls_ = 0;
    std::atomic<bool> throwOnAsync_{false};
};

// 收集型回调：把每次交付都记下来（用来断言「恰好一次」与内容）。
class CollectingCallback : public SendCallback {
public:
    void onSuccess(const SendResult& result) override {
        std::lock_guard<std::mutex> lk(mutex_);
        ++success_;
        msgIds_.push_back(result.msgId);
        offsets_.push_back(result.queueOffset);
    }

    void onException(const std::exception_ptr& error) override {
        std::lock_guard<std::mutex> lk(mutex_);
        ++errors_;
        errorTexts_.push_back(textOf(error));
    }

    int total() const {
        std::lock_guard<std::mutex> lk(mutex_);
        return success_ + errors_;
    }
    int successCount() const {
        std::lock_guard<std::mutex> lk(mutex_);
        return success_;
    }
    int errorCount() const {
        std::lock_guard<std::mutex> lk(mutex_);
        return errors_;
    }
    std::vector<std::string> msgIds() const {
        std::lock_guard<std::mutex> lk(mutex_);
        return msgIds_;
    }
    std::vector<int64_t> offsets() const {
        std::lock_guard<std::mutex> lk(mutex_);
        return offsets_;
    }
    std::vector<std::string> errorTexts() const {
        std::lock_guard<std::mutex> lk(mutex_);
        return errorTexts_;
    }

private:
    mutable std::mutex mutex_;
    int success_ = 0;
    int errors_ = 0;
    std::vector<std::string> msgIds_;
    std::vector<int64_t> offsets_;
    std::vector<std::string> errorTexts_;
};

std::shared_ptr<SendCallback> asCallback(const std::shared_ptr<CollectingCallback>& cb) {
    return std::static_pointer_cast<SendCallback>(cb);
}

// 与 Java / Python / Rust 单测同款 body（1 / 2 / 3 / 4 / 5 字节）
std::vector<Message> makeMessages(size_t n) {
    std::vector<Message> out;
    out.reserve(n);
    for (size_t i = 0; i < n; ++i) {
        out.push_back(Message(TOPIC, std::string(i + 1, '1')));
    }
    return out;
}

// 对照 body：走 `generateFromList`（与累加器 `buildBatch` 的编码口径同源）
std::string referenceBatchBody(const std::vector<Message>& messages) {
    return MessageBatch::generateFromList(messages).encode();
}

// ================================================================ 线程助手

struct Outcome {
    bool ok = false;
    SendResult result;
    std::string error;
};

struct SyncCall {
    std::thread thread;
    std::shared_ptr<Outcome> outcome;
};

void joinCall(SyncCall& call) {
    if (call.thread.joinable()) call.thread.join();
}

// 起一个线程走一次同步 `send`（真实调用链里调用方线程就是这样阻塞在 `add` 里的）。
SyncCall spawnSyncSend(ProduceAccumulator& acc, const Message& msg, const MessageQueue* mq) {
    auto outcome = std::make_shared<Outcome>();
    std::thread thread([&acc, msg, mq, outcome]() {
        try {
            outcome->result = mq == nullptr ? acc.send(msg) : acc.sendWithMq(msg, *mq);
            outcome->ok = true;
        } catch (const std::exception& e) {
            outcome->error = e.what();
        } catch (...) {
            outcome->error = "unknown exception";
        }
    });
    return SyncCall{std::move(thread), outcome};
}

SyncCall spawnSyncSend(ProduceAccumulator& acc, const Message& msg) {
    return spawnSyncSend(acc, msg, nullptr);
}

// 等某个同步批次攒够 `count` 条（`filter` 用来在表里挑出指定的那一批）
std::shared_ptr<MessageAccumulation> waitForBatch(
    ProduceAccumulator& acc, int64_t count,
    const std::function<bool(const std::shared_ptr<MessageAccumulation>&)>& filter) {
    const auto matches = [&]() {
        for (const auto& batch : acc.syncBatchesSnapshot()) {
            if (batch->count() == count && filter(batch)) return true;
        }
        return false;
    };
    if (!waitUntil(matches, 3000)) return nullptr;
    for (const auto& batch : acc.syncBatchesSnapshot()) {
        if (batch->count() == count && filter(batch)) return batch;
    }
    return nullptr;
}

std::shared_ptr<MessageAccumulation> waitForBatch(ProduceAccumulator& acc, int64_t count) {
    return waitForBatch(acc, count, [](const std::shared_ptr<MessageAccumulation>&) { return true; });
}

// ================================================================ 参数

void testDefaultParamsMatchJava() {
    FakeSender sender;
    ProduceAccumulator acc("cpp-accum-params", &sender);
    expectInt(acc.getBatchMaxDelayMs(), PRODUCE_ACCUMULATOR_DEFAULT_HOLD_MS, "default holdMs const");
    expectInt(acc.getBatchMaxBytes(), PRODUCE_ACCUMULATOR_DEFAULT_HOLD_SIZE, "default holdSize const");
    expectInt(acc.totalHoldSize(), PRODUCE_ACCUMULATOR_DEFAULT_TOTAL_HOLD_SIZE,
              "default totalHoldSize const");
    // Java 的 getTotalBatchMaxBytes 实际返回 holdSize（上游笔误，照抄）
    expectInt(acc.getTotalBatchMaxBytes(), PRODUCE_ACCUMULATOR_DEFAULT_HOLD_SIZE,
              "getTotalBatchMaxBytes copies the upstream bug (returns holdSize)");
    expectInt(acc.currentlyHoldSize(), 0, "currentlyHoldSize starts at 0");
    expectInt(acc.getBatchMaxDelayMs(), 10, "default holdMs is 10ms");
    expectInt(acc.getBatchMaxBytes(), 32 * 1024, "default holdSize is 32KB");
    expectInt(acc.totalHoldSize(), 32 * 1024 * 1024, "default totalHoldSize is 32MB");
    expect(acc.instanceName() == "cpp-accum-params", "instanceName kept");
}

void testParamGuardsCopyJavaRanges() {
    FakeSender sender;
    ProduceAccumulator acc("cpp-accum-guards", &sender);

    const auto accepts = [&](const std::function<void()>& fn) {
        try {
            fn();
            return true;
        } catch (const std::exception&) {
            return false;
        }
    };

    expect(accepts([&] { acc.setBatchMaxDelayMs(1); }), "holdMs 1 accepted");
    expect(accepts([&] { acc.setBatchMaxDelayMs(30 * 1000); }), "holdMs 30s accepted");
    expect(!accepts([&] { acc.setBatchMaxDelayMs(0); }), "holdMs 0 rejected");
    expect(!accepts([&] { acc.setBatchMaxDelayMs(30 * 1000 + 1); }), "holdMs 30s+1 rejected");

    expect(accepts([&] { acc.setBatchMaxBytes(1); }), "holdSize 1 accepted");
    expect(accepts([&] { acc.setBatchMaxBytes(2 * 1024 * 1024); }), "holdSize 2MB accepted");
    expect(!accepts([&] { acc.setBatchMaxBytes(0); }), "holdSize 0 rejected");
    expect(!accepts([&] { acc.setBatchMaxBytes(2 * 1024 * 1024 + 1); }), "holdSize 2MB+1 rejected");

    expect(accepts([&] { acc.setTotalBatchMaxBytes(1); }), "totalHoldSize 1 accepted");
    expect(!accepts([&] { acc.setTotalBatchMaxBytes(0); }), "totalHoldSize 0 rejected");

    // 文案逐字对齐 Java（Python / C# / Rust 三侧的断言同样比文案）
    std::string text;
    try {
        acc.setBatchMaxDelayMs(0);
    } catch (const std::exception& e) {
        text = e.what();
    }
    expectStr(text, "batchMaxDelayMs expect between 1ms and 30s, but get 0!", "holdMs error text");
    text.clear();
    try {
        acc.setBatchMaxBytes(0);
    } catch (const std::exception& e) {
        text = e.what();
    }
    expectStr(text, "batchMaxBytes expect between 1B and 2MB, but get 0!", "holdSize error text");
    text.clear();
    try {
        acc.setTotalBatchMaxBytes(0);
    } catch (const std::exception& e) {
        text = e.what();
    }
    expectStr(text, "totalBatchMaxBytes must bigger then 0, but get 0!", "totalHoldSize error text");

    // 越界的值**不**落地（Java 先校验再赋值）
    acc.setBatchMaxDelayMs(PRODUCE_ACCUMULATOR_DEFAULT_HOLD_MS);
    acc.setBatchMaxBytes(PRODUCE_ACCUMULATOR_DEFAULT_HOLD_SIZE);
    expect(!accepts([&] { acc.setBatchMaxDelayMs(31 * 1000); }), "holdMs 31s rejected again");
    expect(!accepts([&] { acc.setBatchMaxBytes(4096 * 1024); }), "holdSize 4MB rejected again");
    expectInt(acc.getBatchMaxDelayMs(), PRODUCE_ACCUMULATOR_DEFAULT_HOLD_MS,
              "rejected holdMs does not mutate");
    expectInt(acc.getBatchMaxBytes(), PRODUCE_ACCUMULATOR_DEFAULT_HOLD_SIZE,
              "rejected holdSize does not mutate");
}

void testRegistryReusesAccumulatorByClientId() {
    // Java `MQClientManager.getOrCreateProduceAccumulator`：按 clientId 复用 ——
    // 第二个 sender 被忽略，两边共享同一份阈值（以及同一对守卫线程）。
    FakeSender first;
    FakeSender second;
    auto a = getOrCreateProduceAccumulator("cpp-accum-shared", &first);
    auto b = getOrCreateProduceAccumulator("cpp-accum-shared", &second);
    expect(a.get() == b.get(), "same clientId returns the same accumulator instance");
    expect(a->hasSender(), "accumulator bound to the first sender");

    a->setBatchMaxDelayMs(1234);
    expectInt(b->getBatchMaxDelayMs(), 1234, "params shared through the registry");
    a->setBatchMaxDelayMs(PRODUCE_ACCUMULATOR_DEFAULT_HOLD_MS);

    // sender 还活着时 `attachSender` 是空操作（Java 只认第一个）
    expect(a->hasSender(), "sender still attached after a second getOrCreate");
}

// ================================================================ 全局字节闸门

void testTryAddMessageGateAndRelease() {
    FakeSender sender;
    ProduceAccumulator acc("cpp-accum-gate", &sender);
    acc.setTotalBatchMaxBytes(10);
    const Message msg(TOPIC, std::string(10, 'a'));  // 10 字节

    expect(acc.tryAddMessage(msg), "first message passes the gate");
    expectInt(acc.currentlyHoldSize(), 10, "gate records the body size");
    // 额度已满（Java：`currentlyHoldSize < totalHoldSize` 才放行）
    expect(!acc.tryAddMessage(Message(TOPIC, std::string(1, 'b'))), "gate rejects when full");
    expectInt(acc.currentlyHoldSize(), 10, "rejected message does not change the hold size");
    acc.releaseHold(10);
    expectInt(acc.currentlyHoldSize(), 0, "releaseHold refunds");
    expect(acc.tryAddMessage(Message(TOPIC, std::string(1, 'b'))), "gate passes again after refund");

    // 空 body 不记账但仍然放行
    acc.releaseHold(1);
    acc.setTotalBatchMaxBytes(5);
    expect(acc.tryAddMessage(Message(TOPIC, std::string())), "empty body always passes");
    expectInt(acc.currentlyHoldSize(), 0, "empty body is not accounted");
}

// ================================================================ 归并键

void testAggregateKeyPartitionsByTopicMqWaitAndTag() {
    const Message msg(TOPIC, std::string(1, 'x'));
    const AggregateKey base = AggregateKey::ofMessage(msg);
    expect(!base.hasTag, "plain message has no tag (null, not empty string)");
    expect(base.waitStoreMsgOk, "plain message is waitStoreMsgOK (Java isWaitStoreMsgOK default)");
    expect(!base.hasMq, "plain key has no pinned mq");
    expectStr(base.topic, TOPIC, "key topic");

    Message tagged(TOPIC, std::string(1, 'x'));
    tagged.setTags("TagA");
    const AggregateKey taggedKey = AggregateKey::ofMessage(tagged);
    expect(taggedKey.hasTag, "tagged key carries the tag");
    expectStr(taggedKey.tag, "TagA", "tag value");
    expect(!(base == taggedKey), "tag partitions the key");

    Message noWait(TOPIC, std::string(1, 'x'));
    noWait.setWaitStoreMsgOk(false);
    const AggregateKey noWaitKey = AggregateKey::ofMessage(noWait);
    expect(!noWaitKey.waitStoreMsgOk, "explicit WAIT=false is keyed false");
    expect(!(base == noWaitKey), "waitStoreMsgOK partitions the key");

    // 空串 tag 与「无 tag」不是一回事（Java 的 null）
    Message emptyTag(TOPIC, std::string(1, 'x'));
    emptyTag.setTags("");
    const AggregateKey emptyTagKey = AggregateKey::ofMessage(emptyTag);
    expect(emptyTagKey.hasTag, "explicit empty tag is still \"present\"");
    expect(!(base == emptyTagKey), "empty tag differs from a missing tag");

    const Message otherMsg("Other", std::string(1, 'x'));
    const AggregateKey otherTopic = AggregateKey::ofMessage(otherMsg);
    expect(!(base == otherTopic), "topic partitions the key");

    const MessageQueue mq(TOPIC, "broker-a", 0);
    const AggregateKey pinned = AggregateKey::ofMessageWithMq(msg, mq);
    expect(pinned.hasMq && pinned.mq == mq, "pinned key carries the queue");
    expect(!(base == pinned), "mq partitions the key");

    // 三个不同的键在表里必须互不覆盖（同步表是 keyed by AggregateKey）
    FakeSender sender;
    ProduceAccumulator acc("cpp-accum-keys-partition", &sender);
    acc.setBatchMaxDelayMs(3000);
    expect(acc.tryAddMessage(msg), "gate for plain");
    expect(acc.tryAddMessage(tagged), "gate for tagged");
    expect(acc.tryAddMessage(otherMsg), "gate for other topic");
    SyncCall calls[3] = {spawnSyncSend(acc, msg), spawnSyncSend(acc, tagged),
                         spawnSyncSend(acc, otherMsg)};
    const bool three = waitUntil([&]() { return acc.syncBatchCount() == 3; }, 3000);
    expect(three, "three distinct keys land in three distinct batches");
    // 三条各自成批 → 逐个手动触发发出去，等待的线程才会被放行
    const auto batches = acc.syncBatchesSnapshot();
    expectInt(static_cast<long long>(batches.size()), 3, "three batches in the table");
    for (const auto& batch : batches) {
        try {
            batch->forceSyncSend();
        } catch (const std::exception& e) {
            std::printf("  forceSyncSend failed: %s\n", e.what());
        }
    }
    for (auto& call : calls) {
        joinCall(call);
        expect(call.outcome->ok, "each partition sent independently", call.outcome->error);
    }
    expectInt(static_cast<long long>(sender.sentCount()), 3, "three batches were sent");
}

void testDifferentTagsDoNotMerge() {
    // tag 不同的两条消息必须落进**两个**批次（一个 MessageBatch 只有一个 TAGS 属性），
    // 而且各自独立发出去。
    FakeSender sender;
    ProduceAccumulator acc("cpp-accum-tags", &sender);
    acc.start();  // 用默认 holdMs(10ms)，让守卫线程把两个批次分别推出去

    auto send = [&acc](const std::string& tag) {
        auto outcome = std::make_shared<Outcome>();
        std::thread thread([&acc, tag, outcome]() {
            try {
                Message msg(TOPIC, std::string(tag.size(), 't'));
                msg.setTags(tag);
                outcome->result = acc.send(msg);
                outcome->ok = true;
            } catch (const std::exception& e) {
                outcome->error = e.what();
            }
        });
        return SyncCall{std::move(thread), outcome};
    };
    SyncCall a = send("TagA");
    SyncCall b = send("TagB");

    const bool both = waitUntil([&]() { return sender.sentCount() == 2; }, 5000);
    // 兜底：万一守卫线程没把两个批次都推出去，也要把等待中的调用方放行（`sendSync` 对
    // 已关闭的批次直接返回，所以这里重复调用是安全的）
    for (const auto& batch : acc.syncBatchesSnapshot()) {
        try {
            batch->forceSyncSend();
        } catch (const std::exception& e) {
            std::printf("  forceSyncSend failed: %s\n", e.what());
        }
    }
    acc.shutdown();
    joinCall(a);
    joinCall(b);
    expect(both, "two distinct tags must produce two distinct batches");
    expect(a.outcome->ok, "tagged send A returned a result");
    expect(b.outcome->ok, "tagged send B returned a result");

    std::set<std::string> tags;
    for (size_t i = 0; i < sender.sentCount(); ++i) {
        tags.insert(sender.at(i).batch.getProperty(MessageConst::PROPERTY_TAGS));
    }
    expectInt(static_cast<long long>(tags.size()), 2, "two batches carry two different TAGS");
    expect(tags.count("TagA") == 1 && tags.count("TagB") == 1, "TAGS values are TagA / TagB");
}

// ================================================================ 归并（同步）

void testSyncBatchMergesMessagesAndReturnsPerMessageResults() {
    // 5 条同键消息攒成 1 个 MessageBatch，各自拿到自己的拆条 SendResult
    FakeSender sender;
    SendResult scripted;
    scripted.sendStatus = SendStatus::SEND_OK;
    scripted.msgId = "id-0,id-1,id-2,id-3,id-4";
    scripted.offsetMsgId = "off-0,off-1,off-2,off-3,off-4";
    scripted.messageQueue = MessageQueue(TOPIC, "broker-a", 0);
    scripted.queueOffset = 100;
    sender.setResult(scripted);

    ProduceAccumulator acc("cpp-accum-sync-batch", &sender);
    // holdMs 拉长到 3s：保证 5 条都进同一批（用例手动触发发送，不等这 3s）
    acc.setBatchMaxDelayMs(3000);

    const std::vector<Message> messages = makeMessages(5);
    // 真实调用链里是 `canBatch` 先 `tryAddMessage` 记账，累加器发完再归还
    for (const Message& m : messages) {
        expect(acc.tryAddMessage(m), "gate accepts a sync message");
    }

    std::vector<std::unique_ptr<SyncCall>> calls;
    for (const Message& m : messages) {
        calls.push_back(std::make_unique<SyncCall>(spawnSyncSend(acc, m)));
    }

    auto batch = waitForBatch(acc, 5);
    expect(batch != nullptr, "5 messages land in one batch");
    if (batch == nullptr) {
        for (auto& call : calls) joinCall(*call);
        return;
    }
    expect(batch->keys().empty(), "sync batch collected no keys (messages had none)");
    expect(batch->messages().size() == 5, "batch holds 5 messages");

    // 手动触发（等价于守卫线程在 holdMs 到点后叫醒某一个等待者去发）
    bool sendOk = true;
    std::string sendError;
    try {
        batch->forceSyncSend();
    } catch (const std::exception& e) {
        sendOk = false;
        sendError = e.what();
    }
    for (auto& call : calls) joinCall(*call);
    expect(sendOk, "forced sync send ok", sendError);

    expectInt(static_cast<long long>(sender.sentCount()), 1, "exactly one batch was sent");
    expect(!sender.at(0).hasMq, "no pinned mq → producer picks the queue");
    expect(!sender.at(0).hasCallback, "the sync path sends without a callback");
    // 子消息集合与 reference 一致。⚠ 只比长度不比字节：5 条是**不同线程**并发 add 的，
    // 批内顺序不确定（Java 的同步用例也因此只断言长度，只有单线程的异步用例才全等比较）。
    expectInt(static_cast<long long>(sender.at(0).batch.body.size()),
              static_cast<long long>(referenceBatchBody(makeMessages(5)).size()),
              "batch body length matches MessageBatch::generateFromList");

    int okCount = 0;
    for (auto& call : calls) {
        expect(call->outcome->ok, "every waiter got its own SendResult", call->outcome->error);
        if (!call->outcome->ok) continue;
        ++okCount;
        const SendResult& result = call->outcome->result;
        // 拆条结果按**批内位置**下发，而位置由 add 的先后决定 —— 并发 add 下「哪条消息拿哪个
        // 下标」不确定（Java 单测同样只能断言"成套"）。所以验：同一条结果里 id-N ↔ off-N ↔
        // 100+N 必须成套（串了就说明拆条下标算错了）。
        const std::string& id = result.msgId;
        expect(id.size() == 4 && id.compare(0, 3, "id-") == 0, "msgId is id-N", id);
        const int position = id.size() == 4 ? id[3] - '0' : -1;
        expectStr(result.offsetMsgId,
                  position >= 0 ? "off-" + std::to_string(position) : std::string(),
                  "offsetMsgId pairs with msgId");
        expectInt(result.queueOffset, 100 + position, "queueOffset increments per message");
        expect(result.sendStatus == SendStatus::SEND_OK, "send status preserved");
        expect(result.messageQueue == scripted.messageQueue, "message queue preserved");
    }
    expectInt(okCount, 5, "all 5 callers returned a result");
    // 发完归还全局额度
    expectInt(acc.currentlyHoldSize(), 0, "hold size refunded after the batch is sent");
    // 发完的批次留在表里（Java 的真实行为：只置 closed，不清 messagesSize）
    expectInt(static_cast<long long>(acc.syncBatchCount()), 1, "sent batch stays in the table");
}

// ================================================================ 归并（异步）

void testAsyncBatchMergesAndFiresEveryCallback() {
    // 异步：5 条同键消息由守卫线程攒成 1 批，回调各自拿到拆条结果
    FakeSender sender;
    SendResult scripted;
    scripted.sendStatus = SendStatus::SEND_OK;
    scripted.msgId = "a,b,c,d,e";
    scripted.offsetMsgId = "p,q,r,s,t";
    scripted.messageQueue = MessageQueue(TOPIC, "broker-a", 0);
    scripted.queueOffset = 7;
    sender.setResult(scripted);

    ProduceAccumulator acc("cpp-accum-async-batch", &sender);
    // holdMs 拉到 200ms：保证 5 条都进同一批（守卫线程每 max(1, holdMs/2)=100ms 扫一轮）
    acc.setBatchMaxDelayMs(200);
    acc.start();

    std::vector<std::shared_ptr<CollectingCallback>> callbacks;
    const std::vector<Message> messages = makeMessages(5);
    for (size_t i = 0; i < messages.size(); ++i) {
        callbacks.push_back(std::make_shared<CollectingCallback>());
        expect(acc.tryAddMessage(messages[i]), "gate accepts an async message");
        acc.sendAsync(messages[i], asCallback(callbacks.back()));
    }

    const bool done = waitUntil(
        [&]() {
            for (const auto& cb : callbacks) {
                if (cb->total() != 1) return false;
            }
            return true;
        },
        5000);
    acc.shutdown();
    expect(done, "guard thread must flush the async batch");

    for (const auto& cb : callbacks) {
        expectInt(cb->errorCount(), 0, "no callback errored");
    }
    std::vector<std::string> ids;
    std::vector<int64_t> offsets;
    for (const auto& cb : callbacks) {
        // ⚠ 必须先把快照绑到局部变量：`cb->msgIds().begin()` / `.end()` 若是两次调用，
        // 拿到的就是**两个不同临时对象**的迭代器（UB）。
        const std::vector<std::string> cbIds = cb->msgIds();
        const std::vector<int64_t> cbOffsets = cb->offsets();
        ids.insert(ids.end(), cbIds.begin(), cbIds.end());
        offsets.insert(offsets.end(), cbOffsets.begin(), cbOffsets.end());
    }
    expect(ids == std::vector<std::string>({"a", "b", "c", "d", "e"}), "callbacks get ids in order");
    expect(offsets == std::vector<int64_t>({7, 8, 9, 10, 11}), "queueOffset increments per message");

    expectInt(static_cast<long long>(sender.sentCount()), 1, "exactly one batch was sent");
    expect(sender.at(0).hasCallback, "the async path sends with a callback");
    // 单线程依次 add → 批内顺序确定，可以逐字节比
    expectStr(sender.at(0).batch.encode(), referenceBatchBody(makeMessages(5)),
              "async batch body matches generateFromList byte for byte");
    // 异步批次**不**收集 keys（Java 的不对称行为），所以 batch 级 KEYS 为空串
    expect(sender.at(0).batch.properties.count(MessageConst::PROPERTY_KEYS) == 1,
           "KEYS is written unconditionally");
    expectStr(sender.at(0).batch.getProperty(MessageConst::PROPERTY_KEYS), "",
              "async batch KEYS is empty");
    expectInt(acc.currentlyHoldSize(), 0, "hold size refunded through the callback path");
}

void testSplitResultsAreSharedWhenMsgIdHasNoComma() {
    // 老 broker / 单条应答：msgId 不含逗号时所有下标指向同一份内容
    FakeSender sender;
    SendResult scripted;
    scripted.sendStatus = SendStatus::SEND_OK;
    scripted.msgId = "single-id";
    scripted.messageQueue = MessageQueue(TOPIC, "b", 0);
    scripted.queueOffset = 3;
    sender.setResult(scripted);

    ProduceAccumulator acc("cpp-accum-shared-result", &sender);
    acc.setBatchMaxDelayMs(3000);
    const std::vector<Message> messages = makeMessages(3);
    std::vector<std::unique_ptr<SyncCall>> calls;
    for (const Message& m : messages) {
        expect(acc.tryAddMessage(m), "gate accepts");
        calls.push_back(std::make_unique<SyncCall>(spawnSyncSend(acc, m)));
    }
    auto batch = waitForBatch(acc, 3);
    expect(batch != nullptr, "3 messages land in one batch");
    if (batch != nullptr) {
        bool ok = true;
        try {
            batch->forceSyncSend();
        } catch (const std::exception& e) {
            ok = false;
            std::printf("  forceSyncSend failed: %s\n", e.what());
        }
        expect(ok, "forced sync send ok");
    }
    for (auto& call : calls) joinCall(*call);

    int okCount = 0;
    for (auto& call : calls) {
        if (!call->outcome->ok) continue;
        ++okCount;
        const SendResult& result = call->outcome->result;
        // Java 是同一个 `SendResult` 实例；这里按值拷贝（语言级差异），
        // 所以断言"每一份内容都等于应答本身"而不是指针相等。
        expectStr(result.msgId, "single-id", "shared result keeps msgId");
        expectStr(result.offsetMsgId, "", "shared result keeps offsetMsgId");
        expectInt(result.queueOffset, 3, "shared result keeps queueOffset");
        expect(result.sendStatus == SendStatus::SEND_OK, "shared result keeps status");
    }
    expectInt(okCount, 3, "all 3 callers returned the shared content");
    expectInt(static_cast<long long>(sender.at(0).batch.body.size()),
              static_cast<long long>(referenceBatchBody(makeMessages(3)).size()),
              "batch body length matches generateFromList");
}

// ================================================================ 定点队列

void testSendWithMessageQueuePinsTheBatch() {
    // 指定 mq（Java 的 `send(msg, mq, producer)`）：mq 原样透传给 sendDirect
    const MessageQueue mq(TOPIC, "broker-pinned", 2);
    FakeSender sender;
    ProduceAccumulator acc("cpp-accum-pinned", &sender);
    acc.setBatchMaxDelayMs(3000);

    const std::vector<Message> messages = makeMessages(2);
    std::vector<std::unique_ptr<SyncCall>> calls;
    for (const Message& m : messages) {
        expect(acc.tryAddMessage(m), "gate accepts");
        calls.push_back(std::make_unique<SyncCall>(spawnSyncSend(acc, m, &mq)));
    }
    auto batch = waitForBatch(acc, 2);
    expect(batch != nullptr, "2 messages land in one batch");
    if (batch != nullptr) {
        bool ok = true;
        try {
            batch->forceSyncSend();
        } catch (const std::exception& e) {
            ok = false;
            std::printf("  forceSyncSend failed: %s\n", e.what());
        }
        expect(ok, "forced sync send ok");
    }
    for (auto& call : calls) joinCall(*call);
    expectInt(static_cast<long long>(sender.sentCount()), 1, "one pinned batch was sent");
    expect(sender.at(0).hasMq && sender.at(0).mq == mq, "the pinned queue reached the sender");

    // 异步 + 指定 mq
    FakeSender sender2;
    ProduceAccumulator acc2("cpp-accum-pinned-async", &sender2);
    acc2.setBatchMaxDelayMs(200);
    acc2.start();
    auto cb = std::make_shared<CollectingCallback>();
    const Message msg(TOPIC, std::string(1, '1'));
    expect(acc2.tryAddMessage(msg), "gate accepts");
    acc2.sendAsyncWithMq(msg, mq, asCallback(cb));
    const bool done = waitUntil([&]() { return cb->total() == 1; }, 5000);
    acc2.shutdown();
    expect(done, "guard thread must flush the pinned async batch");
    expectInt(cb->errorCount(), 0, "no error on the pinned async path");
    expectInt(static_cast<long long>(sender2.sentCount()), 1, "one pinned async batch was sent");
    expect(sender2.at(0).hasMq && sender2.at(0).mq == mq, "the pinned queue reached the sender");
}

// ================================================================ 批级属性

void testBatchMergesKeysWithSpaceSeparator() {
    // 同步批次的 KEYS = 全体子消息 keys 的并集，空格 join（MessageConst::KEY_SEPARATOR）
    FakeSender sender;
    ProduceAccumulator acc("cpp-accum-keys", &sender);
    acc.setBatchMaxDelayMs(3000);

    Message m1(TOPIC, std::string(2, 'a'));
    m1.setKeys("k1 k2");
    Message m2(TOPIC, std::string(3, 'b'));
    m2.setKeys("k2 k3");
    expect(acc.tryAddMessage(m1), "gate accepts m1");
    expect(acc.tryAddMessage(m2), "gate accepts m2");

    SyncCall c1 = spawnSyncSend(acc, m1);
    SyncCall c2 = spawnSyncSend(acc, m2);
    auto batch = waitForBatch(acc, 2);
    expect(batch != nullptr, "2 messages land in one batch");
    if (batch != nullptr) {
        // 批里的 keys 是**并集**（去重）
        const std::set<std::string> keys = batch->keys();
        expectInt(static_cast<long long>(keys.size()), 3, "keys are de-duplicated");
        expect(keys.count("k1") == 1 && keys.count("k2") == 1 && keys.count("k3") == 1,
               "keys are the union k1/k2/k3");
        bool ok = true;
        try {
            batch->forceSyncSend();
        } catch (const std::exception& e) {
            ok = false;
            std::printf("  forceSyncSend failed: %s\n", e.what());
        }
        expect(ok, "forced sync send ok");
    }
    joinCall(c1);
    joinCall(c2);

    // ⚠ 与 Java 有一处**可见**差异：Java 的 keys 是 `HashSet`，`String.join` 的顺序由哈希
    // 决定（未定义）；cpp 侧在 `buildBatch` 里用 `std::set` 的字典序，所以顺序确定。
    // 属性语义是"空格分隔的集合"，顺序无关（broker 只拿它做索引）。
    expectStr(sender.at(0).batch.getProperty(MessageConst::PROPERTY_KEYS), "k1 k2 k3",
              "batch KEYS is space joined");
}

void testBatchWaitStoreMsgOkFollowsAggregateKey() {
    // 回归：Java `AggregateKey(message)` 用 `message.isWaitStoreMsgOK()` —— **缺省即 true**。
    // 直接比 `getProperty("WAIT") == "true"` 会把普通消息（从没设过 WAIT）判成 false，
    // 攒出来的批量就会以 WAIT=false 下发（broker 不等刷盘就回 SEND_OK，持久性静默降级）。
    FakeSender sender;
    ProduceAccumulator acc("cpp-accum-wait", &sender);
    acc.setBatchMaxDelayMs(3000);

    Message plain(TOPIC, std::string(1, '1'));
    Message noWait(TOPIC, std::string(1, '2'));
    noWait.setWaitStoreMsgOk(false);
    expect(!plain.properties.count(MessageConst::PROPERTY_WAIT_STORE_MSG_OK),
           "plain message never wrote the WAIT property");
    expect(acc.tryAddMessage(plain), "gate accepts plain");
    expect(acc.tryAddMessage(noWait), "gate accepts noWait");

    SyncCall c1 = spawnSyncSend(acc, plain);
    SyncCall c2 = spawnSyncSend(acc, noWait);
    const bool two = waitUntil([&]() { return acc.syncBatchCount() == 2; }, 3000);
    expect(two, "WAIT=true and WAIT=false go to two different batches");
    for (const auto& batch : acc.syncBatchesSnapshot()) {
        try {
            batch->forceSyncSend();
        } catch (const std::exception& e) {
            std::printf("  forceSyncSend failed: %s\n", e.what());
        }
    }
    joinCall(c1);
    joinCall(c2);

    std::set<std::string> waits;
    for (size_t i = 0; i < sender.sentCount(); ++i) {
        waits.insert(sender.at(i).batch.getProperty(MessageConst::PROPERTY_WAIT_STORE_MSG_OK));
    }
    expectInt(static_cast<long long>(sender.sentCount()), 2, "two batches were sent");
    expect(waits.count("true") == 1, "the plain batch carries WAIT=true");
    expect(waits.count("false") == 1, "the explicit batch carries WAIT=false");
}

// ================================================================ 守卫线程

void testGuardKeepsClosedBatchUntilNextSend() {
    // 发完的批次 messagesSize 仍 > 0，所以会**留在表里**；
    // 下一次同键 send 拿到它、`add` 返回 -1 才被摘掉重取（Java 的真实行为）。
    FakeSender sender;
    ProduceAccumulator acc("cpp-accum-closed-batch", &sender);
    acc.setBatchMaxDelayMs(3000);
    const Message msg(TOPIC, std::string(1, '1'));

    expect(acc.tryAddMessage(msg), "gate accepts");
    SyncCall firstCall = spawnSyncSend(acc, msg);
    auto first = waitForBatch(acc, 1);
    expect(first != nullptr, "first batch created");
    if (first == nullptr) {
        joinCall(firstCall);
        return;
    }
    bool ok = true;
    try {
        first->forceSyncSend();
    } catch (const std::exception& e) {
        ok = false;
        std::printf("  forceSyncSend failed: %s\n", e.what());
    }
    joinCall(firstCall);
    expect(ok, "first forced send ok");

    expect(first->closed(), "sent batch is closed");
    expectInt(first->messagesSize(), 1, "closed batch keeps its messagesSize (> 0)");
    // 守卫线程看到 size > 0 → 不摘表
    acc.runGuardOnce(true);
    expectInt(static_cast<long long>(acc.syncBatchCount()), 1,
              "guard keeps a non-empty (already sent) batch");

    // 同键再来一条 → 拿到的是那个已关闭的批次 → add 返回 -1 → 摘表重取（新批次）
    expect(acc.tryAddMessage(msg), "gate accepts the second message");
    SyncCall secondCall = spawnSyncSend(acc, msg);
    auto second = waitForBatch(acc, 1, [&](const std::shared_ptr<MessageAccumulation>& b) {
        return b.get() != first.get();
    });
    expect(second != nullptr, "a fresh batch replaces the closed one");
    if (second != nullptr) {
        expect(!second->closed(), "the replacement batch is open");
        ok = true;
        try {
            second->forceSyncSend();
        } catch (const std::exception& e) {
            ok = false;
            std::printf("  forceSyncSend failed: %s\n", e.what());
        }
        expect(ok, "second forced send ok");
    }
    joinCall(secondCall);

    expectInt(static_cast<long long>(sender.sentCount()), 2, "two batches were sent in total");
    expectInt(acc.currentlyHoldSize(), 0, "hold size fully refunded");
}

void testGuardRemovesEmptyBatchWithoutSending() {
    // 空批次（还没人 add）由守卫线程置 closed 并摘表，且**不发**任何请求
    FakeSender sender;
    ProduceAccumulator acc("cpp-accum-empty-batch", &sender);
    acc.setBatchMaxDelayMs(3000);
    AggregateKey key;
    key.topic = TOPIC;
    key.waitStoreMsgOk = true;
    auto empty = acc.putEmptySyncBatch(key);
    expectInt(static_cast<long long>(acc.syncBatchCount()), 1, "empty batch sits in the table");
    expectInt(empty->messagesSize(), 0, "empty batch has size 0");

    acc.runGuardOnce(true);
    expectInt(static_cast<long long>(acc.syncBatchCount()), 0, "guard removes the empty batch");
    expect(empty->closed(), "empty batch was closed");
    expectInt(static_cast<long long>(sender.sentCount()), 0, "nothing was sent");
    expectInt(static_cast<long long>(sender.calls()), 0, "the sender was never called");
}

void testGuardFlushesAsyncEmptyTableWithoutError() {
    // 空表跑一轮守卫不该出事（Java 的 doWork 对空 values 就是只 sleep）
    FakeSender sender;
    ProduceAccumulator acc("cpp-accum-empty-table", &sender);
    acc.runGuardOnce(true);
    acc.runGuardOnce(false);
    expectInt(static_cast<long long>(acc.syncBatchCount()), 0, "sync table still empty");
    expectInt(static_cast<long long>(acc.asyncBatchCount()), 0, "async table still empty");
}

void testAccumulatorCanRestartAfterShutdown() {
    // 累加器是**按 clientId 复用**的，生产者 stop → start 会再调一次 start()：
    // 守卫线程必须能重建（Java ServiceThread 同样可重复 start）
    FakeSender sender;
    ProduceAccumulator acc("cpp-accum-restart", &sender);
    acc.setBatchMaxDelayMs(200);
    acc.start();
    acc.shutdown();
    acc.start();

    auto cb = std::make_shared<CollectingCallback>();
    const Message msg(TOPIC, std::string(1, '1'));
    expect(acc.tryAddMessage(msg), "gate accepts");
    acc.sendAsync(msg, asCallback(cb));
    const bool done = waitUntil([&]() { return cb->total() == 1; }, 5000);
    acc.shutdown();
    expect(done, "restarted guard thread must still flush");
    expectInt(cb->errorCount(), 0, "no error after restart");
    expectInt(static_cast<long long>(sender.sentCount()), 1, "one batch was sent after restart");
}

// ================================================================ 失败路径

void testSyncSendFailureSurfacesAndReturnsHoldSize() {
    // 发送失败：异常抛给**触发发送的那个调用方**，全局额度照还（Java 的 finally）。
    // ⚠ 同一批里还在等的其他调用方拿不到 `sendResult`（本批没有结果可拆）——
    // Java 那边是在 `batch.getSendResults()[index]` 上抛 NPE，这里收敛成
    // `sendResult is illegal`。
    FakeSender sender;
    sender.setError("boom");
    ProduceAccumulator acc("cpp-accum-sync-fail", &sender);
    acc.setBatchMaxDelayMs(3000);
    const Message msg(TOPIC, std::string(1, '1'));
    expect(acc.tryAddMessage(msg), "gate accepts");
    SyncCall call = spawnSyncSend(acc, msg);
    auto batch = waitForBatch(acc, 1);
    expect(batch != nullptr, "batch created");
    if (batch == nullptr) {
        joinCall(call);
        return;
    }

    std::string error;
    try {
        batch->forceSyncSend();
    } catch (const std::exception& e) {
        error = e.what();
    }
    expect(error.find("boom") != std::string::npos, "the trigger caller sees the sender failure",
           error);
    joinCall(call);
    expect(!call.outcome->ok, "the waiter did not get a result");
    expect(call.outcome->error.find("sendResult is illegal") != std::string::npos,
           "the waiter gets \"sendResult is illegal\"", call.outcome->error);
    expectInt(acc.currentlyHoldSize(), 0, "the finally block refunded the hold size");
}

void testAsyncSendFailureReachesEveryCallback() {
    // 一批 3 条 → 每个回调恰好收到**一次**异常（Java 的 `onException` 逐个回调一次）。
    FakeSender sender;
    sender.setError("async boom");
    ProduceAccumulator acc("cpp-accum-async-fail", &sender);
    acc.setBatchMaxDelayMs(200);
    acc.start();

    std::vector<std::shared_ptr<CollectingCallback>> callbacks;
    for (int i = 0; i < 3; ++i) {
        callbacks.push_back(std::make_shared<CollectingCallback>());
        const Message msg(TOPIC, std::string(1, '1'));
        expect(acc.tryAddMessage(msg), "gate accepts");
        acc.sendAsync(msg, asCallback(callbacks.back()));
    }
    const bool done = waitUntil(
        [&]() {
            for (const auto& cb : callbacks) {
                if (cb->total() != 1) return false;
            }
            return true;
        },
        5000);
    acc.shutdown();
    expect(done, "guard thread must deliver the failure to every callback");
    for (const auto& cb : callbacks) {
        expectInt(cb->successCount(), 0, "no success was delivered");
        expectInt(cb->errorCount(), 1, "each callback got exactly one exception");
        const std::vector<std::string> texts = cb->errorTexts();
        expect(texts.size() == 1 && texts[0].find("async boom") != std::string::npos,
               "the exception text is the sender's message",
               texts.empty() ? std::string() : texts[0]);
    }
    // 异步路径：回调交付时才归还额度
    expectInt(acc.currentlyHoldSize(), 0, "hold size refunded by the callback path");
    expectInt(static_cast<long long>(sender.sentCount()), 1, "one batch was attempted");
}

void testAsyncThrowLeaksHoldSizeLikeJava() {
    // ⚠ Java 的漏还路径：异步发送在**发起阶段**就抛异常时，外层 catch 只通知回调，
    // **不**执行 `currentlyHoldSize.addAndGet(-size)`。照抄（改掉就与 Java 对不上了）。
    FakeSender sender;
    sender.setThrowOnAsync(true);
    ProduceAccumulator acc("cpp-accum-async-throw", &sender);
    // 把单批阈值压到 1 字节，让 `add` 自己就触发发送（不依赖守卫线程的时间条件）
    acc.setBatchMaxBytes(1);
    auto cb = std::make_shared<CollectingCallback>();
    const Message msg(TOPIC, std::string(10, 'a'));
    expect(acc.tryAddMessage(msg), "gate accepts");
    acc.sendAsync(msg, asCallback(cb));

    expectInt(cb->successCount(), 0, "no success");
    expectInt(cb->errorCount(), 1, "the callback got the throw");
    const std::vector<std::string> texts = cb->errorTexts();
    expect(texts.size() == 1 && texts[0].find("async throw") != std::string::npos,
           "the callback gets the thrown message", texts.empty() ? std::string() : texts[0]);
    expectInt(acc.currentlyHoldSize(), 10, "hold size is NOT refunded (upstream leak, copied)");
}

// ================================================================ sender 生命周期（cpp 特有）

void testDetachSenderBlocksSyncBatchSend() {
    // cpp 结构性差异：Java 的累加器强引用生产者（永不为 null），这里生产者可能已析构。
    // 同步路径行为对齐 Java 的 `else throw`：抛异常，且 `sendSync` 的 finally 仍归还额度。
    FakeSender sender;
    ProduceAccumulator acc("cpp-accum-detach-sync", &sender);
    acc.setBatchMaxDelayMs(3000);
    expect(acc.hasSender(), "sender is attached");
    const Message msg(TOPIC, std::string(1, '1'));
    expect(acc.tryAddMessage(msg), "gate accepts");
    SyncCall call = spawnSyncSend(acc, msg);
    auto batch = waitForBatch(acc, 1);
    expect(batch != nullptr, "batch created");
    if (batch == nullptr) {
        joinCall(call);
        return;
    }

    acc.detachSender();
    expect(!acc.hasSender(), "sender is detached");

    std::string error;
    try {
        batch->forceSyncSend();
    } catch (const std::exception& e) {
        error = e.what();
    }
    expectStr(error, "defaultMQProducer is null, can not send message", "detached sync send throws");
    joinCall(call);
    expect(call.outcome->error.find("sendResult is illegal") != std::string::npos,
           "the waiter gets \"sendResult is illegal\"", call.outcome->error);
    expectInt(acc.currentlyHoldSize(), 0, "sync path still refunds on failure");
    expectInt(static_cast<long long>(sender.calls()), 0, "the detached sender was never called");
}

void testDetachSenderFailsAsyncBatchWithoutRefund() {
    // 异步路径对齐 Java 的 `else throw` 支路：错误交给回调，额度**不**归还。
    FakeSender sender;
    ProduceAccumulator acc("cpp-accum-detach-async", &sender);
    acc.setBatchMaxBytes(1);  // 让 `add` 自己触发发送
    acc.detachSender();
    auto cb = std::make_shared<CollectingCallback>();
    const Message msg(TOPIC, std::string(10, 'a'));
    expect(acc.tryAddMessage(msg), "gate accepts");
    acc.sendAsync(msg, asCallback(cb));

    expectInt(cb->successCount(), 0, "no success on the detached async path");
    expectInt(cb->errorCount(), 1, "the callback got the failure");
    const std::vector<std::string> texts = cb->errorTexts();
    expect(texts.size() == 1 && texts[0] == "defaultMQProducer is null, can not send message",
           "the callback gets the Java message", texts.empty() ? std::string() : texts[0]);
    expectInt(acc.currentlyHoldSize(), 10, "hold size is NOT refunded (upstream leak, copied)");
    expectInt(static_cast<long long>(sender.calls()), 0, "the detached sender was never called");
}

void testAttachSenderRebindsOnlyWhenDetached() {
    // `attachSender` 只在上一任已 detach 时重绑（Java 是"第一次记下的那个用到天荒地老"）
    FakeSender first;
    FakeSender second;
    ProduceAccumulator acc("cpp-accum-attach", &first);
    acc.attachSender(&second);
    expect(acc.hasSender(), "still attached after a redundant attach");

    acc.setBatchMaxBytes(1);
    auto cb1 = std::make_shared<CollectingCallback>();
    const Message msg(TOPIC, std::string(10, 'a'));
    expect(acc.tryAddMessage(msg), "gate accepts");
    acc.sendAsync(msg, asCallback(cb1));
    expectInt(cb1->successCount(), 1, "the first sender handled the batch");
    expectInt(static_cast<long long>(first.calls()), 1, "the first sender was used");
    expectInt(static_cast<long long>(second.calls()), 0, "the second sender was ignored");

    acc.detachSender();
    acc.attachSender(&second);
    expect(acc.hasSender(), "rebound after detach");
    auto cb2 = std::make_shared<CollectingCallback>();
    const Message msg2(TOPIC, std::string(10, 'b'));
    expect(acc.tryAddMessage(msg2), "gate accepts");
    acc.sendAsync(msg2, asCallback(cb2));
    expectInt(cb2->successCount(), 1, "the rebound sender handled the batch");
    expectInt(static_cast<long long>(second.calls()), 1, "the second sender took over");
}

// ================================================================ 守卫线程的服务名

void testGuardServiceNamesMatchJava() {
    // Java `String.format("Client_%s_GuardFor%sSend", clientInstanceName, "Sync"/"Async")`：
    // 名字只在日志里出现，但它是"这个线程是谁"的唯一线索，值不值得断言见仁见智 ——
    // 这里只断言 start/shutdown 可重复且不抛（上面的 restart 用例覆盖了功能面）。
    FakeSender sender;
    ProduceAccumulator acc("cpp-accum-names", &sender);
    acc.start();
    acc.start();  // 重复 start 是空操作，不能起两根同名线程
    acc.shutdown();
    acc.shutdown();  // 重复 shutdown 也是空操作
    expect(acc.instanceName() == "cpp-accum-names", "instanceName drives the guard thread name");
}

struct TestCase {
    const char* name;
    void (*fn)();
};

const TestCase kTests[] = {
    {"default_params", testDefaultParamsMatchJava},
    {"param_guards", testParamGuardsCopyJavaRanges},
    {"registry_reuse", testRegistryReusesAccumulatorByClientId},
    {"gate", testTryAddMessageGateAndRelease},
    {"aggregate_key", testAggregateKeyPartitionsByTopicMqWaitAndTag},
    {"tags", testDifferentTagsDoNotMerge},
    {"sync_batch", testSyncBatchMergesMessagesAndReturnsPerMessageResults},
    {"async_batch", testAsyncBatchMergesAndFiresEveryCallback},
    {"shared_result", testSplitResultsAreSharedWhenMsgIdHasNoComma},
    {"pinned", testSendWithMessageQueuePinsTheBatch},
    {"keys", testBatchMergesKeysWithSpaceSeparator},
    {"wait_property", testBatchWaitStoreMsgOkFollowsAggregateKey},
    {"guard_closed_batch", testGuardKeepsClosedBatchUntilNextSend},
    {"guard_empty_batch", testGuardRemovesEmptyBatchWithoutSending},
    {"guard_empty_table", testGuardFlushesAsyncEmptyTableWithoutError},
    {"restart", testAccumulatorCanRestartAfterShutdown},
    {"sync_fail", testSyncSendFailureSurfacesAndReturnsHoldSize},
    {"async_fail", testAsyncSendFailureReachesEveryCallback},
    {"async_throw", testAsyncThrowLeaksHoldSizeLikeJava},
    {"detach_sync", testDetachSenderBlocksSyncBatchSend},
    {"detach_async", testDetachSenderFailsAsyncBatchWithoutRefund},
    {"attach_rebind", testAttachSenderRebindsOnlyWhenDetached},
    {"guard_thread_name", testGuardServiceNamesMatchJava},
};

}  // namespace

int main() {
    // 不缓冲：本用例会起多线程，万一某处崩了，已跑过的断言不会留在缓冲区里丢掉
    std::setvbuf(stdout, nullptr, _IONBF, 0);
    for (const TestCase& test : kTests) {
        const int beforeFails = fails;
        std::printf("[run] %s\n", test.name);
        test.fn();
        if (fails != beforeFails) {
            std::printf("  ^^ failures attributed to %s\n", test.name);
        }
    }
    std::printf("produce_accumulator: %d checks, %d failures\n", checks, fails);
    return fails == 0 ? 0 : 1;
}
