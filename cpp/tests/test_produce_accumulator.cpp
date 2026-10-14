// Offline unit tests for the produce accumulator: default parameters and guards, the
// global byte gate, aggregate keys, sync/async merging, the guard thread, failure paths
// and the sender attach/detach lifecycle (23 cases below).
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
#include <utility>
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

// turn an exception_ptr into text (callbacks only receive an exception_ptr, assertions compare content)
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

// ================================================================ fixtures

// One batch handed to the sender: batch object + target queue + whether it is async (with a callback).
struct BatchSlot {
    MessageBatch batch;
    bool hasMq = false;
    MessageQueue mq;
    bool hasCallback = false;
};

class FakeSender : public AccumulatorSender {
public:
    void setResult(const SendResult& result) {
        std::lock_guard<std::mutex> lk(mutex_);
        result_ = result;
        hasResult_ = true;
    }

    void setError(const std::string& message) {
        std::lock_guard<std::mutex> lk(mutex_);
        error_ = message;
    }

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

    // fallback when no result is specified: `SEND_OK` + `123`
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

// collecting callback: records every delivery (used to assert exactly once and the content).
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

std::vector<Message> makeMessages(size_t n) {
    std::vector<Message> out;
    out.reserve(n);
    for (size_t i = 0; i < n; ++i) {
        out.push_back(Message(TOPIC, std::string(i + 1, '1')));
    }
    return out;
}

// Comparison body: via `generateFromList` (same encoding contract as the accumulator's `buildBatch`)
std::string referenceBatchBody(const std::vector<Message>& messages) {
    return MessageBatch::generateFromList(messages).encode();
}

// ================================================================ thread helpers

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

// run one sync `send` on a thread (in the real call chain the caller thread blocks inside `add` exactly like this).
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

// wait until some sync batch accumulates `count` messages (`filter` picks the intended batch from the table)
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

// ================================================================ parameters

void testDefaultParamsMatchContract() {
    FakeSender sender;
    ProduceAccumulator acc("cpp-accum-params", &sender);
    expectInt(acc.getBatchMaxDelayMs(), PRODUCE_ACCUMULATOR_DEFAULT_HOLD_MS, "default holdMs const");
    expectInt(acc.getBatchMaxBytes(), PRODUCE_ACCUMULATOR_DEFAULT_HOLD_SIZE, "default holdSize const");
    expectInt(acc.totalHoldSize(), PRODUCE_ACCUMULATOR_DEFAULT_TOTAL_HOLD_SIZE,
              "default totalHoldSize const");
    expectInt(acc.getTotalBatchMaxBytes(), PRODUCE_ACCUMULATOR_DEFAULT_HOLD_SIZE,
              "getTotalBatchMaxBytes copies the upstream bug (returns holdSize)");
    expectInt(acc.currentlyHoldSize(), 0, "currentlyHoldSize starts at 0");
    expectInt(acc.getBatchMaxDelayMs(), 10, "default holdMs is 10ms");
    expectInt(acc.getBatchMaxBytes(), 32 * 1024, "default holdSize is 32KB");
    expectInt(acc.totalHoldSize(), 32 * 1024 * 1024, "default totalHoldSize is 32MB");
    expect(acc.instanceName() == "cpp-accum-params", "instanceName kept");
}

void testParamGuardRanges() {
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
    FakeSender first;
    FakeSender second;
    auto a = getOrCreateProduceAccumulator("cpp-accum-shared", &first);
    auto b = getOrCreateProduceAccumulator("cpp-accum-shared", &second);
    expect(a.get() == b.get(), "same clientId returns the same accumulator instance");
    expect(a->hasSender(), "accumulator bound to the first sender");

    a->setBatchMaxDelayMs(1234);
    expectInt(b->getBatchMaxDelayMs(), 1234, "params shared through the registry");
    a->setBatchMaxDelayMs(PRODUCE_ACCUMULATOR_DEFAULT_HOLD_MS);

    expect(a->hasSender(), "sender still attached after a second getOrCreate");
}

// ================================================================ global byte gate

void testTryAddMessageGateAndRelease() {
    FakeSender sender;
    ProduceAccumulator acc("cpp-accum-gate", &sender);
    acc.setTotalBatchMaxBytes(10);
    const Message msg(TOPIC, std::string(10, 'a'));  // 10 bytes

    expect(acc.tryAddMessage(msg), "first message passes the gate");
    expectInt(acc.currentlyHoldSize(), 10, "gate records the body size");
    expect(!acc.tryAddMessage(Message(TOPIC, std::string(1, 'b'))), "gate rejects when full");
    expectInt(acc.currentlyHoldSize(), 10, "rejected message does not change the hold size");
    acc.releaseHold(10);
    expectInt(acc.currentlyHoldSize(), 0, "releaseHold refunds");
    expect(acc.tryAddMessage(Message(TOPIC, std::string(1, 'b'))), "gate passes again after refund");

    // an empty body is not accounted but still passes
    acc.releaseHold(1);
    acc.setTotalBatchMaxBytes(5);
    expect(acc.tryAddMessage(Message(TOPIC, std::string())), "empty body always passes");
    expectInt(acc.currentlyHoldSize(), 0, "empty body is not accounted");
}

// ================================================================ aggregate keys

void testAggregateKeyPartitionsByTopicMqWaitAndTag() {
    const Message msg(TOPIC, std::string(1, 'x'));
    const AggregateKey base = AggregateKey::ofMessage(msg);
    expect(!base.hasTag, "plain message has no tag (null, not empty string)");
    expect(base.waitStoreMsgOk, "plain message is waitStoreMsgOK (defaults to true)");
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

    // three distinct keys must not overwrite each other in the table (the sync table is keyed by AggregateKey)
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
    // each of the three forms its own batch, so trigger them one by one to release the waiting threads
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
    // two messages with different tags must land in **two** batches (one MessageBatch has only one TAGS
    // property), and each batch is sent out independently.
    FakeSender sender;
    ProduceAccumulator acc("cpp-accum-tags", &sender);
    acc.start();  // keep the default holdMs(10ms) and let the guard thread push the two batches out separately

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
    // fallback: even if the guard thread did not push both batches out, the waiting callers must be
    // released (`sendSync` returns directly on an already closed batch, so calling it again here is safe)
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

// ================================================================ merging (sync)

void testSyncBatchMergesMessagesAndReturnsPerMessageResults() {
    // 5 same-key messages merge into 1 MessageBatch, each getting its own split SendResult
    FakeSender sender;
    SendResult scripted;
    scripted.sendStatus = SendStatus::SEND_OK;
    scripted.msgId = "id-0,id-1,id-2,id-3,id-4";
    scripted.offsetMsgId = "off-0,off-1,off-2,off-3,off-4";
    scripted.messageQueue = MessageQueue(TOPIC, "broker-a", 0);
    scripted.queueOffset = 100;
    sender.setResult(scripted);

    ProduceAccumulator acc("cpp-accum-sync-batch", &sender);
    // holdMs stretched to 3s: guarantees all 5 join one batch (the case triggers the send manually, no wait for the 3s)
    acc.setBatchMaxDelayMs(3000);

    const std::vector<Message> messages = makeMessages(5);
    // in the real call chain `canBatch` first accounts via `tryAddMessage`; the accumulator refunds after the send
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

    // manual trigger (equivalent to the guard thread waking one waiter to send once holdMs elapses)
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
    expectInt(static_cast<long long>(sender.at(0).batch.body.size()),
              static_cast<long long>(referenceBatchBody(makeMessages(5)).size()),
              "batch body length matches MessageBatch::generateFromList");

    int okCount = 0;
    for (auto& call : calls) {
        expect(call->outcome->ok, "every waiter got its own SendResult", call->outcome->error);
        if (!call->outcome->ok) continue;
        ++okCount;
        const SendResult& result = call->outcome->result;
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
    // after the send, refund the global hold quota
    expectInt(acc.currentlyHoldSize(), 0, "hold size refunded after the batch is sent");
    expectInt(static_cast<long long>(acc.syncBatchCount()), 1, "sent batch stays in the table");
}

// ================================================================ merging (async)

void testAsyncBatchMergesAndFiresEveryCallback() {
    // async: the guard thread merges 5 same-key messages into 1 batch, each callback gets its own split result
    FakeSender sender;
    SendResult scripted;
    scripted.sendStatus = SendStatus::SEND_OK;
    scripted.msgId = "a,b,c,d,e";
    scripted.offsetMsgId = "p,q,r,s,t";
    scripted.messageQueue = MessageQueue(TOPIC, "broker-a", 0);
    scripted.queueOffset = 7;
    sender.setResult(scripted);

    ProduceAccumulator acc("cpp-accum-async-batch", &sender);
    // holdMs stretched to 200ms: guarantees all 5 join one batch (the guard thread scans every max(1, holdMs/2)=100ms)
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
        // ⚠ bind the snapshot to a local first: if `cb->msgIds().begin()` / `.end()` are two separate calls,
        // they are iterators into **two different temporary objects** (UB).
        const std::vector<std::string> cbIds = cb->msgIds();
        const std::vector<int64_t> cbOffsets = cb->offsets();
        ids.insert(ids.end(), cbIds.begin(), cbIds.end());
        offsets.insert(offsets.end(), cbOffsets.begin(), cbOffsets.end());
    }
    expect(ids == std::vector<std::string>({"a", "b", "c", "d", "e"}), "callbacks get ids in order");
    expect(offsets == std::vector<int64_t>({7, 8, 9, 10, 11}), "queueOffset increments per message");

    expectInt(static_cast<long long>(sender.sentCount()), 1, "exactly one batch was sent");
    expect(sender.at(0).hasCallback, "the async path sends with a callback");
    // single-threaded sequential add gives a deterministic in-batch order, so byte-for-byte comparison works
    expectStr(sender.at(0).batch.encode(), referenceBatchBody(makeMessages(5)),
              "async batch body matches generateFromList byte for byte");
    expect(sender.at(0).batch.properties.count(MessageConst::PROPERTY_KEYS) == 1,
           "KEYS is written unconditionally");
    expectStr(sender.at(0).batch.getProperty(MessageConst::PROPERTY_KEYS), "",
              "async batch KEYS is empty");
    expectInt(acc.currentlyHoldSize(), 0, "hold size refunded through the callback path");
}

void testSplitResultsAreSharedWhenMsgIdHasNoComma() {
    // old broker / single reply: when msgId has no comma all indexes point at the same content
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

// ================================================================ pinned queue

void testSendWithMessageQueuePinsTheBatch() {
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

    // async + pinned mq
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

// ================================================================ batch-level properties

void testBatchMergesKeysWithSpaceSeparator() {
    // sync batch KEYS = the union of all sub-message keys, space-joined (MessageConst::KEY_SEPARATOR)
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
        // the batch keys are the **union** (de-duplicated)
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

    expectStr(sender.at(0).batch.getProperty(MessageConst::PROPERTY_KEYS), "k1 k2 k3",
              "batch KEYS is space joined");
}

void testBatchWaitStoreMsgOkFollowsAggregateKey() {
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

// ================================================================ guard thread

void testGuardKeepsClosedBatchUntilNextSend() {
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
    // the guard thread sees size > 0 and leaves the batch in the table
    acc.runGuardOnce(true);
    expectInt(static_cast<long long>(acc.syncBatchCount()), 1,
              "guard keeps a non-empty (already sent) batch");

    // another same-key message gets the closed batch, add returns -1, so it is removed and re-fetched (new batch)
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
    // an empty batch (nobody added yet) is closed and removed by the guard thread, and **no** request is sent
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
    FakeSender sender;
    ProduceAccumulator acc("cpp-accum-empty-table", &sender);
    acc.runGuardOnce(true);
    acc.runGuardOnce(false);
    expectInt(static_cast<long long>(acc.syncBatchCount()), 0, "sync table still empty");
    expectInt(static_cast<long long>(acc.asyncBatchCount()), 0, "async table still empty");
}

void testAccumulatorCanRestartAfterShutdown() {
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

// ================================================================ failure paths

void testSyncSendFailureSurfacesAndReturnsHoldSize() {
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
    // async path: the quota is refunded only when the callback is delivered
    expectInt(acc.currentlyHoldSize(), 0, "hold size refunded by the callback path");
    expectInt(static_cast<long long>(sender.sentCount()), 1, "one batch was attempted");
}

void testAsyncThrowLeaksHoldSize() {
    FakeSender sender;
    sender.setThrowOnAsync(true);
    ProduceAccumulator acc("cpp-accum-async-throw", &sender);
    // squeeze the per-batch threshold to 1 byte so `add` itself triggers the send (independent of the guard thread's timing)
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

// ================================================================ sender lifecycle (cpp-specific)

void testDetachSenderBlocksSyncBatchSend() {
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
    FakeSender sender;
    ProduceAccumulator acc("cpp-accum-detach-async", &sender);
    acc.setBatchMaxBytes(1);  // let `add` trigger the send itself
    acc.detachSender();
    auto cb = std::make_shared<CollectingCallback>();
    const Message msg(TOPIC, std::string(10, 'a'));
    expect(acc.tryAddMessage(msg), "gate accepts");
    acc.sendAsync(msg, asCallback(cb));

    expectInt(cb->successCount(), 0, "no success on the detached async path");
    expectInt(cb->errorCount(), 1, "the callback got the failure");
    const std::vector<std::string> texts = cb->errorTexts();
    expect(texts.size() == 1 && texts[0] == "defaultMQProducer is null, can not send message",
           "the callback gets the recorded message", texts.empty() ? std::string() : texts[0]);
    expectInt(acc.currentlyHoldSize(), 10, "hold size is NOT refunded (upstream leak, copied)");
    expectInt(static_cast<long long>(sender.calls()), 0, "the detached sender was never called");
}

void testAttachSenderRebindsOnlyWhenDetached() {
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

// ================================================================ guard thread service name

void testGuardServiceNamesMatchContract() {
    FakeSender sender;
    ProduceAccumulator acc("cpp-accum-names", &sender);
    acc.start();
    acc.start();  // a repeated start is a no-op and must not spawn a second thread with the same name
    acc.shutdown();
    acc.shutdown();  // a repeated shutdown is also a no-op
    expect(acc.instanceName() == "cpp-accum-names", "instanceName drives the guard thread name");
}

struct TestCase {
    const char* name;
    void (*fn)();
};

const TestCase kTests[] = {
    {"default_params", testDefaultParamsMatchContract},
    {"param_guards", testParamGuardRanges},
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
    {"async_throw", testAsyncThrowLeaksHoldSize},
    {"detach_sync", testDetachSenderBlocksSyncBatchSend},
    {"detach_async", testDetachSenderFailsAsyncBatchWithoutRefund},
    {"attach_rebind", testAttachSenderRebindsOnlyWhenDetached},
    {"guard_thread_name", testGuardServiceNamesMatchContract},
};

}  // namespace

int main() {
    // unbuffered: this case spawns threads, so if something crashes, assertions already run are not lost in the buffer
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
