// Live runbook for consumer-offset reset (the 220 push chain), asserting each scenario in turn.
//
// 220 is the reset command the broker pushes to the **consumer side**; the admin-side
// INVOKE_BROKER_TO_RESET_OFFSET(222) response is only a table of "which offset each queue resets to" -- what truly
// makes the consumer side change its offset is the 220 the broker then pushes oneway (Broker2Client.resetOffset:181-238).
// The broker only takes this push path when useServerSideResetOffset=false
// (the default true changes offsets server-side directly in AdminBrokerProcessor:2255-2263 without notifying a single consumer), so this verification first hot-updates the switch to false (confirmed by read-back) and restores it after the run.
//
// The offline unit tests (tests/test_reset_offset.cpp) already lock the two request-body shapes and the local state of "queue removal
// + code +1 + new offset landing on disk through the revoked tail"; only a real cluster can prove the following:
//
//   S1 "backward reset takes effect immediately + in-flight batches voided" — the offset is reset backward from 10 to 3, with three criteria:
//      a) the committed offset on the broker becomes 3 within ~2s: inside the window **only** that one persist of the reset path writes it
//         (periodic flushing has been stretched to 60s); an implementation that only writes the in-memory table stands still here (the broker stays at 10);
//      b) after the stuck old batch in the listener (fetched offset 10 before the reset) is released, its ack must be voided as a whole batch
//         — sampling point: after releasing the old batch, when the **first** batch of the new queue is in the listener but not yet acked, the local consumed
//         offset must still be "no record / <=3"; an implementation without the epoch gate jumps to 11 here;
//      c) the queue is truly rebuilt: every batch of 3..14 is **redelivered once** (the un-acked 11..14 in the old buffer are voided along with it and
//         can only appear as part of the redelivery), release one round and assert one round.
//
//   S2 "forward skip + recovery" — reset to maxOffset with timestamp=-1: the offset jumps straight to 10 and none of 4..9 is
//      delivered; afterwards new messages are consumed normally and the offset keeps advancing.
//
// Rationale: why the gate must be per batch in the listener — on a real machine the failure mode of "the reset took effect" only shows as "the offset never moved and
// messages are never redelivered", invisible from the admin side; and putting the timing judgement into an offline unit test cannot rely on a real broker's
// pushes and offset table. Per-batch control is what creates the sampling point "the old batch is released, the new queue's first batch is not yet acked".
//
// Usage: ./rmq_live_reset_offset 127.0.0.1:9876
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstdio>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include "rocketmq/client/admin.h"
#include "rocketmq/client/consumer.h"
#include "rocketmq/client/mq_client.h"
#include "rocketmq/client/producer.h"
#include "rocketmq/common/message.h"
#include "rocketmq/common/mix_all.h"
#include "rocketmq/common/types.h"
#include "rocketmq/common/util_all.h"
#include "rocketmq/remoting/protocol/heartbeat.h"
#include "rocketmq/remoting/protocol/route.h"

using namespace rocketmq;

namespace {

int32_t gPass = 0;
int32_t gFail = 0;

void check(const std::string& name, bool ok, const std::string& detail = std::string()) {
    if (ok) {
        ++gPass;
    } else {
        ++gFail;
    }
    std::printf("  [%s] %s%s\n", ok ? "PASS" : "FAIL", name.c_str(),
                detail.empty() ? "" : ("  " + detail).c_str());
}

int64_t nowMs() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::steady_clock::now().time_since_epoch())
        .count();
}

// Wall-clock milliseconds (Unix epoch). **Timestamp-based resets use only this, never nowMs()**: the broker's
// `getOffsetInQueueByTime` compares it against each message's storeTimestamp (the broker's wall clock), while
// steady_clock on macOS is "milliseconds since boot" (three orders of magnitude below the real epoch) — the symptom of
// the wrong clock is silent: the target offset is constantly 0 (the timestamp falls before every message), the reset "succeeds" but replays the whole queue from the head.
int64_t wallMs() {
    return UtilAll::currentTimeMillis();
}

bool waitUntil(const std::function<bool()>& pred, int64_t timeoutMs, int64_t intervalMs = 200) {
    const int64_t deadline = nowMs() + timeoutMs;
    while (nowMs() < deadline) {
        if (pred()) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(intervalMs));
    }
    return pred();
}

// For assertions about "something that is about to happen very soon": sampled denser than waitUntil.
bool waitBefore(const std::function<bool()>& pred, int64_t windowMs) {
    return waitUntil(pred, windowMs, 50);
}

const int32_t kMsgs = 10;
const int64_t kBackTarget = 3;
const char* kSwitchKey = "useServerSideResetOffset";

std::string fmtOffsets(const std::vector<int64_t>& v) {
    std::string s = "[";
    for (size_t i = 0; i < v.size(); ++i) {
        if (i != 0) s += ",";
        s += std::to_string(v[i]);
    }
    return s + "]";
}

std::string fmtTable(const std::map<MessageQueue, int64_t>& table) {
    std::string s = "{";
    for (const auto& kv : table) {
        if (s.size() > 1) s += ", ";
        s += "q" + std::to_string(kv.first.queueId) + ":" + std::to_string(kv.second);
    }
    return s + "}";
}

// Per-batch gating: every batch stops at the gate and the test releases it batch by batch -- the local state between batches can therefore be sampled.
//
// S1's key sampling point (old batch released, the new queue's first batch not yet acked) only exists with per-batch control; a listener
// released all at once would mix 11 (the old batch's ack) and the following redelivery into one instant.
class SteppingListener : public MessageListenerConcurrently {
public:
    ConsumeConcurrentlyStatus consumeMessage(const std::vector<MessageExt>& msgs,
                                             ConsumeConcurrentlyContext&) override {
        {
            std::lock_guard<std::mutex> lk(mtx_);
            std::vector<int64_t> batch;
            for (const MessageExt& m : msgs) {
                batch.push_back(m.queueOffset);
            }
            batches_.push_back(batch);
        }
        std::unique_lock<std::mutex> lk(mtx_);
        // The cap only guards against deadlock: the normal path is released batch by batch by the test
        cv_.wait_for(lk, std::chrono::seconds(90), [this] { return released_; });
        released_ = false;  // one release per batch: reset after releasing and wait for the next batch's release()
        return ConsumeConcurrentlyStatus::CONSUME_SUCCESS;
    }

    void release() {
        {
            std::lock_guard<std::mutex> lk(mtx_);
            released_ = true;
        }
        cv_.notify_all();
    }

    size_t batchCount() {
        std::lock_guard<std::mutex> lk(mtx_);
        return batches_.size();
    }

    std::vector<int64_t> offsets() {
        std::lock_guard<std::mutex> lk(mtx_);
        std::vector<int64_t> out;
        for (const std::vector<int64_t>& b : batches_) {
            out.insert(out.end(), b.begin(), b.end());
        }
        return out;
    }

private:
    std::mutex mtx_;
    std::condition_variable cv_;
    bool released_ = false;
    std::vector<std::vector<int64_t>> batches_;
};

bool waitBatches(SteppingListener& l, size_t n, int64_t timeoutMs) {
    return waitUntil([&] { return l.batchCount() >= n; }, timeoutMs, 50);
}

class RecordingListener : public MessageListenerConcurrently {
public:
    ConsumeConcurrentlyStatus consumeMessage(const std::vector<MessageExt>& msgs,
                                             ConsumeConcurrentlyContext&) override {
        std::lock_guard<std::mutex> lk(mtx_);
        for (const MessageExt& m : msgs) {
            offsets_.push_back(m.queueOffset);
        }
        return ConsumeConcurrentlyStatus::CONSUME_SUCCESS;
    }

    std::vector<int64_t> offsets() {
        std::lock_guard<std::mutex> lk(mtx_);
        return offsets_;
    }

private:
    std::mutex mtx_;
    std::vector<int64_t> offsets_;
};

struct Row {
    MessageQueue mq;
    int64_t maxOffset = -1;
    bool hasCommitted = false;  // false = no record found on the broker (QUERY_NOT_FOUND)
    int64_t committed = -1;
};

std::string fmtRows(const std::vector<Row>& rows) {
    std::string s;
    for (const Row& r : rows) {
        if (!s.empty()) s += " ";
        s += "q" + std::to_string(r.mq.queueId) + ":" +
             (r.hasCommitted ? std::to_string(r.committed) : std::string("None")) + "/max" +
             std::to_string(r.maxOffset);
    }
    return s;
}

struct Fixture {
    std::string namesrv;
    std::string stamp;
    std::string brokerAddr;
    DefaultMQAdminExt admin;
    std::shared_ptr<DefaultMQProducer> producer;

    void start(const std::string& ns) {
        namesrv = ns;
        stamp = std::to_string(nowMs() % 1000000);
        admin.setNamesrvAddr(ns);
        admin.setTimeoutMillis(10000);
        admin.start();
        ClusterInfo cluster = admin.fetchBrokerClusterInfo();
        const std::vector<std::string> addrs = cluster.getBrokerAddrs();
        if (addrs.empty()) {
            throw std::runtime_error("the name server has no registered broker");
        }
        brokerAddr = addrs[0];
        producer = std::make_shared<DefaultMQProducer>("GID_cpp_live_ro_pg_" + stamp);
        producer->setNamesrvAddr(ns);
        producer->setInstanceName("cpp-live-ro-prod-" + stamp);
        producer->start();
    }

    std::vector<MessageQueue> queues(const std::string& topicName) {
        TopicRouteData route = admin.examineTopicRoute(topicName);
        return route.getAllMessageQueue(topicName);
    }

    // [(mq, maxOffset, committed)]; hasCommitted=false means no record found on the broker.
    // setZeroIfNotFound=false: an empty record 0 and "no record" must stay distinguishable.
    std::vector<Row> offsets(const std::string& group, const std::string& topicName) {
        std::vector<Row> out;
        for (const MessageQueue& mq : queues(topicName)) {
            Row r;
            r.mq = mq;
            try {
                r.maxOffset = admin.client().getMaxOffset(mq, 5000, brokerAddr);
            } catch (const std::exception&) {
                r.maxOffset = -1;
            }
            int64_t off = 0;
            try {
                r.hasCommitted =
                    admin.client().queryConsumerOffset(group, mq, off, 5000, brokerAddr,
                                                       /*setZeroIfNotFound=*/false);
                r.committed = off;
            } catch (const std::exception&) {
                r.hasCommitted = false;
            }
            out.push_back(r);
        }
        return out;
    }

    void shutdown() {
        if (producer) {
            try {
                producer->shutdown();
            } catch (const std::exception&) {
            }
        }
        try {
            admin.shutdown();
        } catch (const std::exception&) {
        }
    }
};

std::string readFlag(DefaultMQAdminExt& admin, const std::string& addr, const std::string& key) {
    try {
        PropertyMap cfg = admin.getBrokerConfig(addr);
        auto it = cfg.find(key);
        return it == cfg.end() ? std::string("<missing>") : it->second;
    } catch (const std::exception& e) {
        return std::string("<") + e.what() + ">";
    }
}

bool setServerSideReset(Fixture& fx, const std::string& value) {
    PropertyMap props;
    props[kSwitchKey] = value;
    try {
        fx.admin.updateBrokerConfig(fx.brokerAddr, props);
    } catch (const std::exception& e) {
        std::printf("  [diag] updateBrokerConfig(%s=%s) failed: %s\n", kSwitchKey, value.c_str(),
                    e.what());
        return false;
    }
    std::this_thread::sleep_for(std::chrono::seconds(1));
    return readFlag(fx.admin, fx.brokerAddr, kSwitchKey) == value;
}

// The queue key of this topic from the consumer's point of view (built from the routed mq, same convention as offsetKey).
std::string queueKeyOf(DefaultMQPushConsumer& c, const std::string& topic) {
    for (const MessageQueue& mq : c.fetchSubscribeMessageQueues(topic)) {
        if (mq.queueId == 0) {
            return DefaultMQPushConsumer::offsetKey(mq);
        }
    }
    return std::string();
}

// The "consumed offset" in this client's in-memory table; nullopt = this queue has no record in the table.
std::optional<int64_t> localOffset(DefaultMQPushConsumer& c, const std::string& topic) {
    const std::map<MessageQueue, int64_t> status = c.getConsumerStatus(topic);
    for (const auto& kv : status) {
        if (kv.first.queueId == 0) return kv.second;
    }
    return std::nullopt;
}

std::string offText(const std::optional<int64_t>& v) {
    return v ? std::to_string(*v) : "None";
}

std::shared_ptr<DefaultMQPushConsumer> startConsumer(
    Fixture& fx, const std::string& group, const std::string& role, const std::string& topic,
    std::shared_ptr<MessageListener> listener, int32_t batchSize, int32_t persistIntervalMs) {
    auto c = std::make_shared<DefaultMQPushConsumer>(group);
    c->setNamesrvAddr(fx.namesrv);
    c->setInstanceName("cpp-live-ro-" + role + "-" + fx.stamp);
    c->setConsumeFromWhere(ConsumeFromWhere::CONSUME_FROM_FIRST_OFFSET);
    c->setConsumeMessageBatchMaxSize(batchSize);
    if (persistIntervalMs > 0) {
        c->setPersistConsumerOffsetIntervalMillis(persistIntervalMs);
    }
    c->setMessageListener(listener);
    c->subscribe(topic, "*");
    c->start();
    return c;
}

bool brokerIs(Fixture& fx, const std::string& group, const std::string& topic, int64_t want) {
    const std::vector<Row> rows = fx.offsets(group, topic);
    if (rows.empty()) return false;
    for (const Row& r : rows) {
        if (!r.hasCommitted || r.committed != want) return false;
    }
    return true;
}

// ------------------------------------------------ S1: backward reset + in-flight batches voided

void s1BackwardReset(Fixture& fx, const std::string& topic, const std::string& group) {
    std::printf("\n--- S1: backward reset (10 -> 3) persisted at once + the in-flight batch voided as a whole ---\n");
    // Precondition: first use a plain listener to bring the offset on the broker to 10 (a reset requires the group to have a record there)
    auto l0 = std::make_shared<RecordingListener>();
    std::shared_ptr<DefaultMQPushConsumer> c0 =
        startConsumer(fx, group, "s1seed", topic, l0, /*batchSize=*/1, /*persist=*/0);
    std::this_thread::sleep_for(std::chrono::seconds(1));
    int64_t tMid = 0;
    for (int32_t i = 0; i < kMsgs; ++i) {
        Message m(topic, "rb-" + std::to_string(i));
        fx.producer->send(m);
        if (i == 2) {
            tMid = wallMs();
            // Leave a store-time gap between the message at offset 3 and the three before it, so the later timestamp-based reset reliably hits 3
            std::this_thread::sleep_for(std::chrono::seconds(2));
        }
    }
    const bool seeded =
        waitUntil([&] { return l0->offsets().size() == static_cast<size_t>(kMsgs); }, 30000, 200);
    check("S1-precondition: the consumer consumes " + std::to_string(kMsgs) + " messages", seeded,
          "arrivals=" + fmtOffsets(l0->offsets()));

    bool primed = waitUntil([&] { return brokerIs(fx, group, topic, kMsgs); }, 20000, 500);
    check("S1-precondition: the broker offset is periodically persisted to " + std::to_string(kMsgs) +
              " (a reset requires the group to have a record on the broker)",
          primed, fmtRows(fx.offsets(group, topic)));
    c0->shutdown();
    std::this_thread::sleep_for(std::chrono::seconds(1));

    // The main consumer: batches gated one by one + a 60s periodic flush => inside the window the only path that can move the broker offset is the persist carried by the reset itself
    auto listener = std::make_shared<SteppingListener>();
    std::shared_ptr<DefaultMQPushConsumer> c =
        startConsumer(fx, group, "s1", topic, listener, /*batchSize=*/1, /*persist=*/60000);
    const int64_t tStart = nowMs();
    std::this_thread::sleep_for(std::chrono::seconds(2));
    for (int32_t i = kMsgs; i < kMsgs + 5; ++i) {
        Message m(topic, "rb-" + std::to_string(i));
        fx.producer->send(m);
    }

    std::string key;
    size_t pending = 0;
    const bool arranged = waitUntil(
        [&] {
            if (key.empty()) key = queueKeyOf(*c, topic);
            if (key.empty()) return false;
            pending = c->pendingMessages(key).size();
            return listener->batchCount() >= 1 && pending == 4;
        },
        30000, 50);
    const std::vector<int64_t> firstArrivals = listener->offsets();
    check("S1-window ready: 1 message in flight (offset 10 stuck in the listener) + 4 left in the buffer",
          arranged && !firstArrivals.empty() && firstArrivals[0] == kMsgs,
          "arrivals=" + fmtOffsets(firstArrivals) + " pending=" + std::to_string(pending));
    if (!arranged || key.empty()) {
        listener->release();
        c->shutdown();
        return;
    }

    waitUntil([&] { return nowMs() - tStart > 12000; }, 15000, 200);
    check("S1-the initial periodic flush has passed (no further periodic write for the next 60s)",
          nowMs() - tStart > 12000 && brokerIs(fx, group, topic, kMsgs),
          fmtRows(fx.offsets(group, topic)));

    // Reset to 3 by timestamp: tMid falls between the 3rd and the 4th message => getOffsetInQueueByTime hits 3,
    // and 3 < consumerOffset(10) with isForce=true => the broker pushes {mq: 3}
    const int64_t resetTs = tMid + 500;
    const int64_t t0 = nowMs();
    std::map<MessageQueue, int64_t> table =
        fx.admin.resetOffsetByTimestamp(topic, group, resetTs, /*isForce=*/true);
    check("S1-the target offset in the 222 response is exactly 3",
          table.size() == 1 && table.begin()->second == kBackTarget, fmtTable(table));

    const bool in2s = waitBefore([&] { return brokerIs(fx, group, topic, kBackTarget); }, 2000);
    char detail[160];
    std::snprintf(detail, sizeof(detail), "elapsed=%.1fs %s", (nowMs() - t0) / 1000.0,
                  fmtRows(fx.offsets(group, topic)).c_str());
    check("S1-the broker offset becomes 3 within ~2s (the persist carried by the reset path; periodic flush=60s)", in2s,
          detail);

    std::optional<int64_t> off = localOffset(*c, topic);
    check("S1-no old offset left in the local table after the reset (removeOffset; the new offset goes out through the revoked tail)",
          !off.has_value(),
          "local_offset=" + offText(off) + " epoch=" + std::to_string(c->queueEpoch(key)));

    // Release the old batch: its ack belongs to the already revoked ProcessQueue and must be voided
    listener->release();
    const bool second = waitBatches(*listener, 2, 20000);
    off = localOffset(*c, topic);
    check("S1-the old batch ack is voided (after the release, while the new queue's first batch at offset 3 is already in flight, the local offset still has not passed 3)",
          second && (!off.has_value() || *off <= kBackTarget),
          "local_offset=" + offText(off) + " arrivals=" + fmtOffsets(listener->offsets()));

    // Release batch by batch through the whole redelivery: each of 3..14 is redelivered once (11..14 in the old buffer are voided too and can only appear as redelivery)
    std::vector<int64_t> expected;
    expected.push_back(kMsgs);
    for (int64_t i = kBackTarget; i < kMsgs + 5; ++i) {
        expected.push_back(i);
    }
    for (size_t n = 2; n <= expected.size(); ++n) {
        listener->release();
        if (n < expected.size()) {
            waitBatches(*listener, n + 1, 15000);
        }
    }
    const std::vector<int64_t> arr = listener->offsets();
    check("S1-the queue is truly rebuilt: the redelivery sequence is " + fmtOffsets(expected), arr == expected,
          "arrivals=" + fmtOffsets(arr));
    waitUntil([&] { return localOffset(*c, topic) == std::optional<int64_t>(kMsgs + 5); }, 10000,
              50);

    std::vector<Row> rows = fx.offsets(group, topic);
    bool allBack = !rows.empty();
    for (const Row& r : rows) {
        if (!r.hasCommitted || r.committed != kBackTarget) allBack = false;
    }
    check("S1-only the reset itself wrote the broker inside the window (the offset still sits at 3, periodic flush=60s)", allBack,
          fmtRows(rows));

    // Shutdown flushes once synchronously: only then do the redelivered acks give the final value (15 = the last message 14 plus one)
    c->shutdown();
    const bool advanced = waitUntil([&] { return brokerIs(fx, group, topic, kMsgs + 5); }, 15000,
                                    500);
    check("S1-the shutdown flush writes the redelivered acks back to the broker (the offset advances to 15)", advanced,
          fmtRows(fx.offsets(group, topic)));
}

// ------------------------------------------------ S2: forward skip reset + recovery

void s2ForwardSkip(Fixture& fx, const std::string& topic, const std::string& group) {
    std::printf("\n--- S2: forward skip reset (timestamp=-1 -> maxOffset), no redelivery + new messages consumed as usual ---\n");
    auto listener = std::make_shared<SteppingListener>();
    // The periodic flush keeps the default 5s: this scenario first relies on the initial flush (10s after start) to build a record for the group on the broker
    // (Broker2Client.resetOffset answers SYSTEM_ERROR outright for a group whose queryOffset==-1),
    // then waits for a window of more than 5s to reset -- the reset's persist and the periodic write land at different instants, so the criterion stays clean.
    std::shared_ptr<DefaultMQPushConsumer> c =
        startConsumer(fx, group, "s2", topic, listener, /*batchSize=*/1, /*persist=*/0);
    const int64_t tStart = nowMs();
    std::this_thread::sleep_for(std::chrono::seconds(2));
    for (int32_t i = 0; i < kMsgs; ++i) {
        Message m(topic, "rs-" + std::to_string(i));
        fx.producer->send(m);
    }

    // Release the first 3 messages batch by batch: the 4th (offset 3) stays in the listener as the "in-flight batch"
    for (int n = 1; n <= 4; ++n) {
        if (!waitBatches(*listener, static_cast<size_t>(n), 30000)) break;
        if (n < 4) listener->release();
    }
    std::vector<int64_t> arr = listener->offsets();
    const bool arranged = listener->batchCount() >= 4 && arr.size() >= 4 && arr[0] == 0 &&
                          arr[1] == 1 && arr[2] == 2 && arr[3] == 3;
    check("S2-window ready: the first 3 are acked, the 4th is stuck in the listener", arranged,
          "arrivals=" + fmtOffsets(arr));
    if (!arranged) {
        listener->release();
        c->shutdown();
        return;
    }

    // Wait for the initial flush to create the group (the local consumed offset 3 = the ack of the first three) and leave a silent window of more than 5s
    const bool primed = waitUntil(
        [&] { return nowMs() - tStart > 12000 && brokerIs(fx, group, topic, kBackTarget); },
        20000, 500);
    check("S2-precondition: the group has a record on the broker (q0:3) and the next periodic write is still more than 5s away", primed,
          fmtRows(fx.offsets(group, topic)));

    const int64_t t0 = nowMs();
    std::map<MessageQueue, int64_t> table =
        fx.admin.resetOffsetByTimestamp(topic, group, /*timestamp=*/-1, /*isForce=*/true);
    check("S2-the target offset in the 222 response is exactly maxOffset(10)",
          table.size() == 1 && table.begin()->second == kMsgs, fmtTable(table));

    const bool in2s = waitBefore([&] { return brokerIs(fx, group, topic, kMsgs); }, 2000);
    char detail[160];
    std::snprintf(detail, sizeof(detail), "elapsed=%.1fs %s", (nowMs() - t0) / 1000.0,
                  fmtRows(fx.offsets(group, topic)).c_str());
    check("S2-the broker offset skips forward to 10 within ~2s (the persist carried by the reset path)", in2s, detail);

    listener->release();
    std::this_thread::sleep_for(std::chrono::seconds(3));
    arr = listener->offsets();
    const std::vector<int64_t> seedArrivals = {0, 1, 2, 3};
    check("S2-none of the skipped 4..9 is delivered (the ack of the in-flight one is voided too)", arr == seedArrivals,
          "arrivals=" + fmtOffsets(arr));

    Message after(topic, "rs-new");
    fx.producer->send(after);
    const bool gotNew = waitBatches(*listener, 5, 20000);
    listener->release();
    arr = listener->offsets();
    const std::vector<int64_t> wantArrivals = {0, 1, 2, 3, kMsgs};
    check("S2-the rebuilt queue continues from the queue tail (the new message at offset 10 is delivered normally)",
          gotNew && arr == wantArrivals, "arrivals=" + fmtOffsets(arr));

    // release() only lets the listener return; the ack is persisted afterwards by the consumer thread. Shutting down without waiting
    // for the local offset to really reach 11 lets the shutdown persist run before the ack (writing back 10) -- an assertion race, not a semantics issue.
    waitUntil([&] { return localOffset(*c, topic) == std::optional<int64_t>(kMsgs + 1); }, 10000,
              50);
    c->shutdown();
    const bool advanced = waitUntil([&] { return brokerIs(fx, group, topic, kMsgs + 1); }, 15000,
                                    500);
    check("S2-the shutdown flush writes the new message's ack back to the broker (the offset advances to 11)", advanced,
          fmtRows(fx.offsets(group, topic)));
}

}  // namespace

int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::printf("usage: rmq_live_reset_offset <namesrv>\n");
        return 2;
    }
    const std::string nsAddr = argv[1];
    Fixture fx;
    try {
        fx.start(nsAddr);
    } catch (const std::exception& e) {
        check("cluster liveness", false, e.what());
        return 1;
    }
    check("cluster liveness", true, "broker=" + fx.brokerAddr);

    const std::string topic1 = "CppLiveRoBack" + fx.stamp;
    const std::string topic2 = "CppLiveRoSkip" + fx.stamp;
    const std::string g1 = "GID_cpp_live_ro_back_" + fx.stamp;
    const std::string g2 = "GID_cpp_live_ro_skip_" + fx.stamp;

    try {
        // Hot-update the switch: useServerSideResetOffset=false (the precondition of the 220 push path)
        check("Hot-update the switch: useServerSideResetOffset=false (the precondition of the 220 push path)",
              setServerSideReset(fx, "false"),
              "read back useServerSideResetOffset=" + readFlag(fx.admin, fx.brokerAddr, kSwitchKey));

        fx.admin.createTopic(MixAll::DEFAULT_TOPIC, topic1, 1);
        fx.admin.createTopic(MixAll::DEFAULT_TOPIC, topic2, 1);
        std::this_thread::sleep_for(std::chrono::seconds(3));
        std::printf("topic1=%s topic2=%s (1 queue each, %d messages each)\n", topic1.c_str(),
                    topic2.c_str(), kMsgs);

        s1BackwardReset(fx, topic1, g1);
        s2ForwardSkip(fx, topic2, g2);
    } catch (const std::exception& e) {
        check("the verification threw an exception", false, e.what());
    }

    for (const std::string& t : {topic1, topic2}) {
        try {
            fx.admin.deleteTopic(t);
            std::printf("    (deleteTopic(%s) OK)\n", t.c_str());
        } catch (const std::exception& e) {
            std::printf("    (deleteTopic(%s) failed: %s)\n", t.c_str(), e.what());
        }
    }
    for (const std::string& g : {g1, g2}) {
        try {
            fx.admin.deleteSubscriptionGroup(fx.brokerAddr, g, true);
        } catch (const std::exception& e) {
            std::printf("    (deleteSubscriptionGroup(%s) failed: %s)\n", g.c_str(), e.what());
        }
    }
    check("Restore useServerSideResetOffset=true", setServerSideReset(fx, "true"),
          "read back useServerSideResetOffset=" + readFlag(fx.admin, fx.brokerAddr, kSwitchKey));
    fx.shutdown();

    std::printf("\nPASS=%d FAIL=%d\n", gPass, gFail);
    return gFail == 0 ? 0 : 1;
}
