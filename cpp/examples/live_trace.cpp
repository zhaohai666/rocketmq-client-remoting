// Prerequisite: NameServer + Broker are up and the broker config has **traceTopicEnable=true**
// (otherwise RMQ_SYS_TRACE_TOPIC is not pre-created and admin cannot create it either — it is a system
// topic, and validateSystemTopicWhenUpdateTopic defaults to true and would refuse the creation).
//
// Scenarios S1–S17 map one-to-one to the checks at the end of this file; returns 0 only when all pass.
#include <atomic>
#include <chrono>
#include <cstdio>
#include <functional>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "rocketmq/client/consumer.h"
#include "rocketmq/client/producer.h"
#include "rocketmq/client/trace.h"
#include "rocketmq/common/message.h"
#include "rocketmq/common/message_const.h"
#include "rocketmq/common/mix_all.h"
#include "rocketmq/common/util_all.h"
#include "rocketmq/remoting/protocol/heartbeat.h"

using namespace rocketmq;

namespace {

int32_t gPass = 0;
int32_t gFail = 0;

void check(const std::string& name, bool ok, const std::string& detail = std::string()) {
    if (ok) {
        ++gPass;
        std::printf("  [PASS] %s%s\n", name.c_str(), detail.empty() ? "" : ("  " + detail).c_str());
    } else {
        ++gFail;
        std::printf("  [FAIL] %s%s\n", name.c_str(), detail.empty() ? "" : ("  " + detail).c_str());
    }
}

bool waitUntil(const std::function<bool()>& pred, int32_t timeoutMs) {
    int64_t deadline = UtilAll::currentTimeMillis() + timeoutMs;
    while (UtilAll::currentTimeMillis() < deadline) {
        if (pred()) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }
    return pred();
}

Bytes bytesOf(const std::string& s) { return Bytes(s.begin(), s.end()); }

std::string bodyText(const MessageExt& msg) {
    return std::string(msg.body.begin(), msg.body.end());
}

int64_t countOf(const std::string& s, const std::string& sub) {
    int64_t n = 0;
    size_t pos = 0;
    while ((pos = s.find(sub, pos)) != std::string::npos) {
        ++n;
        pos += sub.size();
    }
    return n;
}

// Consumes RMQ_SYS_TRACE_TOPIC, decodes the trace text into TraceContext and accumulates the records
class TraceCollector : public MessageListenerConcurrently {
public:
    ConsumeConcurrentlyStatus consumeMessage(const std::vector<MessageExt>& msgs,
                                            ConsumeConcurrentlyContext&) override {
        std::lock_guard<std::mutex> lk(mtx_);
        for (const MessageExt& msg : msgs) {
            std::string text = bodyText(msg);
            raw_.push_back({msg.getKeys(), msg.topic, text});
            try {
                std::vector<TraceContext> decoded =
                    TraceDataEncoder::decoderFromTraceDataString(text);
                for (TraceContext& c : decoded) records_.push_back(std::move(c));
            } catch (const std::exception& e) {
                std::printf("  [warn] decode trace failed: %s\n", e.what());
            }
        }
        return ConsumeConcurrentlyStatus::CONSUME_SUCCESS;
    }

    struct Raw {
        std::string keys;
        std::string topic;
        std::string text;
    };

    std::vector<TraceContext> records() {
        std::lock_guard<std::mutex> lk(mtx_);
        return records_;
    }
    std::vector<Raw> raw() {
        std::lock_guard<std::mutex> lk(mtx_);
        return raw_;
    }
    size_t rawCount() {
        std::lock_guard<std::mutex> lk(mtx_);
        return raw_.size();
    }
    bool hasPub(const std::string& msgId) {
        std::lock_guard<std::mutex> lk(mtx_);
        for (const TraceContext& c : records_) {
            if (c.traceType == TraceType::PUB && !c.traceBeans.empty() &&
                c.traceBeans[0].msgId == msgId) {
                return true;
            }
        }
        return false;
    }

private:
    std::mutex mtx_;
    std::vector<TraceContext> records_;
    std::vector<Raw> raw_;
};

// Collects only business messages (for verifying the consumer side)
class MsgCollector : public MessageListenerConcurrently {
public:
    ConsumeConcurrentlyStatus consumeMessage(const std::vector<MessageExt>& msgs,
                                            ConsumeConcurrentlyContext&) override {
        std::lock_guard<std::mutex> lk(mtx_);
        for (const MessageExt& m : msgs) msgs_.push_back(m);
        return ConsumeConcurrentlyStatus::CONSUME_SUCCESS;
    }
    std::vector<MessageExt> snapshot() {
        std::lock_guard<std::mutex> lk(mtx_);
        return msgs_;
    }
    size_t count() {
        std::lock_guard<std::mutex> lk(mtx_);
        return msgs_.size();
    }

private:
    std::mutex mtx_;
    std::vector<MessageExt> msgs_;
};

}  // namespace

int main(int argc, char** argv) {
    std::string namesrv = argc > 1 ? argv[1] : "127.0.0.1:9876";
    int64_t stamp = UtilAll::currentTimeMillis();
    // Both the topic and the business consumer group carry the stamp: the per-language live tools run **sequentially** against the
    // same broker, and fixed names would let the second language inherit the previous run's offsets and leftover messages, making assertion
    // results depend on run order. A fresh topic + fresh group keeps every run's start identical (real robustness comes from elsewhere, see the body filter in S6).
    const std::string TOPIC = "TraceTopicLive_" + std::to_string(stamp);
    const std::string NOTRACE_TOPIC = "TraceNoTraceTopic_" + std::to_string(stamp);
    const std::string PRODUCER_GROUP = "GID_trace_producer_live";
    const std::string CONSUMER_GROUP = "GID_trace_live_" + std::to_string(stamp);
    const std::string TRACE_READER_GROUP = "GID_trace_reader_live_" + std::to_string(stamp);

    std::printf("=== RocketMQ message trace live verify (cpp) ===\n");
    std::printf("namesrv=%s topic=%s\n", namesrv.c_str(), TOPIC.c_str());

    // ---------------- trace reader (must be up before the actions under test) ----------------
    auto collector = std::make_shared<TraceCollector>();
    DefaultMQPushConsumer traceReader(TRACE_READER_GROUP);
    traceReader.setNamesrvAddr(namesrv);
    traceReader.setConsumeFromWhere(ConsumeFromWhere::CONSUME_FROM_FIRST_OFFSET);
    traceReader.subscribe(MixAll::TRACE_TOPIC, "*");
    traceReader.setMessageListener(collector);
    traceReader.start();
    std::printf("trace reader started (group=%s, topic=%s)\n", TRACE_READER_GROUP.c_str(),
                MixAll::TRACE_TOPIC);

    // ---------------- S1 warm up to create the topic ----------------
    DefaultMQProducer warm(PRODUCER_GROUP + "_warm");
    warm.setNamesrvAddr(namesrv);
    warm.start();
    try {
        warm.send(Message(TOPIC, bytesOf("warm-up")), 5000);
        check("S1 warm-up message sent OK (triggers broker auto-creating the topic)", true);
    } catch (const std::exception& e) {
        check("S1 warm-up message sent OK (triggers broker auto-creating the topic)", false, e.what());
        return 1;
    }

    // After warm-up wait a moment before starting the consumer: lowers the race between "offset resolution" and
    // "broker consumequeue async dispatch". This step only widens the timing margin; it is **not** a correctness guarantee — the warm-up message may well be delivered, see the body filter in S6.
    std::this_thread::sleep_for(std::chrono::milliseconds(1500));

    // ---------------- S2 business consumer with tracing on ----------------
    auto msgCollector = std::make_shared<MsgCollector>();
    DefaultMQPushConsumer consumer(CONSUMER_GROUP);
    consumer.setNamesrvAddr(namesrv);
    // See the ⚠ note at the file head: must start from the queue tail, otherwise it eats the keyless warm-up message
    consumer.setConsumeFromWhere(ConsumeFromWhere::CONSUME_FROM_LAST_OFFSET);
    consumer.subscribe(TOPIC, "*");
    consumer.setMessageListener(msgCollector);
    consumer.setEnableMsgTrace(true);  // ← consumer-side trace
    consumer.start();
    bool assigned = waitUntil([&]() { return !consumer.assignedQueueKeys().empty(); }, 30000);
    check("S2 consumer has been assigned queues (consumer started before sending)", assigned,
          "assigned=" + std::to_string(consumer.assignedQueueKeys().size()));

    DefaultMQProducer producer(PRODUCER_GROUP);
    producer.setNamesrvAddr(namesrv);
    producer.setEnableTrace(true);  // ← producer-side trace
    producer.start();

    std::string body = "trace-live-" + std::to_string(stamp);
    std::string keys = "KeyA KeyB " + std::to_string(stamp);
    SendResult result = producer.send(Message(TOPIC, "TagA", keys, bytesOf(body)), 5000);

    check("S3 SendResult.traceOn == true (broker defaults traceOn=true)", result.traceOn,
          std::string("traceOn=") + (result.traceOn ? "true" : "false"));
    check("S4 SendResult.msgId is the client UNIQ_KEY and differs from offsetMsgId",
          !result.msgId.empty() && !result.offsetMsgId.empty() && result.msgId != result.offsetMsgId,
          "msgId=" + result.msgId + " offsetMsgId=" + result.offsetMsgId);
    check("S5 SendResult.regionId resolved (DefaultRegion when absent)",
          result.regionId == TraceConstants::DEFAULT_TRACE_REGION_ID,
          "region=" + result.regionId);

    // ---------------- S6/S7 consumer side ----------------
    // ⚠ Only count **this send's** message: the topic necessarily still holds S1's warm-up message, and the
    // consumer's start offset for a "first-time group" is the maxOffset at that moment — the broker's consumequeue
    // dispatches asynchronously, so the offset may land before the warm-up message and it gets delivered too (all per-language tools have hit this).
    // The assertion means "this business message was consumed", so filter by body instead of betting the topic holds exactly one message.
    auto isPrimary = [](const MessageExt& m) { return bodyText(m).rfind("trace-live-", 0) == 0; };
    bool got = waitUntil([&]() {
        for (const MessageExt& m : msgCollector->snapshot()) {
            if (isPrimary(m)) return true;
        }
        return false;
    }, 25000);
    std::vector<MessageExt> msgs;
    for (const MessageExt& m : msgCollector->snapshot()) {
        if (isPrimary(m)) msgs.push_back(m);
    }
    check("S6 business consumer received exactly the 1 message sent now (warm-up messages filtered out by body)",
          got && msgs.size() == 1, "count=" + std::to_string(msgs.size()));
    std::string recvMsgId = msgs.empty() ? std::string() : msgs[0].msgId;
    std::string recvUniq =
        msgs.empty()
            ? std::string()
            : msgs[0].getProperty(MessageConst::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX);
    check("S7 consumer-side msg_id == SendResult.offset_msg_id, and UNIQ_KEY reaches the consumer side via property",
          !msgs.empty() && recvMsgId == result.offsetMsgId && recvUniq == result.msgId,
          "recv=" + recvMsgId + " offset=" + result.offsetMsgId + " uniq=" + recvUniq);

    // ---------------- keyless message (lets S17 lock decoder robustness with live-cluster data) ----------------
    SendResult keyless = producer.send(Message(TOPIC, bytesOf("no-keys-here")), 5000);
    bool keylessOk = waitUntil([&]() {
        for (const MessageExt& m : msgCollector->snapshot()) {
            if (bodyText(m) == "no-keys-here") return true;
        }
        return false;
    }, 25000);
    std::string keylessId = keyless.offsetMsgId;

    // Shut the consumer down → triggers the consumer-side trace dispatcher flush, ensuring SubBefore/SubAfter land
    consumer.shutdown();
    std::this_thread::sleep_for(std::chrono::milliseconds(1000));

    // ---------------- S8 wait for the Pub trace to arrive ----------------
    bool pubOk = waitUntil([&]() { return collector->hasPub(result.msgId); }, 35000);
    producer.shutdown();  // flush producer-side traces
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    pubOk = waitUntil([&]() { return collector->hasPub(result.msgId); }, 10000) || pubOk;

    std::vector<TraceContext> records = collector->records();
    std::vector<TraceCollector::Raw> raw = collector->raw();
    std::printf("  [info] received %zu trace messages / decoded %zu trace records\n", raw.size(), records.size());
    for (const TraceContext& r : records) {
        std::printf("         - %s topic=%s msgId=%s group=%s success=%s code=%d\n",
                    traceTypeName(r.traceType.value_or(TraceType::PUB)),
                    r.traceBeans.empty() ? "-" : r.traceBeans[0].topic.c_str(),
                    r.traceBeans.empty() ? "-" : r.traceBeans[0].msgId.c_str(),
                    r.groupName.c_str(), r.isSuccess ? "true" : "false", r.contextCode);
    }

    int32_t pubCount = 0;
    const TraceContext* pub = nullptr;
    for (const TraceContext& r : records) {
        if (r.traceType == TraceType::PUB && !r.traceBeans.empty() &&
            r.traceBeans[0].msgId == result.msgId) {
            ++pubCount;
            if (pub == nullptr) pub = &r;
        }
    }
    check("S8 a Pub record appears in the traces with msgId matching SendResult.msg_id", pubOk && pubCount >= 1,
          "count=" + std::to_string(pubCount));
    check("S9 Pub trace topic / groupName are correct",
          pub != nullptr && pub->traceBeans[0].topic == TOPIC && pub->groupName == PRODUCER_GROUP,
          pub == nullptr ? "no pub record"
                         : ("topic=" + pub->traceBeans[0].topic + " group=" + pub->groupName));
    {
        bool keyHit = false;
        for (const TraceCollector::Raw& r : raw) {
            if (r.keys.find(result.msgId) != std::string::npos) keyHit = true;
        }
        check("S10 the trace message carrying the Pub trace has this msgId in its keys (console reverse lookup by keys)", keyHit,
              "msgId=" + result.msgId);
    }

    int32_t subBeforeCount = 0;
    int32_t subAfterCount = 0;
    const TraceContext* subBefore = nullptr;
    const TraceContext* subAfter = nullptr;
    for (const TraceContext& r : records) {
        if (r.traceBeans.empty() || r.traceBeans[0].msgId != recvMsgId) continue;
        if (r.traceType == TraceType::SUB_BEFORE) {
            ++subBeforeCount;
            if (subBefore == nullptr) subBefore = &r;
        } else if (r.traceType == TraceType::SUB_AFTER) {
            ++subAfterCount;
            if (subAfter == nullptr) subAfter = &r;
        }
    }
    check("S11 a SubBefore appears in the traces with msgId matching the consumer side", subBeforeCount >= 1,
          "count=" + std::to_string(subBeforeCount));
    check("S12 SubBefore/SubAfter are paired with the same requestId, success=true, contextCode=0",
          subBefore != nullptr && subAfter != nullptr &&
              subAfter->requestId == subBefore->requestId && subAfter->isSuccess &&
              subAfter->contextCode == 0,
          subAfter == nullptr ? "no sub_after"
                              : ("req=" + subAfter->requestId + " success=" +
                                 (subAfter->isSuccess ? "true" : "false") + " code=" +
                                 std::to_string(subAfter->contextCode)));
    check("S13 SubBefore's retryTimes matches the consumed message",
          subBefore != nullptr && !msgs.empty() &&
              subBefore->traceBeans[0].retryTimes == msgs[0].reconsumeTimes,
          subBefore == nullptr ? "no sub traces"
                               : ("trace=" + std::to_string(subBefore->traceBeans[0].retryTimes) +
                                  " msg=" + std::to_string(msgs.empty() ? -1
                                                                        : msgs[0].reconsumeTimes)));

    // ---------------- S14 recursion guard ----------------
    {
        bool leak = false;
        for (const TraceContext& r : records) {
            if (!r.traceBeans.empty() && r.traceBeans[0].topic == MixAll::TRACE_TOPIC) leak = true;
        }
        check("S14 no trace record's topic is the trace topic itself (recursion guard)", !leak);
    }

    // ---------------- S15 tracing off produces no traces ----------------
    DefaultMQProducer quiet(PRODUCER_GROUP + "_quiet");
    quiet.setNamesrvAddr(namesrv);
    quiet.setEnableTrace(false);  // ← explicitly disabled
    quiet.start();
    quiet.send(Message(NOTRACE_TOPIC, bytesOf("no-trace")), 5000);
    quiet.shutdown();
    std::this_thread::sleep_for(std::chrono::milliseconds(6500));
    {
        int32_t leaked = 0;
        for (const TraceContext& r : collector->records()) {
            if (r.traceType == TraceType::PUB && !r.traceBeans.empty() &&
                r.traceBeans[0].topic == NOTRACE_TOPIC) {
                ++leaked;
            }
        }
        check("S15 a producer with enable_trace=false produces no Pub trace", leaked == 0,
              "leaked=" + std::to_string(leaked));
    }

    // ---------------- S16 encoding structure self-check ----------------
    {
        // Every record ends with FIELD_SPLITOR → the record count equals the number of STX occurrences.
        // Value of this assertion: if the decoder ever dropped any record (hit on live clusters), this goes red immediately.
        int64_t segments = 0;
        for (const TraceCollector::Raw& r : raw) {
            if (r.topic == MixAll::TRACE_TOPIC) segments += countOf(r.text, TraceConstants::FIELD_SPLITOR);
        }
        check("S16 trace-text record count (STX count) == decoded record count (split semantics)",
              segments == static_cast<int64_t>(records.size()) && !records.empty(),
              "segments=" + std::to_string(segments) +
                  " decoded=" + std::to_string(records.size()));
    }

    // ---------------- S17 keyless message traces must also decode ----------------
    {
        int32_t before = 0;
        std::string keylessKeys;
        for (const TraceContext& r : records) {
            if (r.traceType == TraceType::SUB_BEFORE && !r.traceBeans.empty() &&
                r.traceBeans[0].msgId == keylessId) {
                ++before;
                keylessKeys = r.traceBeans[0].keys;
            }
        }
        check("S17 the keyless message also decodes a SubBefore trace (empty keys segment does not crash)",
              keylessOk && before >= 1 && keylessKeys.empty(),
              "consumed=" + std::string(keylessOk ? "true" : "false") +
                  " before=" + std::to_string(before));
    }

    traceReader.shutdown();
    warm.shutdown();

    std::printf("########################################\n");
    std::printf("PASS=%d FAIL=%d\n", gPass, gFail);
    return gFail == 0 ? 0 : 1;
}
