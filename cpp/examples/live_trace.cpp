// 消息轨迹真机验证（对齐 Java client.trace + python/verify_trace_live.py）。
// 用法：rmq_live_trace [127.0.0.1:9876]
//
// 前置：NameServer + Broker 已起，且 broker 配置 **traceTopicEnable=true**
// （否则 RMQ_SYS_TRACE_TOPIC 不会被预建，也没法用 admin 建 —— 它是系统 topic，
// validateSystemTopicWhenUpdateTopic 默认 true 会拒绝创建）。
//
// 场景 S1–S17 与本文件末尾的 check 一一对应，全部命中才返回 0。
//
// ⚠ 两条真机踩出来的硬约定（改脚本时勿回退）：
//   1. 业务消费者必须 CONSUME_FROM_LAST_OFFSET：S1 的预热消息**没有 keys**，
//      用 FIRST_OFFSET 会被一起吃掉，既脏了计数断言，又会多出一条 keys 为空的 SubBefore；
//   2. 消费侧 MessageExt.msgId 是 **offset 基 ID**（Java MessageDecoder:557-561 先 setMsgId
//      再 setOffsetMsgId，同值），所以它能对齐的是 SendResult.offsetMsgId，
//      **不是** SendResult.msgId（UNIQ_KEY）。
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

// 消费 RMQ_SYS_TRACE_TOPIC，把轨迹文本解成 TraceContext 累积起来
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

// 只收集业务消息（验证消费侧）
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
    // topic 与业务消费组都带 stamp：`run_trace_live.sh all` 会让三语言**依次**跑在同一个 broker 上，
    // 固定名会让第二个语言继承上一轮的位点与残留消息，使断言结果依赖运行顺序。全新 topic + 全新组
    // 才能让每次运行的起点一致（真正的鲁棒性另有保障，见 S6 的 body 过滤）。
    const std::string TOPIC = "TraceTopicLive_" + std::to_string(stamp);
    const std::string NOTRACE_TOPIC = "TraceNoTraceTopic_" + std::to_string(stamp);
    const std::string PRODUCER_GROUP = "GID_trace_producer_live";
    const std::string CONSUMER_GROUP = "GID_trace_live_" + std::to_string(stamp);
    const std::string TRACE_READER_GROUP = "GID_trace_reader_live_" + std::to_string(stamp);

    std::printf("=== RocketMQ message trace live verify (cpp) ===\n");
    std::printf("namesrv=%s topic=%s\n", namesrv.c_str(), TOPIC.c_str());

    // ---------------- 轨迹读取者（必须在被测动作之前起来）----------------
    auto collector = std::make_shared<TraceCollector>();
    DefaultMQPushConsumer traceReader(TRACE_READER_GROUP);
    traceReader.setNamesrvAddr(namesrv);
    traceReader.setConsumeFromWhere(ConsumeFromWhere::CONSUME_FROM_FIRST_OFFSET);
    traceReader.subscribe(MixAll::TRACE_TOPIC, "*");
    traceReader.setMessageListener(collector);
    traceReader.start();
    std::printf("trace reader started (group=%s, topic=%s)\n", TRACE_READER_GROUP.c_str(),
                MixAll::TRACE_TOPIC);

    // ---------------- S1 预热建 topic ----------------
    DefaultMQProducer warm(PRODUCER_GROUP + "_warm");
    warm.setNamesrvAddr(namesrv);
    warm.start();
    try {
        warm.send(Message(TOPIC, bytesOf("warm-up")), 5000);
        check("S1 预热消息发送成功（触发 broker 自动建 topic）", true);
    } catch (const std::exception& e) {
        check("S1 预热消息发送成功（触发 broker 自动建 topic）", false, e.what());
        return 1;
    }

    // 预热后等一下再起消费者：降低"位点解析"与"broker consumequeue 异步分发"的竞态。
    // 这一步只是让时序更宽裕，**不是**正确性保障 —— 预热消息本来就可能被投递，见 S6 的 body 过滤。
    std::this_thread::sleep_for(std::chrono::milliseconds(1500));

    // ---------------- S2 开轨迹的业务消费者 ----------------
    auto msgCollector = std::make_shared<MsgCollector>();
    DefaultMQPushConsumer consumer(CONSUMER_GROUP);
    consumer.setNamesrvAddr(namesrv);
    // 见文件头 ⚠ 1：必须从队尾起算，否则会吃掉无 keys 的预热消息
    consumer.setConsumeFromWhere(ConsumeFromWhere::CONSUME_FROM_LAST_OFFSET);
    consumer.subscribe(TOPIC, "*");
    consumer.setMessageListener(msgCollector);
    consumer.setEnableMsgTrace(true);  // ← 消费侧轨迹
    consumer.start();
    bool assigned = waitUntil([&]() { return !consumer.assignedQueueKeys().empty(); }, 30000);
    check("S2 消费者已分配到队列（先起消费者再发消息）", assigned,
          "assigned=" + std::to_string(consumer.assignedQueueKeys().size()));

    DefaultMQProducer producer(PRODUCER_GROUP);
    producer.setNamesrvAddr(namesrv);
    producer.setEnableTrace(true);  // ← 发送侧轨迹
    producer.start();

    std::string body = "trace-live-" + std::to_string(stamp);
    std::string keys = "KeyA KeyB " + std::to_string(stamp);
    SendResult result = producer.send(Message(TOPIC, "TagA", keys, bytesOf(body)), 5000);

    check("S3 SendResult.traceOn == true（broker 默认 traceOn=true）", result.traceOn,
          std::string("traceOn=") + (result.traceOn ? "true" : "false"));
    check("S4 SendResult.msgId 是客户端 UNIQ_KEY，且与 offsetMsgId 不同",
          !result.msgId.empty() && !result.offsetMsgId.empty() && result.msgId != result.offsetMsgId,
          "msgId=" + result.msgId + " offsetMsgId=" + result.offsetMsgId);
    check("S5 SendResult.regionId 已解析（缺省 DefaultRegion）",
          result.regionId == TraceConstants::DEFAULT_TRACE_REGION_ID,
          "region=" + result.regionId);

    // ---------------- S6/S7 消费侧 ----------------
    // ⚠ 只统计**本次发送的**消息：topic 上必然还留着 S1 的预热消息，且消费者的起始位点
    // 在"组首次消费"时取的是当时的 maxOffset —— broker 的 consumequeue 是异步分发的，
    // 位点可能落在预热消息之前，于是预热消息也会被投递过来（实测三语言都踩过）。
    // 断言的本意是"本次业务消息被消费到"，所以按 body 过滤，而不是赌 topic 上只有一条消息。
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
    check("S6 业务消费者收到本次发送的那 1 条消息（按 body 过滤预热消息）",
          got && msgs.size() == 1, "count=" + std::to_string(msgs.size()));
    std::string recvMsgId = msgs.empty() ? std::string() : msgs[0].msgId;
    std::string recvUniq =
        msgs.empty()
            ? std::string()
            : msgs[0].getProperty(MessageConst::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX);
    check("S7 消费侧 msg_id == SendResult.offset_msg_id，且 UNIQ_KEY 经属性带到消费侧",
          !msgs.empty() && recvMsgId == result.offsetMsgId && recvUniq == result.msgId,
          "recv=" + recvMsgId + " offset=" + result.offsetMsgId + " uniq=" + recvUniq);

    // ---------------- 无 keys 的消息（供 S17 用真机数据锁解码器健壮性）----------------
    SendResult keyless = producer.send(Message(TOPIC, bytesOf("no-keys-here")), 5000);
    bool keylessOk = waitUntil([&]() {
        for (const MessageExt& m : msgCollector->snapshot()) {
            if (bodyText(m) == "no-keys-here") return true;
        }
        return false;
    }, 25000);
    std::string keylessId = keyless.offsetMsgId;

    // 关掉消费者 → 触发消费侧轨迹分发器 flush，保证 SubBefore/SubAfter 落盘
    consumer.shutdown();
    std::this_thread::sleep_for(std::chrono::milliseconds(1000));

    // ---------------- S8 等 Pub 轨迹落地 ----------------
    bool pubOk = waitUntil([&]() { return collector->hasPub(result.msgId); }, 35000);
    producer.shutdown();  // flush 发送侧轨迹
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    pubOk = waitUntil([&]() { return collector->hasPub(result.msgId); }, 10000) || pubOk;

    std::vector<TraceContext> records = collector->records();
    std::vector<TraceCollector::Raw> raw = collector->raw();
    std::printf("  [info] 收到 %zu 条轨迹消息 / 解出 %zu 条轨迹记录\n", raw.size(), records.size());
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
    check("S8 轨迹里出现 Pub 记录且 msgId 与 SendResult.msg_id 一致", pubOk && pubCount >= 1,
          "count=" + std::to_string(pubCount));
    check("S9 Pub 轨迹的 topic / groupName 正确",
          pub != nullptr && pub->traceBeans[0].topic == TOPIC && pub->groupName == PRODUCER_GROUP,
          pub == nullptr ? "no pub record"
                         : ("topic=" + pub->traceBeans[0].topic + " group=" + pub->groupName));
    {
        bool keyHit = false;
        for (const TraceCollector::Raw& r : raw) {
            if (r.keys.find(result.msgId) != std::string::npos) keyHit = true;
        }
        check("S10 承载 Pub 轨迹的轨迹消息 keys 里含该 msgId（控制台按 keys 反查）", keyHit,
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
    check("S11 轨迹里出现 SubBefore 且 msgId 与消费侧一致", subBeforeCount >= 1,
          "count=" + std::to_string(subBeforeCount));
    check("S12 SubBefore/SubAfter 配对且 requestId 一致、success=true、contextCode=0",
          subBefore != nullptr && subAfter != nullptr &&
              subAfter->requestId == subBefore->requestId && subAfter->isSuccess &&
              subAfter->contextCode == 0,
          subAfter == nullptr ? "no sub_after"
                              : ("req=" + subAfter->requestId + " success=" +
                                 (subAfter->isSuccess ? "true" : "false") + " code=" +
                                 std::to_string(subAfter->contextCode)));
    check("S13 SubBefore 的 retryTimes 与被消费消息一致",
          subBefore != nullptr && !msgs.empty() &&
              subBefore->traceBeans[0].retryTimes == msgs[0].reconsumeTimes,
          subBefore == nullptr ? "no sub traces"
                               : ("trace=" + std::to_string(subBefore->traceBeans[0].retryTimes) +
                                  " msg=" + std::to_string(msgs.empty() ? -1
                                                                        : msgs[0].reconsumeTimes)));

    // ---------------- S14 防递归 ----------------
    {
        bool leak = false;
        for (const TraceContext& r : records) {
            if (!r.traceBeans.empty() && r.traceBeans[0].topic == MixAll::TRACE_TOPIC) leak = true;
        }
        check("S14 没有任何轨迹记录的 topic 是轨迹 topic 本身（防递归）", !leak);
    }

    // ---------------- S15 关闭轨迹就不产轨迹 ----------------
    DefaultMQProducer quiet(PRODUCER_GROUP + "_quiet");
    quiet.setNamesrvAddr(namesrv);
    quiet.setEnableTrace(false);  // ← 显式关闭
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
        check("S15 enable_trace=false 的生产者不产生 Pub 轨迹", leaked == 0,
              "leaked=" + std::to_string(leaked));
    }

    // ---------------- S16 编码结构自检 ----------------
    {
        // 每条记录都以 FIELD_SPLITOR 结尾 → 记录数就是 STX 出现次数。
        // 这条断言的价值：解码器只要丢过任何一条记录（真机踩过），这里立刻红。
        int64_t segments = 0;
        for (const TraceCollector::Raw& r : raw) {
            if (r.topic == MixAll::TRACE_TOPIC) segments += countOf(r.text, TraceConstants::FIELD_SPLITOR);
        }
        check("S16 轨迹文本记录数（STX 计数）== 解码出的记录数（Java split 语义）",
              segments == static_cast<int64_t>(records.size()) && !records.empty(),
              "segments=" + std::to_string(segments) +
                  " decoded=" + std::to_string(records.size()));
    }

    // ---------------- S17 无 keys 消息的轨迹也要解得出来 ----------------
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
        check("S17 无 keys 的消息也能解出 SubBefore 轨迹（空 keys 段不崩）",
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
