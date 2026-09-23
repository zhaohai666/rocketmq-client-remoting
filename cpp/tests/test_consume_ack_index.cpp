// classic 并发消费路径的 ackIndex 语义单测 —— 不需要集群。
//
// 为什么必须离线锁死：这条路径错了是**静默丢消息**。Java
// ConsumeMessageConcurrentlyService#processConsumeResult:207-269 用 listener 写的
// ackIndex 把本批切成「已认可前缀 / 待回投后缀」，而 RECONSUME_LATER 会强制 ackIndex=-1
//（整批回投）。两个方向写错在真机短期窗口里都看不出差别：
//   - 忽略 ackIndex：尾巴既没回投也没重投，直接丢；
//   - 默认值写成 -1：CONSUME_SUCCESS 也把整批回投，消息无限重复。
// 未 start 的消费者 sendMessageBack 必定失败，所以这里能锁的是「失败分支」
//（回投失败 → 塞回队首 + 位点不越过它）；「回投成功 → 位点整批前进」只能靠真机
//（examples/live_redelivery.cpp 的 S10）。与 Python/Rust 的同名测试一一对应。
#include <cstdio>
#include <limits>
#include <string>
#include <vector>

#include "rocketmq/client/consumer.h"
#include "rocketmq/client/exception.h"
#include "rocketmq/client/result.h"
#include "rocketmq/common/message.h"

using namespace rocketmq;

namespace {

int fails = 0;
int checks = 0;

void expect(bool ok, const std::string& name, const std::string& detail = "") {
    ++checks;
    if (!ok) {
        ++fails;
        std::printf("FAIL %s %s\n", name.c_str(), detail.c_str());
    }
}

const char* kGroup = "GID_AckIndexCppUnit";
const char* kTopic = "AckIndexCppUnitTopic";
const char* kBroker = "broker-a";

MessageQueue queue0() {
    return MessageQueue(kTopic, kBroker, 0);
}

MessageExt ext(int64_t queueOffset, int32_t reconsumeTimes = 0) {
    MessageExt m;
    m.topic = kTopic;
    m.brokerName = kBroker;
    m.queueId = 0;
    m.queueOffset = queueOffset;
    m.reconsumeTimes = reconsumeTimes;
    m.body = "x";
    return m;
}

std::vector<MessageExt> offsetBatch(size_t n) {
    std::vector<MessageExt> out;
    for (size_t i = 0; i < n; i++) {
        out.push_back(ext(static_cast<int64_t>(i)));
    }
    return out;
}

// 按 Java listener 契约：可选地把 ackIndex 收窄，并返回指定状态或直接抛异常。
class AckListener : public MessageListenerConcurrently {
public:
    AckListener(ConsumeConcurrentlyStatus status, int32_t ackIndex, bool thrown)
        : status_(status), ackIndex_(ackIndex), thrown_(thrown) {}

    ConsumeConcurrentlyStatus consumeMessage(const std::vector<MessageExt>&,
                                             ConsumeConcurrentlyContext& context) override {
        if (ackIndex_ != kNoAckIndex) {
            context.ackIndex = ackIndex_;
        }
        if (thrown_) {
            throw MQClientException("listener blew up");
        }
        return status_;
    }

    static const int32_t kNoAckIndex = std::numeric_limits<int32_t>::min();

private:
    ConsumeConcurrentlyStatus status_;
    int32_t ackIndex_;
    bool thrown_;
};

// 一把「未 start」的消费者：sendMessageBack 必定失败（client() 抛 → 内部吞掉 → 返回
// false），正是「回投失败不能推进位点」这条分支的夹具。
class Harness {
public:
    explicit Harness(const std::string& model = MessageModel::CLUSTERING)
        : consumer_(kGroup), key_(DefaultMQPushConsumer::offsetKey(queue0())) {
        consumer_.setMessageModel(model);
        // 真机里 dispatchLoop 是「先从缓冲取走本批、再消费」，所以 key 一定还在表里
        //（可能还有余量）。回投失败要塞回的正是这张表；不预置就等于队列已被撤走。
        consumer_.setPendingMessages(key_, {});
    }

    // 返回 consumeBatch 的结果（true = 位点前进了）
    bool run(const std::vector<MessageExt>& batch, ConsumeConcurrentlyStatus status,
             int32_t ackIndex, bool thrown = false) {
        consumer_.setMessageListener(
            std::make_shared<AckListener>(status, ackIndex, thrown));
        return consumer_.consumeBatch(key_, queue0(), batch);
    }

    std::vector<MessageExt> pending() const { return consumer_.pendingMessages(key_); }
    std::optional<int64_t> offset() const { return consumer_.consumeOffset(key_); }
    int64_t consumedCount() const { return consumer_.consumedCount(); }

    // 队首缓冲的 (queueOffset, reconsumeTimes) 列表，便于一次性比对
    std::vector<std::pair<int64_t, int32_t>> pendingShape() const {
        std::vector<std::pair<int64_t, int32_t>> out;
        for (const MessageExt& m : pending()) {
            out.emplace_back(m.queueOffset, m.reconsumeTimes);
        }
        return out;
    }

    static std::string shapeText(const std::vector<std::pair<int64_t, int32_t>>& shape) {
        std::string s = "[";
        for (size_t i = 0; i < shape.size(); i++) {
            if (i) s += ",";
            s += std::to_string(shape[i].first) + ":" + std::to_string(shape[i].second);
        }
        return s + "]";
    }

private:
    DefaultMQPushConsumer consumer_;
    std::string key_;
};

void expectShape(const Harness& h, const std::vector<std::pair<int64_t, int32_t>>& want,
                 const std::string& name) {
    const auto got = h.pendingShape();
    expect(got == want, name, "want=" + Harness::shapeText(want) + " got=" + Harness::shapeText(got));
}

void expectOffset(const Harness& h, int64_t want, const std::string& name) {
    const auto got = h.offset();
    expect(got.has_value() && *got == want, name,
           "want=" + std::to_string(want) + " got=" + (got ? std::to_string(*got) : "none"));
}

void testContextDefault() {
    // Java ConsumeConcurrentlyContext.ackIndex = Integer.MAX_VALUE（= 整批认可）。
    // 默认值写成 -1 会让每次 CONSUME_SUCCESS 都整批回投 —— 消息无限重复。
    ConsumeConcurrentlyContext ctx(queue0());
    expect(ctx.ackIndex == std::numeric_limits<int32_t>::max(), "default.maxValue",
           std::to_string(ctx.ackIndex));
    expect(ctx.delayLevelWhenNextConsume == 0, "default.delayLevel");
}

void testConsumeSuccess() {
    {
        // 默认（不碰 ackIndex）：一条都不回投，位点整批前进
        Harness h;
        const bool advanced = h.run(offsetBatch(3), ConsumeConcurrentlyStatus::CONSUME_SUCCESS,
                                    AckListener::kNoAckIndex);
        expect(advanced, "success.defaultAck.advanced");
        expect(h.pendingShape().empty(), "success.defaultAck.noRequeue",
               Harness::shapeText(h.pendingShape()));
        expectOffset(h, 3, "success.defaultAck.offset");
        expect(h.consumedCount() == 3, "success.defaultAck.count",
               std::to_string(h.consumedCount()));
    }
    {
        // ackIndex 越界（listener 写了 99）：Java 钳到 size-1，等价整批认可
        Harness h;
        h.run(offsetBatch(3), ConsumeConcurrentlyStatus::CONSUME_SUCCESS, 99);
        expect(h.pendingShape().empty(), "success.clamp.noRequeue",
               Harness::shapeText(h.pendingShape()));
        expectOffset(h, 3, "success.clamp.offset");
    }
    {
        // 部分 ack（ackIndex=0 认可第 0 条）：尾巴 [1,2] 回投；离线回投必败 →
        // 塞回队首（reconsumeTimes +1），位点钳在第 1 条不越过它（Java removeMessage
        // 返回 firstKey），已认可的第 0 条计入消费量
        Harness h;
        const bool advanced = h.run(offsetBatch(3), ConsumeConcurrentlyStatus::CONSUME_SUCCESS, 0);
        expect(!advanced, "success.partial.notAdvanced");
        expectShape(h, {{1, 1}, {2, 1}}, "success.partial.requeued");
        expectOffset(h, 1, "success.partial.offset");
        expect(h.consumedCount() == 1, "success.partial.count",
               std::to_string(h.consumedCount()));
    }
    {
        // ackIndex=-1（listener 显式不认可任何一条）：整批回投，位点原地不动
        Harness h;
        h.run(offsetBatch(3), ConsumeConcurrentlyStatus::CONSUME_SUCCESS, -1);
        expectShape(h, {{0, 1}, {1, 1}, {2, 1}}, "success.none.requeued");
        expect(!h.offset().has_value(), "success.none.offsetFrozen",
               h.offset() ? std::to_string(*h.offset()) : "none");
        expect(h.consumedCount() == 0, "success.none.count", std::to_string(h.consumedCount()));
    }
    {
        // 尾巴里的中间那条回投失败也要钳住（这里离线全失败，检查顺序与条数）
        Harness h;
        h.run(offsetBatch(2), ConsumeConcurrentlyStatus::CONSUME_SUCCESS, 0);
        expectShape(h, {{1, 1}}, "success.tail1.requeued");
        expectOffset(h, 1, "success.tail1.offset");
    }
}

void testReconsumeLater() {
    {
        // RECONSUME_LATER 强制 ackIndex=-1（Java:210-212）：listener 写的宽 ackIndex 无效
        Harness h;
        const bool advanced = h.run(offsetBatch(3), ConsumeConcurrentlyStatus::RECONSUME_LATER, 1);
        expect(!advanced, "later.notAdvanced");
        expectShape(h, {{0, 1}, {1, 1}, {2, 1}}, "later.wholeBatch");
        expect(!h.offset().has_value(), "later.offsetFrozen",
               h.offset() ? std::to_string(*h.offset()) : "none");
    }
    {
        // 消费抛异常按 RECONSUME_LATER 处理（Java catch Throwable）
        Harness h;
        h.run(offsetBatch(2), ConsumeConcurrentlyStatus::CONSUME_SUCCESS, 5, true);
        expectShape(h, {{0, 1}, {1, 1}}, "exception.wholeBatch");
        expect(!h.offset().has_value(), "exception.offsetFrozen",
               h.offset() ? std::to_string(*h.offset()) : "none");
    }
}

void testBroadcasting() {
    {
        // 广播模式没有 %RETRY% 可回投：未认可的尾巴只 warn 就丢掉，整批位点前进
        //（Java:232-237，重启后不重投）
        Harness h(MessageModel::BROADCASTING);
        const bool advanced = h.run(offsetBatch(3), ConsumeConcurrentlyStatus::CONSUME_SUCCESS, 0);
        expect(advanced, "broadcast.advanced");
        expect(h.pendingShape().empty(), "broadcast.noRequeue", Harness::shapeText(h.pendingShape()));
        expectOffset(h, 3, "broadcast.offset");
        expect(h.consumedCount() == 3, "broadcast.count", std::to_string(h.consumedCount()));
    }
    {
        // 广播 + RECONSUME_LATER：同样前进（不回投是本地的既定语义）
        Harness h(MessageModel::BROADCASTING);
        expect(h.run(offsetBatch(2), ConsumeConcurrentlyStatus::RECONSUME_LATER,
                     AckListener::kNoAckIndex),
               "broadcast.later.advanced");
        expectOffset(h, 2, "broadcast.later.offset");
    }
}

}  // namespace

int main() {
    testContextDefault();
    testConsumeSuccess();
    testReconsumeLater();
    testBroadcasting();
    std::printf("consume ack index: %d checks, %d failures\n", checks, fails);
    return fails == 0 ? 0 : 1;
}
