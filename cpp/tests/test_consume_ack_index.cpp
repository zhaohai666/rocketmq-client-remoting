// Unit tests for ackIndex semantics on the classic concurrent-consumption path - no cluster needed.
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

// A consumer that was never started: sendMessageBack is guaranteed to fail (client() throws -> swallowed internally -> returns
// false), exactly the fixture for the "failed redelivery must not advance the offset" branch.
class Harness {
public:
    explicit Harness(const std::string& model = MessageModel::CLUSTERING)
        : consumer_(kGroup), key_(DefaultMQPushConsumer::offsetKey(queue0())) {
        consumer_.setMessageModel(model);
        // In a real run dispatchLoop first takes the batch out of the buffer, then consumes it, so the key is definitely still in the map
        //(possibly with extra left). Failed redelivery must be pushed back into exactly this map; not pre-setting it means the queue was already removed.
        consumer_.setPendingMessages(key_, {});
    }

    // Returns the consumeBatch result (true = the offset advanced)
    bool run(const std::vector<MessageExt>& batch, ConsumeConcurrentlyStatus status,
             int32_t ackIndex, bool thrown = false) {
        consumer_.setMessageListener(
            std::make_shared<AckListener>(status, ackIndex, thrown));
        return consumer_.consumeBatch(key_, queue0(), batch);
    }

    std::vector<MessageExt> pending() const { return consumer_.pendingMessages(key_); }
    std::optional<int64_t> offset() const { return consumer_.consumeOffset(key_); }
    int64_t consumedCount() const { return consumer_.consumedCount(); }

    // (queueOffset, reconsumeTimes) list of the front buffer, for one-shot comparison
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
    ConsumeConcurrentlyContext ctx(queue0());
    expect(ctx.ackIndex == std::numeric_limits<int32_t>::max(), "default.maxValue",
           std::to_string(ctx.ackIndex));
    expect(ctx.delayLevelWhenNextConsume == 0, "default.delayLevel");
}

void testConsumeSuccess() {
    {
        // Default (ackIndex untouched): no redelivery at all, offset advances by the whole batch
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
        Harness h;
        h.run(offsetBatch(3), ConsumeConcurrentlyStatus::CONSUME_SUCCESS, 99);
        expect(h.pendingShape().empty(), "success.clamp.noRequeue",
               Harness::shapeText(h.pendingShape()));
        expectOffset(h, 3, "success.clamp.offset");
    }
    {
        Harness h;
        const bool advanced = h.run(offsetBatch(3), ConsumeConcurrentlyStatus::CONSUME_SUCCESS, 0);
        expect(!advanced, "success.partial.notAdvanced");
        expectShape(h, {{1, 1}, {2, 1}}, "success.partial.requeued");
        expectOffset(h, 1, "success.partial.offset");
        expect(h.consumedCount() == 1, "success.partial.count",
               std::to_string(h.consumedCount()));
    }
    {
        // ackIndex=-1 (listener explicitly acknowledges none): whole batch redelivered, offset frozen in place
        Harness h;
        h.run(offsetBatch(3), ConsumeConcurrentlyStatus::CONSUME_SUCCESS, -1);
        expectShape(h, {{0, 1}, {1, 1}, {2, 1}}, "success.none.requeued");
        expect(!h.offset().has_value(), "success.none.offsetFrozen",
               h.offset() ? std::to_string(*h.offset()) : "none");
        expect(h.consumedCount() == 0, "success.none.count", std::to_string(h.consumedCount()));
    }
    {
        // A failed redelivery of a middle item inside the tail must still clamp (offline all deliveries fail here; check order and count)
        Harness h;
        h.run(offsetBatch(2), ConsumeConcurrentlyStatus::CONSUME_SUCCESS, 0);
        expectShape(h, {{1, 1}}, "success.tail1.requeued");
        expectOffset(h, 1, "success.tail1.offset");
    }
}

void testReconsumeLater() {
    {
        Harness h;
        const bool advanced = h.run(offsetBatch(3), ConsumeConcurrentlyStatus::RECONSUME_LATER, 1);
        expect(!advanced, "later.notAdvanced");
        expectShape(h, {{0, 1}, {1, 1}, {2, 1}}, "later.wholeBatch");
        expect(!h.offset().has_value(), "later.offsetFrozen",
               h.offset() ? std::to_string(*h.offset()) : "none");
    }
    {
        Harness h;
        h.run(offsetBatch(2), ConsumeConcurrentlyStatus::CONSUME_SUCCESS, 5, true);
        expectShape(h, {{0, 1}, {1, 1}}, "exception.wholeBatch");
        expect(!h.offset().has_value(), "exception.offsetFrozen",
               h.offset() ? std::to_string(*h.offset()) : "none");
    }
}

void testBroadcasting() {
    {
        Harness h(MessageModel::BROADCASTING);
        const bool advanced = h.run(offsetBatch(3), ConsumeConcurrentlyStatus::CONSUME_SUCCESS, 0);
        expect(advanced, "broadcast.advanced");
        expect(h.pendingShape().empty(), "broadcast.noRequeue", Harness::shapeText(h.pendingShape()));
        expectOffset(h, 3, "broadcast.offset");
        expect(h.consumedCount() == 3, "broadcast.count", std::to_string(h.consumedCount()));
    }
    {
        // BROADCASTING + RECONSUME_LATER: still advances (not redelivering is the established local semantics)
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
