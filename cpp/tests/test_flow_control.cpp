// Unit test for the pre-pull flow-control decision -- no cluster needed.
#include <cstdio>
#include <limits>
#include <string>
#include <vector>

#include "rocketmq/client/consumer.h"
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

constexpr int32_t kMiB = 1024 * 1024;
const char* kTopic = "FlowControlCppUnitTopic";
const char* kOtherTopic = "FlowControlCppUnitOther";
const char* kBroker = "broker-a";

MessageQueue queueOf(const char* topic, int32_t id) {
    return MessageQueue(topic, kBroker, id);
}

MessageExt sized(const char* topic, int32_t storeSize, int64_t queueOffset) {
    MessageExt m;
    m.topic = topic;
    m.brokerName = kBroker;
    m.queueId = 0;
    m.queueOffset = queueOffset;
    m.storeSize = storeSize;
    m.body = "x";
    return m;
}

// Turn off every gate except the threshold under test: whichever hits first masks the branch.
void gatesOffExceptCounts(DefaultMQPushConsumer& c) {
    c.setPullThresholdForQueue(std::numeric_limits<int32_t>::max());
    c.setPullThresholdSizeForQueue(0);
    c.setConsumeConcurrentlyMaxSpan(0);
    c.setPullThresholdForTopic(-1);
    c.setPullThresholdSizeForTopic(-1);
}

// Preload a batch into one queue's buffer and mark that queue assigned (topic thresholds aggregate via mqMap_).
void stage(DefaultMQPushConsumer& c, const MessageQueue& mq, const std::vector<MessageExt>& msgs) {
    const std::string key = DefaultMQPushConsumer::offsetKey(mq);
    c.setAssignedQueue(key, mq);
    c.setPendingMessages(key, msgs);
}

std::vector<MessageExt> nSized(const char* topic, int32_t storeSize, size_t n) {
    std::vector<MessageExt> out;
    for (size_t i = 0; i < n; i++) {
        out.push_back(sized(topic, storeSize, static_cast<int64_t>(i)));
    }
    return out;
}

void testDefaultsOnlyCountGate() {
    DefaultMQPushConsumer c("GID_FlowControlDefaults");
    const MessageQueue mq = queueOf(kTopic, 0);
    // Defaults 1000 msgs / 100MiB / span 2000, topic level off: a small buffer never trips them
    stage(c, mq, nSized(kTopic, 100, 1));
    expect(!c.flowControlHit(mq, DefaultMQPushConsumer::offsetKey(mq)), "defaults.noHit");
    expect(c.flowControlTriggered() == 0, "defaults.counterUntouched",
           std::to_string(c.flowControlTriggered()));
    // An empty buffer (nothing pulled yet) also misses, and the span must not turn out negative
    stage(c, mq, {});
    expect(!c.flowControlHit(mq, DefaultMQPushConsumer::offsetKey(mq)), "empty.noHit");
}

void testCountGate() {
    DefaultMQPushConsumer c("GID_FlowControlCount");
    const MessageQueue mq = queueOf(kTopic, 0);
    const std::string key = DefaultMQPushConsumer::offsetKey(mq);
    gatesOffExceptCounts(c);
    c.setPullThresholdForQueue(3);
    stage(c, mq, nSized(kTopic, 100, 2));
    expect(!c.flowControlHit(mq, key), "count.below");
    stage(c, mq, nSized(kTopic, 100, 3));
    expect(c.flowControlHit(mq, key), "count.atThreshold");
    expect(c.flowControlTriggered() == 1, "count.counterOnce",
           std::to_string(c.flowControlTriggered()));
    c.setPullThresholdForQueue(0);
    stage(c, mq, nSized(kTopic, 100, 1));
    expect(c.flowControlHit(mq, key), "count.zeroBehavesAsOne");
}

void testSizeGate() {
    DefaultMQPushConsumer c("GID_FlowControlSize");
    const MessageQueue mq = queueOf(kTopic, 0);
    const std::string key = DefaultMQPushConsumer::offsetKey(mq);
    gatesOffExceptCounts(c);
    c.setPullThresholdSizeForQueue(1);           // 1 MiB
    stage(c, mq, nSized(kTopic, 300, 3));        // 900 B
    expect(!c.flowControlHit(mq, key), "size.below");
    // The unit is MiB, not bytes: exactly 1 MiB already counts as a hit (>=)
    stage(c, mq, {sized(kTopic, kMiB, 0)});
    expect(c.flowControlHit(mq, key), "size.exactlyOneMiB");
    stage(c, mq, nSized(kTopic, 2 * kMiB, 4));
    expect(c.flowControlHit(mq, key), "size.over");
    // 0 disables this gate, however large the buffer becomes
    c.setPullThresholdSizeForQueue(0);
    expect(!c.flowControlHit(mq, key), "size.disabled");
}

void testSpanGate() {
    DefaultMQPushConsumer c("GID_FlowControlSpan");
    const MessageQueue mq = queueOf(kTopic, 0);
    const std::string key = DefaultMQPushConsumer::offsetKey(mq);
    gatesOffExceptCounts(c);
    c.setConsumeConcurrentlyMaxSpan(10);
    stage(c, mq, {sized(kTopic, 1, 0), sized(kTopic, 1, 100)});
    expect(c.flowControlHit(mq, key), "span.over");
    c.setConsumeConcurrentlyMaxSpan(100);
    expect(!c.flowControlHit(mq, key), "span.exactlyAtThreshold");
    // An out-of-order buffer must still yield the real span (min/max, not head minus tail)
    c.setConsumeConcurrentlyMaxSpan(10);
    stage(c, mq, {sized(kTopic, 1, 50), sized(kTopic, 1, 5), sized(kTopic, 1, 7)});
    expect(c.flowControlHit(mq, key), "span.unorderedBuffer");
    c.setConsumeConcurrentlyMaxSpan(0);
    expect(!c.flowControlHit(mq, key), "span.disabled");
}

void testTopicCountGate() {
    DefaultMQPushConsumer c("GID_FlowControlTopicCount");
    const MessageQueue q0 = queueOf(kTopic, 0);
    const MessageQueue q1 = queueOf(kTopic, 1);
    const MessageQueue other = queueOf(kOtherTopic, 0);
    gatesOffExceptCounts(c);
    c.setPullThresholdForTopic(2);
    // One message in a single queue: the topic level also sees only 1
    stage(c, q0, nSized(kTopic, 1, 1));
    expect(!c.flowControlHit(q0, DefaultMQPushConsumer::offsetKey(q0)), "topicCount.one");
    // One message in each sibling queue of the same topic => 2 in total, so both queues must stop
    stage(c, q1, nSized(kTopic, 1, 1));
    expect(c.flowControlHit(q0, DefaultMQPushConsumer::offsetKey(q0)), "topicCount.q0");
    expect(c.flowControlHit(q1, DefaultMQPushConsumer::offsetKey(q1)), "topicCount.q1");
    // Other topics must not leak in: drop this topic to 1 message, pile 5 onto the other topic
    stage(c, q1, {});
    stage(c, other, nSized(kOtherTopic, 1, 5));
    expect(!c.flowControlHit(q0, DefaultMQPushConsumer::offsetKey(q0)),
           "topicCount.otherTopicNotCounted");
}

void testTopicSizeGate() {
    DefaultMQPushConsumer c("GID_FlowControlTopicSize");
    const MessageQueue q0 = queueOf(kTopic, 0);
    const MessageQueue q1 = queueOf(kTopic, 1);
    gatesOffExceptCounts(c);
    // Queue-level byte gate **off** (0), only the topic level left: misreading the queue switch would void this case silently
    c.setPullThresholdSizeForTopic(1);
    stage(c, q0, {sized(kTopic, kMiB / 2, 0)});
    expect(!c.flowControlHit(q0, DefaultMQPushConsumer::offsetKey(q0)), "topicSize.halfMiB");
    stage(c, q1, {sized(kTopic, 3 * kMiB / 2, 0)});
    expect(c.flowControlHit(q0, DefaultMQPushConsumer::offsetKey(q0)), "topicSize.aggregated");
    expect(c.flowControlHit(q1, DefaultMQPushConsumer::offsetKey(q1)), "topicSize.aggregatedQ1");
    // With the queue-level gate still open it hits first (order: count -> size -> span -> topic count -> topic size)
    c.setPullThresholdSizeForQueue(1);
    stage(c, q0, {sized(kTopic, 2 * kMiB, 0)});
    const int64_t before = c.flowControlTriggered();
    expect(c.flowControlHit(q0, DefaultMQPushConsumer::offsetKey(q0)), "queueSizeWinsOrdering");
    expect(c.flowControlTriggered() == before + 1, "oneHitOneCount",
           std::to_string(c.flowControlTriggered()));
}

}  // namespace

int main() {
    testDefaultsOnlyCountGate();
    testCountGate();
    testSizeGate();
    testSpanGate();
    testTopicCountGate();
    testTopicSizeGate();
    std::printf("flow control: %d checks, %d failures\n", checks, fails);
    return fails == 0 ? 0 : 1;
}
