// 拉取前流控判定的单测 —— 不需要集群。
//
// 为什么必须离线锁死：Java ProcessQueue 的五个阈值里只有**条数**那一条会在"消息小、拉得快"
// 的场景里先命中，其余四条要等真出问题才看得见（队列级字节阈值失效 ⇒ 大消息把堆撑爆；
// 位点跨度失效 ⇒ 队首一条卡住、后面无限堆；topic 级阈值失效 ⇒ 同 topic 多队列各自为政）。
// 判据与 Python `_flow_control_hit` / Rust `flow_control_hit` 逐条同构：
//   1) 条数   >= pullThresholdForQueue（Java 的 max(1, n) 守卫：配 0 也按 1 条算）
//   2) 字节   >= pullThresholdSizeForQueue，单位 **MiB**（<=0 关闭）
//   3) 跨度   **严格大于** consumeConcurrentlyMaxSpan（pending 里 queueOffset 的 max-min；<=0 关闭）
//   4) topic 累计条数 >= pullThresholdForTopic（本实例该 topic **所有**队列合起来；-1 关闭）
//   5) topic 累计字节 >= pullThresholdSizeForTopic，单位 MiB（-1 关闭，且不复用第 2 条的开关）
// 命中一次只记一格 flowControlTriggered()。
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

// 关掉除待测阈值以外的所有闸门：先命中的那条会掩盖被测分支。
void gatesOffExceptCounts(DefaultMQPushConsumer& c) {
    c.setPullThresholdForQueue(std::numeric_limits<int32_t>::max());
    c.setPullThresholdSizeForQueue(0);
    c.setConsumeConcurrentlyMaxSpan(0);
    c.setPullThresholdForTopic(-1);
    c.setPullThresholdSizeForTopic(-1);
}

// 把一批消息预置进某队列的缓冲，并登记该队列已分配（topic 级阈值要靠 mqMap_ 聚合）。
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
    // 默认 1000 条 / 100MiB / 跨度 2000，topic 级关闭：小缓冲一律不命中
    stage(c, mq, nSized(kTopic, 100, 1));
    expect(!c.flowControlHit(mq, DefaultMQPushConsumer::offsetKey(mq)), "defaults.noHit");
    expect(c.flowControlTriggered() == 0, "defaults.counterUntouched",
           std::to_string(c.flowControlTriggered()));
    // 缓冲为空（还没拉过任何消息）同样不命中，且跨度不能被算成负数
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
    // Java 的 Math.max(1, n) 守卫：配 0 不是"全放行"，而是"1 条就停"
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
    // 单位是 MiB 而不是字节：正好 1 MiB 就算命中（>=）
    stage(c, mq, {sized(kTopic, kMiB, 0)});
    expect(c.flowControlHit(mq, key), "size.exactlyOneMiB");
    stage(c, mq, nSized(kTopic, 2 * kMiB, 4));
    expect(c.flowControlHit(mq, key), "size.over");
    // 0 = 关闭这条闸门，再大的缓冲也不管
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
    // **严格大于**：跨度正好等于阈值不算（Java 同）
    c.setConsumeConcurrentlyMaxSpan(100);
    expect(!c.flowControlHit(mq, key), "span.exactlyAtThreshold");
    // 乱序缓冲也要量出真实跨度（min/max 而不是首尾差）
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
    // 单队列 1 条：topic 级也只看到 1 条
    stage(c, q0, nSized(kTopic, 1, 1));
    expect(!c.flowControlHit(q0, DefaultMQPushConsumer::offsetKey(q0)), "topicCount.one");
    // 同 topic 的兄弟队列各 1 条 ⇒ 累计 2 条，两条队列都必须停
    stage(c, q1, nSized(kTopic, 1, 1));
    expect(c.flowControlHit(q0, DefaultMQPushConsumer::offsetKey(q0)), "topicCount.q0");
    expect(c.flowControlHit(q1, DefaultMQPushConsumer::offsetKey(q1)), "topicCount.q1");
    // 别的 topic 不许掺进来：把本 topic 降到 1 条，另一 topic 堆 5 条
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
    // 队列级字节闸门**关掉**（0），只留 topic 级：Rust 曾误用队列级开关当闸门，会让这条静默失效
    c.setPullThresholdSizeForTopic(1);
    stage(c, q0, {sized(kTopic, kMiB / 2, 0)});
    expect(!c.flowControlHit(q0, DefaultMQPushConsumer::offsetKey(q0)), "topicSize.halfMiB");
    stage(c, q1, {sized(kTopic, 3 * kMiB / 2, 0)});
    expect(c.flowControlHit(q0, DefaultMQPushConsumer::offsetKey(q0)), "topicSize.aggregated");
    expect(c.flowControlHit(q1, DefaultMQPushConsumer::offsetKey(q1)), "topicSize.aggregatedQ1");
    // 队列级那道还开着时先命中队列级（判定顺序：条数 → 字节 → 跨度 → topic 条数 → topic 字节）
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
