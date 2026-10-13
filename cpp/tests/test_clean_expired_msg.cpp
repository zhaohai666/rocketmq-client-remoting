// 为什么必须离线锁死：这条清扫是"listener 卡死不返回"时唯一的回收路径。少了它，一条卡住
// 的消息会让该队列位点永久停在原地，且没有任何异常、日志或超时可见 —— 真机上只能靠
// "消息发了却永远不来第二次"这种间接现象暴露；而清扫本身的窗口是分钟级、每轮 16 条上限
// 也只有在离线用假时刻才能在秒级验证。语义三件套全都"错了很安静"：
//   - 只看队首（最小位点）：漏判 → 队首卡住时后面的消息永远扫不到；
//   - 过期判据**严格大于** consumeTimeout：写成 >= 会把边界消息提前回投（凭空重投）；
//   - 单轮 min(size,16) 且 loop 在进循环前算一次：写成 while 会一轮清空整条队列。
//
//
// 离线能锁的是「回投失败」这一支：未 start 的消费者 sendMessageBack 必失败（函数按返回值
// 表态、不抛异常），所以结算断言是"消息留在原地、位点不许动"；「回投成功 → 摘除 →
// %RETRY% 重投 → reconsumeTimes=1 的第二次投递」只能在真机取证
//（examples/live_clean_expired_msg.cpp 是真机那一腿）。
// 与 python/tests/test_clean_expired_msg.py、rust/src/client/consumer.rs、
#include <cstdio>
#include <memory>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "rocketmq/client/consumer.h"
#include "rocketmq/client/result.h"
#include "rocketmq/common/message.h"
#include "rocketmq/common/message_const.h"

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

const char* kGroup = "GID_CleanExpiredCppUnit";
const char* kTopic = "CleanExpiredCppUnitTopic";
const char* kBroker = "broker-a";
const int64_t kNow = 1800000000000LL;

MessageQueue queue0() {
    return MessageQueue(kTopic, kBroker, 0);
}

MessageExt ext(int64_t queueOffset) {
    MessageExt m;
    m.topic = kTopic;
    m.brokerName = kBroker;
    m.queueId = 0;
    m.queueOffset = queueOffset;
    m.body = "x";
    return m;
}

MessageExt stamped(int64_t queueOffset, int64_t tsMs) {
    MessageExt m = ext(queueOffset);
    m.putProperty(MessageConst::PROPERTY_CONSUME_START_TIMESTAMP, std::to_string(tsMs));
    return m;
}

std::vector<int64_t> offsetsOf(const std::vector<MessageExt>& msgs) {
    std::vector<int64_t> out;
    for (const MessageExt& m : msgs) out.push_back(m.queueOffset);
    return out;
}

bool hasOffset(const std::vector<MessageExt>& msgs, int64_t offset) {
    for (const MessageExt& m : msgs) {
        if (m.queueOffset == offset) return true;
    }
    return false;
}

// 未 start 的消费者即可：清扫/登记/位点推进全在本地状态上，唯一的网络动作是回投
//（sendMessageBack），未 start 时 client() 必抛、函数按契约吞掉并返回 false。
class Harness {
public:
    explicit Harness(int32_t consumeTimeoutMinutes = 1)
        : consumer_(kGroup), key_(DefaultMQPushConsumer::offsetKey(queue0())) {
        consumer_.setConsumeTimeout(consumeTimeoutMinutes);
    }

    DefaultMQPushConsumer& consumer() { return consumer_; }
    const std::string& key() const { return key_; }

    void seedInflight(const std::vector<MessageExt>& msgs) {
        consumer_.setInflightMessages(key_, msgs);
    }
    void seedPending(const std::vector<MessageExt>& msgs) {
        consumer_.setPendingMessages(key_, msgs);
    }
    std::vector<MessageExt> entries() const { return consumer_.processQueueEntries(key_); }
    std::vector<MessageExt> pending() const { return consumer_.pendingMessages(key_); }
    int32_t sweep(int64_t nowMs) { return consumer_.cleanExpiredQueue(key_, nowMs); }

private:
    DefaultMQPushConsumer consumer_;
    std::string key_;
};

class NoopOrderlyListener : public MessageListenerOrderly {
public:
    ConsumeOrderlyStatus consumeMessage(const std::vector<MessageExt>&,
                                        ConsumeOrderlyContext&) override {
        return ConsumeOrderlyStatus::SUCCESS;
    }
};

// ---------------------------------------------------------------- 判据与节奏

void testExpiryArithmetic() {
    expect(!DefaultMQPushConsumer::isConsumeExpired(ext(0), 1, kNow),
           "expiry.emptyStamp.notExpired");

    expect(!DefaultMQPushConsumer::isConsumeExpired(stamped(0, kNow - 60000), 1, kNow),
           "expiry.exactlyAtLimit.notExpired");
    expect(DefaultMQPushConsumer::isConsumeExpired(stamped(0, kNow - 60001), 1, kNow),
           "expiry.oneMsPast.expired");

    // 未来时间戳（时钟回拨/跨机器）也不能被算成过期
    expect(!DefaultMQPushConsumer::isConsumeExpired(stamped(0, kNow + 5000), 1, kNow),
           "expiry.futureStamp.notExpired");

    // 阈值跟消费配置走：同一条消息在默认 15 分钟档还太新
    expect(!DefaultMQPushConsumer::isConsumeExpired(stamped(0, kNow - 60001), 15, kNow),
           "expiry.default15.notExpired");

    MessageExt bad = ext(0);
    bad.putProperty(MessageConst::PROPERTY_CONSUME_START_TIMESTAMP, "not-a-number");
    bool threw = false;
    try {
        (void)DefaultMQPushConsumer::isConsumeExpired(bad, 1, kNow);
    } catch (const std::invalid_argument&) {
        threw = true;
    }
    expect(threw, "expiry.malformedStamp.throws");
}

void testPeriodMatchesSchedule() {
    expect(DefaultMQPushConsumer::cleanExpirePeriodMillis(15) == 900000,
           "period.default15", std::to_string(DefaultMQPushConsumer::cleanExpirePeriodMillis(15)));
    expect(DefaultMQPushConsumer::cleanExpirePeriodMillis(1) == 60000, "period.oneMinute");
    expect(DefaultMQPushConsumer::cleanExpirePeriodMillis(0) == 60000, "period.zeroClamps");
    expect(DefaultMQPushConsumer::cleanExpirePeriodMillis(-5) == 60000, "period.negativeClamps");
}

// ---------------------------------------------------------------- 选条语义

void testOnlyHeadIsConsideredAndExpiryIsStrict() {
    Harness h;
    h.seedInflight({ext(0), stamped(1, kNow - 120000)});
    expect(h.sweep(kNow) == 0, "sweep.unstampedHead.noAttempt");
    expect(offsetsOf(h.entries()) == std::vector<int64_t>({0, 1}), "sweep.unstampedHead.entriesKept");

    // 队首刚盖过章（没过期）同样停
    Harness fresh;
    fresh.seedInflight({stamped(0, kNow - 1000)});
    expect(fresh.sweep(kNow) == 0, "sweep.fresh.notAttempted");

    Harness order;
    order.seedInflight({stamped(5, kNow - 1), stamped(3, kNow - 120000)});
    expect(order.sweep(kNow) == 2, "sweep.headIsMinOffset.notInsertionOrder");
    expect(offsetsOf(order.entries()).size() == 2, "sweep.headIsMinOffset.entriesKept");
}

void testFailedSendBackKeepsTheEntry() {
    Harness h;
    h.seedInflight({stamped(0, kNow - 120000)});
    expect(h.sweep(kNow) == 1, "fail.oneAttempt");
    expect(h.entries().size() == 1, "fail.entryKept", std::to_string(h.entries().size()));
    expect(!h.consumer().consumeOffset(h.key()).has_value(), "fail.offsetUntouched");
    h.sweep(kNow);
    expect(h.entries().size() == 1, "fail.stillKeptOnSecondRound");
}

void testSixteenCapPerPass() {
    Harness h;
    std::vector<MessageExt> msgs;
    for (int i = 0; i < 20; i++) msgs.push_back(stamped(i, kNow - 120000));
    h.seedInflight(msgs);
    const int32_t attempts = h.sweep(kNow);
    expect(attempts == 16, "cap.sixteenAttempts", std::to_string(attempts));
    expect(h.entries().size() == 20, "cap.entriesAllKept", std::to_string(h.entries().size()));
}

void testRemoveOnlyWhenStillHead() {
    Harness h;
    h.seedPending({ext(5), ext(3)});
    const auto head = h.consumer().processQueueHead(h.key());
    expect(head.has_value() && head->queueOffset == 3, "remove.headIsMinOffset");

    h.consumer().removeExpiredEntryIfStillHead(h.key(), ext(3));
    expect(offsetsOf(h.entries()) == std::vector<int64_t>({5}), "remove.stillHeadRemoved");

    // 5 现在挂着，但传进来的是「已经不是队首」的 5：前面若冒出 4，就不许摘 5
    h.seedPending({ext(4), ext(5)});
    h.consumer().removeExpiredEntryIfStillHead(h.key(), ext(5));
    expect(hasOffset(h.entries(), 5), "remove.notHeadLeftAlone");
    expect(h.entries().size() == 2, "remove.notHeadNothingRemoved");

    // 表里没有的位点：no-op（不能顺手摘掉别人）
    h.consumer().removeExpiredEntryIfStillHead(h.key(), ext(99));
    expect(h.entries().size() == 2, "remove.absentIsNoop");
}

void testContainsMessageIsByQueueOffset() {
    Harness h;
    h.seedInflight({stamped(7, kNow - 120000)});
    h.seedPending({ext(8)});
    expect(h.consumer().processQueueContains(h.key(), ext(7)), "contains.inflightSide");
    expect(h.consumer().processQueueContains(h.key(), ext(8)), "contains.pendingSide");
    expect(!h.consumer().processQueueContains(h.key(), ext(9)), "contains.absent");
    expect(!h.consumer().processQueueContains("no-such-queue", ext(7)), "contains.missingKey");
}

void testEntriesViewIsUnionDeduped() {
    Harness h;
    h.seedPending({ext(0), ext(1)});
    h.seedInflight({ext(1), ext(2)});
    expect(offsetsOf(h.entries()).size() == 3, "union.size",
           std::to_string(h.entries().size()));
    expect(hasOffset(h.entries(), 0) && hasOffset(h.entries(), 2), "union.bothSides");
}

// ---------------------------------------------------------------- 清扫范围与顺序消费

void testSweepScopeIsHeldQueuesOnly() {
    DefaultMQPushConsumer c(kGroup);
    c.setConsumeTimeout(1);
    const std::string k0 = DefaultMQPushConsumer::offsetKey(queue0());
    const MessageQueue q1(kTopic, kBroker, 1);
    const std::string k1 = DefaultMQPushConsumer::offsetKey(q1);
    c.setInflightMessages(k0, {stamped(0, kNow - 120000)});
    c.setInflightMessages(k1, {stamped(0, kNow - 120000)});

    expect(c.cleanExpiredMsgOnce(kNow) == 0, "scope.noneHeld");
    c.setAssignedQueue(k0, queue0());
    expect(c.cleanExpiredMsgOnce(kNow) == 1, "scope.onlyHeldSwept");
    c.setAssignedQueue(k1, q1);
    expect(c.cleanExpiredMsgOnce(kNow) == 2, "scope.bothHeld");

    // 队列被撤（OFFSET_ILLEGAL 纠错走的就是 retireQueueLocked）：登记清空、不再被扫
    c.handleOffsetIllegal(k0, 5);
    expect(c.processQueueEntries(k0).empty(), "scope.retiredClearsInflight");
    expect(c.cleanExpiredMsgOnce(kNow) == 1, "scope.retiredNotSwept");
}

void testOrderlyConsumerIsNeverSwept() {
    DefaultMQPushConsumer c(kGroup);
    c.setConsumeTimeout(1);
    c.setMessageListener(std::make_shared<NoopOrderlyListener>());
    const std::string key = DefaultMQPushConsumer::offsetKey(queue0());
    c.setInflightMessages(key, {stamped(0, kNow - 3600000)});
    expect(c.cleanExpiredQueue(key, kNow) == 0, "orderly.noAttempt");
    expect(c.processQueueEntries(key).size() == 1, "orderly.entryKept");
}

// ---------------------------------------------------------------- 与消费路径的配合

class ObservingListener : public MessageListenerConcurrently {
public:
    ObservingListener(DefaultMQPushConsumer* c, std::string key, bool reclaim)
        : consumer_(c), key_(std::move(key)), reclaim_(reclaim) {}

    ConsumeConcurrentlyStatus consumeMessage(const std::vector<MessageExt>& msgs,
                                             ConsumeConcurrentlyContext&) override {
        containsDuring_ = consumer_->processQueueContains(key_, msgs[0]);
        entriesDuring_ = static_cast<int>(consumer_->processQueueEntries(key_).size());
        const auto head = consumer_->processQueueHead(key_);
        // 登记表里躺的必须是**已盖章**的副本：本端口的批次是值拷贝，盖章与登记的顺序
        // 反了的话清扫永远看不见这些消息（真机上静默失效）
        headStamp_ = head.has_value()
                         ? head->getProperty(MessageConst::PROPERTY_CONSUME_START_TIMESTAMP)
                         : std::string();
        if (reclaim_) {
            // 模拟"挂起期间清扫成功回投并摘除"（真机上这一步由清扫线程做）
            consumer_->removeExpiredEntryIfStillHead(key_, msgs[0]);
        }
        return ConsumeConcurrentlyStatus::RECONSUME_LATER;
    }

    bool containsDuring_ = false;
    int entriesDuring_ = 0;
    std::string headStamp_;

private:
    DefaultMQPushConsumer* consumer_;
    std::string key_;
    bool reclaim_;
};

class OkListener : public MessageListenerConcurrently {
public:
    explicit OkListener(int* seen) : seen_(seen) {}
    ConsumeConcurrentlyStatus consumeMessage(const std::vector<MessageExt>&,
                                             ConsumeConcurrentlyContext&) override {
        ++(*seen_);
        return ConsumeConcurrentlyStatus::CONSUME_SUCCESS;
    }

private:
    int* seen_;
};

void testConcurrentBatchSeesItselfInTheProcessQueue() {
    DefaultMQPushConsumer c(kGroup);
    const std::string key = DefaultMQPushConsumer::offsetKey(queue0());
    auto listener = std::make_shared<ObservingListener>(&c, key, /*reclaim=*/false);
    c.setMessageListener(listener);
    // 真分发链路里 pending_ 的槽位在这批取走之前就存在（takeBatchLocked 弹出后留一个空
    // 双端队列），回投失败的条目正是塞回这个槽位 —— 离线补上同样的形状。
    c.setPendingMessages(key, {});

    std::vector<MessageExt> batch{ext(0)};
    const bool advanced = c.consumeBatch(key, queue0(), batch);

    expect(listener->containsDuring_, "batch.containsDuringListener");
    expect(listener->entriesDuring_ == 1, "batch.entriesDuringListener",
           std::to_string(listener->entriesDuring_));
    expect(!listener->headStamp_.empty(), "batch.registeredCopyIsStamped");

    // 未 start：回投失败 → 消息塞回队首、位点不动、返回 false（既有 ackIndex 语义）
    expect(!advanced, "batch.failedSendBackReturnsFalse");
    expect(offsetsOf(c.pendingMessages(key)) == std::vector<int64_t>({0}), "batch.requeuedToPending");
    expect(!c.consumeOffset(key).has_value(), "batch.offsetUntouched");
    // 在途侧必须已注销（留下的那一份是"回投失败塞回缓冲"的 pending 副本）
    expect(hasOffset(c.processQueueEntries(key), 0) && c.processQueueEntries(key).size() == 1,
           "batch.inflightDeregistered");
}

void testSendBackSkipsAMessageTheSweepAlreadyReclaimed() {
    DefaultMQPushConsumer c(kGroup);
    const std::string key = DefaultMQPushConsumer::offsetKey(queue0());
    auto listener = std::make_shared<ObservingListener>(&c, key, /*reclaim=*/true);
    c.setMessageListener(listener);

    std::vector<MessageExt> batch{ext(0)};
    const bool advanced = c.consumeBatch(key, queue0(), batch);

    expect(advanced, "skip.returnsTrueWhenNothingToSendBack");
    expect(c.pendingMessages(key).empty(), "skip.notRequeued");
    expect(c.processQueueEntries(key).empty(), "skip.entryGone",
           std::to_string(c.processQueueEntries(key).size()));
    expect(c.consumeOffset(key) == 1, "skip.offsetAdvanced");
}

void testConcurrentBatchDeregistersOnSuccessAndAcrossRounds() {
    DefaultMQPushConsumer c(kGroup);
    const std::string key = DefaultMQPushConsumer::offsetKey(queue0());
    int seen = 0;
    c.setMessageListener(std::make_shared<OkListener>(&seen));

    std::vector<MessageExt> first{ext(0)};
    expect(c.consumeBatch(key, queue0(), first), "ok.firstReturnsTrue");
    expect(c.processQueueEntries(key).empty(), "ok.deregisteredAfterReturn");
    expect(c.consumeOffset(key) == 1, "ok.offsetAdvanced");
    expect(seen == 1, "ok.listenerCalled");

    // 第二轮：上一批若漏注销，这里的在册视图会看到 2 条（泄漏检测）
    std::vector<MessageExt> second{ext(1)};
    expect(c.consumeBatch(key, queue0(), second), "ok.secondReturnsTrue");
    expect(c.processQueueEntries(key).empty(), "ok.noLeakAcrossRounds");
    expect(c.consumeOffset(key) == 2, "ok.offsetAdvancedAgain");
}

void testTakeThenConsumeKeepsOneCopy() {
    // 真分发链路的离线缩微版：takeBatchForConsume（从 pending 取走并登记在途）→
    // consumeBatch（消费 + 注销）。取走后消息只能存在于在途一侧，两边都在就是重复投递。
    DefaultMQPushConsumer c(kGroup);
    const std::string key = DefaultMQPushConsumer::offsetKey(queue0());
    int seen = 0;
    c.setMessageListener(std::make_shared<OkListener>(&seen));
    c.setPendingMessages(key, {ext(0)});

    uint64_t epoch = 0;
    std::vector<MessageExt> batch = c.takeBatchForConsume(key, &epoch);
    expect(batch.size() == 1, "pipeline.tookOne", std::to_string(batch.size()));
    expect(c.pendingMessages(key).empty(), "pipeline.pendingEmptied");
    // 取走与"登记在途消息"之间有一个瞬时窗口：消息挂在 inflightCount_（计数器）上，
    // 还不在并集视图里。清扫取不到它（没有本轮盖章）、correctTagsOffset 的闸门看的是
    // 计数器 —— 两边都不会误判，所以这里如实锁住窗口的形状。
    expect(c.processQueueEntries(key).empty(), "pipeline.takeWindowNotInUnionView");

    expect(c.consumeBatch(key, queue0(), batch, epoch), "pipeline.consumeReturnsTrue");
    expect(c.processQueueEntries(key).empty(), "pipeline.drained");
    expect(c.consumeOffset(key) == 1, "pipeline.offsetAdvanced");
    expect(seen == 1, "pipeline.listenerCalledOnce");
}

}  // namespace

int main() {
    testExpiryArithmetic();
    testPeriodMatchesSchedule();
    testOnlyHeadIsConsideredAndExpiryIsStrict();
    testFailedSendBackKeepsTheEntry();
    testSixteenCapPerPass();
    testRemoveOnlyWhenStillHead();
    testContainsMessageIsByQueueOffset();
    testEntriesViewIsUnionDeduped();
    testSweepScopeIsHeldQueuesOnly();
    testOrderlyConsumerIsNeverSwept();
    testConcurrentBatchSeesItselfInTheProcessQueue();
    testSendBackSkipsAMessageTheSweepAlreadyReclaimed();
    testConcurrentBatchDeregistersOnSuccessAndAcrossRounds();
    testTakeThenConsumeKeepsOneCopy();
    std::printf("clean expired msg: %d checks, %d failures\n", checks, fails);
    return fails == 0 ? 0 : 1;
}
