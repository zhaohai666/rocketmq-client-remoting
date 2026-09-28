// OFFSET_ILLEGAL 纠错分支（Java DefaultMQPushConsumerImpl:402-427）单测 —— 不需要集群。
//
// 为什么必须离线锁死：这条路径错了是**静默**的两种极端 ——
//   - 只把拉取游标拨到 nextBeginOffset 而不丢队列/不冻结：broker 刚把位点纠正到合法区间，
//     这条队列上**已经取回还没 ack** 的旧批次一 ack 又把位点推回非法值，下一轮拉取再被
//     broker 拒一次 —— 客户端与 broker 之间来回弹跳，永不停歇；
//   - 纠错后的位点没立刻落盘：进程在下一轮周期落盘（默认 5s）之前崩掉，broker 上留着的
//     还是非法位点，重启后从非法位点起拉 —— 这条纠错等于没做。
// 两个方向在真机短期窗口里都看不出差别（消息照消费、只是一直在弹 / 一次崩溃才暴露），
// 所以断言尽量放离线；真机另有一条链路证明（examples/live_offset_illegal.cpp 的 S1/S2：
// 「发现延迟 + 重建后位点恢复推进」与「纠错值 0.0-0.5s 内出现在 broker 上且零投递」）。
//
// 判据来源：PullMessageProcessor 对 OFFSET_OVERFLOW_BADLY / OFFSET_TOO_SMALL / OFFSET_RESET
// 一律回 PULL_OFFSET_MOVED，MQClientAPIImpl:1099 映射成 PullStatus::OFFSET_ILLEGAL，修正值在
// 应答头 nextBeginOffset。Java 的处理是 setNextOffset → ProcessQueue.setDropped(true) →
// { updateAndFreezeOffset; persist; removeProcessQueue } → rebalanceImmediately。
// 本端口：改位点（覆盖写）+ 冻结 → retireQueueLocked（代号 +1）→ onQueuesRevoked 锁外落盘
// → wakeRebalanceLoop。与 python/tests/test_offset_illegal_recover.py 的同名测试同题；
// 「重建后解冻」在 Python 侧离线锁死（那边能直接驱动一轮 rebalance），C++ 侧的
// rebalancePullThreads 依赖已分配集（离线拿不到），由真机 S1 的「committed 推进到 4」证明。
#include <cstdio>
#include <optional>
#include <string>
#include <vector>

#include "rocketmq/client/consumer.h"
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

const char* kGroup = "GID_OffsetIllegalCppUnit";
const char* kTopic = "OffsetIllegalCppUnitTopic";
const char* kBroker = "broker-a";

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

std::vector<MessageExt> offsetBatch(size_t n) {
    std::vector<MessageExt> out;
    for (size_t i = 0; i < n; i++) {
        out.push_back(ext(static_cast<int64_t>(i)));
    }
    return out;
}

// 全部认可（ackIndex 保持默认）的 listener，只记调用次数
class RecordingListener : public MessageListenerConcurrently {
public:
    ConsumeConcurrentlyStatus consumeMessage(const std::vector<MessageExt>& msgs,
                                             ConsumeConcurrentlyContext&) override {
        ++calls;
        lastSize = msgs.size();
        return ConsumeConcurrentlyStatus::CONSUME_SUCCESS;
    }
    int calls = 0;
    size_t lastSize = 0;
};

// 未 start 的消费者即可：handleOffsetIllegal / consumeBatch / correctTagsOffset 全在本地
// 状态上；onQueuesRevoked 在 mqClient_ 为空时直接返回（本端口不做真 RPC 的分支）。
class Harness {
public:
    Harness() : consumer_(kGroup), key_(DefaultMQPushConsumer::offsetKey(queue0())) {
        listener_ = std::make_shared<RecordingListener>();
        consumer_.setMessageListener(listener_);
        consumer_.setAssignedQueue(key_, queue0());
    }

    // 消费一批并原地推进位点：位点表没有直接写入面，用真实消费路径产生一个已知位点
    // （离线时 sendMessageBack 必失败，所以这里只用整批认可的批次）。
    void seedConsumed(int64_t uptoOffsetExclusive) {
        std::vector<MessageExt> batch;
        for (int64_t i = 0; i < uptoOffsetExclusive; i++) {
            batch.push_back(ext(i));
        }
        consumer_.consumeBatch(key_, queue0(), batch, consumer_.queueEpoch(key_));
    }

    void recover(int64_t corrected) { consumer_.handleOffsetIllegal(key_, corrected); }

    const std::string& key() const { return key_; }
    RecordingListener& listener() { return *listener_; }
    DefaultMQPushConsumer& consumer() { return consumer_; }

private:
    DefaultMQPushConsumer consumer_;
    std::string key_;
    std::shared_ptr<RecordingListener> listener_;
};

std::string offText(const std::optional<int64_t>& v) {
    return v ? std::to_string(*v) : "none";
}

void expectOffset(Harness& h, std::optional<int64_t> want, const std::string& name) {
    const auto got = h.consumer().consumeOffset(h.key());
    expect(got == want, name, "want=" + offText(want) + " got=" + offText(got));
}

// ------------------------------------------------- 纠错 = 冻结 + 丢队列 + 代号 +1

void testRecoverDropsQueueState() {
    Harness h;
    h.seedConsumed(3);                    // 位点 3
    expectOffset(h, 3, "recover.seedOffset");
    h.consumer().setPendingMessages(h.key(), {ext(3), ext(4)});
    h.consumer().setLastPullAt(h.key(), 1234567890);

    h.recover(0);

    // 队列的本地状态全部作废：缓冲、游标、已消费位点、拉取时刻表（余下由代号与冻结体现）
    expect(h.consumer().pendingMessages(h.key()).empty(), "recover.pendingCleared");
    expectOffset(h, std::nullopt, "recover.offsetTableCleared");
    expect(h.consumer().queueEpoch(h.key()) == 1, "recover.epochBumped",
           std::to_string(h.consumer().queueEpoch(h.key())));
    expect(h.consumer().offsetFrozen(h.key()), "recover.frozen");
    // 归属被撤：时刻表清空后旧拉取循环下一轮 ownsQueue 即失败退出（也顺带证明
    // 纠错走的就是 rebalance 那条 retireQueueLocked，而不是另写一份半套清理）
    expect(h.consumer().lastPullAt(h.key()) == -1, "recover.ownershipDropped",
           std::to_string(h.consumer().lastPullAt(h.key())));
}

void testRecoverWithoutQueueIsNoop() {
    // 队列已经不在本实例名下（并发撤销）：没有 mq 就没有可落盘的对象，不能凭空造一条；
    // 冻结与代号照常（纠错值仍要挡住任何迟到的 ack），且本端口的网络收尾在哨兵判空后
    // 直接返回 —— 未 start 的消费者上调用不得崩。
    Harness h;
    const std::string key = DefaultMQPushConsumer::offsetKey(MessageQueue("T2", kBroker, 7));
    h.consumer().handleOffsetIllegal(key, 5);
    expect(h.consumer().queueEpoch(key) == 1, "noQueue.epochBumped",
           std::to_string(h.consumer().queueEpoch(key)));
    expect(h.consumer().offsetFrozen(key), "noQueue.frozen");
    expectOffset(h, std::nullopt, "noQueue.noOffsetInvented");
    expect(!h.consumer().offsetFrozen(h.key()), "noQueue.siblingUntouched");
}

void testSecondRecoverKeepsEpochMonotonic() {
    // 连续两次纠错（broker 又被重置）：代号只能单调涨，回落成 1 会让**第一次**纠错误丢的
    // 旧批次突然「代号又对上了」而复活。
    Harness h;
    h.recover(0);
    h.recover(0);
    expect(h.consumer().queueEpoch(h.key()) == 2, "twice.epochMonotonic",
           std::to_string(h.consumer().queueEpoch(h.key())));
    expect(h.consumer().offsetFrozen(h.key()), "twice.stillFrozen");
}

// ------------------------------------------------- 冻结：修正值不被在途 ack 推翻

void testFrozenOffsetIgnoresAck() {
    Harness h;
    h.seedConsumed(3);                    // 位点 3（纠错前）
    h.recover(0);                         // 纠错到 0 并冻结
    const int callsBefore = h.listener().calls;

    // 旧批次（offset 0..2 已取回、还没 ack）迟到的 ack：把位点推回 3 就是 Java 的弹跳现场。
    // 冻结必须挡住它 —— 位点表里不能再出现任何条目（周期落盘也就无物可写）。
    const uint64_t epoch = h.consumer().queueEpoch(h.key());
    h.consumer().consumeBatch(h.key(), queue0(), offsetBatch(3), epoch);

    expect(h.listener().calls > callsBefore, "frozenAck.listenerRan");
    expectOffset(h, std::nullopt, "frozenAck.offsetUntouched");
}

void testFrozenOffsetIgnoresCorrectTagsOffset() {
    Harness h;
    h.recover(0);
    h.consumer().correctTagsOffset(h.key(), PullStatus::NO_NEW_MSG, 110);
    expectOffset(h, std::nullopt, "frozenTags.offsetUntouched");
}

void testFrozenOffsetSurvivesTheDrop() {
    // 有意偏差：Java 在 removeOffset 时解冻、靠 ProcessQueue.isDropped() 兜底；本端口没有
    // per-batch 的 ProcessQueue 对象，冻结一直留到队列重建（更严）——重建前任何 ack 都不许
    // 动位点，宁可让位点停在纠错值上等重建。
    Harness h;
    h.recover(0);
    h.seedConsumed(2);                    // 试图把位点从「无」推到 2
    expectOffset(h, std::nullopt, "frozenSurvives.noResurrect");
    expect(h.consumer().offsetFrozen(h.key()), "frozenSurvives.stillFrozen");
}

// ------------------------------------------------- 旧代号的批次整批作废（Java :267/:339）

void testConsumeBatchEpochGate() {
    // 代号对不上 ⇒ 队列已被撤销/重建，这批消息**连 listener 都不进**（Java
    // ConsumeMessageConcurrentlyService:339 的 isDropped() 短路），也不 ack。
    {
        Harness h;
        const uint64_t epoch = h.consumer().queueEpoch(h.key());
        const bool done = h.consumer().consumeBatch(h.key(), queue0(), offsetBatch(2),
                                                    epoch + 7);
        expect(!done, "epochGate.staleReturnsFalse");
        expect(h.listener().calls == 0, "epochGate.staleNotConsumed",
               std::to_string(h.listener().calls));
        expectOffset(h, std::nullopt, "epochGate.staleNoAck");
    }
    {
        // 代号一致 ⇒ 正常消费并推进（闸门不能把正常路径也挡掉）
        Harness h;
        const uint64_t epoch = h.consumer().queueEpoch(h.key());
        const bool done = h.consumer().consumeBatch(h.key(), queue0(), offsetBatch(2), epoch);
        expect(done, "epochGate.currentConsumed");
        expect(h.listener().calls == 1, "epochGate.currentListenerRan");
        expectOffset(h, 2, "epochGate.currentAck");
    }
}

void testNoCrossQueueInterference() {
    // 纠错只动这一条队列：兄弟队列的代号/冻结/归属必须原样（一条 "clear 全表" 的
    // 实现会让同实例的其它队列全部重投）。
    Harness h;
    const std::string sibling =
        DefaultMQPushConsumer::offsetKey(MessageQueue(kTopic, kBroker, 1));
    h.consumer().setAssignedQueue(sibling, MessageQueue(kTopic, kBroker, 1));
    h.consumer().setLastPullAt(sibling, 1234567890);
    const uint64_t siblingEpoch = h.consumer().queueEpoch(sibling);

    h.recover(0);

    expect(h.consumer().queueEpoch(sibling) == siblingEpoch, "cross.siblingEpoch");
    expect(!h.consumer().offsetFrozen(sibling), "cross.siblingNotFrozen");
    expect(h.consumer().lastPullAt(sibling) == 1234567890, "cross.siblingStillAssigned",
           std::to_string(h.consumer().lastPullAt(sibling)));
}

}  // namespace

int main() {
    testRecoverDropsQueueState();
    testRecoverWithoutQueueIsNoop();
    testSecondRecoverKeepsEpochMonotonic();
    testFrozenOffsetIgnoresAck();
    testFrozenOffsetIgnoresCorrectTagsOffset();
    testFrozenOffsetSurvivesTheDrop();
    testConsumeBatchEpochGate();
    testNoCrossQueueInterference();
    std::printf("offset illegal recover: %d checks, %d failures\n", checks, fails);
    return fails == 0 ? 0 : 1;
}
