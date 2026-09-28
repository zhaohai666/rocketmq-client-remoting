// correctTagsOffset（Java DefaultMQPushConsumerImpl:713-717，调用点 :394-401）单测 —— 不需要集群。
//
// 为什么必须离线锁死：拉取应答是 NO_NEW_MSG / NO_MATCHED_MSG 时，若 ProcessQueue 里没有消息，
// Java 会把"已消费位点"抬到应答的 nextBeginOffset（increaseOnly=true）。漏了这件事是**静默**的：
// broker 侧按 tags/表达式把这一段的每一条都滤掉时，客户端既收不到消息、位点也不动，
// queryConsumerOffset 永远落后，重启后把这批没人要的消息从头再扫一遍。反过来，闸门写松
//（不等在途批次落定就抬位点）同样是静默的：进程崩溃时那批消息被跳过。
//
// 闸门 = Java 的 `0L == processQueue.getMsgCount()`：msgCount 数的是**仍在 ProcessQueue 里**的
// 消息，而并发消费的 removeMessage 要等 listener 返回才跑（ConsumeMessageConcurrentlyService:266），
// 所以在途批次也算数。本端口把 pending_ 为空与 inflightCount_ 为 0 合成同一判据。
// 与 python/tests/test_correct_tags_offset.py、rust/src/client/consumer.rs 的同名测试同题；
// 真机证据（broker 过滤 → NO_MATCHED_MSG → broker 侧位点前移）见 examples/live_correct_tags_offset.cpp。
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

const char* kGroup = "GID_CorrectTagsOffsetCppUnit";
const char* kTopic = "CorrectTagsOffsetCppUnitTopic";
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

// 未 start 的消费者即可：correctTagsOffset / takeBatchForConsume / finishBatchConsume
// 全在本地状态上，不碰网络。
class Harness {
public:
    explicit Harness() : consumer_(kGroup), key_(DefaultMQPushConsumer::offsetKey(queue0())) {
        consumer_.setConsumeMessageBatchMaxSize(2);
    }

    void seedPending(const std::vector<MessageExt>& msgs) {
        consumer_.setPendingMessages(key_, msgs);
    }

    void correct(PullStatus status, int64_t nextOffset) {
        consumer_.correctTagsOffset(key_, status, nextOffset);
    }

    std::optional<int64_t> offset() const { return consumer_.consumeOffset(key_); }

    std::vector<MessageExt> take() { return consumer_.takeBatchForConsume(key_); }
    void finish() { consumer_.finishBatchConsume(key_); }
    std::vector<MessageExt> pending() const { return consumer_.pendingMessages(key_); }

private:
    DefaultMQPushConsumer consumer_;
    std::string key_;
};

std::string offText(const std::optional<int64_t>& v) {
    return v ? std::to_string(*v) : "none";
}

void expectOffset(const Harness& h, std::optional<int64_t> want, const std::string& name) {
    const auto got = h.offset();
    expect(got == want, name, "want=" + offText(want) + " got=" + offText(got));
}

void testStatuses() {
    // 空应答的两个状态都推进；其余状态一律不碰位点
    for (PullStatus s : {PullStatus::NO_NEW_MSG, PullStatus::NO_MATCHED_MSG}) {
        Harness h;
        h.seedPending({});
        h.correct(s, 42);
        expectOffset(h, 42, std::string("empty.") + pullStatusName(s) + ".advance");
        h.correct(s, 43);
        expectOffset(h, 43, std::string("empty.") + pullStatusName(s) + ".advanceAgain");
    }
    for (PullStatus s : {PullStatus::FOUND, PullStatus::OFFSET_ILLEGAL}) {
        Harness h;
        h.seedPending({});
        h.correct(s, 42);
        expectOffset(h, std::nullopt, std::string("notEmpty.") + pullStatusName(s) + ".ignored");
    }
}

void testIncreaseOnly() {
    // Java 的 updateOffset(..., increaseOnly=true)：只前进不回退
    Harness h;
    h.seedPending({});
    h.correct(PullStatus::NO_NEW_MSG, 100);
    expectOffset(h, 100, "increaseOnly.seed");
    h.correct(PullStatus::NO_NEW_MSG, 50);
    expectOffset(h, 100, "increaseOnly.noRegress");
    h.correct(PullStatus::NO_NEW_MSG, 100);
    expectOffset(h, 100, "increaseOnly.equalStays");
    h.correct(PullStatus::NO_NEW_MSG, 101);
    expectOffset(h, 101, "increaseOnly.advance");
}

void testProcessQueueGuard() {
    {
        // 缓冲里还有没消费的：闸门关上
        Harness h;
        h.seedPending({ext(0)});
        h.correct(PullStatus::NO_NEW_MSG, 42);
        expectOffset(h, std::nullopt, "guard.pendingNonEmpty");
    }
    {
        // 缓冲空了，但有一条在 listener 手里（removeMessage 还没跑）：一样关上
        Harness h;
        h.seedPending({ext(0)});
        std::vector<MessageExt> batch = h.take();
        expect(batch.size() == 1, "guard.takeOne", std::to_string(batch.size()));
        h.correct(PullStatus::NO_MATCHED_MSG, 42);
        expectOffset(h, std::nullopt, "guard.inflightBlocks");

        // 消费收尾（含异常回塞路径）后计数归零，修正才允许生效
        h.finish();
        h.correct(PullStatus::NO_MATCHED_MSG, 42);
        expectOffset(h, 42, "guard.afterFinish");
    }
    {
        // 连 pending 表项都没有（从未拉过该队列）按空处理，不能因为缺项漏掉修正
        Harness h;
        h.correct(PullStatus::NO_NEW_MSG, 7);
        expectOffset(h, 7, "guard.missingPendingAllows");
    }
}

void testTakeFinishWiring() {
    {
        // takeBatchForConsume 一次按 consumeMessageBatchMaxSize 取走一批，且必须与
        // 登记在途在同一临界区：这里靠"取走后闸门立刻关上"来观察
        Harness h;
        h.seedPending(offsetBatch(3));
        std::vector<MessageExt> batch = h.take();
        expect(batch.size() == 2, "wiring.batchSize", std::to_string(batch.size()));
        h.correct(PullStatus::NO_NEW_MSG, 42);
        expectOffset(h, std::nullopt, "wiring.takenBlocks");
        expect(h.pending().size() == 1, "wiring.pendingLeft",
               std::to_string(h.pending().size()));

        // 还有余量没被取走：缓冲非空，同样挡着
        h.finish();
        h.correct(PullStatus::NO_NEW_MSG, 42);
        expectOffset(h, std::nullopt, "wiring.pendingStillBlocks");

        // 第二批发完，闸门全开
        std::vector<MessageExt> rest = h.take();
        expect(rest.size() == 1, "wiring.restSize", std::to_string(rest.size()));
        h.finish();
        h.correct(PullStatus::NO_NEW_MSG, 42);
        expectOffset(h, 42, "wiring.drainedAllows");
    }
    {
        // 两批同时在途（两个消费线程）：计数必须累加，一次 finish 不足以放行
        Harness h;
        h.seedPending(offsetBatch(4));
        h.take();      // 2 条
        h.take();      // 又 2 条：在途 2 批
        h.finish();
        h.correct(PullStatus::NO_NEW_MSG, 42);
        expectOffset(h, std::nullopt, "wiring.stillOneInFlight");
        h.finish();
        h.correct(PullStatus::NO_NEW_MSG, 42);
        expectOffset(h, 42, "wiring.bothFinishedAllows");
    }
    {
        // 计数清零后被擦除的 key 必须能重新登记：第二批在途时闸门要重新关上
        Harness h;
        h.seedPending(offsetBatch(3));
        h.take();      // 2 条
        h.finish();
        h.take();      // 第 3 条：在途重新计数
        h.correct(PullStatus::NO_NEW_MSG, 42);
        expectOffset(h, std::nullopt, "wiring.reRegistered");
        h.finish();
        h.correct(PullStatus::NO_NEW_MSG, 42);
        expectOffset(h, 42, "wiring.reRegisteredFinishAllows");
    }
    {
        // 对空队列 take 不该凭空记上一笔在途（否则闸门被自己关死）
        Harness h;
        h.seedPending({});
        expect(h.take().empty(), "wiring.emptyTake");
        h.correct(PullStatus::NO_NEW_MSG, 42);
        expectOffset(h, 42, "wiring.emptyTakeNoLeak");
    }
    {
        // finish 打在已被擦掉的 key 上是 no-op，不能把计数恢复出来
        Harness h;
        h.seedPending(offsetBatch(1));
        h.take();
        h.finish();
        h.finish();
        h.correct(PullStatus::NO_NEW_MSG, 42);
        expectOffset(h, 42, "wiring.doubleFinishNoResurrect");
    }
}

}  // namespace

int main() {
    testStatuses();
    testIncreaseOnly();
    testProcessQueueGuard();
    testTakeFinishWiring();
    std::printf("correct tags offset: %d checks, %d failures\n", checks, fails);
    return fails == 0 ? 0 : 1;
}
