// ProcessQueue 停摆自愈（Java isPullExpired / PULL_MAX_IDLE_TIME）单测 —— 不需要集群。
//
// 为什么必须离线锁死：这条判据失灵的两种形态都是**静默**的。
//   - 不判（本端口改动前）：某队列的拉取循环一旦死掉或卡住，那个队列从此不再消费，
//     客户端不报错、心跳照发、别的队列照常推进 —— 只能从"这个组的位点卡住不动"反推；
//   - 阈值算错方向：判得太狠会把健康队列反复撤走重投，凭空造出重复消费。
// Java 的口径量得很死：ProcessQueue.java:43 读 `rocketmq.client.pull.pullMaxIdleTime`，
// 默认 **120000ms**（网上常写的 60s 是错的），盖章在发起处（pullMessage:253 /
// popMessage:508，**早于**流控与锁判定），撤走+重建在同一趟 rebalance 里
// （RebalanceImpl.updateProcessQueueTableInRebalance:438-461）。
//
// 这里锁阈值算术与逐队列时刻表；「撤掉之后真能重新消费」要起真线程、还要真 broker
// 认位点，只能在真机验（examples/live_redelivery.cpp 的 S11 停摆自愈场景）。
// 与 Python(tests/test_pull_expired.py)/Rust/.NET 的同名测试一一对应。
#include <cstdio>
#include <string>

#include "rocketmq/client/consumer.h"
#include "rocketmq/common/message.h"
#include "rocketmq/common/util_all.h"

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

const char* kGroup = "GID_PullExpiredCppUnit";
const char* kTopic = "PullExpiredCppUnitTopic";
const char* kBroker = "broker-a";

std::string queueKey(int32_t queueId) {
    return DefaultMQPushConsumer::offsetKey(MessageQueue(kTopic, kBroker, queueId));
}

void testThresholdMatchesJava() {
    // Java ProcessQueue:43 的默认值。写成 60s 会把健康队列提前撤走（凭空重投），
    // 写成更大则停摆的队列要更久才复原。
    expect(kPullMaxIdleTime == 120000, "threshold.120s", std::to_string(kPullMaxIdleTime));
}

void testNoStampIsNotStalled() {
    // 刚分配、还没跑到盖章处的循环不算停摆，否则"起线程 → 立刻被撤"来回抖动。
    DefaultMQPushConsumer c(kGroup);
    const std::string key = queueKey(0);
    expect(c.lastPullAt(key) == -1, "table.absent");
    expect(!c.pullStalled(key), "stamp.missing.notStalled");
}

void testFreshStampIsNotStalled() {
    DefaultMQPushConsumer c(kGroup);
    const std::string key = queueKey(0);
    c.setLastPullAt(key, UtilAll::currentTimeMillis());
    expect(!c.pullStalled(key), "stamp.fresh.notStalled");
    expect(c.lastPullAt(key) > 0, "stamp.written");
}

void testThresholdBoundaryIsStrict() {
    // Java 用的是 `>`（(now - lastPullTimestamp) > PULL_MAX_IDLE_TIME）：
    // 正好等于阈值不算停摆。判成 >= 会让边界队列每趟都被撤。
    DefaultMQPushConsumer c(kGroup);
    const std::string key = queueKey(0);
    const int64_t now = UtilAll::currentTimeMillis();
    c.setLastPullAt(key, now - kPullMaxIdleTime);
    expect(!c.pullStalled(key), "stamp.exactlyAtLimit.notStalled");
    c.setLastPullAt(key, now - kPullMaxIdleTime - 1);
    expect(c.pullStalled(key), "stamp.oneMsPast.stalled");
}

void testOldEnoughStampAlwaysStalls() {
    DefaultMQPushConsumer c(kGroup);
    const std::string key = queueKey(0);
    c.setLastPullAt(key, UtilAll::currentTimeMillis() - (kPullMaxIdleTime + 5000));
    expect(c.pullStalled(key), "stamp.stale.stalled");
}

void testLoopExitMarker() {
    // 拉取循环自己返回了、却仍持有该队列（异常打穿/订阅丢了）：线程包装器把时刻标成 0。
    // std::thread 死了问不出 is_alive()（joinable() 仍是 true），所以只能自己报到。
    DefaultMQPushConsumer c(kGroup);
    const std::string key = queueKey(0);
    c.setLastPullAt(key, UtilAll::currentTimeMillis());
    expect(!c.pullStalled(key), "marker.beforeExit.notStalled");
    c.setLastPullAt(key, 0);
    expect(c.pullStalled(key), "marker.afterExit.stalled");
}

void testStampIsPerQueue() {
    // 停摆判据是**逐队列**的：一个队列卡住不能把同 topic 其他队列一起撤走（那才是造重复）。
    DefaultMQPushConsumer c(kGroup);
    const std::string k0 = queueKey(0);
    const std::string k1 = queueKey(1);
    const int64_t now = UtilAll::currentTimeMillis();
    c.setLastPullAt(k0, now - kPullMaxIdleTime - 1000);
    c.setLastPullAt(k1, now);
    expect(c.pullStalled(k0), "perQueue.k0.stalled");
    expect(!c.pullStalled(k1), "perQueue.k1.notStalled");
}

}  // namespace

int main() {
    testThresholdMatchesJava();
    testNoStampIsNotStalled();
    testFreshStampIsNotStalled();
    testThresholdBoundaryIsStrict();
    testOldEnoughStampAlwaysStalls();
    testLoopExitMarker();
    testStampIsPerQueue();
    std::printf("pull expired: %d checks, %d failures\n", checks, fails);
    return fails == 0 ? 0 : 1;
}
