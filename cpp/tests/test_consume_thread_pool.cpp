// 消费线程弹性单测（对应 Java DefaultMQPushConsumer 的线程池配置与 updateCorePoolSize）。
//
// 为什么单独一个文件：Java 的 ThreadPoolExecutor 在 LinkedBlockingQueue（无界）下，
// **真实并发度 == corePoolSize**（max 永远用不到），所以 updateCorePoolSize 是"运行时能改
// 并发度"的 API。C++ 侧此前 POP 路径是"每批一个 detached 线程"（无上限）且
// setConsumeThreadNums() 完全没作用 —— 换成有界 core/max 执行器后，本文件把语义钉死。
//
// 另一个必须钉住的反直觉事实：**Java 5.5.1 的自动弹性（inc/decCorePoolSize）是空实现**，
// adjustThreadPool() 整套是 no-op。我们照抄 no-op，不允许"顺手修好"。
#include <atomic>
#include <chrono>
#include <cstdio>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "rocketmq/client/consume_executor.h"
#include "rocketmq/client/consumer.h"
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

void expectEq(long long actual, long long expected, const std::string& name) {
    ++checks;
    if (actual != expected) {
        ++fails;
        std::printf("FAIL %s (actual=%lld expected=%lld)\n", name.c_str(), actual, expected);
    }
}

void sleepMs(int ms) { std::this_thread::sleep_for(std::chrono::milliseconds(ms)); }

MessageExt makeMsg(int64_t queueOffset, const std::string& maxOffset) {
    MessageExt m;
    m.topic = "T";
    m.body = "x";
    m.setQueueOffset(queueOffset);
    if (!maxOffset.empty()) {
        m.putProperty(MessageConst::PROPERTY_MAX_OFFSET, maxOffset);
    }
    return m;
}

// ---------------------------------------------------------------- ConsumeExecutor

void testSpawnsUpToCoreThenQueues() {
    ConsumeExecutor ex(2, 8, 30.0);
    std::atomic<int> running{0};
    std::atomic<bool> release{false};
    auto block = [&running, &release]() {
        running.fetch_add(1);
        while (!release.load()) sleepMs(5);
        running.fetch_sub(1);
    };
    ex.submit(block);
    ex.submit(block);
    // 两个 worker 被占住；第 3、4 个任务只能排队（未到 core 时不会建线程）
    ex.submit([]() {});
    ex.submit([]() {});
    sleepMs(100);
    expectEq(ex.workerCount(), 2, "spawns up to core");
    expectEq(ex.queuedCount(), 2, "beyond core is queued");
    release.store(true);
    ex.shutdown(true);
    expectEq(running.load(), 0, "all tasks finished after shutdown(true)");
}

void testRaisingCoreSpawnsForQueuedTasks() {
    // Java setCorePoolSize 的启发式：k = min(delta, 队列长度)，逐个补线程，队列空则停。
    ConsumeExecutor ex(1, 8, 30.0);
    std::atomic<bool> release{false};
    ex.submit([&release]() { while (!release.load()) sleepMs(5); });
    sleepMs(50);
    for (int i = 0; i < 3; ++i) ex.submit([]() {});
    expectEq(ex.workerCount(), 1, "only core thread before raising");
    expectEq(ex.queuedCount(), 3, "3 tasks queued");
    ex.setCorePoolSize(4);   // delta=3, queue=3 → 应补 3 个
    sleepMs(150);
    expectEq(ex.workerCount(), 4, "delta workers added on core raise");
    expectEq(ex.queuedCount(), 0, "queue drained");
    release.store(true);
    ex.shutdown(true);
}

void testExtraThreadRetiresCoreThreadDoesNot() {
    // > core 的线程空闲到 keepAlive 退出；<= core 的线程永不退出（Java allowCoreThreadTimeOut=false）
    ConsumeExecutor ex(1, 4, 0.2);
    ex.submit([]() {});
    ex.setCorePoolSize(2);   // 队列空 → 不补线程，core 变 2
    sleepMs(50);
    expectEq(ex.getCorePoolSize(), 2, "core pool size applied");
    ex.submit([]() {});      // 把 worker 抬到 2（仍 <= core）
    sleepMs(50);
    expectEq(ex.workerCount(), 2, "two workers alive");
    ex.setCorePoolSize(1);   // core 降到 1 → 多出来的那个成为"超编"
    sleepMs(600);            // 超过 keepAlive
    expectEq(ex.workerCount(), 1, "extra worker retired after keep alive");
    sleepMs(400);
    expectEq(ex.workerCount(), 1, "core worker never retires");
    ex.shutdown(true);
}

void testTaskExceptionDoesNotKillWorker() {
    ConsumeExecutor ex(1, 2, 30.0);
    ex.submit([]() { throw std::runtime_error("boom"); });
    sleepMs(200);
    expectEq(ex.handlerExceptionCount(), 1, "exception counted");
    expectEq(ex.workerCount(), 1, "worker survived exception");
    std::atomic<bool> ran{false};
    ex.submit([&ran]() { ran.store(true); });
    sleepMs(200);
    expect(ran.load(), "same worker keeps serving");
    ex.shutdown(true);
}

void testSubmitAfterShutdownThrows() {
    ConsumeExecutor ex(1, 2);
    ex.shutdown(false);
    bool threw = false;
    try {
        ex.submit([]() {});
    } catch (const std::runtime_error&) {
        threw = true;
    }
    expect(threw, "submit after shutdown throws");
    ex.shutdown(true);
}

void testShutdownWaitDrainsQueue() {
    ConsumeExecutor ex(1, 2, 30.0);
    std::atomic<int> done{0};
    for (int i = 0; i < 5; ++i) {
        ex.submit([&done]() {
            sleepMs(20);
            done.fetch_add(1);
        });
    }
    ex.shutdown(true);
    expectEq(done.load(), 5, "shutdown(wait) drains queued tasks");
}

void testZeroCoreStillRunsTasks() {
    // Java execute 的兜底分支：入队后若 workerCount == 0 仍要补一个线程
    ConsumeExecutor ex(0, 2, 0.1);
    std::atomic<bool> ran{false};
    ex.submit([&ran]() { ran.store(true); });
    sleepMs(200);
    expect(ran.load(), "core=0 still executes tasks");
    ex.shutdown(true);
}

void testCorePoolSizeMutation() {
    ConsumeExecutor ex(3, 9, 30.0);
    expectEq(ex.getCorePoolSize(), 3, "initial core");
    expectEq(ex.getMaximumPoolSize(), 9, "initial max");
    ex.setCorePoolSize(5);
    expectEq(ex.getCorePoolSize(), 5, "core raised");
    ex.setCorePoolSize(2);
    expectEq(ex.getCorePoolSize(), 2, "core lowered");
    ex.setCorePoolSize(12);  // Java 允许 core > max（等价于把 max 抬到 core）
    expectEq(ex.getMaximumPoolSize(), 12, "max raised to core");
    ex.shutdown(true);
}

// ---------------------------------------------------------------- 消费者侧（不联网）

void testJavaDefaults() {
    DefaultMQPushConsumer c("GID_ThreadPoolUnit");
    expectEq(c.getConsumeThreadMin(), 20, "consumeThreadMin default = 20");
    expectEq(c.getConsumeThreadMax(), 20, "consumeThreadMax default = 20");
    expectEq(c.getAdjustThreadPoolNumsThreshold(), 100000, "adjustThreadPoolNumsThreshold default");
    expectEq(c.getCorePoolSize(), 20, "corePoolSize default = consumeThreadMin");
}

void testUpdateCorePoolSizeGuards() {
    DefaultMQPushConsumer c("GID_ThreadPoolUnit");
    // Java 用无界队列 ⇒ 真实并发度 == core；默认 max=20，故 core 只能往**下**调
    expect(c.updateCorePoolSize(15), "15 accepted (below default max)");
    expectEq(c.getCorePoolSize(), 15, "core updated to 15");
    expect(!c.updateCorePoolSize(0), "0 rejected");
    expect(!c.updateCorePoolSize(-1), "-1 rejected");
    expect(!c.updateCorePoolSize(20), "== consumeThreadMax rejected");
    expect(c.updateCorePoolSize(19), "19 accepted (just below max)");
    expectEq(c.getCorePoolSize(), 19, "core updated to 19");
    // Short.MAX_VALUE 上界：要把 consumeThreadMax 抬上去才轮得到这条守卫
    c.setConsumeThreadMax(40000);
    expect(!c.updateCorePoolSize(32768), "32768 rejected (above Short.MAX_VALUE)");
    expect(c.updateCorePoolSize(32767), "32767 accepted (upper bound itself)");
    expectEq(c.getCorePoolSize(), 32767, "core updated to Short.MAX_VALUE");
}

// 回归：默认 max 曾照抄 4.x 的 64，于是 20~63 这些 Java 会**忽略**的值能生效。
// Java 5.5.1 的 consumeThreadMax 与 min 同为 20（DefaultMQPushConsumer:169），守卫
// `corePoolSize < consumeThreadMax` 把默认配置下的上调全挡掉；差异不报错，只表现为
// "同一个 updateCorePoolSize(30)，本端口真的改了并发度、Java 没改"。
void testDefaultMaxIsJava5xTwenty() {
    DefaultMQPushConsumer c("GID_ThreadPoolUnit");
    expectEq(c.getConsumeThreadMax(), 20, "default max is Java 5.x's 20");
    expect(!c.updateCorePoolSize(30), "30 rejected under default max");
    expect(!c.updateCorePoolSize(21), "21 rejected under default max");
    expectEq(c.getCorePoolSize(), 20, "nothing landed");
    // 抬 max 之后区间重新打开（setter 与 Java 一样是裸赋值，只夹 >= 1）
    c.setConsumeThreadMax(30);
    expect(c.updateCorePoolSize(25), "25 accepted after raising max to 30");
    expectEq(c.getCorePoolSize(), 25, "core updated to 25");
}

void testSetConsumeThreadNumsSetsBothAndCore() {
    DefaultMQPushConsumer c("GID_ThreadPoolUnit");
    c.setConsumeThreadNums(4);
    expectEq(c.getConsumeThreadMin(), 4, "min set by setConsumeThreadNums");
    expectEq(c.getConsumeThreadMax(), 4, "max set by setConsumeThreadNums");
    expectEq(c.getCorePoolSize(), 4, "core follows setConsumeThreadNums");
    expect(!c.updateCorePoolSize(4), "4 rejected after max became 4");
    expect(c.updateCorePoolSize(3), "3 accepted after max became 4");
}

void testConsumeThreadMinSetterMovesCoreMaxDoesNot() {
    DefaultMQPushConsumer c("GID_ThreadPoolUnit");
    c.setConsumeThreadMin(8);
    expectEq(c.getCorePoolSize(), 8, "core follows consumeThreadMin");
    c.setConsumeThreadMax(16);
    expectEq(c.getConsumeThreadMax(), 16, "max updated");
    expectEq(c.getCorePoolSize(), 8, "max change does not move core");
    c.setConsumeThreadMin(0);
    c.setConsumeThreadMax(-5);
    expectEq(c.getConsumeThreadMin(), 1, "min clamped to 1");
    expectEq(c.getConsumeThreadMax(), 1, "max clamped to 1");
}

void testMsgAccCntRules() {
    DefaultMQPushConsumer c("GID_ThreadPoolUnit");
    // accTotal = MAX_OFFSET - queueOffset，取**最后一条**
    c.updateMsgAccCnt("k1", {makeMsg(10, "100"), makeMsg(12, "100")});
    expectEq(c.msgAccCnt("k1"), 88, "acc = maxOffset - lastQueueOffset");
    // 非正不更新
    c.updateMsgAccCnt("k2", {makeMsg(100, "100")});
    expectEq(c.msgAccCnt("k2"), 0, "zero delta ignored");
    c.updateMsgAccCnt("k2", {makeMsg(200, "100")});
    expectEq(c.msgAccCnt("k2"), 0, "negative delta ignored");
    // 属性缺失 / 非数字 / 带尾巴一律忽略
    c.updateMsgAccCnt("k3", {makeMsg(10, "")});
    expectEq(c.msgAccCnt("k3"), 0, "missing property ignored");
    c.updateMsgAccCnt("k3", {makeMsg(10, "not-a-number")});
    expectEq(c.msgAccCnt("k3"), 0, "garbage property ignored");
    c.updateMsgAccCnt("k3", {makeMsg(10, "100abc")});
    expectEq(c.msgAccCnt("k3"), 0, "trailing garbage ignored");
    // 新一批覆盖旧值
    c.updateMsgAccCnt("k4", {makeMsg(10, "500")});
    c.updateMsgAccCnt("k4", {makeMsg(400, "450")});
    expectEq(c.msgAccCnt("k4"), 50, "latest batch overwrites");
    // 求和：换一个干净的消费者，避免被上面几组 key 的残留值影响
    DefaultMQPushConsumer sum("GID_ThreadPoolUnit2");
    sum.updateMsgAccCnt("a", {makeMsg(0, "30")});
    sum.updateMsgAccCnt("b", {makeMsg(0, "70")});
    expectEq(sum.msgAccCnt(), 100, "sum across queues");
    expectEq(sum.computeAccumulationTotal(), 100, "computeAccumulationTotal sums msgAccCnt");
}

void testAdjustThreadPoolIsNoOp() {
    DefaultMQPushConsumer c("GID_ThreadPoolUnit");
    c.setAdjustThreadPoolNumsThreshold(100);
    c.updateMsgAccCnt("a", {makeMsg(0, "500")});   // 500 >= 100 → inc 分支
    const int32_t before = c.getCorePoolSize();
    c.adjustThreadPool();
    expectEq(c.getCorePoolSize(), before, "inc branch is a no-op");
    c.setAdjustThreadPoolNumsThreshold(1000);
    c.updateMsgAccCnt("a", {makeMsg(0, "10")});    // 10 < 800 → dec 分支
    c.adjustThreadPool();
    expectEq(c.getCorePoolSize(), before, "dec branch is a no-op");
    // 自动 no-op ≠ API 失效
    expect(c.updateCorePoolSize(11), "explicit update still works after adjustThreadPool");
    expectEq(c.getCorePoolSize(), 11, "explicit core applied");
}

void testExecutorObservabilityWithoutStart() {
    DefaultMQPushConsumer c("GID_ThreadPoolUnit");
    // 未 start（未建执行器）时观测值恒为 0，不应崩
    expectEq(c.consumeExecutorWorkers(), 0, "no executor before start");
    expectEq(c.consumeExecutorQueued(), 0, "no queued tasks before start");
}

}  // namespace

int main() {
    testSpawnsUpToCoreThenQueues();
    testRaisingCoreSpawnsForQueuedTasks();
    testExtraThreadRetiresCoreThreadDoesNot();
    testTaskExceptionDoesNotKillWorker();
    testSubmitAfterShutdownThrows();
    testShutdownWaitDrainsQueue();
    testZeroCoreStillRunsTasks();
    testCorePoolSizeMutation();
    testJavaDefaults();
    testUpdateCorePoolSizeGuards();
    testDefaultMaxIsJava5xTwenty();
    testSetConsumeThreadNumsSetsBothAndCore();
    testConsumeThreadMinSetterMovesCoreMaxDoesNot();
    testMsgAccCntRules();
    testAdjustThreadPoolIsNoOp();
    testExecutorObservabilityWithoutStart();

    std::printf("%s: %d checks, %d failed\n", fails == 0 ? "PASS" : "FAIL", checks, fails);
    return fails == 0 ? 0 : 1;
}
