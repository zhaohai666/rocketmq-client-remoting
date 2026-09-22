// 异步发送背压基元单测：FairSemaphore 本身（对端是 Java new Semaphore(permits, true)）。
//
// 为什么单独一个文件：整套背压语义都压在这个基元上，而它有两处**不是**随便一个信号量都
// 满足的行为 —— 「公平」（只有队首能拿，后来者不许插队）和「运行时原地改容量」（在途份数
// 保留、并且把卡住的等待者叫醒）。生产者那一侧怎么用（拿不到就回调、只还一次、队满就地跑）
// 在 test_producer_async.cpp 的背压一节。
#include <atomic>
#include <chrono>
#include <cstdio>
#include <functional>
#include <string>
#include <thread>
#include <vector>

#include "rocketmq/client/backpressure.h"

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

bool waitFor(const std::function<bool()>& pred, int timeoutMillis = 3000) {
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeoutMillis);
    while (!pred()) {
        if (std::chrono::steady_clock::now() >= deadline) return false;
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    return true;
}

int64_t sinceMs(const std::chrono::steady_clock::time_point& from) {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::steady_clock::now() - from)
        .count();
}

// 1. 超时返回 false 而不抛（Java tryAcquire(permits, timeout, MILLIS) 只有 interrupt 才抛）
void testTryAcquireTimesOutWithoutThrowing() {
    FairSemaphore sem(1);
    expect(sem.tryAcquire(1, 0), "the first acquire succeeds");
    const auto began = std::chrono::steady_clock::now();
    expect(!sem.tryAcquire(1, 120), "a taken semaphore times out instead of throwing");
    const int64_t waited = sinceMs(began);
    expect(waited >= 100, "the timeout is not returned early", "waited=" + std::to_string(waited));
    expect(waited < 1000, "the timeout is not waited forever", "waited=" + std::to_string(waited));
    // 超时的人已经出队，不会把后面的人永久挡在一个已经消失的请求上
    sem.release(1);
    expectEq(sem.waitingCount(), 0, "a timed out waiter leaves the queue");
    expect(sem.tryAcquire(1, 0), "the next acquirer is not blocked by the departed one");
}

// 2. 非正预算一律不等
void testNonPositiveTimeoutNeverWaits() {
    FairSemaphore sem(0);
    expect(!sem.tryAcquire(1, 0), "zero budget does not wait");
    expect(!sem.tryAcquire(1, -5000), "a negative budget is clamped to zero, not to forever");
    expectEq(sem.waitingCount(), 0, "neither attempt stays queued");
}

// 3. 公平模式的全部意义：排在别人后面的请求不许插队，哪怕许可现在够它
void testOnlyTheQueueHeadIsGranted() {
    FairSemaphore sem(2);
    expect(sem.tryAcquire(2, 0), "drain the semaphore");
    std::vector<std::string> bigResult;
    std::thread big([&]() {
        const bool got = sem.tryAcquire(2, 5000);
        bigResult.push_back(got ? "big-got" : "big-lost");
    });
    expect(waitFor([&] { return sem.waitingCount() == 1; }), "the big request gets queued");
    // 现在空闲 0、队首要 2 个。还 1 个只够小请求 —— 它必须等。
    std::vector<std::string> smallResult;
    std::thread small([&]() {
        const bool got = sem.tryAcquire(1, 300);
        smallResult.push_back(got ? "small-got" : "small-lost");
    });
    expect(waitFor([&] { return sem.waitingCount() == 2; }), "the small request queues behind");
    sem.release(1);
    small.join();
    expect(!smallResult.empty() && smallResult[0] == "small-lost",
           "the later small request must not jump the queue",
           smallResult.empty() ? "no result" : smallResult[0]);
    sem.release(1);  // 补齐队首要的 2 个
    big.join();
    expect(!bigResult.empty() && bigResult[0] == "big-got", "the head is served first",
           bigResult.empty() ? "no result" : bigResult[0]);
    expectEq(sem.availablePermits(), 0, "both grants consumed the two released permits");
}

// 4. 归还顺序就是醒来顺序（公平信号量的可观察承诺）
void testReleaseWakesTheHeadInOrder() {
    FairSemaphore sem(1);
    expect(sem.tryAcquire(1, 0), "drain the semaphore");
    std::mutex orderM;
    std::vector<std::string> order;
    const auto record = [&](const std::string& tag, bool got) {
        std::lock_guard<std::mutex> lk(orderM);
        order.push_back(got ? tag : (tag + "-lost"));
    };
    std::thread a([&] { record("a", sem.tryAcquire(1, 5000)); });
    expect(waitFor([&] { return sem.waitingCount() == 1; }), "a gets in first");
    std::thread b([&] { record("b", sem.tryAcquire(1, 5000)); });
    expect(waitFor([&] { return sem.waitingCount() == 2; }), "b queues behind a");
    sem.release(1);
    sem.release(1);
    a.join();
    b.join();
    std::lock_guard<std::mutex> lk(orderM);
    expect(order.size() == 2 && order[0] == "a" && order[1] == "b",
           "arrival order is the wake order",
           order.empty() ? "no result" : (order[0] + "," + (order.size() > 1 ? order[1] : "")));
}

// 5. 队首拿走许可之后，剩下的空闲若够后面的人，他必须最终拿到
void testGrantedHeadStillFeedsTheWaiterBehindIt() {
    // 锁的是「一次 release 只放一个人」不会发生：空闲 6、队首要 5、第二个人要 1，
    // 两人都该拿到、且都不该等到超时。（tryAcquire 拿到许可后还会再 notify_all 一次，
    // 为的是第二个人在那次通知**之后**才排上队的排布 —— 那个窗口在这里复现不稳定，
    // 另一半的回归守卫是下面那条超时退出的用例。）
    FairSemaphore sem(6);
    expect(sem.tryAcquire(5, 0), "five permits are in flight");  // 空闲 1
    std::atomic<int> headResult{0};
    std::atomic<int> secondResult{0};
    std::atomic<int64_t> secondWaitedMs{-1};
    std::thread head([&] { headResult.store(sem.tryAcquire(5, 3000) ? 1 : -1); });
    expect(waitFor([&] { return sem.waitingCount() == 1; }), "the head request is queued");
    std::thread second([&] {
        const auto began = std::chrono::steady_clock::now();
        const bool got = sem.tryAcquire(1, 3000);
        secondWaitedMs.store(sinceMs(began));
        secondResult.store(got ? 1 : -1);
    });
    expect(waitFor([&] { return sem.waitingCount() == 2; }), "the second request queues behind");
    sem.release(5);  // 空闲 1 → 6：队首够，剩下 1 个正好给第二个
    head.join();
    second.join();
    expectEq(headResult.load(), 1, "the head is granted");
    expectEq(secondResult.load(), 1, "both queued requests are eventually granted");
    // 拿到许可不该花掉整个预算：睡到 3000ms 就是根本没被叫醒
    expect(secondWaitedMs.load() < 2000, "the waiter behind the head is woken promptly",
           "waited=" + std::to_string(static_cast<long long>(secondWaitedMs.load())) + "ms");
    expectEq(sem.availablePermits(), 0, "6 free covered 5 + 1");
}

// 6. 丢唤醒的回归守卫：队首**超时退出**也必须把机会交给后面的人
void testTimedOutHeadWakesTheWaiterBehindIt() {
    // 队首要 4 个 > 总量 3，它永远拿不到；后面那个要 1 个，等得到。
    // 少了离场时的 notify_all，second 会一路睡满 3000ms：真机上就是异步发送白等满预算，
    // 再回调一个 semaphoreAsyncNum timeout，而许可早就空出来了。
    FairSemaphore sem(3);
    expect(sem.tryAcquire(3, 0), "drain the semaphore");
    std::atomic<int> headResult{0};
    std::atomic<int> secondResult{0};
    std::atomic<int64_t> secondWaitedMs{-1};
    std::thread head([&] { headResult.store(sem.tryAcquire(4, 200) ? 1 : -1); });
    expect(waitFor([&] { return sem.waitingCount() == 1; }), "the greedy head is queued");
    std::thread second([&] {
        const auto began = std::chrono::steady_clock::now();
        const bool got = sem.tryAcquire(1, 3000);
        secondWaitedMs.store(sinceMs(began));
        secondResult.store(got ? 1 : -1);
    });
    expect(waitFor([&] { return sem.waitingCount() == 2; }), "the small request queues behind");
    sem.release(3);  // 空闲够 second，但队首是那个贪心的
    head.join();
    second.join();
    expectEq(headResult.load(), -1, "asking for more than the total never succeeds");
    expectEq(secondResult.load(), 1, "the departing head must wake the waiter behind it");
    // 拿到许可不该花掉整个预算：睡到 3000ms 就是根本没被叫醒
    expect(secondWaitedMs.load() < 2000, "the departing head wakes the next waiter",
           "waited=" + std::to_string(static_cast<long long>(secondWaitedMs.load())) + "ms");
}

// 7. 改总量 = 「空闲 = 新总量 - 在途」，与 Java 的 new Semaphore(num - acquired) 同解
void testSetTotalKeepsOutstandingWork() {
    FairSemaphore sem(10);
    expect(sem.tryAcquire(4, 0), "four permits are in flight");
    sem.setTotalPermits(15);
    expectEq(sem.availablePermits(), 11, "free = new total - outstanding");
    expectEq(sem.totalPermits(), 15, "total is the configured value");
    sem.release(4);
    expectEq(sem.availablePermits(), 15, "releasing the in-flight part fills the new total");
}

// 8. Java 的 new Semaphore(负数) 合法，归还许可会把它拉回正数 —— 这里同样接受
void testShrinkBelowOutstandingGivesNegativeFree() {
    FairSemaphore sem(10);
    expect(sem.tryAcquire(6, 0), "six permits are in flight");
    sem.setTotalPermits(2);
    expectEq(sem.availablePermits(), -4, "shrinking below the in-flight part goes negative");
    sem.release(6);
    expectEq(sem.availablePermits(), 2, "releasing pulls it back to the new total");
    expect(sem.tryAcquire(2, 0), "the shrunk capacity is usable");
}

// 9. 扩容要把正堵在队列里的人叫醒 —— 这正是本实现不换 Semaphore 对象的理由
void testGrowWakesSomeoneBlockedOnTheOldCapacity() {
    FairSemaphore sem(1);
    expect(sem.tryAcquire(1, 0), "drain the only permit");
    std::vector<bool> got;
    std::thread waiter([&] { got.push_back(sem.tryAcquire(1, 5000)); });
    expect(waitFor([&] { return sem.waitingCount() == 1; }), "the waiter is blocked on the gate");
    sem.setTotalPermits(5);  // 扩容：Java 此刻换的是**另一个** Semaphore 对象，等待者白等
    sem.release(1);          // 在途的那份还回去
    waiter.join();
    expect(!got.empty() && got[0], "the resize wakes the blocked sender",
           got.empty() ? "no result" : (got[0] ? "got it" : "lost it"));
    expectEq(sem.availablePermits(), 4, "the woken permit is consumed, the rest is free");
}

// 10. Java 的 release() 不校验是否超过总量（信号量可以被"无中生有"地放大）
void testReleaseBeyondTotalIsAllowed() {
    FairSemaphore sem(1);
    sem.release(3);
    expectEq(sem.availablePermits(), 4, "release does not clamp at total");
    expectEq(sem.totalPermits(), 1, "… but it does not move the total either");
}

// 11. 非正数的申请/归还都是 no-op 级别的良性输入（批量消息为空时生产者按 1 算，不靠这里）
void testNonPositiveRequestsAreBenign() {
    FairSemaphore sem(0);
    expect(sem.tryAcquire(0, 0), "a zero-permit request is satisfied even when empty");
    sem.release(0);
    sem.release(-3);
    expectEq(sem.availablePermits(), 0, "a non-positive release adds nothing");
}

// 12. 地板值与 Java 逐字对齐（DefaultMQProducer:1386 / :1402）
void testFloorsMatchJava() {
    expectEq(kMinAsyncSendNum, 10, "backPressureForAsyncSendNum floor");
    expectEq(kMinAsyncSendSize, 1024 * 1024, "backPressureForAsyncSendSize floor");
}

}  // namespace

// 用例里未预期的异常必须变成可读的失败，而不是把整个进程 terminate 掉
static void runCase(const char* name, void (*fn)()) {
    const auto began = std::chrono::steady_clock::now();
    try {
        fn();
    } catch (const std::exception& e) {
        ++checks;
        ++fails;
        std::printf("FAIL %s threw: %s\n", name, e.what());
    } catch (...) {
        ++checks;
        ++fails;
        std::printf("FAIL %s threw: unknown error\n", name);
    }
    const int64_t ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                           std::chrono::steady_clock::now() - began)
                           .count();
    // 这些用例最长的是「等满超时」那一档，超过 4s 就是有人在信号量上挂死了
    if (ms > 4000) std::printf("SLOW %s: %lldms\n", name, static_cast<long long>(ms));
}

int main() {
    runCase("tryAcquireTimesOutWithoutThrowing", testTryAcquireTimesOutWithoutThrowing);
    runCase("nonPositiveTimeoutNeverWaits", testNonPositiveTimeoutNeverWaits);
    runCase("onlyTheQueueHeadIsGranted", testOnlyTheQueueHeadIsGranted);
    runCase("releaseWakesTheHeadInOrder", testReleaseWakesTheHeadInOrder);
    runCase("grantedHeadStillFeedsTheWaiterBehindIt",
            testGrantedHeadStillFeedsTheWaiterBehindIt);
    runCase("timedOutHeadWakesTheWaiterBehindIt", testTimedOutHeadWakesTheWaiterBehindIt);
    runCase("setTotalKeepsOutstandingWork", testSetTotalKeepsOutstandingWork);
    runCase("shrinkBelowOutstandingGivesNegativeFree",
            testShrinkBelowOutstandingGivesNegativeFree);
    runCase("growWakesSomeoneBlockedOnTheOldCapacity",
            testGrowWakesSomeoneBlockedOnTheOldCapacity);
    runCase("releaseBeyondTotalIsAllowed", testReleaseBeyondTotalIsAllowed);
    runCase("nonPositiveRequestsAreBenign", testNonPositiveRequestsAreBenign);
    runCase("floorsMatchJava", testFloorsMatchJava);

    std::printf("%s: %d checks, %d failures\n", fails == 0 ? "PASS" : "FAIL", checks, fails);
    return fails == 0 ? 0 : 1;
}
