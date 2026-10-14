// Why a separate file: the whole backpressure semantics rest on this one primitive, and it has two behaviors that
// no ordinary semaphore gives for free -- "fairness" (only the queue head may acquire; latecomers must not cut in line)
// and "changing capacity in place at runtime" (in-flight counts are kept and stuck waiters are woken). How the producer
// side uses it (call back when it cannot acquire, return exactly once, run inline when the queue is full) lives in the backpressure section of test_producer_async.cpp.
#include <atomic>
#include <chrono>
#include <cstdio>
#include <exception>
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

void testTryAcquireTimesOutWithoutThrowing() {
    FairSemaphore sem(1);
    expect(sem.tryAcquire(1, 0), "the first acquire succeeds");
    const auto began = std::chrono::steady_clock::now();
    expect(!sem.tryAcquire(1, 120), "a taken semaphore times out instead of throwing");
    const int64_t waited = sinceMs(began);
    expect(waited >= 100, "the timeout is not returned early", "waited=" + std::to_string(waited));
    expect(waited < 1000, "the timeout is not waited forever", "waited=" + std::to_string(waited));
    // The timed-out waiter has left the queue and must not block later waiters forever on a vanished request
    sem.release(1);
    expectEq(sem.waitingCount(), 0, "a timed out waiter leaves the queue");
    expect(sem.tryAcquire(1, 0), "the next acquirer is not blocked by the departed one");
}

// 2. Non-positive budgets never wait
void testNonPositiveTimeoutNeverWaits() {
    FairSemaphore sem(0);
    expect(!sem.tryAcquire(1, 0), "zero budget does not wait");
    expect(!sem.tryAcquire(1, -5000), "a negative budget is clamped to zero, not to forever");
    expectEq(sem.waitingCount(), 0, "neither attempt stays queued");
}

// 3. The whole point of fairness: a request queued behind others must not cut in, even if free permits now cover it
void testOnlyTheQueueHeadIsGranted() {
    FairSemaphore sem(2);
    expect(sem.tryAcquire(2, 0), "drain the semaphore");
    std::vector<std::string> bigResult;
    std::thread big([&]() {
        const bool got = sem.tryAcquire(2, 5000);
        bigResult.push_back(got ? "big-got" : "big-lost");
    });
    expect(waitFor([&] { return sem.waitingCount() == 1; }), "the big request gets queued");
    // Now 0 free and the head wants 2. Releasing 1 only covers the small request -- it must keep waiting.
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
    sem.release(1);  // top up the 2 the queue head wants
    big.join();
    expect(!bigResult.empty() && bigResult[0] == "big-got", "the head is served first",
           bigResult.empty() ? "no result" : bigResult[0]);
    expectEq(sem.availablePermits(), 0, "both grants consumed the two released permits");
}

// 4. Release order is the wake order (the observable promise of a fair semaphore)
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

// 5. After the head takes its permits, if the remaining free ones cover a later waiter, it must eventually get them
void testGrantedHeadStillFeedsTheWaiterBehindIt() {
    // Locks down that "one release grants only one waiter" never happens: 6 free, the head wants 5, the second wants 1,
    // both must be granted and neither may hit the timeout. (tryAcquire still notifies once more after acquiring,
    // covering the layout where the second waiter queues **after** that notification -- that window is
    // not reliably reproducible here; the other half of the regression guard is the timed-out-exit case below.)
    FairSemaphore sem(6);
    expect(sem.tryAcquire(5, 0), "five permits are in flight");  // 1 free
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
    sem.release(5);  // free 1 -> 6: the head is covered and the leftover 1 goes exactly to the second
    head.join();
    second.join();
    expectEq(headResult.load(), 1, "the head is granted");
    expectEq(secondResult.load(), 1, "both queued requests are eventually granted");
    // Acquiring must not burn the whole budget: sleeping to 3000ms means it was never woken
    expect(secondWaitedMs.load() < 2000, "the waiter behind the head is woken promptly",
           "waited=" + std::to_string(static_cast<long long>(secondWaitedMs.load())) + "ms");
    expectEq(sem.availablePermits(), 0, "6 free covered 5 + 1");
}

// 6. Lost-wakeup regression guard: even when the head **times out**, it must hand the chance to the waiters behind
void testTimedOutHeadWakesTheWaiterBehindIt() {
    // The head wants 4 > total 3, so it can never succeed; the waiter behind wants 1 and can get it.
    // Without the exit-path notify_all, second sleeps the full 3000ms: on a real machine an async send would burn
    // the whole budget for nothing, then call back a semaphoreAsyncNum timeout while permits have long been free.
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
    sem.release(3);  // free is enough for second, but the queue head is the greedy one
    head.join();
    second.join();
    expectEq(headResult.load(), -1, "asking for more than the total never succeeds");
    expectEq(secondResult.load(), 1, "the departing head must wake the waiter behind it");
    // Acquiring must not burn the whole budget: sleeping to 3000ms means it was never woken
    expect(secondWaitedMs.load() < 2000, "the departing head wakes the next waiter",
           "waited=" + std::to_string(static_cast<long long>(secondWaitedMs.load())) + "ms");
}

void testSetTotalKeepsOutstandingWork() {
    FairSemaphore sem(10);
    expect(sem.tryAcquire(4, 0), "four permits are in flight");
    sem.setTotalPermits(15);
    expectEq(sem.availablePermits(), 11, "free = new total - outstanding");
    expectEq(sem.totalPermits(), 15, "total is the configured value");
    sem.release(4);
    expectEq(sem.availablePermits(), 15, "releasing the in-flight part fills the new total");
}

void testShrinkBelowOutstandingGivesNegativeFree() {
    FairSemaphore sem(10);
    expect(sem.tryAcquire(6, 0), "six permits are in flight");
    sem.setTotalPermits(2);
    expectEq(sem.availablePermits(), -4, "shrinking below the in-flight part goes negative");
    sem.release(6);
    expectEq(sem.availablePermits(), 2, "releasing pulls it back to the new total");
    expect(sem.tryAcquire(2, 0), "the shrunk capacity is usable");
}

// 9. Growing capacity must wake whoever is blocked in the queue -- exactly why this implementation never swaps the Semaphore object
void testGrowWakesSomeoneBlockedOnTheOldCapacity() {
    FairSemaphore sem(1);
    expect(sem.tryAcquire(1, 0), "drain the only permit");
    std::vector<bool> got;
    std::thread waiter([&] { got.push_back(sem.tryAcquire(1, 5000)); });
    expect(waitFor([&] { return sem.waitingCount() == 1; }), "the waiter is blocked on the gate");
    sem.setTotalPermits(5);
    sem.release(1);          // hand back the in-flight permit
    waiter.join();
    expect(!got.empty() && got[0], "the resize wakes the blocked sender",
           got.empty() ? "no result" : (got[0] ? "got it" : "lost it"));
    expectEq(sem.availablePermits(), 4, "the woken permit is consumed, the rest is free");
}

void testReleaseBeyondTotalIsAllowed() {
    FairSemaphore sem(1);
    sem.release(3);
    expectEq(sem.availablePermits(), 4, "release does not clamp at total");
    expectEq(sem.totalPermits(), 1, "... but it does not move the total either");
}

// 11. Non-positive acquire/release requests are benign no-op-level inputs (the producer counts an empty batch as 1; it does not rely on this)
void testNonPositiveRequestsAreBenign() {
    FairSemaphore sem(0);
    expect(sem.tryAcquire(0, 0), "a zero-permit request is satisfied even when empty");
    sem.release(0);
    sem.release(-3);
    expectEq(sem.availablePermits(), 0, "a non-positive release adds nothing");
}

void testFloorsMatchDefaults() {
    expectEq(kMinAsyncSendNum, 10, "backPressureForAsyncSendNum floor");
    expectEq(kMinAsyncSendSize, 1024 * 1024, "backPressureForAsyncSendSize floor");
}

}  // namespace

// An unexpected exception in a case must become a readable failure instead of terminating the whole process
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
    // The longest cases wait out their timeouts; over 4s means someone hangs dead on a semaphore
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
    runCase("floorsMatchDefaults", testFloorsMatchDefaults);

    std::printf("%s: %d checks, %d failures\n", fails == 0 ? "PASS" : "FAIL", checks, fails);
    return fails == 0 ? 0 : 1;
}
