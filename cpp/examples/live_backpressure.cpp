// Live verification of async-send backpressure (the ``enableBackpressureForAsyncMode`` fair-semaphore machinery)
// (asserting scenarios B1–B5 one by one).
// Usage: rmq_live_backpressure 127.0.0.1:9876
//
// Prerequisite: NameServer + Broker up with ``autoCreateTopicEnable=true``.
//
//
// ⚠ Three script disciplines:
//   (1) topics always carry the stamp -- successive live runs share the same broker, and fixed names would inherit the previous round's message counts;
//   (2) "a rejected send never even issued the request" can only be checked via broker-side stored counts (the sum of maxOffset-minOffset over queues);
//      client callbacks alone can be fooled by implementations that error the callback yet still send the request;
//   (3) a newly created topic must wait for the broker to incrementally register its topicConfig with the name server (seconds to tens of seconds), so every
//      broker-side reconciliation **polls until timeout**; a single failed route read is not a failure.
#include <atomic>
#include <chrono>
#include <cstdio>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "rocketmq/client/admin.h"
#include "rocketmq/client/backpressure.h"
#include "rocketmq/client/exception.h"
#include "rocketmq/client/hook.h"
#include "rocketmq/client/producer.h"
#include "rocketmq/client/result.h"
#include "rocketmq/common/message.h"
#include "rocketmq/remoting/protocol/route.h"

using namespace rocketmq;

namespace {

int32_t gPass = 0;
int32_t gFail = 0;
std::vector<std::string> gFailed;

void check(const std::string& name, bool ok, const std::string& detail = std::string()) {
    if (ok) {
        ++gPass;
        std::printf("  [PASS] %s%s\n", name.c_str(), detail.empty() ? "" : ("  " + detail).c_str());
    } else {
        ++gFail;
        gFailed.push_back(name);
        std::printf("  [FAIL] %s%s\n", name.c_str(), detail.empty() ? "" : ("  " + detail).c_str());
    }
}

std::string num(int64_t v) { return std::to_string(v); }

std::string flag(bool b) { return b ? "yes" : "no"; }

int64_t stamp() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::system_clock::now().time_since_epoch())
        .count();
}

int64_t nowMs() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::steady_clock::now().time_since_epoch())
        .count();
}

bool waitUntil(const std::function<bool()>& pred, int32_t timeoutMs) {
    const int64_t deadline = nowMs() + timeoutMs;
    while (nowMs() < deadline) {
        if (pred()) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
    return pred();
}

// Thread-safe callback recorder
class Recorder : public SendCallback {
public:
    void onSuccess(const SendResult& r) override {
        std::lock_guard<std::mutex> lk(m_);
        ++done_;
        if (r.sendStatus == SendStatus::SEND_OK) ++ok_;
    }

    void onException(const std::exception_ptr& e) override {
        std::lock_guard<std::mutex> lk(m_);
        ++done_;
        errors_.push_back(exceptionMessage(e));
        errorTypes_.push_back(exceptionTypeName(e));
        if (firstError_.empty()) firstError_ = errors_.back();
    }

    size_t done() {
        std::lock_guard<std::mutex> lk(m_);
        return done_;
    }

    size_t ok() {
        std::lock_guard<std::mutex> lk(m_);
        return ok_;
    }

    size_t errorCount() {
        std::lock_guard<std::mutex> lk(m_);
        return errors_.size();
    }

    bool allErrorsContain(const std::string& needle, size_t expected) {
        std::lock_guard<std::mutex> lk(m_);
        if (errors_.size() != expected || done_ != expected) return false;
        for (const std::string& e : errors_) {
            if (e.find(needle) == std::string::npos) return false;
        }
        return true;
    }

    bool allErrorsAre(const std::string& typeName, size_t expected) {
        std::lock_guard<std::mutex> lk(m_);
        if (errorTypes_.size() != expected || done_ != expected) return false;
        for (const std::string& t : errorTypes_) {
            if (t != typeName) return false;
        }
        return true;
    }

    std::string summary() {
        std::lock_guard<std::mutex> lk(m_);
        return "ok=" + std::to_string(ok_) + " err=" + std::to_string(errors_.size())
             + (firstError_.empty() ? ""
                                    : " " + firstError_
                                          + (errorTypes_.empty() ? "" : " ["
                                                                           + errorTypes_.front()
                                                                           + "]"));
    }

private:
    std::mutex m_;
    size_t done_ = 0;
    size_t ok_ = 0;
    std::vector<std::string> errors_;
    std::vector<std::string> errorTypes_;
    std::string firstError_;
};

// Sleeps for a while inside ``sendMessageBefore``.
// Permits are taken **when the gate is passed** and released **at the end of the chain**, so the cleanest way to keep in-flight slots occupied is to slow the chain itself:
// blocking the user callback cannot hold a permit (the release happens one step before the result is handed to the user).
class SlowHook : public SendMessageHook {
public:
    explicit SlowHook(int64_t millis) : millis_(millis) {}

    std::string hookName() const override { return "slow-before"; }

    void sendMessageBefore(SendMessageContext&) override {
        const int64_t ms = millis_.load();
        if (ms > 0) std::this_thread::sleep_for(std::chrono::milliseconds(ms));
    }

    void sendMessageAfter(SendMessageContext&) override {}

    void setMillis(int64_t ms) { millis_.store(ms); }

private:
    std::atomic<int64_t> millis_;
};

// ---------------------------------------------------------------- Environment
struct Env {
    std::string nsAddr;
    int64_t s = 0;
    std::unique_ptr<DefaultMQAdminExt> admin;

    std::string topic(const std::string& prefix) const {
        return prefix + "_" + std::to_string(s);
    }
};

// The hook must be registered before start()
std::shared_ptr<DefaultMQProducer> makeProducer(Env& env, const std::string& instance, bool enable,
                                                int32_t num = -1, int32_t size = -1,
                                                std::shared_ptr<SendMessageHook> hook = nullptr) {
    auto p = std::make_shared<DefaultMQProducer>();
    p->setProducerGroup("PID_rmq_bp_cpp_" + std::to_string(env.s));
    p->setNamesrvAddr(env.nsAddr);
    p->setInstanceName("bp-cpp-" + instance + "-" + std::to_string(env.s));
    p->setEnableBackpressureForAsyncMode(enable);
    if (num >= 0) p->setBackPressureForAsyncSendNum(num);
    if (size >= 0) p->setBackPressureForAsyncSendSize(size);
    if (hook) p->registerSendMessageHook(std::move(hook));
    p->start();
    return p;
}

int64_t numPermits(const std::shared_ptr<DefaultMQProducer>& p) {
    return p->getSemaphoreAsyncSendNumAvailablePermits();
}

int64_t sizePermits(const std::shared_ptr<DefaultMQProducer>& p) {
    return p->getSemaphoreAsyncSendSizeAvailablePermits();
}

// How many messages of this topic have landed on the broker (the sum of maxOffset-minOffset over its queues); returns -1 when the route cannot be read
int64_t landedCount(DefaultMQAdminExt& admin, const std::string& topic) {
    std::vector<MessageQueue> queues;
    try {
        queues = admin.examineTopicRoute(topic).getAllMessageQueue(topic);
    } catch (const std::exception&) {
        return -1;  // the route has not been registered yet
    }
    int64_t total = 0;
    for (const MessageQueue& mq : queues) {
        try {
            total += admin.maxOffset(mq) - admin.minOffset(mq);
        } catch (const std::exception&) {
            // this queue was just created and has never been written to
        }
    }
    return total;
}

// Wait until at least expected messages have landed on the broker and return the last reading (registering a new topic in the name server takes seconds)
int64_t waitLanded(Env& env, const std::string& topic, int64_t expected, int seconds = 30) {
    const int64_t deadline = nowMs() + seconds * 1000LL;
    int64_t landed = landedCount(*env.admin, topic);
    while (landed < expected && nowMs() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
        landed = landedCount(*env.admin, topic);
    }
    if (landed < 0) landedCount(*env.admin, topic);  // when it really cannot be read, probe once more to help locate the issue
    return landed;
}

std::string brokerAddr(Env& env) {
    try {
        const ClusterInfo info = env.admin->fetchBrokerClusterInfo();
        const std::vector<std::string> addrs = info.getBrokerAddrs();
        if (!addrs.empty()) return addrs[0];
    } catch (const std::exception&) {
    }
    return "127.0.0.1:10911";
}

// ---------------------------------------------------------------- B1 Default capacity returns every permit
void b1DefaultCapacityNoLeak(Env& env, const std::string& t) {
    auto p = makeProducer(env, "b1", true);
    auto rec = std::make_shared<Recorder>();
    std::vector<std::thread> threads;
    for (int i = 0; i < 40; ++i) {
        const std::string body = "b1-" + std::to_string(i) + "-" + std::string(120, 'x');
        threads.emplace_back([p, t, rec, body]() { p->sendAsync(Message(t, body), rec, 5000); });
    }
    for (std::thread& th : threads) th.join();
    check("B1 all 40 async sends reach their callback", waitUntil([&]() { return rec->done() >= 40; }, 15000),
          rec->summary());
    check("B1 all SEND_OK", rec->ok() == 40 && rec->errorCount() == 0, rec->summary());
    const int64_t landed = waitLanded(env, t, 40);
    check("B1 exactly 40 messages landed on the broker", landed == 40, "landed=" + num(landed));
    check("B1 every count permit is returned", numPermits(p) == 1024, "available=" + num(numPermits(p)));
    check("B1 every size permit is returned", sizePermits(p) == 100LL * 1024 * 1024,
          "available=" + num(sizePermits(p)));
    p->shutdown();
}

// ------------------------------------------------- B2/B3 the count gate: rejection, reconciliation, resizing
void b2AndB3NumGate(Env& env, const std::string& t) {
    auto slow = std::make_shared<SlowHook>(600);
    auto p = makeProducer(env, "b2", true, static_cast<int32_t>(kMinAsyncSendNum), -1, slow);
    auto held = std::make_shared<Recorder>();
    auto rejected = std::make_shared<Recorder>();

    std::vector<std::thread> holders;
    for (int i = 0; i < static_cast<int>(kMinAsyncSendNum); ++i) {
        holders.emplace_back([p, t, held]() { p->sendAsync(Message(t, "held"), held, 8000); });
    }
    check("B2 the free count drops to 0 once the in-flight slots are full", waitUntil([&]() { return numPermits(p) == 0; }, 3000),
          "available=" + num(numPermits(p)));

    // Sends #11 and #12: the budget is only 150ms, so they cannot wait for a permit
    const int64_t began = nowMs();
    std::vector<std::thread> rejects;
    for (int i = 0; i < 2; ++i) {
        rejects.emplace_back([p, t, rejected]() {
            p->sendAsync(Message(t, "rejected"), rejected, 150);
        });
    }
    for (std::thread& th : rejects) th.join();
    const int64_t waited = nowMs() - began;
    check("B2 the gate waits until the budget is exhausted before failing (it does not reject after a single look)", waited >= 130,
          "the caller waited " + num(waited) + "ms");
    check("B2 the two over-limit sends call back TooMuchRequest, with wording verbatim from the protocol",
          rejected->allErrorsContain("send message tryAcquire semaphoreAsyncNum timeout", 2),
          rejected->summary());
    check("B2 the rejected exception type is RemotingTooMuchRequestException (the protocol type, not the generic MQClientException)",
          rejected->allErrorsAre("RemotingTooMuchRequestException", 2), rejected->summary());

    for (std::thread& th : holders) th.join();
    check("B2 all 10 in-flight sends went out",
          waitUntil([&]() { return held->ok() == static_cast<size_t>(kMinAsyncSendNum); }, 20000),
          held->summary());
    const int64_t landed2 = waitLanded(env, t, kMinAsyncSendNum);
    check("B2 the two rejected ones left nothing on the broker (not even a request was sent)", landed2 == kMinAsyncSendNum,
          "landed=" + num(landed2) + " expected " + num(kMinAsyncSendNum));
    check("B2 the count permit is back to 10 once everything has landed",
          waitUntil([&]() { return numPermits(p) == kMinAsyncSendNum; }, 20000),
          "available=" + num(numPermits(p)));

    // B3: fill the 10 in-flight slots again, then raise the capacity to 12 -- whoever is stuck at the gate should be woken up.
    // This round lets the hook sleep for 2s: the time the in-flight slots are held must be far larger than the cost of
    // "start a thread + poll to confirm + start the waiter", otherwise the permits would be returned before the checks finish.
    slow->setMillis(2000);
    auto round2 = std::make_shared<Recorder>();
    std::vector<std::thread> holders2;
    for (int i = 0; i < static_cast<int>(kMinAsyncSendNum); ++i) {
        holders2.emplace_back(
            [p, t, round2]() { p->sendAsync(Message(t, "held2"), round2, 8000); });
    }
    check("B3 the second round fills the in-flight slots the same way", waitUntil([&]() { return numPermits(p) == 0; }, 3000),
          "available=" + num(numPermits(p)));

    auto woken = std::make_shared<Recorder>();
    std::atomic<bool> waiterReturned{false};
    std::thread waiter([p, t, woken, &waiterReturned]() {
        p->sendAsync(Message(t, "woken"), woken, 8000);
        waiterReturned.store(true);
    });
    std::this_thread::sleep_for(std::chrono::milliseconds(300));
    check("B3 the caller really is stuck at the gate before the resize", !waiterReturned.load() && numPermits(p) == 0,
          "caller passed the gate=" + flag(waiterReturned.load())
              + " available=" + num(numPermits(p)));
    p->setBackPressureForAsyncSendNum(static_cast<int32_t>(kMinAsyncSendNum + 2));
    waiter.join();
    // The caller thread being woken already means "it passed the gate", but the result only reaches the callback after the whole chain has run (the hook sleeps 2s this round),
    // so this waits for the callback rather than for the thread to exit.
    check("B3 the resize wakes the sender stuck at the gate and lets it go out", waitUntil([&]() { return woken->ok() >= 1; },
                                                              20000),
          woken->summary());
    for (std::thread& th : holders2) th.join();
    check("B3 the free permits after everything is returned = the new capacity 12",
          waitUntil([&]() { return numPermits(p) == kMinAsyncSendNum + 2; }, 25000),
          "available=" + num(numPermits(p)));
    const int64_t expect3 = 2 * kMinAsyncSendNum + 1;
    const int64_t landed3 = waitLanded(env, t, expect3);
    check("B3 the broker total = both in-flight rounds + the woken send", landed3 == expect3,
          "landed=" + num(landed3) + " expected " + num(expect3));
    p->shutdown();
}

// ------------------------------------------------- B4 the size gate (1M floor + 600KB)
void b4SizeGate(Env& env, const std::string& t) {
    auto p = makeProducer(env, "b4", true, static_cast<int32_t>(kMinAsyncSendNum),
                          static_cast<int32_t>(kMinAsyncSendSize),
                          std::make_shared<SlowHook>(400));
    const std::string big(600 * 1024, '4');
    const int64_t wantInFlight = kMinAsyncSendSize - static_cast<int64_t>(big.size());
    auto one = std::make_shared<Recorder>();
    auto two = std::make_shared<Recorder>();

    std::thread first([p, t, one, big]() { p->sendAsync(Message(t, big), one, 8000); });
    check("B4 the in-flight size permits drop by exactly the body length",
          waitUntil([&]() { return sizePermits(p) == wantInFlight; }, 3000),
          "available=" + num(sizePermits(p)) + " expected " + num(wantInFlight));
    std::thread second([p, t, two, big]() { p->sendAsync(Message(t, big), two, 150); });
    second.join();
    check("B4 the second 600KB send cannot pass the size gate, with wording verbatim from the protocol",
          two->allErrorsContain("send message tryAcquire semaphoreAsyncSize timeout", 1),
          two->summary());
    check("B4 the size-gate rejection raises RemotingTooMuchRequestException",
          two->allErrorsAre("RemotingTooMuchRequestException", 1), two->summary());
    check("B4 the count permit is already returned when the size gate does not pass",
          waitUntil([&]() { return numPermits(p) == kMinAsyncSendNum; }, 3000),
          "available=" + num(numPermits(p)));
    first.join();
    check("B4 the first send lands normally", waitUntil([&]() { return one->done() >= 1; }, 20000), one->summary());
    const int64_t landed = waitLanded(env, t, 1);
    check("B4 only the first send is on the broker (the rejected one left no trace)", landed == 1, "landed=" + num(landed));
    check("B4 everything returned: both the count and the size are back to the configured amount",
          numPermits(p) == kMinAsyncSendNum && sizePermits(p) == kMinAsyncSendSize,
          "num=" + num(numPermits(p)) + " size=" + num(sizePermits(p)));
    p->shutdown();
}

// ------------------------------------------------- B5 turning backpressure off disables throttling
void b5GateOff(Env& env, const std::string& t) {
    auto p = makeProducer(env, "b5", false, static_cast<int32_t>(kMinAsyncSendNum),
                          static_cast<int32_t>(kMinAsyncSendSize),
                          std::make_shared<SlowHook>(200));
    auto rec = std::make_shared<Recorder>();
    std::vector<std::thread> threads;
    for (int i = 0; i < 30; ++i) {
        // One send in three is 300KB: with backpressure off the size gate is not even consulted, while with it on this would certainly be throttled
        const std::string body = (i % 3 == 0) ? std::string(300 * 1024, 'x') : "small";
        threads.emplace_back([p, t, rec, body]() { p->sendAsync(Message(t, body), rec, 8000); });
    }
    for (std::thread& th : threads) th.join();
    check("B5 with backpressure off all 30 concurrent sends succeed", waitUntil([&]() { return rec->done() >= 30; }, 25000),
          rec->summary());
    check("B5 all SEND_OK", rec->ok() == 30 && rec->errorCount() == 0, rec->summary());
    const int64_t landed = waitLanded(env, t, 30);
    check("B5 all 30 messages are on the broker", landed == 30, "landed=" + num(landed));
    p->shutdown();
}

}  // namespace

int main(int argc, char** argv) {
    const std::string nsAddr = argc > 1 ? argv[1] : "127.0.0.1:9876";
    Env env;
    env.nsAddr = nsAddr;
    env.s = stamp();
    env.admin = std::make_unique<DefaultMQAdminExt>("ADMIN_bp");
    env.admin->setNamesrvAddr(nsAddr);
    env.admin->setTimeoutMillis(10000);
    const std::string t1 = env.topic("BpDefault");
    const std::string t2 = env.topic("BpNumGate");
    const std::string t3 = env.topic("BpSizeGate");
    const std::string t4 = env.topic("BpOff");

    std::printf("############ C++ async-send backpressure live verification (namesrv=%s stamp=%lld)\n",
                nsAddr.c_str(), static_cast<long long>(env.s));
    try {
        env.admin->start();
        b1DefaultCapacityNoLeak(env, t1);
        b2AndB3NumGate(env, t2);
        b4SizeGate(env, t3);
        b5GateOff(env, t4);
    } catch (const std::exception& e) {
        check("whole script execution", false, e.what());
    }

    const std::string addr = brokerAddr(env);
    for (const std::string& topic : {t1, t2, t3, t4}) {
        try {
            env.admin->deleteTopicInBroker(addr, topic);
        } catch (const std::exception&) {
            // a cleanup failure does not affect the conclusion
        }
    }
    env.admin->shutdown();

    std::printf("\n############ PASS=%d FAIL=%d ############\n", gPass, gFail);
    for (const std::string& name : gFailed) std::printf("  FAILED: %s\n", name.c_str());
    return gFail == 0 ? 0 : 1;
}
