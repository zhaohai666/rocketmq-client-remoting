// 异步发送背压（``enableBackpressureForAsyncMode`` 那一套公平信号量）真机验证
// （对齐 python/verify_backpressure_live.py 的 B1–B5）。
// 用法：rmq_live_backpressure 127.0.0.1:9876
//
// 前置：NameServer + Broker 已起，``autoCreateTopicEnable=true``。
//
// 离线单测（``tests/test_backpressure.cpp`` 与 ``tests/test_producer_async.cpp`` 的
// 「异步发送背压」一节）锁的是语义；这个脚本锁的是**打到真 broker 时**的五件事：
//   B1  默认容量（1024 条 / 100M 字节）开着背压发一整轮异步消息：全部 SEND_OK，
//       且发完之后两个信号量**满额归还**（真机不泄配额 —— 泄了迟早把生产者自己锁死）。
//   B2  条数闸夹到地板值 10、用 ``sendMessageBefore`` 钩子睡 600ms 把在途占满：
//       第 11、12 笔在**调用方线程**上等不到许可，回调
//       ``send message tryAcquire semaphoreAsyncNum timeout``（Java :654-658 原文案），
//       而且 broker 上**一条都没多** —— 被拒的请求连路由都没查。
//   B3  运行时把容量从 10 调到 12：正卡在闸上的调用方被叫醒，broker 上多出那 1 条，
//       全部落地后空闲许可 = 新容量（这一轮钩子睡 2s，留出足够的观察窗口）。
//   B4  字节闸（容量 1M 地板值 + 600KB body ⇒ 在途只能 1 笔）：第二笔回调
//       ``send message tryAcquire semaphoreAsyncSize timeout``（Java :667-671），
//       在途时空闲字节许可正好是 ``1M - 600K``，broker 上只落 1 条。
//   B5  关掉背压：同样的容量配置**完全不限流**，30 笔并发（含 300KB 大 body）全部落地。
//
// ⚠ 三条脚本纪律（Python 那一轮真机联调踩出来的）：
//   ① topic 一律带 stamp —— 三语言**依次**跑在同一个 broker 上，固定名会继承上一轮的条数；
//   ② 「被拒的发送连请求都没发出去」只能看 broker 侧落库条数（各队列 maxOffset-minOffset 之和），
//      光看客户端回调会被「回调报错但请求照样发出去」的实现蒙过去；
//   ③ 新建 topic 要等 broker 把 topicConfig 增量注册到 namesrv（秒级到十秒级），所以所有
//      broker 侧对账都是**轮询到超时**，读一次路由失败不算失败。
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

// 线程安全的回调记录（对应 Python 验证脚本里的 _Recorder）
class Recorder : public SendCallback {
public:
    void onSuccess(const SendResult& r) override {
        std::lock_guard<std::mutex> lk(m_);
        ++done_;
        if (r.sendStatus == SendStatus::SEND_OK) ++ok_;
    }

    void onException(const std::string& e) override {
        std::lock_guard<std::mutex> lk(m_);
        ++done_;
        errors_.push_back(e);
        if (firstError_.empty()) firstError_ = e;
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

    // 「每一条错误都带这句文案」——Java 的文案是逐字对齐的，只查条数会放过内容漂移
    bool allErrorsContain(const std::string& needle, size_t expected) {
        std::lock_guard<std::mutex> lk(m_);
        if (errors_.size() != expected || done_ != expected) return false;
        for (const std::string& e : errors_) {
            if (e.find(needle) == std::string::npos) return false;
        }
        return true;
    }

    std::string summary() {
        std::lock_guard<std::mutex> lk(m_);
        return "ok=" + std::to_string(ok_) + " err=" + std::to_string(errors_.size())
             + (firstError_.empty() ? "" : " " + firstError_);
    }

private:
    std::mutex m_;
    size_t done_ = 0;
    size_t ok_ = 0;
    std::vector<std::string> errors_;
    std::string firstError_;
};

// 在 ``sendMessageBefore`` 里睡一会儿。
// 许可是**过闸时**拿的、**链终点**还的，所以占住在途最干净的办法是让链本身变慢：
// 堵用户回调占不住许可（归还就在把结果交给用户之前一步）。
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

// ---------------------------------------------------------------- 环境
struct Env {
    std::string nsAddr;
    int64_t s = 0;
    std::unique_ptr<DefaultMQAdminExt> admin;

    std::string topic(const std::string& prefix) const {
        return prefix + "_" + std::to_string(s);
    }
};

// 钩子必须在 start() 之前挂上（Python 验证脚本同样如此）
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

// broker 上这个 topic 一共落了多少条（各队列 maxOffset-minOffset 之和）；读不到路由返回 -1
int64_t landedCount(DefaultMQAdminExt& admin, const std::string& topic) {
    std::vector<MessageQueue> queues;
    try {
        queues = admin.examineTopicRoute(topic).getAllMessageQueue(topic);
    } catch (const std::exception&) {
        return -1;  // 路由还没注册上
    }
    int64_t total = 0;
    for (const MessageQueue& mq : queues) {
        try {
            total += admin.maxOffset(mq) - admin.minOffset(mq);
        } catch (const std::exception&) {
            // 该队列刚建出来还没写过
        }
    }
    return total;
}

// 等 broker 上至少出现 expected 条，返回最后一次读数（新 topic 注册到 namesrv 是秒级的）
int64_t waitLanded(Env& env, const std::string& topic, int64_t expected, int seconds = 30) {
    const int64_t deadline = nowMs() + seconds * 1000LL;
    int64_t landed = landedCount(*env.admin, topic);
    while (landed < expected && nowMs() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
        landed = landedCount(*env.admin, topic);
    }
    if (landed < 0) landedCount(*env.admin, topic);  // 真读不到时再打一次，便于定位
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

// ---------------------------------------------------------------- B1 默认容量不漏配额
void b1DefaultCapacityNoLeak(Env& env, const std::string& t) {
    auto p = makeProducer(env, "b1", true);
    auto rec = std::make_shared<Recorder>();
    std::vector<std::thread> threads;
    for (int i = 0; i < 40; ++i) {
        const std::string body = "b1-" + std::to_string(i) + "-" + std::string(120, 'x');
        threads.emplace_back([p, t, rec, body]() { p->sendAsync(Message(t, body), rec, 5000); });
    }
    for (std::thread& th : threads) th.join();
    check("B1 40 笔异步都走完回调", waitUntil([&]() { return rec->done() >= 40; }, 15000),
          rec->summary());
    check("B1 全部 SEND_OK", rec->ok() == 40 && rec->errorCount() == 0, rec->summary());
    const int64_t landed = waitLanded(env, t, 40);
    check("B1 broker 上正好落了 40 条", landed == 40, "landed=" + num(landed));
    check("B1 条数许可满额归还", numPermits(p) == 1024, "available=" + num(numPermits(p)));
    check("B1 字节许可满额归还", sizePermits(p) == 100LL * 1024 * 1024,
          "available=" + num(sizePermits(p)));
    p->shutdown();
}

// ------------------------------------------------- B2/B3 条数闸：拒绝、对账、扩容
void b2AndB3NumGate(Env& env, const std::string& t) {
    auto slow = std::make_shared<SlowHook>(600);
    auto p = makeProducer(env, "b2", true, static_cast<int32_t>(kMinAsyncSendNum), -1, slow);
    auto held = std::make_shared<Recorder>();
    auto rejected = std::make_shared<Recorder>();

    std::vector<std::thread> holders;
    for (int i = 0; i < static_cast<int>(kMinAsyncSendNum); ++i) {
        holders.emplace_back([p, t, held]() { p->sendAsync(Message(t, "held"), held, 8000); });
    }
    check("B2 在途占满后空闲条数为 0", waitUntil([&]() { return numPermits(p) == 0; }, 3000),
          "available=" + num(numPermits(p)));

    // 第 11、12 笔：预算只有 150ms，等不到许可
    const int64_t began = nowMs();
    std::vector<std::thread> rejects;
    for (int i = 0; i < 2; ++i) {
        rejects.emplace_back([p, t, rejected]() {
            p->sendAsync(Message(t, "rejected"), rejected, 150);
        });
    }
    for (std::thread& th : rejects) th.join();
    const int64_t waited = nowMs() - began;
    check("B2 闸等到预算耗尽才报错（不是看一眼就拒）", waited >= 130,
          "调用方等了 " + num(waited) + "ms");
    check("B2 超限的两笔回调 TooMuchRequest，文案与 Java 逐字一致",
          rejected->allErrorsContain("send message tryAcquire semaphoreAsyncNum timeout", 2),
          rejected->summary());

    for (std::thread& th : holders) th.join();
    check("B2 在途的 10 笔都发出去了",
          waitUntil([&]() { return held->ok() == static_cast<size_t>(kMinAsyncSendNum); }, 20000),
          held->summary());
    const int64_t landed2 = waitLanded(env, t, kMinAsyncSendNum);
    check("B2 被拒的两笔在 broker 上一条没留（连请求都没发）", landed2 == kMinAsyncSendNum,
          "landed=" + num(landed2) + " 期望 " + num(kMinAsyncSendNum));
    check("B2 全部落地后条数许可回到 10",
          waitUntil([&]() { return numPermits(p) == kMinAsyncSendNum; }, 20000),
          "available=" + num(numPermits(p)));

    // B3：再占满 10 个在途，把容量抬到 12 —— 卡在闸上的人应当被叫醒。
    // 这一轮把钩子睡到 2s：占住在途的时间必须远大于「起线程 + 轮询确认 + 起等待方」
    // 这几步的开销，否则检查还没做完整轮就已经归还了。
    slow->setMillis(2000);
    auto round2 = std::make_shared<Recorder>();
    std::vector<std::thread> holders2;
    for (int i = 0; i < static_cast<int>(kMinAsyncSendNum); ++i) {
        holders2.emplace_back(
            [p, t, round2]() { p->sendAsync(Message(t, "held2"), round2, 8000); });
    }
    check("B3 第二轮在途同样占满", waitUntil([&]() { return numPermits(p) == 0; }, 3000),
          "available=" + num(numPermits(p)));

    auto woken = std::make_shared<Recorder>();
    std::atomic<bool> waiterReturned{false};
    std::thread waiter([p, t, woken, &waiterReturned]() {
        p->sendAsync(Message(t, "woken"), woken, 8000);
        waiterReturned.store(true);
    });
    std::this_thread::sleep_for(std::chrono::milliseconds(300));
    check("B3 扩容前调用方确实卡在闸上", !waiterReturned.load() && numPermits(p) == 0,
          "调用方已过闸=" + flag(waiterReturned.load())
              + " available=" + num(numPermits(p)));
    p->setBackPressureForAsyncSendNum(static_cast<int32_t>(kMinAsyncSendNum + 2));
    waiter.join();
    // 调用方线程被叫醒就算「过闸了」，但结果要等整条链跑完（这一轮钩子睡 2s）才回到回调，
    // 所以这里等的是回调，不是线程退出。
    check("B3 扩容把卡在闸上的发送方叫醒并发了出去", waitUntil([&]() { return woken->ok() >= 1; },
                                                              20000),
          woken->summary());
    for (std::thread& th : holders2) th.join();
    check("B3 全部归还后空闲许可 = 新容量 12",
          waitUntil([&]() { return numPermits(p) == kMinAsyncSendNum + 2; }, 25000),
          "available=" + num(numPermits(p)));
    const int64_t expect3 = 2 * kMinAsyncSendNum + 1;
    const int64_t landed3 = waitLanded(env, t, expect3);
    check("B3 broker 总数 = 两轮在途 + 被叫醒的那一笔", landed3 == expect3,
          "landed=" + num(landed3) + " 期望 " + num(expect3));
    p->shutdown();
}

// ------------------------------------------------- B4 字节闸（1M 地板 + 600KB）
void b4SizeGate(Env& env, const std::string& t) {
    auto p = makeProducer(env, "b4", true, static_cast<int32_t>(kMinAsyncSendNum),
                          static_cast<int32_t>(kMinAsyncSendSize),
                          std::make_shared<SlowHook>(400));
    const std::string big(600 * 1024, '4');
    const int64_t wantInFlight = kMinAsyncSendSize - static_cast<int64_t>(big.size());
    auto one = std::make_shared<Recorder>();
    auto two = std::make_shared<Recorder>();

    std::thread first([p, t, one, big]() { p->sendAsync(Message(t, big), one, 8000); });
    check("B4 在途字节许可正好扣掉 body 长度",
          waitUntil([&]() { return sizePermits(p) == wantInFlight; }, 3000),
          "available=" + num(sizePermits(p)) + " 期望 " + num(wantInFlight));
    std::thread second([p, t, two, big]() { p->sendAsync(Message(t, big), two, 150); });
    second.join();
    check("B4 第二笔 600KB 过不了字节闸，文案与 Java 逐字一致",
          two->allErrorsContain("send message tryAcquire semaphoreAsyncSize timeout", 1),
          two->summary());
    // 字节闸没过时，先前拿到的条数许可必须原样还回去（Java BackpressureSendCallBack:599-610
    // 的先 size 后 num 归还）
    check("B4 字节闸没过时条数许可已经归还",
          waitUntil([&]() { return numPermits(p) == kMinAsyncSendNum; }, 3000),
          "available=" + num(numPermits(p)));
    first.join();
    check("B4 第一笔正常落地", waitUntil([&]() { return one->done() >= 1; }, 20000), one->summary());
    const int64_t landed = waitLanded(env, t, 1);
    check("B4 broker 上只有第一笔（被拒的没留痕）", landed == 1, "landed=" + num(landed));
    check("B4 全部归还：条数与字节都回到配置额",
          numPermits(p) == kMinAsyncSendNum && sizePermits(p) == kMinAsyncSendSize,
          "num=" + num(numPermits(p)) + " size=" + num(sizePermits(p)));
    p->shutdown();
}

// ------------------------------------------------- B5 关掉背压就不限流
void b5GateOff(Env& env, const std::string& t) {
    auto p = makeProducer(env, "b5", false, static_cast<int32_t>(kMinAsyncSendNum),
                          static_cast<int32_t>(kMinAsyncSendSize),
                          std::make_shared<SlowHook>(200));
    auto rec = std::make_shared<Recorder>();
    std::vector<std::thread> threads;
    for (int i = 0; i < 30; ++i) {
        // 每三笔里有一笔 300KB：关着背压时连字节闸都不看，开着的话这里必然限流
        const std::string body = (i % 3 == 0) ? std::string(300 * 1024, 'x') : "small";
        threads.emplace_back([p, t, rec, body]() { p->sendAsync(Message(t, body), rec, 8000); });
    }
    for (std::thread& th : threads) th.join();
    check("B5 关背压后 30 笔并发全部成功", waitUntil([&]() { return rec->done() >= 30; }, 25000),
          rec->summary());
    check("B5 全部 SEND_OK", rec->ok() == 30 && rec->errorCount() == 0, rec->summary());
    const int64_t landed = waitLanded(env, t, 30);
    check("B5 broker 上 30 条都在", landed == 30, "landed=" + num(landed));
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

    std::printf("############ C++ 异步发送背压真机验证（namesrv=%s stamp=%lld）\n",
                nsAddr.c_str(), static_cast<long long>(env.s));
    try {
        env.admin->start();
        b1DefaultCapacityNoLeak(env, t1);
        b2AndB3NumGate(env, t2);
        b4SizeGate(env, t3);
        b5GateOff(env, t4);
    } catch (const std::exception& e) {
        check("脚本整体执行", false, e.what());
    }

    const std::string addr = brokerAddr(env);
    for (const std::string& topic : {t1, t2, t3, t4}) {
        try {
            env.admin->deleteTopicInBroker(addr, topic);
        } catch (const std::exception&) {
            // 清理失败不影响结论
        }
    }
    env.admin->shutdown();

    std::printf("\n############ PASS=%d FAIL=%d ############\n", gPass, gFail);
    for (const std::string& name : gFailed) std::printf("  FAILED: %s\n", name.c_str());
    return gFail == 0 ? 0 : 1;
}
