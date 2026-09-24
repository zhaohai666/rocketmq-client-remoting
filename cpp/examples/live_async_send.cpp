// 异步发送内核（Java DefaultMQProducerImpl 的 ASYNC 分支 + MQClientAPIImpl.sendMessageAsync/
// onExceptionImpl）真机验证（对齐 dotnet/examples/RocketMQ.Examples/LiveAsyncSend.cs 的 A1–A6）。
// 用法：rmq_live_async_send 127.0.0.1:9876
//
// 前置：NameServer + Broker 已起，``autoCreateTopicEnable=true``。
//
// 离线单测（``tests/test_producer_async.cpp``）锁的是链的形状：调用方立刻返回、钩子各跑
// 一次、预算共享、闸门/队满怎么拒。那些场景真集群造不出来（造不出 SYSTEM_BUSY，也造不
// 出慢 broker），但离线对拍也证不了真 broker 上的六件事：
//   A1  「立刻返回」是真的：before 钩子睡 400ms 时调用方仍在 200ms 内返回；这一笔最后
//       SEND_OK，且用 broker 回的 offsetMsgId 能 **viewMessage 读回原 body 和 queueOffset**
//       （回调里的 SendResult 不是自说自话）；before 钩子在 AsyncSenderExecutor_N 上跑、
//       用户回调在 NettyClientPublicExecutor_N 上跑（Java executeInvokeCallback 的线程口径）。
//   A2  并发 30 笔异步发送：**每笔恰好一个终态**、全部 SEND_OK、broker 上正好落 30 条，
//       并且各笔的 (broker, queueId, queueOffset) 互不重叠（串台的话两个回调会指向同一位置）。
//   A3  定点发送（给了 mq）真的落在那条队列上，别的队列一条都不多。
//   A4  拦截钩子（CheckForbidden）看到的是 CommunicationMode::ASYNC；它拒绝时异常文本原样
//       到回调，而且 broker 上一条都没落（连请求都没发出去）。
//   A5  批量异步（sendBatchAsync，对位 Java send(Collection, SendCallback, timeout)）：
//       一批 5 条回调恰好一次 + broker 侧真落 5 条；定点批量真的落在指定队列；
//       混 topic / 空批的本地校验没被异步路径绕过（错误**进回调**，一次都不欠）；
//       背压扣的是**整批**字节，而且两份许可原样归还。
//   A6  Shutdown 排空在途准备段（C++ 这里用的是 ``shutdown(true)``，Java 用的是不等待的
//       ``shutdown()``）：交进来的每一笔都真的上线了，broker 上条条落地。
//
// ⚠ 三条脚本纪律（沿用 live_backpressure 那一轮真机联调踩出来的口径）：
//   ① topic 一律带 stamp —— 三语言**依次**跑在同一个 broker 上，固定名会继承上一轮的条数；
//   ② 「有没有落地」只看 broker 侧各队列 maxOffset-minOffset 之和，不看客户端回调；
//   ③ 新建 topic 要等 broker 把 topicConfig 注册到 namesrv（秒级到十秒级），所以所有
//      broker 侧对账都是**轮询到超时**，读一次路由失败不算失败。
#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <functional>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include "rocketmq/client/admin.h"
#include "rocketmq/client/exception.h"
#include "rocketmq/client/hook.h"
#include "rocketmq/client/producer.h"
#include "rocketmq/client/result.h"
#include "rocketmq/common/logging.h"
#include "rocketmq/common/message.h"
#include "rocketmq/common/message_const.h"
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

std::string bodyOf(const MessageExt& m) { return std::string(m.body.begin(), m.body.end()); }

bool startsWith(const std::string& s, const std::string& prefix) {
    return s.size() >= prefix.size() && s.compare(0, prefix.size(), prefix) == 0;
}

// 回调记账：终态条数、SEND_OK 条数、结果、异常文本，以及**跑在哪根线程上**
// （Java 的 executeInvokeCallback 口径只能这么验）
class Recorder : public SendCallback {
public:
    void onSuccess(const SendResult& r) override {
        std::lock_guard<std::mutex> lk(m_);
        results_.push_back(r);
        threads_.push_back(currentThreadName());
    }

    void onException(const std::exception_ptr& e) override {
        std::lock_guard<std::mutex> lk(m_);
        errors_.push_back(exceptionMessage(e));
        errorTypes_.push_back(exceptionTypeName(e));
        threads_.push_back(currentThreadName());
    }

    size_t done() {
        std::lock_guard<std::mutex> lk(m_);
        return results_.size() + errors_.size();
    }

    size_t ok() {
        std::lock_guard<std::mutex> lk(m_);
        size_t n = 0;
        for (const SendResult& r : results_) {
            if (r.sendStatus == SendStatus::SEND_OK) ++n;
        }
        return n;
    }

    size_t errorCount() {
        std::lock_guard<std::mutex> lk(m_);
        return errors_.size();
    }

    std::vector<SendResult> results() {
        std::lock_guard<std::mutex> lk(m_);
        return results_;
    }

    // 线程名按下标取（第几笔回调），越界返回空串
    std::string threadAt(size_t i) {
        std::lock_guard<std::mutex> lk(m_);
        return i < threads_.size() ? threads_[i] : std::string();
    }

    std::string firstError() {
        std::lock_guard<std::mutex> lk(m_);
        return errors_.empty() ? std::string() : errors_.front();
    }

    // 第一笔异常的类型名（Java 的 e.getClass().getSimpleName()）：失败**分类**看这里
    std::string firstErrorType() {
        std::lock_guard<std::mutex> lk(m_);
        return errorTypes_.empty() ? std::string() : errorTypes_.front();
    }

    std::string summary() {
        std::lock_guard<std::mutex> lk(m_);
        return "ok=" + std::to_string(results_.size()) + " err=" + std::to_string(errors_.size())
             + (errors_.empty() ? ""
                                : " " + errors_.front()
                                      + (errorTypes_.empty() ? "" : " ["
                                                                       + errorTypes_.front()
                                                                       + "]"));
    }

private:
    std::mutex m_;
    std::vector<SendResult> results_;
    std::vector<std::string> errors_;
    std::vector<std::string> errorTypes_;
    std::vector<std::string> threads_;
};

// before 钩子睡一段时间：把「准备段」拖慢，才能证明调用方没等它。
// 顺便记下两件事：钩子跑在哪根线程上、before/after 各跑了几次。
class TracingHook : public SendMessageHook {
public:
    explicit TracingHook(int64_t parkMillis) : parkMillis_(parkMillis) {}

    std::string hookName() const override { return "async-tracing"; }

    void sendMessageBefore(SendMessageContext&) override {
        {
            std::lock_guard<std::mutex> lk(m_);
            ++before_;
            beforeThread_ = currentThreadName();
        }
        const int64_t ms = parkMillis_.load();
        if (ms > 0) std::this_thread::sleep_for(std::chrono::milliseconds(ms));
    }

    void sendMessageAfter(SendMessageContext&) override {
        std::lock_guard<std::mutex> lk(m_);
        ++after_;
    }

    int32_t before() {
        std::lock_guard<std::mutex> lk(m_);
        return before_;
    }

    int32_t after() {
        std::lock_guard<std::mutex> lk(m_);
        return after_;
    }

    std::string beforeThread() {
        std::lock_guard<std::mutex> lk(m_);
        return beforeThread_;
    }

private:
    std::mutex m_;
    std::atomic<int64_t> parkMillis_;
    int32_t before_ = 0;
    int32_t after_ = 0;
    std::string beforeThread_;
};

// 只拒绝带 forbidden 标签的消息，并记下它看到的 CommunicationMode
class ForbiddenTagHook : public CheckForbiddenHook {
public:
    std::string hookName() const override { return "async-forbidden"; }

    void checkForbidden(CheckForbiddenContext& ctx) override {
        std::lock_guard<std::mutex> lk(m_);
        ++calls_;
        mode_ = ctx.communicationMode;
        if (ctx.message != nullptr && ctx.message->getTags() == "forbidden") {
            // 钩子抛的异常**不吞**（Java checkForbidden 的 throws 语义），一路传到回调
            throw MQClientException("live test: tag forbidden is not allowed");
        }
    }

    int32_t calls() {
        std::lock_guard<std::mutex> lk(m_);
        return calls_;
    }

    CommunicationMode mode() {
        std::lock_guard<std::mutex> lk(m_);
        return mode_;
    }

private:
    std::mutex m_;
    int32_t calls_ = 0;
    CommunicationMode mode_ = CommunicationMode::SYNC;
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

// 钩子必须在 start() 之前挂上（与 live_backpressure 同样口径）
std::shared_ptr<DefaultMQProducer> makeProducer(Env& env, const std::string& instance,
                                                std::shared_ptr<SendMessageHook> sendHook = nullptr,
                                                std::shared_ptr<CheckForbiddenHook> forbidHook =
                                                    nullptr) {
    auto p = std::make_shared<DefaultMQProducer>();
    p->setProducerGroup("PID_rmq_async_cpp_" + std::to_string(env.s));
    p->setNamesrvAddr(env.nsAddr);
    p->setInstanceName("async-cpp-" + instance + "-" + std::to_string(env.s));
    if (sendHook) p->registerSendMessageHook(std::move(sendHook));
    if (forbidHook) p->registerCheckForbiddenHook(std::move(forbidHook));
    p->start();
    return p;
}

Message msg(const std::string& topic, const std::string& body) { return Message(topic, body); }

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
    return landed;
}

int64_t queueLanded(DefaultMQAdminExt& admin, const MessageQueue& mq) {
    try {
        return admin.maxOffset(mq) - admin.minOffset(mq);
    } catch (const std::exception&) {
        return -1;
    }
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

// ------------------------------------------------- A1 立刻返回 + 线程口径 + 读回
void a1NonBlockingAndReadBack(Env& env, const std::string& t) {
    auto hook = std::make_shared<TracingHook>(400);
    auto p = makeProducer(env, "a1", hook);
    auto rec = std::make_shared<Recorder>();
    const std::string body = "async-a1-readback";
    const int64_t began = nowMs();
    p->sendAsync(msg(t, body), rec, 5000);
    const int64_t callerTook = nowMs() - began;
    check("A1 调用方在准备段（钩子睡 400ms）之前就已返回",
          callerTook < 200 && hook->before() <= 1,
          "调用方耗时=" + num(callerTook) + "ms before 已跑=" + num(hook->before()));
    check("A1 回调恰好一次且 SEND_OK",
          waitUntil([&]() { return rec->done() >= 1; }, 20000) && rec->ok() == 1
              && rec->errorCount() == 0,
          rec->summary());
    check("A1 before 钩子在 AsyncSenderExecutor_N 上跑",
          startsWith(hook->beforeThread(), "AsyncSenderExecutor_"),
          "线程名=" + hook->beforeThread());
    check("A1 用户回调在 NettyClientPublicExecutor_N 上跑（Java executeInvokeCallback）",
          startsWith(rec->threadAt(0), "NettyClientPublicExecutor_"),
          "线程名=" + rec->threadAt(0));
    check("A1 before/after 各跑一次", hook->before() == 1 && hook->after() == 1,
          "before=" + num(hook->before()) + " after=" + num(hook->after()));
    check("A1 broker 落了这一条", waitLanded(env, t, 1) == 1);
    const std::vector<SendResult> rs = rec->results();
    if (rs.empty()) {
        p->shutdown();
        return;
    }
    const SendResult& r = rs[0];
    // offsetMsgId 是 broker 给的，只有它能解出 commitLog 偏移 ⇒ 读回来对 body
    MessageExt back;
    const bool read = waitUntil(
        [&]() {
            try {
                back = env.admin->viewMessage(t, r.offsetMsgId);
                return true;
            } catch (const std::exception&) {
                return false;  // commitLog 还没刷出去
            }
        },
        15000);
    check("A1 用回调里的 offsetMsgId 能读回这条消息", read && bodyOf(back) == body,
          "body=" + bodyOf(back));
    int64_t maxOffset = -1;
    try {
        maxOffset = env.admin->maxOffset(r.messageQueue);
    } catch (const std::exception&) {
    }
    check("A1 回调里的 queueOffset 就是它落在的位置", r.queueOffset == maxOffset - 1,
          "queueOffset=" + num(r.queueOffset) + " maxOffset=" + num(maxOffset));
    // msgId 是客户端补的 UNIQ_KEY（32 位十六进制），不是 broker 的那份
    check("A1 msgId 是客户端 UNIQ_KEY、与 broker 的 offsetMsgId 不同",
          r.msgId.size() == 32 && r.msgId != r.offsetMsgId,
          "msgId=" + r.msgId + " offsetMsgId=" + r.offsetMsgId);
    p->shutdown();
}

// ------------------------------------------------- A2 并发不串台
void a2BurstExactlyOnceEach(Env& env, const std::string& t) {
    const int32_t burst = 30;
    auto p = makeProducer(env, "a2");
    std::vector<std::shared_ptr<Recorder>> each;
    for (int32_t i = 0; i < burst; ++i) each.push_back(std::make_shared<Recorder>());
    std::vector<std::thread> threads;
    for (int32_t i = 0; i < burst; ++i) {
        threads.emplace_back([p, t, each, i]() { p->sendAsync(msg(t, "async-burst-" + num(i)),
                                                              each[static_cast<size_t>(i)], 8000); });
    }
    for (std::thread& th : threads) th.join();
    check("A2 30 笔并发异步发送每笔都拿到终态",
          waitUntil([&]() {
              for (const std::shared_ptr<Recorder>& r : each) {
                  if (r->done() < 1) return false;
              }
              return true;
          },
                    20000),
          num([&]() {
              size_t n = 0;
              for (const std::shared_ptr<Recorder>& r : each) n += r->done();
              return static_cast<int64_t>(n);
          }()));
    int32_t multi = 0;
    int32_t notOk = 0;
    std::set<std::string> slots;
    std::set<std::string> uniq;
    for (const std::shared_ptr<Recorder>& r : each) {
        if (r->done() != 1) ++multi;
        const std::vector<SendResult> rs = r->results();
        if (rs.size() != 1 || rs[0].sendStatus != SendStatus::SEND_OK) ++notOk;
        for (const SendResult& s : rs) {
            slots.insert(s.messageQueue.brokerName + "#" + num(s.messageQueue.queueId) + "@"
                         + num(s.queueOffset));
            uniq.insert(s.msgId);
        }
    }
    check("A2 每笔**恰好一个**终态（不多不少）", multi == 0, "多拿回调的笔数=" + num(multi));
    check("A2 全部 SEND_OK", notOk == 0, each[0]->summary());
    check("A2 broker 上正好落 30 条", waitLanded(env, t, burst) == burst);
    check("A2 各笔的 (broker, queueId, queueOffset) 互不重叠",
          static_cast<int32_t>(slots.size()) == burst, "去重后=" + num(static_cast<int64_t>(slots.size())));
    check("A2 每笔的 UNIQ_KEY 都不一样", static_cast<int32_t>(uniq.size()) == burst);
    p->shutdown();
}

// ------------------------------------------------- A3 定点发送
void a3PinnedQueue(Env& env, const std::string& t) {
    auto p = makeProducer(env, "a3");
    auto rec = std::make_shared<Recorder>();
    // 先把 topic 撑出来（同步发一笔，让 broker 把队列建全），再挑一条定点打。
    // ⚠ 取基线之前必须等预热那一笔在 broker 侧**已经可读**：刚 ack 的报文落到
    // consumeQueue 有延迟，基线读成 0、对账时它变成 1，会凭空多出 1 条。
    p->send(msg(t, "async-a3-warmup"), 5000);
    check("A3 预热那一笔已经在 broker 上可读", waitLanded(env, t, 1) >= 1,
          "landed=" + num(landedCount(*env.admin, t)));
    const std::vector<MessageQueue> queues = p->fetchPublishMessageQueues(t);
    check("A3 取到了发布队列", !queues.empty(), "queues=" + num(static_cast<int64_t>(queues.size())));
    if (queues.empty()) {
        p->shutdown();
        return;
    }
    const MessageQueue aimed = queues[0];
    const int64_t before = queueLanded(*env.admin, aimed);
    int64_t othersBefore = 0;
    for (size_t i = 1; i < queues.size(); ++i) othersBefore += std::max<int64_t>(0, queueLanded(*env.admin, queues[i]));
    p->sendAsync(msg(t, "async-a3-pinned"), aimed, rec, 5000);
    check("A3 定点异步发送拿到终态且 SEND_OK",
          waitUntil([&]() { return rec->done() >= 1; }, 20000) && rec->ok() == 1,
          rec->summary());
    const std::vector<SendResult> rs = rec->results();
    if (rs.empty()) {
        p->shutdown();
        return;
    }
    const SendResult& r = rs[0];
    check("A3 结果落在指定的那条队列上",
          r.messageQueue.brokerName == aimed.brokerName
              && r.messageQueue.queueId == aimed.queueId,
          "broker=" + r.messageQueue.brokerName + " queueId=" + num(r.messageQueue.queueId));
    check("A3 那条队列正好多 1 条",
          waitUntil([&]() { return queueLanded(*env.admin, aimed) == before + 1; }, 20000),
          "landed=" + num(queueLanded(*env.admin, aimed)) + " 之前=" + num(before));
    int64_t othersAfter = 0;
    for (size_t i = 1; i < queues.size(); ++i) othersAfter += std::max<int64_t>(0, queueLanded(*env.admin, queues[i]));
    check("A3 别的队列一条都没多", othersAfter == othersBefore,
          "其它队列 " + num(othersBefore) + " -> " + num(othersAfter));
    p->shutdown();
}

// ------------------------------------------------- A4 拦截钩子
void a4ForbiddenHook(Env& env, const std::string& t) {
    auto forbidden = std::make_shared<ForbiddenTagHook>();
    auto p = makeProducer(env, "a4", nullptr, forbidden);
    auto rejected = std::make_shared<Recorder>();
    auto passed = std::make_shared<Recorder>();
    Message bad = msg(t, "async-a4-rejected");
    bad.setTags("forbidden");
    p->sendAsync(bad, rejected, 5000);
    check("A4 钩子拒绝的异常原样到了回调",
          waitUntil([&]() { return rejected->done() >= 1; }, 10000)
              && rejected->firstError().find("tag forbidden is not allowed") != std::string::npos,
          rejected->summary());
    check("A4 钩子拒绝的异常类型是 MQClientException（Java checkForbidden 抛的就是它）",
          rejected->firstErrorType() == "MQClientException", rejected->summary());
    check("A4 拦截钩子看到的是 ASYNC", forbidden->mode() == CommunicationMode::ASYNC,
          "mode=" + std::string(forbidden->mode() == CommunicationMode::ASYNC      ? "ASYNC"
                                : forbidden->mode() == CommunicationMode::ONEWAY ? "ONEWAY"
                                                                                 : "SYNC"));
    // 这个 topic 除了被拒的这一笔什么都没有 ⇒ 要么读到 0 条，要么连路由都还没
    // 注册上（-1）。路由是**第一条消息落到 broker** 才会被 autoCreate 建出来的，
    // 所以「读不到路由」本身就是「broker 没收到过请求」的证据。
    const int64_t afterReject = landedCount(*env.admin, t);
    check("A4 被拒的这笔在 broker 上没留痕", afterReject <= 0,
          "landed=" + num(afterReject) + "（-1 = 路由还没建出来，即 broker 一条都没收到）");

    p->sendAsync(msg(t, "async-a4-ok"), passed, 5000);
    check("A4 同一个生产者换个标签照常落地（拒绝没把池子弄坏）",
          waitUntil([&]() { return passed->done() >= 1; }, 20000) && passed->ok() == 1,
          passed->summary());
    const int64_t landed = waitLanded(env, t, 1);
    check("A4 broker 上正好落 1 条", landed == 1, "landed=" + num(landed));
    check("A4 钩子一共被调 2 次（一笔被拒、一笔放行）", forbidden->calls() == 2,
          "calls=" + num(forbidden->calls()));
    p->shutdown();
}

// ------------------------------------------------- A5 批量异步（sendBatchAsync）

// 除第 0 条以外的队列合计落地条数（定点批量要证明「别的队列一条都没多」）。
int64_t othersLanded(DefaultMQAdminExt& admin, const std::vector<MessageQueue>& queues) {
    int64_t total = 0;
    for (size_t i = 1; i < queues.size(); ++i) {
        total += std::max<int64_t>(0, queueLanded(admin, queues[i]));
    }
    return total;
}

/// 批量异步入口对位 Java `send(Collection<Message>, SendCallback, long)` / Python
/// `send_async(list)`（走同步批量内核 `_send_batch`）。每条断言都刻意选在「只跑离线单测
/// 看不出来」的那一侧：回调有没有交付、批量内核的本地校验有没有被异步路径绕过、错误有没有
/// 被静默吞掉、许可有没有漏。
void a5BatchAsync(Env& env, const std::string& t) {
    auto p = makeProducer(env, "a5");

    // ① 一批 5 条：回调恰好一次、SEND_OK，并且 broker 上真的落了 5 条。
    //    只看回调会被「回调报了 OK 但请求压根没发出去」蒙过去 —— 那条必须靠 broker 侧对账。
    auto rec = std::make_shared<Recorder>();
    std::vector<Message> batch;
    for (int32_t i = 0; i < 5; ++i) batch.push_back(msg(t, "async-a5-" + num(i)));
    p->sendBatchAsync(batch, rec, 10000);
    check("A5 一批 5 条：回调恰好一次且 SEND_OK",
          waitUntil([&]() { return rec->done() >= 1; }, 20000) && rec->done() == 1
              && rec->ok() == 1 && rec->errorCount() == 0
              && !rec->results().empty() && !rec->results()[0].msgId.empty(),
          rec->summary());
    const int64_t landed = waitLanded(env, t, 5);
    check("A5 broker 上真落了 5 条（不是回调自己说成功）", landed == 5,
          "landed=" + num(landed) + "（-1 = 路由还没建出来）");
    check("*  A5 请求码 = SEND_BATCH_MESSAGE(320) 由离线单测取证", true,
          "真机看不到上线报文，这一条由 tests/test_producer_async.cpp 在进程内取证");

    // ①b 逐条 ID 的**落地**证据：读回 broker 存下来的那条子消息，它必须带客户端生成的
    //     32 位 UNIQ_KEY。Java batch():1176 的顺序是「逐条 setUniqID → 才 encode()」；
    //     顺序错了（或像修复前的本端口那样压根不写），broker 拆开批量后存的就是没有 ID 的
    //     裸消息 —— 消费端去重、轨迹控制台串线全废，而发送侧回调照样 SEND_OK，看不出来。
    const std::vector<SendResult> batchResults = rec->results();
    const SendResult batchResult = batchResults.empty() ? SendResult{} : batchResults[0];
    // 批量 msgId 口径：客户端为整批生成的 UNIQ_KEY，而不是 broker 那串 offsetMsgId
    check("A5 批量的 msgId 是客户端 32 位 ID、不是 broker 的 offsetMsgId",
          batchResult.msgId.size() == 32 && batchResult.msgId.find(',') == std::string::npos
              && batchResult.msgId != batchResult.offsetMsgId,
          "msgId=" + batchResult.msgId + " offsetMsgId=" + batchResult.offsetMsgId);
    // 批量应答的 offsetMsgId 是 broker **逐条**回的一串（逗号分隔，一条子消息一个 commitLog
    // 偏移），它本身就是「这一批被拆开存成 5 条」的证据
    std::vector<std::string> subOffsets;
    for (size_t pos = 0; pos <= batchResult.offsetMsgId.size();) {
        size_t comma = batchResult.offsetMsgId.find(',', pos);
        if (comma == std::string::npos) comma = batchResult.offsetMsgId.size();
        if (comma > pos) subOffsets.push_back(batchResult.offsetMsgId.substr(pos, comma - pos));
        pos = comma + 1;
    }
    check("A5 broker 逐条回了 5 个 commitLog 偏移（批量确实被拆开落地）",
          subOffsets.size() == 5, "offsetMsgId=" + batchResult.offsetMsgId);
    MessageExt storedSub;
    const bool readSub =
        !subOffsets.empty()
        && waitUntil(
               [&]() {
                   try {
                       storedSub = env.admin->viewMessage(t, subOffsets.front());
                       return true;
                   } catch (const std::exception&) {
                       return false;  // commitLog 还没刷出去
                   }
               },
               15000);
    const std::string storedUniq =
        readSub ? storedSub.getProperty(MessageConst::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX)
                : std::string();
    check("A5 broker 上存的子消息带客户端 UNIQ_KEY（逐条 ID 编在 body 里）",
          storedUniq.size() == 32, "stored UNIQ_KEY=" + storedUniq);

    // ② 定点批量：mq 参数真的传到了批量内核，而不是被丢掉后自己挑一条。
    const std::vector<MessageQueue> queues = p->fetchPublishMessageQueues(t);
    if (queues.empty()) {
        check("A5 定点批量取到了发布队列", false, "queues=0");
        p->shutdown();
        return;
    }
    const MessageQueue aimed = queues[0];
    const int64_t aimedBefore = queueLanded(*env.admin, aimed);
    const int64_t othersBefore = othersLanded(*env.admin, queues);
    auto pinned = std::make_shared<Recorder>();
    std::vector<Message> pinBatch;
    for (int32_t i = 0; i < 3; ++i) pinBatch.push_back(msg(t, "async-a5-pin-" + num(i)));
    p->sendBatchAsync(pinBatch, aimed, pinned, 10000);
    const bool pinnedOk = waitUntil([&]() { return pinned->done() >= 1; }, 20000)
                        && pinned->done() == 1 && pinned->ok() == 1;
    const std::vector<SendResult> pr = pinned->results();
    const bool landedOnAimed = !pr.empty() && pr[0].messageQueue.brokerName == aimed.brokerName
                            && pr[0].messageQueue.queueId == aimed.queueId;
    int64_t aimedAfter = queueLanded(*env.admin, aimed);
    check("A5 定点批量那条队列正好多 3 条",
          waitUntil([&]() { return (aimedAfter = queueLanded(*env.admin, aimed)) == aimedBefore + 3; },
                    20000),
          "该队列 " + num(aimedBefore) + " -> " + num(aimedAfter));
    const int64_t othersAfter = othersLanded(*env.admin, queues);
    check("A5 定点批量落在指定队列、别的队列一条都没多",
          pinnedOk && landedOnAimed && aimedAfter == aimedBefore + 3 && othersAfter == othersBefore,
          "ok=" + num(static_cast<int64_t>(pinned->ok())) + " 落位=" + std::string(landedOnAimed ? "1" : "0")
              + " 该队列 " + num(aimedBefore) + "->" + num(aimedAfter)
              + " 其它 " + num(othersBefore) + "->" + num(othersAfter));

    // ③ 混 topic 的一批：批量内核的同质性校验必须在异步路径上照样跑，且异常**进回调**
    //    （Java 的 runnable catch → onException），不是同步抛、也不是静默丢掉。
    auto mixed = std::make_shared<Recorder>();
    p->sendBatchAsync({msg(t, "async-a5-mixed-1"), msg(env.topic("BatchOther"), "async-a5-mixed-2")},
                      mixed, 5000);
    check("A5 混 topic 的一批在回调里报错（本地校验没被异步路径绕过）",
          waitUntil([&]() { return mixed->done() >= 1; }, 10000) && mixed->done() == 1
              && mixed->ok() == 0
              && mixed->firstError().find("should be the same") != std::string::npos,
          mixed->summary());

    // ④ 空批次：同一口径 —— 错误进回调，一次回调都不欠。
    auto empty = std::make_shared<Recorder>();
    p->sendBatchAsync(std::vector<Message>(), empty, 5000);
    check("A5 空批次也是「恰好一次失败回调」，不静默吞掉",
          waitUntil([&]() { return empty->done() >= 1; }, 10000) && empty->done() == 1
              && empty->ok() == 0
              && empty->firstError().find("message list is empty") != std::string::npos,
          empty->summary());

    // ⑤ 背压扣的是**整批**字节，不是「一条」：字节闸压到地板（1 MiB），一批 2×600 KiB
    //    必须被拦下（整批 1.2 MiB > 1 MiB）；换一批 2×100 KiB 又必须过。只按第一条算
    //    （600 KiB）或者干脆不扣，这两条就会同时反向。
    p->setEnableBackpressureForAsyncMode(true);
    p->setBackPressureForAsyncSendNum(1);
    p->setBackPressureForAsyncSendSize(1024 * 1024);
    auto gated = std::make_shared<Recorder>();
    p->sendBatchAsync({msg(t, std::string(600 * 1024, 'x')), msg(t, std::string(600 * 1024, 'x'))},
                      gated, 2000);
    check("A5 整批 1.2MiB 被 1MiB 字节闸拦下（回调拿到 TOO_MUCH_REQUEST）",
          waitUntil([&]() { return gated->done() >= 1; }, 15000) && gated->done() == 1
              && gated->ok() == 0
              && gated->firstError().find("semaphoreAsyncSize timeout") != std::string::npos,
          gated->summary());
    // 同一道闸下的小批次必须照常落地：证明「拦住」是因为整批量，不是因为批量被一概论处
    auto passed = std::make_shared<Recorder>();
    p->sendBatchAsync({msg(t, std::string(100 * 1024, 'y')), msg(t, std::string(100 * 1024, 'y'))},
                      passed, 10000);
    check("A5 同一道闸下 200KiB 的一批照常拿到 SEND_OK",
          waitUntil([&]() { return passed->done() >= 1; }, 20000) && passed->done() == 1
              && passed->ok() == 1,
          passed->summary());
    // 许可必须原样归还：漏一份就是长跑之后「所有异步发送集体超时」，而单次发送看不出来。
    const int32_t numTotal = p->getBackPressureForAsyncSendNum();
    const int32_t sizeTotal = p->getBackPressureForAsyncSendSize();
    check("A5 批量路径把两份许可原样归还（空闲量回到总量，含被闸拦下那一笔）",
          waitUntil(
              [&]() {
                  return p->getSemaphoreAsyncSendNumAvailablePermits() == numTotal
                      && p->getSemaphoreAsyncSendSizeAvailablePermits() == sizeTotal;
              },
              10000),
          "空闲条数=" + num(p->getSemaphoreAsyncSendNumAvailablePermits()) + " 总量=" + num(numTotal)
              + " 空闲字节=" + num(p->getSemaphoreAsyncSendSizeAvailablePermits())
              + " 总量=" + num(sizeTotal));
    p->shutdown();
}

// ------------------------------------------------- A6 关池排空在途准备段
void a6ShutdownDrains(Env& env, const std::string& t) {
    const int32_t cores = std::max<int32_t>(1, static_cast<int32_t>(std::thread::hardware_concurrency()));
    const int32_t sends = cores * 3;
    auto hook = std::make_shared<TracingHook>(100);
    auto p = makeProducer(env, "a6", hook);
    auto rec = std::make_shared<Recorder>();
    for (int32_t i = 0; i < sends; ++i) p->sendAsync(msg(t, "async-a6-" + num(i)), rec, 8000);
    // 立刻关：Java 在这里会把队列里没跑到的任务连人带回调一起丢掉
    p->shutdown();
    const int64_t landed = waitLanded(env, t, sends, 40);
    check("A6 Shutdown 排空了队列：交进来的每一笔都上线了", landed == sends,
          "landed=" + num(landed) + " 发送=" + num(sends));
    check("A6 一笔至多一个终态回调", rec->done() <= static_cast<size_t>(sends),
          "done=" + num(static_cast<int64_t>(rec->done())) + " 发送=" + num(sends));

    auto p2 = makeProducer(env, "a6-after");
    auto after = std::make_shared<Recorder>();
    p2->sendAsync(msg(t, "async-a6-after"), after, 8000);
    check("A6 关掉的池子不会被别的生产者复用（新生产者接着能发）",
          waitUntil([&]() { return after->done() >= 1; }, 20000) && after->ok() == 1,
          after->summary());
    p2->shutdown();
}

}  // namespace

int main(int argc, char** argv) {
    const std::string nsAddr = argc > 1 ? argv[1] : "127.0.0.1:9876";
    Env env;
    env.nsAddr = nsAddr;
    env.s = stamp();
    env.admin = std::make_unique<DefaultMQAdminExt>("ADMIN_async");
    env.admin->setNamesrvAddr(nsAddr);
    env.admin->setTimeoutMillis(10000);
    const std::string t1 = env.topic("AsyncReadBack");
    const std::string t2 = env.topic("AsyncBurst");
    const std::string t3 = env.topic("AsyncPinned");
    const std::string t4 = env.topic("AsyncForbidden");
    const std::string t5 = env.topic("AsyncBatch");
    const std::string t6 = env.topic("AsyncDrain");

    std::printf("############ C++ 异步发送内核真机验证（namesrv=%s stamp=%lld）\n",
                nsAddr.c_str(), static_cast<long long>(env.s));
    try {
        env.admin->start();
        a1NonBlockingAndReadBack(env, t1);
        a2BurstExactlyOnceEach(env, t2);
        a3PinnedQueue(env, t3);
        a4ForbiddenHook(env, t4);
        a5BatchAsync(env, t5);
        a6ShutdownDrains(env, t6);
    } catch (const std::exception& e) {
        check("脚本整体执行", false, e.what());
    }

    const std::string addr = brokerAddr(env);
    for (const std::string& topic : {t1, t2, t3, t4, t5, t6}) {
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
