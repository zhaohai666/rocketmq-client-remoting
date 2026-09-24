// 发送头三个字段（defaultTopic / defaultTopicQueueNums / brokerName）真机验证。
// 用法：rmq_live_send_header 127.0.0.1:9876
//
// 与 Python 的 verify_send_header_live.py、Rust 的 live_send_header.rs、.NET 的
// LiveSendHeader.cs 对齐（H0–H5 一一对应）。
//
// 前置：NameServer + Broker 已起，``autoCreateTopicEnable=true``。
//
// 离线单测（tests/test_send_retry.cpp）锁的是上线形状：V2 头的单字母键 c/d/n 有没有真
// 写进 extFields、五种发送入口是不是带同一份值。但「字段上了线」和「broker 真拿它做了
// 决定」是两件事，后者只有真集群能证：
//   H1  默认值：不带任何配置发到新 topic，broker 按 min(d=4, TBW102.writeQueueNums) 建
//       队列（TopicConfigManager.java:289），与 Java 客户端默认行为一致。
//   H2  setDefaultTopicQueueNums(2) 真的生效：建出来的 topic 只有 2 条队列。修之前这里
//       写死 4，这条必然变 4 —— 那是那个假 setter 唯一可观测的后果。
//   H3  setCreateTopicKey(src) 真的生效：先建一个带 PERM_INHERIT、3 条队列的模板 topic，
//       再以它为 c 发送 → 新 topic 继承模板的 3 条队列，而不是 TBW102 的 8 条。
//   H4  补上这三个字段之后，五种入口（同步 / 定点 / 单向 / 批量 320 / 异步）在真 broker
//       上仍逐条落地，条数一条不差。
//   H5  n（brokerName）：落点就是路由选中的那台 broker 名。⚠ 经典 broker 的发送链路里
//       没有 requestHeader.getBrokerName() 的读者（5.5.1 源码 grep 过），所以 n 在线上
//       的存在只能由离线抓帧证明，这里不假装能观测到它。
#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdio>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "rocketmq/client/admin.h"
#include "rocketmq/client/exception.h"
#include "rocketmq/client/producer.h"
#include "rocketmq/client/result.h"
#include "rocketmq/common/message.h"
#include "rocketmq/common/mix_all.h"
#include "rocketmq/common/sysflag.h"
#include "rocketmq/remoting/protocol/route.h"

using namespace rocketmq;

namespace {

int32_t gPass = 0;
int32_t gFail = 0;

void check(const std::string& name, bool ok, const std::string& detail = std::string()) {
    if (ok) {
        ++gPass;
    } else {
        ++gFail;
    }
    std::printf("  [%s] %s%s\n", ok ? "PASS" : "FAIL", name.c_str(),
                detail.empty() ? "" : ("  " + detail).c_str());
}

std::string num(int32_t v) { return std::to_string(v); }

std::string num64(int64_t v) { return std::to_string(v); }

int64_t nowMs() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::steady_clock::now().time_since_epoch())
        .count();
}

bool waitUntil(const std::function<bool()>& pred, int32_t timeoutMs) {
    const int64_t deadline = nowMs() + timeoutMs;
    while (nowMs() < deadline) {
        if (pred()) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    return pred();
}

Bytes bytesOf(const std::string& s) { return Bytes(s.begin(), s.end()); }

// 一次异步发送的终态
class Latch : public SendCallback {
public:
    void onSuccess(const SendResult& r) override {
        std::lock_guard<std::mutex> lk(m_);
        result_ = r;
        ++oks_;
        cv_.notify_all();
    }
    void onException(const std::exception_ptr& e) override {
        std::lock_guard<std::mutex> lk(m_);
        errors_.push_back(exceptionMessage(e) + " [" + exceptionTypeName(e) + "]");
        ++fails_;
        cv_.notify_all();
    }
    bool wait(int32_t ms) {
        std::unique_lock<std::mutex> lk(m_);
        return cv_.wait_for(lk, std::chrono::milliseconds(ms),
                            [this] { return oks_ + fails_ > 0; });
    }
    size_t oks() {
        std::lock_guard<std::mutex> lk(m_);
        return oks_;
    }
    SendResult result() {
        std::lock_guard<std::mutex> lk(m_);
        return result_;
    }
    std::vector<std::string> errors() {
        std::lock_guard<std::mutex> lk(m_);
        return errors_;
    }

private:
    std::mutex m_;
    std::condition_variable cv_;
    size_t oks_ = 0;
    size_t fails_ = 0;
    SendResult result_;
    std::vector<std::string> errors_;
};

// 环境：一个名字服务地址 + 一组按 stamp 隔离的 topic，跑完自己删干净。
struct Env {
    std::string nsAddr;
    std::string stamp;
    std::vector<std::string> topics;
    DefaultMQAdminExt admin{"HDRADMIN"};
    std::string brokerAddr;
    std::string brokerName;

    std::string topic(const std::string& kind) {
        std::string t = "Hdr" + kind + "_" + stamp;
        topics.push_back(t);
        return t;
    }

    void start() {
        admin.setNamesrvAddr(nsAddr);
        admin.setTimeoutMillis(10000);
        admin.start();
    }

    void cleanup() {
        for (const std::string& t : topics) {
            try {
                admin.deleteTopic(t);
            } catch (const std::exception& e) {
                std::printf("  [diag] deleteTopic(%s) failed: %s\n", t.c_str(), e.what());
            }
        }
        admin.shutdown();
    }
};

// 读回 broker 上该 topic 的 (read, write) 队列数；还不存在时返回 false。
bool queueNums(Env& env, const std::string& topic, int32_t& read, int32_t& write) {
    try {
        const TopicConfig cfg = env.admin.examineTopicConfig(env.brokerAddr, topic);
        read = cfg.readQueueNums;
        write = cfg.writeQueueNums;
        return true;
    } catch (const std::exception&) {
        return false;
    }
}

// 等 topic 在 broker 上出现（自动建 topic 是发送链路里同步做的，配置落地要一点点时间）
bool waitForTopic(Env& env, const std::string& topic, int32_t& read, int32_t& write) {
    return waitUntil([&] { return queueNums(env, topic, read, write); }, 10000);
}

// 该 topic 全 broker 的落库条数（新 topic 的 minOffset 恒为 0）；读不到返回 -1
int64_t totalMessages(Env& env, const std::string& topic) {
    try {
        const TopicStatsTable stats = env.admin.examineTopicStats(topic);
        int64_t total = 0;
        for (const auto& kv : stats.offsetTable) {
            total += kv.second.maxOffset - kv.second.minOffset;
        }
        return total;
    } catch (const std::exception&) {
        return -1;
    }
}

// 按场景配好并启动一个生产者
class ScopedProducer {
public:
    ScopedProducer(Env& env, const std::string& kind, const std::string& createTopicKey = "",
                   int32_t defaultTopicQueueNums = -1)
        : producer_("PID_send_header_" + env.stamp) {
        producer_.setNamesrvAddr(env.nsAddr);
        producer_.setInstanceName("hdr-" + kind + "-" + env.stamp);
        producer_.setSendMsgTimeout(5000);
        if (!createTopicKey.empty()) producer_.setCreateTopicKey(createTopicKey);
        if (defaultTopicQueueNums >= 0) {
            producer_.setDefaultTopicQueueNums(defaultTopicQueueNums);
        }
        producer_.start();
    }
    ~ScopedProducer() {
        try {
            producer_.shutdown();
        } catch (const std::exception& e) {
            std::printf("  [diag] producer shutdown failed: %s\n", e.what());
        }
    }

    DefaultMQProducer& operator*() { return producer_; }
    SendResult send(const std::string& topic, const std::string& body) {
        return producer_.send(Message(topic, bytesOf(body)), 5000);
    }

private:
    DefaultMQProducer producer_;
};

// ---------------- H0/H1/H2/H3：队列数由 c 和 d 决定 ----------------
void checkQueueNums(Env& env) {
    int32_t tbwRead = 0;
    int32_t tbwWrite = 0;
    if (!queueNums(env, MixAll::DEFAULT_TOPIC, tbwRead, tbwWrite)) {
        check("H0 TBW102 模板可读", false, "examineTopicConfig(TBW102) 读不到");
        return;
    }
    check("H0 TBW102 模板可读", tbwWrite > 0,
          "read=" + num(tbwRead) + " write=" + num(tbwWrite));

    // H1：什么都不设，d 走 MixAll::DEFAULT_TOPIC_QUEUE_NUMS(4)
    const std::string t1 = env.topic("Def");
    {
        ScopedProducer p(env, "h1");
        const SendResult r = p.send(t1, "h1");
        check("H1 默认配置发送成功", r.sendStatus == SendStatus::SEND_OK,
              "msgId=" + r.msgId + " broker=" + r.messageQueue.brokerName);
    }
    int32_t r1 = 0;
    int32_t w1 = 0;
    waitForTopic(env, t1, r1, w1);
    const int32_t expected1 = std::min(MixAll::DEFAULT_TOPIC_QUEUE_NUMS, tbwWrite);
    check("H1 默认 d=4 建的 topic 队列数=min(4, TBW102)", w1 == expected1 && r1 == w1,
          "read=" + num(r1) + " write=" + num(w1) + " (TBW102=" + num(tbwWrite) + ")");

    // H2：d=2 必须把队列数带下去——修之前这里写死 4
    const std::string t2 = env.topic("Nums");
    {
        ScopedProducer p(env, "h2", "", 2);
        p.send(t2, "h2");
    }
    int32_t r2 = 0;
    int32_t w2 = 0;
    waitForTopic(env, t2, r2, w2);
    check("H2 defaultTopicQueueNums=2 建的 topic 只有 2 条队列",
          w2 == std::min(2, tbwWrite) && r2 == w2,
          "read=" + num(r2) + " write=" + num(w2) + "（写死 4 的旧行为会是 " + num(w1) + "）");

    // H3：c 指向自己的模板 topic（必须带 PERM_INHERIT，否则 TopicConfigManager:286 的
    // isInherited 不通过，broker 直接拒绝自动建 topic）
    const std::string src = env.topic("Src");
    try {
        env.admin.createTopicInBroker(env.brokerAddr, src, 3, 3,
                                      PermName::PERM_READ | PermName::PERM_WRITE |
                                          PermName::PERM_INHERIT);
    } catch (const std::exception& e) {
        check("H3 模板 topic 创建", false, e.what());
        return;
    }
    int32_t srcRead = 0;
    int32_t srcWrite = 0;
    waitForTopic(env, src, srcRead, srcWrite);
    check("H3 模板 topic 建好（3 条队列、带 INHERIT）",
          srcWrite == 3 && !(srcRead == tbwRead && srcWrite == tbwWrite),
          "src=(" + num(srcRead) + "," + num(srcWrite) + ") tbw102=(" + num(tbwRead) + "," +
              num(tbwWrite) + ")");

    const std::string t3 = env.topic("Inherit");
    {
        ScopedProducer p(env, "h3", src, 8);
        p.send(t3, "h3");
    }
    int32_t r3 = 0;
    int32_t w3 = 0;
    waitForTopic(env, t3, r3, w3);
    check("H3 createTopicKey=模板 topic 时被继承（min(8,3)=3 而不是 TBW102 的 " +
              num(tbwWrite) + "）",
          w3 == 3 && r3 == 3, "read=" + num(r3) + " write=" + num(w3));
}

// ---------------- H4：五种入口都照常落地 ----------------
void checkSendEntries(Env& env) {
    const std::string t4 = env.topic("Entries");
    std::map<std::string, bool> landed;
    auto latch = std::make_shared<Latch>();
    {
        ScopedProducer p(env, "h4");

        const SendResult rs = p.send(t4, "h4-sync");
        landed["sync"] = rs.sendStatus == SendStatus::SEND_OK;

        // 定点发送：显式给 mq，落在另一条调用链上（broker 名由调用方给）
        const MessageQueue mq = rs.messageQueue;
        const SendResult rb = (*p).send(Message(t4, bytesOf("h4-pinned")), mq, 5000);
        landed["pinned"] =
            rb.sendStatus == SendStatus::SEND_OK && rb.messageQueue.brokerName == mq.brokerName;

        try {
            (*p).sendOneway(Message(t4, bytesOf("h4-oneway")));
            landed["oneway"] = true;  // 单向没有应答，只能靠 H4 的总数兜底
        } catch (const std::exception& e) {
            landed["oneway"] = false;
            std::printf("  [diag] oneway threw: %s\n", e.what());
        }

        std::vector<Message> batch;
        for (int32_t i = 0; i < 3; ++i) {
            batch.push_back(Message(t4, bytesOf("h4-batch-" + std::to_string(i))));
        }
        landed["batch"] = (*p).sendBatch(batch, 5000).sendStatus == SendStatus::SEND_OK;

        (*p).sendAsync(Message(t4, bytesOf("h4-async")), latch, 5000);
        landed["async"] = latch->wait(10000) && latch->oks() == 1 &&
                          latch->result().sendStatus == SendStatus::SEND_OK;
    }
    for (const char* entry : {"sync", "pinned", "oneway", "batch", "async"}) {
        auto it = landed.find(entry);
        const bool ok = it != landed.end() && it->second;
        check(std::string("H4 ") + entry + " 入口发送成功", ok,
              ok ? "" : "该入口没有拿到 SEND_OK");
    }
    // 1 同步 + 1 定点 + 1 单向 + 3 批量 + 1 异步 = 7 条
    const bool got = waitUntil([&] { return totalMessages(env, t4) >= 7; }, 20000);
    const int64_t total = totalMessages(env, t4);
    check("H4 七条消息逐条落库（批量按子消息计）", got && total == 7,
          "total=" + num64(total));
}

// ---------------- H5：落点 broker 名与路由一致 ----------------
void checkBrokerName(Env& env) {
    const std::string t5 = env.topic("BrokerName");
    SendResult r5;
    {
        ScopedProducer p(env, "h5");
        r5 = p.send(t5, "h5");
    }
    check("H5 落点 broker 名与路由一致（n 就是它）",
          r5.sendStatus == SendStatus::SEND_OK && r5.messageQueue.brokerName == env.brokerName,
          "落点=" + r5.messageQueue.brokerName + " 路由=" + env.brokerName);
}

}  // namespace

int main(int argc, char** argv) {
    Env env;
    env.nsAddr = argc > 1 ? argv[1] : "127.0.0.1:9876";
    env.stamp = std::to_string(
        std::chrono::duration_cast<std::chrono::seconds>(
            std::chrono::system_clock::now().time_since_epoch())
            .count());

    try {
        env.start();
    } catch (const std::exception& e) {
        std::printf("admin start failed: %s\n", e.what());
        return 1;
    }

    // 集群探活：拿 broker 地址，并反查它的 brokerName（H5 的判据）
    ClusterInfo cluster;
    for (int32_t i = 0; i < 40; ++i) {
        try {
            cluster = env.admin.fetchBrokerClusterInfo();
            if (!cluster.brokerAddrTable.empty()) break;
        } catch (const std::exception& e) {
            if (i == 39) std::printf("  [diag] cluster probe failed: %s\n", e.what());
        }
        if (!cluster.brokerAddrTable.empty()) break;
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }
    if (cluster.brokerAddrTable.empty()) {
        check("集群探活", false, "nameServer 无 broker 注册");
        env.cleanup();
        return 1;
    }
    const auto it = cluster.brokerAddrTable.begin();
    env.brokerName = it->first;
    env.brokerAddr = it->second.selectBrokerAddr();
    check("集群探活", true, "broker=" + env.brokerAddr + " name=" + env.brokerName);

    std::printf("\n-- H0~H3: c/d 决定自动建 topic 的队列数 --\n");
    checkQueueNums(env);
    std::printf("\n-- H4: 五种发送入口逐条落地 --\n");
    checkSendEntries(env);
    std::printf("\n-- H5: n 就是路由选中的那台 broker --\n");
    checkBrokerName(env);

    env.cleanup();
    std::printf("\n== 结果: %d/%d 通过 ==\n", gPass, gPass + gFail);
    return gFail == 0 ? 0 : 1;
}
