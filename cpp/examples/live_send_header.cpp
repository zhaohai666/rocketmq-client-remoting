// Live-cluster verification of the three send-header fields (defaultTopic / defaultTopicQueueNums / brokerName).
// Usage: rmq_live_send_header 127.0.0.1:9876
//
// Assert every scenario H0-H5 one by one on a real cluster.
//
// Prerequisites: NameServer + Broker already running, with ``autoCreateTopicEnable=true``.
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

// Terminal state of one async send
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

// Environment: one name server address plus a set of stamp-isolated topics, deleted completely after the run.
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

// Read back the (read, write) queue numbers of the topic on the broker; false when it does not exist yet.
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

// Wait until the topic appears on the broker (auto topic creation is done synchronously in the send path, and applying the config takes a little time)
bool waitForTopic(Env& env, const std::string& topic, int32_t& read, int32_t& write) {
    return waitUntil([&] { return queueNums(env, topic, read, write); }, 10000);
}

// Total stored messages of the topic across all brokers (a new topic always has minOffset 0); -1 when unreadable
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

// Configure and start one producer per scenario
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

// ---------------- H0/H1/H2/H3: the queue numbers are decided by c and d ----------------
void checkQueueNums(Env& env) {
    int32_t tbwRead = 0;
    int32_t tbwWrite = 0;
    if (!queueNums(env, MixAll::DEFAULT_TOPIC, tbwRead, tbwWrite)) {
        check("H0 TBW102 template readable", false, "cannot read examineTopicConfig(TBW102)");
        return;
    }
    check("H0 TBW102 template readable", tbwWrite > 0,
          "read=" + num(tbwRead) + " write=" + num(tbwWrite));

    // H1: set nothing, d falls back to MixAll::DEFAULT_TOPIC_QUEUE_NUMS(4)
    const std::string t1 = env.topic("Def");
    {
        ScopedProducer p(env, "h1");
        const SendResult r = p.send(t1, "h1");
        check("H1 send succeeds with the default configuration", r.sendStatus == SendStatus::SEND_OK,
              "msgId=" + r.msgId + " broker=" + r.messageQueue.brokerName);
    }
    int32_t r1 = 0;
    int32_t w1 = 0;
    waitForTopic(env, t1, r1, w1);
    const int32_t expected1 = std::min(MixAll::DEFAULT_TOPIC_QUEUE_NUMS, tbwWrite);
    check("H1 default d=4 creates the topic with queue count=min(4, TBW102)", w1 == expected1 && r1 == w1,
          "read=" + num(r1) + " write=" + num(w1) + " (TBW102=" + num(tbwWrite) + ")");

    // H2: d=2 must carry the queue number down; before the fix this was hard-coded to 4
    const std::string t2 = env.topic("Nums");
    {
        ScopedProducer p(env, "h2", "", 2);
        p.send(t2, "h2");
    }
    int32_t r2 = 0;
    int32_t w2 = 0;
    waitForTopic(env, t2, r2, w2);
    check("H2 a topic created with defaultTopicQueueNums=2 has only 2 queues",
          w2 == std::min(2, tbwWrite) && r2 == w2,
          "read=" + num(r2) + " write=" + num(w2) + " (the old hard-coded-4 behaviour would give " + num(w1) + ")");

    // H3: c points at a self-made template topic (it must carry PERM_INHERIT, otherwise the
    // isInherited check at TopicConfigManager:286 fails and the broker outright refuses the auto topic creation)
    const std::string src = env.topic("Src");
    try {
        env.admin.createTopicInBroker(env.brokerAddr, src, 3, 3,
                                      PermName::PERM_READ | PermName::PERM_WRITE |
                                          PermName::PERM_INHERIT);
    } catch (const std::exception& e) {
        check("H3 template topic creation", false, e.what());
        return;
    }
    int32_t srcRead = 0;
    int32_t srcWrite = 0;
    waitForTopic(env, src, srcRead, srcWrite);
    check("H3 template topic ready (3 queues, with INHERIT)",
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
    check("H3 when createTopicKey is the template topic the value is inherited (min(8,3)=3, not TBW102's " +
              num(tbwWrite) + ")",
          w3 == 3 && r3 == 3, "read=" + num(r3) + " write=" + num(w3));
}

// ---------------- H4: all five send entries still land ----------------
void checkSendEntries(Env& env) {
    const std::string t4 = env.topic("Entries");
    std::map<std::string, bool> landed;
    auto latch = std::make_shared<Latch>();
    {
        ScopedProducer p(env, "h4");

        const SendResult rs = p.send(t4, "h4-sync");
        landed["sync"] = rs.sendStatus == SendStatus::SEND_OK;

        // pinned send: mq given explicitly, on a different call chain (the broker name comes from the caller)
        const MessageQueue mq = rs.messageQueue;
        const SendResult rb = (*p).send(Message(t4, bytesOf("h4-pinned")), mq, 5000);
        landed["pinned"] =
            rb.sendStatus == SendStatus::SEND_OK && rb.messageQueue.brokerName == mq.brokerName;

        try {
            (*p).sendOneway(Message(t4, bytesOf("h4-oneway")));
            landed["oneway"] = true;  // oneway has no response; only the H4 total count can back it up
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
        check(std::string("H4 ") + entry + " entry send succeeded", ok,
              ok ? "" : "this entry never got SEND_OK");
    }
    // 1 sync + 1 pinned + 1 oneway + 3 batch + 1 async = 7 messages
    const bool got = waitUntil([&] { return totalMessages(env, t4) >= 7; }, 20000);
    const int64_t total = totalMessages(env, t4);
    check("H4 all seven messages stored one by one (the batch counts its sub-messages)", got && total == 7,
          "total=" + num64(total));
}

// ---------------- H5: the landed broker name matches the route ----------------
void checkBrokerName(Env& env) {
    const std::string t5 = env.topic("BrokerName");
    SendResult r5;
    {
        ScopedProducer p(env, "h5");
        r5 = p.send(t5, "h5");
    }
    check("H5 the landed broker name matches the route (n is exactly it)",
          r5.sendStatus == SendStatus::SEND_OK && r5.messageQueue.brokerName == env.brokerName,
          "landed=" + r5.messageQueue.brokerName + " route=" + env.brokerName);
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

    // Cluster reachability probe: take the broker address and look up its brokerName (the H5 decision value)
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
        check("cluster reachability probe", false, "no broker registered on the name server");
        env.cleanup();
        return 1;
    }
    const auto it = cluster.brokerAddrTable.begin();
    env.brokerName = it->first;
    env.brokerAddr = it->second.selectBrokerAddr();
    check("cluster reachability probe", true, "broker=" + env.brokerAddr + " name=" + env.brokerName);

    std::printf("\n-- H0~H3: c/d decide the queue count of the auto-created topic --\n");
    checkQueueNums(env);
    std::printf("\n-- H4: all five send entries land one by one --\n");
    checkSendEntries(env);
    std::printf("\n-- H5: n is exactly the broker the route picked --\n");
    checkBrokerName(env);

    env.cleanup();
    std::printf("\n== result: %d/%d passed ==\n", gPass, gPass + gFail);
    return gFail == 0 ? 0 : 1;
}
