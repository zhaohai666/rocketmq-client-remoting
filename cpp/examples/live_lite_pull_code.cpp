// lite-pull **request code / broker switch** (#107) verified against a real cluster.
// Scenarios S1-S3 (S2 carries four legs a-d), asserted one by one on a live cluster.
//
// Why a live cluster is mandatory: the `FLAG_LITE_PULL_MESSAGE(0x10)` + `LITE_PULL_MESSAGE(361)` chain is
// always green against an offline fake broker -- with the bit missing, or the code still 11, the frame remains a
// perfectly legal pull, and the fake broker (and a real broker's plain pull branch) keeps answering with messages. Only the real
// broker's `litePullMessageEnable` switch can tell them apart (`PullMessageProcessor:325-331` **blocks only 361**): flip the
// switch to false at runtime (UPDATE_BROKER_CONFIG, no restart needed):
//
//   S1 switch defaults to true: the whole lite pull path works (baseline).
//   S2 switch false:
//      S2a bare 361 request -> NO_PERMISSION(16) + "for lite pull consumer is forbidden";
//      S2b bare 11 request on the same queue and offset -> still SUCCESS with messages (**control**: the switch only governs
//          lite, plain pull is unaffected -- without this leg an S2a failure could just mean the broker is broken);
//      S2c the lite consumer quietly starves: messages are there, yet poll never returns one and the pull cursor does not move
//          (the old implementation left the bit unset / code at 11, so this leg would receive messages -> the discriminator turns red);
//      S2d the push consumer keeps consuming (**control**: the whole consume chain is not broken).
//   S3 switch restored to true: lite pull recovers immediately.
//
// Before exiting, **unconditionally** restore `litePullMessageEnable` to its original value (same as the other live tools of this kind).
//
// Usage: ./rmq_live_lite_pull_code 127.0.0.1:9876
#include <chrono>
#include <cstdio>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "rocketmq/client/admin.h"
#include "rocketmq/client/consumer.h"
#include "rocketmq/client/exception.h"
#include "rocketmq/client/lite_pull_consumer.h"
#include "rocketmq/client/producer.h"
#include "rocketmq/client/result.h"
#include "rocketmq/common/message.h"
#include "rocketmq/common/mix_all.h"
#include "rocketmq/common/sysflag.h"
#include "rocketmq/remoting/protocol/codes.h"
#include "rocketmq/remoting/protocol/heartbeat.h"
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

int64_t nowMs() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::steady_clock::now().time_since_epoch())
        .count();
}

const char* kBrokerName = "broker-a";
const char* kConfigKey = "litePullMessageEnable";
const char* kDenyRemark = "for lite pull consumer is forbidden";

std::string bodyOf(const MessageExt& m) { return std::string(m.body.begin(), m.body.end()); }

std::string join(const std::vector<std::string>& v) {
    std::string s = "[";
    for (size_t i = 0; i < v.size(); ++i) {
        if (i != 0) s += ", ";
        s += v[i];
    }
    return s + "]";
}

int32_t liteFlag() {
    return PullSysFlag::buildSysFlag(/*commitOffset=*/false, /*suspend=*/false,
                                     /*subscription=*/true, /*classFilter=*/false,
                                     /*litePull=*/true);
}

// The 4-arg version of DefaultMQPullConsumerImpl.pullSyncImpl:248; the lite bit must be 0
int32_t classicFlag() {
    return PullSysFlag::buildSysFlag(/*commitOffset=*/false, /*suspend=*/false,
                                     /*subscription=*/true, /*classFilter=*/false);
}

std::string readFlag(DefaultMQAdminExt& admin, const std::string& addr) {
    try {
        PropertyMap cfg = admin.getBrokerConfig(addr);
        auto it = cfg.find(kConfigKey);
        return it == cfg.end() ? std::string() : it->second;
    } catch (const std::exception& e) {
        std::printf("  [diag] getBrokerConfig failed: %s\n", e.what());
        return std::string();
    }
}

bool writeFlag(DefaultMQAdminExt& admin, const std::string& addr, const std::string& value) {
    try {
        PropertyMap props;
        props[kConfigKey] = value;
        admin.updateBrokerConfig(addr, props);
        return true;
    } catch (const std::exception& e) {
        std::printf("  [diag] updateBrokerConfig(%s=%s) failed: %s\n", kConfigKey, value.c_str(),
                    e.what());
        return false;
    }
}

class BodySink {
public:
    std::mutex mtx;
    std::vector<std::string> bodies;
    bool has(const std::string& b) {
        std::lock_guard<std::mutex> lk(mtx);
        for (const std::string& x : bodies) {
            if (x == b) return true;
        }
        return false;
    }
    std::vector<std::string> snapshot() {
        std::lock_guard<std::mutex> lk(mtx);
        return bodies;
    }
};

class CollectListener : public MessageListenerConcurrently {
public:
    explicit CollectListener(BodySink& sink) : sink_(sink) {}
    ConsumeConcurrentlyStatus consumeMessage(const std::vector<MessageExt>& msgs,
                                            ConsumeConcurrentlyContext&) override {
        std::lock_guard<std::mutex> lk(sink_.mtx);
        for (const MessageExt& m : msgs) sink_.bodies.push_back(bodyOf(m));
        return ConsumeConcurrentlyStatus::CONSUME_SUCCESS;
    }

private:
    BodySink& sink_;
};

struct Fixture {
    std::string namesrv;
    std::string stamp;
    std::string brokerAddr;
    DefaultMQAdminExt admin;
    std::shared_ptr<DefaultMQProducer> producer;

    void start(const std::string& ns) {
        namesrv = ns;
        stamp = std::to_string(nowMs() % 1000000);
        admin.setNamesrvAddr(ns);
        admin.start();
        ClusterInfo cluster = admin.fetchBrokerClusterInfo();
        const std::vector<std::string> addrs = cluster.getBrokerAddrs();
        if (addrs.empty()) {
            throw std::runtime_error("no broker registered with the name server");
        }
        brokerAddr = addrs[0];
        producer = std::make_shared<DefaultMQProducer>("GID_cpp_lite_code_pg_" + stamp);
        producer->setNamesrvAddr(ns);
        producer->setInstanceName("cpp-live-lite-code-prod-" + stamp);
        producer->start();
    }

    std::vector<MessageQueue> queues(const std::string& topicName) {
        return admin.examineTopicRoute(topicName).getAllMessageQueue(topicName);
    }

    void shutdown() {
        if (producer) {
            try {
                producer->shutdown();
            } catch (const std::exception&) {
            }
        }
        try {
            admin.shutdown();
        } catch (const std::exception&) {
        }
    }
};

std::unique_ptr<DefaultLitePullConsumer> newLite(const std::string& ns, const std::string& group,
                                                 const MessageQueue& mq) {
    auto c = std::make_unique<DefaultLitePullConsumer>(group);
    c->setNamesrvAddr(ns);
    c->setInstanceName("cpp-lite-code-" + group);
    c->setConsumeFromWhere(ConsumeFromWhere::CONSUME_FROM_FIRST_OFFSET);
    c->setAutoCommit(false);
    c->assign({mq});
    c->start();
    return c;
}

std::vector<std::string> pollFor(DefaultLitePullConsumer& c, int64_t windowMs) {
    std::vector<std::string> out;
    const int64_t deadline = nowMs() + windowMs;
    while (nowMs() < deadline) {
        for (const MessageExt& m : c.poll(300)) {
            out.push_back(bodyOf(m));
        }
    }
    return out;
}

}  // namespace

int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::printf("usage: rmq_live_lite_pull_code <namesrv>\n");
        return 2;
    }
    const std::string nsAddr = argv[1];
    Fixture fx;
    try {
        fx.start(nsAddr);
    } catch (const std::exception& e) {
        std::printf("fixture start failed: %s\n", e.what());
        return 2;
    }

    const std::string topic = "CppLiteCodeLive" + fx.stamp;
    const std::string group1 = "GID_CppLiteCode_g1_" + fx.stamp;
    const std::string group2 = "GID_CppLiteCode_g2_" + fx.stamp;
    const std::string group3 = "GID_CppLiteCode_g3_" + fx.stamp;

    std::unique_ptr<DefaultLitePullConsumer> c1;
    std::unique_ptr<DefaultLitePullConsumer> c2;
    std::unique_ptr<DefaultLitePullConsumer> c3;
    std::shared_ptr<DefaultMQPushConsumer> push;
    std::string original;
    bool haveOriginal = false;

    try {
        fx.admin.createTopic(MixAll::DEFAULT_TOPIC, topic, 1);
        std::this_thread::sleep_for(std::chrono::seconds(3));
        std::printf("topic=%s broker=%s\n", topic.c_str(), kBrokerName);

        std::vector<MessageQueue> mqs = fx.queues(topic);
        if (mqs.empty()) {
            check("route visible: 1 queue", false, "got=0");
            fx.shutdown();
            return 1;
        }
        const MessageQueue q0 = mqs.front();
        check("route visible: 1 queue", true, "queueId=" + std::to_string(q0.queueId));

        original = readFlag(fx.admin, fx.brokerAddr);
        haveOriginal = !original.empty();
        check(std::string("R0 can read the broker's ") + kConfigKey, haveOriginal,
              "value=" + original);
        if (original != "true") {
            check(std::string("R0 temporarily enabled ") + kConfigKey,
                  writeFlag(fx.admin, fx.brokerAddr, "true"));
        }
        if (readFlag(fx.admin, fx.brokerAddr) != "true") {
            check("R0 switch is not true, later checks would be meaningless", false);
            fx.shutdown();
            return 1;
        }

        // ---------------- S1 baseline ----------------
        std::printf("\nS1 switch true: lite pull baseline\n");
        fx.producer->send(Message(topic, Bytes("s1")), q0);
        c1 = newLite(nsAddr, group1, q0);
        std::vector<std::string> got1;
        {
            const int64_t deadline = nowMs() + 20000;
            while (got1.empty() && nowMs() < deadline) {
                for (const MessageExt& m : c1->poll(500)) got1.push_back(bodyOf(m));
            }
        }
        check("S1 lite consumer received the message", !got1.empty(), "got=" + join(got1));
        check("S1 pull cursor advanced", c1->pullCursorOf(q0) >= 1,
              "cursor=" + std::to_string(c1->pullCursorOf(q0)));

        // ---------------- S2 switch false ----------------
        std::printf("\nS2 disable %s at runtime (UPDATE_BROKER_CONFIG, no broker restart)\n", kConfigKey);
        check("S2 switch set to false", writeFlag(fx.admin, fx.brokerAddr, "false"));
        const std::string readBack = readFlag(fx.admin, fx.brokerAddr);
        check("S2 switch read-back confirms", readBack == "false", "value=" + readBack);

        fx.producer->send(Message(topic, Bytes("s2")), q0);

        // S2a: bare 361 -> NO_PERMISSION + fixed remark (code chosen via our own client API)
        bool denied = false;
        std::string denyDetail;
        try {
            MQClientInstance& client = fx.admin.client();
            client.pullMessage(group1, q0, 0, 32, liteFlag(), 0, "*", 0, "TAG",
                               /*timeoutMillis=*/30000, /*maxMsgBytes=*/-1,
                               /*suspendTimeoutMillis=*/15000, fx.brokerAddr);
            denyDetail = "unexpectedly SUCCESS -- lite bit/code had no effect";
        } catch (const MQBrokerException& e) {
            denied = e.getResponseCode() == 16 &&
                     e.getResponseMessage().find(kDenyRemark) != std::string::npos;
            denyDetail = e.what();
        } catch (const std::exception& e) {
            denyDetail = e.what();
        }
        check("S2a bare 361 rejected by the switch (NO_PERMISSION=16)", denied, denyDetail);

        // S2b: bare 11 on the same queue and offset -> still gets messages (control group)
        bool classicOk = false;
        std::string classicDetail;
        try {
            MQClientInstance& client = fx.admin.client();
            PullResult r = client.pullMessage(group1, q0, 0, 32, classicFlag(), 0, "*", 0, "TAG",
                                              /*timeoutMillis=*/30000, /*maxMsgBytes=*/-1,
                                              /*suspendTimeoutMillis=*/15000, fx.brokerAddr);
            std::vector<std::string> got11;
            for (const MessageExt& m : r.msgFoundList) got11.push_back(bodyOf(m));
            classicOk = !got11.empty();
            classicDetail = "status=" + std::to_string(static_cast<int>(r.status)) +
                            " got=" + join(got11);
        } catch (const std::exception& e) {
            classicDetail = e.what();
        }
        check("S2b control: bare 11 is unaffected by the switch and still gets messages", classicOk, classicDetail);

        // S2c: the lite consumer quietly starves
        c2 = newLite(nsAddr, group2, q0);
        const std::vector<std::string> starved = pollFor(*c2, 8000);
        check("S2c the lite consumer receives nothing while the switch is off", starved.empty(),
              "got=" + join(starved));
        check("S2c pull cursor does not move an inch", c2->pullCursorOf(q0) == 0,
              "cursor=" + std::to_string(c2->pullCursorOf(q0)));

        // S2d: the push consumer keeps consuming (control group)
        BodySink sink;
        push = std::make_shared<DefaultMQPushConsumer>("GID_CppLiteCode_push_" + fx.stamp);
        push->setNamesrvAddr(nsAddr);
        push->setMessageListener(std::make_shared<CollectListener>(sink));
        push->subscribe(topic);
        push->start();
        fx.producer->send(Message(topic, Bytes("s2d")), q0);
        {
            const int64_t deadline = nowMs() + 20000;
            while (!sink.has("s2d") && nowMs() < deadline) {
                std::this_thread::sleep_for(std::chrono::milliseconds(300));
            }
        }
        check("S2d control: the push consumer still receives (the switch only gates lite)", sink.has("s2d"),
              "got=" + join(sink.snapshot()));

        // ---------------- S3 restore ----------------
        std::printf("\nS3 switch restored to true: lite recovers\n");
        check("S3 switch restored", writeFlag(fx.admin, fx.brokerAddr, "true"));
        fx.producer->send(Message(topic, Bytes("s3")), q0);
        c3 = newLite(nsAddr, group3, q0);
        std::vector<std::string> got3;
        {
            const int64_t deadline = nowMs() + 20000;
            while (got3.empty() && nowMs() < deadline) {
                for (const MessageExt& m : c3->poll(500)) got3.push_back(bodyOf(m));
            }
        }
        check("S3 the lite consumer recovers immediately after restoration", !got3.empty(), "got=" + join(got3));
    } catch (const std::exception& e) {
        check("case error", false, e.what());
    }

    for (DefaultLitePullConsumer* c : {c1.get(), c2.get(), c3.get()}) {
        if (c != nullptr) {
            try {
                c->shutdown();
            } catch (const std::exception&) {
            }
        }
    }
    if (push) {
        try {
            push->shutdown();
        } catch (const std::exception&) {
        }
    }
    if (haveOriginal) {
        const bool ok = writeFlag(fx.admin, fx.brokerAddr, original);
        std::printf("\n[restore] %s=%s → %s\n", kConfigKey, original.c_str(),
                    ok ? "OK" : "FAILED");
    }
    fx.shutdown();

    std::printf("\nLitePullCode(cpp): PASS=%d FAIL=%d\n", gPass, gFail);
    return gFail == 0 ? 0 : 1;
}
