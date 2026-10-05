// lite-pull **请求码 / broker 开关**（#107）真机验证。
// 与 `python/verify_lite_pull_code_live.py`、rust/csharp 的对应场景同题、逐条对应。
//
// 为什么必须真机：`FLAG_LITE_PULL_MESSAGE(0x10)` + `LITE_PULL_MESSAGE(361)` 这条链在离线
// 假 broker 上永远是绿的 —— 少了位、码还是 11 时，报文依然是一个完全合法的 pull，
// 假 broker（和真 broker 的普通 pull 分支）照常回消息。能把它区分出来的只有真 broker 的
// `litePullMessageEnable` 开关（`PullMessageProcessor:325-331` **只拦 361**）：把开关在
// 运行时翻成 false（UPDATE_BROKER_CONFIG，无需重启）：
//
//   S1 开关默认 true：lite pull 全链路正常（基线）。
//   S2 开关 false：
//      S2a 裸 361 请求 → NO_PERMISSION(16) + "…for lite pull consumer is forbidden"；
//      S2b 同队列同一位点的裸 11 请求 → 照常 SUCCESS 且拿到消息（**对照**：开关只管
//          lite，普通 pull 不受影响 —— 没有这条腿，S2a 的失败可能只是 broker 坏了）；
//      S2c lite 消费者安静饿死：消息明明在，poll 一条不来、拉取游标纹丝不动
//          （旧实现位不置/码为 11，这条腿会收到消息 → 判别器变红）；
//      S2d push 消费者照常消费（**对照**：整条消费链路没坏）。
//   S3 开关还原 true：lite pull 立即恢复。
//
// 退出前**无条件**把 `litePullMessageEnable` 改回原值（与 recall_live.cpp 同款）。
//
// 用法：./rmq_live_lite_pull_code 127.0.0.1:9876
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

// 与 Java DefaultLitePullConsumerImpl#pullSyncImpl:1058 的
// buildSysFlag(false, block, true, false, /*litePull=*/true) 对齐
int32_t liteFlag() {
    return PullSysFlag::buildSysFlag(/*commitOffset=*/false, /*suspend=*/false,
                                     /*subscription=*/true, /*classFilter=*/false,
                                     /*litePull=*/true);
}

// DefaultMQPullConsumerImpl.pullSyncImpl:248 的 4 参版本，lite 位必须为 0
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
            throw std::runtime_error("nameServer 无 broker 注册");
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
            check("路由可见：1 条队列", false, "got=0");
            fx.shutdown();
            return 1;
        }
        const MessageQueue q0 = mqs.front();
        check("路由可见：1 条队列", true, "queueId=" + std::to_string(q0.queueId));

        original = readFlag(fx.admin, fx.brokerAddr);
        haveOriginal = !original.empty();
        check(std::string("R0 能读到 broker 的 ") + kConfigKey, haveOriginal,
              "value=" + original);
        if (original != "true") {
            check(std::string("R0 已临时打开 ") + kConfigKey,
                  writeFlag(fx.admin, fx.brokerAddr, "true"));
        }
        if (readFlag(fx.admin, fx.brokerAddr) != "true") {
            check("R0 开关不在 true，后续检查无意义", false);
            fx.shutdown();
            return 1;
        }

        // ---------------- S1 基线 ----------------
        std::printf("\nS1 开关 true：lite pull 基线\n");
        fx.producer->send(Message(topic, Bytes("s1")), q0);
        c1 = newLite(nsAddr, group1, q0);
        std::vector<std::string> got1;
        {
            const int64_t deadline = nowMs() + 20000;
            while (got1.empty() && nowMs() < deadline) {
                for (const MessageExt& m : c1->poll(500)) got1.push_back(bodyOf(m));
            }
        }
        check("S1 lite 消费者收到消息", !got1.empty(), "got=" + join(got1));
        check("S1 拉取游标已推进", c1->pullCursorOf(q0) >= 1,
              "cursor=" + std::to_string(c1->pullCursorOf(q0)));

        // ---------------- S2 开关 false ----------------
        std::printf("\nS2 运行时关闭 %s（UPDATE_BROKER_CONFIG，不重启 broker）\n", kConfigKey);
        check("S2 开关已改为 false", writeFlag(fx.admin, fx.brokerAddr, "false"));
        const std::string readBack = readFlag(fx.admin, fx.brokerAddr);
        check("S2 开关读回确认", readBack == "false", "value=" + readBack);

        fx.producer->send(Message(topic, Bytes("s2")), q0);

        // S2a：裸 361 → NO_PERMISSION + 固定 remark（走我们自己的客户端 API 选码）
        bool denied = false;
        std::string denyDetail;
        try {
            MQClientInstance& client = fx.admin.client();
            client.pullMessage(group1, q0, 0, 32, liteFlag(), 0, "*", 0, "TAG",
                               /*timeoutMillis=*/30000, /*maxMsgBytes=*/-1,
                               /*suspendTimeoutMillis=*/15000, fx.brokerAddr);
            denyDetail = "竟然 SUCCESS —— lite 位/码没生效";
        } catch (const MQBrokerException& e) {
            denied = e.getResponseCode() == 16 &&
                     e.getResponseMessage().find(kDenyRemark) != std::string::npos;
            denyDetail = e.what();
        } catch (const std::exception& e) {
            denyDetail = e.what();
        }
        check("S2a 裸 361 被开关拒绝（NO_PERMISSION=16）", denied, denyDetail);

        // S2b：同队列同一位点的裸 11 → 照常拿消息（对照组）
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
        check("S2b 对照组：裸 11 不受开关影响，照常拿消息", classicOk, classicDetail);

        // S2c：lite 消费者安静饿死
        c2 = newLite(nsAddr, group2, q0);
        const std::vector<std::string> starved = pollFor(*c2, 8000);
        check("S2c 开关关闭期间 lite 消费者一条都收不到", starved.empty(),
              "got=" + join(starved));
        check("S2c 拉取游标纹丝不动", c2->pullCursorOf(q0) == 0,
              "cursor=" + std::to_string(c2->pullCursorOf(q0)));

        // S2d：push 消费者照常消费（对照组）
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
        check("S2d 对照组：push 消费者照常收到消息（开关只管 lite）", sink.has("s2d"),
              "got=" + join(sink.snapshot()));

        // ---------------- S3 还原 ----------------
        std::printf("\nS3 开关还原 true：lite 恢复\n");
        check("S3 开关已还原", writeFlag(fx.admin, fx.brokerAddr, "true"));
        fx.producer->send(Message(topic, Bytes("s3")), q0);
        c3 = newLite(nsAddr, group3, q0);
        std::vector<std::string> got3;
        {
            const int64_t deadline = nowMs() + 20000;
            while (got3.empty() && nowMs() < deadline) {
                for (const MessageExt& m : c3->poll(500)) got3.push_back(bodyOf(m));
            }
        }
        check("S3 还原后 lite 消费者立即恢复", !got3.empty(), "got=" + join(got3));
    } catch (const std::exception& e) {
        check("用例异常", false, e.what());
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
