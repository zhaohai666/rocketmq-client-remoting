// C++ 客户端 unitName / unitMode / enableStreamRequestType 真机联调
// （对应 python/verify_unit_config_live.py、rust/examples/live_unit_config.rs 的 U1–U5）。
//
// 为什么必须真机：这三个开关在离线单测里只能证明「字段被填了」，证明不了 broker 认不认。
// broker 侧可见的证据只有这几处：
//   * 发送带 unitMode → 自动建出来的 topic 的 topicSysFlag 带 UNIT 位
//     （AbstractSendMessageProcessor.java:487-497）；
//   * 心跳带 ConsumerData.unitMode → %RETRY%group 建出来带 UNIT_SUB 位
//     （ClientManageProcessor.java:113-118）；
//   * clientId 的 @unitName/@STREAM 后缀会出现在 broker 记录的连接信息里
//     （examineConsumerConnectionInfo）—— 这是唯一能证明「上线的确实是拼好的那个
//     clientId」的观测点。
// ReqT 本身对普通 broker 是惰性的（只有 proxy/stream 链路读它），所以 stream 在这里只验
// clientId；钩子顺序（ReqT 必须落在 ACL 签名之内）由 tests/test_acl.cpp 锁死。
//
// 用法：./rmq_live_unit_config [namesrv]（需本地 5.5.1 集群，autoCreateTopicEnable=true）
#include <chrono>
#include <cstdint>
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
#include "rocketmq/common/mix_all.h"
#include "rocketmq/common/message.h"
#include "rocketmq/remoting/protocol/body.h"
#include "rocketmq/remoting/protocol/route.h"

using namespace rocketmq;

namespace {

int gPass = 0;
int gFail = 0;

void check(const std::string& name, bool ok, const std::string& detail = std::string()) {
    if (ok) {
        ++gPass;
    } else {
        ++gFail;
    }
    std::printf("  %s %s%s\n", ok ? "[PASS]" : "[FAIL]", name.c_str(),
                detail.empty() ? "" : ("  " + detail).c_str());
}

int64_t nowMs() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::steady_clock::now().time_since_epoch())
        .count();
}

Bytes bytesOf(const std::string& s) { return Bytes(s.begin(), s.end()); }
std::string bodyOf(const MessageExt& m) { return std::string(m.body.begin(), m.body.end()); }

bool endsWith(const std::string& s, const std::string& suffix) {
    return s.size() >= suffix.size()
           && s.compare(s.size() - suffix.size(), suffix.size(), suffix) == 0;
}

bool hasBody(const std::vector<std::string>& bodies, const std::string& want) {
    for (const std::string& b : bodies) {
        if (b == want) return true;
    }
    return false;
}

std::string join(const std::vector<std::string>& items) {
    std::string out = "[";
    for (size_t i = 0; i < items.size(); ++i) out += (i ? "," : "") + items[i];
    return out + "]";
}

// TopicSysFlag 的两个单元位（Java TopicSysFlag：FLAG_UNIT=0x1、FLAG_UNIT_SUB=0x2）
constexpr int32_t kFlagUnit = 0x1;
constexpr int32_t kFlagUnitSub = 0x2;
constexpr const char* kStreamSuffix = "@" "STREAM";

class CollectListener : public MessageListenerConcurrently {
public:
    explicit CollectListener(std::vector<std::string>& sink) : sink_(sink) {}
    ConsumeConcurrentlyStatus consumeMessage(const std::vector<MessageExt>& msgs,
                                            ConsumeConcurrentlyContext&) override {
        std::lock_guard<std::mutex> lk(mtx_);
        for (const MessageExt& m : msgs) sink_.push_back(bodyOf(m));
        return ConsumeConcurrentlyStatus::CONSUME_SUCCESS;
    }

private:
    std::vector<std::string>& sink_;
    std::mutex mtx_;
};

// 环境：一个名字服务地址 + 一套按 stamp 隔离的 topic/group，跑完自己扫干净。
struct Env {
    std::string nsAddr;
    std::string stamp;
    std::vector<std::string> topics;
    std::vector<std::string> groups;
    DefaultMQAdminExt admin{"UCPPADMIN"};

    std::string topic(const std::string& kind) {
        std::string t = "UnitCpp_" + stamp + "_" + kind;
        topics.push_back(t);
        return t;
    }
    std::string group(const std::string& kind) {
        std::string g = "GID_unit_cpp_" + stamp + "_" + kind;
        groups.push_back(g);
        return g;
    }
    // 默认 topic 的路由里就有全集群 broker，取第一个当管理端点
    std::string brokerAddr() {
        try {
            const TopicRouteData route = admin.examineTopicRoute(MixAll::DEFAULT_TOPIC);
            if (!route.brokerDatas.empty()) {
                const std::string addr = route.brokerDatas.front().selectBrokerAddr();
                if (!addr.empty()) return addr;
            }
        } catch (const std::exception& e) {
            std::printf("  [diag] broker route failed: %s\n", e.what());
        }
        return "127.0.0.1:10911";
    }

    void start() {
        admin.setNamesrvAddr(nsAddr);
        admin.start();
    }

    void cleanup() {
        const std::vector<std::string> topicList = topics;
        const std::vector<std::string> groupList = groups;
        const std::string addr = brokerAddr();
        for (const std::string& t : topicList) {
            try {
                admin.deleteTopic(t);
            } catch (const std::exception& e) {
                std::printf("  [diag] deleteTopic(%s) failed: %s\n", t.c_str(), e.what());
            }
        }
        for (const std::string& g : groupList) {
            // %RETRY%/%DLQ% 随订阅组一起删（只有走删组接口 broker 才会真删 retry topic）
            try {
                admin.deleteSubscriptionGroup(addr, g, true);
            } catch (const std::exception& e) {
                std::printf("  [diag] deleteSubscriptionGroup(%s) failed: %s\n", g.c_str(),
                            e.what());
            }
            try {
                admin.deleteTopic(MixAll::getRetryTopic(g));
            } catch (const std::exception&) {
            }
        }
        admin.shutdown();
    }
};

// 按场景配好并启动一个生产者。instanceName 显式给定，clientId 才可预测
//（默认名 DEFAULT 会被就地改写成 <pid>#<nanoTime>）。
class ScopedProducer {
public:
    ScopedProducer(Env& env, const std::string& kind, const std::string& unitName = std::string(),
                   bool unitMode = false, bool stream = false)
        : producer_("GID_unit_cpp_" + env.stamp + "_" + kind + "_pg") {
        producer_.setNamesrvAddr(env.nsAddr);
        producer_.setInstanceName("uc-cpp-" + kind + "-" + env.stamp);
        producer_.setSendMsgTimeout(5000);
        if (!unitName.empty()) producer_.setUnitName(unitName);
        producer_.setUnitMode(unitMode);
        producer_.setEnableStreamRequestType(stream);
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
    const std::string& clientId() const { return producer_.clientId(); }
    SendResult send(const std::string& topic, const std::string& body) {
        return producer_.send(Message(topic, bytesOf(body)), 5000);
    }

private:
    DefaultMQProducer producer_;
};

// 自动建出来的 topic 要先经 broker→namesrv 注册才查得到路由；wantBit 非 0 时等到该位
// 出现为止，为 0 时只等路由可见（用于「不该带单元位」的对照）。
int32_t waitSysFlag(Env& env, const std::string& topic, int32_t wantBit, int seconds) {
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(seconds);
    int32_t last = -1;
    for (;;) {
        std::vector<QueueData> queues;
        try {
            queues = env.admin.examineTopicRoute(topic).queueDatas;
        } catch (const std::exception&) {
            // 还没建出来，继续等
        }
        if (!queues.empty()) {
            last = queues.front().getTopicSysFlag();
            if (wantBit == 0 || (last & wantBit) != 0) return last;
        }
        if (std::chrono::steady_clock::now() >= deadline) return last;
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
    }
}

bool routeVisible(Env& env, const std::string& topic, int seconds) {
    return waitSysFlag(env, topic, 0, seconds) >= 0;
}

// ---------------------------------------------------------------- U1 unitName → clientId
void u1ClientIdCarriesUnitName(Env& env) {
    ScopedProducer p(env, "u1", "unitA");
    const std::string want = MixAll::cachedIpStr() + "@uc-cpp-u1-" + env.stamp + "@unitA";
    check("U1 clientId = ip@instanceName@unitName", p.clientId() == want,
          "actual=" + p.clientId() + " want=" + want);
    const SendResult r = p.send(env.topic("U1"), "u1");
    check("U1 带 unitName 仍能正常发送", r.sendStatus == SendStatus::SEND_OK, r.msgId);

    ScopedProducer ctl(env, "u1ctl");
    check("U1 不设 unitName 时 clientId 不多出段",
          ctl.clientId() == MixAll::cachedIpStr() + "@uc-cpp-u1ctl-" + env.stamp,
          "actual=" + ctl.clientId());
}

// ---------------------------------------------------------------- U2 stream 消费者的 clientId
void u2StreamConsumerIsVisibleOnBroker(Env& env) {
    const std::string t = env.topic("U2");
    const std::string g = env.group("u2");
    // 先暖一条：topic 建出来 + 路由可见，消费者首 pull 才不会撞 TOPIC_NOT_EXIST
    {
        ScopedProducer warm(env, "u2warm");
        warm.send(t, "warm");
        check("U2 预热消息的路由已可见", routeVisible(env, t, 20));
    }

    std::vector<std::string> bodies;
    DefaultMQPushConsumer c(g);
    c.setNamesrvAddr(env.nsAddr);
    c.setInstanceName("uc-cpp-u2-" + env.stamp);
    c.setUnitName("unitA");
    c.setEnableStreamRequestType(true);
    c.setConsumeFromWhere(ConsumeFromWhere::CONSUME_FROM_FIRST_OFFSET);
    c.setMessageListener(std::make_shared<CollectListener>(bodies));
    c.subscribe(t, "*");
    c.start();
    const std::string wantSuffix = "@unitA" + std::string(kStreamSuffix);
    check("U2 消费者 clientId 以 @unitA@STREAM 收尾", endsWith(c.clientId(), wantSuffix),
          "actual=" + c.clientId());

    {
        ScopedProducer p(env, "u2send");
        p.send(t, "hello-stream");
    }
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(30);
    while (!hasBody(bodies, "hello-stream") && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
    }
    check("U2 stream 消费者收到消息", hasBody(bodies, "hello-stream"), join(bodies));

    // broker 侧连接信息是唯一能证明「上线 clientId 就是拼好的那个」的观测点
    bool seenOnBroker = false;
    std::string observed;
    for (int i = 0; i < 20 && !seenOnBroker; ++i) {
        observed.clear();
        try {
            const ConsumerConnection conn = env.admin.examineConsumerConnectionInfo(g);
            for (const Connection& cn : conn.connectionSet) {
                observed += (observed.empty() ? "" : ",") + cn.clientId;
                if (endsWith(cn.clientId, wantSuffix)) seenOnBroker = true;
            }
        } catch (const std::exception& e) {
            observed = std::string("examine failed: ") + e.what();
        }
        if (!seenOnBroker) std::this_thread::sleep_for(std::chrono::milliseconds(500));
    }
    check("U2 broker 记录的 clientId 也带 @unitA@STREAM", seenOnBroker, observed);
    c.shutdown();
}

// ---------------------------------------------------------------- U3 unitMode 发送 → UNIT 位
void u3UnitModeSendMarksTopic(Env& env) {
    const std::string on = env.topic("U3On");
    const std::string off = env.topic("U3Off");
    {
        ScopedProducer p(env, "u3on", "", /*unitMode=*/true);
        const SendResult r = p.send(on, "unit-on");
        check("U3 unitMode 发送成功", r.sendStatus == SendStatus::SEND_OK, r.msgId);
    }
    {
        ScopedProducer p(env, "u3off");
        const SendResult r = p.send(off, "unit-off");
        check("U3 对照发送成功", r.sendStatus == SendStatus::SEND_OK, r.msgId);
    }
    const int32_t onFlag = waitSysFlag(env, on, kFlagUnit, 30);
    check("U3 unitMode=true 建出的 topic 带 UNIT 位", (onFlag & kFlagUnit) != 0,
          "sysFlag=" + std::to_string(onFlag));
    const int32_t offFlag = waitSysFlag(env, off, 0, 30);
    check("U3 unitMode=false 建出的 topic 不带单元位", (offFlag & kFlagUnit) == 0,
          "sysFlag=" + std::to_string(offFlag));
}

// ---------------------------------------------------------------- U4 心跳 unitMode → %RETRY% UNIT_SUB
void u4UnitModeConsumerMarksRetryTopic(Env& env) {
    const std::string t = env.topic("U4");
    const std::string g = env.group("u4");
    {
        ScopedProducer warm(env, "u4warm");
        warm.send(t, "warm");
        check("U4 预热消息的路由已可见", routeVisible(env, t, 20));
    }
    std::vector<std::string> bodies;
    DefaultMQPushConsumer c(g);
    c.setNamesrvAddr(env.nsAddr);
    c.setInstanceName("uc-cpp-u4-" + env.stamp);
    c.setUnitMode(true);
    c.setConsumeFromWhere(ConsumeFromWhere::CONSUME_FROM_FIRST_OFFSET);
    c.setMessageListener(std::make_shared<CollectListener>(bodies));
    c.subscribe(t, "*");
    c.start();
    // %RETRY%<group> 由 broker 处理心跳时按 ConsumerData.unitMode 建出来
    const int32_t flag = waitSysFlag(env, MixAll::getRetryTopic(g), kFlagUnitSub, 40);
    check("U4 心跳 unitMode=true 让 %RETRY% 带 UNIT_SUB 位", (flag & kFlagUnitSub) != 0,
          "sysFlag=" + std::to_string(flag));
    c.shutdown();
}

// ---------------------------------------------------------------- U5 stream 生产者 + lite 消费者
void u5StreamProducerAndLiteConsumer(Env& env) {
    const std::string t = env.topic("U5");
    const std::string g = env.group("u5");

    DefaultLitePullConsumer lite(g);
    lite.setNamesrvAddr(env.nsAddr);
    lite.setInstanceName("uc-cpp-u5-" + env.stamp);
    lite.setConsumeFromWhere(ConsumeFromWhere::CONSUME_FROM_FIRST_OFFSET);
    lite.setPollTimeoutMillis(1000);
    lite.subscribe(t, "*");
    lite.start();
    // Java 在 DefaultLitePullConsumer 的每个构造函数里置 true ⇒ 默认就该带 @STREAM
    check("U5 lite 消费者默认 clientId 带 @STREAM", endsWith(lite.clientId(), kStreamSuffix),
          "actual=" + lite.clientId());

    ScopedProducer p(env, "u5", "", /*unitMode=*/false, /*stream=*/true);
    check("U5 显式开 stream 的生产者 clientId 带 @STREAM", endsWith(p.clientId(), kStreamSuffix),
          "actual=" + p.clientId());
    for (int i = 0; i < 3; ++i) {
        const SendResult r = p.send(t, "m" + std::to_string(i));
        check("U5 第 " + std::to_string(i) + " 条发送成功", r.sendStatus == SendStatus::SEND_OK,
              r.msgId);
    }

    // 分配必须在**首条消息之后**才等：topic 由第一次发送自动建出来，之前 namesrv 没有
    // 路由，rebalance 拿到空队列是正确行为（不是客户端 bug）。
    for (int i = 0; i < 40 && lite.assignment().empty(); ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
    }
    check("U5 lite 消费者拿到队列分配", !lite.assignment().empty(),
          std::to_string(lite.assignment().size()));

    std::vector<std::string> got;
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(30);
    while (got.size() < 3 && std::chrono::steady_clock::now() < deadline) {
        // poll 会掏空本地缓冲，必须跨多次 poll 累加才凑得齐 3 条
        for (const MessageExt& m : lite.poll(1000)) got.push_back(bodyOf(m));
    }
    check("U5 lite 消费者收到全部 3 条",
          hasBody(got, "m0") && hasBody(got, "m1") && hasBody(got, "m2"), join(got));
    lite.shutdown();
}

}  // namespace

int main(int argc, char* argv[]) {
    Env env;
    env.nsAddr = argc > 1 ? argv[1] : "127.0.0.1:9876";
    env.stamp = std::to_string(nowMs() % 1000000);
    std::printf("namesrv=%s stamp=%s clientIdIp=%s\n\n", env.nsAddr.c_str(), env.stamp.c_str(),
                MixAll::cachedIpStr().c_str());
    env.start();
    try {
        u1ClientIdCarriesUnitName(env);
        u2StreamConsumerIsVisibleOnBroker(env);
        u3UnitModeSendMarksTopic(env);
        u4UnitModeConsumerMarksRetryTopic(env);
        u5StreamProducerAndLiteConsumer(env);
    } catch (const std::exception& e) {
        check("联调异常", false, e.what());
    }
    env.cleanup();
    std::printf("\nPASS=%d FAIL=%d\n", gPass, gFail);
    return gFail == 0 ? 0 : 1;
}
