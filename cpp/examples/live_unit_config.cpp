// C++ client unitName / unitMode / enableStreamRequestType live-cluster integration
// (scenarios U1-U5 asserted one by one on a real cluster).
//
// Why a live cluster is mandatory: offline unit tests of these three switches can only prove "the field was filled in",
// not that the broker honors it. Broker-visible evidence is limited to:
//   * send with unitMode -> the auto-created topic's topicSysFlag carries the UNIT bit;
//   * heartbeat with ConsumerData.unitMode -> the created %RETRY%group carries the UNIT_SUB bit;
//   * the clientId's @unitName/@STREAM suffix shows up in the connection info recorded by the broker
//     (examineConsumerConnectionInfo) -- the only observation point that proves
//     "the clientId that came online really is the assembled one".
// ReqT itself is inert for a plain broker (only the proxy/stream chain reads it), so stream here only verifies
// clientId; the hook ordering (ReqT must land inside the ACL signature) is locked by tests/test_acl.cpp.
//
// Usage: ./rmq_live_unit_config [namesrv] (needs a local 5.5.1 cluster with autoCreateTopicEnable=true)
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

// Environment: one name server address + a stamp-isolated set of topics/groups, cleaned up after the run.
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
    // The default topic's route already contains every broker in the cluster; take the first as the admin endpoint
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
            // %RETRY%/%DLQ% go away with the subscription group (only the delete-group API makes the broker really drop the retry topic)
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

// Configure and start a producer per scenario. instanceName is given explicitly so the clientId is predictable
// (the default name DEFAULT is rewritten in place to <pid>#<nanoTime>).
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

// An auto-created topic must register broker->namesrv before its route is queryable; with wantBit != 0 wait
// until that bit appears, with 0 just wait for route visibility (the control for "should not carry the unit bit").
int32_t waitSysFlag(Env& env, const std::string& topic, int32_t wantBit, int seconds) {
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(seconds);
    int32_t last = -1;
    for (;;) {
        std::vector<QueueData> queues;
        try {
            queues = env.admin.examineTopicRoute(topic).queueDatas;
        } catch (const std::exception&) {
            // Not created yet, keep waiting
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

// ---------------------------------------------------------------- U1 unitName -> clientId
void u1ClientIdCarriesUnitName(Env& env) {
    ScopedProducer p(env, "u1", "unitA");
    const std::string want = MixAll::cachedIpStr() + "@uc-cpp-u1-" + env.stamp + "@unitA";
    check("U1 clientId = ip@instanceName@unitName", p.clientId() == want,
          "actual=" + p.clientId() + " want=" + want);
    const SendResult r = p.send(env.topic("U1"), "u1");
    check("U1 sending still works with unitName", r.sendStatus == SendStatus::SEND_OK, r.msgId);

    ScopedProducer ctl(env, "u1ctl");
    check("U1 without unitName the clientId gains no extra segment",
          ctl.clientId() == MixAll::cachedIpStr() + "@uc-cpp-u1ctl-" + env.stamp,
          "actual=" + ctl.clientId());
}

// ---------------------------------------------------------------- U2 stream consumer clientId
void u2StreamConsumerIsVisibleOnBroker(Env& env) {
    const std::string t = env.topic("U2");
    const std::string g = env.group("u2");
    // Warm up with one message first: topic created + route visible so the consumer's first pull avoids TOPIC_NOT_EXIST
    {
        ScopedProducer warm(env, "u2warm");
        warm.send(t, "warm");
        check("U2 warm message route is visible", routeVisible(env, t, 20));
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
    check("U2 consumer clientId ends with @unitA@STREAM", endsWith(c.clientId(), wantSuffix),
          "actual=" + c.clientId());

    {
        ScopedProducer p(env, "u2send");
        p.send(t, "hello-stream");
    }
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(30);
    while (!hasBody(bodies, "hello-stream") && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
    }
    check("U2 stream consumer received the message", hasBody(bodies, "hello-stream"), join(bodies));

    // Broker-side connection info is the only observation point proving "the online clientId is the assembled one"
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
    check("U2 the clientId recorded on the broker also carries @unitA@STREAM", seenOnBroker, observed);
    c.shutdown();
}

// ---------------------------------------------------------------- U3 unitMode send -> UNIT bit
void u3UnitModeSendMarksTopic(Env& env) {
    const std::string on = env.topic("U3On");
    const std::string off = env.topic("U3Off");
    {
        ScopedProducer p(env, "u3on", "", /*unitMode=*/true);
        const SendResult r = p.send(on, "unit-on");
        check("U3 unitMode send succeeded", r.sendStatus == SendStatus::SEND_OK, r.msgId);
    }
    {
        ScopedProducer p(env, "u3off");
        const SendResult r = p.send(off, "unit-off");
        check("U3 control send succeeded", r.sendStatus == SendStatus::SEND_OK, r.msgId);
    }
    const int32_t onFlag = waitSysFlag(env, on, kFlagUnit, 30);
    check("U3 topic created with unitMode=true carries the UNIT bit", (onFlag & kFlagUnit) != 0,
          "sysFlag=" + std::to_string(onFlag));
    const int32_t offFlag = waitSysFlag(env, off, 0, 30);
    check("U3 topic created with unitMode=false carries no unit bit", (offFlag & kFlagUnit) == 0,
          "sysFlag=" + std::to_string(offFlag));
}

// ---------------------------------------------------------------- U4 heartbeat unitMode -> %RETRY% UNIT_SUB
void u4UnitModeConsumerMarksRetryTopic(Env& env) {
    const std::string t = env.topic("U4");
    const std::string g = env.group("u4");
    {
        ScopedProducer warm(env, "u4warm");
        warm.send(t, "warm");
        check("U4 warm message route is visible", routeVisible(env, t, 20));
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
    // %RETRY%<group> is created by the broker while processing the heartbeat, per ConsumerData.unitMode
    const int32_t flag = waitSysFlag(env, MixAll::getRetryTopic(g), kFlagUnitSub, 40);
    check("U4 heartbeat unitMode=true makes %RETRY% carry the UNIT_SUB bit", (flag & kFlagUnitSub) != 0,
          "sysFlag=" + std::to_string(flag));
    c.shutdown();
}

// ---------------------------------------------------------------- U5 stream producer + lite consumer
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
    check("U5 lite consumer default clientId carries @STREAM", endsWith(lite.clientId(), kStreamSuffix),
          "actual=" + lite.clientId());

    ScopedProducer p(env, "u5", "", /*unitMode=*/false, /*stream=*/true);
    check("U5 producer with stream explicitly enabled has clientId carrying @STREAM", endsWith(p.clientId(), kStreamSuffix),
          "actual=" + p.clientId());
    for (int i = 0; i < 3; ++i) {
        const SendResult r = p.send(t, "m" + std::to_string(i));
        check("U5 send #" + std::to_string(i) + " succeeded", r.sendStatus == SendStatus::SEND_OK,
              r.msgId);
    }

    // The assignment must be waited for only **after the first message**: the topic is auto-created by the first send;
    // before that the namesrv has no route, so rebalance seeing an empty queue is correct (not a client bug).
    for (int i = 0; i < 40 && lite.assignment().empty(); ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
    }
    check("U5 lite consumer got queue assignment", !lite.assignment().empty(),
          std::to_string(lite.assignment().size()));

    std::vector<std::string> got;
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(30);
    while (got.size() < 3 && std::chrono::steady_clock::now() < deadline) {
        // poll drains the local buffer, so all 3 messages only add up across repeated polls
        for (const MessageExt& m : lite.poll(1000)) got.push_back(bodyOf(m));
    }
    check("U5 lite consumer received all 3 messages",
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
        check("live integration error", false, e.what());
    }
    env.cleanup();
    std::printf("\nPASS=%d FAIL=%d\n", gPass, gFail);
    return gFail == 0 ? 0 : 1;
}
