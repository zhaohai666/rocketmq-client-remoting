// C++ client SQL92 filtering + CHECK_CLIENT_CONFIG(46) live-cluster verification (assert scenarios S1-S4 one by one;
// intercepting illegal configuration is locked offline by cpp/tests/test_check_client_config.cpp).
//
//
// Usage (start a local 5.5.1 cluster first; the broker needs enablePropertyFilter=true):
//   ./rmq_sql92_live [namesrv]
#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include "rocketmq/client/admin.h"
#include "rocketmq/client/consumer.h"
#include "rocketmq/client/exception.h"
#include "rocketmq/client/producer.h"
#include "rocketmq/client/result.h"
#include "rocketmq/common/mix_all.h"
#include "rocketmq/common/message.h"
#include "rocketmq/remoting/protocol/body.h"
#include "rocketmq/remoting/protocol/codes.h"
#include "rocketmq/remoting/protocol/route.h"
#include "rocketmq/remoting/rpchook.h"

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

Bytes bytesOf(const std::string& s) { return Bytes(s.begin(), s.end()); }
std::string bodyOf(const MessageExt& m) { return std::string(m.body.begin(), m.body.end()); }

std::string join(const std::vector<std::string>& items) {
    std::string out = "[";
    for (size_t i = 0; i < items.size(); ++i) out += (i ? "," : "") + items[i];
    return out + "]";
}

template <typename F>
bool waitFor(F&& pred, int timeoutMs, int stepMs = 300) {
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeoutMs);
    for (;;) {
        if (pred()) return true;
        if (std::chrono::steady_clock::now() >= deadline) return pred();
        std::this_thread::sleep_for(std::chrono::milliseconds(stepMs));
    }
}

// ---------------------------------------------------------------- forensic hook

// Hooked on the transport, capturing the request 46 really sent during startup (body included).
// doBeforeRequest runs on the sending thread and one hook object may be shared by several threads, so it must carry its own lock.
class CheckConfigProbe : public RPCHook {
public:
    void doBeforeRequest(const std::string& /*remoteAddr*/, RemotingCommand& request) override {
        std::lock_guard<std::mutex> lk(mtx_);
        codes.push_back(request.code);
        if (request.code != RequestCode::CHECK_CLIENT_CONFIG) return;
        CheckClientRequestBody body;
        if (!request.body.empty() && CheckClientRequestBody::decode(request.body, body)) {
            bodies.push_back(body);
        }
    }

    int checkCount() {
        std::lock_guard<std::mutex> lk(mtx_);
        int n = 0;
        for (int32_t c : codes) {
            if (c == RequestCode::CHECK_CLIENT_CONFIG) ++n;
        }
        return n;
    }

    std::string codesStr() {
        std::lock_guard<std::mutex> lk(mtx_);
        std::string out = "[";
        for (size_t i = 0; i < codes.size(); ++i) {
            out += (i ? "," : "") + std::to_string(codes[i]);
        }
        return out + "]";
    }

    bool firstBody(CheckClientRequestBody& out) {
        std::lock_guard<std::mutex> lk(mtx_);
        if (bodies.empty()) return false;
        out = bodies.front();
        return true;
    }

private:
    std::mutex mtx_;
    std::vector<int32_t> codes;
    std::vector<CheckClientRequestBody> bodies;
};

// ---------------------------------------------------------------- collecting listener

// Accumulates the consumed messages (the listener runs on consumer threads and the sink is shared across threads).
struct Sink {
    std::mutex mtx;
    std::vector<std::string> bodies;
    std::vector<std::string> colors;

    size_t count() {
        std::lock_guard<std::mutex> lk(mtx);
        return bodies.size();
    }
    std::vector<std::string> sorted() {
        std::lock_guard<std::mutex> lk(mtx);
        std::vector<std::string> v = bodies;
        std::sort(v.begin(), v.end());
        return v;
    }
    std::set<std::string> colorSet() {
        std::lock_guard<std::mutex> lk(mtx);
        return std::set<std::string>(colors.begin(), colors.end());
    }
};

class CollectListener : public MessageListenerConcurrently {
public:
    explicit CollectListener(Sink& sink) : sink_(sink) {}
    ConsumeConcurrentlyStatus consumeMessage(const std::vector<MessageExt>& msgs,
                                            ConsumeConcurrentlyContext&) override {
        std::lock_guard<std::mutex> lk(sink_.mtx);
        for (const MessageExt& m : msgs) {
            sink_.bodies.push_back(bodyOf(m));
            auto it = m.properties.find("color");
            sink_.colors.push_back(it == m.properties.end() ? std::string("<missing>")
                                                            : it->second);
        }
        return ConsumeConcurrentlyStatus::CONSUME_SUCCESS;
    }

private:
    Sink& sink_;
};

// ---------------------------------------------------------------- environment

std::string gStamp;
std::string gNsAddr;

// A set of stamp-isolated topics/groups, cleaned up completely after the run.
struct Env {
    DefaultMQAdminExt admin{"SQL92CPPADMIN"};
    std::vector<std::string> topics;
    std::vector<std::string> groups;

    std::string topic() {
        std::string t = "Sql92Cpp_" + gStamp;
        topics.push_back(t);
        return t;
    }
    std::string group(const std::string& kind) {
        std::string g = "GID_Sql92Cpp_" + gStamp + "_" + kind;
        groups.push_back(g);
        return g;
    }

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
        admin.setNamesrvAddr(gNsAddr);
        admin.start();
    }

    // Common consumer assembly: address + start point + subscription (TAG expression when selector is null) + optional forensic hook.
    // The group name is given when constructing DefaultMQPushConsumer (this port has no setConsumerGroup).
    void configure(DefaultMQPushConsumer& c, const std::string& topic,
                   const MessageSelector* selector, const std::string& expression,
                   const std::shared_ptr<RPCHook>& probe) {
        c.setNamesrvAddr(gNsAddr);
        c.setConsumeFromWhere(ConsumeFromWhere::CONSUME_FROM_LAST_OFFSET);
        if (probe) c.setRPCHook(probe);
        if (selector != nullptr) {
            c.subscribe(topic, *selector);
        } else {
            c.subscribe(topic, expression);
        }
    }

    int routeQueueCount(const std::string& topic) {
        try {
            const TopicRouteData route = admin.examineTopicRoute(topic);
            int n = 0;
            for (const QueueData& q : route.queueDatas) n += q.readQueueNums;
            return n;
        } catch (const std::exception&) {
            return 0;
        }
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
            // %RETRY%/%DLQ% go with the subscription group (only the delete-group API makes the broker really delete the retry topic)
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

// ---------------------------------------------------------------- S1 wire evidence
void s1WireEvidence(Env& env, const std::string& topic) {
    std::printf("\n---------- S1 request 46 during startup ----------\n");
    const std::string sqlGroup = env.group("SQL");
    auto probe = std::make_shared<CheckConfigProbe>();
    Sink sqlSink;
    std::shared_ptr<CollectListener> sqlListener(new CollectListener(sqlSink));
    DefaultMQPushConsumer c(sqlGroup);
    MessageSelector sel = MessageSelector::bySql("color = 'red'");
    env.configure(c, topic, &sel, std::string(), probe);
    c.setMessageListener(sqlListener);
    try {
        c.start();
        c.shutdown();
    } catch (const std::exception& e) {
        check("S1 SQL92 consumer starts successfully", false, e.what());
    }
    check("S1 a SQL92 subscription triggers exactly one CHECK_CLIENT_CONFIG(46)", probe->checkCount() == 1,
          "codes=" + probe->codesStr());
    CheckClientRequestBody body;
    const bool hasBody = probe->firstBody(body);
    const bool shapeOk = hasBody && body.group == sqlGroup && body.clientId == c.clientId()
        && body.subscriptionData.expressionType == std::string(ExpressionType::SQL92)
        && body.subscriptionData.subString == "color = 'red'"
        && body.subscriptionData.topic == topic;
    check("S1 body is a CheckClientRequestBody (clientId / group / subscriptionData)", shapeOk,
          "hasBody=" + std::to_string(hasBody) + " group=" + body.group
              + " clientId=" + body.clientId + " want=" + c.clientId()
              + " type=" + body.subscriptionData.expressionType
              + " sub=" + body.subscriptionData.subString
              + " topic=" + body.subscriptionData.topic + " want=" + topic);

    const std::string tagGroup = env.group("TAG");
    auto tagProbe = std::make_shared<CheckConfigProbe>();
    Sink tagSink;
    std::shared_ptr<CollectListener> tagListener(new CollectListener(tagSink));
    DefaultMQPushConsumer t(tagGroup);
    env.configure(t, topic, nullptr, "tagA || tagB", tagProbe);
    t.setMessageListener(tagListener);
    try {
        t.start();
    } catch (const std::exception& e) {
        check("S1 TAG consumer starts successfully", false, e.what());
    }
    check("S1 a pure TAG subscription sends not a single request 46 (isTagType short-circuits)", tagProbe->checkCount() == 0,
          "codes=" + tagProbe->codesStr());
    t.shutdown();
}

// ---------------------------------------------------------------- S2 / S3
void s2S3RealFiltering(Env& env, const std::string& topic, DefaultMQProducer& prod) {
    std::printf("\n---------- S2 broker filters by property / S3 an expression that never matches ----------\n");
    MessageSelector redSel = MessageSelector::bySql("color = 'red'");
    MessageSelector greenSel = MessageSelector::bySql("color = 'green'");

    Sink red, green, all;
    std::shared_ptr<CollectListener> redL(new CollectListener(red));
    std::shared_ptr<CollectListener> greenL(new CollectListener(green));
    std::shared_ptr<CollectListener> allL(new CollectListener(all));
    DefaultMQPushConsumer redC(env.group("FILTER"));
    DefaultMQPushConsumer greenC(env.group("NONE"));
    DefaultMQPushConsumer allC(env.group("ALL"));
    env.configure(redC, topic, &redSel, std::string(), nullptr);
    env.configure(greenC, topic, &greenSel, std::string(), nullptr);
    env.configure(allC, topic, nullptr, "*", nullptr);
    redC.setMessageListener(redL);
    greenC.setMessageListener(greenL);
    allC.setMessageListener(allL);
    // They must start before sending: CONSUME_FROM_LAST_OFFSET of a new consumer group skips messages published before startup.
    redC.start();
    greenC.start();
    allC.start();

    std::vector<std::string> redBodiesSent, blueBodiesSent;
    for (const char* color : {"red", "blue"}) {
        for (int i = 0; i < 3; ++i) {
            const std::string text = std::string("body-") + color + "-" + std::to_string(i);
            Message m(topic, bytesOf(text));
            m.setKeys(gStamp + "-" + color + "-" + std::to_string(i));
            m.putProperty("color", color);
            const SendResult r = prod.send(m, 5000);
            check("S2 send " + text + " succeeded", r.sendStatus == SendStatus::SEND_OK, r.msgId);
            if (std::string(color) == "red") {
                redBodiesSent.push_back(text);
            } else {
                blueBodiesSent.push_back(text);
            }
        }
    }
    std::sort(redBodiesSent.begin(), redBodiesSent.end());
    std::sort(blueBodiesSent.begin(), blueBodiesSent.end());

    waitFor([&] { return all.count() >= 6; }, 25000);
    std::this_thread::sleep_for(std::chrono::seconds(4));  // wait a bit more to confirm green is not merely "late"
    const std::vector<std::string> gotRed = red.sorted();
    const std::vector<std::string> gotAll = all.sorted();
    check("S2 SQL92(color=red) received 3 messages, exactly the 3 that were sent", gotRed == redBodiesSent,
          join(gotRed));
    std::vector<std::string> everything = redBodiesSent;
    everything.insert(everything.end(), blueBodiesSent.begin(), blueBodiesSent.end());
    std::sort(everything.begin(), everything.end());
    check("S2 the TAG '*' control group received 6 messages (red and blue all present)", gotAll == everything, join(gotAll));
    bool leaked = false;
    for (const std::string& b : blueBodiesSent) {
        if (std::find(gotRed.begin(), gotRed.end(), b) != gotRed.end()) leaked = true;
    }
    check("S2 the 3 blue messages did not leak into the SQL92 consumer (proof the broker really filters)", !leaked, join(gotRed));
    const std::set<std::string> colors = red.colorSet();
    check("S2 the received messages' property color is readable and always red",
        colors == std::set<std::string>{"red"},
        join(std::vector<std::string>(colors.begin(), colors.end())));
    check("S3 subscribing the never-matching color='green' => not one message received", green.count() == 0,
          join(green.sorted()));
    redC.shutdown();
    greenC.shutdown();
    allC.shutdown();
}

// ---------------------------------------------------------------- S4 counter-evidence
void s4BadExpressionFails(Env& env, const std::string& topic) {
    std::printf("\n---------- S4 an illegal expression fails at startup ----------\n");
    const std::string badGroup = env.group("BAD");
    MessageSelector bad = MessageSelector::bySql("color ==");
    Sink sink;
    std::shared_ptr<CollectListener> listener(new CollectListener(sink));
    DefaultMQPushConsumer c(badGroup);
    env.configure(c, topic, &bad, std::string(), nullptr);
    c.setMessageListener(listener);

    int threw = 0;
    int32_t code = 0;
    std::string msg;
    const auto began = std::chrono::steady_clock::now();
    try {
        c.start();
    } catch (const MQClientException& e) {
        threw = 1;
        code = e.getResponseCode();
        msg = e.what();
    } catch (const std::exception& e) {
        threw = 1;
        msg = std::string("wrong exception type: ") + e.what();
    }
    const long long costMs = std::chrono::duration_cast<std::chrono::milliseconds>(
                                 std::chrono::steady_clock::now() - began)
                                 .count();
    check("S4 start() throws MQClientException", threw == 1, msg);
    check("S4 the error code is the broker's SUBSCRIPTION_PARSE_FAILED(23)",
        code == ResponseCode::SUBSCRIPTION_PARSE_FAILED,
        "code=" + std::to_string(code) + " remark=" + msg);
    check("S4 after the failure the consumer is not left in a started state", !c.isStarted());
    check("S4 the illegal expression fails immediately (<5 s, not waiting out a timeout fallback)", costMs < 5000,
          std::to_string(costMs) + "ms");

    // Rolled back cleanly? The same object with a legal expression should be able to start again
    std::string retryMsg;
    try {
        MessageSelector ok = MessageSelector::bySql("color = 'blue'");
        env.configure(c, topic, &ok, std::string(), nullptr);
        c.start();
    } catch (const std::exception& e) {
        retryMsg = e.what();
    }
    check("S4 after the failure a fixed expression can start again (startup rolled back in place)", retryMsg.empty(), retryMsg);
    if (retryMsg.empty()) c.shutdown();
}

// ---------------------------------------------------------------- S5 send-first + FIRST_OFFSET
void s5SendFirstFirstOffset(Env& env, DefaultMQProducer& prod) {
    std::printf("\n---------- S5 send-first + CONSUME_FROM_FIRST_OFFSET ----------\n");
    // The regression this pins: an earlier draft resolved the initial offset of a SQL92
    // subscription to the max offset, silently skipping every message produced *before*
    // the consumer started (consumer-first passed only because the queues were empty and
    // the max offset was 0 anyway). A FIRST_OFFSET subscription starts at 0
    // regardless of the subscription type and lets the broker filter.
    const std::string topic = "Sql92CppFirst_" + gStamp;
    env.topics.push_back(topic);
    try {
        prod.createTopic(MixAll::DEFAULT_TOPIC, topic, 4);
    } catch (const std::exception& e) {
        check("S5 create topic", false, e.what());
        return;
    }
    const bool ready = waitFor([&] { return env.routeQueueCount(topic) >= 4; }, 20000);
    check("S5 topic route ready (4 queues)", ready,
          "queues=" + std::to_string(env.routeQueueCount(topic)));
    if (!ready) return;

    // Send FIRST: 2 red + 1 blue, no consumer running yet.
    std::vector<std::string> redSent, blueSent;
    for (const char* color : {"red", "blue"}) {
        for (int i = 0; i < 2; ++i) {
            const std::string text = std::string("first-") + color + "-" + std::to_string(i);
            Message m(topic, bytesOf(text));
            m.setKeys(gStamp + "-first-" + std::string(color) + "-" + std::to_string(i));
            m.putProperty("color", color);
            const SendResult r = prod.send(m, 5000);
            check("S5 send " + text + " succeeded", r.sendStatus == SendStatus::SEND_OK, r.msgId);
            if (std::string(color) == "red") {
                redSent.push_back(text);
            } else {
                blueSent.push_back(text);
            }
        }
    }
    std::sort(redSent.begin(), redSent.end());

    Sink sink;
    std::shared_ptr<CollectListener> listener(new CollectListener(sink));
    DefaultMQPushConsumer c(env.group("FIRST"));
    MessageSelector sel = MessageSelector::bySql("color = 'red'");
    env.configure(c, topic, &sel, std::string(), nullptr);
    c.setConsumeFromWhere(ConsumeFromWhere::CONSUME_FROM_FIRST_OFFSET);
    c.setMessageListener(listener);
    try {
        c.start();
    } catch (const std::exception& e) {
        check("S5 consumer starts", false, e.what());
        return;
    }
    waitFor([&] { return sink.count() >= 2; }, 25000);
    std::this_thread::sleep_for(std::chrono::seconds(2));  // rule out "arriving late" before judging the blue side
    const std::vector<std::string> got = sink.sorted();
    check("S5 SQL92 + FIRST_OFFSET receives the 2 red messages sent BEFORE start (was: skipped by the old max-offset short-circuit)",
          got == redSent, join(got));
    bool leaked = false;
    for (const std::string& b : blueSent) {
        if (std::find(got.begin(), got.end(), b) != got.end()) leaked = true;
    }
    check("S5 no blue leaks into the send-first SQL92 consumer", !leaked, join(got));
    c.shutdown();
}

}  // namespace

int main(int argc, char** argv) {
    gNsAddr = argc > 1 ? argv[1] : "127.0.0.1:9876";
    gStamp = std::to_string(
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch())
            .count());
    std::printf("SQL92 / CHECK_CLIENT_CONFIG live check on %s (stamp=%s)\n", gNsAddr.c_str(),
                gStamp.c_str());

    Env env;
    const std::string topic = env.topic();
    env.start();

    DefaultMQProducer prod("GID_Sql92Cpp_" + gStamp + "_P");
    prod.setNamesrvAddr(gNsAddr);
    prod.setSendMsgTimeout(5000);
    prod.start();
    try {
        // Queue count 4: create the topic with exactly 4 queues so queue attribution can be asserted
        prod.createTopic(MixAll::DEFAULT_TOPIC, topic, 4);
        const bool ready = waitFor([&] { return env.routeQueueCount(topic) >= 4; }, 20000);
        check("T0 topic route ready (4 queues)", ready,
              "queues=" + std::to_string(env.routeQueueCount(topic)));

        // Preflight: does this broker accept SQL92 subscriptions at all? The broker answers
        // CHECK_CLIENT_CONFIG(46) with SYSTEM_ERROR ("The broker does not support consumer to
        // filter message by SQL92") unless it runs with enablePropertyFilter=true, and
        // start() fails the same way here. Detect that up front and say what to do,
        // instead of failing confusingly deeper in.
        {
            const std::string preGroup = env.group("PREFLIGHT");
            DefaultMQPushConsumer pre(preGroup);
            MessageSelector preSel = MessageSelector::bySql("color = 'red'");
            env.configure(pre, topic, &preSel, std::string(), nullptr);
            // start() requires a listener (same as every other consumer in this probe)
            Sink preSink;
            std::shared_ptr<CollectListener> preListener(new CollectListener(preSink));
            pre.setMessageListener(preListener);
            bool refused = false;
            std::string refuseMsg;
            try {
                pre.start();
                pre.shutdown();
            } catch (const std::exception& e) {
                refused = true;
                refuseMsg = e.what();
            }
            if (refused &&
                refuseMsg.find("does not support consumer to filter message by") != std::string::npos) {
                std::printf("!! the broker refuses SQL92 subscriptions: %s\n", refuseMsg.c_str());
                std::printf("!! fix: set enablePropertyFilter=true in broker.conf and restart the broker,\n");
                std::printf("!!      then re-run this probe (the requirement is the same for any client).\n");
                std::printf("!! skipping end-to-end SQL92 verification: broker not configured for property filter\n");
                prod.shutdown();
                env.cleanup();
                std::printf("\n%d PASS / %d FAIL\n", gPass, gFail);
                return 0;
            }
            if (refused) {
                check("preflight SQL92 consumer start", false, refuseMsg);
            } else {
                std::printf("preflight OK: the broker accepts SQL92 subscriptions (enablePropertyFilter=true)\n");
            }
        }

        s1WireEvidence(env, topic);
        s2S3RealFiltering(env, topic, prod);
        s4BadExpressionFails(env, topic);
        s5SendFirstFirstOffset(env, prod);
    } catch (const std::exception& e) {
        std::printf("  [FAIL] aborted: %s\n", e.what());
        ++gFail;
    }
    prod.shutdown();
    env.cleanup();

    std::printf("\n%d PASS / %d FAIL\n", gPass, gFail);
    return gFail == 0 ? 0 : 1;
}
