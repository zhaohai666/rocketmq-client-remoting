// Validators / TopicValidator live verification on a real cluster.
// Usage: rmq_validators_live 127.0.0.1:9876
//
// Per-assertion proof that "illegal names are stopped on the first local line, while legal names keep sending and receiving on the cluster".
//
// Prerequisite: NameServer + Broker already running (plain configuration; no ACL/TLS/trace needed).
//
// Scenarios:
//   S1 send path: blank / over-long / illegal-character topics, forbidden broker-internal
//      topics, the three body tiers, INNER_MULTI_DISPATCH separators — all rejected locally in <50ms (with the name server present)
//   S2 batch path: per-message checkMessage (an illegal member must also be caught) + homogeneity check
//   S3 producer start(): reserved group / illegal characters / over-long, three gates + the equal-length(120) boundary passes; failure does not enter started state
//   S4 positive leg: legal names still send and receive on the cluster (push + lite consumers each receive 3 messages)
//   S5 control leg: a legal but **nonexistent** topic takes a real round trip (the broker auto-creates it),
//      an order of magnitude slower than the local rejection — this quantifies "what local validation saves"
//   S6 pull / lite group-name gates + a legal pull group can look up queues and offsets
//   S7 createTopic local rejections (blank / illegal characters / system topic)
//   S8 an addressing failure (10004: not a single name server address) must not be reported as "topic has no route" (10005);
//      an address configured but unreachable (10005) versus an address never configured at all (10004) are two different things, with entirely different troubleshooting directions
#include <chrono>
#include <cstdio>
#include <functional>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include "rocketmq/client/consumer.h"
#include "rocketmq/client/exception.h"
#include "rocketmq/client/lite_pull_consumer.h"
#include "rocketmq/client/mq_client.h"
#include "rocketmq/client/producer.h"
#include "rocketmq/client/pull_consumer.h"
#include "rocketmq/client/validators.h"
#include "rocketmq/common/message.h"
#include "rocketmq/common/message_const.h"
#include "rocketmq/common/mix_all.h"
#include "rocketmq/common/top_addressing.h"
#include "rocketmq/remoting/protocol/codes.h"

using namespace rocketmq;

namespace {

int32_t gPass = 0;
int32_t gFail = 0;
std::string gNamesrv = "127.0.0.1:9876";

/// Timeout budget (ms) for local validation: the real round-trip leg is tens of measured milliseconds, as seen in S5.
constexpr double kLocalBudgetMs = 50.0;
constexpr int32_t kQueueNums = 4;

void check(const std::string& name, bool ok, const std::string& detail = std::string()) {
    if (ok) {
        ++gPass;
        std::printf("  [PASS] %s%s\n", name.c_str(), detail.empty() ? "" : ("  " + detail).c_str());
    } else {
        ++gFail;
        std::printf("  [FAIL] %s%s\n", name.c_str(), detail.empty() ? "" : ("  " + detail).c_str());
    }
}

std::string ms(double v) {
    char buf[32];
    std::snprintf(buf, sizeof(buf), "%.2fms", v);
    return buf;
}

int64_t nowMs() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::steady_clock::now().time_since_epoch())
        .count();
}

int64_t stamp() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::system_clock::now().time_since_epoch())
        .count();
}

std::string bodyOf(const MessageExt& m) { return std::string(m.body.begin(), m.body.end()); }

bool startsWith(const std::string& s, const std::string& p) { return s.rfind(p, 0) == 0; }

/// Run a thunk and flatten the thrown exception by type into (threw, message, response code, elapsed).
/// Anything outside MQClientException (e.g. std::invalid_argument from batches) records code 0, meaning "not in the client error-code system".
struct Captured {
    bool threw = false;
    std::string what;
    int32_t code = 0;
    double elapsedMs = 0.0;
};

Captured capture(const std::function<void()>& fn) {
    Captured c;
    const int64_t began = nowMs();
    try {
        fn();
    } catch (const MQClientException& e) {
        c.threw = true;
        c.what = e.what();
        c.code = e.getResponseCode();
    } catch (const std::exception& e) {
        c.threw = true;
        c.what = e.what();
    }
    c.elapsedMs = static_cast<double>(nowMs() - began);
    return c;
}

/// Assert "rejected locally": threw, message matched, code correct, and not so slow it looks like a network trip. Returns the measured elapsed time.
double expectLocalReject(const std::string& name, const std::function<void()>& fn,
                         const std::string& needle, int32_t wantCode) {
    Captured c = capture(fn);
    check(name,
          c.threw && c.what.find(needle) != std::string::npos && c.code == wantCode &&
              c.elapsedMs < kLocalBudgetMs,
          (c.threw ? c.what : std::string("no exception thrown")) + "  code=" + std::to_string(c.code) +
              "  " + ms(c.elapsedMs));
    return c.elapsedMs;
}

/// Assert "start() rejects locally, and after failing it did not mark itself started".
double expectStartReject(const std::string& name, const std::function<bool()>& startedFlag,
                         const std::function<void()>& startFn, const std::string& needle) {
    Captured c = capture(startFn);
    check(name, c.threw && c.what.find(needle) != std::string::npos && !startedFlag(),
          (c.threw ? c.what : std::string("no exception thrown")) + "  " + ms(c.elapsedMs));
    return c.elapsedMs;
}

Message msg(const std::string& topic, const std::string& body) {
    return Message(topic, Bytes(body.begin(), body.end()));
}

std::string repeatedChar(char ch, size_t n) { return std::string(n, ch); }

const char* kIllegalTail = " contains illegal characters, allowing only ^[%|a-zA-Z0-9_-]+$";

bool waitUntil(const std::function<bool()>& pred, int32_t timeoutMs) {
    const int64_t deadline = nowMs() + timeoutMs;
    while (nowMs() < deadline) {
        if (pred()) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }
    return pred();
}

/// Route queries throw when there is no route (TOPIC_NOT_EXIST / no route); flattened here into "queue count 0".
size_t routeQueueCount(const std::function<std::vector<MessageQueue>()>& fetch) {
    try {
        return fetch().size();
    } catch (const std::exception&) {
        return 0;
    }
}

// Collect consumed messages (thread-safe), filtered by body prefix
class Collector : public MessageListenerConcurrently {
public:
    ConsumeConcurrentlyStatus consumeMessage(const std::vector<MessageExt>& msgs,
                                             ConsumeConcurrentlyContext&) override {
        std::lock_guard<std::mutex> lk(m_);
        for (const MessageExt& m : msgs) msgs_.push_back(m);
        return ConsumeConcurrentlyStatus::CONSUME_SUCCESS;
    }

    std::vector<MessageExt> byPrefix(const std::string& prefix) {
        std::lock_guard<std::mutex> lk(m_);
        std::vector<MessageExt> out;
        for (const MessageExt& m : msgs_) {
            if (startsWith(bodyOf(m), prefix)) out.push_back(m);
        }
        return out;
    }

private:
    std::mutex m_;
    std::vector<MessageExt> msgs_;
};

// ---------------------------------------------------------------- S1 send path

double s1_sendPathRejects(DefaultMQProducer& p) {
    std::printf("== S1 local validation on the send path (name server present, still never touches the network)==\n");
    const std::string longTopic = repeatedChar('a', 128);
    struct Neg {
        const char* label;
        std::string topic;
        std::string needle;
        int32_t code;
    };
    const Neg negs[] = {
        {"S1a empty topic", "", "The specified topic is blank", ResponseCode::SYSTEM_ERROR},
        {"S1b all-whitespace topic", "   ", "The specified topic is blank", ResponseCode::SYSTEM_ERROR},
        {"S1c over-long topic(128)", longTopic, "is longer than topic max length 127",
         ResponseCode::SYSTEM_ERROR},
        {"S1d topic with a dot", "Validators.Live", kIllegalTail, ResponseCode::SYSTEM_ERROR},
        {"S1e non-ASCII topic", "Validators中文", kIllegalTail, ResponseCode::SYSTEM_ERROR},
        {"S1f forbidden broker-internal topic", "SCHEDULE_TOPIC_XXXX", "is forbidden",
         ResponseCode::SYSTEM_ERROR},
    };
    double local = 0.0;
    for (const Neg& n : negs) {
        local += expectLocalReject(
            n.label, [&] { p.send(msg(n.topic, "body")); }, n.needle, n.code);
    }
    local /= static_cast<double>(sizeof(negs) / sizeof(negs[0]));

    // The three body tiers + the LMQ separator: only these tiers carry MESSAGE_ILLEGAL(13)
    expectLocalReject("S1g empty body", [&] { p.send(msg("ValidatorsPositive", "")); },
                      "the message body length is zero", ResponseCode::MESSAGE_ILLEGAL);
    {
        Message m = msg("ValidatorsPositive", "x");
        m.hasBody = false;
        expectLocalReject("S1h null body", [&] { p.send(m); }, "the message body is null",
                          ResponseCode::MESSAGE_ILLEGAL);
    }
    // Exceeding maxMessageSize goes through Validators (the producer's checkMessage is protected,
    // and its semantics are just forwarding to Validators::checkMessage(msg, maxMessageSize_))
    expectLocalReject("S1i exceeds maxMessageSize",
                      [&] { Validators::checkMessage(msg("ValidatorsPositive", "12345"), 4); },
                      "the message body size over max value, MAX: 4",
                      ResponseCode::MESSAGE_ILLEGAL);
    check("S1j exactly equal to maxMessageSize passes",
          !capture([&] { Validators::checkMessage(msg("ValidatorsPositive", "1234"), 4); }).threw);
    {
        Message m = msg("ValidatorsPositive", "hello");
        m.setUserProperty(MessageConst::PROPERTY_INNER_MULTI_DISPATCH,
                          std::string("a") + kFileSeparator + "b");
        expectLocalReject("S1k INNER_MULTI_DISPATCH with a path separator", [&] { p.send(m); },
                          "INNER_MULTI_DISPATCH", ResponseCode::MESSAGE_ILLEGAL);
    }
    {
        Message legal = msg("ValidatorsPositive", "hello");
        legal.setUserProperty(MessageConst::PROPERTY_INNER_MULTI_DISPATCH, "%LMQ%queue:testCID");
        check("S1l an ordinary LMQ value is not killed by mistake",
              !capture([&] { Validators::checkMessage(legal, 4 << 20); }).threw);
    }
    // Order: topic first → forbidden list → body. Empty body + illegal topic must report the topic error
    {
        Captured c = capture([&] { p.send(msg("bad topic", "")); });
        check("S1m validation order: topic before body",
              c.threw && c.what.find("contains illegal characters") != std::string::npos,
              c.threw ? c.what : std::string("no exception thrown"));
    }
    return local;
}

// ---------------------------------------------------------------- S2 batches

void s2_batchRejects(DefaultMQProducer& p, const std::string& topic) {
    std::printf("== S2 per-message validation on the batch path ==\n");
    expectLocalReject(
        "S2a the batch contains one illegal topic",
        [&] { p.sendBatch({msg(topic, "ok-1"), msg("bad.topic", "ok-2")}); },
        std::string("The specified topic[bad.topic]") + kIllegalTail,
        ResponseCode::SYSTEM_ERROR);

    expectLocalReject(
        "S2b the batch contains one empty body",
        [&] { p.sendBatch({msg(topic, "ok-1"), msg(topic, "")}); },
        "the message body length is zero", ResponseCode::MESSAGE_ILLEGAL);

    Captured mixed = capture([&] {
        p.sendBatch({msg(topic, "ok-1"), msg(topic + "Other", "ok-2")});
    });
    check("S2c a non-homogeneous batch topic is rejected (non-MQClientException contract)",
          mixed.threw && mixed.what.find("should be the same") != std::string::npos &&
              mixed.code == 0 && mixed.elapsedMs < kLocalBudgetMs,
          mixed.threw ? mixed.what : std::string("no exception thrown"));
}

// ---------------------------------------------------------------- S3 producer group-name gates

void s3_producerGroupGates() {
    std::printf("== S3 group-name gates of producer start() ==\n");
    const std::string longGroup = repeatedChar('g', 121);
    struct Neg {
        const char* label;
        std::string group;
        std::string needle;
    };
    const Neg negs[] = {
        {"S3a reserved group DEFAULT_PRODUCER", MixAll::DEFAULT_PRODUCER_GROUP,
         "producerGroup can not equal DEFAULT_PRODUCER, please specify another one."},
        {"S3b group with a space", "bad group",
         std::string("the specified group[bad group]") + kIllegalTail},
        {"S3c over-long group(121)", longGroup, "is longer than group max length: 120"},
    };
    for (const Neg& n : negs) {
        // Rejected even when the name server points at the **real** cluster — the gate is the first local line, unrelated to network reachability
        DefaultMQProducer p(n.group);
        p.setNamesrvAddr(gNamesrv);
        expectStartReject(n.label, [&] { return p.isStarted(); }, [&] { p.start(); }, n.needle);
    }
    DefaultMQProducer edge(repeatedChar('g', 120));
    edge.setNamesrvAddr(gNamesrv);
    Captured c = capture([&] { edge.start(); });
    check("S3d a 120-char group name passes (the length test is > not >=)",
          !c.threw || c.what.find("group max length") == std::string::npos,
          c.threw ? c.what : std::string("start did not fail on group-name length"));
    if (edge.isStarted()) edge.shutdown();
}

// ---------------------------------------------------------------- S4 positive leg

void s4_positiveLeg(DefaultMQProducer& p, const std::string& topic, const std::string& group,
                    const std::string& prefix) {
    std::printf("== S4 positive leg: legal names still send and receive on the cluster ==\n");
    auto collector = std::make_shared<Collector>();
    DefaultMQPushConsumer cons(group);
    cons.setNamesrvAddr(gNamesrv);
    cons.setConsumeFromWhere(ConsumeFromWhere::CONSUME_FROM_LAST_OFFSET);
    cons.subscribe(topic, "*");
    cons.setMessageListener(collector);
    Captured started = capture([&] { cons.start(); });
    check("S4a a push consumer with a legal group name can start", !started.threw,
          started.threw ? started.what : std::string("ok"));
    if (started.threw) return;

    DefaultLitePullConsumer lite(group + "_lite");
    lite.setNamesrvAddr(gNamesrv);
    lite.setConsumeFromWhere(ConsumeFromWhere::CONSUME_FROM_LAST_OFFSET);
    lite.subscribe(topic, "*");
    Captured liteStarted = capture([&] { lite.start(); });
    check("S4b a lite consumer with a legal group name can start", !liteStarted.threw,
          liteStarted.threw ? liteStarted.what : std::string("ok"));

    // Bring consumers up before sending: a new group + CONSUME_FROM_LAST_OFFSET takes the queue-tail offset **at the moment of start**
    bool assigned = waitUntil([&] { return !lite.assignment().empty(); }, 20000);
    check("S4c lite gets a queue assignment", assigned,
          std::to_string(lite.assignment().size()) + " queues");

    for (int32_t i = 0; i < 3; ++i) {
        Captured s = capture([&] { p.send(msg(topic, prefix + "-body-" + std::to_string(i)), 5000); });
        check("S4d legal topic send succeeded #" + std::to_string(i), !s.threw,
              s.threw ? s.what : std::string("SEND_OK"));
    }

    bool gotAll = waitUntil([&] { return collector->byPrefix(prefix).size() >= 3; }, 30000);
    check("S4e the push consumer received all 3 messages", gotAll,
          "received " + std::to_string(collector->byPrefix(prefix).size()) + " messages");

    size_t polled = 0;
    const int64_t deadline = nowMs() + 20000;
    while (polled < 3 && nowMs() < deadline) {
        polled += lite.poll(1000).size();
    }
    check("S4f the lite consumer polled all 3 messages", polled >= 3, "polled " + std::to_string(polled) + " messages");
    lite.shutdown();
    cons.shutdown();
}

// ---------------------------------------------------------------- S5 control leg

void s5_controlLeg(DefaultMQProducer& p, double localMs) {
    std::printf("== S5 control leg: what local validation saves ==\n");
    // A legal but **nonexistent** topic: passes locally → real round trip (the broker auto-creates it per TBW102)
    const std::string missing = "ValidatorsMissing" + std::to_string(stamp());
    Captured c = capture([&] { p.send(msg(missing, "x"), 5000); });
    check("S5a a legal-but-missing topic is not falsely hurt locally (the broker auto-creates it)",
          !c.threw || c.what.find(kIllegalTail) == std::string::npos,
          (c.threw ? c.what : std::string("SEND_OK")) + "  " + ms(c.elapsedMs));
    const double baseline = localMs < 0.05 ? 0.05 : localMs;
    check("S5b the cluster leg is more than an order of magnitude slower than the local leg", c.elapsedMs > 10.0 * baseline,
          ms(c.elapsedMs) + " vs local " + ms(baseline));
}

// ---------------------------------------------------------------- S6 consumer side

void s6_consumerGates(const std::string& topic) {
    std::printf("== S6 pull/lite group-name gates + a legal pull group querying offsets ==\n");
    {
        DefaultMQPullConsumer pc(MixAll::DEFAULT_CONSUMER_GROUP);
        pc.setNamesrvAddr(gNamesrv);
        expectStartReject("S6a the pull consumer blocks DEFAULT_CONSUMER", [&] { return pc.isStarted(); },
                          [&] { pc.start(); },
                          "consumerGroup can not equal DEFAULT_CONSUMER, please specify another one.");
    }
    {
        DefaultLitePullConsumer lite(MixAll::DEFAULT_CONSUMER_GROUP);
        lite.setNamesrvAddr(gNamesrv);
        lite.subscribe(topic, "*");
        expectStartReject("S6b the lite consumer blocks DEFAULT_CONSUMER", [&] { return lite.isStarted(); },
                          [&] { lite.start(); },
                          "consumerGroup can not equal DEFAULT_CONSUMER, please specify another one.");
    }
    {
        DefaultLitePullConsumer lite("bad group");
        lite.setNamesrvAddr(gNamesrv);
        lite.subscribe(topic, "*");
        expectStartReject("S6c the lite consumer blocks an illegal-character group", [&] { return lite.isStarted(); },
                          [&] { lite.start(); },
                          std::string("the specified group[bad group]") + kIllegalTail);
    }
    {
        DefaultMQPullConsumer pc("GID_validators_pull_" + std::to_string(stamp()));
        pc.setNamesrvAddr(gNamesrv);
        Captured started = capture([&] { pc.start(); });
        bool ok = !started.threw;
        size_t queues = 0;
        int64_t maxOffset = -1;
        if (ok) {
            queues = routeQueueCount([&] { return pc.fetchSubscribeMessageQueues(topic); });
            if (queues > 0) {
                std::vector<MessageQueue> mqs = pc.fetchSubscribeMessageQueues(topic);
                try {
                    maxOffset = pc.maxOffset(mqs[0]);
                } catch (const std::exception& e) {
                    std::printf("  ... maxOffset failed: %s\n", e.what());
                }
            }
        }
        check("S6d a pull consumer with a legal group name can start and read offsets",
              ok && queues >= static_cast<size_t>(kQueueNums) && maxOffset >= 0,
              started.threw ? started.what
                            : ("queues=" + std::to_string(queues) +
                               " maxOffset=" + std::to_string(maxOffset)));
        if (ok) pc.shutdown();
    }
}

// ---------------------------------------------------------------- S7 createTopic

void s7_createTopicRejects(DefaultMQProducer& p) {
    std::printf("== S7 createTopic local rejections ==\n");
    expectLocalReject("S7a createTopic blocks illegal characters", [&] { p.createTopic(MixAll::DEFAULT_TOPIC, "bad topic", 4); },
                      std::string("The specified topic[bad topic]") + kIllegalTail,
                      ResponseCode::SYSTEM_ERROR);
    expectLocalReject("S7b createTopic blocks system topics",
                      [&] { p.createTopic(MixAll::DEFAULT_TOPIC, "RMQ_SYS_TRACE_TOPIC", 4); },
                      "is conflict with system topic", ResponseCode::SYSTEM_ERROR);
    expectLocalReject("S7c createTopic blocks an empty topic",
                      [&] { p.createTopic(MixAll::DEFAULT_TOPIC, "", 4); },
                      "The specified topic is blank", ResponseCode::SYSTEM_ERROR);
}

// ---------------------------------------------------------------- S8 addressing failure

/// A local port with no listener: the address is "configured but unreachable", used to tell it apart from "never configured at all".
std::string deadAddress() { return "127.0.0.1:19876"; }

struct RouteFunnelProbe : public DefaultMQProducer {
    explicit RouteFunnelProbe(const std::string& group) : DefaultMQProducer(group) {}
    using DefaultMQProducer::topicPublishInfo;
};

void s8_nameServerAddressing(DefaultMQProducer& p, const std::string& topic) {
    std::printf("== S8 the boundary between an addressing failure (10004) and 'topic has no route' (10005) ==\n");
    RouteFunnelProbe funnel("GID_validators_live_funnel");

    MQClientInstance noAddr("validators_live_noaddr", std::vector<std::string>{});
    expectLocalReject("S8a with zero addresses the funnel reports 10004 NO_NAME_SERVER (not 10005)",
                      [&] { funnel.topicPublishInfo(noAddr, topic); },
                      "No name server address, please set it.",
                      ClientErrorCode::NO_NAME_SERVER_EXCEPTION);

    MQClientInstance deadAddr("validators_live_deadaddr",
                              std::vector<std::string>{deadAddress()});
    Captured f = capture([&] { funnel.topicPublishInfo(deadAddr, topic); });
    check("S8b with an unreachable address the funnel does not impersonate 10004 (it judges configuration, not reachability)",
          f.threw && f.code != ClientErrorCode::NO_NAME_SERVER_EXCEPTION &&
              startsWith(f.what, "Can not find Message Queue for topic"),
          f.threw ? f.what + "  code=" + std::to_string(f.code) : std::string("no exception thrown"));
    {
        // End to end: the same send path (the sync-send retry loop qualifies the funnel exception into a client error code)
        DefaultMQProducer mis("GID_validators_live_deadns_" + std::to_string(stamp()));
        mis.setNamesrvAddr(deadAddress());
        Captured started = capture([&] { mis.start(); });
        if (started.threw) {
            check("S8c configured but unreachable → send qualified as 10005 NOT_FOUND_TOPIC", false,
                  "start failed first: " + started.what);
        } else {
            Captured s = capture([&] { mis.send(msg(topic, "dead-namesrv"), 3000); });
            check("S8c configured but unreachable → send qualified as 10005 NOT_FOUND_TOPIC (not 10004)",
                  s.threw && s.code == ClientErrorCode::NOT_FOUND_TOPIC_EXCEPTION,
                  s.threw ? s.what + "  code=" + std::to_string(s.code) + "  " + ms(s.elapsedMs)
                          : std::string("no exception thrown"));
            mis.shutdown();
        }
    }

    if (!DefaultTopAddressing::isConfigured()) {
        DefaultMQProducer unaddressed("GID_validators_live_unaddressed");
        Captured started = capture([&] { unaddressed.start(); });
        check("S8d with no name server configured at all, start() reports 10004 and does not enter started state",
              started.threw && started.code == ClientErrorCode::NO_NAME_SERVER_EXCEPTION &&
                  !unaddressed.isStarted() && started.elapsedMs < kLocalBudgetMs,
              (started.threw ? started.what : std::string("no exception thrown")) +
                  "  code=" + std::to_string(started.code) + "  " + ms(started.elapsedMs));
    } else {
        std::printf("  [skip] S8d: ROCKETMQ_NAMESRV_DOMAIN is configured, start() tolerates an empty static address\n");
    }

    // S8e: the control leg (the real cluster) — with addressing healthy the same topic still returns SEND_OK,
    // showing S8a blocked addressing rather than condemning this topic as a side effect.
    Captured ok = capture([&] { p.send(msg(topic, "addressing-fine"), 5000); });
    check("S8e control: addressing is fine on the real cluster and the same topic still SEND_OK", !ok.threw,
          ok.threw ? ok.what : std::string("SEND_OK"));
}

void report() {
    std::printf("############ PASS=%d FAIL=%d ############\n", gPass, gFail);
}

}  // namespace

int main(int argc, char* argv[]) {
    if (argc > 1) gNamesrv = argv[1];
    const int64_t st = stamp();
    const std::string sfx = std::to_string(st);
    const std::string topic = "ValidatorsCpp_" + sfx;
    const std::string group = "GID_validators_cpp_" + sfx;
    const std::string prefix = "validators-cpp-" + sfx;

    std::printf("=== RocketMQ validators live verify (cpp) ===\n");
    std::printf("namesrv=%s stamp=%lld\n", gNamesrv.c_str(), static_cast<long long>(st));

    DefaultMQProducer p("GID_validators_cpp_producer_" + sfx);
    p.setNamesrvAddr(gNamesrv);
    Captured started = capture([&] { p.start(); });
    if (started.threw) {
        check("S0 producer start", false, started.what);
        report();
        return 1;
    }
    std::printf("  [PASS] S0 producer start  group=%s\n", p.producerGroup().c_str());
    ++gPass;

    const double localMs = s1_sendPathRejects(p);
    s2_batchRejects(p, topic);
    s3_producerGroupGates();

    Captured created = capture([&] { p.createTopic(MixAll::DEFAULT_TOPIC, topic, kQueueNums); });
    if (created.threw) {
        check("S4 precondition: create topic", false, created.what);
        report();
        p.shutdown();
        return 1;
    }
    auto queues = [&] {
        return routeQueueCount([&] { return p.fetchPublishMessageQueues(topic); });
    };
    bool routed = waitUntil([&] { return queues() >= static_cast<size_t>(kQueueNums); }, 20000);
    check("S4 precondition: route discoverable", routed, "queues=" + std::to_string(queues()));
    if (routed) {
        s4_positiveLeg(p, topic, group, prefix);
        s6_consumerGates(topic);
        s8_nameServerAddressing(p, topic);
    }
    s5_controlLeg(p, localMs);
    s7_createTopicRejects(p);

    p.shutdown();
    report();
    return gFail == 0 ? 0 : 1;
}
