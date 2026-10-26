// What a rebalance round OWES depends on whether the assignment moved: the topics whose queue SET
// changed re-share their subscription version, the two topic cache budgets are divided over the queue
// count of that round, ONE immediate heartbeat carries the new versions to every broker, and last the
// application's queue listener hears about exactly the topics that moved -- with the route's full
// queue set and this instance's share of it in the same call.
//
// The classic pull consumer owes the listener half of that (it has no subscription version of its own
// to carry and no broker beat to hurry); the push consumer owes all of it.
//
// Every positive leg sits next to a control that goes red when the restamp runs on every round, when
// the beat or the notification is paid on an unchanged round, or when the restamp covers the whole
// subscription table instead of the moved topics.
#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include "rocketmq/client/consumer.h"
#include "rocketmq/client/exception.h"
#include "rocketmq/client/message_queue_listener.h"
#include "rocketmq/client/mq_client.h"
#include "rocketmq/client/pull_consumer.h"
#include "rocketmq/client/result.h"
#include "rocketmq/common/message.h"
#include "rocketmq/common/mix_all.h"
#include "rocketmq/common/net_compat.h"
#include "rocketmq/common/util_all.h"
#include "rocketmq/remoting/protocol/body.h"
#include "rocketmq/remoting/protocol/codes.h"
#include "rocketmq/remoting/protocol/heartbeat.h"
#include "rocketmq/remoting/protocol/json.h"
#include "rocketmq/remoting/protocol/remoting_command.h"
#include "rocketmq/remoting/protocol/route.h"

using namespace rocketmq;

namespace {

using netcompat::kInvalidSocket;
using netcompat::socket_t;
using netcompat::socklen_type;

#ifdef _WIN32
constexpr int kShutdownHow = SD_BOTH;
#else
constexpr int kShutdownHow = SHUT_RDWR;
#endif

int fails = 0;
int checks = 0;

void expect(bool ok, const std::string& name, const std::string& detail = "") {
    ++checks;
    if (!ok) {
        ++fails;
        std::printf("FAIL %s %s\n", name.c_str(), detail.c_str());
    }
}

void expectInt(long long actual, long long expected, const std::string& name,
               const std::string& detail = "") {
    ++checks;
    if (actual != expected) {
        ++fails;
        std::printf("FAIL %s (actual=%lld expected=%lld) %s\n", name.c_str(), actual, expected,
                    detail.c_str());
    }
}

// ---------------------------------------------------------------- socket utilities

bool readN(socket_t s, char* buf, size_t n) {
    size_t got = 0;
    while (got < n) {
        int r = static_cast<int>(::recv(s, buf + got, static_cast<int>(n - got), 0));
        if (r <= 0) return false;
        got += static_cast<size_t>(r);
    }
    return true;
}

bool writeAll(socket_t s, const Bytes& data) {
    size_t sent = 0;
    while (sent < data.size()) {
        int n = static_cast<int>(::send(s, reinterpret_cast<const char*>(data.data() + sent),
                                        static_cast<int>(data.size() - sent),
                                        netcompat::sendFlags()));
        if (n <= 0) return false;
        sent += static_cast<size_t>(n);
    }
    return true;
}

bool readFrame(socket_t s, const std::atomic<bool>& alive, Bytes& out) {
    for (;;) {
        fd_set rfds;
        FD_ZERO(&rfds);
        FD_SET(s, &rfds);
        timeval tv;
        tv.tv_sec = 0;
        tv.tv_usec = 100 * 1000;
        int ready = ::select(netcompat::selectNfds(s), &rfds, nullptr, nullptr, &tv);
        if (!alive.load()) return false;
        if (ready == 0) continue;
        if (ready < 0) return false;
        break;
    }
    char lenBuf[4];
    if (!readN(s, lenBuf, 4)) return false;
    Bytes lenStr(lenBuf, 4);
    const int32_t totalLen = ByteReader::getInt32At(lenStr, 0);
    if (totalLen <= 0 || totalLen > 8 * 1024 * 1024) return false;
    Bytes body(static_cast<size_t>(totalLen), '\0');
    if (!readN(s, reinterpret_cast<char*>(body.data()), static_cast<size_t>(totalLen))) {
        return false;
    }
    out = lenStr + body;
    return true;
}

// ---------------------------------------------------------------- fake endpoint

// Plays the name server (answers 29 with the scripted route) and the broker (answers 38 with the
// scripted consumer id list, records every 34 HEART_BEAT body, and stays quiet on everything else).
class MockEndpoint {
public:
    MockEndpoint() {
        netcompat::ensureInitialized();
        listen_ = ::socket(AF_INET, SOCK_STREAM, 0);
        int one = 1;
        ::setsockopt(listen_, SOL_SOCKET, SO_REUSEADDR,
                     reinterpret_cast<const char*>(&one), static_cast<int>(sizeof(one)));
        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        addr.sin_port = 0;
        ::bind(listen_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr));
        ::listen(listen_, 16);
        socklen_type len = sizeof(addr);
        ::getsockname(listen_, reinterpret_cast<sockaddr*>(&addr), &len);
        port_ = ntohs(addr.sin_port);
        running_.store(true);
        acceptor_ = std::thread([this]() { acceptLoop(); });
    }

    ~MockEndpoint() {
        running_.store(false);
        if (acceptor_.joinable()) acceptor_.join();
        for (;;) {
            std::vector<std::thread> pending;
            {
                std::lock_guard<std::mutex> lk(workersM_);
                pending.swap(workers_);
            }
            if (pending.empty()) break;
            for (auto& t : pending) {
                if (t.joinable()) t.join();
            }
        }
        netcompat::closeSocket(listen_);
    }

    MockEndpoint(const MockEndpoint&) = delete;
    MockEndpoint& operator=(const MockEndpoint&) = delete;

    std::string address() const { return "127.0.0.1:" + std::to_string(port_); }

    void addRoute(const std::string& topic, const TopicRouteData& route) {
        std::lock_guard<std::mutex> lk(state_);
        routes_[topic] = route.encode();
    }

    void setConsumerList(const std::vector<std::string>& ids) {
        std::lock_guard<std::mutex> lk(state_);
        cidList_ = ids;
    }

    size_t heartbeatCount() {
        std::lock_guard<std::mutex> lk(state_);
        return heartbeats_.size();
    }

    // The subscription version the i-th heartbeat carried for one topic; -1 when that heartbeat does
    // not mention the topic at all.
    int64_t heartbeatSubVersion(size_t i, const std::string& topic) {
        std::string body;
        {
            std::lock_guard<std::mutex> lk(state_);
            if (i >= heartbeats_.size()) return -1;
            body = heartbeats_[i];
        }
        JsonValue v;
        std::string err;
        if (!jsonParse(body, v, &err)) return -2;
        const HeartbeatData hb = HeartbeatData::fromJson(v);
        for (const ConsumerData& cd : hb.consumerDataSet) {
            for (const SubscriptionData& sd : cd.subscriptionDataSet) {
                if (sd.topic == topic) return sd.subVersion;
            }
        }
        return -1;
    }

private:
    void acceptLoop() {
        while (running_.load()) {
            fd_set rfds;
            FD_ZERO(&rfds);
            FD_SET(listen_, &rfds);
            timeval tv;
            tv.tv_sec = 0;
            tv.tv_usec = 100 * 1000;
            int ready = ::select(netcompat::selectNfds(listen_), &rfds, nullptr, nullptr, &tv);
            if (ready == 0) continue;
            if (ready < 0) break;
            socket_t sock = ::accept(listen_, nullptr, nullptr);
            if (sock == kInvalidSocket) break;
            std::lock_guard<std::mutex> lk(workersM_);
            workers_.emplace_back([this, sock]() { serveConn(sock); });
        }
    }

    void serveConn(socket_t sock) {
        Bytes frame;
        while (running_.load() && readFrame(sock, running_, frame)) {
            RemotingCommand req;
            if (!RemotingCommand::tryDecode(frame, req, nullptr)) break;
            RemotingCommand resp;
            resp.opaque = req.opaque;
            resp.markResponseType();
            if (req.code == RequestCode::GET_ROUTEINFO_BY_TOPIC) {
                const std::string topic =
                    req.extFields.count("topic") ? req.extFields.at("topic") : std::string();
                std::lock_guard<std::mutex> lk(state_);
                auto it = routes_.find(topic);
                if (it != routes_.end()) {
                    resp.code = ResponseCode::SUCCESS;
                    resp.body = it->second;
                    resp.hasBody = true;
                } else {
                    resp.code = ResponseCode::TOPIC_NOT_EXIST;
                }
            } else if (req.code == RequestCode::GET_CONSUMER_LIST_BY_GROUP) {
                GetConsumerListByGroupResponseBody body;
                {
                    std::lock_guard<std::mutex> lk(state_);
                    body.consumerIdList = cidList_;
                }
                resp.code = ResponseCode::SUCCESS;
                resp.body = body.encode();
                resp.hasBody = true;
            } else if (req.code == RequestCode::HEART_BEAT) {
                std::lock_guard<std::mutex> lk(state_);
                heartbeats_.push_back(std::string(req.body.begin(), req.body.end()));
                resp.code = ResponseCode::SUCCESS;
            } else {
                resp.code = ResponseCode::SUCCESS;
            }
            Bytes out = resp.encode();
            if (!writeAll(sock, out)) break;
        }
        ::shutdown(sock, kShutdownHow);
        netcompat::closeSocket(sock);
    }

    socket_t listen_ = kInvalidSocket;
    uint16_t port_ = 0;
    std::atomic<bool> running_{false};
    std::mutex state_;
    std::map<std::string, Bytes> routes_;
    std::vector<std::string> cidList_;
    std::vector<std::string> heartbeats_;
    std::mutex workersM_;
    std::vector<std::thread> workers_;
    std::thread acceptor_;
};

// ---------------------------------------------------------------- scaffolding

const char* kTopicA = "RcChangeTopicA";
const char* kTopicB = "RcChangeTopicB";
const char* kBroker = "broker-a";
// A millisecond stamp no clock in this test run can reach: "the version moved" and "the version was
// left alone" both become comparisons against it.
constexpr int64_t kStale = 1700000000000LL;

MessageQueue queueOf(const std::string& topic, int32_t qid,
                     const std::string& broker = kBroker) {
    return MessageQueue(topic, broker, qid);
}

std::string keyOf(const MessageQueue& mq) {
    return DefaultMQPushConsumer::offsetKey(mq);
}

TopicRouteData routeQueues(const std::string& addr, int32_t readQueues) {
    TopicRouteData route;
    std::map<int64_t, std::string> addrs;
    addrs[MixAll::MASTER_ID] = addr;
    route.brokerDatas.emplace_back("DefaultCluster", kBroker, addrs);
    route.queueDatas.emplace_back(kBroker, readQueues, readQueues, 6, 0);
    return route;
}

// One entry per notification, so a leg counts calls per topic instead of only reading the last one.
class RecordingListener : public MessageQueueListener {
public:
    struct Call {
        std::string topic;
        int all;
        int divided;
    };

    void messageQueueChanged(const std::string& topic, const std::vector<MessageQueue>& mqAll,
                             const std::vector<MessageQueue>& mqDivided) override {
        std::lock_guard<std::mutex> lk(mu_);
        calls_.push_back(Call{topic, static_cast<int>(mqAll.size()),
                               static_cast<int>(mqDivided.size())});
    }
    size_t count(const std::string& topic) {
        std::lock_guard<std::mutex> lk(mu_);
        size_t n = 0;
        for (const Call& c : calls_) {
            if (c.topic == topic) ++n;
        }
        return n;
    }
    size_t total() {
        std::lock_guard<std::mutex> lk(mu_);
        return calls_.size();
    }
    Call last(const std::string& topic) {
        std::lock_guard<std::mutex> lk(mu_);
        for (auto it = calls_.rbegin(); it != calls_.rend(); ++it) {
            if (it->topic == topic) return *it;
        }
        return Call{std::string(), -1, -1};
    }

private:
    std::mutex mu_;
    std::vector<Call> calls_;
};

class ModeFlagListener : public MessageListener {
public:
    bool orderly() const override { return false; }
};

using KeySet = std::map<std::string, std::set<std::string>>;

KeySet setOf(std::initializer_list<std::pair<std::string, std::vector<MessageQueue>>> groups) {
    KeySet out;
    for (const auto& kv : groups) {
        for (const MessageQueue& mq : kv.second) out[kv.first].insert(keyOf(mq));
    }
    return out;
}

bool namesTheTopic(const std::vector<std::string>& moved, const std::string& topic) {
    return std::find(moved.begin(), moved.end(), topic) != moved.end();
}

// Wait until the condition holds (5s cap); returns whether it was observed.
template <typename Fn>
bool waitFor(Fn fn, int timeoutMs = 5000) {
    const int64_t deadline = UtilAll::currentTimeMillis() + timeoutMs;
    while (UtilAll::currentTimeMillis() < deadline) {
        if (fn()) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    return fn();
}

}  // namespace

// 1. The detector itself: which rounds moved? A queue is remembered by its key, so growth, shrink,
// hand-over to another broker and a topic appearing or disappearing are all moves -- and a re-ordering
// of the same queues is not.
void testMovedTopicsDetectsEverySetChange() {
    const MessageQueue a0 = queueOf(kTopicA, 0);
    const MessageQueue a1 = queueOf(kTopicA, 1);
    const MessageQueue a0OtherBroker = queueOf(kTopicA, 0, "broker-b");

    // Growth and shrink.
    expect(DefaultMQPushConsumer::movedTopicsForTest(setOf({{kTopicA, {a0}}}),
                                                     setOf({{kTopicA, {a0, a1}}}))
               .size()
           == 1,
           "a queue added is a move");
    expect(DefaultMQPushConsumer::movedTopicsForTest(setOf({{kTopicA, {a0, a1}}}),
                                                    setOf({{kTopicA, {a0}}}))
               .size()
           == 1,
           "a queue dropped is a move");

    // Same count, different broker: the queue changed hands even though nothing grew or shrank.
    const std::vector<std::string> handover = DefaultMQPushConsumer::movedTopicsForTest(
        setOf({{kTopicA, {a0}}}), setOf({{kTopicA, {a0OtherBroker}}}));
    expect(handover.size() == 1 && namesTheTopic(handover, kTopicA),
           "a same-count hand-over to another broker is a move");

    // Paired control: the identical set in another order is the same set.
    expect(DefaultMQPushConsumer::movedTopicsForTest(setOf({{kTopicA, {a0, a1}}}),
                                                     setOf({{kTopicA, {a1, a0}}}))
               .empty(),
           "re-ordering the same queues is NOT a move");

    // Only the topic that moved is named: a second topic with an identical share stays out of it.
    const MessageQueue b0 = queueOf(kTopicB, 0);
    const std::vector<std::string> oneMoved = DefaultMQPushConsumer::movedTopicsForTest(
        setOf({{kTopicA, {a0}}, {kTopicB, {b0}}}), setOf({{kTopicA, {a0, a1}}, {kTopicB, {b0}}}));
    expect(oneMoved.size() == 1 && namesTheTopic(oneMoved, kTopicA),
           "an unchanged topic is not named next to one that moved");

    // A topic that appears or disappears moves by definition (its set goes to or from nothing).
    expect(DefaultMQPushConsumer::movedTopicsForTest(KeySet{}, setOf({{kTopicA, {a0}}})).size() == 1,
           "a topic newly assigned is a move");
    expect(DefaultMQPushConsumer::movedTopicsForTest(setOf({{kTopicA, {a0}}}), KeySet{}).size() == 1,
           "a topic given up entirely is a move");
    expect(DefaultMQPushConsumer::movedTopicsForTest(KeySet{}, KeySet{}).empty(),
           "holding nothing in both rounds is not a change");
}

// 2. The version restamp follows the moved set, topic by topic: a topic whose share moved gets a new
// wall-clock stamp, every other subscription keeps the one it had. The stamp is what makes the broker
// re-read the subscription instead of reusing the filtered result it cached for the old generation.
void testRestampCoversOnlyTheMovedTopic() {
    DefaultMQPushConsumer c("GID_RcRestampScope");
    c.setMessageListener(std::make_shared<ModeFlagListener>());
    c.subscribe(kTopicA, "TagA");
    c.subscribe(kTopicB, "TagB");
    c.setSubscriptionVersionForTest(kTopicA, kStale);
    c.setSubscriptionVersionForTest(kTopicB, kStale);

    c.afterAssignmentChangeForTest({kTopicA});

    const int64_t aAfter = c.subscriptionVersion(kTopicA);
    expect(aAfter != kStale && aAfter > 0, "the moved topic's version was re-shared",
           std::to_string(aAfter));
    expectInt(c.subscriptionVersion(kTopicB), kStale,
              "the topic that did not move keeps its version (restamping every subscription would "
              "throw away the broker-side caching of an untouched filter)");

    // A second move of the same topic re-shares again (the stamp is per round, not once per topic).
    c.afterAssignmentChangeForTest({kTopicA});
    expect(c.subscriptionVersion(kTopicA) >= aAfter, "another move of the same topic re-shares again");

    // A round that moved nothing re-shares nothing at all: both sentinels survive.
    c.setSubscriptionVersionForTest(kTopicA, kStale);
    c.setSubscriptionVersionForTest(kTopicB, kStale);
    c.afterAssignmentChangeForTest({});
    expectInt(c.subscriptionVersion(kTopicA), kStale, "an unchanged round re-shapes no version");
    expectInt(c.subscriptionVersion(kTopicB), kStale, "nor any other subscription's");

    // A moved name with no subscription behind it (the topic was given up) is not stamped: there is
    // nothing to carry, and inventing a table entry would resurrect a subscription the consumer dropped.
    c.afterAssignmentChangeForTest({"RcChangeTopicGone"});
    expectInt(c.subscriptionVersion("RcChangeTopicGone"), 0,
              "no subscription entry, no stamp");
    expectInt(c.subscriptionVersion(kTopicB), kStale,
              "and the untouched subscriptions still hold their version");
    c.shutdown();
}

// 3. The division of the two topic cache budgets runs inside the same change pass, over the queue
// count this round holds -- and never on a round that moved nothing.
void testDivisionFollowsTheRoundQueueCount() {
    DefaultMQPushConsumer c("GID_RcDivideWithChange");
    c.setMessageListener(std::make_shared<ModeFlagListener>());
    c.subscribe(kTopicA, "*");
    c.setPullThresholdForTopic(600);
    c.setPullThresholdSizeForTopic(16);

    // Two queues held: the topic budget is split in two.
    c.setAssignedQueue(keyOf(queueOf(kTopicA, 0)), queueOf(kTopicA, 0));
    c.setAssignedQueue(keyOf(queueOf(kTopicA, 1)), queueOf(kTopicA, 1));
    c.afterAssignmentChangeForTest({kTopicA});
    expectInt(c.pullThresholdForQueue(), 300, "600 divided over 2 queues");
    expectInt(c.pullThresholdSizeForQueue(), 8, "16 MiB divided over 2 queues");

    // One queue revoked: the same numerator over the new divisor, re-run by the very next change.
    c.setAssignedQueue(keyOf(queueOf(kTopicA, 0)), queueOf(kTopicA, 0));
    DefaultMQPushConsumer afterRevoke("GID_RcDivideAfterRevoke");
    afterRevoke.setMessageListener(std::make_shared<ModeFlagListener>());
    afterRevoke.subscribe(kTopicA, "*");
    afterRevoke.setPullThresholdForTopic(600);
    afterRevoke.setPullThresholdSizeForTopic(16);
    afterRevoke.setAssignedQueue(keyOf(queueOf(kTopicA, 0)), queueOf(kTopicA, 0));
    afterRevoke.afterAssignmentChangeForTest({kTopicA});
    expectInt(afterRevoke.pullThresholdForQueue(), 600,
              "the budget is re-shared over the queues this round holds, not left at the old quotient");
    expectInt(afterRevoke.pullThresholdSizeForQueue(), 16, "and the size budget with it");

    // Paired control for "the division belongs to the change, not to every beat": with the budget off
    // (-1) and a hand-set per-queue knob, an unchanged round must leave that number alone.
    DefaultMQPushConsumer hand("GID_RcDivideUnchangedRound");
    hand.setMessageListener(std::make_shared<ModeFlagListener>());
    hand.subscribe(kTopicA, "*");
    hand.setAssignedQueue(keyOf(queueOf(kTopicA, 0)), queueOf(kTopicA, 0));
    hand.setPullThresholdForQueue(1234);
    hand.afterAssignmentChangeForTest({});
    expectInt(hand.pullThresholdForQueue(), 1234,
              "a round that moved nothing re-divides nothing");
    c.shutdown();
    afterRevoke.shutdown();
    hand.shutdown();
}

// 4. The wire half: the move pays one heartbeat immediately, and that beat carries the re-shared
// version of the moved topic while the topic that did not move still reports its old stamp.
void testImmediateHeartbeatCarriesTheNewVersionOnlyForTheMovedTopic() {
    MockEndpoint cluster;
    cluster.addRoute(kTopicA, routeQueues(cluster.address(), /*readQueues=*/1));
    cluster.addRoute(kTopicB, routeQueues(cluster.address(), /*readQueues=*/1));

    DefaultMQPushConsumer c("GID_RcHeartbeatOnMove");
    auto recorder = std::make_shared<RecordingListener>();
    c.setNamesrvAddr(cluster.address());
    c.setMessageListener(std::make_shared<ModeFlagListener>());
    c.setMessageQueueListener(recorder);
    c.subscribe(kTopicA, "TagA");
    c.subscribe(kTopicB, "TagB");
    // The instance's own id has to be in the group's consumer list, or the allocation gives this
    // instance nothing and no round ever moves. It sorts first, so with a one-queue topic B it keeps
    // holding that queue while topic A grows.
    c.start();
    cluster.setConsumerList({c.clientId(), "zzz-other-instance"});

    // First round with a real consumer list: both topics go from nothing to one queue.
    c.doRebalanceForTest();
    expect(waitFor([&] { return recorder->count(kTopicA) >= 1; }),
           "the first assignment notifies the application listener");
    expectInt(static_cast<long long>(recorder->count(kTopicB)), 1,
              "topic B holds its one queue now, so it is told once and never again");

    // Let the beats of the start settle before taking the baseline, then pin both versions.
    expect(waitFor([&] { return cluster.heartbeatCount() >= 1; }), "the start sent a heartbeat");
    c.setSubscriptionVersionForTest(kTopicA, kStale);
    c.setSubscriptionVersionForTest(kTopicB, kStale);
    const size_t beatsBefore = cluster.heartbeatCount();
    const size_t callsA = recorder->count(kTopicA);
    const size_t callsB = recorder->count(kTopicB);
    const size_t callsBefore = recorder->total();

    // Only topic A grows: 3 queues over 2 consumers puts 2 of them in this instance's share (it
    // sorts first), so its set moved. B keeps its single queue either way, so it did not.
    cluster.addRoute(kTopicA, routeQueues(cluster.address(), /*readQueues=*/3));
    c.client().updateTopicRouteInfoFromNameServer(kTopicA);
    expectInt(static_cast<long long>(c.fetchSubscribeMessageQueues(kTopicA).size()), 3,
              "the route change is visible in the client's subscribe view before the round");
    c.doRebalanceForTest();

    const int64_t aVersion = c.subscriptionVersion(kTopicA);
    expect(aVersion != kStale && aVersion > 0, "A's version was re-shared by the move",
           std::to_string(aVersion));
    expect(waitFor([&] { return cluster.heartbeatCount() > beatsBefore; }),
           "the move sent one heartbeat at once, without waiting for the periodic beat");
    const size_t newest = cluster.heartbeatCount() - 1;
    expectInt(cluster.heartbeatSubVersion(newest, kTopicA), aVersion,
              "the beat on the wire carries the version the moved topic now holds");
    expectInt(cluster.heartbeatSubVersion(newest, kTopicB), kStale,
              "and the topic that did not move still reports its old stamp");

    // The application hears about A only, once, with both sides of the split in the same call.
    expect(waitFor([&] { return recorder->count(kTopicA) > callsA; }),
           "the moved topic's listener call landed");
    expectInt(static_cast<long long>(recorder->total()),
              static_cast<long long>(callsBefore + 1),
              "exactly one notification for one moved topic");
    const RecordingListener::Call a = recorder->last(kTopicA);
    expectInt(a.all, 3, "mqAll is the route's full queue set");
    expectInt(a.divided, 2, "mqDivided is this instance's share of it");
    expectInt(static_cast<long long>(recorder->count(kTopicB)), static_cast<long long>(callsB),
              "topic B, whose share did not move, is not re-told");

    // Paired control: the very next round over the same assignment is not a change -- no beat, no
    // re-stamp, no listener call, and the numbers the previous round produced survive.
    const size_t beatsAfterMove = cluster.heartbeatCount();
    const size_t callsAfterMove = recorder->total();
    c.doRebalanceForTest();
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    expectInt(static_cast<long long>(cluster.heartbeatCount()),
              static_cast<long long>(beatsAfterMove), "an unchanged round sends no heartbeat");
    expectInt(static_cast<long long>(recorder->total()), static_cast<long long>(callsAfterMove),
              "an unchanged round notifies nobody");
    expectInt(c.subscriptionVersion(kTopicA), aVersion, "an unchanged round re-shares no version");
    expectInt(c.subscriptionVersion(kTopicB), kStale, "nor a stamp it never earned");
    c.shutdown();
}

// 5. The classic pull consumer: it has no subscription version to hurry to the broker, but an
// application that registered a queue listener is owed the notification whenever the pull-side queue
// set it is reading moves. Registering the listener must not be a no-op.
void testPullConsumerFiresTheQueueListenerOnItsBalanceView() {
    DefaultMQPullConsumer c("PG_RcPullListener");
    auto recorder = std::make_shared<RecordingListener>();
    c.setMessageQueueListener(recorder);

    const MessageQueue q0 = queueOf(kTopicA, 0);
    const MessageQueue q1 = queueOf(kTopicA, 1);

    // First view hands out the queues: the application learns what this instance started to hold.
    expect(c.publishAssignmentForTest(kTopicA, {q0, q1}, {q0}),
           "the first non-empty view notifies");
    expectInt(static_cast<long long>(recorder->count(kTopicA)), 1, "one call for one topic");
    const RecordingListener::Call first = recorder->last(kTopicA);
    expectInt(first.all, 2, "mqAll carries the route's queue set");
    expectInt(first.divided, 1, "mqDivided carries this instance's share");

    // Paired control: the same share again -- a route re-ordering is not a move and pays nothing.
    expect(!c.publishAssignmentForTest(kTopicA, {q0, q1}, {q0}),
           "re-publishing the same share does not notify");
    expect(!c.publishAssignmentForTest(kTopicA, {q1, q0}, {q0}),
           "the route arriving in another order is still the same share, so it does not notify");
    expectInt(static_cast<long long>(recorder->count(kTopicA)), 1,
              "and no second call reaches the application");

    // A moved share notifies again, with the new split.
    expect(c.publishAssignmentForTest(kTopicA, {q0, q1}, {q0, q1}), "a wider share notifies");
    expectInt(static_cast<long long>(recorder->last(kTopicA).divided), 2,
              "the call reports the share it just took");
    expectInt(static_cast<long long>(recorder->count(kTopicA)), 2, "one call per move");

    // Another topic is judged on its own: its first view notifies without touching this one's count.
    expect(c.publishAssignmentForTest(kTopicB, {q0}, {q0}), "a second topic notifies once");
    expectInt(static_cast<long long>(recorder->count(kTopicB)), 1, "counted per topic");
    expectInt(static_cast<long long>(recorder->count(kTopicA)), 2, "the other topic is not re-told");

    // An empty share for a topic nobody holds yet is not a change worth a call.
    DefaultMQPullConsumer fresh("PG_RcPullListenerEmpty");
    auto emptyRecorder = std::make_shared<RecordingListener>();
    fresh.setMessageQueueListener(emptyRecorder);
    expect(!fresh.publishAssignmentForTest(kTopicA, {}, {}),
           "publishing nothing for a topic never held notifies nobody");
    expectInt(static_cast<long long>(emptyRecorder->total()), 0, "no call at all");
}

// 6. The same set-equality rule the push consumer uses, read off the pull consumer's own predicate.
void testPullConsumerSetEqualityRule() {
    const MessageQueue q0 = queueOf(kTopicA, 0);
    const MessageQueue q1 = queueOf(kTopicA, 1);
    const MessageQueue otherBroker = queueOf(kTopicA, 0, "broker-b");
    const std::vector<MessageQueue> empty;

    expect(!DefaultMQPullConsumer::assignmentChangedForTest({q0, q1}, {q1, q0}),
           "a re-ordering is the same set");
    expect(DefaultMQPullConsumer::assignmentChangedForTest({q0}, {q0, q1}), "growth is a change");
    expect(DefaultMQPullConsumer::assignmentChangedForTest({q0, q1}, {q0}), "shrink is a change");
    expect(DefaultMQPullConsumer::assignmentChangedForTest({q0}, {otherBroker}),
           "a same-count hand-over to another broker is a change");
    expect(DefaultMQPullConsumer::assignmentChangedForTest(empty, {q0}),
           "taking a queue for the first time is a change");
    expect(DefaultMQPullConsumer::assignmentChangedForTest({q0}, empty),
           "giving the last queue up is a change");
    expect(!DefaultMQPullConsumer::assignmentChangedForTest(empty, empty),
           "nothing in both rounds is not a change");
}

// 7. A listener that throws is the application's bug, not the consumer's: the notification is
// isolated per topic, so one throw cannot stop the other moved topics from being told.
void testThrowingListenerDoesNotBreakTheRound() {
    class Throwing : public MessageQueueListener {
    public:
        void messageQueueChanged(const std::string& topic, const std::vector<MessageQueue>&,
                                 const std::vector<MessageQueue>&) override {
            if (topic == kTopicA) throw std::runtime_error("application bug");
            {
                std::lock_guard<std::mutex> lk(mu_);
                ++told;
            }
        }
        int told = 0;
        std::mutex mu_;
    };

    DefaultMQPullConsumer c("PG_RcPullThrowing");
    auto throwing = std::make_shared<Throwing>();
    c.setMessageQueueListener(throwing);
    const MessageQueue q0 = queueOf(kTopicA, 0);
    const MessageQueue b0 = queueOf(kTopicB, 0);
    expect(c.publishAssignmentForTest(kTopicA, {q0}, {q0}), "the moved topic is still reported");
    expect(c.publishAssignmentForTest(kTopicB, {b0}, {b0}),
           "and the round continues after a listener threw");
    expectInt(throwing->told, 1, "the second topic's listener really ran");
}

int main() {
    testMovedTopicsDetectsEverySetChange();
    testRestampCoversOnlyTheMovedTopic();
    testDivisionFollowsTheRoundQueueCount();
    testImmediateHeartbeatCarriesTheNewVersionOnlyForTheMovedTopic();
    testPullConsumerFiresTheQueueListenerOnItsBalanceView();
    testPullConsumerSetEqualityRule();
    testThrowingListenerDoesNotBreakTheRound();
    std::printf("rebalance_change: %d checks, %d failed\n", checks, fails);
    return fails == 0 ? 0 : 1;
}
