// Pull-side background rebalance (Java DefaultMQPullConsumer.enableRebalance).
//
// Java's pull consumer is not a "no rebalance" shape: the shared RebalanceService thread calls
// mqClientFactory.doRebalance() every rocketmq.client.rebalance.waitInterval (20s), which walks the
// registered consumers and calls tryRebalance(). That method gates on enableRebalance (default **true**)
// and then runs rebalanceImpl.doRebalance(false) -> allocate -> messageQueueChanged(topic, mqAll, mqDivided).
//
// The criteria are taken from the facade's observable product (getRebalanceTable / the listener calls),
// driven by a loopback fake endpoint that answers the route and GET_CONSUMER_LIST_BY_GROUP(38):
//
//   * enableRebalance == false must still report **balanced** (tryRebalance returns true at
//     DefaultMQPullConsumerImpl:398-402) and must not consult the allocation strategy;
//   * the listener fires **only when the allocation changed** and receives (topic, mqAll, mqDivided);
//   * a throwing listener must not escape (RebalancePullImpl:49-51) -- one bad callback cannot kill the loop;
//   * fetchMessageQueuesInBalance prefers the rebalance table when it is warm and returns a copy.
#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <iterator>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include "rocketmq/client/allocate_strategy.h"
#include "rocketmq/client/exception.h"
#include "rocketmq/client/message_queue_listener.h"
#include "rocketmq/client/pull_consumer.h"
#include "rocketmq/common/message.h"
#include "rocketmq/common/mix_all.h"
#include "rocketmq/common/net_compat.h"
#include "rocketmq/common/util_all.h"
#include "rocketmq/remoting/protocol/body.h"
#include "rocketmq/remoting/protocol/codes.h"
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
// scripted consumer id list, acknowledges HEART_BEAT, and succeeds on everything else).
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

    void setCidList(const std::vector<std::string>& cids) {
        std::lock_guard<std::mutex> lk(state_);
        cidList_ = cids;
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
    std::mutex workersM_;
    std::vector<std::thread> workers_;
    std::thread acceptor_;
};

// ---------------------------------------------------------------- scaffolding

const char* kGroup = "PG_PullReb";
const char* kTopic = "TopicPullReb";
const char* kBroker = "broker-a";

class NoopListener : public MessageQueueListener {
public:
    void messageQueueChanged(const std::string&, const std::vector<MessageQueue>&,
                             const std::vector<MessageQueue>&) override {}
};

class RecordingListener : public MessageQueueListener {
public:
    struct Call {
        std::string topic;
        std::vector<MessageQueue> all;
        std::vector<MessageQueue> divided;
    };

    void messageQueueChanged(const std::string& topic, const std::vector<MessageQueue>& all,
                             const std::vector<MessageQueue>& divided) override {
        std::lock_guard<std::mutex> lk(mu_);
        calls_.push_back(Call{topic, all, divided});
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
        return Call{};
    }

private:
    std::mutex mu_;
    std::vector<Call> calls_;
};

class BoomListener : public MessageQueueListener {
public:
    void messageQueueChanged(const std::string&, const std::vector<MessageQueue>&,
                             const std::vector<MessageQueue>&) override {
        throw std::runtime_error("listener exploded");
    }
};

// Wraps the real strategy so a case can prove it was (not) consulted.
class CountingStrategy : public AllocateMessageQueueStrategy {
public:
    explicit CountingStrategy(std::shared_ptr<AllocateMessageQueueStrategy> inner)
        : inner_(std::move(inner)) {}
    std::vector<MessageQueue> allocate(const std::string& consumerGroup,
                                       const std::string& currentCid,
                                       const std::vector<MessageQueue>& mqAll,
                                       const std::vector<std::string>& cidAll) const override {
        calls_.fetch_add(1);
        return inner_->allocate(consumerGroup, currentCid, mqAll, cidAll);
    }
    std::string getName() const override { return inner_->getName(); }
    int32_t calls() const { return calls_.load(); }

private:
    std::shared_ptr<AllocateMessageQueueStrategy> inner_;
    mutable std::atomic<int32_t> calls_{0};
};

TopicRouteData routeN(int32_t n, const std::string& broker, const std::string& addr) {
    TopicRouteData route;
    std::map<int64_t, std::string> addrs;
    addrs[MixAll::MASTER_ID] = addr;
    route.brokerDatas.emplace_back("DefaultCluster", broker, addrs);
    route.queueDatas.emplace_back(broker, n, n, 6, 0);
    return route;
}

// Two brokers in one route, so a re-ordered queueDatas list is the same queue SET in another order.
TopicRouteData routeTwoBrokers(const std::string& addr, bool reverseOrder) {
    TopicRouteData route;
    std::map<int64_t, std::string> addrs;
    addrs[MixAll::MASTER_ID] = addr;
    route.brokerDatas.emplace_back("DefaultCluster", "broker-a", addrs);
    route.brokerDatas.emplace_back("DefaultCluster", "broker-b", addrs);
    const QueueData qa("broker-a", 2, 2, 6, 0);
    const QueueData qb("broker-b", 1, 1, 6, 0);
    if (reverseOrder) {
        route.queueDatas.push_back(qb);
        route.queueDatas.push_back(qa);
    } else {
        route.queueDatas.push_back(qa);
        route.queueDatas.push_back(qb);
    }
    return route;
}

// Starts a pull consumer against the fake endpoint. The instance name is unique per case (the same
// clientId shares a client instance).
std::unique_ptr<DefaultMQPullConsumer> started(const std::string& instance, const std::string& group,
                                               MockEndpoint& ep, const std::string& topic,
                                               const std::string& model = MessageModel::CLUSTERING,
                                               std::shared_ptr<MessageQueueListener> perTopic = nullptr,
                                               std::shared_ptr<MessageQueueListener> global = nullptr) {
    auto c = std::make_unique<DefaultMQPullConsumer>(group);
    c->setInstanceName(instance);
    c->setNamesrvAddr(ep.address());
    c->setMessageModel(model);
    c->registerMessageQueueListener(topic, perTopic != nullptr ? perTopic
                                                              : std::make_shared<NoopListener>());
    c->start();
    if (global != nullptr) c->setMessageQueueListener(global);
    return c;
}

std::vector<int32_t> queueIds(const std::vector<MessageQueue>& queues) {
    std::vector<int32_t> ids;
    ids.reserve(queues.size());
    for (const MessageQueue& mq : queues) ids.push_back(mq.queueId);
    std::sort(ids.begin(), ids.end());
    return ids;
}

std::set<std::string> identitySet(const std::vector<MessageQueue>& queues) {
    std::set<std::string> out;
    for (const MessageQueue& mq : queues) out.insert(mq.brokerName + "@" + std::to_string(mq.queueId));
    return out;
}

std::vector<std::string> sortedCids(const std::vector<std::string>& in) {
    std::vector<std::string> out = in;
    std::sort(out.begin(), out.end());
    return out;
}

template <typename Fn>
bool waitFor(Fn fn, int timeoutMs = 5000) {
    const int64_t deadline = UtilAll::currentTimeMillis() + timeoutMs;
    while (UtilAll::currentTimeMillis() < deadline) {
        if (fn()) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    return fn();
}

// ---------------------------------------------------------------- 1. switch defaults / round-trip

void testSwitchDefaultsAndClamps() {
    // Java DefaultMQPullConsumer.java:97 `private boolean enableRebalance = true`.
    DefaultMQPullConsumer c("PG_PullRebSwitch");
    expect(c.isEnableRebalance(), "enableRebalance defaults to Java's true");
    c.setEnableRebalance(false);
    expect(!c.isEnableRebalance(), "setter round-trips false");
    c.setEnableRebalance(true);
    expect(c.isEnableRebalance(), "setter round-trips true");

    expectInt(c.rebalanceWaitMillis(), 20000, "waitInterval defaults to 20000ms");
    expectInt(c.rebalanceMinIntervalMillis(), 1000, "minInterval defaults to 1000ms");
    c.setRebalanceWaitMillis(0);
    c.setRebalanceMinIntervalMillis(-5);
    expectInt(c.rebalanceWaitMillis(), 1, "a zero period is clamped (it would spin the loop)");
    expectInt(c.rebalanceMinIntervalMillis(), 1, "a negative floor is clamped");
}

// ---------------------------------------------------------------- 2. disabled switch

void testDisabledReportsBalancedAndNeverAllocates() {
    MockEndpoint ep;
    ep.addRoute(kTopic, routeN(4, kBroker, ep.address()));
    auto c = started("reb_disabled", kGroup, ep, kTopic);
    ep.setCidList({c->clientId(), "zzz-other"});

    auto counting = std::make_shared<CountingStrategy>(std::make_shared<AllocateMessageQueueAveragely>());
    c->setAllocateMessageQueueStrategy(counting);
    c->setEnableRebalance(false);

    // Java tryRebalance returns **true** when the switch is off (:398-402).
    expect(c->doRebalanceForTest(), "off must look balanced, not endless-unbalanced");
    expectInt(counting->calls(), 0, "no allocation may run while the switch is off");
    expect(c->getRebalanceTable().empty(), "nothing may be cached either");
    c->shutdown();
}

void testDisabledKeepsFetchOnTheSpot() {
    MockEndpoint ep;
    ep.addRoute(kTopic, routeN(4, kBroker, ep.address()));
    auto c = started("reb_disabled_fetch", kGroup, ep, kTopic);
    ep.setCidList({c->clientId(), "zzz-other"});

    // A stale table must be ignored while the switch is off.
    c->setRebalanceTableForTest(kTopic, {});
    c->setEnableRebalance(false);
    expectInt(static_cast<long long>(queueIds(c->fetchMessageQueuesInBalance(kTopic)).size()), 2,
              "falls back to on-the-spot averaging");
    const std::vector<int32_t> ids = queueIds(c->fetchMessageQueuesInBalance(kTopic));
    expect(ids == std::vector<int32_t>({0, 1}), "this instance (sorted first) takes the first half");
    c->shutdown();
}

// ---------------------------------------------------------------- 3. broadcasting

void testBroadcastingFirstRoundNotifiesWithAllQueues() {
    MockEndpoint ep;
    ep.addRoute(kTopic, routeN(3, kBroker, ep.address()));
    auto recorder = std::make_shared<RecordingListener>();
    auto c = started("reb_bcast", kGroup, ep, kTopic, MessageModel::BROADCASTING, nullptr, recorder);
    ep.setCidList({"someone-else"});

    expect(!c->doRebalanceForTest(), "the first round changes the empty table");
    expectInt(static_cast<long long>(recorder->count(kTopic)), 1, "one notification for the first view");
    const RecordingListener::Call call = recorder->last(kTopic);
    expect(call.topic == kTopic, "the callback names the topic");
    expect(identitySet(call.all) == identitySet(call.divided),
           "broadcasting: mqAll and mqDivided are the same full set");
    expectInt(static_cast<long long>(call.all.size()), 3, "all 3 queues are reported");

    expect(c->doRebalanceForTest(), "a second round over the same set is balanced");
    expectInt(static_cast<long long>(recorder->count(kTopic)), 1,
              "messageQueueChanged fires only on a real change");
    c->shutdown();
}

// ---------------------------------------------------------------- 4. clustering

void testClusteringTakesItsShare() {
    MockEndpoint ep;
    ep.addRoute(kTopic, routeN(4, kBroker, ep.address()));
    auto c = started("reb_avg", kGroup, ep, kTopic);
    ep.setCidList({c->clientId(), "zzz-other"});

    expect(!c->doRebalanceForTest(), "the first round changes the empty table");
    const auto table = c->getRebalanceTable();
    expect(table.count(kTopic) == 1, "the topic is present in the table");
    if (table.count(kTopic) == 1) {
        expect(queueIds(table.at(kTopic)) == std::vector<int32_t>({0, 1}),
               "averagely: the instance sorting first takes the first half");
    }
    c->shutdown();
}

void testTwoInstancesGetDisjointShares() {
    MockEndpoint ep;
    ep.addRoute(kTopic, routeN(5, kBroker, ep.address()));
    auto a = started("reb_pair_a", kGroup, ep, kTopic);
    auto b = started("reb_pair_b", kGroup, ep, kTopic);
    ep.setCidList(sortedCids({a->clientId(), b->clientId()}));

    a->doRebalanceForTest();
    b->doRebalanceForTest();
    const auto ta = a->getRebalanceTable();
    const auto tb = b->getRebalanceTable();
    expect(ta.count(kTopic) == 1 && tb.count(kTopic) == 1, "both instances produced a table entry");
    if (ta.count(kTopic) == 1 && tb.count(kTopic) == 1) {
        const std::set<std::string> sa = identitySet(ta.at(kTopic));
        const std::set<std::string> sb = identitySet(tb.at(kTopic));
        std::set<std::string> both;
        std::set_intersection(sa.begin(), sa.end(), sb.begin(), sb.end(),
                              std::inserter(both, both.begin()));
        expect(both.empty(), "the same queue must never be owned twice");
        std::set<std::string> all = sa;
        all.insert(sb.begin(), sb.end());
        expectInt(static_cast<long long>(all.size()), 5, "together they cover all 5 queues");
    }
    a->shutdown();
    b->shutdown();
}

void testNewQueueTriggersExactlyOneNotification() {
    MockEndpoint ep;
    ep.addRoute(kTopic, routeN(2, kBroker, ep.address()));
    auto recorder = std::make_shared<RecordingListener>();
    auto c = started("reb_grow", kGroup, ep, kTopic, MessageModel::CLUSTERING, nullptr, recorder);
    ep.setCidList({c->clientId()});

    c->doRebalanceForTest();
    expectInt(static_cast<long long>(recorder->count(kTopic)), 1, "the first round notifies once");

    ep.addRoute(kTopic, routeN(3, kBroker, ep.address()));  // a queue was added
    c->client().updateTopicRouteInfoFromNameServer(kTopic);
    expect(!c->doRebalanceForTest(), "a grown share is unbalanced");
    expectInt(static_cast<long long>(recorder->count(kTopic)), 2, "exactly one more notification");
    const RecordingListener::Call call = recorder->last(kTopic);
    expectInt(static_cast<long long>(call.all.size()), 3, "mqAll is the route's new full set");
    expectInt(static_cast<long long>(call.divided.size()), 3, "the sole instance takes all 3");

    expect(c->doRebalanceForTest(), "the next round over the same set is balanced");
    expectInt(static_cast<long long>(recorder->count(kTopic)), 2, "an unchanged round notifies nobody");
    c->shutdown();
}

void testEmptyFirstAllocationIsBalancedAndSilent() {
    MockEndpoint ep;
    ep.addRoute(kTopic, routeN(4, kBroker, ep.address()));
    auto recorder = std::make_shared<RecordingListener>();
    auto c = started("reb_empty_first", kGroup, ep, kTopic, MessageModel::CLUSTERING, nullptr,
                     recorder);
    // This instance is not in the group's consumer list: the strategy hands it nothing.
    ep.setCidList({"zzz-other"});

    // "nothing -> nothing" is not a change (Java's updateProcessQueueTableInRebalance reports none), so
    // the round is balanced and the listener stays silent.
    expect(c->doRebalanceForTest(), "an empty opening allocation is balanced");
    expectInt(static_cast<long long>(recorder->total()), 0, "and it is not a change worth a callback");
    const auto table = c->getRebalanceTable();
    expect(table.count(kTopic) == 1 && table.at(kTopic).empty(),
           "the table records the empty share this instance owns");
    c->shutdown();
}

void testMissingConsumerIdListIsBalancedAndSilent() {
    MockEndpoint ep;
    ep.addRoute(kTopic, routeN(3, kBroker, ep.address()));
    auto recorder = std::make_shared<RecordingListener>();
    auto c = started("reb_nocid", kGroup, ep, kTopic, MessageModel::CLUSTERING, nullptr, recorder);
    ep.setCidList({});

    // Java (:300-302) only warns; it does not notify and leaves balanced at true.
    expect(c->doRebalanceForTest(), "no consumer list -> the round still looks balanced");
    expectInt(static_cast<long long>(recorder->total()), 0, "no consumer list -> say nothing");
    expect(c->getRebalanceTable().empty(), "and keep the old (empty) allocation");
    c->shutdown();
}

void testVanishedTopicClearsTableAndNotifiesEmpty() {
    MockEndpoint ep;
    ep.addRoute(kTopic, routeN(2, kBroker, ep.address()));
    auto recorder = std::make_shared<RecordingListener>();
    auto c = started("reb_vanish", kGroup, ep, kTopic, MessageModel::CLUSTERING, nullptr, recorder);
    ep.setCidList({c->clientId()});

    c->doRebalanceForTest();
    expectInt(static_cast<long long>(recorder->count(kTopic)), 1, "the first round notifies once");
    expectInt(static_cast<long long>(c->getRebalanceTable().at(kTopic).size()), 2, "2 queues held");

    // A topic whose route no longer carries read queues: the same situation Java reports (:286-294).
    ep.addRoute(kTopic, routeN(0, kBroker, ep.address()));
    c->client().updateTopicRouteInfoFromNameServer(kTopic);
    // Java `break`s out with `balanced` still true (:276-294): an absent topic is not an
    // unbalanced round, or the loop would retry at minInterval every second.
    expect(c->doRebalanceForTest(), "the vanished topic is still a balanced round");
    const auto table = c->getRebalanceTable();
    expect(table.count(kTopic) == 1 && table.at(kTopic).empty(),
           "the table keeps the topic with an empty share");
    expectInt(static_cast<long long>(recorder->count(kTopic)), 2, "the revocation notifies once");
    const RecordingListener::Call call = recorder->last(kTopic);
    expect(call.all.empty() && call.divided.empty(), "both sides of the callback are empty sets");
    c->shutdown();
}

// ---------------------------------------------------------------- 5. listener safety / set equality

void testThrowingListenerDoesNotEscape() {
    MockEndpoint ep;
    ep.addRoute(kTopic, routeN(2, kBroker, ep.address()));
    auto c = started("reb_boom", kGroup, ep, kTopic, MessageModel::CLUSTERING, nullptr,
                     std::make_shared<BoomListener>());
    ep.setCidList({c->clientId()});

    // Java swallows it (RebalancePullImpl:49-51); the call must simply return.
    expect(!c->doRebalanceForTest(), "a throwing listener does not change the unbalanced answer");
    const auto table = c->getRebalanceTable();
    expect(table.count(kTopic) == 1 && table.at(kTopic).size() == 2,
           "the table is still updated after the listener threw");
    c->shutdown();
}

void testSameQueuesInAnotherOrderDoNotNotify() {
    MockEndpoint ep;
    ep.addRoute(kTopic, routeTwoBrokers(ep.address(), /*reverseOrder=*/false));
    auto recorder = std::make_shared<RecordingListener>();
    auto c = started("reb_order", kGroup, ep, kTopic, MessageModel::CLUSTERING, nullptr, recorder);
    ep.setCidList({c->clientId()});

    c->doRebalanceForTest();
    expectInt(static_cast<long long>(recorder->count(kTopic)), 1, "the first round notifies once");

    // Same three queues, arriving in another order: not a change.
    ep.addRoute(kTopic, routeTwoBrokers(ep.address(), /*reverseOrder=*/true));
    c->client().updateTopicRouteInfoFromNameServer(kTopic);
    expect(c->doRebalanceForTest(), "same set, different order is balanced");
    expectInt(static_cast<long long>(recorder->count(kTopic)), 1,
              "a re-ordering of the same queues is not a change");
    c->shutdown();
}

// ---------------------------------------------------------------- 6. fetchMessageQueuesInBalance uses the table

void testTableWinsOverOnTheSpotComputation() {
    MockEndpoint ep;
    ep.addRoute(kTopic, routeN(4, kBroker, ep.address()));
    auto c = started("reb_table_wins", kGroup, ep, kTopic);
    ep.setCidList({c->clientId(), "zzz-other"});

    // On the spot the instance would take [0,1]; the seeded table says only [3], and it is authoritative.
    c->setRebalanceTableForTest(kTopic, {MessageQueue(kTopic, kBroker, 3)});
    expect(queueIds(c->fetchMessageQueuesInBalance(kTopic)) == std::vector<int32_t>({3}),
           "the background rebalance's product is authoritative");
    c->shutdown();
}

void testTableAnswerIsSortedAndIsolatedFromTheCaller() {
    MockEndpoint ep;
    ep.addRoute(kTopic, routeN(4, kBroker, ep.address()));
    auto c = started("reb_table_copy", kGroup, ep, kTopic);
    ep.setCidList({c->clientId()});

    c->setRebalanceTableForTest(
        kTopic, {MessageQueue(kTopic, "broker-b", 1), MessageQueue(kTopic, "broker-a", 2)});
    std::vector<MessageQueue> got = c->fetchMessageQueuesInBalance(kTopic);
    expectInt(static_cast<long long>(got.size()), 2, "both seeded queues are returned");
    if (got.size() == 2) {
        expect(got[0].brokerName == "broker-a" && got[0].queueId == 2,
               "the table answer is sorted by topic/broker/queueId");
        expect(got[1].brokerName == "broker-b" && got[1].queueId == 1, "and keeps the other entry");
    }
    got.clear();
    expectInt(static_cast<long long>(c->getRebalanceTable().at(kTopic).size()), 2,
              "callers must not mutate the table");
    c->shutdown();
}

void testGetRebalanceTableReturnsACopy() {
    MockEndpoint ep;
    ep.addRoute(kTopic, routeN(2, kBroker, ep.address()));
    auto c = started("reb_table_snapshot", kGroup, ep, kTopic);
    ep.setCidList({c->clientId()});

    c->doRebalanceForTest();
    auto snapshot = c->getRebalanceTable();
    expect(snapshot.count(kTopic) == 1, "the round produced a table entry");
    snapshot[kTopic].clear();
    expectInt(static_cast<long long>(c->getRebalanceTable().at(kTopic).size()), 2,
              "the snapshot is a copy, not a view");
    c->shutdown();
}

void testUnfilledTopicStillComputesOnTheSpot() {
    MockEndpoint ep;
    ep.addRoute(kTopic, routeN(4, kBroker, ep.address()));
    auto c = started("reb_unfilled", kGroup, ep, kTopic);
    ep.setCidList({c->clientId(), "zzz-other"});

    expect(c->getRebalanceTable().empty(), "no round has run");
    expect(queueIds(c->fetchMessageQueuesInBalance(kTopic)) == std::vector<int32_t>({0, 1}),
           "an unfilled topic still computes the share on the spot");
    c->shutdown();
}

// ---------------------------------------------------------------- 7. registerMessageQueueListener

void testRegisteredTopicAndListenerAreUsedByTheRound() {
    MockEndpoint ep;
    ep.addRoute(kTopic, routeN(1, kBroker, ep.address()));
    auto perTopic = std::make_shared<RecordingListener>();
    auto c = started("reb_register", kGroup, ep, kTopic, MessageModel::CLUSTERING, perTopic);
    ep.setCidList({c->clientId()});

    expect(c->registerTopics().count(kTopic) == 1, "registerMessageQueueListener registers the topic");
    c->doRebalanceForTest();
    expectInt(static_cast<long long>(perTopic->count(kTopic)), 1,
              "the topic-keyed listener is the one the round tells");

    // Re-registering with a null listener keeps the current one (Java overwrites only when non-null).
    c->registerMessageQueueListener(kTopic, nullptr);
    ep.addRoute(kTopic, routeN(2, kBroker, ep.address()));
    c->client().updateTopicRouteInfoFromNameServer(kTopic);
    c->doRebalanceForTest();
    expectInt(static_cast<long long>(perTopic->count(kTopic)), 2,
              "a null listener leaves the previous listener in place");

    // Another topic registered by the combined call is driven on its own (Java stores one topic at a time).
    const std::string secondTopic = "TopicPullRebSecond";
    c->registerMessageQueueListener(secondTopic, perTopic);
    expect(c->registerTopics().count(secondTopic) == 1,
           "a second registerMessageQueueListener adds its own topic");
    c->shutdown();
}

// ---------------------------------------------------------------- 8. the resident thread

void testBackgroundThreadPopulatesTheTableAndStops() {
    MockEndpoint ep;
    ep.addRoute(kTopic, routeN(2, kBroker, ep.address()));

    auto recorder = std::make_shared<RecordingListener>();
    auto c = std::make_unique<DefaultMQPullConsumer>("PG_PullRebThread");
    c->setInstanceName("reb_thread");
    c->setNamesrvAddr(ep.address());
    c->setRebalanceWaitMillis(50);
    c->setRebalanceMinIntervalMillis(20);
    c->registerMessageQueueListener(kTopic, std::make_shared<NoopListener>());
    c->start();
    c->setMessageQueueListener(recorder);
    ep.setCidList({c->clientId()});

    expect(waitFor([&] { return c->getRebalanceTable().count(kTopic) == 1; }, 5000),
           "the resident thread fills the rebalance table without a manual round");
    expect(waitFor([&] { return recorder->count(kTopic) >= 1; }, 5000),
           "and reports the first non-empty share to the listener");
    const auto table = c->getRebalanceTable();
    if (table.count(kTopic) == 1) {
        expectInt(static_cast<long long>(table.at(kTopic).size()), 2,
                  "the thread's product is this instance's share");
    }
    c->shutdown();  // must return: the stop flag wakes the loop and join is capped
}

}  // namespace

int main() {
    testSwitchDefaultsAndClamps();
    testDisabledReportsBalancedAndNeverAllocates();
    testDisabledKeepsFetchOnTheSpot();
    testBroadcastingFirstRoundNotifiesWithAllQueues();
    testClusteringTakesItsShare();
    testTwoInstancesGetDisjointShares();
    testNewQueueTriggersExactlyOneNotification();
    testEmptyFirstAllocationIsBalancedAndSilent();
    testMissingConsumerIdListIsBalancedAndSilent();
    testVanishedTopicClearsTableAndNotifiesEmpty();
    testThrowingListenerDoesNotEscape();
    testSameQueuesInAnotherOrderDoNotNotify();
    testTableWinsOverOnTheSpotComputation();
    testTableAnswerIsSortedAndIsolatedFromTheCaller();
    testGetRebalanceTableReturnsACopy();
    testUnfilledTopicStillComputesOnTheSpot();
    testRegisteredTopicAndListenerAreUsedByTheRound();
    testBackgroundThreadPopulatesTheTableAndStops();
    std::printf("pull_rebalance: %d checks, %d failures\n", checks, fails);
    return fails == 0 ? 0 : 1;
}
