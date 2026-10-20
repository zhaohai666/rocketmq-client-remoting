// Offline unit tests for the LitePull consumer's topic queue-set change listener
// (registerTopicMessageQueueChangeListener) -- no cluster needed.
//
// What is pinned here is this client's own contract; every judgement is taken from the wire:
//   * the callback fires once only when the **queue set** really changed against the previous snapshot
//     (an unchanged count with different content still counts); registering before start = no snapshot, so
//     the first background round must call back once with the current set, while a mid-run registration
//     records a snapshot at once so the current state is never reported again as a "change".
//   * every comparison round **re-queries the name server** instead of eating the route cache, else a
//     scale-up would take one full polling cycle to become visible.
//   * when no queues are found it **throws** rather than returning an empty list -- "not found" != "this
//     topic shrank to 0 queues"; the latter would hand the listener a fake scale-down callback and flush
//     the snapshot to the empty set.
//   * a route failure for one topic only skips it (the round keeps comparing the others), re-registering
//     the same topic overwrites the old listener, and each round really re-queries GET_ROUTEINFO_BY_TOPIC.
// The background thread delays its first check by 10s and the tests do not wait: they drive fetchTopicMessageQueuesAndCompare directly (public exactly for this).
#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include "rocketmq/client/exception.h"
#include "rocketmq/client/lite_pull_consumer.h"
#include "rocketmq/common/mix_all.h"
#include "rocketmq/common/net_compat.h"
#include "rocketmq/common/util_all.h"
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

std::string idsOf(const std::vector<MessageQueue>& mqs) {
    std::vector<int32_t> ids;
    for (const MessageQueue& mq : mqs) ids.push_back(mq.queueId);
    std::sort(ids.begin(), ids.end());
    std::string s;
    for (int32_t id : ids) {
        if (!s.empty()) s += ",";
        s += std::to_string(id);
    }
    return s;
}

std::string joinTopics(const std::vector<std::string>& topics) {
    std::string s;
    for (const std::string& t : topics) {
        if (!s.empty()) s += " ";
        s += t;
    }
    return s;
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

// ---------------------------------------------------------------- fake name server
//
// The route is **built per request**: after the test calls setQueueNums() mid-run, the next route
// request already reports the new queue count, so "set changed -> callback" is really observed.
class MockNameServer {
public:
    MockNameServer() {
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

    ~MockNameServer() {
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

    MockNameServer(const MockNameServer&) = delete;
    MockNameServer& operator=(const MockNameServer&) = delete;

    std::string address() const { return "127.0.0.1:" + std::to_string(port_); }

    void setQueueNums(const std::string& topic, int32_t n) {
        std::lock_guard<std::mutex> lk(state_);
        queueNums_[topic] = n;
    }

    /// topic present in the map with 0 queues: the route is found but the queue data is empty, i.e.
    /// "shrunk to 0 queues". Absent from the map means "no route for this topic at all".
    void dropRoute(const std::string& topic) {
        std::lock_guard<std::mutex> lk(state_);
        queueNums_.erase(topic);
    }

    int routeRequests() {
        std::lock_guard<std::mutex> lk(state_);
        return routeReqs_;
    }

    /// The route-query topics seen so far (in arrival order, duplicates included).
    std::vector<std::string> requestedTopics() {
        std::lock_guard<std::mutex> lk(state_);
        return requested_;
    }

    void clearRequests() {
        std::lock_guard<std::mutex> lk(state_);
        routeReqs_ = 0;
        requested_.clear();
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
                ++routeReqs_;
                requested_.push_back(topic);
                auto it = queueNums_.find(topic);
                if (it == queueNums_.end()) {
                    resp.code = ResponseCode::TOPIC_NOT_EXIST;
                } else {
                    resp.code = ResponseCode::SUCCESS;
                    resp.body = buildRoute(topic, it->second).encode();
                    resp.hasBody = true;
                }
            } else {
                resp.code = ResponseCode::SUCCESS;
            }
            Bytes out = resp.encode();
            if (!writeAll(sock, out)) break;
        }
        ::shutdown(sock, kShutdownHow);
        netcompat::closeSocket(sock);
    }

    TopicRouteData buildRoute(const std::string& topic, int32_t n) const {
        (void)topic;
        TopicRouteData route;
        std::map<int64_t, std::string> addrs;
        addrs[MixAll::MASTER_ID] = address();
        route.brokerDatas.emplace_back("DefaultCluster", kBroker, addrs);
        route.queueDatas.emplace_back(kBroker, n, n, 6, 0);
        return route;
    }

    static constexpr const char* kBroker = "broker-a";

    socket_t listen_ = kInvalidSocket;
    uint16_t port_ = 0;
    std::atomic<bool> running_{false};
    std::mutex state_;
    std::map<std::string, int32_t> queueNums_;
    int routeReqs_ = 0;
    std::vector<std::string> requested_;
    std::mutex workersM_;
    std::vector<std::thread> workers_;
    std::thread acceptor_;
};

// ---------------------------------------------------------------- recording listener

class Recorder : public TopicMessageQueueChangeListener {
public:
    void onChanged(const std::string& topic, const std::vector<MessageQueue>& queues) override {
        std::lock_guard<std::mutex> lk(m_);
        events_.emplace_back(topic, idsOf(queues));
    }

    std::vector<std::pair<std::string, std::string>> events() {
        std::lock_guard<std::mutex> lk(m_);
        return events_;
    }

    size_t count() {
        std::lock_guard<std::mutex> lk(m_);
        return events_.size();
    }

private:
    std::mutex m_;
    std::vector<std::pair<std::string, std::string>> events_;
};

// ---------------------------------------------------------------- scaffolding

const char* kGroup = "PG_LiteMqUnit";
const char* kTopic = "LiteMqChangeTopic";
// A name sorting before kTopic (the comparison iterates topics in name order) is what gives "does a bad
// topic break the round" its evidence: the bad one is handled first, so an exception escaping the loop body would leave the later healthy topic without a callback.
const char* kGhost = "ALiteMqGhost";

/// Start a lite consumer pointed at the fake name server: subscribe is a precondition for start and
/// doubles as registering the topic in the instance's route-in-use table.
std::unique_ptr<DefaultLitePullConsumer> started(const std::string& instance,
                                                 const MockNameServer& ns,
                                                 const std::string& topic,
                                                 const std::string& nameSpace = std::string()) {
    auto c = std::make_unique<DefaultLitePullConsumer>(kGroup);
    c->setInstanceName(instance);
    c->setNamesrvAddr(ns.address());
    c->setNamespace(nameSpace);
    c->subscribe(topic, "*");
    c->start();
    return c;
}

// ---------------------------------------------------------------- 1. Input guards and interval

void testGuardsAndInterval() {
    DefaultLitePullConsumer c(kGroup);
    bool topicGuard = false;
    bool listenerGuard = false;
    try {
        c.registerTopicMessageQueueChangeListener("", std::make_shared<Recorder>());
    } catch (const MQClientException& e) {
        topicGuard = std::string(e.what()).find("Topic or listener is null") != std::string::npos;
    }
    try {
        c.registerTopicMessageQueueChangeListener(kTopic, nullptr);
    } catch (const MQClientException& e) {
        listenerGuard = std::string(e.what()).find("Topic or listener is null") != std::string::npos;
    }
    expect(topicGuard, "empty topic is rejected");
    expect(listenerGuard, "an empty listener is rejected");
    expectInt(c.topicMetadataCheckIntervalMillis(), 30000, "the default comparison interval is 30s");
    c.setTopicMetadataCheckIntervalMillis(0);
    expectInt(c.topicMetadataCheckIntervalMillis(), 1000, "the interval floor is clamped to 1s (0 would make the background round never advance)");
    c.setTopicMetadataCheckIntervalMillis(5000);
    expectInt(c.topicMetadataCheckIntervalMillis(), 5000, "the interval can be enlarged");
}

// ---------------------------------------------------------------- 2. Registration before start

void testRegistrationBeforeStartFiresOnceThenQuiets() {
    MockNameServer ns;
    ns.setQueueNums(kTopic, 2);
    auto rec = std::make_shared<Recorder>();
    DefaultLitePullConsumer c(kGroup);
    c.setInstanceName("lite_mq_pre");
    c.setNamesrvAddr(ns.address());
    // Registration happens before start(): no snapshot can be taken yet (queues are not queryable), so the first round must call back once.
    c.subscribe(kTopic, "*");
    c.registerTopicMessageQueueChangeListener(kTopic, rec);
    c.start();
    // With no snapshot the first round hands out the "current set" as a change -- the caller thereby
    // gets the initial state and only increments from then on.
    c.fetchTopicMessageQueuesAndCompare();
    expect(rec->count() == 1, "the first round after start calls back once", std::to_string(rec->count()));
    const auto ev = rec->events();
    if (!ev.empty()) expect(ev[0].second == "0,1", "the first callback carries the current queue set", ev[0].second);
    c.fetchTopicMessageQueuesAndCompare();
    expect(rec->count() == 1, "no further callback when the set did not change");
    c.shutdown();
}

// ---------------------------------------------------------------- 3. Mid-run registration + scale up/down

void testRunningRegistrationSnapshotsAndTracksScale() {
    MockNameServer ns;
    ns.setQueueNums(kTopic, 1);
    auto c = started("lite_mq_running", ns, kTopic);
    auto rec = std::make_shared<Recorder>();
    // Mid-run registration: a snapshot is recorded at once, so "what already exists now" must not be reported as a change.
    c->registerTopicMessageQueueChangeListener(kTopic, rec);
    c->fetchTopicMessageQueuesAndCompare();
    expect(rec->count() == 0, "the first round after a mid-run registration does not misfire", std::to_string(rec->count()));

    ns.setQueueNums(kTopic, 3);
    c->fetchTopicMessageQueuesAndCompare();
    auto ev = rec->events();
    expectInt(static_cast<long long>(ev.size()), 1, "the scale-up was observed");
    if (ev.size() == 1) expect(ev[0].second == "0,1,2", "the callback carries the scaled-up set", ev[0].second);
    c->fetchTopicMessageQueuesAndCompare();
    expect(rec->count() == 1, "the same set is not reported twice");

    ns.setQueueNums(kTopic, 2);
    c->fetchTopicMessageQueuesAndCompare();
    ev = rec->events();
    expectInt(static_cast<long long>(ev.size()), 2, "the scale-down was observed too");
    if (ev.size() == 2) expect(ev[1].second == "0,1", "the callback carries the scaled-down set", ev[1].second);
    c->shutdown();
}

// ---------------------------------------------------------------- 4. Every round re-queries the name server

void testEachRoundRequeriesTheNameserver() {
    MockNameServer ns;
    ns.setQueueNums(kTopic, 2);
    auto c = started("lite_mq_fresh", ns, kTopic);
    c->registerTopicMessageQueueChangeListener(kTopic, std::make_shared<Recorder>());
    ns.clearRequests();
    c->fetchTopicMessageQueuesAndCompare();
    const int afterFirst = ns.routeRequests();
    c->fetchTopicMessageQueuesAndCompare();
    expect(afterFirst >= 1, "the comparison round queries the route", std::to_string(afterFirst));
    expect(ns.routeRequests() > afterFirst, "the next round asks again (no route cache, otherwise a scale-up would lag one cycle)",
           std::to_string(afterFirst) + "->" + std::to_string(ns.routeRequests()));
    c->shutdown();
}

// ---------------------------------------------------------------- 5. One failing topic must not starve others

void testFailingTopicDoesNotBlockOthers() {
    MockNameServer ns;
    ns.setQueueNums(kTopic, 2);
    auto good = std::make_shared<Recorder>();
    auto bad = std::make_shared<Recorder>();
    auto c = started("lite_mq_isolate", ns, kTopic);
    c->registerTopicMessageQueueChangeListener(kTopic, good);
    c->registerTopicMessageQueueChangeListener(kGhost, bad);
    ns.setQueueNums(kTopic, 3);
    // Ghost has no route: fetching its queues throws, this round skips only it, and later topics still compare.
    c->fetchTopicMessageQueuesAndCompare();
    expect(bad->count() == 0, "a topic whose route cannot be found gets no callback (and never an empty list faking a scale-down)");
    expectInt(static_cast<long long>(good->count()), 1, "the other topics of the same round proceed normally",
              std::to_string(good->count()));
    const auto ev = good->events();
    if (!ev.empty()) expect(ev[0].second == "0,1,2", "the healthy topic receives the new set", ev[0].second);
    c->shutdown();
}

// ---------------------------------------------------------------- 6. Re-registration overwrites the old one

void testReRegisterOverwritesOldListener() {
    MockNameServer ns;
    ns.setQueueNums(kTopic, 1);
    auto c = started("lite_mq_overwrite", ns, kTopic);
    auto first = std::make_shared<Recorder>();
    auto second = std::make_shared<Recorder>();
    c->registerTopicMessageQueueChangeListener(kTopic, first);
    c->registerTopicMessageQueueChangeListener(kTopic, second);
    ns.setQueueNums(kTopic, 2);
    c->fetchTopicMessageQueuesAndCompare();
    expect(first->count() == 0, "the old listener receives no more callbacks", std::to_string(first->count()));
    expect(second->count() == 1, "only the new listener is called back", std::to_string(second->count()));
    c->shutdown();
}

// ---------------------------------------------------------------- 7. With a namespace

// Internally this client keys everything on the namespace-wrapped topic: registration uses the bare
// name, the callback receives the wrapped key, and every round asks the route with it (the fake endpoint answers the route only for ns%topic).
void testNamespacedKeyIsUsedThroughout() {
    const std::string nameSpace = "MQ_INST_lite";
    const std::string plain = "LiteMqNsTopic";
    const std::string wrapped = nameSpace + "%" + plain;

    MockNameServer ns;
    ns.setQueueNums(wrapped, 1);
    auto rec = std::make_shared<Recorder>();
    auto c = started("lite_mq_ns", ns, plain, nameSpace);
    c->registerTopicMessageQueueChangeListener(plain, rec);
    ns.clearRequests();
    ns.setQueueNums(wrapped, 2);
    c->fetchTopicMessageQueuesAndCompare();
    const auto ev = rec->events();
    expectInt(static_cast<long long>(ev.size()), 1, "a change is still detected with a namespace",
              std::to_string(ev.size()));
    if (!ev.empty()) {
        expect(ev[0].first == wrapped, "the callback topic key is the namespace-wrapped one", ev[0].first);
        expect(ev[0].second == "0,1", "the callback carries the scaled-up set", ev[0].second);
    }
    std::vector<std::string> asked = ns.requestedTopics();
    expect(std::find(asked.begin(), asked.end(), wrapped) != asked.end(),
          "each comparison round asks the route with the namespace-wrapped key", joinTopics(asked));

    // Wrapping must be idempotent: registering the already-wrapped key overwrites the same entry and
    // must not create a second ns%ns%topic listener (such a listener could never resolve a route).
    c->registerTopicMessageQueueChangeListener(wrapped, rec);
    ns.clearRequests();
    ns.setQueueNums(wrapped, 3);
    c->fetchTopicMessageQueuesAndCompare();
    expectInt(static_cast<long long>(rec->count()), 2, "re-registering the same (already wrapped) key does not add a second listener",
              std::to_string(rec->count()));
    asked = ns.requestedTopics();
    bool doubleWrapped = false;
    for (const std::string& t : asked) {
        if (t == nameSpace + "%" + wrapped) doubleWrapped = true;
    }
    expect(!doubleWrapped, "never a double-wrapped query", joinTopics(asked));
    c->shutdown();
}

// ---------------------------------------------------------------- 8. Fetching queues without start

void testFetchQueuesRequiresStartedConsumer() {
    DefaultLitePullConsumer c("PG_LiteMqNotStarted");
    bool threw = false;
    try {
        c.fetchMessageQueues(kTopic);
    } catch (const MQClientException& e) {
        threw = std::string(e.what()).find("not started") != std::string::npos;
    }
    expect(threw, "fetching queues before start throws (an empty list would read as 'no queues')");
}

}  // namespace

int main() {
    testGuardsAndInterval();
    testRegistrationBeforeStartFiresOnceThenQuiets();
    testRunningRegistrationSnapshotsAndTracksScale();
    testEachRoundRequeriesTheNameserver();
    testFailingTopicDoesNotBlockOthers();
    testReRegisterOverwritesOldListener();
    testNamespacedKeyIsUsedThroughout();
    testFetchQueuesRequiresStartedConsumer();
    std::printf("%d checks, %d failures\n", checks, fails);
    return fails == 0 ? 0 : 1;
}
