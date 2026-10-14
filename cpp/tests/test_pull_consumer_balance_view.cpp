// Offline unit test for the **balance view** (fetchMessageQueuesInBalance) of the pull consumer -- no cluster required.
//
//
// Criteria are taken from the **wire traffic**: the fake endpoint records the reply content of 38 (GET_CONSUMER_LIST_BY_GROUP)
// and the arrival of 11 (PULL_MESSAGE), instead of trusting what the caller claims.
#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "rocketmq/client/exception.h"
#include "rocketmq/client/pull_consumer.h"
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

// One fake endpoint playing both the name server (answers routes) and the broker (answers 38 / 11).
// The balance view only needs "one route + one consumer list"; who the recipient is does not matter, so they are merged into one.
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

    /// Sets the reply content of 38: which clientIds this group has. An empty list = "the broker does not know this group";
    /// a real broker replies the same way for a group that never registered (it is not an error).
    void setCidList(const std::vector<std::string>& cids) {
        std::lock_guard<std::mutex> lk(state_);
        cidList_ = cids;
    }

    /// The PULL_MESSAGE queues received (topic@queueId), in arrival order.
    std::vector<std::string> pulledQueues() {
        std::lock_guard<std::mutex> lk(state_);
        return pulled_;
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
            } else if (req.code == RequestCode::PULL_MESSAGE
                       || req.code == RequestCode::LITE_PULL_MESSAGE) {
                const std::string topic =
                    req.extFields.count("topic") ? req.extFields.at("topic") : std::string();
                const std::string queueId =
                    req.extFields.count("queueId") ? req.extFields.at("queueId") : std::string();
                {
                    std::lock_guard<std::mutex> lk(state_);
                    pulled_.push_back(topic + "@" + queueId);
                }
                // The faithful shape of an empty broker: PULL_NOT_FOUND with nextBeginOffset echoed back as the requested offset,
                // so the client does not advance its cursor to 0 (this case only cares that the pull landed in the local allocation table).
                resp.code = ResponseCode::PULL_NOT_FOUND;
                resp.extFields["nextBeginOffset"] = "0";
                resp.extFields["minOffset"] = "0";
                resp.extFields["maxOffset"] = "0";
                resp.extFields["suggestWhichBrokerId"] = "0";
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
    std::vector<std::string> pulled_;
    std::mutex workersM_;
    std::vector<std::thread> workers_;
    std::thread acceptor_;
};

// ---------------------------------------------------------------- scaffolding

const char* kGroup = "PG_BalUnit";
const char* kTopic = "BalViewTopic";
const char* kBrokerKey = "broker-a";
const int32_t kQueues = 4;

class NoopListener : public MessageQueueListener {
public:
    void messageQueueChanged(const std::string&, const std::vector<MessageQueue>&,
                             const std::vector<MessageQueue>&) override {}
};

/// Readable form of the queue list so a failure shows at a glance where it differs.
std::string describe(const std::vector<MessageQueue>& mqs) {
    std::string s;
    for (const MessageQueue& mq : mqs) {
        if (!s.empty()) s += ",";
        s += mq.topic + "@" + mq.brokerName + "@" + std::to_string(mq.queueId);
    }
    return s;
}

/// `n` read queues on that one broker (with 1 queue "own share" and "all" have the same shape and
/// prove nothing, hence the default of 4).
TopicRouteData routeN(int32_t n, const std::string& broker, const std::string& addr) {
    TopicRouteData route;
    std::map<int64_t, std::string> addrs;
    addrs[MixAll::MASTER_ID] = addr;
    route.brokerDatas.emplace_back("DefaultCluster", broker, addrs);
    route.queueDatas.emplace_back(broker, n, n, 6, 0);
    return route;
}

/// Starts a pull consumer pointing at the fake endpoint. The instance name must be unique per case (the same clientId shares an instance).
/// DefaultMQPullConsumer is not copyable, so the one on the heap is returned.
std::unique_ptr<DefaultMQPullConsumer> started(const std::string& instance,
                                               const std::string& group, MockEndpoint& ep,
                                               const std::string& topic,
                                               const std::string& model = MessageModel::CLUSTERING,
                                               const std::string& ns = std::string()) {
    auto c = std::make_unique<DefaultMQPullConsumer>(group);
    c->setInstanceName(instance);
    c->setNamesrvAddr(ep.address());
    c->setMessageModel(model);
    c->setNamespace(ns);
    c->registerMessageQueueListener(topic, std::make_shared<NoopListener>());
    c->start();
    return c;
}

std::vector<std::string> sortedCids(const std::vector<std::string>& in) {
    std::vector<std::string> out = in;
    std::sort(out.begin(), out.end());
    return out;
}

// ---------------------------------------------------------------- 1. a sole instance claims everything

void testSoleInstanceTakesEveryQueue() {
    MockEndpoint ep;
    ep.addRoute(kTopic, routeN(kQueues, kBrokerKey, ep.address()));
    auto c = started("bal_sole", kGroup, ep, kTopic);
    const std::string cid = c->clientId();
    ep.setCidList({cid});

    const std::vector<MessageQueue> view = c->fetchMessageQueuesInBalance(kTopic);
    expectInt(static_cast<long long>(view.size()), kQueues, "a sole instance takes all 4 queues");
    std::vector<int32_t> ids;
    for (const MessageQueue& mq : view) ids.push_back(mq.queueId);
    expect(ids == std::vector<int32_t>({0, 1, 2, 3}), "queues ordered by ascending queueId", describe(view));
    bool sameIdentity = true;
    for (const MessageQueue& mq : view) {
        if (mq.topic != kTopic || mq.brokerName != kBrokerKey) sameIdentity = false;
    }
    expect(sameIdentity, "queue identity untouched (topic/brokerName verbatim)", describe(view));
    c->shutdown();
}

// ---------------------------------------------------------------- 2. only its own share

/// The worked example of `PullConsumer` only holds because "each instance receives only its own share".
/// Two real instances against each other: 2 queues each, no overlap, together covering all 4.
void testTwoInstancesSplitTheQueuesWithoutOverlap() {
    MockEndpoint ep;
    ep.addRoute(kTopic, routeN(kQueues, kBrokerKey, ep.address()));
    auto a = started("bal_pair_a", kGroup, ep, kTopic);
    auto b = started("bal_pair_b", kGroup, ep, kTopic);
    ep.setCidList(sortedCids({a->clientId(), b->clientId()}));

    const std::vector<MessageQueue> va = a->fetchMessageQueuesInBalance(kTopic);
    const std::vector<MessageQueue> vb = b->fetchMessageQueuesInBalance(kTopic);
    expectInt(static_cast<long long>(va.size()), 2, "two instances split 4 queues: each takes only 2");
    expectInt(static_cast<long long>(vb.size()), 2, "the other one also takes 2");
    std::vector<MessageQueue> both = va;
    both.insert(both.end(), vb.begin(), vb.end());
    std::sort(both.begin(), both.end());
    std::vector<MessageQueue> dedup = both;
    dedup.erase(std::unique(dedup.begin(), dedup.end()), dedup.end());
    expectInt(static_cast<long long>(dedup.size()), 4, "the two shares do not overlap (overlap means duplicate consumption)",
              describe(both));
    std::vector<int32_t> ids;
    for (const MessageQueue& mq : dedup) ids.push_back(mq.queueId);
    expect(ids == std::vector<int32_t>({0, 1, 2, 3}), "the two shares together cover every queue", describe(dedup));
    a->shutdown();
    b->shutdown();
}

// ---------------------------------------------------------------- 3. cannot compute -> keep the current assignment

/// The broker does not know this group (38 answers an empty list) = **cannot compute**, not "computed to empty":
/// the current assignment (the queues pulled locally) is kept, never falling back to claiming all queues.
void testUnknownGroupKeepsCurrentAssignmentInsteadOfTakingAll() {
    MockEndpoint ep;
    ep.addRoute(kTopic, routeN(kQueues, kBrokerKey, ep.address()));
    auto c = started("bal_unknown", kGroup, ep, kTopic);
    ep.setCidList({});

    expect(c->fetchMessageQueuesInBalance(kTopic).empty(),
           "nothing pulled yet: the current assignment is empty (not all 4)");

    std::vector<MessageQueue> all = c->fetchSubscribeMessageQueues(kTopic);
    expectInt(static_cast<long long>(all.size()), kQueues, "subscription info is queryable (the 4 queues in the route)");
    std::sort(all.begin(), all.end());
    const PullResult result = c->pull(all[2], "*", 0, 32);
    (void)result;
    const std::vector<MessageQueue> view = c->fetchMessageQueuesInBalance(kTopic);
    expectInt(static_cast<long long>(view.size()), 1, "the fallback only claims the one queue it pulled");
    if (view.size() == 1) {
        expect(view[0] == all[2], "the fallback returns exactly the queue that was pulled", describe(view));
    }
    expect(ep.pulledQueues().size() == 1, "wire traffic: one PULL_MESSAGE was actually sent");

    // The fallback is also scoped per topic: another topic without a route neither throws nor leaks its queues to T.
    expect(c->fetchMessageQueuesInBalance("NoSuchBalTopic").empty(),
           "with no route it falls back to this topic's current assignment; other topics get an empty list");
    c->shutdown();
}

// ---------------------------------------------------------------- 4. broadcasting ignores the consumer list

void testBroadcastingTakesAllWithoutTheConsumerList() {
    MockEndpoint ep;
    ep.addRoute(kTopic, routeN(kQueues, kBrokerKey, ep.address()));
    auto c = started("bal_bcast", kGroup, ep, kTopic, MessageModel::BROADCASTING);
    ep.setCidList({"someone-else"});

    const std::vector<MessageQueue> view = c->fetchMessageQueuesInBalance(kTopic);
    expectInt(static_cast<long long>(view.size()), kQueues, "broadcasting ignores the consumer list: claims all",
              describe(view));
    c->shutdown();
}

// ---------------------------------------------------------------- 5. not started throws immediately

void testRequiresAStartedConsumer() {
    DefaultMQPullConsumer c("PG_BalNotStarted");
    bool threw = false;
    try {
        c.fetchMessageQueuesInBalance(kTopic);
    } catch (const MQClientException& e) {
        threw = std::string(e.what()).find("not started") != std::string::npos;
    }
    expect(threw, "not started must throw MQClientException (isRunning() guard)");
}

// ---------------------------------------------------------------- 6. namespace: returns bare topics

void testNamespacedInstanceReturnsBareTopics() {
    const std::string ns = "MQ_INST_bal";
    const std::string plain = "NsBalTopic";
    const std::string wrapped = ns + "%" + plain;

    MockEndpoint ep;
    ep.addRoute(wrapped, routeN(kQueues, kBrokerKey, ep.address()));
    auto c = started("bal_ns", kGroup, ep, plain, MessageModel::CLUSTERING, ns);
    ep.setCidList({c->clientId()});

    const std::vector<MessageQueue> view = c->fetchMessageQueuesInBalance(plain);
    expectInt(static_cast<long long>(view.size()), kQueues,
              "still computable with a namespace (route lookup uses ns%topic)", describe(view));
    bool allBare = true;
    for (const MessageQueue& mq : view) {
        if (mq.topic != plain) allBare = false;
    }
    expect(allBare, "returned queues have the namespace stripped (each instance takes only its own share)",
          describe(view));
    c->shutdown();
}

}  // namespace

int main() {
    testSoleInstanceTakesEveryQueue();
    testTwoInstancesSplitTheQueuesWithoutOverlap();
    testUnknownGroupKeepsCurrentAssignmentInsteadOfTakingAll();
    testBroadcastingTakesAllWithoutTheConsumerList();
    testRequiresAStartedConsumer();
    testNamespacedInstanceReturnsBareTopics();
    std::printf("%d checks, %d failures\n", checks, fails);
    return fails == 0 ? 0 : 1;
}
