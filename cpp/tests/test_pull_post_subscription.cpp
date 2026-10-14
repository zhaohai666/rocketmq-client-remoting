// P5: offline unit tests for postSubscriptionWhenPull + pullFromWhichNode / findBrokerAddressInSubscribe.
//
//
// The wire shape (the SUBSCRIPTION bit, whether the `subscription` key goes on the wire, master vs slave targeting,
// whether COMMIT_OFFSET is cleared) is evidenced from the socket via in-process fake endpoints, not from the caller's own account.
#include <atomic>
#include <cstdint>
#include <cstdio>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "rocketmq/client/consumer.h"
#include "rocketmq/client/exception.h"
#include "rocketmq/client/mq_client.h"
#include "rocketmq/client/pull_consumer.h"
#include "rocketmq/client/result.h"
#include "rocketmq/common/mix_all.h"
#include "rocketmq/common/net_compat.h"
#include "rocketmq/common/subscription_data.h"
#include "rocketmq/common/sysflag.h"
#include "rocketmq/remoting/exception.h"
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

void expectInt(long long actual, long long expected, const std::string& name) {
    ++checks;
    if (actual != expected) {
        std::printf("FAIL %s (actual=%lld expected=%lld)\n", name.c_str(), actual, expected);
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

// ---------------------------------------------------------------- fake endpoints (name server + broker)

// forensics of one pull request: extFields exactly as they went on the wire
struct PullRecord {
    PropertyMap ext;
};

// Plays both the name server (answers routes) and the broker (answers PULL_MESSAGE).
// Same socket skeleton as MockBroker in test_check_client_config.cpp, except the focus here is pulling.
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

    // script the PULL_MESSAGE reply header (nextBeginOffset / maxOffset / suggestWhichBrokerId...)
    void scriptPullResponse(const PropertyMap& ext) {
        std::lock_guard<std::mutex> lk(state_);
        pullReply_ = ext;
        pulls_.clear();
    }

    size_t pullCount() {
        std::lock_guard<std::mutex> lk(state_);
        return pulls_.size();
    }

    PropertyMap pullExt(size_t i) {
        std::lock_guard<std::mutex> lk(state_);
        if (i >= pulls_.size()) return PropertyMap();
        return pulls_[i].ext;
    }

    bool pullHasKey(size_t i, const std::string& key) {
        PropertyMap ext = pullExt(i);
        return ext.find(key) != ext.end();
    }

    static int64_t extInt(const PropertyMap& ext, const std::string& key, int64_t fallback = -1) {
        auto it = ext.find(key);
        if (it == ext.end()) return fallback;
        try {
            return std::stoll(it->second);
        } catch (...) {
            return fallback;
        }
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
            } else if (req.code == RequestCode::PULL_MESSAGE) {
                PropertyMap reply;
                {
                    std::lock_guard<std::mutex> lk(state_);
                    pulls_.push_back(PullRecord{req.extFields});
                    reply = pullReply_;
                }
                resp.code = ResponseCode::SUCCESS;
                resp.extFields = reply;
            } else {
                // heartbeats and the like must not bother the test
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
    PropertyMap pullReply_;
    std::vector<PullRecord> pulls_;
    std::mutex workersM_;
    std::vector<std::thread> workers_;
    std::thread acceptor_;
};

// ---------------------------------------------------------------- scaffolding

const char* kGroup = "GID_P5Unit";
const char* kTopic = "P5PullTopic";
const char* kBroker = "broker-a";
const char* kClientId = "127.0.0.1@5555#1";

TopicRouteData routeWithMasterAndSlave(const std::string& masterAddr, const std::string& slaveAddr) {
    TopicRouteData route;
    std::map<int64_t, std::string> addrs;
    addrs[MixAll::MASTER_ID] = masterAddr;
    addrs[MixAll::MASTER_ID + 1] = slaveAddr;
    route.brokerDatas.emplace_back("DefaultCluster", kBroker, addrs);
    return route;
}

TopicRouteData routeWithMasterOnly(const std::string& masterAddr) {
    TopicRouteData route;
    std::map<int64_t, std::string> addrs;
    addrs[MixAll::MASTER_ID] = masterAddr;
    route.brokerDatas.emplace_back("DefaultCluster", kBroker, addrs);
    return route;
}

PropertyMap pullRespExt(bool withSuggest, int64_t suggest, int64_t nextOffset = 1) {
    PropertyMap ext;
    ext["nextBeginOffset"] = std::to_string(nextOffset);
    ext["minOffset"] = "0";
    ext["maxOffset"] = "10";
    if (withSuggest) ext["suggestWhichBrokerId"] = std::to_string(suggest);
    return ext;
}

// start an instance and seed kTopic's route into the cache (a real RPC round-trip to the mock name server)
std::unique_ptr<MQClientInstance> seeded(MockEndpoint& namesrv) {
    auto instance = std::make_unique<MQClientInstance>(
        kClientId, std::vector<std::string>{namesrv.address()});
    instance->updateTopicRouteInfoFromNameServer(kTopic);
    return instance;
}

// ---------------------------------------------------------------- 1. findBrokerAddressInSubscribe

void testFindBrokerAddressInSubscribeBranches() {
    std::map<int64_t, std::string> addrs;
    addrs[0] = "127.0.0.1:10911";
    addrs[1] = "127.0.0.1:10921";
    addrs[2] = "127.0.0.1:10931";

    auto hit = MQClientInstance::findBrokerAddressInSubscribe(addrs, 0);
    expect(hit.first == "127.0.0.1:10911" && !hit.second, "master hit: address taken directly, isSlave=false");
    hit = MQClientInstance::findBrokerAddressInSubscribe(addrs, 1);
    expect(hit.first == "127.0.0.1:10921" && hit.second, "slave id=1 hit: isSlave=true");

    std::map<int64_t, std::string> sparse;
    sparse[0] = "127.0.0.1:10911";
    sparse[2] = "127.0.0.1:10931";
    hit = MQClientInstance::findBrokerAddressInSubscribe(sparse, 1);
    expect(hit.first == "127.0.0.1:10931" && hit.second, "missing slave id falls back to trying id+1");

    hit = MQClientInstance::findBrokerAddressInSubscribe(addrs, 9);
    expect(hit.first == "127.0.0.1:10911" && !hit.second, "out-of-range brokerId falls back to the smallest id (master)");
    std::map<int64_t, std::string> onlySlave;
    onlySlave[1] = "127.0.0.1:10921";
    hit = MQClientInstance::findBrokerAddressInSubscribe(onlySlave, 3);
    expect(hit.first == "127.0.0.1:10921" && hit.second, "with only a slave present, fall back to it and mark isSlave");

    // onlyThisBroker: no substitution rather than a wrong one
    hit = MQClientInstance::findBrokerAddressInSubscribe(addrs, 9, /*onlyThisBroker=*/true);
    expect(hit.first.empty() && !hit.second, "no fallback when onlyThisBroker");
    hit = MQClientInstance::findBrokerAddressInSubscribe({}, 0);
    expect(hit.first.empty() && !hit.second, "an empty address table returns empty");
}

// ---------------------------------------------------------------- 2. wire request: choosing the slave

void testPullToSlaveClearsCommitOffsetAndOmitsSubscription() {
    MockEndpoint namesrv;
    MockEndpoint master;
    MockEndpoint slave;
    namesrv.addRoute(kTopic, routeWithMasterAndSlave(master.address(), slave.address()));
    slave.scriptPullResponse(pullRespExt(/*withSuggest=*/true, 0));

    auto instance = seeded(namesrv);
    MessageQueue mq(kTopic, kBroker, 0);
    const int32_t sysFlag = PullSysFlag::buildSysFlag(/*commitOffset=*/true, /*suspend=*/true,
                                                      /*subscription=*/false,
                                                      /*classFilter=*/false);
    expect(PullSysFlag::hasCommitOffsetFlag(sysFlag), "precondition: sysFlag carries COMMIT_OFFSET");

    PullResult result = instance->pullMessage(
        kGroup, mq, 0, 32, sysFlag, /*commitOffset=*/0, "TagA", 0, ExpressionType::TAG,
        /*timeoutMillis=*/3000, /*maxMsgBytes=*/-1, /*suspendTimeoutMillis=*/15000,
        /*addr=*/std::string(), /*requestSource=*/0, /*brokerId=*/MixAll::MASTER_ID + 1);

    expectInt(static_cast<long long>(slave.pullCount()), 1, "the pull reached the slave");
    expectInt(static_cast<long long>(master.pullCount()), 0, "master received nothing");

    PropertyMap sent = slave.pullExt(0);
    const int64_t sentFlag = MockEndpoint::extInt(sent, "sysFlag");
    expect(!PullSysFlag::hasCommitOffsetFlag(static_cast<int32_t>(sentFlag)),
           "COMMIT_OFFSET is meaningless on a slave");
    expect(PullSysFlag::hasSuspendFlag(static_cast<int32_t>(sentFlag)), "the suspend bit is kept");
    expect(!slave.pullHasKey(0, "subscription"),
           "with the SUBSCRIPTION bit off the subscription key never enters extFields");
    expect(result.suggestWhichBrokerId.has_value() && *result.suggestWhichBrokerId == 0,
           "the reply header suggestWhichBrokerId passes through to the caller");
}

// ---------------------------------------------------------------- 3. wire request: choosing the master

// brokerId=0 (master): hits master, COMMIT_OFFSET is kept; `subscription` goes on the wire only
// when the SUBSCRIPTION bit is set; an old broker without suggestWhichBrokerId passes through nothing.
void testPullToMasterKeepsCommitOffsetAndPostsSubscription() {
    MockEndpoint namesrv;
    MockEndpoint master;
    MockEndpoint slave;
    namesrv.addRoute(kTopic, routeWithMasterAndSlave(master.address(), slave.address()));
    master.scriptPullResponse(pullRespExt(/*withSuggest=*/false, 0, /*nextOffset=*/7));

    auto instance = seeded(namesrv);
    MessageQueue mq(kTopic, kBroker, 0);
    const int32_t sysFlag = PullSysFlag::buildSysFlag(/*commitOffset=*/true, /*suspend=*/true,
                                                      /*subscription=*/true,
                                                      /*classFilter=*/false);

    PullResult result = instance->pullMessage(
        kGroup, mq, 3, 32, sysFlag, /*commitOffset=*/0, "TagA||TagB", 0, ExpressionType::TAG,
        /*timeoutMillis=*/3000, /*maxMsgBytes=*/-1, /*suspendTimeoutMillis=*/15000,
        /*addr=*/std::string(), /*requestSource=*/0, /*brokerId=*/MixAll::MASTER_ID);

    expectInt(static_cast<long long>(master.pullCount()), 1, "the pull reached the master");
    expectInt(static_cast<long long>(slave.pullCount()), 0, "slave received nothing");

    PropertyMap sent = master.pullExt(0);
    const int64_t sentFlag = MockEndpoint::extInt(sent, "sysFlag");
    expect(PullSysFlag::hasCommitOffsetFlag(static_cast<int32_t>(sentFlag)), "on master the offset is committed as usual");
    expect(PullSysFlag::hasSubscriptionFlag(static_cast<int32_t>(sentFlag)), "the subscription bit is set");
    auto subIt = sent.find("subscription");
    expect(subIt != sent.end() && subIt->second == "TagA||TagB", "the subscription content is on the wire");
    expectInt(MockEndpoint::extInt(sent, "queueOffset"), 3, "queueOffset is on the wire");
    expect(!result.suggestWhichBrokerId.has_value(), "an old broker without suggest passes through nothing");
    expectInt(result.nextBeginOffset, 7, "nextBeginOffset passes through");
}

// ---------------------------------------------------------------- 4. slave missing, fall back to master

void testMissingSlaveFallsBackToMasterAndKeepsCommitOffset() {
    MockEndpoint namesrv;
    MockEndpoint master;
    namesrv.addRoute(kTopic, routeWithMasterOnly(master.address()));
    master.scriptPullResponse(pullRespExt(/*withSuggest=*/false, 0));

    auto instance = seeded(namesrv);
    MessageQueue mq(kTopic, kBroker, 0);
    const int32_t sysFlag = PullSysFlag::buildSysFlag(/*commitOffset=*/true, /*suspend=*/true,
                                                      /*subscription=*/false,
                                                      /*classFilter=*/false);

    instance->pullMessage(kGroup, mq, 0, 32, sysFlag, /*commitOffset=*/0, "TagA", 0,
                          ExpressionType::TAG, 3000, -1, 15000, std::string(), 0,
                          /*brokerId=*/3);

    expectInt(static_cast<long long>(master.pullCount()), 1, "with the slave missing it falls back to master");
    const int64_t sentFlag = MockEndpoint::extInt(master.pullExt(0), "sysFlag");
    expect(PullSysFlag::hasCommitOffsetFlag(static_cast<int32_t>(sentFlag)),
           "the fallback lands on master so the offset is still committed (isSlave judged by the id actually hit)");
}

// ---------------------------------------------------------------- 5. push-side subscription gating

void testPullSubscriptionGatingFollowsDefault() {
    expect(!DefaultMQPushConsumer::shouldPostSubscriptionWhenPull(false, false),
           "default off, nothing posted");
    expect(DefaultMQPushConsumer::shouldPostSubscriptionWhenPull(true, false),
           "on and not class-filtered, posted");
    expect(!DefaultMQPushConsumer::shouldPostSubscriptionWhenPull(true, true),
           "class-filter mode never posts even when on (the expression is a class name and the broker's TAG filter would misjudge it)");
    expect(!DefaultMQPushConsumer::shouldPostSubscriptionWhenPull(false, true),
           "both off");

    DefaultMQPushConsumer consumer(kGroup);
    expect(!consumer.isPostSubscriptionWhenPull(), "the default is false");
    consumer.setPostSubscriptionWhenPull(true);
    expect(consumer.isPostSubscriptionWhenPull(), "the setter takes effect");
}

// ---------------------------------------------------------------- 6. pull consumer table read/write

void testPullFromWhichNodeRoundTrip() {
    DefaultMQPullConsumer pull(kGroup);
    MessageQueue mq(kTopic, kBroker, 0);
    MessageQueue other(kTopic, kBroker, 1);

    expectInt(pull.recalculatePullFromWhichNode(mq), MixAll::MASTER_ID, "the first round targets the master");

    PullResult withSuggest;
    withSuggest.suggestWhichBrokerId = MixAll::MASTER_ID + 1;
    pull.updatePullFromWhichNode(mq, withSuggest);
    expectInt(pull.recalculatePullFromWhichNode(mq), MixAll::MASTER_ID + 1, "suggest is written back into the table");

    // an old broker without suggest is accounted as master, not keeping the stale value
    pull.updatePullFromWhichNode(mq, PullResult{});
    expectInt(pull.recalculatePullFromWhichNode(mq), MixAll::MASTER_ID,
              "missing suggestWhichBrokerId is accounted as master (a long primitive in the protocol)");

    PullResult two;
    two.suggestWhichBrokerId = 2;
    pull.updatePullFromWhichNode(mq, two);
    expectInt(pull.recalculatePullFromWhichNode(mq), 2, "isolated per queue (this queue updated)");
    expectInt(pull.recalculatePullFromWhichNode(other), MixAll::MASTER_ID, "isolated per queue (other queues unaffected)");
}

}  // namespace

int main() {
    testFindBrokerAddressInSubscribeBranches();
    testPullToSlaveClearsCommitOffsetAndOmitsSubscription();
    testPullToMasterKeepsCommitOffsetAndPostsSubscription();
    testMissingSlaveFallsBackToMasterAndKeepsCommitOffset();
    testPullSubscriptionGatingFollowsDefault();
    testPullFromWhichNodeRoundTrip();
    std::printf("%d checks, %d failures\n", checks, fails);
    return fails == 0 ? 0 : 1;
}
