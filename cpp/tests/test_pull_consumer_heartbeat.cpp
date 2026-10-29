// P6 (#98): offline unit test for the **heartbeat** of the classic pull consumer / lite pull consumer.
//
//
// Criteria are taken from the **wire traffic**: the in-process fake endpoint reads the HEART_BEAT body (JSON) and
// the UNREGISTER_CLIENT extFields off the socket, instead of trusting what the caller claims.
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "rocketmq/client/consumer.h"
#include "rocketmq/client/exception.h"
#include "rocketmq/client/lite_pull_consumer.h"
#include "rocketmq/client/pull_consumer.h"
#include "rocketmq/common/mix_all.h"
#include "rocketmq/common/net_compat.h"
#include "rocketmq/common/util_all.h"
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

void expectInt(long long actual, long long expected, const std::string& name) {
    ++checks;
    if (actual != expected) {
        ++fails;
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

// ---------------------------------------------------------------- fake endpoint

// Plays both the name server (answers routes) and the broker (receives HEART_BEAT / UNREGISTER_CLIENT).
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

    size_t heartbeatCount() {
        std::lock_guard<std::mutex> lk(state_);
        return heartbeats_.size();
    }

    // The first ConsumerData in the i-th heartbeat (in arrival order)
    bool consumerData(size_t i, ConsumerData& out) {
        std::lock_guard<std::mutex> lk(state_);
        if (i >= heartbeats_.size()) return false;
        return decodeConsumerData(heartbeats_[i], out);
    }

    size_t unregisterCount() {
        std::lock_guard<std::mutex> lk(state_);
        return unregisters_.size();
    }

    PropertyMap unregisterExt(size_t i) {
        std::lock_guard<std::mutex> lk(state_);
        if (i >= unregisters_.size()) return PropertyMap();
        return unregisters_[i];
    }

    /// Whether this request code was ever received (offset-store selection is judged on the wire).
    bool sawCode(int32_t code) {
        std::lock_guard<std::mutex> lk(state_);
        for (int32_t c : codes_) {
            if (c == code) return true;
        }
        return false;
    }

    size_t countCode(int32_t code) {
        std::lock_guard<std::mutex> lk(state_);
        size_t n = 0;
        for (int32_t c : codes_) {
            if (c == code) ++n;
        }
        return n;
    }

    static bool decodeConsumerData(const std::string& body, ConsumerData& out) {
        JsonValue v;
        std::string err;
        if (!jsonParse(body, v, &err)) return false;
        HeartbeatData hb = HeartbeatData::fromJson(v);
        if (hb.consumerDataSet.empty()) return false;
        out = hb.consumerDataSet.front();
        return true;
    }

private:
    static std::string offsetWireKey(const PropertyMap& ext) {
        auto at = [&ext](const char* k) {
            auto it = ext.find(k);
            return it == ext.end() ? std::string() : it->second;
        };
        return at("consumerGroup") + "|" + at("topic") + "|" + at("queueId");
    }

    static int64_t wireInt(const PropertyMap& ext, const char* key) {
        auto it = ext.find(key);
        if (it == ext.end()) return -1;
        try {
            return std::stoll(it->second);
        } catch (...) {
            return -1;
        }
    }
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
            {
                std::lock_guard<std::mutex> lk(state_);
                codes_.push_back(req.code);
            }
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
            } else if (req.code == RequestCode::HEART_BEAT) {
                std::lock_guard<std::mutex> lk(state_);
                heartbeats_.push_back(std::string(req.body.begin(), req.body.end()));
                resp.code = ResponseCode::SUCCESS;
            } else if (req.code == RequestCode::UNREGISTER_CLIENT) {
                std::lock_guard<std::mutex> lk(state_);
                unregisters_.push_back(req.extFields);
                resp.code = ResponseCode::SUCCESS;
            } else if (req.code == RequestCode::UPDATE_CONSUMER_OFFSET) {
                std::lock_guard<std::mutex> lk(state_);
                brokerOffsets_[offsetWireKey(req.extFields)] = wireInt(req.extFields, "commitOffset");
                resp.code = ResponseCode::SUCCESS;
            } else if (req.code == RequestCode::QUERY_CONSUMER_OFFSET) {
                std::lock_guard<std::mutex> lk(state_);
                auto it = brokerOffsets_.find(offsetWireKey(req.extFields));
                if (it == brokerOffsets_.end()) {
                    resp.code = ResponseCode::QUERY_NOT_FOUND;
                } else {
                    resp.code = ResponseCode::SUCCESS;
                    resp.extFields["offset"] = std::to_string(it->second);
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

    socket_t listen_ = kInvalidSocket;
    uint16_t port_ = 0;
    std::atomic<bool> running_{false};
    std::mutex state_;
    std::map<std::string, Bytes> routes_;
    std::vector<std::string> heartbeats_;
    std::vector<PropertyMap> unregisters_;
    std::vector<int32_t> codes_;
    std::map<std::string, int64_t> brokerOffsets_;
    std::mutex workersM_;
    std::vector<std::thread> workers_;
    std::thread acceptor_;
};

// ---------------------------------------------------------------- scaffolding

const char* kGroup = "PG_PullHbUnit";
const char* kTopic = "PullHbTopic";
const char* kBroker = "broker-a";

class NoopListener : public MessageQueueListener {
public:
    void messageQueueChanged(const std::string&, const std::vector<MessageQueue>&,
                             const std::vector<MessageQueue>&) override {}
};

TopicRouteData routeOne(const std::string& broker, int64_t id, const std::string& addr) {
    TopicRouteData route;
    std::map<int64_t, std::string> addrs;
    addrs[id] = addr;
    route.brokerDatas.emplace_back("DefaultCluster", broker, addrs);
    // queueDatas determine TopicPublishInfo.msgQueueList (what fetchSubscribeMessageQueues returns)
    route.queueDatas.emplace_back(broker, 4, 4, 6, 0);
    return route;
}

TopicRouteData routeTwo(const std::string& masterAddr, const std::string& slaveAddr) {
    TopicRouteData route;
    std::map<int64_t, std::string> addrs;
    addrs[MixAll::MASTER_ID] = masterAddr;
    addrs[MixAll::MASTER_ID + 1] = slaveAddr;
    route.brokerDatas.emplace_back("DefaultCluster", kBroker, addrs);
    route.queueDatas.emplace_back(kBroker, 4, 4, 6, 0);
    return route;
}

// Wait until the condition holds (default 5s cap); returns whether it was observed
template <typename Fn>
bool waitFor(Fn fn, int timeoutMs = 5000) {
    const int64_t deadline = UtilAll::currentTimeMillis() + timeoutMs;
    while (UtilAll::currentTimeMillis() < deadline) {
        if (fn()) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    return fn();
}

// Point HOME at a throw-away directory: the broadcasting offset store is rooted at $HOME.
void setHomeEnv(const char* value) {
#if defined(_WIN32)
    _putenv_s("HOME", value);
#else
    setenv("HOME", value, 1);
#endif
}

void clearHomeEnv() {
#if defined(_WIN32)
    _putenv_s("HOME", "");
#else
    unsetenv("HOME");
#endif
}

class ScopedHome {
public:
    explicit ScopedHome(const char* tag) {
        const char* old = std::getenv("HOME");
        if (old != nullptr) old_ = old;
        dir_ = (std::filesystem::temp_directory_path() /
                ("rmq_pull_home_" + std::string(tag) + "_" +
                 std::to_string(UtilAll::currentTimeMillis())))
                   .string();
        std::error_code ec;
        std::filesystem::create_directories(dir_, ec);
        setHomeEnv(dir_.c_str());
    }
    ~ScopedHome() {
        if (old_.empty()) {
            clearHomeEnv();
        } else {
            setHomeEnv(old_.c_str());
        }
        std::error_code ec;
        std::filesystem::remove_all(dir_, ec);
    }
    const std::string& path() const { return dir_; }

private:
    std::string dir_;
    std::string old_;
};

// ---------------------------------------------------------------- 1. start registers immediately

void testStartAnnouncesTheGroupWithTheWireShape() {
    MockEndpoint namesrv;
    MockEndpoint broker;
    namesrv.addRoute(kTopic, routeOne(kBroker, MixAll::MASTER_ID, broker.address()));

    DefaultMQPullConsumer consumer(kGroup);
    consumer.setNamesrvAddr(namesrv.address());
    auto listener = std::make_shared<NoopListener>();
    consumer.registerMessageQueueListener(kTopic, listener);
    consumer.setUnitMode(true);
    consumer.start();
    const bool got = waitFor([&] { return broker.heartbeatCount() >= 1; });
    expect(got, "broker receives HEART_BEAT after start()");
    expectInt(consumer.heartbeatCount(), 1, "one synchronous round at startup (heartbeat count=1)");

    ConsumerData cd;
    const bool decoded = broker.consumerData(0, cd);
    expect(decoded, "heartbeat body decodes to ConsumerData using wire field names");
    if (decoded) {
        expect(cd.groupName == kGroup, "ConsumerData.groupName is this group", "got=" + cd.groupName);
        // The pull consumer reports CONSUME_ACTIVELY: it pulls by hand instead of being pushed to.
        expect(cd.consumeType == ConsumeType::CONSUME_ACTIVELY,
               "consumeType=CONSUME_ACTIVELY (same value as the reference implementation)",
               "got=" + cd.consumeType);
        // A fresh CLUSTERING consumer starts at the tail of the queue, not from the beginning.
        expect(cd.consumeFromWhere == ConsumeFromWhere::CONSUME_FROM_LAST_OFFSET,
               "consumeFromWhere=CONSUME_FROM_LAST_OFFSET", "got=" + cd.consumeFromWhere);
        expect(cd.messageModel == MessageModel::CLUSTERING,
               "messageModel=CLUSTERING", "got=" + cd.messageModel);
        expect(cd.unitMode, "unitMode reported truthfully");
        expectInt(static_cast<long long>(cd.subscriptionDataSet.size()), 1,
                  "subscription set comes from registerTopics");
        if (!cd.subscriptionDataSet.empty()) {
            const SubscriptionData& sub = cd.subscriptionDataSet.front();
            expect(sub.topic == kTopic, "subscribed topic verbatim (no namespace prefix)", "got=" + sub.topic);
            expect(sub.subString == "*", "subString=SUB_ALL", "got=" + sub.subString);
            expectInt(sub.subVersion, 0, "subVersion=0 (the default is 0)");
        }
    }

    consumer.shutdown();
}

// ---------------------------------------------------------------- 2. fan-out to every broker

void testHeartbeatFansOutToEveryBrokerId() {
    MockEndpoint namesrv;
    MockEndpoint master;
    MockEndpoint slave;
    namesrv.addRoute(kTopic, routeTwo(master.address(), slave.address()));

    DefaultMQPullConsumer consumer(kGroup);
    consumer.setNamesrvAddr(namesrv.address());
    auto listener = std::make_shared<NoopListener>();
    consumer.registerMessageQueueListener(kTopic, listener);
    consumer.start();
    const bool both = waitFor([&] {
        return master.heartbeatCount() >= 1 && slave.heartbeatCount() >= 1;
    });
    expect(both, "master and slave both receive the heartbeat", "master=" + std::to_string(master.heartbeatCount()) +
                                      " slave=" + std::to_string(slave.heartbeatCount()));
    expectInt(consumer.heartbeatCount(), 1, "one heartbeat round counts once (successful brokers are not recounted)");
    consumer.shutdown();
}

// ---------------------------------------------------------------- 3. the group is registered even without subscriptions

void testHeartbeatStillAnnouncesTheGroupWithoutRegisterTopics() {
    MockEndpoint namesrv;
    MockEndpoint broker;
    namesrv.addRoute(kTopic, routeOne(kBroker, MixAll::MASTER_ID, broker.address()));

    DefaultMQPullConsumer consumer(kGroup);
    consumer.setNamesrvAddr(namesrv.address());
    consumer.start();
    // No registerTopics -> the route table is empty at start() and that round sends 0; fetch queues once to fill the route table
    const std::vector<MessageQueue> mqs = consumer.fetchSubscribeMessageQueues(kTopic);
    expect(!mqs.empty(), "precondition: route fetched (route answered by the mock name server)");
    const int32_t sent = consumer.sendHeartbeatToAllBroker();
    expectInt(sent, 1, "explicit heartbeat sent to 1 broker");

    ConsumerData cd;
    const bool decoded = broker.consumerData(0, cd);
    expect(decoded, "heartbeat body decodable");
    if (decoded) {
        expect(cd.groupName == kGroup, "the group is registered even with empty subscriptions", "got=" + cd.groupName);
        expectInt(static_cast<long long>(cd.subscriptionDataSet.size()), 0,
                  "subscription set is empty (no registerTopics)");
    }
    consumer.shutdown();
}

// ---------------------------------------------------------------- 4. periodic loop

void testHeartbeatLoopRepeats() {
    MockEndpoint namesrv;
    MockEndpoint broker;
    namesrv.addRoute(kTopic, routeOne(kBroker, MixAll::MASTER_ID, broker.address()));

    DefaultMQPullConsumer consumer(kGroup);
    consumer.setNamesrvAddr(namesrv.address());
    consumer.setHeartbeatBrokerIntervalMillis(300);
    auto listener = std::make_shared<NoopListener>();
    consumer.registerMessageQueueListener(kTopic, listener);
    consumer.start();
    const bool repeated = waitFor([&] { return consumer.heartbeatCount() >= 3; }, 6000);
    expect(repeated, "the heartbeat loop repeats on its interval", "count=" + std::to_string(consumer.heartbeatCount()));

    // With the switch off the loop spins empty (the round at startup is unaffected)
    consumer.setHeartbeatEnabled(false);
    const int32_t before = consumer.heartbeatCount();
    std::this_thread::sleep_for(std::chrono::milliseconds(900));
    expectInt(consumer.heartbeatCount(), before, "the loop stops sending after heartbeatEnabled=false");
    consumer.shutdown();
}

// ---------------------------------------------------------------- 5. shutdown unregisters

void testShutdownUnregistersTheGroupOnEveryBroker() {
    MockEndpoint namesrv;
    MockEndpoint master;
    MockEndpoint slave;
    namesrv.addRoute(kTopic, routeTwo(master.address(), slave.address()));

    DefaultMQPullConsumer consumer(kGroup);
    consumer.setNamesrvAddr(namesrv.address());
    auto listener = std::make_shared<NoopListener>();
    consumer.registerMessageQueueListener(kTopic, listener);
    consumer.start();
    waitFor([&] { return master.heartbeatCount() >= 1; });
    consumer.shutdown();

    const bool bothUnregistered = master.unregisterCount() >= 1 && slave.unregisterCount() >= 1;
    expect(bothUnregistered, "shutdown sends UNREGISTER_CLIENT(35) to every broker",
           "master=" + std::to_string(master.unregisterCount()) +
               " slave=" + std::to_string(slave.unregisterCount()));
    if (bothUnregistered) {
        const PropertyMap ext = master.unregisterExt(0);
        expect(ext.count("consumerGroup") && ext.at("consumerGroup") == kGroup,
               "35 carries consumerGroup", "ext=" + (ext.count("consumerGroup") ? ext.at("consumerGroup")
                                                                           : std::string("<none>")));
        // The empty slot is not sent (the broker ClientManageProcessor:228/237 checks group != null)
        expect(ext.count("producerGroup") == 0, "35 omits the producerGroup slot");
    }
    // Idempotent: a repeated shutdown sends nothing more
    const size_t before = master.unregisterCount();
    consumer.shutdown();
    expectInt(static_cast<long long>(master.unregisterCount()), static_cast<long long>(before),
              "repeated shutdown is idempotent");
}

// ---------------------------------------------------------------- 6. the lite pull consumer is ACTIVELY too

void testLitePullHeartbeatIsActivelyTyped() {
    MockEndpoint namesrv;
    MockEndpoint broker;
    namesrv.addRoute(kTopic, routeOne(kBroker, MixAll::MASTER_ID, broker.address()));

    DefaultLitePullConsumer consumer(kGroup);
    consumer.setNamesrvAddr(namesrv.address());
    consumer.subscribe(kTopic, "*");
    consumer.start();
    const bool got = waitFor([&] { return broker.heartbeatCount() >= 1; });
    expect(got, "lite broker receives HEART_BEAT after start()");

    ConsumerData cd;
    const bool decoded = broker.consumerData(0, cd);
    expect(decoded, "lite heartbeat body decodable");
    if (decoded) {
        expect(cd.groupName == kGroup, "lite ConsumerData.groupName is this group", "got=" + cd.groupName);
        expect(cd.consumeType == ConsumeType::CONSUME_ACTIVELY,
               "lite consumeType=CONSUME_ACTIVELY (same value as the reference implementation)",
               "got=" + cd.consumeType);
    }
    consumer.shutdown();
}

// ---------------------------------------------------------------- 7. offset store selection

void testPullBroadcastingKeepsOffsetsLocal() {
    ScopedHome home("broadcast_offsets");
    MockEndpoint namesrv;
    MockEndpoint broker;
    namesrv.addRoute(kTopic, routeOne(kBroker, MixAll::MASTER_ID, broker.address()));

    DefaultMQPullConsumer consumer("PG_PullBroadcastOffsets");
    consumer.setInstanceName("pull_broadcast_offsets");
    consumer.setNamesrvAddr(namesrv.address());
    consumer.setMessageModel(MessageModel::BROADCASTING);
    auto listener = std::make_shared<NoopListener>();
    consumer.registerMessageQueueListener(kTopic, listener);
    consumer.start();
    expect(waitFor([&] { return broker.heartbeatCount() >= 1; }),
           "precondition: the broker is reachable, so any offset RPC would be observed");

    const MessageQueue mq(kTopic, kBroker, 0);
    int64_t missing = -1;
    expect(!consumer.fetchConsumeOffset(mq, missing),
           "broadcasting has no offset before one is committed");

    consumer.updateConsumeOffset(mq, 12);
    int64_t fetched = -1;
    const bool answered = consumer.fetchConsumeOffset(mq, fetched);
    expect(answered && fetched == 12, "broadcasting answers the offset it committed",
           "answered=" + std::string(answered ? "yes" : "no") + " offset=" + std::to_string(fetched));

    const std::string path = home.path() + "/.rocketmq_offsets/" + consumer.clientId() +
                             "/PG_PullBroadcastOffsets/offsets.json";
    const std::map<std::string, int64_t> table = DefaultMQPushConsumer::loadLocalOffsetsAt(path);
    const std::string key = DefaultMQPushConsumer::offsetKey(mq);
    expect(table.count(key) && table.at(key) == 12,
           "broadcasting commit lands in the local offset file under the queue key");

    expect(!broker.sawCode(RequestCode::UPDATE_CONSUMER_OFFSET),
           "broadcasting never sends UPDATE_CONSUMER_OFFSET");
    expect(!broker.sawCode(RequestCode::QUERY_CONSUMER_OFFSET),
           "broadcasting never sends QUERY_CONSUMER_OFFSET");
    consumer.shutdown();
}

void testPullClusteringUsesBrokerOffsets() {
    MockEndpoint namesrv;
    MockEndpoint broker;
    namesrv.addRoute(kTopic, routeOne(kBroker, MixAll::MASTER_ID, broker.address()));

    DefaultMQPullConsumer consumer("PG_PullClusterOffsets");
    consumer.setInstanceName("pull_cluster_offsets");
    consumer.setNamesrvAddr(namesrv.address());
    consumer.setMessageModel(MessageModel::CLUSTERING);
    auto listener = std::make_shared<NoopListener>();
    consumer.registerMessageQueueListener(kTopic, listener);
    consumer.start();
    expect(waitFor([&] { return broker.heartbeatCount() >= 1; }),
           "precondition: the broker is reachable");

    const MessageQueue mq(kTopic, kBroker, 0);
    consumer.updateConsumeOffset(mq, 21);
    expect(waitFor([&] { return broker.sawCode(RequestCode::UPDATE_CONSUMER_OFFSET); }),
           "clustering sends UPDATE_CONSUMER_OFFSET to the broker");

    int64_t fetched = -1;
    const bool answered = consumer.fetchConsumeOffset(mq, fetched);
    expect(answered && fetched == 21, "clustering reads the offset back from the broker",
           "answered=" + std::string(answered ? "yes" : "no") + " offset=" + std::to_string(fetched));
    expect(broker.sawCode(RequestCode::QUERY_CONSUMER_OFFSET),
           "clustering sends QUERY_CONSUMER_OFFSET to the broker");
    consumer.shutdown();
}

}  // namespace

int main() {
    testStartAnnouncesTheGroupWithTheWireShape();
    testHeartbeatFansOutToEveryBrokerId();
    testHeartbeatStillAnnouncesTheGroupWithoutRegisterTopics();
    testHeartbeatLoopRepeats();
    testShutdownUnregistersTheGroupOnEveryBroker();
    testLitePullHeartbeatIsActivelyTyped();
    testPullBroadcastingKeepsOffsetsLocal();
    testPullClusteringUsesBrokerOffsets();
    std::printf("%d checks, %d failures\n", checks, fails);
    return fails == 0 ? 0 : 1;
}
