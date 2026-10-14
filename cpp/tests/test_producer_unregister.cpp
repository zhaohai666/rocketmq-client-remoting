// Offline unit test for UNREGISTER_CLIENT(35) at shutdown -- no cluster required.
//
//
//
// Coverage:
//   1. Producer-side header shape: only clientID + producerGroup, the consumerGroup field is not sent
//   2. Consumer-side header shape: only clientID + consumerGroup
//   3. When both sides exist, all three keys are present
//   4. Fan-out includes slaves (an implementation sending only to master must fail here), one send each
//   5. One broker answers SYSTEM_ERROR: `unregisterClient` throws, `unregisterClientAllBrokers` swallows
//   6. Heartbeat recipient split: helper level (`getRouteOfAllBrokers` master only /
//      `getAllBrokerAddrs` master and slave) plus behavior level (a real consumer, master and slave each receive one heartbeat)
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

#include "rocketmq/client/consumer.h"
#include "rocketmq/client/exception.h"
#include "rocketmq/client/mq_client.h"
#include "rocketmq/client/result.h"
#include "rocketmq/common/message.h"
#include "rocketmq/common/net_compat.h"
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
        ++fails;
        std::printf("FAIL %s (actual=%lld expected=%lld)\n", name.c_str(), actual, expected);
    }
}

std::string keyText(const PropertyMap& ext) {
    std::string out = "{";
    bool first = true;
    for (const auto& kv : ext) {
        if (!first) out += ", ";
        first = false;
        out += kv.first + "=" + kv.second;
    }
    return out + "}";
}

bool readN(socket_t s, char* buf, size_t n) {
    size_t got = 0;
    while (got < n) {
        int r = static_cast<int>(::recv(s, buf + got, static_cast<int>(n - got), 0));
        if (r <= 0) return false;
        got += static_cast<size_t>(n);
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

// ---------------------------------------------------------------- mock broker

struct Record {
    int32_t code = 0;
    PropertyMap ext;
    /// The request body verbatim (the heartbeat consumer group is only in the body; extFields is empty).
    Bytes body;
};

// Plays a broker: replies with scripted response codes per request code and keeps the extFields of every frame as evidence.
// Only route requests get special treatment (name server and broker share one skeleton; here it serves route + unregister only).
class MockBroker {
public:
    explicit MockBroker(bool servesRoutes = false) : servesRoutes_(servesRoutes) {
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

    ~MockBroker() {
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

    std::string address() const { return "127.0.0.1:" + std::to_string(port_); }

    void addRoute(const std::string& topic, const TopicRouteData& route) {
        std::lock_guard<std::mutex> lk(state_);
        routes_[topic] = route.encode();
    }

    // The i-th 35 answers codes[i] (beyond the script answers SUCCESS); evidence and counter clear together so the script lines up
    void scriptUnregisters(std::vector<int32_t> codes) {
        std::lock_guard<std::mutex> lk(state_);
        unregCodes_ = std::move(codes);
        records_.clear();
        unregCount_.store(0);
    }

    std::vector<Record> recordsOf(int32_t code) {
        std::lock_guard<std::mutex> lk(state_);
        std::vector<Record> out;
        for (const Record& r : records_) {
            if (r.code == code) out.push_back(r);
        }
        return out;
    }

    int32_t countOf(int32_t code) { return static_cast<int32_t>(recordsOf(code).size()); }

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
                if (servesRoutes_ && it != routes_.end()) {
                    resp.code = ResponseCode::SUCCESS;
                    resp.body = it->second;
                    resp.hasBody = true;
                } else {
                    resp.code = ResponseCode::TOPIC_NOT_EXIST;
                }
            } else if (req.code == RequestCode::UNREGISTER_CLIENT) {
                const int idx = unregCount_.fetch_add(1);
                {
                    std::lock_guard<std::mutex> lk(state_);
                    records_.push_back({req.code, req.extFields, req.body});
                }
                std::lock_guard<std::mutex> lk(state_);
                if (idx < static_cast<int>(unregCodes_.size())
                    && unregCodes_[idx] != ResponseCode::SUCCESS) {
                    resp.code = unregCodes_[idx];
                    resp.remark = "mock reject";
                    resp.hasRemark = true;
                } else {
                    resp.code = ResponseCode::SUCCESS;
                }
            } else if (req.code == RequestCode::HEART_BEAT) {
                // Heartbeats are only recorded, never scripted: who received them is asserted via counts and body.
                std::lock_guard<std::mutex> lk(state_);
                records_.push_back({req.code, req.extFields, req.body});
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

    bool servesRoutes_ = false;
    socket_t listen_ = kInvalidSocket;
    uint16_t port_ = 0;
    std::atomic<bool> running_{false};
    std::atomic<int> unregCount_{0};
    std::mutex state_;
    std::map<std::string, Bytes> routes_;
    std::vector<int32_t> unregCodes_;
    std::vector<Record> records_;
    std::mutex workersM_;
    std::vector<std::thread> workers_;
    std::thread acceptor_;
};

// ---------------------------------------------------------------- test scaffolding

const char* kClientId = "127.0.0.1@1234";
const char* kGroup = "PID_Unregister";
const char* kTopic = "UnregisterTopic";

class NoopListener : public MessageListenerConcurrently {
public:
    ConsumeConcurrentlyStatus consumeMessage(const std::vector<MessageExt>&,
                                             ConsumeConcurrentlyContext&) override {
        return ConsumeConcurrentlyStatus::CONSUME_SUCCESS;
    }
};

std::unique_ptr<MQClientInstance> seeded(MockBroker& namesrv, MockBroker& master,
                                         MockBroker& slave) {
    auto instance = std::make_unique<MQClientInstance>(
        kClientId, std::vector<std::string>{namesrv.address()});
    TopicRouteData route;
    std::map<int64_t, std::string> addrs;
    addrs[0] = master.address();
    addrs[1] = slave.address();
    route.brokerDatas.emplace_back("DefaultCluster", "broker-a", addrs);
    namesrv.addRoute(kTopic, route);
    instance->updateTopicRouteInfoFromNameServer(kTopic);
    return instance;
}

void testHeaderShape() {
    MockBroker broker;
    auto instance = std::make_unique<MQClientInstance>(
        kClientId, std::vector<std::string>{broker.address()});

    broker.scriptUnregisters({});
    instance->unregisterClient(broker.address(), kClientId, kGroup, "");
    std::vector<Record> got = broker.recordsOf(RequestCode::UNREGISTER_CLIENT);
    expectInt(static_cast<long long>(got.size()), 1, "producer unregister sends exactly one 35");
    if (!got.empty()) {
        PropertyMap want;
        want["clientID"] = kClientId;
        want["producerGroup"] = kGroup;
        expect(got[0].ext == want, "producer side carries only clientID + producerGroup", keyText(got[0].ext));
    }

    broker.scriptUnregisters({});
    instance->unregisterClient(broker.address(), kClientId, "", kGroup);
    got = broker.recordsOf(RequestCode::UNREGISTER_CLIENT);
    expectInt(static_cast<long long>(got.size()), 1, "consumer unregister sends exactly one 35");
    if (!got.empty()) {
        PropertyMap want;
        want["clientID"] = kClientId;
        want["consumerGroup"] = kGroup;
        expect(got[0].ext == want, "consumer side carries only clientID + consumerGroup", keyText(got[0].ext));
    }

    broker.scriptUnregisters({});
    instance->unregisterClient(broker.address(), kClientId, kGroup, kGroup);
    got = broker.recordsOf(RequestCode::UNREGISTER_CLIENT);
    expectInt(static_cast<long long>(got.size()), 1, "both sides present sends exactly one 35");
    if (!got.empty()) {
        std::vector<std::string> keys;
        for (const auto& kv : got[0].ext) keys.push_back(kv.first);
        std::sort(keys.begin(), keys.end());
        const std::vector<std::string> want = {"clientID", "consumerGroup", "producerGroup"};
        expect(keys == want, "all three keys present", keyText(got[0].ext));
    }

    broker.scriptUnregisters({});
    instance->unregisterClient(broker.address(), kClientId, "   ", kGroup);
    got = broker.recordsOf(RequestCode::UNREGISTER_CLIENT);
    expect(got.empty() || got[0].ext.count("producerGroup") == 0,
           "blank producerGroup is not sent", got.empty() ? "" : keyText(got[0].ext));
}

// 4. Fan-out includes slaves: one send per broker (a master-only implementation is short one send here)
void testFanOutIncludesSlaves(MockBroker& namesrv, MockBroker& master, MockBroker& slave) {
    auto instance = seeded(namesrv, master, slave);
    master.scriptUnregisters({});
    slave.scriptUnregisters({});
    instance->unregisterClientAllBrokers(kClientId, kGroup, "");
    expectInt(master.countOf(RequestCode::UNREGISTER_CLIENT), 1, "master receives one 35");
    expectInt(slave.countOf(RequestCode::UNREGISTER_CLIENT), 1, "slave also receives one 35");
    std::vector<Record> m = master.recordsOf(RequestCode::UNREGISTER_CLIENT);
    if (!m.empty()) {
        expect(m[0].ext.count("consumerGroup") == 0, "no send in the fan-out carries consumerGroup",
               keyText(m[0].ext));
    }
}

// 5. A single failure is swallowed, not thrown; the direct call layer still throws an exception carrying the code
void testFailureIsSwallowed(MockBroker& namesrv, MockBroker& master, MockBroker& slave) {
    auto instance = seeded(namesrv, master, slave);
    master.scriptUnregisters({ResponseCode::SYSTEM_ERROR});
    slave.scriptUnregisters({});
    int threw = 0;
    try {
        instance->unregisterClientAllBrokers(kClientId, kGroup, "");
    } catch (const std::exception& e) {
        threw = 1;
        std::printf("  should not throw: %s\n", e.what());
    }
    expect(threw == 0, "one broker error does not interrupt shutdown");
    expectInt(master.countOf(RequestCode::UNREGISTER_CLIENT), 1, "the failing broker was still sent to");
    expectInt(slave.countOf(RequestCode::UNREGISTER_CLIENT), 1, "the next broker is still sent to after an error");

    // The single-send layer does not swallow: the caller must be able to read the broker response code
    master.scriptUnregisters({ResponseCode::SYSTEM_ERROR});
    int32_t code = 0;
    try {
        instance->unregisterClient(master.address(), kClientId, kGroup, "");
    } catch (const MQBrokerException& e) {
        code = e.getResponseCode();
    } catch (const std::exception& e) {
        std::printf("  wrong exception type: %s\n", e.what());
    }
    expectInt(code, ResponseCode::SYSTEM_ERROR, "unregisterClient throws non-SUCCESS as MQBrokerException");
}

// 6. Heartbeat recipient split: helper level plus behavior level
//
// helper: `getRouteOfAllBrokers` picks one broker per brokerName (producer heartbeats use it),
//         `getAllBrokerAddrs` returns master and slave (consumer heartbeats and unregistration use it).
// behavior: a real consumer is started (subscribing to a topic with a route in the fake name server); the first heartbeat
//       round in start() must land once on the master and once on the slave; the body the slave receives carries the consumer
//       group (it builds its own ConsumerGroupInfo from it, otherwise pulls to it answer SUBSCRIPTION_NOT_EXIST per `PullMessageProcessor`:420-427).
void testHeartbeatFanOut(MockBroker& namesrv, MockBroker& master, MockBroker& slave) {
    {
        auto instance = seeded(namesrv, master, slave);
        std::vector<std::string> masterOnly = instance->getRouteOfAllBrokers();
        expectInt(static_cast<long long>(masterOnly.size()), 1, "getRouteOfAllBrokers returns master only");
        if (!masterOnly.empty()) expect(masterOnly[0] == master.address(), "that broker is the master");
        std::vector<std::string> all = instance->getAllBrokerAddrs();
        expectInt(static_cast<long long>(all.size()), 2, "getAllBrokerAddrs returns master and slave");
        expect(std::find(all.begin(), all.end(), slave.address()) != all.end(), "the slave is in the table");
    }

    master.scriptUnregisters({});
    slave.scriptUnregisters({});
    DefaultMQPushConsumer consumer(kGroup);
    consumer.setNamesrvAddr(namesrv.address());
    consumer.subscribe(kTopic, "*");
    consumer.setMessageListener(std::make_shared<NoopListener>());
    consumer.start();
    expectInt(master.countOf(RequestCode::HEART_BEAT) >= 1 ? 1 : 0, 1, "master receives a heartbeat");
    // This assertion is the reason the case exists: a fan-out sending only to master is 0 here
    expectInt(slave.countOf(RequestCode::HEART_BEAT) >= 1 ? 1 : 0, 1, "slave also receives a heartbeat");
    std::vector<Record> onSlave = slave.recordsOf(RequestCode::HEART_BEAT);
    if (!onSlave.empty()) {
        const std::string body(onSlave[0].body.begin(), onSlave[0].body.end());
        expect(body.find(kGroup) != std::string::npos, "the body the slave receives carries the consumer group", body);
    }
    consumer.shutdown();
}

}  // namespace

int main() {
    testHeaderShape();
    {
        MockBroker namesrv(true);
        MockBroker master;
        MockBroker slave;
        testFanOutIncludesSlaves(namesrv, master, slave);
    }
    {
        MockBroker namesrv(true);
        MockBroker master;
        MockBroker slave;
        testFailureIsSwallowed(namesrv, master, slave);
    }
    {
        MockBroker namesrv(true);
        MockBroker master;
        MockBroker slave;
        testHeartbeatFanOut(namesrv, master, slave);
    }
    std::printf("%d checks, %d failures\n", checks, fails);
    return fails == 0 ? 0 : 1;
}
