// Offline unit test for the client-level broker VIP channel switch (port - 2), no cluster needed.
//
// What it pins: with the switch ON the rewritten family (an offset request and a queue lock) is sent
// to port - 2 of the advertised route address, while with the switch OFF (the default) the same request
// is sent to the plain port. It also pins that the never-rewritten family -- HEART_BEAT, PULL, POP, ACK,
// UNREGISTER_CLIENT and the recall request -- stays on the plain port even when the switch is ON.
//
// The administration facade is covered on both halves: the broker-wide reads it issues itself and the
// offset / consumer-list reads it delegates to the shared client instance follow the switch, while the
// read of a broker's own configuration stays on the listening port whatever the switch says.
//
// The broker's VIP channel is the route address minus 2 (10911 -> 10909); a name-server request is
// never shifted. To make the destination observable offline, the route address is advertised on a plain
// listener while a second listener is bound two ports below it: whichever listener receives the request
// frame tells us which port the client actually dialed. Two ports with this exact relationship are
// allocated together so the pair is stable for the whole run.
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "rocketmq/client/admin.h"
#include "rocketmq/client/mq_client.h"
#include "rocketmq/common/byte_buffer.h"
#include "rocketmq/common/mix_all.h"
#include "rocketmq/common/net_compat.h"
#include "rocketmq/common/subscription_data.h"
#include "rocketmq/remoting/protocol/codes.h"
#include "rocketmq/remoting/protocol/remoting_command.h"

using namespace rocketmq;

// Must be at rocketmq namespace scope -- the friend declaration in mq_client.h binds the name there,
// and one in an anonymous namespace would be a different type. Grants the test direct write access to
// the private broker address table so a queue lock can resolve a broker name without a name server.
namespace rocketmq {
struct MQClientInstanceTestAccess {
    static void seedBrokerAddr(MQClientInstance& inst, const std::string& brokerName,
                               int64_t brokerId, const std::string& addr) {
        inst.brokerAddrTable_[brokerName][brokerId] = addr;
    }
};
}  // namespace rocketmq

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

// ---------------------------------------------------------------- frame helpers

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

// select until readable (a 100ms round so it exits promptly once alive is false), then read one whole frame.
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

// ---------------------------------------------------------------- capturing listener

// Binds one explicit loopback port, records every request code it receives and answers SUCCESS so a
// synchronous client call returns immediately. Which listener saw a frame is the observable that
// reveals the port the client dialed.
class CapturingListener {
public:
    CapturingListener(socket_t listenFd, uint16_t port)
        : listen_(listenFd), port_(port) {
        running_.store(true);
        acceptor_ = std::thread([this]() { acceptLoop(); });
    }

    ~CapturingListener() {
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

    CapturingListener(const CapturingListener&) = delete;
    CapturingListener& operator=(const CapturingListener&) = delete;

    std::string address() const { return "127.0.0.1:" + std::to_string(port_); }

    void clear() {
        std::lock_guard<std::mutex> lk(state_);
        codes_.clear();
    }

    int countOf(int32_t code) {
        std::lock_guard<std::mutex> lk(state_);
        auto it = codes_.find(code);
        return it == codes_.end() ? 0 : it->second;
    }

    int totalCount() {
        std::lock_guard<std::mutex> lk(state_);
        int n = 0;
        for (const auto& kv : codes_) n += kv.second;
        return n;
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
            if (req.isResponseType()) {
                continue;  // A response frame is not an incoming request; do not record it.
            }
            {
                std::lock_guard<std::mutex> lk(state_);
                ++codes_[req.code];
            }
            RemotingCommand resp;
            resp.opaque = req.opaque;
            resp.markResponseType();
            resp.code = ResponseCode::SUCCESS;
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
    std::map<int32_t, int> codes_;
    std::mutex workersM_;
    std::vector<std::thread> workers_;
    std::thread acceptor_;
};

// ---------------------------------------------------------------- port pair allocation

socket_t tryListen(uint16_t port) {
    socket_t s = ::socket(AF_INET, SOCK_STREAM, 0);
    if (!netcompat::isValid(s)) return kInvalidSocket;
    int one = 1;
    ::setsockopt(s, SOL_SOCKET, SO_REUSEADDR, reinterpret_cast<const char*>(&one),
                 static_cast<int>(sizeof(one)));
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = htons(port);
    if (::bind(s, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
        netcompat::closeSocket(s);
        return kInvalidSocket;
    }
    if (::listen(s, 16) != 0) {
        netcompat::closeSocket(s);
        return kInvalidSocket;
    }
    return s;
}

// Find a plain port whose (port - 2) is also free; bind both and hand the sockets to the listeners.
bool allocateVipPair(socket_t& plainFd, socket_t& vipFd, uint16_t& plainPort) {
    netcompat::ensureInitialized();
    for (uint32_t p = 40000; p + 2 <= 60000; ++p) {
        socket_t a = tryListen(static_cast<uint16_t>(p));
        if (!netcompat::isValid(a)) continue;
        socket_t b = tryListen(static_cast<uint16_t>(p - 2));
        if (!netcompat::isValid(b)) {
            netcompat::closeSocket(a);
            continue;
        }
        plainFd = a;
        vipFd = b;
        plainPort = static_cast<uint16_t>(p);
        return true;
    }
    return false;
}

// ---------------------------------------------------------------- scaffolding

const char* kGroup = "GID_VipChannel";
const char* kTopic = "VipChannelTopic";
const char* kBroker = "broker-a";
const char* kClientId = "127.0.0.1@vip#1";

MessageQueue testMq() { return MessageQueue(kTopic, kBroker, 0); }

std::unique_ptr<MQClientInstance> makeInstance(bool vip) {
    auto inst = std::make_unique<MQClientInstance>(
        kClientId, std::vector<std::string>{"127.0.0.1:1"});
    inst->setVipChannelEnabled(vip);
    // Queue locks resolve a broker name through the publish table; seed it with the plain port so the
    // test does not need a name server (findBrokerAddressInPublish is what lockBatchMq reads).
    return inst;
}

// Poll a predicate a bounded number of times: a synchronous client call returns after the frame was
// already recorded, but settle any residual timing so a "must stay at zero" check is meaningful.
bool eventually(const std::function<bool()>& pred, int maxMs) {
    for (int waited = 0; waited <= maxMs; waited += 10) {
        if (pred()) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    return pred();
}

void runQuietly(const std::function<void()>& fn) {
    try {
        fn();
    } catch (const std::exception&) {
        // A scripted SUCCESS response is not always parseable by every request type; the destination
        // (already captured by the listener before the reply) is the only thing under test here.
    }
}

}  // namespace

int main() {
    socket_t plainFd = kInvalidSocket;
    socket_t vipFd = kInvalidSocket;
    uint16_t plainPort = 0;
    if (!allocateVipPair(plainFd, vipFd, plainPort)) {
        std::printf("vip_channel: could not allocate a (port, port-2) pair; test skipped\n");
        return 0;
    }
    CapturingListener plain(plainFd, plainPort);
    CapturingListener vip(vipFd, static_cast<uint16_t>(plainPort - 2));
    const std::string plainAddr = plain.address();

    // (i) offset + lock go to port-2 when the switch is ON.
    {
        plain.clear();
        vip.clear();
        auto inst = makeInstance(true);
        MQClientInstanceTestAccess::seedBrokerAddr(*inst, kBroker, MixAll::MASTER_ID, plainAddr);

        int64_t outOffset = -1;
        runQuietly([&] { inst->queryConsumerOffset(kGroup, testMq(), outOffset, 3000, plainAddr); });
        expect(eventually([&] { return vip.countOf(RequestCode::QUERY_CONSUMER_OFFSET) == 1; }, 1000),
               "vip_on.queryOffset.arrivesAtPortMinus2");
        expect(plain.countOf(RequestCode::QUERY_CONSUMER_OFFSET) == 0,
               "vip_on.queryOffset.plainPortIdle");

        runQuietly([&] { inst->lockBatchMq(kGroup, kClientId, {testMq()}, 3000); });
        expect(eventually([&] { return vip.countOf(RequestCode::LOCK_BATCH_MQ) == 1; }, 1000),
               "vip_on.lock.arrivesAtPortMinus2");
        expect(plain.countOf(RequestCode::LOCK_BATCH_MQ) == 0, "vip_on.lock.plainPortIdle");
    }

    // (i) offset + lock go to the plain port when the switch is OFF (the default behavior).
    {
        plain.clear();
        vip.clear();
        auto inst = makeInstance(false);
        MQClientInstanceTestAccess::seedBrokerAddr(*inst, kBroker, MixAll::MASTER_ID, plainAddr);

        int64_t outOffset = -1;
        runQuietly([&] { inst->queryConsumerOffset(kGroup, testMq(), outOffset, 3000, plainAddr); });
        expect(eventually([&] { return plain.countOf(RequestCode::QUERY_CONSUMER_OFFSET) == 1; }, 1000),
               "vip_off.queryOffset.arrivesAtPlainPort");
        expect(vip.countOf(RequestCode::QUERY_CONSUMER_OFFSET) == 0,
               "vip_off.queryOffset.vipPortIdle");

        runQuietly([&] { inst->lockBatchMq(kGroup, kClientId, {testMq()}, 3000); });
        expect(eventually([&] { return plain.countOf(RequestCode::LOCK_BATCH_MQ) == 1; }, 1000),
               "vip_off.lock.arrivesAtPlainPort");
        expect(vip.countOf(RequestCode::LOCK_BATCH_MQ) == 0, "vip_off.lock.vipPortIdle");
    }

    // (ii) the never-rewritten family stays on the plain port even when the switch is ON.
    {
        auto inst = makeInstance(true);
        HeartbeatData hb(kClientId);
        RecallMessageRequestHeader recallHeader;

        plain.clear();
        vip.clear();
        runQuietly([&] { inst->sendHeartbeat(plainAddr, hb, 3000); });
        expect(eventually([&] { return plain.countOf(RequestCode::HEART_BEAT) == 1; }, 1000),
               "vip_on.heartbeat.staysPlain");
        expect(vip.countOf(RequestCode::HEART_BEAT) == 0, "vip_on.heartbeat.notShifted");

        plain.clear();
        vip.clear();
        runQuietly([&] {
            inst->pullMessage(kGroup, testMq(), /*queueOffset=*/0, /*maxMsgNums=*/32, /*sysFlag=*/0,
                              /*commitOffset=*/0, "*", /*subVersion=*/0, ExpressionType::TAG, 3000,
                              -1, 15000, plainAddr);
        });
        expect(eventually([&] { return plain.countOf(RequestCode::PULL_MESSAGE) == 1; }, 1000),
               "vip_on.pull.staysPlain");
        expect(vip.countOf(RequestCode::PULL_MESSAGE) == 0, "vip_on.pull.notShifted");

        plain.clear();
        vip.clear();
        runQuietly([&] {
            inst->popMessage(kGroup, kTopic, 0, 32, 60000, 10000, 0, std::string(),
                             std::string(), false, 3000, kBroker, plainAddr);
        });
        expect(eventually([&] { return plain.countOf(RequestCode::POP_MESSAGE) == 1; }, 1000),
               "vip_on.pop.staysPlain");
        expect(vip.countOf(RequestCode::POP_MESSAGE) == 0, "vip_on.pop.notShifted");

        plain.clear();
        vip.clear();
        runQuietly([&] {
            inst->ackMessage(kGroup, kTopic, 0, "extra-info", 1, 3000, kBroker, plainAddr);
        });
        expect(eventually([&] { return plain.countOf(RequestCode::ACK_MESSAGE) == 1; }, 1000),
               "vip_on.ack.staysPlain");
        expect(vip.countOf(RequestCode::ACK_MESSAGE) == 0, "vip_on.ack.notShifted");

        plain.clear();
        vip.clear();
        runQuietly([&] {
            inst->unregisterClient(plainAddr, kClientId, kGroup, std::string(), 3000);
        });
        expect(eventually([&] { return plain.countOf(RequestCode::UNREGISTER_CLIENT) == 1; }, 1000),
               "vip_on.unregister.staysPlain");
        expect(vip.countOf(RequestCode::UNREGISTER_CLIENT) == 0, "vip_on.unregister.notShifted");

        plain.clear();
        vip.clear();
        runQuietly([&] { inst->recallMessage(plainAddr, recallHeader, 3000); });
        expect(eventually([&] { return plain.countOf(RequestCode::RECALL_MESSAGE) == 1; }, 1000),
               "vip_on.recall.staysPlain");
        expect(vip.countOf(RequestCode::RECALL_MESSAGE) == 0, "vip_on.recall.notShifted");
    }

    // (iv) the administration facade: its own broker-wide reads move, the delegated offset and
    // consumer-list reads move too, and the read of a broker's own configuration never moves.
    // The delegated half is the part that silently breaks when the admin keeps the switch to
    // itself, so the same case flips the switch after start() and expects the landing port to
    // follow in both directions.
    {
        plain.clear();
        vip.clear();
        DefaultMQAdminExt admin("ADMIN_VipChannelTest");
        admin.setNamesrvAddr(plainAddr);
        admin.setTimeoutMillis(3000);
        admin.setVipChannelEnabled(true);
        admin.start();
        // The delegated reads resolve a broker name through the publish table, so seed it with
        // the plain port: the route cache then holds exactly what a name server would advertise.
        MQClientInstanceTestAccess::seedBrokerAddr(admin.getMQClientInstance(), kBroker,
                                                   MixAll::MASTER_ID, plainAddr);

        runQuietly([&] { admin.fetchBrokerRuntimeStats(plainAddr); });
        runQuietly([&] { admin.getAllTopicConfig(plainAddr); });
        runQuietly([&] { admin.maxOffset(testMq()); });
        runQuietly([&] { admin.getConsumerListByGroup(kGroup, plainAddr); });
        expect(eventually([&] { return vip.countOf(RequestCode::GET_BROKER_RUNTIME_INFO) == 1; }, 1000),
               "vip_admin.runtimeStats.arrivesAtPortMinus2");
        expect(plain.countOf(RequestCode::GET_BROKER_RUNTIME_INFO) == 0,
               "vip_admin.runtimeStats.plainPortIdle");
        expect(vip.countOf(RequestCode::GET_ALL_TOPIC_CONFIG) == 1,
               "vip_admin.allTopicConfig.arrivesAtPortMinus2");
        expect(vip.countOf(RequestCode::GET_MAX_OFFSET) == 1,
               "vip_admin.delegatedMaxOffset.arrivesAtPortMinus2");
        expect(vip.countOf(RequestCode::GET_CONSUMER_LIST_BY_GROUP) == 1,
               "vip_admin.delegatedConsumerList.arrivesAtPortMinus2");

        // Reading a broker's own configuration answers on the listening port only: aiming it at
        // the fast channel would dial a port that is closed whenever the channel is off.
        runQuietly([&] { admin.getBrokerConfig(plainAddr); });
        expect(eventually([&] { return plain.countOf(RequestCode::GET_BROKER_CONFIG) == 1; }, 1000),
               "vip_admin.brokerConfig.staysPlain");
        expect(vip.countOf(RequestCode::GET_BROKER_CONFIG) == 0, "vip_admin.brokerConfig.notShifted");

        // The same read with the switch turned off after start(): the delegated family goes back
        // to the listening port, which only happens if the switch reached the instance.
        plain.clear();
        vip.clear();
        admin.setVipChannelEnabled(false);
        runQuietly([&] { admin.maxOffset(testMq()); });
        runQuietly([&] { admin.fetchBrokerRuntimeStats(plainAddr); });
        runQuietly([&] { admin.getBrokerConfig(plainAddr); });
        expect(eventually([&] { return plain.countOf(RequestCode::GET_MAX_OFFSET) == 1; }, 1000),
               "vip_off_admin.delegatedMaxOffset.staysPlain");
        expect(vip.countOf(RequestCode::GET_MAX_OFFSET) == 0, "vip_off_admin.delegatedMaxOffset.notShifted");
        expect(plain.countOf(RequestCode::GET_BROKER_RUNTIME_INFO) == 1,
               "vip_off_admin.runtimeStats.staysPlain");
        expect(plain.countOf(RequestCode::GET_BROKER_CONFIG) == 1,
               "vip_off_admin.brokerConfig.staysPlain");
        admin.shutdown();
    }

    // The switch reads back exactly what was set, and the default is off.
    {
        auto inst = makeInstance(false);
        expect(!inst->isVipChannelEnabled(), "default.vipChannelOff");
        inst->setVipChannelEnabled(true);
        expect(inst->isVipChannelEnabled(), "setter.readBackOn");
    }

    std::printf("vip_channel: %d checks, %d failures\n", checks, fails);
    return fails == 0 ? 0 : 1;
}
