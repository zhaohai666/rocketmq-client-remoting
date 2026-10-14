// loopback quirk: the failure assertions (E's cannot-connect, D's timeout rounds) all get a budget of >= 8s.
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <map>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "rocketmq/client/producer.h"
#include "rocketmq/common/message.h"
#include "rocketmq/common/net_compat.h"
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

// ---------------------------------------------------------------- socket helpers

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

// select until readable (one round every 100ms so the thread exits quickly once alive turns false), then read one whole frame
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

// a loopback port that was bound and then released immediately: connecting to it is always refused (more reliable than a fixed port)
std::string deadAddress() {
    netcompat::ensureInitialized();
    socket_t s = ::socket(AF_INET, SOCK_STREAM, 0);
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = 0;
    ::bind(s, reinterpret_cast<sockaddr*>(&addr), sizeof(addr));
    ::listen(s, 1);
    socklen_type len = sizeof(addr);
    ::getsockname(s, reinterpret_cast<sockaddr*>(&addr), &len);
    const int port = ntohs(addr.sin_port);
    netcompat::closeSocket(s);
    return "127.0.0.1:" + std::to_string(port);
}

// ---------------------------------------------------------------- mock endpoint

// the scripted response for one SEND request
struct SendReply {
    int32_t code = ResponseCode::SUCCESS;
    int delayMillis = 0;
};

// the evidence captured for one outbound request
struct WireRecord {
    int32_t code = 0;
    PropertyMap ext;
    Bytes body;
    bool hasBody = false;
};

// the answer modes for GET_MAX_OFFSET(30)
enum class ProbeMode {
    ANSWER,   // SUCCESS + offset=0 (the whole broker path is healthy)
    SWALLOW,  // the request is taken but never answered -- the client times out at detectTimeout
};

// plays both the name server (answers with a route) and the broker (answers SEND / GET_MAX_OFFSET per the script).
// It uses the same socket skeleton as MockEndpoint in rmq_test_send_retry.
class DetectorMock {
public:
    DetectorMock() {
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

    ~DetectorMock() {
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

    void scriptSend(std::vector<SendReply> replies) {
        std::lock_guard<std::mutex> lk(state_);
        sends_ = std::move(replies);
        sendCount_.store(0);
    }

    void setProbeMode(ProbeMode mode) { probeMode_.store(static_cast<int>(mode)); }

    // total number of outbound requests with request code code (this is what GET_MAX_OFFSET is counted by)
    int countRequests(int32_t code) {
        std::lock_guard<std::mutex> lk(state_);
        int n = 0;
        for (const WireRecord& r : requestLog_) {
            if (r.code == code) ++n;
        }
        return n;
    }

    // evidence for the i-th request of `code`; out of range returns an empty record
    WireRecord recordOf(size_t i, int32_t code) {
        std::lock_guard<std::mutex> lk(state_);
        size_t seen = 0;
        for (const WireRecord& r : requestLog_) {
            if (r.code != code) continue;
            if (seen++ == i) return r;
        }
        return WireRecord{};
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
                Bytes body;
                bool found = false;
                {
                    std::lock_guard<std::mutex> lk(state_);
                    auto it = routes_.find(topic);
                    if (it != routes_.end()) {
                        body = it->second;
                        found = true;
                    }
                }
                resp.code = found ? ResponseCode::SUCCESS : ResponseCode::TOPIC_NOT_EXIST;
                if (found) {
                    resp.body = body;
                    resp.hasBody = true;
                }
            } else if (req.code == RequestCode::GET_MAX_OFFSET) {
                {
                    std::lock_guard<std::mutex> lk(state_);
                    requestLog_.push_back({req.code, req.extFields, req.body, req.hasBody});
                }
                if (static_cast<ProbeMode>(probeMode_.load()) == ProbeMode::SWALLOW) {
                    continue;  // no reply is sent: the client times out at detectTimeout
                }
                resp.code = ResponseCode::SUCCESS;
                resp.extFields["offset"] = "0";
            } else if (req.code == RequestCode::SEND_MESSAGE_V2
                       || req.code == RequestCode::SEND_MESSAGE
                       || req.code == RequestCode::SEND_BATCH_MESSAGE) {
                const int idx = sendCount_.fetch_add(1);
                SendReply reply;
                {
                    std::lock_guard<std::mutex> lk(state_);
                    if (idx < static_cast<int>(sends_.size())) reply = sends_[idx];
                }
                for (int i = 0; i < reply.delayMillis / 50 && running_.load(); ++i) {
                    std::this_thread::sleep_for(std::chrono::milliseconds(50));
                }
                resp.code = reply.code;
                if (reply.code == ResponseCode::SUCCESS) {
                    resp.extFields["msgId"] = "AC10000100001234567890ABCDEF0001";
                    resp.extFields["queueId"] = "0";
                    resp.extFields["queueOffset"] = std::to_string(1000 + idx);
                }
            } else {
                // HEARTBEAT and the like: always succeed; oneway requests get no response
                if (req.isOnewayRpc()) continue;
                resp.code = ResponseCode::SUCCESS;
            }
            const Bytes out = resp.encode();
            if (!writeAll(sock, out)) break;
        }
        ::shutdown(sock, kShutdownHow);
        netcompat::closeSocket(sock);
    }

    socket_t listen_ = kInvalidSocket;
    uint16_t port_ = 0;
    std::atomic<bool> running_{false};
    std::atomic<int> probeMode_{static_cast<int>(ProbeMode::ANSWER)};
    std::atomic<int> sendCount_{0};
    mutable std::mutex state_;
    std::map<std::string, Bytes> routes_;
    std::vector<SendReply> sends_;
    std::vector<WireRecord> requestLog_;
    std::mutex workersM_;
    std::vector<std::thread> workers_;
    std::thread acceptor_;
};

TopicRouteData makeRoute(const std::string& brokerAddr, const std::string& brokerName) {
    TopicRouteData route;
    route.queueDatas.emplace_back(brokerName, 1, 1, 6, 0);
    std::map<int64_t, std::string> addrs;
    addrs[0] = brokerAddr;
    route.brokerDatas.emplace_back("DefaultCluster", brokerName, addrs);
    return route;
}

Message plainMessage(const std::string& topic) {
    Message m(topic, Bytes{'h', 'i'});
    m.setTags("T1");
    return m;
}

// poll until cond is true or the budget runs out (one round every 100ms)
template <typename Cond>
bool pollFor(Cond&& cond, int budgetMillis) {
    for (int waited = 0; waited < budgetMillis; waited += 100) {
        if (cond()) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    return cond();
}

// ---------------------------------------------------------------- A. defaults / no probing by default

void testDefaults() {
    LatencyFaultToleranceImpl tol;
    expectInt(tol.detectTimeout(), 200, "detectTimeout defaults to 200");
    expectInt(tol.detectInterval(), 2000, "detectInterval defaults to 2000");
    expect(!tol.isStartDetectorEnable(), "startDetectorEnable defaults to false");

    MQFaultStrategy s;
    expect(!s.isStartDetectorEnable(), "the strategy does not enable probing by default");
    s.setStartDetectorEnable(true);
    expect(s.isStartDetectorEnable(), "the strategy switch is forwarded to the tolerance table");
    expect(s.latencyFaultTolerance().isStartDetectorEnable(), "the tolerance table receives the switch");

    DefaultMQProducer p("PG_detect_defaults");
    expect(!p.isStartDetectorEnable(), "the producer does not enable probing by default");
    expectInt(p.getDetectTimeout(), 200, "the producer defaults to detectTimeout=200");
    expectInt(p.getDetectInterval(), 2000, "the producer defaults to detectInterval=2000");
    p.setStartDetectorEnable(true);
    p.setDetectTimeout(1234);
    p.setDetectInterval(5678);
    expect(p.isStartDetectorEnable(), "setStartDetectorEnable takes effect");
    expectInt(p.getDetectTimeout(), 1234, "setDetectTimeout takes effect");
    expectInt(p.getDetectInterval(), 5678, "setDetectInterval takes effect");
}

// The switch is off by default: a failed send fills the fault table, yet probing still issues no GET_MAX_OFFSET
void testDisabledByDefault(DetectorMock& mock) {
    const std::string topic = "DetectDefaultOff";
    mock.addRoute(topic, makeRoute(mock.address(), "broker-a"));
    mock.scriptSend({{ResponseCode::SERVICE_NOT_AVAILABLE, 0}});
    DefaultMQProducer p("PG_detect_off");
    p.setNamesrvAddr(mock.address());
    p.setSendLatencyFaultEnable(true);
    p.setRetryTimesWhenSendFailed(0);
    p.start();
    try {
        p.send(plainMessage(topic), 3000);
    } catch (const std::exception&) {
    }
    expect(p.mqFaultStrategy().latencyFaultTolerance().getFaultItem("broker-a") != nullptr,
           "the fault item is in the table (otherwise 0 probes would be meaningless)");
    std::this_thread::sleep_for(std::chrono::seconds(4));
    expectInt(mock.countRequests(RequestCode::GET_MAX_OFFSET), 0,
              "zero probes while the switch is off");
    expect(!p.mqFaultStrategy().latencyFaultTolerance().isReachable("broker-a"),
           "nobody puts the broker back");
    p.shutdown();
}

// ---------------------------------------------------------------- B. detectByOneRound offline semantics

void testDetectByOneRoundOffline() {
    // flipping back to reachable + a failure leaves the flag alone + slot-reservation rate limiting
    {
        LatencyFaultToleranceImpl tol;
        tol.setDetectInterval(200);
        int probes = 0;
        int lastTimeout = -1;
        tol.setResolver([](const std::string&) { return std::string("127.0.0.1:10911"); });
        tol.setServiceDetector([&probes, &lastTimeout](const std::string& addr, int32_t timeout) {
            (void)addr;
            ++probes;
            lastTimeout = timeout;
            return true;
        });
        tol.updateFaultItem("broker-a", 1.0, 0, /*reachable=*/false);
        tol.detectByOneRound();
        expectInt(probes, 1, "an expired fault item is probed once");
        expectInt(lastTimeout, 200, "the probe carries detectTimeout");
        expect(tol.isReachable("broker-a"), "a successful probe flips it back to reachable");

        tol.updateFaultItem("broker-b", 1.0, 0, false);
        tol.setServiceDetector([&probes](const std::string&, int32_t) {
            ++probes;
            return false;
        });
        probes = 0;
        std::this_thread::sleep_for(std::chrono::milliseconds(250));
        tol.detectByOneRound();
        expectInt(probes, 2, "after detectInterval expires both items are probed once each");
        expect(!tol.isReachable("broker-b"), "a failed probe leaves the reachable flag untouched");
        expect(tol.isReachable("broker-a"), "an already reachable broker is not blacklisted by a failed probe");

        // slot reservation: run another round immediately, nobody should be probed
        probes = 0;
        tol.detectByOneRound();
        expectInt(probes, 0, "checkStamp reserves the slot: no repeated probe inside detectInterval");
    }
    {
        LatencyFaultToleranceImpl tol;
        int probes = 0;
        tol.setResolver([](const std::string&) { return std::string(); });
        tol.setServiceDetector([&probes](const std::string&, int32_t) {
            ++probes;
            return true;
        });
        tol.updateFaultItem("broker-gone", 1.0, 0, false);
        tol.detectByOneRound();
        expectInt(probes, 0, "no probe when the address cannot be resolved");
        expect(tol.getFaultItem("broker-gone") == nullptr, "the fault item is removed when the address cannot be resolved");
        expect(tol.isReachable("broker-gone"), "back to the default reachable state after removal");
    }
    // no crash when no resolver / detector is installed
    {
        LatencyFaultToleranceImpl tol;
        tol.updateFaultItem("broker-x", 1.0, 0, false);
        tol.detectByOneRound();  // no resolver: the whole round is skipped
        expect(tol.getFaultItem("broker-x") != nullptr, "nothing changes without a resolver");
        tol.setResolver([](const std::string&) { return std::string("127.0.0.1:1"); });
        tol.detectByOneRound();  // no detector: this item is skipped
        expect(tol.getFaultItem("broker-x") != nullptr, "nothing changes without a detector");
        expect(!tol.isReachable("broker-x"), "without a detector nothing is let through by mistake");
    }
}

// ---------------------------------------------------------------- C. periodic trigger + release after recovery

void testPeriodicAndRecovery(DetectorMock& mock) {
    const std::string topic = "DetectRecover";
    mock.addRoute(topic, makeRoute(mock.address(), "broker-a"));
    mock.scriptSend({{ResponseCode::SERVICE_NOT_AVAILABLE, 0}});
    mock.setProbeMode(ProbeMode::ANSWER);

    DefaultMQProducer p("PG_detect_recover");
    p.setNamesrvAddr(mock.address());
    p.setSendLatencyFaultEnable(true);
    p.setStartDetectorEnable(true);
    p.setDetectInterval(500);
    p.setRetryTimesWhenSendFailed(0);
    p.start();
    try {
        p.send(plainMessage(topic), 3000);
    } catch (const std::exception&) {
    }
    LatencyFaultToleranceImpl& tol = p.mqFaultStrategy().latencyFaultTolerance();
    expect(!tol.isReachable("broker-a"), "the broker is recorded as unreachable after a failure");
    expect(!tol.isAvailable("broker-a"), "the broker enters isolation after a failure");

    // the probe cycle ticks every 3s: wait for the detector to put it back (12s budget, the first tick should already hit)
    const bool recovered = pollFor(
        [&tol]() {
            return tol.isReachable("broker-a");
        },
        12000);
    expect(recovered, "probing restores reachable (let through after recovery)");

    const WireRecord probe = mock.recordOf(0, RequestCode::GET_MAX_OFFSET);
    expectInt(probe.code, RequestCode::GET_MAX_OFFSET, "the probe uses GET_MAX_OFFSET(30)");
    const auto t = probe.ext.find("topic");
    expect(t != probe.ext.end() && t->second == topic, "the probe topic is one of the topics that were sent to");
    const auto q = probe.ext.find("queueId");
    expect(q != probe.ext.end() && q->second == "0", "the probe queueId is fixed at 0");

    // periodic trigger: wait one more tick, the probe count must keep growing
    const int afterFirst = mock.countRequests(RequestCode::GET_MAX_OFFSET);
    const bool periodic = pollFor(
        [&mock, afterFirst]() {
            return mock.countRequests(RequestCode::GET_MAX_OFFSET) > afterFirst;
        },
        8000);
    expect(periodic, "probes are triggered periodically at a fixed cadence");
    p.shutdown();
}

// ---------------------------------------------------------------- D. a timeout marks it unavailable

void testTimeoutKeepsUnreachable(DetectorMock& mock) {
    const std::string topic = "DetectTimeout";
    mock.addRoute(topic, makeRoute(mock.address(), "broker-a"));
    mock.scriptSend({{ResponseCode::SERVICE_NOT_AVAILABLE, 0}});
    // the probe is swallowed (no reply): the client times out at detectTimeout (200ms by default)
    mock.setProbeMode(ProbeMode::SWALLOW);

    DefaultMQProducer p("PG_detect_timeout");
    p.setNamesrvAddr(mock.address());
    p.setSendLatencyFaultEnable(true);
    p.setStartDetectorEnable(true);
    p.setDetectInterval(500);
    p.setRetryTimesWhenSendFailed(0);
    p.start();
    try {
        p.send(plainMessage(topic), 3000);
    } catch (const std::exception&) {
    }
    LatencyFaultToleranceImpl& tol = p.mqFaultStrategy().latencyFaultTolerance();
    expect(!tol.isReachable("broker-a"), "the broker is recorded as unreachable after a failure");

    // at least two probe rounds both time out (a 3s tick plus a 200ms timeout per round, 10s budget); a timeout must not let it through
    const bool probed = pollFor(
        [&mock]() {
            return mock.countRequests(RequestCode::GET_MAX_OFFSET) >= 2;
        },
        10000);
    expect(probed, "the probe really ran at least two rounds (each one times out at detectTimeout)");
    expect(!tol.isReachable("broker-a"), "a probe timeout cannot put the broker back");

    // after the broker starts answering: the next probe round lets it through as usual
    mock.setProbeMode(ProbeMode::ANSWER);
    const bool recovered = pollFor(
        [&tol]() {
            return tol.isReachable("broker-a");
        },
        10000);
    expect(recovered, "the broker is still put back once it resumes answering after the timeouts");
    p.shutdown();
}

// ---------------------------------------------------------------- E. a transport failure records unreachable

void testConnectFailureMarksUnreachable(DetectorMock& mock) {
    const std::string topic = "DetectConnect";
    mock.addRoute(topic, makeRoute(deadAddress(), "broker-a"));
    DefaultMQProducer p("PG_detect_connect");
    p.setNamesrvAddr(mock.address());
    p.setSendLatencyFaultEnable(true);
    p.setStartDetectorEnable(true);
    p.setRetryTimesWhenSendFailed(0);
    p.start();
    try {
        // loopback quirk: connecting to a just-closed port takes ~2-3s, so the budget is generous
        p.send(plainMessage(topic), 8000);
    } catch (const std::exception&) {
    }
    const FaultItem* item = p.mqFaultStrategy().latencyFaultTolerance().getFaultItem("broker-a");
    expect(item != nullptr, "a transport failure records a fault item");
    if (item != nullptr) {
        expect(!item->isReachable(), "probing on: a transport failure records unreachable (the detector puts it back)");
        expect(!item->isAvailable(), "a transport failure also isolates that broker");
    }
    p.shutdown();
}

}  // namespace

int main() {
    testDefaults();
    testDetectByOneRoundOffline();

    {
        DetectorMock mock;
        testDisabledByDefault(mock);
        testPeriodicAndRecovery(mock);
    }
    {
        DetectorMock mock;
        testTimeoutKeepsUnreachable(mock);
    }
    {
        DetectorMock mock;
        testConnectFailureMarksUnreachable(mock);
    }

    std::printf("fault detector: %d checks, %d failures\n", checks, fails);
    return fails == 0 ? 0 : 1;
}
