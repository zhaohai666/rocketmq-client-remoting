// loopback 怪癖：失败类断言（E 的连不上、D 的超时轮询）都给了 ≥8s 预算。
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

// ---------------------------------------------------------------- socket 工具

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

// select 等到可读（100ms 一轮，便于 alive 转 false 后尽快退出），再读一整帧
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

// 已被占用过、随后立即释放的回环端口：连它必然被拒（比固定端口可靠）
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

// ---------------------------------------------------------------- mock 端点

// 一条 SEND 请求的脚本化响应
struct SendReply {
    int32_t code = ResponseCode::SUCCESS;
    int delayMillis = 0;
};

// 一条上线报文的取证
struct WireRecord {
    int32_t code = 0;
    PropertyMap ext;
    Bytes body;
    bool hasBody = false;
};

// GET_MAX_OFFSET(30) 的应答模式
enum class ProbeMode {
    ANSWER,   // SUCCESS + offset=0（broker 全链路正常）
    SWALLOW,  // 收下请求不回包 —— 客户端在 detectTimeout 处超时
};

// 同时扮演 name server（回路由）与 broker（按脚本回 SEND / GET_MAX_OFFSET）。
// 与 rmq_test_send_retry 的 MockEndpoint 同一套 socket 骨架。
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

    // 请求码为 code 的上线报文总条数（GET_MAX_OFFSET 计数就用它）
    int countRequests(int32_t code) {
        std::lock_guard<std::mutex> lk(state_);
        int n = 0;
        for (const WireRecord& r : requestLog_) {
            if (r.code == code) ++n;
        }
        return n;
    }

    // 第 i 笔 `code` 请求的取证；越界返回空记录
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
                    continue;  // 不回包：客户端按 detectTimeout 超时
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
                // HEARTBEAT 等：一律成功；oneway 请求不回响应
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

// 轮询到 cond 为真或预算耗尽（100ms 一轮）
template <typename Cond>
bool pollFor(Cond&& cond, int budgetMillis) {
    for (int waited = 0; waited < budgetMillis; waited += 100) {
        if (cond()) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    return cond();
}

// ---------------------------------------------------------------- A. 默认值 / 默认不探测

void testDefaults() {
    LatencyFaultToleranceImpl tol;
    expectInt(tol.detectTimeout(), 200, "detectTimeout 默认 200");
    expectInt(tol.detectInterval(), 2000, "detectInterval 默认 2000");
    expect(!tol.isStartDetectorEnable(), "startDetectorEnable 默认 false");

    MQFaultStrategy s;
    expect(!s.isStartDetectorEnable(), "策略默认不开探测");
    s.setStartDetectorEnable(true);
    expect(s.isStartDetectorEnable(), "策略开关转发到容忍表");
    expect(s.latencyFaultTolerance().isStartDetectorEnable(), "容忍表收到开关");

    DefaultMQProducer p("PG_detect_defaults");
    expect(!p.isStartDetectorEnable(), "producer 默认不开探测");
    expectInt(p.getDetectTimeout(), 200, "producer 默认 detectTimeout=200");
    expectInt(p.getDetectInterval(), 2000, "producer 默认 detectInterval=2000");
    p.setStartDetectorEnable(true);
    p.setDetectTimeout(1234);
    p.setDetectInterval(5678);
    expect(p.isStartDetectorEnable(), "setStartDetectorEnable 生效");
    expectInt(p.getDetectTimeout(), 1234, "setDetectTimeout 生效");
    expectInt(p.getDetectInterval(), 5678, "setDetectInterval 生效");
}

// 开关默认关：发了失败消息把故障表填上，探测也不会发一笔 GET_MAX_OFFSET
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
           "故障项已入表（否则 0 笔探测没有意义）");
    std::this_thread::sleep_for(std::chrono::seconds(4));
    expectInt(mock.countRequests(RequestCode::GET_MAX_OFFSET), 0,
              "开关关闭时零笔探测");
    expect(!p.mqFaultStrategy().latencyFaultTolerance().isReachable("broker-a"),
           "没有人把 broker 放回");
    p.shutdown();
}

// ---------------------------------------------------------------- B. detectByOneRound 离线语义

void testDetectByOneRoundOffline() {
    // 翻回 reachable + 失败不动标志 + 占坑限频
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
        expectInt(probes, 1, "到期的故障项被探测一次");
        expectInt(lastTimeout, 200, "探测带上 detectTimeout");
        expect(tol.isReachable("broker-a"), "探测成功翻回 reachable");

        tol.updateFaultItem("broker-b", 1.0, 0, false);
        tol.setServiceDetector([&probes](const std::string&, int32_t) {
            ++probes;
            return false;
        });
        probes = 0;
        std::this_thread::sleep_for(std::chrono::milliseconds(250));
        tol.detectByOneRound();
        expectInt(probes, 2, "detectInterval 到期后两项各探测一次");
        expect(!tol.isReachable("broker-b"), "探测失败不动 reachable 标志");
        expect(tol.isReachable("broker-a"), "已可达的 broker 不被失败探测拉黑");

        // 占坑限频：立刻再跑一轮，谁都不该被探测
        probes = 0;
        tol.detectByOneRound();
        expectInt(probes, 0, "checkStamp 占坑：detectInterval 内不重复探测");
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
        expectInt(probes, 0, "解析不到地址不探测");
        expect(tol.getFaultItem("broker-gone") == nullptr, "解析不到地址摘除故障项");
        expect(tol.isReachable("broker-gone"), "摘除后回到默认可达");
    }
    // 没装 resolver / detector 不炸
    {
        LatencyFaultToleranceImpl tol;
        tol.updateFaultItem("broker-x", 1.0, 0, false);
        tol.detectByOneRound();  // 没有 resolver：整跳放弃
        expect(tol.getFaultItem("broker-x") != nullptr, "没有 resolver 什么都不动");
        tol.setResolver([](const std::string&) { return std::string("127.0.0.1:1"); });
        tol.detectByOneRound();  // 没有 detector：跳过该项
        expect(tol.getFaultItem("broker-x") != nullptr, "没有 detector 什么都不动");
        expect(!tol.isReachable("broker-x"), "没有 detector 不会误放行");
    }
}

// ---------------------------------------------------------------- C. 周期触发 + 恢复放行

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
    expect(!tol.isReachable("broker-a"), "broker 失败后记为不可达");
    expect(!tol.isAvailable("broker-a"), "broker 失败后进隔离");

    // 探测周期 3s 一跳：等探测器把它放回（预算 12s，实际第一跳就该命中）
    const bool recovered = pollFor(
        [&tol]() {
            return tol.isReachable("broker-a");
        },
        12000);
    expect(recovered, "探测恢复 reachable（恢复后放行）");

    const WireRecord probe = mock.recordOf(0, RequestCode::GET_MAX_OFFSET);
    expectInt(probe.code, RequestCode::GET_MAX_OFFSET, "探测走 GET_MAX_OFFSET(30)");
    const auto t = probe.ext.find("topic");
    expect(t != probe.ext.end() && t->second == topic, "探测 topic 取发送过的候选 topic");
    const auto q = probe.ext.find("queueId");
    expect(q != probe.ext.end() && q->second == "0", "探测 queueId 固定 0");

    // 周期触发：再等一跳，探测笔数必须继续增长
    const int afterFirst = mock.countRequests(RequestCode::GET_MAX_OFFSET);
    const bool periodic = pollFor(
        [&mock, afterFirst]() {
            return mock.countRequests(RequestCode::GET_MAX_OFFSET) > afterFirst;
        },
        8000);
    expect(periodic, "探测按固定节奏周期触发");
    p.shutdown();
}

// ---------------------------------------------------------------- D. 超时置不可用

void testTimeoutKeepsUnreachable(DetectorMock& mock) {
    const std::string topic = "DetectTimeout";
    mock.addRoute(topic, makeRoute(mock.address(), "broker-a"));
    mock.scriptSend({{ResponseCode::SERVICE_NOT_AVAILABLE, 0}});
    // 探测被吞（不回包）：客户端在 detectTimeout（默认 200ms）处超时
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
    expect(!tol.isReachable("broker-a"), "broker 失败后记为不可达");

    // 至少两轮探测都超时（每轮 3s 跳 + 200ms 超时，预算 10s）—— 超时不能放行
    const bool probed = pollFor(
        [&mock]() {
            return mock.countRequests(RequestCode::GET_MAX_OFFSET) >= 2;
        },
        10000);
    expect(probed, "探测确实跑了两轮以上（每笔在 detectTimeout 处超时）");
    expect(!tol.isReachable("broker-a"), "探测超时不能把 broker 放回");

    // broker 开始应答之后：下一轮探测照常放行
    mock.setProbeMode(ProbeMode::ANSWER);
    const bool recovered = pollFor(
        [&tol]() {
            return tol.isReachable("broker-a");
        },
        10000);
    expect(recovered, "超时之后 broker 恢复应答仍能被放回");
    p.shutdown();
}

// ---------------------------------------------------------------- E. 传输失败记 unreachable

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
        // loopback 怪癖：连一个刚关掉的端口要 ~2-3s，预算给足
        p.send(plainMessage(topic), 8000);
    } catch (const std::exception&) {
    }
    const FaultItem* item = p.mqFaultStrategy().latencyFaultTolerance().getFaultItem("broker-a");
    expect(item != nullptr, "传输失败记录故障项");
    if (item != nullptr) {
        expect(!item->isReachable(), "探测开着：传输失败记 unreachable（等探测器放回）");
        expect(!item->isAvailable(), "传输失败同时隔离该 broker");
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
