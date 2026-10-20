// Offline unit tests for the lite-pull consumer's **pull cursor** (#105) -- fake name server + fake broker (real sockets), never touching a real cluster.
//
//
// Why this must be pinned offline: the old implementation advanced the cursor only on FOUND, via
// `msgs.back().queueOffset + 1`, so a NO_MATCHED_MSG reply (the whole span scanned this round matched
// nothing) left the cursor where it was -- the next round re-scanned the same span from the same offset
// and spun forever; OFFSET_ILLEGAL's correction was ignored too, so an out-of-range offset never healed.
// On real hardware both looked like "the consumer is alive but never receives anything", hard to attribute.
// On the live side another chain carries the same symptom: the S4/S5/S6 legs of examples/live_lite_pull.cpp.
//
// The evidence comes from the **wire**: the fake broker answers per script and hands back the extFields
// of every PULL_MESSAGE read off the socket -- "the cursor really moved to 5" is judged by the next request starting at 5, not by an in-memory table.
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstdio>
#include <deque>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "rocketmq/client/exception.h"
#include "rocketmq/client/lite_pull_consumer.h"
#include "rocketmq/client/pull_consumer.h"
#include "rocketmq/client/result.h"
#include "rocketmq/common/message.h"
#include "rocketmq/common/message_decoder.h"
#include "rocketmq/common/mix_all.h"
#include "rocketmq/common/net_compat.h"
#include "rocketmq/common/sysflag.h"
#include "rocketmq/common/util_all.h"
#include "rocketmq/remoting/protocol/codes.h"
#include "rocketmq/remoting/protocol/heartbeat.h"
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

// One scripted PULL_MESSAGE reply.
struct PullReply {
    int32_t code = ResponseCode::PULL_NOT_FOUND;
    PropertyMap ext;
    Bytes body;
};

// Plays both the name server (answers route queries) and the broker (answers QUERY_CONSUMER_OFFSET / PULL_MESSAGE).
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
        {
            // if the gate was never released, open it first so a worker thread stuck in wait cannot hold up the destructor.
            std::lock_guard<std::mutex> lk(gateMutex_);
            gateOpen_ = true;
            gateCv_.notify_all();
        }
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

    /// Queue one PULL_MESSAGE reply (hit in FIFO order, each reply used exactly once).
    void scriptPull(int32_t code, const PropertyMap& ext, const Bytes& body = Bytes()) {
        std::lock_guard<std::mutex> lk(state_);
        PullReply reply;
        reply.code = code;
        reply.ext = ext;
        reply.body = body;
        pullScripts_.push_back(reply);
    }

    /// Arm a departure gate for the next PULL_MESSAGE: the reply waits for releasePullGate() first. Used
    /// to turn the "reply still in flight while the client already seeked" window into a deterministic one.
    void gateNextPull() {
        std::lock_guard<std::mutex> lk(gateMutex_);
        gateArmed_ = true;
        gateOpen_ = false;
    }

    void releasePullGate() {
        std::lock_guard<std::mutex> lk(gateMutex_);
        gateOpen_ = true;
        gateCv_.notify_all();
    }

    size_t pullCount() {
        std::lock_guard<std::mutex> lk(state_);
        return pulls_.size();
    }

    PropertyMap pullExt(size_t i) {
        std::lock_guard<std::mutex> lk(state_);
        if (i >= pulls_.size()) return PropertyMap();
        return pulls_[i];
    }

    int32_t pullCode(size_t i) {
        std::lock_guard<std::mutex> lk(state_);
        if (i >= pullCodes_.size()) return -1;
        return pullCodes_[i];
    }

    /// The **request code** of the i-th pull request (11=PULL_MESSAGE / 361=LITE_PULL_MESSAGE).
    int32_t pullRequestCode(size_t i) {
        std::lock_guard<std::mutex> lk(state_);
        if (i >= pullReqCodes_.size()) return -1;
        return pullReqCodes_[i];
    }

    size_t pullReqCodeCount() {
        std::lock_guard<std::mutex> lk(state_);
        return pullReqCodes_.size();
    }

    /// Whether any PULL_MESSAGE at all started from `offset`.
    bool pullHasOffset(int64_t offset) {
        std::lock_guard<std::mutex> lk(state_);
        for (const PropertyMap& ext : pulls_) {
            auto it = ext.find("queueOffset");
            if (it != ext.end() && extInt(ext, "queueOffset", -1) == offset) return true;
        }
        return false;
    }

    /// Whether this request code was ever received (backs the "must not send this" assertions, e.g. GET_MIN_OFFSET).
    bool sawRequest(int32_t code) {
        std::lock_guard<std::mutex> lk(state_);
        for (int32_t c : codes_) {
            if (c == code) return true;
        }
        return false;
    }

    static int64_t extInt(const PropertyMap& ext, const std::string& key, int64_t fallback) {
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
                codes_.push_back(req.code);
                auto it = routes_.find(topic);
                if (it != routes_.end()) {
                    resp.code = ResponseCode::SUCCESS;
                    resp.body = it->second;
                    resp.hasBody = true;
                } else {
                    resp.code = ResponseCode::TOPIC_NOT_EXIST;
                }
            } else if (req.code == RequestCode::PULL_MESSAGE ||
                       req.code == RequestCode::LITE_PULL_MESSAGE) {
                answerPull(req, resp);
            } else if (req.code == RequestCode::QUERY_CONSUMER_OFFSET) {
                // the group never committed an offset: a real broker answers QUERY_NOT_FOUND (the 22 of
                // 21/22), which is how the client falls into the consumeFromWhere branch to compute the start.
                std::lock_guard<std::mutex> lk(state_);
                codes_.push_back(req.code);
                resp.code = ResponseCode::QUERY_NOT_FOUND;
            } else {
                std::lock_guard<std::mutex> lk(state_);
                codes_.push_back(req.code);
                resp.code = ResponseCode::SUCCESS;
            }
            Bytes out = resp.encode();
            if (!writeAll(sock, out)) break;
        }
        ::shutdown(sock, kShutdownHow);
        netcompat::closeSocket(sock);
    }

    /// The request is recorded **before** the gate: when the gate stretches the "reply still in flight"
    /// window, pullCount() must already be visible, else the "wait for the first pull" case idles at the gate. pullCodes_ records the **reply** codes.
    void answerPull(const RemotingCommand& req, RemotingCommand& resp) {
        {
            std::lock_guard<std::mutex> lk(state_);
            codes_.push_back(req.code);
            pullReqCodes_.push_back(req.code);
            pulls_.push_back(req.extFields);
        }
        bool gated = false;
        {
            std::lock_guard<std::mutex> lk(gateMutex_);
            if (gateArmed_) {
                gateArmed_ = false;
                gated = true;
            }
        }
        if (gated) {
            std::unique_lock<std::mutex> lk(gateMutex_);
            // 5s fallback: even a failing case must not wedge the worker thread forever (the destructor joins it).
            gateCv_.wait_for(lk, std::chrono::seconds(5), [this] { return gateOpen_; });
        }
        PullReply reply;
        bool hasScript = false;
        {
            std::lock_guard<std::mutex> lk(state_);
            if (!pullScripts_.empty()) {
                reply = pullScripts_.front();
                pullScripts_.pop_front();
                hasScript = true;
            }
        }
        if (!hasScript) {
            reply.code = ResponseCode::PULL_NOT_FOUND;
            reply.ext["nextBeginOffset"] =
                std::to_string(extInt(req.extFields, "queueOffset", 0));
            reply.ext["minOffset"] = "0";
            reply.ext["maxOffset"] = "0";
        }
        {
            std::lock_guard<std::mutex> lk(state_);
            pullCodes_.push_back(reply.code);
        }
        resp.code = reply.code;
        resp.extFields = reply.ext;
        if (!reply.body.empty()) {
            resp.body = reply.body;
            resp.hasBody = true;
        }
    }

    socket_t listen_ = kInvalidSocket;
    uint16_t port_ = 0;
    std::atomic<bool> running_{false};
    std::mutex state_;
    std::map<std::string, Bytes> routes_;
    std::vector<PropertyMap> pulls_;
    std::vector<int32_t> pullReqCodes_;
    std::vector<int32_t> pullCodes_;
    std::vector<int32_t> codes_;
    std::deque<PullReply> pullScripts_;
    std::mutex gateMutex_;
    std::condition_variable gateCv_;
    bool gateArmed_ = false;
    bool gateOpen_ = false;
    std::mutex workersM_;
    std::vector<std::thread> workers_;
    std::thread acceptor_;
};

// ---------------------------------------------------------------- scaffolding

const char* kTopic = "LiteCursorTopic";
const char* kBroker = "broker-a";

MessageQueue queue0() { return MessageQueue(kTopic, kBroker, 0); }

TopicRouteData routeMaster(const std::string& addr) {
    TopicRouteData route;
    std::map<int64_t, std::string> addrs;
    addrs[MixAll::MASTER_ID] = addr;
    route.brokerDatas.emplace_back("DefaultCluster", kBroker, addrs);
    route.queueDatas.emplace_back(kBroker, 1, 1, 6, 0);
    return route;
}

// Reply header: nextBeginOffset / minOffset / maxOffset
PropertyMap pullResp(int64_t next, int64_t min = 0, int64_t max = 9) {
    PropertyMap ext;
    ext["nextBeginOffset"] = std::to_string(next);
    ext["minOffset"] = std::to_string(min);
    ext["maxOffset"] = std::to_string(max);
    return ext;
}

// Wait until the condition holds (5s cap by default); returns whether it was seen
template <typename Fn>
bool waitFor(Fn fn, int timeoutMs = 5000) {
    const int64_t deadline = UtilAll::currentTimeMillis() + timeoutMs;
    while (UtilAll::currentTimeMillis() < deadline) {
        if (fn()) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    return fn();
}

// Start a lite-pull consumer pointed at the fake endpoint (assign + FIRST_OFFSET: the start is the
// literal 0, no RPC beyond the start-offset query, so the first PULL_MESSAGE lands on queueOffset=0 for sure).
std::unique_ptr<DefaultLitePullConsumer> startedLite(const std::string& group,
                                                     MockEndpoint& cluster) {
    auto c = std::make_unique<DefaultLitePullConsumer>(group);
    c->setNamesrvAddr(cluster.address());
    c->setConsumeFromWhere(ConsumeFromWhere::CONSUME_FROM_FIRST_OFFSET);
    c->assign({queue0()});
    c->start();
    return c;
}

// ---------------------------------------------------------------- 1. NO_MATCHED_MSG

void testCursorFollowsNextBeginOffsetOnNoMatchedMsg() {
    MockEndpoint cluster;
    cluster.addRoute(kTopic, routeMaster(cluster.address()));
    cluster.scriptPull(ResponseCode::PULL_RETRY_IMMEDIATELY, pullResp(/*next=*/5));
    auto c = startedLite("LitePG_CursorNoMatch", cluster);

    const MessageQueue mq = queue0();
    expect(waitFor([&] { return c->pullCursorOf(mq) == 5; }),
           "after NO_MATCHED_MSG the pull cursor follows nextBeginOffset=5");
    // Not just a counter moved in an in-memory table: the next PULL_MESSAGE really starts at 5
    expect(waitFor([&] { return cluster.pullHasOffset(5); }),
           "the next PULL_MESSAGE starts at 5 (the cursor really went on the wire)");
    expect(!cluster.sawRequest(RequestCode::GET_MIN_OFFSET),
           "FIRST_OFFSET start is literal 0: not a single minOffset query may be sent");
    expect(waitFor([&] { return cluster.pullCode(0) == ResponseCode::PULL_RETRY_IMMEDIATELY; }),
           "precondition: the first pull hit exactly the scripted NO_MATCHED_MSG");
    expect(c->poll(50).empty(), "NO_MATCHED_MSG has no deliverable message");
    c->shutdown();
}

// ---------------------------------------------------------------- 2. OFFSET_ILLEGAL

// For OFFSET_ILLEGAL the nextBeginOffset is the broker's correction of an out-of-range offset: only
// following it counts as self-healing; staying put means receiving the same correction every round.
void testCursorAdoptsTheBrokersOffsetCorrection() {
    MockEndpoint cluster;
    cluster.addRoute(kTopic, routeMaster(cluster.address()));
    cluster.scriptPull(ResponseCode::PULL_OFFSET_MOVED, pullResp(/*next=*/42, /*min=*/40));
    auto c = startedLite("LitePG_CursorIllegal", cluster);

    const MessageQueue mq = queue0();
    expect(waitFor([&] { return c->pullCursorOf(mq) == 42; }),
           "after OFFSET_ILLEGAL the pull cursor adopts the broker correction");
    expect(waitFor([&] { return cluster.pullHasOffset(42); }),
           "the next PULL_MESSAGE starts at 42");
    c->shutdown();
}

// ---------------------------------------------------------------- 3. In-flight seek

void testInFlightSeekWinsOverThePullResult() {
    MockEndpoint cluster;
    cluster.addRoute(kTopic, routeMaster(cluster.address()));
    MessageExt late;
    late.topic = kTopic;
    late.brokerName = kBroker;
    late.queueId = 0;
    late.queueOffset = 2;
    late.body = "late";
    cluster.scriptPull(ResponseCode::SUCCESS, pullResp(/*next=*/7),
                       encodeMessageExt(late, false));
    cluster.gateNextPull();
    auto c = startedLite("LitePG_CursorSeekRace", cluster);

    expect(waitFor([&] { return cluster.pullCount() >= 1; }), "the first pull reached the broker");
    const MessageQueue mq = queue0();
    c->seek(mq, 99);
    cluster.releasePullGate();

    expect(waitFor([&] { return cluster.pullHasOffset(99); }),
           "after seek the next PULL_MESSAGE starts at 99");
    expectInt(c->pullCursorOf(mq), 99, "an in-flight reply must not overwrite the offset written by seek");
    expect(c->poll(50).empty(), "the braked round must not enter the buffer");
    c->shutdown();
}

// ---------------------------------------------------------------- 4. Lite flag / request code (#107)

void testLitePullCarriesTheLiteCodeAndFlag() {
    MockEndpoint cluster;
    cluster.addRoute(kTopic, routeMaster(cluster.address()));
    auto c = startedLite("LitePG_LiteCode", cluster);

    expect(waitFor([&] { return cluster.pullReqCodeCount() >= 1; }),
           "at least one pull reached the broker");
    const size_t n = cluster.pullReqCodeCount();
    bool all361 = true;
    bool allFlag = true;
    for (size_t i = 0; i < n; ++i) {
        if (cluster.pullRequestCode(i) != RequestCode::LITE_PULL_MESSAGE) all361 = false;
        if (!PullSysFlag::hasLitePullFlag(
                static_cast<int32_t>(MockEndpoint::extInt(cluster.pullExt(i), "sysFlag", -1)))) {
            allFlag = false;
        }
    }
    expect(n >= 1 && all361, "all lite pull request codes are LITE_PULL_MESSAGE(361)");
    expect(n >= 1 && allFlag, "every pull header.sysFlag sets FLAG_LITE_PULL_MESSAGE(0x10)");
    const PropertyMap ext0 = cluster.pullExt(0);
    const int32_t flag0 = static_cast<int32_t>(MockEndpoint::extInt(ext0, "sysFlag", -1));
    expect(!PullSysFlag::hasCommitOffsetFlag(flag0) && !PullSysFlag::hasSuspendFlag(flag0) &&
               PullSysFlag::hasSubscriptionFlag(flag0) &&
               !PullSysFlag::hasClassFilterFlag(flag0),
           "the remaining lite-pull flag bits match the pull contract bit for bit");
    const size_t liteCount = cluster.pullReqCodeCount();
    c->shutdown();
    expect(liteCount >= 1, "the lite consumer sent at least one pull (otherwise the 11 comparison below is meaningless)");

    // Comparison: the classic pull (DefaultMQPullConsumer's 4-arg build) must still be code
    // 11 without the lite bit -- without this leg, "361" could just mean every pull became 361.
    DefaultMQPullConsumer classic("LitePG_LiteCodeClassic");
    classic.setNamesrvAddr(cluster.address());
    classic.start();
    classic.pull(queue0(), "*", 0, 32, 3000);
    classic.shutdown();

    int32_t classicIdx = -1;
    for (size_t i = 0; i < cluster.pullReqCodeCount(); ++i) {
        if (cluster.pullRequestCode(i) == RequestCode::PULL_MESSAGE) {
            classicIdx = static_cast<int32_t>(i);
            break;
        }
    }
    expect(classicIdx >= 0, "the classic pull request code is PULL_MESSAGE(11)");
    if (classicIdx >= 0) {
        const int32_t clFlag = static_cast<int32_t>(MockEndpoint::extInt(
            cluster.pullExt(static_cast<size_t>(classicIdx)), "sysFlag", -1));
        expect(!PullSysFlag::hasLitePullFlag(clFlag),
               "the classic pull sysFlag carries no lite bit (the classic 4-arg build)");
    }
}

}  // namespace

int main() {
    testCursorFollowsNextBeginOffsetOnNoMatchedMsg();
    testCursorAdoptsTheBrokersOffsetCorrection();
    testInFlightSeekWinsOverThePullResult();
    testLitePullCarriesTheLiteCodeAndFlag();
    std::printf("lite_pull_cursor: %d checks, %d failed\n", checks, fails);
    return fails == 0 ? 0 : 1;
}
