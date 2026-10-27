// Offline unit tests for the two high-level pull-consumer capabilities:
//
//   1. MQPullConsumerScheduleService (the per-queue pull scheduling is folded into it): the
//      scheduling shell itself -- due check, offset advance, rescheduling delay -- plus per-topic PullTaskCallback
//      registration, queue-dimension task fan-out for **this instance's share**, strict per-queue
//      seriality, the three scheduling channels (return value > pullMessageLater > setPullInterval >
//      status default), the exception delay, reassignment cancelling revoked queues, and a shutdown
//      that drains the in-flight round.
//   2. DefaultMQPullConsumer::pullAsync (the async form of pull(mq, subExpression, offset, maxNums, callback, timeout)):
//      immediate return, exactly-one onSuccess/onException on a worker/callback thread, synchronous
//      throw before start().
//
// The evidence is the wire traffic of the fake endpoint (route / consumer list / pull requests and
// their queueOffset progression) plus the callback-side observations -- never a claim of the impl.
// Everything runs against the in-frame MockEndpoint; no cluster, no dead-port waits.
#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstdio>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "rocketmq/client/exception.h"
#include "rocketmq/client/pull_schedule_service.h"
#include "rocketmq/common/message_decoder.h"
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

int64_t steadyMs() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::steady_clock::now().time_since_epoch())
        .count();
}

template <typename Fn>
bool waitFor(Fn fn, int timeoutMs = 8000) {
    const int64_t deadline = steadyMs() + timeoutMs;
    while (steadyMs() < deadline) {
        if (fn()) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    return fn();
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

// One scripted PULL_MESSAGE reply (code + extFields + body); consumed FIFO, each used once.
struct PullReply {
    int32_t code = ResponseCode::PULL_NOT_FOUND;
    PropertyMap ext;
    Bytes body;
};

// Plays name server (route) and broker (38 consumer list / 14 offset query / 30 max offset / 11 pull)
// in one listening socket, the established shape of the pull-consumer offline suites.
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

    void setMaxOffset(int64_t v) {
        std::lock_guard<std::mutex> lk(state_);
        maxOffset_ = v;
    }

    /// Queue one PULL_MESSAGE reply (FIFO, one-shot). When the queue is empty the faithful empty-broker
    /// shape answers: PULL_NOT_FOUND echoing the requested offset back as nextBeginOffset.
    void scriptPull(const PullReply& reply) {
        std::lock_guard<std::mutex> lk(state_);
        pullScripts_.push_back(reply);
    }

    /// Recorded pull requests: "topic@queueId", in arrival order.
    std::vector<std::string> pulledQueues() {
        std::lock_guard<std::mutex> lk(state_);
        return pulled_;
    }

    /// queueOffset of the i-th PULL_MESSAGE (the cursor progression evidence); -1 past the end.
    int64_t pullOffset(size_t i) {
        std::lock_guard<std::mutex> lk(state_);
        if (i >= pullOffsets_.size()) return -1;
        return pullOffsets_[i];
    }

    size_t pullCount() {
        std::lock_guard<std::mutex> lk(state_);
        return pulled_.size();
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

    static int64_t extInt(const PropertyMap& ext, const std::string& key, int64_t fallback) {
        auto it = ext.find(key);
        if (it == ext.end() || it->second.empty()) return fallback;
        try {
            return std::stoll(it->second);
        } catch (...) {
            return fallback;
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
            } else if (req.code == RequestCode::QUERY_CONSUMER_OFFSET) {
                // The group never committed an offset here: the real broker's QUERY_NOT_FOUND (22).
                resp.code = ResponseCode::QUERY_NOT_FOUND;
            } else if (req.code == RequestCode::GET_MAX_OFFSET) {
                resp.code = ResponseCode::SUCCESS;
                std::lock_guard<std::mutex> lk(state_);
                resp.extFields["offset"] = std::to_string(maxOffset_);
            } else if (req.code == RequestCode::PULL_MESSAGE
                       || req.code == RequestCode::LITE_PULL_MESSAGE) {
                const std::string topic =
                    req.extFields.count("topic") ? req.extFields.at("topic") : std::string();
                const std::string queueId =
                    req.extFields.count("queueId") ? req.extFields.at("queueId") : std::string();
                const int64_t reqOffset = extInt(req.extFields, "queueOffset", -1);
                PullReply scripted;
                bool hasScripted = false;
                {
                    std::lock_guard<std::mutex> lk(state_);
                    pulled_.push_back(topic + "@" + queueId);
                    pullOffsets_.push_back(reqOffset);
                    if (!pullScripts_.empty()) {
                        scripted = pullScripts_.front();
                        pullScripts_.erase(pullScripts_.begin());
                        hasScripted = true;
                    }
                }
                if (hasScripted) {
                    resp.code = scripted.code;
                    resp.extFields = scripted.ext;
                    if (!scripted.body.empty()) {
                        resp.body = scripted.body;
                        resp.hasBody = true;
                    }
                    // A SUCCESS reply without nextBeginOffset would stall the cursor at 0; the scripted
                    // caller is expected to fill the ext like the broker does.
                } else {
                    resp.code = ResponseCode::PULL_NOT_FOUND;
                    resp.extFields["nextBeginOffset"] = std::to_string(std::max<int64_t>(0, reqOffset));
                    resp.extFields["minOffset"] = "0";
                    resp.extFields["maxOffset"] = "0";
                    resp.extFields["suggestWhichBrokerId"] = "0";
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
    std::vector<std::string> cidList_;
    int64_t maxOffset_ = 0;
    std::vector<PullReply> pullScripts_;
    std::vector<std::string> pulled_;
    std::vector<int64_t> pullOffsets_;
    std::mutex workersM_;
    std::vector<std::thread> workers_;
    std::thread acceptor_;
};

// ---------------------------------------------------------------- scaffolding

const char* kBrokerKey = "broker-a";

TopicRouteData routeN(int32_t n, const std::string& addr) {
    TopicRouteData route;
    std::map<int64_t, std::string> addrs;
    addrs[MixAll::MASTER_ID] = addr;
    route.brokerDatas.emplace_back("DefaultCluster", kBrokerKey, addrs);
    route.queueDatas.emplace_back(kBrokerKey, n, n, 6, 0);
    return route;
}

// Kept as scaffolding for the empty-answer shape of the pull protocol; the scripted replies below
// build their own frames.
[[maybe_unused]] PullReply emptyPullReply(int64_t nextBeginOffset) {
    PullReply r;
    r.code = ResponseCode::PULL_NOT_FOUND;
    r.ext["nextBeginOffset"] = std::to_string(nextBeginOffset);
    r.ext["minOffset"] = "0";
    r.ext["maxOffset"] = "9";
    r.ext["suggestWhichBrokerId"] = "0";
    return r;
}

PullReply foundPullReply(int64_t nextBeginOffset, const std::string& topic, int32_t queueId,
                         int64_t queueOffset, const std::string& text) {
    MessageExt m;
    m.topic = topic;
    m.queueId = queueId;
    m.queueOffset = queueOffset;
    m.body = text;
    PullReply r;
    r.code = ResponseCode::SUCCESS;
    r.ext["nextBeginOffset"] = std::to_string(nextBeginOffset);
    r.ext["minOffset"] = "0";
    r.ext["maxOffset"] = "9";
    r.ext["suggestWhichBrokerId"] = "0";
    r.body = encodeMessageExt(m, false);
    return r;
}

/// Point a schedule service's consumer at the fake endpoint and configure the scheduler for fast
/// offline rounds (the production defaults -- 5s refresh / 3s empty-reply delay -- would stretch the
/// suite into minutes without changing what is proven).
void prepare(MQPullConsumerScheduleService& svc, MockEndpoint& ep, const std::string& instance) {
    svc.defaultMQPullConsumer().setInstanceName(instance);
    svc.defaultMQPullConsumer().setNamesrvAddr(ep.address());
    svc.setQueueRefreshIntervalMillis(100);
}

// ---------------------------------------------------------------- 1. fan-out + per-queue seriality

class FanOutCallback : public PullTaskCallback {
public:
    int32_t doPullTask(const MessageQueue& mq, PullResult& pullResult,
                       PullTaskContext& context) override {
        {
            std::lock_guard<std::mutex> lk(m_);
            ++rounds_[mq.queueId];
            statuses_[pullResult.status]++;
            sawConsumer = context.pullConsumer() != nullptr;
            if (firstOffset_.count(mq.queueId) == 0) {
                firstOffset_[mq.queueId] = pullResult.nextBeginOffset;
            }
        }
        // Strict seriality per queue: a second concurrent entry for the same queueId is a violation.
        const int32_t prev = inFlight_[mq.queueId].fetch_add(1);
        if (prev != 0) {
            overlaps_.fetch_add(1);
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(2));
        inFlight_[mq.queueId].fetch_sub(1);
        return 10;  // channel 1: keep the rounds coming fast
    }

    int32_t rounds(int32_t queueId) {
        std::lock_guard<std::mutex> lk(m_);
        auto it = rounds_.find(queueId);
        return it == rounds_.end() ? 0 : it->second;
    }
    int32_t overlapCount() const { return overlaps_.load(); }
    bool sawConsumerPointer() {
        std::lock_guard<std::mutex> lk(m_);
        return sawConsumer;
    }
    size_t distinctStatuses() {
        std::lock_guard<std::mutex> lk(m_);
        return statuses_.size();
    }

private:
    std::mutex m_;
    std::map<int32_t, int32_t> rounds_;
    std::map<PullStatus, int32_t> statuses_;
    std::map<int32_t, int64_t> firstOffset_;
    std::atomic<int32_t> inFlight_[8] = {};
    std::atomic<int32_t> overlaps_{0};
    bool sawConsumer = false;
};

void testFanOutAndPerQueueSerial() {
    const char* kTopic = "SchedFanOutTopic";
    MockEndpoint ep;
    ep.addRoute(kTopic, routeN(4, ep.address()));

    MQPullConsumerScheduleService svc("PG_SchedFanOut");
    prepare(svc, ep, "sched_fanout");
    auto cb = std::make_shared<FanOutCallback>();
    svc.registerPullTaskCallback(kTopic, cb);
    svc.start();
    ep.setCidList({svc.defaultMQPullConsumer().clientId()});

    expect(waitFor([&] { return svc.taskCount() == 4; }),
           "the refresh fans the sole instance's share out into 4 queue-dimension tasks");
    expect(waitFor([&] {
               return cb->rounds(0) >= 2 && cb->rounds(1) >= 2 && cb->rounds(2) >= 2
                      && cb->rounds(3) >= 2;
           }),
           "every assigned queue is pulled repeatedly (queue-dimension tasks run)");
    expectInt(cb->overlapCount(), 0, "the same queue never runs two rounds concurrently (serial)");
    expect(cb->sawConsumerPointer(), "PullTaskContext carries the service's consumer pointer");
    expectInt(static_cast<long long>(cb->distinctStatuses()), 1,
              "the empty broker answers every round NO_NEW_MSG and the result is passed in");
    {
        const std::vector<std::string> pulled = ep.pulledQueues();
        bool saw0 = std::find(pulled.begin(), pulled.end(), std::string(kTopic) + "@0") != pulled.end();
        bool saw3 = std::find(pulled.begin(), pulled.end(), std::string(kTopic) + "@3") != pulled.end();
        expect(saw0 && saw3, "wire traffic: PULL_MESSAGE really reached the broker for the queues");
    }
    svc.shutdown();
    expect(!svc.isStarted(), "shutdown flips the service back to not-started");
    expectInt(static_cast<long long>(svc.taskCount()), 0, "shutdown clears the task table");
}

// ---------------------------------------------------------------- 2. the three scheduling channels

class ChannelCallback : public PullTaskCallback {
public:
    struct Round {
        int64_t atMs = 0;
        int32_t queueId = -1;
    };

    int32_t doPullTask(const MessageQueue& mq, PullResult&, PullTaskContext& context) override {
        std::lock_guard<std::mutex> lk(m_);
        rounds_.push_back(Round{steadyMs(), mq.queueId});
        const size_t idx = rounds_.size();
        if (idx == 1) {
            // later(250) vs interval(20): pullMessageLater must win the next slot.
            context.setPullInterval(20);
            context.pullMessageLater(250);
            return kUseContextDelay;
        }
        if (idx == 2) {
            // return value(300) vs interval(20) vs later(50): the return value outranks both.
            context.setPullInterval(20);
            context.pullMessageLater(50);
            return 300;
        }
        if (idx == 3) {
            // interval(250) alone: the third channel still schedules when nothing overrides it.
            context.setPullInterval(250);
            return kUseContextDelay;
        }
        return 60000;  // park: later rounds must not disturb the timing assertions
    }

    std::vector<Round> rounds() {
        std::lock_guard<std::mutex> lk(m_);
        return rounds_;
    }

private:
    std::mutex m_;
    std::vector<Round> rounds_;
};

void testThreeChannelPriority() {
    const char* kTopic = "SchedChannelTopic";
    MockEndpoint ep;
    ep.addRoute(kTopic, routeN(1, ep.address()));

    MQPullConsumerScheduleService svc("PG_SchedChannel");
    prepare(svc, ep, "sched_channel");
    auto cb = std::make_shared<ChannelCallback>();
    svc.registerPullTaskCallback(kTopic, cb);
    svc.start();
    ep.setCidList({svc.defaultMQPullConsumer().clientId()});

    expect(waitFor([&] { return cb->rounds().size() >= 4; }, 15000),
           "four rounds ran (return value / later / interval each rescheduled the queue)");
    const std::vector<ChannelCallback::Round> rounds = cb->rounds();
    if (rounds.size() >= 4) {
        const int64_t gap1 = rounds[1].atMs - rounds[0].atMs;
        const int64_t gap2 = rounds[2].atMs - rounds[1].atMs;
        const int64_t gap3 = rounds[3].atMs - rounds[2].atMs;
        expect(gap1 >= 200 && gap1 < 3000,
               "pullMessageLater(250) beats setPullInterval(20)", "gap1=" + std::to_string(gap1));
        expect(gap2 >= 250 && gap2 < 3000,
               "the return value(300) beats pullMessageLater(50) and setPullInterval(20)",
               "gap2=" + std::to_string(gap2));
        expect(gap3 >= 200 && gap3 < 3000,
               "setPullInterval(250) schedules the next round when nothing overrides it",
               "gap3=" + std::to_string(gap3));
    }
    svc.shutdown();
}

// ---------------------------------------------------------------- 3. status default + offset bookkeeping

class StatusCallback : public PullTaskCallback {
public:
    struct Round {
        int64_t atMs = 0;
        PullStatus status = PullStatus::NO_NEW_MSG;
        size_t msgCount = 0;
        std::string body;
    };

    int32_t doPullTask(const MessageQueue&, PullResult& pullResult, PullTaskContext&) override {
        std::lock_guard<std::mutex> lk(m_);
        Round r;
        r.atMs = steadyMs();
        r.status = pullResult.status;
        r.msgCount = pullResult.msgFoundList.size();
        if (!pullResult.msgFoundList.empty()) {
            r.body = pullResult.msgFoundList[0].body;
        }
        rounds_.push_back(r);
        if (rounds_.size() >= 3) {
            return 60000;  // park after the two measured gaps
        }
        return kUseContextDelay;  // defer to the status default both times
    }

    std::vector<Round> rounds() {
        std::lock_guard<std::mutex> lk(m_);
        return rounds_;
    }

private:
    std::mutex m_;
    std::vector<Round> rounds_;
};

void testStatusDefaultAndOffsetAdvance() {
    const char* kTopic = "SchedStatusTopic";
    MockEndpoint ep;
    ep.addRoute(kTopic, routeN(1, ep.address()));
    // Round 1 finds one message and hands the broker's nextBeginOffset=5 back; round 2 is empty.
    ep.scriptPull(foundPullReply(/*next=*/5, kTopic, 0, /*queueOffset=*/0, "scheduled-body"));

    MQPullConsumerScheduleService svc("PG_SchedStatus");
    prepare(svc, ep, "sched_status");
    // The exception-class delay defaults to 3s; scaled down so the empty-reply gap is measurable.
    svc.setPullTimeDelayMillisWhenException(400);
    auto cb = std::make_shared<StatusCallback>();
    svc.registerPullTaskCallback(kTopic, cb);
    svc.start();
    ep.setCidList({svc.defaultMQPullConsumer().clientId()});

    expect(waitFor([&] { return cb->rounds().size() >= 3; }, 15000),
           "FOUND -> immediate next round, empty reply -> the exception-class delay");
    const std::vector<StatusCallback::Round> rounds = cb->rounds();
    if (rounds.size() >= 3) {
        expect(rounds[0].status == PullStatus::FOUND, "round 1 gets the scripted FOUND result");
        expectInt(static_cast<long long>(rounds[0].msgCount), 1, "round 1 carries the decoded message");
        expect(rounds[0].body == "scheduled-body", "the message body survives the round trip");
        expect(rounds[1].status == PullStatus::NO_NEW_MSG, "round 2 is the empty-broker reply");
        const int64_t gap1 = rounds[1].atMs - rounds[0].atMs;
        const int64_t gap2 = rounds[2].atMs - rounds[1].atMs;
        expect(gap1 < 350, "FOUND reschedules at once (status default 0)",
               "gap1=" + std::to_string(gap1));
        expect(gap2 >= 330 && gap2 < 3000,
               "the empty reply waits pullTimeDelayMillisWhenException before the next round",
               "gap2=" + std::to_string(gap2));
    }
    // Offset bookkeeping (the service pulls on the task's behalf): the first pull starts at maxOffset (=0 here,
    // QUERY_CONSUMER_OFFSET answered QUERY_NOT_FOUND), the next pull follows nextBeginOffset=5.
    expect(waitFor([&] { return ep.pullCount() >= 2; }), "two pulls reached the broker");
    expectInt(ep.pullOffset(0), 0, "the uninitialized task starts at the queried maxOffset");
    expectInt(ep.pullOffset(1), 5, "the second pull follows the reply's nextBeginOffset");
    svc.shutdown();
}

// ---------------------------------------------------------------- 4. throwing callback -> exception delay

class ThrowingCallback : public PullTaskCallback {
public:
    int32_t doPullTask(const MessageQueue&, PullResult&, PullTaskContext&) override {
        std::lock_guard<std::mutex> lk(m_);
        calls_.push_back(steadyMs());
        if (calls_.size() == 1) {
            throw std::runtime_error("callback blew up");
        }
        return 60000;  // park after the recovery round
    }

    std::vector<int64_t> calls() {
        std::lock_guard<std::mutex> lk(m_);
        return calls_;
    }

private:
    std::mutex m_;
    std::vector<int64_t> calls_;
};

void testCallbackExceptionRetriesAfterExceptionDelay() {
    const char* kTopic = "SchedThrowTopic";
    MockEndpoint ep;
    ep.addRoute(kTopic, routeN(1, ep.address()));

    MQPullConsumerScheduleService svc("PG_SchedThrow");
    prepare(svc, ep, "sched_throw");
    svc.setPullTaskExceptionDelayMillis(300);  // the default is 1000ms; scaled for the offline budget
    auto cb = std::make_shared<ThrowingCallback>();
    svc.registerPullTaskCallback(kTopic, cb);
    svc.start();
    ep.setCidList({svc.defaultMQPullConsumer().clientId()});

    expect(waitFor([&] { return cb->calls().size() >= 2; }, 15000),
           "a throwing doPullTask does not kill the task: the next round still comes");
    const std::vector<int64_t> calls = cb->calls();
    if (calls.size() >= 2) {
        const int64_t gap = calls[1] - calls[0];
        expect(gap >= 250 && gap < 3000,
               "the retry waits pullTaskExceptionDelayMillis (default 1000ms)",
               "gap=" + std::to_string(gap));
    }
    svc.shutdown();
}

// ---------------------------------------------------------------- 5. shutdown drains the in-flight round

class BlockingCallback : public PullTaskCallback {
public:
    int32_t doPullTask(const MessageQueue&, PullResult&, PullTaskContext&) override {
        {
            std::unique_lock<std::mutex> lk(m_);
            entered_ = true;
            enteredCv_.notify_all();
            gateCv_.wait(lk, [this] { return released_; });
            finished_ = true;
        }
        return 60000;
    }

    void waitEntered() {
        std::unique_lock<std::mutex> lk(m_);
        enteredCv_.wait(lk, [this] { return entered_; });
    }
    void release() {
        {
            std::lock_guard<std::mutex> lk(m_);
            released_ = true;
        }
        gateCv_.notify_all();
    }
    bool finished() {
        std::lock_guard<std::mutex> lk(m_);
        return finished_;
    }

private:
    std::mutex m_;
    std::condition_variable enteredCv_;
    std::condition_variable gateCv_;
    bool entered_ = false;
    bool released_ = false;
    bool finished_ = false;
};

void testShutdownDrainsInflight() {
    const char* kTopic = "SchedDrainTopic";
    MockEndpoint ep;
    ep.addRoute(kTopic, routeN(1, ep.address()));

    MQPullConsumerScheduleService svc("PG_SchedDrain");
    prepare(svc, ep, "sched_drain");
    auto cb = std::make_shared<BlockingCallback>();
    svc.registerPullTaskCallback(kTopic, cb);
    svc.start();
    ep.setCidList({svc.defaultMQPullConsumer().clientId()});
    cb->waitEntered();

    std::atomic<bool> shutdownReturned{false};
    std::thread shutdownThread([&svc, &shutdownReturned]() {
        svc.shutdown();
        shutdownReturned.store(true);
    });
    // While the round is inside doPullTask, shutdown must be waiting (drain, not abandon).
    std::this_thread::sleep_for(std::chrono::milliseconds(300));
    expect(!shutdownReturned.load(), "shutdown blocks while a doPullTask round is in flight");
    expect(!cb->finished(), "the in-flight round has not finished yet (still gated)");
    cb->release();
    shutdownThread.join();
    expect(shutdownReturned.load(), "shutdown returns once the in-flight round completed");
    expect(cb->finished(), "the gated round ran to completion before shutdown returned (drained)");
    expectInt(static_cast<long long>(svc.taskCount()), 0, "the task table is empty after shutdown");
}

// ---------------------------------------------------------------- 6. reassignment cancels revoked queues

void testReassignmentCancelsRevokedQueues() {
    const char* kTopic = "SchedReassignTopic";
    MockEndpoint ep;
    ep.addRoute(kTopic, routeN(4, ep.address()));

    MQPullConsumerScheduleService svc("PG_SchedReassign");
    prepare(svc, ep, "sched_reassign");
    auto cb = std::make_shared<FanOutCallback>();
    svc.registerPullTaskCallback(kTopic, cb);
    svc.start();
    const std::string self = svc.defaultMQPullConsumer().clientId();
    // "0" sorts before any dotted-quad clientId, so the averagely strategy gives self queues 2 and 3.
    ep.setCidList({self});
    expect(waitFor([&] { return svc.taskCount() == 4; }), "starts with the full 4-queue share");
    expect(waitFor([&] { return cb->rounds(0) >= 1 && cb->rounds(3) >= 1; }),
           "queues 0..3 all receive rounds before the reassignment");

    ep.setCidList({"000_other_instance", self});
    expect(waitFor([&] { return svc.taskCount() == 2; }),
           "the next refresh aligns the task table with the shrunken share (putTask cancels the rest)");

    // Let an already-submitted round of a revoked queue finish, then freeze the evidence.
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    const int32_t q0 = cb->rounds(0);
    const int32_t q1 = cb->rounds(1);
    const int32_t q2 = cb->rounds(2);
    const int32_t q3 = cb->rounds(3);
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    expectInt(cb->rounds(0), q0, "a revoked queue gets no further rounds (task cancelled)");
    expectInt(cb->rounds(1), q1, "the second revoked queue is silent too");
    expect(cb->rounds(2) > q2 && cb->rounds(3) > q3,
           "the retained queues keep pulling while the revoked ones are frozen");
    svc.shutdown();
}

// ---------------------------------------------------------------- 7. putTask bookkeeping without a cluster

void testPutTaskTableWithoutStart() {
    MQPullConsumerScheduleService svc("PG_SchedPutTask");
    auto cb = std::make_shared<FanOutCallback>();
    bool threwTopic = false;
    try {
        svc.registerPullTaskCallback("", cb);
    } catch (const MQClientException&) {
        threwTopic = true;
    }
    bool threwNull = false;
    try {
        svc.registerPullTaskCallback("T", nullptr);
    } catch (const MQClientException&) {
        threwNull = true;
    }
    expect(threwTopic && threwNull, "registerPullTaskCallback rejects an empty topic / null callback");

    svc.registerPullTaskCallback("PutTopic", cb);
    const MessageQueue a("PutTopic", kBrokerKey, 0);
    const MessageQueue b("PutTopic", kBrokerKey, 1);
    const MessageQueue other("OtherTopic", kBrokerKey, 0);
    svc.putTask("PutTopic", {a, b, other});  // `other` has a different topic: putTask must still track it
    expectInt(static_cast<long long>(svc.taskCount()), 3, "putTask creates one task per new queue");
    svc.putTask("PutTopic", {a, other});
    expectInt(static_cast<long long>(svc.taskCount()), 2, "queues leaving the set drop out of the table");
    svc.putTask("AnotherTopic", {b});
    expectInt(static_cast<long long>(svc.taskCount()), 3,
              "putTask of one topic never touches other topics' tasks");
}

// ---------------------------------------------------------------- 8. pullAsync: success exactly once

class RecordingPullCallback : public PullCallback {
public:
    void onSuccess(const PullResult& pullResult) override {
        std::lock_guard<std::mutex> lk(m_);
        ++successCount_;
        result_ = pullResult;
        callbackThreadId_ = std::this_thread::get_id();
        done_ = true;
        cv_.notify_all();
    }
    void onException(const std::exception_ptr& error) override {
        std::lock_guard<std::mutex> lk(m_);
        ++exceptionCount_;
        error_ = error;
        callbackThreadId_ = std::this_thread::get_id();
        done_ = true;
        cv_.notify_all();
    }

    bool waitDone(int timeoutMs = 10000) {
        std::unique_lock<std::mutex> lk(m_);
        return cv_.wait_for(lk, std::chrono::milliseconds(timeoutMs), [this] { return done_; });
    }
    int successCount() {
        std::lock_guard<std::mutex> lk(m_);
        return successCount_;
    }
    int exceptionCount() {
        std::lock_guard<std::mutex> lk(m_);
        return exceptionCount_;
    }
    PullResult result() {
        std::lock_guard<std::mutex> lk(m_);
        return result_;
    }
    std::exception_ptr error() {
        std::lock_guard<std::mutex> lk(m_);
        return error_;
    }
    std::thread::id callbackThreadId() {
        std::lock_guard<std::mutex> lk(m_);
        return callbackThreadId_;
    }

private:
    std::mutex m_;
    std::condition_variable cv_;
    bool done_ = false;
    int successCount_ = 0;
    int exceptionCount_ = 0;
    PullResult result_;
    std::exception_ptr error_;
    std::thread::id callbackThreadId_;
};

void testPullAsyncSuccessExactlyOnce() {
    const char* kTopic = "AsyncPullTopic";
    MockEndpoint ep;
    ep.addRoute(kTopic, routeN(2, ep.address()));
    ep.scriptPull(foundPullReply(/*next=*/3, kTopic, 0, /*queueOffset=*/0, "async-body"));

    DefaultMQPullConsumer c("PG_AsyncPullOk");
    c.setInstanceName("async_ok");
    c.setNamesrvAddr(ep.address());
    c.start();

    auto cb = std::make_shared<RecordingPullCallback>();
    const MessageQueue mq(kTopic, kBrokerKey, 0);
    const std::thread::id callerId = std::this_thread::get_id();
    const int64_t began = steadyMs();
    c.pullAsync(mq, "*", 0, 32, cb, 5000);
    const int64_t returnedAfter = steadyMs() - began;
    expect(returnedAfter < 1000, "pullAsync returns immediately (no synchronous round trip)",
           "took " + std::to_string(returnedAfter) + "ms");
    expect(cb->waitDone(), "the async pull callback fires");
    expectInt(cb->successCount(), 1, "onSuccess fires");
    expectInt(cb->exceptionCount(), 0, "onException does not fire on the success path");
    const PullResult r = cb->result();
    expect(r.status == PullStatus::FOUND, "the scripted FOUND result is delivered");
    expectInt(static_cast<long long>(r.nextBeginOffset), 3, "nextBeginOffset survives");
    expectInt(static_cast<long long>(r.msgFoundList.size()), 1, "the decoded message is delivered");
    if (!r.msgFoundList.empty()) {
        expect(r.msgFoundList[0].body == "async-body", "the message body survives the round trip");
    }
    expect(cb->callbackThreadId() != callerId, "the callback runs on a worker thread, not the caller's");
    // "Exactly once" needs a quiet window: a double delivery would show up here.
    std::this_thread::sleep_for(std::chrono::milliseconds(300));
    expectInt(cb->successCount(), 1, "no second delivery after the quiet window (exactly once)");
    c.shutdown();
}

// ---------------------------------------------------------------- 9. pullAsync: broker error -> onException once

void testPullAsyncExceptionExactlyOnce() {
    const char* kTopic = "AsyncPullErrTopic";
    MockEndpoint ep;
    ep.addRoute(kTopic, routeN(1, ep.address()));
    PullReply err;
    err.code = ResponseCode::SYSTEM_ERROR;
    err.ext["nextBeginOffset"] = "0";
    ep.scriptPull(err);

    DefaultMQPullConsumer c("PG_AsyncPullErr");
    c.setInstanceName("async_err");
    c.setNamesrvAddr(ep.address());
    c.start();

    auto cb = std::make_shared<RecordingPullCallback>();
    c.pullAsync(MessageQueue(kTopic, kBrokerKey, 0), "*", 0, 32, cb, 5000);
    expect(cb->waitDone(), "the failing pull reports through the callback, not a throw on the caller");
    expectInt(cb->exceptionCount(), 1, "onException fires once");
    expectInt(cb->successCount(), 0, "onSuccess does not fire on the failure path");
    bool brokerError = false;
    try {
        if (cb->error()) std::rethrow_exception(cb->error());
    } catch (const MQBrokerException& e) {
        brokerError = e.getResponseCode() == ResponseCode::SYSTEM_ERROR;
    } catch (...) {
        brokerError = false;
    }
    expect(brokerError, "the exception_ptr carries the broker's SYSTEM_ERROR");
    std::this_thread::sleep_for(std::chrono::milliseconds(300));
    expectInt(cb->exceptionCount(), 1, "no second delivery after the quiet window (exactly once)");
    c.shutdown();
}

// ---------------------------------------------------------------- 10. pullAsync guards before start()

void testPullAsyncGuards() {
    DefaultMQPullConsumer c("PG_AsyncPullGuard");
    auto cb = std::make_shared<RecordingPullCallback>();
    bool threwNotStarted = false;
    try {
        c.pullAsync(MessageQueue("T", kBrokerKey, 0), "*", 0, 32, cb, 5000);
    } catch (const MQClientException& e) {
        threwNotStarted = std::string(e.what()).find("not started") != std::string::npos;
    }
    expect(threwNotStarted, "pullAsync before start() throws synchronously (MQClientException)");

    // The null-callback guard sits on the same synchronous path (a started consumer is required to
    // reach it, so this leg reuses a fake endpoint).
    const char* kTopic = "AsyncPullGuardTopic";
    MockEndpoint ep;
    ep.addRoute(kTopic, routeN(1, ep.address()));
    DefaultMQPullConsumer started("PG_AsyncPullGuard2");
    started.setInstanceName("async_guard");
    started.setNamesrvAddr(ep.address());
    started.start();
    bool threwNull = false;
    try {
        started.pullAsync(MessageQueue(kTopic, kBrokerKey, 0), "*", 0, 32, nullptr, 5000);
    } catch (const MQClientException& e) {
        threwNull = std::string(e.what()).find("null") != std::string::npos;
    }
    expect(threwNull, "pullAsync with a null callback throws synchronously");
    // Late (post-shutdown) pullAsync must throw instead of dropping the callback silently.
    started.shutdown();
    bool threwAfterShutdown = false;
    try {
        started.pullAsync(MessageQueue(kTopic, kBrokerKey, 0), "*", 0, 32, cb, 5000);
    } catch (const MQClientException&) {
        threwAfterShutdown = true;
    }
    expect(threwAfterShutdown, "pullAsync after shutdown throws (never silently dropped)");
    expectInt(cb->successCount() + cb->exceptionCount(), 0,
              "none of the rejected pullAsync calls delivered a callback");
}

}  // namespace

int main() {
    testFanOutAndPerQueueSerial();
    testThreeChannelPriority();
    testStatusDefaultAndOffsetAdvance();
    testCallbackExceptionRetriesAfterExceptionDelay();
    testShutdownDrainsInflight();
    testReassignmentCancelsRevokedQueues();
    testPutTaskTableWithoutStart();
    testPullAsyncSuccessExactlyOnce();
    testPullAsyncExceptionExactlyOnce();
    testPullAsyncGuards();
    std::printf("%d checks, %d failures\n", checks, fails);
    return fails == 0 ? 0 : 1;
}
