// The start point of a freshly assigned queue is decided by one table: the stored offset first, then
// consumeFromWhere -- never the subscription type and never the offset *storage mode*.
//
// Why this must be pinned offline: the wrong shapes fail silently on real hardware. Short-circuiting
// SQL92 subscriptions to the queue tail drops the whole backlog (send-first + CONSUME_FROM_FIRST_OFFSET
// received nothing; consumer-first only passed because the queues were empty). Broadcasting used to
// return before the timestamp branch, so CONSUME_FROM_TIMESTAMP degraded to "start now" for exactly
// the mode that has no broker-side offset at all. And a first-time %RETRY% queue that starts at the
// tail never replays the messages that triggered the retry.
//
// The table is separated from the I/O that gathers its inputs (local file / broker commit table for
// the stored offset, maxOffset and searchOffset RPCs for the two computed starts), so every cell is
// reachable without a cluster.
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <map>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include "rocketmq/client/consumer.h"
#include "rocketmq/client/exception.h"
#include "rocketmq/client/result.h"
#include "rocketmq/common/byte_buffer.h"
#include "rocketmq/common/mix_all.h"
#include "rocketmq/common/net_compat.h"
#include "rocketmq/common/util_all.h"
#include "rocketmq/remoting/protocol/body.h"
#include "rocketmq/remoting/protocol/codes.h"
#include "rocketmq/remoting/protocol/remoting_command.h"
#include "rocketmq/remoting/protocol/route.h"

using namespace rocketmq;

namespace {

int fails = 0;
int checks = 0;

const char* kPlanNames[] = {"stored", "zero", "tail", "search"};

const char* planName(StartOffsetPlan p) {
    return kPlanNames[static_cast<int>(p)];
}

void expectPlan(StartOffsetPlan actual, StartOffsetPlan want, const std::string& name) {
    ++checks;
    if (actual != want) {
        ++fails;
        std::printf("FAIL %s (want=%s got=%s)\n", name.c_str(), planName(want), planName(actual));
    }
}

void expect(bool ok, const std::string& name, const std::string& detail = "") {
    ++checks;
    if (!ok) {
        ++fails;
        std::printf("FAIL %s %s\n", name.c_str(), detail.c_str());
    }
}

const std::string kFirst = ConsumeFromWhere::CONSUME_FROM_FIRST_OFFSET;
const std::string kLast = ConsumeFromWhere::CONSUME_FROM_LAST_OFFSET;
const std::string kTs = ConsumeFromWhere::CONSUME_FROM_TIMESTAMP;
// The deprecated/alias variants share the LAST_OFFSET cell of the table.
const std::string kLastOrMin = ConsumeFromWhere::CONSUME_FROM_LAST_OFFSET_AND_FROM_MIN_WHEN_BOOT_FIRST;
const std::string kMin = ConsumeFromWhere::CONSUME_FROM_MIN_OFFSET;
const std::string kMax = ConsumeFromWhere::CONSUME_FROM_MAX_OFFSET;

const std::string kBusiness = "InitialOffsetPolicyTopic";
std::string retryTopic() { return MixAll::getRetryTopic("GID_InitialOffsetPolicy"); }

// ------------------------------------------------- a committed offset always wins

void testStoredOffsetWinsOverEveryPolicy() {
    for (const std::string& where : {kFirst, kLast, kTs, kLastOrMin, kMin, kMax}) {
        expectPlan(planInitialOffset(0, where, false), StartOffsetPlan::kStored,
                   "stored.zero beats " + where);
        expectPlan(planInitialOffset(7, where, true), StartOffsetPlan::kStored,
                   "stored.on retry queue beats " + where);
        // Paired control: with nothing committed the same policy must not report a stored start,
        // else the two assertions above would pass on an implementation that always returns kStored.
        expect(planInitialOffset(-1, where, false) != StartOffsetPlan::kStored,
               "nothing committed is not a stored start: " + where);
    }
}

void testNoStoredOffsetLeavesTheNegativeSentinelAlone() {
    // 0 is a real offset ("start at the first message"); only a negative answer means "never consumed".
    expectPlan(planInitialOffset(0, kLast, false), StartOffsetPlan::kStored, "offset 0 is a commit");
    expectPlan(planInitialOffset(-1, kLast, false), StartOffsetPlan::kQueueTail, "-1 is no commit");
}

// ------------------------------------------------- FIRST_OFFSET replays from 0

void testFirstOffsetNeverQueriesTheFloor() {
    expectPlan(planInitialOffset(-1, kFirst, false), StartOffsetPlan::kFromZero,
               "FIRST starts at the literal 0");
    // Same cell for a retry queue: FIRST is already the earliest possible start.
    expectPlan(planInitialOffset(-1, kFirst, true), StartOffsetPlan::kFromZero,
               "FIRST on a retry queue is still 0");
}

// ------------------------------------------------- TIMESTAMP: search, except on the retry queue

void testTimestampStartSearchesTheConfiguredInstant() {
    expectPlan(planInitialOffset(-1, kTs, false), StartOffsetPlan::kSearchAtConfiguredTime,
               "TIMESTAMP searches the configured instant");
    // Paired control: a %RETRY% queue is created by the first redelivery, long after that instant, so
    // searching there answers nothing -- it must take the tail instead. If the retry case were
    // dropped, these two cells would collapse to the same plan and one of them goes red.
    expectPlan(planInitialOffset(-1, kTs, true), StartOffsetPlan::kQueueTail,
               "TIMESTAMP on a retry queue takes the tail");
}

// ------------------------------------------------- LAST (and its aliases): tail, except on the retry queue

void testLastOffsetSkipsTheBacklogButRedeliveryDoesNot() {
    for (const std::string& where : {kLast, kLastOrMin, kMin, kMax}) {
        expectPlan(planInitialOffset(-1, where, false), StartOffsetPlan::kQueueTail,
                   where + " starts at the tail");
        expectPlan(planInitialOffset(-1, where, true), StartOffsetPlan::kFromZero,
                   where + " on a retry queue replays from 0");
    }
}

// ------------------------------------------------- the retry topic really is the one the client writes

void testRetryTopicClassificationMatchesTheWriter() {
    expect(MixAll::isRetryTopic(retryTopic()), "the group's retry topic classifies as retry");
    expect(!MixAll::isRetryTopic(kBusiness), "a business topic does not");
}

// ------------------------------------------------- consumeTimestamp is a hard format gate

void testConsumeTimestampParsesRoundTrippedInstants() {
    const int64_t t = UtilAll::parseConsumeTimestamp("20230101000000");
    expect(t % 1000 == 0, "a parsed instant is whole-second", std::to_string(t));
    expect(UtilAll::timeMillisToHumanString3(t) == "20230101000000",
           "parse and format round-trip", UtilAll::timeMillisToHumanString3(t));
    expect(UtilAll::parseConsumeTimestamp("20230101000001") - t == 1000,
           "one second later is 1000ms apart");
    expect(UtilAll::parseConsumeTimestamp("20230101010000") > t, "a later hour sorts later");
}

std::string parseError(const std::string& ts) {
    try {
        UtilAll::parseConsumeTimestamp(ts);
    } catch (const std::exception& e) {
        return e.what();
    }
    return "";
}

void testMalformedConsumeTimestampIsRejected() {
    for (const std::string& bad : {std::string(), std::string("2023010100000"),
                                   std::string("2023010100000a"), std::string("abcdefghijklmn"),
                                   std::string("202301010000000")}) {
        const std::string what = parseError(bad);
        expect(what.find("consumeTimestamp is invalid, the valid format is yyyyMMddHHmmss,but received " +
                             bad) == 0,
               "malformed timestamp rejected", "input=<" + bad + "> what=" + what);
    }
    expect(parseError("20230101000000").empty(), "a valid timestamp is not rejected");
}

void testStartupChecksTheTimestampWhateverThePolicyIs() {
    DefaultMQPushConsumer c("GID_InitialOffsetPolicyCheckConfig");
    // The default is a real instant (boot time minus 30 minutes), so start() must not fire on it --
    // an empty default would silently turn CONSUME_FROM_TIMESTAMP into "from now".
    expect(c.consumeTimestamp().size() == 14, "default consumeTimestamp is 14 digits",
           c.consumeTimestamp());
    bool defaultOk = true;
    try {
        c.checkConfigRanges();
    } catch (const std::exception& e) {
        defaultOk = false;
        std::printf("  default config rejected: %s\n", e.what());
    }
    expect(defaultOk, "the default configuration passes checkConfig");

    c.setConsumeTimestamp("2023010100000");
    const std::string what = [&c] {
        try {
            c.checkConfigRanges();
        } catch (const std::exception& e) {
            return std::string(e.what());
        }
        return std::string();
    }();
    expect(what.find("consumeTimestamp is invalid") != std::string::npos,
           "a malformed consumeTimestamp fails at startup", what);

    // Paired control: the same consumer with a well-formed value passes, so the rejection above is
    // the format gate and not a constant throw.
    c.setConsumeTimestamp("20200101000000");
    bool fixed = true;
    try {
        c.checkConfigRanges();
    } catch (const std::exception&) {
        fixed = false;
    }
    expect(fixed, "a corrected consumeTimestamp starts again");
}

// ------------------------------------- broker offset query failure (rebalance round)

using netcompat::kInvalidSocket;
using netcompat::socket_t;
using netcompat::socklen_type;

#ifdef _WIN32
constexpr int kShutdownHow = SD_BOTH;
#else
constexpr int kShutdownHow = SHUT_RDWR;
#endif

template <typename Fn>
bool waitFor(Fn fn, int timeoutMs = 5000) {
    const int64_t deadline = UtilAll::currentTimeMillis() + timeoutMs;
    while (UtilAll::currentTimeMillis() < deadline) {
        if (fn()) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    return fn();
}

class NoopConcurrentListener : public MessageListenerConcurrently {
public:
    ConsumeConcurrentlyStatus consumeMessage(const std::vector<MessageExt>&,
                                             ConsumeConcurrentlyContext&) override {
        return ConsumeConcurrentlyStatus::CONSUME_SUCCESS;
    }
};

TopicRouteData routeOneQueue(const std::string& broker, const std::string& addr) {
    TopicRouteData route;
    std::map<int64_t, std::string> addrs;
    addrs[MixAll::MASTER_ID] = addr;
    route.brokerDatas.emplace_back("DefaultCluster", broker, addrs);
    route.queueDatas.emplace_back(broker, 1, 1, 6, 0);
    return route;
}

// Name server + broker in one loopback endpoint: the QUERY_CONSUMER_OFFSET answer is scriptable, which
// is what drives the "query failed" path; the max-offset answer stays 0.
class MockEndpoint {
public:
    MockEndpoint() {
        netcompat::ensureInitialized();
        listen_ = ::socket(AF_INET, SOCK_STREAM, 0);
        int one = 1;
        ::setsockopt(listen_, SOL_SOCKET, SO_REUSEADDR, reinterpret_cast<const char*>(&one),
                     static_cast<int>(sizeof(one)));
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
            for (std::thread& t : pending) {
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

    void setQueryCode(int32_t code) {
        std::lock_guard<std::mutex> lk(state_);
        queryCode_ = code;
    }

    size_t pullCount() {
        std::lock_guard<std::mutex> lk(state_);
        return pullCount_;
    }

    bool sawCode(int32_t code) {
        std::lock_guard<std::mutex> lk(state_);
        for (int32_t c : codes_) {
            if (c == code) return true;
        }
        return false;
    }

private:
    static bool readN(socket_t s, char* buf, size_t n) {
        size_t got = 0;
        while (got < n) {
            int r = static_cast<int>(::recv(s, buf + got, static_cast<int>(n - got), 0));
            if (r <= 0) return false;
            got += static_cast<size_t>(r);
        }
        return true;
    }

    static bool writeAll(socket_t s, const Bytes& data) {
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

    bool readFrame(socket_t s, Bytes& out) {
        for (;;) {
            fd_set rfds;
            FD_ZERO(&rfds);
            FD_SET(s, &rfds);
            timeval tv;
            tv.tv_sec = 0;
            tv.tv_usec = 100 * 1000;
            int ready = ::select(netcompat::selectNfds(s), &rfds, nullptr, nullptr, &tv);
            if (!running_.load()) return false;
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
        while (running_.load() && readFrame(sock, frame)) {
            RemotingCommand req;
            if (!RemotingCommand::tryDecode(frame, req, nullptr)) break;
            if (req.isResponseType()) break;
            {
                std::lock_guard<std::mutex> lk(state_);
                codes_.push_back(req.code);
            }
            RemotingCommand resp;
            resp.opaque = req.opaque;
            resp.markResponseType();
            resp.code = ResponseCode::SUCCESS;
            if (req.code == RequestCode::GET_ROUTEINFO_BY_TOPIC) {
                const std::string topic =
                    req.extFields.count("topic") ? req.extFields.at("topic") : std::string();
                std::lock_guard<std::mutex> lk(state_);
                auto it = routes_.find(topic);
                if (it == routes_.end()) {
                    resp.code = ResponseCode::TOPIC_NOT_EXIST;
                } else {
                    resp.body = it->second;
                    resp.hasBody = true;
                }
            } else if (req.code == RequestCode::GET_CONSUMER_LIST_BY_GROUP) {
                GetConsumerListByGroupResponseBody body;
                {
                    std::lock_guard<std::mutex> lk(state_);
                    body.consumerIdList = cidList_;
                }
                resp.body = body.encode();
                resp.hasBody = true;
            } else if (req.code == RequestCode::QUERY_CONSUMER_OFFSET) {
                std::lock_guard<std::mutex> lk(state_);
                resp.code = queryCode_;
            } else if (req.code == RequestCode::GET_MAX_OFFSET) {
                resp.extFields["offset"] = "0";
            } else if (req.code == RequestCode::PULL_MESSAGE) {
                std::lock_guard<std::mutex> lk(state_);
                ++pullCount_;
                resp.code = ResponseCode::PULL_NOT_FOUND;
                resp.extFields["nextBeginOffset"] = "0";
                resp.extFields["minOffset"] = "0";
                resp.extFields["maxOffset"] = "0";
                resp.extFields["suggestWhichBrokerId"] = "0";
            }
            if (!writeAll(sock, resp.encode())) break;
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
    int32_t queryCode_ = ResponseCode::QUERY_NOT_FOUND;
    std::vector<int32_t> codes_;
    size_t pullCount_ = 0;
    std::mutex workersM_;
    std::vector<std::thread> workers_;
    std::thread acceptor_;
};

// A broker offset query that fails (NOT the "no committed offset" answer) must drop the queue from this
// rebalance round instead of falling back to the configured start position: starting at the tail would
// silently skip the backlog. The next round retries the query and assigns the queue normally.
void testBrokerOffsetQueryFailureSkipsTheQueueForTheRound() {
    const std::string broker = "broker-a";
    MockEndpoint ep;
    ep.addRoute(kBusiness, routeOneQueue(broker, ep.address()));
    ep.setQueryCode(ResponseCode::SYSTEM_ERROR);

    DefaultMQPushConsumer c("GID_InitialOffsetQueryFailure");
    c.setInstanceName("initial_offset_query_failure");
    c.setNamesrvAddr(ep.address());
    c.setMessageListener(std::make_shared<NoopConcurrentListener>());
    c.subscribe(kBusiness, "*");
    c.start();

    ep.setCidList({c.clientId()});
    c.doRebalanceForTest();
    expect(c.assignedQueueKeys().empty(),
           "a queue whose broker offset query failed is not assigned this round");
    expect(!ep.sawCode(RequestCode::GET_MAX_OFFSET),
           "a failed query is not read as 'nothing committed' (no tail fallback)");
    expect(ep.pullCount() == 0, "the skipped queue is not pulled this round",
           "pullCount=" + std::to_string(ep.pullCount()));

    // The next round gets the legitimate QUERY_NOT_FOUND answer: LAST_OFFSET resolves at the tail.
    ep.setQueryCode(ResponseCode::QUERY_NOT_FOUND);
    c.doRebalanceForTest();
    expect(!c.assignedQueueKeys().empty(),
           "the next rebalance retries the query and assigns the queue");
    expect(waitFor([&] { return ep.pullCount() >= 1; }),
           "the queue assigned on the retried round is pulled",
           "pullCount=" + std::to_string(ep.pullCount()));
    c.shutdown();
}

}  // namespace

int main() {
    testStoredOffsetWinsOverEveryPolicy();
    testNoStoredOffsetLeavesTheNegativeSentinelAlone();
    testFirstOffsetNeverQueriesTheFloor();
    testTimestampStartSearchesTheConfiguredInstant();
    testLastOffsetSkipsTheBacklogButRedeliveryDoesNot();
    testRetryTopicClassificationMatchesTheWriter();
    testConsumeTimestampParsesRoundTrippedInstants();
    testMalformedConsumeTimestampIsRejected();
    testStartupChecksTheTimestampWhateverThePolicyIs();
    testBrokerOffsetQueryFailureSkipsTheQueueForTheRound();
    std::printf("initial offset policy: %d checks, %d failed\n", checks, fails);
    return fails == 0 ? 0 : 1;
}
