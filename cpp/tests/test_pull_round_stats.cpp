// Offline unit tests pinning the PULL-path consume-statistics contract
// (queuePullLoop -> ConsumerStatsManager PULL_RT / PULL_TPS, keyed topic@group):
//
//   leg A  a round that did not answer FOUND records NOTHING: not the empty-answer round
//          (NO_NEW_MSG), not the round that ended in a broker exception. PULL_RT is the latency of
//          a pull round and only a round that returned messages has a delivery round trip to price.
//   leg B  a FOUND round whose rows were ALL removed by the pre-delivery filter (second tag pass /
//          FilterMessageHook) records RT (the round happened) but NO TPS (nothing was delivered);
//          and a FOUND round that carried no rows at all still records RT.
//   leg C  a FOUND round that keeps N of M rows records TPS == N, the post-filter deliverable count,
//          never the raw msgFoundList size.
//   leg D  the failed round of leg A, read as a WAIT: a non-215 broker answer makes the loop take this
//          consumer's configured general error backoff (an untouched consumer takes the port's
//          default), and a FLOW_CONTROL(215) answer keeps its own short pause whatever the knob says.
//   leg E  the round cadence, read as the wait the loop recorded: a FOUND round that delivered rows
//          waits the configured pull interval, while a FOUND round the filter emptied re-pulls at once
//          (cadence 0), so a delivered-then-emptied sequence ends on 0 -- the pair that catches a
//          cadence sleep hoisted out of the delivered branch.
//
// The evidence is read out of the consumer instance's own ConsumerStatsManager (the same object the
// production loop calls): StatsItem.times() counts the incPullRT / incPullTPS invocations and
// StatsItem.value() sums their arguments, so "which leg fired, with what number" is decided on
// recorded state, never on a claim of the impl. The rounds are driven through the real queuePullLoop
// against the in-frame fake endpoint (route / consumer list / offset queries / scripted PULL_MESSAGE
// replies, consumed FIFO by the single assigned queue, so the round ORDER is deterministic without a
// timing guess). No cluster, no live broker: nothing here asserts what a broker does on its side.
#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <limits>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "rocketmq/client/consumer.h"
#include "rocketmq/client/consumer_stats.h"
#include "rocketmq/client/mq_client.h"
#include "rocketmq/client/result.h"
#include "rocketmq/common/byte_buffer.h"
#include "rocketmq/common/logging.h"
#include "rocketmq/common/message.h"
#include "rocketmq/common/message_decoder.h"
#include "rocketmq/common/mix_all.h"
#include "rocketmq/common/net_compat.h"
#include "rocketmq/common/sysflag.h"
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

const char* kTopic = "PullRoundStatsTopic";
const char* kBroker = "broker-a";
const char* kGroup = "GID_PullRoundStats";

int64_t steadyMs() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::steady_clock::now().time_since_epoch())
        .count();
}

template <typename Fn>
bool waitFor(Fn fn, int timeoutMs = 15000) {
    const int64_t deadline = steadyMs() + timeoutMs;
    while (steadyMs() < deadline) {
        if (fn()) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    return fn();
}

// Silences the fake-cluster error logs the exception round and the shutdown path produce on purpose.
class QuietLog {
public:
    QuietLog() {
        previous_ = logLevel();
        setLogLevel(LOG_OFF);
    }
    ~QuietLog() { setLogLevel(previous_); }

private:
    int previous_ = 0;
};

// Captures the process log so a WARN the gate emits can be read back from the log, not assumed.
class ScopedLogFile {
public:
    explicit ScopedLogFile(const std::string& name) {
        path_ = (std::filesystem::temp_directory_path() / name).string();
        std::error_code ec;
        std::filesystem::remove(path_, ec);
        setLogFile(path_);
    }
    ~ScopedLogFile() { setLogFile(std::string()); }

    std::string contents() {
        flushLogFile();
        std::ifstream in(path_);
        return std::string((std::istreambuf_iterator<char>(in)),
                           std::istreambuf_iterator<char>());
    }

private:
    std::string path_;
};

// Point HOME at a throw-away directory: the broadcasting offset file is rooted at $HOME.
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
                ("rmq_pull_round_home_" + std::string(tag) + "_" +
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

private:
    std::string dir_;
    std::string old_;
};

// ---------------------------------------------------------------- fake endpoint

// One scripted PULL_MESSAGE reply; consumed FIFO by the single assigned queue, which makes the
// round sequence deterministic (the pull loop is strictly serial per queue).
struct PullReply {
    int32_t code = ResponseCode::PULL_NOT_FOUND;
    PropertyMap ext;
    Bytes body;
};

// Plays name server (route) and broker (38 consumer list / 14 offset query / 30 max offset / 11
// pull). Same shape as the established pull-offline harnesses; when the script is exhausted the
// faithful empty answer (PULL_NOT_FOUND) keeps the loop running WITHOUT touching the scripts.
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

    void setMaxOffset(int64_t v) {
        std::lock_guard<std::mutex> lk(state_);
        maxOffset_ = v;
    }

    void scriptPull(const PullReply& reply) {
        std::lock_guard<std::mutex> lk(state_);
        pullScripts_.push_back(reply);
    }

    size_t pullCount() {
        std::lock_guard<std::mutex> lk(state_);
        return pullCount_;
    }

    // Every request code the endpoint received; the orderly gate is judged on this wire evidence.
    size_t countCode(int32_t code) {
        std::lock_guard<std::mutex> lk(state_);
        size_t n = 0;
        for (int32_t c : codes_) {
            if (c == code) ++n;
        }
        return n;
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
                // The group never committed an offset here: the faithful QUERY_NOT_FOUND (22) shape,
                // which makes the consumer resolve its initial offset through GET_MAX_OFFSET.
                resp.code = ResponseCode::QUERY_NOT_FOUND;
            } else if (req.code == RequestCode::GET_MAX_OFFSET) {
                std::lock_guard<std::mutex> lk(state_);
                resp.extFields["offset"] = std::to_string(maxOffset_);
            } else if (req.code == RequestCode::PULL_MESSAGE) {
                PullReply scripted;
                bool hasScripted = false;
                {
                    std::lock_guard<std::mutex> lk(state_);
                    ++pullCount_;
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
                } else {
                    // Exhausted script: the empty answer the real broker gives an idle queue. Never
                    // FOUND, so in compliant code it must move neither PULL_RT nor PULL_TPS.
                    resp.code = ResponseCode::PULL_NOT_FOUND;
                    resp.extFields["nextBeginOffset"] = "0";
                    resp.extFields["minOffset"] = "0";
                    resp.extFields["maxOffset"] = "0";
                    resp.extFields["suggestWhichBrokerId"] = "0";
                }
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
    int64_t maxOffset_ = 0;
    std::vector<PullReply> pullScripts_;
    size_t pullCount_ = 0;
    std::vector<int32_t> codes_;
    std::mutex workersM_;
    std::vector<std::thread> workers_;
    std::thread acceptor_;
};

// ---------------------------------------------------------------- fixtures

TopicRouteData routeOneQueue(const std::string& addr) {
    TopicRouteData route;
    std::map<int64_t, std::string> addrs;
    addrs[MixAll::MASTER_ID] = addr;
    route.brokerDatas.emplace_back("DefaultCluster", kBroker, addrs);
    route.queueDatas.emplace_back(kBroker, 1, 1, 6, 0);
    return route;
}

// The same broker with no writable/readable queue yet: a consumer starting on this route has an empty
// assignment, which keeps its rebalance loop on the short retry arm.
TopicRouteData routeNoQueue(const std::string& addr) {
    TopicRouteData route;
    std::map<int64_t, std::string> addrs;
    addrs[MixAll::MASTER_ID] = addr;
    route.brokerDatas.emplace_back("DefaultCluster", kBroker, addrs);
    return route;
}

PropertyMap pullExt(int64_t next) {
    PropertyMap ext;
    ext["nextBeginOffset"] = std::to_string(next);
    ext["minOffset"] = "0";
    ext["maxOffset"] = "9";
    ext["suggestWhichBrokerId"] = "0";
    return ext;
}

MessageExt taggedRow(const std::string& tags, int64_t queueOffset) {
    MessageExt m;
    m.topic = kTopic;
    m.brokerName = kBroker;
    m.queueId = 0;
    m.queueOffset = queueOffset;
    m.body = "payload-" + std::to_string(queueOffset);
    m.bornTimestamp = UtilAll::currentTimeMillis();
    m.setTags(tags);
    return m;
}

// A FOUND answer carrying exactly these rows (the multi-message body is the plain concatenation the
// decoder walks).
PullReply foundReply(int64_t next, const std::vector<MessageExt>& rows) {
    PullReply r;
    r.code = ResponseCode::SUCCESS;
    r.ext = pullExt(next);
    for (const MessageExt& m : rows) r.body += encodeMessageExt(m, false);
    return r;
}

// A FOUND answer that carries NO rows at all: the status says FOUND, the body is empty.
PullReply foundEmptyReply(int64_t next) {
    PullReply r;
    r.code = ResponseCode::SUCCESS;
    r.ext = pullExt(next);
    return r;
}

// An answer the pull path raises as MQBrokerException (the code is not one of the mapped pull
// statuses): the round ends in the loop's catch arm, before any result-level handling.
PullReply errorReply() {
    PullReply r;
    r.code = ResponseCode::SYSTEM_ERROR;
    return r;
}

class CountingListener : public MessageListenerConcurrently {
public:
    ConsumeConcurrentlyStatus consumeMessage(const std::vector<MessageExt>& msgs,
                                             ConsumeConcurrentlyContext&) override {
        std::lock_guard<std::mutex> lk(mu_);
        delivered_ += static_cast<int>(msgs.size());
        return ConsumeConcurrentlyStatus::CONSUME_SUCCESS;
    }
    int delivered() {
        std::lock_guard<std::mutex> lk(mu_);
        return delivered_;
    }

private:
    std::mutex mu_;
    int delivered_ = 0;
};

// An orderly listener: its `orderly()` flag is what makes the consumer go through the lock gate.
class CountingOrderlyListener : public MessageListenerOrderly {
public:
    ConsumeOrderlyStatus consumeMessage(const std::vector<MessageExt>& msgs,
                                        ConsumeOrderlyContext&) override {
        std::lock_guard<std::mutex> lk(mu_);
        delivered_ += static_cast<int>(msgs.size());
        return ConsumeOrderlyStatus::SUCCESS;
    }
    int delivered() {
        std::lock_guard<std::mutex> lk(mu_);
        return delivered_;
    }

private:
    std::mutex mu_;
    int delivered_ = 0;
};

// Start the consumer against the fake endpoint and let it own the single queue: the synchronous
// first allocation inside start() ran while the group list was still empty, so the test publishes
// its own clientId and re-runs the allocation through the public offline entry. From there the REAL
// queuePullLoop drives the scripted rounds, serially on this one queue.
void assignQueueAndRunRounds(DefaultMQPushConsumer& c, MockEndpoint& ep, size_t roundsAtLeast) {
    ep.setCidList({c.clientId()});
    c.doRebalanceForTest();
    expect(waitFor([&] { return ep.pullCount() >= roundsAtLeast; }),
           "the pull loop ran at least the expected number of rounds",
           "pullCount=" + std::to_string(ep.pullCount()));
}

// ---------------------------------------------------------------- leg A: nothing on non-FOUND

// Rounds 1..N of this consumer are exactly one broker-exception round followed by NO_NEW_MSG rounds
// (the exhausted script keeps answering PULL_NOT_FOUND). Compliant code never touches the two
// cells, and the StatsItem is only created by the FIRST inc call -- so "the cells do not exist" is
// the strongest possible zero: no revert that records on a non-FOUND or a failed round can pass.
void testNonFoundAndFailedRoundsRecordNothing() {
    MockEndpoint ep;
    ep.addRoute(kTopic, routeOneQueue(ep.address()));
    ep.setMaxOffset(0);
    ep.scriptPull(errorReply());  // round 1 ends in the MQBrokerException arm

    auto listener = std::make_shared<CountingListener>();
    DefaultMQPushConsumer c("GID_PullStatsNonFound");
    c.setInstanceName("pull_stats_nonfound");
    c.setNamesrvAddr(ep.address());
    c.setMessageListener(listener);
    c.subscribe(kTopic, "TagA");
    {
        QuietLog quiet;
        c.start();
        assignQueueAndRunRounds(c, ep, 5);

        ConsumerStatsManager& stats = c.client().consumerStats();
        const std::string key = std::string(kTopic) + "@" + std::string("GID_PullStatsNonFound");
        std::shared_ptr<StatsItem> rt = stats.topicAndGroupPullRT().find(key);
        std::shared_ptr<StatsItem> tps = stats.topicAndGroupPullTPS().find(key);
        expect(rt == nullptr, "legA.noRtCellForNonFoundOrFailedRounds",
               rt ? std::to_string(rt->times()) : std::string("nullptr"));
        expect(tps == nullptr, "legA.noTpsCellForNonFoundOrFailedRounds",
               tps ? std::to_string(tps->times()) : std::string("nullptr"));
        // Nothing was FOUND either, so nothing reached the listener.
        expectInt(listener->delivered(), 0, "legA.nothingDelivered");
        c.shutdown();
    }
}

// ---------------------------------------------------------------- legs B + C: the FOUND rounds

// The scripted round sequence on ONE queue, consumed in order by the serial pull loop:
//   round 1  FOUND with 3 rows, subscription TagA: 2 rows are removed by the pre-delivery tag
//            re-check -> RT once, TPS += 1 (the KEPT count, not the raw 3)          [leg C]
//   round 2  FOUND with 2 rows, both TagB: the filter empties the round
//            -> RT a second time, TPS does not move at all                            [leg B]
//   round 3  FOUND carrying no rows at all
//            -> RT a third time, TPS does not move                                    [leg B, no-rows arm]
//   round 4+ NO_NEW_MSG (exhausted script)
//            -> neither cell moves again, so the final counts are exactly (RT=3, TPS=1/+1)
// The trailing spin is deliberate: a revert to the old "RT on every round" line would keep growing
// rt->times() here instead of freezing at 3.
void testFoundRoundsRecordRtAlwaysAndTpsOnlyForDeliveredRows() {
    MockEndpoint ep;
    ep.addRoute(kTopic, routeOneQueue(ep.address()));
    ep.setMaxOffset(0);
    // The offsets advance 0 -> 3 -> 5 -> 6 across the three scripted rounds.
    ep.scriptPull(foundReply(3, {taggedRow("TagA", 0), taggedRow("TagB", 1), taggedRow("TagB", 2)}));
    ep.scriptPull(foundReply(5, {taggedRow("TagB", 3), taggedRow("TagB", 4)}));
    ep.scriptPull(foundEmptyReply(6));

    const std::string group = "GID_PullStatsFoundRounds";
    auto listener = std::make_shared<CountingListener>();
    DefaultMQPushConsumer c(group);
    c.setInstanceName("pull_stats_found");
    c.setNamesrvAddr(ep.address());
    c.setMessageListener(listener);
    c.subscribe(kTopic, "TagA");
    {
        QuietLog quiet;
        c.start();
        // Round k is FINISHED as soon as round k+1 has been requested (the queue is strictly serial),
        // so asking for one round more than the scripted three + the first NO_NEW_MSG spin guarantees
        // all three FOUND rounds have been fully processed before the cells are read.
        assignQueueAndRunRounds(c, ep, 5);

        ConsumerStatsManager& stats = c.client().consumerStats();
        const std::string key = std::string(kTopic) + "@" + group;
        std::shared_ptr<StatsItem> rt = stats.topicAndGroupPullRT().find(key);
        std::shared_ptr<StatsItem> tps = stats.topicAndGroupPullTPS().find(key);
        expect(rt != nullptr, "legBC.rtCellExists");
        expect(tps != nullptr, "legBC.tpsCellExists");
        if (rt != nullptr) {
            // Exactly the three FOUND rounds; the NO_NEW_MSG spin since added nothing.
            expectInt(rt->times(), 3, "legB.everyFoundRoundRecordsRtIncludingEmptyAndAllFiltered",
                      std::to_string(rt->times()));
        }
        if (tps != nullptr) {
            // Only round 1 delivered anything: one single call...
            expectInt(tps->times(), 1, "legB.allFilteredAndEmptyFoundRecordNoTps",
                      std::to_string(tps->times()));
            // ... and its number is the post-filter kept count 1, never the raw 3 of the answer.
            expectInt(tps->value(), 1, "legC.tpsIsTheKeptCountNotTheRawFoundCount",
                      std::to_string(tps->value()));
        }
        // The same rows the TPS number counts are the rows the listener actually got. The dispatch
        // loop runs on its own cadence, so the count is polled, not assumed: the leg fails on the
        // FINAL value, never on a snapshot taken too early.
        waitFor([&] { return listener->delivered() == 1; }, 8000);
        expectInt(listener->delivered(), 1, "legC.deliveredMatchesTheTpsNumber");
        // 2 rows dropped in round 1 + 2 rows dropped in round 2 = 4, by the very filter whose
        // removal the TPS above refuses to count.
        expectInt(c.filteredMessageCount(), 4, "legB.filteredRowsAreTrackedAsFiltered",
                  std::to_string(c.filteredMessageCount()));
        c.shutdown();
    }
}

// ---------------------------------------------------------------- leg D: the failed round's wait

// leg A's broker-exception round, read through the pause the REAL loop recorded: the general error
// backoff is this consumer's configurable knob, so the round waits what the knob says and a loop that
// ignored the knob shows up as a wrong recorded pause. The 215 round is the control on the other
// side: no knob value changes it.
void testFailedRoundWaitsTheConfiguredBackoff() {
    constexpr int32_t kKnobProbeMillis = 137;

    // The knob drives the wait the loop takes.
    {
        MockEndpoint ep;
        ep.addRoute(kTopic, routeOneQueue(ep.address()));
        ep.setMaxOffset(0);
        ep.scriptPull(errorReply());  // a non-215 broker code
        auto listener = std::make_shared<CountingListener>();
        DefaultMQPushConsumer c("GID_PullPauseKnob");
        c.setInstanceName("pull_pause_knob");
        c.setNamesrvAddr(ep.address());
        c.setMessageListener(listener);
        c.setPullPauseWhenPullErrorMillis(kKnobProbeMillis);
        c.subscribe(kTopic, "TagA");
        {
            QuietLog quiet;
            c.start();
            // Round 1 is the scripted error answer; the pause it records happens before round 2 is
            // issued, so five rounds guarantee the decision below was taken.
            assignQueueAndRunRounds(c, ep, 5);
            expectInt(c.lastPullPauseMillis(), kKnobProbeMillis,
                      "the failed round records the configured error backoff");
            c.shutdown();
        }
    }
    // Control 1: a consumer that never touched the knob waits the port's default.
    {
        MockEndpoint ep;
        ep.addRoute(kTopic, routeOneQueue(ep.address()));
        ep.setMaxOffset(0);
        ep.scriptPull(errorReply());
        auto listener = std::make_shared<CountingListener>();
        DefaultMQPushConsumer c("GID_PullPauseDefault");
        c.setInstanceName("pull_pause_default");
        c.setNamesrvAddr(ep.address());
        c.setMessageListener(listener);
        c.subscribe(kTopic, "TagA");
        {
            QuietLog quiet;
            c.start();
            assignQueueAndRunRounds(c, ep, 5);
            expectInt(c.lastPullPauseMillis(), kDefaultPullPauseWhenPullErrorMillis,
                      "the untouched knob records the default backoff");
            c.shutdown();
        }
    }
    // Control 2: the broker's FLOW_CONTROL(215) answer ignores the knob entirely.
    {
        MockEndpoint ep;
        ep.addRoute(kTopic, routeOneQueue(ep.address()));
        ep.setMaxOffset(0);
        PullReply throttled;
        throttled.code = ResponseCode::FLOW_CONTROL;
        ep.scriptPull(throttled);
        auto listener = std::make_shared<CountingListener>();
        DefaultMQPushConsumer c("GID_PullPauseThrottled");
        c.setInstanceName("pull_pause_throttled");
        c.setNamesrvAddr(ep.address());
        c.setMessageListener(listener);
        c.setPullPauseWhenPullErrorMillis(kKnobProbeMillis);
        c.subscribe(kTopic, "TagA");
        {
            QuietLog quiet;
            c.start();
            assignQueueAndRunRounds(c, ep, 5);
            expectInt(c.lastPullPauseMillis(), kPullPauseWhenBrokerFlowControl,
                      "a 215 answer keeps its own short pause even with the knob set");
            c.shutdown();
        }
    }
}

// ---------------------------------------------------------------- leg E: the delivered round's cadence

// The round cadence of the REAL queuePullLoop, read through the wait the loop recorded: the configured
// pull interval paces ONLY a round that delivered rows, exactly the arm PULL_TPS counts. The paired
// control is a FOUND round the filter emptied: it re-pulls at once, so its recorded cadence is 0. This
// is the leg that catches a cadence sleep hoisted out of the delivered branch -- such a sleep would make
// the emptied FOUND round wait the interval too, and the final read would be 40 instead of 0.
void testDeliveredRoundPacesAndIdleFoundDoesNot() {
    constexpr int32_t kCadenceProbeMillis = 40;

    // A delivered FOUND round waits the configured interval, then the trailing NO_NEW_MSG spin leaves
    // that last FOUND round's cadence in place (the not-FOUND rounds pace nothing).
    {
        MockEndpoint ep;
        ep.addRoute(kTopic, routeOneQueue(ep.address()));
        ep.setMaxOffset(0);
        ep.scriptPull(foundReply(3, {taggedRow("TagA", 0)}));  // FOUND, kept by the TagA filter
        auto listener = std::make_shared<CountingListener>();
        DefaultMQPushConsumer c("GID_PullCadenceDelivered");
        c.setInstanceName("pull_cadence_delivered");
        c.setNamesrvAddr(ep.address());
        c.setMessageListener(listener);
        c.setPullIntervalMillis(kCadenceProbeMillis);
        c.subscribe(kTopic, "TagA");
        {
            QuietLog quiet;
            c.start();
            assignQueueAndRunRounds(c, ep, 5);
            expectInt(c.lastPullCadenceMillis(), kCadenceProbeMillis,
                      "legE.deliveredRoundWaitsThePullInterval");
            c.shutdown();
        }
    }
    // The discriminator: a delivered round first records the interval, then a FOUND round the filter
    // empties must bring the cadence back to 0. If the wait sat outside the delivered branch the empty
    // round would keep it at 40 and this read fails.
    {
        MockEndpoint ep;
        ep.addRoute(kTopic, routeOneQueue(ep.address()));
        ep.setMaxOffset(0);
        ep.scriptPull(foundReply(3, {taggedRow("TagA", 0)}));  // round 1 delivers -> cadence 40
        ep.scriptPull(foundReply(6, {taggedRow("TagB", 3)}));  // round 2 FOUND, all filtered -> cadence 0
        auto listener = std::make_shared<CountingListener>();
        DefaultMQPushConsumer c("GID_PullCadenceIdleFound");
        c.setInstanceName("pull_cadence_idle");
        c.setNamesrvAddr(ep.address());
        c.setMessageListener(listener);
        c.setPullIntervalMillis(kCadenceProbeMillis);
        c.subscribe(kTopic, "TagA");
        {
            QuietLog quiet;
            c.start();
            assignQueueAndRunRounds(c, ep, 5);
            waitFor([&] { return listener->delivered() >= 1; }, 8000);
            expectInt(listener->delivered(), 1, "legE.onlyTheRealDeliveryReachedTheListener",
                      std::to_string(listener->delivered()));
            // The last FOUND round to run delivered nothing, so the loop is pacing at 0.
            expectInt(c.lastPullCadenceMillis(), 0, "legE.idleFoundRoundWaitsNothing");
            c.shutdown();
        }
    }
    // An interval of 0 paces even a delivered round nowhere: re-pull at once, so the recorded cadence
    // stays 0 rather than inheriting any default.
    {
        MockEndpoint ep;
        ep.addRoute(kTopic, routeOneQueue(ep.address()));
        ep.setMaxOffset(0);
        ep.scriptPull(foundReply(3, {taggedRow("TagA", 0)}));
        auto listener = std::make_shared<CountingListener>();
        DefaultMQPushConsumer c("GID_PullCadenceZeroInterval");
        c.setInstanceName("pull_cadence_zero");
        c.setNamesrvAddr(ep.address());
        c.setMessageListener(listener);
        c.setPullIntervalMillis(0);
        c.subscribe(kTopic, "TagA");
        {
            QuietLog quiet;
            c.start();
            assignQueueAndRunRounds(c, ep, 5);
            expectInt(c.lastPullCadenceMillis(), 0, "legE.zeroIntervalDeliveredRoundDoesNotWait");
            c.shutdown();
        }
    }
}

// ---------------------------------------------------------------- leg F: the orderly lock gate

// A BROADCASTING orderly consumer has no competing consumer for the queue, so every grant is implicit:
// it must pull and deliver without any LOCK_BATCH_MQ grant and never take part in the lock protocol.
void testOrderlyBroadcastingPullsWithoutLockGrant() {
    ScopedHome home("orderly_broadcast");
    MockEndpoint ep;
    // Start on a route without queues: the broadcast assignment is empty, so the rebalance loop keeps
    // its short (2s) retry arm and the shutdown at the end of this case does not wait a full period.
    ep.addRoute(kTopic, routeNoQueue(ep.address()));
    ep.setMaxOffset(0);
    ep.scriptPull(foundReply(1, {taggedRow("TagA", 0)}));

    auto listener = std::make_shared<CountingOrderlyListener>();
    DefaultMQPushConsumer c("GID_OrderlyBroadcastGate");
    c.setMessageModel(MessageModel::BROADCASTING);
    c.setInstanceName("orderly_broadcast_gate");
    c.setNamesrvAddr(ep.address());
    c.setMessageListener(listener);
    // The subscription exists from the start (start() requires one), but its route has no queues yet.
    c.subscribe(kTopic, "*");
    {
        QuietLog quiet;
        c.start();
        // The queue appears after startup: the next rebalance round takes it with no lock protocol.
        ep.addRoute(kTopic, routeOneQueue(ep.address()));
        c.client().updateTopicRouteInfoFromNameServer(kTopic);
        c.doRebalanceForTest();
        expect(waitFor([&] { return ep.pullCount() >= 3; }),
               "the broadcasting orderly consumer pulls with no lock grant",
               "pullCount=" + std::to_string(ep.pullCount()));
        expectInt(static_cast<long long>(ep.countCode(RequestCode::LOCK_BATCH_MQ)), 0,
                  "broadcasting never sends LOCK_BATCH_MQ");
        expect(waitFor([&] { return listener->delivered() >= 1; }),
               "the broadcasting orderly consumer delivers without a grant");
        c.shutdown();
    }
}

// A CLUSTERING orderly consumer must not pull a queue before the broker confirmed its lock: with no
// grant the pull count stays frozen, and only a recorded fresh grant opens the gate.
void testOrderlyClusteringWaitsForAGrant() {
    MockEndpoint ep;
    ep.addRoute(kTopic, routeOneQueue(ep.address()));
    ep.setMaxOffset(0);

    auto listener = std::make_shared<CountingOrderlyListener>();
    DefaultMQPushConsumer c("GID_OrderlyClusterGate");
    c.setInstanceName("orderly_cluster_gate");
    c.setNamesrvAddr(ep.address());
    c.setMessageListener(listener);
    c.subscribe(kTopic, "*");
    {
        QuietLog quiet;
        c.start();
        ep.setCidList({c.clientId()});
        c.doRebalanceForTest();
        expect(waitFor([&] { return !c.assignedQueueKeys().empty(); }),
               "precondition: the queue is assigned to this consumer");
        std::this_thread::sleep_for(std::chrono::milliseconds(400));
        expectInt(static_cast<long long>(ep.pullCount()), 0,
                  "no pull is issued before a lock grant");
        expectInt(listener->delivered(), 0, "nothing is delivered before a lock grant");

        const MessageQueue mq(kTopic, kBroker, 0);
        c.grantOrderlyLockForTest(mq, 0);
        expect(waitFor([&] { return ep.pullCount() >= 3; }),
               "a fresh lock grant opens the gate",
               "pullCount=" + std::to_string(ep.pullCount()));
        c.shutdown();
    }
}

// A grant older than the 30s trust window may already belong to another consumer: the queue must stay
// held (no pull), one warning must be logged, and only a new grant may resume pulling.
void testOrderlyStaleGrantHoldsTheQueueAndWarns() {
    ScopedLogFile log("pull_orderly_lock_expiry.log");
    MockEndpoint ep;
    ep.addRoute(kTopic, routeOneQueue(ep.address()));
    ep.setMaxOffset(0);

    auto listener = std::make_shared<CountingOrderlyListener>();
    DefaultMQPushConsumer c("GID_OrderlyLockExpiry");
    c.setInstanceName("orderly_lock_expiry");
    c.setNamesrvAddr(ep.address());
    c.setMessageListener(listener);
    c.subscribe(kTopic, "*");
    {
        c.start();
        ep.setCidList({c.clientId()});
        c.doRebalanceForTest();
        expect(waitFor([&] { return !c.assignedQueueKeys().empty(); }),
               "precondition: the queue is assigned to this consumer");

        const MessageQueue mq(kTopic, kBroker, 0);
        c.grantOrderlyLockForTest(mq, 31000);
        const bool warned = waitFor([&] {
            return log.contents().find("orderly lock expired for queue") != std::string::npos;
        }, 4000);
        expect(warned, "a grant older than the trust window is reported as expired");
        std::this_thread::sleep_for(std::chrono::milliseconds(300));
        expectInt(static_cast<long long>(ep.pullCount()), 0,
                  "an expired grant does not open the gate");

        c.grantOrderlyLockForTest(mq, 0);
        expect(waitFor([&] { return ep.pullCount() >= 3; }),
               "a fresh grant after the expiry resumes pulling",
               "pullCount=" + std::to_string(ep.pullCount()));
        c.shutdown();
    }
}

// ---------------------------------------------------------------- leg G: inner-batch unwrap on pull

MessageExt innerEnvelope(int64_t queueOffset, const Bytes& batchBody, const std::string& msgId) {
    MessageExt env = taggedRow("", queueOffset);
    env.sysFlag = MessageSysFlag::INNER_BATCH_FLAG | MessageSysFlag::NEED_UNWRAP_FLAG;
    env.body = batchBody;
    env.msgId = msgId;
    return env;
}

void testInnerBatchEnvelopeIsUnwrappedOnPull() {
    MockEndpoint ep;
    ep.addRoute(kTopic, routeOneQueue(ep.address()));

    Message m0;
    m0.body = "member-0";
    m0.flag = 0x11;
    m0.setTags("TagA");
    m0.properties["memberKey"] = "v0";
    Message m1;
    m1.body = "member-1";
    m1.flag = 0x22;
    m1.setTags("TagB");
    m1.properties["memberKey"] = "v1";
    const Bytes batch = encodeMessages({m0, m1});

    PullReply expanded;
    expanded.code = ResponseCode::SUCCESS;
    expanded.ext = pullExt(42);
    expanded.body = encodeMessageExt(innerEnvelope(40, batch, "ENVELOPE-1"), false);
    ep.scriptPull(expanded);

    const Bytes truncated = batch.substr(0, batch.size() - 3);
    PullReply damaged;
    damaged.code = ResponseCode::SUCCESS;
    damaged.ext = pullExt(52);
    damaged.body = encodeMessageExt(innerEnvelope(50, truncated, "ENVELOPE-2"), false);
    ep.scriptPull(damaged);

    auto instance = std::make_unique<MQClientInstance>(
        "127.0.0.1@6666#1", std::vector<std::string>{ep.address()});
    instance->updateTopicRouteInfoFromNameServer(kTopic);
    const MessageQueue mq(kTopic, kBroker, 0);
    {
        QuietLog quiet;
        const PullResult first =
            instance->pullMessage(kGroup, mq, 40, 32, /*sysFlag=*/0, /*commitOffset=*/0, "*", 0,
                                  ExpressionType::TAG, /*timeoutMillis=*/3000);
        expectInt(static_cast<long long>(first.msgFoundList.size()), 2,
                  "an inner-batch envelope expands to one message per member");
        if (first.msgFoundList.size() == 2) {
            const MessageExt& a = first.msgFoundList[0];
            const MessageExt& b = first.msgFoundList[1];
            expectInt(a.queueOffset, 40, "the first member keeps the envelope offset");
            expectInt(b.queueOffset, 41, "the next member offset advances by one");
            expectInt(a.queueId, 0, "members carry the envelope queue id");
            expect(a.topic == kTopic && a.brokerName == kBroker,
                   "members take routing identity from the envelope");
            expect(a.body == "member-0" && b.body == "member-1",
                   "each member keeps its own body", "a=" + a.body + " b=" + b.body);
            expect(a.getTags() == "TagA" && b.getTags() == "TagB",
                   "each member keeps its own properties");
            expect(a.getProperty("memberKey") == "v0" && b.getProperty("memberKey") == "v1",
                   "non-tag member properties survive the expansion");
            expectInt(a.flag, 0x11, "the member flag comes from the member entry");
            expectInt(b.flag, 0x22, "the second member flag comes from the second entry");
        }
        expectInt(first.nextBeginOffset, 42,
                  "the cursor still follows the envelope's nextBeginOffset");

        const PullResult second =
            instance->pullMessage(kGroup, mq, 50, 32, /*sysFlag=*/0, /*commitOffset=*/0, "*", 0,
                                  ExpressionType::TAG, /*timeoutMillis=*/3000);
        expectInt(static_cast<long long>(second.msgFoundList.size()), 1,
                  "a truncated batch body is not partially expanded");
        if (second.msgFoundList.size() == 1) {
            const MessageExt& kept = second.msgFoundList[0];
            expect(kept.body == truncated,
                   "the damaged envelope is kept verbatim instead of losing members");
            expectInt(kept.queueOffset, 50, "the kept envelope keeps its own offset");
            expect(MessageSysFlag::check(kept.sysFlag, MessageSysFlag::INNER_BATCH_FLAG),
                   "the kept envelope still carries its inner-batch flag");
        }
    }
    instance->shutdown();
}

}  // namespace

int main() {
    testNonFoundAndFailedRoundsRecordNothing();
    testFoundRoundsRecordRtAlwaysAndTpsOnlyForDeliveredRows();
    testFailedRoundWaitsTheConfiguredBackoff();
    testDeliveredRoundPacesAndIdleFoundDoesNot();
    testOrderlyBroadcastingPullsWithoutLockGrant();
    testOrderlyClusteringWaitsForAGrant();
    testOrderlyStaleGrantHoldsTheQueueAndWarns();
    testInnerBatchEnvelopeIsUnwrappedOnPull();
    std::printf("pull_round_stats: %d checks, %d failures\n", checks, fails);
    return fails == 0 ? 0 : 1;
}
