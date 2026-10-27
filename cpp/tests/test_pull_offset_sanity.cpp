// Offline unit tests pinning the cpp pull-result offset sanity warning on the push-consumer PULL path
// (DefaultMQPushConsumer::queuePullLoop, end of a FOUND round, after the cursor advanced and the
// batch was handed to the consume side):
//
//   The contract: the round remembers the cursor it requested with (prevRequestOffset), advances the
//   cursor to nextBeginOffset, and at the END of the FOUND round emits one WARN when
//       nextBeginOffset < prevRequestOffset   OR   firstMsgOffset < prevRequestOffset
//   where firstMsgOffset is the queue offset of the FIRST row of the post-filter deliverable list;
//   an empty round carries the int64 max sentinel so the second leg can never fire. Purely
//   diagnostic: neither leg may move the stored offset, the cadence, or any counter.
//
//   leg 1  the pure predicate alone, and the real loop emitting one WARN carrying all three numbers.
//   leg 2  firstMsg behind prev with next >= prev: fires, numbers all present.
//   leg 3  a consistent FOUND round: no warn, cursor advances as usual.
//   leg 4  an empty FOUND round (no rows, and rows all filtered away): the sentinel leg never fires.
//   leg 5  a FIRING round stores exactly its nextBeginOffset and takes the same delivered-round
//          cadence as an identical-shape NON-firing round: the warning must not perturb either.
//
// The rounds are driven through the real queuePullLoop against the in-frame fake endpoint (scripted
// PULL_MESSAGE replies consumed FIFO by the single assigned queue, which also records the queueOffset
// EACH round requested -- the read-out for "the cursor stored was exactly nextBeginOffset"). The WARN
// is read back from the process log file (setLogFile), never from a claim of the impl.
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
#include "rocketmq/client/mq_client.h"
#include "rocketmq/client/result.h"
#include "rocketmq/common/byte_buffer.h"
#include "rocketmq/common/logging.h"
#include "rocketmq/common/message.h"
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

constexpr const char* kMarker = "pull offset sanity:";

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

const char* kTopic = "PullOffsetSanityTopic";
const char* kBroker = "broker-a";

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

size_t countOccurrences(const std::string& haystack, const std::string& needle) {
    size_t n = 0;
    size_t pos = 0;
    while ((pos = haystack.find(needle, pos)) != std::string::npos) {
        ++n;
        pos += needle.size();
    }
    return n;
}

// ---------------------------------------------------------------- log capture
// Points the process-wide log sink at a fresh temp file for one test case, so the WARN the loop
// emits can be read back as recorded state. The default level (INFO) already passes WARN.
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

// ---------------------------------------------------------------- fake endpoint

struct PullReply {
    int32_t code = ResponseCode::PULL_NOT_FOUND;
    PropertyMap ext;
    Bytes body;
};

// Plays name server (route) and broker (consumer list / offset query / max offset / pull). When the
// script is exhausted the faithful empty answer (PULL_NOT_FOUND, nextBeginOffset 0) keeps the loop
// running without FOUND rounds, so no further warning can be produced after the scripted legs.
// Records the queueOffset EACH PULL_MESSAGE requested: round k+1 being issued proves round k fully
// finished on this strictly serial queue, and the recorded number is the cursor round k stored.
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

    std::vector<int64_t> requestedOffsets() {
        std::lock_guard<std::mutex> lk(state_);
        return requestedOffsets_;
    }

    size_t pullCount() {
        std::lock_guard<std::mutex> lk(state_);
        return requestedOffsets_.size();
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
                resp.code = ResponseCode::QUERY_NOT_FOUND;
            } else if (req.code == RequestCode::GET_MAX_OFFSET) {
                std::lock_guard<std::mutex> lk(state_);
                resp.extFields["offset"] = std::to_string(maxOffset_);
            } else if (req.code == RequestCode::PULL_MESSAGE) {
                const std::string off =
                    req.extFields.count("queueOffset") ? req.extFields.at("queueOffset") : "-999";
                PullReply scripted;
                bool hasScripted = false;
                {
                    std::lock_guard<std::mutex> lk(state_);
                    requestedOffsets_.push_back(std::strtoll(off.c_str(), nullptr, 10));
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
    std::vector<int64_t> requestedOffsets_;
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

PullReply foundReply(int64_t next, const std::vector<MessageExt>& rows) {
    PullReply r;
    r.code = ResponseCode::SUCCESS;
    r.ext = pullExt(next);
    for (const MessageExt& m : rows) r.body += encodeMessageExt(m, false);
    return r;
}

PullReply foundEmptyReply(int64_t next) {
    PullReply r;
    r.code = ResponseCode::SUCCESS;
    r.ext = pullExt(next);
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

// Start the consumer against the fake endpoint and let it own the single queue; from there the REAL
// queuePullLoop drives the scripted rounds, serially on this one queue. Waiting for roundsAtLeast
// ISSUED pulls (not replies) means round k-1 has been fully handled once round k was requested.
void assignQueueAndRunRounds(DefaultMQPushConsumer& c, MockEndpoint& ep, size_t roundsAtLeast) {
    ep.setCidList({c.clientId()});
    c.doRebalanceForTest();
    expect(waitFor([&] { return ep.pullCount() >= roundsAtLeast; }),
           "the pull loop ran at least the expected number of rounds",
           "pullCount=" + std::to_string(ep.pullCount()));
}

// ---------------------------------------------------------------- leg 1a: the pure predicate

// Both legs and their boundaries, driven without any broker: next < prev fires alone, firstMsg < prev
// fires alone even with next >= prev, consistent numbers never fire, and the int64 max sentinel of
// an empty round can never fire the second leg.
void testHelperPredicateLegs() {
    const int64_t kMax = std::numeric_limits<int64_t>::max();
    expect(DefaultMQPushConsumer::pullOffsetsLookWrong(3, 5, 5),
           "leg1.helper.nextBehindPrevFires");
    expect(DefaultMQPushConsumer::pullOffsetsLookWrong(3, 3, 5),
           "leg1.helper.nextAndFirstBothBehindFires");
    expect(DefaultMQPushConsumer::pullOffsetsLookWrong(8, 2, 5),
           "leg2.helper.firstBehindPrevWithNextAheadFires");
    expect(!DefaultMQPushConsumer::pullOffsetsLookWrong(5, 5, 5), "leg3.helper.equalBoundariesQuiet");
    expect(!DefaultMQPushConsumer::pullOffsetsLookWrong(8, 6, 5), "leg3.helper.consistentQuiet");
    expect(!DefaultMQPushConsumer::pullOffsetsLookWrong(6, kMax, 5),
           "leg4.helper.emptyRoundSentinelNeverFires");
    expect(DefaultMQPushConsumer::pullOffsetsLookWrong(1, kMax, 5),
           "leg4.helper.sentinelStillLeavesNextLeg");
}

// ---------------------------------------------------------------- leg 1b: the real loop warns

// Round 1 is a consistent FOUND (0 -> 5, head row 0): no warn. Round 2 is requested at 5 and its
// answer REWINDS the cursor to 3 (head row 3): the round must end with exactly one WARN carrying all
// three numbers.
void testRewoundCursorEmitsWarn() {
    MockEndpoint ep;
    ep.addRoute(kTopic, routeOneQueue(ep.address()));
    ep.setMaxOffset(0);
    ep.scriptPull(foundReply(5, {taggedRow("TagA", 0), taggedRow("TagA", 1)}));
    ep.scriptPull(foundReply(3, {taggedRow("TagA", 3), taggedRow("TagA", 4)}));
    ScopedLogFile log("rmq_pull_offset_sanity_rewind.log");
    auto listener = std::make_shared<CountingListener>();
    DefaultMQPushConsumer c("GID_OffsetSanityRewind");
    c.setInstanceName("offset_sanity_rewind");
    c.setNamesrvAddr(ep.address());
    c.setMessageListener(listener);
    c.subscribe(kTopic, "TagA");
    c.start();
    assignQueueAndRunRounds(c, ep, 3);  // round 3 issued => round 2 fully handled
    const std::string text = log.contents();
    expectInt(static_cast<long long>(countOccurrences(text, kMarker)), 1,
              "leg1.exactlyOneWarnForTheRewindingRound");
    expect(text.find("round requested offset=5") != std::string::npos,
           "leg1.warnCarriesPrevRequestOffset");
    expect(text.find("answer nextBeginOffset=3") != std::string::npos,
           "leg1.warnCarriesNextBeginOffset");
    expect(text.find("firstMsgOffset=3") != std::string::npos, "leg1.warnCarriesFirstMsgOffset");
    c.shutdown();
}

// ---------------------------------------------------------------- leg 2: firstMsg behind prev

// Round 2 is requested at 5, the cursor ADVANCES (next=8 >= 5) but the head of the delivered batch
// sits at 2: the second leg alone must fire, and the warn must carry all three numbers.
void testFirstMsgBehindPrevEmitsWarn() {
    MockEndpoint ep;
    ep.addRoute(kTopic, routeOneQueue(ep.address()));
    ep.setMaxOffset(0);
    ep.scriptPull(foundReply(5, {taggedRow("TagA", 0), taggedRow("TagA", 1)}));
    ep.scriptPull(foundReply(8, {taggedRow("TagA", 2), taggedRow("TagA", 6)}));
    ScopedLogFile log("rmq_pull_offset_sanity_first.log");
    auto listener = std::make_shared<CountingListener>();
    DefaultMQPushConsumer c("GID_OffsetSanityFirstMsg");
    c.setInstanceName("offset_sanity_first");
    c.setNamesrvAddr(ep.address());
    c.setMessageListener(listener);
    c.subscribe(kTopic, "TagA");
    c.start();
    assignQueueAndRunRounds(c, ep, 3);
    const std::string text = log.contents();
    expectInt(static_cast<long long>(countOccurrences(text, kMarker)), 1,
              "leg2.exactlyOneWarnForTheBehindHeadRound");
    expect(text.find("round requested offset=5") != std::string::npos,
           "leg2.warnCarriesPrevRequestOffset");
    expect(text.find("answer nextBeginOffset=8") != std::string::npos, "leg2.warnCarriesNext");
    expect(text.find("firstMsgOffset=2") != std::string::npos, "leg2.warnCarriesFirstMsg");
    c.shutdown();
}

// ---------------------------------------------------------------- leg 3: consistent round, quiet

void testConsistentRoundStaysQuiet() {
    MockEndpoint ep;
    ep.addRoute(kTopic, routeOneQueue(ep.address()));
    ep.setMaxOffset(0);
    ep.scriptPull(foundReply(5, {taggedRow("TagA", 0), taggedRow("TagA", 1)}));
    ep.scriptPull(foundReply(8, {taggedRow("TagA", 5), taggedRow("TagA", 6)}));
    ScopedLogFile log("rmq_pull_offset_sanity_consistent.log");
    auto listener = std::make_shared<CountingListener>();
    DefaultMQPushConsumer c("GID_OffsetSanityConsistent");
    c.setInstanceName("offset_sanity_consistent");
    c.setNamesrvAddr(ep.address());
    c.setMessageListener(listener);
    c.subscribe(kTopic, "TagA");
    c.start();
    assignQueueAndRunRounds(c, ep, 3);
    const std::string text = log.contents();
    expectInt(static_cast<long long>(countOccurrences(text, kMarker)), 0,
              "leg3.noWarnForConsistentRounds");
    // The rounds are UNCHANGED: the cursor advanced exactly to each nextBeginOffset.
    const std::vector<int64_t> req = ep.requestedOffsets();
    expect(req.size() >= 3, "leg3.threeRoundsIssued");
    if (req.size() >= 3) {
        expectInt(req[0], 0, "leg3.round1RequestedInitialCursor");
        expectInt(req[1], 5, "leg3.round2RequestedNextBeginOfRound1");
        expectInt(req[2], 8, "leg3.round3RequestedNextBeginOfRound2");
    }
    c.shutdown();
}

// ---------------------------------------------------------------- leg 4: empty FOUND, sentinel

// Round 2 FOUNDs with NO rows at all; round 3 FOUNDs rows the TagA filter empties entirely. Both
// rounds carry the sentinel firstMsgOffset, so with a forward cursor neither may warn.
void testEmptyFoundRoundsStayQuiet() {
    MockEndpoint ep;
    ep.addRoute(kTopic, routeOneQueue(ep.address()));
    ep.setMaxOffset(0);
    ep.scriptPull(foundReply(5, {taggedRow("TagA", 0)}));
    ep.scriptPull(foundEmptyReply(6));
    ep.scriptPull(foundReply(8, {taggedRow("TagB", 6), taggedRow("TagB", 7)}));
    ScopedLogFile log("rmq_pull_offset_sanity_empty.log");
    auto listener = std::make_shared<CountingListener>();
    DefaultMQPushConsumer c("GID_OffsetSanityEmpty");
    c.setInstanceName("offset_sanity_empty");
    c.setNamesrvAddr(ep.address());
    c.setMessageListener(listener);
    c.subscribe(kTopic, "TagA");
    c.start();
    assignQueueAndRunRounds(c, ep, 4);  // round 4 issued => all three scripted rounds handled
    const std::string text = log.contents();
    expectInt(static_cast<long long>(countOccurrences(text, kMarker)), 0,
              "leg4.noWarnForEmptyAndAllFilteredFoundRounds");
    const std::vector<int64_t> req = ep.requestedOffsets();
    expect(req.size() >= 4, "leg4.fourRoundsIssued");
    if (req.size() >= 4) {
        expectInt(req[2], 6, "leg4.emptyFoundRoundStillAdvancedCursor");
        expectInt(req[3], 8, "leg4.allFilteredRoundStillAdvancedCursor");
    }
    // Only the one delivered row (round 1) ever reached the consume side.
    waitFor([&] { return listener->delivered() >= 1; }, 8000);
    expectInt(listener->delivered(), 1, "leg4.deliveredIsOnlyRound1sRow");
    c.shutdown();
}

// ---------------------------------------------------------------- leg 5: warn does not perturb

// The pair: two consumers run the SAME shape of rounds at the SAME configured pull interval; the
// only difference is that A's round 2 rewinds the cursor (warns) while B's advances it (quiet). A
// firing round must store exactly its nextBeginOffset (round 3 requests it) and, having delivered
// rows, take the delivered-round cadence = the configured interval -- the identical decision B's
// non-firing round makes with its own numbers.
void testFiringRoundKeepsOffsetAndCadence() {
    constexpr int32_t kIntervalMillis = 40;

    // A: round 2 requested at 5, FOUND next=3 with head row 3 -> the warn fires.
    MockEndpoint epA;
    epA.addRoute(kTopic, routeOneQueue(epA.address()));
    epA.setMaxOffset(0);
    epA.scriptPull(foundReply(5, {taggedRow("TagA", 0)}));
    epA.scriptPull(foundReply(3, {taggedRow("TagA", 3), taggedRow("TagA", 4)}));
    ScopedLogFile logA("rmq_pull_offset_sanity_pair_firing.log");
    auto listenerA = std::make_shared<CountingListener>();
    DefaultMQPushConsumer a("GID_OffsetSanityPairFiring");
    a.setInstanceName("offset_sanity_pair_firing");
    a.setNamesrvAddr(epA.address());
    a.setMessageListener(listenerA);
    a.setPullIntervalMillis(kIntervalMillis);
    a.subscribe(kTopic, "TagA");
    a.start();
    assignQueueAndRunRounds(a, epA, 3);

    // B: same rounds except round 2 moves forward (next=7, head row 5) -> no warn.
    MockEndpoint epB;
    epB.addRoute(kTopic, routeOneQueue(epB.address()));
    epB.setMaxOffset(0);
    epB.scriptPull(foundReply(5, {taggedRow("TagA", 0)}));
    epB.scriptPull(foundReply(7, {taggedRow("TagA", 5), taggedRow("TagA", 6)}));
    ScopedLogFile logB("rmq_pull_offset_sanity_pair_quiet.log");
    auto listenerB = std::make_shared<CountingListener>();
    DefaultMQPushConsumer b("GID_OffsetSanityPairQuiet");
    b.setInstanceName("offset_sanity_pair_quiet");
    b.setNamesrvAddr(epB.address());
    b.setMessageListener(listenerB);
    b.setPullIntervalMillis(kIntervalMillis);
    b.subscribe(kTopic, "TagA");
    b.start();
    assignQueueAndRunRounds(b, epB, 3);

    const std::string textA = logA.contents();
    const std::string textB = logB.contents();
    expectInt(static_cast<long long>(countOccurrences(textA, kMarker)), 1,
              "leg5.firingRoundWarnedOnce");
    expectInt(static_cast<long long>(countOccurrences(textB, kMarker)), 0,
              "leg5.quietRoundNeverWarned");

    // Stored cursor: each round stored its OWN nextBeginOffset, warn or no warn -- the third pull of
    // each consumer requests exactly round 2's nextBeginOffset.
    const std::vector<int64_t> reqA = epA.requestedOffsets();
    const std::vector<int64_t> reqB = epB.requestedOffsets();
    expect(reqA.size() >= 3 && reqB.size() >= 3, "leg5.bothRanThreeRounds");
    if (reqA.size() >= 3) expectInt(reqA[2], 3, "leg5.firingRoundStoredItsNextBeginOffset");
    if (reqB.size() >= 3) expectInt(reqB[2], 7, "leg5.quietRoundStoredItsNextBeginOffset");

    // Cadence decision: both round 2s delivered rows, so BOTH record the configured interval as the
    // last cadence -- the warn did not add, remove, or change any wait.
    expectInt(a.lastPullCadenceMillis(), kIntervalMillis, "leg5.firingRoundCadenceIsTheInterval");
    expectInt(b.lastPullCadenceMillis(), kIntervalMillis, "leg5.quietRoundCadenceIsTheInterval");

    // Consumption itself is untouched on the firing side: all 3 scripted rows still reached the
    // listener (1 in round 1 + 2 in round 2).
    waitFor([&] { return listenerA->delivered() >= 3; }, 8000);
    expectInt(listenerA->delivered(), 3, "leg5.firingRoundStillDeliveredItsRows");

    a.shutdown();
    b.shutdown();
}

}  // namespace

int main() {
    testHelperPredicateLegs();
    testRewoundCursorEmitsWarn();
    testFirstMsgBehindPrevEmitsWarn();
    testConsistentRoundStaysQuiet();
    testEmptyFoundRoundsStayQuiet();
    testFiringRoundKeepsOffsetAndCadence();
    std::printf("pull_offset_sanity: %d checks, %d failures\n", checks, fails);
    return fails == 0 ? 0 : 1;
}
