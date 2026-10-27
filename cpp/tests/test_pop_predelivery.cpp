// Offline unit tests for the POP pre-delivery pipeline -- no cluster needed.
//
// Three things a popped batch owes before a listener ever sees it, each of which fails quietly on a
// real broker:
//   (1) the pipeline order (client-side tag re-check -> FilterMessageHook -> reconsume-budget drop) and
//       the ack of **every** row it removed. A dropped row without an ack comes back when invisibleTime
//       expires, so the visible symptom is "the filter does not work", minutes later;
//   (2) the round's statistics and its pacing keyed on the rows that actually survived, not on the raw
//       answer -- recording before the filter counts rows the listener never saw;
//   (3) the delivery entry restoring the business topic out of either POP retry form
//       "%RETRY%<group>_<topic>" and "%RETRY%<group>+<topic>", while leaving the classic form and
//       another group's form alone.
//
// The budget drop and the pacing are decisions, so they are pinned as pure functions with a paired
// control for every positive case. The acking is wire behaviour, so it is pinned against a loopback
// endpoint that keeps every frame it read: assertions are made on recorded request codes and fields,
// never on what the caller claims.
#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <functional>
#include <limits>
#include <map>
#include <memory>
#include <mutex>
#include <random>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include "rocketmq/client/consumer.h"
#include "rocketmq/client/consumer_stats.h"
#include "rocketmq/client/hook.h"
#include "rocketmq/client/mq_client.h"
#include "rocketmq/client/result.h"
#include "rocketmq/common/byte_buffer.h"
#include "rocketmq/common/logging.h"
#include "rocketmq/common/message.h"
#include "rocketmq/common/message_const.h"
#include "rocketmq/common/mix_all.h"
#include "rocketmq/common/net_compat.h"
#include "rocketmq/common/subscription_data.h"
#include "rocketmq/common/util_all.h"
#include "rocketmq/remoting/protocol/codes.h"
#include "rocketmq/remoting/protocol/extra_info.h"
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

void expectInt(long long actual, long long expected, const std::string& name,
               const std::string& detail) {
    ++checks;
    if (actual != expected) {
        ++fails;
        std::printf("FAIL %s (actual=%lld expected=%lld) %s\n", name.c_str(), actual, expected,
                    detail.c_str());
    }
}

const char* kGroup = "GID_PopPreDeliverCpp";
const char* kTopic = "PopPreDeliverCppTopic";
const char* kBroker = "broker-a";

MessageQueue queue0() { return MessageQueue(kTopic, kBroker, 0); }

// ---------------------------------------------------------------- fixtures

// A popped row: a distinct msgId (the difference between the popped list and the kept list is taken by
// msgId), the tag the subscription asks for unless told otherwise, and a fresh 8-segment POP_CK so the
// consume side does not judge the whole batch as already past invisibleTime.
MessageExt poppedRow(const std::string& msgId, int64_t queueOffset, int32_t reconsumeTimes,
                     const std::string& tags = "TagA", const std::string& topic = kTopic) {
    MessageExt m;
    m.topic = topic;
    m.brokerName = kBroker;
    m.queueId = 0;
    m.queueOffset = queueOffset;
    m.msgId = msgId;
    m.body = "body-" + msgId;
    m.reconsumeTimes = reconsumeTimes;
    m.bornTimestamp = UtilAll::currentTimeMillis();
    m.setTags(tags);
    m.putProperty(MessageConst::PROPERTY_POP_CK,
                  extra_info::buildExtraInfo(/*ckQueueOffset=*/queueOffset,
                                             /*popTime=*/UtilAll::currentTimeMillis(),
                                             /*invisibleTime=*/60000, /*reviveQid=*/0, topic, kBroker,
                                             /*queueId=*/0, /*msgQueueOffset=*/queueOffset));
    return m;
}

std::string idsOf(const std::vector<MessageExt>& msgs) {
    std::string out;
    for (const MessageExt& m : msgs) {
        if (!out.empty()) out += ",";
        out += m.msgId;
    }
    return out;
}

std::string topicsOf(const std::vector<MessageExt>& msgs) {
    std::string out;
    for (const MessageExt& m : msgs) {
        if (!out.empty()) out += ",";
        out += m.topic;
    }
    return out;
}

std::string joined(const std::set<std::string>& values) {
    std::string out;
    for (const std::string& v : values) {
        if (!out.empty()) out += ",";
        out += v;
    }
    return out;
}

// The POP retry form of this group's subscription, and the two shapes that must not be confused with it.
std::string popRetryTopic(const std::string& group, const std::string& topic) {
    return std::string(MixAll::RETRY_GROUP_TOPIC_PREFIX) + group + "_" + topic;
}
std::string plusRetryTopic(const std::string& group, const std::string& topic) {
    return std::string(MixAll::RETRY_GROUP_TOPIC_PREFIX) + group + "+" + topic;
}

// Silences the recorder: these fixtures walk paths whose ack cannot reach a broker offline, and the
// log wording of the one leg that is asserted is captured explicitly instead.
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

// Recorder redirected into a file, so "one INFO line per dropped row" is counted rather than assumed.
class LogCapture {
public:
    LogCapture() {
        static std::mt19937_64 rng{std::random_device{}()};
        path_ = "/tmp/rmq_cpp_pop_predelivery_capture_" + std::to_string(rng()) + ".log";
        std::remove(path_.c_str());
        previousLevel_ = logLevel();
        setLogFile(path_);
        setLogLevel(LOG_DEBUG);
    }
    ~LogCapture() {
        flushLogFile();
        setLogFile("");
        flushLogFile();
        setLogLevel(previousLevel_);
        std::remove(path_.c_str());
    }
    std::vector<std::string> lines(const std::string& prefix) {
        flushLogFile();
        std::vector<std::string> out;
        std::FILE* f = std::fopen(path_.c_str(), "rb");
        if (f != nullptr) {
            std::string line;
            int ch = EOF;
            while ((ch = std::fgetc(f)) != EOF) {
                if (ch == '\n') {
                    if (line.find(prefix) != std::string::npos) out.push_back(line);
                    line.clear();
                } else {
                    line.push_back(static_cast<char>(ch));
                }
            }
            if (!line.empty() && line.find(prefix) != std::string::npos) out.push_back(line);
            std::fclose(f);
        }
        return out;
    }

private:
    std::string path_;
    int previousLevel_ = 0;
};

class RecordingConcurrentListener : public MessageListenerConcurrently {
public:
    ConsumeConcurrentlyStatus consumeMessage(const std::vector<MessageExt>& msgs,
                                            ConsumeConcurrentlyContext&) override {
        std::lock_guard<std::mutex> lk(mu_);
        batches_.push_back(msgs);
        return ConsumeConcurrentlyStatus::CONSUME_SUCCESS;
    }

    size_t batchCount() {
        std::lock_guard<std::mutex> lk(mu_);
        return batches_.size();
    }
    std::vector<MessageExt> delivered() {
        std::lock_guard<std::mutex> lk(mu_);
        std::vector<MessageExt> all;
        for (const std::vector<MessageExt>& b : batches_) {
            for (const MessageExt& m : b) all.push_back(m);
        }
        return all;
    }

private:
    std::mutex mu_;
    std::vector<std::vector<MessageExt>> batches_;
};

class RecordingOrderlyListener : public MessageListenerOrderly {
public:
    ConsumeOrderlyStatus consumeMessage(const std::vector<MessageExt>& msgs,
                                        ConsumeOrderlyContext&) override {
        std::lock_guard<std::mutex> lk(mu_);
        delivered_ = msgs;
        ++calls_;
        return ConsumeOrderlyStatus::SUCCESS;
    }
    int calls() {
        std::lock_guard<std::mutex> lk(mu_);
        return calls_;
    }
    std::vector<MessageExt> delivered() {
        std::lock_guard<std::mutex> lk(mu_);
        return delivered_;
    }

private:
    std::mutex mu_;
    int calls_ = 0;
    std::vector<MessageExt> delivered_;
};

// A filter hook that removes rows by msgId: the second stage of the pipeline, whose removal the POP
// path also has to ack.
class DropByIdHook : public FilterMessageHook {
public:
    explicit DropByIdHook(std::vector<std::string> ids) : ids_(std::move(ids)) {}
    std::string hookName() const override { return "drop-by-msg-id"; }
    void filterMessage(FilterMessageContext& context) override {
        std::vector<MessageExt> kept;
        for (const MessageExt& m : context.msgList) {
            if (std::find(ids_.begin(), ids_.end(), m.msgId) == ids_.end()) kept.push_back(m);
        }
        context.msgList = kept;
    }

private:
    std::vector<std::string> ids_;
};

// ---------------------------------------------------------------- loopback endpoint

struct Captured {
    int32_t code = 0;
    PropertyMap extFields;
};

// One loopback listener playing both roles the POP path addresses: the name server (answers 105 with
// the scripted route, so a broker name resolves to this very port) and the broker (records every
// request frame it reads and answers it).
class PopEndpoint {
public:
    PopEndpoint() {
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
        ::listen(listen_, 32);
        socklen_type len = sizeof(addr);
        ::getsockname(listen_, reinterpret_cast<sockaddr*>(&addr), &len);
        port_ = ntohs(addr.sin_port);
        running_.store(true);
        acceptor_ = std::thread([this]() { acceptLoop(); });
    }

    ~PopEndpoint() {
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

    PopEndpoint(const PopEndpoint&) = delete;
    PopEndpoint& operator=(const PopEndpoint&) = delete;

    std::string address() const { return "127.0.0.1:" + std::to_string(port_); }

    void addRoute(const std::string& topic) {
        TopicRouteData route;
        std::map<int64_t, std::string> addrs;
        addrs[MixAll::MASTER_ID] = address();
        route.brokerDatas.emplace_back("DefaultCluster", kBroker, addrs);
        route.queueDatas.emplace_back(kBroker, 1, 1, 6, 0);
        std::lock_guard<std::mutex> lk(state_);
        routes_[topic] = route.encode();
    }

    std::vector<Captured> requestsOf(int32_t code) const {
        std::lock_guard<std::mutex> lk(state_);
        std::vector<Captured> out;
        for (const Captured& c : captured_) {
            if (c.code == code) out.push_back(c);
        }
        return out;
    }

    int countOf(int32_t code) const {
        std::lock_guard<std::mutex> lk(state_);
        int n = 0;
        for (const Captured& c : captured_) {
            if (c.code == code) ++n;
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

    // 100ms select rounds: every server-side wait is bounded and exits promptly once running_ is false.
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
            if (req.isResponseType()) continue;
            {
                std::lock_guard<std::mutex> lk(state_);
                captured_.push_back(Captured{req.code, req.extFields});
            }
            RemotingCommand resp;
            resp.opaque = req.opaque;
            resp.markResponseType();
            resp.code = ResponseCode::SUCCESS;
            std::lock_guard<std::mutex> lk(state_);
            if (req.code == RequestCode::GET_ROUTEINFO_BY_TOPIC) {
                const std::string topic =
                    req.extFields.count("topic") ? req.extFields.at("topic") : std::string();
                auto it = routes_.find(topic);
                if (it == routes_.end()) {
                    // Unknown topic: no queue of it can be assigned, so no background pop loop starts.
                    resp.code = ResponseCode::TOPIC_NOT_EXIST;
                } else {
                    resp.body = it->second;
                    resp.hasBody = true;
                }
            } else if (req.code == RequestCode::POP_MESSAGE) {
                // A long poll that came back empty: the background loop (if any ever got a queue)
                // neither delivers nor acks anything, so the frames asserted below stay the test's own.
                resp.code = ResponseCode::POLLING_TIMEOUT;
            }
            if (!writeAll(sock, resp.encode())) break;
        }
        ::shutdown(sock, kShutdownHow);
        netcompat::closeSocket(sock);
    }

    socket_t listen_ = kInvalidSocket;
    uint16_t port_ = 0;
    std::atomic<bool> running_{false};
    mutable std::mutex state_;
    std::map<std::string, Bytes> routes_;
    std::vector<Captured> captured_;
    std::mutex workersM_;
    std::vector<std::thread> workers_;
    std::thread acceptor_;
};

// Bounded predicate poll; maxMs is the explicit budget of the leg.
bool eventually(const std::function<bool()>& pred, int maxMs) {
    for (int waited = 0; waited <= maxMs; waited += 10) {
        if (pred()) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    return pred();
}

// ---------------------------------------------------------------- (1) the budget drop

// Budget 2 with rows at reconsume counts 1, 2 and 3 must drop ONLY the 3: the line is "strictly over",
// so a >= comparison (which would also kill the row that reached the budget) goes red on the second
// row, and a "drop nothing" comparison goes red on the third.
void testBudgetDropIsStrictlyOverTheEffectiveBudget() {
    QuietLog quiet;
    DefaultMQPushConsumer c(kGroup);
    c.setMaxReconsumeTimes(2);
    expectInt(c.maxReconsumeTimesOrDefault(), 2, "budget.knobIsHonoured");

    std::vector<MessageExt> rows;
    rows.push_back(poppedRow("r1", 1, 1));
    rows.push_back(poppedRow("r2", 2, 2));
    rows.push_back(poppedRow("r3", 3, 3));
    const std::vector<MessageExt> kept = c.dropRowsOverReconsumeBudget(rows);
    expect(idsOf(kept) == "r1,r2", "budget.onlyTheRowOverTheLineDrops", idsOf(kept));
    expectInt(static_cast<long long>(kept.size()), 2, "budget.theRowAtTheLineSurvives");
    // The popped list is not modified in place: the ack difference is taken against it afterwards.
    expectInt(static_cast<long long>(rows.size()), 3, "budget.inputListUntouched");

    // Nothing over the line: the whole batch is handed on.
    std::vector<MessageExt> under;
    under.push_back(poppedRow("u1", 1, 0));
    under.push_back(poppedRow("u2", 2, 2));
    expectInt(static_cast<long long>(c.dropRowsOverReconsumeBudget(under).size()), 2,
              "budget.noneOverTheLineKeepsAll");

    // An empty answer gives an empty kept list instead of throwing.
    expect(c.dropRowsOverReconsumeBudget(std::vector<MessageExt>()).empty(),
           "budget.emptyInEmptyOut");
}

// maxReconsumeTimes = -1 means "the broker decides", and for this path that is the broker default 16 --
// NOT the integer maximum the orderly side resolves it to. A row at 17 drops and a row at 16 is
// delivered: exactly the pair the two resolutions disagree on.
void testDefaultBudgetIsSixteenAndNotUnlimited() {
    QuietLog quiet;
    DefaultMQPushConsumer c(kGroup);
    expectInt(c.maxReconsumeTimesOrDefault(), 16, "defaultBudget.minusOneResolvesTo16");

    std::vector<MessageExt> rows;
    rows.push_back(poppedRow("a", 1, 15));
    rows.push_back(poppedRow("b", 2, 16));
    rows.push_back(poppedRow("c", 3, 17));
    rows.push_back(poppedRow("d", 4, std::numeric_limits<int32_t>::max()));
    const std::vector<MessageExt> kept = c.dropRowsOverReconsumeBudget(rows);
    expect(idsOf(kept) == "a,b", "defaultBudget.onlyOver16Drops", idsOf(kept));
    expect(kept.size() < rows.size(), "defaultBudget.notUnlimited");
    // The same fixture through the orderly-side resolution would keep all four rows.
    expectInt(c.orderlyMaxReconsumeTimes(), std::numeric_limits<int32_t>::max(),
              "defaultBudget.theOtherResolutionIsNotUsed");
}

// One INFO line per row the budget removed, naming its reconsume count and saying the row is acked:
// the only trace of why a popped row never reached a listener.
void testBudgetDropLogsOneInfoLinePerRow() {
    DefaultMQPushConsumer c(kGroup);
    c.setMaxReconsumeTimes(2);
    std::vector<MessageExt> rows;
    rows.push_back(poppedRow("k1", 1, 1));
    rows.push_back(poppedRow("k2", 2, 3));
    rows.push_back(poppedRow("k3", 3, 4));
    size_t kept = 0;
    {
        LogCapture log;
        kept = c.dropRowsOverReconsumeBudget(rows).size();
        const std::vector<std::string> infos = log.lines(" INFO  [");
        int mentions3 = 0;
        int mentions4 = 0;
        int ackLines = 0;
        int mentionsKeptRow = 0;
        for (const std::string& line : infos) {
            if (line.find("reconsumeTimes=3") != std::string::npos) ++mentions3;
            if (line.find("reconsumeTimes=4") != std::string::npos) ++mentions4;
            if (line.find("k1") != std::string::npos) ++mentionsKeptRow;
            if (line.find("ack") != std::string::npos) ++ackLines;
        }
        expectInt(static_cast<long long>(kept), 1, "budgetLog.theRowAtOneSurvives");
        expectInt(mentions3, 1, "budgetLog.oneLineForTheRowAt3");
        expectInt(mentions4, 1, "budgetLog.oneLineForTheRowAt4");
        expectInt(mentionsKeptRow, 0, "budgetLog.keptRowGetsNoLine");
        // Two dropped rows make two lines, not one summary line for the batch.
        expectInt(ackLines, 2, "budgetLog.eachLineSaysTheRowIsAcked");
    }
}

// The budget drop is POP-only. filterMessagesForDelivery is shared with the pull path, whose cursor
// advances anyway and which has no ack to give, so the very same over-budget row must still come out
// of the pull path's pre-delivery step intact.
void testPullPathStillDeliversTheOverBudgetRow() {
    QuietLog quiet;
    DefaultMQPushConsumer c(kGroup);
    c.setMaxReconsumeTimes(2);
    const SubscriptionData sub = FilterAPI::buildSubscriptionData(kTopic, "TagA");
    std::vector<MessageExt> rows;
    rows.push_back(poppedRow("p1", 1, 1));
    rows.push_back(poppedRow("p2", 2, 9));

    // Pull path: the tag re-check and the hooks only, so the over-budget row is still delivered.
    expect(idsOf(c.filterMessagesForDelivery(queue0(), &sub, rows)) == "p1,p2",
           "pullPath.deliversTheOverBudgetRow", idsOf(c.filterMessagesForDelivery(queue0(), &sub, rows)));
    // POP path on the same rows: the budget stage is what removes it.
    expect(idsOf(c.dropRowsOverReconsumeBudget(rows)) == "p1", "popPath.dropsItInstead");

    // The shared step keeps doing its own job: a row whose tag does not match never reaches a listener.
    std::vector<MessageExt> wrongTag;
    wrongTag.push_back(poppedRow("p1", 1, 1));
    wrongTag.push_back(poppedRow("p2", 2, 9, "TagB"));
    expect(idsOf(c.filterMessagesForDelivery(queue0(), &sub, wrongTag)) == "p1",
           "sharedStep.stillReChecksTags");
    // Subscribing to everything (an empty tag set) filters nothing -- the "*" guard.
    const SubscriptionData all = FilterAPI::buildSubscriptionData(kTopic, "*");
    expectInt(static_cast<long long>(c.filterMessagesForDelivery(queue0(), &all, wrongTag).size()),
              2, "sharedStep.wildcardFiltersNothing");
    // No subscription at all (nullptr) skips the tag stage as well.
    expectInt(static_cast<long long>(c.filterMessagesForDelivery(queue0(), nullptr, wrongTag).size()),
              2, "sharedStep.noSubscriptionFiltersNothing");
}

// ---------------------------------------------------------------- (2) every dropped row is acked

// One round through the whole pipeline: tag re-check -> hook -> budget. Each of the three removals is
// acked against the ORIGINAL popped list; the surviving row is handed on and not acked here.
void testPipelineAcksEveryDroppedRowOnTheTransport() {
    PopEndpoint ep;
    ep.addRoute(kTopic);

    auto listener = std::make_shared<RecordingConcurrentListener>();
    DefaultMQPushConsumer c(kGroup);
    c.setNamesrvAddr(ep.address());
    c.setPopMode(true);
    c.setMaxReconsumeTimes(2);
    c.setMessageListener(listener);
    c.subscribe(kTopic, "TagA");
    c.registerFilterMessageHook(std::make_shared<DropByIdHook>(std::vector<std::string>{"h1"}));
    {
        QuietLog quiet;
        c.start();
    }

    const SubscriptionData sub = FilterAPI::buildSubscriptionData(kTopic, "TagA");
    std::vector<MessageExt> rows;
    rows.push_back(poppedRow("t1", 100, 1, "TagB"));  // removed by the tag re-check
    rows.push_back(poppedRow("h1", 101, 1));          // removed by the filter hook
    rows.push_back(poppedRow("b1", 102, 3));          // removed by the reconsume budget
    rows.push_back(poppedRow("keep", 103, 2));        // survives: exactly at the budget, right tag
    auto pq = std::make_shared<PopProcessQueue>();
    pq->incFoundMsg(static_cast<int32_t>(rows.size()));  // what the pop loop stamps before the pipeline

    std::vector<MessageExt> kept;
    {
        QuietLog quiet;
        kept = c.preparePopBatchForDelivery(queue0(), &sub, rows, pq);
    }
    expect(idsOf(kept) == "keep", "pipeline.onlyTheSurvivorIsHandedOn", idsOf(kept));

    expect(eventually([&] { return ep.countOf(RequestCode::ACK_MESSAGE) == 3; }, 3000),
           "pipeline.threeAcksOnTheWire", std::to_string(ep.countOf(RequestCode::ACK_MESSAGE)));
    std::set<std::string> ackedOffsets;
    std::set<std::string> ackedExtraInfo;
    for (const Captured& cap : ep.requestsOf(RequestCode::ACK_MESSAGE)) {
        auto off = cap.extFields.find("offset");
        auto info = cap.extFields.find("extraInfo");
        auto topic = cap.extFields.find("topic");
        auto group = cap.extFields.find("consumerGroup");
        if (off != cap.extFields.end()) ackedOffsets.insert(off->second);
        if (info != cap.extFields.end()) ackedExtraInfo.insert(info->second);
        expect(topic != cap.extFields.end() && topic->second == kTopic,
               "pipeline.ackNamesTheBusinessTopic",
               topic == cap.extFields.end() ? std::string("<missing>") : topic->second);
        expect(group != cap.extFields.end() && group->second == kGroup,
               "pipeline.ackNamesTheGroup",
               group == cap.extFields.end() ? std::string("<missing>") : group->second);
    }
    expect(ackedOffsets.count("100") == 1 && ackedOffsets.count("101") == 1 &&
               ackedOffsets.count("102") == 1,
           "pipeline.everyDroppedRowIsAckedAgainstThePoppedList", joined(ackedOffsets));
    // The paired control: the row handed to the listener is not acked by the pipeline. Confirming a
    // delivered row is the consume side's job, and acking it early would drop a message the listener
    // has not finished with.
    expect(ackedOffsets.count("103") == 0, "pipeline.deliveredRowIsNotAcked",
           joined(ackedOffsets));
    expectInt(static_cast<long long>(ackedExtraInfo.size()), 3, "pipeline.oneAckPerDroppedRow",
              joined(ackedExtraInfo));

    // Every ack also releases one slot of this queue's wait-ack debt, so a filtered row cannot pin
    // the round gate open.
    expectInt(pq->waitAckCount(), 1, "pipeline.debtReleasedPerAck",
              std::to_string(pq->waitAckCount()));
    expectInt(c.filteredMessageCount(), 3, "pipeline.countsEveryRemoval",
              std::to_string(c.filteredMessageCount()));

    // A clean round: nothing removed, so nothing acked and no counter moved.
    std::vector<MessageExt> clean;
    clean.push_back(poppedRow("keep2", 104, 0));
    clean.push_back(poppedRow("keep3", 105, 1));
    {
        QuietLog quiet;
        expect(idsOf(c.preparePopBatchForDelivery(queue0(), &sub, clean, pq)) == "keep2,keep3",
               "pipeline.cleanRoundHandsEverythingOn");
    }
    expectInt(ep.countOf(RequestCode::ACK_MESSAGE), 3, "pipeline.cleanRoundAcksNothing");
    expectInt(c.filteredMessageCount(), 3, "pipeline.cleanRoundCountsNothing");
    expectInt(pq->waitAckCount(), 1, "pipeline.cleanRoundChangesNoDebt",
              std::to_string(pq->waitAckCount()));

    {
        QuietLog quiet;
        c.shutdown();
    }
}

// ---------------------------------------------------------------- (3) statistics and pacing

// The round's numbers describe what it delivered. A FOUND keeps its round-trip time even when the
// pipeline emptied it, but a pull TPS of rows no listener ever saw would be a plain lie.
void testAnswerStatsKeyOnTheRowsThatSurvive() {
    QuietLog quiet;
    auto listener = std::make_shared<RecordingConcurrentListener>();
    DefaultMQPushConsumer c(kGroup);
    c.setMaxReconsumeTimes(2);
    c.setMessageListener(listener);
    ConsumerStatsManager stats;
    const std::string key = std::string(kTopic) + "@" + kGroup;
    const SubscriptionData sub = FilterAPI::buildSubscriptionData(kTopic, "TagA");

    // FOUND with three rows, of which the budget removes one.
    PopResult found;
    found.status = PopStatus::FOUND;
    found.msgFoundList.push_back(poppedRow("s1", 1, 1));
    found.msgFoundList.push_back(poppedRow("s2", 2, 2));
    found.msgFoundList.push_back(poppedRow("s3", 3, 3));
    auto pq = std::make_shared<PopProcessQueue>();
    const int64_t began = UtilAll::currentTimeMillis() - 40;
    c.processPopAnswer(queue0(), sub, found, pq, stats, began);

    expectInt(static_cast<long long>(found.msgFoundList.size()), 2,
              "stats.keptRowsArePutBackIntoTheAnswer", idsOf(found.msgFoundList));
    // The kept rows are what reached the listener (no pop executor offline, so the dispatch is
    // synchronous and readable right here).
    expect(idsOf(listener->delivered()) == "s1,s2", "stats.deliveredAreExactlyTheKeptRows",
           idsOf(listener->delivered()));

    std::shared_ptr<StatsItem> rt = stats.topicAndGroupPullRT().find(key);
    std::shared_ptr<StatsItem> tps = stats.topicAndGroupPullTPS().find(key);
    expect(rt != nullptr, "stats.rtCellExists");
    expect(tps != nullptr, "stats.tpsCellExists");
    if (rt == nullptr || tps == nullptr) return;
    expectInt(rt->times(), 1, "stats.rtRecordedOnce");
    const int64_t firstRoundRt = rt->value();
    expect(firstRoundRt >= 40 && firstRoundRt < 5000, "stats.rtIsTheRoundTrip",
           std::to_string(firstRoundRt));
    // The load-bearing pair: TPS carries the KEPT count, not the popped count.
    expectInt(tps->times(), 1, "stats.tpsRecordedOnce");
    expectInt(tps->value(), 2, "stats.tpsCountsKeptRowsOnly", std::to_string(tps->value()));
    // The popped batch's debt is fully accounted either way: 3 counted in, 1 acked by the pipeline,
    // 2 confirmed by the successful consume.
    expectInt(pq->waitAckCount(), 0, "stats.wholeBatchLeavesNoDebt",
              std::to_string(pq->waitAckCount()));

    // A FOUND whose rows are ALL filtered out: the round still happened, so its RT is recorded, but it
    // delivered nothing -- no TPS slot and no submit at all.
    PopResult allGone;
    allGone.status = PopStatus::FOUND;
    allGone.msgFoundList.push_back(poppedRow("x1", 4, 7));
    allGone.msgFoundList.push_back(poppedRow("x2", 5, 8));
    auto pq2 = std::make_shared<PopProcessQueue>();
    const size_t batchesBefore = listener->batchCount();
    c.processPopAnswer(queue0(), sub, allGone, pq2, stats, UtilAll::currentTimeMillis() - 10);
    expect(allGone.msgFoundList.empty(), "stats.everyRowWasFiltered");
    expectInt(rt->times(), 2, "stats.filteredOutFoundStillRecordsRt");
    expect(rt->value() - firstRoundRt >= 10, "stats.filteredOutFoundRtIsItsOwnRoundTrip",
           std::to_string(rt->value() - firstRoundRt));
    expectInt(tps->times(), 1, "stats.filteredOutFoundRecordsNoTps", std::to_string(tps->times()));
    expectInt(static_cast<long long>(listener->batchCount()), static_cast<long long>(batchesBefore),
              "stats.filteredOutFoundSubmitsNothing");
    expectInt(pq2->waitAckCount(), 0, "stats.filteredOutBatchLeavesNoDebt",
              std::to_string(pq2->waitAckCount()));

    // An empty-handed long poll is not a pull round at all: neither cell may move, or the average pull
    // time gets diluted by the broker's suspend duration into fake numbers.
    PopResult polling;
    polling.status = PopStatus::POLLING_NOT_FOUND;
    c.processPopAnswer(queue0(), sub, polling, pq2, stats, UtilAll::currentTimeMillis() - 5000);
    expectInt(rt->times(), 2, "stats.pollingRecordsNoRt");
    expectInt(tps->times(), 1, "stats.pollingRecordsNoTps");
}

// The pacing decision as a table over (answer, kept count, pull interval) -> wait in ms, tested
// directly so no leg depends on wall time.
void testRescheduleDecisionTable() {
    expectInt(DefaultMQPushConsumer::popNextRoundDelayMillis(PopStatus::FOUND, 3, 0), 0,
              "pace.foundWithRowsAndNoIntervalGoesStraightOn");
    expectInt(DefaultMQPushConsumer::popNextRoundDelayMillis(PopStatus::FOUND, 3, 100), 100,
              "pace.foundWithRowsWaitsThePullInterval");
    // The paired control: the interval paces rounds that DELIVERED rows, not rounds that had rows but
    // lost every one of them to the filter.
    expectInt(DefaultMQPushConsumer::popNextRoundDelayMillis(PopStatus::FOUND, 0, 100), 0,
              "pace.foundWithNothingLeftIgnoresTheInterval");
    expectInt(DefaultMQPushConsumer::popNextRoundDelayMillis(PopStatus::NO_NEW_MSG, 0, 100), 0,
              "pace.noNewMsgGoesStraightOn");
    expectInt(DefaultMQPushConsumer::popNextRoundDelayMillis(PopStatus::POLLING_NOT_FOUND, 0, 500), 0,
              "pace.pollingNotFoundGoesStraightOn");
    // POLLING_FULL is the broker saying its polling window is full: back off, whatever else the round
    // looked like -- an interval set, rows still kept, or a round that answered instantly.
    expectInt(DefaultMQPushConsumer::popNextRoundDelayMillis(PopStatus::POLLING_FULL, 0, 100), 3000,
              "pace.pollingFullBacksOff");
    expectInt(DefaultMQPushConsumer::popNextRoundDelayMillis(PopStatus::POLLING_FULL, 4, 0), 3000,
              "pace.pollingFullBacksOffEvenWithRowsKept");
    expectInt(DefaultMQPushConsumer::popNextRoundDelayMillis(PopStatus::POLLING_FULL, 0, 0), 3000,
              "pace.pollingFullIsNotSwallowedByTheShortRoundNap");
    // An answer this client does not know is treated like a failed round, not like an empty one.
    expectInt(DefaultMQPushConsumer::popNextRoundDelayMillis(static_cast<PopStatus>(7), 0, 100), 3000,
              "pace.unknownAnswerBacksOff");
    expectInt(kPopBackoffWhenAnswerIsFull, 3000, "pace.backoffIsTheExceptionPauseValue");
    // An interval of zero or less is "no pacing", never a wait of its own.
    expectInt(DefaultMQPushConsumer::popNextRoundDelayMillis(PopStatus::FOUND, 1, -5), 0,
              "pace.negativeIntervalIsNoWait");
}

// ---------------------------------------------------------------- (4) retry topic at the delivery entry

// Four shapes, one of which must not move: both POP retry forms of THIS group carry the business topic
// in their own name, the classic form and another group's form do not.
void testPrefixStripAdmitsBothWireSpellingsOfThisGroup() {
    DefaultMQPushConsumer c(kGroup);
    std::vector<MessageExt> msgs(4);
    msgs[0].topic = popRetryTopic(kGroup, "T");          // this group's underscore form
    msgs[1].topic = MixAll::getRetryTopic(kGroup);       // the classic "%RETRY%<group>"
    msgs[2].topic = popRetryTopic("OtherGrp", "T");      // another group's POP retry form
    msgs[3].topic = plusRetryTopic(kGroup, "T");         // this group's plus form
    c.stripPopRetryTopicPrefix(msgs);
    expect(msgs[0].topic == "T", "strip.underscoreFormBecomesTheBusinessTopic", msgs[0].topic);
    expect(msgs[1].topic == MixAll::getRetryTopic(kGroup), "strip.classicFormUntouched",
           msgs[1].topic);
    expect(msgs[2].topic == popRetryTopic("OtherGrp", "T"), "strip.otherGroupFormUntouched",
           msgs[2].topic);
    expect(msgs[3].topic == "T", "strip.plusFormBecomesTheBusinessTopic", msgs[3].topic);

    // A topic that is only the prefix (nothing left to restore) and an ordinary business topic both
    // stay as they are.
    std::vector<MessageExt> edge(4);
    edge[0].topic = std::string(MixAll::RETRY_GROUP_TOPIC_PREFIX) + kGroup + "_";
    edge[1].topic = kTopic;
    edge[2].topic = std::string(MixAll::RETRY_GROUP_TOPIC_PREFIX) + kGroup + "+";
    edge[3].topic = popRetryTopic(kGroup, "T_With_Underscore");
    c.stripPopRetryTopicPrefix(edge);
    expect(edge[0].topic == std::string(MixAll::RETRY_GROUP_TOPIC_PREFIX) + kGroup + "_",
           "strip.bareUnderscorePrefixUntouched", edge[0].topic);
    expect(edge[1].topic == kTopic, "strip.businessTopicUntouched", edge[1].topic);
    expect(edge[2].topic == std::string(MixAll::RETRY_GROUP_TOPIC_PREFIX) + kGroup + "+",
           "strip.barePlusPrefixUntouched", edge[2].topic);
    expect(edge[3].topic == "T_With_Underscore", "strip.keepsEverySeparatorInTheTail",
           edge[3].topic);

    // Anchored at the start, not a substring search.
    std::vector<MessageExt> lookalike(1);
    lookalike[0].topic = kTopic;
    c.stripPopRetryTopicPrefix(lookalike);
    expect(lookalike[0].topic == kTopic, "strip.isAnchoredAtTheStart", lookalike[0].topic);

    // Idempotent: a second pass over an already-restored batch changes nothing.
    c.stripPopRetryTopicPrefix(msgs);
    expect(msgs[0].topic == "T", "strip.secondPassChangesNothing", msgs[0].topic);

    // An empty batch is a no-op.
    std::vector<MessageExt> none;
    c.stripPopRetryTopicPrefix(none);
    expect(none.empty(), "strip.emptyBatchIsNoOp");
}

// The strip sits at the concurrent batch dispatch entries, immediately in front of the property-based
// restore -- which it must not replace: the classic form still recovers through the RETRY_TOPIC
// property. The orderly dispatch path is out of scope and keeps the topic the broker wrote.
void testConcurrentDispatchEntriesApplyTheStrip() {
    QuietLog quiet;
    const std::string classicRetry = MixAll::getRetryTopic(kGroup);

    {
        // The POP concurrent batch entry.
        auto listener = std::make_shared<RecordingConcurrentListener>();
        DefaultMQPushConsumer c(kGroup);
        c.setMessageListener(listener);
        std::vector<MessageExt> batch;
        batch.push_back(poppedRow("d1", 1, 0, "TagA", popRetryTopic(kGroup, kTopic)));
        batch.push_back(poppedRow("d2", 2, 0, "TagA", plusRetryTopic(kGroup, kTopic)));
        batch.push_back(poppedRow("d3", 3, 0, "TagA", classicRetry));
        batch.push_back(poppedRow("d4", 4, 0, "TagA", popRetryTopic("OtherGrp", kTopic)));
        batch[2].putProperty(MessageConst::PROPERTY_RETRY_TOPIC, kTopic);
        auto pq = std::make_shared<PopProcessQueue>();
        pq->incFoundMsg(static_cast<int32_t>(batch.size()));
        // No pop executor offline, so the dispatch runs here and the listener's view is readable.
        c.submitPopConsumeRequest(batch, pq, queue0());
        const std::vector<MessageExt> got = listener->delivered();
        expectInt(static_cast<long long>(got.size()), 4, "dispatch.popEntryDeliversTheWholeBatch",
                  topicsOf(got));
        if (got.size() == 4) {
            expect(got[0].topic == kTopic, "dispatch.popEntryStripsTheUnderscoreForm", got[0].topic);
            expect(got[1].topic == kTopic, "dispatch.popEntryStripsThePlusForm", got[1].topic);
            expect(got[2].topic == kTopic, "dispatch.popEntryStillRestoresByProperty", got[2].topic);
            expect(got[3].topic == popRetryTopic("OtherGrp", kTopic),
                   "dispatch.popEntryKeepsAnotherGroupsForm", got[3].topic);
        }
    }

    {
        // The pull path's concurrent batch entry takes the same step.
        auto listener = std::make_shared<RecordingConcurrentListener>();
        DefaultMQPushConsumer c(kGroup);
        c.setMessageListener(listener);
        const std::string key = DefaultMQPushConsumer::offsetKey(queue0());
        c.setPendingMessages(key, {});
        std::vector<MessageExt> batch;
        batch.push_back(poppedRow("e1", 1, 0, "TagA", popRetryTopic(kGroup, kTopic)));
        batch.push_back(poppedRow("e2", 2, 0, "TagA", plusRetryTopic(kGroup, kTopic)));
        c.consumeBatch(key, queue0(), batch);
        const std::vector<MessageExt> got = listener->delivered();
        expectInt(static_cast<long long>(got.size()), 2, "dispatch.pullEntryDeliversTheBatch",
                  topicsOf(got));
        if (got.size() == 2) {
            expect(got[0].topic == kTopic, "dispatch.pullEntryStripsTheUnderscoreForm", got[0].topic);
            expect(got[1].topic == kTopic, "dispatch.pullEntryStripsThePlusForm", got[1].topic);
        }
    }

    {
        // The same helper on the same entry, but with an orderly listener: the orderly dispatch path
        // keeps the topic the broker wrote it under.
        auto orderly = std::make_shared<RecordingOrderlyListener>();
        DefaultMQPushConsumer c(kGroup);
        c.setMessageListener(orderly);
        const std::string key = DefaultMQPushConsumer::offsetKey(queue0());
        c.setPendingMessages(key, {});
        const std::string prefixed = popRetryTopic(kGroup, kTopic);
        std::vector<MessageExt> batch;
        batch.push_back(poppedRow("o1", 1, 0, "TagA", prefixed));
        c.consumeBatch(key, queue0(), batch);
        expectInt(orderly->calls(), 1, "dispatch.orderlyEntryStillDispatches");
        const std::vector<MessageExt> got = orderly->delivered();
        expect(!got.empty() && got[0].topic == prefixed, "dispatch.orderlyEntryIsNotStripped",
               got.empty() ? std::string("<none>") : got[0].topic);
    }
}

}  // namespace

int main() {
    testBudgetDropIsStrictlyOverTheEffectiveBudget();
    testDefaultBudgetIsSixteenAndNotUnlimited();
    testBudgetDropLogsOneInfoLinePerRow();
    testPullPathStillDeliversTheOverBudgetRow();
    testPipelineAcksEveryDroppedRowOnTheTransport();
    testAnswerStatsKeyOnTheRowsThatSurvive();
    testRescheduleDecisionTable();
    testPrefixStripAdmitsBothWireSpellingsOfThisGroup();
    testConcurrentDispatchEntriesApplyTheStrip();
    std::printf("pop_predelivery: %d checks, %d failures\n", checks, fails);
    return fails == 0 ? 0 : 1;
}
