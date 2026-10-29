// A real broker cannot be scripted into refusing CONSUMER_SEND_MSG_BACK on demand, so both legs of
// the send-back path (the bounce request and the ordinary send it degrades to) are driven by a mock
// name server + broker over a real socket. Everything asserted here is read off the wire.
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "rocketmq/client/exception.h"
#include "rocketmq/client/pull_consumer.h"
#include "rocketmq/common/byte_buffer.h"
#include "rocketmq/common/message_const.h"
#include "rocketmq/common/mix_all.h"
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

void expectStr(const std::string& actual, const std::string& expected, const std::string& name) {
    ++checks;
    if (actual != expected) {
        ++fails;
        std::printf("FAIL %s (actual=%s expected=%s)\n", name.c_str(), actual.c_str(),
                    expected.c_str());
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

struct Conn {
    explicit Conn(socket_t s) : sock(s) {}
    socket_t sock;
    std::mutex writeM;
    std::atomic<int> refs{1};
    std::atomic<bool> closing{false};
};

using ConnRef = std::shared_ptr<Conn>;

void releaseConn(const ConnRef& c) {
    if (c->refs.fetch_sub(1) == 1 && c->closing.load()) {
        ::shutdown(c->sock, kShutdownHow);
        netcompat::closeSocket(c->sock);
    }
}

// ---------------------------------------------------------------- mock endpoint

struct WireRecord {
    int32_t code = 0;
    int64_t seq = 0;
    PropertyMap ext;
    Bytes body;
    bool hasBody = false;
};

// Scripted answer for one request kind; the nth request of that kind takes the nth reply.
struct Reply {
    int32_t code = ResponseCode::SUCCESS;
    std::string remark;
};

constexpr size_t kRequestLogCap = 500;

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

    std::string address() const { return "127.0.0.1:" + std::to_string(port_); }

    void addRoute(const std::string& topic) {
        TopicRouteData route;
        route.queueDatas.emplace_back("broker-a", 1, 1, 6, 0);
        std::map<int64_t, std::string> addrs;
        addrs[0] = address();
        route.brokerDatas.emplace_back("DefaultCluster", "broker-a", addrs);
        std::lock_guard<std::mutex> lk(state_);
        routes_[topic] = route.encode();
    }

    void scriptBounce(std::vector<Reply> replies) {
        std::lock_guard<std::mutex> lk(state_);
        bounceReplies_ = std::move(replies);
        bounceCount_.store(0);
    }

    void scriptSend(std::vector<Reply> replies) {
        std::lock_guard<std::mutex> lk(state_);
        sendReplies_ = std::move(replies);
        sendCount_.store(0);
    }

    void clearRequests() {
        std::lock_guard<std::mutex> lk(state_);
        requestLog_.clear();
    }

    std::vector<WireRecord> requests() {
        std::lock_guard<std::mutex> lk(state_);
        return requestLog_;
    }

    std::vector<WireRecord> requestsWithCode(int32_t code) {
        std::lock_guard<std::mutex> lk(state_);
        std::vector<WireRecord> out;
        for (const WireRecord& r : requestLog_) {
            if (r.code == code) out.push_back(r);
        }
        return out;
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
            ConnRef conn = std::make_shared<Conn>(sock);
            std::lock_guard<std::mutex> lk(workersM_);
            workers_.emplace_back([this, conn]() { serveConn(conn); });
        }
    }

    void serveConn(const ConnRef& conn) {
        while (running_.load()) {
            Bytes frame;
            if (!readFrame(conn->sock, running_, frame)) break;
            RemotingCommand req;
            if (!RemotingCommand::tryDecode(frame, req, nullptr)) break;
            conn->refs.fetch_add(1);
            {
                std::lock_guard<std::mutex> lk(workersM_);
                workers_.emplace_back([this, conn, req]() {
                    respond(conn, req);
                    releaseConn(conn);
                });
            }
        }
        conn->closing.store(true);
        releaseConn(conn);
    }

    Reply replyFor(std::vector<Reply>& replies, std::atomic<int>& counter) {
        const int idx = counter.fetch_add(1);
        std::lock_guard<std::mutex> lk(state_);
        if (idx < static_cast<int>(replies.size())) return replies[static_cast<size_t>(idx)];
        return Reply{};
    }

    static bool isSendCode(int32_t code) {
        return code == RequestCode::SEND_MESSAGE_V2 || code == RequestCode::SEND_MESSAGE ||
               code == RequestCode::SEND_BATCH_MESSAGE ||
               code == RequestCode::SEND_REPLY_MESSAGE_V2;
    }

    void respond(const ConnRef& conn, RemotingCommand req) {
        const int64_t seq = seq_.fetch_add(1);
        int32_t code = ResponseCode::SUCCESS;
        std::string remark;
        Bytes routeBody;
        bool sendOk = false;
        {
            std::lock_guard<std::mutex> lk(state_);
            if (requestLog_.size() < kRequestLogCap) {
                requestLog_.push_back({req.code, seq, req.extFields, req.body, req.hasBody});
            }
        }

        if (req.code == RequestCode::GET_ROUTEINFO_BY_TOPIC) {
            const std::string topic = req.extFields.count("topic") ? req.extFields["topic"]
                                                                   : std::string();
            bool found = false;
            {
                std::lock_guard<std::mutex> lk(state_);
                auto it = routes_.find(topic);
                if (it != routes_.end()) {
                    routeBody = it->second;
                    found = true;
                }
            }
            code = found ? ResponseCode::SUCCESS : ResponseCode::TOPIC_NOT_EXIST;
            remark = found ? "route ok" : "topic not exist";
        } else if (req.code == RequestCode::CONSUMER_SEND_MSG_BACK) {
            const Reply r = replyFor(bounceReplies_, bounceCount_);
            code = r.code;
            remark = r.remark;
        } else if (isSendCode(req.code)) {
            const Reply r = replyFor(sendReplies_, sendCount_);
            code = r.code;
            remark = r.remark.empty() ? "mock send" : r.remark;
            sendOk = code == ResponseCode::SUCCESS;
        }

        // Oneway requests get no response (a broker does the same).
        if (req.isOnewayRpc()) return;
        RemotingCommand resp;
        resp.opaque = req.opaque;
        resp.markResponseType();
        resp.code = code;
        resp.remark = remark;
        resp.hasRemark = !remark.empty();
        if (!routeBody.empty()) {
            resp.body = routeBody;
            resp.hasBody = true;
        }
        if (sendOk) {
            resp.extFields["msgId"] = "AC10000100001234567890ABCDEF0001";
            resp.extFields["queueId"] = "0";
            resp.extFields["queueOffset"] = "0";
        }
        const Bytes out = resp.encode();
        std::lock_guard<std::mutex> lk(conn->writeM);
        writeAll(conn->sock, out);
    }

    socket_t listen_ = kInvalidSocket;
    uint16_t port_ = 0;
    std::atomic<bool> running_{false};
    std::atomic<int> bounceCount_{0};
    std::atomic<int> sendCount_{0};
    std::atomic<int64_t> seq_{0};
    std::mutex state_;
    std::map<std::string, Bytes> routes_;
    std::vector<Reply> bounceReplies_;
    std::vector<Reply> sendReplies_;
    std::vector<WireRecord> requestLog_;
    std::mutex workersM_;
    std::vector<std::thread> workers_;
    std::thread acceptor_;
};

// ---------------------------------------------------------------- helpers

const char* kGroup = "PG_send_back";
// Message properties travel as key<0x01>value<0x02>key<0x01>value.
const char kKvSep = '\x01';

std::string retryTopic() { return MixAll::getRetryTopic(kGroup); }

MessageExt sourceMessage(const std::string& topic, int32_t reconsumeTimes) {
    MessageExt msg(Message(topic, Bytes{'h', 'i'}));
    msg.msgId = "MSG-ORIGIN-ID";
    msg.commitLogOffset = 4242;
    msg.brokerName = "broker-a";
    msg.reconsumeTimes = reconsumeTimes;
    msg.putProperty(MessageConst::PROPERTY_TRANSACTION_PREPARED, "true");
    return msg;
}

std::string prop(const char* name, const std::string& value) {
    return std::string(name) + kKvSep + value;
}

// The V2 send header keeps the properties in the compact key `i`.
bool propertiesContain(const WireRecord& rec, const std::string& entry) {
    auto it = rec.ext.find("i");
    return it != rec.ext.end() && it->second.find(entry) != std::string::npos;
}

std::string extOr(const WireRecord& rec, const char* key) {
    auto it = rec.ext.find(key);
    return it == rec.ext.end() ? std::string() : it->second;
}

// ---------------------------------------------------------------- cases

void testAcceptedBounceSendsNothingElse(MockEndpoint& mock) {
    const std::string topic = "SendBackAccepted";
    mock.addRoute(topic);
    mock.addRoute(retryTopic());
    mock.scriptBounce({{ResponseCode::SUCCESS, "ok"}});
    mock.scriptSend({{ResponseCode::SUCCESS, ""}});
    mock.clearRequests();

    DefaultMQPullConsumer c(kGroup);
    c.setNamesrvAddr(mock.address());
    c.setInstanceName("send-back-accepted");
    c.start();
    c.fetchSubscribeMessageQueues(topic);

    expect(c.maxReconsumeTimes() == 16, "retry ceiling defaults to 16");

    c.sendMessageBack(sourceMessage(topic, 0), 2);

    const auto bounces = mock.requestsWithCode(RequestCode::CONSUMER_SEND_MSG_BACK);
    const auto sends = mock.requestsWithCode(RequestCode::SEND_MESSAGE_V2);
    expect(bounces.size() == 1, "an accepted bounce goes out exactly once",
           "bounces=" + std::to_string(bounces.size()));
    expect(sends.empty(), "an accepted bounce must not send a retry copy",
           "sends=" + std::to_string(sends.size()));
    if (!bounces.empty()) {
        const WireRecord& b = bounces[0];
        expectStr(extOr(b, "group"), kGroup, "bounce carries the group");
        expectStr(extOr(b, "offset"), "4242", "bounce carries the commit log offset");
        expectStr(extOr(b, "delayLevel"), "2", "bounce carries the caller's delay level");
        expectStr(extOr(b, "originMsgId"), "MSG-ORIGIN-ID", "bounce carries the original msgId");
        expectStr(extOr(b, "originTopic"), topic, "bounce carries the original topic");
        expectStr(extOr(b, "maxReconsumeTimes"), "16",
                  "bounce carries the default retry ceiling");
    }
    c.shutdown();
}

void testRefusedBounceFallsBackToRetrySend(MockEndpoint& mock) {
    const std::string topic = "SendBackRefused";
    mock.addRoute(topic);
    mock.addRoute(retryTopic());
    mock.scriptBounce({{ResponseCode::SYSTEM_ERROR, "bounce rejected"}});
    mock.scriptSend({{ResponseCode::SUCCESS, ""}});
    mock.clearRequests();

    DefaultMQPullConsumer c(kGroup);
    c.setNamesrvAddr(mock.address());
    c.setInstanceName("send-back-refused");
    c.start();
    c.fetchSubscribeMessageQueues(topic);

    c.sendMessageBack(sourceMessage(topic, 1), 3);

    std::vector<WireRecord> bounces;
    std::vector<WireRecord> sends;
    for (const WireRecord& r : mock.requests()) {
        if (r.code == RequestCode::CONSUMER_SEND_MSG_BACK) bounces.push_back(r);
        if (r.code == RequestCode::SEND_MESSAGE_V2) sends.push_back(r);
    }
    expect(bounces.size() == 1, "the refused bounce still reached the broker once");
    expect(sends.size() == 1, "a refused bounce degrades to exactly one ordinary send",
           "sends=" + std::to_string(sends.size()));
    if (bounces.empty() || sends.empty()) {
        c.shutdown();
        return;
    }
    expect(bounces[0].seq < sends[0].seq, "the bounce is tried before the retry copy");

    const WireRecord& s = sends[0];
    expectStr(extOr(s, "a"), MixAll::CLIENT_INNER_PRODUCER_GROUP,
              "the retry copy leaves through the internal producer");
    expectStr(extOr(s, "b"), retryTopic(), "the retry copy is addressed to %RETRY%group");
    expect(s.body == (Bytes{'h', 'i'}), "the retry copy keeps the original body");
    expectStr(extOr(s, "k"), "false", "the retry copy carries this consumer's unitMode");
    expect(propertiesContain(s, prop(MessageConst::PROPERTY_RETRY_TOPIC, topic)),
           "the retry copy records its original topic");
    expect(propertiesContain(s, prop(MessageConst::PROPERTY_RECONSUME_TIME, "2")),
           "the retry copy bumps the reconsume counter");
    expect(propertiesContain(s, prop(MessageConst::PROPERTY_MAX_RECONSUME_TIMES, "16")),
           "the retry copy carries the retry ceiling");
    expect(propertiesContain(s, prop(MessageConst::PROPERTY_ORIGIN_MESSAGE_ID, "MSG-ORIGIN-ID")),
           "the retry copy keeps the original msgId");
    expect(propertiesContain(s, prop(MessageConst::PROPERTY_DELAY_TIME_LEVEL, "4")),
           "the retry copy delays by 3 + reconsumeTimes");
    expect(!propertiesContain(s, MessageConst::PROPERTY_TRANSACTION_PREPARED),
           "the retry copy drops the half-message marker");
    c.shutdown();
}

void testBothLegsFailingRethrowsTheOriginalError(MockEndpoint& mock) {
    const std::string topic = "SendBackBothFail";
    mock.addRoute(topic);
    mock.addRoute(retryTopic());
    mock.scriptBounce({{ResponseCode::SYSTEM_ERROR, "bounce rejected"}});
    mock.scriptSend({{ResponseCode::SYSTEM_ERROR, "send rejected"}});
    mock.clearRequests();

    DefaultMQPullConsumer c(kGroup);
    c.setNamesrvAddr(mock.address());
    c.setInstanceName("send-back-both-fail");
    c.start();
    c.fetchSubscribeMessageQueues(topic);

    bool threw = false;
    std::string what;
    int32_t code = 0;
    try {
        c.sendMessageBack(sourceMessage(topic, 0), 1);
    } catch (const MQBrokerException& e) {
        threw = true;
        what = e.what();
        code = e.getResponseCode();
    } catch (const std::exception& e) {
        threw = true;
        what = e.what();
    }
    expect(threw, "a doubly-failed send-back reports the failure to the caller");
    expect(code == ResponseCode::SYSTEM_ERROR, "the caller sees the bounce's broker code",
           "code=" + std::to_string(code));
    expect(what.find("bounce rejected") != std::string::npos,
           "the caller sees the original error, not the fallback's", "what=" + what);
    expect(mock.requestsWithCode(RequestCode::CONSUMER_SEND_MSG_BACK).size() == 1,
           "the bounce was attempted once");
    expect(mock.requestsWithCode(RequestCode::SEND_MESSAGE_V2).size() == 1,
           "the retry copy was attempted once");
    c.shutdown();
}

void testRetryCeilingKnobRidesOnBothLegs(MockEndpoint& mock) {
    const std::string topic = "SendBackCeilingKnob";
    mock.addRoute(topic);
    mock.addRoute(retryTopic());
    mock.scriptBounce({{ResponseCode::SYSTEM_ERROR, "bounce rejected"}});
    mock.scriptSend({{ResponseCode::SUCCESS, ""}});
    mock.clearRequests();

    DefaultMQPullConsumer c(kGroup);
    c.setNamesrvAddr(mock.address());
    c.setInstanceName("send-back-ceiling");
    c.setMaxReconsumeTimes(5);
    c.start();
    c.fetchSubscribeMessageQueues(topic);

    c.sendMessageBack(sourceMessage(topic, 2), 1);

    const auto bounces = mock.requestsWithCode(RequestCode::CONSUMER_SEND_MSG_BACK);
    const auto sends = mock.requestsWithCode(RequestCode::SEND_MESSAGE_V2);
    expect(bounces.size() == 1 && sends.size() == 1, "one bounce then one retry copy");
    if (!bounces.empty()) {
        expectStr(extOr(bounces[0], "maxReconsumeTimes"), "5",
                  "the configured ceiling rides on the bounce");
    }
    if (!sends.empty()) {
        expect(propertiesContain(sends[0],
                                 prop(MessageConst::PROPERTY_MAX_RECONSUME_TIMES, "5")),
               "the configured ceiling is written onto the retry copy");
        expect(propertiesContain(sends[0], prop(MessageConst::PROPERTY_RECONSUME_TIME, "3")),
               "the retry copy still bumps the counter");
        expect(propertiesContain(sends[0], prop(MessageConst::PROPERTY_DELAY_TIME_LEVEL, "5")),
               "the delay follows the reconsume count, not the ceiling");
    }
    c.shutdown();
}

void testUnlimitedSentinelReadsAsSixteen(MockEndpoint& mock) {
    const std::string topic = "SendBackSentinel";
    mock.addRoute(topic);
    mock.addRoute(retryTopic());
    mock.scriptBounce({{ResponseCode::SUCCESS, "ok"}});
    mock.scriptSend({{ResponseCode::SUCCESS, ""}});
    mock.clearRequests();

    DefaultMQPullConsumer c(kGroup);
    c.setNamesrvAddr(mock.address());
    c.setInstanceName("send-back-sentinel");
    c.setMaxReconsumeTimes(-1);
    c.start();
    c.fetchSubscribeMessageQueues(topic);

    c.sendMessageBack(sourceMessage(topic, 0), 1);

    const auto bounces = mock.requestsWithCode(RequestCode::CONSUMER_SEND_MSG_BACK);
    expect(bounces.size() == 1, "the bounce with the sentinel ceiling reached the broker");
    if (!bounces.empty()) {
        expectStr(extOr(bounces[0], "maxReconsumeTimes"), "16",
                  "-1 reads as the broker default 16");
    }
    c.shutdown();
}

void testMissingMasterStillFallsBack(MockEndpoint& mock) {
    const std::string topic = "SendBackMissingMaster";
    mock.addRoute(topic);
    mock.addRoute(retryTopic());
    mock.scriptBounce({{ResponseCode::SUCCESS, "ok"}});
    mock.scriptSend({{ResponseCode::SUCCESS, ""}});
    mock.clearRequests();

    DefaultMQPullConsumer c(kGroup);
    c.setNamesrvAddr(mock.address());
    c.setInstanceName("send-back-missing-master");
    c.start();
    c.fetchSubscribeMessageQueues(topic);

    MessageExt msg = sourceMessage(topic, 0);
    msg.brokerName = "broker-absent";
    c.sendMessageBack(msg, 1);

    expect(mock.requestsWithCode(RequestCode::CONSUMER_SEND_MSG_BACK).empty(),
           "no bounce goes out when the master address is unknown");
    expect(mock.requestsWithCode(RequestCode::SEND_MESSAGE_V2).size() == 1,
           "an unknown master degrades to the retry copy");
    c.shutdown();
}

void testUnstartedConsumerRefuses() {
    DefaultMQPullConsumer c("PG_send_back_unstarted");
    bool threw = false;
    std::string what;
    try {
        c.sendMessageBack(sourceMessage("Any", 0), 1);
    } catch (const MQClientException& e) {
        threw = true;
        what = e.what();
    }
    expect(threw, "sendMessageBack before start() throws");
    expectStr(what, "consumer not started, call start() first",
              "the unstarted wording stays stable");
}

}  // namespace

namespace {

void runCase(const char* name, MockEndpoint& mock, void (*fn)(MockEndpoint&)) {
    const auto begin = std::chrono::steady_clock::now();
    try {
        fn(mock);
    } catch (const std::exception& e) {
        ++checks;
        ++fails;
        std::printf("FAIL %s threw: %s\n", name, e.what());
    } catch (...) {
        ++checks;
        ++fails;
        std::printf("FAIL %s threw: unknown error\n", name);
    }
    const long long ms = static_cast<long long>(
        std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now()
                                                              - begin)
            .count());
    if (ms > 2000) std::printf("SLOW %s: %lldms\n", name, ms);
}

}  // namespace

int main() {
    MockEndpoint mock;

    runCase("acceptedBounceSendsNothingElse", mock, testAcceptedBounceSendsNothingElse);
    runCase("refusedBounceFallsBackToRetrySend", mock, testRefusedBounceFallsBackToRetrySend);
    runCase("bothLegsFailingRethrowsTheOriginalError", mock,
            testBothLegsFailingRethrowsTheOriginalError);
    runCase("retryCeilingKnobRidesOnBothLegs", mock, testRetryCeilingKnobRidesOnBothLegs);
    runCase("unlimitedSentinelReadsAsSixteen", mock, testUnlimitedSentinelReadsAsSixteen);
    runCase("missingMasterStillFallsBack", mock, testMissingMasterStillFallsBack);
    testUnstartedConsumerRefuses();

    std::printf("%s: %d checks, %d failures\n", fails == 0 ? "PASS" : "FAIL", checks, fails);
    return fails == 0 ? 0 : 1;
}
