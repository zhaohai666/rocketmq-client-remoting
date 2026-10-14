// Unit test for the admin-initiated CONSUME_MESSAGE_DIRECTLY(309) -- no cluster required.
//
//
// Why it must be locked down offline: every failure on this path is **silent** -- a wrong msgId key name means the broker-side
// header cannot be parsed, the message pushed to the consumer matches nothing, and the remark/msgId in the admin result are all wrong;
// picking the wrong target broker (the first in the route instead of the storage host) is invisible in a single-broker cluster and,
// with several brokers, shows up as the broker answering NO_MESSAGE or the probe pushed to the wrong instance. The mock broker plays four request types:
// route(105) / VIEW_MESSAGE_BY_ID(33) / QUERY_MESSAGE(12) / CONSUME_MESSAGE_DIRECTLY(309),
// sharing the socket skeleton with the MockBroker of rmq_test_check_client_config.
#include <atomic>
#include <cstdint>
#include <cstdio>
#include <map>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "rocketmq/client/admin.h"
#include "rocketmq/client/exception.h"
#include "rocketmq/common/message.h"
#include "rocketmq/common/message_const.h"
#include "rocketmq/common/message_decoder.h"
#include "rocketmq/common/net_compat.h"
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

// select until readable (one 100ms round, so it exits promptly once alive turns false), then read one whole frame
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

// ---------------------------------------------------------------- mock broker

// The scripted reply for one request (the reply body of 309 is ConsumeMessageDirectlyResult JSON,
// the reply body of 33 / 12 is a 17-section storage-format message stream)
struct ScriptedReply {
    int32_t code = ResponseCode::SUCCESS;
    std::string remark;
    Bytes body;
};

// The evidence kept for one request
struct WireRecord {
    int32_t code = 0;
    PropertyMap ext;
    Bytes body;
};

// Plays both the name server (answers routes) and the broker (33 / 12 / 309 reply per script).
class MockBroker {
public:
    MockBroker() {
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

    ~MockBroker() {
        running_.store(false);
        if (acceptor_.joinable()) acceptor_.join();
        // Connection threads see running_ turn false inside the 100ms select round and stop;
        // destroying a joinable std::thread would terminate the process outright, so they must be reaped here.
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
    int port() const { return port_; }

    void addRoute(const std::string& topic, const TopicRouteData& route) {
        std::lock_guard<std::mutex> lk(state_);
        routes_[topic] = route.encode();
    }

    // Each request type has its own script (consumed in arrival order; beyond the script the default reply is used).
    // Default: VIEW_MESSAGE_BY_ID / QUERY_MESSAGE answer NO_MESSAGE (message absent),
    // 309 answers SUCCESS + empty body (triggering the admin "no consume result" branch).
    void scriptView(std::vector<ScriptedReply> replies) {
        std::lock_guard<std::mutex> lk(state_);
        viewReplies_ = std::move(replies);
        viewRecords_.clear();
        viewCount_.store(0);
    }

    void scriptQuery(std::vector<ScriptedReply> replies) {
        std::lock_guard<std::mutex> lk(state_);
        queryReplies_ = std::move(replies);
        queryRecords_.clear();
        queryCount_.store(0);
    }

    void scriptDirect(std::vector<ScriptedReply> replies) {
        std::lock_guard<std::mutex> lk(state_);
        directReplies_ = std::move(replies);
        directRecords_.clear();
        directCount_.store(0);
    }

    int viewCount() const { return viewCount_.load(); }
    int queryCount() const { return queryCount_.load(); }
    int directCount() const { return directCount_.load(); }

    WireRecord directRecord(size_t i) {
        std::lock_guard<std::mutex> lk(state_);
        if (i >= directRecords_.size()) return WireRecord{};
        return directRecords_[i];
    }

    WireRecord viewRecord(size_t i) {
        std::lock_guard<std::mutex> lk(state_);
        if (i >= viewRecords_.size()) return WireRecord{};
        return viewRecords_[i];
    }

    WireRecord queryRecord(size_t i) {
        std::lock_guard<std::mutex> lk(state_);
        if (i >= queryRecords_.size()) return WireRecord{};
        return queryRecords_[i];
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

    ScriptedReply takeReply(std::vector<ScriptedReply>& replies, int idx) {
        if (idx < static_cast<int>(replies.size())) return replies[idx];
        return ScriptedReply{};
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
            } else if (req.code == RequestCode::VIEW_MESSAGE_BY_ID ||
                       req.code == RequestCode::QUERY_MESSAGE ||
                       req.code == RequestCode::CONSUME_MESSAGE_DIRECTLY) {
                std::vector<ScriptedReply> replies;
                std::vector<WireRecord>* records = nullptr;
                std::atomic<int>* counter = nullptr;
                ScriptedReply fallback{ResponseCode::NO_MESSAGE, std::string(), Bytes()};
                if (req.code == RequestCode::VIEW_MESSAGE_BY_ID) {
                    counter = &viewCount_;
                    records = &viewRecords_;
                } else if (req.code == RequestCode::QUERY_MESSAGE) {
                    counter = &queryCount_;
                    records = &queryRecords_;
                } else {
                    counter = &directCount_;
                    records = &directRecords_;
                    fallback = ScriptedReply{ResponseCode::SUCCESS, std::string(), Bytes()};
                }
                const int idx = counter->fetch_add(1);
                ScriptedReply reply = fallback;
                {
                    std::lock_guard<std::mutex> lk(state_);
                    records->push_back({req.code, req.extFields, req.body});
                    reply = takeReply(repliesFor(req.code), idx);
                }
                resp.code = reply.code;
                resp.remark = reply.remark;
                resp.hasRemark = !reply.remark.empty();
                if (!reply.body.empty()) {
                    resp.body = reply.body;
                    resp.hasBody = true;
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

    // Picks the script matching the request code out of the three
    std::vector<ScriptedReply>& repliesFor(int32_t code) {
        if (code == RequestCode::VIEW_MESSAGE_BY_ID) return viewReplies_;
        if (code == RequestCode::QUERY_MESSAGE) return queryReplies_;
        return directReplies_;
    }

    socket_t listen_ = kInvalidSocket;
    uint16_t port_ = 0;
    std::atomic<bool> running_{false};
    std::atomic<int> viewCount_{0};
    std::atomic<int> queryCount_{0};
    std::atomic<int> directCount_{0};
    std::mutex state_;
    std::map<std::string, Bytes> routes_;
    std::vector<ScriptedReply> viewReplies_;
    std::vector<ScriptedReply> queryReplies_;
    std::vector<ScriptedReply> directReplies_;
    std::vector<WireRecord> viewRecords_;
    std::vector<WireRecord> queryRecords_;
    std::vector<WireRecord> directRecords_;
    std::mutex workersM_;
    std::vector<std::thread> workers_;
    std::thread acceptor_;
};

// ---------------------------------------------------------------- test scaffolding

const char* kGroup = "GID_DirectConsumeAdmin";
const char* kTopic = "DirectConsumeAdminTopic";
const char* kClientId = "127.0.0.1@1234#5678";

MessageExt storedMessage(MockBroker& mock, int64_t commitLogOffset, bool withUniqKey,
                         const std::string& uniqKey) {
    // One message in the broker store: storage host = the mock itself (the 309 target address comes from here),
    // physical offset given (on the decode side both msgId and offsetMsgId are derived from storeHost+offset).
    MessageExt m;
    m.topic = kTopic;
    m.body = "direct-consume-payload";
    m.storeHost = "127.0.0.1";
    m.storeHostPort = mock.port();
    m.commitLogOffset = commitLogOffset;
    m.queueId = 2;
    m.queueOffset = commitLogOffset;
    if (withUniqKey) {
        m.putProperty(MessageConst::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX, uniqKey);
        m.putProperty(MessageConst::PROPERTY_KEYS, "ORDER_2026");
    }
    return m;
}

// A message stored on this broker means offsetMsgId = createMessageId(storeAddr, offset).
// While decoding, this port derives both msgId and offsetMsgId from the storeHost + PHYSICALOFFSET of the 17-section payload.
std::string offsetMsgIdFor(MockBroker& mock, int64_t commitLogOffset) {
    return createMessageId(ipAndPortToBytes("127.0.0.1", mock.port()), commitLogOffset);
}

ConsumeMessageDirectlyResult directResultBody() {
    ConsumeMessageDirectlyResult r;
    r.consumeResult = "CR_SUCCESS";
    r.remark = "consumed by client";
    r.spentTimeMills = 12;
    r.order = false;
    r.autoCommit = true;
    return r;
}

void expectDirectExt(const WireRecord& rec, const std::string& wantMsgId, const char* name) {
    expectInt(rec.code, RequestCode::CONSUME_MESSAGE_DIRECTLY, std::string(name) + ".requestCode");
    auto it = rec.ext.find("topic");
    expect(it != rec.ext.end() && it->second == kTopic, std::string(name) + ".topic");
    it = rec.ext.find("consumerGroup");
    expect(it != rec.ext.end() && it->second == kGroup, std::string(name) + ".consumerGroup");
    it = rec.ext.find("clientId");
    expect(it != rec.ext.end() && it->second == kClientId, std::string(name) + ".clientId");
    it = rec.ext.find("msgId");
    expect(it != rec.ext.end() && it->second == wantMsgId, std::string(name) + ".msgId",
           it == rec.ext.end() ? "(absent)" : it->second);
}

// ------------------------------------------------ 309 header key names (a wrong one is silent)

void testExtFieldKeyNames() {
    PropertyMap ext = DefaultMQAdminExt::buildConsumeMessageDirectlyExtFields(
        "TopicA", "GID_A", "cid-1", "msg-1");
    expectInt(static_cast<long long>(ext.size()), 4, "the 309 header has exactly four fields");
    auto it = ext.find("topic");
    expect(it != ext.end() && it->second == "TopicA", "309 header topic");
    it = ext.find("consumerGroup");
    expect(it != ext.end() && it->second == "GID_A", "309 header consumerGroup");
    it = ext.find("clientId");
    expect(it != ext.end() && it->second == "cid-1", "309 header clientId");
    it = ext.find("msgId");
    expect(it != ext.end() && it->second == "msg-1", "309 header msgId");
    expect(ext.find("brokerName") == ext.end(), "the 309 header omits brokerName");
    expect(ext.find("topicSysFlag") == ext.end(), "the 309 header omits topicSysFlag");
    expect(ext.find("groupSysFlag") == ext.end(), "the 309 header omits groupSysFlag");
}

// ------------------------------------------------ direct offsetMsgId lookup path

void testOffsetMsgIdPath(DefaultMQAdminExt& admin, MockBroker& mock) {
    const std::string msgId = offsetMsgIdFor(mock, 100);
    const MessageExt stored = storedMessage(mock, 100, /*withUniqKey=*/false, "");
    mock.scriptView({{ResponseCode::SUCCESS, "", encodeMessageExt(stored)}});
    mock.scriptQuery({});
    mock.scriptDirect({{ResponseCode::SUCCESS, "", directResultBody().encode()}});

    ConsumeMessageDirectlyResult r =
        admin.consumeMessageDirectly(kGroup, kClientId, kTopic, msgId);

    expect(r.consumeResult == "CR_SUCCESS", "offsetPath.result.consumeResult", r.consumeResult);
    expect(r.remark == "consumed by client", "offsetPath.result.remark", r.remark);
    expectInt(r.spentTimeMills, 12, "offsetPath.result.spentTimeMills");
    expect(!r.order, "offsetPath.result.orderFalse");
    expect(r.autoCommit, "offsetPath.result.autoCommitTrue");

    // The offset path of viewMessage: one VIEW_MESSAGE_BY_ID, sent straight to the address encoded in the msgId
    expectInt(mock.viewCount(), 1, "offsetPath.viewCount");
    const WireRecord view = mock.viewRecord(0);
    expectInt(view.code, RequestCode::VIEW_MESSAGE_BY_ID, "offsetPath.viewCode");
    expect(view.ext.count("offset") == 1 && view.ext.at("offset") == "100", "offsetPath.viewOffset",
           view.ext.count("offset") ? view.ext.at("offset") : "(absent)");
    expectInt(mock.queryCount(), 0, "offsetPath.noUniqKeyFallback");

    // Exactly one 309, targeted at the storage host of the message (= the mock itself), msgId passed through verbatim
    expectInt(mock.directCount(), 1, "offsetPath.directCount");
    expectDirectExt(mock.directRecord(0), msgId, "offsetPath.direct");
}

// ------------------------------------------------ uniqKey path + offsetMsgId substitution

void testUniqKeyPath(DefaultMQAdminExt& admin, MockBroker& mock) {
    // The uniq key itself is not a valid offset msgId: viewMessage first tries the direct offsetMsgId lookup
    // (the script answers NO_MESSAGE here), then falls back on failure to the uniqKey search (QUERY_MESSAGE, whose
    // body is a 17-section message stream; the derived msgId must equal the key to count as a hit).
    const std::string uniqKey = offsetMsgIdFor(mock, 200);
    const MessageExt stored = storedMessage(mock, 200, /*withUniqKey=*/true, uniqKey);
    mock.scriptView({{ResponseCode::NO_MESSAGE, "message not found", Bytes()}});
    mock.scriptQuery({{ResponseCode::SUCCESS, "", encodeMessageExt(stored)}});
    mock.scriptDirect({{ResponseCode::SUCCESS, "", directResultBody().encode()}});

    ConsumeMessageDirectlyResult r =
        admin.consumeMessageDirectly(kGroup, kClientId, kTopic, uniqKey);

    expect(r.consumeResult == "CR_SUCCESS", "uniqKeyPath.result.consumeResult", r.consumeResult);
    expectInt(mock.viewCount(), 1, "uniqKeyPath.viewAttempted");
    expectInt(mock.queryCount(), 1, "uniqKeyPath.queryCount");

    // Request shape of the uniqKey search: indexType=U + MixAll.UNIQUE_MSG_QUERY_FLAG (a key name, not a flag bit)
    const WireRecord query = mock.queryRecord(0);
    expectInt(query.code, RequestCode::QUERY_MESSAGE, "uniqKeyPath.queryCode");
    expect(query.ext.count("_UNIQUE_KEY_QUERY") == 1 && query.ext.at("_UNIQUE_KEY_QUERY") == "true",
           "uniqKeyPath.uniqueKeyQueryFlag",
           query.ext.count("_UNIQUE_KEY_QUERY") ? query.ext.at("_UNIQUE_KEY_QUERY") : "(absent)");
    expect(query.ext.count("indexType") == 1 && query.ext.at("indexType") == "U",
           "uniqKeyPath.indexType");
    expect(query.ext.count("key") == 1 && query.ext.at("key") == uniqKey, "uniqKeyPath.queryKey");

    expectInt(mock.directCount(), 1, "uniqKeyPath.directCount");
    expectDirectExt(mock.directRecord(0), uniqKey, "uniqKeyPath.direct");
}

// ------------------------------------------------ broker rejection / empty result body

void testBrokerRejectPropagates(DefaultMQAdminExt& admin, MockBroker& mock) {
    // With the consumer offline the broker has nowhere to forward: a non-SUCCESS response code is thrown with it
    const std::string msgId = offsetMsgIdFor(mock, 300);
    const MessageExt stored = storedMessage(mock, 300, false, "");
    mock.scriptView({{ResponseCode::SUCCESS, "", encodeMessageExt(stored)}});
    mock.scriptQuery({});
    mock.scriptDirect(
        {{ResponseCode::CONSUMER_NOT_ONLINE, "consumer not online [clientId=" + std::string(kClientId) + "]",
          Bytes()}});

    int threw = 0;
    int32_t code = 0;
    std::string msg;
    try {
        admin.consumeMessageDirectly(kGroup, kClientId, kTopic, msgId);
    } catch (const MQClientException& e) {
        threw = 1;
        code = e.getResponseCode();
        msg = e.what();
    } catch (const std::exception& e) {
        std::printf("  wrong exception type: %s\n", e.what());
    }
    expect(threw == 1, "reject.throws");
    expectInt(code, ResponseCode::CONSUMER_NOT_ONLINE, "reject.responseCode");
    expect(msg.find("consumer not online") != std::string::npos, "reject.keepsRemark", msg);
    expectInt(mock.directCount(), 1, "reject.requestSent");
}

void testEmptyResultBodyThrows(DefaultMQAdminExt& admin, MockBroker& mock) {
    const std::string msgId = offsetMsgIdFor(mock, 400);
    const MessageExt stored = storedMessage(mock, 400, false, "");
    mock.scriptView({{ResponseCode::SUCCESS, "", encodeMessageExt(stored)}});
    mock.scriptQuery({});
    mock.scriptDirect({{ResponseCode::SUCCESS, "", Bytes()}});

    int threw = 0;
    std::string msg;
    try {
        admin.consumeMessageDirectly(kGroup, kClientId, kTopic, msgId);
    } catch (const MQClientException& e) {
        threw = 1;
        msg = e.what();
    } catch (const std::exception& e) {
        std::printf("  wrong exception type: %s\n", e.what());
    }
    expect(threw == 1, "emptyBody.throws");
    expect(msg.find("no consume result") != std::string::npos, "emptyBody.message", msg);
    expect(msg.find(kClientId) != std::string::npos, "emptyBody.namesClient", msg);
}

void testMessageNotFoundSendsNothing(DefaultMQAdminExt& admin, MockBroker& mock) {
    // When the msgId cannot be decoded (invalid hex and the uniqKey search finds nothing) viewMessage throws,
    // and not even one 309 should be sent -- whichever broker it reached, there would be nowhere to forward.
    mock.scriptView({});
    mock.scriptQuery({{ResponseCode::QUERY_NOT_FOUND, "", Bytes()}});
    mock.scriptDirect({});

    int threw = 0;
    std::string msg;
    try {
        admin.consumeMessageDirectly(kGroup, kClientId, kTopic, "not-a-valid-msg-id");
    } catch (const MQClientException& e) {
        threw = 1;
        msg = e.what();
    } catch (const std::exception& e) {
        std::printf("  wrong exception type: %s\n", e.what());
    }
    expect(threw == 1, "notFound.throws");
    expect(msg.find("not-a-valid-msg-id") != std::string::npos, "notFound.namesMsgId", msg);
    expectInt(mock.directCount(), 0, "notFound.noDirectRequest");
    expectInt(mock.queryCount(), 1, "notFound.uniqKeyFallbackTried");
}

}  // namespace

int main() {
    testExtFieldKeyNames();

    MockBroker mock;
    DefaultMQAdminExt admin("DirectConsumeAdminTest");
    admin.setNamesrvAddr(mock.address());
    admin.setTimeoutMillis(5000);
    try {
        admin.start();
    } catch (const std::exception& e) {
        std::printf("FAIL admin.start: %s\n", e.what());
        return 1;
    }
    // Puts the route of kTopic into the cache (the uniqKey search locates brokers through the route)
    TopicRouteData route;
    std::map<int64_t, std::string> addrs;
    addrs[0] = mock.address();
    route.brokerDatas.emplace_back("DefaultCluster", "broker-a", addrs);
    mock.addRoute(kTopic, route);
    admin.getMQClientInstance().updateTopicRouteInfoFromNameServer(kTopic);

    testOffsetMsgIdPath(admin, mock);
    testUniqKeyPath(admin, mock);
    testBrokerRejectPropagates(admin, mock);
    testEmptyResultBodyThrows(admin, mock);
    testMessageNotFoundSendsNothing(admin, mock);

    admin.shutdown();
    std::printf("admin consume directly: %d checks, %d failures\n", checks, fails);
    return fails == 0 ? 0 : 1;
}
