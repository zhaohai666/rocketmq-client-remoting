// Offline unit test for CHECK_CLIENT_CONFIG(46) -- no cluster required.
#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "rocketmq/client/exception.h"
#include "rocketmq/client/mq_client.h"
#include "rocketmq/common/net_compat.h"
#include "rocketmq/remoting/exception.h"
#include "rocketmq/remoting/protocol/body.h"
#include "rocketmq/remoting/protocol/codes.h"
#include "rocketmq/remoting/protocol/json.h"
#include "rocketmq/remoting/protocol/remoting_command.h"
#include "rocketmq/remoting/protocol/route.h"
#include "rocketmq/common/subscription_data.h"

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

// A loopback port that was occupied and then released immediately: connecting to it is always refused (more reliable than a fixed port)
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

// ---------------------------------------------------------------- mock broker

// The scripted reply for one 46
struct CheckReply {
    int32_t code = ResponseCode::SUCCESS;
    std::string remark;
};

// The evidence kept for one 46 request
struct CheckRecord {
    int32_t code = 0;
    size_t extCount = 0;
    Bytes body;
};

// Plays both the name server (answers routes) and the broker (answers 46 with the scripted response code).
// It shares the socket skeleton with the MockEndpoint of rmq_test_send_retry; only two kinds of requests are needed here.
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

    void addRoute(const std::string& topic, const TopicRouteData& route) {
        std::lock_guard<std::mutex> lk(state_);
        routes_[topic] = route.encode();
    }

    // The `i`-th 46 answers `codes[i]`; beyond the script answers SUCCESS
    void scriptChecks(std::vector<CheckReply> replies) {
        std::lock_guard<std::mutex> lk(state_);
        replies_ = std::move(replies);
        records_.clear();
        count_.store(0);
    }

    int checkCount() const { return count_.load(); }

    CheckRecord checkRecord(size_t i) {
        std::lock_guard<std::mutex> lk(state_);
        if (i >= records_.size()) return CheckRecord{};
        return records_[i];
    }

    // The JSON parsed from the body of the `i`-th 46 (Null when it does not parse)
    JsonValue bodyJson(size_t i) {
        CheckRecord r = checkRecord(i);
        JsonValue v;
        if (!jsonParse(std::string(r.body.begin(), r.body.end()), v)) return JsonValue();
        return v;
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
                auto it = routes_.find(topic);
                if (it != routes_.end()) {
                    resp.code = ResponseCode::SUCCESS;
                    resp.body = it->second;
                    resp.hasBody = true;
                } else {
                    resp.code = ResponseCode::TOPIC_NOT_EXIST;
                }
            } else if (req.code == RequestCode::CHECK_CLIENT_CONFIG) {
                const int idx = count_.fetch_add(1);
                {
                    std::lock_guard<std::mutex> lk(state_);
                    records_.push_back({req.code, req.extFields.size(), req.body});
                }
                CheckReply reply;
                {
                    std::lock_guard<std::mutex> lk(state_);
                    if (idx < static_cast<int>(replies_.size())) reply = replies_[idx];
                }
                resp.code = reply.code;
                resp.remark = reply.remark;
                resp.hasRemark = !reply.remark.empty();
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
    std::atomic<int> count_{0};
    std::mutex state_;
    std::map<std::string, Bytes> routes_;
    std::vector<CheckReply> replies_;
    std::vector<CheckRecord> records_;
    std::mutex workersM_;
    std::vector<std::thread> workers_;
    std::thread acceptor_;
};

// ---------------------------------------------------------------- test scaffolding

const char* kGroup = "GID_CheckCfg";
const char* kTopic = "Sql92Topic";
const char* kClientId = "127.0.0.1@1234#5678";

SubscriptionData sql92Sub(const std::string& expression = "a > 10") {
    SubscriptionData s(kTopic, expression);
    s.expressionType = ExpressionType::SQL92;
    return s;
}

SubscriptionData tagSub(const std::string& type = ExpressionType::TAG) {
    SubscriptionData s(kTopic, "tagA || tagB");
    s.expressionType = type;
    return s;
}

// Starts an instance and puts the route of kTopic into the cache (pointing at the mock itself)
std::unique_ptr<MQClientInstance> seeded(MockBroker& mock) {
    auto instance = std::make_unique<MQClientInstance>(kClientId,
                                                       std::vector<std::string>{mock.address()});
    TopicRouteData route;
    std::map<int64_t, std::string> addrs;
    addrs[0] = mock.address();
    route.brokerDatas.emplace_back("DefaultCluster", "broker-a", addrs);
    mock.addRoute(kTopic, route);
    instance->updateTopicRouteInfoFromNameServer(kTopic);
    return instance;
}

// ---------------------------------------------------------------- test cases

void testTagOnlySendsNothing(MockBroker& mock) {
    auto instance = seeded(mock);
    mock.scriptChecks({});
    std::vector<SubscriptionData> subs;
    subs.push_back(tagSub(ExpressionType::TAG));
    subs.push_back(tagSub(""));
    instance->checkSubscriptionsInBroker(kGroup, subs);
    expectInt(mock.checkCount(), 0, "neither TAG nor an empty expressionType sends 46");
}

void testSql92RequestShape(MockBroker& mock) {
    auto instance = seeded(mock);
    mock.scriptChecks({});
    std::vector<SubscriptionData> subs;
    subs.push_back(sql92Sub());
    instance->checkSubscriptionsInBroker(kGroup, subs);
    expectInt(mock.checkCount(), 1, "an SQL92 subscription sends exactly one 46");
    CheckRecord r = mock.checkRecord(0);
    expectInt(r.code, RequestCode::CHECK_CLIENT_CONFIG, "request code is 46");
    // A null request header means no extFields on the wire
    expectInt(static_cast<long long>(r.extCount), 0, "the 46 request carries no extFields");

    JsonValue body = mock.bodyJson(0);
    expect(body.isObject(), "body is a JSON object");
    expect(body.get("clientId").stringValue() == kClientId, "body.clientId");
    expect(body.get("group").stringValue() == std::string(kGroup), "body.group");
    const JsonValue* sd = body.find("subscriptionData");
    expect(sd != nullptr && sd->isObject(), "body.subscriptionData is an object");
    if (sd == nullptr) return;
    expect(sd->get("topic").stringValue() == std::string(kTopic), "subscriptionData.topic");
    expect(sd->get("subString").stringValue() == "a > 10", "subscriptionData.subString");
    expect(sd->get("expressionType").stringValue() == std::string(ExpressionType::SQL92),
           "subscriptionData.expressionType");
    std::vector<std::string> keys;
    for (const auto& kv : sd->objectItems()) keys.push_back(kv.first);
    std::sort(keys.begin(), keys.end());
    const std::vector<std::string> want = {"classFilterMode", "codeSet", "expressionType",
                                           "subString", "subVersion", "tagsSet", "topic"};
    expect(keys == want, "subscriptionData has only those 7 wire keys", jsonDump(body).c_str());
    expect(sd->find("filterClassSource") == nullptr, "filterClassSource is not serialized");
}

// 3 / 4. The broker reject code is carried through verbatim -- this is the criterion for a startup failure
void testBrokerRejectCode(MockBroker& mock, int32_t code, const char* name) {
    auto instance = seeded(mock);
    mock.scriptChecks({{code, "remark-" + std::to_string(code)}});
    std::vector<SubscriptionData> subs;
    subs.push_back(sql92Sub("a >"));
    int threw = 0;
    int32_t got = 0;
    std::string msg;
    try {
        instance->checkSubscriptionsInBroker(kGroup, subs);
    } catch (const MQClientException& e) {
        threw = 1;
        got = e.getResponseCode();
        msg = e.what();
    } catch (const std::exception& e) {
        std::printf("  wrong exception for %s: %s\n", name, e.what());
    }
    expect(threw == 1, name);
    expectInt(got, code, std::string(name) + " carries the broker response code");
    expect(msg.find("remark") != std::string::npos, std::string(name) + " keeps the remark", msg);
}

void testNoRouteSkips(MockBroker& mock) {
    netcompat::ensureInitialized();
    auto instance = std::make_unique<MQClientInstance>(
        kClientId, std::vector<std::string>{mock.address()});
    mock.scriptChecks({});
    std::vector<SubscriptionData> subs;
    subs.push_back(sql92Sub());
    instance->checkSubscriptionsInBroker(kGroup, subs);  // no route for this topic in the cache
    expectInt(mock.checkCount(), 0, "a subscription without a route sends not even one 46");
    expect(instance->findBrokerAddrByTopic(kTopic).empty(), "findBrokerAddrByTopic only reads the cache");
}

void testTransportErrorWrapped(MockBroker& mock) {
    TopicRouteData route;
    std::map<int64_t, std::string> addrs;
    addrs[0] = deadAddress();
    route.brokerDatas.emplace_back("DefaultCluster", "broker-a", addrs);
    mock.addRoute(kTopic, route);
    auto instance = std::make_unique<MQClientInstance>(
        kClientId, std::vector<std::string>{mock.address()});
    instance->updateTopicRouteInfoFromNameServer(kTopic);
    mock.scriptChecks({});
    std::vector<SubscriptionData> subs;
    subs.push_back(sql92Sub());
    int threw = 0;
    std::string msg;
    try {
        instance->checkSubscriptionsInBroker(kGroup, subs);
    } catch (const MQClientException& e) {
        threw = 1;
        msg = e.what();
    } catch (const std::exception& e) {
        std::printf("  wrong exception type: %s\n", e.what());
    }
    expect(threw == 1, "an unreachable broker still throws MQClientException");
    expect(msg.find("SQL92") != std::string::npos, "the message includes the expression type", msg);
    expect(msg.find("server has not been upgraded to support") != std::string::npos,
           "the message matches the reference wording", msg);
    expectInt(mock.checkCount(), 0, "the broker received no 46 when the transport failed");
}

void testEmptySubs(MockBroker& mock) {
    auto instance = seeded(mock);
    mock.scriptChecks({});
    instance->checkSubscriptionsInBroker(kGroup, std::vector<SubscriptionData>());
    expectInt(mock.checkCount(), 0, "an empty subscription sends no 46");
}

// 8. The master is preferred; only when the master is absent is a slave picked at random
void testMasterPreferred(MockBroker& mock) {
    TopicRouteData route;
    std::map<int64_t, std::string> addrs;
    addrs[0] = mock.address();
    addrs[1] = deadAddress();  // slave: selecting it always fails to connect
    route.brokerDatas.emplace_back("DefaultCluster", "broker-a", addrs);
    mock.addRoute(kTopic, route);
    auto instance = std::make_unique<MQClientInstance>(
        kClientId, std::vector<std::string>{mock.address()});
    instance->updateTopicRouteInfoFromNameServer(kTopic);
    mock.scriptChecks({});
    std::vector<SubscriptionData> subs;
    subs.push_back(sql92Sub());
    for (int i = 0; i < 5; ++i) {
        instance->checkSubscriptionsInBroker(kGroup, subs);  // an exception means test failure
    }
    expectInt(mock.checkCount(), 5, "with a master present only the master is ever called");
}

// 9. CheckClientRequestBody round trip + the namespace is serialized only when the flag is set
void testBodyRoundTrip() {
    CheckClientRequestBody in;
    in.clientId = "cid";
    in.group = "gid";
    in.subscriptionData = sql92Sub("color = 'red'");
    JsonValue v = in.toJson();
    expect(v.find("namespace") == nullptr, "namespace is not serialized while unset");
    CheckClientRequestBody out;
    expect(CheckClientRequestBody::decode(in.encode(), out), "body decodes back");
    expect(out.clientId == in.clientId && out.group == in.group, "clientId / group round trip");
    expect(out.subscriptionData.topic == kTopic
               && out.subscriptionData.subString == "color = 'red'"
               && out.subscriptionData.expressionType == std::string(ExpressionType::SQL92),
           "subscriptionData round trip");
    in.hasNamespace = true;
    in.clientNamespace = "nsA";
    CheckClientRequestBody out2;
    expect(CheckClientRequestBody::decode(in.encode(), out2) && out2.hasNamespace
               && out2.clientNamespace == "nsA",
           "round trip after the namespace is set");
}

}  // namespace

int main() {
    MockBroker mock;
    testTagOnlySendsNothing(mock);
    testSql92RequestShape(mock);
    testBrokerRejectCode(mock, ResponseCode::SUBSCRIPTION_PARSE_FAILED, "SUBSCRIPTION_PARSE_FAILED");
    testBrokerRejectCode(mock, ResponseCode::SYSTEM_ERROR, "SYSTEM_ERROR without enablePropertyFilter");
    testNoRouteSkips(mock);
    testTransportErrorWrapped(mock);
    testEmptySubs(mock);
    testMasterPreferred(mock);
    testBodyRoundTrip();
    std::printf("%d checks, %d failures\n", checks, fails);
    return fails == 0 ? 0 : 1;
}
