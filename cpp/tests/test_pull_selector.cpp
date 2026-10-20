// Offline unit tests for **what the pull request carries as the subscription** -- fake name server +
// fake broker (real sockets), never touching a real cluster.
//
// Why this must be pinned from the wire: a pull that always reports TAG turns a selector subscription
// into a silent "filter nothing / filter everything" mismatch between the two ends. The broker filters
// with the expression it holds for the group, the request header says something else, and the local
// second pass then drops messages on an expression that is not a tag list at all. Nothing on a live
// cluster reports that: consumption looks healthy while the wrong messages arrive.
//
// The evidence is the recorded PULL_MESSAGE header (expressionType / subscription / subVersion) plus
// the HEART_BEAT body, both read off the socket by the fake endpoint. Every positive case is paired
// with the historic tag shape (TAG + subVersion 0 + the parsed tagsSet driving the local re-filter), so
// a change that drops the selector on the floor -- or one that also rewrites the tag path -- goes red.
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <deque>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "rocketmq/client/lite_pull_consumer.h"
#include "rocketmq/client/pull_consumer.h"
#include "rocketmq/client/result.h"
#include "rocketmq/common/message.h"
#include "rocketmq/common/message_decoder.h"
#include "rocketmq/common/mix_all.h"
#include "rocketmq/common/net_compat.h"
#include "rocketmq/common/subscription_data.h"
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

void expectStr(const std::string& actual, const std::string& expected, const std::string& name) {
    ++checks;
    if (actual != expected) {
        ++fails;
        std::printf("FAIL %s (actual=[%s] expected=[%s])\n", name.c_str(), actual.c_str(),
                    expected.c_str());
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
    if (!readN(s, reinterpret_cast<char*>(body.data()), static_cast<size_t>(totalLen))) return false;
    out = lenStr + body;
    return true;
}

// ---------------------------------------------------------------- fake cluster

struct PullReply {
    int32_t code = ResponseCode::PULL_NOT_FOUND;
    PropertyMap ext;
    Bytes body;
};

// Plays the name server (routes), the broker (PULL_MESSAGE / LITE_PULL_MESSAGE / QUERY_CONSUMER_OFFSET)
// and records every HEART_BEAT body, so both the request header and the registration are observable.
class FakeCluster {
public:
    FakeCluster() {
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

    ~FakeCluster() {
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

    FakeCluster(const FakeCluster&) = delete;
    FakeCluster& operator=(const FakeCluster&) = delete;

    std::string address() const { return "127.0.0.1:" + std::to_string(port_); }

    void addRoute(const std::string& topic, const TopicRouteData& route) {
        std::lock_guard<std::mutex> lk(state_);
        routes_[topic] = route.encode();
    }

    void scriptPull(int32_t code, const PropertyMap& ext, const Bytes& body = Bytes()) {
        std::lock_guard<std::mutex> lk(state_);
        PullReply reply;
        reply.code = code;
        reply.ext = ext;
        reply.body = body;
        pullScripts_.push_back(reply);
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

    std::string pullField(size_t i, const std::string& key) {
        const PropertyMap ext = pullExt(i);
        auto it = ext.find(key);
        return it == ext.end() ? std::string() : it->second;
    }

    int64_t pullIntField(size_t i, const std::string& key, int64_t fallback) {
        const std::string text = pullField(i, key);
        if (text.empty()) return fallback;
        try {
            return std::stoll(text);
        } catch (...) {
            return fallback;
        }
    }

    size_t heartbeatCount() {
        std::lock_guard<std::mutex> lk(state_);
        return heartbeats_.size();
    }

    bool lastHeartbeatSubscription(const std::string& topic, SubscriptionData& out) {
        std::lock_guard<std::mutex> lk(state_);
        for (size_t round = heartbeats_.size(); round > 0; --round) {
            JsonValue v;
            if (!jsonParse(heartbeats_[round - 1], v, nullptr)) continue;
            const HeartbeatData hb = HeartbeatData::fromJson(v);
            for (const ConsumerData& cd : hb.consumerDataSet) {
                for (const SubscriptionData& sub : cd.subscriptionDataSet) {
                    if (sub.topic == topic) {
                        out = sub;
                        return true;
                    }
                }
            }
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
            } else if (req.code == RequestCode::HEART_BEAT) {
                std::lock_guard<std::mutex> lk(state_);
                heartbeats_.push_back(std::string(req.body.begin(), req.body.end()));
                resp.code = ResponseCode::SUCCESS;
            } else if (req.code == RequestCode::QUERY_CONSUMER_OFFSET) {
                resp.code = ResponseCode::QUERY_NOT_FOUND;
            } else {
                resp.code = ResponseCode::SUCCESS;
            }
            Bytes out = resp.encode();
            if (!writeAll(sock, out)) break;
        }
        ::shutdown(sock, kShutdownHow);
        netcompat::closeSocket(sock);
    }

    void answerPull(const RemotingCommand& req, RemotingCommand& resp) {
        {
            std::lock_guard<std::mutex> lk(state_);
            pulls_.push_back(req.extFields);
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
    std::deque<PullReply> pullScripts_;
    std::vector<std::string> heartbeats_;
    std::mutex workersM_;
    std::vector<std::thread> workers_;
    std::thread acceptor_;
};

// ---------------------------------------------------------------- scaffolding

const char* kTopic = "SelectorPullTopic";
const char* kBroker = "broker-a";
const char* kTagExpr = "TagA||TagB";
const char* kSqlExpr = "color = 'red' AND weight > 10";

MessageQueue queue0() { return MessageQueue(kTopic, kBroker, 0); }

TopicRouteData routeMaster(const std::string& addr) {
    TopicRouteData route;
    std::map<int64_t, std::string> addrs;
    addrs[MixAll::MASTER_ID] = addr;
    route.brokerDatas.emplace_back("DefaultCluster", kBroker, addrs);
    route.queueDatas.emplace_back(kBroker, 1, 1, 6, 0);
    return route;
}

PropertyMap pullResp(int64_t next, int64_t min = 0, int64_t max = 9) {
    PropertyMap ext;
    ext["nextBeginOffset"] = std::to_string(next);
    ext["minOffset"] = std::to_string(min);
    ext["maxOffset"] = std::to_string(max);
    return ext;
}

MessageExt taggedMsg(const std::string& tags, int64_t queueOffset) {
    MessageExt m;
    m.topic = kTopic;
    m.brokerName = kBroker;
    m.queueId = 0;
    m.queueOffset = queueOffset;
    m.body = "payload";
    m.setTags(tags);
    return m;
}

template <typename Fn>
bool waitFor(Fn fn, int timeoutMs = 5000) {
    const int64_t deadline = UtilAll::currentTimeMillis() + timeoutMs;
    while (UtilAll::currentTimeMillis() < deadline) {
        if (fn()) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    return fn();
}

// ---------------------------------------------------------------- 1. FilterAPI: the two shapes

// Pure logic, no sockets: what a selector build must never do is parse the expression as a tag list --
// that is exactly what turns the local re-filter into a message eater.
void testSelectorAwareBuilder() {
    const SubscriptionData tag =
        FilterAPI::buildSubscriptionData(kTopic, kTagExpr, ExpressionType::TAG);
    expectStr(tag.expressionType, ExpressionType::TAG, "tagBuilder.expressionType");
    expectStr(tag.subString, kTagExpr, "tagBuilder.subString");
    expectInt(static_cast<long long>(tag.tagsSet.size()), 2, "tagBuilder.parsesTags");
    expectInt(static_cast<long long>(tag.codeSet.size()), 2, "tagBuilder.parsesCodes");
    // The historic pull shape: a tag subscription carries subVersion 0.
    expectInt(FilterAPI::pullSubVersionOf(tag), 0, "tagBuilder.pullSubVersionIsZero");
    expectStr(FilterAPI::pullExpression(tag), kTagExpr, "tagBuilder.pullExpression");

    const SubscriptionData wildcard =
        FilterAPI::buildSubscriptionData(kTopic, std::string(), ExpressionType::TAG);
    expectStr(FilterAPI::pullExpression(wildcard), "*", "tagBuilder.emptyBecomesWildcard");

    const SubscriptionData sql =
        FilterAPI::buildSubscriptionData(kTopic, kSqlExpr, ExpressionType::SQL92);
    expectStr(sql.expressionType, ExpressionType::SQL92, "sqlBuilder.expressionType");
    // Verbatim: no trimming, no normalising, no wildcard substitution.
    expectStr(sql.subString, kSqlExpr, "sqlBuilder.expressionVerbatim");
    expectInt(static_cast<long long>(sql.tagsSet.size()), 0, "sqlBuilder.tagsSetStaysEmpty");
    expectInt(static_cast<long long>(sql.codeSet.size()), 0, "sqlBuilder.codeSetStaysEmpty");
    expectInt(FilterAPI::pullSubVersionOf(sql), sql.subVersion, "sqlBuilder.pullCarriesItsVersion");
    expect(FilterAPI::pullSubVersionOf(sql) != 0, "sqlBuilder.versionIsNotTheTagShapeZero");
    expectStr(FilterAPI::pullExpression(sql), kSqlExpr, "sqlBuilder.pullExpressionVerbatim");

    // An expression that looks like a tag list is still a selector expression: nothing gets parsed.
    const SubscriptionData sqlTags =
        FilterAPI::buildSubscriptionData(kTopic, kTagExpr, ExpressionType::SQL92);
    expectInt(static_cast<long long>(sqlTags.tagsSet.size()), 0,
              "sqlBuilder.tagShapedExpressionIsNotParsed");
    expectStr(FilterAPI::pullExpression(sqlTags), kTagExpr, "sqlBuilder.tagShapedExpressionVerbatim");
}

// ---------------------------------------------------------------- 2. pull consumer: both shapes

void testPullConsumerTagShapeUnchanged(FakeCluster& cluster) {
    DefaultMQPullConsumer c("PG_Selector_ClassicTag");
    c.setNamesrvAddr(cluster.address());
    c.start();
    c.pull(queue0(), kTagExpr, 0, 32, 3000);
    expectInt(static_cast<long long>(cluster.pullCount()), 1, "tag: one pull reached the broker");
    expectStr(cluster.pullField(0, "expressionType"), "TAG", "tag.expressionType");
    expectStr(cluster.pullField(0, "subscription"), kTagExpr, "tag.subscription");
    expectInt(cluster.pullIntField(0, "subVersion", -1), 0, "tag.subVersionStaysZero");

    // The selector spelling of the same tag expression is byte-identical to the string form: the
    // overloads differ in how the caller writes them, not in what goes on the wire.
    c.pull(queue0(), MessageSelector::byTag(kTagExpr), 0, 32, 3000);
    expectInt(static_cast<long long>(cluster.pullCount()), 2, "tagSelector: one pull reached the broker");
    expectStr(cluster.pullField(1, "expressionType"), "TAG", "tagSelector.expressionType");
    expectStr(cluster.pullField(1, "subscription"), kTagExpr, "tagSelector.subscription");
    expectInt(cluster.pullIntField(1, "subVersion", -1), 0, "tagSelector.subVersionStaysZero");
    c.shutdown();
}

void testPullConsumerSelectorReachesTheWire(FakeCluster& cluster) {
    DefaultMQPullConsumer c("PG_Selector_SqlPull");
    c.setNamesrvAddr(cluster.address());
    c.start();
    c.pull(queue0(), MessageSelector::bySql(kSqlExpr), 0, 32, 3000);
    expectInt(static_cast<long long>(cluster.pullCount()), 1, "sql: one pull reached the broker");
    expectStr(cluster.pullField(0, "expressionType"), "SQL92", "sql.expressionTypeOnTheWire");
    expectStr(cluster.pullField(0, "subscription"), kSqlExpr, "sql.subscriptionIsTheRawExpression");
    expect(cluster.pullIntField(0, "subVersion", 0) != 0, "sql.subVersionIsTheSubscriptionVersion");

    // Suspended variant (long polling): same kernel, same header.
    c.pullBlockIfNotFound(queue0(), MessageSelector::bySql(kSqlExpr), 0, 32);
    expectInt(static_cast<long long>(cluster.pullCount()), 2, "sql.blockIfNotFound: pulled");
    expectStr(cluster.pullField(1, "expressionType"), "SQL92", "sql.blockIfNotFound.expressionType");
    expectStr(cluster.pullField(1, "subscription"), kSqlExpr, "sql.blockIfNotFound.subscription");
    expect(cluster.pullIntField(1, "subVersion", 0) != 0,
           "sql.blockIfNotFound.subVersionIsTheSubscriptionVersion");
    // ... and its tag control still keeps the historic shape.
    c.pullBlockIfNotFound(queue0(), kTagExpr, 0, 32);
    expectStr(cluster.pullField(2, "expressionType"), "TAG", "tag.blockIfNotFound.expressionType");
    expectInt(cluster.pullIntField(2, "subVersion", -1), 0, "tag.blockIfNotFound.subVersionStaysZero");
    c.shutdown();
}

void testPullAsyncUsesTheSelectorShape(FakeCluster& cluster) {
    class RecordingCallback : public PullCallback {
    public:
        void onSuccess(const PullResult&) override { done.store(true); }
        void onException(const std::exception_ptr&) override { done.store(true); }
        std::atomic<bool> done{false};
    };

    DefaultMQPullConsumer c("PG_Selector_AsyncPull");
    c.setNamesrvAddr(cluster.address());
    c.start();
    auto sqlCallback = std::make_shared<RecordingCallback>();
    c.pullAsync(queue0(), MessageSelector::bySql(kSqlExpr), 0, 32, sqlCallback, 3000);
    expect(waitFor([&] { return sqlCallback->done.load(); }), "async sql: callback delivered");
    expectStr(cluster.pullField(0, "expressionType"), "SQL92", "async.sql.expressionType");
    expectStr(cluster.pullField(0, "subscription"), kSqlExpr, "async.sql.subscription");
    expect(cluster.pullIntField(0, "subVersion", 0) != 0, "async.sql.subVersionIsNotZero");

    auto tagCallback = std::make_shared<RecordingCallback>();
    c.pullAsync(queue0(), kTagExpr, 0, 32, tagCallback, 3000);
    expect(waitFor([&] { return tagCallback->done.load(); }), "async tag: callback delivered");
    expectStr(cluster.pullField(1, "expressionType"), "TAG", "async.tag.expressionType");
    expectStr(cluster.pullField(1, "subscription"), kTagExpr, "async.tag.subscription");
    expectInt(cluster.pullIntField(1, "subVersion", -1), 0, "async.tag.subVersionStaysZero");
    c.shutdown();
}

// ---------------------------------------------------------------- 3. lite pull consumer

void testLitePullTagSubscriptionKeepsTheShape(FakeCluster& cluster) {
    DefaultLitePullConsumer c("LitePG_SelectorTag");
    c.setNamesrvAddr(cluster.address());
    c.setConsumeFromWhere(ConsumeFromWhere::CONSUME_FROM_FIRST_OFFSET);
    c.setSubExpressionForAssign(kTopic, kTagExpr);
    c.assign({queue0()});
    c.start();
    expect(waitFor([&] { return cluster.pullCount() >= 1; }), "lite tag: a pull reached the broker");
    expectStr(cluster.pullField(0, "expressionType"), "TAG", "lite.tag.expressionType");
    expectStr(cluster.pullField(0, "subscription"), kTagExpr, "lite.tag.subscription");
    expectInt(cluster.pullIntField(0, "subVersion", -1), 0, "lite.tag.subVersionStaysZero");
    c.shutdown();
}

// The decisive pair: the same broker answer, the same messages, two subscription kinds -- a tag
// subscription is re-filtered locally, a selector subscription is not.
void testLitePullSelectorIsNotRefilteredLocally(FakeCluster& cluster) {
    DefaultLitePullConsumer c("LitePG_SelectorSql");
    c.setNamesrvAddr(cluster.address());
    c.setConsumeFromWhere(ConsumeFromWhere::CONSUME_FROM_FIRST_OFFSET);
    c.setSubExpressionForAssign(kTopic, MessageSelector::bySql(kSqlExpr));
    c.assign({queue0()});
    cluster.scriptPull(ResponseCode::SUCCESS, pullResp(2),
                       encodeMessageExt(taggedMsg("TagZ", 0), false));
    c.start();
    // The pull loop walks one queue at a time, so the second request of the queue proves the first
    // round (filter + buffer) is finished: no timing guess is needed before the delivery check.
    expect(waitFor([&] { return cluster.pullCount() >= 2; }), "lite sql: two pull rounds reached the broker");
    expectStr(cluster.pullField(0, "expressionType"), "SQL92", "lite.sql.expressionType");
    expectStr(cluster.pullField(0, "subscription"), kSqlExpr, "lite.sql.subscriptionVerbatim");
    expect(cluster.pullIntField(0, "subVersion", 0) != 0, "lite.sql.subVersionIsNotTheTagZero");

    const std::vector<MessageExt> delivered = c.poll(200);
    expectInt(static_cast<long long>(delivered.size()), 1,
              "lite.sql: the broker keeps what a selector matches -- no local tag re-filter");
    if (!delivered.empty()) {
        expectStr(delivered[0].getTags(), "TagZ", "lite.sql.messagePassesThroughUntouched");
    }
    c.shutdown();

    // Control: a tag subscription of the same shape (one message, a tag outside the expression) does
    // run the local second pass, so nothing is delivered.
    FakeCluster other;
    other.addRoute(kTopic, routeMaster(other.address()));
    DefaultLitePullConsumer t("LitePG_SelectorTagRefilter");
    t.setNamesrvAddr(other.address());
    t.setConsumeFromWhere(ConsumeFromWhere::CONSUME_FROM_FIRST_OFFSET);
    t.setSubExpressionForAssign(kTopic, "TagA||TagB");
    t.assign({queue0()});
    other.scriptPull(ResponseCode::SUCCESS, pullResp(2), encodeMessageExt(taggedMsg("TagZ", 0), false));
    t.start();
    expect(waitFor([&] { return other.pullCount() >= 2; }),
           "lite tag control: two pull rounds reached the broker");
    expectInt(static_cast<long long>(t.poll(200).size()), 0,
              "lite.tag: the local re-filter still drops a non-matching tag");
    t.shutdown();
}

// The heartbeat registers the selector with its own expression type (the pull is only half of it).
void testLitePullHeartbeatRegistersTheSelector(FakeCluster& cluster) {
    DefaultLitePullConsumer c("LitePG_SelectorHb");
    c.setNamesrvAddr(cluster.address());
    c.subscribe(kTopic, MessageSelector::bySql(kSqlExpr));
    c.start();
    expect(waitFor([&] { return cluster.heartbeatCount() >= 1; }), "lite sql: heartbeat reached the broker");
    SubscriptionData registered;
    expect(cluster.lastHeartbeatSubscription(kTopic, registered),
           "lite sql: the heartbeat carries this topic's subscription");
    expectStr(registered.expressionType, "SQL92", "lite.sql.heartbeatExpressionType");
    expectStr(registered.subString, kSqlExpr, "lite.sql.heartbeatExpressionVerbatim");
    expectInt(static_cast<long long>(registered.tagsSet.size()), 0,
              "lite.sql.heartbeatHasNoTagSet");
    c.shutdown();

    // Control: the tag subscription of the same shape registers TAG with the parsed tag set.
    FakeCluster other;
    other.addRoute(kTopic, routeMaster(other.address()));
    DefaultLitePullConsumer t("LitePG_SelectorHbTag");
    t.setNamesrvAddr(other.address());
    t.subscribe(kTopic, kTagExpr);
    t.start();
    expect(waitFor([&] { return other.heartbeatCount() >= 1; }), "lite tag: heartbeat reached the broker");
    SubscriptionData tagSub;
    expect(other.lastHeartbeatSubscription(kTopic, tagSub),
           "lite tag: the heartbeat carries this topic's subscription");
    expectStr(tagSub.expressionType, "TAG", "lite.tag.heartbeatExpressionType");
    expectInt(static_cast<long long>(tagSub.tagsSet.size()), 2, "lite.tag.heartbeatCarriesTheTagSet");
    t.shutdown();
}

}  // namespace

int main() {
    testSelectorAwareBuilder();
    {
        FakeCluster cluster;
        cluster.addRoute(kTopic, routeMaster(cluster.address()));
        testPullConsumerTagShapeUnchanged(cluster);
    }
    {
        FakeCluster cluster;
        cluster.addRoute(kTopic, routeMaster(cluster.address()));
        testPullConsumerSelectorReachesTheWire(cluster);
    }
    {
        FakeCluster cluster;
        cluster.addRoute(kTopic, routeMaster(cluster.address()));
        testPullAsyncUsesTheSelectorShape(cluster);
    }
    {
        FakeCluster cluster;
        cluster.addRoute(kTopic, routeMaster(cluster.address()));
        testLitePullTagSubscriptionKeepsTheShape(cluster);
    }
    {
        FakeCluster cluster;
        cluster.addRoute(kTopic, routeMaster(cluster.address()));
        testLitePullSelectorIsNotRefilteredLocally(cluster);
    }
    {
        FakeCluster cluster;
        cluster.addRoute(kTopic, routeMaster(cluster.address()));
        testLitePullHeartbeatRegistersTheSelector(cluster);
    }
    std::printf("pull_selector: %d checks, %d failed\n", checks, fails);
    return fails == 0 ? 0 : 1;
}
