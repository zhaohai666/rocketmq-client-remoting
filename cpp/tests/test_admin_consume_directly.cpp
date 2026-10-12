// Admin 发起端 CONSUME_MESSAGE_DIRECTLY(309) 单测 —— 不需要集群。
//
//
// 为什么必须离线锁死：这条链路错起来全是**静默**的 —— msgId 键名写错时 broker 侧头
// 解析不到、推给消费者的消息对不上号，admin 拿到的结果里 remark/msgId 全错；目标
// broker 取错（打路由第一台而不是存储主机）在单 broker 集群里完全看不出来，多 broker
// 时表现为 broker 回 NO_MESSAGE 或把探针推错实例。mock broker 扮演四类请求：
// 路由(105) / VIEW_MESSAGE_BY_ID(33) / QUERY_MESSAGE(12) / CONSUME_MESSAGE_DIRECTLY(309)，
// 与 rmq_test_check_client_config 的 MockBroker 同一套 socket 骨架。
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

// ---------------------------------------------------------------- socket 工具

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

// select 等到可读（100ms 一轮，便于 alive 转 false 后尽快退出），再读一整帧
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

// 一笔请求的脚本化应答（309 的应答体是 ConsumeMessageDirectlyResult JSON，
// 33 / 12 的应答体是 17 段存储格式消息流）
struct ScriptedReply {
    int32_t code = ResponseCode::SUCCESS;
    std::string remark;
    Bytes body;
};

// 一笔请求的取证
struct WireRecord {
    int32_t code = 0;
    PropertyMap ext;
    Bytes body;
};

// 同时扮演 name server（回路由）与 broker（33 / 12 / 309 按脚本应答）。
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
        // 连接线程在 100ms 一轮的 select 里看到 running_ 转 false 就收手；
        // joinable 的 std::thread 析构会直接 terminate，所以必须在这里收干净。
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

    // 每类请求各自一套脚本（按到达顺序取用；越界回默认应答）。
    // 默认：VIEW_MESSAGE_BY_ID / QUERY_MESSAGE 回 NO_MESSAGE（消息不存在），
    // 309 回 SUCCESS + 空体（触发 admin 的 "no consume result" 分支）。
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

    // 取三类脚本里与该请求码对应的那个
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

// ---------------------------------------------------------------- 用例脚手架

const char* kGroup = "GID_DirectConsumeAdmin";
const char* kTopic = "DirectConsumeAdminTopic";
const char* kClientId = "127.0.0.1@1234#5678";

MessageExt storedMessage(MockBroker& mock, int64_t commitLogOffset, bool withUniqKey,
                         const std::string& uniqKey) {
    // broker 存储里的一条消息：存储主机 = mock 自己（309 的目标地址从这来）、
    // 物理偏移给定（解码侧 msgId / offsetMsgId 都从 storeHost+offset 反推）。
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

// 消息落在这台 broker 上 ⇒ offsetMsgId = createMessageId(storeAddr, offset)。
// 本端口解码时 msgId / offsetMsgId 都从 17 段报文的 storeHost + PHYSICALOFFSET 反推。
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

// ------------------------------------------------ 309 头键名（发错是静默的）

void testExtFieldKeyNames() {
    PropertyMap ext = DefaultMQAdminExt::buildConsumeMessageDirectlyExtFields(
        "TopicA", "GID_A", "cid-1", "msg-1");
    expectInt(static_cast<long long>(ext.size()), 4, "309 头恰好四个字段");
    auto it = ext.find("topic");
    expect(it != ext.end() && it->second == "TopicA", "309 头 topic");
    it = ext.find("consumerGroup");
    expect(it != ext.end() && it->second == "GID_A", "309 头 consumerGroup");
    it = ext.find("clientId");
    expect(it != ext.end() && it->second == "cid-1", "309 头 clientId");
    it = ext.find("msgId");
    expect(it != ext.end() && it->second == "msg-1", "309 头 msgId");
    expect(ext.find("brokerName") == ext.end(), "309 头不带 brokerName");
    expect(ext.find("topicSysFlag") == ext.end(), "309 头不带 topicSysFlag");
    expect(ext.find("groupSysFlag") == ext.end(), "309 头不带 groupSysFlag");
}

// ------------------------------------------------ offsetMsgId 直查路径

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

    // viewMessage 的 offset 路径：一笔 VIEW_MESSAGE_BY_ID，直接打 msgId 里编的地址
    expectInt(mock.viewCount(), 1, "offsetPath.viewCount");
    const WireRecord view = mock.viewRecord(0);
    expectInt(view.code, RequestCode::VIEW_MESSAGE_BY_ID, "offsetPath.viewCode");
    expect(view.ext.count("offset") == 1 && view.ext.at("offset") == "100", "offsetPath.viewOffset",
           view.ext.count("offset") ? view.ext.at("offset") : "(absent)");
    expectInt(mock.queryCount(), 0, "offsetPath.noUniqKeyFallback");

    // 309 恰好一笔，目标是消息的存储主机（= mock 自己），msgId 原样透传
    expectInt(mock.directCount(), 1, "offsetPath.directCount");
    expectDirectExt(mock.directRecord(0), msgId, "offsetPath.direct");
}

// ------------------------------------------------ uniqKey 路径 + offsetMsgId 替换

void testUniqKeyPath(DefaultMQAdminExt& admin, MockBroker& mock) {
    // uniq key 本身不是合法的 offset msgId：viewMessage 先按 offsetMsgId 直查
    // （这里脚本回 NO_MESSAGE），失败退 uniqKey 检索（QUERY_MESSAGE，body 是
    // 17 段消息流，msgId 反推后必须等于 key 才算命中）。
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

    // uniqKey 检索的请求形状：indexType=U + MixAll.UNIQUE_MSG_QUERY_FLAG（键名，不是标志位）
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

// ------------------------------------------------ broker 拒绝 / 空结果体

void testBrokerRejectPropagates(DefaultMQAdminExt& admin, MockBroker& mock) {
    // 消费者不在线时 broker 无处转推：非 SUCCESS 的响应码原样带上抛出
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
    // msgId 解不出来（非法 hex + uniqKey 也查无此消息）时 viewMessage 抛错，
    // 309 一笔都不该发 —— 打到哪个 broker 都是无处转推的空转。
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
    // 把 kTopic 的路由塞进缓存（uniqKey 检索按路由找 broker）
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
