// CHECK_CLIENT_CONFIG(46) 离线单测 —— 不需要集群。
//
// 为什么必须有这一步：SQL92 表达式写错时 broker **不会**报错 ——
// ExpressionMessageFilter#isMatched 在 ConsumeQueue 阶段拿不到编译好的过滤数据就直接
// return true（静默放行全部消息），消费者启动照常成功。Java 因此在
// DefaultMQPushConsumerImpl.start:1014 主动发一笔 46 号请求，把「写错的表达式」变成
// 启动期错误。本端口此前只有 RequestCode 常量、没有调用方，所以这里锁死协议形状与
// 分支语义（真机行为见 examples/sql92_live.cpp）。
//
// 对齐基准（Java 5.5.1，逐行读过；与 python/tests/test_check_client_config.py、
// rust/src/client/mq_client.rs 的同一组断言对拍）：
//   * MQClientAPIImpl#checkClientInBroker:3256 —— 请求头是 **null**（线上没有 extFields），
//     body 是 CheckClientRequestBody 的 JSON（clientId / group / subscriptionData）；
//     响应码非 SUCCESS 时抛 MQClientException(响应码, remark)。
//   * MQClientInstance#checkClientInBroker:534 —— 只查非 TAG 订阅
//     （ExpressionType.isTagType：null / "" / TAG 都算 TAG）；broker 地址来自
//     findBrokerAddrByTopic（**只读缓存**路由、随机一个 broker、优先 master），
//     取不到就跳过；网络类异常换成固定文案的 MQClientException。
//   * ClientConfig#mqClientApiTimeout 默认 **3000ms**，这笔请求用的就是它。
//
// 覆盖：
//   1. TAG（含空串）订阅一笔 46 都不发
//   2. SQL92 请求形状：恰好一笔、无 extFields、body 三个字段与 SubscriptionData 的 7 个键
//   3. broker 回 SUBSCRIPTION_PARSE_FAILED(23) → MQClientException 带该响应码与 remark
//   4. broker 未开 enablePropertyFilter 回 SYSTEM_ERROR(1) → 同一条码
//   5. 查不到路由 → 跳过（一笔不发，也不抛）
//   6. 连不上 broker → 换成 Java 的固定文案
//   7. 空订阅集合 → 什么都不发
//   8. 只有 master 缺席时才随机取从节点（selectBrokerAddr 语义）
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

// 已被占用过、随后立即释放的回环端口：连它必然被拒（比固定端口可靠）
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

// 一笔 46 的脚本化应答
struct CheckReply {
    int32_t code = ResponseCode::SUCCESS;
    std::string remark;
};

// 一条 46 请求的取证
struct CheckRecord {
    int32_t code = 0;
    size_t extCount = 0;
    Bytes body;
};

// 同时扮演 name server（回路由）与 broker（按脚本回 46 的响应码）。
// 与 rmq_test_send_retry 的 MockEndpoint 同一套 socket 骨架，只是这里只需要两类请求。
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

    void addRoute(const std::string& topic, const TopicRouteData& route) {
        std::lock_guard<std::mutex> lk(state_);
        routes_[topic] = route.encode();
    }

    // 第 `i` 笔 46 回 `codes[i]`；越界回 SUCCESS
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

    // 第 `i` 笔 46 的 body 解出来的 JSON（解不出来回 Null）
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

// ---------------------------------------------------------------- 用例脚手架

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

// 起一个实例并把 kTopic 的路由塞进缓存（指向 mock 自己）
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

// ---------------------------------------------------------------- 用例

// 1. TAG（含空串）一律不发 46：Java ExpressionType.isTagType 短路
void testTagOnlySendsNothing(MockBroker& mock) {
    auto instance = seeded(mock);
    mock.scriptChecks({});
    std::vector<SubscriptionData> subs;
    subs.push_back(tagSub(ExpressionType::TAG));
    subs.push_back(tagSub(""));
    instance->checkSubscriptionsInBroker(kGroup, subs);
    expectInt(mock.checkCount(), 0, "TAG / 空 expressionType 都不发 46");
}

// 2. SQL92 请求形状对齐 Java
void testSql92RequestShape(MockBroker& mock) {
    auto instance = seeded(mock);
    mock.scriptChecks({});
    std::vector<SubscriptionData> subs;
    subs.push_back(sql92Sub());
    instance->checkSubscriptionsInBroker(kGroup, subs);
    expectInt(mock.checkCount(), 1, "SQL92 订阅正好一笔 46");
    CheckRecord r = mock.checkRecord(0);
    expectInt(r.code, RequestCode::CHECK_CLIENT_CONFIG, "请求码是 46");
    // 请求头 null ⇒ 线上没有 extFields
    expectInt(static_cast<long long>(r.extCount), 0, "46 请求不带 extFields");

    JsonValue body = mock.bodyJson(0);
    expect(body.isObject(), "body 是 JSON 对象");
    expect(body.get("clientId").stringValue() == kClientId, "body.clientId");
    expect(body.get("group").stringValue() == std::string(kGroup), "body.group");
    const JsonValue* sd = body.find("subscriptionData");
    expect(sd != nullptr && sd->isObject(), "body.subscriptionData 是对象");
    if (sd == nullptr) return;
    expect(sd->get("topic").stringValue() == std::string(kTopic), "subscriptionData.topic");
    expect(sd->get("subString").stringValue() == "a > 10", "subscriptionData.subString");
    expect(sd->get("expressionType").stringValue() == std::string(ExpressionType::SQL92),
           "subscriptionData.expressionType");
    // Java SubscriptionData 的序列化字段名（filterClassSource 是 @JSONField(serialize=false)）
    std::vector<std::string> keys;
    for (const auto& kv : sd->objectItems()) keys.push_back(kv.first);
    std::sort(keys.begin(), keys.end());
    const std::vector<std::string> want = {"classFilterMode", "codeSet", "expressionType",
                                           "subString", "subVersion", "tagsSet", "topic"};
    expect(keys == want, "subscriptionData 只有 Java 的那 7 个键", jsonDump(body).c_str());
    expect(sd->find("filterClassSource") == nullptr, "filterClassSource 不参与序列化");
}

// 3 / 4. broker 的拒绝码原样带上响应码 —— 这是启动失败的判据
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
    expectInt(got, code, std::string(name) + " 带上 broker 响应码");
    expect(msg.find("remark") != std::string::npos, std::string(name) + " 保留 remark", msg);
}

// 5. 查不到路由 → 跳过（Java findBrokerAddrByTopic 返回 null 即 continue）
void testNoRouteSkips(MockBroker& mock) {
    netcompat::ensureInitialized();
    auto instance = std::make_unique<MQClientInstance>(
        kClientId, std::vector<std::string>{mock.address()});
    mock.scriptChecks({});
    std::vector<SubscriptionData> subs;
    subs.push_back(sql92Sub());
    instance->checkSubscriptionsInBroker(kGroup, subs);  // 缓存里没有这个 topic 的路由
    expectInt(mock.checkCount(), 0, "无路由的订阅一笔 46 都不发");
    expect(instance->findBrokerAddrByTopic(kTopic).empty(), "findBrokerAddrByTopic 只读缓存");
}

// 6. 连不上 broker 时 Java 不吞异常，而是换成一段固定文案再抛
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
    expect(threw == 1, "连不上 broker 也抛 MQClientException");
    expect(msg.find("SQL92") != std::string::npos, "文案里带上表达式类型", msg);
    expect(msg.find("server has not been upgraded to support") != std::string::npos,
           "文案与 Java 一致", msg);
    expectInt(mock.checkCount(), 0, "传输失败时 broker 没收到 46");
}

// 7. 空订阅集合什么都不发（Java 的 return 语义在本端口由调用方各自保证）
void testEmptySubs(MockBroker& mock) {
    auto instance = seeded(mock);
    mock.scriptChecks({});
    instance->checkSubscriptionsInBroker(kGroup, std::vector<SubscriptionData>());
    expectInt(mock.checkCount(), 0, "空订阅不发 46");
}

// 8. master 优先；只有 master 缺席时才随机取从节点
void testMasterPreferred(MockBroker& mock) {
    TopicRouteData route;
    std::map<int64_t, std::string> addrs;
    addrs[0] = mock.address();
    addrs[1] = deadAddress();  // 从节点：选中它就必然连不上
    route.brokerDatas.emplace_back("DefaultCluster", "broker-a", addrs);
    mock.addRoute(kTopic, route);
    auto instance = std::make_unique<MQClientInstance>(
        kClientId, std::vector<std::string>{mock.address()});
    instance->updateTopicRouteInfoFromNameServer(kTopic);
    mock.scriptChecks({});
    std::vector<SubscriptionData> subs;
    subs.push_back(sql92Sub());
    for (int i = 0; i < 5; ++i) {
        instance->checkSubscriptionsInBroker(kGroup, subs);  // 抛异常即测试失败
    }
    expectInt(mock.checkCount(), 5, "有 master 时永远只打 master");
}

// 9. CheckClientRequestBody 往返 + namespace 只在置位时参与序列化
void testBodyRoundTrip() {
    CheckClientRequestBody in;
    in.clientId = "cid";
    in.group = "gid";
    in.subscriptionData = sql92Sub("color = 'red'");
    JsonValue v = in.toJson();
    expect(v.find("namespace") == nullptr, "namespace 未置位时不序列化");
    CheckClientRequestBody out;
    expect(CheckClientRequestBody::decode(in.encode(), out), "body 可解回");
    expect(out.clientId == in.clientId && out.group == in.group, "clientId / group 往返");
    expect(out.subscriptionData.topic == kTopic
               && out.subscriptionData.subString == "color = 'red'"
               && out.subscriptionData.expressionType == std::string(ExpressionType::SQL92),
           "subscriptionData 往返");
    in.hasNamespace = true;
    in.clientNamespace = "nsA";
    CheckClientRequestBody out2;
    expect(CheckClientRequestBody::decode(in.encode(), out2) && out2.hasNamespace
               && out2.clientNamespace == "nsA",
           "namespace 置位后往返");
}

}  // namespace

int main() {
    MockBroker mock;
    testTagOnlySendsNothing(mock);
    testSql92RequestShape(mock);
    testBrokerRejectCode(mock, ResponseCode::SUBSCRIPTION_PARSE_FAILED, "SUBSCRIPTION_PARSE_FAILED");
    testBrokerRejectCode(mock, ResponseCode::SYSTEM_ERROR, "未开 enablePropertyFilter 回 SYSTEM_ERROR");
    testNoRouteSkips(mock);
    testTransportErrorWrapped(mock);
    testEmptySubs(mock);
    testMasterPreferred(mock);
    testBodyRoundTrip();
    std::printf("%d checks, %d failures\n", checks, fails);
    return fails == 0 ? 0 : 1;
}
