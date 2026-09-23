// 退出注销 UNREGISTER_CLIENT(35) 的离线单测 —— 不需要集群。
//
// 为什么要这一步：35 是**只在 shutdown 那一瞬间**发一次的请求，真机只能看到「组消失了」
// 这个最终结果，看不到线上到底走了什么形状的头、到底打给了哪几台 broker。而 broker 端
// `ClientManageProcessor#unregisterClient`:213-249 的两处判据恰好都是「字段级」的：
//   * `if (group != null) producerManager.unregisterProducer(group, info)`
//   * `if (group != null) { findSubscriptionGroupConfig(group); ... }`
// 也就是说 Java 空着的那个槽位传的是 **null（整个字段不上线）**，我们若传空串，broker 会
// 拿 `""` 去查订阅组配置、再白做一轮注销。这类偏差真机不会报错，只能靠抓帧锁住。
//
// 对齐基准（Java 5.5.1，逐行读过；与 python/tests/test_producer_unregister.py、
// rust/src/client/mq_client.rs 的同一组断言对拍）：
//   * `MQClientInstance#unregisterProducer`:1198-1201 → 私有
//     `unregisterClient(producerGroup, null)`:1158-1182 —— 遍历 `brokerAddrTable` 的
//     **每个 brokerId**（主 + 从），每台一发，超时 `getMqClientApiTimeout()`（3000ms），
//     RemotingException / InterruptedException / MQBrokerException 一律吞成 log.warn。
//   * `MQClientAPIImpl#unregisterClient`:1615-1639 —— 头是
//     `UnregisterClientRequestHeader{clientID, producerGroup, consumerGroup}`，
//     注意键名是大写 ID 的 `clientID`。
//   * 心跳一侧仍只打「主优先」的那台：`getRouteOfAllBrokers`（`selectBrokerAddr`）与
//     `getAllBrokerAddrs` 的分工必须守住，这里一起锁。
//
// 覆盖：
//   1. 生产者侧头形状：只有 clientID + producerGroup，consumerGroup 整个字段不上线
//   2. 消费者侧头形状：只有 clientID + consumerGroup
//   3. 两侧都有时三个键都在
//   4. 扇出含从节点（改回只打 master 的用例必然失败），且每台各一发
//   5. 单台 broker 回 SYSTEM_ERROR：`unregisterClient` 抛、`unregisterClientAllBrokers` 吞
//   6. 心跳用的 `getRouteOfAllBrokers` 依然只返回 master 那一台
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

std::string keyText(const PropertyMap& ext) {
    std::string out = "{";
    bool first = true;
    for (const auto& kv : ext) {
        if (!first) out += ", ";
        first = false;
        out += kv.first + "=" + kv.second;
    }
    return out + "}";
}

bool readN(socket_t s, char* buf, size_t n) {
    size_t got = 0;
    while (got < n) {
        int r = static_cast<int>(::recv(s, buf + got, static_cast<int>(n - got), 0));
        if (r <= 0) return false;
        got += static_cast<size_t>(n);
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

// ---------------------------------------------------------------- mock broker

struct Record {
    int32_t code = 0;
    PropertyMap ext;
};

// 扮演 broker：按请求码回脚本化的响应码，并把每一帧的 extFields 留证。
// 只有路由请求会被特殊对待（name server 与 broker 同一套骨架，这里只服务路由 + 注销）。
class MockBroker {
public:
    explicit MockBroker(bool servesRoutes = false) : servesRoutes_(servesRoutes) {
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

    // 第 i 笔 35 回 codes[i]（越界回 SUCCESS）；取证与计数一起清零，脚本才对得上
    void scriptUnregisters(std::vector<int32_t> codes) {
        std::lock_guard<std::mutex> lk(state_);
        unregCodes_ = std::move(codes);
        records_.clear();
        unregCount_.store(0);
    }

    std::vector<Record> recordsOf(int32_t code) {
        std::lock_guard<std::mutex> lk(state_);
        std::vector<Record> out;
        for (const Record& r : records_) {
            if (r.code == code) out.push_back(r);
        }
        return out;
    }

    int32_t countOf(int32_t code) { return static_cast<int32_t>(recordsOf(code).size()); }

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
                if (servesRoutes_ && it != routes_.end()) {
                    resp.code = ResponseCode::SUCCESS;
                    resp.body = it->second;
                    resp.hasBody = true;
                } else {
                    resp.code = ResponseCode::TOPIC_NOT_EXIST;
                }
            } else if (req.code == RequestCode::UNREGISTER_CLIENT) {
                const int idx = unregCount_.fetch_add(1);
                {
                    std::lock_guard<std::mutex> lk(state_);
                    records_.push_back({req.code, req.extFields});
                }
                std::lock_guard<std::mutex> lk(state_);
                if (idx < static_cast<int>(unregCodes_.size())
                    && unregCodes_[idx] != ResponseCode::SUCCESS) {
                    resp.code = unregCodes_[idx];
                    resp.remark = "mock reject";
                    resp.hasRemark = true;
                } else {
                    resp.code = ResponseCode::SUCCESS;
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

    bool servesRoutes_ = false;
    socket_t listen_ = kInvalidSocket;
    uint16_t port_ = 0;
    std::atomic<bool> running_{false};
    std::atomic<int> unregCount_{0};
    std::mutex state_;
    std::map<std::string, Bytes> routes_;
    std::vector<int32_t> unregCodes_;
    std::vector<Record> records_;
    std::mutex workersM_;
    std::vector<std::thread> workers_;
    std::thread acceptor_;
};

// ---------------------------------------------------------------- 用例脚手架

const char* kClientId = "127.0.0.1@1234";
const char* kGroup = "PID_Unregister";
const char* kTopic = "UnregisterTopic";

// 一台 brokerName 下两个 brokerId：0 是 master、1 是 slave（Java 的 brokerAddrTable 形状）
std::unique_ptr<MQClientInstance> seeded(MockBroker& namesrv, MockBroker& master,
                                         MockBroker& slave) {
    auto instance = std::make_unique<MQClientInstance>(
        kClientId, std::vector<std::string>{namesrv.address()});
    TopicRouteData route;
    std::map<int64_t, std::string> addrs;
    addrs[0] = master.address();
    addrs[1] = slave.address();
    route.brokerDatas.emplace_back("DefaultCluster", "broker-a", addrs);
    namesrv.addRoute(kTopic, route);
    instance->updateTopicRouteInfoFromNameServer(kTopic);
    return instance;
}

// 1 / 2 / 3. 头的形状：空槽位整个不上线（Java 传的是 null，不是 ""）
void testHeaderShape() {
    MockBroker broker;
    auto instance = std::make_unique<MQClientInstance>(
        kClientId, std::vector<std::string>{broker.address()});

    broker.scriptUnregisters({});
    instance->unregisterClient(broker.address(), kClientId, kGroup, "");
    std::vector<Record> got = broker.recordsOf(RequestCode::UNREGISTER_CLIENT);
    expectInt(static_cast<long long>(got.size()), 1, "生产者注销正好一笔 35");
    if (!got.empty()) {
        PropertyMap want;
        want["clientID"] = kClientId;
        want["producerGroup"] = kGroup;
        expect(got[0].ext == want, "生产者侧只有 clientID + producerGroup", keyText(got[0].ext));
    }

    broker.scriptUnregisters({});
    instance->unregisterClient(broker.address(), kClientId, "", kGroup);
    got = broker.recordsOf(RequestCode::UNREGISTER_CLIENT);
    expectInt(static_cast<long long>(got.size()), 1, "消费者注销正好一笔 35");
    if (!got.empty()) {
        PropertyMap want;
        want["clientID"] = kClientId;
        want["consumerGroup"] = kGroup;
        expect(got[0].ext == want, "消费者侧只有 clientID + consumerGroup", keyText(got[0].ext));
    }

    broker.scriptUnregisters({});
    instance->unregisterClient(broker.address(), kClientId, kGroup, kGroup);
    got = broker.recordsOf(RequestCode::UNREGISTER_CLIENT);
    expectInt(static_cast<long long>(got.size()), 1, "两侧都有时正好一笔 35");
    if (!got.empty()) {
        std::vector<std::string> keys;
        for (const auto& kv : got[0].ext) keys.push_back(kv.first);
        std::sort(keys.begin(), keys.end());
        const std::vector<std::string> want = {"clientID", "consumerGroup", "producerGroup"};
        expect(keys == want, "三个键都在", keyText(got[0].ext));
    }

    // 纯空白也按「没这个组」处理：Java 那边是 null，broker 判的是 group != null
    broker.scriptUnregisters({});
    instance->unregisterClient(broker.address(), kClientId, "   ", kGroup);
    got = broker.recordsOf(RequestCode::UNREGISTER_CLIENT);
    expect(got.empty() || got[0].ext.count("producerGroup") == 0,
           "空白 producerGroup 不上线", got.empty() ? "" : keyText(got[0].ext));
}

// 4. 扇出含从节点：每台各一发（只用 master 的实现这里必然少一发）
void testFanOutIncludesSlaves(MockBroker& namesrv, MockBroker& master, MockBroker& slave) {
    auto instance = seeded(namesrv, master, slave);
    master.scriptUnregisters({});
    slave.scriptUnregisters({});
    instance->unregisterClientAllBrokers(kClientId, kGroup, "");
    expectInt(master.countOf(RequestCode::UNREGISTER_CLIENT), 1, "master 收到一发 35");
    expectInt(slave.countOf(RequestCode::UNREGISTER_CLIENT), 1, "slave 也收到一发 35");
    std::vector<Record> m = master.recordsOf(RequestCode::UNREGISTER_CLIENT);
    if (!m.empty()) {
        expect(m[0].ext.count("consumerGroup") == 0, "扇出的每一发都不带 consumerGroup",
               keyText(m[0].ext));
    }
}

// 5. 单台失败只吞不抛；直接调用的那一层照旧抛出带码的异常
void testFailureIsSwallowed(MockBroker& namesrv, MockBroker& master, MockBroker& slave) {
    auto instance = seeded(namesrv, master, slave);
    // master 先回 SYSTEM_ERROR、slave 回 SUCCESS：Java 的 catch 只 log.warn，
    // 剩下那台照样要注销到。
    master.scriptUnregisters({ResponseCode::SYSTEM_ERROR});
    slave.scriptUnregisters({});
    int threw = 0;
    try {
        instance->unregisterClientAllBrokers(kClientId, kGroup, "");
    } catch (const std::exception& e) {
        threw = 1;
        std::printf("  不该抛: %s\n", e.what());
    }
    expect(threw == 0, "单台 broker 报错不会把 shutdown 打断");
    expectInt(master.countOf(RequestCode::UNREGISTER_CLIENT), 1, "报错那台也照样发过");
    expectInt(slave.countOf(RequestCode::UNREGISTER_CLIENT), 1, "报错之后下一台继续发");

    // 底层单发不吞：调用方要能拿到 broker 的响应码
    master.scriptUnregisters({ResponseCode::SYSTEM_ERROR});
    int32_t code = 0;
    try {
        instance->unregisterClient(master.address(), kClientId, kGroup, "");
    } catch (const MQBrokerException& e) {
        code = e.getResponseCode();
    } catch (const std::exception& e) {
        std::printf("  异常类型不对: %s\n", e.what());
    }
    expectInt(code, ResponseCode::SYSTEM_ERROR, "unregisterClient 把非 SUCCESS 抛成 MQBrokerException");
}

// 6. 心跳那侧仍只挑 master：两个 helper 的分工不能被合并
void testHeartbeatHelperStillMasterOnly(MockBroker& namesrv, MockBroker& master,
                                        MockBroker& slave) {
    auto instance = seeded(namesrv, master, slave);
    std::vector<std::string> masterOnly = instance->getRouteOfAllBrokers();
    expectInt(static_cast<long long>(masterOnly.size()), 1, "getRouteOfAllBrokers 只给 master");
    if (!masterOnly.empty()) expect(masterOnly[0] == master.address(), "那台就是 master");
    std::vector<std::string> all = instance->getAllBrokerAddrs();
    expectInt(static_cast<long long>(all.size()), 2, "getAllBrokerAddrs 主从都给");
    expect(std::find(all.begin(), all.end(), slave.address()) != all.end(), "从节点在表里");
}

}  // namespace

int main() {
    testHeaderShape();
    {
        MockBroker namesrv(true);
        MockBroker master;
        MockBroker slave;
        testFanOutIncludesSlaves(namesrv, master, slave);
    }
    {
        MockBroker namesrv(true);
        MockBroker master;
        MockBroker slave;
        testFailureIsSwallowed(namesrv, master, slave);
    }
    {
        MockBroker namesrv(true);
        MockBroker master;
        MockBroker slave;
        testHeartbeatHelperStillMasterOnly(namesrv, master, slave);
    }
    std::printf("%d checks, %d failures\n", checks, fails);
    return fails == 0 ? 0 : 1;
}
