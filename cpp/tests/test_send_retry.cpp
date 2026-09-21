// 发送重试链路单测：本机起一个 mock（同时扮演 name server 与 broker），离线、确定性地
// 锁死 Java DefaultMQProducerImpl#sendDefaultImpl 的重试分类语义。
//
// 这些分支真机集群给不了：真 broker 不会稳定返回 FLUSH_DISK_TIMEOUT / SYSTEM_ERROR，
// 也不会刚好"路由里的 broker 地址连不上"。所以这里用脚本化的响应码驱动，
// 走的是**真实的** DefaultMQProducer::send + 真实 socket。
//
// 覆盖：
//   1. retryResponseCodes：可重试码换 broker 继续；非可重试码立即把 MQBrokerException 原样抛出；
//   2. retryAnotherBrokerWhenNotStoreOK：FLUSH_DISK_TIMEOUT 默认原样返回，打开后才换 broker；
//   3. 重试耗尽后仍有非 SEND_OK 结果 → 原样返回该结果（Java: if (sendResult != null) return）；
//   4. sendMsgMaxTimeoutPerRequest：还剩重试机会时单次请求超时被压到该值（慢 broker 必须被换掉）；
//   5. 总超时用尽 → RemotingTooMuchRequestException；
//   6. 失败原因映射 ClientErrorCode：连不上→10001，broker 码→原码，无路由→10005；
//   7. 容错表分档：broker 错误码=隔离+不可达，传输异常=隔离但可达，成功=只记延迟。
//   8. unitMode 上线：SEND_MESSAGE_V2 的单字母键 `k`。
//   9. 请求钩子上线：stream 的 `ReqT=0`、ACL 的 AccessKey/Signature，以及
//      「ReqT 必须在签名内容之内」这条顺序约束（broker 侧算法重放验签）。
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
#include "rocketmq/client/hook.h"
#include "rocketmq/client/lite_pull_consumer.h"
#include "rocketmq/client/producer.h"
#include "rocketmq/common/byte_buffer.h"
#include "rocketmq/common/mix_all.h"
#include "rocketmq/common/message.h"
#include "rocketmq/common/net_compat.h"
#include "rocketmq/remoting/exception.h"
#include "rocketmq/remoting/protocol/codes.h"
#include "rocketmq/remoting/protocol/remoting_command.h"
#include "rocketmq/remoting/protocol/route.h"
#include "rocketmq/remoting/rpchook.h"

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

// 一条被接受的连接：引用计数持有，最后放手的人负责关（否则慢应答会写到已关闭的句柄上，
// Windows 还会把句柄号复用给别的连接）。
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

// ---------------------------------------------------------------- mock 端点

// 一笔上线报文的取证：请求码（V1/V2、路由、心跳…）、extFields 和原始 body
struct WireRecord {
    int32_t code = 0;
    PropertyMap ext;
    Bytes body;
    bool hasBody = false;
};

// 上线报文取证的条数上限：lite 消费者的拉取循环会持续打，留几百条足够判断"有没有打标"
constexpr size_t kRequestLogCap = 500;

// 一条 SEND 请求的脚本化响应
struct SendReply {
    int32_t code = ResponseCode::SUCCESS;
    int delayMillis = 0;
};

// 同时扮演 name server（回路由）与 broker（回发送结果）。路由里所有 broker 都指向本端点，
// 所以"换 broker 重试"只需要数 SEND 次数即可判断。
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
        // 连接线程与应答线程都在 100ms / 脚本延迟内收手，join 前不能先关 socket
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

    // topic -> 路由；未登记的 topic 一律回 TOPIC_NOT_EXIST（与真 name server 一致）
    void addRoute(const std::string& topic, const TopicRouteData& route) {
        std::lock_guard<std::mutex> lk(state_);
        routes_[topic] = route.encode();
    }

    void scriptSend(std::vector<SendReply> replies) {
        std::lock_guard<std::mutex> lk(state_);
        sends_ = std::move(replies);
        sendLog_.clear();
        sendCount_.store(0);
    }
    int sendCount() const { return sendCount_.load(); }

    // 第 `i` 次 SEND 的取证；越界时返回空记录（调用方按"没有这一笔"处理）。
    WireRecord sendRecord(size_t i) {
        std::lock_guard<std::mutex> lk(state_);
        if (i >= sendLog_.size()) return WireRecord{};
        return sendLog_[i];
    }

    // 清空"所有上线报文"的取证，钩子用例只关心自己那一段流量。
    void clearRequests() {
        std::lock_guard<std::mutex> lk(state_);
        requestLog_.clear();
    }

    // 已记录的上线报文快照（SEND 之外的 GET_ROUTEINFO / HEARTBEAT / PULL 都在里面）。
    std::vector<WireRecord> requests() {
        std::lock_guard<std::mutex> lk(state_);
        return requestLog_;
    }

    // 请求码为 `code` 的报文里，有多少条带 key==value 的扩展字段。
    int countRequestsWith(int32_t code, const std::string& key, const std::string& value) {
        std::lock_guard<std::mutex> lk(state_);
        int n = 0;
        for (const WireRecord& r : requestLog_) {
            if (r.code != code) continue;
            auto it = r.ext.find(key);
            if (it != r.ext.end() && it->second == value) ++n;
        }
        return n;
    }

    // 请求码为 `code` 的报文总条数。
    int countRequests(int32_t code) {
        std::lock_guard<std::mutex> lk(state_);
        int n = 0;
        for (const WireRecord& r : requestLog_) {
            if (r.code == code) ++n;
        }
        return n;
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
            // 每条请求交给独立线程答复：真 broker 会并发处理，慢请求不该挡住后面的
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

    void respond(const ConnRef& conn, RemotingCommand req) {
        // 每一笔上线报文都留一份证：钩子注入的扩展字段（ReqT / AccessKey / Signature）
        // 只在客户端编码前才写进 extFields，本地断言看不到，只有这里能取证。
        {
            std::lock_guard<std::mutex> lk(state_);
            if (requestLog_.size() < kRequestLogCap) {
                requestLog_.push_back({req.code, req.extFields, req.body, req.hasBody});
            }
        }
        RemotingCommand resp;
        resp.opaque = req.opaque;
        resp.markResponseType();

        if (req.code == RequestCode::GET_ROUTEINFO_BY_TOPIC) {
            const std::string topic = req.extFields.count("topic") ? req.extFields["topic"]
                                                                   : std::string();
            Bytes body;
            bool found = false;
            {
                std::lock_guard<std::mutex> lk(state_);
                auto it = routes_.find(topic);
                if (it != routes_.end()) {
                    body = it->second;
                    found = true;
                }
            }
            resp.code = found ? ResponseCode::SUCCESS : ResponseCode::TOPIC_NOT_EXIST;
            resp.remark = found ? "route ok" : "topic not exist";
            resp.hasRemark = true;
            if (found) {
                resp.body = body;
                resp.hasBody = true;
            }
        } else if (req.code == RequestCode::SEND_MESSAGE_V2
                   || req.code == RequestCode::SEND_MESSAGE
                   || req.code == RequestCode::SEND_REPLY_MESSAGE_V2) {
            const int idx = sendCount_.fetch_add(1);
            {
                // 记录上线的请求码与 extFields：重试分类之外的字段口径（unitMode/batch）
                // 只能在这里取证。
                std::lock_guard<std::mutex> lk(state_);
                sendLog_.push_back({req.code, req.extFields, req.body, req.hasBody});
            }

            SendReply reply;
            {
                std::lock_guard<std::mutex> lk(state_);
                if (idx < static_cast<int>(sends_.size())) reply = sends_[idx];
            }
            // 分段睡：析构时能尽快收手
            for (int i = 0; i < reply.delayMillis / 50 && running_.load(); ++i) {
                std::this_thread::sleep_for(std::chrono::milliseconds(50));
            }
            resp.code = reply.code;
            resp.remark = "mock send";
            resp.hasRemark = true;
            if (reply.code == ResponseCode::SUCCESS) {
                resp.extFields["msgId"] = "AC10000100001234567890ABCDEF0001";
                resp.extFields["queueId"] = "0";
                resp.extFields["queueOffset"] = std::to_string(1000 + idx);
            }
        } else {
            // HEARTBEAT 等：一律成功；oneway 请求不回响应（broker 也是这么做的）
            if (req.isOnewayRpc()) return;
            resp.code = ResponseCode::SUCCESS;
        }
        const Bytes out = resp.encode();
        std::lock_guard<std::mutex> lk(conn->writeM);
        writeAll(conn->sock, out);
    }

    socket_t listen_ = kInvalidSocket;
    uint16_t port_ = 0;
    std::atomic<bool> running_{false};
    std::atomic<int> sendCount_{0};
    std::mutex state_;
    std::map<std::string, Bytes> routes_;
    std::vector<SendReply> sends_;
    std::vector<WireRecord> sendLog_;
    std::vector<WireRecord> requestLog_;
    std::mutex workersM_;
    std::vector<std::thread> workers_;
    std::thread acceptor_;
};

// 造一个 brokers 台 broker、每台 queuesPerBroker 个队列的路由（地址全部指向同一个端点）
TopicRouteData makeRoute(const std::string& brokerAddr, int brokers, int queuesPerBroker) {
    TopicRouteData route;
    for (int b = 0; b < brokers; ++b) {
        const std::string name = "broker-" + std::string(1, static_cast<char>('a' + b));
        route.queueDatas.emplace_back(name, queuesPerBroker, queuesPerBroker, 6, 0);
        std::map<int64_t, std::string> addrs;
        addrs[0] = brokerAddr;
        route.brokerDatas.emplace_back("DefaultCluster", name, addrs);
    }
    return route;
}

Message plainMessage(const std::string& topic) {
    Message m(topic, Bytes{'h', 'i'});
    m.setTags("T1");
    return m;
}

// ---------------------------------------------------------------- 用例

// 1. 可重试码换 broker；非可重试码立刻抛出且只发一次
void testRetryResponseCodes(MockEndpoint& mock) {
    const std::string topic = "SendRetryTopicExist";
    mock.addRoute(topic, makeRoute(mock.address(), 2, 1));

    {
        mock.scriptSend({{ResponseCode::SYSTEM_ERROR, 0}, {ResponseCode::SUCCESS, 0}});
        DefaultMQProducer p("PG_retry_ok");
        p.setNamesrvAddr(mock.address());
        p.start();
        SendResult r = p.send(plainMessage(topic), 3000);
        expectInt(static_cast<int>(r.sendStatus), static_cast<int>(SendStatus::SEND_OK),
                  "retryable code then success -> SEND_OK");
        expectInt(mock.sendCount(), 2, "retryable broker code retried once");
        expectInt(static_cast<long long>(r.queueOffset), 1001,
                  "the retried send is the one whose result comes back");
        p.shutdown();
    }
    {
        mock.scriptSend({{ResponseCode::MESSAGE_ILLEGAL, 0}, {ResponseCode::SUCCESS, 0}});
        DefaultMQProducer p("PG_retry_no");
        p.setNamesrvAddr(mock.address());
        p.start();
        int threw = 0;
        try {
            p.send(plainMessage(topic), 3000);
        } catch (const MQBrokerException& e) {
            threw = 1;
            expectInt(e.getResponseCode(), ResponseCode::MESSAGE_ILLEGAL,
                      "non-retryable code keeps the original MQBrokerException");
        } catch (const std::exception& e) {
            std::printf("  wrong exception: %s\n", e.what());
        }
        expectInt(threw, 1, "MESSAGE_ILLEGAL is not in retryResponseCodes -> throws");
        expectInt(mock.sendCount(), 1, "non-retryable code must not retry");
        p.shutdown();
    }
    {
        // addRetryResponseCode 能把非可重试码变成可重试
        mock.scriptSend({{ResponseCode::MESSAGE_ILLEGAL, 0}, {ResponseCode::SUCCESS, 0}});
        DefaultMQProducer p("PG_retry_added");
        p.setNamesrvAddr(mock.address());
        p.addRetryResponseCode(ResponseCode::MESSAGE_ILLEGAL);
        p.start();
        SendResult r = p.send(plainMessage(topic), 3000);
        expectInt(static_cast<int>(r.sendStatus), static_cast<int>(SendStatus::SEND_OK),
                  "addRetryResponseCode makes it retryable");
        expectInt(mock.sendCount(), 2, "addRetryResponseCode retried");
        p.shutdown();
    }
}

// 2/3. storeOK 重试开关
void testRetryAnotherBrokerWhenNotStoreOK(MockEndpoint& mock) {
    const std::string topic = "SendRetryStoreOk";
    mock.addRoute(topic, makeRoute(mock.address(), 2, 1));

    {
        mock.scriptSend({{ResponseCode::FLUSH_DISK_TIMEOUT, 0}, {ResponseCode::SUCCESS, 0}});
        DefaultMQProducer p("PG_storeok_off");
        p.setNamesrvAddr(mock.address());
        p.start();
        SendResult r = p.send(plainMessage(topic), 3000);
        expectInt(static_cast<int>(r.sendStatus), static_cast<int>(SendStatus::FLUSH_DISK_TIMEOUT),
                  "default: FLUSH_DISK_TIMEOUT returned as-is");
        expectInt(mock.sendCount(), 1, "default: no broker switch on not-store-OK");
        p.shutdown();
    }
    {
        mock.scriptSend({{ResponseCode::FLUSH_DISK_TIMEOUT, 0}, {ResponseCode::SUCCESS, 0}});
        DefaultMQProducer p("PG_storeok_on");
        p.setNamesrvAddr(mock.address());
        p.setRetryAnotherBrokerWhenNotStoreOK(true);
        p.start();
        SendResult r = p.send(plainMessage(topic), 3000);
        expectInt(static_cast<int>(r.sendStatus), static_cast<int>(SendStatus::SEND_OK),
                  "enabled: switch broker and get SEND_OK");
        expectInt(mock.sendCount(), 2, "enabled: retried on another broker");
        p.shutdown();
    }
    {
        // 全程都不 OK：耗尽后把最后一次结果原样返回（Java if (sendResult != null) return）
        mock.scriptSend({{ResponseCode::FLUSH_SLAVE_TIMEOUT, 0},
                         {ResponseCode::SLAVE_NOT_AVAILABLE, 0},
                         {ResponseCode::FLUSH_DISK_TIMEOUT, 0}});
        DefaultMQProducer p("PG_storeok_last");
        p.setNamesrvAddr(mock.address());
        p.setRetryAnotherBrokerWhenNotStoreOK(true);
        p.start();
        SendResult r = p.send(plainMessage(topic), 3000);
        expectInt(static_cast<int>(r.sendStatus), static_cast<int>(SendStatus::FLUSH_DISK_TIMEOUT),
                  "exhausted retries still return the last non-OK result");
        expectInt(mock.sendCount(), 3, "storeOK retry honours retryTimesWhenSendFailed");
        p.shutdown();
    }
}

// 4. sendMsgMaxTimeoutPerRequest：慢 broker 必须在被压过的单次超时处放弃
void testSendMsgMaxTimeoutPerRequest(MockEndpoint& mock) {
    const std::string topic = "SendRetryCap";
    mock.addRoute(topic, makeRoute(mock.address(), 2, 1));

    {
        // 不开 cap：第一次请求等得到 400ms 的响应，只发一次
        mock.scriptSend({{ResponseCode::SUCCESS, 400}, {ResponseCode::SUCCESS, 0}});
        DefaultMQProducer p("PG_cap_off");
        p.setNamesrvAddr(mock.address());
        p.start();
        p.send(plainMessage(topic), 3000);
        expectInt(mock.sendCount(), 1, "no cap: the slow first broker still answers");
        p.shutdown();
    }
    {
        mock.scriptSend({{ResponseCode::SUCCESS, 400}, {ResponseCode::SUCCESS, 0}});
        DefaultMQProducer p("PG_cap_on");
        p.setNamesrvAddr(mock.address());
        p.setSendMsgMaxTimeoutPerRequest(150);
        p.start();
        const auto begin = std::chrono::steady_clock::now();
        SendResult r = p.send(plainMessage(topic), 3000);
        const long long elapsedMs = static_cast<long long>(
            std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::steady_clock::now() - begin)
                .count());
        expectInt(static_cast<int>(r.sendStatus), static_cast<int>(SendStatus::SEND_OK),
                  "cap: retry still ends up SEND_OK");
        expectInt(mock.sendCount(), 2, "cap: first attempt abandoned at 150ms, then retried");
        expect(elapsedMs < 1000, "cap: total cost stays well below the 3000ms budget",
               "elapsed=" + std::to_string(elapsedMs));
        p.shutdown();
    }
    {
        // cap 比总预算还大 = 不生效（Java 只在 curTimeout > max 时才压）
        mock.scriptSend({{ResponseCode::SUCCESS, 300}, {ResponseCode::SUCCESS, 0}});
        DefaultMQProducer p("PG_cap_big");
        p.setNamesrvAddr(mock.address());
        p.setRetryTimesWhenSendFailed(1);
        p.setSendMsgMaxTimeoutPerRequest(10000);
        p.start();
        SendResult r = p.send(plainMessage(topic), 3000);
        expectInt(static_cast<int>(r.sendStatus), static_cast<int>(SendStatus::SEND_OK),
                  "cap larger than the budget is a no-op");
        expectInt(mock.sendCount(), 1, "no abandonment when the cap exceeds the timeout");
        p.shutdown();
    }
}

// 只拖慢第一次发送的钩子：Java 的 costTime 是"上一次尝试开始时"取的，钩子耗时算在
// 这一次尝试里，所以第二次尝试一进门就会发现预算已经用完。
class OneShotSlowHook : public SendMessageHook {
public:
    explicit OneShotSlowHook(int32_t sleepMillis) : sleepMillis_(sleepMillis) {}
    std::string hookName() const override { return "OneShotSlowHook"; }
    void sendMessageBefore(SendMessageContext&) override {
        if (fired_.exchange(true)) return;
        std::this_thread::sleep_for(std::chrono::milliseconds(sleepMillis_));
    }
    void sendMessageAfter(SendMessageContext&) override {}

private:
    std::atomic<bool> fired_{false};
    int32_t sleepMillis_;
};

// 5. 总超时用尽 -> RemotingTooMuchRequestException
void testCallTimeout(MockEndpoint& mock) {
    const std::string topic = "SendRetryCallTimeout";
    mock.addRoute(topic, makeRoute(mock.address(), 2, 1));
    // 第一次尝试：钩子先睡 300ms（> 整个 200ms 预算），broker 回可重试码 -> 继续循环；
    // 第二次尝试：costTime(300) > timeout(200) -> 直接放弃（callTimeout）。
    mock.scriptSend({{ResponseCode::SYSTEM_ERROR, 0}, {ResponseCode::SUCCESS, 0}});
    DefaultMQProducer p("PG_call_timeout");
    p.setNamesrvAddr(mock.address());
    p.registerSendMessageHook(std::make_shared<OneShotSlowHook>(300));
    p.start();
    const int before = mock.sendCount();
    bool tooMuch = false;
    try {
        p.send(plainMessage(topic), 200);
    } catch (const RemotingTooMuchRequestException&) {
        tooMuch = true;
    } catch (const std::exception& e) {
        std::printf("  wrong exception: %s\n", e.what());
    }
    expect(tooMuch, "budget exhausted -> RemotingTooMuchRequestException");
    expectInt(mock.sendCount() - before, 1, "callTimeout 后不再发下一个 broker");
    p.shutdown();
}

// 6. 失败原因 -> ClientErrorCode
void testErrorCodeMapping(MockEndpoint& mock) {
    const std::string deadTopic = "SendRetryDeadBroker";
    mock.addRoute(deadTopic, makeRoute(deadAddress(), 1, 1));
    {
        // Windows 上连一个刚关掉的 loopback 端口要 ~1s，三次尝试会把预算吃光，
        // 所以只跑一次并给足预算，确保看到的是"连不上"而不是"总超时"。
        DefaultMQProducer p("PG_connect");
        p.setNamesrvAddr(mock.address());
        p.setRetryTimesWhenSendFailed(0);
        p.start();
        int code = 0;
        try {
            p.send(plainMessage(deadTopic), 8000);
        } catch (const MQClientException& e) {
            code = e.getResponseCode();
        } catch (const std::exception& e) {
            std::printf("  wrong exception: %s\n", e.what());
        }
        expectInt(code, ClientErrorCode::CONNECT_BROKER_EXCEPTION,
                  "connect failure -> CONNECT_BROKER_EXCEPTION(10001)");
        p.shutdown();
    }
    {
        // 无路由：立即 NOT_FOUND_TOPIC，不把重试次数空转掉
        DefaultMQProducer p("PG_noroute");
        p.setNamesrvAddr(mock.address());
        p.start();
        const int before = mock.sendCount();
        int code = 0;
        try {
            p.send(plainMessage("SendRetryNeverRegistered"), 1000);
        } catch (const MQClientException& e) {
            code = e.getResponseCode();
        }
        expectInt(code, ClientErrorCode::NOT_FOUND_TOPIC_EXCEPTION,
                  "no route at all -> NOT_FOUND_TOPIC_EXCEPTION(10005)");
        expectInt(mock.sendCount() - before, 0, "no route -> never reaches the broker");
        p.shutdown();
    }
    {
        // broker 明确回非可重试码：原码透传
        const std::string topic = "SendRetryBrokerCode";
        mock.addRoute(topic, makeRoute(mock.address(), 1, 1));
        mock.scriptSend({{ResponseCode::NOT_LEADER_FOR_QUEUE, 0}});
        DefaultMQProducer p("PG_broker_code");
        p.setNamesrvAddr(mock.address());
        p.start();
        int code = 0;
        try {
            p.send(plainMessage(topic), 1000);
        } catch (const MQBrokerException& e) {
            code = e.getResponseCode();
        }
        expectInt(code, ResponseCode::NOT_LEADER_FOR_QUEUE,
                  "non-retryable MQBrokerException propagates its own code");
        p.shutdown();
    }
}

// 7. 容错表分档（Java updateFaultItem 的 isolation/reachable 组合）
void testFaultItemFlags(MockEndpoint& mock) {
    const std::string deadTopic = "SendFaultConnect";
    mock.addRoute(deadTopic, makeRoute(deadAddress(), 1, 1));
    {
        DefaultMQProducer p("PG_fault_connect");
        p.setNamesrvAddr(mock.address());
        p.setSendLatencyFaultEnable(true);
        p.setRetryTimesWhenSendFailed(0);
        p.start();
        try {
            p.send(plainMessage(deadTopic), 8000);
        } catch (const std::exception&) {
        }
        const FaultItem* item =
            p.mqFaultStrategy().latencyFaultTolerance().getFaultItem("broker-a");
        expect(item != nullptr, "transport failure records a fault item");
        if (item != nullptr) {
            expect(item->isReachable(),
                   "RemotingException isolates but keeps reachable (no detector thread here)");
            expect(!item->isAvailable(), "RemotingException isolates the broker");
        }
        p.shutdown();
    }
    {
        const std::string topic = "SendFaultBrokerCode";
        mock.addRoute(topic, makeRoute(mock.address(), 1, 1));
        mock.scriptSend({{ResponseCode::SERVICE_NOT_AVAILABLE, 0},
                         {ResponseCode::SERVICE_NOT_AVAILABLE, 0},
                         {ResponseCode::SERVICE_NOT_AVAILABLE, 0}});
        DefaultMQProducer p("PG_fault_broker");
        p.setNamesrvAddr(mock.address());
        p.setSendLatencyFaultEnable(true);
        p.start();
        int code = 0;
        try {
            p.send(plainMessage(topic), 3000);
        } catch (const MQClientException& e) {
            code = e.getResponseCode();
        }
        expectInt(code, ResponseCode::SERVICE_NOT_AVAILABLE,
                  "exhausted retryable broker codes surface the last broker code");
        const FaultItem* item =
            p.mqFaultStrategy().latencyFaultTolerance().getFaultItem("broker-a");
        expect(item != nullptr, "broker failure records a fault item");
        if (item != nullptr) {
            expect(!item->isReachable(), "MQBrokerException marks the broker unreachable");
            expect(!item->isAvailable(), "MQBrokerException isolates the broker");
        }
        p.shutdown();
    }
    {
        // 成功发送写实测延迟，不隔离
        const std::string topic = "SendFaultSuccess";
        mock.addRoute(topic, makeRoute(mock.address(), 1, 1));
        mock.scriptSend({{ResponseCode::SUCCESS, 0}});
        DefaultMQProducer p("PG_fault_ok");
        p.setNamesrvAddr(mock.address());
        p.setSendLatencyFaultEnable(true);
        p.start();
        p.send(plainMessage(topic), 3000);
        const FaultItem* item =
            p.mqFaultStrategy().latencyFaultTolerance().getFaultItem("broker-a");
        expect(item != nullptr && item->isAvailable() && item->isReachable(),
               "successful send records latency without isolating");
        // 回归守卫：本地往返常常不足 1ms，整数毫秒差会把延迟记成 0，
        // 于是容错表的延迟阈值永远不生效。
        if (item != nullptr) {
            expect(item->currentLatency() > 0.0,
                   "success records a strictly positive latency",
                   "latency=" + std::to_string(item->currentLatency()));
        }
        p.shutdown();
    }
}

// 8. unitMode 上线：走的是 SendMessageRequestHeaderV2 的单字母键 `k`
//（Java SendMessageRequestHeaderV2.java:62 `@CFNullable private Boolean k; // unitMode`，
// 由 DefaultMQProducerImpl:1004 `requestHeader.setUnitMode(this.isUnitMode())` 填）。
// 键名写错 broker 就读不到，单元化路由整条链路静默失效，故离线取证。
void testUnitModeReachesWire(MockEndpoint& mock) {
    const std::string topic = "SendRetryUnitMode";
    mock.addRoute(topic, makeRoute(mock.address(), 1, 1));

    {
        mock.scriptSend({{ResponseCode::SUCCESS, 0}});
        DefaultMQProducer p("PG_unit_mode_on");
        p.setNamesrvAddr(mock.address());
        p.setInstanceName("unit-mode-on");
        p.setUnitMode(true);
        p.start();
        p.send(plainMessage(topic), 3000);
        const WireRecord rec = mock.sendRecord(0);
        expectInt(rec.code, RequestCode::SEND_MESSAGE_V2, "send goes out as SEND_MESSAGE_V2");
        expect(rec.ext.count("k") == 1, "unitMode is on the wire as V2 key k");
        expect(rec.ext.count("k") > 0 && rec.ext.at("k") == "true", "unitMode=true -> k=true");
        expect(rec.ext.count("a") > 0 && rec.ext.at("a") == "PG_unit_mode_on",
               "producerGroup is V2 key a in the same header");
        p.shutdown();
    }
    {
        // 默认关闭时字段仍然存在（Java 的 Boolean 非 null ⇒ fastjson 会写出 false）
        mock.scriptSend({{ResponseCode::SUCCESS, 0}});
        DefaultMQProducer p("PG_unit_mode_off");
        p.setNamesrvAddr(mock.address());
        p.setInstanceName("unit-mode-off");
        p.start();
        p.send(plainMessage(topic), 3000);
        const WireRecord rec = mock.sendRecord(0);
        expect(rec.ext.count("k") > 0 && rec.ext.at("k") == "false", "unitMode=false -> k=false");
        p.shutdown();
    }
}

// 9. 请求钩子的**上线**取证。钩子是在报文编码前的最后一刻才改写 extFields 的，本地断言
// 看不到结果；而 lite 消费者曾经绕过 composeRequestHooks 直接注册用户钩子，让 `ReqT`
// 静默漏发（test_acl 锁的是钩子自身的组合与顺序，锁不住 facade 有没有用它）。
// 所以这里从 socket 这头数扩展字段。
void testRequestHooksReachWire(MockEndpoint& mock) {
    const std::string topic = "SendRetryReqT";
    mock.addRoute(topic, makeRoute(mock.address(), 1, 1));
    const std::string ak = "AK-wire-probe";
    const std::string sk = "SK-wire-probe";

    {
        // 生产者默认关 stream（Java DefaultMQProducer 从不置 enableStreamRequestType）：
        // 只该看到 ACL 字段
        mock.scriptSend({{ResponseCode::SUCCESS, 0}});
        mock.clearRequests();
        DefaultMQProducer p("PG_reqt_off");
        p.setNamesrvAddr(mock.address());
        p.setCredentials(ak, sk);
        p.setInstanceName("reqt-off");
        p.start();
        p.send(plainMessage(topic), 3000);
        p.shutdown();
        const WireRecord rec = mock.sendRecord(0);
        expect(rec.ext.count(SessionCredentials::ACCESS_KEY) == 1, "acl: AccessKey on the wire");
        expect(rec.ext.count(SessionCredentials::SIGNATURE) == 1, "acl: Signature on the wire");
        expect(rec.ext.count(std::string(MixAll::REQ_T)) == 0, "stream off -> no ReqT");
    }
    {
        // 打开 stream：ReqT 与 ACL 字段并存，而且**在签进去的内容里**。判据用 broker 的
        // 算法：拿上线的 extFields + body 重放一遍 HMAC-SHA1，等式成立才说明顺序是
        // Stream → ACL（Java MQClientAPIImpl:329-332 "Inject stream rpc hook first to
        // make reserve field signature"）；反了的话开鉴权的 broker 会直接验签失败。
        mock.scriptSend({{ResponseCode::SUCCESS, 0}});
        mock.clearRequests();
        DefaultMQProducer p("PG_reqt_on");
        p.setNamesrvAddr(mock.address());
        p.setCredentials(ak, sk);
        p.setEnableStreamRequestType(true);
        p.setInstanceName("reqt-on");
        p.start();
        p.send(plainMessage(topic), 3000);
        p.shutdown();
        const WireRecord rec = mock.sendRecord(0);
        // 值是 RequestType.STREAM 的 code（"0"），不是枚举名
        const auto reqt = rec.ext.find(std::string(MixAll::REQ_T));
        expect(reqt != rec.ext.end() && reqt->second == "0", "stream on -> ReqT=0 on the wire");
        const auto sig = rec.ext.find(std::string(SessionCredentials::SIGNATURE));
        expect(sig != rec.ext.end(), "stream on keeps the ACL signature");
        RemotingCommand replay;
        replay.code = rec.code;
        replay.extFields = rec.ext;
        replay.body = rec.body;
        replay.hasBody = rec.hasBody;
        const bool matches =
            sig != rec.ext.end() && AclClientRPCHook::calcSignature(sk, replay) == sig->second;
        expect(matches, "ReqT sits inside the signed content (broker-side replay matches)");
    }
    {
        // lite 消费者默认开 stream（Java DefaultLitePullConsumer:213/228 构造里置真）：
        // 路由 / 心跳这些**非 SEND** 报文也必须打标 —— 那次漏发的回归守卫。
        mock.clearRequests();
        DefaultLitePullConsumer c("PG_lite_reqt");
        c.setNamesrvAddr(mock.address());
        c.setInstanceName("lite-reqt");
        c.subscribe(topic, "*");
        c.start();
        c.shutdown();
        const int routeTotal = mock.countRequests(RequestCode::GET_ROUTEINFO_BY_TOPIC);
        const int routeTagged =
            mock.countRequestsWith(RequestCode::GET_ROUTEINFO_BY_TOPIC, MixAll::REQ_T, "0");
        expect(routeTotal > 0, "lite consumer asked the namesrv for its route",
               "routeRequests=" + std::to_string(routeTotal));
        expectInt(routeTotal, routeTagged, "every lite-consumer route request carries ReqT");
        expect(mock.countRequestsWith(RequestCode::HEART_BEAT, MixAll::REQ_T, "0") > 0,
               "lite consumer heartbeat carries ReqT");
    }
    {
        // 显式关掉 stream 的同一条路径必须一条都不带（默认值不是"硬编码开着"）
        mock.clearRequests();
        DefaultLitePullConsumer c("PG_lite_no_reqt");
        c.setNamesrvAddr(mock.address());
        c.setInstanceName("lite-no-reqt");
        c.setEnableStreamRequestType(false);
        c.subscribe(topic, "*");
        c.start();
        c.shutdown();
        expectInt(mock.countRequestsWith(RequestCode::GET_ROUTEINFO_BY_TOPIC, MixAll::REQ_T, "0"), 0,
                  "setEnableStreamRequestType(false) really turns ReqT off");
        expect(mock.countRequests(RequestCode::GET_ROUTEINFO_BY_TOPIC) > 0,
               "the consumer did talk to the namesrv, so that 0 means something");
    }
}

}  // namespace

namespace {

// 用例里未预期的异常必须变成可读的失败，而不是把整个进程 terminate 掉
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
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - begin)
            .count());
    // 用例都是本机 mock，正常在几百毫秒内；超过 2s 说明有请求在等超时，必须能一眼看出是谁
    if (ms > 2000) std::printf("SLOW %s: %lldms\n", name, ms);
}

}  // namespace

int main() {
    MockEndpoint mock;

    runCase("retryResponseCodes", mock, testRetryResponseCodes);
    runCase("retryAnotherBrokerWhenNotStoreOK", mock, testRetryAnotherBrokerWhenNotStoreOK);
    runCase("sendMsgMaxTimeoutPerRequest", mock, testSendMsgMaxTimeoutPerRequest);
    runCase("callTimeout", mock, testCallTimeout);
    runCase("errorCodeMapping", mock, testErrorCodeMapping);
    runCase("faultItemFlags", mock, testFaultItemFlags);
    runCase("unitModeReachesWire", mock, testUnitModeReachesWire);
    runCase("requestHooksReachWire", mock, testRequestHooksReachWire);

    std::printf("%s: %d checks, %d failures\n", fails == 0 ? "PASS" : "FAIL", checks, fails);
    return fails == 0 ? 0 : 1;
}
