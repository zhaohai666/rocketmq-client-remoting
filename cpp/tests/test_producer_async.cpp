// 异步发送链路单测：本机起 mock（同时扮演 name server 与 broker），离线、确定性地锁死
// Java DefaultMQProducerImpl(ASYNC) + MQClientAPIImpl#sendMessageAsync/onExceptionImpl 的语义。
//
// 为什么必须离线：真 broker 不会稳定地"连不上""回超时""队满"，而这三种恰恰是异步重试链
// 的判据来源（Java 的分类是 `instanceof`，见 ``MQClientAPIImpl:680-693``）。
//
// 覆盖：
//   1. sendAsync 立即返回，准备工作在 AsyncSenderExecutor_N 上跑，用户回调在
//      NettyClientPublicExecutor_N 上跑（Java 的 ThreadFactoryImpl 命名 + publicExecutor）；
//   2. 用户回调抛异常必须被吞掉，不能带走线程池的 worker；
//   3. 发送前校验失败走 onException，不给调用方抛异常；
//   4. 未启动 / 已 shutdown：入口就地抛；
//   5. broker 回非 SUCCESS 响应码**不重试**（Java operationSucceed 的 catch，needRetry=false），
//      与同步发送按 retryResponseCodes 换 broker 的行为**刻意不同**，且容错表的
//      reachable 位两者也不同（异步 true / 同步 false）；
//   6. 响应超时：一次尝试就把剩余预算吃光 -> 只发一次，文案 "wait response timeout, cost=N"；
//   7. 建连失败：快、可重试 -> 按 retryTimesWhenSendAsyncFailed 重试且**换 broker**，
//      重试次数上限生效（用日志取证，见下方说明）；
//   8. 定点异步发送（带 mq）永不换 broker（Java 传下去的 topicPublishInfo 是 null）；
//   9. AsyncSenderExecutor 队满 -> MQClientException("executor rejected")。
//
// ⚠ 为什么第 7/8 条用日志而不是上线报文取证：Java 的语义决定了"能换 broker 的失败"只有
//   建连级失败（快），而它根本到不了 broker；能到 broker 的失败只有响应超时（必然吃掉整个
//   剩余预算，于是一次都不会重试）与"响应处理失败"（Java 明确不重试）。所以
//   "重试了几次、每次换没换 broker"在离线用例里只能从 warn 日志的
//   "async send msg by retry N times. ..., brokerName=..." 这一行取证 —— 那行日志本身就是
//   运维排查异步重试的唯一线索，把它一起锁死更划算。
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include "rocketmq/client/consume_executor.h"
#include "rocketmq/client/exception.h"
#include "rocketmq/client/hook.h"
#include "rocketmq/client/producer.h"
#include "rocketmq/common/byte_buffer.h"
#include "rocketmq/common/logging.h"
#include "rocketmq/common/message.h"
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

int64_t sinceMs(const std::chrono::steady_clock::time_point& from) {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::steady_clock::now() - from)
        .count();
}

bool waitFor(const std::function<bool()>& pred, int timeoutMillis) {
    const auto deadline = std::chrono::steady_clock::now()
                          + std::chrono::milliseconds(timeoutMillis);
    while (std::chrono::steady_clock::now() < deadline) {
        if (pred()) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    return pred();
}

bool startsWith(const std::string& s, const std::string& prefix) {
    return s.size() >= prefix.size() && s.compare(0, prefix.size(), prefix) == 0;
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

// 一条被占用过、随后立即释放的回环端口：连它必然被拒（比固定端口可靠）
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

// ---------------------------------------------------------------- 路由表

// 全进程共享：扮演 name server 的那个 mock 从这里查 topic 路由。
std::mutex g_routeMutex;
std::map<std::string, Bytes> g_routes;

// 一笔上线 SEND 报文的取证（opaque 是异步重试链的关键：每次重发必须换新）
struct Attempt {
    int32_t code = 0;
    int32_t opaque = 0;
    PropertyMap ext;
};

// 一条 SEND 请求的脚本化响应
struct Reply {
    int32_t code = ResponseCode::SUCCESS;
    int delayMillis = 0;
    // true = 收下请求但永不回响应：让传输层超时（Java 的 RemotingTimeoutException 分支）
    bool silence = false;
};

class MockServer {
public:
    MockServer() {
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

    ~MockServer() {
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

    void script(std::vector<Reply> replies) {
        std::lock_guard<std::mutex> lk(state_);
        replies_ = std::move(replies);
        attempts_.clear();
        sendCount_.store(0);
    }

    int sendCount() const { return sendCount_.load(); }

    std::vector<Attempt> attempts() {
        std::lock_guard<std::mutex> lk(state_);
        return attempts_;
    }

    // 每次重发都会换一个新的 opaque：把这一串取出来，好让用例断言"没有复用"
    std::vector<int32_t> opaqueList() {
        std::vector<Attempt> a = attempts();
        std::vector<int32_t> out;
        out.reserve(a.size());
        for (const Attempt& x : a) out.push_back(x.opaque);
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

    void respond(const ConnRef& conn, const RemotingCommand& req) {
        RemotingCommand resp;
        resp.opaque = req.opaque;
        resp.markResponseType();

        if (req.code == RequestCode::GET_ROUTEINFO_BY_TOPIC) {
            const std::string topic = req.extFields.count("topic") ? req.extFields.at("topic")
                                                                   : std::string();
            Bytes body;
            bool found = false;
            {
                std::lock_guard<std::mutex> lk(g_routeMutex);
                auto it = g_routes.find(topic);
                if (it != g_routes.end()) {
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
                   || req.code == RequestCode::SEND_BATCH_MESSAGE) {
            const int idx = sendCount_.fetch_add(1);
            {
                std::lock_guard<std::mutex> lk(state_);
                attempts_.push_back({req.code, req.opaque, req.extFields});
            }
            Reply reply;
            {
                std::lock_guard<std::mutex> lk(state_);
                if (idx < static_cast<int>(replies_.size())) reply = replies_[idx];
            }
            for (int i = 0; i < reply.delayMillis / 20 && running_.load(); ++i) {
                std::this_thread::sleep_for(std::chrono::milliseconds(20));
            }
            if (reply.silence) return;  // 永不回响应，逼出传输层超时
            resp.code = reply.code;
            resp.remark = "mock send";
            resp.hasRemark = true;
            if (reply.code == ResponseCode::SUCCESS) {
                resp.extFields["msgId"] = "AC10000100001234567890ABCDEF0001";
                resp.extFields["queueId"] = "0";
                resp.extFields["queueOffset"] = std::to_string(1000 + idx);
            }
        } else {
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
    std::vector<Reply> replies_;
    std::vector<Attempt> attempts_;
    std::mutex workersM_;
    std::vector<std::thread> workers_;
    std::thread acceptor_;
};

// brokers: (brokerName, brokerAddr)；地址可以各指一个 mock，也可以指"连不上"的端口
void installRoute(const std::string& topic,
                  const std::vector<std::pair<std::string, std::string>>& brokers) {
    TopicRouteData route;
    for (const auto& b : brokers) {
        route.queueDatas.emplace_back(b.first, 1, 1, 6, 0);
        std::map<int64_t, std::string> addrs;
        addrs[0] = b.second;
        route.brokerDatas.emplace_back("DefaultCluster", b.first, addrs);
    }
    std::lock_guard<std::mutex> lk(g_routeMutex);
    g_routes[topic] = route.encode();
}

Message plainMessage(const std::string& topic) {
    Message m(topic, Bytes{'h', 'i'});
    m.setTags("T1");
    return m;
}

// ---------------------------------------------------------------- 取证用回调/钩子

// 记住回调跑在哪个线程、拿到什么结果
class RecordingCallback : public SendCallback {
public:
    struct Snapshot {
        int successCount = 0;
        int exceptionCount = 0;
        SendStatus status = SendStatus::SEND_OK;
        int64_t offset = -1;
        std::string message;
        std::string threadName;
        std::thread::id threadId;
    };

    void onSuccess(const SendResult& result) override {
        std::lock_guard<std::mutex> lk(m_);
        ++successCount;
        status = result.sendStatus;
        offset = result.queueOffset;
        threadName = currentThreadName();
        threadId = std::this_thread::get_id();
        done.store(true);
    }
    void onException(const std::string& error) override {
        std::lock_guard<std::mutex> lk(m_);
        ++exceptionCount;
        message = error;
        threadName = currentThreadName();
        done.store(true);
    }

    bool waitDone(int timeoutMillis) {
        return waitFor([this] { return done.load(); }, timeoutMillis);
    }

    Snapshot snapshot() {
        std::lock_guard<std::mutex> lk(m_);
        Snapshot s;
        s.successCount = successCount;
        s.exceptionCount = exceptionCount;
        s.status = status;
        s.offset = offset;
        s.message = message;
        s.threadName = threadName;
        s.threadId = threadId;
        return s;
    }

private:
    std::mutex m_;
    int successCount = 0;
    int exceptionCount = 0;
    SendStatus status = SendStatus::SEND_OK;
    int64_t offset = -1;
    std::string message;
    std::string threadName;
    std::thread::id threadId{};
    std::atomic<bool> done{false};
};

// 只抛一次异常的用户回调（验证"抛异常不能带走 worker"）
class ThrowingCallback : public SendCallback {
public:
    void onSuccess(const SendResult&) override { throw std::runtime_error("boom in callback"); }
    void onException(const std::string&) override { throw std::runtime_error("boom in cb"); }
};

// 记录 sendMessageBefore/After 各自跑在哪个线程；可选地把 before 卡住一段时间
class ThreadNameHook : public SendMessageHook {
public:
    explicit ThreadNameHook(int32_t blockMillis = 0) : blockMillis_(blockMillis) {}
    std::string hookName() const override { return "ThreadNameHook"; }
    void sendMessageBefore(SendMessageContext&) override {
        {
            std::lock_guard<std::mutex> lk(nameM);
            beforeThread = currentThreadName();
        }
        ++beforeCount;
        if (blockMillis_ > 0) {
            std::this_thread::sleep_for(std::chrono::milliseconds(blockMillis_));
        }
    }
    void sendMessageAfter(SendMessageContext& context) override {
        {
            std::lock_guard<std::mutex> lk(nameM);
            afterThread = currentThreadName();
            afterException = context.exception;
        }
        ++afterCount;
    }

    std::string beforeName() {
        std::lock_guard<std::mutex> lk(nameM);
        return beforeThread;
    }
    std::string afterName() {
        std::lock_guard<std::mutex> lk(nameM);
        return afterThread;
    }

    std::atomic<int> beforeCount{0};
    std::atomic<int> afterCount{0};

private:
    std::string beforeThread;  // 池里有多个线程，读写都要过锁
    std::string afterThread;
    std::string afterException;
    std::mutex nameM;
    int32_t blockMillis_;
};

// ---------------------------------------------------------------- 日志取证

// 把 WARN 及以上重定向到临时文件，返回文件路径；用例从里面数异步重试的行数。
std::string captureLogFile(const std::string& tag) {
    const std::string path = (std::filesystem::temp_directory_path()
                              / ("rmq_async_test_" + tag + ".log")).string();
    std::ofstream truncate(path, std::ios::trunc);
    truncate.close();
    setLogLevel(LOG_WARN);
    setLogFile(path);
    return path;
}

std::vector<std::string> readLogLines(const std::string& path) {
    flushLogFile();
    std::ifstream in(path);
    std::vector<std::string> out;
    std::string line;
    while (std::getline(in, line)) out.push_back(line);
    return out;
}

// 异步重试的每一笔都会打一行 warn（Java ``onExceptionImpl:726``），带上第几次、
// 换到哪台 broker —— 这是离线能拿到的最直接的链路证据。
int countRetryLines(const std::string& path, std::vector<std::string>* lines = nullptr) {
    int n = 0;
    for (const std::string& line : readLogLines(path)) {
        if (line.find("async send msg by retry ") != std::string::npos) {
            ++n;
            if (lines != nullptr) lines->push_back(line);
        }
    }
    return n;
}

std::string fieldOf(const std::string& line, const std::string& key) {
    const size_t begin = line.find(key);
    if (begin == std::string::npos) return std::string();
    const size_t i = begin + key.size();
    // 值到下一个分隔符为止：',' 是字段分隔，':' 是 brokerName 后面那句错误原因的开头
    const size_t comma = line.find(',', i);
    const size_t colon = line.find(':', i);
    size_t end = comma == std::string::npos ? line.size() : comma;
    if (colon != std::string::npos && colon < end) end = colon;
    return line.substr(i, end - i);
}

}  // namespace

// ================================================================ 用例

namespace {

// 1. sendAsync 立即返回；准备工作在 AsyncSenderExecutor_N，回调在 NettyClientPublicExecutor_N
void testNonBlockingAndThreadOwnership() {
    MockServer ns;
    MockServer broker;
    const std::string topic = "AsyncBasic";
    installRoute(topic, {{"broker-a", broker.address()}});
    broker.script({{ResponseCode::SUCCESS, 500, false}});  // 500ms 后才回

    auto hook = std::make_shared<ThreadNameHook>();
    auto cb = std::make_shared<RecordingCallback>();
    DefaultMQProducer p("PG_async_basic");
    p.setNamesrvAddr(ns.address());
    p.registerSendMessageHook(hook);
    p.start();

    const auto began = std::chrono::steady_clock::now();
    p.sendAsync(plainMessage(topic), cb, 3000);
    const int64_t returnedIn = sinceMs(began);
    // 同步发送会等满 500ms；这里必须立刻返回，否则"异步"只是把阻塞挪了个地方
    expect(returnedIn < 200, "sendAsync returns without waiting for the response",
           "returnedIn=" + std::to_string(returnedIn));
    expect(waitFor([&] { return hook->beforeCount.load() >= 1; }, 2000),
           "the send runs off the caller thread");
    expect(cb->waitDone(3000), "callback fires");
    const int64_t totalMs = sinceMs(began);

    auto s = cb->snapshot();
    expectInt(s.successCount, 1, "onSuccess called once");
    expectInt(s.exceptionCount, 0, "no onException on success");
    expect(s.status == SendStatus::SEND_OK, "send status SEND_OK");
    expectInt(static_cast<long long>(s.offset), 1000, "result carries the broker's queueOffset");
    // Java ThreadFactoryImpl("AsyncSenderExecutor_") / clientCallbackExecutorThreads 池
    expect(startsWith(hook->beforeName(), "AsyncSenderExecutor_"),
           "prepare+send runs on AsyncSenderExecutor_N", hook->beforeName());
    expect(startsWith(hook->afterName(), "NettyClientPublicExecutor_"),
           "sendMessageAfter runs on NettyClientPublicExecutor_N", hook->afterName());
    expect(startsWith(s.threadName, "NettyClientPublicExecutor_"),
           "the user callback runs on the callback pool, not the connection reader thread",
           s.threadName);
    expect(s.threadId != std::this_thread::get_id(), "callback is not on the caller thread");
    expect(totalMs >= 400, "the callback still waits for the delayed response",
           "totalMs=" + std::to_string(totalMs));

    // 回调抛异常不能带走 worker：下一笔必须照常完成
    p.sendAsync(plainMessage(topic), std::make_shared<ThrowingCallback>(), 3000);
    auto cb2 = std::make_shared<RecordingCallback>();
    p.sendAsync(plainMessage(topic), cb2, 3000);
    expect(cb2->waitDone(3000), "the pool survives a throwing user callback");
    expectInt(cb2->snapshot().successCount, 1, "second send still succeeds");
    expectInt(broker.sendCount(), 3, "three sends reached the broker");
    // 每一笔请求的 opaque 必须唯一（在途表按 opaque 索引，撞号会让两次尝试的应答串台）
    std::vector<int32_t> ops = broker.opaqueList();
    expectInt(static_cast<long long>(ops.size()), 3, "one record per send");
    bool unique = ops.size() == 3 && ops[0] != ops[1] && ops[1] != ops[2] && ops[0] != ops[2];
    expect(unique, "every request carries a fresh opaque",
           "opaques=" + std::to_string(ops[0]) + "," + std::to_string(ops[1]) + ","
               + std::to_string(ops[2]));
    p.shutdown();
}

// 2. 发送前的校验失败走 onException，不给调用方抛异常（Java：准备工作在池里，异常进回调）
void testValidationFailureGoesToCallback() {
    MockServer ns;
    MockServer broker;
    const std::string topic = "AsyncTooBig";
    installRoute(topic, {{"broker-a", broker.address()}});

    auto hook = std::make_shared<ThreadNameHook>();
    auto cb = std::make_shared<RecordingCallback>();
    DefaultMQProducer p("PG_async_validate");
    p.setNamesrvAddr(ns.address());
    p.setMaxMessageSize(1024);
    p.registerSendMessageHook(hook);
    p.start();
    Message big(topic, Bytes(4096, 'x'));
    bool threw = false;
    try {
        p.sendAsync(big, cb, 3000);
    } catch (const std::exception& e) {
        threw = true;
        std::printf("  unexpected throw: %s\n", e.what());
    }
    expect(!threw, "an oversized message is not thrown at the caller");
    expect(cb->waitDone(3000), "the validation failure reaches the callback");
    auto s = cb->snapshot();
    expectInt(s.exceptionCount, 1, "onException carries the validation failure");
    expectInt(s.successCount, 0, "no onSuccess");
    expect(s.message.find("the message body size over max value, MAX: 1024")
               != std::string::npos,
           "validation text matches Validators/Java", s.message);
    expectInt(broker.sendCount(), 0, "an invalid message never reaches the broker");
    expectInt(hook->beforeCount.load(), 0, "no send hook runs for a message that failed checking");
    p.shutdown();
}

// 3. 未启动 / 已 shutdown：入口就地抛，不会静默丢消息
void testNotStartedAndShutdown() {
    DefaultMQProducer p("PG_async_lifecycle");
    p.setNamesrvAddr("127.0.0.1:9876");
    auto cb = std::make_shared<RecordingCallback>();
    bool threw = false;
    std::string what;
    try {
        p.sendAsync(plainMessage("AsyncLifecycle"), cb, 1000);
    } catch (const MQClientException& e) {
        threw = true;
        what = e.what();
    }
    expect(threw, "sendAsync before start() throws");
    expect(what.find("not started") != std::string::npos, "the message says the producer is not started", what);
    expectInt(cb->snapshot().successCount + cb->snapshot().exceptionCount, 0,
              "no callback on a synchronous rejection");

    MockServer ns;
    MockServer broker;
    installRoute("AsyncLifecycle", {{"broker-a", broker.address()}});
    DefaultMQProducer p2("PG_async_shutdown");
    p2.setNamesrvAddr(ns.address());
    p2.start();
    p2.shutdown();
    bool threw2 = false;
    std::string what2;
    try {
        p2.sendAsync(plainMessage("AsyncLifecycle"), cb, 1000);
    } catch (const MQClientException& e) {
        threw2 = true;
        what2 = e.what();
    }
    expect(threw2, "sendAsync after shutdown() throws");
    // shutdown() 先摘掉两个池、等队列排空，最后才把 started_ 置回 false。排空之后调用方
    // 看到的是"未启动"，只有排空窗口里才会看到"already shutdown" —— 两者都是就地拒绝。
    expect(what2.find("producer") != std::string::npos,
           "the rejection is a producer lifecycle error", what2);
}

// 4. broker 回非 SUCCESS 响应码：异步**不重试**（Java operationSucceed 的 catch），
//    与同步按 retryResponseCodes 换 broker 不同；容错表的 reachable 位也不同。
void testBrokerErrorCodeDoesNotRetry() {
    MockServer ns;
    MockServer broker;
    const std::string topic = "AsyncBrokerError";
    installRoute(topic, {{"broker-a", broker.address()}});
    broker.script({{ResponseCode::SYSTEM_ERROR, 0, false},
                   {ResponseCode::SYSTEM_ERROR, 0, false},
                   {ResponseCode::SYSTEM_ERROR, 0, false}});

    {
        auto cb = std::make_shared<RecordingCallback>();
        DefaultMQProducer p("PG_async_broker_error");
        p.setNamesrvAddr(ns.address());
        p.setSendLatencyFaultEnable(true);
        p.start();
        p.sendAsync(plainMessage(topic), cb, 3000);
        expect(cb->waitDone(3000), "a broker error code still calls the callback");
        auto s = cb->snapshot();
        expectInt(s.exceptionCount, 1, "a broker error code goes to onException");
        expect(s.message.find("CODE:") != std::string::npos,
               "the broker's own exception text is passed through unwrapped", s.message);
        expectInt(broker.sendCount(), 1, "an answered request is never retried on another broker");
        const FaultItem* item = p.mqFaultStrategy().latencyFaultTolerance().getFaultItem("broker-a");
        expect(item != nullptr, "the failed broker is isolated in the fault table");
        if (item != nullptr) {
            expect(!item->isAvailable(), "an async failure isolates the broker");
            // Java updateFaultItem(brokerName, cost, true, true)：响应是 broker 给的，
            // 所以链路是**通**的，只隔离不判不可达（同步路径传的是 false）。
            expect(item->isReachable(),
                   "an answered response keeps the broker reachable (Java async passes reachable=true)",
                   "reachable=" + std::to_string(item->isReachable()));
        }
        p.shutdown();
    }
    {
        // 同一份脚本走同步：SYSTEM_ERROR 在 retryResponseCodes 里，必须重试满 3 次
        broker.script({{ResponseCode::SYSTEM_ERROR, 0, false},
                       {ResponseCode::SYSTEM_ERROR, 0, false},
                       {ResponseCode::SYSTEM_ERROR, 0, false}});
        DefaultMQProducer p("PG_sync_broker_error");
        p.setNamesrvAddr(ns.address());
        p.setSendLatencyFaultEnable(true);
        p.start();
        int threw = 0;
        try {
            p.send(plainMessage(topic), 3000);
        } catch (const MQClientException& e) {
            threw = static_cast<int>(e.getResponseCode());
        }
        expectInt(threw, ResponseCode::SYSTEM_ERROR, "sync send surfaces the broker code");
        expectInt(broker.sendCount(), 3, "sync retries the same code, async does not");
        const FaultItem* item = p.mqFaultStrategy().latencyFaultTolerance().getFaultItem("broker-a");
        if (item != nullptr) {
            expect(!item->isReachable(),
                   "the sync path marks the broker unreachable",
                   "reachable=" + std::to_string(item->isReachable()));
        }
        p.shutdown();
    }
}

// 5. 响应超时：一次尝试就吃掉整个剩余预算（Java 传下去的是 timeoutMillis - cost），
//    所以超时后剩余为 0，链路停下，文案是 Java 的 "wait response timeout, cost=N"。
void testTimeoutSharesOneBudget() {
    MockServer ns;
    MockServer silent;
    MockServer live;
    const std::string topic = "AsyncTimeout";
    // 名字决定路由队列表的先后顺序（getAllMessageQueue 按 queueDatas 原序），
    // "AsyncASilent" 排第一 -> 第一次尝试必定打到永不回响应的那台。
    installRoute(topic, {{"AsyncASilent", silent.address()}, {"AsyncBLive", live.address()}});
    silent.script({{ResponseCode::SUCCESS, 0, true}, {ResponseCode::SUCCESS, 0, true},
                   {ResponseCode::SUCCESS, 0, true}});

    auto cb = std::make_shared<RecordingCallback>();
    DefaultMQProducer p("PG_async_timeout");
    p.setNamesrvAddr(ns.address());
    p.setRetryTimesWhenSendAsyncFailed(2);
    p.start();
    const auto began = std::chrono::steady_clock::now();
    p.sendAsync(plainMessage(topic), cb, 800);
    expect(cb->waitDone(5000), "a timed-out send still calls back");
    const int64_t totalMs = sinceMs(began);
    auto s = cb->snapshot();
    expectInt(s.exceptionCount, 1, "timeout goes to onException");
    expect(s.message.find("wait response timeout, cost=") != std::string::npos,
           "Java's timeout wording is preserved", s.message);
    expectInt(silent.sendCount(), 1,
              "the shared remaining budget stops the chain after one timed-out attempt");
    expectInt(live.sendCount(), 0, "no second attempt once the budget is gone");
    // 超时的判分在清理线程上（Java 的 scanResponseTable 每 1s 一轮），所以只卡下界
    expect(totalMs >= 700, "one full request timeout was waited out",
           "totalMs=" + std::to_string(totalMs));
    p.shutdown();
}

// 6. 建连失败：快、可重试 -> 按 retryTimesWhenSendAsyncFailed 重试，且每次**避开刚失败的那台**
void testConnectFailureRetriesOntoAnotherBroker() {
    MockServer ns;
    const std::string topic = "AsyncConnectRetry";
    installRoute(topic, {{"AsyncDeadA", deadAddress()}, {"AsyncDeadB", deadAddress()}});
    const std::string log = captureLogFile("connect_retry");

    auto cb = std::make_shared<RecordingCallback>();
    DefaultMQProducer p("PG_async_connect_retry");
    p.setNamesrvAddr(ns.address());
    p.setRetryTimesWhenSendAsyncFailed(2);
    p.start();
    const auto began = std::chrono::steady_clock::now();
    p.sendAsync(plainMessage(topic), cb, 8000);
    expect(cb->waitDone(5000), "a connect failure still calls back");
    const int64_t totalMs = sinceMs(began);
    auto s = cb->snapshot();
    expectInt(s.exceptionCount, 1, "the failure surfaces through onException");
    expect(s.message.find("connect failed to") != std::string::npos,
           "Java passes the raw transport error, not a wrapped one", s.message);
    // 预算 8000ms 而三次建连各在毫秒级失败：慢下来就说明它在等超时，说明重试判据写错了
    expect(totalMs < 1500, "connect-level failures retry without burning the budget",
           "totalMs=" + std::to_string(totalMs));

    std::vector<std::string> lines;
    expectInt(countRetryLines(log, &lines), 2,
              "retryTimesWhenSendAsyncFailed=2 -> exactly two retries");
    if (lines.size() == 2) {
        const int firstNo = fieldOf(lines[0], "by retry ").empty() ? 0
                             : std::stoi(fieldOf(lines[0], "by retry "));
        const int secondNo = fieldOf(lines[1], "by retry ").empty() ? 0
                              : std::stoi(fieldOf(lines[1], "by retry "));
        expectInt(firstNo, 1, "the first retry line is numbered 1");
        expectInt(secondNo, 2, "the second retry line is numbered 2");
        const std::string first = fieldOf(lines[0], "brokerName=");
        const std::string second = fieldOf(lines[1], "brokerName=");
        expect(!first.empty() && !second.empty() && first != second,
               "each retry avoids the broker that just failed", first + " / " + second);
    }
    p.shutdown();
}

// 7. 建连失败后换到的那台 broker 真的把消息发出去了：链路恢复，回调拿到 SEND_OK
void testConnectFailureRecoversOnAnotherBroker() {
    MockServer ns;
    MockServer live;
    const std::string topic = "AsyncRecover";
    installRoute(topic, {{"AsyncRecoverDead", deadAddress()},
                         {"AsyncRecoverLive", live.address()}});
    const std::string log = captureLogFile("recover");

    auto cb = std::make_shared<RecordingCallback>();
    DefaultMQProducer p("PG_async_recover");
    p.setNamesrvAddr(ns.address());
    p.setSendLatencyFaultEnable(true);
    p.setRetryTimesWhenSendAsyncFailed(2);
    p.start();
    p.sendAsync(plainMessage(topic), cb, 8000);
    expect(cb->waitDone(5000), "the retry chain recovers");
    auto s = cb->snapshot();
    expectInt(s.successCount, 1, "recovery reports onSuccess: " + s.message);
    expect(s.status == SendStatus::SEND_OK, "recovered send is SEND_OK");
    expectInt(live.sendCount(), 1, "only the live broker got the message");
    expectInt(countRetryLines(log), 1, "exactly one retry happened before recovering");
    // Java 的 operationFail/外层 catch：建连失败既隔离也不可达（同步路径同样传 false）
    const FaultItem* dead =
        p.mqFaultStrategy().latencyFaultTolerance().getFaultItem("AsyncRecoverDead");
    expect(dead != nullptr, "the unreachable broker is recorded");
    if (dead != nullptr) {
        expect(!dead->isAvailable() && !dead->isReachable(),
               "a connect failure isolates and marks the broker unreachable");
    }
    p.shutdown();
}

// 8. 定点异步发送（带 mq）永不换 broker：Java 传下去的 topicPublishInfo 是 null
void testPinnedQueueNeverSwitchesBroker() {
    MockServer ns;
    MockServer live;
    const std::string topic = "AsyncPinned";
    installRoute(topic, {{"AsyncPinnedA", deadAddress()}, {"AsyncPinnedB", live.address()}});
    const std::string log = captureLogFile("pinned");

    auto cb = std::make_shared<RecordingCallback>();
    DefaultMQProducer p("PG_async_pinned");
    p.setNamesrvAddr(ns.address());
    p.setRetryTimesWhenSendAsyncFailed(2);
    p.start();
    const MessageQueue pinned(topic, "AsyncPinnedA", 0);
    p.sendAsync(plainMessage(topic), pinned, cb, 8000);
    expect(cb->waitDone(5000), "a pinned send still calls back");
    auto s = cb->snapshot();
    expectInt(s.exceptionCount, 1, "a pinned send to an unreachable broker fails");
    expectInt(live.sendCount(), 0, "a pinned send never moves to another broker");
    std::vector<std::string> lines;
    expectInt(countRetryLines(log, &lines), 2, "the pinned chain retries on the same broker");
    for (const std::string& line : lines) {
        expect(fieldOf(line, "brokerName=") == "AsyncPinnedA",
               "every retry stays on the pinned broker", fieldOf(line, "brokerName="));
    }
    p.shutdown();
}

// 9. AsyncSenderExecutor 队满 -> MQClientException("executor rejected")（就地抛给调用方）
void testQueueFullRejects() {
    MockServer ns;
    MockServer broker;
    const std::string topic = "AsyncQueueFull";
    installRoute(topic, {{"broker-a", broker.address()}});

    int32_t cores = static_cast<int32_t>(std::thread::hardware_concurrency());
    if (cores <= 0) cores = 1;

    auto hook = std::make_shared<ThreadNameHook>(400);  // 占住每个 worker
    DefaultMQProducer p("PG_async_queue_full");
    p.setNamesrvAddr(ns.address());
    p.setAsyncSenderQueueCapacity(1);
    p.registerSendMessageHook(hook);
    p.start();

    const int32_t attempts = cores * 2 + 4;
    std::vector<std::shared_ptr<RecordingCallback>> accepted;
    std::vector<std::shared_ptr<RecordingCallback>> rejected;
    for (int32_t i = 0; i < attempts; ++i) {
        auto cb = std::make_shared<RecordingCallback>();
        try {
            p.sendAsync(plainMessage(topic), cb, 8000);
            accepted.push_back(cb);
        } catch (const MQClientException& e) {
            expect(std::string(e.what()) == "executor rejected",
                   "a full queue throws Java's rejection message", e.what());
            rejected.push_back(cb);
        }
    }
    expect(!rejected.empty(), "the bounded queue rejects instead of growing without limit",
           "rejected=" + std::to_string(rejected.size()));
    expect(static_cast<int32_t>(accepted.size()) >= cores,
           "up to corePoolSize threads plus the queue are accepted",
           "accepted=" + std::to_string(accepted.size()));
    expectInt(static_cast<long long>(accepted.size() + rejected.size()), attempts,
              "every submission is either accepted or rejected");
    int completed = 0;
    for (const std::shared_ptr<RecordingCallback>& cb : accepted) {
        if (cb->waitDone(15000)) ++completed;
    }
    expectInt(completed, static_cast<int>(accepted.size()),
              "every accepted async send eventually calls back");
    // 被拒的那批**不能**有任何回调：一次投递要么交给池，要么就地抛给调用方
    for (const std::shared_ptr<RecordingCallback>& cb : rejected) {
        auto s = cb->snapshot();
        expectInt(s.successCount + s.exceptionCount, 0, "a rejected submission calls back never");
    }
    p.shutdown();
}

// 用例里未预期的异常必须变成可读的失败，而不是把整个进程 terminate 掉
void runCase(const char* name, void (*fn)()) {
    const auto began = std::chrono::steady_clock::now();
    try {
        fn();
    } catch (const std::exception& e) {
        ++checks;
        ++fails;
        std::printf("FAIL %s threw: %s\n", name, e.what());
    } catch (...) {
        ++checks;
        ++fails;
        std::printf("FAIL %s threw: unknown error\n", name);
    }
    const int64_t ms = sinceMs(began);
    // 除超时/队满两个用例外，其余都该在几百毫秒内跑完；慢的用例要能一眼看出是谁
    if (ms > 4000) std::printf("SLOW %s: %lldms\n", name, static_cast<long long>(ms));
}

}  // namespace

int main() {
    runCase("nonBlockingAndThreadOwnership", testNonBlockingAndThreadOwnership);
    runCase("validationFailureGoesToCallback", testValidationFailureGoesToCallback);
    runCase("notStartedAndShutdown", testNotStartedAndShutdown);
    runCase("brokerErrorCodeDoesNotRetry", testBrokerErrorCodeDoesNotRetry);
    runCase("timeoutSharesOneBudget", testTimeoutSharesOneBudget);
    runCase("connectFailureRetriesOntoAnotherBroker", testConnectFailureRetriesOntoAnotherBroker);
    runCase("connectFailureRecoversOnAnotherBroker", testConnectFailureRecoversOnAnotherBroker);
    runCase("pinnedQueueNeverSwitchesBroker", testPinnedQueueNeverSwitchesBroker);
    runCase("queueFullRejects", testQueueFullRejects);

    std::printf("%s: %d checks, %d failures\n", fails == 0 ? "PASS" : "FAIL", checks, fails);
    return fails == 0 ? 0 : 1;
}
