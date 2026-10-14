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
#include <set>
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
#include "rocketmq/common/message_const.h"
#include "rocketmq/common/message_decoder.h"
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
    if (!readN(s, reinterpret_cast<char*>(body.data()), static_cast<size_t>(totalLen))) {
        return false;
    }
    out = lenStr + body;
    return true;
}

// A loopback port that was once occupied and immediately released: connecting to it is guaranteed to be refused (more reliable than a fixed port)
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

// ---------------------------------------------------------------- route table

// Shared process-wide: the mock playing the name server looks up topic routes here.
std::mutex g_routeMutex;
std::map<std::string, Bytes> g_routes;

// Forensics of one on-the-wire SEND request (opaque is key to the async retry chain: every resend must use a fresh one)
struct Attempt {
    int32_t code = 0;
    int32_t opaque = 0;
    PropertyMap ext;
    // The body on the wire: sub-message IDs of batched messages can only be evidenced from the encoded payload
    Bytes body;
};

// A scripted response for one SEND request
struct Reply {
    int32_t code = ResponseCode::SUCCESS;
    int delayMillis = 0;
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

    // Each resend swaps in a fresh opaque: take the sequence out so the case can assert"no reuse"
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
                attempts_.push_back({req.code, req.opaque, req.extFields, req.body});
            }
            Reply reply;
            {
                std::lock_guard<std::mutex> lk(state_);
                if (idx < static_cast<int>(replies_.size())) reply = replies_[idx];
            }
            for (int i = 0; i < reply.delayMillis / 20 && running_.load(); ++i) {
                std::this_thread::sleep_for(std::chrono::milliseconds(20));
            }
            if (reply.silence) return;  // never answers, forcing the transport-layer timeout
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

// brokers: (brokerName, brokerAddr); addresses may each point to a mock or to a"unconnectable" port
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

// ---------------------------------------------------------------- forensics callbacks / hooks

// Remembers which thread the callback ran on and what result it got
class RecordingCallback : public SendCallback {
public:
    struct Snapshot {
        int successCount = 0;
        int exceptionCount = 0;
        SendStatus status = SendStatus::SEND_OK;
        int64_t offset = -1;
        std::string message;
        std::string errorType;
        int32_t errorCode = 0;  // meaningful only for MQBrokerException
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
    void onException(const std::exception_ptr& error) override {
        std::lock_guard<std::mutex> lk(m_);
        ++exceptionCount;
        message = exceptionMessage(error);
        errorType = exceptionTypeName(error);
        try {
            std::rethrow_exception(error);
        } catch (const MQBrokerException& e) {
            errorCode = e.responseCode;
        } catch (...) {
            errorCode = 0;
        }
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
        s.errorType = errorType;
        s.errorCode = errorCode;
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
    std::string errorType;
    int32_t errorCode = 0;
    std::string threadName;
    std::thread::id threadId{};
    std::atomic<bool> done{false};
};

// A user callback that throws once per delivery (verifies"a throw must not take the worker down")
class ThrowingCallback : public SendCallback {
public:
    void onSuccess(const SendResult&) override { throw std::runtime_error("boom in callback"); }
    void onException(const std::exception_ptr&) override {
        throw std::runtime_error("boom in cb");
    }
};

// Records which thread sendMessageBefore/After each run on; optionally blocks before for a while
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
    std::string beforeThread;  // the pool has several threads, reads and writes both go through the lock
    std::string afterThread;
    std::string afterException;
    std::mutex nameM;
    int32_t blockMillis_;
};

// ---------------------------------------------------------------- log forensics

// Redirect WARN and above to a temp file and return its path; the case counts async-retry lines in it.
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
    // the value ends at the next separator: ',' separates fields, ':' starts the error reason that follows brokerName
    const size_t comma = line.find(',', i);
    const size_t colon = line.find(':', i);
    size_t end = comma == std::string::npos ? line.size() : comma;
    if (colon != std::string::npos && colon < end) end = colon;
    return line.substr(i, end - i);
}

}  // namespace

// ================================================================ test cases

namespace {

// 1. sendAsync returns immediately; preparation runs on AsyncSenderExecutor_N, the callback on NettyClientPublicExecutor_N
void testNonBlockingAndThreadOwnership() {
    MockServer ns;
    MockServer broker;
    const std::string topic = "AsyncBasic";
    installRoute(topic, {{"broker-a", broker.address()}});
    broker.script({{ResponseCode::SUCCESS, 500, false}});  // replies only after 500ms

    auto hook = std::make_shared<ThreadNameHook>();
    auto cb = std::make_shared<RecordingCallback>();
    DefaultMQProducer p("PG_async_basic");
    p.setNamesrvAddr(ns.address());
    p.registerSendMessageHook(hook);
    p.start();

    const auto began = std::chrono::steady_clock::now();
    p.sendAsync(plainMessage(topic), cb, 3000);
    const int64_t returnedIn = sinceMs(began);
    // a sync send would wait out the full 500ms; this must return at once, otherwise"async" just moves the blocking elsewhere
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

    // a throwing callback must not take the worker down: the next send still completes normally
    p.sendAsync(plainMessage(topic), std::make_shared<ThrowingCallback>(), 3000);
    auto cb2 = std::make_shared<RecordingCallback>();
    p.sendAsync(plainMessage(topic), cb2, 3000);
    expect(cb2->waitDone(3000), "the pool survives a throwing user callback");
    expectInt(cb2->snapshot().successCount, 1, "second send still succeeds");
    expectInt(broker.sendCount(), 3, "three sends reached the broker");
    // every request's opaque must be unique (the in-flight table is indexed by opaque; a collision cross-talks the two attempts' responses)
    std::vector<int32_t> ops = broker.opaqueList();
    expectInt(static_cast<long long>(ops.size()), 3, "one record per send");
    bool unique = ops.size() == 3 && ops[0] != ops[1] && ops[1] != ops[2] && ops[0] != ops[2];
    expect(unique, "every request carries a fresh opaque",
           "opaques=" + std::to_string(ops[0]) + "," + std::to_string(ops[1]) + ","
               + std::to_string(ops[2]));
    p.shutdown();
}

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
           "validation text matches the Validators contract", s.message);
    expectInt(broker.sendCount(), 0, "an invalid message never reaches the broker");
    expectInt(hook->beforeCount.load(), 0, "no send hook runs for a message that failed checking");
    p.shutdown();
}

// 3. not started / already shut down: throws at the entry synchronously, never silently drops a message
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
    // shutdown() first detaches the two pools, waits for the queues to drain, and only then sets started_ back to false.
    // After draining the caller sees"not started"; only inside the drain window would it see"already shutdown" — both are synchronous refusals.
    expect(what2.find("producer") != std::string::npos,
           "the rejection is a producer lifecycle error", what2);
}

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
        expect(s.errorType == "MQBrokerException",
               "the callback receives an MQBrokerException, not a generic client error",
               "type=" + s.errorType);
        expectInt(s.errorCode, static_cast<int>(ResponseCode::SYSTEM_ERROR),
                  "the broker's response code rides along on the exception");
        expectInt(broker.sendCount(), 1, "an answered request is never retried on another broker");
        const FaultItem* item = p.mqFaultStrategy().latencyFaultTolerance().getFaultItem("broker-a");
        expect(item != nullptr, "the failed broker is isolated in the fault table");
        if (item != nullptr) {
            expect(!item->isAvailable(), "an async failure isolates the broker");
            expect(item->isReachable(),
                   "an answered response keeps the broker reachable (async path passes reachable=true)",
                   "reachable=" + std::to_string(item->isReachable()));
        }
        p.shutdown();
    }
    {
        // the same script run synchronously: SYSTEM_ERROR is in retryResponseCodes, so it must retry the full 3 times
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

void testTimeoutSharesOneBudget() {
    MockServer ns;
    MockServer silent;
    MockServer live;
    const std::string topic = "AsyncTimeout";
    // names decide the order of the route queue list (getAllMessageQueue keeps queueDatas order),
    // "AsyncASilent" sorts first -> the first attempt always hits the server that never replies.
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
           "the timeout wording is preserved verbatim", s.message);
    expect(s.errorType == "MQClientException",
           "the transport timeout reaches the callback as the client wrapper type",
           "type=" + s.errorType);
    expectInt(silent.sendCount(), 1,
              "the shared remaining budget stops the chain after one timed-out attempt");
    expectInt(live.sendCount(), 0, "no second attempt once the budget is gone");
    expect(totalMs >= 700, "one full request timeout was waited out",
           "totalMs=" + std::to_string(totalMs));
    p.shutdown();
}

// 6. connect failure: fast, retryable -> retries per retryTimesWhenSendAsyncFailed, each time **avoiding the broker that just failed**
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
    // localhost loopback refusals of a"just-closed port" can be stretched to ~2s (plus 1s-granularity connection polling, ~3s per attempt):
    // the default 8s budget cannot cover 3 retries and would be judged callTimeout. Widened so the retry chain runs to its natural end.
    p.sendAsync(plainMessage(topic), cb, 30000);
    expect(cb->waitDone(40000), "a connect failure still calls back");
    const int64_t totalMs = sinceMs(began);
    auto s = cb->snapshot();
    expectInt(s.exceptionCount, 1, "the failure surfaces through onException");
    expect(s.message.find("connect failed to") != std::string::npos,
           "the raw transport error reaches the callback, not a wrapped one", s.message);
    // the chain ends naturally after three connect failures (the connect failed to above rules out a callTimeout cutoff):
    // totalMs is millisecond-level on machines where refusal is millisecond-level, and ~9s on machines with ~2s refusals
    expect(totalMs < 30000, "connect-level failures retry without burning the budget",
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

// 7. after a connect failure the switched-to broker really sends the message: the chain recovers, the callback gets SEND_OK
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
    const FaultItem* dead =
        p.mqFaultStrategy().latencyFaultTolerance().getFaultItem("AsyncRecoverDead");
    expect(dead != nullptr, "the unreachable broker is recorded");
    if (dead != nullptr) {
        expect(!dead->isAvailable() && !dead->isReachable(),
               "a connect failure isolates and marks the broker unreachable");
    }
    p.shutdown();
}

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
    // same as testConnectFailureRetriesOntoAnotherBroker: ~2s connect refusals need a 30s budget
    p.sendAsync(plainMessage(topic), pinned, cb, 30000);
    expect(cb->waitDone(40000), "a pinned send still calls back");
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

// 9. AsyncSenderExecutor queue full -> MQClientException("executor rejected") (thrown at the caller synchronously)
void testQueueFullRejects() {
    MockServer ns;
    MockServer broker;
    const std::string topic = "AsyncQueueFull";
    installRoute(topic, {{"broker-a", broker.address()}});

    int32_t cores = static_cast<int32_t>(std::thread::hardware_concurrency());
    if (cores <= 0) cores = 1;

    auto hook = std::make_shared<ThreadNameHook>(400);  // occupy every worker
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
                   "a full queue throws the expected rejection message", e.what());
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
    // the rejected batch must **not** get any callback: one submission either goes to the pool or throws at the caller on the spot
    for (const std::shared_ptr<RecordingCallback>& cb : rejected) {
        auto s = cb->snapshot();
        expectInt(s.successCount + s.exceptionCount, 0, "a rejected submission calls back never");
    }
    p.shutdown();
}

// ================================================================ async-send backpressure
//
//
// Holding an in-flight request needs no hook: the mock's ``delayMillis`` makes the broker answer late, so the permit is not returned before the callback.

namespace {

// one mock name server + one mock broker; ``delayMillis`` makes the broker answer late,
// so this in-flight request (and the permits it borrowed) stays held until the response arrives.
struct AsyncFixture {
    MockServer ns;
    MockServer broker;
    std::string topic;

    AsyncFixture(const std::string& name, int32_t delayMillis = 0, int replies = 20)
        : topic(name) {
        installRoute(topic, {{topic + "-broker", broker.address()}});
        std::vector<Reply> scripted;
        for (int i = 0; i < replies; ++i) {
            scripted.push_back({ResponseCode::SUCCESS, delayMillis, false});
        }
        broker.script(scripted);
    }
};

}  // namespace

void testBackPressureDefaultsAndGateOff() {
    AsyncFixture fx("BpDefaults");
    DefaultMQProducer p("PG_bp_defaults");
    p.setNamesrvAddr(fx.ns.address());
    p.start();
    expectInt(p.getBackPressureForAsyncSendNum(), 1024, "backPressureForAsyncSendNum default");
    expectInt(p.getBackPressureForAsyncSendSize(), 100LL * 1024 * 1024,
             "backPressureForAsyncSendSize default");
    expect(!p.isEnableBackpressureForAsyncMode(), "the switch is off by default");
    expectInt(p.getSemaphoreAsyncSendNumAvailablePermits(), 1024, "num semaphore starts full");
    expectInt(p.getSemaphoreAsyncSendSizeAvailablePermits(), 100LL * 1024 * 1024,
             "size semaphore starts full");

    auto cb = std::make_shared<RecordingCallback>();
    p.sendAsync(plainMessage(fx.topic), cb, 3000);
    expect(cb->waitDone(3000), "a send with the gate off still succeeds");
    expectInt(cb->snapshot().successCount, 1, "onSuccess on the off path");
    // with the switch off even tryAcquire must not be called — otherwise one toggle permanently eats capacity
    expectInt(p.getSemaphoreAsyncSendNumAvailablePermits(), 1024, "off path moves no num permit");
    expectInt(p.getSemaphoreAsyncSendSizeAvailablePermits(), 100LL * 1024 * 1024,
             "off path moves no size permit");
    p.shutdown();
}

void testBackPressureFloors() {
    AsyncFixture fx("BpFloors");
    DefaultMQProducer p("PG_bp_floors");
    p.setNamesrvAddr(fx.ns.address());
    p.setBackPressureForAsyncSendNum(1);
    p.setBackPressureForAsyncSendSize(1);
    expectInt(p.getBackPressureForAsyncSendNum(), 10, "the num config is floored at 10");
    expectInt(p.getBackPressureForAsyncSendSize(), 1024 * 1024,
             "the size config is floored at 1 MiB");
    expectInt(p.getSemaphoreAsyncSendNumAvailablePermits(), 10, "the semaphore honours the floor");
    expectInt(p.getSemaphoreAsyncSendSizeAvailablePermits(), 1024 * 1024,
             "the size semaphore honours the floor");
    p.setBackPressureForAsyncSendNum(11);
    expectInt(p.getSemaphoreAsyncSendNumAvailablePermits(), 11, "11 > 10 is taken literally");
}

// 12. num gate saturated: the 11th send calls back on the **caller thread** after waiting out its budget, and not one request was sent
void testNumGateRejectsOnCallerThread() {
    AsyncFixture fx("BpNumGate", 600 /* mock replies 600ms late, holding the request in flight */);
    DefaultMQProducer p("PG_bp_num_gate");
    p.setNamesrvAddr(fx.ns.address());
    p.setEnableBackpressureForAsyncMode(true);
    p.setBackPressureForAsyncSendNum(static_cast<int32_t>(kMinAsyncSendNum));
    p.start();

    std::vector<std::shared_ptr<RecordingCallback>> inFlight;
    for (int32_t i = 0; i < kMinAsyncSendNum; ++i) {
        auto cb = std::make_shared<RecordingCallback>();
        p.sendAsync(plainMessage(fx.topic), cb, 8000);
        inFlight.push_back(cb);
    }
    expect(waitFor([&] { return p.getSemaphoreAsyncSendNumAvailablePermits() == 0; }, 3000),
           "ten in-flight sends occupy the whole num gate",
           "available=" + std::to_string(p.getSemaphoreAsyncSendNumAvailablePermits()));

    auto rejected = std::make_shared<RecordingCallback>();
    p.sendAsync(plainMessage(fx.topic), rejected, 150);
    auto s = rejected->snapshot();
    expectInt(s.exceptionCount, 1, "the over-budget send calls back an error");
    expect(s.message == "send message tryAcquire semaphoreAsyncNum timeout",
           "the num gate text matches word for word", s.message);
    expect(s.errorType == "RemotingTooMuchRequestException",
           "the gate rejection keeps the expected type (callers branch on it)",
           "type=" + s.errorType);
    expect(!startsWith(s.threadName, "AsyncSenderExecutor_")
               && !startsWith(s.threadName, "NettyClientPublicExecutor_"),
           "the rejection is reported on the caller thread, not on a pool", s.threadName);
    // a gated send never even queried the route: the broker-side counter is the hardest evidence
    expectInt(fx.broker.sendCount(), kMinAsyncSendNum,
              "a gated request never reaches the broker");

    for (const std::shared_ptr<RecordingCallback>& cb : inFlight) {
        expect(cb->waitDone(10000), "the in-flight sends complete");
    }
    expect(waitFor([&] {
               return p.getSemaphoreAsyncSendNumAvailablePermits() == kMinAsyncSendNum;
           }, 3000),
           "every in-flight send gives its permit back",
           "available=" + std::to_string(p.getSemaphoreAsyncSendNumAvailablePermits()));
    expectInt(static_cast<long long>(rejected->snapshot().successCount), 0,
              "a gated request never also reports success");
    p.shutdown();
}

// 13. size gate: the second big message is blocked, but **the num permit already taken must be given back**
void testSizeGateRejectsAndGivesTheNumPermitBack() {
    AsyncFixture fx("BpSizeGate", 600);
    DefaultMQProducer p("PG_bp_size_gate");
    p.setNamesrvAddr(fx.ns.address());
    p.setEnableBackpressureForAsyncMode(true);
    p.setBackPressureForAsyncSendNum(static_cast<int32_t>(kMinAsyncSendNum));
    p.setBackPressureForAsyncSendSize(static_cast<int32_t>(kMinAsyncSendSize));
    p.start();

    Message big(fx.topic, Bytes(600 * 1024, '4'));
    auto first = std::make_shared<RecordingCallback>();
    p.sendAsync(big, first, 8000);
    const int64_t borrowed = kMinAsyncSendSize - static_cast<int64_t>(600 * 1024);
    expect(waitFor([&] { return p.getSemaphoreAsyncSendSizeAvailablePermits() == borrowed; }, 3000),
           "the size gate charges exactly the body length",
           "available=" + std::to_string(p.getSemaphoreAsyncSendSizeAvailablePermits()));

    auto second = std::make_shared<RecordingCallback>();
    p.sendAsync(big, second, 150);
    auto s = second->snapshot();
    expectInt(s.exceptionCount, 1, "the second big send is gated");
    expect(s.message == "send message tryAcquire semaphoreAsyncSize timeout",
           "the size gate text matches word for word", s.message);
    expect(s.errorType == "RemotingTooMuchRequestException",
           "the size gate keeps the expected type too", "type=" + s.errorType);
    expectInt(p.getSemaphoreAsyncSendNumAvailablePermits(), kMinAsyncSendNum - 1,
             "the in-flight send still holds exactly one num permit");
    expect(first->waitDone(10000), "the first big send still completes");
    expectInt(fx.broker.sendCount(), 1, "only the first big send reached the broker");
    expect(waitFor([&] { return p.getSemaphoreAsyncSendSizeAvailablePermits() == kMinAsyncSendSize; },
                   3000),
           "the size permit is back after the send settles");
    expectInt(p.getSemaphoreAsyncSendNumAvailablePermits(), kMinAsyncSendNum,
             "the num permit is back too");
    p.shutdown();
}

// 14. one in-flight send debits both gates: num -1, size -body length (body is the original uncompressed length)
void testInFlightSendBorrowsBothPermits() {
    AsyncFixture fx("BpBorrow", 600);
    DefaultMQProducer p("PG_bp_borrow");
    p.setNamesrvAddr(fx.ns.address());
    p.setEnableBackpressureForAsyncMode(true);
    p.start();
    const Message msg = plainMessage(fx.topic);  // body "hi" = 2 bytes
    auto cb = std::make_shared<RecordingCallback>();
    p.sendAsync(msg, cb, 8000);
    expect(waitFor([&] { return p.getSemaphoreAsyncSendNumAvailablePermits() == 1023; }, 3000),
           "an in-flight send borrows one num permit",
           "available=" + std::to_string(p.getSemaphoreAsyncSendNumAvailablePermits()));
    const int64_t expected = 100LL * 1024 * 1024 - static_cast<int64_t>(msg.getBody().size());
    expect(waitFor([&] { return p.getSemaphoreAsyncSendSizeAvailablePermits() == expected; }, 3000),
           "and body.length size permits",
           "available=" + std::to_string(p.getSemaphoreAsyncSendSizeAvailablePermits()));
    expect(cb->waitDone(10000), "the send completes");
    expectInt(cb->snapshot().successCount, 1, "SEND_OK on the gated path");
    expectInt(p.getSemaphoreAsyncSendNumAvailablePermits(), 1024, "num permit returned");
    expectInt(p.getSemaphoreAsyncSendSizeAvailablePermits(), 100LL * 1024 * 1024,
             "size permit returned");
    p.shutdown();
}

void testRuntimeResizeWakesABlockedSender() {
    AsyncFixture fx("BpResize", 2000 /* saturate the 10 permits, leaving an observation window */);
    DefaultMQProducer p("PG_bp_resize");
    p.setNamesrvAddr(fx.ns.address());
    p.setEnableBackpressureForAsyncMode(true);
    p.setBackPressureForAsyncSendNum(static_cast<int32_t>(kMinAsyncSendNum));
    p.start();

    std::vector<std::shared_ptr<RecordingCallback>> inFlight;
    for (int32_t i = 0; i < kMinAsyncSendNum; ++i) {
        auto cb = std::make_shared<RecordingCallback>();
        p.sendAsync(plainMessage(fx.topic), cb, 8000);
        inFlight.push_back(cb);
    }
    expect(waitFor([&] { return p.getSemaphoreAsyncSendNumAvailablePermits() == 0; }, 3000),
           "the gate is saturated");

    auto woke = std::make_shared<RecordingCallback>();
    std::thread blocked([&] { p.sendAsync(plainMessage(fx.topic), woke, 8000); });
    std::this_thread::sleep_for(std::chrono::milliseconds(300));
    expect(blocked.joinable(), "the 11th sender is stuck on the gate", "it already returned");
    p.setBackPressureForAsyncSendNum(static_cast<int32_t>(kMinAsyncSendNum) + 2);
    blocked.join();
    expect(woke->waitDone(10000), "the resize wakes the blocked sender and it sends");
    expectInt(woke->snapshot().successCount, 1, "the woken send reports SEND_OK");
    for (const std::shared_ptr<RecordingCallback>& cb : inFlight) {
        expect(cb->waitDone(15000), "the saturated sends settle");
    }
    expect(waitFor([&] {
               return p.getSemaphoreAsyncSendNumAvailablePermits() == kMinAsyncSendNum + 2;
           }, 5000),
           "all permits are back at the **new** capacity",
           "available=" + std::to_string(p.getSemaphoreAsyncSendNumAvailablePermits()));
    expectInt(fx.broker.sendCount(), kMinAsyncSendNum + 1, "every accepted send reached the broker");
    p.shutdown();
}

void testQueueFullRunsWithBackPressureOn() {
    AsyncFixture fx("BpQueueFull");
    int32_t cores = static_cast<int32_t>(std::thread::hardware_concurrency());
    if (cores <= 0) cores = 1;
    auto hook = std::make_shared<ThreadNameHook>(400);  // occupy every worker
    DefaultMQProducer p("PG_bp_queue_full");
    p.setNamesrvAddr(fx.ns.address());
    p.setAsyncSenderQueueCapacity(1);
    p.setEnableBackpressureForAsyncMode(true);
    p.registerSendMessageHook(hook);
    p.start();

    const int32_t attempts = cores * 2 + 4;
    std::vector<std::shared_ptr<RecordingCallback>> cbs;
    int threw = 0;
    for (int32_t i = 0; i < attempts; ++i) {
        auto cb = std::make_shared<RecordingCallback>();
        cbs.push_back(cb);
        try {
            p.sendAsync(plainMessage(fx.topic), cb, 8000);
        } catch (const MQClientException& e) {
            ++threw;
            std::printf("unexpected throw %s\n", e.what());
        }
    }
    // the same config throws executor rejected when the switch is off (see testQueueFullRejects)
    expectInt(threw, 0, "with back-pressure on a full queue is absorbed, not thrown");
    int completed = 0;
    for (const std::shared_ptr<RecordingCallback>& cb : cbs) {
        if (cb->waitDone(20000)) ++completed;
    }
    expectInt(completed, attempts, "every send calls back, inline ones included");
    expectInt(fx.broker.sendCount(), attempts, "and every send actually reached the broker");
    expectInt(p.getSemaphoreAsyncSendNumAvailablePermits(), 1024,
             "the inline runs gave their permits back");
    expectInt(p.getSemaphoreAsyncSendSizeAvailablePermits(), 100LL * 1024 * 1024,
             "including the size permits");
    p.shutdown();
}

// 17. failures and retry chains each return the permit exactly once: a broker error or a connect-failure broker switch leaves no leak
void testFailedAndRetriedSendsReleasePermits() {
    {
        MockServer ns;
        MockServer broker;
        const std::string topic = "BpReleaseError";
        installRoute(topic, {{topic + "-broker", broker.address()}});
        broker.script({{ResponseCode::SYSTEM_ERROR, 0, false}});
        DefaultMQProducer p("PG_bp_release_error");
        p.setNamesrvAddr(ns.address());
        p.setEnableBackpressureForAsyncMode(true);
        p.start();
        auto cb = std::make_shared<RecordingCallback>();
        p.sendAsync(plainMessage(topic), cb, 3000);
        expect(cb->waitDone(3000), "a broker error calls back");
        expectInt(cb->snapshot().exceptionCount, 1, "and it is an error");
        expectInt(p.getSemaphoreAsyncSendNumAvailablePermits(), 1024,
                 "a failed send still gives its num permit back");
        expectInt(p.getSemaphoreAsyncSendSizeAvailablePermits(), 100LL * 1024 * 1024,
                 "and its size permits");
        p.shutdown();
    }
    {
        // a connect failure retries onto other brokers per retryTimesWhenSendAsyncFailed: the whole chain borrows one permit
        MockServer ns;
        MockServer live;
        const std::string topic = "BpReleaseRetry";
        installRoute(topic, {{topic + "-dead", deadAddress()}, {topic + "-live", live.address()}});
        DefaultMQProducer p("PG_bp_release_retry");
        p.setNamesrvAddr(ns.address());
        p.setEnableBackpressureForAsyncMode(true);
        p.setRetryTimesWhenSendAsyncFailed(2);
        p.start();
        auto cb = std::make_shared<RecordingCallback>();
        p.sendAsync(plainMessage(topic), cb, 8000);
        expect(cb->waitDone(8000), "the retry chain recovers on the live broker");
        expectInt(cb->snapshot().successCount, 1, "recovery reports onSuccess");
        expectInt(p.getSemaphoreAsyncSendNumAvailablePermits(), 1024,
                 "three attempts borrow one num permit, not three");
        expectInt(p.getSemaphoreAsyncSendSizeAvailablePermits(), 100LL * 1024 * 1024,
                 "and one message's worth of size permits");
        p.shutdown();
    }
}

void testBatchAsyncOneRequestOneCallback() {
    AsyncFixture fx("BatchAsync");
    auto hook = std::make_shared<ThreadNameHook>();
    DefaultMQProducer p("PG_batch_async");
    p.setNamesrvAddr(fx.ns.address());
    p.setMaxMessageSize(1024);
    p.registerSendMessageHook(hook);
    p.start();

    std::vector<Message> msgs;
    for (int i = 0; i < 3; ++i) {
        msgs.push_back(plainMessage(fx.topic));
    }
    auto cb = std::make_shared<RecordingCallback>();
    p.sendBatchAsync(msgs, cb, 3000);
    expect(cb->waitDone(3000), "a batch async send calls the callback");
    const RecordingCallback::Snapshot s = cb->snapshot();
    expectInt(s.successCount, 1, "a good batch delivers exactly one onSuccess");
    expectInt(s.exceptionCount, 0, "a good batch never reports onException");
    expectInt(fx.broker.sendCount(), 1, "the whole batch is ONE request, not one per message");
    const std::vector<Attempt> attempts = fx.broker.attempts();
    expect(!attempts.empty() && attempts[0].code == RequestCode::SEND_BATCH_MESSAGE,
           "the batch goes out as SEND_BATCH_MESSAGE",
           attempts.empty() ? "no attempt recorded" : std::to_string(attempts[0].code));
    // the sync batch kernel already calls sendWithHooks, so batch async does not bypass hooks
    expectInt(hook->beforeCount.load(), 1, "the batch runs SendMessageHook.before");
    expectInt(hook->afterCount.load(), 1, "and after exactly once — not twice (no async-chain after)");

    // an oversized **sub-message**: the whole batch fails on the spot and nothing is sent. Without
    // per-message Validators.checkMessage the batch would go to the broker as-is, making local validation a no-op for the batch path.
    auto bad = std::make_shared<RecordingCallback>();
    std::vector<Message> oversized = {plainMessage(fx.topic), Message(fx.topic, Bytes(4096, 'x'))};
    p.sendBatchAsync(oversized, bad, 3000);
    expect(bad->waitDone(3000), "an oversized sub-message reaches the callback");
    expectInt(bad->snapshot().exceptionCount, 1, "the oversized batch is rejected");
    expect(bad->snapshot().message.find("the message body size over max value, MAX: 1024")
               != std::string::npos,
           "the per-sub-message validation text matches the Validators contract", bad->snapshot().message);
    expectInt(fx.broker.sendCount(), 1, "the rejected batch never reaches the broker");

    // empty batch: the same wording as the sync kernel, delivered via the callback instead of thrown at the caller
    auto empty = std::make_shared<RecordingCallback>();
    p.sendBatchAsync(std::vector<Message>{}, empty, 3000);
    expect(empty->waitDone(3000), "an empty batch reaches the callback");
    expect(empty->snapshot().message.find("message list is empty") != std::string::npos,
           "the empty-batch text matches the sync kernel", empty->snapshot().message);
    expectInt(fx.broker.sendCount(), 1, "an empty batch never reaches the broker");
    p.shutdown();
}

// 19. a batch charged to the size gate must sum **every** sub-message's length.
//     charging it as one unit would not fail any send; it would just let a 100-message batch occupy one unit of capacity —
//     the size gate would be a no-op for batches, exposed only when a long run blows up the queue.
void testBatchAsyncChargesTheWholeBatchToTheSizeGate() {
    AsyncFixture fx("BatchBp", 600);
    DefaultMQProducer p("PG_batch_bp");
    p.setNamesrvAddr(fx.ns.address());
    p.setEnableBackpressureForAsyncMode(true);
    p.setBackPressureForAsyncSendNum(static_cast<int32_t>(kMinAsyncSendNum));
    p.setBackPressureForAsyncSendSize(static_cast<int32_t>(kMinAsyncSendSize));
    p.start();

    std::vector<Message> msgs;
    for (int i = 0; i < 2; ++i) {
        // 300 KiB each, 600 KiB per batch: the default 1 MiB size gate can take it, but leaves only 420 KiB
        msgs.push_back(Message(fx.topic, Bytes(300 * 1024, '7')));
    }
    auto first = std::make_shared<RecordingCallback>();
    p.sendBatchAsync(msgs, first, 8000);
    const int64_t wholeBatch = kMinAsyncSendSize - 2LL * 300 * 1024;
    expect(waitFor([&] { return p.getSemaphoreAsyncSendSizeAvailablePermits() == wholeBatch; }, 3000),
           "the size gate charges the WHOLE batch, not one message",
           "available=" + std::to_string(p.getSemaphoreAsyncSendSizeAvailablePermits()));
    expectInt(p.getSemaphoreAsyncSendNumAvailablePermits(), kMinAsyncSendNum - 1,
              "a batch still borrows only one num permit");

    auto second = std::make_shared<RecordingCallback>();
    p.sendBatchAsync(msgs, second, 150);
    expect(second->snapshot().exceptionCount == 1
               && second->snapshot().message
                      == "send message tryAcquire semaphoreAsyncSize timeout",
           "the second batch is gated with the exact text", second->snapshot().message);
    expect(first->waitDone(10000), "the first batch still completes");
    expectInt(fx.broker.sendCount(), 1, "only the first batch reached the broker");
    expect(waitFor([&] { return p.getSemaphoreAsyncSendSizeAvailablePermits() == kMinAsyncSendSize
                                  && p.getSemaphoreAsyncSendNumAvailablePermits() == kMinAsyncSendNum; },
                   3000),
           "both permits come back after the batch settles");
    p.shutdown();
}

// 20. before start(), batch async and single async share one contract: throw at the entry, deliver no callback
void testBatchAsyncRejectsBeforeStart() {
    DefaultMQProducer p("PG_batch_async_lifecycle");
    p.setNamesrvAddr("127.0.0.1:9876");
    auto cb = std::make_shared<RecordingCallback>();
    bool threw = false;
    std::string what;
    try {
        p.sendBatchAsync({plainMessage("BatchLifecycle")}, cb, 1000);
    } catch (const MQClientException& e) {
        threw = true;
        what = e.what();
    }
    expect(threw, "sendBatchAsync before start() throws");
    expect(what.find("producer") != std::string::npos,
           "the rejection is a producer lifecycle error", what);
    expectInt(cb->snapshot().successCount + cb->snapshot().exceptionCount, 0,
              "no callback on a synchronous rejection");
}

void testBatchAsyncWritesClientIdsBeforeEncodingTheBody() {
    AsyncFixture fx("BatchIds");
    DefaultMQProducer p("PG_batch_ids");
    p.setNamesrvAddr(fx.ns.address());
    p.start();

    std::vector<Message> msgs;
    for (int i = 0; i < 3; ++i) {
        msgs.push_back(plainMessage(fx.topic));
    }
    auto cb = std::make_shared<RecordingCallback>();
    p.sendBatchAsync(msgs, cb, 3000);
    expect(cb->waitDone(3000), "the batch settles");
    const std::vector<Attempt> attempts = fx.broker.attempts();
    expectInt(static_cast<int>(attempts.size()), 1, "the whole batch is one request");
    if (attempts.empty()) {
        p.shutdown();
        return;
    }
    const std::vector<Message> subs = decodeBatchMessages(attempts[0].body);
    expectInt(static_cast<int>(subs.size()), 3, "the batch body carries all three sub-messages");
    std::set<std::string> ids;
    bool everySubHasOne = !subs.empty();
    for (const Message& m : subs) {
        const std::string id = m.getProperty(MessageConst::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX);
        if (id.size() != 32) everySubHasOne = false;
        ids.insert(id);
    }
    expect(everySubHasOne, "every sub-message carries a 32-hex client UNIQ_KEY");
    expectInt(static_cast<int>(ids.size()), static_cast<int>(subs.size()),
              "and they are all different — one shared ID would look like a duplicate");
    const auto props = attempts[0].ext.find("i");  // the properties field of SendMessageRequestHeaderV2
    expect(props != attempts[0].ext.end()
               && props->second.find(MessageConst::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX)
                      != std::string::npos,
           "the batch message itself carries a UNIQ_KEY (uniq id set on the batch)",
           props == attempts[0].ext.end() ? "no properties on the wire" : props->second);
    p.shutdown();
}

void testAsyncSendHeaderCarriesBrokerNameAndTopicKeys() {
    AsyncFixture fx("AsyncHeaderFields");
    DefaultMQProducer p("PG_async_header");
    p.setNamesrvAddr(fx.ns.address());
    p.setCreateTopicKey("AsyncTopicKey");
    p.setDefaultTopicQueueNums(11);
    p.start();

    auto cb = std::make_shared<RecordingCallback>();
    p.sendAsync(plainMessage(fx.topic), cb, 3000);
    expect(cb->waitDone(3000), "the async send settles");
    const std::vector<Attempt> attempts = fx.broker.attempts();
    expectInt(static_cast<int>(attempts.size()), 1, "one request on the wire");
    if (attempts.empty()) {
        p.shutdown();
        return;
    }
    const Attempt& a = attempts[0];
    expect(a.code == RequestCode::SEND_MESSAGE_V2, "the async request is 310");
    expect(a.ext.count("n") == 1 && a.ext.at("n") == fx.topic + "-broker",
           "async request names its broker as V2 key n");
    expect(a.ext.count("c") == 1 && a.ext.at("c") == "AsyncTopicKey",
           "async request carries the producer createTopicKey as c");
    expect(a.ext.count("d") == 1 && a.ext.at("d") == "11",
           "and the producer defaultTopicQueueNums as d");
    p.shutdown();
}

void testPinnedTopicGuardAsync() {
    AsyncFixture fx("AsyncPinnedGuard");
    DefaultMQProducer p("PG_async_pinned_guard");
    p.setNamesrvAddr(fx.ns.address());
    p.start();
    const MessageQueue pinned(fx.topic, fx.topic + "-broker", 0);

    {
        // (1) single async with mismatched topic: async wording, zero requests
        auto cb = std::make_shared<RecordingCallback>();
        p.sendAsync(plainMessage("AsyncPinnedGuardOther"), pinned, cb, 3000);
        expect(cb->waitDone(5000), "a refused async send still calls back");
        auto s = cb->snapshot();
        expectInt(s.exceptionCount, 1, "the mismatch surfaces through onException");
        expectInt(s.successCount, 0, "no success on the mismatch");
        expect(s.message == "Topic of the message does not match its target message queue",
               "the async guard keeps its distinct wording verbatim", s.message);
        expectInt(fx.broker.sendCount(), 0, "nothing reached the broker");
    }
    {
        // (2) same topic still sends (the guard must not be so loose it misfires)
        auto cb = std::make_shared<RecordingCallback>();
        p.sendAsync(plainMessage(fx.topic), pinned, cb, 3000);
        expect(cb->waitDone(5000), "a matching pinned async send settles");
        expectInt(cb->snapshot().successCount, 1, "onSuccess on the matching path");
        expectInt(fx.broker.sendCount(), 1, "exactly one request on the wire");
    }
    {
        // (3) batch async shares the same guard and the same wording
        std::vector<Message> bad{plainMessage("AsyncPinnedGuardOther"),
                                 plainMessage("AsyncPinnedGuardOther")};
        auto cb = std::make_shared<RecordingCallback>();
        p.sendBatchAsync(bad, pinned, cb, 3000);
        expect(cb->waitDone(5000), "a refused async batch still calls back");
        auto s = cb->snapshot();
        expectInt(s.exceptionCount, 1, "the batch mismatch surfaces through onException");
        expect(s.message == "Topic of the message does not match its target message queue",
               "the async batch shares the async wording", s.message);
        expectInt(fx.broker.sendCount(), 1, "the refused batch adds no request");

        std::vector<Message> good{plainMessage(fx.topic), plainMessage(fx.topic)};
        auto cb2 = std::make_shared<RecordingCallback>();
        p.sendBatchAsync(good, pinned, cb2, 3000);
        expect(cb2->waitDone(5000), "a matching pinned async batch settles");
        expectInt(cb2->snapshot().successCount, 1, "onSuccess on the matching batch");
        expectInt(fx.broker.sendCount(), 2, "one batch request more");
    }
    p.shutdown();
}

// an unexpected exception inside a case must become a readable failure instead of terminating the whole process
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
    // except the timeout / queue-full cases, every case should finish within a few hundred ms; a slow case must stand out at a glance
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
    runCase("backPressureDefaultsAndGateOff", testBackPressureDefaultsAndGateOff);
    runCase("backPressureFloors", testBackPressureFloors);
    runCase("numGateRejectsOnCallerThread", testNumGateRejectsOnCallerThread);
    runCase("sizeGateRejectsAndGivesTheNumPermitBack",
            testSizeGateRejectsAndGivesTheNumPermitBack);
    runCase("inFlightSendBorrowsBothPermits", testInFlightSendBorrowsBothPermits);
    runCase("runtimeResizeWakesABlockedSender", testRuntimeResizeWakesABlockedSender);
    runCase("queueFullRunsWithBackPressureOn", testQueueFullRunsWithBackPressureOn);
    runCase("failedAndRetriedSendsReleasePermits", testFailedAndRetriedSendsReleasePermits);
    runCase("batchAsyncOneRequestOneCallback", testBatchAsyncOneRequestOneCallback);
    runCase("batchAsyncChargesTheWholeBatchToTheSizeGate",
            testBatchAsyncChargesTheWholeBatchToTheSizeGate);
    runCase("batchAsyncRejectsBeforeStart", testBatchAsyncRejectsBeforeStart);
    runCase("batchAsyncWritesClientIdsBeforeEncodingTheBody",
            testBatchAsyncWritesClientIdsBeforeEncodingTheBody);
    runCase("asyncSendHeaderCarriesBrokerNameAndTopicKeys",
            testAsyncSendHeaderCarriesBrokerNameAndTopicKeys);
    runCase("pinnedTopicGuardAsync", testPinnedTopicGuardAsync);

    std::printf("%s: %d checks, %d failures\n", fails == 0 ? "PASS" : "FAIL", checks, fails);
    return fails == 0 ? 0 : 1;
}
