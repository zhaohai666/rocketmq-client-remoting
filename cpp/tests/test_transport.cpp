// 传输层单测：真实起一个本机 TCP 服务端，用 RemotingClient 真连、真发、真收。
//
// 覆盖点（都是真实 socket 行为，不用 mock）：
//   1. invokeSync 正常收发 + opaque 匹配 + 连接复用
//   2. 半包/粘包：服务端把响应拆成两段发送，客户端必须正确重组
//   3. invokeAsync 回调在读线程触发
//   4. invokeOneway 发出即返回
//   5. 建连失败 -> RemotingConnectException
//   6. 服务端不回 -> RemotingTimeoutException（且不污染后续调用）
//   7. closeChannel / isChannelWritable / shutdown 幂等
#include <atomic>
#include <chrono>
#include <cstdint>
#include <iostream>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "rocketmq/common/byte_buffer.h"
#include "rocketmq/common/net_compat.h"
#include "rocketmq/remoting/exception.h"
#include "rocketmq/remoting/protocol/codes.h"
#include "rocketmq/remoting/protocol/remoting_command.h"
#include "rocketmq/remoting/remoting_client.h"

using namespace rocketmq;

static int g_pass = 0;
static int g_fail = 0;

#define CHECK(cond, msg)                                                       \
    do {                                                                       \
        if (cond) {                                                            \
            ++g_pass;                                                          \
        } else {                                                               \
            ++g_fail;                                                          \
            std::cout << "[FAIL] " << (msg) << "\n";                           \
        }                                                                      \
    } while (0)

namespace {

using netcompat::socket_t;
using netcompat::socklen_type;
using netcompat::kInvalidSocket;

// 阻塞读取 n 字节；返回 false 表示对端关闭
bool readN(socket_t s, char* buf, size_t n) {
    size_t got = 0;
    while (got < n) {
        int r = static_cast<int>(::recv(s, buf + got, static_cast<int>(n - got), 0));
        if (r <= 0) {
            return false;
        }
        got += static_cast<size_t>(r);
    }
    return true;
}

// 读取一个完整帧（含 4 字节 totalLength 前缀）
bool readFrame(socket_t s, Bytes& out) {
    char lenBuf[4];
    if (!readN(s, lenBuf, 4)) {
        return false;
    }
    Bytes lenStr(lenBuf, 4);
    int32_t totalLen = ByteReader::getInt32At(lenStr, 0);
    if (totalLen <= 0 || totalLen > RemotingClient::MAX_FRAME_LENGTH) {
        return false;
    }
    Bytes body(static_cast<size_t>(totalLen), '\0');
    if (!readN(s, &body[0], static_cast<size_t>(totalLen))) {
        return false;
    }
    out = lenStr + body;
    return true;
}

bool writeAll(socket_t s, const Bytes& data) {
    size_t sent = 0;
    while (sent < data.size()) {
        int n = static_cast<int>(::send(s, data.data() + sent,
                                       static_cast<int>(data.size() - sent),
                                       netcompat::sendFlags()));
        if (n <= 0) {
            return false;
        }
        sent += static_cast<size_t>(n);
    }
    return true;
}

enum class ServerMode {
    Normal,    // 正常响应
    HalfSplit, // 响应拆两段发（测半包重组）
    Silent,    // 收到但永不回复（测超时）
    GoAway     // 前 goAwayConns_ 条连接一律回 GO_AWAY(1500)，之后的连接正常响应
};

// 本机回环测试服务端：accept 一个连接，按 mode 处理若干请求
class TestServer {
public:
    explicit TestServer(ServerMode mode, int maxRequests = 8, int goAwayConns = 0)
        : mode_(mode), maxRequests_(maxRequests), goAwayConns_(goAwayConns) {
        netcompat::ensureInitialized();
        listenSock_ = ::socket(AF_INET, SOCK_STREAM, 0);
        int one = 1;
        ::setsockopt(listenSock_, SOL_SOCKET, SO_REUSEADDR,
                     reinterpret_cast<const char*>(&one), static_cast<int>(sizeof(one)));
        struct sockaddr_in addr;
        addr.sin_family = AF_INET;
        addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        addr.sin_port = 0;  // 让内核分配端口
        ::bind(listenSock_, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr));
        ::listen(listenSock_, 4);
        socklen_type len = sizeof(addr);
        ::getsockname(listenSock_, reinterpret_cast<struct sockaddr*>(&addr), &len);
        port_ = ntohs(addr.sin_port);
        thread_ = std::thread([this]() { serve(); });
    }

    ~TestServer() {
        // 先置位、再 join、最后关监听 socket：Windows 上 closesocket 不会唤醒阻塞在
        // accept 里的线程，join 前先关句柄反而会让 serve() 使用已失效的 socket。
        stop_.store(true);
        if (thread_.joinable()) {
            thread_.join();
        }
        netcompat::closeSocket(listenSock_);
    }

    std::string address() const { return "127.0.0.1:" + std::to_string(port_); }
    int served() const { return served_.load(); }
    int connections() const { return conns_.load(); }
    // 服务端见过的 opaque：GO_AWAY 重发必须是**新** opaque，用这个断言
    std::vector<int32_t> opaques() const {
        std::lock_guard<std::mutex> lk(opaqueMutex);
        return opaqueLog_;
    }

private:
    void serve() {
        // 循环 accept：closeChannel 后客户端会重连，服务端必须愿意接第二次。
        // 用 select 轮询而不是裸 accept：这样 stop_ 置位后线程能在 100ms 内退出
        // （Windows 的 closesocket 不会唤醒阻塞中的 accept）。
        while (!stop_.load() && served_.load() < maxRequests_) {
            fd_set rfds;
            FD_ZERO(&rfds);
            FD_SET(listenSock_, &rfds);
            timeval tv;
            tv.tv_sec = 0;
            tv.tv_usec = 100 * 1000;
            const int ready = ::select(netcompat::selectNfds(listenSock_), &rfds, nullptr, nullptr, &tv);
            if (ready == 0) {
                continue;  // 超时，回去看 stop_
            }
            if (ready < 0) {
                break;  // 监听 socket 已被关闭
            }
            socket_t conn = ::accept(listenSock_, nullptr, nullptr);
            if (conn == kInvalidSocket) {
                break;
            }
            handleConn(conn, conns_.fetch_add(1));
            netcompat::closeSocket(conn);
        }
    }

    void handleConn(socket_t conn, int connIndex) {
        while (!stop_.load() && served_.load() < maxRequests_) {
            Bytes frame;
            if (!readFrame(conn, frame)) {
                break;  // 对端关闭或半包中断
            }
            RemotingCommand req;
            if (!RemotingCommand::tryDecode(frame, req, nullptr)) {
                break;
            }
            served_.fetch_add(1);
            {
                std::lock_guard<std::mutex> lk(opaqueMutex);
                opaqueLog_.push_back(req.opaque);
            }
            if (mode_ == ServerMode::Silent) {
                // 不回复：让客户端超时
                std::this_thread::sleep_for(std::chrono::milliseconds(1200));
                continue;
            }
            RemotingCommand resp;
            resp.code = mode_ == ServerMode::GoAway && connIndex < goAwayConns_
                            ? ResponseCode::GO_AWAY
                            : ResponseCode::SUCCESS;
            resp.opaque = req.opaque;  // 必须回填同一个 opaque
            resp.markResponseType();
            resp.remark = "ok";
            resp.hasRemark = true;
            resp.extFields["echoCode"] = std::to_string(req.code);
            Bytes out = resp.encode();
            if (mode_ == ServerMode::HalfSplit && out.size() > 4) {
                // 先发 3 字节（不足长度前缀），再发剩余 —— 强制客户端处理半包
                Bytes head = out.substr(0, 3);
                Bytes tail = out.substr(3);
                writeAll(conn, head);
                std::this_thread::sleep_for(std::chrono::milliseconds(120));
                writeAll(conn, tail);
            } else {
                writeAll(conn, out);
            }
        }
    }

    ServerMode mode_;
    int maxRequests_;
    int goAwayConns_ = 0;
    std::atomic<int> conns_{0};
    mutable std::mutex opaqueMutex;
    std::vector<int32_t> opaqueLog_;
    socket_t listenSock_ = kInvalidSocket;
    uint16_t port_ = 0;
    std::atomic<bool> stop_{false};
    std::atomic<int> served_{0};
    std::thread thread_;
};

bool throwsConnectException(const std::string& addr) {
    RemotingClient client(300, 1000);
    RemotingCommand req = RemotingCommand::createRequestCommand(RequestCode::HEART_BEAT);
    try {
        client.invokeSync(addr, req, 1000);
        return false;
    } catch (const RemotingConnectException&) {
        return true;
    } catch (const RemotingException&) {
        return false;
    }
}

bool throwsTimeoutException(const std::string& addr) {
    RemotingClient client(1000, 300);
    RemotingCommand req = RemotingCommand::createRequestCommand(RequestCode::HEART_BEAT);
    try {
        client.invokeSync(addr, req, 300);
        return false;
    } catch (const RemotingTimeoutException&) {
        return true;
    } catch (const RemotingException&) {
        return false;
    }
}

}  // namespace

int main() {
    // ---------------------------------------------------------- 1. 正常同步调用 + 连接复用
    {
        TestServer server(ServerMode::Normal, 4);
        RemotingClient client;
        const std::string addr = server.address();

        RemotingCommand req = RemotingCommand::createRequestCommand(RequestCode::HEART_BEAT);
        const int32_t opaque = req.opaque;
        RemotingCommand resp = client.invokeSync(addr, req);
        CHECK(resp.code == ResponseCode::SUCCESS, "invokeSync response code == SUCCESS");
        CHECK(resp.opaque == opaque, "invokeSync opaque matched");
        CHECK(resp.isResponseType(), "invokeSync response flag set");
        CHECK(resp.remark == "ok", "invokeSync remark round-trip");
        CHECK(resp.getExtField("echoCode") == std::to_string(RequestCode::HEART_BEAT),
              "invokeSync extFields round-trip");
        CHECK(client.isChannelWritable(addr), "isChannelWritable true after invoke");
        CHECK(client.connectionCount() == 1, "one connection established");

        // 第二次调用应复用同一连接
        RemotingCommand req2 = RemotingCommand::createRequestCommand(RequestCode::PULL_MESSAGE);
        RemotingCommand resp2 = client.invokeSync(addr, req2);
        CHECK(resp2.code == ResponseCode::SUCCESS, "second invokeSync ok (connection reused)");
        CHECK(resp2.opaque == req2.opaque, "second invokeSync opaque distinct/match");
        CHECK(opaque != req2.opaque, "opaque increments across requests");
        CHECK(client.connectionCount() == 1, "connection reused (still 1)");
        CHECK(server.served() >= 2, "server saw both requests");
        client.shutdown();
    }

    // ---------------------------------------------------------- 2. 半包重组
    {
        TestServer server(ServerMode::HalfSplit, 2);
        RemotingClient client;
        RemotingCommand req = RemotingCommand::createRequestCommand(RequestCode::HEART_BEAT);
        RemotingCommand resp = client.invokeSync(server.address(), req);
        CHECK(resp.code == ResponseCode::SUCCESS, "half-split frame reassembled");
        CHECK(resp.opaque == req.opaque, "half-split opaque matched");
        CHECK(resp.remark == "ok", "half-split remark intact after reassembly");
        client.shutdown();
    }

    // ---------------------------------------------------------- 3. 异步回调
    {
        TestServer server(ServerMode::Normal, 2);
        RemotingClient client;
        std::atomic<bool> called{false};
        std::atomic<int32_t> gotCode{0};
        std::atomic<int32_t> gotOpaque{0};
        std::atomic<int32_t> errLen{0};
        std::atomic<int32_t> kind{-1};

        RemotingCommand req = RemotingCommand::createRequestCommand(RequestCode::HEART_BEAT);
        const int32_t opaque = req.opaque;
        client.invokeAsync(server.address(), req,
                           [&](const RemotingCommand& r, const InvokeError& err) {
                               gotCode.store(r.code);
                               gotOpaque.store(r.opaque);
                               kind.store(static_cast<int32_t>(err.kind));
                               errLen.store(static_cast<int32_t>(err.message.size()));
                               called.store(true);
                           });
        for (int i = 0; i < 200 && !called.load(); ++i) {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        CHECK(called.load(), "invokeAsync callback fired");
        CHECK(gotCode.load() == ResponseCode::SUCCESS, "invokeAsync response code");
        CHECK(gotOpaque.load() == opaque, "invokeAsync opaque matched");
        CHECK(errLen.load() == 0, "invokeAsync success carries no error");
        CHECK(kind.load() == static_cast<int32_t>(InvokeError::Kind::NONE),
              "invokeAsync success carries kind NONE");
        client.shutdown();
    }

    // ------------------------------------------------------ 3b. 异步超时回调
    // 对端收下请求但永不回复：timeoutMillis 必须生效，回调仍要触发**一次**（带 error）。
    // 修复前该参数被直接丢弃，在途表项和回调都会永久悬挂。
    {
        TestServer server(ServerMode::Silent, 1);
        RemotingClient client;
        std::atomic<int32_t> fired{0};
        std::atomic<int32_t> errLen{0};
        std::atomic<int32_t> timeoutKind{-1};

        RemotingCommand req = RemotingCommand::createRequestCommand(RequestCode::HEART_BEAT);
        client.invokeAsync(
            server.address(), req,
            [&](const RemotingCommand&, const InvokeError& err) {
                ++fired;
                errLen.store(static_cast<int32_t>(err.message.size()));
                timeoutKind.store(static_cast<int32_t>(err.kind));
            },
            300);
        for (int i = 0; i < 300 && fired.load() == 0; ++i) {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        CHECK(fired.load() == 1, "invokeAsync timeout fires the callback once");
        CHECK(errLen.load() > 0, "invokeAsync timeout carries an error");
        // 超时必须是 TIMEOUT：异步发送的重试判定按异常**类型**分流（Java 的
        // RemotingTimeoutException 会重试，unknown reason 不会），文案不可作为依据。
        CHECK(timeoutKind.load() == static_cast<int32_t>(InvokeError::Kind::TIMEOUT),
              "invokeAsync timeout carries kind TIMEOUT");
        client.shutdown();
    }

    // ---------------------------------------------------------- 4. 单向调用
    {
        TestServer server(ServerMode::Normal, 2);
        RemotingClient client;
        RemotingCommand req = RemotingCommand::createRequestCommand(RequestCode::HEART_BEAT);
        bool threw = false;
        try {
            client.invokeOneway(server.address(), req);
        } catch (const RemotingException&) {
            threw = true;
        }
        CHECK(!threw, "invokeOneway does not throw");
        CHECK(req.isOnewayRpc(), "invokeOneway marks oneway flag");
        // 等服务端确实收到
        for (int i = 0; i < 100 && server.served() < 1; ++i) {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        CHECK(server.served() >= 1, "server received oneway request");
        client.shutdown();
    }

    // ---------------------------------------------------------- 5. 建连失败
    {
        // 127.0.0.1:1 基本不可能有服务
        CHECK(throwsConnectException("127.0.0.1:1"), "connect failure -> RemotingConnectException");
    }

    // ---------------------------------------------------------- 6. 超时
    {
        TestServer server(ServerMode::Silent, 2);
        CHECK(throwsTimeoutException(server.address()),
              "no response -> RemotingTimeoutException");
    }

    // ---------------------------------------------------------- 7. closeChannel / shutdown
    {
        TestServer server(ServerMode::Normal, 4);
        RemotingClient client;
        const std::string addr = server.address();
        RemotingCommand req = RemotingCommand::createRequestCommand(RequestCode::HEART_BEAT);
        client.invokeSync(addr, req);
        CHECK(client.isChannelWritable(addr), "channel writable before close");
        client.closeChannel(addr);
        CHECK(!client.isChannelWritable(addr), "channel not writable after close");
        CHECK(client.connectionCount() == 0, "connection removed after closeChannel");
        // 关闭后仍能重新建连
        RemotingCommand req2 = RemotingCommand::createRequestCommand(RequestCode::HEART_BEAT);
        RemotingCommand resp2 = client.invokeSync(addr, req2);
        CHECK(resp2.code == ResponseCode::SUCCESS, "reconnect after closeChannel works");
        client.shutdown();
        client.shutdown();  // 幂等
        CHECK(client.connectionCount() == 0, "connectionCount 0 after shutdown");
        CHECK(!client.isChannelWritable(addr), "not writable after shutdown");
    }

    // ------------------------- 8. GO_AWAY(1500)：换连接重发一次（Java invokeImpl 同语义）
    {
        // 8.1 第一条连接回 GO_AWAY，第二条正常 -> 调用方只看到成功
        TestServer server(ServerMode::GoAway, 4, 1);
        RemotingClient client;
        const std::string addr = server.address();
        RemotingCommand req = RemotingCommand::createRequestCommand(RequestCode::SEND_MESSAGE_V2);
        const int32_t firstOpaque = req.opaque;
        RemotingCommand resp = client.invokeSync(addr, req, 5000);
        CHECK(resp.code == ResponseCode::SUCCESS, "GO_AWAY: retry gets the real response");
        CHECK(server.connections() == 2, "GO_AWAY: retry must use a brand new connection");
        CHECK(server.served() == 2, "GO_AWAY: the request is sent exactly twice");
        CHECK(server.opaques().size() == 2 && server.opaques()[0] == firstOpaque
                  && server.opaques()[1] != firstOpaque,
              "GO_AWAY: the retry carries a fresh opaque, else responses mis-pair");
        client.shutdown();
    }
    {
        // 8.2 两次都 GO_AWAY -> 抛错而不是无限重连
        TestServer server(ServerMode::GoAway, 4, 1000);
        RemotingClient client;
        RemotingCommand req = RemotingCommand::createRequestCommand(RequestCode::SEND_MESSAGE_V2);
        bool threw = false;
        std::string what;
        try {
            client.invokeSync(server.address(), req, 5000);
        } catch (const RemotingSendRequestException& e) {
            threw = true;
            what = e.what();
        } catch (const std::exception& e) {
            what = e.what();
        }
        CHECK(threw, "GO_AWAY twice -> RemotingSendRequestException");
        CHECK(what.find("GO_AWAY twice") != std::string::npos,
              "GO_AWAY twice: message matches Java (" + what + ")");
        CHECK(server.connections() == 2, "GO_AWAY twice: retries exactly once");
        client.shutdown();
    }
    {
        // 8.3 开关关掉：直接失败，一条连接都不重连
        TestServer server(ServerMode::GoAway, 4, 1000);
        RemotingClient client;
        client.setEnableReconnectForGoAway(false);
        CHECK(!client.enableReconnectForGoAway(), "setEnableReconnectForGoAway is readable");
        RemotingCommand req = RemotingCommand::createRequestCommand(RequestCode::SEND_MESSAGE_V2);
        bool threw = false;
        std::string what;
        try {
            client.invokeSync(server.address(), req, 5000);
        } catch (const RemotingSendRequestException& e) {
            threw = true;
            what = e.what();
        }
        CHECK(threw, "GO_AWAY with the flag off -> RemotingSendRequestException");
        CHECK(what.find("Receive GO_AWAY from channel") != std::string::npos,
              "GO_AWAY flag off: message matches Java (" + what + ")");
        CHECK(server.connections() == 1, "GO_AWAY flag off: no reconnect at all");
        client.shutdown();
    }
    {
        // 8.4 异步路径共用同一套语义（Java 里同步/异步都走 invokeImpl）。
        //     重发不能在 reading 线程上等，所以由清理线程代跑 —— 这里等回调落地。
        TestServer server(ServerMode::GoAway, 4, 1);
        RemotingClient client;
        const std::string addr = server.address();
        RemotingCommand req = RemotingCommand::createRequestCommand(RequestCode::SEND_MESSAGE_V2);
        std::mutex m;
        std::atomic<bool> done{false};
        int code = -1;
        std::string error;
        client.invokeAsync(addr, req,
                           [&](const RemotingCommand& resp, const InvokeError& err) {
                               std::lock_guard<std::mutex> lk(m);
                               code = resp.code;
                               error = err.message;
                               done.store(true);
                           },
                           5000);
        for (int i = 0; i < 500 && !done.load(); ++i) {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        CHECK(done.load(), "async GO_AWAY: callback fires exactly once");
        CHECK(error.empty(), "async GO_AWAY: successful retry carries no error (" + error + ")");
        CHECK(code == ResponseCode::SUCCESS, "async GO_AWAY: retry gets the real response");
        CHECK(server.connections() == 2, "async GO_AWAY: retried on a new connection");
        client.shutdown();
    }
    {
        // 8.5 异步两次 GO_AWAY -> 折成回调错误，不能悄悄吞掉
        TestServer server(ServerMode::GoAway, 4, 1000);
        RemotingClient client;
        RemotingCommand req = RemotingCommand::createRequestCommand(RequestCode::SEND_MESSAGE_V2);
        std::mutex m;
        std::atomic<bool> done{false};
        std::string error;
        client.invokeAsync(server.address(), req,
                           [&](const RemotingCommand& resp, const InvokeError& err) {
                               (void)resp;
                               std::lock_guard<std::mutex> lk(m);
                               error = err.message;
                               done.store(true);
                           },
                           5000);
        for (int i = 0; i < 500 && !done.load(); ++i) {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        CHECK(done.load(), "async GO_AWAY twice: callback fires");
        CHECK(error.find("GO_AWAY twice") != std::string::npos,
              "async GO_AWAY twice: error carries the Java message (" + error + ")");
        CHECK(server.connections() == 2, "async GO_AWAY twice: retries exactly once");
        client.shutdown();
    }

    // ---------------------------------------------------------- 9. 地址解析
    {
        std::string h;
        std::string p;
        RemotingClient::parseAddress("127.0.0.1:10911", h, p);
        CHECK(h == "127.0.0.1" && p == "10911", "parseAddress ipv4");
        RemotingClient::parseAddress("[::1]:10911", h, p);
        CHECK(h == "::1" && p == "10911", "parseAddress ipv6 bracket");
        RemotingClient::parseAddress("broker.example.com:10911", h, p);
        CHECK(h == "broker.example.com" && p == "10911", "parseAddress hostname");
    }

    std::cout << "transport checks: " << g_pass << " passed, " << g_fail << " failed\n";
    return g_fail == 0 ? 0 : 1;
}
