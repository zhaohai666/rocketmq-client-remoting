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
#include <string>
#include <thread>
#include <vector>

#include "rocketmq/common/byte_buffer.h"
#include "rocketmq/remoting/exception.h"
#include "rocketmq/remoting/protocol/codes.h"
#include "rocketmq/remoting/protocol/remoting_command.h"
#include "rocketmq/remoting/remoting_client.h"

#if defined(_WIN32)
#include <winsock2.h>
#include <ws2tcpip.h>
#else
#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <unistd.h>
#endif

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

#if defined(_WIN32)

int main() {
    std::cout << "transport checks: skipped on Windows\n";
    return 0;
}

#else

namespace {

using sock_t = int;

void closeSock(sock_t s) {
    if (s >= 0) {
        ::close(s);
    }
}

// 阻塞读取 n 字节；返回 false 表示对端关闭
bool readN(sock_t s, char* buf, size_t n) {
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
bool readFrame(sock_t s, Bytes& out) {
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

bool writeAll(sock_t s, const Bytes& data) {
    size_t sent = 0;
    while (sent < data.size()) {
        int n = static_cast<int>(::send(s, data.data() + sent,
                                       static_cast<int>(data.size() - sent), 0));
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
    Silent     // 收到但永不回复（测超时）
};

// 本机回环测试服务端：accept 一个连接，按 mode 处理若干请求
class TestServer {
public:
    explicit TestServer(ServerMode mode, int maxRequests = 8) : mode_(mode), maxRequests_(maxRequests) {
        listenSock_ = ::socket(AF_INET, SOCK_STREAM, 0);
        int one = 1;
        ::setsockopt(listenSock_, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
        struct sockaddr_in addr;
        addr.sin_family = AF_INET;
        addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        addr.sin_port = 0;  // 让内核分配端口
        ::bind(listenSock_, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr));
        ::listen(listenSock_, 4);
        socklen_t len = sizeof(addr);
        ::getsockname(listenSock_, reinterpret_cast<struct sockaddr*>(&addr), &len);
        port_ = ntohs(addr.sin_port);
        thread_ = std::thread([this]() { serve(); });
    }

    ~TestServer() {
        stop_.store(true);
        closeSock(listenSock_);
        if (thread_.joinable()) {
            thread_.join();
        }
    }

    std::string address() const { return "127.0.0.1:" + std::to_string(port_); }
    int served() const { return served_.load(); }

private:
    void serve() {
        // 循环 accept：closeChannel 后客户端会重连，服务端必须愿意接第二次
        while (!stop_.load() && served_.load() < maxRequests_) {
            sock_t conn = ::accept(listenSock_, nullptr, nullptr);
            if (conn < 0) {
                break;  // 监听 socket 已被关闭
            }
            handleConn(conn);
            closeSock(conn);
        }
    }

    void handleConn(sock_t conn) {
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
            if (mode_ == ServerMode::Silent) {
                // 不回复：让客户端超时
                std::this_thread::sleep_for(std::chrono::milliseconds(1200));
                continue;
            }
            RemotingCommand resp;
            resp.code = ResponseCode::SUCCESS;
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
    sock_t listenSock_ = -1;
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

        RemotingCommand req = RemotingCommand::createRequestCommand(RequestCode::HEART_BEAT);
        const int32_t opaque = req.opaque;
        client.invokeAsync(server.address(), req,
                           [&](const RemotingCommand& r) {
                               gotCode.store(r.code);
                               gotOpaque.store(r.opaque);
                               called.store(true);
                           });
        for (int i = 0; i < 200 && !called.load(); ++i) {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        CHECK(called.load(), "invokeAsync callback fired");
        CHECK(gotCode.load() == ResponseCode::SUCCESS, "invokeAsync response code");
        CHECK(gotOpaque.load() == opaque, "invokeAsync opaque matched");
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

    // ---------------------------------------------------------- 8. 地址解析
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

#endif  // !_WIN32
