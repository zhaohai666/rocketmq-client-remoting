// 断连时立刻失败在途请求（对应 Java NettyRemotingAbstract#failFast / #requestFail）。
//
// Java 的 NettyRemotingHandler#close 在 closeChannel 之后紧接着调 failFast(channel)
// （NettyRemotingClient.java:1191）：把这条连接名下的在途请求逐条 requestFail ——
// setSendRequestOK(false) + putResponse(null) + 恰好投递一次回调。缺了这一步会错两件事：
//   1. 时机：同步调用等满 invoke 超时、异步回调等 timeout + 1s（超时清理线程的宽限口径），
//      而对端在第一次 EOF 时就已经确定"再也不会有响应了"。
//   2. 语义：报成 TIMEOUT，Java 报 SEND_REQUEST。异步发送的重试判据按异常类型分流，
//      错类型等于错重试决策（见 InvokeError::Kind 的注释）。
//
// 全部用真 socket 对端（读完请求就关连接），不用 mock：只有真 EOF 才走读线程退出那条路。
#include <atomic>
#include <chrono>
#include <cstdint>
#include <functional>
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

// 读一个完整帧，只为了知道"请求到了"；不解析内容
bool readFrame(socket_t s) {
    char lenBuf[4];
    if (!readN(s, lenBuf, 4)) {
        return false;
    }
    Bytes lenStr(lenBuf, 4);
    const int32_t totalLen = ByteReader::getInt32At(lenStr, 0);
    if (totalLen <= 0 || totalLen > RemotingClient::MAX_FRAME_LENGTH) {
        return false;
    }
    Bytes body(static_cast<size_t>(totalLen), '\0');
    return readN(s, &body[0], static_cast<size_t>(totalLen));
}

bool waitUntil(const std::function<bool()>& pred, int timeoutMs = 8000) {
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeoutMs);
    while (std::chrono::steady_clock::now() < deadline) {
        if (pred()) {
            return true;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    return pred();
}

// 本机回环服务端：每条连接读完一个请求后按 mode 处置。
//   Drop —— 读完立刻关连接且**一个字节都不回**（模拟对端掉线/重启）
//   Hold —— 读完挂住不关也不回（用来证明"别人家的请求不该被牵连"）
//   Reply —— 读完回 SUCCESS（正向对照：换连接之后还能正常工作）
enum class Mode { Drop, Hold, Reply };

class PeerServer {
public:
    explicit PeerServer(Mode mode, int maxRequests = 8)
        : mode_(mode), maxRequests_(maxRequests) {
        netcompat::ensureInitialized();
        listenSock_ = ::socket(AF_INET, SOCK_STREAM, 0);
        int one = 1;
        ::setsockopt(listenSock_, SOL_SOCKET, SO_REUSEADDR,
                     reinterpret_cast<const char*>(&one), static_cast<int>(sizeof(one)));
        struct sockaddr_in addr;
        addr.sin_family = AF_INET;
        addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        addr.sin_port = 0;
        ::bind(listenSock_, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr));
        ::listen(listenSock_, 4);
        socklen_type len = sizeof(addr);
        ::getsockname(listenSock_, reinterpret_cast<struct sockaddr*>(&addr), &len);
        port_ = ntohs(addr.sin_port);
        thread_ = std::thread([this]() { serve(); });
    }

    ~PeerServer() {
        stop_.store(true);
        if (thread_.joinable()) {
            thread_.join();
        }
        netcompat::closeSocket(listenSock_);
    }

    std::string address() const { return "127.0.0.1:" + std::to_string(port_); }
    int served() const { return served_.load(); }
    int connections() const { return conns_.load(); }

private:
    void serve() {
        while (!stop_.load() && served_.load() < maxRequests_) {
            fd_set rfds;
            FD_ZERO(&rfds);
            FD_SET(listenSock_, &rfds);
            timeval tv;
            tv.tv_sec = 0;
            tv.tv_usec = 100 * 1000;
            const int ready = ::select(netcompat::selectNfds(listenSock_), &rfds, nullptr, nullptr, &tv);
            if (ready == 0) {
                continue;
            }
            if (ready < 0) {
                break;
            }
            socket_t conn = ::accept(listenSock_, nullptr, nullptr);
            if (conn == kInvalidSocket) {
                break;
            }
            handleConn(conn);
            netcompat::closeSocket(conn);  // Drop 模式在这里真的关连接，客户端读到 EOF
        }
    }

    void handleConn(socket_t conn) {
        served_.fetch_add(1);
        if (mode_ == Mode::Hold) {
            // 挂着不回也不关：这条请求应当一直在途
            Bytes dummy;
            readFrame(conn);
            while (!stop_.load()) {
                std::this_thread::sleep_for(std::chrono::milliseconds(50));
            }
            return;
        }
        if (mode_ == Mode::Reply) {
            Bytes frame;
            char lenBuf[4];
            if (!readN(conn, lenBuf, 4)) {
                return;
            }
            Bytes lenStr(lenBuf, 4);
            const int32_t totalLen = ByteReader::getInt32At(lenStr, 0);
            frame = Bytes(static_cast<size_t>(totalLen), '\0');
            if (!readN(conn, &frame[0], static_cast<size_t>(totalLen))) {
                return;
            }
            frame = lenStr + frame;
            RemotingCommand req;
            if (!RemotingCommand::tryDecode(frame, req, nullptr)) {
                return;
            }
            RemotingCommand resp;
            resp.code = ResponseCode::SUCCESS;
            resp.opaque = req.opaque;
            resp.markResponseType();
            resp.remark = "ok";
            resp.hasRemark = true;
            const Bytes out = resp.encode();
            size_t sent = 0;
            while (sent < out.size()) {
                const int n = static_cast<int>(
                    ::send(conn, out.data() + sent, static_cast<int>(out.size() - sent),
                           netcompat::sendFlags()));
                if (n <= 0) {
                    break;
                }
                sent += static_cast<size_t>(n);
            }
            return;
        }
        // Drop：读完这一帧就返回 -> 析构里关掉连接
        readFrame(conn);
    }

    Mode mode_;
    int maxRequests_;
    std::atomic<int> conns_{0};
    socket_t listenSock_ = kInvalidSocket;
    uint16_t port_ = 0;
    std::atomic<bool> stop_{false};
    std::atomic<int> served_{0};
    std::thread thread_;
};

// 远超断连传播所需的时间；loopback 上 EOF 毫秒级就到，用它当"没修好"的判据
constexpr int32_t kLongTimeout = 30000;

struct AsyncResult {
    std::atomic<int> fired{0};
    std::atomic<int32_t> kind{static_cast<int32_t>(InvokeError::Kind::NONE)};
    std::string message;
    std::mutex m;
};

}  // namespace

int main() {
    // ------------------------------------------- 1. 同步调用：断连立刻抛发送失败，不等超时
    {
        PeerServer server(Mode::Drop, 1);
        RemotingClient client(2000, kLongTimeout);
        RemotingCommand req = RemotingCommand::createRequestCommand(RequestCode::HEART_BEAT);
        const auto started = std::chrono::steady_clock::now();
        std::string what;
        bool sendRequestThrew = false;
        bool timeoutThrew = false;
        try {
            client.invokeSync(server.address(), req, kLongTimeout);
        } catch (const RemotingTimeoutException& e) {
            timeoutThrew = true;
            what = e.what();
        } catch (const RemotingSendRequestException& e) {
            sendRequestThrew = true;
            what = e.what();
        } catch (const RemotingException& e) {
            what = e.what();
        }
        const auto costMs = std::chrono::duration_cast<std::chrono::milliseconds>(
                                std::chrono::steady_clock::now() - started)
                                .count();
        CHECK(sendRequestThrew, "sync: dead connection -> RemotingSendRequestException");
        CHECK(!timeoutThrew, "sync: must NOT report a timeout for a connection that is gone");
        CHECK(costMs < kLongTimeout / 3,
              "sync: returned after " + std::to_string(costMs) + "ms, expected far below timeout");
        CHECK(what.find("connection closed") != std::string::npos,
              "sync: message says why (" + what + ")");
        client.shutdown();
    }

    // --------------------------------------------- 2. 异步回调：立刻拿到 SEND_REQUEST
    {
        PeerServer server(Mode::Drop, 1);
        RemotingClient client(2000, kLongTimeout);
        RemotingCommand req = RemotingCommand::createRequestCommand(RequestCode::HEART_BEAT);
        AsyncResult r;
        const auto started = std::chrono::steady_clock::now();
        client.invokeAsync(
            server.address(), req,
            [&r](const RemotingCommand& /*resp*/, const InvokeError& error) {
                ++r.fired;
                r.kind.store(static_cast<int32_t>(error.kind));
                std::lock_guard<std::mutex> lk(r.m);
                r.message = error.message;
            },
            kLongTimeout);
        CHECK(waitUntil([&r] { return r.fired.load() >= 1; }), "async: callback fired");
        const auto costMs = std::chrono::duration_cast<std::chrono::milliseconds>(
                                std::chrono::steady_clock::now() - started)
                                .count();
        CHECK(r.fired.load() == 1, "async: callback fires exactly once");
        CHECK(r.kind.load() == static_cast<int32_t>(InvokeError::Kind::SEND_REQUEST),
              "async: kind must be SEND_REQUEST, not TIMEOUT (retry decisions key off it)");
        CHECK(costMs < kLongTimeout / 3,
              "async: callback took " + std::to_string(costMs) + "ms, near timeout+grace");
        client.shutdown();
    }

    // ------------------------------ 3. 断连与超时清理线程抢同一条请求：回调只能有一次
    {
        PeerServer server(Mode::Drop, 1);
        RemotingClient client(2000, 200);  // 短超时：两条路径都会想去抢这个条目
        RemotingCommand req = RemotingCommand::createRequestCommand(RequestCode::HEART_BEAT);
        AsyncResult r;
        client.invokeAsync(
            server.address(), req,
            [&r](const RemotingCommand& /*resp*/, const InvokeError& error) {
                ++r.fired;
                r.kind.store(static_cast<int32_t>(error.kind));
            },
            200);
        CHECK(waitUntil([&r] { return r.fired.load() >= 1; }), "once: callback fired");
        std::this_thread::sleep_for(std::chrono::milliseconds(2500));  // 跨过 timeout + 1s 宽限
        CHECK(r.fired.load() == 1, "once: callback fired " + std::to_string(r.fired.load()) + " times");
        client.shutdown();
    }

    // ------------------ 4. 只失败断开那条连接自己的请求；另一条地址上的请求不受牵连
    {
        PeerServer dying(Mode::Drop, 1);
        PeerServer quiet(Mode::Hold, 1);
        RemotingClient client(2000, kLongTimeout);

        RemotingCommand reqQuiet = RemotingCommand::createRequestCommand(RequestCode::HEART_BEAT);
        AsyncResult quietResult;
        client.invokeAsync(
            quiet.address(), reqQuiet,
            [&quietResult](const RemotingCommand& /*resp*/, const InvokeError& error) {
                ++quietResult.fired;
                quietResult.kind.store(static_cast<int32_t>(error.kind));
            },
            kLongTimeout);
        CHECK(waitUntil([&quiet] { return quiet.served() == 1; }), "collateral: quiet peer got it");

        RemotingCommand reqDying = RemotingCommand::createRequestCommand(RequestCode::HEART_BEAT);
        AsyncResult dyingResult;
        client.invokeAsync(
            dying.address(), reqDying,
            [&dyingResult](const RemotingCommand& /*resp*/, const InvokeError& error) {
                ++dyingResult.fired;
                dyingResult.kind.store(static_cast<int32_t>(error.kind));
            },
            kLongTimeout);
        CHECK(waitUntil([&dyingResult] { return dyingResult.fired.load() >= 1; }),
              "collateral: dying request failed fast");
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
        CHECK(quietResult.fired.load() == 0,
              "collateral: another connection's in-flight request must stay in flight");
        CHECK(dyingResult.kind.load() == static_cast<int32_t>(InvokeError::Kind::SEND_REQUEST),
              "collateral: dying request reports SEND_REQUEST");

        // shutdown 收掉仍在途的那条：回调必须落地，不能悄悄消失
        client.shutdown();
        CHECK(quietResult.fired.load() == 1, "shutdown: in-flight callback still delivered");
        CHECK(quietResult.kind.load() == static_cast<int32_t>(InvokeError::Kind::SEND_REQUEST),
              "shutdown: reported as a send failure");
    }

    // ------------------- 5. 断连判死之后，同一个地址还能照常建新连接、跑完新请求
    // failFast 只该收拾"已经不会有人响应"的那些请求：地址本身是干净的，下一次调用
    // 必须能重新建连并拿到真应答。（按 Connection 对象身份认领就是在守这个窗口：
    // 旧读线程收尾时，同地址的新连接可能已经有请求在途。）
    {
        PeerServer drop(Mode::Drop, 1);
        RemotingClient client(2000, kLongTimeout);

        RemotingCommand first = RemotingCommand::createRequestCommand(RequestCode::HEART_BEAT);
        AsyncResult firstResult;
        client.invokeAsync(
            drop.address(), first,
            [&firstResult](const RemotingCommand& /*resp*/, const InvokeError& error) {
                ++firstResult.fired;
                firstResult.kind.store(static_cast<int32_t>(error.kind));
            },
            kLongTimeout);
        CHECK(waitUntil([&firstResult] { return firstResult.fired.load() == 1; }),
              "reuse: the dead connection's request failed fast");
        CHECK(firstResult.kind.load() == static_cast<int32_t>(InvokeError::Kind::SEND_REQUEST),
              "reuse: failed request reports SEND_REQUEST");

        // 同地址再来一次同步调用：服务端这条新连接正常应答
        PeerServer live(Mode::Reply, 1);
        RemotingClient client2(2000, kLongTimeout);
        RemotingCommand second = RemotingCommand::createRequestCommand(RequestCode::HEART_BEAT);
        RemotingCommand resp2;
        bool threw = false;
        try {
            resp2 = client2.invokeSync(live.address(), second, 5000);
        } catch (const RemotingException&) {
            threw = true;
        }
        CHECK(!threw && resp2.code == ResponseCode::SUCCESS,
              "reuse: a fresh connection to a peer is unaffected");
        client.shutdown();
        client2.shutdown();
    }

    std::cout << "[PASS] " << g_pass << " checks, [FAIL] " << g_fail << "\n";
    return g_fail == 0 ? 0 : 1;
}
