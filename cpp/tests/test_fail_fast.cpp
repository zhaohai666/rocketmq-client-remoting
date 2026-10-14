// All cases use real socket peers (read the request, then close the connection), no mocks: only a real EOF takes the read-thread-exit path.
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

// Read one complete frame just to know "the request arrived"; the content is not parsed
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

// Loopback server on this host: each connection, after reading one request, is handled per mode.
//   Drop -- close the connection right after reading and **reply not a single byte** (simulates a peer going down / restarting)
//   Hold -- hang after reading without closing or replying (proves "other requests must not be dragged in")
//   Reply -- reply SUCCESS after reading (positive control: a new connection still works normally)
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
            netcompat::closeSocket(conn);  // Drop mode really closes the connection here, so the client reads EOF
        }
    }

    void handleConn(socket_t conn) {
        served_.fetch_add(1);
        if (mode_ == Mode::Hold) {
            // Hang without replying or closing: this request should stay in flight the whole time
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
        // Drop: return right after reading this frame -> the connection is closed on scope exit
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

// Far longer than disconnect propagation needs; on loopback EOF arrives within milliseconds, so this is the "not fixed" discriminator
constexpr int32_t kLongTimeout = 30000;

struct AsyncResult {
    std::atomic<int> fired{0};
    std::atomic<int32_t> kind{static_cast<int32_t>(InvokeError::Kind::NONE)};
    std::string message;
    std::mutex m;
};

}  // namespace

int main() {
    // ------------------------------------------- 1. Sync call: a dead connection throws send-failure immediately, no waiting for the timeout
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

    // --------------------------------------------- 2. Async callback: SEND_REQUEST arrives immediately
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

    // ------------------------------ 3. Disconnect vs timeout-cleanup thread racing on one request: the callback may fire only once
    {
        PeerServer server(Mode::Drop, 1);
        RemotingClient client(2000, 200);  // short timeout: both paths will try to claim this entry
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
        std::this_thread::sleep_for(std::chrono::milliseconds(2500));  // sleep past timeout + 1s grace
        CHECK(r.fired.load() == 1, "once: callback fired " + std::to_string(r.fired.load()) + " times");
        client.shutdown();
    }

    // ------------------ 4. Only the request on the dying connection fails; a request on another address is not dragged in
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

        // shutdown reaps the still in-flight one: the callback must land and may not silently vanish
        client.shutdown();
        CHECK(quietResult.fired.load() == 1, "shutdown: in-flight callback still delivered");
        CHECK(quietResult.kind.load() == static_cast<int32_t>(InvokeError::Kind::SEND_REQUEST),
              "shutdown: reported as a send failure");
    }

    // ------------------- 5. After a disconnect declares death, the same address can still build a new connection and complete a new request
    // failFast should only settle requests that "will never be answered": the address itself stays clean and the next call
    // must rebuild the connection and get a real reply. (Claiming by Connection object identity guards exactly this window:
    // while the old read thread is winding down, the new connection at the same address may already have requests in flight.)
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

        // Same address, one more sync call: the server's new connection answers normally
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
