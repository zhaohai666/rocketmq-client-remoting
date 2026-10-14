// Transport-layer unit test: really starts a local TCP server, uses RemotingClient to really connect, really send, really receive.
//
// Coverage (all real socket behavior, no mocks):
//   1. invokeSync normal send/receive + opaque matching + connection reuse
//   2. partial / coalesced frames: the server splits the response into two sends, the client must reassemble correctly
//   3. invokeAsync callback fires on the reader thread
//   4. invokeOneway returns as soon as it is sent
//   5. connect failure -> RemotingConnectException
//   6. server never replies -> RemotingTimeoutException (and does not poison later calls)
//   7. closeChannel / isChannelWritable / shutdown are idempotent
#include <atomic>
#include <chrono>
#include <cstdint>
#include <exception>
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

// Print one progress marker line (leading `[`) on entering each section. When CI kills the whole case at 120s via ctest,
// the partial output captured by --output-on-failure shows which section it stopped in, and with the stack watchdog pinpoints the frame.
// The accumulated milliseconds distinguish two kinds of red: a section that itself burns tens of seconds (the budget amplified by retries),
// and being fully parked after entering the section (truly parked). Locally it is always green (on macOS closing the fd wakes a blocked recv),
// so this kind of information can only be obtained from CI.
//
// The start point must be the **shared** one: writing the static directly in the macro initializes one copy per `}` block,
// so every line prints +0ms, which is as good as having no duration information at all.
#define STEP(label) std::cout << "[transport] " << (label) << " +" << transportElapsedMs() << "ms" << std::endl

namespace {

// Milliseconds since process start: all STEP lines share this single start point.
int64_t transportElapsedMs() {
    static const std::chrono::steady_clock::time_point began = std::chrono::steady_clock::now();
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::steady_clock::now() - began)
        .count();
}

using netcompat::socket_t;
using netcompat::socklen_type;
using netcompat::kInvalidSocket;

// Every wait gets an explicit upper bound; do not fall back on the constructor defaults.
//
// The default invoke timeout is 15s: this case has seven synchronous calls, and the worst path (all of them timing out) accumulates 100s+;
// on a low-spec CI runner the whole case ends as "***Timeout 120 sec" — not a single assertion result is visible, making "slow" and "hung"
// indistinguishable. With the budget squeezed to seconds, the same worst case still finishes and prints exactly which assertion FAILed.
constexpr int32_t kConnectTimeoutMs = 1000;
constexpr int32_t kInvokeTimeoutMs = 4000;
// The GO_AWAY deadline is the budget shared by "first try + reconnect + resend"; keep one extra tier
constexpr int32_t kGoAwayDeadlineMs = 5000;

// Blocking read of n bytes; returning false means the peer closed, an error occurred, or a stop request was received.
//
// Poll with select instead of a bare recv: on POSIX, closing the local fd does not wake a thread blocked in recv;
// while a server thread is stuck "waiting for the next request", ~TestServer's join can only wait for the ctest timeout.
// A 100ms tick both sees stop_ in time and does not spin the CPU.
bool readN(socket_t s, char* buf, size_t n, const std::atomic<bool>& stop) {
    size_t got = 0;
    while (got < n) {
        fd_set rfds;
        FD_ZERO(&rfds);
        FD_SET(s, &rfds);
        timeval tv;
        tv.tv_sec = 0;
        tv.tv_usec = 100 * 1000;
        const int ready = ::select(netcompat::selectNfds(s), &rfds, nullptr, nullptr, &tv);
        if (ready == 0) {
            if (stop.load()) {
                return false;
            }
            continue;  // no data and no stop signal, keep waiting
        }
        if (ready < 0) {
            return false;
        }
        int r = static_cast<int>(::recv(s, buf + got, static_cast<int>(n - got), 0));
        if (r <= 0) {
            return false;
        }
        got += static_cast<size_t>(r);
    }
    return true;
}

// Read one complete frame (including the 4-byte totalLength prefix)
bool readFrame(socket_t s, Bytes& out, const std::atomic<bool>& stop) {
    char lenBuf[4];
    if (!readN(s, lenBuf, 4, stop)) {
        return false;
    }
    Bytes lenStr(lenBuf, 4);
    int32_t totalLen = ByteReader::getInt32At(lenStr, 0);
    if (totalLen <= 0 || totalLen > RemotingClient::MAX_FRAME_LENGTH) {
        return false;
    }
    Bytes body(static_cast<size_t>(totalLen), '\0');
    if (!readN(s, &body[0], static_cast<size_t>(totalLen), stop)) {
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
    Normal,    // reply normally
    HalfSplit, // split the response into two sends (tests partial-frame reassembly)
    Silent,    // receive but never reply (tests timeout)
    GoAway     // the first goAwayConns_ connections all reply GO_AWAY(1500); later connections respond normally
};

// Local loopback test server: accepts one connection and handles a few requests per mode
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
        addr.sin_port = 0;  // let the kernel assign a port
        ::bind(listenSock_, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr));
        ::listen(listenSock_, 4);
        socklen_type len = sizeof(addr);
        ::getsockname(listenSock_, reinterpret_cast<struct sockaddr*>(&addr), &len);
        port_ = ntohs(addr.sin_port);
        thread_ = std::thread([this]() { serve(); });
    }

    ~TestServer() {
        // Set the flag first, then join, only then close the listen socket: on Windows closesocket does not
        // wake a thread blocked in accept, so closing the handle before the join would let serve() use an
        // already-invalidated socket. accept and the per-connection reads both poll with select for 100ms
        // and check stop_, so this join is bounded whether or not the client ever closed its connection.
        stop_.store(true);
        if (thread_.joinable()) {
            thread_.join();
        }
        netcompat::closeSocket(listenSock_);
    }

    std::string address() const { return "127.0.0.1:" + std::to_string(port_); }
    int served() const { return served_.load(); }
    int connections() const { return conns_.load(); }
    // opaques the server has seen: a GO_AWAY resend must carry a **new** opaque, asserted via this
    std::vector<int32_t> opaques() const {
        std::lock_guard<std::mutex> lk(opaqueMutex);
        return opaqueLog_;
    }

private:
    void serve() {
        // accept in a loop: the client reconnects after closeChannel, so the server must be
        // willing to accept a second time. Poll with select instead of a bare accept: that way
        // the thread exits within 100ms after stop_ is set (Windows' closesocket does not wake a blocked accept).
        while (!stop_.load() && served_.load() < maxRequests_) {
            fd_set rfds;
            FD_ZERO(&rfds);
            FD_SET(listenSock_, &rfds);
            timeval tv;
            tv.tv_sec = 0;
            tv.tv_usec = 100 * 1000;
            const int ready = ::select(netcompat::selectNfds(listenSock_), &rfds, nullptr, nullptr, &tv);
            if (ready == 0) {
                continue;  // timed out, go back and check stop_
            }
            if (ready < 0) {
                break;  // the listen socket has been closed
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
            if (!readFrame(conn, frame, stop_)) {
                break;  // peer closed, or a partial frame was interrupted
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
                // Do not reply, and **do not close the connection**: this case tests timeout cleanup when the
                // peer never stops receiving (the scanResponseTable contract). An earlier version slept 1200ms
                // and returned, i.e. closed before timeout + grace — failFast delivered the callback first.
                while (!stop_.load()) {
                    std::this_thread::sleep_for(std::chrono::milliseconds(50));
                }
                return;
            }
            RemotingCommand resp;
            resp.code = mode_ == ServerMode::GoAway && connIndex < goAwayConns_
                            ? ResponseCode::GO_AWAY
                            : ResponseCode::SUCCESS;
            resp.opaque = req.opaque;  // must echo back the same opaque
            resp.markResponseType();
            resp.remark = "ok";
            resp.hasRemark = true;
            resp.extFields["echoCode"] = std::to_string(req.code);
            Bytes out = resp.encode();
            if (mode_ == ServerMode::HalfSplit && out.size() > 4) {
                // send 3 bytes first (short of the length prefix), then the rest — forces the client to handle a partial frame
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
    // ---------------------------------------------------------- 1. normal sync call + connection reuse
    STEP("1 sync + reuse");
    {
        TestServer server(ServerMode::Normal, 4);
        RemotingClient client(kConnectTimeoutMs, kInvokeTimeoutMs);
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

        // the second call should reuse the same connection
        RemotingCommand req2 = RemotingCommand::createRequestCommand(RequestCode::PULL_MESSAGE);
        RemotingCommand resp2 = client.invokeSync(addr, req2);
        CHECK(resp2.code == ResponseCode::SUCCESS, "second invokeSync ok (connection reused)");
        CHECK(resp2.opaque == req2.opaque, "second invokeSync opaque distinct/match");
        CHECK(opaque != req2.opaque, "opaque increments across requests");
        CHECK(client.connectionCount() == 1, "connection reused (still 1)");
        CHECK(server.served() >= 2, "server saw both requests");
        client.shutdown();
    }

    // ---------------------------------------------------------- 2. partial-frame reassembly
    STEP("2 half-split");
    {
        TestServer server(ServerMode::HalfSplit, 2);
        RemotingClient client(kConnectTimeoutMs, kInvokeTimeoutMs);
        RemotingCommand req = RemotingCommand::createRequestCommand(RequestCode::HEART_BEAT);
        RemotingCommand resp = client.invokeSync(server.address(), req);
        CHECK(resp.code == ResponseCode::SUCCESS, "half-split frame reassembled");
        CHECK(resp.opaque == req.opaque, "half-split opaque matched");
        CHECK(resp.remark == "ok", "half-split remark intact after reassembly");
        client.shutdown();
    }

    // ---------------------------------------------------------- 3. async callback
    STEP("3 async callback");
    {
        TestServer server(ServerMode::Normal, 2);
        RemotingClient client(kConnectTimeoutMs, kInvokeTimeoutMs);
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
        for (int i = 0; i < 600 && !called.load(); ++i) {
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

    // ------------------------------------------------------ 3b. async timeout callback
    // The peer takes the request but never replies: timeoutMillis must take effect and the callback must still
    // fire **once** (with an error). Before the fix the parameter was dropped, so both the in-flight entry and the callback hung forever.
    STEP("3b async timeout");
    {
        TestServer server(ServerMode::Silent, 1);
        RemotingClient client(kConnectTimeoutMs, kInvokeTimeoutMs);
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
        CHECK(timeoutKind.load() == static_cast<int32_t>(InvokeError::Kind::TIMEOUT),
              "invokeAsync timeout carries kind TIMEOUT");
        client.shutdown();
    }

    // ---------------------------------------------------------- 4. one-way call
    STEP("4 oneway");
    {
        TestServer server(ServerMode::Normal, 2);
        RemotingClient client(kConnectTimeoutMs, kInvokeTimeoutMs);
        RemotingCommand req = RemotingCommand::createRequestCommand(RequestCode::HEART_BEAT);
        bool threw = false;
        try {
            client.invokeOneway(server.address(), req);
        } catch (const RemotingException&) {
            threw = true;
        }
        CHECK(!threw, "invokeOneway does not throw");
        CHECK(req.isOnewayRpc(), "invokeOneway marks oneway flag");
        // wait until the server really received it
        for (int i = 0; i < 100 && server.served() < 1; ++i) {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        CHECK(server.served() >= 1, "server received oneway request");
        client.shutdown();
    }

    // ---------------------------------------------------------- 5. connect failure
    STEP("5 connect failure");
    {
        // 127.0.0.1:1 almost certainly has no service
        CHECK(throwsConnectException("127.0.0.1:1"), "connect failure -> RemotingConnectException");
    }

    // ---------------------------------------------------------- 6. timeout
    STEP("6 no response");
    {
        TestServer server(ServerMode::Silent, 2);
        CHECK(throwsTimeoutException(server.address()),
              "no response -> RemotingTimeoutException");
    }

    // ---------------------------------------------------------- 7. closeChannel / shutdown
    STEP("7 closeChannel + shutdown");
    {
        TestServer server(ServerMode::Normal, 4);
        RemotingClient client(kConnectTimeoutMs, kInvokeTimeoutMs);
        const std::string addr = server.address();
        RemotingCommand req = RemotingCommand::createRequestCommand(RequestCode::HEART_BEAT);
        client.invokeSync(addr, req);
        CHECK(client.isChannelWritable(addr), "channel writable before close");
        client.closeChannel(addr);
        CHECK(!client.isChannelWritable(addr), "channel not writable after close");
        CHECK(client.connectionCount() == 0, "connection removed after closeChannel");
        // still able to reconnect after close
        RemotingCommand req2 = RemotingCommand::createRequestCommand(RequestCode::HEART_BEAT);
        RemotingCommand resp2 = client.invokeSync(addr, req2);
        CHECK(resp2.code == ResponseCode::SUCCESS, "reconnect after closeChannel works");
        client.shutdown();
        client.shutdown();  // idempotent
        CHECK(client.connectionCount() == 0, "connectionCount 0 after shutdown");
        CHECK(!client.isChannelWritable(addr), "not writable after shutdown");
    }

    STEP("8.1 goaway retry");
    {
        // 8.1 the first connection answers GO_AWAY, the second responds normally -> the caller only sees success
        TestServer server(ServerMode::GoAway, 4, 1);
        RemotingClient client(kConnectTimeoutMs, kInvokeTimeoutMs);
        const std::string addr = server.address();
        RemotingCommand req = RemotingCommand::createRequestCommand(RequestCode::SEND_MESSAGE_V2);
        const int32_t firstOpaque = req.opaque;
        RemotingCommand resp = client.invokeSync(addr, req, kGoAwayDeadlineMs);
        CHECK(resp.code == ResponseCode::SUCCESS, "GO_AWAY: retry gets the real response");
        CHECK(server.connections() == 2, "GO_AWAY: retry must use a brand new connection");
        CHECK(server.served() == 2, "GO_AWAY: the request is sent exactly twice");
        CHECK(server.opaques().size() == 2 && server.opaques()[0] == firstOpaque
                  && server.opaques()[1] != firstOpaque,
              "GO_AWAY: the retry carries a fresh opaque, else responses mis-pair");
        client.shutdown();
    }
    STEP("8.2 goaway twice");
    {
        // 8.2 both attempts GO_AWAY -> throw instead of reconnecting forever
        TestServer server(ServerMode::GoAway, 4, 1000);
        RemotingClient client(kConnectTimeoutMs, kInvokeTimeoutMs);
        RemotingCommand req = RemotingCommand::createRequestCommand(RequestCode::SEND_MESSAGE_V2);
        bool threw = false;
        std::string what;
        try {
            client.invokeSync(server.address(), req, kGoAwayDeadlineMs);
        } catch (const RemotingSendRequestException& e) {
            threw = true;
            what = e.what();
        } catch (const std::exception& e) {
            what = e.what();
        }
        CHECK(threw, "GO_AWAY twice -> RemotingSendRequestException");
        CHECK(what.find("GO_AWAY twice") != std::string::npos,
              "GO_AWAY twice: message matches the recorded text (" + what + ")");
        CHECK(server.connections() == 2, "GO_AWAY twice: retries exactly once");
        client.shutdown();
    }
    STEP("8.3 goaway flag off");
    {
        // 8.3 with the switch off: fail directly, no reconnect on any connection
        TestServer server(ServerMode::GoAway, 4, 1000);
        RemotingClient client(kConnectTimeoutMs, kInvokeTimeoutMs);
        client.setEnableReconnectForGoAway(false);
        CHECK(!client.enableReconnectForGoAway(), "setEnableReconnectForGoAway is readable");
        RemotingCommand req = RemotingCommand::createRequestCommand(RequestCode::SEND_MESSAGE_V2);
        bool threw = false;
        std::string what;
        try {
            client.invokeSync(server.address(), req, kGoAwayDeadlineMs);
        } catch (const RemotingSendRequestException& e) {
            threw = true;
            what = e.what();
        }
        CHECK(threw, "GO_AWAY with the flag off -> RemotingSendRequestException");
        CHECK(what.find("Receive GO_AWAY from channel") != std::string::npos,
              "GO_AWAY flag off: message matches the recorded text (" + what + ")");
        CHECK(server.connections() == 1, "GO_AWAY flag off: no reconnect at all");
        client.shutdown();
    }
    STEP("8.4 async goaway retry");
    {
        TestServer server(ServerMode::GoAway, 4, 1);
        RemotingClient client(kConnectTimeoutMs, kInvokeTimeoutMs);
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
                           kGoAwayDeadlineMs);
        for (int i = 0; i < 800 && !done.load(); ++i) {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        CHECK(done.load(), "async GO_AWAY: callback fires exactly once");
        CHECK(error.empty(), "async GO_AWAY: successful retry carries no error (" + error + ")");
        CHECK(code == ResponseCode::SUCCESS, "async GO_AWAY: retry gets the real response");
        CHECK(server.connections() == 2, "async GO_AWAY: retried on a new connection");
        client.shutdown();
    }
    STEP("8.5 async goaway twice");
    {
        // 8.5 async with two GO_AWAYs -> folded into a callback error, must not be silently swallowed
        TestServer server(ServerMode::GoAway, 4, 1000);
        RemotingClient client(kConnectTimeoutMs, kInvokeTimeoutMs);
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
                           kGoAwayDeadlineMs);
        for (int i = 0; i < 800 && !done.load(); ++i) {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        CHECK(done.load(), "async GO_AWAY twice: callback fires");
        CHECK(error.find("GO_AWAY twice") != std::string::npos,
              "async GO_AWAY twice: error carries the recorded text (" + error + ")");
        CHECK(server.connections() == 2, "async GO_AWAY twice: retries exactly once");
        client.shutdown();
    }

    // ---------------------------------------------------------- 9. address parsing
    STEP("9 parse address");
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

    STEP("all sections done");
    std::cout << "transport checks: " << g_pass << " passed, " << g_fail << " failed\n";
    return g_fail == 0 ? 0 : 1;
}
