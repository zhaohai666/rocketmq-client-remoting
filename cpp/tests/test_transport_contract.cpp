// Remoting transport connection-lifecycle contract, offline against loopback fake peers.
//
//   ctest -R transport_contract
//
// Five behaviours, each measured on a real socket this process owns, and each asserted on two
// things at once: the connection table afterwards, and the log line that has to explain it.
// "The frame vanished and nothing was said" is the defect under test here, so a leg that only
// checks the table proves half of it.
//
//   T1 a frame that does not decode -- and a length prefix outside the accepted range, which is
//      the same event one level earlier -- is an ERROR naming the peer, and the connection goes
//      the way a peer-disconnected one goes; the peer's next valid request is served on a fresh
//      connection, and a *different* connection is untouched.
//   T2 a response whose opaque no request is waiting on is dropped with a WARNING naming the
//      opaque and the address, and the connection stays up.
//   T3 the writable question is about the outbound side, not about having connected: no
//      connection => true, a connection with room => true, a connection whose send buffer is
//      full => false.
//   T4 a connection idle past the limit is closed by the sweep while one still carrying traffic
//      survives; a non-positive configured limit restores the default instead of switching the
//      sweep off, because the sweep is always installed.
//   T5 a synchronous wait that runs out closes the connection it went out on by default and
//      keeps it when that is turned off; an expired in-flight entry logs one line per entry.
//
// Every wait has an explicit bound, and every peer thread exits within 100 ms of the stop flag
// (polling select rather than a bare blocking call), so a parked leg shows up as a FAIL line
// naming the assertion instead of a silent 120 s ctest timeout.
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <fstream>
#include <functional>
#include <iostream>
#include <mutex>
#include <random>
#include <string>
#include <thread>
#include <vector>

#include "rocketmq/common/byte_buffer.h"
#include "rocketmq/common/logging.h"
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

constexpr int32_t kConnectTimeoutMs = 1000;
constexpr int32_t kInvokeTimeoutMs = 3000;

void sleepMs(int ms) { std::this_thread::sleep_for(std::chrono::milliseconds(ms)); }

int64_t steadyNowMs() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::steady_clock::now().time_since_epoch())
        .count();
}

bool waitFor(const std::function<bool()>& pred, int timeoutMs) {
    const int64_t deadline = steadyNowMs() + timeoutMs;
    while (steadyNowMs() < deadline) {
        if (pred()) return true;
        sleepMs(20);
    }
    return pred();
}

// ------------------------------------------------------------------ log capture
// The transport reports these events through its own logger, so each leg points the log file at
// a fresh temp path, then reads the lines back. The level is part of the rendered line
// ("yyyy-mm-dd hh:mm:ss.mmm ERROR [pid] [thread] [file:line] - message"), which is what lets a
// leg insist on ERROR rather than a mere WARN.
std::string g_logPath;

void resetLogCapture(const char* tag) {
    static std::mt19937_64 rng{std::random_device{}()};
    // Never $HOME: this port's default log file lives there, and a capture must not append to it.
    g_logPath = "/tmp/rmq_cpp_contract_" + std::string(tag) + "_" + std::to_string(rng()) + ".log";
    std::remove(g_logPath.c_str());
    setLogFile(g_logPath);
    setLogLevel(LOG_DEBUG);
}

std::vector<std::string> logLines() {
    flushLogFile();
    std::vector<std::string> out;
    std::ifstream in(g_logPath);
    std::string line;
    while (std::getline(in, line)) {
        if (!line.empty()) out.push_back(line);
    }
    return out;
}

// Lines carrying every one of the given fragments (case-sensitive, plain substring).
std::vector<std::string> linesWith(const std::vector<std::string>& needles) {
    std::vector<std::string> out;
    for (const auto& l : logLines()) {
        bool all = true;
        for (const auto& n : needles) {
            if (l.find(n) == std::string::npos) {
                all = false;
                break;
            }
        }
        if (all) out.push_back(l);
    }
    return out;
}

// ------------------------------------------------------------------ socket helpers
bool writeAll(socket_t s, const Bytes& data) {
    size_t sent = 0;
    while (sent < data.size()) {
        const int n = static_cast<int>(
            ::send(s, data.data() + sent, static_cast<int>(data.size() - sent),
                   netcompat::sendFlags()));
        if (n <= 0) return false;
        sent += static_cast<size_t>(n);
    }
    return true;
}

// ------------------------------------------------------------------ fake peers
enum class PeerMode {
    Echo,          // answer every request with a matching SUCCESS
    Silent,        // read requests, never answer, never close
    BadHeader,     // answer the first request with an undecodable frame, echo later ones
    BadLength,     // answer the first request with an illegal length prefix, echo later ones
    StrayFirst,    // answer the first request with an orphan response, then the real one
    Sink,          // accept and never read at all (builds send-buffer backpressure)
};

// A loopback peer that keeps accepting until stopped, so a client that dropped a connection can
// reconnect to the same address. Counters are what the legs assert on: a close the peer observes
// is a close the **client** initiated, because no mode here ever closes a connection itself.
class Peer {
public:
    explicit Peer(PeerMode mode) : mode_(mode) {
        netcompat::ensureInitialized();
        listen_ = ::socket(AF_INET, SOCK_STREAM, 0);
        int one = 1;
        ::setsockopt(listen_, SOL_SOCKET, SO_REUSEADDR, reinterpret_cast<const char*>(&one),
                     static_cast<int>(sizeof(one)));
        struct sockaddr_in a;
        a.sin_family = AF_INET;
        a.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        a.sin_port = 0;
        ::bind(listen_, reinterpret_cast<struct sockaddr*>(&a), sizeof(a));
        ::listen(listen_, 8);
        socklen_type len = sizeof(a);
        ::getsockname(listen_, reinterpret_cast<struct sockaddr*>(&a), &len);
        port_ = ntohs(a.sin_port);
        thread_ = std::thread([this]() { serve(); });
    }

    ~Peer() {
        stop_.store(true);
        if (thread_.joinable()) thread_.join();
        netcompat::closeSocket(listen_);
    }

    std::string address() const { return "127.0.0.1:" + std::to_string(port_); }
    int requests() const { return requests_.load(); }
    int connections() const { return connections_.load(); }
    int clientCloses() const { return clientCloses_.load(); }
    // Switch a Sink peer from "never reads" to "drains forever", which is what frees a send
    // buffer the client parked in.
    void startDraining() { drain_.store(true); }

private:
    void serve() {
        while (!stop_.load()) {
            fd_set rfds;
            FD_ZERO(&rfds);
            FD_SET(listen_, &rfds);
            timeval tv;
            tv.tv_sec = 0;
            tv.tv_usec = 100 * 1000;
            const int ready = ::select(netcompat::selectNfds(listen_), &rfds, nullptr, nullptr, &tv);
            if (ready == 0) continue;
            if (ready < 0) break;
            socket_t conn = ::accept(listen_, nullptr, nullptr);
            if (conn == kInvalidSocket) break;
            handle(conn);
            netcompat::closeSocket(conn);
        }
    }

    void handle(socket_t conn) {
        connections_.fetch_add(1);
        if (mode_ == PeerMode::Sink) {
            // Never read: the peer's receive buffer, then the client's send buffer, fill up.
            // Only stop_ ends the connection, and draining is a leg-controlled switch.
            while (!stop_.load()) {
                if (!drain_.load()) {
                    sleepMs(20);
                    continue;
                }
                char scratch[65536];
                fd_set rfds;
                FD_ZERO(&rfds);
                FD_SET(conn, &rfds);
                timeval tv;
                tv.tv_sec = 0;
                tv.tv_usec = 100 * 1000;
                if (::select(netcompat::selectNfds(conn), &rfds, nullptr, nullptr, &tv) <= 0) {
                    continue;
                }
                if (::recv(conn, scratch, sizeof(scratch), 0) <= 0) {
                    clientCloses_.fetch_add(1);
                    return;
                }
            }
            return;
        }

        Bytes pending;
        while (!stop_.load()) {
            Bytes frame;
            if (!nextFrame(conn, pending, frame)) {
                clientCloses_.fetch_add(1);  // the client hung up (or sent us junk we cannot frame)
                return;
            }
            RemotingCommand req;
            if (!RemotingCommand::tryDecode(frame, req, nullptr)) {
                return;
            }
            const int32_t opaque = req.opaque;
            const int seen = requests_.fetch_add(1) + 1;
            if (mode_ == PeerMode::Silent) {
                continue;  // read, keep the connection, say nothing
            }
            if (mode_ == PeerMode::BadHeader && seen == 1) {
                writeAll(conn, undecodableFrame());
                continue;
            }
            if (mode_ == PeerMode::BadLength && seen == 1) {
                writeAll(conn, illegalLengthFrame());
                continue;
            }
            if (mode_ == PeerMode::StrayFirst && seen == 1) {
                writeAll(conn, responseFrame(kStrayOpaque));
            }
            writeAll(conn, responseFrame(opaque));
        }
    }

    // One complete frame off the stream, keeping any leftover bytes for the next call.
    bool nextFrame(socket_t conn, Bytes& pending, Bytes& out) {
        while (!stop_.load()) {
            if (pending.size() >= 4) {
                const int32_t total = ByteReader::getInt32At(pending, 0);
                if (total <= 0 || total > RemotingClient::MAX_FRAME_LENGTH) return false;
                if (pending.size() >= static_cast<size_t>(4 + total)) {
                    out = pending.substr(0, static_cast<size_t>(4 + total));
                    pending.erase(0, static_cast<size_t>(4 + total));
                    return true;
                }
            }
            char chunk[8192];
            fd_set rfds;
            FD_ZERO(&rfds);
            FD_SET(conn, &rfds);
            timeval tv;
            tv.tv_sec = 0;
            tv.tv_usec = 100 * 1000;
            const int ready = ::select(netcompat::selectNfds(conn), &rfds, nullptr, nullptr, &tv);
            if (ready < 0) return false;
            if (ready == 0) continue;
            const int n = static_cast<int>(::recv(conn, chunk, sizeof(chunk), 0));
            if (n <= 0) return false;
            pending.append(chunk, static_cast<size_t>(n));
        }
        return false;
    }

    static constexpr int32_t kStrayOpaque = 987654;

    static Bytes responseFrame(int32_t opaque) {
        RemotingCommand resp = RemotingCommand::createResponseCommand(ResponseCode::SUCCESS, "tc");
        resp.opaque = opaque;
        return resp.encode();
    }

    // A frame inside a legal length prefix whose own header length does not fit inside it:
    // RemotingCommand::decode rejects it, which is exactly the case the contract says is fatal
    // to the socket.
    static Bytes undecodableFrame() {
        Bytes f;
        char head[8];
        writeInt32(head, 8);            // totalLen: legal, and it claims 8 bytes follow
        writeInt32(head + 4, 0x00FFFFFF);  // header length far beyond what is on the wire
        f.append(head, 8);
        f.append("junk", 4);
        return f;
    }

    // A length prefix of -1: the reader cannot even tell how many bytes belong to the frame.
    static Bytes illegalLengthFrame() {
        Bytes f;
        char head[4];
        writeInt32(head, -1);
        f.append(head, 4);
        return f;
    }

    static void writeInt32(char* p, int32_t v) {
        p[0] = static_cast<char>((v >> 24) & 0xFF);
        p[1] = static_cast<char>((v >> 16) & 0xFF);
        p[2] = static_cast<char>((v >> 8) & 0xFF);
        p[3] = static_cast<char>(v & 0xFF);
    }

    PeerMode mode_;
    socket_t listen_ = kInvalidSocket;
    uint16_t port_ = 0;
    std::thread thread_;
    std::atomic<bool> stop_{false};
    std::atomic<bool> drain_{false};
    std::atomic<int> connections_{0};
    std::atomic<int> requests_{0};
    std::atomic<int> clientCloses_{0};
};

RemotingCommand heartBeat() {
    return RemotingCommand::createRequestCommand(RequestCode::HEART_BEAT);
}

// The transport takes the request by reference -- it may hand back a fresh opaque when the one it
// carries already has a request in flight -- so a send needs a named object, not a temporary.
void sendOneway(RemotingClient& client, const std::string& addr) {
    RemotingCommand req = heartBeat();
    client.invokeOneway(addr, req);
}

// ------------------------------------------------------------------ T1
void testUndecodableFrameClosesAndNamesThePeer() {
    std::cout << "[contract] T1 undecodable frame\n";
    Peer echo(PeerMode::Echo);
    Peer bad(PeerMode::BadHeader);
    RemotingClient client(kConnectTimeoutMs, kInvokeTimeoutMs);

    // A second, healthy connection that must survive the whole leg.
    RemotingCommand probe = heartBeat();
    CHECK(client.invokeSync(echo.address(), probe).code == ResponseCode::SUCCESS,
          "T1: the healthy connection round-trips before the broken frame");

    resetLogCapture("t1_decode");
    RemotingCommand req = heartBeat();
    bool threw = false;
    try {
        client.invokeSync(bad.address(), req, 2000);
    } catch (const std::exception&) {
        threw = true;
    }
    const bool closed = waitFor([&]() { return bad.clientCloses() >= 1; }, 3000);

    CHECK(threw, "T1: the request that got the undecodable frame does not return a response");
    CHECK(closed, "T1: the client takes the connection down, not just the frame");
    CHECK(client.connectionCount() == 1, "T1: only the broken connection leaves the table");

    const auto errors = linesWith({"ERROR", "cannot decode frame"});
    CHECK(errors.size() == 1, "T1: exactly one ERROR for the undecodable frame");
    CHECK(!errors.empty() && errors[0].find(bad.address()) != std::string::npos,
          "T1: the ERROR names the peer the frame came from");
    CHECK(!errors.empty() && errors[0].find("closing the connection") != std::string::npos,
          "T1: the ERROR says the connection is going, not merely that the frame is bad");
    CHECK(linesWith({"cannot decode frame"}).size() == errors.size(),
          "T1: the frame is reported once, at ERROR level");

    // The point of closing: the same peer's next request is served on a fresh connection.
    RemotingCommand retry = heartBeat();
    bool okOnFresh = false;
    try {
        okOnFresh = client.invokeSync(bad.address(), retry, 3000).code == ResponseCode::SUCCESS;
    } catch (const std::exception&) {
        okOnFresh = false;
    }
    CHECK(okOnFresh, "T1: the peer's next valid request is answered on a fresh connection");
    CHECK(bad.connections() == 2, "T1: and it really is a second connection, not the broken one");

    RemotingCommand probe2 = heartBeat();
    CHECK(client.invokeSync(echo.address(), probe2).code == ResponseCode::SUCCESS,
          "T1: the other connection was untouched");
    CHECK(echo.clientCloses() == 0, "T1: the healthy peer never saw a close");
    client.shutdown();
}

void testBadFrameLengthTakesTheConnectionDown() {
    std::cout << "[contract] T1 bad length prefix\n";
    Peer echo(PeerMode::Echo);
    Peer bad(PeerMode::BadLength);
    RemotingClient client(kConnectTimeoutMs, kInvokeTimeoutMs);
    RemotingCommand probe = heartBeat();
    client.invokeSync(echo.address(), probe);

    resetLogCapture("t1_length");
    RemotingCommand req = heartBeat();
    bool threw = false;
    try {
        client.invokeSync(bad.address(), req, 2000);
    } catch (const std::exception&) {
        threw = true;
    }
    CHECK(threw, "T1 length: the request does not return a response");
    CHECK(waitFor([&]() { return bad.clientCloses() >= 1; }, 3000),
          "T1 length: an out-of-range length prefix closes the connection");

    const auto errors = linesWith({"ERROR", "illegal frame length"});
    CHECK(errors.size() == 1, "T1 length: one ERROR for the bad length prefix");
    CHECK(!errors.empty() && errors[0].find(bad.address()) != std::string::npos,
          "T1 length: the ERROR names the peer");
    CHECK(linesWith({"cannot decode frame"}).empty(),
          "T1 length: a broken length prefix is not also reported as a decode failure");

    RemotingCommand probe2 = heartBeat();
    CHECK(client.invokeSync(echo.address(), probe2).code == ResponseCode::SUCCESS,
          "T1 length: the other connection is untouched");
    client.shutdown();
}

// ------------------------------------------------------------------ T2
void testUnmatchedResponseWarnsAndKeepsTheConnection() {
    std::cout << "[contract] T2 unmatched response\n";
    Peer peer(PeerMode::StrayFirst);
    RemotingClient client(kConnectTimeoutMs, kInvokeTimeoutMs);

    resetLogCapture("t2");
    RemotingCommand req = heartBeat();
    int32_t code = -1;
    try {
        code = client.invokeSync(peer.address(), req, 3000).code;
    } catch (const std::exception&) {
        code = -1;
    }
    const auto warns = linesWith({"response from", "does not match any in-flight request"});
    CHECK(code == ResponseCode::SUCCESS, "T2: the real answer still reaches the caller");
    CHECK(warns.size() == 1, "T2: the dropped response is logged once");
    CHECK(!warns.empty()
              && warns[0].find(std::to_string(987654)) != std::string::npos,
          "T2: the warning names the opaque nobody was waiting for");
    CHECK(!warns.empty() && warns[0].find(peer.address()) != std::string::npos,
          "T2: and the address it arrived from");
    CHECK(peer.clientCloses() == 0, "T2: a correctly framed stray response keeps the connection");
    CHECK(client.connectionCount() == 1, "T2: and it is still in the table");

    // A follow-up on the same connection proves it is really usable, not merely unreaped.
    RemotingCommand again = heartBeat();
    CHECK(client.invokeSync(peer.address(), again, 3000).code == ResponseCode::SUCCESS,
          "T2: the surviving connection still serves the next request");
    CHECK(peer.connections() == 1, "T2: and it is the same connection, not a reconnect");
    client.shutdown();
}

// ------------------------------------------------------------------ T3
void testWritable() {
    std::cout << "[contract] T3 writable\n";
    RemotingClient client(kConnectTimeoutMs, kInvokeTimeoutMs);
    CHECK(client.isChannelWritable("127.0.0.1:1"),
          "T3: an address with no connection is writable -- the next send connects");
    CHECK(client.connectionCount() == 0, "T3: and answering the question created nothing");

    Peer drained(PeerMode::Silent);
    sendOneway(client, drained.address());
    CHECK(client.isChannelWritable(drained.address()),
          "T3: a live connection whose send buffer has room is writable");

    // The one unwritable answer: a peer that accepts and never reads, so the kernel buffers fill
    // and the client's own send parks inside sendAll. A writer thread holds that park while the
    // leg asks the question.
    Peer sink(PeerMode::Sink);
    RemotingClient sinkClient(kConnectTimeoutMs, kInvokeTimeoutMs);
    sendOneway(sinkClient, sink.address());  // establishes the pooled socket
    std::atomic<bool> stopSender{false};
    std::atomic<bool> sawFalse{false};
    std::atomic<bool> sawTrueAgain{false};
    const std::string sinkAddr = sink.address();

    std::thread writer([&]() {
        Bytes big(256 * 1024, 'x');
        RemotingCommand req = heartBeat();
        req.body = big;
        for (int i = 0; i < 4096 && !stopSender.load(); ++i) {
            try {
                sinkClient.invokeOneway(sinkAddr, req);
            } catch (const std::exception&) {
                break;  // the connection went away; the leg has what it needs
            }
        }
    });

    const bool saturated = waitFor([&]() {
        if (!sinkClient.isChannelWritable(sinkAddr)) {
            sawFalse.store(true);
            return true;
        }
        return false;
    }, 8000);
    CHECK(saturated, "T3: the peer that never reads really filled the send buffer");
    CHECK(sawFalse.load() && !sinkClient.isChannelWritable(sinkAddr),
          "T3: a live connection with a full send buffer is the unwritable case");

    sink.startDraining();
    const bool drainedAgain = waitFor([&]() {
        if (sinkClient.isChannelWritable(sinkAddr)) {
            sawTrueAgain.store(true);
            return true;
        }
        return false;
    }, 8000);
    CHECK(drainedAgain, "T3: once there is room again the same connection is writable");
    stopSender.store(true);
    if (writer.joinable()) writer.join();
    sinkClient.shutdown();
    client.shutdown();
}

// ------------------------------------------------------------------ T4
void testIdleSweep() {
    std::cout << "[contract] T4 idle sweep\n";
    Peer quiet(PeerMode::Silent);
    Peer busy(PeerMode::Silent);
    RemotingClient client(kConnectTimeoutMs, kInvokeTimeoutMs);

    client.setChannelIdleTimeoutMillis(400);
    CHECK(client.channelIdleTimeoutMillis() == 400, "T4: a positive limit is kept as configured");

    sendOneway(client, quiet.address());
    sendOneway(client, busy.address());

    std::atomic<bool> stopBeat{false};
    std::thread beat([&]() {
        while (!stopBeat.load()) {
            try {
                sendOneway(client, busy.address());
            } catch (const std::exception&) {
                break;
            }
            sleepMs(100);  // well inside the 400 ms idle limit
        }
    });

    resetLogCapture("t4");
    const bool reaped = waitFor([&]() { return quiet.clientCloses() >= 1; }, 5000);
    const auto warns = linesWith({"WARN", "idle for", "closing it"});
    CHECK(reaped, "T4: the connection that stopped carrying traffic was closed");
    CHECK(warns.size() == 1, "T4: exactly one close is logged for it");
    CHECK(!warns.empty() && warns[0].find(quiet.address()) != std::string::npos,
          "T4: the warning names the address it closed");
    CHECK(client.connectionCount() == 1, "T4: only the idle connection left the table");
    stopBeat.store(true);
    if (beat.joinable()) beat.join();
    CHECK(client.isChannelWritable(busy.address()) && busy.clientCloses() == 0,
          "T4: a connection carrying traffic every 100 ms is never the idle one");
    CHECK(client.sweepIdleConnections() == 0,
          "T4: and an explicit pass over what is left closes nothing more");

    client.shutdown();
}

void testIdleLimitNeverDisablesTheSweep() {
    std::cout << "[contract] T4 idle knob\n";
    RemotingClient client(kConnectTimeoutMs, kInvokeTimeoutMs);
    CHECK(client.channelIdleTimeoutMillis() == RemotingClient::DEFAULT_CHANNEL_IDLE_TIMEOUT_MILLIS,
          "T4 knob: an untouched client carries the 120 s default");
    const int64_t bad[] = {0, -1, -120};
    for (int64_t v : bad) {
        client.setChannelIdleTimeoutMillis(v);
        CHECK(client.channelIdleTimeoutMillis() == RemotingClient::DEFAULT_CHANNEL_IDLE_TIMEOUT_MILLIS,
              "T4 knob: a non-positive value restores the default instead of disabling the sweep");
    }
    client.setChannelIdleTimeoutMillis(30000);
    CHECK(client.channelIdleTimeoutMillis() == 30000, "T4 knob: a positive value is honoured");
    client.shutdown();

    // A quiet connection is reaped by the sweep with the default limit only after minutes, but a
    // small configured limit is reaped by the transport's own pass -- no explicit call from us.
    Peer quiet(PeerMode::Silent);
    RemotingClient auto_(kConnectTimeoutMs, kInvokeTimeoutMs);
    auto_.setChannelIdleTimeoutMillis(300);
    sendOneway(auto_, quiet.address());
    resetLogCapture("t4_auto");
    CHECK(waitFor([&]() { return quiet.clientCloses() >= 1; }, 5000),
          "T4 knob: the transport drives its own idle sweep, without any caller-side tick");
    auto_.shutdown();
}

// ------------------------------------------------------------------ T5
void testExpiredEntriesWarnOneEach() {
    std::cout << "[contract] T5 expiry sweep\n";
    Peer peer(PeerMode::Silent);
    RemotingClient client(kConnectTimeoutMs, kInvokeTimeoutMs);

    std::atomic<int> ok{0};
    std::atomic<int> timeouts{0};
    auto cb = [&](const RemotingCommand&, const InvokeError& err) {
        if (err.kind == InvokeError::Kind::TIMEOUT) {
            ++timeouts;
        } else if (err.empty()) {
            ++ok;
        }
    };
    resetLogCapture("t5");
    RemotingCommand a = heartBeat();
    RemotingCommand b = heartBeat();
    client.invokeAsync(peer.address(), a, cb, 200);
    client.invokeAsync(peer.address(), b, cb, 200);
    // The sweep reclaims an entry one grace period after its deadline, so this waits on time
    // rather than on a connection event.
    const bool settled = waitFor([&]() { return timeouts.load() == 2; }, 6000);
    const auto warns = linesWith({"WARN", "expired", "failed with a timeout"});
    CHECK(settled, "T5: both abandoned requests get their timeout back");
    CHECK(timeouts.load() == 2 && ok.load() == 0, "T5: exactly one timeout per entry, no success");
    CHECK(warns.size() == 2, "T5: one warning line per reclaimed entry");
    CHECK(warns.size() == 2 && warns[0].find(peer.address()) != std::string::npos,
          "T5: the warning names the address the request went to");
    CHECK(client.pendingRequestCount() == 0, "T5: the in-flight table is left empty");
    CHECK(peer.clientCloses() == 0, "T5: reclaiming a request is not a connection event");
    client.shutdown();
}

// ------------------------------------------------------------------ T6
// One sync wait that runs out against a peer that accepted and never answered.
struct SyncTimeoutRun {
    bool threw = false;
    bool timedOut = false;
    bool alive = true;
    size_t closeWarns = 0;
    size_t timeoutWarns = 0;
};

SyncTimeoutRun runSyncTimeout(PeerMode mode, bool closeOnTimeout, int32_t timeoutMillis) {
    Peer peer(mode);
    RemotingClient client(kConnectTimeoutMs, kInvokeTimeoutMs);
    client.setCloseSocketOnSyncTimeout(closeOnTimeout);
    const std::string addr = peer.address();
    // Bring the connection up first, so "kept" and "closed" are statements about a socket that
    // really exists.
    sendOneway(client, addr);
    CHECK(client.closeSocketOnSyncTimeout() == closeOnTimeout,
          "T6: the knob reads back as configured");

    resetLogCapture("t6");
    SyncTimeoutRun r;
    RemotingCommand req = heartBeat();
    try {
        client.invokeSync(addr, req, timeoutMillis);
    } catch (const RemotingTimeoutException&) {
        r.threw = true;
        r.timedOut = true;
    } catch (const std::exception&) {
        r.threw = true;
    }
    sleepMs(300);  // let a close reach the peer before the counters are read
    r.alive = peer.clientCloses() == 0;
    r.closeWarns = linesWith({"closing the connection"}).size();
    r.timeoutWarns = linesWith({"wait response from", "timeout"}).size();
    client.shutdown();
    return r;
}

void testSyncTimeoutClosesTheConnection() {
    std::cout << "[contract] T6 sync timeout\n";
    const SyncTimeoutRun on = runSyncTimeout(PeerMode::Silent, true, 400);
    CHECK(on.threw && on.timedOut, "T6: the caller still gets the timeout it asked for");
    CHECK(!on.alive, "T6: by default the connection the wait ran out on is closed");
    CHECK(on.closeWarns == 1, "T6: the close is logged");
    CHECK(on.timeoutWarns == 1, "T6: and so is the timeout itself");

    const SyncTimeoutRun off = runSyncTimeout(PeerMode::Silent, false, 400);
    CHECK(off.threw && off.timedOut, "T6 off: the timeout is still reported to the caller");
    CHECK(off.alive, "T6 off: with the knob off the socket is kept -- the timeout is caller-side");
    CHECK(off.closeWarns == 0, "T6 off: no close is logged");
    CHECK(off.timeoutWarns == 1, "T6 off: the timeout line still appears");
}

// The two floors of the close decision, on the only budget this port can hand the wait.
//
// This transport does not subtract the connect cost, so the time still available when the wait
// began is the whole timeout. The floors then leave exactly one shape that keeps the socket: a
// budget of zero, which no wait can ever be answered on.
void testSyncTimeoutCloseRuleFloors() {
    std::cout << "[contract] T6 floors\n";
    const SyncTimeoutRun zero = runSyncTimeout(PeerMode::Silent, true, 0);
    CHECK(zero.threw, "T6 floor: a zero budget still reports the timeout to the caller");
    CHECK(zero.alive, "T6 floor: a zero budget is at both floors, so the connection is kept");
    CHECK(zero.closeWarns == 0, "T6 floor: and nothing is closed");

    const SyncTimeoutRun justAbove = runSyncTimeout(PeerMode::Silent, true, 101);
    CHECK(!justAbove.alive, "T6 floor: one millisecond above the floor is a close, not a keep");
    CHECK(justAbove.closeWarns == 1, "T6 floor: and the close is logged once");
}

}  // namespace

int main() {
    netcompat::ensureInitialized();
    testUndecodableFrameClosesAndNamesThePeer();
    testBadFrameLengthTakesTheConnectionDown();
    testUnmatchedResponseWarnsAndKeepsTheConnection();
    testWritable();
    testIdleSweep();
    testIdleLimitNeverDisablesTheSweep();
    testExpiredEntriesWarnOneEach();
    testSyncTimeoutClosesTheConnection();
    testSyncTimeoutCloseRuleFloors();
    setLogFile("");  // back to the default target once the legs are done

    std::cout << "\ntransport contract: " << g_pass << " checks, " << g_fail << " failed\n";
    return g_fail == 0 ? 0 : 1;
}
