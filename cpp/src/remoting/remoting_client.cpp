// RemotingClient implementation: a request/response model over blocking sockets with one reader thread per connection.
//
// Two easy traps, both handled here:
//   1. SIGPIPE: writing to a socket already closed by the peer raises SIGPIPE, whose default action **kills the process**.
//      On Linux use send(..., MSG_NOSIGNAL); on macOS/BSD use the SO_NOSIGPIPE socket option.
//   2. Reader exit: readers must not sit in a blocking recv for long, otherwise shutdown cannot reap them promptly.
//      select() with a 300ms timeout is used instead: prompt exit without a CPU-burning spin.
#include "rocketmq/remoting/remoting_client.h"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstring>
#include <exception>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#include "rocketmq/common/byte_buffer.h"
#include "rocketmq/common/logging.h"
#include "rocketmq/common/net_compat.h"
#include "rocketmq/remoting/exception.h"
#include "rocketmq/remoting/protocol/codes.h"
#include "rocketmq/remoting/socks_proxy.h"
#include "tls_session.h"

#ifndef _WIN32
#include <fcntl.h>  // used for non-blocking connect (fcntl/F_GETFL/F_SETFL)
#endif

namespace rocketmq {

namespace {

// Platform socket primitives all come from netcompat (see include/rocketmq/common/net_compat.h).
// using declarations bring them into file scope so names like socket_t/closeSocket stay unchanged below.
using netcompat::closeSocket;
using netcompat::ensureInitialized;
using netcompat::kInvalidSocket;
using netcompat::sendFlags;
using netcompat::socket_t;
using netcompat::tuneSocket;

// connect with a timeout: set non-blocking first, select for writability, then restore blocking mode
socket_t connectWithTimeout(const std::string& host, const std::string& port,
                            int32_t timeoutMillis, std::string& err) {
    struct addrinfo hints;
    std::memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    struct addrinfo* res = nullptr;
    if (::getaddrinfo(host.c_str(), port.c_str(), &hints, &res) != 0 || res == nullptr) {
        err = "cannot resolve host " + host;
        return kInvalidSocket;
    }

    socket_t sock = kInvalidSocket;
    for (struct addrinfo* ai = res; ai != nullptr; ai = ai->ai_next) {
        sock = ::socket(ai->ai_family, ai->ai_socktype, ai->ai_protocol);
        if (sock == kInvalidSocket) {
            continue;
        }
#ifdef _WIN32
        u_long nb = 1;
        ::ioctlsocket(sock, FIONBIO, &nb);
#else
        int flags = ::fcntl(sock, F_GETFL, 0);
        ::fcntl(sock, F_SETFL, flags | O_NONBLOCK);
#endif
        int rc = ::connect(sock, ai->ai_addr, static_cast<int>(ai->ai_addrlen));
        bool connected = (rc == 0);
        if (!connected) {
            fd_set wfds;
            FD_ZERO(&wfds);
            FD_SET(sock, &wfds);
            struct timeval tv;
            tv.tv_sec = timeoutMillis / 1000;
            tv.tv_usec = (timeoutMillis % 1000) * 1000;
            int sel = ::select(netcompat::selectNfds(sock), nullptr, &wfds, nullptr, &tv);
            if (sel > 0) {
                // Writability does not prove the connection succeeded; SO_ERROR must confirm it
                int soErr = 0;
                netcompat::socklen_type len = sizeof(soErr);
                ::getsockopt(sock, SOL_SOCKET, SO_ERROR,
                             reinterpret_cast<char*>(&soErr), &len);
                connected = (soErr == 0);
            }
        }
        if (connected) {
            // Restore blocking mode; the read loop paces itself with select()
#ifdef _WIN32
            u_long blk = 0;
            ::ioctlsocket(sock, FIONBIO, &blk);
#else
            int flags2 = ::fcntl(sock, F_GETFL, 0);
            ::fcntl(sock, F_SETFL, flags2 & ~O_NONBLOCK);
#endif
            tuneSocket(sock);
            break;
        }
        closeSocket(sock);
        sock = kInvalidSocket;
    }
    ::freeaddrinfo(res);
    if (sock == kInvalidSocket) {
        err = "connect failed to " + host + ":" + port;
    }
    return sock;
}

// Write all bytes; false means the peer closed the connection or an error occurred
bool sendAll(socket_t sock, const Bytes& data) {
    size_t sent = 0;
    const int flags = sendFlags();
    while (sent < data.size()) {
        int n = static_cast<int>(::send(sock, data.data() + sent,
                                        static_cast<int>(data.size() - sent), flags));
        if (n <= 0) {
            return false;
        }
        sent += static_cast<size_t>(n);
    }
    return true;
}

std::string formatAddr(const std::string& addr) { return addr; }

}  // namespace

// ---------------------------------------------------------------- Impl
struct RemotingClient::Impl {
    // Upper bound on any single blocking wait in the reader: the same order as select's 300ms, so exiting waits at most one more tick.
    static constexpr int32_t kReaderTimeoutMs = 300;

    struct Connection {
        std::string addr;
        // The reader re-claims it every round (see readLoop): other threads close the fd and reset it
        // to kInvalidSocket on write failure, closeChannel and shutdown, and the kernel immediately hands the
        // recycled fd number to a new connection. A plain int makes this cross-thread read/write a data race; atomic is the only honest form.
        std::atomic<socket_t> sock{kInvalidSocket};
        std::mutex writeMutex;
        std::atomic<bool> readerDone{false};
        // Monotonic millis of the last byte actually read or written on this connection; the
        // idle sweep closes a connection whose stamp is older than the configured limit.
        std::atomic<int64_t> lastActivityMs{0};
        // TLS session (non-null when tlsEnable; read/write go through SSL_*)
        std::unique_ptr<TlsSession> tls;
    };

    struct Future {
        std::mutex m;
        std::condition_variable cv;
        bool done = false;
        bool hasResponse = false;
        RemotingCommand response;
        InvokeCallback callback;
        std::string addr;
        int64_t deadlineMs = 0;
        int64_t timeoutMs = 0;
        bool callbackFired = false;
        std::shared_ptr<Connection> conn;
        InvokeError failure;

        static int64_t monoNowMs() {
            return std::chrono::duration_cast<std::chrono::milliseconds>(
                       std::chrono::steady_clock::now().time_since_epoch())
                .count();
        }
    };

    std::atomic<bool> running{true};
    int32_t connectTimeout = 3000;
    int32_t invokeTimeout = 15000;
    bool enableReconnectForGoAway = true;
    bool tlsEnable = false;
    std::shared_ptr<void> tlsCtx;
    TlsOptions tlsOptions;

    // Idle limit for one connection; a non-positive configured value is normalised to the
    // default at the setter, so the sweep always has a real deadline to measure against.
    std::atomic<int64_t> channelIdleTimeoutMs{RemotingClient::DEFAULT_CHANNEL_IDLE_TIMEOUT_MILLIS};
    // Whether a sync invoke that times out also closes the connection it went out on.
    std::atomic<bool> closeSocketOnSyncTimeout{true};

    SocksProxyConfig socksProxy;

    mutable std::mutex connMutex;
    std::unordered_map<std::string, std::shared_ptr<Connection>> conns;

    std::mutex respMutex;
    std::unordered_map<int32_t, std::shared_ptr<Future>> respTable;

    // Broker-initiated request handler table: requestCode -> handler. Used only for transaction
    // check-back (CHECK_TRANSACTION_STATE=39) and other commands where the server calls back into us.
    std::mutex procMutex;
    std::unordered_map<int32_t, RequestProcessor> processors;

    // RPC hooks (ACL etc.). Registration happens during the start phase; the send path takes only one
    // shared_ptr copy under the lock and **never calls a hook while holding it** (hooks may compute signatures or even do I/O).
    std::mutex hookMutex;
    std::shared_ptr<RPCHook> rpcHook;

    std::shared_ptr<RPCHook> currentHook() {
        std::lock_guard<std::mutex> lk(hookMutex);
        return rpcHook;
    }

    // Reader thread ledger: <connection, thread>. A finished thread sets connection->readerDone and
    // pruneThreadsLocked reaps it (join, then remove from the ledger), so thread handles never pile up unboundedly.
    std::mutex threadMutex;
    std::vector<std::pair<std::shared_ptr<Connection>, std::thread>> threads;

    void pruneThreadsLocked() {
        for (auto it = threads.begin(); it != threads.end();) {
            if (it->first->readerDone.load()) {
                if (it->second.joinable()) {
                    it->second.join();
                }
                it = threads.erase(it);
            } else {
                ++it;
            }
        }
    }

    void pruneThreads() {
        std::lock_guard<std::mutex> lk(threadMutex);
        pruneThreadsLocked();
    }

    // Fetch and create/reuse a connection; callers that do not hold connMutex must pass an already-locked context
    std::shared_ptr<Connection> getOrCreateConnection(const std::string& addr) {
        // No new connections after shutdown: reader threads deliver callbacks on exit (failFast / normal
        // response), and a callback re-entering the transport would push a new thread into the ledger exactly
        // while shutdown holds threadMutex waiting to join -- at worst iterator invalidation, at worst thread handles never reclaimed and ~std::thread(joinable) calling terminate.
        if (!running.load()) {
            throw RemotingConnectException("remoting client is shut down, " + addr);
        }
        {
            std::lock_guard<std::mutex> lk(connMutex);
            auto it = conns.find(addr);
            if (it != conns.end() && it->second->sock != kInvalidSocket) {
                return it->second;
            }
        }
        // Connect outside the lock so a slow connect does not block other addresses
        std::string host;
        std::string port;
        RemotingClient::parseAddress(addr, host, port);
        std::string err;
        std::string proxyHost;
        std::string proxyPort;
        const bool viaProxy = !socksProxy.empty();
        if (viaProxy) {
            socksProxy.resolveHostPort(proxyHost, proxyPort);
        }
        socket_t sock = connectWithTimeout(viaProxy ? proxyHost : host,
                                           viaProxy ? proxyPort : port,
                                           connectTimeout, err);
        if (sock == kInvalidSocket) {
            if (viaProxy) {
                err = "connect to socks proxy " + proxyHost + ":" + proxyPort + " failed";
            }
            throw RemotingConnectException(err);
        }

        if (viaProxy) {
            std::string socksErr;
            if (!socksConnect(sock, socksProxy, host, port, connectTimeout, socksErr)) {
                closeSocket(sock);
                throw RemotingConnectException("socks proxy " + proxyHost + ":" + proxyPort +
                                               " to " + host + ":" + port + ": " + socksErr);
            }
        }

        auto conn = std::make_shared<Connection>();
        conn->addr = addr;
        conn->sock = sock;

        if (tlsEnable) {
            std::string tlsErr;
            auto session = std::make_unique<TlsSession>(tlsCtx, tlsOptions.serverName);
            if (!session->handshake(sock, host, connectTimeout, tlsErr)) {
                closeSocket(sock);
                throw RemotingConnectException(tlsErr);
            }
            conn->tls = std::move(session);
        }

        pruneThreads();

        {
            std::lock_guard<std::mutex> lk(connMutex);
            // Concurrent connect: if another thread already established one, give up our own socket
            auto it = conns.find(addr);
            if (it != conns.end() && it->second->sock != kInvalidSocket) {
                closeSocket(sock);
                return it->second;
            }
            conn->lastActivityMs.store(Future::monoNowMs());
            conns[addr] = conn;
        }
        // The idle sweep runs on the sweeper thread, so a client that only ever uses
        // invokeSync / invokeOneway still gets its quiet connections reaped.
        ensureSweeper();

        const std::string connAddr = conn->addr;
        std::thread reader([this, conn, connAddr]() {
            setThreadName("RemotingClientReader-" + connAddr);
            readLoop(conn);
        });
        {
            std::lock_guard<std::mutex> lk(threadMutex);
            threads.emplace_back(conn, std::move(reader));
        }
        return conn;
    }

    void readLoop(const std::shared_ptr<Connection>& conn) {
        Bytes buf;
        char chunk[65536];
        socket_t sock = kInvalidSocket;
        while (running.load()) {
            // Re-claim the fd every round. Caching it once at thread start (the earlier approach) hung in CI:
            // closeChannel closed the old connection, the immediate reconnect got **the same fd number**, and the stale
            // reader still slept on that number, so it read the new connection's bytes -- the new connection never got
            // its response, the old reader never exited, and shutdown()'s join blocked too (on Linux closing an fd does not wake a blocked recv).
            const socket_t current = conn->sock;
            if (current == kInvalidSocket) {
                break;
            }
            if (current != sock) {
                sock = current;
                // A changed fd number means a new connection: the receive timeout only needs setting once per connection.
                netcompat::setRecvTimeout(sock, kReaderTimeoutMs);
            }
            // TLS: SSL may still hold decrypted bytes not yet delivered (select on the fd would miss them); skip select in that case
            if (!(conn->tls && conn->tls->pending() > 0)) {
                fd_set rfds;
                FD_ZERO(&rfds);
                FD_SET(sock, &rfds);
                struct timeval tv;
                tv.tv_sec = 0;
                tv.tv_usec = 300000;  // 300ms: exits promptly without spinning
                int sel = ::select(static_cast<int>(sock) + 1, &rfds, nullptr, nullptr, &tv);
                if (sel < 0) {
                    // After shutdown() closes the fd, select necessarily fails; this is the normal exit path. Logged at DEBUG:
                    // invisible at the default INFO level (no "bogus error on exit" noise), and
                    // ROCKETMQ_CLIENT_LOG_LEVEL=DEBUG shows the connection lifecycle when it needs inspecting.
                    logger_debug("remoting reader: select failed on " + conn->addr + ", reader exiting");
                    break;
                }
                if (sel == 0) {
                    continue;
                }
            }
            int n;
            if (conn->tls) {
                n = conn->tls->read(sock, chunk, sizeof(chunk));
                if (n == -2) {
                    // SO_RCVTIMEO expired (or want-write): go back to the select loop
                    continue;
                }
            } else {
                n = static_cast<int>(::recv(sock, chunk, sizeof(chunk), 0));
                if (n < 0 && (netcompat::isWouldBlock() || netcompat::isInterrupted())) {
                    // SO_RCVTIMEO expired / interrupted by a signal: the connection is fine, go back to select and re-claim the fd
                    continue;
                }
            }
            if (n <= 0) {
                logger_debug("remoting reader: connection " + conn->addr + " closed, recv=" +
                             std::to_string(n));
                break;
            }
            buf.append(chunk, static_cast<size_t>(n));
            // Bytes actually came off the socket: the connection just did real work, so its idle
            // clock restarts here.
            conn->lastActivityMs.store(Future::monoNowMs());
            // Split frames on the totalLength prefix; partial / coalesced frames are handled here
            bool streamUntrustworthy = false;
            while (true) {
                if (buf.size() < 4) {
                    break;
                }
                int32_t totalLen = ByteReader::getInt32At(buf, 0);
                if (totalLen <= 0 || totalLen > RemotingClient::MAX_FRAME_LENGTH) {
                    // An untrustworthy length field means the byte stream is corrupt: length-prefixed
                    // framing cannot realign itself, so every byte after this header is misframed.
                    // Report it and take the connection down the same way a peer disconnect does
                    // (exit the loop -> close the socket -> failFast); the other connections stay.
                    logger_error("remoting reader: illegal frame length " + std::to_string(totalLen)
                                 + " from " + conn->addr + ", closing the connection");
                    buf.clear();
                    streamUntrustworthy = true;
                    break;
                }
                if (buf.size() < static_cast<size_t>(4 + totalLen)) {
                    break;  // partial frame, keep receiving
                }
                Bytes frame = buf.substr(0, static_cast<size_t>(4 + totalLen));
                buf.erase(0, static_cast<size_t>(4 + totalLen));
                if (!dispatch(frame, conn->addr)) {
                    streamUntrustworthy = true;
                    break;
                }
            }
            if (streamUntrustworthy) {
                break;
            }
        }
        {
            std::lock_guard<std::mutex> lk(connMutex);
            auto it = conns.find(conn->addr);
            if (it != conns.end() && it->second == conn) {
                conns.erase(it);
            }
        }
        closeConnectionSocket(conn);
        // failFast delivers business callbacks **on this reader thread** and must finish before readerDone is set:
        // a callback that re-enters the transport reaches getOrCreateConnection -> pruneThreads, which joins
        // every thread handle whose readerDone is set -- including our own, and std::thread::join on itself
        // throws std::system_error right there, terminating the reader.
        failFast(conn);
        conn->readerDone.store(true);
    }

    // Close a connection's fd, idempotent. writeMutex also guards the socket lifetime: the write path
    // closes it under this lock on failure and so does here, which avoids both a double close (the fd number
    // may already be reused by a new connection, and closing again would hit the wrong one) and a window where one thread sends while another takes the fd away.
    void closeConnectionSocket(const std::shared_ptr<Connection>& conn) {
        std::lock_guard<std::mutex> wlk(conn->writeMutex);
        const socket_t sock = conn->sock;
        if (sock != kInvalidSocket) {
            // Unpublish the fd before closing: the reader re-claims the fd each round, sees kInvalidSocket and exits,
            // so it never keeps recv'ing on a number about to be reused by a new connection.
            conn->sock = kInvalidSocket;
            closeSocket(sock);
        }
    }

    /// Claim by Connection **object identity**, not by address string: the same address with a replaced connection is a common shape
    /// (GO_AWAY reconnect-and-resend, close after a write failure), so a retiring reader must not kill the new connection's requests.
    int failFast(const std::shared_ptr<Connection>& conn) {
        std::vector<std::pair<std::shared_ptr<Future>, int32_t>> doomed;
        {
            std::lock_guard<std::mutex> lk(respMutex);
            for (auto it = respTable.begin(); it != respTable.end();) {
                if (it->second && it->second->conn == conn) {
                    doomed.emplace_back(it->second, it->first);
                    it = respTable.erase(it);
                } else {
                    ++it;
                }
            }
        }
        for (auto& kv : doomed) {
            const std::shared_ptr<Future>& f = kv.first;
            const InvokeError error(InvokeError::Kind::SEND_REQUEST,
                                    f->addr + " connection closed before response, opaque="
                                        + std::to_string(kv.second));
            bool mine = false;
            {
                std::lock_guard<std::mutex> flk(f->m);
                f->failure = error;
                f->done = true;
                if (!f->callbackFired) {
                    f->callbackFired = true;
                    mine = true;
                }
            }
            f->cv.notify_all();
            // Same reasoning as sweepExpired: callbacks must be delivered outside respMutex
            if (mine && f->callback) {
                f->callback(RemotingCommand(), error);
            }
        }
        if (!doomed.empty()) {
            logger_warn("remoting reader: connection " + conn->addr + " closed, "
                        + std::to_string(doomed.size()) + " in-flight request(s) failed fast");
        }
        return static_cast<int>(doomed.size());
    }

    // Returns true while the connection is still usable, false when this frame took the
    // connection down and the reader must stop reading from it.
    bool dispatch(const Bytes& frame, const std::string& from) {
        RemotingCommand cmd;
        if (!RemotingCommand::tryDecode(frame, cmd, nullptr)) {
            // The bytes no longer line up with the frame boundaries, so nothing that arrives
            // after this frame on this stream can be trusted for any later request: report the
            // peer and take the connection down through the same route a peer disconnect takes
            // (the reader exits, closes the socket and failFasts the in-flight requests).
            logger_error("remoting reader: cannot decode frame from " + from + " ("
                         + std::to_string(frame.size()) + " bytes), closing the connection");
            return false;
        }
        std::shared_ptr<Future> future;
        {
            std::lock_guard<std::mutex> lk(respMutex);
            auto it = respTable.find(cmd.opaque);
            if (it != respTable.end()) {
                future = it->second;
                respTable.erase(it);
            }
        }
        if (future == nullptr) {
            // A response whose opaque is no longer in the in-flight table: the request was
            // already answered, timed out or failed fast. Dropping it is right, saying so is
            // what makes a double-answering peer or a stale request visible; the frame itself
            // was well-formed, so the connection stays up for the requests still on it.
            if (cmd.isResponseType()) {
                logger_warn("remoting reader: response from " + from + " does not match any "
                            "in-flight request (opaque=" + std::to_string(cmd.opaque)
                            + "), dropped");
                return true;
            }
            // A command that is not a response, and that no in-flight request owns, is a
            // **broker-initiated request**: a registered processor takes it over (typically
            // transaction check-back CHECK_TRANSACTION_STATE=39).
            RequestProcessor proc;
            {
                std::lock_guard<std::mutex> plk(procMutex);
                auto pit = processors.find(cmd.code);
                if (pit != processors.end()) {
                    proc = pit->second;
                }
            }
            if (proc) {
                // The processor runs on the reader thread: exceptions must be caught, otherwise the reader dies
                // and every remaining response on this connection is lost (far worse than losing one check-back).
                std::optional<RemotingCommand> resp;
                try {
                    resp = proc(cmd, from);
                } catch (const std::exception& e) {
                    logger_warn("remoting: processor for request code " +
                                std::to_string(cmd.code) + " threw: " + e.what());
                    resp = RemotingCommand::createResponseCommand(
                        ResponseCode::SYSTEM_ERROR, "process request fail");
                } catch (...) {
                    logger_warn("remoting: processor for request code " +
                                std::to_string(cmd.code) + " threw unknown exception");
                    resp = RemotingCommand::createResponseCommand(
                        ResponseCode::SYSTEM_ERROR, "process request fail");
                }
                if (resp.has_value() && !cmd.isOnewayRpc()) {
                    resp->opaque = cmd.opaque;
                    sendResponseByAddr(from, *resp);
                }
            } else {
                // Having no registered processor is expected (the broker does not send this when transactions are off), so do not warn
                logger_debug("remoting: no processor for broker request code " +
                             std::to_string(cmd.code) + " from " + from);
            }
            return true;
        }
        InvokeCallback cb;
        {
            std::lock_guard<std::mutex> lk(future->m);
            future->response = cmd;
            future->hasResponse = true;
            future->done = true;
            if (!future->callbackFired) {
                future->callbackFired = true;
                cb = future->callback;
            }
        }
        future->cv.notify_all();
        if (cb) {
            cb(cmd, InvokeError());
        }
        return true;
    }

    // Acquire an opaque **unique among in-flight requests** and register the future under the same lock.
    std::shared_ptr<Future> registerFutureAcquiringOpaque(RemotingCommand& request,
                                                          InvokeCallback cb,
                                                          const std::string& addr = std::string(),
                                                          int64_t timeoutMillis = 0) {
        auto future = std::make_shared<Future>();
        future->callback = std::move(cb);
        future->addr = addr;
        future->timeoutMs = timeoutMillis;
        // timeoutMillis == 0 means the sweeper is not involved (the sync path waits and unregisters on its own).
        future->deadlineMs = timeoutMillis > 0 ? Future::monoNowMs() + timeoutMillis : 0;
        std::lock_guard<std::mutex> lk(respMutex);
        if (respTable.find(request.opaque) != respTable.end()) {
            int32_t candidate = 0;
            do {
                candidate = RemotingCommand::nextOpaque();
            } while (candidate == request.opaque
                     || respTable.find(candidate) != respTable.end());
            request.opaque = candidate;
        }
        respTable[request.opaque] = future;
        return future;
    }

    void unregisterFuture(int32_t opaque) {
        std::lock_guard<std::mutex> lk(respMutex);
        respTable.erase(opaque);
    }

    RemotingCommand invokeOnce(const std::string& addr, RemotingCommand& request,
                               int32_t timeoutMillis) {
        // Pass 0 as timeoutMillis: the sync path waits and unregisters itself instead of letting the sweeper judge the timeout twice.
        auto future = registerFutureAcquiringOpaque(request, nullptr, addr, 0);
        const int32_t opaque = request.opaque;
        try {
            future->conn = sendRequest(addr, request);
        } catch (...) {
            unregisterFuture(opaque);
            throw;
        }
        std::unique_lock<std::mutex> lk(future->m);
        const bool ok = future->cv.wait_for(lk, std::chrono::milliseconds(timeoutMillis),
                                            [&]() { return future->done; });
        if (!ok) {
            lk.unlock();
            unregisterFuture(opaque);
            // A sync wait that ran out with the response still missing has no evidence the peer
            // is readable at all: the frame may be lost on the way, and nothing else will ever
            // deliver it. Both floors below measure the budget the wait was handed when it
            // began -- this port does not subtract the connect and write cost, so that budget IS
            // the timeout and the second floor is out of reach for any positive value. A
            // positive timeout therefore always closes; zero is the one shape that keeps the
            // socket, and such a request was never going to be answered anyway.
            const int64_t leftMillis = timeoutMillis;
            if (closeSocketOnSyncTimeout.load()
                && (leftMillis > 100 || leftMillis > timeoutMillis / 4)) {
                logger_warn("remoting: sync wait to " + addr + " timed out (opaque="
                            + std::to_string(opaque) + ", timeout=" + std::to_string(timeoutMillis)
                            + " ms), closing the connection");
                closeConnection(addr);
            }
            logger_warn("remoting: wait response from " + addr + " timeout, opaque="
                        + std::to_string(opaque) + ", " + std::to_string(timeoutMillis) + " ms");
            throw RemotingTimeoutException(addr + " wait response timeout "
                                           + std::to_string(timeoutMillis) + " ms, opaque="
                                           + std::to_string(opaque));
        }
        if (!future->hasResponse) {
            lk.unlock();
            unregisterFuture(opaque);
            throw RemotingSendRequestException(future->failure.message);
        }
        return future->response;
    }

    void closeConnection(const std::string& addr) {
        std::shared_ptr<Connection> conn;
        {
            std::lock_guard<std::mutex> lk(connMutex);
            auto it = conns.find(addr);
            if (it == conns.end()) {
                return;
            }
            conn = it->second;
            conns.erase(it);
        }
        // Closing the socket makes the reader's select()/recv() return at once and exit on its own;
        // on exit it is responsible for failFasting this connection's in-flight requests (see the end of readLoop).
        closeConnectionSocket(conn);
    }

    RemotingCommand retryAfterGoAway(const std::string& addr, const RemotingCommand& request,
                                     int64_t deadlineMs) {
        if (!enableReconnectForGoAway) {
            throw RemotingSendRequestException("Receive GO_AWAY from channel " + addr);
        }
        logger_info("remoting: receive GO_AWAY from " + addr + ", reconnect and retry once");
        closeConnection(addr);
        const int64_t remaining = deadlineMs - Future::monoNowMs();
        if (remaining <= 0) {
            throw RemotingTimeoutException(addr + " GO_AWAY retry budget exhausted, opaque="
                                           + std::to_string(request.opaque));
        }
        // The resend must use a **new** opaque: the old request already received its answer, and reusing the number would mismatch the next response.
        RemotingCommand retry = request;
        retry.opaque = RemotingCommand::nextOpaque();
        const RemotingCommand response = invokeOnce(addr, retry, static_cast<int32_t>(remaining));
        if (response.code == ResponseCode::GO_AWAY) {
            throw RemotingSendRequestException("Receive GO_AWAY twice in request from channel "
                                               + addr);
        }
        return response;
    }

    std::mutex sweepMutex;
    std::condition_variable sweepCv;
    std::thread sweeper;
    bool sweeperStarted = false;

    // Resend job after an async call receives GO_AWAY. It must run on a **non-reader thread**: resending builds a new
    // connection and waits for that connection's reader to deliver the response, and waiting inside a reader blocks delivery of
    // every other in-flight request on this connection. Reusing the sweeper thread (instead of detaching one) keeps the lifetime tied to RemotingClient.
    struct GoAwayRetry {
        std::string addr;
        RemotingCommand request;
        InvokeCallback callback;
        int64_t deadlineMs;
    };
    std::mutex retryMutex;
    std::vector<GoAwayRetry> retryQueue;

    void scheduleGoAwayRetry(GoAwayRetry job) {
        {
            std::lock_guard<std::mutex> lk(retryMutex);
            retryQueue.push_back(std::move(job));
        }
        sweepCv.notify_all();  // work immediately instead of waiting for the next 100ms tick
    }

    void drainGoAwayRetries() {
        std::vector<GoAwayRetry> jobs;
        {
            std::lock_guard<std::mutex> lk(retryMutex);
            jobs.swap(retryQueue);
        }
        for (auto& job : jobs) {
            try {
                const RemotingCommand response =
                    retryAfterGoAway(job.addr, job.request, job.deadlineMs);
                job.callback(response, InvokeError());
            } catch (const RemotingTimeoutException& e) {
                job.callback(RemotingCommand(),
                             InvokeError(InvokeError::Kind::TIMEOUT, e.what()));
            } catch (const RemotingSendRequestException& e) {
                job.callback(RemotingCommand(),
                             InvokeError(InvokeError::Kind::SEND_REQUEST, e.what()));
            } catch (const RemotingConnectException& e) {
                job.callback(RemotingCommand(),
                             InvokeError(InvokeError::Kind::CONNECT, e.what()));
            } catch (const RemotingTooMuchRequestException& e) {
                job.callback(RemotingCommand(),
                             InvokeError(InvokeError::Kind::TOO_MUCH_REQUEST, e.what()));
            } catch (const std::exception& e) {
                job.callback(RemotingCommand(), InvokeError(InvokeError::Kind::OTHER, e.what()));
            }
        }
    }

    // Hand not-yet-resent jobs back to the business layer before exiting: a callback that never fires is harder to debug than one that fires with an error.
    void failPendingRetries(const std::string& reason) {
        std::vector<GoAwayRetry> jobs;
        {
            std::lock_guard<std::mutex> lk(retryMutex);
            jobs.swap(retryQueue);
        }
        for (auto& job : jobs) {
            job.callback(RemotingCommand(), InvokeError(InvokeError::Kind::OTHER, reason));
        }
    }

    void ensureSweeper() {
        {
            std::lock_guard<std::mutex> lk(sweepMutex);
            if (sweeperStarted) return;
            sweeperStarted = true;
        }
        sweeper = std::thread([this]() {
            while (true) {
                std::unique_lock<std::mutex> lk(sweepMutex);
                sweepCv.wait_for(lk, std::chrono::milliseconds(100), [this]() {
                    if (!running.load() || !sweeperStarted) {
                        return true;
                    }
                    // The nested lock order is fixed as sweepMutex -> retryMutex: scheduleGoAwayRetry
                    // notifies only after **releasing** retryMutex and never holds them in reverse, so there is no deadlock cycle.
                    std::lock_guard<std::mutex> rlk(retryMutex);
                    return !retryQueue.empty();
                });
                lk.unlock();
                if (!running.load()) return;
                sweepExpired();
                sweepIdleConnections();
                drainGoAwayRetries();
            }
        });
    }

    void stopSweeper() {
        {
            std::lock_guard<std::mutex> lk(sweepMutex);
            sweeperStarted = false;
        }
        sweepCv.notify_all();
        if (sweeper.joinable()) {
            sweeper.join();
        }
    }

    void sweepExpired() {
        constexpr int64_t kSweepGraceMs = 1000;
        const int64_t now = Future::monoNowMs();
        std::vector<std::pair<std::shared_ptr<Future>, int32_t>> expired;
        {
            std::lock_guard<std::mutex> lk(respMutex);
            for (auto it = respTable.begin(); it != respTable.end();) {
                const std::shared_ptr<Future>& f = it->second;
                if (f->deadlineMs != 0 && now >= f->deadlineMs + kSweepGraceMs) {
                    expired.emplace_back(f, it->first);
                    it = respTable.erase(it);
                } else {
                    ++it;
                }
            }
        }
        // Callbacks must run **outside** respMutex: they often re-enter the transport or business layer, and
        // calling back under the lock both contends with shutdown for the same mutex and can self-deadlock.
        for (auto& kv : expired) {
            const std::shared_ptr<Future>& f = kv.first;
            // One event, one log line: without this a swept request is invisible, because the
            // business callback deciding to log anything is out of the transport's hands.
            logger_warn("remoting: in-flight request to " + f->addr + " expired (opaque="
                        + std::to_string(kv.second) + ", timeout=" + std::to_string(f->timeoutMs)
                        + " ms), failed with a timeout");
            bool mine = false;
            {
                std::lock_guard<std::mutex> flk(f->m);
                f->done = true;
                if (!f->callbackFired) {
                    f->callbackFired = true;
                    mine = true;
                }
            }
            f->cv.notify_all();
            if (mine && f->callback) {
                // The kind must be TIMEOUT: async send retry decisions rely on it to tell "wait and see" from "switch broker"
                f->callback(RemotingCommand(),
                            InvokeError(InvokeError::Kind::TIMEOUT,
                                        f->addr + " async invoke timeout "
                                            + std::to_string(f->timeoutMs) + " ms, opaque="
                                            + std::to_string(kv.second)));
            }
        }
    }

    // Close every connection idle strictly longer than the configured limit and WARN each one.
    // The close goes through the same route a peer disconnect does (drop the table entry, close
    // the socket, and let that connection's reader failFast its in-flight requests), so a
    // connection that stopped carrying traffic takes those requests down with it while the other
    // connections are untouched.
    int sweepIdleConnections() {
        const int64_t limit = channelIdleTimeoutMs.load();
        const int64_t now = Future::monoNowMs();
        struct Reaped {
            std::string addr;
            std::shared_ptr<Connection> conn;
            int64_t idleMs;
        };
        std::vector<Reaped> idle;
        {
            std::lock_guard<std::mutex> lk(connMutex);
            for (auto it = conns.begin(); it != conns.end();) {
                // Unstamped entries belong to a connection still being established; a connection
                // whose idle time merely reaches the limit is not past it.
                const int64_t last = it->second->lastActivityMs.load();
                if (last != 0 && now - last > limit) {
                    idle.push_back(Reaped{it->first, it->second, now - last});
                    it = conns.erase(it);
                } else {
                    ++it;
                }
            }
        }
        for (auto& r : idle) {
            logger_warn("remoting: connection to " + r.addr + " idle for "
                        + std::to_string(r.idleMs) + " ms (limit " + std::to_string(limit)
                        + " ms), closing it");
            closeConnectionSocket(r.conn);
        }
        return static_cast<int>(idle.size());
    }

    // Write the response back to an **existing** connection (never create one).
    //
    // Use: responses to broker-initiated requests (typically PUSH_REPLY_MESSAGE_TO_CLIENT=326) must go back over the same path,
    // and dispatch() only has the peer address string, so the connection table is looked up by address here. When the connection was
    // already closed only a warn is logged -- the answer itself was delivered to business code, and losing a response must not affect the reader.
    void sendResponseByAddr(const std::string& addr, RemotingCommand& response) {
        std::shared_ptr<Connection> conn;
        {
            std::lock_guard<std::mutex> lk(connMutex);
            auto it = conns.find(addr);
            if (it != conns.end()) {
                conn = it->second;
            }
        }
        if (conn == nullptr) {
            logger_warn("remoting: cannot answer broker request from " + addr +
                        ", connection is gone");
            return;
        }
        Bytes data = response.encode();
        std::lock_guard<std::mutex> wlk(conn->writeMutex);
        const socket_t sock = conn->sock;
        if (sock == kInvalidSocket) {
            logger_warn("remoting: cannot answer broker request from " + addr +
                        ", connection is gone");
            return;
        }
        const bool ok = conn->tls ? conn->tls->writeAll(sock,
                                                        reinterpret_cast<const char*>(data.data()),
                                                        data.size())
                                  : sendAll(sock, data);
        if (!ok) {
            logger_warn("remoting: failed to write response to " + addr);
        } else {
            conn->lastActivityMs.store(Future::monoNowMs());
        }
    }

    std::shared_ptr<Connection> sendRequest(const std::string& addr, RemotingCommand& request) {
        // RPC hooks must run **before** encode(): the ACL hook writes AccessKey/Signature
        // into extFields, and the signature covers exactly this extFields + body that is about to go on the wire.
        // Take a snapshot before invoking so no lock is held while the hook runs.
        if (auto hook = currentHook()) {
            hook->doBeforeRequest(addr, request);
        }
        auto conn = getOrCreateConnection(addr);
        Bytes data = request.encode();
        std::lock_guard<std::mutex> lk(conn->writeMutex);
        // Read the fd exactly once inside writeMutex: the reader and close paths mutate it, and two reads could yield two different numbers.
        const socket_t sock = conn->sock;
        const bool sentOk = sock == kInvalidSocket
            ? false
            : (conn->tls ? conn->tls->writeAll(sock,
                                               reinterpret_cast<const char*>(data.data()),
                                               data.size())
                         : sendAll(sock, data));
        if (!sentOk) {
            // Idempotent close: the reader may already have closed it and cleared the fd (the peer disconnected at the same time), and
            // closing an fd number already returned to the kernel would close a new connection by mistake.
            if (conn->sock != kInvalidSocket) {
                const socket_t dead = conn->sock;
                conn->sock = kInvalidSocket;
                closeSocket(dead);
            }
            {
                std::lock_guard<std::mutex> clk(connMutex);
                auto it = conns.find(addr);
                if (it != conns.end() && it->second == conn) {
                    conns.erase(it);
                }
            }
            throw RemotingSendRequestException(formatAddr(addr));
        }
        // The bytes went out: the connection just did real work, so its idle clock restarts here.
        conn->lastActivityMs.store(Future::monoNowMs());
        return conn;
    }

    void shutdown() {
        bool expected = true;
        if (!running.compare_exchange_strong(expected, false)) {
            return;  // already shut down
        }
        // Stop the sweeper first: its callbacks re-enter business code, so it must not see a half-shut client.
        stopSweeper();
        // Queued GO_AWAY jobs that have not been resent get an error callback; they must not be swallowed silently.
        failPendingRetries("remoting client shutdown, GO_AWAY retry abandoned");
        std::vector<std::shared_ptr<Connection>> all;
        {
            std::lock_guard<std::mutex> lk(connMutex);
            for (auto& kv : conns) {
                all.push_back(kv.second);
            }
            conns.clear();
        }
        // Close sockets so readers return immediately: unpublish the fd first (the reader sees kInvalidSocket on its
        // next claim and exits), then close -- closeSocket calls shutdown(SHUT_RDWR) first internally, which is the only
        // way to wake a reader sleeping in recv/select and a blocked send (on Linux close alone does not wake them).
        // writeMutex is deliberately **not** held here: waiting under a lock for a thread stuck inside sendAll would just move the
        // same unbounded hang from the read path to the write path.
        for (auto& c : all) {
            const socket_t sock = c->sock;
            c->sock = kInvalidSocket;
            closeSocket(sock);
        }
        std::lock_guard<std::mutex> lk(threadMutex);
        for (auto& t : threads) {
            if (t.second.joinable()) {
                t.second.join();
            }
        }
        threads.clear();
        std::lock_guard<std::mutex> rlk(respMutex);
        respTable.clear();
    }
};

// ---------------------------------------------------------------- Public interface
RemotingClient::RemotingClient(int32_t connectTimeoutMillis, int32_t invokeTimeoutMillis)
    : impl_(new Impl()), connectTimeoutMillis_(connectTimeoutMillis),
      invokeTimeoutMillis_(invokeTimeoutMillis) {
    ensureInitialized();
    impl_->connectTimeout = connectTimeoutMillis;
    impl_->invokeTimeout = invokeTimeoutMillis;
}

RemotingClient::~RemotingClient() {
    if (impl_) {
        impl_->shutdown();
    }
}

void RemotingClient::parseAddress(const std::string& addr, std::string& host,
                                  std::string& port) {
    if (!addr.empty() && addr[0] == '[') {  // IPv6 literal: [::1]:10911
        size_t end = addr.find(']');
        if (end != std::string::npos) {
            host = addr.substr(1, end - 1);
            size_t colon = addr.find(':', end);
            port = (colon == std::string::npos) ? std::string() : addr.substr(colon + 1);
            return;
        }
    }
    // IPv4 / hostname: everything after the last ':' is the port
    size_t colon = addr.rfind(':');
    if (colon == std::string::npos) {
        host = addr;
        port.clear();
        return;
    }
    host = addr.substr(0, colon);
    port = addr.substr(colon + 1);
}

RemotingCommand RemotingClient::invokeSync(const std::string& addr, RemotingCommand& request,
                                           int32_t timeoutMillis) {
    const int32_t timeout = timeoutMillis >= 0 ? timeoutMillis : invokeTimeoutMillis_;
    const int64_t deadline = Impl::Future::monoNowMs() + timeout;
    const RemotingCommand response = impl_->invokeOnce(addr, request, timeout);
    if (response.code != ResponseCode::GO_AWAY) {
        return response;
    }
    return impl_->retryAfterGoAway(addr, request, deadline);
}

void RemotingClient::invokeAsync(const std::string& addr, RemotingCommand& request,
                                 InvokeCallback callback, int32_t timeoutMillis) {
    const int32_t timeout = timeoutMillis >= 0 ? timeoutMillis : invokeTimeoutMillis_;
    impl_->ensureSweeper();
    const int64_t deadline = Impl::Future::monoNowMs() + timeout;
    InvokeCallback user = std::move(callback);
    RemotingCommand original = request;
    auto future = impl_->registerFutureAcquiringOpaque(
        request,
        [this, user, addr, deadline, original](const RemotingCommand& response,
                                               const InvokeError& error) {
            if (!error.empty() || response.code != ResponseCode::GO_AWAY) {
                user(response, error);
                return;
            }
            Impl::GoAwayRetry job;
            job.addr = addr;
            job.request = original;
            job.callback = user;
            job.deadlineMs = deadline;
            impl_->scheduleGoAwayRetry(std::move(job));
        },
        addr, timeout);
    const int32_t opaque = request.opaque;
    try {
        future->conn = impl_->sendRequest(addr, request);
    } catch (...) {
        impl_->unregisterFuture(opaque);
        throw;
    }
}

void RemotingClient::invokeOneway(const std::string& addr, RemotingCommand& request) {
    request.markOnewayRpc();
    impl_->sendRequest(addr, request);
}

void RemotingClient::registerProcessor(int32_t requestCode, RequestProcessor handler) {
    std::lock_guard<std::mutex> lk(impl_->procMutex);
    impl_->processors[requestCode] = std::move(handler);
}

void RemotingClient::unregisterProcessor(int32_t requestCode) {
    std::lock_guard<std::mutex> lk(impl_->procMutex);
    impl_->processors.erase(requestCode);
}

bool RemotingClient::registerRPCHook(std::shared_ptr<RPCHook> hook) {
    std::lock_guard<std::mutex> lk(impl_->hookMutex);
    if (impl_->rpcHook) {
        return false;
    }
    impl_->rpcHook = std::move(hook);
    return true;
}

void RemotingClient::unregisterRPCHook() {
    std::lock_guard<std::mutex> lk(impl_->hookMutex);
    impl_->rpcHook.reset();
}

void RemotingClient::setTlsEnable(bool enable) {
    if (!enable) {
        impl_->tlsEnable = false;
        impl_->tlsCtx.reset();
        return;
    }
    if (!impl_->tlsCtx) {
        std::string err;
        std::shared_ptr<void> ctx;
        if (!impl_->tlsOptions.caCert.empty()) {
            ctx = createStrictClientSslContext(impl_->tlsOptions.caCert,
                                               impl_->tlsOptions.clientCert,
                                               impl_->tlsOptions.clientKey, err);
        } else {
            ctx = createClientSslContext(err, impl_->tlsOptions.clientCert,
                                         impl_->tlsOptions.clientKey);
        }
        if (!ctx) {
            throw RemotingException("enable TLS failed: " + err);
        }
        impl_->tlsCtx = ctx;
    }
    impl_->tlsEnable = true;
}

void RemotingClient::setTlsOptions(const TlsOptions& options) {
    impl_->tlsOptions = options;
    if (impl_->tlsEnable) {
        // Already enabled: when options change, the ctx must be rebuilt (e.g. ca_verify switching to mtls)
        std::string err;
        std::shared_ptr<void> ctx;
        if (!impl_->tlsOptions.caCert.empty()) {
            ctx = createStrictClientSslContext(impl_->tlsOptions.caCert,
                                               impl_->tlsOptions.clientCert,
                                               impl_->tlsOptions.clientKey, err);
        } else {
            ctx = createClientSslContext(err, impl_->tlsOptions.clientCert,
                                         impl_->tlsOptions.clientKey);
        }
        if (!ctx) {
            throw RemotingException("setTlsOptions failed: " + err);
        }
        impl_->tlsCtx = ctx;
    }
}

bool RemotingClient::tlsEnable() const { return impl_->tlsEnable; }

void RemotingClient::setSocksProxy(const SocksProxyConfig& proxy) {
    // Must be set before the first connection is established (same rule as TLS); existing connections are not rebuilt here,
    // they keep using the original tunnel while newly created connections follow the new configuration.
    impl_->socksProxy = proxy;
}

bool RemotingClient::socksProxyEnabled() const { return !impl_->socksProxy.empty(); }

bool RemotingClient::isChannelWritable(const std::string& addr) const {
    std::lock_guard<std::mutex> lk(impl_->connMutex);
    auto it = impl_->conns.find(addr);
    if (it == impl_->conns.end() || it->second->sock == kInvalidSocket) {
        // "Nothing is open yet" is not "this cannot take data": this transport connects on
        // demand, so the next send simply creates the connection. Same for a table entry whose
        // socket is already gone -- the next send replaces it.
        return true;
    }
    // A live socket: ask the kernel whether it would take a write right now. This is a snapshot
    // of the send buffer, not a queue-depth watermark, so a live connection that is merely slow
    // but currently drained still reports writable -- as does one whose peer just died (the
    // read path is what notices that and takes the channel down). On a TLS connection the probe
    // sees the underlying socket only, not the crypto layer's own buffers.
    const socket_t sock = it->second->sock;
    fd_set wfds;
    FD_ZERO(&wfds);
    FD_SET(sock, &wfds);
    struct timeval tv {
        0, 0
    };
    const int ready = ::select(static_cast<int>(sock) + 1, nullptr, &wfds, nullptr, &tv);
    if (ready < 0) {
        // The probe itself failed: report writable and let the write path
        // (send failure -> close the connection) decide, rather than refusing the send.
        return true;
    }
    return FD_ISSET(sock, &wfds);
}

void RemotingClient::setChannelIdleTimeoutMillis(int64_t millis) {
    // A non-positive value never disables the sweep: the sweep is always installed, so an
    // unsettable deadline would mean "reap immediately". Restore the default instead.
    impl_->channelIdleTimeoutMs.store(millis > 0 ? millis
                                                 : DEFAULT_CHANNEL_IDLE_TIMEOUT_MILLIS);
}

int64_t RemotingClient::channelIdleTimeoutMillis() const {
    return impl_->channelIdleTimeoutMs.load();
}

int RemotingClient::sweepIdleConnections() { return impl_->sweepIdleConnections(); }

void RemotingClient::setCloseSocketOnSyncTimeout(bool close) {
    impl_->closeSocketOnSyncTimeout.store(close);
}

bool RemotingClient::closeSocketOnSyncTimeout() const {
    return impl_->closeSocketOnSyncTimeout.load();
}

void RemotingClient::closeChannel(const std::string& addr) { impl_->closeConnection(addr); }

void RemotingClient::setEnableReconnectForGoAway(bool enable) {
    impl_->enableReconnectForGoAway = enable;
}

bool RemotingClient::enableReconnectForGoAway() const { return impl_->enableReconnectForGoAway; }

void RemotingClient::updateNameServerAddressList(const std::vector<std::string>& /*addrs*/) {
    // The upper MQClientInstance maintains the name server list; the transport layer does not hold it
}

void RemotingClient::shutdown() { impl_->shutdown(); }

size_t RemotingClient::connectionCount() const {
    std::lock_guard<std::mutex> lk(impl_->connMutex);
    return impl_->conns.size();
}

size_t RemotingClient::pendingRequestCount() const {
    std::lock_guard<std::mutex> lk(impl_->respMutex);
    return impl_->respTable.size();
}

}  // namespace rocketmq
