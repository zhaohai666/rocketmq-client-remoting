// RemotingClient 的实现：基于阻塞 socket + 每连接读线程的请求/响应模型。
//
// 两个容易踩的坑，这里都做了处理：
//   1. SIGPIPE：向已被对端关闭的 socket 写入会触发 SIGPIPE，默认行为是**杀掉进程**。
//      Linux 上用 send(..., MSG_NOSIGNAL)，macOS/BSD 上用 SO_NOSIGPIPE 套接字选项。
//   2. 读线程退出：不能靠阻塞 recv 长时间挂着，否则 shutdown 时无法及时回收。
//      这里用 select() 带 300ms 超时轮询，既能及时退出也不会空转烧 CPU。
#include "rocketmq/remoting/remoting_client.h"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstring>
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
#include "tls_session.h"

#ifndef _WIN32
#include <fcntl.h>  // 非阻塞 connect 用（fcntl/F_GETFL/F_SETFL）
#endif

namespace rocketmq {

namespace {

// 平台 socket 原语统一来自 netcompat（见 include/rocketmq/common/net_compat.h）。
// 用 using 声明引入文件作用域，保持下面代码里 socket_t/closeSocket 等名字不变。
using netcompat::closeSocket;
using netcompat::ensureInitialized;
using netcompat::kInvalidSocket;
using netcompat::sendFlags;
using netcompat::socket_t;
using netcompat::tuneSocket;

// 带超时的 connect：先置非阻塞，select 等可写，再恢复阻塞
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
                // 可写不代表连上了，必须用 SO_ERROR 复核
                int soErr = 0;
                netcompat::socklen_type len = sizeof(soErr);
                ::getsockopt(sock, SOL_SOCKET, SO_ERROR,
                             reinterpret_cast<char*>(&soErr), &len);
                connected = (soErr == 0);
            }
        }
        if (connected) {
            // 恢复阻塞模式，读循环用 select() 控制节奏
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

// 写全部字节；返回 false 表示对端已关闭或出错
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
    struct Connection {
        std::string addr;
        socket_t sock = kInvalidSocket;
        std::mutex writeMutex;
        std::atomic<bool> readerDone{false};
        // TLS 会话（tlsEnable 时非空；read/write 走 SSL_*）
        std::unique_ptr<TlsSession> tls;
    };

    struct Future {
        std::mutex m;
        std::condition_variable cv;
        bool done = false;
        bool hasResponse = false;
        RemotingCommand response;
        InvokeCallback callback;
        // 异步请求的超时账目（对应 Java ResponseFuture 的 timeoutMillis + beginTimestamp）。
        // addr/deadlineMs 只在登记时写入；callbackFired 由 notifyExpired 与 dispatch 竞争设置。
        std::string addr;
        int64_t deadlineMs = 0;
        int64_t timeoutMs = 0;
        bool callbackFired = false;

        static int64_t monoNowMs() {
            return std::chrono::duration_cast<std::chrono::milliseconds>(
                       std::chrono::steady_clock::now().time_since_epoch())
                .count();
        }
    };

    std::atomic<bool> running{true};
    int32_t connectTimeout = 3000;
    int32_t invokeTimeout = 15000;
    // TLS（对应 Java NettyRemotingClient 的 isUseTLS + 构造期 buildSslContext）。
    // tlsCtx 持有 SSL_CTX*；setTlsEnable(true) 时创建。
    bool tlsEnable = false;
    std::shared_ptr<void> tlsCtx;

    mutable std::mutex connMutex;
    std::unordered_map<std::string, std::shared_ptr<Connection>> conns;

    std::mutex respMutex;
    std::unordered_map<int32_t, std::shared_ptr<Future>> respTable;

    // broker 主动请求处理器表：requestCode -> handler。仅用于事务回查
    // (CHECK_TRANSACTION_STATE=39) 这类「服务端反过来找我」的命令。
    std::mutex procMutex;
    std::unordered_map<int32_t, RequestProcessor> processors;

    // RPC 钩子（ACL 等）。注册发生在 start 阶段；发送路径只在锁内取一次
    // shared_ptr 拷贝，**绝不持锁调用钩子**（钩子内部可能做签名计算甚至 I/O）。
    std::mutex hookMutex;
    std::shared_ptr<RPCHook> rpcHook;

    std::shared_ptr<RPCHook> currentHook() {
        std::lock_guard<std::mutex> lk(hookMutex);
        return rpcHook;
    }

    // 读线程账本：<连接, 线程>。线程结束后置 connection->readerDone，
    // 由 pruneThreadsLocked 回收（join 后从账本移除），避免线程句柄无限堆积。
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

    // 取出并创建/复用连接；不持有 connMutex 的调用方负责传入已锁定的上下文
    std::shared_ptr<Connection> getOrCreateConnection(const std::string& addr) {
        {
            std::lock_guard<std::mutex> lk(connMutex);
            auto it = conns.find(addr);
            if (it != conns.end() && it->second->sock != kInvalidSocket) {
                return it->second;
            }
        }
        // 建连放到锁外，避免慢 connect 阻塞其它地址
        std::string host;
        std::string port;
        RemotingClient::parseAddress(addr, host, port);
        std::string err;
        socket_t sock = connectWithTimeout(host, port, connectTimeout, err);
        if (sock == kInvalidSocket) {
            throw RemotingConnectException(err);
        }

        auto conn = std::make_shared<Connection>();
        conn->addr = addr;
        conn->sock = sock;

        // TLS：在任何 RocketMQ 帧之前完成握手（对应 Java pipeline.addFirst(SslHandler)）。
        // 失败按建连失败处理，异常信息带握手原因。
        if (tlsEnable) {
            std::string tlsErr;
            auto session = std::make_unique<TlsSession>(tlsCtx);
            if (!session->handshake(sock, host, connectTimeout, tlsErr)) {
                closeSocket(sock);
                throw RemotingConnectException(tlsErr);
            }
            conn->tls = std::move(session);
        }

        pruneThreads();

        {
            std::lock_guard<std::mutex> lk(connMutex);
            // 并发建连：若已有别人先建成，放弃自己的 socket
            auto it = conns.find(addr);
            if (it != conns.end() && it->second->sock != kInvalidSocket) {
                closeSocket(sock);
                return it->second;
            }
            conns[addr] = conn;
        }

        // 每连接一个读线程，命名后日志里能直接看出是哪条链路（对应 Java 的
        // NettyClientWorkerThread；Java 用线程池复用，这里是一连接一线程，故带上地址）。
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
        const socket_t sock = conn->sock;
        char chunk[65536];
        while (running.load()) {
            // TLS：SSL 内部可能还有未交付的解密字节（select 对 fd 会漏报），此时跳过 select
            if (!(conn->tls && conn->tls->pending() > 0)) {
                fd_set rfds;
                FD_ZERO(&rfds);
                FD_SET(sock, &rfds);
                struct timeval tv;
                tv.tv_sec = 0;
                tv.tv_usec = 300000;  // 300ms：既能及时退出，也不空转
                int sel = ::select(static_cast<int>(sock) + 1, &rfds, nullptr, nullptr, &tv);
                if (sel < 0) {
                    // shutdown() 关闭 fd 后 select 必然失败，属正常退出路径。这里统一记 DEBUG：
                    // 默认 INFO 级别下不可见（不会出现"退出时的假异常"噪声），
                    // 需要看连接生命周期时 ROCKETMQ_CPP_LOG_LEVEL=DEBUG 即可。
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
                    // SO_RCVTIMEO 到点（或 want-write）：回到 select 循环
                    continue;
                }
            } else {
                n = static_cast<int>(::recv(sock, chunk, sizeof(chunk), 0));
            }
            if (n <= 0) {
                // n == 0 对端正常关闭，n < 0 读错误。Java 侧 Netty 的 channelInactive 会打一行，
                // 但正常 shutdown 也会走到这里，为免默认 INFO 下变成噪声同样降到 DEBUG。
                logger_debug("remoting reader: connection " + conn->addr + " closed, recv=" +
                             std::to_string(n));
                break;
            }
            buf.append(chunk, static_cast<size_t>(n));
            // 按 totalLength 前缀切帧；粘包/半包都由这里处理
            while (true) {
                if (buf.size() < 4) {
                    break;
                }
                int32_t totalLen = ByteReader::getInt32At(buf, 0);
                if (totalLen <= 0 || totalLen > RemotingClient::MAX_FRAME_LENGTH) {
                    // 真正的协议异常（对应 Java NettyRemotingAbstract 的 "decode message length error"）：
                    // 必须可见，否则会表现为"请求莫名超时"而没人知道原因。
                    logger_warn("remoting reader: illegal frame length " + std::to_string(totalLen) +
                                " from " + conn->addr + ", dropping " +
                                std::to_string(buf.size()) + " buffered bytes");
                    buf.clear();
                    break;
                }
                if (buf.size() < static_cast<size_t>(4 + totalLen)) {
                    break;  // 半包，继续收
                }
                Bytes frame = buf.substr(0, static_cast<size_t>(4 + totalLen));
                buf.erase(0, static_cast<size_t>(4 + totalLen));
                dispatch(frame, conn->addr);
            }
        }
        conn->readerDone.store(true);
        // 把自己从连接表摘掉（避免留下失效条目）
        {
            std::lock_guard<std::mutex> lk(connMutex);
            auto it = conns.find(conn->addr);
            if (it != conns.end() && it->second == conn) {
                conns.erase(it);
            }
        }
    }

    void dispatch(const Bytes& frame, const std::string& from) {
        RemotingCommand cmd;
        if (!RemotingCommand::tryDecode(frame, cmd, nullptr)) {
            // Java 侧这里同样是 warn（解码失败意味着这条响应永久丢失，调用方只会看到超时）。
            logger_warn("remoting reader: drop undecodable frame (" + std::to_string(frame.size()) +
                        " bytes) from " + from);
            return;  // 解不出的帧直接丢，不影响其它请求
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
            // 在途表里查不到：要么是迟到的响应，要么是 **broker 主动发来的请求**。
            // 后者由已注册的处理器接管（典型：事务回查 CHECK_TRANSACTION_STATE=39）。
            if (!cmd.isResponseType()) {
                RequestProcessor proc;
                {
                    std::lock_guard<std::mutex> plk(procMutex);
                    auto pit = processors.find(cmd.code);
                    if (pit != processors.end()) {
                        proc = pit->second;
                    }
                }
                if (proc) {
                    // 处理器在读线程里执行：异常必须兜住，否则读线程会死掉，
                    // 导致该连接上其余响应全部丢失（比丢一条回查严重得多）。
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
                    // 有返回值就回写（opaque 必须原样带回，broker 侧 invokeSync 靠它对上号）；
                    // oneway 请求不回响应，与 Java NettyRemotingAbstract 的
                    // `if (!cmd.isOnewayRpc()) { ... writeResponse ... }` 一致。
                    if (resp.has_value() && !cmd.isOnewayRpc()) {
                        resp->opaque = cmd.opaque;
                        sendResponseByAddr(from, *resp);
                    }
                } else {
                    // 没有注册处理器属于预期情况（未开启事务时 broker 不会发），别用 warn
                    logger_debug("remoting: no processor for broker request code " +
                                 std::to_string(cmd.code) + " from " + from);
                }
            }
            return;  // 迟到的响应 / 已处理完的 broker 请求
        }
        InvokeCallback cb;
        {
            std::lock_guard<std::mutex> lk(future->m);
            future->response = cmd;
            future->hasResponse = true;
            future->done = true;
            // 与超时清理线程抢同一个回调：谁先置位谁投递，另一个只能放弃（Java 用
            // ResponseFuture 上的 executeOnceCallback 表达同一约束）。
            if (!future->callbackFired) {
                future->callbackFired = true;
                cb = future->callback;
            }
        }
        future->cv.notify_all();
        if (cb) {
            cb(cmd, std::string());
        }
    }

    // 取得一个**在途请求中唯一**的 opaque，并在同一把锁内登记 future。
    //
    // 为什么不能简单用 "opaque == 0 就重分配"：RemotingCommand 的 opaque 计数器从 0
    // 开始，所以 createRequestCommand() 产出的**第一个**请求 opaque 就是 0，把 0 当
    // "未设置"会把它改掉，导致调用方与服务端回填的 opaque 不一致（调用方拿不到响应）。
    // 这里只在**真的与在途请求冲突**时才重分配，其余情况原样保留（含 0），与 Java
    // NettyRemotingClient 从不改写调用方 opaque 的行为一致。
    std::shared_ptr<Future> registerFutureAcquiringOpaque(RemotingCommand& request,
                                                          InvokeCallback cb,
                                                          const std::string& addr = std::string(),
                                                          int64_t timeoutMillis = 0) {
        auto future = std::make_shared<Future>();
        future->callback = std::move(cb);
        future->addr = addr;
        future->timeoutMs = timeoutMillis;
        // timeoutMillis == 0 表示不交给清理线程（同步路径自己等、自己摘）。
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

    // ---- 异步请求的超时清理（对应 Java NettyRemotingAbstract.scanResponseTable）----
    // 没有它，invokeAsync 的 timeoutMillis 就无处生效：对端不回包时回调永远不触发，
    // 在途表项也永久留在 respTable 里（长连接复用久了就是内存泄漏 + 悬挂的发送请求）。
    std::mutex sweepMutex;
    std::condition_variable sweepCv;
    std::thread sweeper;
    bool sweeperStarted = false;

    void ensureSweeper() {
        {
            std::lock_guard<std::mutex> lk(sweepMutex);
            if (sweeperStarted) return;
            sweeperStarted = true;
        }
        sweeper = std::thread([this]() {
            while (true) {
                std::unique_lock<std::mutex> lk(sweepMutex);
                sweepCv.wait_for(lk, std::chrono::milliseconds(100),
                                 [this]() { return !running.load() || !sweeperStarted; });
                lk.unlock();
                if (!running.load()) return;
                sweepExpired();
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
        // 与 Java scanResponseTable 同式：beginTimestamp + timeoutMillis + 1000 <= now。
        // 这 1s 宽限是给"响应已经在路上"留的余量——超时后 1s 内到达的响应仍按成功投递，
        // 避免清理线程把刚好赶上末班的响应抢成一次超时失败。
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
        // 回调必须在 respMutex **之外**执行：回调里常常还要回到传输层或业务层，
        // 持锁回调既会和 shutdown 抢同一把锁，也可能自锁。
        for (auto& kv : expired) {
            const std::shared_ptr<Future>& f = kv.first;
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
                f->callback(RemotingCommand(), f->addr + " async invoke timeout "
                                               + std::to_string(f->timeoutMs) + " ms, opaque="
                                               + std::to_string(kv.second));
            }
        }
    }

    // 把响应写回**已存在**的连接（不新建）。
    //
    // 用途：broker 主动请求（典型 PUSH_REPLY_MESSAGE_TO_CLIENT=326）的响应必须原路返回，
    // 而 dispatch() 只有对端地址字符串，故这里按地址查连接表。连接已被关掉时只记一条
    // warn —— 应答本身已经投递给业务了，丢一个响应不该影响读线程。
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
        const bool ok = conn->tls ? conn->tls->writeAll(conn->sock,
                                                        reinterpret_cast<const char*>(data.data()),
                                                        data.size())
                                  : sendAll(conn->sock, data);
        if (!ok) {
            logger_warn("remoting: failed to write response to " + addr);
        }
    }

    void sendRequest(const std::string& addr, RemotingCommand& request) {        // RPC 钩子必须在 encode() **之前**执行：ACL 钩子把 AccessKey/Signature
        // 写进 extFields，而签名覆盖的正是「即将上线的这份 extFields + body」。
        // 先取快照再调用，避免持锁跑钩子。
        if (auto hook = currentHook()) {
            hook->doBeforeRequest(addr, request);
        }
        auto conn = getOrCreateConnection(addr);
        Bytes data = request.encode();
        std::lock_guard<std::mutex> lk(conn->writeMutex);
        const bool sentOk = conn->tls ? conn->tls->writeAll(conn->sock,
                                                            reinterpret_cast<const char*>(data.data()),
                                                            data.size())
                                      : sendAll(conn->sock, data);
        if (!sentOk) {
            closeSocket(conn->sock);
            conn->sock = kInvalidSocket;
            {
                std::lock_guard<std::mutex> clk(connMutex);
                auto it = conns.find(addr);
                if (it != conns.end() && it->second == conn) {
                    conns.erase(it);
                }
            }
            throw RemotingSendRequestException(formatAddr(addr));
        }
    }

    void shutdown() {
        bool expected = true;
        if (!running.compare_exchange_strong(expected, false)) {
            return;  // 已关闭
        }
        // 先停清理线程：它会在回调里回到业务层，不能让它看到半关闭的客户端。
        stopSweeper();
        std::vector<std::shared_ptr<Connection>> all;
        {
            std::lock_guard<std::mutex> lk(connMutex);
            for (auto& kv : conns) {
                all.push_back(kv.second);
            }
            conns.clear();
        }
        // 关闭 socket 让阻塞中的 reader 立刻返回
        for (auto& c : all) {
            closeSocket(c->sock);
            c->sock = kInvalidSocket;
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

// ---------------------------------------------------------------- 公开接口
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
    if (!addr.empty() && addr[0] == '[') {  // IPv6 字面量：[::1]:10911
        size_t end = addr.find(']');
        if (end != std::string::npos) {
            host = addr.substr(1, end - 1);
            size_t colon = addr.find(':', end);
            port = (colon == std::string::npos) ? std::string() : addr.substr(colon + 1);
            return;
        }
    }
    // IPv4 / 主机名：最后一个 ':' 之后是端口
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
    int32_t timeout = timeoutMillis >= 0 ? timeoutMillis : invokeTimeoutMillis_;
    auto future = impl_->registerFutureAcquiringOpaque(request, nullptr);
    const int32_t opaque = request.opaque;
    try {
        impl_->sendRequest(addr, request);
    } catch (...) {
        impl_->unregisterFuture(opaque);
        throw;
    }

    std::unique_lock<std::mutex> lk(future->m);
    bool ok = future->cv.wait_for(lk, std::chrono::milliseconds(timeout),
                                 [&]() { return future->done; });
    if (!ok) {
        lk.unlock();
        impl_->unregisterFuture(opaque);
        throw RemotingTimeoutException(addr + " wait response timeout " + std::to_string(timeout)
                                       + " ms, opaque=" + std::to_string(opaque));
    }
    return future->response;
}

void RemotingClient::invokeAsync(const std::string& addr, RemotingCommand& request,
                                 InvokeCallback callback, int32_t timeoutMillis) {
    const int32_t timeout = timeoutMillis >= 0 ? timeoutMillis : invokeTimeoutMillis_;
    impl_->ensureSweeper();
    impl_->registerFutureAcquiringOpaque(request, std::move(callback), addr, timeout);
    const int32_t opaque = request.opaque;
    try {
        impl_->sendRequest(addr, request);
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
        return false;  // first-wins：已有钩子，保留旧的（与 Java 绑定时机一致）
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
    // 只创建一次 SSL_CTX（对应 Java 构造期 buildSslContext(true)）
    if (!impl_->tlsCtx) {
        std::string err;
        auto ctx = createClientSslContext(err);
        if (!ctx) {
            throw RemotingException("enable TLS failed: " + err);
        }
        impl_->tlsCtx = ctx;
    }
    impl_->tlsEnable = true;
}

bool RemotingClient::tlsEnable() const { return impl_->tlsEnable; }

bool RemotingClient::isChannelWritable(const std::string& addr) const {
    std::lock_guard<std::mutex> lk(impl_->connMutex);
    auto it = impl_->conns.find(addr);
    return it != impl_->conns.end() && it->second->sock != kInvalidSocket;
}

void RemotingClient::closeChannel(const std::string& addr) {
    std::shared_ptr<Impl::Connection> conn;
    {
        std::lock_guard<std::mutex> lk(impl_->connMutex);
        auto it = impl_->conns.find(addr);
        if (it == impl_->conns.end()) {
            return;
        }
        conn = it->second;
        impl_->conns.erase(it);
    }
    // 关闭 socket 会让读线程的 select()/recv() 立刻返回并自行退出
    closeSocket(conn->sock);
    conn->sock = kInvalidSocket;
}

void RemotingClient::updateNameServerAddressList(const std::vector<std::string>& /*addrs*/) {
    // 由上层 MQClientInstance 维护 NameServer 列表；传输层不持有
}

void RemotingClient::shutdown() { impl_->shutdown(); }

size_t RemotingClient::connectionCount() const {
    std::lock_guard<std::mutex> lk(impl_->connMutex);
    return impl_->conns.size();
}

}  // namespace rocketmq
