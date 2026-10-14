#pragma once

// Windows / POSIX socket compatibility layer.
//
// Purpose: let util_all, remoting_client and every other socket-using code share one set of primitives,
// and handle the platform differences here in a single place instead of rewriting them per file (a missing
// spot becomes a compile error that only shows up on Windows).
//
// Differences handled centrally:
//   1. **NOMINMAX must be defined before any windows.h**. Otherwise windef.h defines min/max
//      macros that expand every std::min/std::max into `std::((a)<(b)?(a):(b))` and compilation fails.
//   2. Socket handle type: Windows uses `SOCKET`(UINT_PTR), POSIX uses `int`;
//      the corresponding "invalid value" is `INVALID_SOCKET` and `-1`. **Never round-trip through int** (truncation on 64-bit).
//   3. Closing: Windows uses `closesocket`, POSIX uses `close`.
//   4. `socklen_t` does not exist on Windows; `socklen_type` is used uniformly.
//   5. SIGPIPE: on POSIX, writing to a closed connection **kills the process** by default; use MSG_NOSIGNAL or set
//      SO_NOSIGPIPE after connecting.
//   6. `WSAStartup` must run before any winsock call, and only once per process.

#ifndef NOMINMAX
#define NOMINMAX
#endif
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif

#include <cerrno>
#include <cstdint>
#include <mutex>

#ifdef _WIN32
#include <winsock2.h>
#include <ws2tcpip.h>
#else
#include <arpa/inet.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/select.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>
#endif

namespace rocketmq {
namespace netcompat {

#ifdef _WIN32

using socket_t = SOCKET;
using socklen_type = int;
const socket_t kInvalidSocket = INVALID_SOCKET;

inline bool isValid(socket_t s) { return s != INVALID_SOCKET; }

// shutdown(SD_BOTH) before closing the socket: see the same note in the POSIX branch.
inline void closeSocket(socket_t s) {
    if (isValid(s)) {
        ::shutdown(s, SD_BOTH);
        ::closesocket(s);
    }
}

// Windows has no SIGPIPE semantics: a failed send returns SOCKET_ERROR instead of raising a signal
inline int sendFlags() { return 0; }

inline int lastError() { return ::WSAGetLastError(); }

// Process-level WSA initialization (thread safe, runs only on the first call)
inline void ensureInitialized() {
    static std::once_flag once;
    std::call_once(once, []() {
        WSADATA data;
        ::WSAStartup(MAKEWORD(2, 2), &data);
    });
}

#else

using socket_t = int;
using socklen_type = socklen_t;
const socket_t kInvalidSocket = -1;

inline bool isValid(socket_t s) { return s >= 0; }

// shutdown(SHUT_RDWR) before closing the socket: on POSIX close(fd) does **not** wake threads already asleep in
// recv/select (macOS does, so this kind of hang only surfaces on Linux). Without this step,
// threads still reading this connection stay blocked forever, and the exit path joining them (shutdown/destructor) never returns.
// A failed shutdown does not matter (an unconnected/already-closed fd returns ENOTCONN); close follows right after.
inline void closeSocket(socket_t s) {
    if (isValid(s)) {
        ::shutdown(s, SHUT_RDWR);
        ::close(s);
    }
}

#if defined(MSG_NOSIGNAL)
inline int sendFlags() { return MSG_NOSIGNAL; }
#else
// macOS/BSD have no MSG_NOSIGNAL: tuneSocket sets SO_NOSIGPIPE instead
inline int sendFlags() { return 0; }
#endif

inline int lastError() { return errno; }

inline void ensureInitialized() {}

#endif

// Set uniformly after a successful connection: disable Nagle to improve latency-sensitive requests; on POSIX disable SIGPIPE per platform.
inline void tuneSocket(socket_t s) {
    int one = 1;
    ::setsockopt(s, IPPROTO_TCP, TCP_NODELAY,
                 reinterpret_cast<const char*>(&one), sizeof(one));
#if !defined(_WIN32) && defined(SO_NOSIGPIPE)
    ::setsockopt(s, SOL_SOCKET, SO_NOSIGPIPE,
                 reinterpret_cast<const char*>(&one), sizeof(one));
#endif
}

// Put an upper bound on a blocking recv. Between select reporting "readable" and the actual recv there is still a
// window where the fd can be withdrawn/reused (see readLoop in remoting_client.cpp); the select timeout alone does not cover
// that instant asleep inside recv; SO_RCVTIMEO expires with an EAGAIN failure, so every blocking round of the read thread is bounded.
inline void setRecvTimeout(socket_t s, int millis) {
#ifdef _WIN32
    DWORD timeout = static_cast<DWORD>(millis);
    ::setsockopt(s, SOL_SOCKET, SO_RCVTIMEO,
                 reinterpret_cast<const char*>(&timeout), sizeof(timeout));
#else
    struct timeval tv;
    tv.tv_sec = millis / 1000;
    tv.tv_usec = (millis % 1000) * 1000;
    ::setsockopt(s, SOL_SOCKET, SO_RCVTIMEO,
                 reinterpret_cast<const char*>(&tv), sizeof(tv));
#endif
}

// "no data this round" rather than "the connection dropped": an SO_RCVTIMEO expiry or an interrupt by a signal must keep waiting,
// otherwise an idle connection would be mistaken for a disconnect by its own read thread.
inline bool isWouldBlock() {
#ifdef _WIN32
    return lastError() == WSAEWOULDBLOCK;
#else
    return lastError() == EAGAIN || lastError() == EWOULDBLOCK;
#endif
}

inline bool isInterrupted() {
#ifdef _WIN32
    return lastError() == WSAEINTR;
#else
    return lastError() == EINTR;
#endif
}

// First argument of select(): POSIX wants "highest fd + 1", Windows **ignores** the argument.
// Wrapped in its own function to avoid hard-casting a 64-bit SOCKET to int on Windows (pointless and warning-prone).
inline int selectNfds(socket_t s) {
#ifdef _WIN32
    (void)s;
    return 0;
#else
    return static_cast<int>(s) + 1;
#endif
}

}  // namespace netcompat
}  // namespace rocketmq
