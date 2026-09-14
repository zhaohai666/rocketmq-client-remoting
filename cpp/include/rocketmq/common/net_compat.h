#pragma once

// Windows / POSIX socket 兼容层。
//
// 目的：让 util_all、remoting_client 等所有需要 socket 的代码共用同一套原语，
// 并把两平台的差异集中在这里处理，避免每个文件各写一遍（写漏一处就是一个只能在
// Windows 上暴露的编译错误）。
//
// 集中处理的差异：
//   1. **NOMINMAX 必须在任何 windows.h 之前定义**。否则 windef.h 会定义 min/max
//      宏，把代码里所有 std::min/std::max 展开成 `std::((a)<(b)?(a):(b))` 而编译失败。
//   2. socket 句柄类型：Windows 是 `SOCKET`(UINT_PTR)，POSIX 是 `int`；
//      对应"无效值"分别是 `INVALID_SOCKET` 与 `-1`。**不能互转 int**（64 位下截断）。
//   3. 关闭：Windows 用 `closesocket`，POSIX 用 `close`。
//   4. `socklen_t` 在 Windows 上不存在，统一用 `socklen_type`。
//   5. SIGPIPE：POSIX 上向已关闭连接写入默认会**杀掉进程**，用 MSG_NOSIGNAL 或在
//      建连后设置 SO_NOSIGPIPE。
//   6. `WSAStartup` 必须在任何 winsock 调用之前执行且进程内只需一次。

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

inline void closeSocket(socket_t s) {
    if (isValid(s)) {
        ::closesocket(s);
    }
}

// Windows 无 SIGPIPE 语义：send 失败返回 SOCKET_ERROR 而非发信号
inline int sendFlags() { return 0; }

inline int lastError() { return ::WSAGetLastError(); }

// 进程级 WSA 初始化（线程安全，只在首次调用时执行）
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

inline void closeSocket(socket_t s) {
    if (isValid(s)) {
        ::close(s);
    }
}

#if defined(MSG_NOSIGNAL)
inline int sendFlags() { return MSG_NOSIGNAL; }
#else
// macOS/BSD 没有 MSG_NOSIGNAL：改由 tuneSocket 设置 SO_NOSIGPIPE
inline int sendFlags() { return 0; }
#endif

inline int lastError() { return errno; }

inline void ensureInitialized() {}

#endif

// 建连成功后统一设置：禁用 Nagle 提升延迟敏感请求的表现；POSIX 上按平台关闭 SIGPIPE。
inline void tuneSocket(socket_t s) {
    int one = 1;
    ::setsockopt(s, IPPROTO_TCP, TCP_NODELAY,
                 reinterpret_cast<const char*>(&one), sizeof(one));
#if !defined(_WIN32) && defined(SO_NOSIGPIPE)
    ::setsockopt(s, SOL_SOCKET, SO_NOSIGPIPE,
                 reinterpret_cast<const char*>(&one), sizeof(one));
#endif
}

// select() 的第一个参数：POSIX 要 "最大 fd + 1"，Windows **忽略**该参数。
// 单独封装成函数，避免在 Windows 上把 64 位 SOCKET 硬转成 int（无意义且有告警）。
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
