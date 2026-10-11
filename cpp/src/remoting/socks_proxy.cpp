// SOCKS 代理握手的实现（SOCKS5 RFC 1928 + RFC 1929 用户名密码认证 / SOCKS4 + 4a 扩展）。
//
// 全部走阻塞 socket：调用方（RemotingClient::getOrCreateConnection）刚用
// connectWithTimeout 连上代理并恢复了阻塞模式，这里先把 SO_RCVTIMEO/SO_SNDTIMEO
// 设成剩余预算，之后每个阶段一次 recv/send。任何一步不符预期都带着**阶段名 + 原因**
// 返回 false —— 排障时最好能直接看出是「方法协商被拒」还是「认证失败」还是「CONNECT
// 被 远端拒绝」。
//
// TopAddressing 的动态取址 HTTP（top_addressing.cpp 的 httpGet）不走这里：SOCKS 只挂在
// RemotingClient 的建连路径上，与 Java 把 Socks5ProxyHandler 只加进 Netty bootstrap 的
// 口径一致。
#include "rocketmq/remoting/socks_proxy.h"

#include <chrono>
#include <cstring>
#include <string>
#include <vector>

#ifdef _WIN32
#include <winsock2.h>
#include <ws2tcpip.h>
#else
#include <arpa/inet.h>
#include <netdb.h>
#include <sys/socket.h>
#include <sys/time.h>
#endif

namespace rocketmq {

namespace {

// 单字节 / 定长读取。依赖 SO_RCVTIMEO 兜底超时；返回 false 表示对端关闭或超时。
bool readFull(netcompat::socket_t sock, unsigned char* buf, size_t n) {
    size_t got = 0;
    while (got < n) {
        int r = static_cast<int>(::recv(sock, reinterpret_cast<char*>(buf + got),
                                        static_cast<int>(n - got), 0));
        if (r <= 0) {
            return false;
        }
        got += static_cast<size_t>(r);
    }
    return true;
}

bool writeFull(netcompat::socket_t sock, const unsigned char* buf, size_t n) {
    size_t sent = 0;
    while (sent < n) {
        int w = static_cast<int>(::send(sock, reinterpret_cast<const char*>(buf + sent),
                                        static_cast<int>(n - sent), netcompat::sendFlags()));
        if (w <= 0) {
            return false;
        }
        sent += static_cast<size_t>(w);
    }
    return true;
}

void setIoTimeout(netcompat::socket_t sock, int32_t timeoutMillis) {
    struct timeval tv;
    tv.tv_sec = timeoutMillis / 1000;
    tv.tv_usec = (timeoutMillis % 1000) * 1000;
    ::setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, reinterpret_cast<const char*>(&tv), sizeof(tv));
    ::setsockopt(sock, SOL_SOCKET, SO_SNDTIMEO, reinterpret_cast<const char*>(&tv), sizeof(tv));
}

// RFC 1928 的 reply code 文案（CONNECT 响应第 2 字节）
const char* socks5RepMessage(unsigned char rep) {
    switch (rep) {
        case 0x00: return "succeeded";
        case 0x01: return "general SOCKS server failure";
        case 0x02: return "connection not allowed by ruleset";
        case 0x03: return "network unreachable";
        case 0x04: return "host unreachable";
        case 0x05: return "connection refused";
        case 0x06: return "TTL expired";
        case 0x07: return "command not supported";
        case 0x08: return "address type not supported";
        default: return "unknown SOCKS5 reply code";
    }
}

// SOCKS4 响应第 2 字节（0x5A=90 是唯一成功码）
const char* socks4ReplyMessage(unsigned char code) {
    switch (code) {
        case 0x5A: return "succeeded";
        case 0x5B: return "request rejected or failed";
        case 0x5C: return "request failed because client is not running identd";
        case 0x5D: return "request failed because client's identd cannot confirm the user id";
        default: return "unknown SOCKS4 reply code";
    }
}

// 剩余毫秒数：整个握手共用一个预算，阶段间对账，防止慢代理把每一步都拖满超时
int32_t remainingMs(int64_t deadlineMs) {
    const int64_t now = std::chrono::duration_cast<std::chrono::milliseconds>(
                            std::chrono::steady_clock::now().time_since_epoch())
                            .count();
    if (now >= deadlineMs) {
        return 1;  // 至少给 IO 一次立刻失败的机会
    }
    return static_cast<int32_t>(deadlineMs - now);
}

// ---- SOCKS5：方法协商 + 可选的 RFC 1929 子协商 ----
// 超时统一由 setIoTimeout 设好的 SO_RCVTIMEO/SO_SNDTIMEO 兜底。
bool socks5Greeting(netcompat::socket_t sock, const SocksProxyConfig& proxy, std::string& err) {
    const bool useAuth = !proxy.username.empty() || !proxy.password.empty();
    unsigned char greeting[4] = {0x05, 0x01, useAuth ? 0x02u : 0x00u, 0x00};
    if (!writeFull(sock, greeting, 3)) {
        err = "send method selection failed";
        return false;
    }
    unsigned char picked[2] = {0, 0};
    if (!readFull(sock, picked, 2)) {
        err = "read method selection reply failed";
        return false;
    }
    if (picked[0] != 0x05) {
        err = "method selection reply is not SOCKS5 (ver=" + std::to_string(picked[0]) + ")";
        return false;
    }
    if (picked[1] == 0xFF) {
        err = "proxy accepts none of the offered methods"
              + std::string(useAuth ? " (username/password was offered)" : " (no auth offered)");
        return false;
    }
    if (picked[1] == 0x02) {
        // RFC 1929：ver(1) ulen(1) uname ulen plen(1) passwd plen
        std::string user = proxy.username;
        std::string pass = proxy.password;
        if (user.size() > 255 || pass.size() > 255) {
            err = "username/password longer than 255 bytes";
            return false;
        }
        std::vector<unsigned char> auth;
        auth.push_back(0x01);
        auth.push_back(static_cast<unsigned char>(user.size()));
        auth.insert(auth.end(), user.begin(), user.end());
        auth.push_back(static_cast<unsigned char>(pass.size()));
        auth.insert(auth.end(), pass.begin(), pass.end());
        if (!writeFull(sock, auth.data(), auth.size())) {
            err = "send username/password auth failed";
            return false;
        }
        unsigned char verdict[2] = {0, 0};
        if (!readFull(sock, verdict, 2)) {
            err = "read username/password auth reply failed";
            return false;
        }
        if (verdict[0] != 0x01) {
            err = "username/password auth reply has wrong version " + std::to_string(verdict[0]);
            return false;
        }
        if (verdict[1] != 0x00) {
            err = "username/password auth rejected by proxy (status=" +
                  std::to_string(verdict[1]) + ")";
            return false;
        }
        return true;
    }
    if (picked[1] == 0x00) {
        if (useAuth) {
            // 只报了 0x02 还被选中 0x00 属于服务端违规，继续走必然对不上账
            err = "proxy picked no-auth although credentials were configured";
            return false;
        }
        return true;
    }
    err = "proxy picked unsupported method " + std::to_string(picked[1]);
    return false;
}

// 把目标地址按 ATYP 编码（IPv4 字面量 / IPv6 字面量 / 域名直传）
bool socks5EncodeTarget(const std::string& host, const std::string& portStr,
                        std::vector<unsigned char>& out, std::string& err) {
    int port = 0;
    try {
        port = std::stoi(portStr);
    } catch (const std::exception&) {
        err = "invalid target port " + portStr;
        return false;
    }
    if (port <= 0 || port > 65535) {
        err = "invalid target port " + portStr;
        return false;
    }
    out.push_back(0x05);
    out.push_back(0x01);  // CONNECT
    out.push_back(0x00);  // RSV
    unsigned char v4[4];
    unsigned char v6[16];
    if (::inet_pton(AF_INET, host.c_str(), v4) == 1) {
        out.push_back(0x01);
        out.insert(out.end(), v4, v4 + 4);
    } else if (::inet_pton(AF_INET6, host.c_str(), v6) == 1) {
        out.push_back(0x04);
        out.insert(out.end(), v6, v6 + 16);
    } else {
        if (host.empty() || host.size() > 255) {
            err = "target host must be 1..255 bytes for SOCKS5 domain encoding";
            return false;
        }
        out.push_back(0x03);
        out.push_back(static_cast<unsigned char>(host.size()));
        out.insert(out.end(), host.begin(), host.end());
    }
    out.push_back(static_cast<unsigned char>((port >> 8) & 0xFF));
    out.push_back(static_cast<unsigned char>(port & 0xFF));
    return true;
}

bool socks5Connect(netcompat::socket_t sock, const std::string& targetHost,
                   const std::string& targetPort, std::string& err) {
    std::vector<unsigned char> request;
    if (!socks5EncodeTarget(targetHost, targetPort, request, err)) {
        return false;
    }
    if (!writeFull(sock, request.data(), request.size())) {
        err = "send CONNECT request failed";
        return false;
    }
    // 先读定长 4 字节头，再按 ATYP 把 BND.ADDR/BND.PORT 消费干净
    unsigned char head[4] = {0, 0, 0, 0};
    if (!readFull(sock, head, 4)) {
        err = "read CONNECT reply failed";
        return false;
    }
    if (head[0] != 0x05) {
        err = "CONNECT reply is not SOCKS5 (ver=" + std::to_string(head[0]) + ")";
        return false;
    }
    if (head[1] != 0x00) {
        err = std::string("CONNECT rejected by proxy: ") + socks5RepMessage(head[1]) +
              " (rep=" + std::to_string(head[1]) + ")";
        return false;
    }
    size_t tail = 0;
    if (head[3] == 0x01) {
        tail = 4 + 2;
    } else if (head[3] == 0x03) {
        unsigned char len = 0;
        if (!readFull(sock, &len, 1)) {
            err = "read CONNECT reply (domain length) failed";
            return false;
        }
        tail = static_cast<size_t>(len) + 2;
    } else if (head[3] == 0x04) {
        tail = 16 + 2;
    } else {
        err = "CONNECT reply has unknown ATYP " + std::to_string(head[3]);
        return false;
    }
    std::vector<unsigned char> bnd(tail, 0);
    if (!readFull(sock, bnd.data(), tail)) {
        err = "read CONNECT reply (bound address) failed";
        return false;
    }
    return true;
}

// ---- SOCKS4 / SOCKS4a ----
bool socks4Connect(netcompat::socket_t sock, const std::string& targetHost,
                   const std::string& targetPort, const SocksProxyConfig& proxy,
                   std::string& err) {
    int port = 0;
    try {
        port = std::stoi(targetPort);
    } catch (const std::exception&) {
        err = "invalid target port " + targetPort;
        return false;
    }
    if (port <= 0 || port > 65535) {
        err = "invalid target port " + targetPort;
        return false;
    }
    unsigned char ip[4] = {0, 0, 0, 0};
    const bool isV4 = ::inet_pton(AF_INET, targetHost.c_str(), ip) == 1;
    std::vector<unsigned char> request;
    request.push_back(0x04);
    request.push_back(0x01);  // CONNECT
    request.push_back(static_cast<unsigned char>((port >> 8) & 0xFF));
    request.push_back(static_cast<unsigned char>(port & 0xFF));
    if (isV4) {
        request.insert(request.end(), ip, ip + 4);
    } else {
        // SOCKS4a：目标不是 IPv4 字面量时用 0.0.0.x（x != 0）标记 + userid 后的域名。
        // 0.0.0.1 是惯例取值（curl / openssh 同）。
        request.push_back(0);
        request.push_back(0);
        request.push_back(0);
        request.push_back(1);
    }
    request.insert(request.end(), proxy.username.begin(), proxy.username.end());
    request.push_back(0x00);  // userid 终止符
    if (!isV4) {
        if (targetHost.empty() || targetHost.size() > 255) {
            err = "target host must be 1..255 bytes for SOCKS4a domain encoding";
            return false;
        }
        request.insert(request.end(), targetHost.begin(), targetHost.end());
        request.push_back(0x00);  // 域名终止符
    }
    if (!writeFull(sock, request.data(), request.size())) {
        err = "send SOCKS4 CONNECT request failed";
        return false;
    }
    unsigned char reply[8] = {0, 0, 0, 0, 0, 0, 0, 0};
    if (!readFull(sock, reply, 8)) {
        err = "read SOCKS4 CONNECT reply failed";
        return false;
    }
    if (reply[1] != 0x5A) {
        err = std::string("SOCKS4 CONNECT rejected by proxy: ") + socks4ReplyMessage(reply[1]) +
              " (code=" + std::to_string(reply[1]) + ")";
        return false;
    }
    return true;
}

}  // namespace

void SocksProxyConfig::resolveHostPort(std::string& host, std::string& portStr) const {
    // addr 自带 ":port"（Java 口径，如 "10.0.0.1:1080"、IPv6 "[::1]:1080"）时覆盖 port 字段
    if (!addr.empty() && addr[0] == '[') {
        const size_t end = addr.find(']');
        if (end != std::string::npos) {
            host = addr.substr(1, end - 1);
            const size_t colon = addr.find(':', end);
            portStr = (colon == std::string::npos) ? std::to_string(port) : addr.substr(colon + 1);
            return;
        }
    }
    const size_t colon = addr.rfind(':');
    if (colon != std::string::npos && addr.find(':') == colon) {
        // 单个 ':' —— "host:port" 形式；多个 ':' 是裸 IPv6 字面量，整体当 host
        host = addr.substr(0, colon);
        portStr = addr.substr(colon + 1);
        return;
    }
    host = addr;
    portStr = std::to_string(port);
}

bool socksConnect(netcompat::socket_t sock, const SocksProxyConfig& proxy,
                  const std::string& targetHost, const std::string& targetPort,
                  int32_t timeoutMillis, std::string& err) {
    if (proxy.empty()) {
        err = "socks proxy is not configured";
        return false;
    }
    const int64_t deadlineMs =
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now().time_since_epoch())
            .count() +
        timeoutMillis;
    setIoTimeout(sock, remainingMs(deadlineMs));
    bool ok = false;
    if (proxy.type == SocksProxyConfig::Type::SOCKS5) {
        ok = socks5Greeting(sock, proxy, err) && socks5Connect(sock, targetHost, targetPort, err);
    } else {
        ok = socks4Connect(sock, targetHost, targetPort, proxy, err);
    }
    if (!ok) {
        return false;
    }
    // 隧道建好后恢复默认 IO 超时（读线程自己用 select 控节奏，不留 SO_RCVTIMEO 干扰）
    setIoTimeout(sock, 0);
    return true;
}

}  // namespace rocketmq
