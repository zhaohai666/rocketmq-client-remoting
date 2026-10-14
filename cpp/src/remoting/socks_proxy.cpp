// SOCKS proxy handshake implementation (SOCKS5 RFC 1928 + RFC 1929 username/password auth / SOCKS4 + the 4a extension).
//
// Everything runs on a blocking socket: the caller (RemotingClient::getOrCreateConnection) has just
// reached the proxy through connectWithTimeout and restored blocking mode, so SO_RCVTIMEO/SO_SNDTIMEO
// is set to the remaining budget first and each stage then does one recv/send. Any unexpected step
// returns false with the **stage name + reason** -- troubleshooting should tell at a glance whether
// method negotiation was rejected, authentication failed or CONNECT was refused by the remote.
#include "rocketmq/remoting/socks_proxy.h"

#include <chrono>
#include <cstring>
#include <exception>
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

// Single-byte / fixed-length reads, relying on SO_RCVTIMEO as the fallback timeout; false means the peer closed or timed out.
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

// RFC 1928 reply code texts (second byte of the CONNECT response)
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

// Second byte of the SOCKS4 response (0x5A=90 is the only success code)
const char* socks4ReplyMessage(unsigned char code) {
    switch (code) {
        case 0x5A: return "succeeded";
        case 0x5B: return "request rejected or failed";
        case 0x5C: return "request failed because client is not running identd";
        case 0x5D: return "request failed because client's identd cannot confirm the user id";
        default: return "unknown SOCKS4 reply code";
    }
}

// Remaining milliseconds: the whole handshake shares one budget, reconciled between stages so a slow proxy cannot burn the full timeout at every step
int32_t remainingMs(int64_t deadlineMs) {
    const int64_t now = std::chrono::duration_cast<std::chrono::milliseconds>(
                            std::chrono::steady_clock::now().time_since_epoch())
                            .count();
    if (now >= deadlineMs) {
        return 1;  // still leave IO one chance to fail immediately
    }
    return static_cast<int32_t>(deadlineMs - now);
}

// ---- SOCKS5: method negotiation plus the optional RFC 1929 subnegotiation ----
// Timeouts are covered uniformly by the SO_RCVTIMEO/SO_SNDTIMEO set by setIoTimeout.
bool socks5Greeting(netcompat::socket_t sock, const SocksProxyConfig& proxy, std::string& err) {
    const bool useAuth = !proxy.username.empty() || !proxy.password.empty();
    unsigned char greeting[4] = {0x05, 0x01,
                                 static_cast<unsigned char>(useAuth ? 0x02u : 0x00u), 0x00};
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
        // RFC 1929: ver(1) ulen(1) uname ulen plen(1) passwd plen
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
            // advertising only 0x02 yet selecting 0x00 is a server-side violation, and going on can never work out
            err = "proxy picked no-auth although credentials were configured";
            return false;
        }
        return true;
    }
    err = "proxy picked unsupported method " + std::to_string(picked[1]);
    return false;
}

// Encode the target address according to ATYP (IPv4 literal / IPv6 literal / domain passed through)
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
    // Read the fixed 4-byte header first, then consume BND.ADDR/BND.PORT in full according to ATYP
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
        // SOCKS4a: when the target is not an IPv4 literal, mark it with 0.0.0.x (x != 0) plus the domain after the userid.
        // 0.0.0.1 is the conventional value (curl / openssh do the same).
        request.push_back(0);
        request.push_back(0);
        request.push_back(0);
        request.push_back(1);
    }
    request.insert(request.end(), proxy.username.begin(), proxy.username.end());
    request.push_back(0x00);  // userid terminator
    if (!isV4) {
        if (targetHost.empty() || targetHost.size() > 255) {
            err = "target host must be 1..255 bytes for SOCKS4a domain encoding";
            return false;
        }
        request.insert(request.end(), targetHost.begin(), targetHost.end());
        request.push_back(0x00);  // domain name terminator
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
        // a single ':' is the "host:port" form; several ':' mean a bare IPv6 literal, taken as the host in full
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
    // Once the tunnel is up the default IO timeout is restored (the reader thread paces itself with select, so no SO_RCVTIMEO is left to interfere)
    setIoTimeout(sock, 0);
    return true;
}

}  // namespace rocketmq
