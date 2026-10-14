// SOCKS proxy transport unit test: a local fake SOCKS5/SOCKS4 server plus a real remoting
// target server, so RemotingClient really connects, handshakes and sends one request.
//
// Coverage:
//   1. SOCKS5 no auth: CONNECT opens the tunnel, one request round trips, tunnel is reused
//   2. SOCKS5 username/password sub-negotiation (RFC 1929): valid credentials succeed
//   3. SOCKS5 auth refused (no client credentials, proxy answers 0xFF): RemotingConnectException
//   4. SOCKS5 wrong password (sub-negotiation status=1): RemotingConnectException
//   5. SOCKS4: CONNECT to an IPv4 literal target succeeds, plus one request
//   6. SocksProxyConfig::resolveHostPort split rules for "host:port" and bare IPv6
//
// Failure assertions get a generous timeout budget for the loopback quirk (a port just closed
// locally still refuses for about 2s): connect timeouts start at 30s, slow beats a false red.
#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstring>
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
#include "rocketmq/remoting/socks_proxy.h"

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

bool readN(socket_t s, unsigned char* buf, size_t n) {
    size_t got = 0;
    while (got < n) {
        int r = static_cast<int>(::recv(s, reinterpret_cast<char*>(buf + got),
                                        static_cast<int>(n - got), 0));
        if (r <= 0) {
            return false;
        }
        got += static_cast<size_t>(r);
    }
    return true;
}

bool writeAll(socket_t s, const unsigned char* buf, size_t n) {
    size_t sent = 0;
    while (sent < n) {
        int w = static_cast<int>(::send(s, reinterpret_cast<const char*>(buf + sent),
                                        static_cast<int>(n - sent), netcompat::sendFlags()));
        if (w <= 0) {
            return false;
        }
        sent += static_cast<size_t>(w);
    }
    return true;
}

// TCP connect (blocking, with the same timeout-via-select style as the transport layer)
socket_t dial(const std::string& host, uint16_t port) {
    struct addrinfo hints;
    std::memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    struct addrinfo* res = nullptr;
    if (::getaddrinfo(host.c_str(), std::to_string(port).c_str(), &hints, &res) != 0 ||
        res == nullptr) {
        return kInvalidSocket;
    }
    socket_t sock = kInvalidSocket;
    for (struct addrinfo* ai = res; ai != nullptr; ai = ai->ai_next) {
        sock = ::socket(ai->ai_family, ai->ai_socktype, ai->ai_protocol);
        if (sock == kInvalidSocket) {
            continue;
        }
        if (::connect(sock, ai->ai_addr, static_cast<int>(ai->ai_addrlen)) == 0) {
            break;
        }
        netcompat::closeSocket(sock);
        sock = kInvalidSocket;
    }
    ::freeaddrinfo(res);
    return sock;
}

// single-threaded select relay in both directions: EOF or an error on either side closes both fds
void relay(socket_t a, socket_t b) {
    while (true) {
        fd_set rfds;
        FD_ZERO(&rfds);
        FD_SET(a, &rfds);
        FD_SET(b, &rfds);
        if (::select(netcompat::selectNfds(a) > netcompat::selectNfds(b)
                         ? netcompat::selectNfds(a)
                         : netcompat::selectNfds(b),
                     &rfds, nullptr, nullptr, nullptr) <= 0) {
            break;
        }
        char buf[16384];
        for (socket_t from : {a, b}) {
            if (!FD_ISSET(from, &rfds)) {
                continue;
            }
            int n = static_cast<int>(::recv(from, buf, sizeof(buf), 0));
            if (n <= 0) {
                netcompat::closeSocket(a);
                netcompat::closeSocket(b);
                return;
            }
            socket_t to = (from == a) ? b : a;
            if (!writeAll(to, reinterpret_cast<const unsigned char*>(buf),
                          static_cast<size_t>(n))) {
                netcompat::closeSocket(a);
                netcompat::closeSocket(b);
                return;
            }
        }
    }
    netcompat::closeSocket(a);
    netcompat::closeSocket(b);
}

// A real remoting target server: accept -> decode frame -> reply SUCCESS (opaque echoed back).
// Same shape as TestServer(Normal) in test_transport.cpp.
class MiniRemotingServer {
public:
    MiniRemotingServer() {
        netcompat::ensureInitialized();
        listenSock_ = ::socket(AF_INET, SOCK_STREAM, 0);
        int one = 1;
        ::setsockopt(listenSock_, SOL_SOCKET, SO_REUSEADDR,
                     reinterpret_cast<const char*>(&one), sizeof(one));
        struct sockaddr_in addr;
        std::memset(&addr, 0, sizeof(addr));
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
    ~MiniRemotingServer() {
        stop_.store(true);
        if (thread_.joinable()) {
            thread_.join();
        }
        netcompat::closeSocket(listenSock_);
    }
    std::string address() const { return "127.0.0.1:" + std::to_string(port_); }
    int served() const { return served_.load(); }

private:
    void serve() {
        while (!stop_.load() && served_.load() < 8) {
            fd_set rfds;
            FD_ZERO(&rfds);
            FD_SET(listenSock_, &rfds);
            timeval tv;
            tv.tv_sec = 0;
            tv.tv_usec = 100 * 1000;
            if (::select(netcompat::selectNfds(listenSock_), &rfds, nullptr, nullptr, &tv) <= 0) {
                continue;
            }
            socket_t conn = ::accept(listenSock_, nullptr, nullptr);
            if (conn == kInvalidSocket) {
                break;
            }
            handle(conn);
            netcompat::closeSocket(conn);
        }
    }
    void handle(socket_t conn) {
        while (!stop_.load() && served_.load() < 8) {
            char lenBuf[4];
            if (!readN(conn, reinterpret_cast<unsigned char*>(lenBuf), 4)) {
                return;
            }
            Bytes lenStr(lenBuf, 4);
            const int32_t totalLen = ByteReader::getInt32At(lenStr, 0);
            if (totalLen <= 0 || totalLen > RemotingClient::MAX_FRAME_LENGTH) {
                return;
            }
            Bytes rest(static_cast<size_t>(totalLen), '\0');
            if (!readN(conn, reinterpret_cast<unsigned char*>(&rest[0]),
                       static_cast<size_t>(totalLen))) {
                return;
            }
            RemotingCommand req;
            if (!RemotingCommand::tryDecode(lenStr + rest, req, nullptr)) {
                return;
            }
            served_.fetch_add(1);
            RemotingCommand resp;
            resp.code = ResponseCode::SUCCESS;
            resp.opaque = req.opaque;
            resp.markResponseType();
            resp.remark = "via-socks";
            resp.hasRemark = true;
            Bytes out = resp.encode();
            writeAll(conn, reinterpret_cast<const unsigned char*>(out.data()), out.size());
        }
    }
    socket_t listenSock_ = kInvalidSocket;
    uint16_t port_ = 0;
    std::atomic<bool> stop_{false};
    std::atomic<int> served_{0};
    std::thread thread_;
};

// Fake SOCKS server: minimal handshake + CONNECT + raw byte relay to the target.
struct SocksConnectRecord {
    std::string host;   // target host from the CONNECT request (recorded verbatim)
    int port = 0;
};

class FakeSocksServer {
public:
    enum class Mode {
        Socks5NoAuth,      // accepts 0x00 only (no authentication)
        Socks5Auth,        // requires 0x02, credentials must equal expectUser/expectPass
        Socks5NoCreds,     // requires 0x02, answers 0xFF when the client never offers it (reject path)
        Socks5AlwaysDeny,  // accepts 0x02 but always fails the username/password check (status=1)
        Socks4,            // SOCKS4 / 4a
    };

    FakeSocksServer(Mode mode, std::string expectUser = std::string(),
                    std::string expectPass = std::string())
        : mode_(mode), expectUser_(std::move(expectUser)), expectPass_(std::move(expectPass)) {
        netcompat::ensureInitialized();
        listenSock_ = ::socket(AF_INET, SOCK_STREAM, 0);
        int one = 1;
        ::setsockopt(listenSock_, SOL_SOCKET, SO_REUSEADDR,
                     reinterpret_cast<const char*>(&one), sizeof(one));
        struct sockaddr_in addr;
        std::memset(&addr, 0, sizeof(addr));
        addr.sin_family = AF_INET;
        addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        addr.sin_port = 0;
        ::bind(listenSock_, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr));
        ::listen(listenSock_, 4);
        socklen_type len = sizeof(addr);
        ::getsockname(listenSock_, reinterpret_cast<struct sockaddr*>(&addr), &len);
        port_ = ntohs(addr.sin_port);
        acceptThread_ = std::thread([this]() { serve(); });
    }

    ~FakeSocksServer() {
        stop_.store(true);
        if (acceptThread_.joinable()) {
            acceptThread_.join();
        }
        for (std::thread& t : handlers_) {
            if (t.joinable()) {
                t.join();
            }
        }
        netcompat::closeSocket(listenSock_);
    }

    int port() const { return port_; }
    int connectCount() const {
        std::lock_guard<std::mutex> lk(m_);
        return static_cast<int>(records_.size());
    }
    std::vector<SocksConnectRecord> records() const {
        std::lock_guard<std::mutex> lk(m_);
        return records_;
    }
    std::vector<std::string> authUsers() const {
        std::lock_guard<std::mutex> lk(m_);
        return authUsers_;
    }
    int rejections() const { return rejections_.load(); }

private:
    void serve() {
        while (!stop_.load()) {
            fd_set rfds;
            FD_ZERO(&rfds);
            FD_SET(listenSock_, &rfds);
            timeval tv;
            tv.tv_sec = 0;
            tv.tv_usec = 100 * 1000;
            if (::select(netcompat::selectNfds(listenSock_), &rfds, nullptr, nullptr, &tv) <= 0) {
                continue;
            }
            socket_t conn = ::accept(listenSock_, nullptr, nullptr);
            if (conn == kInvalidSocket) {
                break;
            }
            handlers_.emplace_back([this, conn]() { handle(conn); });
        }
    }

    void handle(socket_t client) {
        if (mode_ == Mode::Socks4) {
            handleSocks4(client);
        } else {
            handleSocks5(client);
        }
        netcompat::closeSocket(client);
    }

    // Read one 0x00-terminated string (SOCKS4 userid / 4a hostname)
    bool readCString(socket_t s, std::string& out) {
        out.clear();
        unsigned char c = 0;
        while (true) {
            if (!readN(s, &c, 1)) {
                return false;
            }
            if (c == 0x00) {
                return true;
            }
            out.push_back(static_cast<char>(c));
        }
    }

    void handleSocks5(socket_t client) {
        unsigned char head[2] = {0, 0};
        if (!readN(client, head, 2) || head[0] != 0x05 || head[1] < 1 || head[1] > 255) {
            return;
        }
        std::vector<unsigned char> methods(head[1], 0);
        if (!readN(client, methods.data(), methods.size())) {
            return;
        }
        const bool offered0 = std::find(methods.begin(), methods.end(), 0x00) != methods.end();
        const bool offered2 = std::find(methods.begin(), methods.end(), 0x02) != methods.end();

        bool authenticated = false;
        if (mode_ == Mode::Socks5NoAuth) {
            if (!offered0) {
                const unsigned char reply[2] = {0x05, 0xFF};
                writeAll(client, reply, 2);
                rejections_.fetch_add(1);
                return;
            }
            const unsigned char reply[2] = {0x05, 0x00};
            if (!writeAll(client, reply, 2)) {
                return;
            }
            authenticated = true;
        } else {  // Socks5Auth / Socks5NoCreds / Socks5AlwaysDeny
            if (!offered2) {
                const unsigned char reply[2] = {0x05, 0xFF};
                writeAll(client, reply, 2);
                rejections_.fetch_add(1);
                return;
            }
            const unsigned char pick[2] = {0x05, 0x02};
            if (!writeAll(client, pick, 2)) {
                return;
            }
            unsigned char ver = 0;
            if (!readN(client, &ver, 1) || ver != 0x01) {
                return;
            }
            unsigned char ulen = 0;
            if (!readN(client, &ulen, 1)) {
                return;
            }
            std::string user(static_cast<size_t>(ulen), '\0');
            if (!readN(client, reinterpret_cast<unsigned char*>(&user[0]),
                       static_cast<size_t>(ulen))) {
                return;
            }
            unsigned char plen = 0;
            if (!readN(client, &plen, 1)) {
                return;
            }
            std::string pass(static_cast<size_t>(plen), '\0');
            if (!readN(client, reinterpret_cast<unsigned char*>(&pass[0]),
                       static_cast<size_t>(plen))) {
                return;
            }
            const bool ok = mode_ != Mode::Socks5AlwaysDeny && user == expectUser_ &&
                            pass == expectPass_;
            const unsigned char verdict[2] = {0x01, static_cast<unsigned char>(ok ? 0x00 : 0x01)};
            if (!writeAll(client, verdict, 2)) {
                return;
            }
            if (!ok) {
                std::lock_guard<std::mutex> lk(m_);
                authUsers_.push_back(user);
                return;
            }
            std::lock_guard<std::mutex> lk(m_);
            authUsers_.push_back(user);
            authenticated = true;
        }
        if (!authenticated) {
            return;
        }

        unsigned char req[4] = {0, 0, 0, 0};
        if (!readN(client, req, 4) || req[0] != 0x05 || req[1] != 0x01) {  // CONNECT only
            return;
        }
        std::string host;
        if (req[3] == 0x01) {
            unsigned char ip[4] = {0, 0, 0, 0};
            if (!readN(client, ip, 4)) {
                return;
            }
            char buf[16];
            std::snprintf(buf, sizeof(buf), "%u.%u.%u.%u", ip[0], ip[1], ip[2], ip[3]);
            host = buf;
        } else if (req[3] == 0x03) {
            unsigned char len = 0;
            if (!readN(client, &len, 1)) {
                return;
            }
            std::string domain(static_cast<size_t>(len), '\0');
            if (!readN(client, reinterpret_cast<unsigned char*>(&domain[0]),
                       static_cast<size_t>(len))) {
                return;
            }
            host = domain;
        } else if (req[3] == 0x04) {
            unsigned char ip[16] = {0};
            if (!readN(client, ip, 16)) {
                return;
            }
            host = "<ipv6>";
        } else {
            return;
        }
        unsigned char portBuf[2] = {0, 0};
        if (!readN(client, portBuf, 2)) {
            return;
        }
        const int port = (static_cast<int>(portBuf[0]) << 8) | portBuf[1];
        {
            std::lock_guard<std::mutex> lk(m_);
            records_.push_back(SocksConnectRecord{host, port});
        }
        socket_t target = dial(host, static_cast<uint16_t>(port));
        if (target == kInvalidSocket) {
            // target unreachable: reply connection refused for the ATYP in use
            const unsigned char refused[10] = {0x05, 0x05, 0x00, 0x01,
                                               0x00, 0x00, 0x00, 0x00, 0x00, 0x00};
            writeAll(client, refused, 10);
            return;
        }
        const unsigned char okReply[10] = {0x05, 0x00, 0x00, 0x01,
                                           0x00, 0x00, 0x00, 0x00, 0x00, 0x00};
        if (!writeAll(client, okReply, 10)) {
            netcompat::closeSocket(target);
            return;
        }
        relay(client, target);
    }

    void handleSocks4(socket_t client) {
        unsigned char req[8] = {0, 0, 0, 0, 0, 0, 0, 0};
        if (!readN(client, req, 8) || req[0] != 0x04 || req[1] != 0x01) {
            return;
        }
        const int port = (static_cast<int>(req[2]) << 8) | req[3];
        std::string userId;
        if (!readCString(client, userId)) {
            return;
        }
        std::string host;
        if (req[4] == 0 && req[5] == 0 && req[6] == 0 && req[7] == 1) {
            // SOCKS4a marker: the domain name follows the userid
            if (!readCString(client, host)) {
                return;
            }
        } else {
            char buf[16];
            std::snprintf(buf, sizeof(buf), "%u.%u.%u.%u", req[4], req[5], req[6], req[7]);
            host = buf;
        }
        {
            std::lock_guard<std::mutex> lk(m_);
            records_.push_back(SocksConnectRecord{host, port});
            authUsers_.push_back(userId);
        }
        socket_t target = dial(host, static_cast<uint16_t>(port));
        if (target == kInvalidSocket) {
            const unsigned char fail[8] = {0, 0x5B, 0, 0, 0, 0, 0, 0};
            writeAll(client, fail, 8);
            return;
        }
        const unsigned char ok[8] = {0, 0x5A, 0, 0, 0, 0, 0, 0};
        if (!writeAll(client, ok, 8)) {
            netcompat::closeSocket(target);
            return;
        }
        relay(client, target);
    }

    Mode mode_;
    std::string expectUser_;
    std::string expectPass_;
    socket_t listenSock_ = kInvalidSocket;
    uint16_t port_ = 0;
    std::atomic<bool> stop_{false};
    std::atomic<int> rejections_{0};
    std::thread acceptThread_;
    std::vector<std::thread> handlers_;
    mutable std::mutex m_;
    std::vector<SocksConnectRecord> records_;
    std::vector<std::string> authUsers_;
};

bool throwsConnectExceptionWith(const std::string& needle, const SocksProxyConfig& proxy,
                                const std::string& addr) {
    // budget >= 30s for failure assertions (a just-closed loopback port still refuses for ~2s)
    RemotingClient client(30000, 5000);
    client.setSocksProxy(proxy);
    RemotingCommand req = RemotingCommand::createRequestCommand(RequestCode::HEART_BEAT);
    try {
        client.invokeSync(addr, req, 10000);
        return false;
    } catch (const RemotingConnectException& e) {
        return std::string(e.what()).find(needle) != std::string::npos;
    } catch (const RemotingException&) {
        return false;
    }
}

}  // namespace

int main() {
    // ------------------------------------------------ 0. resolveHostPort split rules
    {
        SocksProxyConfig plain;
        plain.addr = "10.0.0.9";
        plain.port = 1080;
        std::string host;
        std::string port;
        plain.resolveHostPort(host, port);
        CHECK(host == "10.0.0.9" && port == "1080", "resolveHostPort: bare host + port field");

        SocksProxyConfig bundled;
        bundled.addr = "10.0.0.9:2080";
        bundled.port = 1080;
        bundled.resolveHostPort(host, port);
        CHECK(host == "10.0.0.9" && port == "2080",
              "resolveHostPort: host:port string overrides port field");

        SocksProxyConfig v6;
        v6.addr = "[::1]:3080";
        v6.port = 1080;
        v6.resolveHostPort(host, port);
        CHECK(host == "::1" && port == "3080", "resolveHostPort: [::1]:port form");

        SocksProxyConfig bareV6;
        bareV6.addr = "::1";
        bareV6.port = 1080;
        bareV6.resolveHostPort(host, port);
        CHECK(host == "::1" && port == "1080", "resolveHostPort: bare IPv6 literal is a host");
    }

    // ------------------------------------------------ 1. SOCKS5 no auth: tunnel setup + request round trip
    {
        MiniRemotingServer target;
        FakeSocksServer proxy(FakeSocksServer::Mode::Socks5NoAuth);
        RemotingClient client;
        SocksProxyConfig proxyCfg;
        proxyCfg.type = SocksProxyConfig::Type::SOCKS5;
        proxyCfg.addr = "127.0.0.1";
        proxyCfg.port = proxy.port();
        client.setSocksProxy(proxyCfg);
        CHECK(client.socksProxyEnabled(), "socksProxyEnabled true after setSocksProxy");

        RemotingCommand req = RemotingCommand::createRequestCommand(RequestCode::HEART_BEAT);
        const int32_t opaque = req.opaque;
        RemotingCommand resp = client.invokeSync(target.address(), req);
        CHECK(resp.code == ResponseCode::SUCCESS, "socks5 no-auth: invokeSync SUCCESS");
        CHECK(resp.opaque == opaque, "socks5 no-auth: opaque matched");
        CHECK(resp.remark == "via-socks", "socks5 no-auth: response came through the tunnel");

        // the second call reuses the same tunnel
        RemotingCommand req2 = RemotingCommand::createRequestCommand(RequestCode::PULL_MESSAGE);
        RemotingCommand resp2 = client.invokeSync(target.address(), req2);
        CHECK(resp2.code == ResponseCode::SUCCESS, "socks5 no-auth: second invoke reused tunnel");
        CHECK(proxy.connectCount() == 1, "socks5 no-auth: exactly one CONNECT for two invokes");
        CHECK(target.served() == 2, "socks5 no-auth: target served both requests");

        // the CONNECT request carried the real target address to the proxy
        const auto recs = proxy.records();
        CHECK(recs.size() == 1 && recs[0].host == "127.0.0.1" &&
                  recs[0].port == std::stoi(target.address().substr(target.address().find(':') + 1)),
              "socks5 no-auth: CONNECT carried the real target host/port");
        client.shutdown();
    }

    // ------------------------------------------------ 2. SOCKS5 username/password sub-negotiation (valid credentials)
    {
        MiniRemotingServer target;
        FakeSocksServer proxy(FakeSocksServer::Mode::Socks5Auth, "socksUser", "socksPass");
        RemotingClient client;
        SocksProxyConfig proxyCfg;
        proxyCfg.addr = "127.0.0.1";
        proxyCfg.port = proxy.port();
        proxyCfg.username = "socksUser";
        proxyCfg.password = "socksPass";
        client.setSocksProxy(proxyCfg);

        RemotingCommand req = RemotingCommand::createRequestCommand(RequestCode::HEART_BEAT);
        RemotingCommand resp = client.invokeSync(target.address(), req);
        CHECK(resp.code == ResponseCode::SUCCESS, "socks5 auth: invokeSync SUCCESS");
        CHECK(resp.remark == "via-socks", "socks5 auth: response came through the tunnel");
        const auto users = proxy.authUsers();
        CHECK(users.size() == 1 && users[0] == "socksUser",
              "socks5 auth: proxy saw the RFC1929 username");
        client.shutdown();
    }

    // ------------------------------------- 3. auth refused: client sends no credentials, proxy answers 0xFF
    {
        MiniRemotingServer target;
        FakeSocksServer proxy(FakeSocksServer::Mode::Socks5NoCreds, "socksUser", "socksPass");
        SocksProxyConfig proxyCfg;
        proxyCfg.addr = "127.0.0.1";
        proxyCfg.port = proxy.port();  // no username/password configured
        CHECK(throwsConnectExceptionWith("none of the offered methods", proxyCfg,
                                         target.address()),
              "socks5 no-creds vs auth-required proxy -> RemotingConnectException");
        CHECK(proxy.rejections() >= 1, "socks5 no-creds: proxy recorded the rejection");
        CHECK(target.served() == 0, "socks5 no-creds: target never saw a request");
    }

    // ------------------------------------------- 4. auth refused: wrong password (sub-negotiation status=1)
    {
        MiniRemotingServer target;
        FakeSocksServer proxy(FakeSocksServer::Mode::Socks5Auth, "socksUser", "socksPass");
        SocksProxyConfig proxyCfg;
        proxyCfg.addr = "127.0.0.1";
        proxyCfg.port = proxy.port();
        proxyCfg.username = "socksUser";
        proxyCfg.password = "wrongPass";
        CHECK(throwsConnectExceptionWith("auth rejected", proxyCfg, target.address()),
              "socks5 wrong password -> RemotingConnectException (auth rejected)");
        CHECK(target.served() == 0, "socks5 wrong password: target never saw a request");
    }

    // ------------------------------------------------ 5. SOCKS4: IPv4 literal CONNECT
    {
        MiniRemotingServer target;
        FakeSocksServer proxy(FakeSocksServer::Mode::Socks4, "uid4");
        RemotingClient client;
        SocksProxyConfig proxyCfg;
        proxyCfg.type = SocksProxyConfig::Type::SOCKS4;
        proxyCfg.addr = "127.0.0.1:" + std::to_string(proxy.port());
        client.setSocksProxy(proxyCfg);

        RemotingCommand req = RemotingCommand::createRequestCommand(RequestCode::HEART_BEAT);
        RemotingCommand resp = client.invokeSync(target.address(), req);
        CHECK(resp.code == ResponseCode::SUCCESS, "socks4: invokeSync SUCCESS");
        CHECK(resp.remark == "via-socks", "socks4: response came through the tunnel");
        const auto recs = proxy.records();
        const int targetPort =
            std::stoi(target.address().substr(target.address().find(':') + 1));
        CHECK(recs.size() == 1 && recs[0].host == "127.0.0.1" && recs[0].port == targetPort,
              "socks4: CONNECT carried the real target host/port");
        client.shutdown();
    }

    if (g_fail == 0) {
        std::cout << "socks_proxy: ALL PASS (" << g_pass << " checks)\n";
        return 0;
    }
    std::cout << "socks_proxy: PASS=" << g_pass << " FAIL=" << g_fail << "\n";
    return 1;
}
