#ifndef ROCKETMQ_REMOTING_SOCKS_PROXY_H
#define ROCKETMQ_REMOTING_SOCKS_PROXY_H

#include <cstdint>
#include <string>

#include "rocketmq/common/net_compat.h"

namespace rocketmq {

struct SocksProxyConfig {
    enum class Type {
        SOCKS5 = 5,
        SOCKS4 = 4,
    };

    Type type = Type::SOCKS5;
    std::string addr;
    int32_t port = 1080;   // Proxy port; addr wins when it already carries ":port"
    std::string username;  // Optional credentials (SOCKS5 username/password sub-negotiation / SOCKS4 userid)
    std::string password;

    bool empty() const { return addr.empty(); }

    void resolveHostPort(std::string& host, std::string& portStr) const;
};

// Completes the SOCKS handshake (CONNECT) on a socket **already connected to the proxy**,
// establishing a tunnel to targetHost:targetPort.
//
//   * SOCKS5: send method negotiation first (offer only 0x02 when username/password are
//     configured, otherwise only 0x00); once 0x02 is selected, continue RFC 1929 username/password
//   * SOCKS4: the target must be an IPv4 literal (filled in locally); otherwise the domain name is
//     resolved by the proxy via the SOCKS4a extension (4-byte address set to the 0.0.0.1 marker plus the domain suffix).
//
// Returns true on success, and sock becomes the tunnel to the target; returns false on failure and
// puts the reason in err (including the SOCKS5 REP / SOCKS4 code wording); the caller closes the socket.
bool socksConnect(netcompat::socket_t sock, const SocksProxyConfig& proxy,
                  const std::string& targetHost, const std::string& targetPort,
                  int32_t timeoutMillis, std::string& err);

}  // namespace rocketmq

#endif  // ROCKETMQ_REMOTING_SOCKS_PROXY_H
