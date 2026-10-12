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
    int32_t port = 1080;   // 代理端口；addr 自带 ":port" 时以 addr 为准
    std::string username;  // 可选凭据（SOCKS5 用户名密码子协商 / SOCKS4 userid）
    std::string password;

    bool empty() const { return addr.empty(); }

    void resolveHostPort(std::string& host, std::string& portStr) const;
};

// 在**已连到代理**的 socket 上完成 SOCKS 握手（CONNECT），建立到
// targetHost:targetPort 的隧道。
//
//   * SOCKS5：先发方法协商（配了 username/password 只报 0x02，否则只报 0x00），
//     选中 0x02 时继续 RFC 1929 用户名/密码子协商；
//   * SOCKS4：目标必须是 IPv4 字面量（本地直填）；否则按 SOCKS4a 扩展把域名交给
//     代理解析（4 字节地址填 0.0.0.1 标记 + 域名后缀）。
//
// 成功返回 true，sock 即为通往目标的隧道；失败返回 false 并在 err 给出原因
// （含 SOCKS5 REP / SOCKS4 code 的文案），调用方负责关闭 socket。
bool socksConnect(netcompat::socket_t sock, const SocksProxyConfig& proxy,
                  const std::string& targetHost, const std::string& targetPort,
                  int32_t timeoutMillis, std::string& err);

}  // namespace rocketmq

#endif  // ROCKETMQ_REMOTING_SOCKS_PROXY_H
