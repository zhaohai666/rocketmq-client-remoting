// SOCKS 代理支持（对应 Java ClientConfig#socksProxyConfig / NettyClientConfig 的
// socksProxyConfig + NettyRemotingClient#createBootstrap(SocksProxyConfig)）。
//
// Java 侧的字段是 addr（"host:port" 一整个串）/ username / password，Netty 用
// Socks5ProxyHandler 固定走 SOCKS5；C++ 端在此基础上补一个显式 type（SOCKS4 / SOCKS5，
// 默认 SOCKS5），认证方式对齐 Java：SOCKS5 用户名密码子协商（RFC 1929，method 0x02），
// 不配凭据即走无认证（method 0x00）。
//
// 挂接点（对应 Java pipeline.addFirst(Socks5ProxyHandler)）：TCP 先连**代理**，完成
// SOCKS CONNECT 握手建立隧道之后再做 TLS 握手与 RocketMQ 帧。目标地址不在本端解析
// （SOCKS5 域名 ATYP 直传，DNS 由代理侧完成 —— 与 Java bootstrap.resolver(
// NoopAddressResolverGroup.INSTANCE) 的行为一致）。
#ifndef ROCKETMQ_REMOTING_SOCKS_PROXY_H
#define ROCKETMQ_REMOTING_SOCKS_PROXY_H

#include <cstdint>
#include <string>

#include "rocketmq/common/net_compat.h"

namespace rocketmq {

struct SocksProxyConfig {
    // Java 侧固定 SOCKS5；这里显式给出两种协议版本
    enum class Type {
        SOCKS5 = 5,
        SOCKS4 = 4,
    };

    Type type = Type::SOCKS5;
    std::string addr;      // 代理地址（host）；也接受 Java 口径的 "host:port" 整串
    int32_t port = 1080;   // 代理端口；addr 自带 ":port" 时以 addr 为准
    std::string username;  // 可选凭据（SOCKS5 用户名密码子协商 / SOCKS4 userid）
    std::string password;

    bool empty() const { return addr.empty(); }

    // 归一出代理 host 与端口字符串。addr 形如 "host:port"（对齐 Java
    // SocksProxyConfig.addr 的口径）时拆开并覆盖 port 字段，否则直接用 addr + port。
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
