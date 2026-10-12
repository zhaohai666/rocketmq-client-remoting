<?php

declare(strict_types=1);

namespace RocketMQ\Remoting;

/**
 * SOCKS 代理配置（对应 org.apache.rocketmq.remoting.proxy.SocksProxyConfig）。
 *
 * Java 侧字段：addr（"host:port"）、username、password，经 NettyClientConfig 的
 * ``socksProxyConfig`` JSON 注入后按 CIDR 匹配生效；本端口不搞 CIDR 匹配表——
 * 配了就作用于全部出向 remoting 连接（NameServer + Broker），语义见
 * RemotingClient::$socksProxy。TopAddressing 的 HTTP 取址不走代理（与 Java 同口径：
 * 动态取址走 HttpTinyClient，不经过带 Socks5ProxyHandler 的 bootstrap）。
 *
 * 在 Java 之上多一个 ``version`` 字段：Java 用 Netty Socks5ProxyHandler 只支持
 * SOCKS5；本端口手写握手，顺带补 SOCKS4（地址解析不了时退 SOCKS4a 的域名扩展）。
 * 缺省 socks5，与 Java 对齐。
 */
final class SocksProxyConfig
{
    public const VERSION_SOCKS4 = 'socks4';
    public const VERSION_SOCKS5 = 'socks5';

    public function __construct(
        /** 代理地址，``host:port``（Java 同名字段；IPv6 字面量写 ``[::1]:1080``）。 */
        private ?string $addr = null,
        /** 认证用户名（SOCKS5 用户名/密码子协商；SOCKS4 作为 userid）。 */
        private ?string $username = null,
        /** 认证密码（仅 SOCKS5 子协商使用）。 */
        private ?string $password = null,
        /** 协议版本：socks4 / socks5（缺省 socks5，与 Java 的 Socks5ProxyHandler 对齐）。 */
        private string $version = self::VERSION_SOCKS5,
    ) {
    }

    public function getAddr(): ?string
    {
        return $this->addr;
    }

    public function setAddr(?string $addr): void
    {
        $this->addr = $addr;
    }

    public function getUsername(): ?string
    {
        return $this->username;
    }

    public function setUsername(?string $username): void
    {
        $this->username = $username;
    }

    public function getPassword(): ?string
    {
        return $this->password;
    }

    public function setPassword(?string $password): void
    {
        $this->password = $password;
    }

    public function getVersion(): string
    {
        return $this->version;
    }

    public function setVersion(string $version): void
    {
        $this->version = $version;
    }

    /** 是否已配置到可用状态（addr 非空）。 */
    public function isConfigured(): bool
    {
        return $this->addr !== null && $this->addr !== '';
    }

    /**
     * 归一化版本字段：'4'/'socks4' → socks4，'5'/'socks5' → socks5；其余抛
     * InvalidArgumentException（配置写错在启动期暴露，而不是连接期静默降级）。
     */
    public function normalizedVersion(): string
    {
        $v = strtolower(trim($this->version));
        return match ($v) {
            '4', self::VERSION_SOCKS4 => self::VERSION_SOCKS4,
            '5', self::VERSION_SOCKS5 => self::VERSION_SOCKS5,
            default => throw new \InvalidArgumentException(sprintf('unknown socks proxy version: %s', $this->version)),
        };
    }

    /** 对齐 Java SocksProxyConfig#toString 的口径。 */
    public function __toString(): string
    {
        return sprintf('SocksProxy address: %s, username: %s, password: %s', $this->addr, $this->username, $this->password);
    }
}
