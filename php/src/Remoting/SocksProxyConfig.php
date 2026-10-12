<?php

declare(strict_types=1);

namespace RocketMQ\Remoting;

final class SocksProxyConfig
{
    public const VERSION_SOCKS4 = 'socks4';
    public const VERSION_SOCKS5 = 'socks5';

    public function __construct(
        private ?string $addr = null,
        /** 认证用户名（SOCKS5 用户名/密码子协商；SOCKS4 作为 userid）。 */
        private ?string $username = null,
        /** 认证密码（仅 SOCKS5 子协商使用）。 */
        private ?string $password = null,
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

    public function __toString(): string
    {
        return sprintf('SocksProxy address: %s, username: %s, password: %s', $this->addr, $this->username, $this->password);
    }
}
