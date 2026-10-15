<?php

declare(strict_types=1);

namespace RocketMQ\Remoting;

final class SocksProxyConfig
{
    public const VERSION_SOCKS4 = 'socks4';
    public const VERSION_SOCKS5 = 'socks5';

    public function __construct(
        private ?string $addr = null,
        /** Auth username (SOCKS5 username/password sub-negotiation; used as userid for SOCKS4). */
        private ?string $username = null,
        /** Auth password (used only in the SOCKS5 sub-negotiation). */
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

    /** Whether configured to a usable state (addr non-empty). */
    public function isConfigured(): bool
    {
        return $this->addr !== null && $this->addr !== '';
    }

    /**
     * Normalizes the version field: '4'/'socks4' -> socks4, '5'/'socks5' -> socks5; anything else
     * throws InvalidArgumentException (misconfigurations surface at startup instead of degrading
     * silently at connect time).
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
