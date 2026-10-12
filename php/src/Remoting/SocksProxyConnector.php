<?php

declare(strict_types=1);

namespace RocketMQ\Remoting;

final class SocksProxyConnector
{
    // ---------- SOCKS 常量 ----------
    private const VER_SOCKS5 = 0x05;
    private const VER_AUTH = 0x01;          // RFC 1929 用户名/密码子协商版本
    private const METHOD_NO_AUTH = 0x00;
    private const METHOD_USERPASS = 0x02;
    private const METHOD_UNACCEPTABLE = 0xFF;
    private const CMD_CONNECT = 0x01;
    private const ATYP_IPV4 = 0x01;
    private const ATYP_DOMAIN = 0x03;
    private const ATYP_IPV6 = 0x04;

    /**
     * 在已连上代理的流上完成 CONNECT 握手，成功返回后该流即可当目标地址的直连流用。
     *
     * @param resource $conn      与代理服务器的 TCP 流（阻塞模式）
     * @param string   $host      目标主机（域名或 IP 字面量）
     * @param int      $port      目标端口
     * @param float    $timeoutSec 整个握手的预算秒数（本端口用 connectTimeout）
     */
    public static function handshake($conn, string $host, int $port, SocksProxyConfig $cfg, float $timeoutSec): void
    {
        if ($conn === null || !is_resource($conn)) {
            throw new \RuntimeException('proxy connection is not open');
        }
        $deadline = microtime(true) + max(0.001, $timeoutSec);
        if ($cfg->normalizedVersion() === SocksProxyConfig::VERSION_SOCKS4) {
            self::handshakeSocks4($conn, $host, $port, $cfg, $deadline);
        } else {
            self::handshakeSocks5($conn, $host, $port, $cfg, $deadline);
        }
    }

    // ---------- SOCKS5（RFC 1928 + RFC 1929） ----------

    private static function handshakeSocks5($conn, string $host, int $port, SocksProxyConfig $cfg, float $deadline): void
    {
        $hasCredentials = $cfg->getUsername() !== null && $cfg->getUsername() !== '';

        // 1) 方法协商：与 Netty Socks5ProxyHandler 同口径——带了用户名就只报 0x02，
        //    否则只报 0x00（不报 0x02 让代理被迫选密码法，认证字节流会错位）。
        $method = $hasCredentials ? self::METHOD_USERPASS : self::METHOD_NO_AUTH;
        self::writeAll($conn, chr(self::VER_SOCKS5) . "\x01" . chr($method), $deadline);
        $reply = unpack('Cver/Cmethod', self::readExact($conn, 2, $deadline));
        if ($reply['ver'] !== self::VER_SOCKS5) {
            throw new \RuntimeException(sprintf('unexpected socks version %d (expect 5)', $reply['ver']));
        }
        if ($reply['method'] === self::METHOD_USERPASS) {
            if (!$hasCredentials) {
                throw new \RuntimeException('proxy requires username/password auth but none configured');
            }
            self::socks5Auth($conn, (string)$cfg->getUsername(), (string)$cfg->getPassword(), $deadline);
        } elseif ($reply['method'] !== self::METHOD_NO_AUTH) {
            throw new \RuntimeException(sprintf(
                'no acceptable auth method (proxy chose 0x%02X)',
                $reply['method'] ?? self::METHOD_UNACCEPTABLE
            ));
        }

        // 2) CONNECT：VER CMD RSV ATYP DST.ADDR DST.PORT
        self::writeAll($conn, chr(self::VER_SOCKS5) . chr(self::CMD_CONNECT) . "\x00" . self::socks5Target($host, $port), $deadline);
        $reply = unpack('Cver/Crep/Crsv/Catyp', self::readExact($conn, 4, $deadline));
        if ($reply['ver'] !== self::VER_SOCKS5) {
            throw new \RuntimeException(sprintf('unexpected socks version %d in connect reply', $reply['ver']));
        }
        if ($reply['rep'] !== 0x00) {
            throw new \RuntimeException(self::socks5RepMessage((int)$reply['rep']));
        }
        // BND.ADDR 变长，按 ATYP 跳过，再吃掉 2 字节 BND.PORT
        $bndLen = match ((int)$reply['atyp']) {
            self::ATYP_IPV4 => 4,
            self::ATYP_IPV6 => 16,
            self::ATYP_DOMAIN => ord(self::readExact($conn, 1, $deadline)),
            $atyp => throw new \RuntimeException(sprintf('unexpected address type 0x%02X in connect reply', $atyp)),
        };
        self::readExact($conn, $bndLen + 2, $deadline);
    }

    /** RFC 1929 用户名/密码子协商：VER(1) ULEN(1) UNAME PLEN(1) PASSWD → VER(1) STATUS(1)。 */
    private static function socks5Auth($conn, string $user, string $pass, float $deadline): void
    {
        self::writeAll($conn, chr(self::VER_AUTH) . chr(strlen($user)) . $user . chr(strlen($pass)) . $pass, $deadline);
        $reply = unpack('Cver/Cstatus', self::readExact($conn, 2, $deadline));
        if ($reply['ver'] !== self::VER_AUTH) {
            throw new \RuntimeException(sprintf('unexpected auth subnegotiation version %d', $reply['ver']));
        }
        if ($reply['status'] !== 0x00) {
            throw new \RuntimeException('username/password authentication failed');
        }
    }

    /** CONNECT 请求的 ATYP + ADDR + PORT 段（IPv4/IPv6 字面量走二进制，其余按域名）。 */
    private static function socks5Target(string $host, int $port): string
    {
        $portBytes = pack('n', $port & 0xFFFF);
        $ipv4 = filter_var($host, FILTER_VALIDATE_IP, FILTER_FLAG_IPV4);
        if ($ipv4 !== false) {
            return chr(self::ATYP_IPV4) . (string)inet_pton($host) . $portBytes;
        }
        if (filter_var($host, FILTER_VALIDATE_IP, FILTER_FLAG_IPV6) !== false) {
            return chr(self::ATYP_IPV6) . (string)inet_pton($host) . $portBytes;
        }
        return chr(self::ATYP_DOMAIN) . chr(min(255, strlen($host))) . $host . $portBytes;
    }

    private static function socks5RepMessage(int $rep): string
    {
        return match ($rep) {
            0x01 => 'general SOCKS server failure',
            0x02 => 'connection not allowed by ruleset',
            0x03 => 'network unreachable',
            0x04 => 'host unreachable',
            0x05 => 'connection refused',
            0x06 => 'TTL expired',
            0x07 => 'command not supported',
            0x08 => 'address type not supported',
            default => sprintf('socks5 connect failed (rep 0x%02X)', $rep),
        };
    }

    // ---------- SOCKS4（含 4a 域名扩展） ----------

    private static function handshakeSocks4($conn, string $host, int $port, SocksProxyConfig $cfg, float $deadline): void
    {
        // SOCKS4 没有方法协商：直接发 CONNECT 请求（VER CMD DST.PORT DST.IP USERID 0x00）。
        $userid = (string)($cfg->getUsername() ?? '');
        $ip = filter_var($host, FILTER_VALIDATE_IP, FILTER_FLAG_IPV4)
            ? (string)inet_pton($host)
            : self::resolveIpv4($host);
        if ($ip !== null) {
            // 经典 SOCKS4：客户端自己解析域名，只送 4 字节 IP
            self::writeAll($conn, "\x04\x01" . pack('n', $port & 0xFFFF) . $ip . $userid . "\x00", $deadline);
        } else {
            // 解析不了 → SOCKS4a 扩展：DST.IP 用 0.0.0.x（x != 0）占位，USERID 后跟域名
            self::writeAll($conn, "\x04\x01" . pack('n', $port & 0xFFFF) . "\x00\x00\x00\x01" . $userid . "\x00" . $host . "\x00", $deadline);
        }
        // 回复定长 8 字节：VN(0) CD，CD == 90(0x5A) 才算成功
        $reply = unpack('Cvn/Ccd', self::readExact($conn, 2, $deadline));
        if ($reply['vn'] !== 0x00) {
            throw new \RuntimeException(sprintf('unexpected socks4 reply version %d', $reply['vn']));
        }
        if ($reply['cd'] !== 0x5A) {
            throw new \RuntimeException(sprintf('socks4 connect failed (code %d)', $reply['cd']));
        }
        self::readExact($conn, 6, $deadline); // DST.PORT + DST.IP
    }

    /** 域名 → 4 字节 IPv4；解析不了返回 null（调用方退 SOCKS4a）。 */
    private static function resolveIpv4(string $host): ?string
    {
        $ip = @gethostbyname($host);
        if ($ip === $host) {
            return null; // gethostbyname 失败时原样返回主机名
        }
        $packed = @inet_pton($ip);
        return ($packed !== false && strlen($packed) === 4) ? $packed : null;
    }

    // ---------- 阻塞 I/O（带 deadline） ----------

    private static function writeAll($conn, string $data, float $deadline): void
    {
        $total = strlen($data);
        $written = 0;
        while ($written < $total) {
            self::armTimeout($conn, $deadline);
            $n = @fwrite($conn, substr($data, $written));
            if ($n === false || $n === 0) {
                if (feof($conn)) {
                    throw new \RuntimeException('connection closed by proxy');
                }
                usleep(2000);
                continue;
            }
            $written += $n;
        }
    }

    private static function readExact($conn, int $need, float $deadline): string
    {
        $buf = '';
        while (strlen($buf) < $need) {
            self::armTimeout($conn, $deadline);
            $chunk = @fread($conn, $need - strlen($buf));
            if ($chunk === false || $chunk === '') {
                if (feof($conn)) {
                    throw new \RuntimeException('connection closed by proxy');
                }
                usleep(2000);
                continue;
            }
            $buf .= $chunk;
        }
        return $buf;
    }

    /** 把流的 socket 超时压到剩余预算：代理不回话时 fread 也能按时收口，不会挂死。 */
    private static function armTimeout($conn, float $deadline): void
    {
        $remaining = $deadline - microtime(true);
        if ($remaining <= 0) {
            throw new \RuntimeException('socks handshake timeout');
        }
        stream_set_timeout($conn, (int)$remaining, (int)(fmod($remaining, 1.0) * 1e6));
    }
}
