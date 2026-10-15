<?php

declare(strict_types=1);

namespace RocketMQ\Remoting;

final class SocksProxyConnector
{
    // ---------- SOCKS constants ----------
    private const VER_SOCKS5 = 0x05;
    private const VER_AUTH = 0x01;          // RFC 1929 username/password sub-negotiation version
    private const METHOD_NO_AUTH = 0x00;
    private const METHOD_USERPASS = 0x02;
    private const METHOD_UNACCEPTABLE = 0xFF;
    private const CMD_CONNECT = 0x01;
    private const ATYP_IPV4 = 0x01;
    private const ATYP_DOMAIN = 0x03;
    private const ATYP_IPV6 = 0x04;

    /**
     * Completes the CONNECT handshake over a stream already connected to the proxy; on success
     * the stream can be used as a direct stream to the target address.
     *
     * @param resource $conn      TCP stream to the proxy server (blocking mode)
     * @param string   $host      target host (domain or IP literal)
     * @param int      $port      target port
     * @param float    $timeoutSec budget in seconds for the whole handshake (this port uses connectTimeout)
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

    // ---------- SOCKS5 (RFC 1928 + RFC 1929) ----------

    private static function handshakeSocks5($conn, string $host, int $port, SocksProxyConfig $cfg, float $deadline): void
    {
        $hasCredentials = $cfg->getUsername() !== null && $cfg->getUsername() !== '';

        // 1) Method negotiation: same semantics as Netty Socks5ProxyHandler — with a username
        //    configured, offer only 0x02, otherwise only 0x00 (offering 0x02 without credentials
        //    would let the proxy pick the password method and misalign the auth byte stream).
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

        // 2) CONNECT: VER CMD RSV ATYP DST.ADDR DST.PORT
        self::writeAll($conn, chr(self::VER_SOCKS5) . chr(self::CMD_CONNECT) . "\x00" . self::socks5Target($host, $port), $deadline);
        $reply = unpack('Cver/Crep/Crsv/Catyp', self::readExact($conn, 4, $deadline));
        if ($reply['ver'] !== self::VER_SOCKS5) {
            throw new \RuntimeException(sprintf('unexpected socks version %d in connect reply', $reply['ver']));
        }
        if ($reply['rep'] !== 0x00) {
            throw new \RuntimeException(self::socks5RepMessage((int)$reply['rep']));
        }
        // BND.ADDR is variable-length; skip it per ATYP, then consume the 2-byte BND.PORT
        $bndLen = match ((int)$reply['atyp']) {
            self::ATYP_IPV4 => 4,
            self::ATYP_IPV6 => 16,
            self::ATYP_DOMAIN => ord(self::readExact($conn, 1, $deadline)),
            $atyp => throw new \RuntimeException(sprintf('unexpected address type 0x%02X in connect reply', $atyp)),
        };
        self::readExact($conn, $bndLen + 2, $deadline);
    }

    /** RFC 1929 username/password sub-negotiation: VER(1) ULEN(1) UNAME PLEN(1) PASSWD -> VER(1) STATUS(1). */
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

    /** ATYP + ADDR + PORT segment of the CONNECT request (IPv4/IPv6 literals go binary, everything else as a domain). */
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

    // ---------- SOCKS4 (including the 4a domain extension) ----------

    private static function handshakeSocks4($conn, string $host, int $port, SocksProxyConfig $cfg, float $deadline): void
    {
        // SOCKS4 has no method negotiation: send the CONNECT request directly (VER CMD DST.PORT DST.IP USERID 0x00).
        $userid = (string)($cfg->getUsername() ?? '');
        $ip = filter_var($host, FILTER_VALIDATE_IP, FILTER_FLAG_IPV4)
            ? (string)inet_pton($host)
            : self::resolveIpv4($host);
        if ($ip !== null) {
            // Classic SOCKS4: the client resolves the domain itself and sends only the 4-byte IP
            self::writeAll($conn, "\x04\x01" . pack('n', $port & 0xFFFF) . $ip . $userid . "\x00", $deadline);
        } else {
            // Unresolvable -> SOCKS4a extension: DST.IP uses 0.0.0.x (x != 0) as a placeholder,
            // followed by USERID then the domain
            self::writeAll($conn, "\x04\x01" . pack('n', $port & 0xFFFF) . "\x00\x00\x00\x01" . $userid . "\x00" . $host . "\x00", $deadline);
        }
        // The reply is a fixed 8 bytes: VN(0) CD; CD == 90(0x5A) means success
        $reply = unpack('Cvn/Ccd', self::readExact($conn, 2, $deadline));
        if ($reply['vn'] !== 0x00) {
            throw new \RuntimeException(sprintf('unexpected socks4 reply version %d', $reply['vn']));
        }
        if ($reply['cd'] !== 0x5A) {
            throw new \RuntimeException(sprintf('socks4 connect failed (code %d)', $reply['cd']));
        }
        self::readExact($conn, 6, $deadline); // DST.PORT + DST.IP
    }

    /** Domain -> 4-byte IPv4; returns null when unresolvable (the caller falls back to SOCKS4a). */
    private static function resolveIpv4(string $host): ?string
    {
        $ip = @gethostbyname($host);
        if ($ip === $host) {
            return null; // gethostbyname returns the hostname as-is on failure
        }
        $packed = @inet_pton($ip);
        return ($packed !== false && strlen($packed) === 4) ? $packed : null;
    }

    // ---------- Blocking I/O (with deadline) ----------

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

    /** Clamps the stream's socket timeout to the remaining budget: if the proxy never replies, fread still closes on time instead of hanging. */
    private static function armTimeout($conn, float $deadline): void
    {
        $remaining = $deadline - microtime(true);
        if ($remaining <= 0) {
            throw new \RuntimeException('socks handshake timeout');
        }
        stream_set_timeout($conn, (int)$remaining, (int)(fmod($remaining, 1.0) * 1e6));
    }
}
