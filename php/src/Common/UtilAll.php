<?php

declare(strict_types=1);

namespace RocketMQ\Common;

/**
 * Common utilities: time formatting, hex string conversion in both directions, local IP probing and the
 * protocol-side string hash.
 */
final class UtilAll
{
    /** The timestamp format used by logs / traces: year-month-day hour:minute:second */
    public const DATE_FORMAT = 'Y-m-d H:i:s';
    /** The timestamp format carrying milliseconds: 3-digit milliseconds after the seconds via the 'v' placeholder */
    public const DATE_FORMAT_SSS = 'Y-m-d H:i:s.v';

    public const HEX_ARRAY = '0123456789ABCDEF';

    public static function currentTimeMillis(): int
    {
        return (int) (microtime(true) * 1000);
    }

    public static function currentSeconds(): int
    {
        return time();
    }

    public static function offset2Filename(int $offset): string
    {
        return sprintf('%020d', $offset);
    }

    public static function computeElapseTimeMillis(int $lastTime): int
    {
        return self::currentTimeMillis() - $lastTime;
    }

    public static function timeToHumanString(int $ts, string $pattern = self::DATE_FORMAT): string
    {
        if ($ts <= 0) {
            return '-';
        }
        // Truncate the milliseconds before formatting: the default format does not include sub-second parts anyway.
        return date($pattern, intdiv($ts, 1000));
    }

    public static function isBlank(?string $s): bool
    {
        return $s === null || trim($s) === '';
    }

    public static function isNotBlank(?string $s): bool
    {
        return !self::isBlank($s);
    }

    public static function getPid(): int
    {
        return getmypid() ?: 0;
    }

    public static function isIpv4(string $addr): bool
    {
        return filter_var($addr, FILTER_VALIDATE_IP, FILTER_FLAG_IPV4) !== false;
    }

    /**
     * The protocol-side index does not need obfuscation, so it is returned as-is (the interface is kept
     * so that callers do not have to change).
     */
    public static function string2UnicodeShift(?string $index, int $offset): ?string
    {
        return $index;
    }

    /**
     * The value the BODYCRC field of a stored message carries.
     *
     * The broker does not store the bare CRC-32 of the body: it writes the IEEE CRC-32 with the high
     * (sign) bit dropped, i.e. ``crc32($data) & 0x7FFFFFFF``. A body of ``ba-all-0`` whose IEEE CRC-32
     * is 4085475159 (0xF3836757) is stored with BODYCRC 1937991511 (0x73836757).
     *
     * Returning the masked value here is what makes the field comparable: a check against the whole
     * 32-bit CRC disagrees for roughly half of all bodies (exactly those whose CRC has the high bit
     * set), which would make every such body look corrupted.
     */
    public static function crc32(string $data): int
    {
        return crc32($data) & 0x7FFFFFFF;
    }

    /**
     * Converts byte by byte to **upper-case** hex.
     */
    public static function bytes2String(string $bs): string
    {
        return strtoupper(bin2hex($bs));
    }

    /**
     * Hex string -> bytes (not a UTF-8 decode).
     * Parses in pairs by len//2 (a trailing odd character is dropped), an illegal character throws
     * ValueError.
     */
    public static function string2Bytes(?string $hexString): ?string
    {
        if ($hexString === null || $hexString === '') {
            return null;
        }
        $hexString = strtoupper($hexString);
        $length = intdiv(strlen($hexString), 2);
        $out = '';
        for ($i = 0; $i < $length; $i++) {
            $pos = $i * 2;
            $out .= chr((self::charToByte($hexString[$pos]) << 4) | self::charToByte($hexString[$pos + 1]));
        }
        return $out;
    }

    public static function charToByte(string $c): int
    {
        $pos = strpos(self::HEX_ARRAY, $c);
        if ($pos === false) {
            throw new \ValueError(sprintf('"%s" is not a valid hex char', $c));
        }
        return $pos;
    }

    public static function emptyBytes(): string
    {
        return '';
    }

    public static function nextMillis(): int
    {
        return self::currentTimeMillis();
    }

    public static function localIp(): string
    {
        $ip = MixAll::getIpStr();
        if ($ip !== '127.0.0.1') {
            return $ip;
        }
        // Fall back to gethostbyname(gethostname()) when the UDP probe fails, and to 127.0.0.1 when that fails too
        $host = gethostname();
        if ($host !== false) {
            $resolved = gethostbyname($host);
            if ($resolved !== $host && filter_var($resolved, FILTER_VALIDATE_IP) !== false) {
                return $resolved;
            }
        }
        return '127.0.0.1';
    }

    /**
     * The protocol-side string hash: h = 31*h + ch accumulated per code unit, the result wrapping as a
     * signed 32-bit value.
     *
     * It must be implemented explicitly: the values computed by the PHP built-in hash differ completely
     * from what the protocol requires, and using the wrong one makes the broker's tag filtering never
     * match — the message is still sent and stored, and the consumer silently never receives it.
     * Used for `SubscriptionData.codeSet` (the hash set of subscribed tags).
     *
     * Iterates UTF-16 code units (mb_convert_encoding to UTF-16BE then reads 2 bytes at a time);
     * characters inside the BMP (including all ASCII tags) give the same result as iterating by code
     * point.
     * Cross-check vectors: TagA=2598919, TagB=2598920, P=80, PA=2545, "*"=42.
     */
    public static function wireStringHash(string $s): int
    {
        $u16 = mb_convert_encoding($s, 'UTF-16BE', 'UTF-8');
        $h = 0;
        for ($i = 0, $n = strlen($u16); $i < $n; $i += 2) {
            $ch = (ord($u16[$i]) << 8) | ord($u16[$i + 1]);
            $h = (31 * $h + $ch) & 0xFFFFFFFF;
        }
        return $h >= 0x80000000 ? $h - 0x100000000 : $h;
    }
}
