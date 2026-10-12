<?php

declare(strict_types=1);

namespace RocketMQ\Common;

/**
 * 通用工具：时间格式化、十六进制串互转、本机 IP 探测、协议侧字符串哈希。
 */
final class UtilAll
{
    /** 日志/轨迹用的时间戳格式：年-月-日 时:分:秒 */
    public const DATE_FORMAT = 'Y-m-d H:i:s';
    /** 带毫秒的时间戳格式：秒后用 'v' 占位输出 3 位毫秒 */
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
        // 毫秒先截掉再格式化：默认格式本就不含秒以下部分。
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
     * 协议侧这个索引不需要混淆，原样返回（保留接口是为了调用方不改）。
     */
    public static function string2UnicodeShift(?string $index, int $offset): ?string
    {
        return $index;
    }

    public static function crc32(string $data): int
    {
        return crc32($data) & 0xFFFFFFFF;
    }

    /**
     * 逐字节转**大写**十六进制。
     */
    public static function bytes2String(string $bs): string
    {
        return strtoupper(bin2hex($bs));
    }

    /**
     * 十六进制字符串 -> 字节（不是 UTF-8 解码）。
     * 按 len//2 成对解析（奇数位尾字符丢弃），非法字符抛 ValueError。
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
        // UDP 探测失败时退回 gethostbyname(gethostname())，再失败给 127.0.0.1
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
     * 协议侧字符串哈希：h = 31*h + ch 逐码元累加，结果按 32 位有符号回绕。
     *
     * 必须显式实现：PHP 内置 hash 算出的数值与协议要求的完全不同，用错会让
     * broker 的 tag 过滤永远不命中——消息照发照存，消费端静默收不到。
     * 用途：`SubscriptionData.codeSet`（订阅 tag 的哈希集合）。
     *
     * 按 UTF-16 码元遍历（mb_convert_encoding 到 UTF-16BE 后逐 2 字节取值）；
     * BMP 内字符（含全部 ASCII tag）与逐码点遍历结果相同。
     * 对拍向量：TagA=2598919、TagB=2598920、P=80、PA=2545、"*"=42。
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
