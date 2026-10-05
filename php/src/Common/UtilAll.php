<?php

declare(strict_types=1);

namespace RocketMQ\Common;

/**
 * 通用工具（对应 org.apache.rocketmq.common.UtilAll，移植自 python/common/util_all.py）。
 */
final class UtilAll
{
    /** Java 相同的日期格式化（PHP date() 格式，输出与 Python "%Y-%m-%d %H:%M:%S" 一致） */
    public const DATE_FORMAT = 'Y-m-d H:i:s';
    /** Python "%Y-%m-%d %H:%M:%S.%f"（毫秒部分用 PHP 'v' 占位，秒后带 3 位毫秒） */
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
        // Python 侧从 ts/1000 构造 datetime 再 strftime，默认格式不含毫秒，秒以下本就截断。
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
     * 对应 Java：取字符串与整数的异或混淆（保持接口兼容）。
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
     * Java UtilAll.bytes2string：逐字节转大写十六进制。
     */
    public static function bytes2String(string $bs): string
    {
        return strtoupper(bin2hex($bs));
    }

    /**
     * Java UtilAll.string2bytes：十六进制字符串 -> 字节（非 UTF-8 编码）。
     * 与 Python 一致：按 len//2 成对解析（奇数位尾字符丢弃），非法字符抛 ValueError。
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
        // Python：UDP 探测失败时 gethostbyname(gethostname())，再失败给 127.0.0.1
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
     * Java `String.hashCode()`：h = 31*h + ch，按 32 位有符号回绕。
     *
     * 必须显式实现：PHP 的内置 hash 与 Java 完全不同。用途之一：
     * `SubscriptionData.codeSet`（Java `FilterAPI.buildSubscriptionData` 写入
     * `tag.hashCode()`，broker 侧 `ExpressionMessageFilter.isMatchedByConsumeQueue`
     * 按它过滤）。
     *
     * 按 Java 语义遍历 UTF-16 码元（mb_convert_encoding 到 UTF-16BE 后逐 2 字节取值）；
     * 对 BMP 内字符（含全部 ASCII tag）与 Java/Python 移植版逐 code point 完全一致。
     * 对拍向量：TagA=2598919、TagB=2598920、P=80、PA=2545、"*"=42。
     */
    public static function javaStringHash(string $s): int
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
