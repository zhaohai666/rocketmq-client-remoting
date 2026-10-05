<?php

declare(strict_types=1);

namespace RocketMQ\Common;

/**
 * 权限位常量（对应 org.apache.rocketmq.common.PermName，移植自 sysflag.py 的 PermName）。
 */
final class PermName
{
    public const PERM_PRIORITY = 0x1 << 3;
    public const PERM_READ = 0x4;
    public const PERM_WRITE = 0x2;
    public const PERM_INHERIT = 0x1;
    public const PERM_OWNER = 0x1 << 4;

    /**
     * 对应 Java PermName.isValid：``perm >= 0 && perm < PERM_PRIORITY``。
     *
     * Java 的 ``isValid(String)`` 直接 ``Integer.parseInt``，非数字会抛
     * NumberFormatException；这里保持一致（抛 ValueError），调用方需自行捕获。
     */
    public static function isValid(int|string $perm): bool
    {
        if (is_string($perm)) {
            if (!preg_match('/^-?\d+$/', $perm)) {
                throw new \ValueError(sprintf('"%s" is not a valid integer perm', $perm));
            }
            $perm = (int) $perm;
        }
        return 0 <= $perm && $perm < self::PERM_PRIORITY;
    }

    public static function permToString(int $perm): string
    {
        $sb = '';
        $sb .= self::PERM_READ === ($perm & self::PERM_READ) ? 'R' : '-';
        $sb .= self::PERM_WRITE === ($perm & self::PERM_WRITE) ? 'W' : '-';
        $sb .= self::PERM_INHERIT === ($perm & self::PERM_INHERIT) ? 'X' : '-';
        return $sb;
    }

    public static function perm2string(int $perm): string
    {
        return self::permToString($perm);
    }

    public static function checkPerm(int $perm, int $wantedPerm): bool
    {
        return ($perm & $wantedPerm) === $wantedPerm;
    }
}
