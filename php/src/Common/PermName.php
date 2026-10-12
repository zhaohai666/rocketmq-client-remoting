<?php

declare(strict_types=1);

namespace RocketMQ\Common;

/**
 * 权限位常量：topic / 订阅组的 perm 字段按位解释（R=4、W=2、X=1）。
 */
final class PermName
{
    public const PERM_PRIORITY = 0x1 << 3;
    public const PERM_READ = 0x4;
    public const PERM_WRITE = 0x2;
    public const PERM_INHERIT = 0x1;
    public const PERM_OWNER = 0x1 << 4;

    /**
     * 合法权限值：``perm >= 0 && perm < PERM_PRIORITY``。
     *
     * 字符串入参不是数字时抛 ValueError——perm 来自配置文本，把坏值静默当 0
     * 会变成误放行；调用方需自行捕获。
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
