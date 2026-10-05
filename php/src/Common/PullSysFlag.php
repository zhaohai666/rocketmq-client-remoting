<?php

declare(strict_types=1);

namespace RocketMQ\Common;

/**
 * Pull 请求系统标志位（对应 org.apache.rocketmq.common.sysflag.PullSysFlag，移植自 sysflag.py）。
 */
final class PullSysFlag
{
    public const FLAG_COMMIT_OFFSET = 0x1;
    public const FLAG_SUSPEND = 0x1 << 1;
    public const FLAG_SUBSCRIPTION = 0x1 << 2;
    public const FLAG_CLASS_FILTER = 0x1 << 3;
    public const FLAG_LITE_PULL_MESSAGE = 0x1 << 4;
    public const FLAG_PROXY_BLOCK = 0x1 << 5;
    public const FLAG_EXT_BROKER_GROUP = 0x1 << 6;
    public const FLAG_INNER_SQL = 0x1 << 7;
    public const FLAG_MULTI_TAG = 0x1 << 8;
    public const FLAG_START_OFFSET = 0x1 << 9;

    public static function buildSysFlag(
        bool $commitOffset,
        bool $suspend,
        bool $subscription,
        bool $classFilter,
        bool $litePull = false
    ): int {
        $flag = 0;
        if ($commitOffset) {
            $flag |= self::FLAG_COMMIT_OFFSET;
        }
        if ($suspend) {
            $flag |= self::FLAG_SUSPEND;
        }
        if ($subscription) {
            $flag |= self::FLAG_SUBSCRIPTION;
        }
        if ($classFilter) {
            $flag |= self::FLAG_CLASS_FILTER;
        }
        if ($litePull) {
            $flag |= self::FLAG_LITE_PULL_MESSAGE;
        }
        return $flag;
    }

    public static function clearCommitOffsetFlag(int $sysFlag): int
    {
        return $sysFlag & ~self::FLAG_COMMIT_OFFSET;
    }

    public static function hasCommitOffsetFlag(int $sysFlag): bool
    {
        return ($sysFlag & self::FLAG_COMMIT_OFFSET) === self::FLAG_COMMIT_OFFSET;
    }

    public static function hasSuspendFlag(int $sysFlag): bool
    {
        return ($sysFlag & self::FLAG_SUSPEND) === self::FLAG_SUSPEND;
    }

    public static function clearSuspendFlag(int $sysFlag): int
    {
        return $sysFlag & ~self::FLAG_SUSPEND;
    }

    public static function hasSubscriptionFlag(int $sysFlag): bool
    {
        return ($sysFlag & self::FLAG_SUBSCRIPTION) === self::FLAG_SUBSCRIPTION;
    }

    public static function buildSysFlagWithSubscription(int $sysFlag): int
    {
        return $sysFlag | self::FLAG_SUBSCRIPTION;
    }

    public static function hasClassFilterFlag(int $sysFlag): bool
    {
        return ($sysFlag & self::FLAG_CLASS_FILTER) === self::FLAG_CLASS_FILTER;
    }

    public static function hasLitePullFlag(int $sysFlag): bool
    {
        return ($sysFlag & self::FLAG_LITE_PULL_MESSAGE) === self::FLAG_LITE_PULL_MESSAGE;
    }
}
