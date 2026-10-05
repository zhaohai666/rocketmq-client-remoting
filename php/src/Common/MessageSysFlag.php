<?php

declare(strict_types=1);

namespace RocketMQ\Common;

/**
 * 消息系统标志位（对应 org.apache.rocketmq.common.sysflag.MessageSysFlag，移植自 sysflag.py）。
 *
 * 位含义（低 -> 高）：
 *     bit0  COMPRESSED
 *     bit1  MULTI_TAGS
 *     bit2  TRANSACTION_PREPARED
 *     bit3  TRANSACTION_COMMIT(0x2<<2) / TRANSACTION_ROLLBACK(0x3<<2) 共用掩码
 *     bit4  BORNHOST_V6
 *     bit5  STOREHOSTADDRESS_V6
 *     bit6  NEED_UNWRAP
 *     bit7  INNER_BATCH
 *     bit8~10 COMPRESSION_TYPE（7 种取值，掩码 0x7<<8）
 */
final class MessageSysFlag
{
    public const COMPRESSED_FLAG = 0x1;
    public const MULTI_TAGS_FLAG = 0x1 << 1;
    public const TRANSACTION_NOT_TYPE = 0;
    public const TRANSACTION_PREPARED_TYPE = 0x1 << 2;
    public const TRANSACTION_COMMIT_TYPE = 0x2 << 2;
    public const TRANSACTION_ROLLBACK_TYPE = 0x3 << 2;
    public const BORNHOST_V6_FLAG = 0x1 << 4;
    public const STOREHOSTADDRESS_V6_FLAG = 0x1 << 5;
    /** 批量消息解包标记（避免与其它位冲突） */
    public const NEED_UNWRAP_FLAG = 0x1 << 6;
    /** 内部批量标记 */
    public const INNER_BATCH_FLAG = 0x1 << 7;

    public const COMPRESSION_LZ4_TYPE = 0x1 << 8;
    public const COMPRESSION_ZSTD_TYPE = 0x2 << 8;
    public const COMPRESSION_ZLIB_TYPE = 0x3 << 8;
    public const COMPRESSION_TYPE_COMPARATOR = 0x7 << 8;
    public const COMPRESSION_TYPE_SHIFT = 8;

    // 兼容历史命名（供 compression_type 数值 -> flag 位）
    public const LZ4_TYPE = 1;
    public const ZSTD_TYPE = 2;
    public const ZLIB_TYPE = 3;
    public const SNAPPY_TYPE = 4;

    /** Java: (flag & COMPRESSION_TYPE_COMPARATOR) >> 8。 */
    public static function getCompressionType(int $sysFlag): int
    {
        return ($sysFlag & self::COMPRESSION_TYPE_COMPARATOR) >> self::COMPRESSION_TYPE_SHIFT;
    }

    public static function setCompressionType(int $sysFlag, int $compressionType): int
    {
        return ($sysFlag & ~self::COMPRESSION_TYPE_COMPARATOR) | (
            ($compressionType << self::COMPRESSION_TYPE_SHIFT) & self::COMPRESSION_TYPE_COMPARATOR
        );
    }

    public static function isCompressed(int $sysFlag): bool
    {
        return ($sysFlag & self::COMPRESSED_FLAG) === self::COMPRESSED_FLAG;
    }

    public static function clearCompressedFlag(int $sysFlag): int
    {
        return $sysFlag & ~self::COMPRESSED_FLAG;
    }

    /** Java: flag & TRANSACTION_ROLLBACK_TYPE。 */
    public static function getTransactionValue(int $flag): int
    {
        return $flag & self::TRANSACTION_ROLLBACK_TYPE;
    }

    public static function resetTransactionValue(int $flag, int $transactionType): int
    {
        return ($flag & ~self::TRANSACTION_ROLLBACK_TYPE) | $transactionType;
    }

    public static function check(int $flag, int $expectedFlag): bool
    {
        return ($flag & $expectedFlag) !== 0;
    }
}
