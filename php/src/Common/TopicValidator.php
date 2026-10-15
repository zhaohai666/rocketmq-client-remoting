<?php

declare(strict_types=1);

namespace RocketMQ\Common;

/**
 * Legality checks for topic / group names.
 *
 * The whitelist of allowed characters in the protocol is ``^[%|a-zA-Z0-9_-]+$``, implemented as a
 * 128-entry ``VALID_CHAR_BIT_MAP``: **any code point >= 128 is illegal**. It is copied byte for byte
 * here because the broker side (``TopicConfigValidator``) uses the same table, and letting the client
 * accept a name that the broker rejects only defers the error to the moment the topic is created, while
 * the name is something that should be settled **on the send path**.
 */
final class TopicValidator
{
    public const TOPIC_MAX_LENGTH = 127;
    /** A group name takes part in composing %RETRY%group_topic / %DLQ%group_topic, so it is shorter than a topic */
    public const GROUP_MAX_LENGTH = 120;
    public const RETRY_OR_DLQ_TOPIC_MAX_LENGTH = 255;

    public const VALID_CHAR_PATTERN = '^[%|a-zA-Z0-9_-]+$';

    public const AUTO_CREATE_TOPIC_KEY_TOPIC = 'TBW102';
    public const RMQ_SYS_SCHEDULE_TOPIC = 'SCHEDULE_TOPIC_XXXX';
    public const RMQ_SYS_BENCHMARK_TOPIC = 'BenchmarkTest';
    public const RMQ_SYS_TRANS_HALF_TOPIC = 'RMQ_SYS_TRANS_HALF_TOPIC';
    public const RMQ_SYS_ROCKSDB_TRANS_HALF_TOPIC = 'RMQ_SYS_ROCKSDB_TRANS_HALF_TOPIC';
    public const RMQ_SYS_TRACE_TOPIC = 'RMQ_SYS_TRACE_TOPIC';
    public const RMQ_SYS_TRANS_OP_HALF_TOPIC = 'RMQ_SYS_TRANS_OP_HALF_TOPIC';
    public const RMQ_SYS_ROCKSDB_TRANS_OP_HALF_TOPIC = 'RMQ_SYS_ROCKSDB_TRANS_OP_HALF_TOPIC';
    public const RMQ_SYS_TRANS_CHECK_MAX_TIME_TOPIC = 'TRANS_CHECK_MAX_TIME_TOPIC';
    public const RMQ_SYS_SELF_TEST_TOPIC = 'SELF_TEST_TOPIC';
    public const RMQ_SYS_OFFSET_MOVED_EVENT = 'OFFSET_MOVED_EVENT';
    public const RMQ_SYS_ROCKSDB_OFFSET_TOPIC = 'CHECKPOINT_TOPIC';

    public const SYSTEM_TOPIC_PREFIX = 'rmq_sys_';

    private const SYSTEM_TOPIC_SET = [
        self::AUTO_CREATE_TOPIC_KEY_TOPIC,
        self::RMQ_SYS_SCHEDULE_TOPIC,
        self::RMQ_SYS_BENCHMARK_TOPIC,
        self::RMQ_SYS_TRANS_HALF_TOPIC,
        self::RMQ_SYS_TRACE_TOPIC,
        self::RMQ_SYS_TRANS_OP_HALF_TOPIC,
        self::RMQ_SYS_TRANS_CHECK_MAX_TIME_TOPIC,
        self::RMQ_SYS_SELF_TEST_TOPIC,
        self::RMQ_SYS_OFFSET_MOVED_EVENT,
        self::RMQ_SYS_ROCKSDB_OFFSET_TOPIC,
        self::RMQ_SYS_ROCKSDB_TRANS_HALF_TOPIC,
        self::RMQ_SYS_ROCKSDB_TRANS_OP_HALF_TOPIC,
    ];

    /**
     * The topics the client **must not send to** directly: these are internal broker state streams (half
     * messages, delay, trace check...), and a user sending into them would pollute the broker's
     * transaction / delay / check logic.
     */
    private const NOT_ALLOWED_SEND_TOPIC_SET = [
        self::RMQ_SYS_SCHEDULE_TOPIC,
        self::RMQ_SYS_TRANS_HALF_TOPIC,
        self::RMQ_SYS_TRANS_OP_HALF_TOPIC,
        self::RMQ_SYS_TRANS_CHECK_MAX_TIME_TOPIC,
        self::RMQ_SYS_SELF_TEST_TOPIC,
        self::RMQ_SYS_OFFSET_MOVED_EVENT,
        self::RMQ_SYS_ROCKSDB_TRANS_HALF_TOPIC,
        self::RMQ_SYS_ROCKSDB_TRANS_OP_HALF_TOPIC,
    ];

    private const ALLOWED_CHARS = '%-_|0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ';

    /**
     * Returns false for an empty string (emptiness is handled by the isBlank step).
     *
     * Iterates byte by byte: all legal characters are ASCII, so any byte >= 0x80 (that is, every
     * non-ASCII code point) is illegal — equivalent to checking each code point for >= 128.
     */
    public static function isTopicOrGroupIllegal(string $name): bool
    {
        $n = strlen($name);
        for ($i = 0; $i < $n; $i++) {
            $b = ord($name[$i]);
            if ($b >= 128 || strpos(self::ALLOWED_CHARS, $name[$i]) === false) {
                return true;
            }
        }
        return false;
    }

    public static function isSystemTopic(string $topic): bool
    {
        return in_array($topic, self::SYSTEM_TOPIC_SET, true) || str_starts_with($topic, self::SYSTEM_TOPIC_PREFIX);
    }

    public static function isNotAllowedSendTopic(string $topic): bool
    {
        return in_array($topic, self::NOT_ALLOWED_SEND_TOPIC_SET, true);
    }
}
