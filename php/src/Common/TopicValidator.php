<?php

declare(strict_types=1);

namespace RocketMQ\Common;

/**
 * topic / group 名字的合法性判定（对应 ``org.apache.rocketmq.common.topic.TopicValidator``，
 * 移植自 topic_validator.py）。
 *
 * Java 的字符表白名单是 ``^[%|a-zA-Z0-9_-]+$``，实现方式是一张 128 长的
 * ``VALID_CHAR_BIT_MAP``：**码点 >= 128 一律非法**。这里逐字节照抄，因为 broker 侧
 * （``TopicConfigValidator``）用的是同一张表，客户端放行而 broker 拒绝只会把错误推迟到
 * 建 topic 那一刻，而名字是**发送路径**上就该定的东西。
 */
final class TopicValidator
{
    public const TOPIC_MAX_LENGTH = 127;
    /** group 名要参与拼 %RETRY%group_topic / %DLQ%group_topic，所以比 topic 更短 */
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
     * 客户端**不能直接发**的 topic：这几个是 broker 内部状态流水（半消息、延迟、轨迹校验…），
     * 用户发进去会污染 broker 的事务/延迟/校验逻辑。
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
     * 对应 ``TopicValidator.isTopicOrGroupIllegal``：空串返回 False（空由 isBlank 那步管）。
     *
     * 按字节遍历：合法字符全为 ASCII，任何 >= 0x80 的字节（即所有非 ASCII 码点）
     * 一律非法，与 Python 逐字符 ``ord(ch) >= 128`` 语义等价。
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
