<?php

declare(strict_types=1);

namespace RocketMQ\Common;

/**
 * 对应 org.apache.rocketmq.common.message.MessageQueue（移植自 message.py 的 MessageQueue）。
 */
final class MessageQueue
{
    public function __construct(
        public string $topic = '',
        public string $brokerName = '',
        public int $queueId = 0,
    ) {
    }

    // ---- Java 风格 getter / setter（保证 JSON 字段名 camelCase）----

    public function getTopic(): string
    {
        return $this->topic;
    }

    public function setTopic(string $topic): void
    {
        $this->topic = $topic;
    }

    public function getBrokerName(): string
    {
        return $this->brokerName;
    }

    public function setBrokerName(string $brokerName): void
    {
        $this->brokerName = $brokerName;
    }

    public function getQueueId(): int
    {
        return $this->queueId;
    }

    public function setQueueId(int $queueId): void
    {
        $this->queueId = $queueId;
    }

    public function getQueueIdStr(): string
    {
        return (string) $this->queueId;
    }

    /**
     * 与 Java hashCode 对齐：((31+bh)*31+qid)*31+th，结果按 32 位有符号回绕。
     */
    public function hashcode(): int
    {
        $topicHash = self::strHash($this->topic);
        $brokerHash = self::strHash($this->brokerName);
        return self::wrapInt32(((31 + $brokerHash) * 31 + $this->queueId) * 31 + $topicHash);
    }

    private static function strHash(string $s): int
    {
        // Python 侧按 code point 累加；这里按 UTF-16 码元与 Java 一致（BMP 内等价），
        // 最后统一做 32 位有符号回绕
        $h = 0;
        $u16 = mb_convert_encoding($s, 'UTF-16BE', 'UTF-8');
        for ($i = 0, $n = strlen($u16); $i < $n; $i += 2) {
            $ch = (ord($u16[$i]) << 8) | ord($u16[$i + 1]);
            $h = 31 * $h + $ch;
        }
        return self::wrapInt32($h);
    }

    /** 任意整数按 32 位有符号回绕。 */
    public static function wrapInt32(int $x): int
    {
        $x &= 0xFFFFFFFF;
        return $x < 0x80000000 ? $x : $x - 0x100000000;
    }

    public function equals(MessageQueue $other): bool
    {
        return $this->topic === $other->topic
            && $this->brokerName === $other->brokerName
            && $this->queueId === $other->queueId;
    }

    public function compareTo(MessageQueue $other): int
    {
        if ($this->topic !== $other->topic) {
            return $this->topic < $other->topic ? -1 : 1;
        }
        if ($this->brokerName !== $other->brokerName) {
            return $this->brokerName < $other->brokerName ? -1 : 1;
        }
        if ($this->queueId !== $other->queueId) {
            return $this->queueId < $other->queueId ? -1 : 1;
        }
        return 0;
    }

    public function __toString(): string
    {
        return sprintf("MessageQueue(topic='%s', broker='%s', qid=%d)", $this->topic, $this->brokerName, $this->queueId);
    }
}
