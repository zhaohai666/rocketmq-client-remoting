<?php

declare(strict_types=1);

namespace RocketMQ\Common;

/**
 * 对应 Java ``RecallMessageHandle.HandleV1``（移植自 recall_message_handle.py 的 HandleV1）。
 *
 * ``timestampStr`` 保留字符串而不是转成 int：Java 就是原样存 ``String``，
 * 撤回时要把它回填到请求里，非法时间戳由 broker 判定（``ILLEGAL_OPERATION``）。
 */
final class HandleV1
{
    public function __construct(
        public ?string $topic = null,
        public ?string $brokerName = null,
        public ?string $timestampStr = null,
        public ?string $messageId = null,
    ) {
    }

    public function equals(HandleV1 $other): bool
    {
        return $this->topic === $other->topic
            && $this->brokerName === $other->brokerName
            && $this->timestampStr === $other->timestampStr
            && $this->messageId === $other->messageId;
    }

    public function __toString(): string
    {
        return sprintf(
            'HandleV1(topic=%s, broker_name=%s, timestamp_str=%s, message_id=%s)',
            var_export($this->topic, true),
            var_export($this->brokerName, true),
            var_export($this->timestampStr, true),
            var_export($this->messageId, true)
        );
    }
}
