<?php

declare(strict_types=1);

namespace RocketMQ\Common;

/**
 * 定时消息撤回句柄 v1：topic / brokerName / timestampStr / messageId 四段。
 *
 * ``timestampStr`` 保留字符串而不是转成 int：协议里它就是原样存的文本，
 * 撤回时要把它一字不差回填到请求里，非法时间戳由 broker 判定（``ILLEGAL_OPERATION``）。
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
