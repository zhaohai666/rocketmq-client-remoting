<?php

declare(strict_types=1);

namespace RocketMQ\Common;

/**
 * Scheduled message recall handle v1: four segments, topic / brokerName / timestampStr / messageId.
 *
 * ``timestampStr`` is kept as a string instead of being converted to an int: the protocol stores it as
 * plain text verbatim, and a recall has to fill it back into the request byte for byte; an illegal
 * timestamp is judged by the broker (``ILLEGAL_OPERATION``).
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
