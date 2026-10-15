<?php

declare(strict_types=1);

namespace RocketMQ\Common;

/**
 * Message property accessor: centralized read/write entry point for the property table (reconsume times, recall, trace, etc. all go through here).
 */
final class MessageAccessor
{
    public static function putProperty(Message $msg, string $name, string $value): void
    {
        $msg->putProperty($name, $value);
    }

    public static function getProperty(Message $msg, string $name): ?string
    {
        return $msg->getProperty($name);
    }

    public static function clearProperty(Message $msg, string $name): void
    {
        $msg->removeProperty($name);
    }

    public static function setKeys(Message $msg, string $keys): void
    {
        $msg->properties[MessageConst::PROPERTY_KEYS] = $keys;
    }

    public static function getKeys(Message $msg): ?string
    {
        return $msg->properties[MessageConst::PROPERTY_KEYS] ?? null;
    }

    public static function setTags(Message $msg, string $tags): void
    {
        $msg->properties[MessageConst::PROPERTY_TAGS] = $tags;
    }

    public static function getTags(Message $msg): ?string
    {
        return $msg->properties[MessageConst::PROPERTY_TAGS] ?? null;
    }

    public static function setDelayTimeLevel(Message $msg, int $level): void
    {
        $msg->properties[MessageConst::PROPERTY_DELAY_TIME_LEVEL] = (string) $level;
    }

    public static function getDelayTimeLevel(Message $msg): ?string
    {
        return $msg->properties[MessageConst::PROPERTY_DELAY_TIME_LEVEL] ?? null;
    }

    public static function setWaitStoreMsgOk(Message $msg, bool $ok): void
    {
        $msg->properties[MessageConst::PROPERTY_WAIT_STORE_MSG_OK] = $ok ? 'true' : 'false';
    }

    public static function setTransactionId(Message $msg, ?string $transactionId): void
    {
        $msg->setTransactionId($transactionId);
    }

    public static function getTransactionId(Message $msg): ?string
    {
        return $msg->getTransactionId();
    }

    public static function setOriginMessageId(Message $msg, string $originMessageId): void
    {
        $msg->properties[MessageConst::PROPERTY_ORIGIN_MESSAGE_ID] = $originMessageId;
    }

    public static function getOriginMessageId(Message $msg): ?string
    {
        return $msg->properties[MessageConst::PROPERTY_ORIGIN_MESSAGE_ID] ?? null;
    }

    public static function setConsumeStartTimestamp(Message $msg, int $ts): void
    {
        $msg->properties[MessageConst::PROPERTY_CONSUME_START_TIMESTAMP] = (string) $ts;
    }

    public static function getConsumeStartTimestamp(Message $msg): ?string
    {
        return $msg->properties[MessageConst::PROPERTY_CONSUME_START_TIMESTAMP] ?? null;
    }

    /**
     * Reconsume times are attached to the message as a **property**, written only by the send-back path (``sendMessageBack``),
     * and are NOT the same as the reconsumeTimes in MessageExt's 13th wire field ——
     * the broker delivers the latter to the client, so reading the wrong source yields 0.
     */
    public static function getReconsumeTime(Message $msg): ?string
    {
        return $msg->properties[MessageConst::PROPERTY_RECONSUME_TIME] ?? null;
    }

    public static function setReconsumeTime(Message $msg, int|string $v): void
    {
        $msg->properties[MessageConst::PROPERTY_RECONSUME_TIME] = (string) $v;
    }

    public static function getMaxReconsumeTimes(Message $msg): ?string
    {
        return $msg->properties[MessageConst::PROPERTY_MAX_RECONSUME_TIMES] ?? null;
    }

    public static function setMaxReconsumeTimes(Message $msg, int $v): void
    {
        $msg->properties[MessageConst::PROPERTY_MAX_RECONSUME_TIMES] = (string) $v;
    }

    public static function setTransactionPrepared(Message $msg): void
    {
        $msg->properties[MessageConst::PROPERTY_TRANSACTION_PREPARED] = 'true';
    }

    public static function isTransactionPrepared(Message $msg): bool
    {
        return ($msg->properties[MessageConst::PROPERTY_TRANSACTION_PREPARED] ?? null) === 'true';
    }

    public static function setTransactionPreparedQueueOffset(Message $msg, int $offset): void
    {
        $msg->properties[MessageConst::PROPERTY_TRANSACTION_PREPARED_QUEUE_OFFSET] = (string) $offset;
    }
}
