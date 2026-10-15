<?php

declare(strict_types=1);

namespace RocketMQ\Common;

/**
 * Generation and read/write of the client-side unique message ID (UNIQ_KEY).
 *
 * Usage:
 *   * ``createUniqId()`` generates a 32-char hex unique ID (IP + PID + class hash + current-month millis + auto-increment);
 *   * ``setUniqId($msg)``  writes UNIQ_KEY into the message properties before sending: **non-batch** messages are written
 *     once inside the send kernel; batch messages are written per-message and once for the whole batch;
 *   * ``getUniqId($msg)``  reads UNIQ_KEY — used by SendResult.msgId, the message trace's msgId, and the transaction
 *     message's transactionId.
 *
 * WARNING: Without it, SendResult.msgId can only fall back to the broker's offsetMsgId (which includes the commitlog
 * offset) — that is neither the ID used for key-based queries, nor does it match the consumer-side msgId in traces.
 */
final class MessageClientIdSetter
{
    public static function createUniqId(): string
    {
        return InnerIdGenerator::createUniqId();
    }

    /**
     * Only write UNIQ_KEY when it is missing: never overwrite an existing value, otherwise a retry would change the msgId.
     */
    public static function setUniqId(?Message $msg): void
    {
        if ($msg === null) {
            return;
        }
        if ($msg->getProperty(MessageConst::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX) === null) {
            MessageAccessor::putProperty(
                $msg,
                MessageConst::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX,
                self::createUniqId()
            );
        }
    }

    public static function getUniqId(?Message $msg): ?string
    {
        if ($msg === null) {
            return null;
        }
        return $msg->getProperty(MessageConst::PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX);
    }
}
