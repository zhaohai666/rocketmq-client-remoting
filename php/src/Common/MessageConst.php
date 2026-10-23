<?php

declare(strict_types=1);

namespace RocketMQ\Common;

/**
 * Message property key constants: these strings are exactly the property key names in the protocol; the broker and
 * console match them literally, so changing a single character is equivalent to disabling that feature.
 */
final class MessageConst
{
    public const PROPERTY_KEYS = 'KEYS';
    public const PROPERTY_TAGS = 'TAGS';
    public const PROPERTY_WAIT_STORE_MSG_OK = 'WAIT';
    public const PROPERTY_DELAY_TIME_LEVEL = 'DELAY';
    // The 5.x timer spellings of "deliver later": relative seconds, relative
    // milliseconds, an absolute deliver-at timestamp. Like the delay level they ask for
    // a schedule, so every gate that refuses a delayed message checks all four.
    public const PROPERTY_TIMER_DELAY_SEC = 'TIMER_DELAY_SEC';
    public const PROPERTY_TIMER_DELAY_MS = 'TIMER_DELAY_MS';
    public const PROPERTY_TIMER_DELIVER_MS = 'TIMER_DELIVER_MS';
    public const PROPERTY_RETRY_TOPIC = 'RETRY_TOPIC';
    public const PROPERTY_REAL_TOPIC = 'REAL_TOPIC';
    public const PROPERTY_REAL_QUEUE_ID = 'REAL_QID';
    public const PROPERTY_TRANSACTION_PREPARED = 'TRAN_MSG';
    public const PROPERTY_PRODUCER_GROUP = 'PGROUP';
    public const PROPERTY_MIN_OFFSET = 'MIN_OFFSET';
    public const PROPERTY_MAX_OFFSET = 'MAX_OFFSET';
    public const PROPERTY_BUYER_ID = 'BUYER_ID';
    public const PROPERTY_ORIGIN_MESSAGE_ID = 'ORIGIN_MESSAGE_ID';
    public const PROPERTY_TRANSFER_FLAG = 'TRANSFER_FLAG';
    public const PROPERTY_CHECK_IMMUNITY_TIME_IN_SECONDS = 'CHECK_IMMUNITY_TIME_IN_SECONDS';
    public const PROPERTY_RECONSUME_TIME = 'RECONSUME_TIME';
    public const PROPERTY_MSG_REGION = 'MSG_REGION';
    /**
     * Message trace switch: the broker returns it in the SEND response header (SendMessageProcessor writes
     * String.valueOf(brokerConfig.isTraceOn()), default true), and the client uses it to decide whether to record traces;
     * the consumer side reads the same key from the message property (ConsumeMessageTraceHookImpl).
     */
    public const PROPERTY_TRACE_SWITCH = 'TRACE_ON';
    public const PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX = 'UNIQ_KEY';
    public const PROPERTY_MAX_RECONSUME_TIMES = 'MAX_RECONSUME_TIMES';
    public const PROPERTY_CONSUME_START_TIMESTAMP = 'CONSUME_START_TIME';

    public const PROPERTY_TRANSACTION_PREPARED_QUEUE_OFFSET = 'TRAN_PREPARED_QUEUE_OFFSET';
    public const PROPERTY_TRANSACTION_CHECK_TIMES = 'TRANSACTION_CHECK_TIMES';
    public const PROPERTY_CHECKED_TOPIC = 'CHECKED_TOPIC';
    public const PROPERTY_BORN_HOST = 'BORN_HOST';
    public const PROPERTY_BORN_TIMESTAMP = 'BORN_TIMESTAMP';
    public const PROPERTY_STORE_HOST = 'STORE_HOST';
    public const PROPERTY_STORE_TIMESTAMP = 'STORE_TIMESTAMP';
    public const PROPERTY_MSG_ID = 'MSG_ID';
    public const PROPERTY_WAIT_STORE_MSG_OK_PROP = 'WAIT_STORE_MSG_OK';
    public const PROPERTY_INSTANCE_ID = 'INSTANCE_ID';
    public const PROPERTY_CLUSTER = 'CLUSTER';
    public const PROPERTY_MESSAGE_TYPE = 'MSG_TYPE';
    /**
     * Request-Reply (5.x): the request message carries CORRELATION_ID/REPLY_TO_CLIENT/TTL, and the replying side (consumer)
     * sends back a message with MSG_TYPE="reply"; the broker pushes the reply back to the requester's
     * PUSH_REPLY_MESSAGE_TO_CLIENT(326) channel based on REPLY_TO_CLIENT.
     */
    public const PROPERTY_CORRELATION_ID = 'CORRELATION_ID';
    public const PROPERTY_MESSAGE_REPLY_TO_CLIENT = 'REPLY_TO_CLIENT';
    public const PROPERTY_MESSAGE_TTL = 'TTL';
    public const PROPERTY_REPLY_MESSAGE_ARRIVE_TIME = 'REPLY_MESSAGE_ARRIVE_TIME';
    public const PROPERTY_PUSH_REPLY_TIME = 'PUSH_REPLY_TIME';
    public const PROPERTY_INNER_MULTI_DISPATCH = 'INNER_MULTI_DISPATCH';
    public const PROPERTY_INNER_MULTI_QUEUE_OFFSET = 'INNER_MULTI_QUEUE_OFFSET';
    public const PROPERTY_POP_CK = 'POP_CK';
    public const PROPERTY_POP_CK_OFFSET = 'POP_CK_OFFSET';
    public const PROPERTY_POP_TIME = 'POP_TIME';
    /**
     * The key name is literally "1ST_POP_TIME" (the first character is the digit 1, not the letter I); writing it wrong
     * means nobody can read it. The client "only when missing" fills it in during POP response processing, with the value
     * being the popTime from the response header.
     */
    public const PROPERTY_FIRST_POP_TIME = '1ST_POP_TIME';
    public const PROPERTY_INVISIBLE_TIME = 'INVISIBLE_TIME';
    public const PROPERTY_START_TIME = 'START_TIME';
    public const PROPERTY_END_TIME = 'END_TIME';
    public const PROPERTY_EXPIRE_TIME = 'EXPIRE_TIME';
    public const PROPERTY_LAST_CONSUME_TIMESTAMP = 'LAST_CONSUME_TIME';
    public const PROPERTY_SELF_CONSUME_ENABLE = 'SELF_CONSUME';
    public const PROPERTY_RECONSUME_GROUP = 'RECONSUME_GROUP';
    public const PROPERTY_RECONSUME_TOPIC = 'RECONSUME_TOPIC';
    public const PROPERTY_KEYS_CONST = 'KEYS';
    public const PROPERTY_ORIGIN_QUEUE_ID = 'ORIGIN_QID';
    public const PROPERTY_ORIGIN_TOPIC = 'ORIGIN_TOPIC';

    public const STRING_HASH_SET = 1;

    public const KEY_SEPARATOR = ' ';
    public const KEY_SEPARATOR_CHAR = ' ';
    public const CHARACTER_MAX_LENGTH = 255;
    public const MESSAGE_ID_PREFIX = 'MSGID-';

    /**
     * Index query type (corresponds to MessageConst.INDEX_KEY_TYPE / INDEX_UNIQUE_TYPE / INDEX_TAG_TYPE).
     * The broker's QueryMessageRequestHeader.indexType takes these values; when empty the broker treats it as "K".
     */
    public const INDEX_KEY_TYPE = 'K';
    public const INDEX_UNIQUE_TYPE = 'U';
    public const INDEX_TAG_TYPE = 'T';

    public static function messageIdPrefix(): string
    {
        return self::MESSAGE_ID_PREFIX;
    }
}
