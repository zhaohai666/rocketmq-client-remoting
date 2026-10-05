<?php

declare(strict_types=1);

namespace RocketMQ\Common;

/**
 * 消息属性键常量（对应 org.apache.rocketmq.common.message.MessageConst，
 * 移植自 message_const.py）。
 */
final class MessageConst
{
    public const PROPERTY_KEYS = 'KEYS';
    public const PROPERTY_TAGS = 'TAGS';
    public const PROPERTY_WAIT_STORE_MSG_OK = 'WAIT';
    public const PROPERTY_DELAY_TIME_LEVEL = 'DELAY';
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
     * 消息轨迹开关：broker 在 SEND 响应头里带回（SendMessageProcessor 写
     * String.valueOf(brokerConfig.isTraceOn())，默认 true），客户端据此决定是否落轨迹；
     * 消费侧则从消息属性里读同一个 key（ConsumeMessageTraceHookImpl）。
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
     * Request-Reply（5.x）：请求消息带 CORRELATION_ID/REPLY_TO_CLIENT/TTL，
     * 应答方（消费者）回一条 MSG_TYPE="reply" 的消息，broker 按 REPLY_TO_CLIENT
     * 把应答推回请求方的 PUSH_REPLY_MESSAGE_TO_CLIENT(326) 通道。
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
     * Java 侧写的是 "1ST_POP_TIME"（MessageConst.PROPERTY_FIRST_POP_TIME），
     * 客户端在 POP 响应后处理时"仅在缺失时"补上，值为响应头的 popTime。
     */
    public const PROPERTY_FIRST_POP_TIME = '1ST_POP_TIME';
    public const PROPERTY_INVISIBLE_TIME = 'INVISIBLE_TIME';
    public const PROPERTY_DELAY_TIME = 'DELAY_TIME';
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
     * 索引查询类型（对应 MessageConst.INDEX_KEY_TYPE / INDEX_UNIQUE_TYPE / INDEX_TAG_TYPE）
     * broker 的 QueryMessageRequestHeader.indexType 取这些值；为空时 broker 按 "K" 处理。
     */
    public const INDEX_KEY_TYPE = 'K';
    public const INDEX_UNIQUE_TYPE = 'U';
    public const INDEX_TAG_TYPE = 'T';

    public static function messageIdPrefix(): string
    {
        return self::MESSAGE_ID_PREFIX;
    }
}
