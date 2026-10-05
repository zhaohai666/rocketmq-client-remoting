# -*- coding: utf-8 -*-
"""消息属性访问器（对应 org.apache.rocketmq.common.message.MessageAccessor）。"""
from __future__ import annotations

from .message import Message
from .message_const import MessageConst


class MessageAccessor:
    @staticmethod
    def put_property(msg: Message, name: str, value: str) -> None:
        msg.put_property(name, value)

    @staticmethod
    def get_property(msg: Message, name: str):
        return msg.get_property(name)

    @staticmethod
    def clear_property(msg: Message, name: str) -> None:
        msg.remove_property(name)

    @staticmethod
    def set_keys(msg: Message, keys: str) -> None:
        msg.properties[MessageConst.PROPERTY_KEYS] = keys

    @staticmethod
    def get_keys(msg: Message):
        return msg.properties.get(MessageConst.PROPERTY_KEYS)

    @staticmethod
    def set_tags(msg: Message, tags: str) -> None:
        msg.properties[MessageConst.PROPERTY_TAGS] = tags

    @staticmethod
    def get_tags(msg: Message):
        return msg.properties.get(MessageConst.PROPERTY_TAGS)

    @staticmethod
    def set_delay_time_level(msg: Message, level: int) -> None:
        msg.properties[MessageConst.PROPERTY_DELAY_TIME_LEVEL] = str(level)

    @staticmethod
    def get_delay_time_level(msg: Message):
        return msg.properties.get(MessageConst.PROPERTY_DELAY_TIME_LEVEL)

    @staticmethod
    def set_wait_store_msg_ok(msg: Message, ok: bool) -> None:
        msg.properties[MessageConst.PROPERTY_WAIT_STORE_MSG_OK] = "true" if ok else "false"

    @staticmethod
    def set_transaction_id(msg: Message, transaction_id: str) -> None:
        msg.set_transaction_id(transaction_id)

    @staticmethod
    def get_transaction_id(msg: Message):
        return msg.get_transaction_id()

    @staticmethod
    def set_origin_message_id(msg: Message, origin_message_id: str) -> None:
        msg.properties[MessageConst.PROPERTY_ORIGIN_MESSAGE_ID] = origin_message_id

    @staticmethod
    def get_origin_message_id(msg: Message):
        return msg.properties.get(MessageConst.PROPERTY_ORIGIN_MESSAGE_ID)

    @staticmethod
    def set_consume_start_timestamp(msg: Message, ts: int) -> None:
        msg.properties[MessageConst.PROPERTY_CONSUME_START_TIMESTAMP] = str(ts)

    @staticmethod
    def get_consume_start_timestamp(msg: Message):
        return msg.properties.get(MessageConst.PROPERTY_CONSUME_START_TIMESTAMP)

    @staticmethod
    def get_reconsume_time(msg: Message):
        """Java MessageAccessor.getReconsumeTime：重试次数以**属性**形式挂在消息上，
        只由回投链路（``sendMessageBack``）写入，与 MessageExt 线上第 13 字段的
        reconsumeTimes 不是一回事 —— broker 消费投递给客户端的是后者。"""
        return msg.properties.get(MessageConst.PROPERTY_RECONSUME_TIME)

    @staticmethod
    def set_reconsume_time(msg: Message, v) -> None:
        msg.properties[MessageConst.PROPERTY_RECONSUME_TIME] = str(v)

    @staticmethod
    def get_max_reconsume_times(msg: Message):
        return msg.properties.get(MessageConst.PROPERTY_MAX_RECONSUME_TIMES)

    @staticmethod
    def set_max_reconsume_times(msg: Message, v: int) -> None:
        msg.properties[MessageConst.PROPERTY_MAX_RECONSUME_TIMES] = str(v)

    @staticmethod
    def set_transaction_prepared(msg: Message) -> None:
        msg.properties[MessageConst.PROPERTY_TRANSACTION_PREPARED] = "true"

    @staticmethod
    def is_transaction_prepared(msg: Message) -> bool:
        return msg.properties.get(MessageConst.PROPERTY_TRANSACTION_PREPARED) == "true"

    @staticmethod
    def set_transaction_prepared_queue_offset(msg: Message, offset: int) -> None:
        msg.properties[MessageConst.PROPERTY_TRANSACTION_PREPARED_QUEUE_OFFSET] = str(offset)