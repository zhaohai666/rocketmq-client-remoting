# -*- coding: utf-8 -*-
import os
from typing import Optional

from common.message import Message
from common.message_const import MessageConst
from common.topic_validator import (
    GROUP_MAX_LENGTH,
    TOPIC_MAX_LENGTH,
    VALID_CHAR_PATTERN,
    is_not_allowed_send_topic as _is_not_allowed_send_topic,
    is_system_topic as _is_system_topic,
    is_topic_or_group_illegal,
)
from remoting.protocol.codes import ResponseCode
from .exception import MQClientException

CHARACTER_MAX_LENGTH = 255

FILE_SEPARATOR = os.sep


def check_group(group: Optional[str]) -> None:
    """对应 ``Validators.checkGroup``。"""
    if group is None or not group.strip():
        raise MQClientException("the specified group is blank")
    if len(group) > GROUP_MAX_LENGTH:
        raise MQClientException(
            "the specified group[%s] is longer than group max length: %s." % (group, GROUP_MAX_LENGTH))
    if is_topic_or_group_illegal(group):
        raise MQClientException(
            "the specified group[%s] contains illegal characters, allowing only %s"
            % (group, VALID_CHAR_PATTERN))


def check_topic(topic: Optional[str]) -> None:
    """对应 ``Validators.checkTopic``。"""
    if topic is None or not topic.strip():
        raise MQClientException("The specified topic is blank")
    if len(topic) > TOPIC_MAX_LENGTH:
        raise MQClientException(
            "The specified topic is longer than topic max length %d." % TOPIC_MAX_LENGTH)
    if is_topic_or_group_illegal(topic):
        raise MQClientException(
            "The specified topic[%s] contains illegal characters, allowing only %s"
            % (topic, VALID_CHAR_PATTERN))


def is_system_topic(topic: str) -> None:
    """对应 ``Validators.isSystemTopic``：命中系统 topic 直接抛，正常时返回 None。"""
    if _is_system_topic(topic):
        raise MQClientException("The topic[%s] is conflict with system topic." % topic)


def is_not_allowed_send_topic(topic: str) -> None:
    pass
    if _is_not_allowed_send_topic(topic):
        raise MQClientException("Sending message to topic[%s] is forbidden." % topic)


def check_message(msg: Optional[Message], max_message_size: int) -> None:
    """对应 ``Validators.checkMessage(msg, producer)``（顺序与文案逐条照抄）。"""
    if msg is None:
        raise MQClientException("the message is null", ResponseCode.MESSAGE_ILLEGAL)
    check_topic(msg.topic)
    is_not_allowed_send_topic(msg.topic)
    body = msg.body
    if body is None:
        raise MQClientException("the message body is null", ResponseCode.MESSAGE_ILLEGAL)
    if len(body) == 0:
        raise MQClientException("the message body length is zero", ResponseCode.MESSAGE_ILLEGAL)
    if len(body) > max_message_size:
        raise MQClientException(
            "the message body size over max value, MAX: %d" % max_message_size,
            ResponseCode.MESSAGE_ILLEGAL)
    # 多队列分发（LMQ）的路径里带文件系统分隔符会让 broker 侧建队列时拼出越界路径
    lmq_path = msg.get_property(MessageConst.PROPERTY_INNER_MULTI_DISPATCH)
    if lmq_path and FILE_SEPARATOR in lmq_path:
        raise MQClientException(
            "INNER_MULTI_DISPATCH %s can not contains %s character" % (lmq_path, FILE_SEPARATOR),
            ResponseCode.MESSAGE_ILLEGAL)
