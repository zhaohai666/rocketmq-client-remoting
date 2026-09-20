# -*- coding: utf-8 -*-
"""发送/订阅入口上的名字校验（对应 ``org.apache.rocketmq.client.Validators``）。

**为什么要在客户端就拦下来**：topic/group 名字非法时 broker 也会拒，但要等到请求
真的打出去才拿到 ``TOPIC_NOT_EXIST``/``ILLEGAL_TOPIC``，而 ``TOPIC_NOT_EXIST`` 在
发送重试的**可重试码**集合里 —— 于是每条必然失败的消息都会把重试次数和超时预算空转
一遍，最后报的还是同一个原因。本地校验让这类输入在 ``send()`` 的第一行就失败。

逐条对齐 Java：
  * ``checkTopic`` / ``checkGroup``：blank → 长度（127 / 120）→ 字符表 ``^[%|a-zA-Z0-9_-]+$``，
    顺序与文案都照抄。这三步走的是 Java 的 ``MQClientException(String, Throwable)``
    构造器，``responseCode`` 是 **-1**（不是某个 broker 码，表示"纯客户端错误"）。
  * ``checkMessage``：只有它带 ``ResponseCode.MESSAGE_ILLEGAL``(13)，且**顺序**要对——
    Java 先查 topic、再查禁发 topic、最后才查 body。
  * ``isNotAllowedSendTopic``：只禁 broker 内部流水那 8 个（``SCHEDULE_TOPIC_XXXX``、
    半消息、轨迹校验…）。**``%RETRY%`` 不在名单里**：``sendMessageBack`` 就是往
    ``%RETRY%group`` 写的，禁掉会打断重投链路。
"""
import os
from typing import Optional

from ..common.message import Message
from ..common.message_const import MessageConst
from ..common.topic_validator import (
    GROUP_MAX_LENGTH,
    TOPIC_MAX_LENGTH,
    VALID_CHAR_PATTERN,
    is_not_allowed_send_topic as _is_not_allowed_send_topic,
    is_system_topic as _is_system_topic,
    is_topic_or_group_illegal,
)
from ..remoting.protocol.codes import ResponseCode
from .exception import MQClientException

# 对应 Java ``Validators.CHARACTER_MAX_LENGTH``，本文件里没用到但保留常量口径
CHARACTER_MAX_LENGTH = 255

# 批量/普通消息都会走 checkMessage，这条与 Java 一样放在模块级方便测试引用
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
    """对应 ``Validators.isNotAllowedSendTopic``。

    码值口径照抄 Java，别想当然：``checkMessage`` 里**只有** null message / null body /
    zero body / oversize body / INNER_MULTI_DISPATCH 这五条带 MESSAGE_ILLEGAL(13)；禁发
    topic、系统 topic、``checkTopic``、``checkGroup`` 走的是 ``MQClientException(String, null)``，
    responseCode 是 -1（纯客户端错误）。于是「往 SCHEDULE_TOPIC_XXXX 发消息」在这里报 -1
    而不是 13——看着别扭，但上层按 responseCode 分支时必须知道这一点，四语言保持一致。
    """
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
