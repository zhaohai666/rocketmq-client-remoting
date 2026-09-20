# -*- coding: utf-8 -*-
"""定时消息的撤回句柄（对应 org.apache.rocketmq.common.producer.RecallMessageHandle）。

句柄不是客户端自己造的：发送带 ``TIMER_DELIVER_MS`` / ``TIMER_DELAY_MS`` / ``TIMER_DELAY_SEC``
的定时消息时，broker 在 ``SendMessageProcessor#attachRecallHandle`` 里把这个句柄挂到
SEND 响应头的 ``recallHandle`` 字段上返回，客户端只负责原样带回去调用 ``recallMessage``。
因此普通消息的响应里根本没有这个字段。

编码格式与 Java 完全一致：``base64url("v1 <topic> <brokerName> <timestampStr> <messageId>")``，
5 段、空格分隔。

与 Java 的两处显式差异：
  * Java ``buildHandle`` 用 ``Base64.getUrlEncoder()``（**带** ``=`` 填充），
    ``decodeHandle`` 用 ``getUrlDecoder()``（严格，无填充串会抛错）。这里编码同样带填充，
    解码则两种都吃：其他客户端移植版用无填充解码器，只发无填充句柄的客户端写下的
    消息也要能撤回。
  * Java 解码失败抛 ``DecoderException``，``DefaultMQProducerImpl#recallMessage`` 再包成
    ``MQClientException(e.getMessage())``。Python 没有 checked exception，这里直接抛
    ``MQClientException``，文案仍是 Java 的 ``"recall handle is invalid"``。
"""
from __future__ import annotations

import base64
from typing import Optional

from ..client.exception import MQClientException

SEPARATOR = " "
VERSION_1 = "v1"
INVALID_HANDLE = "recall handle is invalid"


class HandleV1:
    """对应 Java ``RecallMessageHandle.HandleV1``。

    ``timestamp_str`` 保留字符串而不是转成 int：Java 就是原样存 ``String``，
    撤回时要把它回填到请求里，非法时间戳由 broker 判定（``ILLEGAL_OPERATION``）。
    """

    def __init__(self, topic: Optional[str] = None, broker_name: Optional[str] = None,
                 timestamp_str: Optional[str] = None, message_id: Optional[str] = None):
        self.topic = topic
        self.broker_name = broker_name
        self.timestamp_str = timestamp_str
        self.message_id = message_id

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, HandleV1):
            return NotImplemented
        return (self.topic, self.broker_name, self.timestamp_str, self.message_id) == \
               (other.topic, other.broker_name, other.timestamp_str, other.message_id)

    def __repr__(self) -> str:
        return ("HandleV1(topic=%r, broker_name=%r, timestamp_str=%r, message_id=%r)"
                % (self.topic, self.broker_name, self.timestamp_str, self.message_id))


def build_handle(topic: str, broker_name: str, timestamp_str: str, message_id: str) -> str:
    """对应 Java ``RecallMessageHandle.buildHandle``（带 ``=`` 填充，与 Java 一致）。"""
    raw = SEPARATOR.join([VERSION_1, topic, broker_name, timestamp_str, message_id])
    return base64.urlsafe_b64encode(raw.encode("utf-8")).decode("ascii")


def decode_handle(handle: str) -> HandleV1:
    """对应 Java ``RecallMessageHandle.decodeHandle``，见模块 docstring 的容忍度差异。"""
    if not handle:
        raise MQClientException(INVALID_HANDLE)
    padded = handle + "=" * (-len(handle) % 4)
    try:
        raw = base64.urlsafe_b64decode(padded)
    except Exception:  # noqa: BLE001  # binascii.Error 等，Java 此处同样只给一个固定文案
        raise MQClientException(INVALID_HANDLE)
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError:
        raise MQClientException(INVALID_HANDLE)
    items = text.split(SEPARATOR)
    if len(items) < 5 or items[0] != VERSION_1:
        raise MQClientException(INVALID_HANDLE)
    # Java 取 items[1..4]，多余的分段直接忽略（"v1 t b ts id extra" 仍然合法）。
    return HandleV1(items[1], items[2], items[3], items[4])
