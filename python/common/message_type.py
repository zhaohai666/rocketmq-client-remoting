# -*- coding: utf-8 -*-
"""消息类型（对应 org.apache.rocketmq.common.message.MessageType）。

Java 侧 TraceBean 的 ``msgType`` 在编码轨迹时用的是 **ordinal()**（见
TraceDataEncoder 的 Pub/EndTransaction 分支），所以这里的整型值必须与 Java 的枚举
声明顺序严格一致：Normal_Msg=0, Trans_Msg_Half=1, Trans_msg_Commit=2,
Delay_Msg=3, Order_Msg=4。
"""
from __future__ import annotations

from enum import Enum


class MessageType(Enum):
    NORMAL_MSG = 0
    TRANS_MSG_HALF = 1
    TRANS_MSG_COMMIT = 2
    DELAY_MSG = 3
    ORDER_MSG = 4

    @property
    def short_name(self) -> str:
        return _SHORT_NAMES[self]

    @staticmethod
    def get_by_short_name(short_name: str) -> "MessageType":
        for t, s in _SHORT_NAMES.items():
            if s == short_name:
                return t
        return MessageType.NORMAL_MSG


_SHORT_NAMES = {
    MessageType.NORMAL_MSG: "Normal",
    MessageType.TRANS_MSG_HALF: "Trans",
    MessageType.TRANS_MSG_COMMIT: "TransCommit",
    MessageType.DELAY_MSG: "Delay",
    MessageType.ORDER_MSG: "Order",
}

__all__ = ["MessageType"]
