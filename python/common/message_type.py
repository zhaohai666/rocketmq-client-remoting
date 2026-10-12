# -*- coding: utf-8 -*-
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
