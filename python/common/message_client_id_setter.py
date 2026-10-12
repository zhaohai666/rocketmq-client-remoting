# -*- coding: utf-8 -*-
from __future__ import annotations

from typing import Optional

from .message import Message
from .message_accessor import MessageAccessor
from .message_const import MessageConst
from .util_all import InnerIdGenerator


def create_uniq_id() -> str:
    return InnerIdGenerator.create_uniq_id()


def set_uniq_id(msg: Message) -> None:
    pass
    if msg is None:
        return
    if msg.get_property(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX) is None:
        MessageAccessor.put_property(msg, MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX,
                                     create_uniq_id())


def get_uniq_id(msg: Message) -> Optional[str]:
    if msg is None:
        return None
    return msg.get_property(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX)


__all__ = ["create_uniq_id", "set_uniq_id", "get_uniq_id"]
