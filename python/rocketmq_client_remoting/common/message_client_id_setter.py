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


def client_msg_id(msg: Message) -> str:
    """Java ``MessageClientExt#getMsgId()`` parity.

    Client-side decoding (``MessageDecoder.decodesBatch(..., isClient=true)``)
    yields ``MessageClientExt``, whose ``getMsgId()`` returns the ``UNIQ_KEY``
    property and only falls back to the broker offset id when that property is
    absent.  This port keeps the decoded ``msg.msg_id`` as the offset id (and
    exposes it separately via ``get_offset_msg_id()``), so every place that in
    Java calls ``msg.getMsgId()`` on a *consumed* message must go through this
    helper instead of reading the offset id directly.
    """
    if msg is None:
        return ""
    uniq = get_uniq_id(msg)
    if uniq:
        return uniq
    return getattr(msg, "msg_id", None) or ""


__all__ = ["create_uniq_id", "set_uniq_id", "get_uniq_id", "client_msg_id"]
