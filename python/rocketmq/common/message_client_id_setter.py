# -*- coding: utf-8 -*-
"""客户端消息唯一 ID（对应 org.apache.rocketmq.common.message.MessageClientIDSetter）。

用途：
  * ``create_uniq_id()`` 生成 32 位十六进制唯一 ID（IP + PID + 类哈希 + 当月毫秒 + 自增）；
  * ``set_uniq_id(msg)``  发送前把 UNIQ_KEY 写到消息属性上（Java 在
    ``DefaultMQProducerImpl.sendKernelImpl`` 里对**非批量**消息调用）；
  * ``get_uniq_id(msg)``  取 UNIQ_KEY —— SendResult.msgId、消息轨迹的 msgId、
    事务消息的 transactionId 都用它。

⚠ 没有它的话 SendResult.msgId 只能退化成 broker 的 offsetMsgId（含 commitlog 偏移），
与 Java 的语义不同，且轨迹里的 msgId 与消费侧对不上。
"""
from __future__ import annotations

from typing import Optional

from .message import Message, MessageBatch
from .message_accessor import MessageAccessor
from .message_const import MessageConst
from .util_all import InnerIdGenerator


def create_uniq_id() -> str:
    return InnerIdGenerator.create_uniq_id()


def set_uniq_id(msg: Message) -> None:
    """UNIQ_KEY 缺失时才写入（Java MessageClientIDSetter.setUniqID）。"""
    if msg is None:
        return
    if msg.get_property(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX) is None:
        MessageAccessor.put_property(msg, MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX,
                                     create_uniq_id())


def get_uniq_id(msg: Message) -> Optional[str]:
    if msg is None:
        return None
    return msg.get_property(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX)


def get_uniq_id_from_batch(batch: "MessageBatch") -> Optional[str]:
    """批量消息的 UNIQ_KEY：逐条 ID 用 ',' 拼接（Java getUniqID(MessageBatch)）。"""
    if batch is None:
        return None
    if not isinstance(batch, MessageBatch):
        return get_uniq_id(batch)
    ids = []
    for inner in getattr(batch, "messages", None) or []:
        uid = get_uniq_id(inner)
        if uid:
            ids.append(uid)
    return ",".join(ids) if ids else None


__all__ = ["create_uniq_id", "set_uniq_id", "get_uniq_id", "get_uniq_id_from_batch"]
