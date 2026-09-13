# -*- coding: utf-8 -*-
"""org.apache.rocketmq.common.message 的 Python 对应：消息模型与编解码。"""
from __future__ import annotations

import socket
import struct
import zlib
from typing import Dict, List, Optional

from .mix_all import MixAll


class MessageQueue:
    """对应 org.apache.rocketmq.common.message.MessageQueue."""

    def __init__(self, topic: str = "", broker_name: str = "", queue_id: int = 0):
        self.topic = topic
        self.broker_name = broker_name
        self.queue_id = queue_id

    # ---- Java 风格 getter / setter（保证 JSON 字段名 camelCase）----
    def get_topic(self) -> str:
        return self.topic

    def set_topic(self, topic: str) -> None:
        self.topic = topic

    def get_broker_name(self) -> str:
        return self.broker_name

    def set_broker_name(self, broker_name: str) -> None:
        self.broker_name = broker_name

    def get_queue_id(self) -> int:
        return self.queue_id

    def set_queue_id(self, queue_id: int) -> None:
        self.queue_id = queue_id

    def get_queue_id_str(self) -> str:
        return str(self.queue_id)

    def hashcode(self) -> int:
        """与 Java hashCode 对齐：((31+bh)*31+qid)*31+th，结果按 32 位有符号回绕。"""
        topic_hash = self._str_hash(self.topic)
        broker_hash = self._str_hash(self.broker_name)
        return c_int32(((31 + broker_hash) * 31 + self.queue_id) * 31 + topic_hash)

    @staticmethod
    def _str_hash(s: str) -> int:
        h = 0
        for ch in s:
            h = 31 * h + ord(ch)
        # Java int 溢出：以 32 位有符号回绕
        return c_int32(h)

    def __hash__(self) -> int:
        return self.hashcode()

    def __eq__(self, other) -> bool:
        if not isinstance(other, MessageQueue):
            return False
        return (self.topic == other.topic and self.broker_name == other.broker_name
                and self.queue_id == other.queue_id)

    def compare_to(self, other: "MessageQueue") -> int:
        if self.topic != other.topic:
            return -1 if self.topic < other.topic else 1
        if self.broker_name != other.broker_name:
            return -1 if self.broker_name < other.broker_name else 1
        if self.queue_id != other.queue_id:
            return -1 if self.queue_id < other.queue_id else 1
        return 0

    def __repr__(self):
        return "MessageQueue(topic='%s', broker='%s', qid=%d)" % (
            self.topic, self.broker_name, self.queue_id)


def c_int32(x: int) -> int:
    x &= 0xFFFFFFFF
    return x if x < 0x80000000 else x - 0x100000000


class Message:
    """对应 org.apache.rocketmq.common.message.Message."""

    def __init__(self, topic: str = "", body: Optional[bytes] = None,
                 tags: Optional[str] = None, keys: Optional[str] = None,
                 flag: int = 0):
        self.topic = topic
        self.flag = flag
        self.properties: Dict[str, str] = {}
        if body is None:
            body = b""
        self.body = body
        if tags is not None and tags:
            self.properties["TAGS"] = tags
        if keys is not None and keys:
            self.properties["KEYS"] = keys
        self.transaction_id: Optional[str] = None

    # ---- 属性 ----
    def set_tags(self, tags: str) -> None:
        self.properties["TAGS"] = tags

    def get_tags(self) -> Optional[str]:
        return self.properties.get("TAGS")

    def set_keys(self, keys: str) -> None:
        self.properties["KEYS"] = keys

    def get_keys(self) -> Optional[str]:
        return self.properties.get("KEYS")

    def set_delay_time_level(self, level: int) -> None:
        self.properties["DELAY"] = str(level)

    def get_delay_time_level(self) -> Optional[str]:
        return self.properties.get("DELAY")

    def set_wait_store_msg_ok(self, ok: bool) -> None:
        self.properties["WAIT"] = "true" if ok else "false"

    def get_wait_store_msg_ok(self) -> Optional[str]:
        return self.properties.get("WAIT")

    def set_user_property(self, name: str, value: str) -> None:
        self.properties[name] = value

    def get_user_property(self, name: str) -> Optional[str]:
        return self.properties.get(name)

    def put_property(self, name: str, value: str) -> None:
        self.properties[name] = value

    def remove_property(self, name: str) -> None:
        self.properties.pop(name, None)

    def get_property(self, name: str) -> Optional[str]:
        return self.properties.get(name)

    def clear_property(self) -> None:
        self.properties.clear()

    # ---- Java 风格 ----
    def get_topic(self) -> str:
        return self.topic

    def set_topic(self, topic: str) -> None:
        self.topic = topic

    def get_body(self) -> bytes:
        return self.body

    def set_body(self, body: bytes) -> None:
        self.body = body

    def get_flag(self) -> int:
        return self.flag

    def set_flag(self, flag: int) -> None:
        self.flag = flag

    def get_properties(self) -> Dict[str, str]:
        return self.properties

    def set_properties(self, properties: Dict[str, str]) -> None:
        self.properties = properties

    def get_transaction_id(self) -> Optional[str]:
        return self.transaction_id

    def set_transaction_id(self, transaction_id: Optional[str]) -> None:
        self.transaction_id = transaction_id

    def __repr__(self):
        return "Message(topic='%s', body=%d bytes)" % (self.topic, len(self.body))


class MessageExt(Message):
    """对应 org.apache.rocketmq.common.message.MessageExt（拉取到的消息）。"""

    def __init__(self, topic: str = "", body: Optional[bytes] = None,
                 tags: Optional[str] = None, keys: Optional[str] = None, flag: int = 0):
        super().__init__(topic, body, tags, keys, flag)
        self.queue_id: int = 0
        self.store_size: int = 0
        self.queue_offset: int = 0
        self.sys_flag: int = 0
        self.born_timestamp: int = 0
        self.born_host: Optional[str] = None
        self.born_host_port: int = 0
        self.store_timestamp: int = 0
        self.store_host: Optional[str] = None
        self.store_host_port: int = 0
        self.msg_id: Optional[str] = None
        self.commit_log_offset: int = 0
        self.body_crc: int = 0
        self.reconsume_times: int = 0
        self.prepared_transaction_offset: int = 0
        self.broker_name: Optional[str] = None
        self.offset_msg_id: Optional[str] = None
        self.msg_type: Optional[str] = None

    def get_queue_id(self) -> int:
        return self.queue_id

    def set_queue_id(self, queue_id: int) -> None:
        self.queue_id = queue_id

    def get_store_size(self) -> int:
        return self.store_size

    def set_store_size(self, size: int) -> None:
        self.store_size = size

    def get_queue_offset(self) -> int:
        return self.queue_offset

    def set_queue_offset(self, queue_offset: int) -> None:
        self.queue_offset = queue_offset

    def get_sys_flag(self) -> int:
        return self.sys_flag

    def set_sys_flag(self, sys_flag: int) -> None:
        self.sys_flag = sys_flag

    def get_born_timestamp(self) -> int:
        return self.born_timestamp

    def set_born_timestamp(self, born_timestamp: int) -> None:
        self.born_timestamp = born_timestamp

    def get_born_host(self) -> Optional[str]:
        return self.born_host

    def set_born_host(self, born_host: Optional[str]) -> None:
        self.born_host = born_host

    def get_store_timestamp(self) -> int:
        return self.store_timestamp

    def set_store_timestamp(self, ts: int) -> None:
        self.store_timestamp = ts

    def get_store_host(self) -> Optional[str]:
        return self.store_host

    def set_store_host(self, store_host: Optional[str]) -> None:
        self.store_host = store_host

    def get_msg_id(self) -> Optional[str]:
        return self.msg_id

    def set_msg_id(self, msg_id: Optional[str]) -> None:
        self.msg_id = msg_id

    def get_commit_log_offset(self) -> int:
        return self.commit_log_offset

    def set_commit_log_offset(self, offset: int) -> None:
        self.commit_log_offset = offset

    def get_body_crc(self) -> int:
        return self.body_crc

    def set_body_crc(self, crc: int) -> None:
        self.body_crc = crc

    def get_reconsume_times(self) -> int:
        return self.reconsume_times

    def set_reconsume_times(self, n: int) -> None:
        self.reconsume_times = n

    def get_prepared_transaction_offset(self) -> int:
        return self.prepared_transaction_offset

    def set_prepared_transaction_offset(self, offset: int) -> None:
        self.prepared_transaction_offset = offset

    def set_broker_name(self, broker_name: Optional[str]) -> None:
        self.broker_name = broker_name

    def get_broker_name(self) -> Optional[str]:
        return self.broker_name

    def get_offset_msg_id(self) -> Optional[str]:
        return self.offset_msg_id

    def set_offset_msg_id(self, msg_id: Optional[str]) -> None:
        self.offset_msg_id = msg_id

    def get_msg_type(self) -> Optional[str]:
        return self.msg_type

    def set_msg_type(self, msg_type: Optional[str]) -> None:
        self.msg_type = msg_type

    def get_born_host_string(self) -> Optional[str]:
        if self.born_host and self.born_host_port:
            return "%s:%d" % (self.born_host, self.born_host_port)
        return self.born_host

    def get_store_host_string(self) -> Optional[str]:
        if self.store_host and self.store_host_port:
            return "%s:%d" % (self.store_host, self.store_host_port)
        return self.store_host

    def __repr__(self):
        return "MessageExt(topic='%s', msgId='%s', queueOffset=%d, body=%d bytes)" % (
            self.topic, self.msg_id, self.queue_offset, len(self.body))


class MessageBatch(Message):
    """批量消息（对应 org.apache.rocketmq.common.message.MessageBatch）。

    没有自己的序列化字段，body 由 ``encode()`` 生成：
    ``MessageDecoder.encodeMessages(messages)`` 的拼接结果（每条为 6 段轻量格式）。
    """

    def __init__(self, messages: Optional[List[Message]] = None):
        super().__init__()
        self.messages: List[Message] = list(messages) if messages else []

    def encode(self) -> bytes:
        """对应 Java MessageBatch.encode()。"""
        from .message_decoder import encode_messages
        return encode_messages(self.messages)

    def __iter__(self):
        return iter(self.messages)

    def __len__(self) -> int:
        return len(self.messages)

    @staticmethod
    def generate_from_list(messages: List[Message]) -> "MessageBatch":
        """对应 Java MessageBatch.generateFromList。

        约束：非空；同一 topic；同一 waitStoreMsgOK；不允许延时消息；不允许重试 topic。
        Java 抛 UnsupportedOperationException / IllegalArgumentException，
        这里统一映射为 Python 的 ValueError。
        """
        if not messages:
            raise ValueError("messages must not be null or empty")

        message_list: List[Message] = []
        first: Optional[Message] = None
        for message in messages:
            delay_level = message.get_delay_time_level()
            if delay_level and int(delay_level) > 0:
                raise ValueError("Delayed messages are not supported for batching")
            if message.get_topic().startswith(MixAll.RETRY_GROUP_TOPIC_PREFIX):
                raise ValueError("Retry Group is not supported for batching")
            if first is None:
                first = message
            else:
                if first.get_topic() != message.get_topic():
                    raise ValueError("The topic of the messages in one batch should be the same")
                if first.get_wait_store_msg_ok() != message.get_wait_store_msg_ok():
                    raise ValueError("The waitStoreMsgOK of the messages in one batch should be the same")
            message_list.append(message)

        batch = MessageBatch(message_list)
        batch.set_topic(first.get_topic())
        batch.set_wait_store_msg_ok(first.get_wait_store_msg_ok() == "true")
        batch.set_body(batch.encode())
        return batch