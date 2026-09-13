# -*- coding: utf-8 -*-
"""发送结果（对应 org.apache.rocketmq.client.producer.SendResult / SendStatus）。"""
from __future__ import annotations

from enum import Enum
from typing import Optional

from ..common.message import MessageQueue


class SendStatus(Enum):
    SEND_OK = 0
    FLUSH_DISK_TIMEOUT = 1
    FLUSH_SLAVE_TIMEOUT = 2
    SLAVE_NOT_AVAILABLE = 3

    @staticmethod
    def from_code(code: int) -> "SendStatus":
        try:
            return SendStatus(code)
        except ValueError:
            return SendStatus.SEND_OK


class SendResult:
    def __init__(self, send_status: SendStatus = SendStatus.SEND_OK,
                 msg_id: Optional[str] = None, message_queue: Optional[MessageQueue] = None,
                 queue_offset: int = 0, transaction_id: Optional[str] = None,
                 offset_msg_id: Optional[str] = None, region_id: Optional[str] = None):
        self.send_status = send_status
        self.msg_id = msg_id
        self.message_queue = message_queue
        self.queue_offset = queue_offset
        self.transaction_id = transaction_id
        self.offset_msg_id = offset_msg_id
        self.region_id = region_id

    def get_send_status(self) -> SendStatus:
        return self.send_status

    def get_msg_id(self) -> Optional[str]:
        return self.msg_id

    def get_message_queue(self) -> Optional[MessageQueue]:
        return self.message_queue

    def get_queue_offset(self) -> int:
        return self.queue_offset

    def get_transaction_id(self) -> Optional[str]:
        return self.transaction_id

    def set_transaction_id(self, transaction_id: Optional[str]) -> None:
        self.transaction_id = transaction_id

    def get_offset_msg_id(self) -> Optional[str]:
        return self.offset_msg_id

    def __repr__(self):
        return "SendResult [sendStatus=%s, msgId=%s, offsetMsgId=%s, messageQueue=%s, queueOffset=%d, transactionId=%s]" % (
            self.send_status, self.msg_id, self.offset_msg_id, self.message_queue,
            self.queue_offset, self.transaction_id)


__all__ = ["SendStatus", "SendResult"]