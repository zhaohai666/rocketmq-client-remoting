# -*- coding: utf-8 -*-
"""消费/拉取结果类型（对应 org.apache.rocketmq.client.consumer.* 及 pull.*）。"""
from __future__ import annotations

from enum import Enum
from typing import List, Optional

from ..common.message import MessageExt


class PullStatus(Enum):
    FOUND = 0
    NO_NEW_MSG = 1
    NO_MATCHED_MSG = 2
    OFFSET_ILLEGAL = 3

    @staticmethod
    def from_code(code: int) -> "PullStatus":
        try:
            return PullStatus(code)
        except ValueError:
            return PullStatus.FOUND


class PullResult:
    def __init__(self, status: PullStatus, next_begin_offset: int = 0,
                 min_offset: int = 0, max_offset: int = 0,
                 msg_found_list: Optional[List[MessageExt]] = None):
        self.status = status
        self.next_begin_offset = next_begin_offset
        self.min_offset = min_offset
        self.max_offset = max_offset
        self.msg_found_list = msg_found_list or []

    def __repr__(self):
        return "PullResult [status=%s, nextBeginOffset=%d, minOffset=%d, maxOffset=%d, msgFoundList.size=%d]" % (
            self.status, self.next_begin_offset, self.min_offset, self.max_offset, len(self.msg_found_list))


class ConsumeConcurrentlyStatus(Enum):
    CONSUME_SUCCESS = 0
    RECONSUME_LATER = 1


class ConsumeOrderlyStatus(Enum):
    SUCCESS = 0
    SUSPEND_CURRENT_QUEUE_A_MOMENT = 1


class ConsumeConcurrentlyContext:
    def __init__(self, message_queue=None):
        self.message_queue = message_queue
        # 对应 Java ConsumeConcurrentlyContext.delayLevelWhenNextConsume（缺省 0）。
        # 为 0 时由 caller 改写为 3 + reconsumeTimes（见 consumer.py _send_back_batch）。
        self.delay_level_when_next_consume = 0
        self.ack_index = -1


class ConsumeOrderlyContext:
    def __init__(self, message_queue=None):
        self.message_queue = message_queue
        self.auto_commit = True
        # 对应 Java ConsumeOrderlyContext.suspendCurrentQueueTimeMillis
        self.suspend_current_queue_time_millis = 1000


class MessageListenerConcurrently:
    def consume_message(self, msgs: List[MessageExt], context: ConsumeConcurrentlyContext) -> ConsumeConcurrentlyStatus:
        raise NotImplementedError


class MessageListenerOrderly:
    def consume_message(self, msgs: List[MessageExt], context: ConsumeOrderlyContext) -> ConsumeOrderlyStatus:
        raise NotImplementedError


class MessageListener:
    """兼容别名：先按并发监听器对待。"""

    def consume_message(self, msgs: List[MessageExt], context) -> ConsumeConcurrentlyStatus:
        raise NotImplementedError


__all__ = [
    "PullStatus", "PullResult", "ConsumeConcurrentlyStatus", "ConsumeOrderlyStatus",
    "ConsumeConcurrentlyContext", "ConsumeOrderlyContext",
    "MessageListenerConcurrently", "MessageListenerOrderly", "MessageListener",
]