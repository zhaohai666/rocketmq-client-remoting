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


class ConsumeReturnType(Enum):
    """消费返回类型（对应 Java client.consumer.listener.ConsumeReturnType）。

    ⚠ 顺序即 ordinal —— 轨迹 SubAfter 的 ``contextCode`` 用的正是 ordinal
    （Java ConsumeMessageTraceHookImpl:113），改动顺序会让控制台显示错乱。
    """

    SUCCESS = 0
    TIME_OUT = 1
    EXCEPTION = 2
    RETURNNULL = 3
    FAILED = 4


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


class PopStatus(Enum):
    """对应 Java ``org.apache.rocketmq.client.consumer.PopStatus``。"""

    FOUND = 0
    NO_NEW_MSG = 1
    POLLING_FULL = 2
    POLLING_NOT_FOUND = 3


class PopResult:
    """POP 响应（对应 Java ``PopResult``）。

    ``start_offset_info`` / ``msg_offset_info`` / ``order_count_info`` 保留 broker 原样
    字符串，解析交给 ``remoting.protocol.extra_info``；``msg_found_list`` 里的每条消息
    都已盖好 ``POP_CK``（客户端反构）与 ``1ST_POP_TIME`` 属性。
    """

    def __init__(self, status: PopStatus, msg_found_list: Optional[List[MessageExt]] = None,
                 rest_num: int = 0, pop_time: int = 0, invisible_time: int = 0,
                 revive_qid: int = 0, start_offset_info: Optional[str] = None,
                 msg_offset_info: Optional[str] = None,
                 order_count_info: Optional[str] = None):
        self.status = status
        self.msg_found_list = msg_found_list or []
        self.rest_num = rest_num
        self.pop_time = pop_time
        self.invisible_time = invisible_time
        self.revive_qid = revive_qid
        self.start_offset_info = start_offset_info
        self.msg_offset_info = msg_offset_info
        self.order_count_info = order_count_info

    def __repr__(self):
        return ("PopResult [status=%s, restNum=%d, popTime=%d, invisibleTime=%d, reviveQid=%d, "
                "msgFoundList.size=%d]") % (
            self.status, self.rest_num, self.pop_time, self.invisible_time, self.revive_qid,
            len(self.msg_found_list))


class ChangeInvisibleTimeResult:
    """``change_invisible_time`` 的结果。

    ``extra_info`` 是用响应里**新的** popTime/invisibleTime/reviveQid 重建的 8 段 CK 串，
    后续 ACK 要用它（不是请求时传进去的那个旧串）。
    """

    def __init__(self, response_code: int, pop_time: int = 0, invisible_time: int = 0,
                 revive_qid: int = 0, extra_info: Optional[str] = None):
        self.response_code = response_code
        self.pop_time = pop_time
        self.invisible_time = invisible_time
        self.revive_qid = revive_qid
        self.extra_info = extra_info
        self.success = response_code == 0

    def __repr__(self):
        return ("ChangeInvisibleTimeResult [responseCode=%d, popTime=%d, invisibleTime=%d, "
                "reviveQid=%d]") % (self.response_code, self.pop_time, self.invisible_time, self.revive_qid)


class ConsumeConcurrentlyStatus(Enum):
    CONSUME_SUCCESS = 0
    RECONSUME_LATER = 1


class ConsumeOrderlyStatus(Enum):
    # 声明顺序与 Java ``ConsumeOrderlyStatus`` 逐字对齐（ORDINAL 不是线上值，但两侧
    # 一致才不会在"按 ordinal 写死"的调用方手里错位）；COMMIT/ROLLBACK 在 Java 侧
    # 标注为 ``@Deprecated`` + "only for binlog consumption"，语义见 ``_consume_batch``。
    SUCCESS = 0
    ROLLBACK = 1
    COMMIT = 2
    SUSPEND_CURRENT_QUEUE_A_MOMENT = 3


def consume_status_name(status) -> str:
    """钩子上下文里的 ``status``：Java ``status.toString()`` 的形态，即**裸枚举成员名**。

    Java 写进 ``ConsumeMessageContext.status`` 的是归一化后那个枚举的 ``toString()``
    （并发 ``ConsumeMessageConcurrentlyService:408``、顺序 ``ConsumeMessageOrderlyService:507``），
    默认实现就是成员名：``CONSUME_SUCCESS`` / ``RECONSUME_LATER`` / ``SUCCESS`` /
    ``SUSPEND_CURRENT_QUEUE_A_MOMENT`` / ``COMMIT`` / ``ROLLBACK``。Python 的
    ``str(Enum member)`` 给的是 ``"ConsumeOrderlyStatus.SUCCESS"`` 这种带类名的形态，
    不是 Java 的；钩子 status 是**公开可见**的（用户 ``ConsumeMessageHook`` 直接读它），
    所以这里统一按 ``name`` 取。非枚举（动态类型下 listener 可能返回别的东西）保持
    ``str()`` 兜底，与归一化兜底分支的取值一致。
    """
    name = getattr(status, "name", None)
    return name if isinstance(name, str) else str(status)


class ConsumeConcurrentlyContext:
    def __init__(self, message_queue=None):
        self.message_queue = message_queue
        # 对应 Java ConsumeConcurrentlyContext.delayLevelWhenNextConsume（缺省 0）。
        # 为 0 时由 caller 改写为 3 + reconsumeTimes（见 consumer.py _send_back_batch）。
        self.delay_level_when_next_consume = 0
        # 对应 Java ConsumeConcurrentlyContext.ackIndex（默认 Integer.MAX_VALUE）：
        # 「listener 认可到第几条」，下标含自身，其后的消息按状态回投/丢弃。
        # 默认值是「全批认可」，只有 listener 主动调小才会部分 ack。
        self.ack_index = (1 << 31) - 1


class ConsumeOrderlyContext:
    def __init__(self, message_queue=None):
        self.message_queue = message_queue
        # 对应 Java ConsumeOrderlyContext.autoCommit：true 时由客户端按 listener 的
        # 状态提交位点（正常路径）；listener 置 false 拿走提交权（Java 的 binlog 用法），
        # 只有 COMMIT 才提交、ROLLBACK 回滚重投。
        self.auto_commit = True
        # 对应 Java ConsumeOrderlyContext.suspendCurrentQueueTimeMillis，**默认 -1**
        # （Java 就是这个默认值）：-1 表示"没指定"，挂起时长回落到消费者配置的
        # ``suspend_current_queue_time_millis``（默认 1000）；解析出来的值再由调度侧
        # 钳到 [10, 30000]（Java ConsumeMessageOrderlyService#submitConsumeRequestLater:216-225）。
        self.suspend_current_queue_time_millis = -1


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