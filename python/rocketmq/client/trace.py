# -*- coding: utf-8 -*-
"""消息轨迹（对应 org.apache.rocketmq.client.trace 包 + client.AccessChannel）。

包含：
  * ``TraceConstants``     —— 常量（org.apache.rocketmq.client.trace.TraceConstants）
  * ``TraceType``          —— Pub / Recall / SubBefore / SubAfter / EndTransaction
  * ``AccessChannel``      —— LOCAL / CLOUD（org.apache.rocketmq.client.AccessChannel）
  * ``TraceBean``          —— 轨迹里被追踪的那条消息
  * ``TraceContext``       —— 一次追踪上下文（Pub/SubBefore/... 各一份）
  * ``TraceTransferBean``  —— 编码结果：trans_data + trans_key
  * ``TraceDataEncoder``   —— 文本编解码（与 Java 逐字节一致，见 tests/test_trace.py 的对拍向量）

⚠ 两个必须保留的 Java 语义差异（否则编解码对不上）：
  1. ``TraceConstants.CONTENT_SPLITOR`` = \\x01、``FIELD_SPLITOR`` = \\x02，
     编码时**每条记录末尾补 FIELD_SPLITOR**；解码用 Java 的 ``String.split``
     语义（**丢弃末尾空串**）切分 —— 见 ``java_split``。
  2. Python 的 ``str.split`` 保留末尾空串，直接用它切分会多出一个空段并把
     字段整体错位。本模块一律走 ``java_split``。
"""
from __future__ import annotations

import socket
import time
from enum import Enum
from typing import List, Optional

from ..common.message_const import MessageConst
from ..common.message_type import MessageType
from ..common.mix_all import MixAll
from ..common.util_all import InnerIdGenerator, UtilAll
from ..logging import get_logger

logger = get_logger()


class TraceConstants:
    """对应 org.apache.rocketmq.client.trace.TraceConstants。"""

    GROUP_NAME_PREFIX = "_INNER_TRACE_PRODUCER"
    CONTENT_SPLITOR = "\u0001"
    FIELD_SPLITOR = "\u0002"
    TRACE_INSTANCE_NAME = "PID_CLIENT_INNER_TRACE_PRODUCER"
    TRACE_TOPIC_PREFIX = "rmq_sys_TRACE_DATA_"
    TO_PREFIX = "To_"
    FROM_PREFIX = "From_"
    END_TRANSACTION = "EndTransaction"

    ROCKETMQ_SERVICE = "rocketmq"
    ROCKETMQ_SUCCESS = "rocketmq.success"
    ROCKETMQ_TAGS = "rocketmq.tags"
    ROCKETMQ_KEYS = "rocketmq.keys"
    ROCKETMQ_STORE_HOST = "rocketmq.store_host"
    ROCKETMQ_BODY_LENGTH = "rocketmq.body_length"
    ROCKETMQ_MSG_ID = "rocketmq.mgs_id"
    ROCKETMQ_MSG_TYPE = "rocketmq.mgs_type"
    ROCKETMQ_REGION_ID = "rocketmq.region_id"
    ROCKETMQ_TRANSACTION_ID = "rocketmq.transaction_id"
    ROCKETMQ_TRANSACTION_STATE = "rocketmq.transaction_state"
    ROCKETMQ_IS_FROM_TRANSACTION_CHECK = "rocketmq.is_from_transaction_check"
    ROCKETMQ_RETRY_TIMERS = "rocketmq.retry_times"


class TraceType(Enum):
    """对应 org.apache.rocketmq.client.trace.TraceType（枚举名即线上字段第 1 段）。"""

    PUB = "Pub"
    RECALL = "Recall"
    SUB_BEFORE = "SubBefore"
    SUB_AFTER = "SubAfter"
    END_TRANSACTION = "EndTransaction"


class AccessChannel(Enum):
    """对应 org.apache.rocketmq.client.AccessChannel。

    只在 SubAfter 编码时起作用：非 CLOUD 才追加 timestamp + groupName 两段
    （Java TraceDataEncoder:208）。
    """

    LOCAL = "LOCAL"
    CLOUD = "CLOUD"


def java_split(value: str, sep: str) -> List[str]:
    """复刻 Java ``String.split``：**丢弃末尾的空串**。

    Python 的 ``str.split`` 会保留末尾空串，而编码结果末尾正好补了 FIELD_SPLITOR，
    所以直接 split 会多出一段空记录；对内容段（CONTENT_SPLITOR）同理。
    这条差异是真金白银的对拍坑，勿改成 str.split。
    """
    parts = value.split(sep)
    while parts and parts[-1] == "":
        parts.pop()
    return parts


def _field(line: List[str], index: int, default: str = "") -> str:
    """按下标取段，越界返回 ``default``。

    ⚠ 这是**有意偏离 Java** 的一处健壮性处理。Java ``decoderFromTraceDataString``
    直接 ``line[7]``，而 SubBefore 的加密串形如
    ``SubBefore\\x01ts\\x01region\\x01group\\x01reqId\\x01msgId\\x01retryTimes\\x01<keys>\\x01\\x02``：
    当被消费的消息**没有 keys** 时，``<keys>`` 为空，Java ``String.split`` 会连同末尾
    的 CONTENT_SPLITOR 一起丢弃 → 数组只剩 7 段 → ``line[7]`` 抛
    ArrayIndexOutOfBoundsException（我们的 Python 参考实现照抄后同样抛 IndexError）。
    轨迹读取方（控制台 / 本项目的验证脚本）不该因为一条无 key 的合法记录就崩掉，
    所以这里改为「缺段当空串」，其余字段顺序与 Java 逐字保持一致。
    """
    return line[index] if index < len(line) else default


def _local_address() -> str:
    """对应 Java TraceBean 静态块里的 LOCAL_ADDRESS（storeHost/clientHost 默认值）。"""
    ip = MixAll.get_ip_str()
    if UtilAll.is_ipv4(ip):
        return ip
    try:
        packed = socket.inet_pton(socket.AF_INET6, ip)
        return ":".join(packed[i:i + 2].hex() for i in range(0, 16, 2))
    except Exception:  # noqa: BLE001
        return ip


LOCAL_ADDRESS = _local_address()


class TraceBean:
    """对应 org.apache.rocketmq.client.trace.TraceBean。"""

    def __init__(self) -> None:
        self.topic: str = ""
        self.msg_id: str = ""
        self.offset_msg_id: str = ""
        self.tags: str = ""
        self.keys: str = ""
        self.store_host: str = LOCAL_ADDRESS
        self.client_host: str = LOCAL_ADDRESS
        self.store_time: int = 0
        self.retry_times: int = 0
        self.body_length: int = 0
        self.msg_type: Optional[MessageType] = None
        self.transaction_state = None          # LocalTransactionState
        self.transaction_id: Optional[str] = None
        self.from_transaction_check: bool = False


class TraceContext:
    """对应 org.apache.rocketmq.client.trace.TraceContext。

    ``request_id`` 默认取 MessageClientIDSetter.createUniqID()，与 Java 一致 ——
    SubBefore 与 SubAfter 共用一个 request_id，是控制台把一次消费前后串起来的关键。
    """

    def __init__(self) -> None:
        self.trace_type: Optional[TraceType] = None
        self.time_stamp: int = int(time.time() * 1000)
        self.region_id: str = ""
        self.region_name: str = ""
        self.group_name: str = ""
        self.cost_time: int = 0
        self.is_success: bool = True
        self.request_id: str = InnerIdGenerator.create_uniq_id()
        self.context_code: int = 0
        self.access_channel: Optional[AccessChannel] = None
        self.trace_beans: List[TraceBean] = []

    # Java 属性名 accessChannel/isSuccess 的别名，便于移植时逐字对照
    @property
    def access_channel_or_local(self) -> AccessChannel:
        return self.access_channel or AccessChannel.LOCAL

    def __repr__(self) -> str:  # 对应 Java TraceContext.toString
        beans = "".join("%s_%s_" % (b.msg_id, b.topic) for b in (self.trace_beans or []))
        return "TraceContext{%s_%s_%s_%s_%s}" % (
            self.trace_type.value if self.trace_type else "", self.group_name,
            self.region_id, self.is_success, beans)


class TraceTransferBean:
    """对应 org.apache.rocketmq.client.trace.TraceTransferBean。"""

    def __init__(self) -> None:
        self.trans_data: str = ""
        self.trans_key: set = set()


class TraceDataEncoder:
    """对应 org.apache.rocketmq.client.trace.TraceDataEncoder。

    编码结果的字段顺序**逐字节对齐 Java**，回归守卫是 tests/test_trace.py 里
    由 Java 官方实现打印出来的固定字符串。
    """

    @staticmethod
    def decoder_from_trace_data_string(trace_data: Optional[str]) -> List[TraceContext]:
        """把线上轨迹文本解回 TraceContext 列表（对应 Java decoderFromTraceDataString）。"""
        res: List[TraceContext] = []
        if not trace_data:
            return res
        for context in java_split(trace_data, TraceConstants.FIELD_SPLITOR):
            if not context:
                continue
            try:
                ctx = TraceDataEncoder._decode_context(context)
            except Exception as e:  # noqa: BLE001
                # 有意偏离 Java（Java 此处会把异常抛给调用方，整条轨迹消息全丢）：
                # 单条坏记录只跳过自己，其余记录照常解出。
                logger.warning("decode trace context failed: %s, context=%r", e, context)
                continue
            if ctx is not None:
                res.append(ctx)
        return res

    @staticmethod
    def _decode_context(context: str) -> Optional[TraceContext]:
        """解一条轨迹记录（一段 FIELD_SPLITOR 之内）。无法识别时返回 None。"""
        line = java_split(context, TraceConstants.CONTENT_SPLITOR)
        if not line:
            return None
        kind = line[0]
        if kind == TraceType.PUB.value:
            ctx = TraceContext()
            ctx.trace_type = TraceType.PUB
            ctx.time_stamp = int(line[1])
            ctx.region_id = line[2]
            ctx.group_name = line[3]
            bean = TraceBean()
            bean.topic = line[4]
            bean.msg_id = line[5]
            bean.tags = line[6]
            bean.keys = line[7]
            bean.store_host = line[8]
            bean.body_length = int(line[9])
            ctx.cost_time = int(line[10])
            bean.msg_type = MessageType(int(line[11]))
            if len(line) == 13:                                   # 老版本：无 offsetMsgId
                ctx.is_success = line[12] == "true"
            elif len(line) == 14:
                bean.offset_msg_id = line[12]
                ctx.is_success = line[13] == "true"
            if len(line) >= 15:                                   # 兼容更老版本
                bean.offset_msg_id = line[12]
                ctx.is_success = line[13] == "true"
                bean.client_host = line[14]
            ctx.trace_beans = [bean]
            return ctx
        if kind == TraceType.SUB_BEFORE.value:
            ctx = TraceContext()
            ctx.trace_type = TraceType.SUB_BEFORE
            ctx.time_stamp = int(line[1])
            ctx.region_id = line[2]
            ctx.group_name = line[3]
            ctx.request_id = line[4]
            bean = TraceBean()
            bean.msg_id = line[5]
            bean.retry_times = int(line[6])
            bean.keys = _field(line, 7)          # 无 keys 的消息会缺这段，见 _field 注释
            ctx.trace_beans = [bean]
            return ctx
        if kind == TraceType.SUB_AFTER.value:
            ctx = TraceContext()
            ctx.trace_type = TraceType.SUB_AFTER
            ctx.request_id = line[1]
            bean = TraceBean()
            bean.msg_id = line[2]
            bean.keys = line[5]
            ctx.trace_beans = [bean]
            ctx.cost_time = int(line[3])
            ctx.is_success = line[4] == "true"
            if len(line) >= 7:
                ctx.context_code = int(line[6])
            if len(line) >= 9:                                    # 兼容老版本
                ctx.time_stamp = int(line[7])
                ctx.group_name = line[8]
            return ctx
        if kind == TraceType.END_TRANSACTION.value:
            ctx = TraceContext()
            ctx.trace_type = TraceType.END_TRANSACTION
            ctx.time_stamp = int(line[1])
            ctx.region_id = line[2]
            ctx.group_name = line[3]
            bean = TraceBean()
            bean.topic = line[4]
            bean.msg_id = line[5]
            bean.tags = line[6]
            bean.keys = line[7]
            bean.store_host = line[8]
            bean.msg_type = MessageType(int(line[9]))
            bean.transaction_id = line[10]
            bean.transaction_state = line[11]
            bean.from_transaction_check = line[12] == "true"
            ctx.trace_beans = [bean]
            return ctx
        if kind == TraceType.RECALL.value:
            ctx = TraceContext()
            ctx.trace_type = TraceType.RECALL
            ctx.time_stamp = int(line[1])
            ctx.region_id = line[2]
            ctx.group_name = line[3]
            bean = TraceBean()
            bean.topic = line[4]
            bean.msg_id = line[5]
            ctx.is_success = line[6] == "true"
            ctx.trace_beans = [bean]
            return ctx
        return None

    @staticmethod
    def encoder_from_context_bean(ctx: Optional[TraceContext]) -> Optional[TraceTransferBean]:
        """把 TraceContext 编成可发送的文本（对应 Java encoderFromContextBean）。"""
        if ctx is None:
            return None
        SOH = TraceConstants.CONTENT_SPLITOR
        STX = TraceConstants.FIELD_SPLITOR
        tb = TraceTransferBean()
        sb: List[str] = []
        t = ctx.trace_type
        if t == TraceType.PUB:
            bean = ctx.trace_beans[0]
            sb += [t.value, str(ctx.time_stamp), ctx.region_id, ctx.group_name,
                   bean.topic, bean.msg_id, bean.tags, bean.keys,
                   bean.store_host, str(bean.body_length), str(ctx.cost_time),
                   str(bean.msg_type.value if bean.msg_type is not None else 0),
                   bean.offset_msg_id, str(ctx.is_success).lower()]
            tb.trans_data = SOH.join(sb) + STX
        elif t == TraceType.SUB_BEFORE:
            for bean in ctx.trace_beans:
                sb += [t.value, str(ctx.time_stamp), ctx.region_id, ctx.group_name,
                       ctx.request_id, bean.msg_id, str(bean.retry_times), bean.keys]
                tb.trans_data += SOH.join(sb) + STX
                sb = []
        elif t == TraceType.SUB_AFTER:
            for bean in ctx.trace_beans:
                sb += [t.value, ctx.request_id, bean.msg_id, str(ctx.cost_time),
                       str(ctx.is_success).lower(), bean.keys, str(ctx.context_code)]
                # Java：非 CLOUD 才补 timestamp + groupName（accessChannel 为 null 时
                # Java 会 NPE，这里按 LOCAL 处理 —— 唯一一处有意的健壮性偏离）
                if ctx.access_channel_or_local != AccessChannel.CLOUD:
                    sb += [str(ctx.time_stamp), ctx.group_name]
                tb.trans_data += SOH.join(sb) + STX
                sb = []
        elif t == TraceType.END_TRANSACTION:
            bean = ctx.trace_beans[0]
            state = bean.transaction_state
            state_name = getattr(state, "name", None) or str(state)
            sb += [t.value, str(ctx.time_stamp), ctx.region_id, ctx.group_name,
                   bean.topic, bean.msg_id, bean.tags, bean.keys, bean.store_host,
                   str(bean.msg_type.value if bean.msg_type is not None else 0),
                   bean.transaction_id or "", str(state_name),
                   str(bean.from_transaction_check).lower()]
            tb.trans_data = SOH.join(sb) + STX
        elif t == TraceType.RECALL:
            bean = ctx.trace_beans[0]
            sb += [t.value, str(ctx.time_stamp), ctx.region_id, ctx.group_name,
                   bean.topic, bean.msg_id, str(ctx.is_success).lower()]
            tb.trans_data = SOH.join(sb) + STX
        # 收集 keys：msgId + 按空格拆开的业务 keys（Java split(KEY_SEPARATOR)）
        for bean in ctx.trace_beans:
            tb.trans_key.add(bean.msg_id)
            if bean.keys:
                tb.trans_key.update(bean.keys.split(MessageConst.KEY_SEPARATOR))
        return tb


__all__ = [
    "TraceConstants", "TraceType", "AccessChannel", "TraceBean", "TraceContext",
    "TraceTransferBean", "TraceDataEncoder", "java_split", "LOCAL_ADDRESS",
]
