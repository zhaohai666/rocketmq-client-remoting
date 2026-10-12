# -*- coding: utf-8 -*-
from __future__ import annotations

import socket
import time
from enum import Enum
from typing import List, Optional

from common.message_const import MessageConst
from common.message_type import MessageType
from common.mix_all import MixAll
from common.util_all import InnerIdGenerator, UtilAll
from rocketmq_logging import get_logger

logger = get_logger()


class TraceConstants:
    pass

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
    pass

    PUB = "Pub"
    RECALL = "Recall"
    SUB_BEFORE = "SubBefore"
    SUB_AFTER = "SubAfter"
    END_TRANSACTION = "EndTransaction"


class AccessChannel(Enum):
    pass

    LOCAL = "LOCAL"
    CLOUD = "CLOUD"


def split_dropping_trailing_empty(value: str, sep: str) -> List[str]:
    pass
    parts = value.split(sep)
    while parts and parts[-1] == "":
        parts.pop()
    return parts


def _field(line: List[str], index: int, default: str = "") -> str:
    pass
    return line[index] if index < len(line) else default


def _local_address() -> str:
    pass
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
    pass

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
    pass

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

    @property
    def access_channel_or_local(self) -> AccessChannel:
        return self.access_channel or AccessChannel.LOCAL

    def __repr__(self) -> str:
        beans = "".join("%s_%s_" % (b.msg_id, b.topic) for b in (self.trace_beans or []))
        return "TraceContext{%s_%s_%s_%s_%s}" % (
            self.trace_type.value if self.trace_type else "", self.group_name,
            self.region_id, self.is_success, beans)


class TraceTransferBean:
    pass

    def __init__(self) -> None:
        self.trans_data: str = ""
        self.trans_key: set = set()


class TraceDataEncoder:
    pass

    @staticmethod
    def decoder_from_trace_data_string(trace_data: Optional[str]) -> List[TraceContext]:
        pass
        res: List[TraceContext] = []
        if not trace_data:
            return res
        for context in split_dropping_trailing_empty(trace_data, TraceConstants.FIELD_SPLITOR):
            if not context:
                continue
            try:
                ctx = TraceDataEncoder._decode_context(context)
            except Exception as e:  # noqa: BLE001
                logger.warning("decode trace context failed: %s, context=%r", e, context)
                continue
            if ctx is not None:
                res.append(ctx)
        return res

    @staticmethod
    def _decode_context(context: str) -> Optional[TraceContext]:
        """解一条轨迹记录（一段 FIELD_SPLITOR 之内）。无法识别时返回 None。"""
        line = split_dropping_trailing_empty(context, TraceConstants.CONTENT_SPLITOR)
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
        pass
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
        for bean in ctx.trace_beans:
            tb.trans_key.add(bean.msg_id)
            if bean.keys:
                tb.trans_key.update(bean.keys.split(MessageConst.KEY_SEPARATOR))
        return tb


__all__ = [
    "TraceConstants", "TraceType", "AccessChannel", "TraceBean", "TraceContext",
    "TraceTransferBean", "TraceDataEncoder", "split_dropping_trailing_empty", "LOCAL_ADDRESS",
]
