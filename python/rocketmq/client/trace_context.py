# -*- coding: utf-8 -*-
"""W3C Trace Context（traceparent）透传（OpenTracing/OTel 场景的消息级上下文）。

格式（W3C Trace Context，https://www.w3.org/TR/trace-context/）::

    traceparent: 00-<trace-id 32hex>-<parent-id 16hex>-<flags 2hex>

* trace-id / parent-id 不能全 0；version 00 时 flags 任意两位 hex；
* 生产侧：消息没有 `traceparent` 属性时注入一个根 span 上下文（opt-in，
  ``enable_trace_context=True``）——Java 客户端把这类注入交给外部链路追踪的
  SendMessageHook（SkyWalking/OTel），本实现内建等价能力，键名沿用 W3C 的小写
  ``traceparent``；已有值**不覆盖**（调用方传播的上下文优先）；
* 消费侧：从 ``MessageExt.properties`` 里取出，供业务做父子 span 关联。
"""

from __future__ import annotations

import os
import secrets
from typing import Optional

from ..common.message import Message, MessageExt

# W3C 键名（小写，与 OTel / HTTP 头一致）
TRACE_CONTEXT_PROPERTY = "traceparent"
TRACE_STATE_PROPERTY = "tracestate"

_ALL_HEX = set("0123456789abcdef")


def _hex(n: int) -> str:
    return secrets.token_hex(n // 2)


def generate_traceparent() -> str:
    """生成合法的根 traceparent：``00-<32hex>-<16hex>-01``（记录采样）。"""
    return "00-%s-%s-01" % (_hex(32), _hex(16))


def is_valid_traceparent(value: Optional[str]) -> bool:
    """按 W3C 语法与"不全 0"规则校验。宽松接受大写 hex（转发不重写）。"""
    if not value:
        return False
    parts = value.strip().split("-")
    if len(parts) != 4:
        return False
    version, trace_id, parent_id, flags = parts
    if version != "00" and not (len(version) == 2 and all(c in _ALL_HEX for c in version.lower())):
        return False
    if version == "ff":
        return False
    if len(trace_id) != 32 or len(parent_id) != 16 or len(flags) != 2:
        return False
    for part, disallow_zero in ((trace_id, True), (parent_id, True), (flags, False)):
        low = part.lower()
        if not all(c in _ALL_HEX for c in low):
            return False
        if disallow_zero and low == "0" * len(low):
            return False
    return True


def child_traceparent(parent: Optional[str]) -> Optional[str]:
    """同一 trace-id 下生成子 span（换 parent-id）；parent 非法返回 None。"""
    if not is_valid_traceparent(parent):
        return None
    parts = parent.strip().split("-")
    return "00-%s-%s-01" % (parts[1].lower(), _hex(16))


def inject_trace_context(message: Message) -> str:
    """消息没有 traceparent 属性时注入根上下文；返回（注入后的）值。

    已有值**不覆盖**——上游传播进来的上下文优先（对齐链路追踪的通用约定）。
    """
    existing = message.get_property(TRACE_CONTEXT_PROPERTY)
    if existing:
        return existing
    tp = generate_traceparent()
    message.put_property(TRACE_CONTEXT_PROPERTY, tp)
    return tp


def extract_traceparent(msg: MessageExt) -> Optional[str]:
    """从消息属性里取出 traceparent（未注入/为空返回 None）。"""
    return msg.get_property(TRACE_CONTEXT_PROPERTY) or None


def trace_context_enabled_from_env() -> bool:
    """env `ROCKETMQ_TRACE_CONTEXT_ENABLE`（对齐其它开关的 env 惯例）。"""
    return os.environ.get("ROCKETMQ_TRACE_CONTEXT_ENABLE", "").strip().lower() in (
        "1", "true", "yes")
