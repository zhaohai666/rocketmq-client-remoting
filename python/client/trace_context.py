# -*- coding: utf-8 -*-

from __future__ import annotations

import os
import secrets
from typing import Optional

from common.message import Message, MessageExt

# W3C key names (lowercase, consistent with OTel / HTTP headers)
TRACE_CONTEXT_PROPERTY = "traceparent"
TRACE_STATE_PROPERTY = "tracestate"

_ALL_HEX = set("0123456789abcdef")


def _hex(n: int) -> str:
    return secrets.token_hex(n // 2)


def generate_traceparent() -> str:
    """Generate a valid root traceparent: ``00-<32hex>-<16hex>-01`` (records sampling)."""
    return "00-%s-%s-01" % (_hex(32), _hex(16))


def is_valid_traceparent(value: Optional[str]) -> bool:
    """Validate per W3C syntax and the "not all zeros" rule. Loosely accept uppercase hex (forwarding does not rewrite)."""
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
    """Generate a child span under the same trace-id (swap parent-id); return None if parent is invalid."""
    if not is_valid_traceparent(parent):
        return None
    parts = parent.strip().split("-")
    return "00-%s-%s-01" % (parts[1].lower(), _hex(16))


def inject_trace_context(message: Message) -> str:
    """Inject the root context when the message has no traceparent property; return the (injected) value.

    An existing value is **not overwritten** -- context propagated from upstream takes priority
    (aligning with common tracing conventions).
    """
    existing = message.get_property(TRACE_CONTEXT_PROPERTY)
    if existing:
        return existing
    tp = generate_traceparent()
    message.put_property(TRACE_CONTEXT_PROPERTY, tp)
    return tp


def extract_traceparent(msg: MessageExt) -> Optional[str]:
    """Read traceparent from the message property (return None if not injected / empty)."""
    return msg.get_property(TRACE_CONTEXT_PROPERTY) or None


def trace_context_enabled_from_env() -> bool:
    """env `ROCKETMQ_TRACE_CONTEXT_ENABLE` (aligning with the env convention of other switches)."""
    return os.environ.get("ROCKETMQ_TRACE_CONTEXT_ENABLE", "").strip().lower() in (
        "1", "true", "yes")
