# -*- coding: utf-8 -*-
"""OpenTracing-style hook trio for the message lifecycle.

Port of the Java client's ``SendMessageOpenTracingHookImpl`` /
``ConsumeMessageOpenTracingHookImpl`` / ``EndTransactionOpenTracingHookImpl``
(the same trio the PHP port already carries as ``OpenTracingHook.php``), on top
of the duck-typed Tracer SPI this module defines -- zero third-party deps:

- ``Tracer`` duck-type: ``start_span(operation, tags=None, refs=None) -> Span``,
  ``extract(carrier) -> SpanContext | None`` (the carrier is the message's
  property map), ``inject(span_context, carrier) -> None`` (writes the
  propagation keys into the property map, which then travels in-band).
- ``Span`` duck-type: ``set_tag(key, value)``, ``log(fields)``, ``finish()``,
  plus ``context()``/``get_context()`` returning the SpanContext for inject.

The span rides in the context's ``mq_trace_context`` slot, exactly like the
Java original (``context.setMqTraceContext(span)``): before opens the span and
stashes it, after reads it back and closes it. The in-flight span therefore
belongs to whoever registered last on that slot -- registering both the trace
dispatcher hooks and these hooks on the same producer is not supported (same
restriction as Java).

Registration is through the ordinary hook lists, so every method really runs:
  - Producer.register_send_message_hook(SendMessageOpenTracingHook(tracer))
  - Producer.register_end_transaction_hook(EndTransactionOpenTracingHook(tracer))
  - PushConsumer.register_consume_message_hook(ConsumeMessageOpenTracingHook(tracer))
"""
from __future__ import annotations

from typing import Any, Dict, Iterable, List, Optional

from client.hook import (ConsumeMessageContext, ConsumeMessageHook,
                         EndTransactionContext, EndTransactionHook,
                         SendMessageContext, SendMessageHook)
from client.send_result import SendResult, SendStatus
from client.trace import TraceConstants
from common.message import Message, MessageExt
from common.message_const import MessageConst
from remoting.protocol.namespace_util import NamespaceUtil

# OpenTracing standard tags (io.opentracing.tag.Tags).
SPAN_KIND = "span.kind"
SPAN_KIND_PRODUCER = "producer"
SPAN_KIND_CONSUMER = "consumer"
PEER_SERVICE = "peer.service"
MESSAGE_BUS_DESTINATION = "message_bus.destination"

# The transaction leg reports the commit state only (Java MessageType
# .Trans_msg_Commit.name()).
TRANS_MSG_COMMIT = "Trans_msg_Commit"


def _msg_type_name(msg_type: Any) -> str:
    """Java parity: the tag value is the MessageType enum *name* (NORMAL_MSG...)."""
    name = getattr(msg_type, "name", None)
    return name if isinstance(name, str) else str(msg_type)


def _inject_into_message(tracer: Any, span: Any, msg: Message) -> None:
    """Injects the just-started span's context into the message properties.

    ``get_properties()`` returns the live dict, so whatever keys the tracer's
    injector writes land on the message directly; the downstream leg must
    parent onto THIS send, not onto whatever upstream produced the context.
    """
    span_context = None
    for getter in ("context", "get_context"):
        fn = getattr(span, getter, None)
        if callable(fn):
            ctx = fn()
            if ctx is not None:
                span_context = ctx
                break
    if span_context is None:
        span_context = span
    carrier = msg.get_properties()
    try:
        tracer.inject(span_context, carrier)
    except Exception:
        pass  # propagation must never break the send


def _extract_upstream(tracer: Any, props: Dict[str, str]) -> Optional[Any]:
    try:
        return tracer.extract(props)
    except Exception:
        return None


class SendMessageOpenTracingHook(SendMessageHook):
    """Send leg: span ``To_<topic>``, kind producer; tags kind + service +
    destination + tags + keys + store_host + msg_type + body_length at open;
    success / msg_id / region_id at close. No result, or no region id on it:
    the span is left open, exactly like the Java close-out flow."""

    def __init__(self, tracer: Any) -> None:
        self.tracer = tracer

    def hook_name(self) -> str:
        return "SendMessageOpenTracingHook"

    def send_message_before(self, context: SendMessageContext) -> None:
        if context is None or context.message is None:
            return
        msg = context.message
        props = msg.get_properties()
        upstream = _extract_upstream(self.tracer, props)
        tags: Dict[str, Any] = {SPAN_KIND: SPAN_KIND_PRODUCER}
        refs = [upstream] if upstream is not None else None
        span = self.tracer.start_span(
            TraceConstants.TO_PREFIX + msg.get_topic(), tags=tags, refs=refs)
        span.set_tag(PEER_SERVICE, TraceConstants.ROCKETMQ_SERVICE)
        span.set_tag(MESSAGE_BUS_DESTINATION, msg.get_topic())
        span.set_tag(TraceConstants.ROCKETMQ_TAGS, msg.get_tags() or "")
        span.set_tag(TraceConstants.ROCKETMQ_KEYS, msg.get_keys() or "")
        span.set_tag(TraceConstants.ROCKETMQ_STORE_HOST, context.broker_addr)
        span.set_tag(TraceConstants.ROCKETMQ_MSG_TYPE,
                     _msg_type_name(context.msg_type))
        span.set_tag(TraceConstants.ROCKETMQ_BODY_LENGTH,
                     len(msg.get_body()) if msg.get_body() is not None else 0)
        _inject_into_message(self.tracer, span, msg)
        context.mq_trace_context = span

    def send_message_after(self, context: SendMessageContext) -> None:
        if context is None or context.mq_trace_context is None:
            return
        send_result: Optional[SendResult] = context.send_result
        if send_result is None or send_result.region_id is None:
            return  # nothing to report; the span stays open
        span = context.mq_trace_context
        span.set_tag(TraceConstants.ROCKETMQ_SUCCESS,
                     send_result.send_status == SendStatus.SEND_OK)
        span.set_tag(TraceConstants.ROCKETMQ_MSG_ID, send_result.msg_id or "")
        span.set_tag(TraceConstants.ROCKETMQ_REGION_ID, send_result.region_id)
        span.finish()


class ConsumeMessageOpenTracingHook(ConsumeMessageHook):
    """Consume leg: one span per message, ``From_<topic>``, kind consumer;
    msg_id / tags / keys / body_length (store size) / retry_times / region_id
    at open; success at close."""

    def __init__(self, tracer: Any) -> None:
        self.tracer = tracer

    def hook_name(self) -> str:
        return "ConsumeMessageOpenTracingHook"

    def consume_message_before(self, context: ConsumeMessageContext) -> None:
        if context is None or not context.msg_list:
            return
        spans: List[Any] = []
        for msg in context.msg_list:
            if not isinstance(msg, MessageExt):
                continue
            upstream = _extract_upstream(self.tracer, msg.get_properties())
            tags: Dict[str, Any] = {SPAN_KIND: SPAN_KIND_CONSUMER}
            refs = [upstream] if upstream is not None else None
            span = self.tracer.start_span(
                TraceConstants.FROM_PREFIX + msg.get_topic(), tags=tags, refs=refs)
            span.set_tag(PEER_SERVICE, TraceConstants.ROCKETMQ_SERVICE)
            span.set_tag(MESSAGE_BUS_DESTINATION,
                         NamespaceUtil.without_namespace(msg.get_topic()))
            span.set_tag(TraceConstants.ROCKETMQ_MSG_ID, msg.get_msg_id() or "")
            span.set_tag(TraceConstants.ROCKETMQ_TAGS, msg.get_tags() or "")
            span.set_tag(TraceConstants.ROCKETMQ_KEYS, msg.get_keys() or "")
            span.set_tag(TraceConstants.ROCKETMQ_BODY_LENGTH, msg.get_store_size())
            span.set_tag(TraceConstants.ROCKETMQ_RETRY_TIMERS,
                         msg.get_reconsume_times())
            span.set_tag(TraceConstants.ROCKETMQ_REGION_ID,
                         msg.get_property(MessageConst.PROPERTY_MSG_REGION) or "")
            spans.append(span)
        context.mq_trace_context = spans

    def consume_message_after(self, context: ConsumeMessageContext) -> None:
        if context is None or not context.msg_list:
            return
        spans = context.mq_trace_context
        if not spans:
            return
        for span in spans:
            span.set_tag(TraceConstants.ROCKETMQ_SUCCESS, context.success)
            span.finish()


class EndTransactionOpenTracingHook(EndTransactionHook):
    """Transaction leg: span ``EndTransaction`` with the transaction identity
    keys, closed immediately. The message is left untouched on this leg (the
    send side already put the context on the properties)."""

    def __init__(self, tracer: Any) -> None:
        self.tracer = tracer

    def hook_name(self) -> str:
        return "EndTransactionOpenTracingHook"

    def end_transaction(self, context: EndTransactionContext) -> None:
        if context is None:
            return
        msg: Optional[Message] = context.message
        props = msg.get_properties() if msg is not None else {}
        upstream = _extract_upstream(self.tracer, props)
        tags: Dict[str, Any] = {SPAN_KIND: SPAN_KIND_PRODUCER}
        refs = [upstream] if upstream is not None else None
        span = self.tracer.start_span(TraceConstants.END_TRANSACTION,
                                      tags=tags, refs=refs)
        span.set_tag(PEER_SERVICE, TraceConstants.ROCKETMQ_SERVICE)
        span.set_tag(MESSAGE_BUS_DESTINATION, msg.get_topic() if msg else "")
        span.set_tag(TraceConstants.ROCKETMQ_TAGS,
                     (msg.get_tags() or "") if msg else "")
        span.set_tag(TraceConstants.ROCKETMQ_KEYS,
                     (msg.get_keys() or "") if msg else "")
        span.set_tag(TraceConstants.ROCKETMQ_STORE_HOST, context.broker_addr)
        span.set_tag(TraceConstants.ROCKETMQ_MSG_ID, context.msg_id or "")
        span.set_tag(TraceConstants.ROCKETMQ_MSG_TYPE, TRANS_MSG_COMMIT)
        span.set_tag(TraceConstants.ROCKETMQ_TRANSACTION_ID,
                     context.transaction_id or "")
        state = context.transaction_state
        state_name = getattr(state, "name", None)
        span.set_tag(TraceConstants.ROCKETMQ_TRANSACTION_STATE,
                     state_name if isinstance(state_name, str) else str(state or ""))
        span.set_tag(TraceConstants.ROCKETMQ_IS_FROM_TRANSACTION_CHECK,
                     bool(context.from_transaction_check))
        span.finish()


__all__ = ["SPAN_KIND", "SPAN_KIND_CONSUMER", "SPAN_KIND_PRODUCER",
           "PEER_SERVICE", "MESSAGE_BUS_DESTINATION", "TRANS_MSG_COMMIT",
           "SendMessageOpenTracingHook", "ConsumeMessageOpenTracingHook",
           "EndTransactionOpenTracingHook"]
