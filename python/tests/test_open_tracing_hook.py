# -*- coding: utf-8 -*-
"""Offline tests for the OpenTracing-style hook trio (Java parity).

Mirrors php/tests/RunClientOpenTracing.php: a recording fake Tracer asserts the
span names, tag sets, close-out rules and propagation write-back for the send /
consume / end-transaction legs, plus the injection write-back into the message
properties.
"""
from __future__ import annotations

import pytest

from client.hook import (ConsumeMessageContext, EndTransactionContext,
                         SendMessageContext)
from client.open_tracing_hook import (MESSAGE_BUS_DESTINATION, PEER_SERVICE,
                                      SPAN_KIND, SPAN_KIND_CONSUMER,
                                      SPAN_KIND_PRODUCER, TRANS_MSG_COMMIT,
                                      ConsumeMessageOpenTracingHook,
                                      EndTransactionOpenTracingHook,
                                      SendMessageOpenTracingHook)
from client.producer import LocalTransactionState
from client.send_result import SendResult, SendStatus
from client.trace import TraceConstants
from common.message import Message, MessageExt
from common.message_const import MessageConst
from common.message_type import MessageType


class FakeSpan:
    def __init__(self, tracer, operation, tags, refs):
        self.tracer = tracer
        self.operation = operation
        self.tags = dict(tags)
        self.refs = list(refs or [])
        self.finished = False
        self.context_obj = object()

    def context(self):
        return self.context_obj

    def set_tag(self, key, value):
        self.tags[key] = value

    def finish(self):
        self.finished = True


class FakeTracer:
    """Records every span; inject writes a marker key into the carrier."""

    INJECT_KEY = "traceparent"

    def __init__(self, upstream=None):
        self.spans = []
        self.upstream = upstream  # returned by extract, or None
        self.injected = []

    def start_span(self, operation, tags=None, refs=None):
        span = FakeSpan(self, operation, tags or {}, refs)
        self.spans.append(span)
        return span

    def extract(self, carrier):
        if self.upstream is not None and carrier is not None:
            return self.upstream
        return None

    def inject(self, span_context, carrier):
        carrier[self.INJECT_KEY] = "fake-trace-context"
        self.injected.append(span_context)


def make_send_context(msg_type=MessageType.NORMAL_MSG):
    msg = Message("TopicTest", b"hello-world", tags="TagA", keys="KeyA")
    ctx = SendMessageContext()
    ctx.message = msg
    ctx.broker_addr = "127.0.0.1:10911"
    ctx.msg_type = msg_type
    return ctx, msg


# ---------------------------------------------------------------- send leg
def test_send_before_opens_producer_span_with_documented_tags():
    tracer = FakeTracer()
    hook = SendMessageOpenTracingHook(tracer)
    ctx, msg = make_send_context()
    hook.send_message_before(ctx)

    assert len(tracer.spans) == 1
    span = tracer.spans[0]
    assert span.operation == "To_TopicTest"
    assert span.tags[SPAN_KIND] == SPAN_KIND_PRODUCER
    assert span.tags[PEER_SERVICE] == "rocketmq"
    assert span.tags[MESSAGE_BUS_DESTINATION] == "TopicTest"
    assert span.tags[TraceConstants.ROCKETMQ_TAGS] == "TagA"
    assert span.tags[TraceConstants.ROCKETMQ_KEYS] == "KeyA"
    assert span.tags[TraceConstants.ROCKETMQ_STORE_HOST] == "127.0.0.1:10911"
    assert span.tags[TraceConstants.ROCKETMQ_MSG_TYPE] == "NORMAL_MSG"
    assert span.tags[TraceConstants.ROCKETMQ_BODY_LENGTH] == len(b"hello-world")
    # the span rides in the mq_trace_context slot (Java setMqTraceContext)
    assert ctx.mq_trace_context is span
    assert not span.finished


def test_send_before_injects_span_context_into_message_properties():
    tracer = FakeTracer()
    hook = SendMessageOpenTracingHook(tracer)
    ctx, msg = make_send_context()
    hook.send_message_before(ctx)

    assert msg.get_properties().get(FakeTracer.INJECT_KEY) == "fake-trace-context"
    assert tracer.injected == [tracer.spans[0].context_obj]


def test_send_before_uses_upstream_context_as_ref():
    upstream = object()
    tracer = FakeTracer(upstream=upstream)
    hook = SendMessageOpenTracingHook(tracer)
    ctx, _ = make_send_context()
    hook.send_message_before(ctx)
    assert tracer.spans[0].refs == [upstream]


def test_send_msg_type_tag_uses_enum_name():
    tracer = FakeTracer()
    hook = SendMessageOpenTracingHook(tracer)
    ctx, _ = make_send_context(msg_type=MessageType.TRANS_MSG_HALF)
    hook.send_message_before(ctx)
    assert tracer.spans[0].tags[TraceConstants.ROCKETMQ_MSG_TYPE] == "TRANS_MSG_HALF"


def test_send_after_closes_with_success_msg_id_region_id():
    tracer = FakeTracer()
    hook = SendMessageOpenTracingHook(tracer)
    ctx, _ = make_send_context()
    hook.send_message_before(ctx)
    ctx.send_result = SendResult(SendStatus.SEND_OK, "MSG-ID-1", region_id="DefaultRegion")
    hook.send_message_after(ctx)

    span = tracer.spans[0]
    assert span.tags[TraceConstants.ROCKETMQ_SUCCESS] is True
    assert span.tags[TraceConstants.ROCKETMQ_MSG_ID] == "MSG-ID-1"
    assert span.tags[TraceConstants.ROCKETMQ_REGION_ID] == "DefaultRegion"
    assert span.finished


def test_send_after_failure_reports_success_false():
    tracer = FakeTracer()
    hook = SendMessageOpenTracingHook(tracer)
    ctx, _ = make_send_context()
    hook.send_message_before(ctx)
    ctx.send_result = SendResult(SendStatus.FLUSH_DISK_TIMEOUT, "MSG-ID-1",
                                 region_id="DefaultRegion")
    hook.send_message_after(ctx)
    assert tracer.spans[0].tags[TraceConstants.ROCKETMQ_SUCCESS] is False
    assert tracer.spans[0].finished


def test_send_after_no_result_leaves_span_open():
    tracer = FakeTracer()
    hook = SendMessageOpenTracingHook(tracer)
    ctx, _ = make_send_context()
    hook.send_message_before(ctx)
    hook.send_message_after(ctx)  # no send result at all
    assert not tracer.spans[0].finished


def test_send_after_missing_region_id_leaves_span_open():
    tracer = FakeTracer()
    hook = SendMessageOpenTracingHook(tracer)
    ctx, _ = make_send_context()
    hook.send_message_before(ctx)
    ctx.send_result = SendResult(SendStatus.SEND_OK, "MSG-ID-1", region_id=None)
    hook.send_message_after(ctx)
    assert not tracer.spans[0].finished


def test_send_after_without_before_is_noop():
    tracer = FakeTracer()
    hook = SendMessageOpenTracingHook(tracer)
    ctx, _ = make_send_context()
    ctx.send_result = SendResult(SendStatus.SEND_OK, "M", region_id="R")
    hook.send_message_after(ctx)  # no span stashed: nothing happens
    assert tracer.spans == []


# ---------------------------------------------------------------- consume leg
def make_ext(topic="TopicTest", store_size=42, retry=2, region="DefaultRegion"):
    msg = MessageExt()
    msg.set_topic(topic)
    msg.set_body(b"x" * 10)
    msg.msg_id = "EXT-MSG-ID"
    msg.set_store_size(store_size)
    msg.set_reconsume_times(retry)
    msg.put_property(MessageConst.PROPERTY_MSG_REGION, region)
    msg.put_property("TAGS", "TagC")
    msg.put_property("KEYS", "KeyC")
    return msg


def test_consume_before_opens_one_span_per_message():
    tracer = FakeTracer()
    hook = ConsumeMessageOpenTracingHook(tracer)
    ctx = ConsumeMessageContext("CID_test", [make_ext(), make_ext()])
    hook.consume_message_before(ctx)

    assert len(tracer.spans) == 2
    for span in tracer.spans:
        assert span.operation == "From_TopicTest"
        assert span.tags[SPAN_KIND] == SPAN_KIND_CONSUMER
        assert span.tags[PEER_SERVICE] == "rocketmq"
        assert span.tags[MESSAGE_BUS_DESTINATION] == "TopicTest"
        assert span.tags[TraceConstants.ROCKETMQ_MSG_ID] == "EXT-MSG-ID"
        assert span.tags[TraceConstants.ROCKETMQ_TAGS] == "TagC"
        assert span.tags[TraceConstants.ROCKETMQ_KEYS] == "KeyC"
        assert span.tags[TraceConstants.ROCKETMQ_BODY_LENGTH] == 42
        assert span.tags[TraceConstants.ROCKETMQ_RETRY_TIMERS] == 2
        assert span.tags[TraceConstants.ROCKETMQ_REGION_ID] == "DefaultRegion"
        assert not span.finished
    assert ctx.mq_trace_context == tracer.spans


def test_consume_after_tags_success_and_finishes_all():
    tracer = FakeTracer()
    hook = ConsumeMessageOpenTracingHook(tracer)
    ctx = ConsumeMessageContext("CID_test", [make_ext()])
    hook.consume_message_before(ctx)
    ctx.success = False
    hook.consume_message_after(ctx)
    assert tracer.spans[0].tags[TraceConstants.ROCKETMQ_SUCCESS] is False
    assert tracer.spans[0].finished


def test_consume_empty_msg_list_is_noop():
    tracer = FakeTracer()
    hook = ConsumeMessageOpenTracingHook(tracer)
    ctx = ConsumeMessageContext("CID_test", [])
    hook.consume_message_before(ctx)
    hook.consume_message_after(ctx)
    assert tracer.spans == []
    assert ctx.mq_trace_context is None


# ---------------------------------------------------------------- transaction leg
def test_end_transaction_opens_and_closes_immediately():
    tracer = FakeTracer()
    hook = EndTransactionOpenTracingHook(tracer)
    msg = Message("TopicTest", b"tx", tags="TagT", keys="KeyT")
    ctx = EndTransactionContext()
    ctx.message = msg
    ctx.broker_addr = "127.0.0.1:10911"
    ctx.msg_id = "TX-MSG-ID"
    ctx.transaction_id = "TX-ID-9"
    ctx.transaction_state = LocalTransactionState.COMMIT_MESSAGE
    ctx.from_transaction_check = True
    hook.end_transaction(ctx)

    assert len(tracer.spans) == 1
    span = tracer.spans[0]
    assert span.operation == "EndTransaction"
    assert span.tags[SPAN_KIND] == SPAN_KIND_PRODUCER
    assert span.tags[MESSAGE_BUS_DESTINATION] == "TopicTest"
    assert span.tags[TraceConstants.ROCKETMQ_TAGS] == "TagT"
    assert span.tags[TraceConstants.ROCKETMQ_KEYS] == "KeyT"
    assert span.tags[TraceConstants.ROCKETMQ_STORE_HOST] == "127.0.0.1:10911"
    assert span.tags[TraceConstants.ROCKETMQ_MSG_ID] == "TX-MSG-ID"
    assert span.tags[TraceConstants.ROCKETMQ_MSG_TYPE] == TRANS_MSG_COMMIT
    assert span.tags[TraceConstants.ROCKETMQ_TRANSACTION_ID] == "TX-ID-9"
    assert span.tags[TraceConstants.ROCKETMQ_TRANSACTION_STATE] == "COMMIT_MESSAGE"
    assert span.tags[TraceConstants.ROCKETMQ_IS_FROM_TRANSACTION_CHECK] is True
    assert span.finished


def test_end_transaction_without_message_still_opens_span():
    tracer = FakeTracer()
    hook = EndTransactionOpenTracingHook(tracer)
    ctx = EndTransactionContext()
    ctx.msg_id = "TX-MSG-ID"
    ctx.transaction_state = LocalTransactionState.ROLLBACK_MESSAGE
    hook.end_transaction(ctx)
    span = tracer.spans[0]
    assert span.tags[MESSAGE_BUS_DESTINATION] == ""
    assert span.tags[TraceConstants.ROCKETMQ_TRANSACTION_STATE] == "ROLLBACK_MESSAGE"
    assert span.finished


def test_hook_names_match_java_impls():
    assert SendMessageOpenTracingHook(None).hook_name() == "SendMessageOpenTracingHook"
    assert ConsumeMessageOpenTracingHook(None).hook_name() == "ConsumeMessageOpenTracingHook"
    assert EndTransactionOpenTracingHook(None).hook_name() == "EndTransactionOpenTracingHook"


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-q"]))
