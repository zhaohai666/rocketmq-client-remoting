# -*- coding: utf-8 -*-
"""Java ``MessageClientExt#getMsgId()`` parity on consumed messages.

Java client-side decoding produces ``MessageClientExt``; its ``getMsgId()``
returns the ``UNIQ_KEY`` property and only falls back to the broker offset id.
The port keeps ``msg.msg_id`` as the offset id, so every Java call site that
uses ``msg.getMsgId()`` on a *consumed* message must go through
``client_msg_id()``:

* ``ConsumeMessageTraceHookImpl:70`` -> SubBefore/SubAfter bean.msgId
* ``MQClientAPIImpl:1691``           -> CONSUMER_SEND_MSG_BACK.originMsgId
* ``DefaultMQPushConsumerImpl:792``  -> fallback ORIGIN_MESSAGE_ID property
"""
from __future__ import annotations

from client.consumer import _build_retry_message
from client.hook import ConsumeMessageContext
from client.trace import TraceType
from client.trace_hook import ConsumeMessageTraceHook
from common.message import MessageExt, MessageQueue
from common.message_client_id_setter import client_msg_id
from common.message_const import MessageConst

OFFSET_ID = "0A0B0C0D00002A9F0000000000000EA7"
UNIQ_ID = "AC1E0A0B2C3D4E5F60718293A4B5C6D7"


def consumed(with_uniq=True):
    msg = MessageExt("TopicTest", b"body")
    msg.msg_id = OFFSET_ID
    msg.queue_id = 0
    msg.broker_name = "broker-a"
    msg.reconsume_times = 1
    msg.store_timestamp = 1700000000000
    msg.store_size = 42
    if with_uniq:
        msg.put_property(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX, UNIQ_ID)
    return msg


class CapturingDispatcher:
    def __init__(self):
        self.appended = []

    def append(self, ctx):
        self.appended.append(ctx)


def test_client_msg_id_prefers_uniq_key_and_falls_back_to_offset_id():
    assert client_msg_id(consumed(True)) == UNIQ_ID
    assert client_msg_id(consumed(False)) == OFFSET_ID
    assert client_msg_id(None) == ""


def test_consume_trace_beans_carry_the_uniq_key():
    d = CapturingDispatcher()
    hook = ConsumeMessageTraceHook(d)
    ctx = ConsumeMessageContext("CID_test", [consumed(True)],
                                MessageQueue("TopicTest", "broker-a", 0))
    ctx.props = {"ConsumeContextType": "SUCCESS"}
    ctx.success = True

    hook.consume_message_before(ctx)
    hook.consume_message_after(ctx)

    assert [c.trace_type for c in d.appended] == [TraceType.SUB_BEFORE, TraceType.SUB_AFTER]
    assert d.appended[0].trace_beans[0].msg_id == UNIQ_ID
    assert d.appended[1].trace_beans[0].msg_id == UNIQ_ID
    assert d.appended[1].request_id == d.appended[0].request_id


def test_consume_trace_falls_back_to_offset_id_without_uniq_key():
    d = CapturingDispatcher()
    hook = ConsumeMessageTraceHook(d)
    ctx = ConsumeMessageContext("CID_test", [consumed(False)],
                                MessageQueue("TopicTest", "broker-a", 0))

    hook.consume_message_before(ctx)

    assert d.appended[0].trace_beans[0].msg_id == OFFSET_ID


def test_retry_message_origin_id_is_the_uniq_key():
    copy = _build_retry_message(consumed(True), "GID_test", 16)

    assert copy.properties[MessageConst.PROPERTY_ORIGIN_MESSAGE_ID] == UNIQ_ID
    assert copy.properties[MessageConst.PROPERTY_ORIGIN_MESSAGE_ID] != OFFSET_ID
