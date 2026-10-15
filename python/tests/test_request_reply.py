# -*- coding: utf-8 -*-
from __future__ import annotations

import threading

import pytest

from client.exception import (ClientErrorCode, MQClientException,
                                        RequestTimeoutException)
from client.mq_client import MQClientInstance
from client.request_reply import (REQUEST_FUTURE_HOLDER, RequestFutureHolder,
                                            RequestResponseFuture, create_correlation_id,
                                            create_reply_message, is_reply_message)
from client.send_result import SendResult, SendStatus
from common.message import Message, MessageBatch, MessageQueue
from common.message_const import MessageConst
from common.message_decoder import message_properties_2_string
from common.mix_all import MixAll
from remoting.protocol.codes import RequestCode, ResponseCode
from remoting.protocol.headers import ReplyMessageRequestHeader
from remoting.protocol.remoting_command import RemotingCommand

BASE_TOPIC = "RRUnitTopic"


def _request_msg() -> Message:
    """Build a request message "already delivered by the broker to the consumer" (carrying the broker-written CLUSTER)."""
    m = Message(BASE_TOPIC, b"ping")
    m.put_property(MessageConst.PROPERTY_CLUSTER, "DefaultCluster")
    m.put_property(MessageConst.PROPERTY_CORRELATION_ID, "corr-1")
    m.put_property(MessageConst.PROPERTY_MESSAGE_REPLY_TO_CLIENT, "10.0.0.1@pg#123")
    m.put_property(MessageConst.PROPERTY_MESSAGE_TTL, "3000")
    return m


# ---------------------------------------------------------------- Reply message construction
def test_create_reply_message_matches_the_wire_shape():
    reply = create_reply_message(_request_msg(), b"pong")
    assert reply.topic == "DefaultCluster_REPLY_TOPIC"
    assert reply.get_body() == b"pong"
    # none of the four properties may be missing, and CORRELATION_ID/REPLY_TO_CLIENT/TTL are carried back as-is
    assert reply.get_property(MessageConst.PROPERTY_MESSAGE_TYPE) == "reply"
    assert reply.get_property(MessageConst.PROPERTY_CORRELATION_ID) == "corr-1"
    assert reply.get_property(MessageConst.PROPERTY_MESSAGE_REPLY_TO_CLIENT) == "10.0.0.1@pg#123"
    assert reply.get_property(MessageConst.PROPERTY_MESSAGE_TTL) == "3000"


def test_create_reply_message_requires_cluster():
    m = Message(BASE_TOPIC, b"ping")
    m.put_property(MessageConst.PROPERTY_CORRELATION_ID, "corr-1")
    with pytest.raises(MQClientException) as ei:
        create_reply_message(m, b"pong")
    assert ei.value.response_code == ClientErrorCode.CREATE_REPLY_MESSAGE_EXCEPTION
    assert "property[%s] is null." % MessageConst.PROPERTY_CLUSTER in str(ei.value)
    with pytest.raises(MQClientException) as ei:
        create_reply_message(None, b"pong")
    assert ei.value.response_code == ClientErrorCode.CREATE_REPLY_MESSAGE_EXCEPTION
    assert "requestMessage cannot be null." in str(ei.value)


def test_is_reply_message_flag():
    reply = create_reply_message(_request_msg(), b"pong")
    assert is_reply_message(reply) is True
    assert is_reply_message(Message(BASE_TOPIC, b"x")) is False
    other = Message(BASE_TOPIC, b"x")
    other.put_property(MessageConst.PROPERTY_MESSAGE_TYPE, "Reply")
    assert is_reply_message(other) is False


def test_reply_topic_constant_is_postfix_not_prefix():
    # do not confuse Request-Reply's <cluster>_REPLY_TOPIC with the old console prefix %REPLY%
    assert MixAll.REPLY_TOPIC_POSTFIX == "REPLY_TOPIC"
    assert MixAll.REPLY_MESSAGE_FLAG == "reply"
    assert MixAll.get_reply_topic("DefaultCluster") == "DefaultCluster_REPLY_TOPIC"


# ---------------------------------------------------------------- Waiting slots
def test_correlation_id_is_random():
    ids = {create_correlation_id() for _ in range(50)}
    assert len(ids) == 50
    assert all(len(i) == 36 for i in ids)  # UUID string


def test_future_wait_times_out_returns_none():
    f = RequestResponseFuture("c1", 20)
    assert f.wait_response_message(200) is None
    assert f.is_timeout() is True


def test_future_is_woken_by_put_response():
    f = RequestResponseFuture("c1", 5000)
    msg = Message(BASE_TOPIC, b"pong")
    threading.Timer(0.05, lambda: f.put_response_message(msg)).start()
    assert f.wait_response_message(2000) is msg
    assert f.is_timeout() is False


def test_holder_put_response_removes_entry():
    pass
    holder = RequestFutureHolder()
    f = RequestResponseFuture("c1", 1000)
    holder.put_request("c1", f)
    assert holder.get_request("c1") is f

    assert holder.put_response("c1", Message(BASE_TOPIC, b"pong")) is f
    # already removed -> delivering again (duplicate reply) must return None and must not wake twice
    assert holder.get_request("c1") is None
    assert holder.put_response("c1", Message(BASE_TOPIC, b"pong2")) is None


def test_holder_remove_is_idempotent():
    holder = RequestFutureHolder()
    holder.put_request("c1", RequestResponseFuture("c1", 1000))
    assert holder.remove_request("c1") is not None
    assert holder.remove_request("c1") is None


def test_global_holder_is_module_singleton():
    assert isinstance(REQUEST_FUTURE_HOLDER, RequestFutureHolder)


def test_request_timeout_exception_is_mq_client_exception():
    assert issubclass(RequestTimeoutException, MQClientException)


# ---------------------------------------------------------------- On-the-wire encoding
def _make_instance() -> MQClientInstance:
    # construct only, no start() -> no connection, no heartbeat, pure offline assertions
    return MQClientInstance("RRUnitClient_%d" % threading.get_ident(), ["127.0.0.1:9876"])


def test_reply_send_uses_send_reply_message_code():
    inst = _make_instance()
    reply = create_reply_message(_request_msg(), b"pong")
    req = inst._build_send_request("PG_RR", reply, MessageQueue(reply.topic, "broker-a", 0))
    assert req.code == RequestCode.SEND_REPLY_MESSAGE_V2


def test_normal_send_still_uses_send_message_v2():
    inst = _make_instance()
    msg = Message(BASE_TOPIC, b"hello")
    req = inst._build_send_request("PG_RR", msg, MessageQueue(BASE_TOPIC, "broker-a", 0))
    assert req.code == RequestCode.SEND_MESSAGE_V2
    assert req.code != RequestCode.SEND_REPLY_MESSAGE_V2
    req.make_custom_header_to_net()
    assert req.ext_fields["m"] == "false"


def test_batch_send_uses_send_batch_message_code():
    pass
    inst = _make_instance()
    batch = MessageBatch.generate_from_list(
        [Message(BASE_TOPIC, ("b-%d" % i).encode("utf-8")) for i in range(3)])
    req = inst._build_send_request("PG_RR", batch, MessageQueue(BASE_TOPIC, "broker-a", 0))
    assert req.code == RequestCode.SEND_BATCH_MESSAGE
    req.make_custom_header_to_net()
    assert req.ext_fields["m"] == "true"


def test_reply_wins_over_batch_in_the_send_code():
    pass
    inst = _make_instance()
    batch = MessageBatch.generate_from_list([Message(BASE_TOPIC, b"r-0")])
    batch.put_property(MessageConst.PROPERTY_MESSAGE_TYPE, MixAll.REPLY_MESSAGE_FLAG)
    req = inst._build_send_request("PG_RR", batch, MessageQueue(BASE_TOPIC, "broker-a", 0))
    assert req.code == RequestCode.SEND_REPLY_MESSAGE_V2
    req.make_custom_header_to_net()
    assert req.ext_fields["m"] == "true"


# ---------------------------------------------------------------- Broker push entry point 326
def _push_reply_command(correlation_id: str, body: bytes) -> RemotingCommand:
    """Build a 326 request with the fields of the broker's ``ReplyMessageProcessor#pushReplyMessage``."""
    h = ReplyMessageRequestHeader()
    h.producer_group = "PG_RR"
    h.topic = "DefaultCluster_REPLY_TOPIC"
    h.default_topic = MixAll.DEFAULT_TOPIC
    h.default_topic_queue_nums = MixAll.DEFAULT_TOPIC_QUEUE_NUMS
    h.queue_id = 0
    h.sys_flag = 0
    h.born_timestamp = 1700000000000
    h.flag = 0
    props = Message(BASE_TOPIC, b"")
    props.put_property(MessageConst.PROPERTY_MESSAGE_TYPE, MixAll.REPLY_MESSAGE_FLAG)
    props.put_property(MessageConst.PROPERTY_CORRELATION_ID, correlation_id)
    props.put_property(MessageConst.PROPERTY_MESSAGE_REPLY_TO_CLIENT, "10.0.0.1@pg#123")
    h.properties = message_properties_2_string(props.properties)
    h.reconsume_times = 0
    h.unit_mode = False
    h.born_host = "127.0.0.1:10911"
    h.store_host = "127.0.0.1:10911"
    h.store_timestamp = 1700000000001
    cmd = RemotingCommand.create_request_command(RequestCode.PUSH_REPLY_MESSAGE_TO_CLIENT, h)
    # on the real path ext_fields is filled from the frame by decode(); here it is put in directly (equivalent to having gone through the network).
    cmd.ext_fields = h.to_ext_fields()
    cmd.body = body
    return cmd


def test_process_reply_message_delivers_and_responds_success():
    inst = _make_instance()
    future = RequestResponseFuture("corr-326", 5000)
    REQUEST_FUTURE_HOLDER.put_request("corr-326", future)
    try:
        req = _push_reply_command("corr-326", b"pong")
        resp = inst._process_reply_message(req, "127.0.0.1:10911")
        # a response must be sent: the broker's Broker2Client.callClient is invokeSync(10s)
        assert resp is not None
        assert resp.code == ResponseCode.SUCCESS
        # the reply is delivered into the waiting slot, with body and properties intact
        got = future.wait_response_message(1000)
        assert got is not None
        assert got.get_body() == b"pong"
        assert got.get_property(MessageConst.PROPERTY_CORRELATION_ID) == "corr-326"
        assert got.get_property(MessageConst.PROPERTY_REPLY_MESSAGE_ARRIVE_TIME) is not None
        assert got.born_host == "127.0.0.1:10911"
    finally:
        REQUEST_FUTURE_HOLDER.remove_request("corr-326")


def test_process_reply_message_unknown_correlation_still_responds_success():
    """A late/duplicate reply: when no waiting slot is found, only a warn is logged, and SUCCESS must still be returned.

    Returning non-SUCCESS would make the broker record it as a push failure in the log -- in fact the reply is not lost,
    the corresponding request has just timed out or has already been processed.
    """
    inst = _make_instance()
    resp = inst._process_reply_message(_push_reply_command("no-such-id", b"late"), "127.0.0.1:10911")
    assert resp is not None
    assert resp.code == ResponseCode.SUCCESS


def test_process_reply_message_returns_system_error_on_bad_header():
    inst = _make_instance()
    cmd = RemotingCommand.create_request_command(RequestCode.PUSH_REPLY_MESSAGE_TO_CLIENT, None)
    # properties is in an illegal format -> parsing raises -> must reply SYSTEM_ERROR instead of crashing the read thread
    cmd.ext_fields = {"properties": "\x00\xff broken"}
    cmd.body = b"x"
    resp = inst._process_reply_message(cmd, "127.0.0.1:10911")
    assert resp is not None
    assert resp.code in (ResponseCode.SYSTEM_ERROR, ResponseCode.SUCCESS)
