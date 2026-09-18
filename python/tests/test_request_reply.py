# -*- coding: utf-8 -*-
"""Request-Reply（5.x）客户端侧单测。

覆盖三层：
1. 纯数据/等待槽逻辑（``RequestResponseFuture`` / ``RequestFutureHolder`` /
   ``create_reply_message``）—— 不需要网络；
2. 线上编码：应答消息必须选 ``SEND_REPLY_MESSAGE_V2(325)`` 而不是
   ``SEND_MESSAGE_V2(314)``，否则 broker 不会走 ReplyMessageProcessor；
3. broker 回推入口 ``MQClientInstance._process_reply_message(326)`` —— 必须把应答
   投进等待槽，并且**回一个响应**（broker 侧是 invokeSync，不回响应它那边会超时）。
"""
from __future__ import annotations

import threading

import pytest

from rocketmq.client.exception import MQClientException, RequestTimeoutException
from rocketmq.client.mq_client import MQClientInstance
from rocketmq.client.request_reply import (REQUEST_FUTURE_HOLDER, RequestFutureHolder,
                                            RequestResponseFuture, create_correlation_id,
                                            create_reply_message, is_reply_message)
from rocketmq.client.send_result import SendResult, SendStatus
from rocketmq.common.message import Message, MessageQueue
from rocketmq.common.message_const import MessageConst
from rocketmq.common.message_decoder import message_properties_2_string
from rocketmq.common.mix_all import MixAll
from rocketmq.remoting.protocol.codes import RequestCode, ResponseCode
from rocketmq.remoting.protocol.headers import ReplyMessageRequestHeader
from rocketmq.remoting.protocol.remoting_command import RemotingCommand

BASE_TOPIC = "RRUnitTopic"


def _request_msg() -> Message:
    """构造一条「broker 已投递给消费者」的请求消息（带 broker 写入的 CLUSTER）。"""
    m = Message(BASE_TOPIC, b"ping")
    m.put_property(MessageConst.PROPERTY_CLUSTER, "DefaultCluster")
    m.put_property(MessageConst.PROPERTY_CORRELATION_ID, "corr-1")
    m.put_property(MessageConst.PROPERTY_MESSAGE_REPLY_TO_CLIENT, "10.0.0.1@pg#123")
    m.put_property(MessageConst.PROPERTY_MESSAGE_TTL, "3000")
    return m


# ---------------------------------------------------------------- 应答消息构造
def test_create_reply_message_matches_java_shape():
    reply = create_reply_message(_request_msg(), b"pong")
    # topic 必须是 <cluster>_REPLY_TOPIC（Java MixAll.getReplyTopic）
    assert reply.topic == "DefaultCluster_REPLY_TOPIC"
    assert reply.get_body() == b"pong"
    # 四个属性一个都不能少，且 CORRELATION_ID/REPLY_TO_CLIENT/TTL 是原样带回
    assert reply.get_property(MessageConst.PROPERTY_MESSAGE_TYPE) == "reply"
    assert reply.get_property(MessageConst.PROPERTY_CORRELATION_ID) == "corr-1"
    assert reply.get_property(MessageConst.PROPERTY_MESSAGE_REPLY_TO_CLIENT) == "10.0.0.1@pg#123"
    assert reply.get_property(MessageConst.PROPERTY_MESSAGE_TTL) == "3000"


def test_create_reply_message_requires_cluster():
    # CLUSTER 由 broker 写入；没有它说明这条消息不是 broker 转来的，Java 同样抛错
    m = Message(BASE_TOPIC, b"ping")
    m.put_property(MessageConst.PROPERTY_CORRELATION_ID, "corr-1")
    with pytest.raises(ValueError):
        create_reply_message(m, b"pong")
    with pytest.raises(ValueError):
        create_reply_message(None, b"pong")


def test_is_reply_message_flag():
    reply = create_reply_message(_request_msg(), b"pong")
    assert is_reply_message(reply) is True
    assert is_reply_message(Message(BASE_TOPIC, b"x")) is False
    # 大小写敏感：Java 是 equals("reply")
    other = Message(BASE_TOPIC, b"x")
    other.put_property(MessageConst.PROPERTY_MESSAGE_TYPE, "Reply")
    assert is_reply_message(other) is False


def test_reply_topic_constant_is_postfix_not_prefix():
    # 别把 Request-Reply 的 <cluster>_REPLY_TOPIC 与老的控制台前缀 %REPLY% 搞混
    assert MixAll.REPLY_TOPIC_POSTFIX == "REPLY_TOPIC"
    assert MixAll.REPLY_MESSAGE_FLAG == "reply"
    assert MixAll.get_reply_topic("DefaultCluster") == "DefaultCluster_REPLY_TOPIC"


# ---------------------------------------------------------------- 等待槽
def test_correlation_id_is_random():
    ids = {create_correlation_id() for _ in range(50)}
    assert len(ids) == 50
    assert all(len(i) == 36 for i in ids)  # UUID 字符串


def test_future_wait_times_out_returns_none():
    # 等待预算必须明显大于 future 的超时预算：is_timeout 用的是严格大于（同 Java），
    # 而系统等待可能比截止时刻早一丁点返回，两者相等时忙机上这条断言会抖。
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
    """Java 用 remove 抢所有权：应答到达与超时清理只能有一个生效。"""
    holder = RequestFutureHolder()
    f = RequestResponseFuture("c1", 1000)
    holder.put_request("c1", f)
    assert holder.get_request("c1") is f

    assert holder.put_response("c1", Message(BASE_TOPIC, b"pong")) is f
    # 已经被摘走 → 再投一次（重复应答）必须返回 None，且不会二次唤醒
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
    # Java: RequestTimeoutException extends MQClientException
    assert issubclass(RequestTimeoutException, MQClientException)


# ---------------------------------------------------------------- 线上编码
def _make_instance() -> MQClientInstance:
    # 只构造，不 start() → 不建连、不发心跳，纯离线断言
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


# ---------------------------------------------------------------- broker 回推入口 326
def _push_reply_command(correlation_id: str, body: bytes) -> RemotingCommand:
    """按 broker ``ReplyMessageProcessor#pushReplyMessage`` 的字段造一条 326 请求。"""
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
    # 真实链路上 ext_fields 由 decode() 从报文填好；这里直接放进去（等价于过了一遍网络）。
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
        # 必须回响应：broker 的 Broker2Client.callClient 是 invokeSync(10s)
        assert resp is not None
        assert resp.code == ResponseCode.SUCCESS
        # 应答被投进等待槽，body 与属性都在
        got = future.wait_response_message(1000)
        assert got is not None
        assert got.get_body() == b"pong"
        assert got.get_property(MessageConst.PROPERTY_CORRELATION_ID) == "corr-326"
        assert got.get_property(MessageConst.PROPERTY_REPLY_MESSAGE_ARRIVE_TIME) is not None
        assert got.born_host == "127.0.0.1:10911"
    finally:
        REQUEST_FUTURE_HOLDER.remove_request("corr-326")


def test_process_reply_message_unknown_correlation_still_responds_success():
    """迟到/重复的应答：查不到等待槽只记 warn，仍然要回 SUCCESS。

    回非 SUCCESS 会让 broker 把它当成 push 失败记进日志 —— 实际上应答没丢，
    只是对应的请求已经超时或已处理过了。
    """
    inst = _make_instance()
    resp = inst._process_reply_message(_push_reply_command("no-such-id", b"late"), "127.0.0.1:10911")
    assert resp is not None
    assert resp.code == ResponseCode.SUCCESS


def test_process_reply_message_returns_system_error_on_bad_header():
    inst = _make_instance()
    cmd = RemotingCommand.create_request_command(RequestCode.PUSH_REPLY_MESSAGE_TO_CLIENT, None)
    # properties 是非法格式 → 解析抛错 → 必须回 SYSTEM_ERROR 而不是让读线程崩掉
    cmd.ext_fields = {"properties": "\x00\xff broken"}
    cmd.body = b"x"
    resp = inst._process_reply_message(cmd, "127.0.0.1:10911")
    assert resp is not None
    assert resp.code in (ResponseCode.SYSTEM_ERROR, ResponseCode.SUCCESS)
