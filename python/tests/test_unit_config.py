# -*- coding: utf-8 -*-
from __future__ import annotations

import threading

import pytest

from client.consumer import (DefaultLitePullConsumer, DefaultMQPullConsumer,
                                      DefaultMQPushConsumer, filter_messages_for_delivery)
from client.hook import (CheckForbiddenContext, CheckForbiddenHook,
                                 FilterMessageContext, FilterMessageHook)
from client.mq_client import MQClientInstance
from client.producer import DefaultMQProducer
from common.message import Message, MessageExt, MessageQueue
from common.mix_all import MixAll
from remoting.protocol.headers import SendMessageRequestHeaderV2
from remoting.rpchook import RPCHook, StreamTypeRPCHook

MQ = MessageQueue("T", "broker-a", 0)


def _new_instance(prefix: str, **kwargs) -> MQClientInstance:
    """只构造、不 start() 的实例：不建连、不发心跳，纯离线断言。

    ``MQClientInstance.__init__`` 会写 ``INSTANCE_MAP[client_id]``，所以用完要摘掉，
    否则同进程后续用例可能拿到这个假实例。
    """
    inst = MQClientInstance("%s_%d" % (prefix, threading.get_ident()),
                            ["127.0.0.1:9876"], **kwargs)
    return inst


def _drop(inst: MQClientInstance) -> None:
    MQClientInstance.INSTANCE_MAP.pop(inst.client_id, None)


# ---------------------------------------------------------------- StreamTypeRPCHook
def test_stream_hook_only_adds_the_req_t_ext_field():
    pass
    from remoting.protocol.remoting_command import RemotingCommand
    req = RemotingCommand.create_request_command(SendMessageRequestHeaderV2())
    StreamTypeRPCHook().do_before_request("127.0.0.1:9876", req)
    assert MixAll.REQ_T == "ReqT"
    assert req.ext_fields[MixAll.REQ_T] == "0"


def test_stream_hook_is_registered_before_the_user_hook():
    pass
    inst = _new_instance("stream_order", enable_stream_request_type=True)
    try:
        user = RPCHook()
        inst.remoting_client.register_rpc_hook(user)
        kinds = [type(h) for h in inst.remoting_client.rpc_hooks]
        assert kinds.index(StreamTypeRPCHook) < kinds.index(RPCHook), kinds
    finally:
        _drop(inst)


def test_no_stream_hook_when_disabled():
    inst = _new_instance("no_stream")
    try:
        assert [h for h in inst.remoting_client.rpc_hooks
                if isinstance(h, StreamTypeRPCHook)] == []
    finally:
        _drop(inst)


@pytest.mark.parametrize("factory, expected", [
    (lambda: DefaultMQProducer("PID_stream_default"), False),
    (lambda: DefaultMQPushConsumer("CID_stream_default"), False),
    (lambda: DefaultMQPullConsumer("CID_stream_default"), True),
    (lambda: DefaultLitePullConsumer("CID_stream_default"), True),
])
def test_enable_stream_request_type_defaults_match_the_request_builders(factory, expected):
    pass
    assert factory().enable_stream_request_type is expected


# ---------------------------------------------------------------- unitName → 取址
def test_unit_name_reaches_the_address_server_url():
    pass
    inst = _new_instance("unit_addr", unit_name="unitA")
    try:
        assert inst.top_addressing._unit_name == "unitA"
    finally:
        _drop(inst)


def test_default_unit_name_is_empty():
    inst = _new_instance("no_unit_addr")
    try:
        assert inst.top_addressing._unit_name == ""
        # 没有 unit 就不该出现 ?nofix=1（build_url 的另一条分支）
        assert "?nofix" not in inst.top_addressing.build_url()
    finally:
        _drop(inst)


# ---------------------------------------------------------------- unitMode → 线上
def test_send_request_header_carries_unit_mode():
    pass
    inst = _new_instance("unit_send")
    msg = Message("T_UnitSend", b"body")
    try:
        on = inst._build_send_request("PG_unit", msg, MQ, 3000, 0, True)
        off = inst._build_send_request("PG_unit", msg, MQ, 3000, 0)
        assert on.custom_header.unit_mode is True
        assert off.custom_header.unit_mode is False
        on.make_custom_header_to_net()
        assert str(on.ext_fields["k"]).lower() == "true"
    finally:
        _drop(inst)


def test_producer_send_passes_its_own_unit_mode():
    """facade 的 unitMode 要一路传到 send_message，中途不能被写死成 false。"""
    seen = []

    class _Client:
        publish = None

        def __init__(self):
            from client.mq_client import TopicPublishInfo
            self.publish = TopicPublishInfo()
            self.publish.msg_queue_list = [MQ]

        def register_topic_in_use(self, topic):
            pass

        def get_topic_publish_info(self, topic, is_default=False):
            return self.publish

        def find_broker_address_in_publish(self, broker_name):
            return "127.0.0.1:10911"

        def publish_addr_for(self, broker_name, topic):
            return self.find_broker_address_in_publish(broker_name)

        def send_message(self, group, msg, mq, timeout, sys_flag, unit_mode=False,
                         default_topic=None, default_topic_queue_nums=None):
            seen.append(unit_mode)
            from client.send_result import SendResult, SendStatus
            return SendResult(SendStatus.SEND_OK, msg_id="0" * 32, message_queue=mq)

    p = DefaultMQProducer("PID_unit_mode")
    p.set_namesrv_addr("127.0.0.1:9876")
    p.set_unit_mode(True)
    p._mq_client = _Client()
    p._started = True
    assert p.is_unit_mode() is True
    p.send(Message("T", b"x"))
    assert seen == [True]


def test_heartbeat_consumer_data_carries_unit_mode():
    pass
    for facade in (DefaultMQPushConsumer("CID_unit_hb"), DefaultLitePullConsumer("CID_unit_hb")):
        facade.client_id = "unit-hb-client"
        assert list(facade._build_heartbeat().consumer_data_set)[0].unit_mode is False
        facade.set_unit_mode(True)
        cd = list(facade._build_heartbeat().consumer_data_set)[0]
        assert cd.unit_mode is True, type(facade).__name__
        assert cd.to_dict()["unitMode"] is True, type(facade).__name__


class _RecorderForbidden(CheckForbiddenHook):
    def __init__(self):
        self.contexts = []

    def hook_name(self) -> str:
        return "record-forbidden"

    def check_forbidden(self, context: CheckForbiddenContext) -> None:
        self.contexts.append(context)


class _RecorderFilter(FilterMessageHook):
    def __init__(self):
        self.contexts = []

    def hook_name(self) -> str:
        return "record-filter"

    def filter_message(self, context: FilterMessageContext) -> None:
        self.contexts.append(context)


def test_check_forbidden_context_carries_unit_mode():
    pass
    for enabled, expected in ((True, True), (False, False)):
        p = DefaultMQProducer("PID_unit_forbidden")
        p.set_unit_mode(enabled)
        hook = _RecorderForbidden()
        p.register_check_forbidden_hook(hook)
        p._execute_check_forbidden(Message("T", b"x"), MQ, "127.0.0.1:10911")
        assert [c.unit_mode for c in hook.contexts] == [expected]


def test_filter_message_context_carries_unit_mode():
    pass
    msg = MessageExt("T", b"x")
    for enabled, expected in ((True, True), (False, False)):
        hook = _RecorderFilter()
        kept = filter_messages_for_delivery("CID_unit_filter", [hook], MQ, None, [msg],
                                           enabled)
        assert kept == [msg]
        assert [c.unit_mode for c in hook.contexts] == [expected]


def test_push_consumer_filter_uses_its_own_unit_mode():
    """模块级函数的参数不能只由测试传：推送消费者那条路也要接上自己的配置。"""
    consumer = DefaultMQPushConsumer("CID_unit_filter")
    consumer.set_unit_mode(True)
    hook = _RecorderFilter()
    consumer.register_filter_message_hook(hook)
    msg = MessageExt("T", b"x")
    consumer._filter_messages_for_delivery(MQ, None, [msg])
    assert [c.unit_mode for c in hook.contexts] == [True]


def test_pull_consumer_filter_uses_its_own_unit_mode():
    consumer = DefaultMQPullConsumer("CID_unit_filter")
    consumer.set_unit_mode(True)
    hook = _RecorderFilter()
    consumer.register_filter_message_hook(hook)
    consumer._filter_messages_for_delivery(MQ, [MessageExt("T", b"x")])
    assert [c.unit_mode for c in hook.contexts] == [True]


def test_unit_mode_setters_exist_on_every_facade():
    pass
    for facade in (DefaultMQProducer("PID_unit_setter"), DefaultMQPushConsumer("CID_unit_setter"),
                   DefaultMQPullConsumer("CID_unit_setter"),
                   DefaultLitePullConsumer("CID_unit_setter")):
        facade.set_unit_name("unitA")
        facade.set_unit_mode(True)
        assert facade.get_unit_name() == "unitA", type(facade).__name__
        assert facade.is_unit_mode() is True, type(facade).__name__
        assert facade.unit_mode is True, type(facade).__name__
