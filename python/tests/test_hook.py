# -*- coding: utf-8 -*-
from __future__ import annotations

import pytest

from client.consumer import (DefaultMQPullConsumer, DefaultMQPushConsumer,
                                      client_side_tag_filter, filter_messages_for_delivery)
from client.hook import (CheckForbiddenContext, CheckForbiddenHook,
                                  CommunicationMode, FilterMessageContext, FilterMessageHook)
from client.exception import MQClientException
from client.mq_client import TopicPublishInfo
from client.producer import DefaultMQProducer
from client.send_result import SendResult, SendStatus
from common.message import Message, MessageExt, MessageQueue
from common.subscription_data import ExpressionType, FilterAPI
from common.util_all import utf16_int32_hash

MQ = MessageQueue("TopicTest", "broker-a", 0)


# ---------------------------------------------------------------- Subscription semantics cross-check
def test_sub_all_leaves_tags_and_codes_empty():
    for raw in ("*", None, ""):
        sub = FilterAPI.build_subscription_data("T", raw)
        assert sub.sub_string == "*", raw
        assert sub.tags_set == set(), raw
        assert sub.code_set == set(), raw


def test_blank_substring_is_not_isempty():
    sub = FilterAPI.build_subscription_data("T", "   ")
    assert sub.sub_string == "   "
    assert sub.tags_set == set()
    assert sub.code_set == set()


def test_all_separator_substring_throws():
    for raw in ("||", "||||"):
        with pytest.raises(ValueError):
            FilterAPI.build_subscription_data("T", raw)


def test_single_pipe_is_a_literal_tag():
    sub = FilterAPI.build_subscription_data("T", "|||")
    assert sub.tags_set == {"|"}
    assert sub.code_set == {124}


def test_explicit_tag_fills_tags_and_codes():
    sub = FilterAPI.build_subscription_data("T", "TagA")
    assert sub.sub_string == "TagA"
    assert sub.tags_set == {"TagA"}
    assert sub.code_set == {2598919}


def test_multi_tag_split_trim_and_codes():
    sub = FilterAPI.build_subscription_data("T", "TagA||TagB")
    assert sub.tags_set == {"TagA", "TagB"}
    assert sub.code_set == {2598919, 2598920}
    spaced = FilterAPI.build_subscription_data("T", " TagA || TagB ")
    assert spaced.sub_string == " TagA || TagB "
    assert spaced.tags_set == {"TagA", "TagB"}
    assert spaced.code_set == {2598919, 2598920}


def test_split_error_raises_on_an_empty_tag_list():
    with pytest.raises(ValueError):
        FilterAPI.build_subscription_data("T", "||")


def test_tag_hash_vectors():
    # Vectors printed by a local probe program
    assert utf16_int32_hash("TagA") == 2598919
    assert utf16_int32_hash("TagB") == 2598920
    assert utf16_int32_hash("P") == 80
    assert utf16_int32_hash("PA") == 2545
    assert utf16_int32_hash("*") == 42
    assert utf16_int32_hash("RocketMQClient") == -47506045
    assert utf16_int32_hash("polling") == -397904957
    assert utf16_int32_hash("PID_CLIENT_INNER_TRACE_PRODUCER") == -660858443
    assert utf16_int32_hash("a" * 20) == 1542361408


# ---------------------------------------------------------------- CheckForbiddenHook
class _RecordingForbidden(CheckForbiddenHook):
    def __init__(self, forbid: bool = True, name: str = "h1"):
        self.calls = []
        self.forbid = forbid
        self.name = name

    def hook_name(self) -> str:
        return self.name

    def check_forbidden(self, context: CheckForbiddenContext) -> None:
        self.calls.append(context)
        if self.forbid:
            raise MQClientException("forbidden by test hook")


class _FakeClient:
    """Minimal usable MQClientInstance double: only covers the interfaces send() touches."""

    def __init__(self):
        self.sends = 0
        self.publish = TopicPublishInfo()
        self.publish.msg_queue_list = [MQ]
        self.sent = []

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
        self.sends += 1
        self.sent.append((mq, msg))
        return SendResult(SendStatus.SEND_OK, msg_id="0" * 32, message_queue=mq)


def _producer_with_fake_client(hooks=(), retry=2):
    p = DefaultMQProducer("GID_test")
    p.set_namesrv_addr("127.0.0.1:9876")
    p.set_retry_times_when_send_failed(retry)
    for h in hooks:
        p.register_check_forbidden_hook(h)
    p._mq_client = _FakeClient()
    p._started = True
    return p


def test_register_and_has():
    p = DefaultMQProducer("GID_test")
    assert p.has_check_forbidden_hook() is False
    p.register_check_forbidden_hook(_RecordingForbidden())
    assert p.has_check_forbidden_hook() is True
    p.register_check_forbidden_hook(None)          # None ignored
    assert len(p.check_forbidden_hook_list) == 1


def test_context_fields_match_the_rpc_contract():
    hook = _RecordingForbidden(forbid=False)
    p = _producer_with_fake_client([hook])
    p._execute_check_forbidden(Message("TopicTest", b"x"), MQ, "127.0.0.1:10911",
                               arg={"biz": 1}, communication_mode=CommunicationMode.ONEWAY)
    ctx = hook.calls[0]
    assert ctx.group == "GID_test"
    assert ctx.name_srv_addr == "127.0.0.1:9876"
    assert ctx.broker_addr == "127.0.0.1:10911"
    assert ctx.communication_mode == CommunicationMode.ONEWAY
    assert ctx.mq is MQ
    assert ctx.arg == {"biz": 1}
    assert ctx.unit_mode is False
    assert ctx.send_result is None                 # not sent yet (the difference from SendMessageContext)


def test_exception_is_not_swallowed():
    """Exceptions from CheckForbiddenHook must propagate upward (this is how "interception" is implemented),
    in contrast to send/consume hooks whose exceptions are always swallowed."""
    p = _producer_with_fake_client([_RecordingForbidden(forbid=True)])
    with pytest.raises(MQClientException):
        p._execute_check_forbidden(Message("TopicTest", b"x"), MQ, "addr")


def test_send_is_blocked_and_hook_runs_per_attempt():
    hook = _RecordingForbidden(forbid=True)
    p = _producer_with_fake_client([hook], retry=2)
    with pytest.raises(MQClientException):
        p.send(Message("TopicTest", b"hello"))
    assert len(hook.calls) == 3, "retryTimesWhenSendFailed=2 → 3 attempts total, each must pass the hook"
    assert p._mq_client.sends == 0, "an intercepted send must not actually go out"


def test_send_passes_when_hook_allows():
    hook = _RecordingForbidden(forbid=False)
    p = _producer_with_fake_client([hook], retry=2)
    result = p.send(Message("TopicTest", b"hello"))
    assert result.send_status == SendStatus.SEND_OK
    assert len(hook.calls) == 1
    assert p._mq_client.sends == 1
    assert hook.calls[0].communication_mode == CommunicationMode.SYNC


def test_multiple_hooks_all_run():
    a, b = _RecordingForbidden(forbid=False, name="a"), _RecordingForbidden(forbid=False, name="b")
    p = _producer_with_fake_client([a, b])
    p.send(Message("TopicTest", b"hello"))
    assert len(a.calls) == 1 and len(b.calls) == 1


def test_oneway_is_intercepted_too():
    hook = _RecordingForbidden(forbid=True)
    p = _producer_with_fake_client([hook])
    with pytest.raises(MQClientException):
        p.send_oneway(Message("TopicTest", b"x"))
    assert hook.calls[0].communication_mode == CommunicationMode.ONEWAY


def test_no_hook_means_no_interception():
    p = _producer_with_fake_client([])
    assert p._has_send_interceptors() is False
    p.send(Message("TopicTest", b"x"))
    assert p._mq_client.sends == 1


# ---------------------------------------------------------------- FilterMessageHook
class _DroppingFilter(FilterMessageHook):
    def __init__(self, drop_body: bytes = b"drop"):
        self.drop_body = drop_body
        self.calls = 0

    def hook_name(self) -> str:
        return "drop"

    def filter_message(self, context: FilterMessageContext) -> None:
        self.calls += 1
        context.msg_list = [m for m in context.msg_list if m.body != self.drop_body]


class _BoomFilter(FilterMessageHook):
    def hook_name(self) -> str:
        return "boom"

    def filter_message(self, context: FilterMessageContext) -> None:
        raise RuntimeError("hook exploded")


def _msgs(*tags_bodies):
    out = []
    for tags, body in tags_bodies:
        m = MessageExt("TopicTest", body, tags=tags)
        out.append(m)
    return out


def test_client_side_tag_filter_skips_sub_all():
    """With subscription "*" tags_set is empty → no filtering (this is why SUB_ALL must stay empty)."""
    msgs = _msgs(("TagA", b"a"), ("TagB", b"b"))
    sub_all = FilterAPI.build_subscription_data("TopicTest", "*")
    assert client_side_tag_filter(sub_all, msgs) == msgs


def test_client_side_tag_filter_keeps_only_matching_tags():
    msgs = _msgs(("TagA", b"a"), ("TagB", b"b"), ("TagA", b"c"))
    sub = FilterAPI.build_subscription_data("TopicTest", "TagA")
    kept = client_side_tag_filter(sub, msgs)
    assert [m.body for m in kept] == [b"a", b"c"]


def test_client_side_tag_filter_ignores_class_filter_mode():
    msgs = _msgs(("TagA", b"a"))
    sub = FilterAPI.build_subscription_data("TopicTest", "TagB")
    sub.class_filter_mode = True
    assert client_side_tag_filter(sub, msgs) == msgs


def test_filter_hook_drops_messages():
    msgs = _msgs(("TagA", b"drop"), ("TagA", b"keep"))
    hook = _DroppingFilter()
    kept = filter_messages_for_delivery("GID_test", [hook], MQ, None, msgs)
    assert [m.body for m in kept] == [b"keep"]
    assert hook.calls == 1


def test_filter_hook_exception_is_swallowed():
    pass
    msgs = _msgs(("TagA", b"a"))
    kept = filter_messages_for_delivery("GID_test", [_BoomFilter()], MQ, None, msgs)
    assert kept == msgs


def test_filter_hook_can_replace_list_entirely():
    class Replacer(FilterMessageHook):
        def hook_name(self):
            return "replacer"

        def filter_message(self, context):
            context.msg_list = []

    assert filter_messages_for_delivery("G", [Replacer()], MQ, None, _msgs(("T", b"a"))) == []


def test_push_consumer_registers_filter_hook():
    c = DefaultMQPushConsumer("GID_test")
    assert c.has_filter_message_hook() is False
    c.register_filter_message_hook(_DroppingFilter())
    assert c.has_filter_message_hook() is True
    kept = c._filter_messages_for_delivery(MQ, None, _msgs(("T", b"drop"), ("T", b"keep")))
    assert [m.body for m in kept] == [b"keep"]


def test_pull_consumer_registers_filter_hook():
    c = DefaultMQPullConsumer("GID_test")
    assert c.has_filter_message_hook() is False
    c.register_filter_message_hook(_DroppingFilter())
    kept = c._filter_messages_for_delivery(MQ, _msgs(("T", b"drop"), ("T", b"keep")))
    assert [m.body for m in kept] == [b"keep"]
    # Returned as-is when there is no hook
    c2 = DefaultMQPullConsumer("GID_test2")
    msgs = _msgs(("T", b"drop"))
    assert c2._filter_messages_for_delivery(MQ, msgs) == msgs


def test_filter_hook_runs_after_tag_filter():
    pass
    seen = []

    class Sniffer(FilterMessageHook):
        def hook_name(self):
            return "sniffer"

        def filter_message(self, context):
            seen.extend(m.body for m in context.msg_list)

    sub = FilterAPI.build_subscription_data("TopicTest", "TagA")
    msgs = _msgs(("TagB", b"wrong-tag"), ("TagA", b"right-tag"))
    filter_messages_for_delivery("G", [Sniffer()], MQ, sub, msgs)
    assert seen == [b"right-tag"]


def test_expression_type_default_is_tag():
    assert FilterAPI.build_subscription_data("T", "TagA").expression_type == ExpressionType.TAG
