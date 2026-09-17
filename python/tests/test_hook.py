# -*- coding: utf-8 -*-
"""CheckForbiddenHook / FilterMessageHook 与订阅语义（tagsSet/codeSet）的单测。

对齐基准是 Java 5.5.1 官方实现：
  * `FilterAPI.buildSubscriptionData` 的对拍向量由探针
    （`/tmp/subprobe/SubProbe.java`，`java -cp <rmq classpath> SubProbe.java`）打印：
      "*" / null / ""   → subString="*"，tagsSet=[]，codeSet=[]
      "TagA"            → tagsSet=[TagA]，codeSet=[2598919]
      "TagA||TagB"      → tagsSet=[TagA,TagB]，codeSet=[2598919,2598920]
      " TagA || TagB "  → **subString 原样保留空格**，标签各自 trim
  * CheckForbiddenHook 的调用点在 `DefaultMQProducerImpl.sendKernelImpl:956-965`，
    异常沿 `sendDefaultImpl` 的 `catch (MQClientException e) { ... continue; }` 重试；
  * FilterMessageHook 在两个位置：`PullAPIWrapper.processPullResult:124-128`（拉取）
    与 `DefaultMQPushConsumerImpl.processPopResult:637-661`（POP，摘掉的要 ackAsync）。
"""
from __future__ import annotations

import pytest

from rocketmq.client.consumer import (DefaultMQPullConsumer, DefaultMQPushConsumer,
                                      client_side_tag_filter, filter_messages_for_delivery)
from rocketmq.client.hook import (CheckForbiddenContext, CheckForbiddenHook,
                                  CommunicationMode, FilterMessageContext, FilterMessageHook)
from rocketmq.client.exception import MQClientException
from rocketmq.client.mq_client import TopicPublishInfo
from rocketmq.client.producer import DefaultMQProducer
from rocketmq.client.send_result import SendResult, SendStatus
from rocketmq.common.message import Message, MessageExt, MessageQueue
from rocketmq.common.subscription_data import ExpressionType, FilterAPI
from rocketmq.common.util_all import java_string_hash

MQ = MessageQueue("TopicTest", "broker-a", 0)


# ---------------------------------------------------------------- 订阅语义对拍
def test_sub_all_leaves_tags_and_codes_empty():
    # Java：StringUtils.isEmpty(subString) || subString.equals("*") → setSubString("*") 后直接 return
    for raw in ("*", None, ""):
        sub = FilterAPI.build_subscription_data("T", raw)
        assert sub.sub_string == "*", raw
        assert sub.tags_set == set(), raw
        assert sub.code_set == set(), raw


def test_blank_substring_is_not_isempty():
    # 纯空白 ≠ 空串：Java StringUtils.isEmpty 只认 null/""，
    # 所以 "   " 会走 split 分支 —— 标签被 trim 成空 ⇒ 集合仍空，但 subString **原样保留**。
    # 探针实测：subString=[   ] -> sub=[   ] tags=[] codes=[]
    sub = FilterAPI.build_subscription_data("T", "   ")
    assert sub.sub_string == "   "
    assert sub.tags_set == set()
    assert sub.code_set == set()


def test_all_separator_substring_throws():
    # Java String.split("\\|\\|") 丢弃末尾空串 → "||" 切完是空数组 → tags.length == 0
    # → 抛 new Exception("subString split error")
    for raw in ("||", "||||"):
        with pytest.raises(ValueError):
            FilterAPI.build_subscription_data("T", raw)


def test_single_pipe_is_a_literal_tag():
    # 确认我们实现的是 Java-split 而不是"丢所有空段"：
    # "|||".split("\\|\\|") == ["", "|"] → 丢末尾空串后非空 → 标签是字面量 "|"（hash 124）
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
    # Java 不重写 subString：两侧空格原样保留
    spaced = FilterAPI.build_subscription_data("T", " TagA || TagB ")
    assert spaced.sub_string == " TagA || TagB "
    assert spaced.tags_set == {"TagA", "TagB"}
    assert spaced.code_set == {2598919, 2598920}


def test_split_error_matches_java_exception():
    # Java：split 后无标签 → throw new Exception("subString split error")
    with pytest.raises(ValueError):
        FilterAPI.build_subscription_data("T", "||")


def test_java_string_hash_vectors():
    # 向量由探针 /tmp/subprobe/HashProbe.java + SubProbe.java 打印
    assert java_string_hash("TagA") == 2598919
    assert java_string_hash("TagB") == 2598920
    assert java_string_hash("P") == 80
    assert java_string_hash("PA") == 2545
    assert java_string_hash("*") == 42
    # 负数回绕（Java int 语义，必须与 Python 的任意精度整数区分开）
    assert java_string_hash("RocketMQClient") == -47506045
    assert java_string_hash("polling") == -397904957
    assert java_string_hash("PID_CLIENT_INNER_TRACE_PRODUCER") == -660858443
    assert java_string_hash("a" * 20) == 1542361408


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
    """最小可用的 MQClientInstance 替身：只覆盖 send() 会碰到的接口。"""

    def __init__(self):
        self.sends = 0
        self.publish = TopicPublishInfo()
        self.publish.msg_queue_list = [MQ]
        self.sent = []

    def register_topic_in_use(self, topic):
        pass

    def get_topic_publish_info(self, topic, is_default=False):
        return self.publish

    def broker_addr_of(self, broker_name):
        return "127.0.0.1:10911"

    def send_message(self, group, msg, mq, timeout, sys_flag):
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
    p.register_check_forbidden_hook(None)          # None 忽略
    assert len(p.check_forbidden_hook_list) == 1


def test_context_fields_match_java():
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
    assert ctx.send_result is None                 # 此刻还没发（与 SendMessageContext 的差别）


def test_exception_is_not_swallowed():
    """CheckForbiddenHook 的异常必须向上抛（这是"拦截"的实现方式），
    对比 send/consume 钩子的异常一律吞掉。"""
    p = _producer_with_fake_client([_RecordingForbidden(forbid=True)])
    with pytest.raises(MQClientException):
        p._execute_check_forbidden(Message("TopicTest", b"x"), MQ, "addr")


def test_send_is_blocked_and_hook_runs_per_attempt():
    hook = _RecordingForbidden(forbid=True)
    p = _producer_with_fake_client([hook], retry=2)
    with pytest.raises(MQClientException):
        p.send(Message("TopicTest", b"hello"))
    assert len(hook.calls) == 3, "retryTimesWhenSendFailed=2 → 共 3 次尝试，每次都要过钩子"
    assert p._mq_client.sends == 0, "被拦截的发送不能真的发出去"


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
    """订阅 "*" 时 tags_set 为空 → 不过滤（这正是 SUB_ALL 必须留空的原因）。"""
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
    sub.class_filter_mode = True                   # Java：classFilterMode 时跳过
    assert client_side_tag_filter(sub, msgs) == msgs


def test_filter_hook_drops_messages():
    msgs = _msgs(("TagA", b"drop"), ("TagA", b"keep"))
    hook = _DroppingFilter()
    kept = filter_messages_for_delivery("GID_test", [hook], MQ, None, msgs)
    assert [m.body for m in kept] == [b"keep"]
    assert hook.calls == 1


def test_filter_hook_exception_is_swallowed():
    """过滤钩子抛异常不能影响消费（Java PullAPIWrapper.executeHook 记 error 后继续）。"""
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
    # 无钩子时原样返回
    c2 = DefaultMQPullConsumer("GID_test2")
    msgs = _msgs(("T", b"drop"))
    assert c2._filter_messages_for_delivery(MQ, msgs) == msgs


def test_filter_hook_runs_after_tag_filter():
    """顺序：先二次 tag 过滤，再跑钩子（Java PullAPIWrapper 亦然）。"""
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
