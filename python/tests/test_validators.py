# -*- coding: utf-8 -*-
"""Validators / TopicValidator 测试（对应 Java ``client.Validators`` + ``common.topic.TopicValidator``）。

这层校验的意义在于**失败得足够早**：topic/group 名字非法时 broker 也会拒，但
``TOPIC_NOT_EXIST`` 属于发送重试的可重试码集合，于是每条必然失败的消息都会把重试次数
和超时预算空转一遍。所以本地必须先把关，而且把关的位置要在任何网络动作之前。

码值口径照抄 Java，别凭直觉（详见 ``rocketmq/client/validators.py`` 的模块注释）：
  * ``check_group`` / ``check_topic`` / ``is_system_topic`` / ``is_not_allowed_send_topic``
    走 ``MQClientException(String, Throwable)``，Java 里是 -1，本项目用 ``None`` 表达
    同一件事（"纯客户端错误，没有 broker 码"）。
  * 只有 ``check_message`` 的五条分支带 ``MESSAGE_ILLEGAL``(13)：null message、null body、
    zero-length body、超长 body、INNER_MULTI_DISPATCH 含文件分隔符。
"""
from __future__ import annotations

import os
import re

import pytest

from rocketmq.client import validators
from rocketmq.client.consumer import (
    DefaultLitePullConsumer,
    DefaultMQPullConsumer,
    DefaultMQPushConsumer,
    SimpleMessageListener,
)
from rocketmq.client.consumer_result import ConsumeConcurrentlyStatus
from rocketmq.client.exception import MQClientException
from rocketmq.client.producer import DefaultMQProducer
from rocketmq.common.message import Message
from rocketmq.common.message_const import MessageConst
from rocketmq.common.topic_validator import (
    GROUP_MAX_LENGTH,
    NOT_ALLOWED_SEND_TOPIC_SET,
    RETRY_OR_DLQ_TOPIC_MAX_LENGTH,
    SYSTEM_TOPIC_PREFIX,
    SYSTEM_TOPIC_SET,
    TOPIC_MAX_LENGTH,
    VALID_CHAR_PATTERN,
    is_not_allowed_send_topic,
    is_system_topic,
    is_topic_or_group_illegal,
)
from rocketmq.remoting.protocol.codes import ResponseCode

# Java 用查表实现字符白名单，这里用注释里的那条正则当独立参照，防止两边一起写错。
JAVA_PATTERN = re.compile(r"^[%|a-zA-Z0-9_-]+$")


def msg(topic: str = "T1", body: bytes = b"x") -> Message:
    m = Message(topic)
    m.body = body
    return m


class TestTopicOrGroupCharset:
    @pytest.mark.parametrize("ch", list("%-_|") + list("abcXYZ") + list("0189"))
    def test_allowed_chars(self, ch):
        assert is_topic_or_group_illegal("t%s1" % ch) is False
        assert JAVA_PATTERN.match("t%s1" % ch)

    @pytest.mark.parametrize(
        "ch",
        [" ", ".", "/", "\\", ":", "*", "#", "@", "+", "=", "~", "!", "\t", "\n", "\x00", "\x7f"],
    )
    def test_illegal_ascii(self, ch):
        assert is_topic_or_group_illegal("t%s1" % ch) is True
        assert JAVA_PATTERN.match("t%s1" % ch) is None

    @pytest.mark.parametrize("ch", ["\u0080", "中", "\u00e9", "\uff01", "\U0001f600"])
    def test_non_ascii_is_illegal(self, ch):
        # Java 的 VALID_CHAR_BIT_MAP 只有 128 位：码点 >= 128 一律非法，不做 Unicode 归类
        assert is_topic_or_group_illegal("topic" + ch) is True

    def test_empty_name_is_not_illegal(self):
        # 空/纯空白由 check_topic / check_group 的第一步单独管，判定函数本身返回 False
        assert is_topic_or_group_illegal("") is False

    def test_pipe_is_a_legal_char(self):
        assert is_topic_or_group_illegal("topic|1") is False

    def test_pattern_constant_matches_java_regex(self):
        assert VALID_CHAR_PATTERN == "^[%|a-zA-Z0-9_-]+$"

    def test_length_limits(self):
        # group 要参与拼 %RETRY%group_topic / %DLQ%group_topic，所以比 topic 短
        assert (TOPIC_MAX_LENGTH, GROUP_MAX_LENGTH, RETRY_OR_DLQ_TOPIC_MAX_LENGTH) == (127, 120, 255)


class TestSystemTopicSet:
    def test_set_membership(self):
        assert len(SYSTEM_TOPIC_SET) == 12
        assert "TBW102" in SYSTEM_TOPIC_SET
        assert is_system_topic("TBW102") is True
        assert is_system_topic(SYSTEM_TOPIC_PREFIX + "anything") is True
        assert is_system_topic("OrderTopic") is False

    def test_not_allowed_send_set_is_a_subset_of_the_system_set(self):
        assert len(NOT_ALLOWED_SEND_TOPIC_SET) == 8
        assert NOT_ALLOWED_SEND_TOPIC_SET <= SYSTEM_TOPIC_SET

    def test_retry_topic_is_sendable(self):
        # ⚠ sendMessageBack 就是往 %RETRY%group 写的，禁掉会打断重投链路
        assert is_not_allowed_send_topic("%RETRY%GID_any") is False

    def test_tbw102_is_not_forbidden(self):
        # 自动建 topic 的 key topic：在系统名单里，但允许直接发（Java 同款差异）
        assert is_not_allowed_send_topic("TBW102") is False

    @pytest.mark.parametrize("topic", sorted(NOT_ALLOWED_SEND_TOPIC_SET))
    def test_every_forbidden_topic_is_blocked(self, topic):
        assert is_not_allowed_send_topic(topic) is True


class TestCheckGroup:
    @pytest.mark.parametrize("group", [None, "", "   ", "\t"])
    def test_blank(self, group):
        with pytest.raises(MQClientException) as ei:
            validators.check_group(group)
        assert str(ei.value) == "the specified group is blank"
        assert ei.value.response_code is None

    def test_length_boundary(self):
        validators.check_group("g" * GROUP_MAX_LENGTH)
        with pytest.raises(MQClientException) as ei:
            validators.check_group("g" * (GROUP_MAX_LENGTH + 1))
        assert str(ei.value) == (
            "the specified group[%s] is longer than group max length: %d."
            % ("g" * (GROUP_MAX_LENGTH + 1), GROUP_MAX_LENGTH))

    def test_charset(self):
        with pytest.raises(MQClientException) as ei:
            validators.check_group("bad group")
        assert str(ei.value) == (
            "the specified group[bad group] contains illegal characters, allowing only %s"
            % VALID_CHAR_PATTERN)

    def test_blank_wins_over_length_and_charset(self):
        # 校验顺序：blank → 长度 → 字符表（照抄 Java）
        with pytest.raises(MQClientException) as ei:
            validators.check_group(" ")
        assert "blank" in str(ei.value)


class TestCheckTopic:
    @pytest.mark.parametrize("topic", [None, "", "  "])
    def test_blank(self, topic):
        with pytest.raises(MQClientException) as ei:
            validators.check_topic(topic)
        assert str(ei.value) == "The specified topic is blank"
        assert ei.value.response_code is None

    def test_length_boundary(self):
        validators.check_topic("t" * TOPIC_MAX_LENGTH)
        with pytest.raises(MQClientException) as ei:
            validators.check_topic("t" * (TOPIC_MAX_LENGTH + 1))
        assert str(ei.value) == (
            "The specified topic is longer than topic max length %d." % TOPIC_MAX_LENGTH)

    def test_charset(self):
        with pytest.raises(MQClientException) as ei:
            validators.check_topic("topic.name")
        assert str(ei.value) == (
            "The specified topic[topic.name] contains illegal characters, allowing only %s"
            % VALID_CHAR_PATTERN)


class TestNamingGuards:
    def test_system_topic_raises_without_a_broker_code(self):
        with pytest.raises(MQClientException) as ei:
            validators.is_system_topic("RMQ_SYS_TRACE_TOPIC")
        assert str(ei.value) == "The topic[RMQ_SYS_TRACE_TOPIC] is conflict with system topic."
        assert ei.value.response_code is None

    def test_forbidden_send_topic_raises_without_a_broker_code(self):
        # Java 在这里用的是 (String, Throwable) 构造器 ⇒ -1，而不是 MESSAGE_ILLEGAL。
        # 看着别扭，但上层按 response_code 分支时必须知道，四语言保持一致。
        with pytest.raises(MQClientException) as ei:
            validators.is_not_allowed_send_topic("SCHEDULE_TOPIC_XXXX")
        assert str(ei.value) == "Sending message to topic[SCHEDULE_TOPIC_XXXX] is forbidden."
        assert ei.value.response_code != ResponseCode.MESSAGE_ILLEGAL
        assert ei.value.response_code is None

    def test_legal_names_pass(self):
        validators.is_system_topic("OrderTopic")
        validators.is_not_allowed_send_topic("OrderTopic")


class TestCheckMessage:
    def test_null_message(self):
        with pytest.raises(MQClientException) as ei:
            validators.check_message(None, 1024)
        assert str(ei.value) == "the message is null"
        assert ei.value.response_code == ResponseCode.MESSAGE_ILLEGAL

    @pytest.mark.parametrize("body", [None, b""])
    def test_null_or_empty_body(self, body):
        m = Message("T1")
        m.body = body
        with pytest.raises(MQClientException) as ei:
            validators.check_message(m, 1024)
        expected = "the message body is null" if body is None else "the message body length is zero"
        assert str(ei.value) == expected
        assert ei.value.response_code == ResponseCode.MESSAGE_ILLEGAL

    def test_oversize_body_boundary(self):
        m = msg("T1", b"1" * 5)
        with pytest.raises(MQClientException) as ei:
            validators.check_message(m, 4)
        assert str(ei.value) == "the message body size over max value, MAX: 4"
        assert ei.value.response_code == ResponseCode.MESSAGE_ILLEGAL
        # Java 用的是 `>`，等于上限放行
        validators.check_message(msg("T1", b"1" * 4), 4)

    def test_lmq_path_with_file_separator(self):
        m = msg()
        m.put_property(MessageConst.PROPERTY_INNER_MULTI_DISPATCH, "a%sb" % os.sep)
        with pytest.raises(MQClientException) as ei:
            validators.check_message(m, 1024)
        assert str(ei.value) == (
            "INNER_MULTI_DISPATCH a%sb can not contains %s character" % (os.sep, os.sep))
        assert ei.value.response_code == ResponseCode.MESSAGE_ILLEGAL

    def test_lmq_path_without_separator_is_fine(self):
        m = msg()
        m.put_property(MessageConst.PROPERTY_INNER_MULTI_DISPATCH, "topic%DLQ%group")
        validators.check_message(m, 1024)

    def test_topic_is_checked_before_body(self):
        # 顺序照抄 Java：checkTopic → 禁发 topic → body。空 topic + 空 body 时报的是 topic
        m = Message("")
        m.body = b""
        with pytest.raises(MQClientException) as ei:
            validators.check_message(m, 1024)
        assert str(ei.value) == "The specified topic is blank"
        assert ei.value.response_code is None

    def test_forbidden_topic_reported_before_body(self):
        m = msg("SCHEDULE_TOPIC_XXXX", b"")
        with pytest.raises(MQClientException) as ei:
            validators.check_message(m, 1024)
        assert "is forbidden" in str(ei.value)


class TestProducerWiring:
    """校验必须排在任何网络动作之前——否则「名字写错」会退化成连不上/超时。"""

    PROBE_ADDR = ["127.0.0.1:1"]

    @pytest.mark.parametrize(
        "group,needle",
        [
            ("DEFAULT_PRODUCER", "producerGroup can not equal DEFAULT_PRODUCER"),
            ("bad group", "contains illegal characters"),
            ("g" * (GROUP_MAX_LENGTH + 1), "is longer than group max length"),
        ],
    )
    def test_start_rejects_bad_group_without_io(self, group, needle):
        p = DefaultMQProducer(group)
        p.name_server_addrs = list(self.PROBE_ADDR)
        with pytest.raises(MQClientException) as ei:
            p.start()
        assert needle in str(ei.value)
        assert not p._started, "失败的 start 不能留在已启动状态"

    def test_group_is_validated_after_the_namespace_wrap(self):
        # Java 的 start 先 withNamespace 再 impl.start()，所以按包装后的形状校验
        p = DefaultMQProducer("g" * 110)
        p.namespace = "ns" * 30
        p.name_server_addrs = list(self.PROBE_ADDR)
        with pytest.raises(MQClientException) as ei:
            p.start()
        assert "is longer than group max length" in str(ei.value)
        assert not p._started

    def test_send_rejects_illegal_topic_before_the_network(self):
        # 未启动时先报 not started；真正要证的是 check_message 在 send 内核的第一步
        p = DefaultMQProducer("GID_validators")
        with pytest.raises(MQClientException):
            p._check_message(None)
        with pytest.raises(MQClientException) as ei:
            p._check_message(msg("bad topic", b"x"))
        assert "contains illegal characters" in str(ei.value)


class TestConsumerWiring:
    """三种消费者的 checkConfig 都排在地址/订阅/监听器校验之前（Java 同款顺序）。

    用 127.0.0.1:1 兜底：万一顺序被改坏，测试会连不上而失败，不会打到真集群。
    """

    @pytest.mark.parametrize(
        "group,needle",
        [
            ("DEFAULT_CONSUMER", "consumerGroup can not equal DEFAULT_CONSUMER"),
            ("bad group", "contains illegal characters"),
            ("g" * (GROUP_MAX_LENGTH + 1), "is longer than group max length"),
        ],
    )
    def test_push_consumer_rejects_bad_group_first(self, group, needle):
        c = DefaultMQPushConsumer(group)
        c.name_server_addrs = ["127.0.0.1:1"]
        c.subscribe("T1", "TagA")
        c.set_message_listener(
            SimpleMessageListener(lambda msgs: ConsumeConcurrentlyStatus.CONSUME_SUCCESS))
        with pytest.raises(MQClientException) as ei:
            c.start()
        assert needle in str(ei.value)
        assert not c._started, "失败的 start 不能留在已启动状态"

    @pytest.mark.parametrize(
        "cls,needle",
        [
            (DefaultMQPullConsumer, "consumerGroup can not equal DEFAULT_CONSUMER"),
            (DefaultLitePullConsumer, "consumerGroup can not equal DEFAULT_CONSUMER"),
        ],
    )
    def test_pull_consumers_reject_the_reserved_group(self, cls, needle):
        c = cls("DEFAULT_CONSUMER")
        c.name_server_addrs = ["127.0.0.1:1"]
        with pytest.raises(MQClientException) as ei:
            c.start()
        assert needle in str(ei.value)
        assert not c._started

    @pytest.mark.parametrize("cls", [DefaultMQPullConsumer, DefaultLitePullConsumer])
    def test_pull_consumers_reject_illegal_chars(self, cls):
        c = cls("bad group")
        c.name_server_addrs = ["127.0.0.1:1"]
        with pytest.raises(MQClientException) as ei:
            c.start()
        assert "contains illegal characters" in str(ei.value)

    def test_legal_group_reaches_the_next_gate(self):
        # 反向对照：组名合法时才会走到订阅/地址校验，证明上面的报错不是被别的检查抢先了
        c = DefaultLitePullConsumer("GID_validators_lite")
        c.name_server_addrs = ["127.0.0.1:1"]
        with pytest.raises(MQClientException) as ei:
            c.start()
        assert "subscription is not set" in str(ei.value)
