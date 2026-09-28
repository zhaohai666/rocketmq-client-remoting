# -*- coding: utf-8 -*-
"""定点发送（显式给了 ``mq``）的 topic 一致性守卫的单测。

对齐基准是 Java 5.5.1 ``DefaultMQProducerImpl`` 的两处守卫：

  * ``:1234-1236`` 同步 ``send(msg, mq, timeout)``：``Validators.checkMessage`` 之后、
    ``sendKernelImpl`` 之前 —— ``if (!msg.getTopic().equals(mq.getTopic())) throw new
    MQClientException("message's topic not equal mq's topic", null);``
  * ``:1277-1278`` 异步：同一个判定用了**另一处**文案 ``"Topic of the message does not
    match its target message queue"``，且抛在投进 ``AsyncSenderExecutor`` 的 runnable 里、
    由 ``catch (Exception e) { newCallBack.onException(e); }`` 转交用户回调。

比较的是**各自拼过命名空间之后**的名字：``DefaultMQProducer`` 的公开入口先
``msg.setTopic(withNamespace(...))``、再把 mq 过 ``queueWithNamespace``（``:601-602`` 等），
``wrapNamespace`` 幂等，所以同一命名空间下两个资源名相等即放行。少了这道守卫，topic 与
目标队列不符的消息照样发得出去：broker 按请求里带的队列名写入，SendResult 一切正常，
消息却落进了另一个 topic 的分区，无人消费也无人报错。

两处**有意**的「不守卫」也按 Java 钉住：``sendOneway(msg, mq)``（Java ``:1303-1310``）
没有这道检查，选择器入口同样没有。
"""
from __future__ import annotations

import time
from typing import List, Optional

import pytest

from rocketmq.client.exception import MQClientException
from rocketmq.client.mq_client import TopicPublishInfo
from rocketmq.client.producer import DefaultMQProducer, MessageQueueSelector, SendCallback
from rocketmq.client.send_result import SendResult, SendStatus
from rocketmq.common.message import Message, MessageQueue
from rocketmq.remoting.protocol.codes import RequestCode
from rocketmq.remoting.protocol.remoting_command import RemotingCommand

ADDR = "127.0.0.1:10911"
NS = "ns1"
SYNC_WORDING = "message's topic not equal mq's topic"
ASYNC_WORDING = "Topic of the message does not match its target message queue"


class _FakeRemoting:
    def __init__(self):
        self.oneway: List[tuple] = []

    def invoke_oneway(self, addr, cmd) -> None:
        self.oneway.append((addr, cmd))


class _RecordingClient:
    """MQClientInstance 替身：只记录「上线时 broker 看到的 topic」，不碰网络。

    守卫必须排在真正建请求之前，所以被拒绝的那次发送在这份记录里必须**什么都没有**。
    """

    def __init__(self):
        self.publish = TopicPublishInfo()
        self.publish.msg_queue_list = [MessageQueue("TopicTest", "broker-a", 0)]
        self.name_server_addrs = ["127.0.0.1:9876"]
        self.topics: List[str] = []
        self.remoting_client = _FakeRemoting()

    def register_topic_in_use(self, topic):
        pass

    def get_topic_publish_info(self, topic, is_default=False):
        return self.publish

    def broker_addr_of(self, broker_name):
        return ADDR

    def send_message(self, group, msg, mq, timeout, sys_flag, unit_mode=False,
                     default_topic=None, default_topic_queue_nums=None):
        self.topics.append(msg.topic)
        return SendResult(SendStatus.SEND_OK, msg_id="0" * 32, message_queue=mq)

    def send_message_oneway(self, group, msg, mq, addr, timeout, sys_flag, unit_mode=False,
                            default_topic=None, default_topic_queue_nums=None):
        self.topics.append(msg.topic)

    def build_send_request(self, group, msg, mq, timeout, sys_flag, unit_mode=False,
                           default_topic=None, default_topic_queue_nums=None):
        return RemotingCommand.create_request_command(RequestCode.SEND_MESSAGE_V2, None)

    def send_message_async(self, addr, request, msg, mq, timeout, on_complete):
        self.topics.append(msg.topic)
        on_complete(SendResult(SendStatus.SEND_OK, msg_id="0" * 32, message_queue=mq), None)

    def shutdown(self):
        pass


class _FirstQueueSelector(MessageQueueSelector):
    def select(self, queues, msg, arg):
        return queues[0]


class _RecordingCallback(SendCallback):
    def __init__(self):
        self.results: List[SendResult] = []
        self.errors: List[BaseException] = []
        self.done = False

    def on_success(self, send_result: SendResult) -> None:
        self.results.append(send_result)
        self.done = True

    def on_exception(self, e: BaseException) -> None:
        self.errors.append(e)
        self.done = True


def _producer(namespace: Optional[str] = None) -> DefaultMQProducer:
    p = DefaultMQProducer("GID_pinned_topic", namespace=namespace or "")
    p.set_namesrv_addr("127.0.0.1:9876")
    p.set_retry_times_when_send_failed(0)
    p.retry_times_when_send_async_failed = 0
    # start() 会建真实客户端，这里只补齐发送链需要的：started 标记 + 两个池
    p._create_async_executors()
    p._mq_client = _RecordingClient()
    p._started = True
    return p


def _wait_done(cb: _RecordingCallback, timeout: float = 5.0) -> None:
    deadline = time.monotonic() + timeout
    while not cb.done and time.monotonic() < deadline:
        time.sleep(0.005)
    assert cb.done, "回调没在超时内跑完"


# ---------------------------------------------------------------- 同步
def test_sync_pinned_send_rejects_a_mismatched_topic():
    p = _producer()
    client = p._mq_client
    msg = Message("TopicTest", b"x")

    with pytest.raises(MQClientException) as ei:
        p.send(msg, mq=MessageQueue("OtherTopic", "broker-a", 0))

    assert str(ei.value) == SYNC_WORDING
    assert ei.value.response_code is None  # Java 传的是 null cause，没有错误码
    assert client.topics == [], "被拒绝的发送不该碰到网络"
    assert msg.topic == "TopicTest" and msg.body == b"x"


def test_sync_pinned_send_accepts_a_matching_topic():
    """负向对照：topic 一致（不设命名空间）必须照常发出。"""
    p = _producer()
    client = p._mq_client
    msg = Message("TopicTest", b"x")

    result = p.send(msg, mq=MessageQueue("TopicTest", "broker-a", 0))

    assert result.send_status == SendStatus.SEND_OK
    assert client.topics == ["TopicTest"]


def test_namespace_does_not_false_reject_the_pinned_send():
    """带命名空间时两边分别被包一层，比的仍是同一对资源名。

    三种入参组合都要放行：队列 topic 是裸名、已经带前缀、以及路由里拿到的带前缀队列。
    """
    p = _producer(NS)
    client = p._mq_client

    for queue_topic in ("TopicTest", "%s%%TopicTest" % NS):
        msg = Message("TopicTest", b"x")
        result = p.send(msg, mq=MessageQueue(queue_topic, "broker-a", 0))
        assert result.send_status == SendStatus.SEND_OK
        assert msg.topic == "TopicTest"

    route_queue = client.publish.msg_queue_list[0]
    msg = Message("TopicTest", b"x")
    result = p.send(msg, mq=MessageQueue(route_queue.topic, "broker-a", 0))
    assert result.send_status == SendStatus.SEND_OK

    assert client.topics == ["%s%%TopicTest" % NS] * 3


def test_namespaced_message_topic_still_must_match():
    """消息 topic 与队列 topic 在不同的命名空间下同样不一致（不是"有前缀就放行"）。"""
    p = _producer(NS)
    msg = Message("TopicTest", b"x")

    with pytest.raises(MQClientException) as ei:
        p.send(msg, mq=MessageQueue("ns2%TopicTest", "broker-a", 0))

    assert str(ei.value) == SYNC_WORDING


# ---------------------------------------------------------------- 批量
def test_batch_pinned_send_rejects_a_mismatched_topic():
    p = _producer()
    client = p._mq_client
    msgs = [Message("TopicTest", b"a"), Message("TopicTest", b"b")]

    with pytest.raises(MQClientException) as ei:
        p.send(msgs, mq=MessageQueue("OtherTopic", "broker-a", 0))

    assert str(ei.value) == SYNC_WORDING
    assert client.topics == []


def test_batch_pinned_send_accepts_a_matching_topic():
    p = _producer(NS)
    client = p._mq_client
    msgs = [Message("TopicTest", b"a"), Message("TopicTest", b"b")]

    result = p.send(msgs, mq=MessageQueue("TopicTest", "broker-a", 0))

    assert result.send_status == SendStatus.SEND_OK
    assert client.topics == ["%s%%TopicTest" % NS]


# ---------------------------------------------------------------- 异步
def test_async_pinned_send_reports_the_java_wording_through_the_callback():
    p = _producer()
    client = p._mq_client
    cb = _RecordingCallback()
    msg = Message("TopicTest", b"x")

    p.send_async(msg, cb, mq=MessageQueue("OtherTopic", "broker-a", 0))
    _wait_done(cb)

    assert cb.results == []
    assert len(cb.errors) == 1
    assert isinstance(cb.errors[0], MQClientException)
    assert str(cb.errors[0]) == ASYNC_WORDING
    assert client.topics == [], "被拒绝的发送不该碰到网络"


def test_async_pinned_send_accepts_a_matching_topic():
    p = _producer(NS)
    client = p._mq_client
    cb = _RecordingCallback()
    msg = Message("TopicTest", b"x")

    p.send_async(msg, cb, mq=MessageQueue("TopicTest", "broker-a", 0))
    _wait_done(cb)

    assert cb.errors == []
    assert len(cb.results) == 1
    assert client.topics == ["%s%%TopicTest" % NS]


def test_async_batch_pinned_send_uses_the_async_wording():
    """Java 的批量异步定点发送同样落在 ``:1277-1278`` 那处文案上。"""
    p = _producer()
    client = p._mq_client
    cb = _RecordingCallback()
    msgs = [Message("TopicTest", b"a"), Message("TopicTest", b"b")]

    p.send_async(msgs, cb, mq=MessageQueue("OtherTopic", "broker-a", 0))
    _wait_done(cb)

    assert cb.results == []
    assert len(cb.errors) == 1
    assert str(cb.errors[0]) == ASYNC_WORDING
    assert client.topics == []


# ---------------------------------------------------------------- 按 Java 不设守卫的入口
def test_oneway_pinned_send_has_no_guard_like_java():
    """Java ``sendOneway(Message, MessageQueue):1303-1310`` 只做 state + Validators，
    没有 topic 一致性检查；这里按原样钉住（免得后来的"顺手补上"造成与 Java 不一致）。"""
    p = _producer()
    client = p._mq_client
    msg = Message("TopicTest", b"x")

    p.send_oneway(msg, mq=MessageQueue("OtherTopic", "broker-a", 0))

    assert client.topics == ["TopicTest"]


def test_selector_send_has_no_guard_like_java():
    """选择器入口由选择器自己挑队列（Java ``:729`` 的 ``queueWithNamespace`` 只补前缀），
    没有调用方指定的队列可比。"""
    p = _producer(NS)
    client = p._mq_client
    msg = Message("TopicTest", b"x")

    result = p.send_by_selector(msg, _FirstQueueSelector(), None)

    assert result.send_status == SendStatus.SEND_OK
    assert client.topics == ["%s%%TopicTest" % NS]
