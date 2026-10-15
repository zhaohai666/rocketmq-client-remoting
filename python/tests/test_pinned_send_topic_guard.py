# -*- coding: utf-8 -*-
from __future__ import annotations

import time
from typing import List, Optional

import pytest

from client.exception import MQClientException
from client.mq_client import TopicPublishInfo
from client.producer import DefaultMQProducer, MessageQueueSelector, SendCallback
from client.send_result import SendResult, SendStatus
from common.message import Message, MessageQueue
from remoting.protocol.codes import RequestCode
from remoting.protocol.remoting_command import RemotingCommand

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
    """MQClientInstance double: only records "the topic the broker sees at registration", never touches the network.

    The guard must run before the request is actually built, so the rejected send must leave **nothing** in this record.
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

    def find_broker_address_in_publish(self, broker_name):
        return ADDR

    def publish_addr_for(self, broker_name, topic):
        return self.find_broker_address_in_publish(broker_name)

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
    # start() builds a real client; here only what the send chain needs is filled in: the started marker + two pools
    p._create_async_executors()
    p._mq_client = _RecordingClient()
    p._started = True
    return p


def _wait_done(cb: _RecordingCallback, timeout: float = 5.0) -> None:
    deadline = time.monotonic() + timeout
    while not cb.done and time.monotonic() < deadline:
        time.sleep(0.005)
    assert cb.done, "the callback did not finish within the timeout"


# ---------------------------------------------------------------- Sync
def test_sync_pinned_send_rejects_a_mismatched_topic():
    p = _producer()
    client = p._mq_client
    msg = Message("TopicTest", b"x")

    with pytest.raises(MQClientException) as ei:
        p.send(msg, mq=MessageQueue("OtherTopic", "broker-a", 0))

    assert str(ei.value) == SYNC_WORDING
    assert ei.value.response_code is None
    assert client.topics == [], "a rejected send must not touch the network"
    assert msg.topic == "TopicTest" and msg.body == b"x"


def test_sync_pinned_send_accepts_a_matching_topic():
    """Negative control: matching topics (no namespace set) must be sent as usual."""
    p = _producer()
    client = p._mq_client
    msg = Message("TopicTest", b"x")

    result = p.send(msg, mq=MessageQueue("TopicTest", "broker-a", 0))

    assert result.send_status == SendStatus.SEND_OK
    assert client.topics == ["TopicTest"]


def test_namespace_does_not_false_reject_the_pinned_send():
    """With a namespace, each side is wrapped separately, but it is still the same pair of resource names being compared.

    All three input combinations must pass: a bare queue topic, an already-prefixed one, and a prefixed queue obtained from the route.
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
    """The message topic and the queue topic are equally mismatched under different namespaces (not "anything prefixed passes")."""
    p = _producer(NS)
    msg = Message("TopicTest", b"x")

    with pytest.raises(MQClientException) as ei:
        p.send(msg, mq=MessageQueue("ns2%TopicTest", "broker-a", 0))

    assert str(ei.value) == SYNC_WORDING


# ---------------------------------------------------------------- Batch
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


# ---------------------------------------------------------------- Async
def test_async_pinned_send_reports_the_protocol_wording_through_the_callback():
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
    assert client.topics == [], "a rejected send must not touch the network"


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
    pass
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


def test_oneway_pinned_send_skips_the_guard():
    pass
    p = _producer()
    client = p._mq_client
    msg = Message("TopicTest", b"x")

    p.send_oneway(msg, mq=MessageQueue("OtherTopic", "broker-a", 0))

    assert client.topics == ["TopicTest"]


def test_selector_send_skips_the_guard():
    pass
    p = _producer(NS)
    client = p._mq_client
    msg = Message("TopicTest", b"x")

    result = p.send_by_selector(msg, _FirstQueueSelector(), None)

    assert result.send_status == SendStatus.SEND_OK
    assert client.topics == ["%s%%TopicTest" % NS]
