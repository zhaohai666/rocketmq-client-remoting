# -*- coding: utf-8 -*-
"""Message redelivery: a refused CONSUMER_SEND_MSG_BACK must not lose the message.

Both consumers degrade to re-sending the message as a normal message to
``%RETRY%<group>`` through the internal producer, and only report an error when
that send fails too. The client is faked at the RPC boundary, so what is
asserted is the request/answer handling and the retry message that reaches the
producer -- not an internal call graph.
"""
from __future__ import annotations

import pytest

from client.consumer import DefaultMQPullConsumer, DefaultMQPushConsumer
from client.exception import MQClientException
from common.message import MessageExt, MessageQueue
from common.message_const import MessageConst
from common.mix_all import MixAll
from remoting.protocol.codes import ResponseCode

GROUP = "GID_SendBackFallbackUnit"
TOPIC = "SendBackFallbackTopic"
RETRY_TOPIC = MixAll.get_retry_topic(GROUP)
BROKER = "broker-a"
ADDR = "127.0.0.1:10911"
MSG_ID = "0A0B0C0D0000000000000000000EA7"


class FakeResponse:
    def __init__(self, code):
        self.code = code


class FakePublishInfo:
    def select_one_message_queue(self, *filters):
        return MessageQueue(RETRY_TOPIC, BROKER, 0)


class FakeClient:
    """Records the send-back RPC and the internal producer's normal send."""

    def __init__(self, answer_code=ResponseCode.SUCCESS, rpc_error=None,
                 no_master=False, send_error=None):
        self.answer_code = answer_code
        self.rpc_error = rpc_error
        self.no_master = no_master
        self.send_error = send_error
        self.send_back_requests = []
        self.inner_sends = []
        self.publish_topic = None

    def find_broker_address_in_publish(self, broker_name):
        return None if self.no_master else ADDR

    def vip_addr(self, addr):
        return addr

    def _invoke_sync(self, addr, request, timeout):
        self.send_back_requests.append((addr, request, timeout))
        if self.rpc_error is not None:
            raise self.rpc_error
        return FakeResponse(self.answer_code)

    def _check_response(self, response):
        if response.code != ResponseCode.SUCCESS:
            raise MQClientException("simulated broker answer %s" % response.code)

    def get_topic_publish_info(self, topic, is_default=False):
        self.publish_topic = (topic, is_default)
        return FakePublishInfo()

    def send_message(self, group, msg, mq, timeout, unit_mode=False):
        if self.send_error is not None:
            raise self.send_error
        self.inner_sends.append((group, msg, mq, timeout, unit_mode))


def message(reconsume_times=0) -> MessageExt:
    m = MessageExt(topic=TOPIC, body=b"bounce-me")
    m.queue_id = 0
    m.broker_name = BROKER
    m.queue_offset = 3
    m.commit_log_offset = 1003
    m.msg_id = MSG_ID
    m.reconsume_times = reconsume_times
    # A half message that failed its delivery must lose the marker in the copy.
    m.properties = {MessageConst.PROPERTY_TRANSACTION_PREPARED: "true"}
    return m


def push_consumer(client) -> DefaultMQPushConsumer:
    c = DefaultMQPushConsumer(GROUP)
    c._started = True
    c._mq_client = client
    return c


def pull_consumer(client) -> DefaultMQPullConsumer:
    c = DefaultMQPullConsumer(GROUP)
    c._started = True
    c._mq_client = client
    return c


def assert_retry_send(send):
    group, new_msg, mq, _timeout, _unit_mode = send
    assert group == MixAll.CLIENT_INNER_PRODUCER_GROUP
    assert new_msg.topic == RETRY_TOPIC
    assert new_msg.body == b"bounce-me"
    assert mq.topic == RETRY_TOPIC
    props = new_msg.properties
    assert props[MessageConst.PROPERTY_RETRY_TOPIC] == TOPIC
    assert props[MessageConst.PROPERTY_RECONSUME_TIME] == "1"
    assert props[MessageConst.PROPERTY_MAX_RECONSUME_TIMES] == "16"
    assert props[MessageConst.PROPERTY_DELAY_TIME_LEVEL] == "3"
    assert props[MessageConst.PROPERTY_ORIGIN_MESSAGE_ID] == MSG_ID
    assert MessageConst.PROPERTY_TRANSACTION_PREPARED not in props


# ------------------------------------------------------------------ push


def test_push_refused_answer_falls_back_to_normal_send():
    client = FakeClient(answer_code=ResponseCode.SYSTEM_ERROR)
    c = push_consumer(client)

    assert c.send_message_back(message(), 0) is None

    assert len(client.send_back_requests) == 1
    header = client.send_back_requests[0][1].custom_header
    assert header.max_reconsume_times == 16
    assert len(client.inner_sends) == 1
    assert_retry_send(client.inner_sends[0])


def test_push_transport_error_falls_back_to_normal_send():
    client = FakeClient(rpc_error=RuntimeError("connection reset"))
    c = push_consumer(client)

    assert c.send_message_back(message(), 0) is None

    assert len(client.inner_sends) == 1
    assert_retry_send(client.inner_sends[0])


def test_push_missing_master_falls_back_to_normal_send():
    client = FakeClient(no_master=True)
    c = push_consumer(client)

    assert c.send_message_back(message(), 0) is None

    assert client.send_back_requests == []
    assert len(client.inner_sends) == 1
    assert_retry_send(client.inner_sends[0])


def test_push_fallback_failure_raises_the_original_error():
    client = FakeClient(no_master=True, send_error=RuntimeError("broker unreachable"))
    c = push_consumer(client)

    with pytest.raises(MQClientException) as caught:
        c.send_message_back(message(), 0)
    assert "master node does not exist" in str(caught.value)


# ------------------------------------------------------------------ pull


def test_pull_refused_answer_falls_back_to_normal_send():
    client = FakeClient(answer_code=ResponseCode.SYSTEM_ERROR)
    c = pull_consumer(client)

    assert c.send_message_back(message(), 0) is None

    addr, request, timeout = client.send_back_requests[0]
    assert addr == ADDR
    assert timeout == 3000
    assert request.custom_header.max_reconsume_times == 16
    assert len(client.inner_sends) == 1
    assert_retry_send(client.inner_sends[0])


def test_pull_max_reconsume_times_knob_reaches_the_header():
    client = FakeClient()
    c = pull_consumer(client)
    assert c.get_max_reconsume_times() == 16

    c.set_max_reconsume_times(5)
    c.send_message_back(message(), 0)
    assert client.send_back_requests[-1][1].custom_header.max_reconsume_times == 5

    # -1 means "unlimited": the broker's own retryMaxTimes decides, so 16 is sent.
    c.set_max_reconsume_times(-1)
    c.send_message_back(message(), 0)
    assert client.send_back_requests[-1][1].custom_header.max_reconsume_times == 16


def test_pull_missing_master_falls_back_to_normal_send():
    client = FakeClient(no_master=True)
    c = pull_consumer(client)

    assert c.send_message_back(message(), 0) is None

    assert client.send_back_requests == []
    assert len(client.inner_sends) == 1
    assert_retry_send(client.inner_sends[0])
