# -*- coding: utf-8 -*-
"""The broker-side batch creation requests: a topic list frame whose attached-but
field-less header still serializes an empty extFields map, a subscription group list
frame whose absent header keeps the map out of the JSON, and neither doing any
client-side validation of the entries."""
from __future__ import annotations

import json

import pytest

from client.admin import DefaultMQAdminExt
from client.exception import MQBrokerException, MQClientException
from common.topic_config import TopicConfig
from remoting.protocol.codes import RequestCode, ResponseCode
from remoting.protocol.remoting_command import RemotingCommand
from remoting.protocol.subscription import SubscriptionGroupConfig

ADDR = "127.0.0.1:10911"
VIP_ADDR = "127.0.0.1:10909"


class _FakeClient:
    def __init__(self, responses=None):
        self._responses = list(responses or [])
        self.requests = []

    def _invoke_sync(self, addr, request, timeout_millis=None):
        self.requests.append((addr, request, timeout_millis))
        if not self._responses:
            raise AssertionError("unexpected extra request: %s" % request.code)
        response = self._responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response

    def _check_response(self, response):
        if response.code == ResponseCode.SUCCESS:
            return response
        raise MQBrokerException(response.code, response.remark or "")


def _ok() -> RemotingCommand:
    r = RemotingCommand()
    r.code = ResponseCode.SUCCESS
    return r


def _fail(code: int, remark: str) -> RemotingCommand:
    r = RemotingCommand()
    r.code = code
    r.remark = remark
    return r


def _admin(client: _FakeClient) -> DefaultMQAdminExt:
    admin = DefaultMQAdminExt()
    admin._mq_client = client
    admin._started = True
    return admin


def _wire(request: RemotingCommand) -> dict:
    return json.loads(request.header_encode())


# ------------------------------------------------------------------ topic list (18)

def test_topic_list_frame_serializes_an_empty_ext_fields_map():
    client = _FakeClient([_ok()])
    admin = _admin(client)

    admin.create_and_update_topic_config_list(ADDR, [
        TopicConfig("BatchTopicA", read_queue_nums=8, write_queue_nums=8),
        TopicConfig("BatchTopicB", read_queue_nums=4, write_queue_nums=4),
    ])

    _, request, timeout = client.requests[0]
    assert request.code == RequestCode.UPDATE_AND_CREATE_TOPIC_LIST
    assert timeout == 20000
    # the attached field-less header must still put the empty map on the wire
    wire = _wire(request)
    assert wire["extFields"] == {}
    assert json.loads(request.body.decode("utf-8")) == {"topicConfigList": [
        {"topicName": "BatchTopicA", "readQueueNums": 8, "writeQueueNums": 8,
         "perm": 6, "topicFilterType": "SINGLE_TAG", "topicSysFlag": 0,
         "order": False, "attributes": {}},
        {"topicName": "BatchTopicB", "readQueueNums": 4, "writeQueueNums": 4,
         "perm": 6, "topicFilterType": "SINGLE_TAG", "topicSysFlag": 0,
         "order": False, "attributes": {}},
    ]}


def test_topic_list_form_validates_nothing():
    # no client-side check of the entries: an empty batch and out-of-range queue
    # numbers go on the wire untouched
    client = _FakeClient([_ok(), _ok()])
    admin = _admin(client)

    admin.create_and_update_topic_config_list(ADDR, [])

    _, request, _ = client.requests[0]
    assert json.loads(request.body.decode("utf-8")) == {"topicConfigList": []}

    odd = TopicConfig("OddTopic", read_queue_nums=-1, write_queue_nums=-1, perm=0)
    odd.topic_filter_type = "NOT_A_FILTER_TYPE"
    admin.create_and_update_topic_config_list(ADDR, [odd])
    _, request, _ = client.requests[1]
    sent = json.loads(request.body.decode("utf-8"))["topicConfigList"][0]
    assert sent["readQueueNums"] == -1
    assert sent["writeQueueNums"] == -1
    assert sent["perm"] == 0
    assert sent["topicFilterType"] == "NOT_A_FILTER_TYPE"


def test_topic_list_form_failure_raises_a_client_exception():
    client = _FakeClient([_fail(ResponseCode.SYSTEM_ERROR, "topic exists")])
    admin = _admin(client)
    with pytest.raises(MQClientException) as exc:
        admin.create_and_update_topic_config_list(ADDR, [TopicConfig("T")])
    assert not isinstance(exc.value, MQBrokerException)
    assert exc.value.response_code == ResponseCode.SYSTEM_ERROR
    assert "topic exists" in str(exc.value)


def test_topic_list_form_rides_the_vip_channel():
    client = _FakeClient([_ok()])
    admin = _admin(client)
    admin.set_vip_channel_enabled(True)
    admin.create_and_update_topic_config_list(ADDR, [TopicConfig("T")])
    assert client.requests[0][0] == VIP_ADDR


# ------------------------------------------------------------------ group list (225)

def _group(name: str) -> SubscriptionGroupConfig:
    cfg = SubscriptionGroupConfig(name)
    return cfg


def test_subscription_group_list_frame_omits_ext_fields_entirely():
    client = _FakeClient([_ok()])
    admin = _admin(client)

    admin.create_and_update_subscription_group_config_list(ADDR, [_group("GID_A")])

    _, request, timeout = client.requests[0]
    assert request.code == RequestCode.UPDATE_AND_CREATE_SUBSCRIPTIONGROUP_LIST
    assert timeout == 20000
    # the null header keeps the extFields key out of the JSON frame
    wire = _wire(request)
    assert "extFields" not in wire
    body = json.loads(request.body.decode("utf-8"))
    assert list(body) == ["groupConfigList"]
    assert body["groupConfigList"][0]["groupName"] == "GID_A"


def test_subscription_group_list_form_accepts_an_empty_batch():
    client = _FakeClient([_ok()])
    admin = _admin(client)
    admin.create_and_update_subscription_group_config_list(ADDR, [])
    _, request, _ = client.requests[0]
    assert json.loads(request.body.decode("utf-8")) == {"groupConfigList": []}


def test_subscription_group_list_form_failure_raises_a_client_exception():
    client = _FakeClient([_fail(ResponseCode.SYSTEM_ERROR, "group exists")])
    admin = _admin(client)
    with pytest.raises(MQClientException) as exc:
        admin.create_and_update_subscription_group_config_list(ADDR, [_group("G")])
    assert not isinstance(exc.value, MQBrokerException)
    assert "group exists" in str(exc.value)


def test_subscription_group_list_form_rides_the_vip_channel():
    client = _FakeClient([_ok()])
    admin = _admin(client)
    admin.set_vip_channel_enabled(True)
    admin.create_and_update_subscription_group_config_list(ADDR, [_group("G")])
    assert client.requests[0][0] == VIP_ADDR
