# -*- coding: utf-8 -*-
"""The lite-topic admin family: one shared invocation with VIP routing, headers that
carry primitive fields even at their default value, and bodies decoded only when the
broker sent one."""
from __future__ import annotations

import pytest

from client.admin import DefaultMQAdminExt
from client.exception import MQBrokerException
from remoting.protocol.admin_body import (ClientGroup, GetBrokerLiteInfoResponseBody,
                                          GetLiteClientInfoResponseBody,
                                          GetLiteGroupInfoResponseBody,
                                          GetLiteTopicInfoResponseBody,
                                          GetParentTopicInfoResponseBody,
                                          LiteLagInfo, OffsetWrapper, TopicOffset)
from remoting.protocol.codes import RequestCode, ResponseCode
from remoting.protocol.remoting_command import RemotingCommand
from remoting.protocol.serialize import RemotingSerializable

ADDR = "127.0.0.1:10911"
VIP_ADDR = "127.0.0.1:10909"
TOPIC = "LiteParent"
LITE = "LiteParent_0"
GROUP = "GID_Lite"


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


def _ok(body: bytes = None) -> RemotingCommand:
    r = RemotingCommand()
    r.code = ResponseCode.SUCCESS
    r.body = body
    return r


def _admin(client: _FakeClient) -> DefaultMQAdminExt:
    admin = DefaultMQAdminExt()
    admin._mq_client = client
    admin._started = True
    return admin


def _last(client: _FakeClient):
    return client.requests[-1]


# ------------------------------------------------------------------ the shared invocation

def test_lite_family_rides_the_vip_channel_when_the_switch_is_on():
    client = _FakeClient([_ok() for _ in range(6)])
    admin = _admin(client)
    admin.set_vip_channel_enabled(True)

    admin.get_broker_lite_info(ADDR)
    assert _last(client)[0] == VIP_ADDR
    admin.get_parent_topic_info(ADDR, TOPIC)
    assert _last(client)[0] == VIP_ADDR
    admin.get_lite_topic_info(ADDR, TOPIC)
    assert _last(client)[0] == VIP_ADDR
    admin.get_lite_client_info(ADDR, TOPIC, GROUP)
    assert _last(client)[0] == VIP_ADDR
    admin.get_lite_group_info(ADDR, GROUP)
    assert _last(client)[0] == VIP_ADDR
    admin.trigger_lite_dispatch(ADDR, GROUP)
    assert _last(client)[0] == VIP_ADDR


def test_lite_family_stays_on_the_plain_port_with_the_switch_off():
    client = _FakeClient([_ok() for _ in range(6)])
    admin = _admin(client)

    admin.get_broker_lite_info(ADDR)
    admin.get_parent_topic_info(ADDR, TOPIC)
    admin.get_lite_topic_info(ADDR, TOPIC)
    admin.get_lite_client_info(ADDR, TOPIC, GROUP)
    admin.get_lite_group_info(ADDR, GROUP)
    admin.trigger_lite_dispatch(ADDR, GROUP)

    assert {addr for addr, _, _ in client.requests} == {ADDR}


def test_lite_family_uses_the_admin_timeout():
    client = _FakeClient([_ok()])
    admin = _admin(client)
    admin.get_broker_lite_info(ADDR)
    assert _last(client)[2] == 20000


def test_lite_failure_raises_a_broker_exception_carrying_the_address():
    failed = RemotingCommand()
    failed.code = ResponseCode.SYSTEM_ERROR
    failed.remark = "lite store disabled"
    client = _FakeClient([failed])
    admin = _admin(client)
    admin.set_vip_channel_enabled(True)

    with pytest.raises(MQBrokerException) as exc:
        admin.get_parent_topic_info(ADDR, TOPIC)

    assert exc.value.response_code == ResponseCode.SYSTEM_ERROR
    assert exc.value.error_message == "lite store disabled"
    assert exc.value.addr == ADDR


# ------------------------------------------------------------------ per-method wire shapes

def test_get_broker_lite_info_sends_no_header_and_decodes_the_body():
    body = GetBrokerLiteInfoResponseBody()
    body.store_type = "ROCKSDB"
    body.max_lmq_num = 1000000
    body.current_lmq_num = 7
    body.lite_subscription_count = 3
    body.order_info_count = 1
    body.cq_table_size = 11
    body.offset_table_size = 12
    body.event_map_size = 13
    client = _FakeClient([_ok(body.encode())])
    admin = _admin(client)

    result = admin.get_broker_lite_info(ADDR)

    _, request, _ = _last(client)
    assert request.code == RequestCode.GET_BROKER_LITE_INFO
    assert request.ext_fields == {}
    assert result.store_type == "ROCKSDB"
    assert result.max_lmq_num == 1000000
    assert result.current_lmq_num == 7
    assert result.lite_subscription_count == 3
    assert result.order_info_count == 1
    assert result.cq_table_size == 11
    assert result.offset_table_size == 12
    assert result.event_map_size == 13


def test_get_parent_topic_info_ext_and_body():
    body = GetParentTopicInfoResponseBody()
    body.topic = TOPIC
    body.ttl = 4000
    body.groups = {GROUP}
    body.lmq_num = 2
    body.lite_topic_count = 5
    client = _FakeClient([_ok(body.encode())])
    admin = _admin(client)

    result = admin.get_parent_topic_info(ADDR, TOPIC)

    _, request, _ = _last(client)
    assert request.code == RequestCode.GET_PARENT_TOPIC_INFO
    assert request.ext_fields == {"topic": TOPIC}
    assert result.topic == TOPIC
    assert result.ttl == 4000
    assert result.groups == {GROUP}
    assert result.lmq_num == 2
    assert result.lite_topic_count == 5


def test_get_lite_topic_info_sends_the_optional_lite_topic_only_when_set():
    client = _FakeClient([_ok(), _ok()])
    admin = _admin(client)

    admin.get_lite_topic_info(ADDR, TOPIC)
    _, request, _ = _last(client)
    assert request.code == RequestCode.GET_LITE_TOPIC_INFO
    assert request.ext_fields == {"parentTopic": TOPIC}

    admin.get_lite_topic_info(ADDR, TOPIC, lite_topic=LITE)
    _, request, _ = _last(client)
    assert request.ext_fields == {"parentTopic": TOPIC, "liteTopic": LITE}


def test_get_lite_topic_info_decodes_subscribers_and_offsets():
    body = GetLiteTopicInfoResponseBody()
    body.parent_topic = TOPIC
    body.lite_topic = LITE
    body.subscriber = {ClientGroup("cid-1", GROUP)}
    body.topic_offset = TopicOffset(min_offset=3, max_offset=9)
    body.sharding_to_broker = True
    client = _FakeClient([_ok(body.encode())])
    admin = _admin(client)

    result = admin.get_lite_topic_info(ADDR, TOPIC, lite_topic=LITE)

    assert result.parent_topic == TOPIC
    assert result.lite_topic == LITE
    assert result.subscriber == {ClientGroup("cid-1", GROUP)}
    assert result.topic_offset.min_offset == 3
    assert result.topic_offset.max_offset == 9
    assert result.sharding_to_broker is True


def test_get_lite_client_info_always_carries_the_max_count_default():
    client = _FakeClient([_ok(), _ok()])
    admin = _admin(client)

    admin.get_lite_client_info(ADDR, TOPIC, GROUP)
    _, request, _ = _last(client)
    assert request.code == RequestCode.GET_LITE_CLIENT_INFO
    # maxCount is a primitive int: the default 1000 is a real wire value
    assert request.ext_fields == {"parentTopic": TOPIC, "group": GROUP,
                                  "maxCount": "1000"}

    admin.get_lite_client_info(ADDR, TOPIC, GROUP, client_id="cid-9")
    _, request, _ = _last(client)
    assert request.ext_fields == {"parentTopic": TOPIC, "group": GROUP,
                                  "clientId": "cid-9", "maxCount": "1000"}


def test_get_lite_client_info_decodes_the_body():
    body = GetLiteClientInfoResponseBody()
    body.parent_topic = TOPIC
    body.group = GROUP
    body.client_id = "cid-1"
    body.last_access_time = 111
    body.last_consume_time = 222
    body.lite_topic_count = 4
    body.lite_topic_set = {LITE}
    client = _FakeClient([_ok(body.encode())])
    admin = _admin(client)

    result = admin.get_lite_client_info(ADDR, TOPIC, GROUP)

    assert result.parent_topic == TOPIC
    assert result.client_id == "cid-1"
    assert result.last_access_time == 111
    assert result.last_consume_time == 222
    assert result.lite_topic_count == 4
    assert result.lite_topic_set == {LITE}


def test_get_lite_group_info_always_carries_top_k_and_decodes_the_body():
    body = GetLiteGroupInfoResponseBody()
    body.group = GROUP
    body.parent_topic = TOPIC
    body.lite_topic = LITE
    body.earliest_unconsumed_timestamp = 12345
    body.total_lag_count = 42
    body.lite_topic_offset_wrapper = OffsetWrapper(broker_offset=50, consumer_offset=8)
    body.lag_count_top_k = [LiteLagInfo(LITE, 7, 99)]
    body.lag_timestamp_top_k = [LiteLagInfo(LITE, 2, 12345)]
    client = _FakeClient([_ok(body.encode())])
    admin = _admin(client)

    result = admin.get_lite_group_info(ADDR, GROUP, lite_topic=LITE, top_k=8)

    _, request, _ = _last(client)
    assert request.code == RequestCode.GET_LITE_GROUP_INFO
    assert request.ext_fields == {"group": GROUP, "liteTopic": LITE, "topK": "8"}
    assert result.group == GROUP
    assert result.parent_topic == TOPIC
    assert result.total_lag_count == 42
    assert result.earliest_unconsumed_timestamp == 12345
    assert result.lite_topic_offset_wrapper.consumer_offset == 8
    assert result.lag_count_top_k == [LiteLagInfo(LITE, 7, 99)]
    assert result.lag_timestamp_top_k == [LiteLagInfo(LITE, 2, 12345)]


def test_get_lite_group_info_sends_zero_top_k_without_a_filter():
    client = _FakeClient([_ok()])
    admin = _admin(client)
    admin.get_lite_group_info(ADDR, GROUP)
    _, request, _ = _last(client)
    # topK is a primitive int: 0 is a real wire value, liteTopic stays omitted
    assert request.ext_fields == {"group": GROUP, "topK": "0"}


def test_trigger_lite_dispatch_is_void_with_an_optional_client_id():
    client = _FakeClient([_ok(), _ok()])
    admin = _admin(client)

    admin.trigger_lite_dispatch(ADDR, GROUP)
    _, request, _ = _last(client)
    assert request.code == RequestCode.TRIGGER_LITE_DISPATCH
    assert request.ext_fields == {"group": GROUP}

    admin.trigger_lite_dispatch(ADDR, GROUP, client_id="cid-1")
    _, request, _ = _last(client)
    assert request.ext_fields == {"group": GROUP, "clientId": "cid-1"}


# ------------------------------------------------------------------ null-body semantics

def test_lite_reads_answer_none_when_the_broker_sends_no_body():
    client = _FakeClient([_ok(), _ok(), _ok(), _ok(), _ok()])
    admin = _admin(client)
    assert admin.get_broker_lite_info(ADDR) is None
    assert admin.get_parent_topic_info(ADDR, TOPIC) is None
    assert admin.get_lite_topic_info(ADDR, TOPIC) is None
    assert admin.get_lite_client_info(ADDR, TOPIC, GROUP) is None
    assert admin.get_lite_group_info(ADDR, GROUP) is None


def test_lite_body_decoders_reject_nothing_on_null_input():
    assert GetBrokerLiteInfoResponseBody.decode(None) is None
    assert GetParentTopicInfoResponseBody.decode(None) is None
    assert GetLiteTopicInfoResponseBody.decode(None) is None
    assert GetLiteClientInfoResponseBody.decode(None) is None
    assert GetLiteGroupInfoResponseBody.decode(None) is None
    assert GetBrokerLiteInfoResponseBody.decode(b"") is None


# ------------------------------------------------------------------ dto wire shapes

def test_client_group_is_a_value_type():
    a = ClientGroup("cid", "g")
    b = ClientGroup("cid", "g")
    assert a == b
    assert hash(a) == hash(b)
    assert a != ClientGroup("cid", "other")
    wire = RemotingSerializable.encode(a.to_dict())
    assert RemotingSerializable.decode_json(wire) == {"clientId": "cid", "group": "g"}


def test_lite_lag_info_omits_nothing_but_the_optional_timestamp():
    wire = RemotingSerializable.encode(LiteLagInfo(LITE, 5).to_dict())
    d = RemotingSerializable.decode_json(wire)
    assert d == {"lagCount": 5, "earliestUnconsumedTimestamp": -1, "liteTopic": LITE}
    assert LiteLagInfo.from_dict(d) == LiteLagInfo(LITE, 5, -1)


def test_client_group_decodes_from_partial_maps():
    assert ClientGroup.from_dict({"clientId": "c"}) == ClientGroup("c", None)
    assert ClientGroup.from_dict(None) == ClientGroup(None, None)
