# -*- coding: utf-8 -*-
from __future__ import annotations

from client.admin import DefaultMQAdminExt
from client.exception import MQBrokerException, MQClientException
from common.message import MessageQueue
from common.mix_all import MixAll
from remoting.protocol.body import ResetOffsetBody, TopicList
from remoting.protocol.codes import LanguageCode, RequestCode, ResponseCode
from remoting.protocol.remoting_command import RemotingCommand
from remoting.protocol.route import BrokerData, TopicRouteData

GROUP = "GID_AdminResetUnit"
TOPIC = "AdminResetUnitTopic"
ADDR_A = "127.0.0.1:10911"
ADDR_B = "127.0.0.1:10912"
GROUP_RETRY = MixAll.get_retry_topic(GROUP)


# ------------------------------------------------------------------ Test doubles

def _ok(body: bytes = b"") -> RemotingCommand:
    r = RemotingCommand()
    r.code = ResponseCode.SUCCESS
    r.body = body
    return r


def _err(code: int, remark: str) -> RemotingCommand:
    r = RemotingCommand()
    r.code = code
    r.remark = remark
    return r


class _FakeClient:
    """Only replaces the three entry points used by admin: synchronous call, offset write, and route lookup."""

    def __init__(self, responses, route=None):
        self._responses = list(responses)
        self.requests = []        # [(addr, RemotingCommand)]
        self.offset_updates = []  # [(group, mq, offset, addr)]
        self.route_lookups = []   # [topic]
        self.route = route

    def _invoke_sync(self, addr, request, timeout_millis=None):
        self.requests.append((addr, request))
        if not self._responses:
            raise AssertionError("unexpected extra request: %s" % request.code)
        response = self._responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response

    def update_consumer_offset(self, consumer_group, mq, offset,
                               timeout_millis=5000, addr=None):
        self.offset_updates.append((consumer_group, mq, offset, addr))

    def get_topic_route_data(self, topic, timeout_millis=None):
        self.route_lookups.append(topic)
        return self.route

    def _check_response(self, response):
        """Same semantics as `MQClientInstance._check_response:524`: raise MQBrokerException on non-SUCCESS."""
        if response.code == ResponseCode.SUCCESS:
            return response
        raise MQBrokerException(response.code, response.remark or "")


def _admin(client: _FakeClient) -> DefaultMQAdminExt:
    admin = DefaultMQAdminExt()
    # No threads, no cluster connection: just inject the client so the method under test reaches the protocol encoding layer
    admin._mq_client = client
    admin._started = True
    return admin


def _route(*broker_names_and_addrs) -> TopicRouteData:
    route = TopicRouteData()
    for name, addr in broker_names_and_addrs:
        route.broker_datas.append(BrokerData("DefaultCluster", name, {0: addr}))
    return route


def _reset_body(mq: MessageQueue, offset: int) -> bytes:
    body = ResetOffsetBody()
    body.offset_table[mq] = offset
    return body.encode()


# ------------------------------------------------------------------ 222 encoding

def test_reset_offset_by_queue_id_sends_update_then_222_with_queue_id():
    pass
    target_mq = MessageQueue(TOPIC, "", 3)
    # The first call (25) is swallowed by the double at the client.update_consumer_offset layer; only the 222 response remains in the queue
    client = _FakeClient([_ok(_reset_body(MessageQueue(TOPIC, "broker-a", 3), 7))])
    admin = _admin(client)

    result = admin.reset_offset_by_queue_id(ADDR_A, GROUP, TOPIC, 3, 7)

    # First call: 25 is issued by MQClientInstance and must be sent explicitly to the given broker
    assert client.offset_updates == [(GROUP, target_mq, 7, ADDR_A)]
    # Second call: 222
    assert len(client.requests) == 1
    addr, request = client.requests[0]
    assert addr == ADDR_A
    assert request.code == RequestCode.INVOKE_BROKER_TO_RESET_OFFSET
    assert request.ext_fields["queueId"] == "3"
    assert request.ext_fields["offset"] == "7"
    assert request.ext_fields["timestamp"] == "0"
    assert request.ext_fields["isForce"] == "false"
    assert "force" not in request.ext_fields
    assert request.ext_fields["topic"] == TOPIC
    assert request.ext_fields["group"] == GROUP
    assert result == {MessageQueue(TOPIC, "broker-a", 3): 7}


def test_reset_offset_by_timestamp_omits_queue_id_and_keeps_map_body_language():
    pass
    client = _FakeClient([_ok(_reset_body(MessageQueue(TOPIC, "broker-a", 0), 9))],
                         route=_route(("broker-a", ADDR_A)))
    admin = _admin(client)

    offsets = admin.reset_offset_by_timestamp(TOPIC, GROUP, 1234567890, True)

    addr, request = client.requests[0]
    assert addr == ADDR_A
    assert "queueId" not in request.ext_fields
    assert request.ext_fields["offset"] == "-1"
    assert request.ext_fields["timestamp"] == "1234567890"
    assert request.ext_fields["isForce"] == "true"
    assert "force" not in request.ext_fields
    assert request.language == LanguageCode.PYTHON
    assert request.language != LanguageCode.CPP
    assert offsets and list(offsets.values()) == [9]
    # The route is looked up by the business topic (LMQ/wheel_timer use clusterName instead)
    assert client.route_lookups == [TOPIC]


def test_reset_offset_by_timestamp_can_opt_into_the_cpp_body():
    """``is_cpp=True`` still overrides language to CPP (only pure C++ consumer groups need the ForC array body);
    this is a negative control against the "default no-override" behavior above, preventing the switch from being casually removed someday."""
    client = _FakeClient([_ok(_reset_body(MessageQueue(TOPIC, "broker-a", 0), 9))],
                         route=_route(("broker-a", ADDR_A)))
    admin = _admin(client)

    admin.reset_offset_by_timestamp(TOPIC, GROUP, 1234567890, True, is_cpp=True)

    _, request = client.requests[0]
    assert request.language == LanguageCode.CPP


def test_reset_offset_by_queue_id_propagates_broker_remark():
    """Out-of-range offsets are rejected by the broker's `resetOffsetInner` (real-machine remark:
    `Target offset 102 not in consume queue range [0-2]`); the code+remark must be carried through as-is,
    instead of being swallowed into "reset succeeded but empty table".
    """
    client = _FakeClient([
        _err(ResponseCode.SYSTEM_ERROR, "Target offset 102 not in consume queue range [0-2]"),
    ])
    admin = _admin(client)

    try:
        admin.reset_offset_by_queue_id(ADDR_A, GROUP, TOPIC, 0, 102)
        raise AssertionError("expected MQClientException")
    except MQClientException as e:
        assert e.response_code == ResponseCode.SYSTEM_ERROR
        assert "not in consume queue range" in str(e)
    assert len(client.offset_updates) == 1


def test_reset_offset_by_timestamp_requires_at_least_one_offset_table():
    pass
    client = _FakeClient([_ok(b"")], route=_route(("broker-a", ADDR_A)))
    admin = _admin(client)
    try:
        admin.reset_offset_by_timestamp(TOPIC, GROUP, 1, True)
        raise AssertionError("expected MQClientException")
    except MQClientException:
        pass


# ---------------------------------------------------------- 343 queryTopicsByConsumer

def _topic_list_body(topics) -> bytes:
    tl = TopicList()
    tl.topic_list = list(topics)
    tl.broker_addr = ADDR_A
    return tl.encode()


def test_query_topics_by_consumer_to_broker_uses_343_with_group():
    client = _FakeClient([_ok(_topic_list_body([TOPIC, GROUP_RETRY]))])
    admin = _admin(client)

    result = admin.query_topics_by_consumer_to_broker(ADDR_A, GROUP)

    addr, request = client.requests[0]
    assert (addr, request.code) == (ADDR_A, RequestCode.QUERY_TOPICS_BY_CONSUMER)
    assert request.ext_fields == {"group": GROUP}
    assert TOPIC in result.get_topic_list()


def test_query_topics_by_consumer_routes_by_retry_topic_and_dedupes():
    pass
    client = _FakeClient(
        [_ok(_topic_list_body([TOPIC, GROUP_RETRY])), _ok(_topic_list_body([TOPIC]))],
        route=_route(("broker-a", ADDR_A), ("broker-b", ADDR_B)))
    admin = _admin(client)

    result = admin.query_topics_by_consumer(GROUP)

    assert client.route_lookups == [GROUP_RETRY]
    assert [addr for addr, _ in client.requests] == [ADDR_A, ADDR_B]
    assert sorted(result.get_topic_list()) == sorted({TOPIC, GROUP_RETRY})


def test_query_topics_by_consumer_returns_empty_topic_list_when_group_has_no_offset():
    """The broker's `whichTopicByConsumer` reads the offset table: a group that never committed offsets returns an empty list instead of an error."""
    client = _FakeClient([_ok(_topic_list_body([]))], route=_route(("broker-a", ADDR_A)))
    admin = _admin(client)

    assert admin.query_topics_by_consumer(GROUP).get_topic_list() == []
