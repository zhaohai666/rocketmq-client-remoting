# -*- coding: utf-8 -*-
"""The concurrent fan-out admin tools: route discovery with fallback order, latch-bounded
per-broker execution whose task failures stay contained, TPS accumulation, and the
AdminToolResult envelope codes."""
from __future__ import annotations

import threading
import time

import pytest

from client.admin import DefaultMQAdminExt, MessageTrack, TrackType
from client.exception import MQBrokerException, MQClientException
from common.message import MessageExt, MessageQueue
from common.mix_all import MixAll
from remoting.protocol.admin_body import (AdminToolsResultCodeEnum, ConsumeStats,
                                          OffsetWrapper, TopicOffset, TopicStatsTable)
from remoting.protocol.body import ConsumerConnection, TopicList
from remoting.protocol.codes import RequestCode, ResponseCode
from remoting.protocol.remoting_command import RemotingCommand
from remoting.protocol.serialize import RemotingSerializable
from remoting.protocol.route import BrokerData, QueueData, TopicRouteData

GROUP = "GID_Fanout"
TOPIC = "FanoutTopic"
RETRY_TOPIC = MixAll.get_retry_topic(GROUP)
POP_RETRY_TOPIC = MixAll.build_pop_retry_topic(TOPIC, GROUP)
ADDR_A = "127.0.0.1:10911"
ADDR_B = "127.0.0.1:10912"
NS_ADDR = "127.0.0.1:9876"


def _ok(body: bytes = None, remark: str = None) -> RemotingCommand:
    r = RemotingCommand()
    r.code = ResponseCode.SUCCESS
    r.body = body
    r.remark = remark
    return r


def _fail(code: int, remark: str = "failed") -> RemotingCommand:
    r = RemotingCommand()
    r.code = code
    r.remark = remark
    return r


class _FakeClient:
    """Thread-safe double: route discovery is table driven, every RPC goes through a
    per-(address, code) responder, and broker-side mutations are recorded."""

    def __init__(self, routes=None, responder=None):
        self.lock = threading.RLock()
        self.requests = []            # (addr, request, timeout)
        self.attempted_routes = []    # route discovery order
        self.topic_route_table = {}
        self.topic_route_lock = threading.RLock()
        self._routes = routes or {}
        self._responder = responder
        self.delete_behavior = {}     # addr -> exception to raise
        self.deleted = []             # (addr, topic)

    def update_topic_route_info_from_name_server(self, topic, timeout_millis=None,
                                                 is_default=False):
        with self.lock:
            self.attempted_routes.append(topic)
        route = self._routes.get(topic)
        if route is None:
            return False
        self.topic_route_table[topic] = route
        return True

    def get_topic_route_data(self, topic, timeout_millis=None):
        return self._routes.get(topic)

    def _invoke_sync(self, addr, request, timeout_millis=None):
        with self.lock:
            self.requests.append((addr, request, timeout_millis))
        if self._responder is None:
            return _ok()
        return self._responder(addr, request)

    def _check_response(self, response):
        if response.code == ResponseCode.SUCCESS:
            return response
        raise MQBrokerException(response.code, response.remark or "")

    def delete_topic_in_broker(self, addr, topic, timeout_millis=5000):
        with self.lock:
            self.deleted.append((addr, topic))
        err = self.delete_behavior.get(addr)
        if err is not None:
            raise err


def _admin(client: _FakeClient) -> DefaultMQAdminExt:
    admin = DefaultMQAdminExt()
    admin._mq_client = client
    admin._started = True
    return admin


def _route(*broker_names_and_addrs, read_queue_nums=8) -> TopicRouteData:
    route = TopicRouteData()
    for name, addr in broker_names_and_addrs:
        route.broker_datas.append(BrokerData("DefaultCluster", name, {0: addr}))
        route.queue_datas.append(QueueData(name, read_queue_nums, read_queue_nums))
    return route


def _stats_table(tps: float, *queue_specs) -> TopicStatsTable:
    table = TopicStatsTable()
    table.topic_put_tps = tps
    for topic, broker, queue_id, max_offset in queue_specs:
        table.offset_table[MessageQueue(topic, broker, queue_id)] = TopicOffset(
            min_offset=0, max_offset=max_offset)
    return table


def _consume_stats(tps: float, *queue_specs) -> ConsumeStats:
    stats = ConsumeStats()
    stats.consume_tps = tps
    for topic, broker, queue_id, broker_offset, consumer_offset in queue_specs:
        stats.offset_table[MessageQueue(topic, broker, queue_id)] = OffsetWrapper(
            broker_offset=broker_offset, consumer_offset=consumer_offset)
    return stats


def _responder_by_code(handlers):
    """handlers: {code: fn(addr, request) -> RemotingCommand} with a default ok."""
    def _respond(addr, request):
        handler = handlers.get(request.code)
        if handler is None:
            return _ok()
        return handler(addr, request)
    return _respond


# ------------------------------------------------------------------ topic stats

def test_examine_topic_stats_concurrent_merges_offsets_and_sums_tps():
    handlers = {
        RequestCode.GET_TOPIC_STATS_INFO: lambda addr, req: _ok(
            _stats_table(1.5, (TOPIC, "broker-a", 0, 100)).encode()
            if addr == ADDR_A else _stats_table(2.5, (TOPIC, "broker-b", 0, 60)).encode()),
    }
    client = _FakeClient(routes={TOPIC: _route(("broker-a", ADDR_A), ("broker-b", ADDR_B))},
                         responder=_responder_by_code(handlers))
    admin = _admin(client)

    result = admin.examine_topic_stats_concurrent(TOPIC)

    assert result.success is True
    assert result.code == AdminToolsResultCodeEnum.SUCCESS
    table = result.data
    assert MessageQueue(TOPIC, "broker-a", 0) in table.offset_table
    assert MessageQueue(TOPIC, "broker-b", 0) in table.offset_table
    assert table.offset_table[MessageQueue(TOPIC, "broker-b", 0)].max_offset == 60
    assert table.topic_put_tps == pytest.approx(4.0)
    assert {addr for addr, req, _ in client.requests
            if req.code == RequestCode.GET_TOPIC_STATS_INFO} == {ADDR_A, ADDR_B}


def test_examine_topic_stats_concurrent_without_route_is_a_success_with_empty_table():
    client = _FakeClient()
    result = _admin(client).examine_topic_stats_concurrent(TOPIC)
    assert result.success is True
    assert result.data.offset_table == {}
    assert client.attempted_routes == [TOPIC]
    assert client.requests == []


def test_examine_topic_stats_concurrent_contains_a_broker_failure():
    handlers = {
        RequestCode.GET_TOPIC_STATS_INFO: lambda addr, req: (
            _ok(_stats_table(1.0, (TOPIC, "broker-a", 0, 100)).encode())
            if addr == ADDR_A else _fail(ResponseCode.SYSTEM_ERROR, "stats offline")),
    }
    client = _FakeClient(routes={TOPIC: _route(("broker-a", ADDR_A), ("broker-b", ADDR_B))},
                         responder=_responder_by_code(handlers))
    result = _admin(client).examine_topic_stats_concurrent(TOPIC)
    # the failing broker is dropped, the healthy one still lands in the table
    assert result.success is True
    assert list(result.data.offset_table) == [MessageQueue(TOPIC, "broker-a", 0)]
    assert result.data.topic_put_tps == pytest.approx(1.0)


def test_examine_topic_stats_concurrent_maps_transport_errors_to_the_remote_code():
    from remoting.exception import RemotingConnectException

    client = _FakeClient()
    client.update_topic_route_info_from_name_server = lambda *a, **k: (
        lambda: (_ for _ in ()).throw(RemotingConnectException("ns down")))()
    result = _admin(client).examine_topic_stats_concurrent(TOPIC)
    assert result.success is False
    assert result.code == AdminToolsResultCodeEnum.REMOTING_ERROR


# ------------------------------------------------------------------ consume stats

def test_examine_consume_stats_concurrent_prefers_the_retry_topic_route():
    handlers = {
        RequestCode.GET_CONSUME_STATS: lambda addr, req: _ok(
            _consume_stats(1.5, (TOPIC, "broker-a", 0, 100, 90)).encode()),
    }
    client = _FakeClient(
        routes={RETRY_TOPIC: _route(("broker-a", ADDR_A))},
        responder=_responder_by_code(handlers))

    result = _admin(client).examine_consume_stats_concurrent(GROUP, topic=TOPIC)

    assert result.success is True
    assert client.attempted_routes == [RETRY_TOPIC]  # the first hit short-circuits
    assert result.data.consume_tps == pytest.approx(1.5)
    assert result.data.offset_table[MessageQueue(TOPIC, "broker-a", 0)].consumer_offset == 90


def test_examine_consume_stats_concurrent_falls_back_to_topic_then_pop_retry():
    handlers = {
        RequestCode.GET_CONSUME_STATS: lambda addr, req: _ok(
            _consume_stats(2.0, (TOPIC, "broker-b", 0, 50, 50)).encode()),
    }
    client = _FakeClient(
        routes={TOPIC: _route(("broker-b", ADDR_B))},
        responder=_responder_by_code(handlers))

    result = _admin(client).examine_consume_stats_concurrent(GROUP, topic=TOPIC)

    assert client.attempted_routes == [RETRY_TOPIC, TOPIC]  # pop-retry never probed
    assert result.success is True


def test_examine_consume_stats_concurrent_without_any_route_reports_route_not_exist():
    client = _FakeClient()
    result = _admin(client).examine_consume_stats_concurrent(GROUP, topic=TOPIC)
    assert result.success is False
    assert result.code == AdminToolsResultCodeEnum.TOPIC_ROUTE_INFO_NOT_EXIST
    assert result.error_msg == "topic router info not found"
    assert client.attempted_routes == [RETRY_TOPIC, TOPIC, POP_RETRY_TOPIC]


def test_examine_consume_stats_concurrent_sums_the_tps_of_every_answering_broker():
    handlers = {
        RequestCode.GET_CONSUME_STATS: lambda addr, req: _ok(
            _consume_stats(1.5, (TOPIC, "broker-a", 0, 100, 90)).encode()
            if addr == ADDR_A else _consume_stats(
                2.0, (TOPIC, "broker-b", 0, 60, 40)).encode()),
    }
    client = _FakeClient(routes={RETRY_TOPIC: _route(("broker-a", ADDR_A),
                                                     ("broker-b", ADDR_B))},
                         responder=_responder_by_code(handlers))

    result = _admin(client).examine_consume_stats_concurrent(GROUP, topic=TOPIC)

    assert result.data.consume_tps == pytest.approx(3.5)
    assert set(result.data.offset_table) == {
        MessageQueue(TOPIC, "broker-a", 0), MessageQueue(TOPIC, "broker-b", 0)}


def _connection_probe_admin(connection, monkeypatch):
    client = _FakeClient(routes={RETRY_TOPIC: _route(("broker-a", ADDR_A))})
    admin = _admin(client)

    def _raise(group):
        raise MQBrokerException(ResponseCode.CONSUMER_NOT_ONLINE, "not online")
    if connection is None:
        monkeypatch.setattr(admin, "examine_consumer_connection_info", _raise)
    else:
        monkeypatch.setattr(admin, "examine_consumer_connection_info",
                            lambda group: connection)
    return admin


def test_examine_consume_stats_concurrent_empty_offsets_reports_consumer_not_online(monkeypatch):
    admin = _connection_probe_admin(None, monkeypatch)
    result = admin.examine_consume_stats_concurrent(GROUP, topic=TOPIC)
    assert result.success is False
    assert result.code == AdminToolsResultCodeEnum.CONSUMER_NOT_ONLINE
    assert "maybe the consumer not consume any message" in result.error_msg


def test_examine_consume_stats_concurrent_empty_offsets_reports_broadcast_mode(monkeypatch):
    connection = ConsumerConnection()
    connection.message_model = "BROADCASTING"
    admin = _connection_probe_admin(connection, monkeypatch)
    result = admin.examine_consume_stats_concurrent(GROUP, topic=TOPIC)
    assert result.success is False
    assert result.code == AdminToolsResultCodeEnum.BROADCAST_CONSUMPTION


def test_examine_consume_stats_concurrent_broker_failure_keeps_the_rest(monkeypatch):
    handlers = {
        RequestCode.GET_CONSUME_STATS: lambda addr, req: (
            _ok(_consume_stats(1.0, (TOPIC, "broker-a", 0, 100, 90)).encode())
            if addr == ADDR_A else _fail(ResponseCode.SYSTEM_ERROR, "offline")),
    }
    client = _FakeClient(routes={RETRY_TOPIC: _route(("broker-a", ADDR_A),
                                                     ("broker-b", ADDR_B))},
                         responder=_responder_by_code(handlers))
    result = _admin(client).examine_consume_stats_concurrent(GROUP, topic=TOPIC)
    assert result.success is True
    assert result.data.consume_tps == pytest.approx(1.0)
    assert list(result.data.offset_table) == [MessageQueue(TOPIC, "broker-a", 0)]


# ------------------------------------------------------------------ delete topic in broker

def test_delete_topic_in_broker_concurrent_reports_each_broker_outcome():
    client = _FakeClient()
    client.delete_behavior[ADDR_B] = MQBrokerException(ResponseCode.SYSTEM_ERROR, "busy")

    result = _admin(client).delete_topic_in_broker_concurrent({ADDR_A, ADDR_B}, TOPIC)

    assert result.success is True  # per-broker outcomes are data, not a tool failure
    assert result.data.success_list == [ADDR_A]
    assert result.data.failure_list == [ADDR_B]
    assert sorted(client.deleted) == [(ADDR_A, TOPIC), (ADDR_B, TOPIC)]


def test_delete_topic_in_broker_concurrent_with_no_addresses_answers_empty():
    client = _FakeClient()
    result = _admin(client).delete_topic_in_broker_concurrent(set(), TOPIC)
    assert result.success is True
    assert result.data.success_list == []
    assert result.data.failure_list == []
    assert client.deleted == []


# ------------------------------------------------------------------ reset offset

def test_reset_offset_new_concurrent_resets_every_broker_through_the_new_rpc():
    client = _FakeClient(routes={TOPIC: _route(("broker-a", ADDR_A), ("broker-b", ADDR_B))})
    admin = _admin(client)
    resets = []

    def _fake_reset(addr, topic, group, timestamp, force, millisecond):
        resets.append((addr, topic, group, timestamp, force, millisecond))
        return {MessageQueue(TOPIC, "broker-a", 0): OffsetWrapper()}
    admin._invoke_broker_reset_offset = _fake_reset

    result = admin.reset_offset_new_concurrent(GROUP, TOPIC, 1234567890)

    assert result.success is True
    assert sorted(resets) == [
        (ADDR_A, TOPIC, GROUP, 1234567890, True, False),
        (ADDR_B, TOPIC, GROUP, 1234567890, True, False),
    ]
    assert sorted(result.data.success_list) == [ADDR_A, ADDR_B]
    assert result.data.failure_list == []


def test_reset_offset_new_concurrent_falls_back_to_the_old_reset_on_consumer_not_online():
    client = _FakeClient(routes={TOPIC: _route(("broker-a", ADDR_A))})
    admin = _admin(client)

    def _not_online(*args):
        raise MQClientException("consumer not online", ResponseCode.CONSUMER_NOT_ONLINE)
    admin._invoke_broker_reset_offset = _not_online
    old_calls = []
    monkey_fallback = lambda addr, queue_data, group, topic, timestamp, force: (
        old_calls.append((addr, queue_data.broker_name, queue_data.read_queue_nums)) or [])
    admin._reset_offset_old_on_broker = monkey_fallback

    result = admin.reset_offset_new_concurrent(GROUP, TOPIC, 1000)

    assert result.success is True
    assert old_calls == [(ADDR_A, "broker-a", 8)]
    assert result.data.success_list == [ADDR_A]


def test_reset_offset_new_concurrent_counts_system_error_as_done():
    client = _FakeClient(routes={TOPIC: _route(("broker-a", ADDR_A))})
    admin = _admin(client)

    def _system_error(*args):
        raise MQClientException("system error", ResponseCode.SYSTEM_ERROR)
    admin._invoke_broker_reset_offset = _system_error

    result = admin.reset_offset_new_concurrent(GROUP, TOPIC, 1000)
    # a group that never came online has nothing to reset: counted, not failed
    assert result.success is True
    assert result.data.success_list == [ADDR_A]


def test_reset_offset_new_concurrent_partial_failure_is_a_failure_with_data():
    client = _FakeClient(routes={TOPIC: _route(("broker-a", ADDR_A), ("broker-b", ADDR_B))})
    admin = _admin(client)

    def _reset(addr, *rest):
        if addr == ADDR_A:
            return {}
        raise MQClientException("boom", 4096)
    admin._invoke_broker_reset_offset = _reset

    result = admin.reset_offset_new_concurrent(GROUP, TOPIC, 1000)

    assert result.success is False
    assert result.code == AdminToolsResultCodeEnum.MQ_BROKER_ERROR
    assert result.error_msg == "operator failure"
    assert result.data.success_list == [ADDR_A]
    assert result.data.failure_list == [ADDR_B]


def test_reset_offset_new_concurrent_without_route_fails_with_route_not_exist():
    client = _FakeClient()
    result = _admin(client).reset_offset_new_concurrent(GROUP, TOPIC, 1000)
    assert result.success is False
    assert result.code == AdminToolsResultCodeEnum.TOPIC_ROUTE_INFO_NOT_EXIST
    assert result.error_msg == "topic router info not found"


# ------------------------------------------------------------------ old reset internals

class _OffsetClient:
    """Records the offset RPCs the old reset path drives."""

    def __init__(self, max_offset=100, searched_offset=40):
        self.max_offset_value = max_offset
        self.searched_offset_value = searched_offset
        self.searched = []
        self.updated = []

    def get_max_offset(self, mq, timeout_millis=None, addr=None):
        return self.max_offset_value

    def search_offset_by_timestamp(self, mq, timestamp, timeout_millis=None, addr=None):
        self.searched.append((mq, timestamp))
        return self.searched_offset_value

    def update_consumer_offset(self, group, mq, offset, timeout_millis=None, addr=None):
        self.updated.append((group, mq, offset))


def test_reset_offset_old_on_broker_resets_only_the_consumed_topic_queues():
    client = _FakeClient(routes={TOPIC: _route(("broker-a", ADDR_A))})
    admin = _admin(client)
    offset_client = _OffsetClient()
    admin._mq_client = offset_client

    stats = _consume_stats(
        0.0, (TOPIC, "broker-a", 0, 100, 50), ("other-topic", "broker-a", 1, 9, 9))
    monkey_examine = lambda addr, group, topic=None, topic_list=None: stats
    admin.examine_consume_stats = monkey_examine

    rollback = admin._reset_offset_old_on_broker(ADDR_A, None, GROUP, TOPIC, -1, True)

    # the queue of another topic is skipped, the topic queue is reset from max offset
    assert [(g, mq.queue_id, offset) for g, mq, offset in offset_client.updated] == [
        (GROUP, 0, 100)]
    assert rollback == [{
        "brokerName": "broker-a", "queueId": 0, "brokerOffset": 100,
        "consumerOffset": 50, "timestampOffset": 100, "rollbackOffset": 100,
    }]


def test_reset_offset_old_on_broker_falls_back_to_read_queues_when_never_consumed():
    route = _route(("broker-a", ADDR_A), read_queue_nums=3)
    client = _FakeClient(routes={TOPIC: route})
    admin = _admin(client)
    offset_client = _OffsetClient(max_offset=77, searched_offset=60)
    admin._mq_client = offset_client

    empty = ConsumeStats()
    admin.examine_consume_stats = lambda addr, group, topic=None, topic_list=None: empty
    topic_status = TopicStatsTable()
    for qid in range(3):
        topic_status.offset_table[MessageQueue(TOPIC, "broker-a", qid)] = TopicOffset(
            min_offset=5 + qid, max_offset=77)
    admin.examine_topic_stats_by_broker = lambda addr, topic: topic_status

    rollback = admin._reset_offset_old_on_broker(
        ADDR_A, route.queue_datas[0], GROUP, TOPIC, 500, True)

    assert len(rollback) == 3  # one entry per read queue
    assert [(g, mq.queue_id, offset) for g, mq, offset in offset_client.updated] == [
        (GROUP, 0, 60), (GROUP, 1, 60), (GROUP, 2, 60)]
    assert offset_client.searched == [
        (MessageQueue(TOPIC, "broker-a", qid), 500) for qid in range(3)]
    # the synthesized wrappers take max/min of the topic status
    assert rollback[0]["brokerOffset"] == 77
    assert rollback[0]["consumerOffset"] == 5


def test_reset_offset_consume_offset_holds_the_rollback_when_not_forced_and_ahead():
    client = _FakeClient()
    admin = _admin(client)
    offset_client = _OffsetClient(max_offset=100)
    admin._mq_client = offset_client

    stats = admin._reset_offset_consume_offset(
        ADDR_A, GROUP, MessageQueue(TOPIC, "broker-a", 0),
        OffsetWrapper(broker_offset=90, consumer_offset=50), -1, False)

    assert offset_client.updated == []  # reset ahead of the consumer, not forced
    assert stats["timestampOffset"] == 100
    assert stats["rollbackOffset"] == 50  # stays at the consumer position


def test_reset_offset_consume_offset_rolls_back_when_forced():
    client = _FakeClient()
    admin = _admin(client)
    offset_client = _OffsetClient(max_offset=100)
    admin._mq_client = offset_client

    stats = admin._reset_offset_consume_offset(
        ADDR_A, GROUP, MessageQueue(TOPIC, "broker-a", 0),
        OffsetWrapper(broker_offset=90, consumer_offset=80), -1, True)

    assert offset_client.updated == [(GROUP, MessageQueue(TOPIC, "broker-a", 0), 100)]
    assert stats["rollbackOffset"] == 100


def test_reset_offset_consume_offset_searches_by_timestamp_when_given():
    client = _FakeClient()
    admin = _admin(client)
    offset_client = _OffsetClient(searched_offset=33)
    admin._mq_client = offset_client

    admin._reset_offset_consume_offset(
        ADDR_A, GROUP, MessageQueue(TOPIC, "broker-a", 2),
        OffsetWrapper(broker_offset=90, consumer_offset=10), 4242, True)

    assert offset_client.searched == [(MessageQueue(TOPIC, "broker-a", 2), 4242)]
    assert offset_client.updated == [(GROUP, MessageQueue(TOPIC, "broker-a", 2), 33)]


# ------------------------------------------------------------------ topics by consumer

def test_query_topics_by_consumer_concurrent_unions_the_broker_answers():
    handlers = {
        RequestCode.QUERY_TOPICS_BY_CONSUMER: lambda addr, req: _ok(
            TopicList.from_dict({"topicList": (
                ["Shared", "OnlyA"] if addr == ADDR_A else ["Shared", "OnlyB"])}).encode()),
    }
    client = _FakeClient(routes={RETRY_TOPIC: _route(("broker-a", ADDR_A),
                                                     ("broker-b", ADDR_B))},
                         responder=_responder_by_code(handlers))
    admin = _admin(client)

    result = admin.query_topics_by_consumer_concurrent(GROUP)

    assert result.success is True
    # Answers are appended per broker in order, duplicates included.
    assert result.data.get_topic_list() == ["Shared", "OnlyA", "Shared", "OnlyB"]
    _, request, _ = client.requests[0]
    assert request.code == RequestCode.QUERY_TOPICS_BY_CONSUMER
    assert request.ext_fields == {"group": GROUP}


def test_query_topics_by_consumer_concurrent_without_route_fails():
    client = _FakeClient()
    result = _admin(client).query_topics_by_consumer_concurrent(GROUP)
    assert result.success is False
    assert result.code == AdminToolsResultCodeEnum.TOPIC_ROUTE_INFO_NOT_EXIST
    assert result.error_msg == "router info not found."
    assert client.attempted_routes == [RETRY_TOPIC]


# ------------------------------------------------------------------ consume time span

def test_query_consume_time_span_concurrent_concatenates_the_broker_answers():
    spans_a = {"consumeTimeSpanSet": [{"topic": TOPIC, "fromStamp": 1, "toStamp": 2}]}
    spans_b = {"consumeTimeSpanSet": [{"topic": TOPIC, "fromStamp": 3, "toStamp": 4}]}
    handlers = {
        RequestCode.QUERY_CONSUME_TIME_SPAN: lambda addr, req: _ok(
            RemotingSerializable.encode(spans_a if addr == ADDR_A else spans_b)),
    }
    client = _FakeClient(routes={TOPIC: _route(("broker-a", ADDR_A), ("broker-b", ADDR_B))},
                         responder=_responder_by_code(handlers))
    admin = _admin(client)

    result = admin.query_consume_time_span_concurrent(TOPIC, GROUP)

    assert result.success is True
    assert result.data == [
        {"topic": TOPIC, "fromStamp": 1, "toStamp": 2},
        {"topic": TOPIC, "fromStamp": 3, "toStamp": 4},
    ]
    _, request, _ = client.requests[0]
    assert request.code == RequestCode.QUERY_CONSUME_TIME_SPAN
    assert request.ext_fields == {"topic": TOPIC, "group": GROUP}


def test_query_consume_time_span_concurrent_without_route_is_a_success_with_no_spans():
    client = _FakeClient()
    result = _admin(client).query_consume_time_span_concurrent(TOPIC, GROUP)
    assert result.success is True
    assert result.data == []


# ------------------------------------------------------------------ message track

def _msg() -> MessageExt:
    msg = MessageExt(TOPIC, tags="TagA")
    msg.store_host = "10.0.0.9"
    msg.store_host_port = 10911
    msg.queue_id = 1
    msg.queue_offset = 10
    return msg


def test_message_track_detail_concurrent_matches_the_serial_verdicts(monkeypatch):
    client = _FakeClient(routes={TOPIC: _route(("broker-a", ADDR_A))})
    admin = _admin(client)
    connection = ConsumerConnection()
    connection.consume_type = "CONSUME_PASSIVELY"

    monkeypatch.setattr(admin, "query_topic_consume_by_who",
                        lambda addr, topic: {"g-b", "g-a"})
    monkeypatch.setattr(admin, "examine_consumer_connection_info",
                        lambda group: connection)
    monkeypatch.setattr(admin, "consumed", lambda msg, group: group == "g-a")

    concurrent = admin.message_track_detail_concurrent(_msg())
    serial = admin.message_track_detail(_msg())

    assert sorted(t.consumer_group for t in concurrent) == ["g-a", "g-b"]
    for got, expected in zip(sorted(concurrent, key=lambda t: t.consumer_group), serial):
        assert got.consumer_group == expected.consumer_group
        assert got.track_type == expected.track_type
    by_group = {t.consumer_group: t.track_type for t in concurrent}
    assert by_group["g-a"] == TrackType.CONSUMED
    assert by_group["g-b"] == TrackType.NOT_CONSUME_YET


def test_message_track_detail_concurrent_encodes_failures_into_the_track(monkeypatch):
    client = _FakeClient(routes={TOPIC: _route(("broker-a", ADDR_A))})
    admin = _admin(client)

    monkeypatch.setattr(admin, "query_topic_consume_by_who",
                        lambda addr, topic: {"g-offline"})

    def _not_online(group):
        raise MQBrokerException(ResponseCode.CONSUMER_NOT_ONLINE, "not online")
    monkeypatch.setattr(admin, "examine_consumer_connection_info", _not_online)

    tracks = admin.message_track_detail_concurrent(_msg())
    assert len(tracks) == 1
    assert tracks[0].track_type == TrackType.NOT_ONLINE
    assert tracks[0].exception_desc == "CODE:206 DESC:not online"


def test_message_track_detail_concurrent_without_broker_answers_empty(monkeypatch):
    client = _FakeClient(routes={TOPIC: _route()})
    admin = _admin(client)
    assert admin.message_track_detail_concurrent(_msg()) == []


# ------------------------------------------------------------------ latch timeout

def test_fan_out_wait_is_bounded_by_the_timeout_but_keeps_the_fast_answers():
    started = time.monotonic()
    handlers = {
        RequestCode.GET_TOPIC_STATS_INFO: lambda addr, req: (
            (time.sleep(2.0), _ok(_stats_table(9.9).encode()))[1]
            if addr == ADDR_B else
            _ok(_stats_table(1.0, (TOPIC, "broker-a", 0, 100)).encode())),
    }
    client = _FakeClient(routes={TOPIC: _route(("broker-a", ADDR_A),
                                               ("broker-b", ADDR_B))},
                         responder=_responder_by_code(handlers))
    admin = _admin(client)
    admin.set_timeout_millis(200)

    result = admin.examine_topic_stats_concurrent(TOPIC)

    elapsed = time.monotonic() - started
    # the slow broker must not hold the answer beyond the budget
    assert elapsed < 1.5
    assert result.success is True
    # the fast broker's data landed, the slow one's did not (it is still late)
    assert list(result.data.offset_table) == [MessageQueue(TOPIC, "broker-a", 0)]
    assert result.data.topic_put_tps == pytest.approx(1.0)


# ------------------------------------------------------------------ envelope mapping

def test_admin_tool_envelope_maps_exception_kinds_to_codes():
    from remoting.exception import RemotingException
    admin = _admin(_FakeClient())

    def _raise(e):
        def _h():
            raise e
        return admin._admin_tool_execute(_h)

    result = _raise(RemotingException("conn refused"))
    assert (result.success, result.code) == (False, AdminToolsResultCodeEnum.REMOTING_ERROR)

    result = _raise(MQClientException("topic route not exist", ResponseCode.TOPIC_NOT_EXIST))
    assert (result.success, result.code) == (
        False, AdminToolsResultCodeEnum.TOPIC_ROUTE_INFO_NOT_EXIST)

    result = _raise(MQClientException("other client trouble", 4096))
    assert (result.success, result.code) == (False, AdminToolsResultCodeEnum.MQ_CLIENT_ERROR)

    result = _raise(InterruptedError("interrupted"))
    assert (result.success, result.code) == (False, AdminToolsResultCodeEnum.INTERRUPT_ERROR)

    result = _raise(MQBrokerException(ResponseCode.SYSTEM_ERROR, "broker boom"))
    assert (result.success, result.code) == (False, AdminToolsResultCodeEnum.MQ_BROKER_ERROR)

    # a handler that completes is returned untouched
    assert admin._admin_tool_execute(lambda: "payload") == "payload"
