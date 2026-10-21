# -*- coding: utf-8 -*-
"""Client configuration knobs: defaults, wire behaviour and the paged metadata reads.

Every positive assertion below is paired with the state that must differ when the
corresponding behaviour is removed: a switch turned off, an explicit call-site value,
or a second page that changes the response version.
"""
from __future__ import annotations

import os
import threading
import types
import zlib

import pytest

from client.admin import DefaultMQAdminExt
from client.consumer import (DefaultLitePullConsumer, DefaultMQPullConsumer,
                             DefaultMQPushConsumer)
from client.exception import MQClientException
from client.mq_client import MQClientInstance
from client.producer import DefaultMQProducer
from common.message import MessageExt, MessageQueue
from common.message_decoder import crc32, encode_message_ext
from common.mix_all import MixAll
from common.subscription_data import SubscriptionData
from common.sysflag import MessageSysFlag, PullSysFlag
from common.topic_config import TopicConfig
from remoting.protocol.admin_body import TopicConfigSerializeWrapper
from remoting.protocol.codes import RequestCode, ResponseCode
from remoting.protocol.heartbeat import ConsumerData, HeartbeatData, ProducerData
from remoting.protocol.remoting_command import RemotingCommand
from remoting.protocol.route import BrokerData, TopicRouteData
from remoting.protocol.serialize import RemotingSerializable
from remoting.protocol.subscription import SubscriptionGroupConfig, SubscriptionGroupWrapper

GROUP = "GID_KnobsUnit"
TOPIC = "KnobUnitTopic"
NS_ADDR = "127.0.0.1:9876"
MASTER = "127.0.0.1:10911"
SLAVE = "127.0.0.1:10912"


# ------------------------------------------------------------------ Test doubles

def _ok(body: bytes = b"", ext: dict = None, version: int = 0) -> RemotingCommand:
    r = RemotingCommand()
    r.code = ResponseCode.SUCCESS
    r.body = body
    r.ext_fields = dict(ext or {})
    r.version = version
    return r


class _RecordingClient:
    """Records every outgoing request; replies come from a fixed queue, never from a socket."""

    def __init__(self, responses):
        self._responses = list(responses)
        self.requests = []  # [(addr, RemotingCommand, timeout_millis)]

    def _invoke_sync(self, addr, request, timeout_millis=None):
        self.requests.append((addr, request, timeout_millis))
        if not self._responses:
            raise AssertionError("unexpected extra request: code=%s" % request.code)
        response = self._responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response

    def _check_response(self, response):
        if response.code == ResponseCode.SUCCESS:
            return response
        from client.exception import MQBrokerException
        raise MQBrokerException(response.code, response.remark or "")


def _instance(**kwargs) -> MQClientInstance:
    kwargs.setdefault("decode_read_body", True)
    return MQClientInstance("knobs@client", [NS_ADDR], **kwargs)


def _admin(client) -> DefaultMQAdminExt:
    admin = DefaultMQAdminExt()
    admin._mq_client = client
    admin._started = True
    return admin


# ------------------------------------------------------------- Message builders

def _message(body: bytes, queue_offset: int, queue_id: int = 0,
             properties: dict = None, sys_flag: int = 0,
             body_crc: int = None) -> MessageExt:
    msg = MessageExt()
    msg.set_topic(TOPIC)
    msg.set_body(body)
    msg.set_flag(2)
    msg.set_sys_flag(sys_flag)
    msg.set_body_crc(crc32(body) if body_crc is None else body_crc)
    msg.set_queue_id(queue_id)
    msg.set_queue_offset(queue_offset)
    msg.set_commit_log_offset(1024 + queue_offset)
    msg.set_born_timestamp(1700000000000)
    msg.set_store_timestamp(1700000000123)
    msg.set_born_host("127.0.0.1")
    msg.born_host_port = 54321
    msg.set_store_host("127.0.0.1")
    msg.store_host_port = 10911
    msg.set_reconsume_times(0)
    msg.set_prepared_transaction_offset(0)
    msg.set_properties(dict(properties or {"TAGS": "TagA"}))
    return msg


def _batch() -> bytes:
    compressed = zlib.compress(b"three")
    frames = [
        _message(b"one", 11, properties={"TAGS": "First", "KEYS": "k1"}),
        _message(b"two", 12, properties={"TAGS": "Second"}),
        _message(compressed, 13, properties={"TAGS": "Third"},
                 sys_flag=MessageSysFlag.COMPRESSED_FLAG
                 | MessageSysFlag.COMPRESSION_ZLIB_TYPE),
    ]
    return b"".join(encode_message_ext(m) for m in frames)


def _pull(inst: MQClientInstance, body: bytes):
    response = _ok(body, ext={"nextBeginOffset": "99", "minOffset": "0",
                              "maxOffset": "100"})
    inst._invoke_sync = lambda addr, request, timeout_millis=None: response
    mq = MessageQueue(topic=TOPIC, broker_name="broker-a", queue_id=4)
    return inst.pull_message(GROUP, mq, 0, 32,
                             PullSysFlag.build_sys_flag(True, True, True, False),
                             0, "*", 0, "TAG", addr=MASTER)


def _pop(inst: MQClientInstance, body: bytes):
    response = _ok(body, ext={})
    inst._invoke_sync = lambda addr, request, timeout_millis=None: response
    return inst.pop_message(GROUP, TOPIC, queue_id=0, addr=MASTER,
                            broker_name="broker-a")


# ------------------------------------------------------------------ Defaults

FACADES = [
    ("producer", lambda: DefaultMQProducer(GROUP), False),
    ("push", lambda: DefaultMQPushConsumer(GROUP), True),
    ("pull", lambda: DefaultMQPullConsumer(GROUP), True),
    ("lite", lambda: DefaultLitePullConsumer(GROUP), True),
]


@pytest.mark.parametrize("label,make,has_decode_switch", FACADES)
def test_facade_defaults_and_non_default_values(label, make, has_decode_switch):
    facade = make()
    expected_pool = os.cpu_count() or 1

    assert facade.mq_client_api_timeout == 3000, label
    assert facade.enable_concurrent_heartbeat is False, label
    assert facade.concurrent_heartbeat_thread_pool_size == expected_pool, label
    if has_decode_switch:
        assert facade.decode_read_body is True, label

    # The controls: each setting must be able to move away from its default.
    facade.set_mq_client_api_timeout(1234)
    assert facade.mq_client_api_timeout == 1234, label
    facade.set_enable_concurrent_heartbeat(True)
    assert facade.enable_concurrent_heartbeat is True, label
    facade.set_concurrent_heartbeat_thread_pool_size(expected_pool + 1)
    assert facade.concurrent_heartbeat_thread_pool_size == expected_pool + 1, label
    if has_decode_switch:
        facade.set_decode_read_body(False)
        assert facade.decode_read_body is False, label


def test_facade_setters_reach_an_already_attached_instance():
    inst = _instance()
    consumer = DefaultMQPushConsumer(GROUP)
    consumer._mq_client = inst

    consumer.set_mq_client_api_timeout(1234)
    consumer.set_enable_concurrent_heartbeat(True)
    consumer.set_concurrent_heartbeat_thread_pool_size(3)
    consumer.set_decode_read_body(False)

    assert inst.mq_client_api_timeout == 1234
    assert inst.enable_concurrent_heartbeat is True
    assert inst.concurrent_heartbeat_thread_pool_size == 3
    assert inst.decode_read_body is False


def test_instance_defaults_and_constructor_values():
    inst = _instance()
    expected_pool = os.cpu_count() or 1
    assert inst.decode_read_body is True
    assert inst.mq_client_api_timeout == 3000
    assert inst.enable_concurrent_heartbeat is False
    assert inst.concurrent_heartbeat_thread_pool_size == expected_pool

    configured = _instance(decode_read_body=False, mq_client_api_timeout=1234,
                           enable_concurrent_heartbeat=True,
                           concurrent_heartbeat_thread_pool_size=3)
    assert configured.decode_read_body is False
    assert configured.mq_client_api_timeout == 1234
    assert configured.enable_concurrent_heartbeat is True
    assert configured.concurrent_heartbeat_thread_pool_size == 3


# ------------------------------------------------------------------ decode switch

def test_pull_decode_on_returns_bodies_and_pull_decode_off_keeps_the_batch():
    inst = _instance()
    on = _pull(inst, _batch())
    assert [m.get_body() for m in on.msg_found_list] == [b"one", b"two", b"three"]
    assert [m.get_queue_offset() for m in on.msg_found_list] == [11, 12, 13]
    assert [m.get_properties().get("TAGS") for m in on.msg_found_list] == [
        "First", "Second", "Third"]
    # the compressed frame was decompressed and its flag cleared
    assert on.msg_found_list[2].get_sys_flag() & MessageSysFlag.COMPRESSED_FLAG == 0

    inst.decode_read_body = False
    off = _pull(inst, _batch())
    assert len(off.msg_found_list) == 3
    assert [m.get_body() for m in off.msg_found_list] == [None, None, None]
    assert [m.get_queue_offset() for m in off.msg_found_list] == [11, 12, 13]
    assert [m.get_properties().get("TAGS") for m in off.msg_found_list] == [
        "First", "Second", "Third"]
    # the compressed bit is left untouched because the body was never decompressed
    assert off.msg_found_list[2].get_sys_flag() & MessageSysFlag.COMPRESSED_FLAG


def test_pull_decode_off_skips_crc_validation():
    frame = _message(b"payload", 21, body_crc=crc32(b"different-body"))

    inst = _instance()
    assert _pull(inst, encode_message_ext(frame)).msg_found_list == []

    inst.decode_read_body = False
    off = _pull(inst, encode_message_ext(frame))
    assert len(off.msg_found_list) == 1
    assert off.msg_found_list[0].get_body() is None
    assert off.msg_found_list[0].get_queue_offset() == 21
    assert off.msg_found_list[0].get_properties().get("TAGS") == "TagA"


def test_lite_pull_request_flag_uses_the_same_decode_switch():
    inst = _instance()
    requests = []

    def _invoke(addr, request, timeout_millis=None):
        requests.append(request)
        return _ok(_batch(), ext={"nextBeginOffset": "99", "minOffset": "0",
                                  "maxOffset": "100"})

    inst._invoke_sync = _invoke
    mq = MessageQueue(topic=TOPIC, broker_name="broker-a", queue_id=0)
    sys_flag = PullSysFlag.build_sys_flag(True, False, True, False, lite_pull=True)

    on = inst.pull_message(GROUP, mq, 0, 32, sys_flag, 0, "*", 0, "TAG", addr=MASTER)
    assert requests[-1].code == RequestCode.LITE_PULL_MESSAGE
    assert [m.get_body() for m in on.msg_found_list] == [b"one", b"two", b"three"]

    inst.decode_read_body = False
    off = inst.pull_message(GROUP, mq, 0, 32, sys_flag, 0, "*", 0, "TAG", addr=MASTER)
    assert requests[-1].code == RequestCode.LITE_PULL_MESSAGE
    assert len(off.msg_found_list) == 3
    assert [m.get_body() for m in off.msg_found_list] == [None, None, None]


def test_pop_decode_on_returns_bodies_and_pop_decode_off_keeps_the_batch():
    inst = _instance()
    on = _pop(inst, _batch())
    assert [m.get_body() for m in on.msg_found_list] == [b"one", b"two", b"three"]
    assert [m.get_queue_offset() for m in on.msg_found_list] == [11, 12, 13]

    inst.decode_read_body = False
    off = _pop(inst, _batch())
    assert len(off.msg_found_list) == 3
    assert [m.get_body() for m in off.msg_found_list] == [None, None, None]
    assert [m.get_queue_offset() for m in off.msg_found_list] == [11, 12, 13]
    assert [m.get_properties().get("TAGS") for m in off.msg_found_list] == [
        "First", "Second", "Third"]


def test_producer_query_message_decodes_regardless_of_the_switch():
    stream = _batch()

    class _QueryClient:
        def query_message(self, topic, key, max_num, begin, end):
            return stream

    producer = DefaultMQProducer(GROUP)
    producer._mq_client = _QueryClient()
    producer._started = True

    assert producer.decode_read_body is True
    on = producer.query_message(TOPIC, "k", 32, 0, 999)
    assert [m.get_body() for m in on] == [b"one", b"two", b"three"]

    # The switch belongs to the pull / POP receive paths: a query result keeps its
    # bodies either way.
    producer.set_decode_read_body(False)
    off = producer.query_message(TOPIC, "k", 32, 0, 999)
    assert [m.get_body() for m in off] == [b"one", b"two", b"three"]
    assert [m.get_queue_offset() for m in off] == [11, 12, 13]


# ------------------------------------------------------------------ API timeout

def _spy_instance():
    inst = _instance()
    seen = []

    def _invoke(addr, request, timeout_millis=None):
        seen.append((addr, request, timeout_millis))
        return _ok()

    inst._invoke_sync = _invoke
    return inst, seen


def test_heartbeat_timeout_default_configured_and_explicit():
    inst, seen = _spy_instance()
    hb = HeartbeatData("knobs-client")
    hb.producer_data_set.add(ProducerData(GROUP))

    inst.send_heartbeat(MASTER, hb)
    assert seen[-1][2] == 3000
    assert seen[-1][1].code == RequestCode.HEART_BEAT

    inst.send_heartbeat(MASTER, hb, 4321)
    assert seen[-1][2] == 4321

    inst.mq_client_api_timeout = 1234
    inst.send_heartbeat(MASTER, hb)
    assert seen[-1][2] == 1234

    # An explicit per-call timeout still wins over the configured value.
    inst.send_heartbeat(MASTER, hb, 4321)
    assert seen[-1][2] == 4321

    # The V2 send is the same HEART_BEAT(34) frame and reads the same budget.
    inst.mq_client_api_timeout = 3000
    inst.send_heartbeat_v2(MASTER, hb)
    assert seen[-1][2] == 3000
    assert seen[-1][1].code == RequestCode.HEART_BEAT
    inst.mq_client_api_timeout = 1234
    inst.send_heartbeat_v2(MASTER, hb)
    assert seen[-1][2] == 1234
    inst.send_heartbeat_v2(MASTER, hb, 4321)
    assert seen[-1][2] == 4321


def test_route_fetch_timeout_covers_the_topic_and_the_default_topic():
    inst, seen = _spy_instance()
    assert inst.update_topic_route_info_from_name_server(TOPIC) is False
    assert [timeout for _addr, _req, timeout in seen] == [3000]
    assert [request.ext_fields["topic"] for _addr, request, _t in seen] == [TOPIC]

    seen.clear()
    inst.mq_client_api_timeout = 1234
    assert inst.update_topic_route_info_from_name_server(TOPIC, is_default=True) is False
    assert [timeout for _addr, _req, timeout in seen] == [1234, 1234]
    assert [request.ext_fields["topic"] for _addr, request, _t in seen] == [
        TOPIC, MixAll.DEFAULT_TOPIC]

    seen.clear()
    assert inst.update_topic_route_info_from_name_server(
        TOPIC, 4321, is_default=True) is False
    assert [timeout for _addr, _req, timeout in seen] == [4321, 4321]


def test_unregister_and_check_config_timeouts():
    inst, seen = _spy_instance()
    sub = SubscriptionData(topic=TOPIC, sub_string="*")

    inst.unregister_client(MASTER, "cid-1", GROUP, "")
    inst.check_client_config(MASTER, GROUP, "cid-1", sub)
    assert [timeout for _addr, _req, timeout in seen] == [3000, 3000]
    assert [request.code for _addr, request, _t in seen] == [
        RequestCode.UNREGISTER_CLIENT, RequestCode.CHECK_CLIENT_CONFIG]

    inst.mq_client_api_timeout = 1234
    seen.clear()
    inst.unregister_client(MASTER, "cid-1", GROUP, "")
    inst.check_client_config(MASTER, GROUP, "cid-1", sub)
    assert [timeout for _addr, _req, timeout in seen] == [1234, 1234]

    seen.clear()
    inst.unregister_client(MASTER, "cid-1", GROUP, "", 4321)
    inst.check_client_config(MASTER, GROUP, "cid-1", sub, 4322)
    assert [timeout for _addr, _req, timeout in seen] == [4321, 4322]


def test_consumer_list_lookup_timeout_follows_the_instance_value():
    from remoting.protocol.body import GetConsumerListByGroupResponseBody
    body = GetConsumerListByGroupResponseBody()
    body.consumer_id_list = ["cid-1"]

    inst, seen = _spy_instance()
    route = TopicRouteData()
    route.broker_datas.append(BrokerData("DefaultCluster", "broker-a", {0: MASTER}))
    inst.topic_route_table[TOPIC] = route
    inst._invoke_sync = lambda addr, request, timeout_millis=None: (
        seen.append((addr, request, timeout_millis)) or _ok(body.encode()))

    assert inst.get_consumer_id_list_by_group(TOPIC, GROUP) == ["cid-1"]
    assert seen[-1][2] == 3000

    inst.mq_client_api_timeout = 1234
    assert inst.get_consumer_id_list_by_group(TOPIC, GROUP) == ["cid-1"]
    assert seen[-1][2] == 1234


# ------------------------------------------------------------------ Concurrent heartbeat

def _heartbeat_inst(enable=True, size=2):
    inst = _instance(enable_concurrent_heartbeat=enable,
                     concurrent_heartbeat_thread_pool_size=size)
    inst.broker_addr_table = {"broker-a": {0: MASTER, 1: SLAVE}}
    return inst


def test_concurrent_heartbeat_reaches_consumer_slaves_but_producers_only_masters():
    inst = _heartbeat_inst()
    sent = []
    inst.start()
    try:
        assert inst._heartbeat_executor is not None
        inst.send_heartbeat = lambda addr, hb, timeout=None: sent.append(addr) or 0

        consumer_hb = HeartbeatData("knobs-client")
        consumer_hb.consumer_data_set.add(ConsumerData(GROUP))
        assert inst.send_heartbeat_concurrently(consumer_hb) == 2
        assert sorted(sent) == sorted([MASTER, SLAVE])

        sent.clear()
        producer_hb = HeartbeatData("knobs-client")
        producer_hb.producer_data_set.add(ProducerData(GROUP))
        assert inst.send_heartbeat_concurrently(producer_hb) == 1
        assert sent == [MASTER]
    finally:
        inst.shutdown()
    assert inst._heartbeat_executor is None


def test_concurrent_heartbeat_success_count_excludes_failed_brokers():
    inst = _heartbeat_inst()
    sent = []

    def _send(addr, hb, timeout=None):
        if addr == SLAVE:
            raise RuntimeError("broker gone away")
        sent.append(addr)
        return 0

    inst._heartbeat_executor = _SynchronousExecutor()
    inst.send_heartbeat = _send
    hb = HeartbeatData("knobs-client")
    hb.producer_data_set.add(ProducerData(GROUP))

    assert inst.send_heartbeat_concurrently(hb) == 1
    assert sent == [MASTER]


class _SynchronousExecutor:
    """Runs submitted work inline; optionally rejects one address."""

    def __init__(self, reject_addr=None):
        self.reject_addr = reject_addr

    def submit(self, fn, addr):
        if addr == self.reject_addr:
            raise RuntimeError("cannot schedule new futures after shutdown")
        fn(addr)


def test_concurrent_heartbeat_rejected_task_still_releases_the_wait():
    inst = _heartbeat_inst()
    inst._heartbeat_executor = _SynchronousExecutor(reject_addr=SLAVE)
    sent = []
    inst.send_heartbeat = lambda addr, hb, timeout=None: sent.append(addr) or 0
    hb = HeartbeatData("knobs-client")
    hb.producer_data_set.add(ProducerData(GROUP))

    result = []
    thread = threading.Thread(
        target=lambda: result.append(inst.send_heartbeat_concurrently(hb)))
    thread.start()
    thread.join(5)

    # A rejected submission that leaks its permit would leave this call blocked forever.
    assert not thread.is_alive()
    assert result == [1]
    assert sent == [MASTER]


def test_heartbeat_dispatch_order_v2_then_concurrent_then_serial():
    class _FacadeClient:
        def __init__(self):
            self.calls = []

        def get_all_broker_addrs(self):
            return [MASTER, SLAVE]

        def send_heartbeat_concurrently(self, hb, timeout_millis):
            self.calls.append(("concurrent", timeout_millis))
            return 2

        def send_heartbeat_to_broker_v2(self, addr, hb, owner, timeout_millis):
            self.calls.append(("v2", addr, timeout_millis))
            return 0

        def send_heartbeat(self, addr, hb, timeout_millis):
            self.calls.append(("v1", addr, timeout_millis))
            return 0

    client = _FacadeClient()
    consumer = DefaultMQPushConsumer(GROUP)
    consumer._mq_client = client
    consumer._started = True

    consumer.use_heartbeat_v2 = True
    consumer.enable_concurrent_heartbeat = True
    assert consumer._send_heartbeat_to_all_broker() == 2
    assert [call[0] for call in client.calls] == ["v2", "v2"]

    client.calls.clear()
    consumer.use_heartbeat_v2 = False
    assert consumer._send_heartbeat_to_all_broker() == 2
    assert client.calls == [("concurrent", 3000)]

    client.calls.clear()
    consumer.set_mq_client_api_timeout(1234)
    assert consumer._send_heartbeat_to_all_broker() == 2
    assert client.calls == [("concurrent", 1234)]

    client.calls.clear()
    consumer.enable_concurrent_heartbeat = False
    assert consumer._send_heartbeat_to_all_broker() == 2
    assert client.calls == [("v1", MASTER, 1234), ("v1", SLAVE, 1234)]


def test_concurrent_heartbeat_emits_nothing_for_an_empty_body():
    inst = _heartbeat_inst()
    sent = []
    inst.send_heartbeat = lambda addr, hb, timeout=None: sent.append(addr) or 0
    assert inst.send_heartbeat_concurrently(HeartbeatData("knobs-client")) == 0
    assert sent == []


# ------------------------------------------------------------------ Page size

PAGE_SIZE = 2000


def _topic_page(topics, data_version, total=None) -> RemotingCommand:
    wrapper = TopicConfigSerializeWrapper()
    wrapper.topic_config_table = {name: TopicConfig(name) for name in topics}
    wrapper.data_version = dict(data_version)
    response = _ok(wrapper.encode())
    if total is not None:
        response.ext_fields["totalTopicNum"] = str(total)
    return response


def _group_page(groups, data_version, total=None) -> RemotingCommand:
    wrapper = SubscriptionGroupWrapper()
    wrapper.subscription_group_table = {
        name: SubscriptionGroupConfig(group_name=name) for name in groups}
    wrapper.data_version = dict(data_version)
    response = _ok(wrapper.encode())
    if total is not None:
        response.ext_fields["totalGroupNum"] = str(total)
    return response


def test_admin_page_size_default_and_configured_reach_the_topic_request():
    client = _RecordingClient([_topic_page({"topic-a"}, {"counter": 1})])
    admin = _admin(client)
    assert admin.max_page_size_in_get_metadata == PAGE_SIZE

    admin.get_all_topic_config(MASTER)
    assert client.requests[0][1].ext_fields["maxTopicNum"] == str(PAGE_SIZE)

    client = _RecordingClient([_topic_page({"topic-a"}, {"counter": 1})])
    admin = _admin(client)
    admin.set_max_page_size_in_get_metadata(5)
    assert admin.max_page_size_in_get_metadata == 5
    admin.get_all_topic_config(MASTER)
    assert client.requests[0][1].ext_fields["maxTopicNum"] == "5"


def test_admin_page_size_default_and_configured_reach_the_group_request():
    client = _RecordingClient([_group_page({"group-a"}, {"counter": 1})])
    admin = _admin(client)
    admin.get_all_subscription_group(MASTER)
    assert client.requests[0][1].ext_fields["maxGroupNum"] == str(PAGE_SIZE)

    client = _RecordingClient([_group_page({"group-a"}, {"counter": 1})])
    admin = _admin(client)
    admin.set_max_page_size_in_get_metadata(7)
    admin.get_all_subscription_group(MASTER)
    assert client.requests[0][1].ext_fields["maxGroupNum"] == "7"


def test_group_first_page_uses_an_empty_data_version():
    client = _RecordingClient([_group_page({"group-a"}, {"counter": 1})])
    admin = _admin(client)
    admin.get_all_subscription_group(MASTER)
    assert client.requests[0][1].ext_fields["groupSeq"] == "0"
    assert client.requests[0][1].ext_fields["dataVersion"] == ""


def test_topic_paging_two_pages_merge_and_carry_the_version():
    client = _RecordingClient([
        _topic_page({"topic-a"}, {"counter": 1}, total=3),
        _topic_page({"topic-b"}, {"counter": 1}, total=3),
    ])
    admin = _admin(client)
    admin.set_max_page_size_in_get_metadata(1)

    result = admin.get_all_topic_config(MASTER)

    assert len(client.requests) == 2
    first = client.requests[0][1].ext_fields
    second = client.requests[1][1].ext_fields
    assert first == {"topicSeq": "0", "maxTopicNum": "1", "dataVersion": ""}
    assert second == {
        "topicSeq": "1", "maxTopicNum": "1",
        "dataVersion": RemotingSerializable.to_json({"counter": 1}),
    }
    assert set(result.topic_config_table) == {"topic-a", "topic-b"}


def test_group_paging_restarts_at_zero_under_the_new_version():
    client = _RecordingClient([
        _group_page({"stale-a"}, {"counter": 1}, total=3),
        _group_page({"stale-b"}, {"counter": 2}, total=3),
        _group_page({"fresh-a"}, {"counter": 2}, total=3),
        _group_page({"fresh-b"}, {"counter": 2}, total=3),
    ])
    admin = _admin(client)
    admin.set_max_page_size_in_get_metadata(1)

    result = admin.get_all_subscription_group(MASTER)

    assert len(client.requests) == 4
    seqs = [r[1].ext_fields["groupSeq"] for r in client.requests]
    versions = [r[1].ext_fields["dataVersion"] for r in client.requests]
    assert seqs == ["0", "1", "0", "1"]
    assert versions == [
        "",
        RemotingSerializable.to_json({"counter": 1}),
        RemotingSerializable.to_json({"counter": 2}),
        RemotingSerializable.to_json({"counter": 2}),
    ]
    assert set(result.subscription_group_table) == {"fresh-a", "fresh-b"}
    assert result.data_version == {"counter": 2}


def test_topic_paging_stops_when_the_broker_reports_no_total():
    client = _RecordingClient([_topic_page({"topic-a", "topic-b"}, {"counter": 1})])
    admin = _admin(client)

    result = admin.get_all_topic_config(MASTER)

    # An old broker returns every topic in one reply; a second request would hit the
    # fake's "unexpected extra request" guard.
    assert len(client.requests) == 1
    assert set(result.topic_config_table) == {"topic-a", "topic-b"}


def test_topic_paging_restarts_at_zero_under_the_new_version():
    client = _RecordingClient([
        _topic_page({"stale-a"}, {"counter": 1}, total=3),
        _topic_page({"stale-b"}, {"counter": 2}, total=3),
        _topic_page({"fresh-a"}, {"counter": 2}, total=3),
        _topic_page({"fresh-b"}, {"counter": 2}, total=3),
    ])
    admin = _admin(client)
    admin.set_max_page_size_in_get_metadata(1)

    result = admin.get_all_topic_config(MASTER)

    assert len(client.requests) == 4
    first = client.requests[0][1].ext_fields
    second = client.requests[1][1].ext_fields
    third = client.requests[2][1].ext_fields
    fourth = client.requests[3][1].ext_fields
    assert first["topicSeq"] == "0" and first["dataVersion"] == ""
    assert second["topicSeq"] == "1"
    assert second["dataVersion"] == RemotingSerializable.to_json({"counter": 1})
    # The restart walks from zero under the new version, so the broker serves the fresh
    # snapshot instead of being asked for the old one again.
    assert third == {
        "topicSeq": "0", "maxTopicNum": "1",
        "dataVersion": RemotingSerializable.to_json({"counter": 2}),
    }
    assert fourth["topicSeq"] == "1"
    assert fourth["dataVersion"] == RemotingSerializable.to_json({"counter": 2})
    # The table cleared on restart, so the stale pages are gone.
    assert set(result.topic_config_table) == {"fresh-a", "fresh-b"}


def test_topic_paging_raises_when_the_elapsed_budget_is_exhausted(monkeypatch):
    client = _RecordingClient([
        _topic_page({"topic-a"}, {"counter": 1}, total=3),
        _topic_page({"topic-b"}, {"counter": 1}, total=3),
    ])
    admin = _admin(client)
    admin.set_max_page_size_in_get_metadata(1)
    clock = iter([0.0, 0.0, 10.0])
    monkeypatch.setattr("client.admin.time",
                        types.SimpleNamespace(monotonic=lambda: next(clock)))

    with pytest.raises(MQClientException) as exc:
        admin.get_all_topic_config(MASTER, timeout_millis=10)
    assert "timeout" in str(exc.value)
    # The control is the unpatched paging test above: with a live clock it returns both pages.
    assert len(client.requests) == 1
