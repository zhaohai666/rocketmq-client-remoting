# -*- coding: utf-8 -*-
"""Wire shape of the #47 ops RPC group (Java MQClientAPIImpl:2773-3816):
queryCorrectionOffset(308), the three unit-topic reads (311/312/313),
getAllProducerInfo(328), getBrokerHAStatus(907), resetMasterFlushOffset(908),
electMaster(1002), getBrokerEpochCache(1007) and switchTimerEngine(5001).
Everything runs against the fake client; no real cluster involved."""
from __future__ import annotations

import json

from client.admin import CONTROLLER_RPC_TIMEOUT, DefaultMQAdminExt
from client.exception import MQBrokerException, MQClientException
from remoting.protocol.codes import RequestCode, ResponseCode
from remoting.protocol.remoting_command import RemotingCommand

ADDR = "127.0.0.1:10911"
CONTROLLER_ADDR = "127.0.0.1:12011"
LEADER_ADDR = "127.0.0.1:12012"
NS_ADDR = "127.0.0.1:9876"
TOPIC = "OpsTopic"
GROUP = "GID_OpsTopic"


class _FakeClient:
    """Replaces the synchronous call entry point and replays queued responses,
    recording every (address, request, timeout) actually sent."""

    def __init__(self, responses=None, name_server_addrs=None):
        self._responses = list(responses or [])
        self.requests = []
        self.name_server_addrs = [NS_ADDR] if name_server_addrs is None \
            else list(name_server_addrs)

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


def _ok(body: bytes = b"", ext=None) -> RemotingCommand:
    r = RemotingCommand()
    r.code = ResponseCode.SUCCESS
    r.body = body
    r.ext_fields = dict(ext or {})
    return r


def _admin(client: _FakeClient) -> DefaultMQAdminExt:
    admin = DefaultMQAdminExt()
    admin._mq_client = client
    admin._started = True
    return admin


def _meta_response(leader_addr: str = LEADER_ADDR) -> RemotingCommand:
    r = _ok()
    r.ext_fields = {"controllerLeaderAddress": leader_addr}
    return r


# ------------------------------------------------------------------ 308

def test_query_correction_offset_sends_the_three_header_fields():
    body = json.dumps({"correctionOffsets": {"0": 110, "3": 407}}).encode()
    client = _FakeClient([_ok(body)])
    admin = _admin(client)

    out = admin.query_correction_offset(ADDR, TOPIC, GROUP, {"GID_other", "GID_x"})

    addr, request, timeout = client.requests[0]
    assert request.code == RequestCode.QUERY_CORRECTION_OFFSET
    assert addr == ADDR
    assert request.body is None
    # Java joins the Set itself, comma-joined; compareGroup carries the group.
    assert request.ext_fields["topic"] == TOPIC
    assert request.ext_fields["compareGroup"] == GROUP
    assert set(request.ext_fields["filterGroups"].split(",")) == {"GID_other", "GID_x"}
    assert out == {0: 110, 3: 407}
    assert timeout == admin.timeout_millis


def test_query_correction_offset_vip_rewrites_while_the_switch_is_on():
    client = _FakeClient([_ok(json.dumps({"correctionOffsets": {}}).encode())])
    admin = _admin(client)
    admin.set_vip_channel_enabled(True)

    admin.query_correction_offset(ADDR, TOPIC, GROUP)

    # Java wraps this one RPC in brokerVIPChannel(flag, addr): port - 2 when enabled.
    assert client.requests[0][0] == "127.0.0.1:10909"


def test_query_correction_offset_without_filter_groups_omits_the_field():
    client = _FakeClient([_ok(json.dumps({"correctionOffsets": {}}).encode())])
    admin = _admin(client)

    admin.query_correction_offset(ADDR, TOPIC, GROUP)

    ext = client.requests[0][1].ext_fields
    assert "filterGroups" not in ext
    assert ext["topic"] == TOPIC and ext["compareGroup"] == GROUP


# ------------------------------------------------------------------ 311/312/313

def test_unit_topic_list_family_hits_one_name_server_with_a_null_header():
    body = json.dumps({"topicList": ["TopicA", "%RETRY%G"]}).encode()
    for code, call in ((RequestCode.GET_UNIT_TOPIC_LIST, "get_unit_topic_list"),
                       (RequestCode.GET_HAS_UNIT_SUB_TOPIC_LIST, "get_has_unit_sub_topic_list"),
                       (RequestCode.GET_HAS_UNIT_SUB_UNUNIT_TOPIC_LIST,
                        "get_has_unit_sub_un_unit_topic_list")):
        client = _FakeClient([_ok(body)])
        admin = _admin(client)

        out = getattr(admin, call)(contain_retry=True)

        addr, request, timeout = client.requests[0]
        assert request.code == code
        assert addr == NS_ADDR          # Java invokeSync(null, ...) -> a name server
        assert request.ext_fields == {}  # null header: no ext fields at all
        assert request.body is None
        assert out.topic_list == ["TopicA", "%RETRY%G"]
        assert out.broker_addr is None


def test_unit_topic_list_filters_retry_topics_locally_when_not_contained():
    body = json.dumps({"topicList": ["TopicA", "%RETRY%G", "%RETRY%G_lite"]}).encode()
    client = _FakeClient([_ok(body)])
    admin = _admin(client)

    out = admin.get_unit_topic_list(contain_retry=False)

    # Java strips the %RETRY% prefix client-side (MQClientAPIImpl:2817-2825).
    assert out.topic_list == ["TopicA"]
    # the raw answer is untouched on the wire: the filter happens after decoding
    assert client.requests[0][1].ext_fields == {}


def test_unit_topic_list_failure_raises_a_client_exception():
    client = _FakeClient([_fail_response()])
    admin = _admin(client)

    import pytest
    with pytest.raises(MQClientException):
        admin.get_has_unit_sub_topic_list()


def _fail_response():
    r = RemotingCommand()
    r.code = ResponseCode.SYSTEM_ERROR
    r.remark = "boom"
    return r


# ------------------------------------------------------------------ 328

def test_get_all_producer_info_decodes_the_producer_table():
    body = json.dumps({"data": {"G": [{"clientId": "cid-1", "remoteIP": "10.0.0.1",
                                       "language": "JAVA", "version": 500,
                                       "lastUpdateTimestamp": 1700000000000}]}}).encode()
    client = _FakeClient([_ok(body)])
    admin = _admin(client)

    table = admin.get_all_producer_info(ADDR)

    addr, request, _timeout = client.requests[0]
    assert request.code == RequestCode.GET_ALL_PRODUCER_INFO
    assert addr == ADDR
    assert request.ext_fields == {}   # GetAllProducerInfoRequestHeader is empty
    info = table.data["G"][0]
    assert info.client_id == "cid-1"
    assert info.remote_ip == "10.0.0.1"
    assert info.version == 500
    assert info.last_update_timestamp == 1700000000000


def test_get_all_producer_info_vip_rewrites_while_the_switch_is_on():
    client = _FakeClient([_ok(b"{}")])
    admin = _admin(client)
    admin.set_vip_channel_enabled(True)

    admin.get_all_producer_info(ADDR)

    assert client.requests[0][0] == "127.0.0.1:10909"


# ------------------------------------------------------------------ 907

def test_get_broker_ha_status_decodes_nested_runtime_info():
    body = json.dumps({
        "master": True,
        "masterCommitLogMaxOffset": 9001,
        "inSyncSlaveNums": 2,
        "haConnectionInfo": [{"addr": "10.0.0.2:10912", "slaveAckOffset": 8900,
                              "diff": 101, "inSync": True,
                              "transferredByteInSecond": 2048,
                              "transferFromWhere": 8700}],
        "haClientRuntimeInfo": {"masterAddr": "10.0.0.1:10912",
                                "transferredByteInSecond": 12,
                                "maxOffset": 9001, "lastReadTimestamp": 111,
                                "lastWriteTimestamp": 222, "masterFlushOffset": 8800,
                                "activated": True},
    }).encode()
    client = _FakeClient([_ok(body)])
    admin = _admin(client)

    info = admin.get_broker_ha_status(ADDR)

    addr, request, _timeout = client.requests[0]
    assert request.code == RequestCode.GET_BROKER_HA_STATUS
    assert addr == ADDR             # plain broker address, no -2 rewrite
    assert request.ext_fields == {}  # null header
    assert info.master is True
    assert info.master_commit_log_max_offset == 9001
    assert info.in_sync_slave_nums == 2
    conn = info.ha_connection_info[0]
    assert (conn.addr, conn.slave_ack_offset, conn.diff, conn.in_sync) == \
        ("10.0.0.2:10912", 8900, 101, True)
    assert conn.transferred_byte_in_second == 2048
    assert conn.transfer_from_where == 8700
    hc = info.ha_client_runtime_info
    assert hc.master_addr == "10.0.0.1:10912" and hc.max_offset == 9001
    assert hc.master_flush_offset == 8800 and hc.is_activated is True


# ------------------------------------------------------------------ 908

def test_reset_master_flush_offset_sends_the_primitive_long():
    client = _FakeClient([_ok()])
    admin = _admin(client)

    admin.reset_master_flush_offset(ADDR, 12345)

    addr, request, timeout = client.requests[0]
    assert request.code == RequestCode.RESET_MASTER_FLUSH_OFFSET
    assert addr == ADDR
    assert request.ext_fields == {"masterFlushOffset": "12345"}
    assert request.body is None
    assert timeout == CONTROLLER_RPC_TIMEOUT   # Java pins this family to 3000


# ------------------------------------------------------------------ 1002

def test_elect_master_resolves_the_leader_then_sends_the_admin_trigger_header():
    member_body = json.dumps({
        "cluster": "DefaultCluster", "brokerName": "broker-a",
        "brokerAddrs": {"0": "10.0.0.1:10911", "1": "10.0.0.2:10911"},
    }).encode()
    client = _FakeClient([_meta_response(), _ok(member_body, ext={
        "masterBrokerId": "1", "masterAddress": "10.0.0.2:10911",
        "masterEpoch": "3", "syncStateSetEpoch": "7"})])
    admin = _admin(client)

    header, group = admin.elect_master(CONTROLLER_ADDR, "DefaultCluster",
                                       "broker-a", 1)

    # step 1: 1005 metadata read names the leader
    assert client.requests[0][0] == CONTROLLER_ADDR
    assert client.requests[0][1].code == RequestCode.CONTROLLER_GET_METADATA_INFO
    # step 2: 1002 goes to the leader with the ofAdminTrigger shape
    addr, request, timeout = client.requests[1]
    assert addr == LEADER_ADDR
    assert request.code == RequestCode.CONTROLLER_ELECT_MASTER
    assert request.ext_fields["clusterName"] == "DefaultCluster"
    assert request.ext_fields["brokerName"] == "broker-a"
    assert request.ext_fields["brokerId"] == "1"
    assert request.ext_fields["designateElect"] == "true"
    assert int(request.ext_fields["invokeTime"]) > 0
    assert timeout == CONTROLLER_RPC_TIMEOUT   # Java pins the elect leg to 3000
    assert header.master_broker_id == 1
    assert header.master_address == "10.0.0.2:10911"
    assert header.master_epoch == 3 and header.sync_state_set_epoch == 7
    assert group.cluster == "DefaultCluster" and group.broker_name == "broker-a"
    assert group.broker_addrs == {0: "10.0.0.1:10911", 1: "10.0.0.2:10911"}


def test_elect_master_without_a_designated_id_sends_minus_one():
    client = _FakeClient([_meta_response(), _ok(b"{}")])
    admin = _admin(client)

    admin.elect_master(CONTROLLER_ADDR, "DefaultCluster", "broker-a")

    assert client.requests[1][1].ext_fields["brokerId"] == "-1"


# ------------------------------------------------------------------ 1007

def test_get_broker_epoch_cache_decodes_the_epoch_list():
    body = json.dumps({
        "clusterName": "DefaultCluster", "brokerName": "broker-a", "brokerId": 1,
        "epochList": [{"epoch": 1, "startOffset": 0, "endOffset": 500},
                      {"epoch": 2, "startOffset": 500}],
        "maxOffset": 900,
    }).encode()
    client = _FakeClient([_ok(body)])
    admin = _admin(client)

    cache = admin.get_broker_epoch_cache(ADDR)

    addr, request, timeout = client.requests[0]
    assert request.code == RequestCode.GET_BROKER_EPOCH_CACHE
    assert addr == ADDR
    assert request.ext_fields == {}
    assert timeout == CONTROLLER_RPC_TIMEOUT   # Java pins this family to 3000
    assert (cache.cluster_name, cache.broker_name, cache.broker_id) == \
        ("DefaultCluster", "broker-a", 1)
    assert cache.max_offset == 900
    assert [(e.epoch, e.start_offset, e.end_offset) for e in cache.epoch_list] == \
        [(1, 0, 500), (2, 500, 9223372036854775807)]  # Java LAST_EPOCH_END_OFFSET


# ------------------------------------------------------------------ 5001

def test_switch_timer_engine_sends_the_single_e_t_field():
    client = _FakeClient([_ok()])
    admin = _admin(client)

    admin.switch_timer_engine(ADDR, "rocksdb", timeout_millis=4500)

    addr, request, timeout = client.requests[0]
    assert request.code == RequestCode.SWITCH_TIMER_ENGINE
    assert addr == ADDR
    assert request.ext_fields == {"E_T": "rocksdb"}
    assert request.body is None
    assert timeout == 4500


# ------------------------------------------------------------------ failure shapes

def test_broker_family_failure_raises_a_broker_exception():
    client = _FakeClient([_fail_response()])
    admin = _admin(client)

    import pytest
    with pytest.raises(MQBrokerException):
        admin.get_broker_ha_status(ADDR)


def test_elect_master_failure_raises_a_broker_exception():
    client = _FakeClient([_meta_response(), _fail_response()])
    admin = _admin(client)

    import pytest
    with pytest.raises(MQBrokerException):
        admin.elect_master(CONTROLLER_ADDR, "DefaultCluster", "broker-a")
