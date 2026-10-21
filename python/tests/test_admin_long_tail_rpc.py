# -*- coding: utf-8 -*-
"""Wire shape of the single-request admin RPC families: container membership,
controller reads/writes, cold data flow control, commitlog read-ahead, pop record
export, the RocksDB cold-queue checks and the order-conf KV round trip."""
from __future__ import annotations

import json

import pytest

from client.admin import (ADMIN_CONTAINER_TIMEOUT, CONTROLLER_RPC_TIMEOUT,
                          NAMESPACE_ORDER_TOPIC_CONFIG, DefaultMQAdminExt)
from client.exception import MQBrokerException, MQClientException
from remoting.protocol.admin_body import (BrokerReplicasInfo, ReplicasInfo,
                                          ReplicaIdentity,
                                          CheckRocksdbCqWriteResult,
                                          CheckRocksdbCqWriteStatus)
from remoting.protocol.codes import RequestCode, ResponseCode
from remoting.protocol.remoting_command import RemotingCommand

ADDR = "127.0.0.1:10911"
CONTAINER_ADDR = "127.0.0.1:11011"
CONTROLLER_ADDR = "127.0.0.1:12011"
LEADER_ADDR = "127.0.0.1:12012"
NS_ADDR = "127.0.0.1:9876"
NS_ADDR2 = "127.0.0.1:9877"
TOPIC = "LongTailTopic"


class _FakeClient:
    """Replaces the synchronous call entry point and replays queued responses,
    recording every (address, request, timeout) actually sent."""

    def __init__(self, responses=None, name_server_addrs=None):
        self._responses = list(responses or [])
        self.requests = []
        self.name_server_addrs = list(name_server_addrs) if name_server_addrs is not None \
            else [NS_ADDR]

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


def _ok(body: bytes = b"", remark: str = None) -> RemotingCommand:
    r = RemotingCommand()
    r.code = ResponseCode.SUCCESS
    r.body = body
    r.remark = remark
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


def _meta_response(leader_addr: str = LEADER_ADDR) -> RemotingCommand:
    r = _ok()
    r.ext_fields = {
        "group": "controller-group",
        "controllerLeaderId": "leader-1",
        "controllerLeaderAddress": leader_addr,
        "isLeader": "false",
        "peers": "n0; n1",
    }
    return r


# ------------------------------------------------------------------ container membership

def test_add_broker_to_container_sends_config_path_on_the_plain_port():
    client = _FakeClient([_ok()])
    admin = _admin(client)
    admin.set_vip_channel_enabled(True)

    admin.add_broker_to_container(CONTAINER_ADDR, "/home/admin/rmq/broker.conf")

    addr, request, timeout = client.requests[0]
    assert request.code == RequestCode.ADD_BROKER
    assert addr == CONTAINER_ADDR  # the container RPC never rewrites the port
    assert request.ext_fields == {"configPath": "/home/admin/rmq/broker.conf"}
    assert request.body is None
    assert timeout == ADMIN_CONTAINER_TIMEOUT
    assert timeout == 20000


def test_remove_broker_from_container_sends_the_identity_header():
    client = _FakeClient([_ok()])
    admin = _admin(client)

    admin.remove_broker_from_container(CONTAINER_ADDR, "DefaultCluster", "broker-a", 1)

    addr, request, timeout = client.requests[0]
    assert request.code == RequestCode.REMOVE_BROKER
    assert addr == CONTAINER_ADDR
    assert request.ext_fields == {
        "brokerName": "broker-a",
        "brokerClusterName": "DefaultCluster",
        "brokerId": "1",
    }
    assert request.body is None
    assert timeout == 20000


def test_container_failures_surface_as_broker_exceptions():
    client = _FakeClient([_fail(ResponseCode.SYSTEM_ERROR, "container busy")])
    admin = _admin(client)
    with pytest.raises(MQBrokerException):
        admin.add_broker_to_container(CONTAINER_ADDR, "p")


# ------------------------------------------------------------------ controller

def test_get_controller_meta_data_sends_1005_with_the_3s_budget():
    client = _FakeClient([_meta_response()])
    admin = _admin(client)

    meta = admin.get_controller_meta_data(CONTROLLER_ADDR)

    addr, request, timeout = client.requests[0]
    assert request.code == RequestCode.CONTROLLER_GET_METADATA_INFO
    assert addr == CONTROLLER_ADDR
    assert timeout == CONTROLLER_RPC_TIMEOUT
    assert timeout == 3000
    assert meta.group == "controller-group"
    assert meta.controller_leader_id == "leader-1"
    assert meta.controller_leader_address == LEADER_ADDR
    assert meta.is_leader is False
    assert meta.peers == "n0; n1"


def test_get_in_sync_state_data_is_a_two_phase_leader_read():
    replicas = BrokerReplicasInfo()
    replicas.add_replica_info("broker-a", ReplicasInfo(
        master_broker_id=0, master_address=ADDR, master_epoch=1,
        sync_state_set_epoch=2,
        in_sync_replicas=[ReplicaIdentity("broker-a", 0, ADDR, True)],
        not_in_sync_replicas=[]))
    client = _FakeClient([_meta_response(), _ok(replicas.encode())])
    admin = _admin(client)

    result = admin.get_in_sync_state_data(CONTROLLER_ADDR, ["broker-a"])

    first_addr, first_request, first_timeout = client.requests[0]
    assert first_request.code == RequestCode.CONTROLLER_GET_METADATA_INFO
    assert first_addr == CONTROLLER_ADDR
    assert first_timeout == 3000

    second_addr, second_request, second_timeout = client.requests[1]
    assert second_request.code == RequestCode.CONTROLLER_GET_SYNC_STATE_DATA
    assert second_addr == LEADER_ADDR  # the sync state read goes to the leader
    assert second_timeout == 3000
    assert json.loads(second_request.body.decode("utf-8")) == ["broker-a"]
    assert result.replicas_info_table["broker-a"].master_address == ADDR
    assert result.replicas_info_table["broker-a"].in_sync_replicas[0].broker_name == "broker-a"


def test_clean_controller_broker_data_routes_to_leader_and_always_sends_invoke_time():
    client = _FakeClient([_meta_response(), _ok()])
    admin = _admin(client)

    admin.clean_controller_broker_data(CONTROLLER_ADDR, "DefaultCluster", "broker-a",
                                       broker_controller_ids_to_clean="1;2",
                                       is_clean_living_broker=True)

    addr, request, timeout = client.requests[1]
    assert request.code == RequestCode.CLEAN_BROKER_DATA
    assert addr == LEADER_ADDR
    assert timeout == 3000
    assert request.ext_fields["clusterName"] == "DefaultCluster"
    assert request.ext_fields["brokerName"] == "broker-a"
    assert request.ext_fields["brokerControllerIdsToClean"] == "1;2"
    assert request.ext_fields["isCleanLivingBroker"] == "true"
    # invokeTime is a primitive captured at construction: never omitted
    assert int(request.ext_fields["invokeTime"]) > 0


def test_clean_controller_broker_data_defaults_to_the_living_flag_false():
    client = _FakeClient([_meta_response(), _ok()])
    admin = _admin(client)

    admin.clean_controller_broker_data(CONTROLLER_ADDR, "DefaultCluster", "broker-a")

    _, request, _ = client.requests[1]
    assert request.ext_fields["isCleanLivingBroker"] == "false"
    assert "brokerControllerIdsToClean" not in request.ext_fields


def test_get_controller_config_reads_each_given_server():
    client = _FakeClient([_ok(b"controllerPath=/ctrl\n"), _ok(b"notifyRoleChange=true\n")])
    admin = _admin(client)

    config = admin.get_controller_config([CONTROLLER_ADDR, LEADER_ADDR])

    assert [addr for addr, _, _ in client.requests] == [CONTROLLER_ADDR, LEADER_ADDR]
    for _, request, timeout in client.requests:
        assert request.code == RequestCode.GET_CONTROLLER_CONFIG
        assert timeout == 20000  # the admin default, not the controller 3s budget
    assert config == {CONTROLLER_ADDR: {"controllerPath": "/ctrl"},
                      LEADER_ADDR: {"notifyRoleChange": "true"}}


def test_get_controller_config_falls_back_to_name_servers():
    client = _FakeClient([_ok(b"a=1\n"), _ok(b"b=2\n")],
                         name_server_addrs=[NS_ADDR, NS_ADDR2])
    admin = _admin(client)

    config = admin.get_controller_config()

    assert [addr for addr, _, _ in client.requests] == [NS_ADDR, NS_ADDR2]
    assert config == {NS_ADDR: {"a": "1"}, NS_ADDR2: {"b": "2"}}


def test_get_controller_config_without_targets_answers_none_and_sends_nothing():
    client = _FakeClient([], name_server_addrs=[])
    admin = _admin(client)
    assert admin.get_controller_config() is None
    assert client.requests == []


def test_get_controller_config_raises_client_exception_on_failure():
    client = _FakeClient([_fail(ResponseCode.SYSTEM_ERROR, "no quorum")])
    admin = _admin(client)
    with pytest.raises(MQClientException) as exc:
        admin.get_controller_config([CONTROLLER_ADDR])
    assert exc.value.response_code == ResponseCode.SYSTEM_ERROR
    assert "no quorum" in str(exc.value)


def test_update_controller_config_no_ops_on_empty_input():
    client = _FakeClient()
    admin = _admin(client)
    admin.update_controller_config({})
    admin.update_controller_config({"k": "v"}, controllers=[])
    assert client.requests == []


def test_update_controller_config_broadcasts_and_keeps_the_last_error():
    client = _FakeClient([_fail(1, "first refusal"), _fail(2, "second refusal")])
    admin = _admin(client)

    with pytest.raises(MQClientException) as exc:
        admin.update_controller_config({"controllerPath": "/x"}, [CONTROLLER_ADDR, LEADER_ADDR])

    assert [addr for addr, _, _ in client.requests] == [CONTROLLER_ADDR, LEADER_ADDR]
    _, request, timeout = client.requests[0]
    assert request.code == RequestCode.UPDATE_CONTROLLER_CONFIG
    assert request.body == b"controllerPath=/x\n"
    assert timeout == 20000
    # the last failing answer is the one rethrown
    assert "second refusal" in str(exc.value)
    assert exc.value.response_code == 2


def test_update_controller_config_is_silent_when_all_succeed():
    client = _FakeClient([_ok(), _ok()])
    admin = _admin(client)
    admin.update_controller_config({"k": "v"}, [CONTROLLER_ADDR, LEADER_ADDR])
    assert len(client.requests) == 2


# ------------------------------------------------------------------ cold data flow control

def test_update_cold_data_group_config_sends_properties_text_over_vip():
    client = _FakeClient([_ok()])
    admin = _admin(client)
    admin.set_vip_channel_enabled(True)

    admin.update_cold_data_flow_ctr_group_config(ADDR, {"bizKey1": "1024", "bizKey2": "8"})

    addr, request, timeout = client.requests[0]
    assert request.code == RequestCode.UPDATE_COLD_DATA_FLOW_CTR_CONFIG
    assert addr == "127.0.0.1:10909"  # this family rides the VIP channel
    assert request.body == b"bizKey1=1024\nbizKey2=8\n"
    assert request.ext_fields == {}
    assert timeout == 20000


def test_update_cold_data_group_config_skips_empty_properties():
    client = _FakeClient()
    admin = _admin(client)
    admin.update_cold_data_flow_ctr_group_config(ADDR, {})
    assert client.requests == []


def test_remove_cold_data_group_config_sends_a_plain_group_string_body():
    client = _FakeClient([_ok()])
    admin = _admin(client)
    admin.set_vip_channel_enabled(True)

    admin.remove_cold_data_flow_ctr_group_config(ADDR, "GID_order")

    addr, request, _ = client.requests[0]
    assert request.code == RequestCode.REMOVE_COLD_DATA_FLOW_CTR_CONFIG
    assert addr == "127.0.0.1:10909"
    # the body is the raw group name, not a JSON document
    assert request.body == b"GID_order"
    assert request.ext_fields == {}


def test_remove_cold_data_group_config_skips_an_empty_group():
    client = _FakeClient()
    admin = _admin(client)
    admin.remove_cold_data_flow_ctr_group_config(ADDR, "")
    assert client.requests == []


def test_get_cold_data_info_returns_the_raw_body_and_stays_on_the_plain_port():
    client = _FakeClient([_ok(b"groupCount=2\nbizKey1=1024\n")])
    admin = _admin(client)
    admin.set_vip_channel_enabled(True)

    assert admin.get_cold_data_flow_ctr_info(ADDR) == "groupCount=2\nbizKey1=1024\n"

    addr, request, _ = client.requests[0]
    assert request.code == RequestCode.GET_COLD_DATA_FLOW_CTR_INFO
    assert addr == ADDR  # the read is answered on the plain port only
    assert request.body is None


def test_get_cold_data_info_without_body_answers_none():
    client = _FakeClient([_ok()])
    admin = _admin(client)
    assert admin.get_cold_data_flow_ctr_info(ADDR) is None


# ------------------------------------------------------------------ commitlog read ahead

def test_set_commit_log_read_ahead_mode_sends_the_mode_key_and_returns_the_remark():
    client = _FakeClient([_ok(remark="FORCE")])
    admin = _admin(client)
    admin.set_vip_channel_enabled(True)

    assert admin.set_commit_log_read_ahead_mode(ADDR, "FORCE") == "FORCE"

    addr, request, timeout = client.requests[0]
    assert request.code == RequestCode.SET_COMMITLOG_READ_MODE
    assert addr == ADDR  # plain port even with the switch on
    assert request.ext_fields == {"READ_AHEAD_MODE": "FORCE"}
    assert request.body is None
    assert timeout == 20000


def test_set_commit_log_read_ahead_mode_maps_null_remark_to_none():
    client = _FakeClient([_ok(remark=None)])
    admin = _admin(client)
    assert admin.set_commit_log_read_ahead_mode(ADDR, "NONE") is None


def test_set_commit_log_read_ahead_mode_raises_broker_exception_on_failure():
    client = _FakeClient([_fail(ResponseCode.SYSTEM_ERROR, "unsupported mode")])
    admin = _admin(client)
    with pytest.raises(MQBrokerException):
        admin.set_commit_log_read_ahead_mode(ADDR, "BOGUS")


# ------------------------------------------------------------------ pop record export

def test_export_pop_records_forwards_the_caller_timeout_verbatim():
    client = _FakeClient([_ok()])
    admin = _admin(client)
    admin.set_vip_channel_enabled(True)

    admin.export_pop_records(ADDR, 4321)

    addr, request, timeout = client.requests[0]
    assert request.code == RequestCode.POP_ROLLBACK
    assert addr == ADDR
    assert timeout == 4321  # the admin default does not apply here
    assert request.body is None
    assert request.ext_fields == {}


# ------------------------------------------------------------------ RocksDB cold queue

def test_check_rocksdb_cq_write_progress_always_sends_the_store_time():
    body = CheckRocksdbCqWriteResult("ok", CheckRocksdbCqWriteStatus.CHECK_IN_PROGRESS)
    client = _FakeClient([_ok(body.encode())])
    admin = _admin(client)
    admin.set_vip_channel_enabled(True)

    result = admin.check_rocksdb_cq_write_progress(ADDR, TOPIC, 0)

    addr, request, _ = client.requests[0]
    assert request.code == RequestCode.CHECK_ROCKSDB_CQ_WRITE_PROGRESS
    assert addr == ADDR  # plain port even with the switch on
    # checkStoreTime is a primitive long: 0 is a real value and must be on the wire
    assert request.ext_fields == {"topic": TOPIC, "checkStoreTime": "0"}
    assert result.check_status == CheckRocksdbCqWriteStatus.CHECK_IN_PROGRESS
    assert result.check_result == "ok"


def test_check_rocksdb_cq_write_progress_failure_is_a_client_exception():
    client = _FakeClient([_fail(ResponseCode.SYSTEM_ERROR, "rocksdb closed")])
    admin = _admin(client)
    with pytest.raises(MQClientException) as exc:
        admin.check_rocksdb_cq_write_progress(ADDR, TOPIC, 123)
    assert "rocksdb closed" in str(exc.value)


def test_export_rocksdb_config_to_json_joins_types_with_a_trailing_separator():
    client = _FakeClient([_ok()])
    admin = _admin(client)
    admin.set_vip_channel_enabled(True)

    admin.export_rocksdb_config_to_json(
        ADDR, ["topics", "SubscriptionGroups"])

    addr, request, _ = client.requests[0]
    assert request.code == RequestCode.EXPORT_ROCKSDB_CONFIG_TO_JSON
    assert addr == ADDR
    # the lookup is case-insensitive, the wire value canonical, and the separator
    # is appended after every member
    assert request.ext_fields == {"configType": "topics;subscriptionGroups;"}
    assert request.body is None


def test_export_rocksdb_config_to_json_rejects_unknown_types():
    client = _FakeClient()
    admin = _admin(client)
    with pytest.raises(ValueError):
        admin.export_rocksdb_config_to_json(ADDR, ["notAType"])
    assert client.requests == []


def test_export_rocksdb_config_to_json_failure_is_a_client_exception():
    client = _FakeClient([_fail(ResponseCode.SYSTEM_ERROR, "io refused")])
    admin = _admin(client)
    with pytest.raises(MQClientException) as exc:
        admin.export_rocksdb_config_to_json(ADDR, ["topics"])
    assert "io refused" in str(exc.value)


# ------------------------------------------------------------------ order conf

def test_order_conf_cluster_mode_broadcasts_put_kv_config_to_every_name_server():
    client = _FakeClient([_ok(), _ok()], name_server_addrs=[NS_ADDR, NS_ADDR2])
    admin = _admin(client)

    admin.create_or_update_order_conf("orderTopic", "broker-a:8", is_cluster=True)

    assert [addr for addr, _, _ in client.requests] == [NS_ADDR, NS_ADDR2]
    _, request, timeout = client.requests[0]
    assert request.code == RequestCode.PUT_KV_CONFIG
    assert request.ext_fields == {
        "namespace": NAMESPACE_ORDER_TOPIC_CONFIG,
        "key": "orderTopic",
        "value": "broker-a:8",
    }
    assert request.ext_fields["namespace"] == "ORDER_TOPIC_CONFIG"
    assert timeout == 20000


def test_order_conf_cluster_mode_rethrows_the_last_failing_name_server():
    client = _FakeClient([_ok(), _fail(1, "ns2 refused")],
                         name_server_addrs=[NS_ADDR, NS_ADDR2])
    admin = _admin(client)
    with pytest.raises(MQClientException) as exc:
        admin.create_or_update_order_conf("orderTopic", "broker-a:8", is_cluster=True)
    assert "ns2 refused" in str(exc.value)


def test_order_conf_broker_mode_merges_into_the_existing_kv_value():
    get_response = _ok()
    get_response.ext_fields = {"value": "broker-a:3;broker-b:4"}
    client = _FakeClient([get_response, _ok()], name_server_addrs=[NS_ADDR])
    admin = _admin(client)

    admin.create_or_update_order_conf("orderTopic", "broker-b:9", is_cluster=False)

    get_addr, get_request, _ = client.requests[0]
    assert get_request.code == RequestCode.GET_KV_CONFIG
    assert get_request.ext_fields == {"namespace": "ORDER_TOPIC_CONFIG",
                                      "key": "orderTopic"}
    _, put_request, _ = client.requests[1]
    assert put_request.code == RequestCode.PUT_KV_CONFIG
    # the untouched broker keeps its entry, the updated one is replaced
    assert put_request.ext_fields == {
        "namespace": "ORDER_TOPIC_CONFIG",
        "key": "orderTopic",
        "value": "broker-a:3;broker-b:9",
    }


def test_order_conf_broker_mode_falls_back_to_the_new_value_when_the_read_fails():
    client = _FakeClient([_fail(ResponseCode.SYSTEM_ERROR, "no kv yet"), _ok()],
                         name_server_addrs=[NS_ADDR])
    admin = _admin(client)

    admin.create_or_update_order_conf("orderTopic", "broker-a:3", is_cluster=False)

    _, put_request, _ = client.requests[1]
    assert put_request.ext_fields["value"] == "broker-a:3"


def test_order_conf_requires_key_and_value():
    client = _FakeClient()
    admin = _admin(client)
    with pytest.raises(MQClientException):
        admin.create_or_update_order_conf("", "v")
    with pytest.raises(MQClientException):
        admin.create_or_update_order_conf("k", "")
    assert client.requests == []
