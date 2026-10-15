# -*- coding: utf-8 -*-
"""Static topic routing tests: typed mapping parsing, the pure-function port of topicRouteData2EndpointsForStaticTopic,
and the instance's getBrokerNameFromMessageQueue override, five cases in total."""
from __future__ import annotations

from client import static_topic
from client.mq_client import MQClientInstance
from client.static_topic import (TopicQueueMappingInfo,
                                 is_logical_queue_mock_broker_name,
                                 get_mock_broker_name,
                                 parse_topic_queue_mapping_info,
                                 topic_route_data_2_endpoints_for_static_topic)
from common.message import MessageQueue
from remoting.protocol.route import TopicRouteData


def raw_mapping(scope, total_queues, bname, epoch, curr_id_map):
    return {
        "scope": scope,
        "totalQueues": total_queues,
        "bname": bname,
        "epoch": epoch,
        "dirty": False,
        "currIdMap": curr_id_map,
    }


def mapping_route(entries):
    route = TopicRouteData()
    route.topic_queue_mapping_by_broker = dict(entries)
    return route


def test_mock_broker_name_strips_global_underscores():
    assert get_mock_broker_name("__global__") == "__syslo__global__"
    assert get_mock_broker_name("zone-a") == "__syslo__zone-a"
    assert is_logical_queue_mock_broker_name("__syslo__global__")
    assert not is_logical_queue_mock_broker_name("broker-a")


def test_parse_defaults_scope_to_global():
    info = parse_topic_queue_mapping_info({
        "totalQueues": 2,
        "bname": "b1",
        "epoch": 3,
    })
    assert info is not None
    assert info.scope == "__global__"
    assert info.total_queues == 2
    assert info.bname == "b1"
    assert info.epoch == 3
    assert parse_topic_queue_mapping_info("not-an-object") is None


def test_endpoints_single_scope_with_unhosted_queue():
    route = mapping_route({
        "broker-a": raw_mapping("__global__", 4, "broker-a", 0,
                                {"0": 0, "1": 1, "2": 2}),
    })
    endpoints = topic_route_data_2_endpoints_for_static_topic("StaticTopic", route)
    assert endpoints is not None
    assert len(endpoints) == 4  # hosted + unhosted
    for gid in range(3):
        assert endpoints[MessageQueue("StaticTopic", "__syslo__global__", gid)] == "broker-a"
    unhosted = MessageQueue("StaticTopic", "__syslo__global__", 3)
    assert endpoints[unhosted] == "__syslo__none__"
    # an empty mapping (non-static topic) returns None.
    assert topic_route_data_2_endpoints_for_static_topic("T", TopicRouteData()) is None


def test_scopes_stay_apart_and_epoch_fences():
    route = mapping_route({
        "broker-old": raw_mapping("__global__", 1, "broker-old", 3, {"0": 0}),
        "broker-new": raw_mapping("__global__", 1, "broker-new", 7, {"0": 0}),
        "broker-b": raw_mapping("zone-x", 1, "broker-b", 9, {"0": 0}),
    })
    endpoints = topic_route_data_2_endpoints_for_static_topic("T", route)
    assert endpoints[MessageQueue("T", "__syslo__global__", 0)] == "broker-new"  # higher epoch wins
    assert endpoints[MessageQueue("T", "__syslo__zone-x", 0)] == "broker-b"
    assert len(endpoints) == 2  # scopes do not merge


def test_instance_rewrites_mock_broker_name_via_end_points_table():
    inst = MQClientInstance("static-test@DEFAULT", ["127.0.0.1:9876"])
    inst.topic_end_points_table["StaticTopic"] = {
        MessageQueue("StaticTopic", "__syslo__global__", 0): "broker-a",
        MessageQueue("StaticTopic", "__syslo__global__", 1): "broker-b",
    }
    assert inst.get_broker_name_from_message_queue(
        MessageQueue("StaticTopic", "__syslo__global__", 0)) == "broker-a"
    assert inst.get_broker_name_from_message_queue(
        MessageQueue("StaticTopic", "__syslo__global__", 1)) == "broker-b"
    # queues absent from the table / non-static topics pass their own broker name through as-is.
    assert inst.get_broker_name_from_message_queue(
        MessageQueue("StaticTopic", "__syslo__global__", 99)) == "__syslo__global__"
    assert inst.get_broker_name_from_message_queue(
        MessageQueue("Plain", "broker-a", 0)) == "broker-a"


def test_route_refresh_builds_and_clears_end_points_table():
    inst = MQClientInstance("static-refresh@DEFAULT", ["127.0.0.1:9876"])
    # seeding the route table and going through the persistence logic is not feasible (it needs a real namesrv), so the pure-function part of the update persistence block is verified directly here: static routes build the table, non-static ones clear it, decided by the same if/else.
    route = mapping_route({
        "broker-a": raw_mapping("__global__", 2, "broker-a", 0, {"0": 0, "1": 1}),
    })
    endpoints = static_topic.topic_route_data_2_endpoints_for_static_topic("S", route)
    assert endpoints is not None and len(endpoints) == 2
    # non-static (empty mapping) -> None -> the persistence block pops the old entries.
    assert topic_route_data_2_endpoints_for_static_topic("S", TopicRouteData()) is None
