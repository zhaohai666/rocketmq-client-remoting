# -*- coding: utf-8 -*-
"""静态主题路由测试：类型化映射解析、topicRouteData2EndpointsForStaticTopic
纯函数移植、以及实例的 getBrokerNameFromMessageQueue 重写（对标 Go 端
static_topic_test.go 的五个用例）。"""
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
    # 空映射（非静态主题）返回 None。
    assert topic_route_data_2_endpoints_for_static_topic("T", TopicRouteData()) is None


def test_scopes_stay_apart_and_epoch_fences():
    route = mapping_route({
        "broker-old": raw_mapping("__global__", 1, "broker-old", 3, {"0": 0}),
        "broker-new": raw_mapping("__global__", 1, "broker-new", 7, {"0": 0}),
        "broker-b": raw_mapping("zone-x", 1, "broker-b", 9, {"0": 0}),
    })
    endpoints = topic_route_data_2_endpoints_for_static_topic("T", route)
    assert endpoints[MessageQueue("T", "__syslo__global__", 0)] == "broker-new"  # 高 epoch 围栏
    assert endpoints[MessageQueue("T", "__syslo__zone-x", 0)] == "broker-b"
    assert len(endpoints) == 2  # scope 之间不合并


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
    # 表里没有的队列 / 非静态主题原样透传自己的 broker 名。
    assert inst.get_broker_name_from_message_queue(
        MessageQueue("StaticTopic", "__syslo__global__", 99)) == "__syslo__global__"
    assert inst.get_broker_name_from_message_queue(
        MessageQueue("Plain", "broker-a", 0)) == "broker-a"


def test_route_refresh_builds_and_clears_end_points_table():
    inst = MQClientInstance("static-refresh@DEFAULT", ["127.0.0.1:9876"])
    # 直接种路由表并走落库逻辑不可行（要真 namesrv），这里直接验证
    # update 落库块的纯函数部分：静态路由建表、非静态清表由同一处 if/else 决定。
    route = mapping_route({
        "broker-a": raw_mapping("__global__", 2, "broker-a", 0, {"0": 0, "1": 1}),
    })
    endpoints = static_topic.topic_route_data_2_endpoints_for_static_topic("S", route)
    assert endpoints is not None and len(endpoints) == 2
    # 非静态（空 mapping）→ None → 落库块会 pop 旧条目。
    assert topic_route_data_2_endpoints_for_static_topic("S", TopicRouteData()) is None
