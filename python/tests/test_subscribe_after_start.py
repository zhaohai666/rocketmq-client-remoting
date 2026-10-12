# -*- coding: utf-8 -*-
from __future__ import annotations

from typing import List

from client.consumer import DefaultMQPushConsumer, MessageSelector
from common.subscription_data import ExpressionType
from remoting.protocol.heartbeat import HeartbeatData

TOPIC = "SubAfterStartTopic"


class FakeClient:
    """只实现心跳路径要用的两个方法：截报文体 + 记录路由登记。"""

    def __init__(self) -> None:
        self.heartbeats: List[HeartbeatData] = []
        self.topics_in_use: List[str] = []

    def get_route_of_all_brokers(self) -> List[str]:
        return ["127.0.0.1:10911"]

    def get_all_broker_addrs(self) -> List[str]:
        # 消费者心跳取的是「每个 brokerId 一台」的地址表（主 + 从）。这个替身只有一台，
        # 两份地址表内容相同；扇出是否真的覆盖从节点由
        # test_producer_unregister.test_consumer_heartbeat_fans_out_to_slaves_... 锁。
        return self.get_route_of_all_brokers()

    def send_heartbeat(self, addr: str, hb: HeartbeatData, timeout_millis: int) -> None:
        self.heartbeats.append(hb)

    def register_topic_in_use(self, topic: str) -> None:
        self.topics_in_use.append(topic)


def started(group: str = "GID_SubAfterStart"):
    """造一个「已启动」的消费者，client 是替身，心跳报文可截获。"""
    c = DefaultMQPushConsumer(group)
    fake = FakeClient()
    c._mq_client = fake
    c._started = True
    return c, fake


def subs_of(hb: HeartbeatData):
    """心跳体里那一份 ConsumerData 的订阅表（topic → SubscriptionData）。"""
    cd = list(hb.consumer_data_set)[0]
    return {s.topic: s for s in cd.subscription_data_set}


def test_subscribe_after_start_reaches_the_live_table_and_pushes_one_heartbeat():
    pass
    c, fake = started()

    c.subscribe(TOPIC, "TagA||TagB")

    assert len(fake.heartbeats) == 1, "一次 subscribe 恰好一轮心跳"
    body = subs_of(fake.heartbeats[0])
    assert TOPIC in body, "心跳报文必须带新订阅（否则 broker 记不到 topicGroupTable）"
    assert body[TOPIC].sub_string == "TagA||TagB"
    assert body[TOPIC].tags_set == {"TagA", "TagB"}
    assert fake.topics_in_use == [TOPIC]
    assert [s.topic for s in c.subscriptions()] == [TOPIC]


def test_selector_subscribe_after_start_keeps_expression_type_in_the_body():
    pass
    c, fake = started()
    c.subscribe_with_selector(TOPIC, MessageSelector.by_sql("a > 1"))

    assert len(fake.heartbeats) == 1
    body = subs_of(fake.heartbeats[0])
    assert body[TOPIC].expression_type == ExpressionType.SQL92
    assert body[TOPIC].sub_string == "a > 1"
    assert not body[TOPIC].tags_set, "SQL92 不带 tagsSet"

    c2, fake2 = started("GID_SubAfterStart2")
    c2.subscribe_with_selector(TOPIC, MessageSelector.by_tag("X||Y"))
    assert len(fake2.heartbeats) == 1
    tag_body = subs_of(fake2.heartbeats[0])[TOPIC]
    assert tag_body.expression_type == ExpressionType.TAG
    assert tag_body.tags_set == {"X", "Y"}


def test_subscribe_before_start_only_records():
    pass
    c = DefaultMQPushConsumer("GID_SubBeforeStart")
    c.subscribe(TOPIC, "*")
    assert [s.topic for s in c.subscriptions()] == [TOPIC]

    fake = FakeClient()
    c._mq_client = fake
    c._started = False
    c.subscribe("T2", "*")
    assert fake.heartbeats == []
    assert fake.topics_in_use == []


def test_unsubscribe_does_not_push_a_heartbeat():
    pass
    c, fake = started()
    c.subscribe(TOPIC, "*")
    assert len(fake.heartbeats) == 1

    c.unsubscribe(TOPIC)

    assert len(fake.heartbeats) == 1, "unsubscribe 不发心跳"
    assert c.subscriptions() == []


def test_every_heartbeat_is_rebuilt_from_the_live_table():
    pass
    c, fake = started()
    c.subscribe("T_A", "*")
    c.subscribe("T_B", "*")
    c.unsubscribe("T_A")

    c._send_heartbeat_to_all_broker()

    latest = subs_of(fake.heartbeats[-1])
    assert set(latest) == {"T_B"}, "撤掉的 T_A 不能再出现在报文里"
    assert set(subs_of(fake.heartbeats[0])) == {"T_A"}


def test_resubscribe_same_topic_replaces_and_heartbeats_again():
    pass
    c, fake = started()
    c.subscribe(TOPIC, "A")
    c.subscribe(TOPIC, "B")

    assert len(fake.heartbeats) == 2
    assert subs_of(fake.heartbeats[-1])[TOPIC].sub_string == "B"
    cd = list(fake.heartbeats[-1].consumer_data_set)[0]
    assert len(cd.subscription_data_set) == 1, "同一 topic 覆盖，不是两条"
