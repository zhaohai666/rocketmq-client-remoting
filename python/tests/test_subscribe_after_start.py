# -*- coding: utf-8 -*-
from __future__ import annotations

from typing import List

from client.consumer import DefaultMQPushConsumer, MessageSelector
from common.subscription_data import ExpressionType
from remoting.protocol.heartbeat import HeartbeatData

TOPIC = "SubAfterStartTopic"


class FakeClient:
    """Implements only the two methods needed by the heartbeat path: intercepting the report body + recording route registration."""

    def __init__(self) -> None:
        self.heartbeats: List[HeartbeatData] = []
        self.topics_in_use: List[str] = []

    def get_route_of_all_brokers(self) -> List[str]:
        return ["127.0.0.1:10911"]

    def get_all_broker_addrs(self) -> List[str]:
        # the consumer heartbeat takes the "one per brokerId" address table (master + slaves). This stand-in has only one machine,
        # so the two address tables have the same contents; whether the fan-out really covers slaves is locked by
        # test_producer_unregister.test_consumer_heartbeat_fans_out_to_slaves_...
        return self.get_route_of_all_brokers()

    def send_heartbeat(self, addr: str, hb: HeartbeatData, timeout_millis: int) -> None:
        self.heartbeats.append(hb)

    def register_topic_in_use(self, topic: str) -> None:
        self.topics_in_use.append(topic)


def started(group: str = "GID_SubAfterStart"):
    """Build a "started" consumer whose client is a stand-in and whose heartbeat reports can be intercepted."""
    c = DefaultMQPushConsumer(group)
    fake = FakeClient()
    c._mq_client = fake
    c._started = True
    return c, fake


def subs_of(hb: HeartbeatData):
    """The subscription table of the ConsumerData in the heartbeat body (topic -> SubscriptionData)."""
    cd = list(hb.consumer_data_set)[0]
    return {s.topic: s for s in cd.subscription_data_set}


def test_subscribe_after_start_reaches_the_live_table_and_pushes_one_heartbeat():
    pass
    c, fake = started()

    c.subscribe(TOPIC, "TagA||TagB")

    assert len(fake.heartbeats) == 1, "one subscribe triggers exactly one heartbeat"
    body = subs_of(fake.heartbeats[0])
    assert TOPIC in body, "the heartbeat report must carry the new subscription (otherwise the broker cannot record it in topicGroupTable)"
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
    assert not body[TOPIC].tags_set, "SQL92 carries no tagsSet"

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

    assert len(fake.heartbeats) == 1, "unsubscribe does not push a heartbeat"
    assert c.subscriptions() == []


def test_every_heartbeat_is_rebuilt_from_the_live_table():
    pass
    c, fake = started()
    c.subscribe("T_A", "*")
    c.subscribe("T_B", "*")
    c.unsubscribe("T_A")

    c._send_heartbeat_to_all_broker()

    latest = subs_of(fake.heartbeats[-1])
    assert set(latest) == {"T_B"}, "the revoked T_A must no longer appear in the report"
    assert set(subs_of(fake.heartbeats[0])) == {"T_A"}


def test_resubscribe_same_topic_replaces_and_heartbeats_again():
    pass
    c, fake = started()
    c.subscribe(TOPIC, "A")
    c.subscribe(TOPIC, "B")

    assert len(fake.heartbeats) == 2
    assert subs_of(fake.heartbeats[-1])[TOPIC].sub_string == "B"
    cd = list(fake.heartbeats[-1].consumer_data_set)[0]
    assert len(cd.subscription_data_set) == 1, "the same topic is replaced, not two entries"
