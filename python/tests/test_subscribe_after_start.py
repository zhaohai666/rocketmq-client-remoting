# -*- coding: utf-8 -*-
"""订阅后置 + 立即心跳（#73）离线单测，不需要集群。

对齐基准（Java 5.5.1 ``DefaultMQPushConsumerImpl``）：
  * ``subscribe(topic, subExpression):1265-1275``、class-filter 变体 ``:1277-1287``、
    MessageSelector 变体 —— 三者都是 put 进 ``subscriptionInner`` 之后
    ``if (this.mQClientFactory != null) this.mQClientFactory.sendHeartbeatToAllBrokerWithLock();``
    **没有**「started 之后禁止订阅」这道闸门，且心跳是**同步立即**发的。
  * ``unsubscribe(topic):1317-1319`` 只 remove，**不**发心跳。
  * 心跳体 ``subscriptions():1385-1387`` = ``subscriptionInner.values()``，即**当前**订阅表。

为什么必须离线锁死：真机上「新订阅没推给 broker」是静默的 —— 只表现为
``QUERY_TOPIC_CONSUME_BY_WHO(300)`` 查不到本组、新 topic 分不到队列，客户端一声不响。
这里用替身 client 把**真实**心跳报文体（``_build_heartbeat`` 造的）截下来，
断言里面的订阅集就是刚写进去的那一份，以及 ``unsubscribe`` 一轮都不多发。
"""
from __future__ import annotations

from typing import List

from rocketmq.client.consumer import DefaultMQPushConsumer, MessageSelector
from rocketmq.common.subscription_data import ExpressionType
from rocketmq.remoting.protocol.heartbeat import HeartbeatData

TOPIC = "SubAfterStartTopic"


class FakeClient:
    """只实现心跳路径要用的两个方法：截报文体 + 记录路由登记。"""

    def __init__(self) -> None:
        self.heartbeats: List[HeartbeatData] = []
        self.topics_in_use: List[str] = []

    def get_route_of_all_brokers(self) -> List[str]:
        return ["127.0.0.1:10911"]

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
    """Java :1265-1275：start() 之后 put 完立即发一轮心跳，报文里带上新订阅。"""
    c, fake = started()

    c.subscribe(TOPIC, "TagA||TagB")

    assert len(fake.heartbeats) == 1, "一次 subscribe 恰好一轮心跳"
    body = subs_of(fake.heartbeats[0])
    assert TOPIC in body, "心跳报文必须带新订阅（否则 broker 记不到 topicGroupTable）"
    assert body[TOPIC].sub_string == "TagA||TagB"
    assert body[TOPIC].tags_set == {"TagA", "TagB"}
    # 路由刷新任务要覆盖新 topic（对应 Java 周期任务遍历 live 订阅表收集 topic）
    assert fake.topics_in_use == [TOPIC]
    assert [s.topic for s in c.subscriptions()] == [TOPIC]


def test_selector_subscribe_after_start_keeps_expression_type_in_the_body():
    """Java :1289-1303：MessageSelector 变体同样立即发心跳，且 expressionType 照原样。"""
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
    """Java 的 ``mQClientFactory == null`` 分支：没启动就只落订阅表，不碰网络。"""
    c = DefaultMQPushConsumer("GID_SubBeforeStart")
    c.subscribe(TOPIC, "*")
    assert [s.topic for s in c.subscriptions()] == [TOPIC]

    # 就算 client 已经建好、只是还没 start()，也不发（Java 的门是 mQClientFactory != null，
    # 本端口的门是 _started and _mq_client —— start() 末尾才会同时成立）。
    fake = FakeClient()
    c._mq_client = fake
    c._started = False
    c.subscribe("T2", "*")
    assert fake.heartbeats == []
    assert fake.topics_in_use == []


def test_unsubscribe_does_not_push_a_heartbeat():
    """Java :1317-1319：只删表项。多发一轮心跳会掩盖「订阅已撤但 broker 仍记着」的差异。"""
    c, fake = started()
    c.subscribe(TOPIC, "*")
    assert len(fake.heartbeats) == 1

    c.unsubscribe(TOPIC)

    assert len(fake.heartbeats) == 1, "unsubscribe 不发心跳"
    assert c.subscriptions() == []


def test_every_heartbeat_is_rebuilt_from_the_live_table():
    """心跳体是「发出时刻」的订阅表快照，不是启动时的固定集合（Java subscriptions():1385）。"""
    c, fake = started()
    c.subscribe("T_A", "*")
    c.subscribe("T_B", "*")
    c.unsubscribe("T_A")

    c._send_heartbeat_to_all_broker()

    latest = subs_of(fake.heartbeats[-1])
    assert set(latest) == {"T_B"}, "撤掉的 T_A 不能再出现在报文里"
    assert set(subs_of(fake.heartbeats[0])) == {"T_A"}


def test_resubscribe_same_topic_replaces_and_heartbeats_again():
    """Java 每次 subscribe 都是 put + 发心跳（覆盖写也算一次订阅变更）。"""
    c, fake = started()
    c.subscribe(TOPIC, "A")
    c.subscribe(TOPIC, "B")

    assert len(fake.heartbeats) == 2
    assert subs_of(fake.heartbeats[-1])[TOPIC].sub_string == "B"
    cd = list(fake.heartbeats[-1].consumer_data_set)[0]
    assert len(cd.subscription_data_set) == 1, "同一 topic 覆盖，不是两条"
