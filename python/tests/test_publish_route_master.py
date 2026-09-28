# -*- coding: utf-8 -*-
"""发布路由只收「有 master」的 broker，订阅信息不受这条约束（Java MQClientInstance:294-332）。

Java 把「上游路由 → 客户端队列集」分成两条口径，同一个 topic 会得到两份不同的答案：

* **发布信息** ``topicRouteData2TopicPublishInfo:294-303``：只看**写**位与 writeQueueNums，
  **并且**要求该 broker 的 ``brokerAddrs`` 里有 ``MASTER_ID(0)``；
* **订阅信息** ``topicRouteData2TopicSubscribeInfo:318-332``：只看**读**位与 readQueueNums，
  **不**要求 master。

master 那一条不是冗余判断：从节点自己也会注册进 namesrv，而且默认配置下它的写位不会被抹掉
（``RouteInfoManager:344-346`` 只在「prime slave 且 enableActingMaster」时才清 WRITE），
所以 master 一掉线，路由里同一个 brokerName 就只剩 brokerId=1、``perm`` 依然是 6。生产者若
把它当可写队列，消息就会发到从节点上，而从节点对发送请求一律 reject
（``SendMessageProcessor:131`` ⇒ ``SYSTEM_BUSY``，还是个可重试码），白烧一轮超时。

反过来，消费侧**必须**保留这些队列（主挂后仍要从从节点拉取），所以两条口径不能合成一条 ——
本文件最后两个用例就是这条边界的守卫。
"""
from __future__ import annotations

from rocketmq.client.mq_client import MQClientInstance
from rocketmq.common.message import MessageQueue
from rocketmq.remoting.protocol.route import BrokerData, QueueData, TopicRouteData

MASTER = 0
SLAVE = 1


def _route(queue_datas, broker_datas) -> TopicRouteData:
    route = TopicRouteData()
    route.queue_datas = list(queue_datas)
    route.broker_datas = list(broker_datas)
    return route


def test_publish_queues_skip_a_broker_whose_master_is_gone():
    route = _route(
        [QueueData("broker-a", 4, 4, 6, 0)],          # master 掉线，写位仍是 6
        [BrokerData("c", "broker-a", {SLAVE: "127.0.0.1:10931"})],
    )
    assert route.get_all_message_queue("T") == []


def test_publish_queues_come_back_once_the_master_registers():
    """负控：跳的是「没有 master」，不是 broker-a 这个名字。"""
    bd = BrokerData("c", "broker-a", {SLAVE: "127.0.0.1:10931"})
    route = _route([QueueData("broker-a", 4, 4, 6, 0)], [bd])
    assert route.get_all_message_queue("T") == []
    bd.broker_addrs[MASTER] = "127.0.0.1:10911"       # master 重新注册
    assert route.get_all_message_queue("T") == [
        MessageQueue("T", "broker-a", i) for i in range(4)]


def test_publish_queues_keep_the_healthy_broker_only():
    route = _route(
        [QueueData("broker-a", 2, 2, 6, 0), QueueData("broker-b", 3, 3, 6, 0)],
        [
            BrokerData("c", "broker-a", {SLAVE: "a:10931"}),
            BrokerData("c", "broker-b", {MASTER: "b:10911", SLAVE: "b:10931"}),
        ],
    )
    assert route.get_all_message_queue("T") == [
        MessageQueue("T", "broker-b", i) for i in range(3)]


def test_subscribe_info_keeps_the_masterless_broker():
    """消费侧反过来：从节点也能服务拉取，Java 的订阅信息不筛 master。"""
    route = _route(
        [QueueData("broker-a", 2, 2, 6, 0)],
        [BrokerData("c", "broker-a", {SLAVE: "127.0.0.1:10931"})],
    )
    assert route.get_all_subscribe_message_queue("T") == [
        MessageQueue("T", "broker-a", i) for i in range(2)]


def test_subscribe_info_uses_read_perm_and_read_queue_nums():
    """只读 topic（perm=4）在 Java 里照样能被消费；条数按 readQueueNums 而不是 writeQueueNums。"""
    route = _route(
        [QueueData("broker-a", 3, 1, 4, 0)],
        [BrokerData("c", "broker-a", {MASTER: "a:10911"})],
    )
    assert route.get_all_message_queue("T") == []
    assert route.get_all_subscribe_message_queue("T") == [
        MessageQueue("T", "broker-a", i) for i in range(3)]


def test_instance_subscribe_info_is_served_from_the_route_table():
    """消费者的入口 get_topic_subscribe_info：路由已在表里就直接给队列（不再打 namesrv）。"""
    inst = MQClientInstance("route-test@unit", ["127.0.0.1:9876"])
    inst.topic_route_table["T"] = _route(
        [QueueData("broker-a", 2, 2, 6, 0)],
        [BrokerData("c", "broker-a", {SLAVE: "127.0.0.1:10931"})],
    )
    assert inst.get_topic_subscribe_info("T") == [
        MessageQueue("T", "broker-a", i) for i in range(2)]
