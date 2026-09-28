# -*- coding: utf-8 -*-
"""发布侧「只要 master」的两条口径，订阅信息不受这条约束（Java MQClientInstance:294-332）。

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

文件的第二半是**地址**侧的同一条分界线：``topicRouteData2TopicPublishInfo`` 挑出了队列，发送时
还要把 brokerName 解析成地址，Java 那边是 ``findBrokerAddressInPublish:1295-1305``
（``brokerAddrTable.get(brokerName).get(MASTER_ID)``，**只认主**、拿不到返回 null），
而不是 ``findBrokerAddressInAdmin``（主优先、没主退一台从节点）。从「队列集」到「地址」
两处都用发布口径，主从切换期间才会是本端立刻报错，而不是把请求打到从节点上再被拒。
"""
from __future__ import annotations

import pytest

from rocketmq.client.exception import MQClientException
from rocketmq.client.mq_client import MQClientInstance
from rocketmq.common.message import MessageQueue
from rocketmq.remoting.protocol.codes import ResponseCode
from rocketmq.remoting.protocol.remoting_command import RemotingCommand
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


# ---------------------------------------------- 地址侧：findBrokerAddressInPublish
def _route_of(addrs_by_broker) -> TopicRouteData:
    return _route(
        [QueueData(name, 2, 2, 6, 0) for name in addrs_by_broker],
        [BrokerData("c", name, addrs) for name, addrs in addrs_by_broker.items()],
    )


def _serving(route_holder):
    """离线实例：``_invoke_sync`` 一律回 ``route_holder[0]`` 那份路由（None = namesrv 说没有）。

    刷路由走的是**真的** ``update_topic_route_info_from_name_server`` —— 发布地址平表的
    写入点就在它里面，绕过它直接往 ``broker_addr_table`` 塞值等于这条路径没测。
    """
    inst = MQClientInstance("route-test@unit", ["127.0.0.1:9876"])
    calls = []

    def fake_invoke(addr, request, timeout_millis=None):
        calls.append(addr)
        response = RemotingCommand()
        response.code = ResponseCode.SUCCESS
        response.ext_fields = {"offset": "7"}
        route = route_holder[0]
        response.body = route.encode() if route is not None else None
        return response

    inst._invoke_sync = fake_invoke     # type: ignore[method-assign]
    return inst, calls


def test_publish_lookup_takes_only_the_master_while_the_admin_lookup_falls_back():
    """``findBrokerAddressInPublish`` 只认 brokerId=0；同一份路由上管理口径要退到从节点。"""
    holder = [_route_of({"broker-a": {SLAVE: "127.0.0.1:10931"}})]
    inst, _ = _serving(holder)
    assert inst.update_topic_route_info_from_name_server("T") is True
    assert inst.find_broker_address_in_publish("broker-a") is None
    # 负控：退让口径（心跳/拉取/位点查询用）在**同一张路由**上必须拿得到从节点地址
    assert inst.broker_addr_of("broker-a") == "127.0.0.1:10931"

    holder[0] = _route_of({"broker-a": {MASTER: "127.0.0.1:10911",
                                        SLAVE: "127.0.0.1:10931"}})
    inst.update_topic_route_info_from_name_server("T")
    assert inst.find_broker_address_in_publish("broker-a") == "127.0.0.1:10911"


def test_publish_addr_for_refreshes_the_route_then_rechecks():
    """Java ``sendKernelImpl:919-924``：发布地址查不到，按 topic 刷一次路由再查。"""
    holder = [None]      # 还没见过这个 topic
    inst, calls = _serving(holder)
    holder[0] = _route_of({"broker-a": {MASTER: "127.0.0.1:10911"}})
    assert inst.publish_addr_for("broker-a", "T") == "127.0.0.1:10911"
    assert calls == ["127.0.0.1:9876"]


def test_publish_addr_for_reports_not_exist_when_the_master_is_gone():
    """主掉线（路由里只剩 brokerId=1）：本端报「broker 不存在」，一次请求都不发。"""
    holder = [_route_of({"broker-a": {SLAVE: "127.0.0.1:10931"}})]
    inst, calls = _serving(holder)
    with pytest.raises(MQClientException) as exc:
        inst.publish_addr_for("broker-a", "T")
    assert str(exc.value) == "The broker[broker-a] not exist"
    # 本端报的错，没有 broker 侧错误码（Java 的双参构造器给 -1）
    assert exc.value.response_code is None
    # 报错前必须先刷一次路由（Java 的 tryToFindTopicPublishInfo），不能直接拿旧结论结账
    assert calls == ["127.0.0.1:9876"]
    # 负控：跳的是「没有 master」，不是 broker-a 这个名字 —— master 一注册立刻解析得出
    holder[0] = _route_of({"broker-a": {MASTER: "127.0.0.1:10911",
                                        SLAVE: "127.0.0.1:10931"}})
    assert inst.publish_addr_for("broker-a", "T") == "127.0.0.1:10911"


def test_publish_addr_for_reports_not_exist_for_an_unknown_broker():
    """路由里压根没有这个 brokerName（拼错/已下线）：同样报 not exist，不退到别的 broker。"""
    holder = [_route_of({"broker-b": {MASTER: "127.0.0.1:10912"}})]
    inst, _ = _serving(holder)
    with pytest.raises(MQClientException) as exc:
        inst.publish_addr_for("broker-a", "T")
    assert str(exc.value) == "The broker[broker-a] not exist"


def test_admin_offset_queries_are_master_only_too():
    """``MQAdminImpl:195-264`` 的四个 offset 查询同一口径：只打主，主没了就报 not exist。

    四个查询在主端点名的地址解析是同一个入口：``earliest_msg_store_time`` 没有客户端层方法，
    本端落在 ``_publish_addr_in_admin`` 上（admin.py:949 / producer.py:2045 / consumer.py:3689
    三个门面都只填地址、不各自解析），所以这里连它一起钉住。
    """
    holder = [_route_of({"broker-a": {SLAVE: "127.0.0.1:10931"}})]
    inst, calls = _serving(holder)
    mq = MessageQueue("T", "broker-a", 0)

    # 主没了：四个查询一律本端报错，一次 broker 请求都不发（旧行为是退到从节点上把查询做完）
    queries = {
        "maxOffset": lambda: inst.get_max_offset(mq),
        "minOffset": lambda: inst.get_min_offset(mq),
        "searchOffset": lambda: inst.search_offset_by_timestamp(mq, 1700000000000),
        "earliestMsgStoreTime": lambda: inst._publish_addr_in_admin(mq),
    }
    for name, call in queries.items():
        with pytest.raises(MQClientException) as exc:
            call()
        assert str(exc.value) == "The broker[broker-a] not exist", name
        # 本端报的错，没有 broker 侧错误码（Java 的双参构造器给 -1）
        assert exc.value.response_code is None, name

    # 负控：主回来之后四路都解析得出，且请求落在**主**地址上（不是路由里那台从节点）
    holder[0] = _route_of({"broker-a": {MASTER: "127.0.0.1:10911",
                                        SLAVE: "127.0.0.1:10931"}})
    assert inst.get_max_offset(mq) == 7
    assert inst.get_min_offset(mq) == 7
    assert inst.search_offset_by_timestamp(mq, 1700000000000) == 7
    assert inst._publish_addr_in_admin(mq) == "127.0.0.1:10911"
    # 从节点地址一次都没被打过（三条查询各一次 master，转接解析不打 broker）
    assert calls[-3:] == ["127.0.0.1:10911"] * 3
    assert "127.0.0.1:10931" not in calls


# -------------------------------------- 订阅口径：findBrokerAddressInSubscribe(MASTER_ID, true)
#
# Java 的四处调用点形状一致（RebalanceImpl#unlock:74 / unlockAll:104 / #lock:153 / #lockAll:195、
# PullAPIWrapper#popAsync:369-373、RemoteBrokerOffsetStore#fetchConsumeOffsetFromBroker:237-241）：
# ``findBrokerAddressInSubscribe(brokerName, MASTER_ID, true)`` —— 只认主、**不刷路由**；
# 只有 POP 与位点查询在查不到时补一次 ``updateTopicRouteInfoFromNameServer(topic)``，
# 且位点查询的**重查**放宽到从节点（``onlyThisBroker=false``），POP 的重查仍只认主。
def _invoking(route_holder):
    """离线实例：namesrv 回 ``route_holder[0]``；broker 请求按码回包并逐个记 ``(addr, code)``。"""
    from rocketmq.remoting.protocol.body import LockBatchRequestBody, LockBatchResponseBody
    from rocketmq.remoting.protocol.codes import RequestCode

    inst = MQClientInstance("route-test@unit", ["127.0.0.1:9876"])
    calls = []

    def fake_invoke(addr, request, timeout_millis=None):
        calls.append((addr, request.code))
        response = RemotingCommand()
        response.code = ResponseCode.SUCCESS
        response.ext_fields = {}
        if request.code == RequestCode.GET_ROUTEINFO_BY_TOPIC:
            route = route_holder[0]
            response.body = route.encode() if route is not None else None
        elif request.code == RequestCode.LOCK_BATCH_MQ:
            # 把请求体的 mqSet 原样回成 lockOKMQSet：锁集非空即证明「这一发真的落地了」
            rb = LockBatchResponseBody()
            rb.lock_ok_mq_set = list(LockBatchRequestBody.decode(request.body).mq_set)
            response.body = rb.encode()
        elif request.code == RequestCode.QUERY_CONSUMER_OFFSET:
            response.ext_fields = {"offset": "424242" if addr.endswith("10931") else "111"}
        elif request.code == RequestCode.POP_MESSAGE:
            response.code = ResponseCode.POLLING_TIMEOUT     # 长轮询空手而归是常态
        return response

    inst._invoke_sync = fake_invoke     # type: ignore[method-assign]
    return inst, calls


def test_orderly_locks_skip_the_broker_when_the_master_is_gone():
    """Java ``RebalanceImpl#lock:153 / lockAll:195``（解锁 ``:74/:104``）：只认主、不刷路由。

    路由里只剩从节点时 LOCK/UNLOCK 一条都不该上线：从节点上锁等于锁在它自己的锁管理器里，
    master 不知情，顺序消费的互斥保证静默失效。
    """
    from rocketmq.remoting.protocol.codes import RequestCode

    holder = [_route_of({"broker-a": {SLAVE: "127.0.0.1:10931"}})]
    inst, calls = _invoking(holder)
    inst.update_topic_route_info_from_name_server("T")
    calls.clear()
    mqs = [MessageQueue("T", "broker-a", 0), MessageQueue("T", "broker-a", 1)]

    assert inst.lock_batch_mq("GID", "cid", mqs) == []
    inst.unlock_batch_mq("GID", "cid", mqs)
    # 也不许刷路由：Java 的 lock/unlock 直接 findBrokerAddressInSubscribe(false) 收场，
    # 没有发送路径上的那一次 tryToFindTopicPublishInfo
    assert calls == []

    # 负控：主回来之后锁/解锁都落在主地址上、返回的锁集就是请求的那两个队列
    holder[0] = _route_of({"broker-a": {MASTER: "127.0.0.1:10911",
                                        SLAVE: "127.0.0.1:10931"}})
    inst.update_topic_route_info_from_name_server("T")
    calls.clear()
    locked = inst.lock_batch_mq("GID", "cid", mqs)
    inst.unlock_batch_mq("GID", "cid", mqs)
    assert [(m.topic, m.broker_name, m.queue_id) for m in locked] == [
        ("T", "broker-a", 0), ("T", "broker-a", 1)]
    assert calls == [("127.0.0.1:10911", RequestCode.LOCK_BATCH_MQ),
                     ("127.0.0.1:10911", RequestCode.UNLOCK_BATCH_MQ)]


def test_pop_message_is_master_only_and_reports_not_exist():
    """Java ``PullAPIWrapper#popAsync:369-373``：POP 只认主，查不到刷一次路由再查，仍抛 not exist。"""
    from rocketmq.client.consumer_result import PopStatus
    from rocketmq.remoting.protocol.codes import RequestCode

    holder = [_route_of({"broker-a": {SLAVE: "127.0.0.1:10931"}})]
    inst, calls = _invoking(holder)
    inst.update_topic_route_info_from_name_server("T")
    calls.clear()

    with pytest.raises(MQClientException) as exc:
        inst.pop_message("GID", "T", broker_name="broker-a")
    assert str(exc.value) == "The broker[broker-a] not exist"
    assert exc.value.response_code is None
    # 报错前必须刷过一次路由（popAsync 的 findBrokerAddressInSubscribe + 重查）
    assert calls == [("127.0.0.1:9876", RequestCode.GET_ROUTEINFO_BY_TOPIC)]

    # 负控：主注册后（这一发顺带触发一次路由刷新）请求落在主地址上，从节点一条都没有
    holder[0] = _route_of({"broker-a": {MASTER: "127.0.0.1:10911",
                                        SLAVE: "127.0.0.1:10931"}})
    calls.clear()
    result = inst.pop_message("GID", "T", broker_name="broker-a")
    assert result.status == PopStatus.POLLING_NOT_FOUND
    assert (("127.0.0.1:10911", RequestCode.POP_MESSAGE) in calls)
    assert all(addr != "127.0.0.1:10931" for addr, _ in calls)


def test_consumer_offset_falls_back_to_the_slave_after_a_refresh():
    """Java ``RemoteBrokerOffsetStore#fetchConsumeOffsetFromBroker:237-241``：主没了退到从节点。

    与管理侧 offset 查询（一律打主、主没了报错）的差别只在这最后一步：位点是 HA 复制来的
    同一份数据，Java 允许从从节点读。
    """
    from rocketmq.remoting.protocol.codes import RequestCode

    holder = [_route_of({"broker-a": {SLAVE: "127.0.0.1:10931"}})]
    inst, calls = _invoking(holder)
    inst.update_topic_route_info_from_name_server("T")
    calls.clear()
    mq = MessageQueue("T", "broker-a", 0)

    assert inst.query_consumer_offset("GID", mq) == 424242      # 从节点那份
    assert calls == [("127.0.0.1:9876", RequestCode.GET_ROUTEINFO_BY_TOPIC),
                     ("127.0.0.1:10931", RequestCode.QUERY_CONSUMER_OFFSET)]

    # 负控：主回来之后只打主（从节点计数不再增长）
    holder[0] = _route_of({"broker-a": {MASTER: "127.0.0.1:10911",
                                        SLAVE: "127.0.0.1:10931"}})
    inst.update_topic_route_info_from_name_server("T")
    calls.clear()
    assert inst.query_consumer_offset("GID", mq) == 111
    assert calls == [("127.0.0.1:10911", RequestCode.QUERY_CONSUMER_OFFSET)]

    # 路由里压根没有 broker-a（连从节点都没有）：刷一次路由后仍查不到 ⇒ 本端报 not exist
    holder2 = [_route_of({"broker-b": {MASTER: "127.0.0.1:10912"}})]
    inst2, calls2 = _invoking(holder2)
    with pytest.raises(MQClientException) as exc:
        inst2.query_consumer_offset("GID", mq)
    assert str(exc.value) == "The broker[broker-a] not exist"
    assert calls2 == [("127.0.0.1:9876", RequestCode.GET_ROUTEINFO_BY_TOPIC)]
