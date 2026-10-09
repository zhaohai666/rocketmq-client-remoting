# -*- coding: utf-8 -*-
"""主动拉取消费者（DefaultMQPullConsumer）本地单测（不需要集群）。

为什么要有这个文件：`DefaultMQPullConsumer` 此前**既没有单测也没有真机脚本**
（grep 全仓 0 命中）——是纯"纸面能力"。这里覆盖不需要 broker 的那部分契约：
生命周期守卫、命名空间包装、以及**拉取 sysFlag 与 Java 对齐**（真机踩过超时的坑）。
"""
from __future__ import annotations

import inspect
from types import SimpleNamespace

import pytest

from client.consumer import DefaultMQPullConsumer
from client.exception import MQClientException
from common.message import MessageQueue
from common.sysflag import PullSysFlag
from remoting.protocol.heartbeat import (ConsumeFromWhere, ConsumeType,
                                                  MessageModel)


class TestLifecycle:
    def test_empty_consumer_group_rejected(self):
        with pytest.raises(MQClientException):
            DefaultMQPullConsumer("")
        with pytest.raises(MQClientException):
            DefaultMQPullConsumer("   ")

    def test_start_without_namesrv_raises(self):
        c = DefaultMQPullConsumer("PG_UnitTest")
        with pytest.raises(MQClientException):
            c.start()

    def test_operations_before_start_raise(self):
        c = DefaultMQPullConsumer("PG_UnitTest")
        c.set_namesrv_addr("127.0.0.1:9876")
        mq = MessageQueue("TopicUnitTest", "broker-a", 0)
        # 没 start() 时任何需要 client 的操作都要抛，而不是 NPE / AttributeError
        with pytest.raises(MQClientException):
            c.fetch_subscribe_message_queues("TopicUnitTest")
        with pytest.raises(MQClientException):
            c.pull(mq, "*", 0, 32, 1000)
        with pytest.raises(MQClientException):
            c.update_consume_offset(mq, 0)

    def test_shutdown_before_start_is_noop(self):
        c = DefaultMQPullConsumer("PG_UnitTest")
        c.shutdown()  # 不应抛

    def test_set_namesrv_addr_splits_semicolon(self):
        c = DefaultMQPullConsumer("PG_UnitTest")
        c.set_namesrv_addr(" 127.0.0.1:9876 ; 127.0.0.1:9877 ;; ")
        assert c.name_server_addrs == ["127.0.0.1:9876", "127.0.0.1:9877"]

    def test_register_message_queue_listener_records_topic(self):
        c = DefaultMQPullConsumer("PG_UnitTest")

        class _L:
            def message_queue_changed(self, topic, mq_all, mq_divided):
                pass

        listener = _L()
        c.set_message_queue_listener(listener)
        assert c.message_queue_listener is listener


class TestPullSysFlagParity:
    """拉取标志位必须与 Java DefaultMQPullConsumerImpl.pullSyncImpl(:248) 一致。

    Java: sysFlag = PullSysFlag.buildSysFlag(false /*commitOffset*/, block /*suspend*/,
                                             true /*subscription*/, false /*classFilter*/)
    即 pull() = 短轮询（suspend=False）、pull_block_if_not_found() = 长轮询（suspend=True），
    且**两者都不带 commitOffset 位**（位点由调用方 update_consume_offset 自己提交）。

    真机教训：曾把 pull() 的 suspend 写成 True → broker 在队尾挂起到
    brokerSuspendMaxTimeMillis(20s)，客户端 5s 超时 → RemotingTimeoutException。
    """

    def test_pull_is_short_poll_without_commit_offset(self):
        src = inspect.getsource(DefaultMQPullConsumer.pull)
        assert "suspend=False" in src, "pull() 必须是短轮询（suspend=False）"
        assert "commit_offset=False" in src, "pull() 不自带 commitOffset 位"

    def test_pull_block_if_not_found_is_long_poll(self):
        src = inspect.getsource(DefaultMQPullConsumer.pull_block_if_not_found)
        assert "suspend=True" in src, "pull_block_if_not_found() 才是长轮询"

    def test_flag_bits(self):
        short = PullSysFlag.build_sys_flag(commit_offset=False, suspend=False,
                                           subscription=True, class_filter=False)
        assert PullSysFlag.has_commit_offset_flag(short) is False
        assert PullSysFlag.has_suspend_flag(short) is False

        long_ = PullSysFlag.build_sys_flag(commit_offset=False, suspend=True,
                                           subscription=True, class_filter=False)
        assert PullSysFlag.has_commit_offset_flag(long_) is False
        assert PullSysFlag.has_suspend_flag(long_) is True

    def test_message_model_defaults_to_clustering(self):
        c = DefaultMQPullConsumer("PG_UnitTest")
        assert c.message_model == MessageModel.CLUSTERING

    def test_namespace_field_present(self):
        c = DefaultMQPullConsumer("PG_UnitTest")
        assert c.namespace == ""
        c.namespace = "NS1"
        assert c.namespace == "NS1"


class _RecordingClient:
    """只覆盖心跳路径要用的方法的替身 client（记录每一次收发）。"""

    def __init__(self, addrs=("127.0.0.1:10911", "127.0.0.1:10912")):
        self._addrs = list(addrs)
        self.heartbeats = []          # [(addr, HeartbeatData)]
        self.topics_in_use = []
        self.routed = []
        self.unregistered = []        # [(client_id, producer_group, consumer_group)]
        self.shutdown_called = 0
        self.start_called = 0
        self.remoting_client = SimpleNamespace(register_rpc_hook=lambda *a, **k: None)

    def start(self):
        self.start_called += 1

    def get_all_broker_addrs(self):
        return list(self._addrs)

    def send_heartbeat(self, addr, hb, timeout_millis):
        self.heartbeats.append((addr, hb))

    def register_topic_in_use(self, topic):
        self.topics_in_use.append(topic)

    def get_topic_publish_info(self, topic):
        self.routed.append(topic)
        return SimpleNamespace(msg_queue_list=[])

    def unregister_client_all_brokers(self, client_id, producer_group, consumer_group,
                                      timeout_millis=3000):
        self.unregistered.append((client_id, producer_group, consumer_group))

    def shutdown(self):
        self.shutdown_called += 1


def _started_pull(topics=("TopicHbA", "TopicHbB")):
    """造一个「已启动」的拉模式消费者，client 是替身，心跳报文可截获。"""
    c = DefaultMQPullConsumer("PG_HbUnit")
    c.client_id = "cid@fixed"
    c.register_topics.update(topics)
    fake = _RecordingClient()
    c._mq_client = fake
    c._started = True
    return c, fake


class TestConsumerHeartbeat:
    """拉模式消费者也要向 broker 注册（Java DefaultMQPullConsumerImpl.start:746）。

    为什么必须离线锁死：Java 把拉模式消费者登记进 consumerTable，由实例的
    sendHeartbeatToAllBrokerWithLock 代发一份 `consumeType=CONSUME_ACTIVELY` 的
    ConsumerData。少了它，broker 的 ConsumerManager.consumerTable 里根本没有这个组：
    `mqadmin consumerConnection` 看不到、GET_CONSUMER_LIST_BY_GROUP(38) 查不到，
    而 `rejectPullConsumerEnabled=true` 的 broker 会直接拒绝它的拉取
    （`PullMessageProcessor:493-505` "the pull consumer is rejected by server"）。
    """

    def test_heartbeat_is_actively_typed_and_carries_register_topics(self):
        c, fake = _started_pull()
        assert c._send_heartbeat_to_all_broker() == 2, "主 + 从各一发"
        assert [a for a, _ in fake.heartbeats] == ["127.0.0.1:10911", "127.0.0.1:10912"]
        hb = fake.heartbeats[0][1]
        assert hb.client_id == "cid@fixed"
        cd = list(hb.consumer_data_set)[0]
        assert cd.group_name == "PG_HbUnit"
        # Java DefaultMQPullConsumerImpl.consumeType():348 恒为 CONSUME_ACTIVELY
        # （与推送消费者的 PASSIVELY 是两个口径），consumeFromWhere():353 恒为 LAST。
        assert cd.consume_type == ConsumeType.CONSUME_ACTIVELY
        assert cd.consume_from_where == ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET
        subs = {s.topic: s for s in cd.subscription_data_set}
        assert sorted(subs) == ["TopicHbA", "TopicHbB"]
        # Java subscriptions():375-382 里 `ms.setSubVersion(0L)`：拉模式没有订阅版本
        # 语义，带当前时间戳会让 broker 每次都判成"订阅变了"。
        for sub in subs.values():
            assert sub.sub_string == "*"
            assert sub.sub_version == 0
        assert c.heartbeat_count() == 1

    def test_heartbeat_still_announces_the_group_without_register_topics(self):
        # registerTopics 为空时订阅集是空的，但 ConsumerData 本身照发 —— 否则
        # 调用方"只拉一两个 topic 不预先登记"时 broker 侧永远看不见这个组。
        c, fake = _started_pull(topics=())
        assert c._send_heartbeat_to_all_broker() == 2
        cd = list(fake.heartbeats[0][1].consumer_data_set)[0]
        assert cd.group_name == "PG_HbUnit"
        assert list(cd.subscription_data_set) == []

    def test_start_refreshes_routes_then_sends_the_first_heartbeat(self, monkeypatch):
        import client.consumer as consumer_mod

        holder = {}

        def _factory(*args, **kwargs):
            holder["client"] = _RecordingClient()
            return holder["client"]

        monkeypatch.setattr(consumer_mod, "MQClientInstance", _factory)
        c = DefaultMQPullConsumer("PG_HbStart")
        c.set_namesrv_addr("127.0.0.1:9876")
        c.register_topics.add("TopicHbStart")
        c.heartbeat_interval_millis = 3600_000  # 只测启动那一轮，别让循环插进来
        c.start()
        try:
            fake = holder["client"]
            # 路由必须先刷：心跳目标只来自路由表（Java 靠实例级路由任务做到同一件事）
            assert fake.topics_in_use == ["TopicHbStart"]
            assert fake.routed == ["TopicHbStart"]
            assert len(fake.heartbeats) == 2
            assert c.heartbeat_count() == 1
        finally:
            c.shutdown()

    def test_shutdown_stops_the_loop_and_unregisters_the_group(self, monkeypatch):
        import client.consumer as consumer_mod

        holder = {}

        def _factory(*args, **kwargs):
            holder["client"] = _RecordingClient()
            return holder["client"]

        monkeypatch.setattr(consumer_mod, "MQClientInstance", _factory)
        c = DefaultMQPullConsumer("PG_HbStop")
        c.set_namesrv_addr("127.0.0.1:9876")
        c.register_topics.add("TopicHbStop")
        c.heartbeat_interval_millis = 3600_000
        c.start()
        c.shutdown()
        fake = holder["client"]
        # Java DefaultMQPullConsumerImpl.shutdown:689-692：unregisterConsumer 后
        # 才 mQClientFactory.shutdown()。35 只带本组、不带生产者组。
        assert fake.unregistered == [(c.client_id, "", "PG_HbStop")]
        assert fake.shutdown_called == 1
        assert c._heartbeat_stop.is_set()
        # 幂等：再 shutdown 一次不再发报文
        c.shutdown()
        assert fake.shutdown_called == 1


class _BalanceClient:
    """替身 client：只提供 fetchMessageQueuesInBalance 需要的两个查询口。

    subscribe / cidList 都可以设成 None 来模拟「路由拿不到」「broker 没答」，
    这时方法必须退回本地记账（保留现有分配），绝不能独占全部队列。
    """

    def __init__(self, subscribe=None, cid_list=None, raise_on_route=False):
        self._subscribe = subscribe or []
        self._cid_list = cid_list
        self.raise_on_route = raise_on_route
        self.remoting_client = SimpleNamespace(register_rpc_hook=lambda *a, **k: None)

    def get_topic_subscribe_info(self, topic):
        if self.raise_on_route:
            raise MQClientException("route failed")
        # 真 client 按 topic 取订阅信息（Java topicSubscribeInfoTable[topic]），替身同口径。
        return [q for q in self._subscribe if q.topic == topic]

    def get_consumer_id_list_by_group(self, topic, consumer_group, timeout_millis=5000):
        return None if self._cid_list is None else list(self._cid_list)


def _balance_consumer(mq_all, cid_all, model=MessageModel.CLUSTERING, pulled=()):
    c = DefaultMQPullConsumer("PG_BalUnit")
    c.client_id = "cid-A"
    c.message_model = model
    c._mq_client = _BalanceClient(subscribe=mq_all, cid_list=cid_all)
    c._started = True
    for mq in pulled:
        c._pull_from_which_node[mq] = 0
    return c


class TestFetchMessageQueuesInBalance:
    """MQPullConsumer#fetchMessageQueuesInBalance(:187) 的本端口移植。

    Java（DefaultMQPullConsumerImpl:120-135）读的是后台 rebalance 填出来的
    processQueueTable；本端口拉模式没有那条后台线程，所以按 RebalanceImpl.rebalanceByTopic
    的同一条公式当场算：mqAll=订阅信息(读位)、cidAll=GET_CONSUMER_LIST_BY_GROUP、
    分配策略取本实例那一份。取不到就保留本地记账（绝不回退成独占全部队列）。
    """

    _TOPIC = "TopicBalUnit"

    def _queues(self, n):
        return [MessageQueue(self._TOPIC, "broker-a", i) for i in range(n)]

    def test_not_started_raises(self):
        c = DefaultMQPullConsumer("PG_BalUnit")
        with pytest.raises(MQClientException):
            c.fetch_message_queues_in_balance(self._TOPIC)

    def test_null_topic_raises_illegal_argument(self):
        # Java :122-124 是 IllegalArgumentException，本端口对应 ValueError。
        c = _balance_consumer(self._queues(1), ["cid-A"])
        with pytest.raises(ValueError):
            c.fetch_message_queues_in_balance(None)

    def test_returns_only_this_instance_share(self):
        got = _balance_consumer(self._queues(4), ["cid-A", "cid-B"]) \
            .fetch_message_queues_in_balance(self._TOPIC)
        assert [q.queue_id for q in got] == [0, 1], "averagely 策略下 A 只拿前一半"

    def test_single_instance_takes_all(self):
        got = _balance_consumer(self._queues(3), ["cid-A"]) \
            .fetch_message_queues_in_balance(self._TOPIC)
        assert [q.queue_id for q in got] == [0, 1, 2]

    def test_result_is_sorted_and_deterministic(self):
        mq_all = [MessageQueue(self._TOPIC, "broker-b", 0),
                  MessageQueue(self._TOPIC, "broker-a", 1),
                  MessageQueue(self._TOPIC, "broker-a", 0)]
        got = _balance_consumer(mq_all, ["cid-A"]).fetch_message_queues_in_balance(self._TOPIC)
        assert [(q.broker_name, q.queue_id) for q in got] == [
            ("broker-a", 0), ("broker-a", 1), ("broker-b", 0)]

    def test_other_topics_are_not_leaked(self):
        # 分配按 topic 隔离：别的 topic 的队列不能混进来（Java 按 topic 过滤表键）。
        other = [MessageQueue("TopicOther", "broker-a", i) for i in range(2)]
        got = _balance_consumer(other + self._queues(2), ["cid-A"]) \
            .fetch_message_queues_in_balance(self._TOPIC)
        assert all(q.topic == self._TOPIC for q in got)
        assert [q.queue_id for q in got] == [0, 1]

    def test_broadcasting_takes_every_queue(self):
        # Java rebalanceByTopic 对 BROADCASTING 不查消费者列表、全量分配。
        got = _balance_consumer(self._queues(3), ["cid-A", "cid-B"],
                                model=MessageModel.BROADCASTING) \
            .fetch_message_queues_in_balance(self._TOPIC)
        assert [q.queue_id for q in got] == [0, 1, 2]

    def test_no_consumer_list_keeps_current_assignment(self):
        # broker 没答消费者列表 → 保留本实例正在拉的队列，不独占全部（同组会重复消费）。
        pulled = [MessageQueue(self._TOPIC, "broker-a", 2)]
        got = _balance_consumer(self._queues(4), None, pulled=pulled) \
            .fetch_message_queues_in_balance(self._TOPIC)
        assert [q.queue_id for q in got] == [2]

    def test_route_failure_falls_back_to_pulled_queues(self):
        c = _balance_consumer([], None, pulled=[MessageQueue(self._TOPIC, "broker-a", 5)])
        c._mq_client.raise_on_route = True
        got = c.fetch_message_queues_in_balance(self._TOPIC)
        assert [q.queue_id for q in got] == [5]

    def test_fresh_consumer_without_broker_returns_empty(self):
        # 没拉过、又查不到 → 空表。Java 在 rebalance 跑之前同样是空集，不是"全部"。
        assert _balance_consumer([], None).fetch_message_queues_in_balance(self._TOPIC) == []
