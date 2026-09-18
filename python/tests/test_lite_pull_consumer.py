# -*- coding: utf-8 -*-
"""DefaultLitePullConsumer 本地单测（不需要集群）。

为什么存在：LitePullConsumer 是三个语言都缺失的能力（Java DefaultLitePullConsumer）。
这里覆盖不需要 broker 的契约：生命周期守卫、assign / subscribe 两种模式的 pull 服务、
本地缓冲 drain（poll）、seek 重置位点、auto-commit 提交、rebalance 单实例全量分配。

通过把 ``_create_client`` 替换成内存 MockClient，验证后台拉取循环 + 本地缓冲 + poll 的
端到端逻辑，而不依赖真实集群。
"""
from __future__ import annotations

from types import SimpleNamespace
from typing import Dict, List, Tuple

import pytest

from rocketmq.client.consumer import DefaultLitePullConsumer
from rocketmq.client.consumer_result import PullResult, PullStatus
from rocketmq.common.message import MessageExt, MessageQueue
from rocketmq.client.exception import MQClientException
from rocketmq.remoting.protocol.heartbeat import ConsumeFromWhere


def _make_msg(topic: str, broker: str, qid: int, offset: int, body: str) -> MessageExt:
    m = MessageExt(topic, body.encode())
    m.broker_name = broker
    m.queue_id = qid
    m.queue_offset = offset
    # 既设属性也走 set_tags（写 properties），与真实 decode 出来的 MessageExt 行为一致：
    # 真实消息 tag 在 properties["TAGS"]，由 get_tags() 读取，.tags 属性为 None。
    m.tags = "TagA" if "A" in body else "TagB"
    m.set_tags(m.tags)
    return m


class MockClient:
    """内存版 MQClientInstance：按 (broker,queue_id) 持有消息列表，模拟 pull。"""

    def __init__(self, client_id: str, store: Dict[Tuple[str, int], List[str]]):
        self.client_id = client_id
        self.remoting_client = SimpleNamespace(register_rpc_hook=lambda *a, **k: None)
        self._store = store
        self.committed: Dict[Tuple[str, int], int] = {}

    def start(self) -> None:
        pass

    def shutdown(self) -> None:
        pass

    def get_route_of_all_brokers(self):
        return []  # 无真实 broker，心跳发往空集合（no-op）

    def get_topic_publish_info(self, topic):
        class _Info:
            msg_queue_list = [MessageQueue(topic, b, q) for (b, q) in self._store.keys()]
        return _Info()

    def get_consumer_id_list_by_group(self, topic, consumer_group, timeout_millis=5000):
        return [self.client_id]

    def get_max_offset(self, mq):
        return len(self._store.get((mq.broker_name, mq.queue_id), []))

    def get_min_offset(self, mq):
        return 0

    def query_consumer_offset(self, consumer_group, mq):
        return None

    def update_consumer_offset(self, consumer_group, mq, offset):
        self.committed[(mq.broker_name, mq.queue_id)] = offset

    def search_offset_by_timestamp(self, mq, timestamp):
        return 0

    def pull_message(self, consumer_group, mq, queue_offset, max_msg_nums, sys_flag,
                     commit_offset, subscription, sub_version, expression_type,
                     timeout_millis=30000, max_msg_bytes=-1, suspend_timeout_millis=15000,
                     addr=None, request_source=0) -> PullResult:
        msgs = self._store.get((mq.broker_name, mq.queue_id), [])
        window = msgs[queue_offset:queue_offset + max_msg_nums]
        found = []
        for i, body in enumerate(window):
            found.append(_make_msg(mq.topic, mq.broker_name, mq.queue_id,
                                   queue_offset + i, body))
        if found:
            return PullResult(PullStatus.FOUND, queue_offset + len(found), 0,
                              len(msgs), found)
        return PullResult(PullStatus.NO_NEW_MSG, queue_offset, 0, len(msgs), [])


class _Lite(DefaultLitePullConsumer):
    """把底层 client 替换成内存 Mock，便于无集群测试。"""

    def __init__(self, *args, **kwargs):
        store = kwargs.pop("_store", {})
        super().__init__(*args, **kwargs)
        self.client_id = "DEFAULT@testclient"
        # 测试用的 MockClient 里消息是"已经存在"的，故默认从队首消费，
        # 对齐 Java CONSUME_FROM_FIRST_OFFSET 语义（LAST 会从队尾之后开始，读不到预置消息）
        self.set_consume_from_where(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET)
        self._mock = MockClient(self.client_id, store)

    def _create_client(self):
        return self._mock


STORE = {("broker-a", 0): ["A0", "B1", "A2", "B3", "A4"]}


class TestLifecycle:
    def test_empty_consumer_group_rejected(self):
        with pytest.raises(MQClientException):
            DefaultLitePullConsumer("")
        with pytest.raises(MQClientException):
            DefaultLitePullConsumer("   ")

    def test_start_without_namesrv_raises(self):
        c = DefaultLitePullConsumer("LitePG_UT")
        with pytest.raises(MQClientException):
            c.start()

    def test_start_without_subscription_or_assign_raises(self):
        c = _Lite("LitePG_UT", _store=STORE)
        c.set_namesrv_addr("127.0.0.1:9876")
        with pytest.raises(MQClientException):
            c.start()

    def test_shutdown_before_start_is_noop(self):
        c = _Lite("LitePG_UT", _store=STORE)
        c.shutdown()  # 不应抛

    def test_config_defaults(self):
        # 用真实类（不经由 _Lite 夹具覆写 consume_from_where）检查默认配置
        c = DefaultLitePullConsumer("LitePG_UT")
        assert c.auto_commit is True
        assert c.pull_batch_size == 32
        assert c.poll_timeout_millis == 5000
        assert c.consume_from_where == "CONSUME_FROM_LAST_OFFSET"


class TestAssignMode:
    def test_assign_then_poll_drains_all(self):
        c = _Lite("LitePG_UT", _store=STORE)
        c.set_namesrv_addr("127.0.0.1:9876")
        c.set_pull_batch_size(2)
        c.assign([MessageQueue("T", "broker-a", 0)])
        c.start()
        try:
            collected = []
            # 5 条消息，批 2 → 2+2+1 三次 poll
            for _ in range(3):
                collected.extend(c.poll(timeout=2000))
            assert [m.body.decode() for m in collected] == ["A0", "B1", "A2", "B3", "A4"]
            # 缓冲已空，下一次 poll 超时返回空列表（不永久阻塞）
            assert c.poll(timeout=200) == []
        finally:
            c.shutdown()

    def test_assign_mode_sets_assignment(self):
        c = _Lite("LitePG_UT", _store=STORE)
        c.set_namesrv_addr("127.0.0.1:9876")
        mq = MessageQueue("T", "broker-a", 0)
        c.assign([mq])
        assert c.assignment() == [mq]

    def test_seek_resets_next_offset_and_drops_buffered(self):
        c = _Lite("LitePG_UT", _store=STORE)
        c.set_namesrv_addr("127.0.0.1:9876")
        c.set_pull_batch_size(2)
        c.assign([MessageQueue("T", "broker-a", 0)])
        c.start()
        try:
            first = c.poll(timeout=2000)
            assert [m.body.decode() for m in first] == ["A0", "B1"]
            # 回到队首重新消费
            c.seek(MessageQueue("T", "broker-a", 0), 0)
            again = c.poll(timeout=2000)
            assert [m.body.decode() for m in again][:2] == ["A0", "B1"]
        finally:
            c.shutdown()

    def test_commit_records_offset(self):
        c = _Lite("LitePG_UT", _store=STORE)
        c.set_namesrv_addr("127.0.0.1:9876")
        c.set_pull_batch_size(2)
        c.set_auto_commit(False)  # 关掉自动提交，验证显式 commit
        c.assign([MessageQueue("T", "broker-a", 0)])
        c.start()
        try:
            c.poll(timeout=2000)  # 拉到 A0,B1 → next_offset=2
            c.commit()
            assert c._mock.committed[("broker-a", 0)] == 2
        finally:
            c.shutdown()


class TestSubscribeMode:
    def test_single_consumer_gets_all_queues(self):
        # subscribe 模式：单实例组里只有自己 → rebalance 把全部队列分给它
        c = _Lite("LitePG_UT", _store=STORE)
        c.set_namesrv_addr("127.0.0.1:9876")
        c.set_pull_batch_size(2)
        c.subscribe("T", "*")
        c.start()
        try:
            collected = []
            for _ in range(3):
                collected.extend(c.poll(timeout=2000))
            assert [m.body.decode() for m in collected] == ["A0", "B1", "A2", "B3", "A4"]
            # subscribe 模式自动 rebalance，assignment 非空
            assert len(c.assignment()) == 1
        finally:
            c.shutdown()

    def test_subscribe_tag_filter(self):
        c = _Lite("LitePG_UT", _store=STORE)
        c.set_namesrv_addr("127.0.0.1:9876")
        c.set_pull_batch_size(5)
        c.subscribe("T", "TagA")  # 只消费 TagA 的消息
        c.start()
        try:
            collected = []
            for _ in range(3):
                collected.extend(c.poll(timeout=2000))
            # A0 A2 A4 命中 TagA
            assert [m.body.decode() for m in collected] == ["A0", "A2", "A4"]
        finally:
            c.shutdown()


class TestPauseResume:
    def test_pause_stops_pulling(self):
        c = _Lite("LitePG_UT", _store=STORE)
        c.set_namesrv_addr("127.0.0.1:9876")
        c.set_pull_batch_size(2)
        mq = MessageQueue("T", "broker-a", 0)
        c.assign([mq])
        c.pause([mq])
        c.start()
        try:
            # 暂停期间后台不拉取，poll 超时返回空
            assert c.poll(timeout=300) == []
            c.resume([mq])
            got = c.poll(timeout=2000)
            assert [m.body.decode() for m in got][:2] == ["A0", "B1"]
        finally:
            c.shutdown()
