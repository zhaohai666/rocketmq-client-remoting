# -*- coding: utf-8 -*-
"""DefaultLitePullConsumer 本地单测（不需要集群）。

为什么存在：LitePullConsumer 是三个语言都缺失的能力（Java DefaultLitePullConsumer）。
这里覆盖不需要 broker 的契约：生命周期守卫、assign / subscribe 两种模式的 pull 服务、
本地缓冲 drain（poll）、seek 重置位点、auto-commit 提交、rebalance 单实例全量分配。

通过把 ``_create_client`` 替换成内存 MockClient，验证后台拉取循环 + 本地缓冲 + poll 的
端到端逻辑，而不依赖真实集群。
"""
from __future__ import annotations

import time
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
        # 提交流水：位点契约要看的不是"最后等于几"，而是"发没发、按什么顺序发、
        # 发了哪几条队列"，所以逐笔记下来。
        self.updates: List[Tuple[str, int, int]] = []
        self.queries: List[Tuple[str, int]] = []
        # start() 必须先把 topic 登记为「在用」并拉一次路由，再发首轮心跳（心跳目标
        # 只来自路由表）。顺序用事件流水记录，便于断言而不是只断言发生过。
        self.events: List[str] = []

    def start(self) -> None:
        pass

    def shutdown(self) -> None:
        pass

    def get_route_of_all_brokers(self):
        self.events.append("brokers")
        return []  # 无真实 broker，心跳发往空集合（no-op）

    def register_topic_in_use(self, topic):
        self.events.append("register:%s" % topic)

    def get_topic_publish_info(self, topic):
        self.events.append("route:%s" % topic)

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
        # 像 broker 一样记账：位点表里有什么就回什么，没有就是 QUERY_NOT_FOUND（None）。
        # 这样 committed() 的"先看内存表"才测得出来——全部回 None 的话，
        # 内存命中与回落 broker 两种实现给出同一个答案。
        self.queries.append((mq.broker_name, mq.queue_id))
        return self.committed.get((mq.broker_name, mq.queue_id))

    def update_consumer_offset(self, consumer_group, mq, offset):
        self.committed[(mq.broker_name, mq.queue_id)] = offset
        self.updates.append((mq.broker_name, mq.queue_id, offset))

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
        # Java DefaultLitePullConsumer.java:168：默认 now-30min 的 14 位 yyyyMMddHHmmss
        assert len(c.consume_timestamp) == 14
        assert c.consume_timestamp.isdigit()

    def test_consume_timestamp_must_be_wall_clock(self):
        c = _Lite("LitePG_UT", _store=STORE)
        c.set_namesrv_addr("127.0.0.1:9876")
        c.assign([MessageQueue("T", "broker-a", 0)])
        # 纯数字的 epoch 毫秒必须被拒（旧实现按 epoch 解释，静默算出错位起点）
        c.set_consume_timestamp("1700000000000")
        with pytest.raises(MQClientException, match="consumeTimestamp is invalid"):
            c.start()
        # 合法值不能被这条启动守卫误杀
        c.set_consume_timestamp("20230101000000")
        c.start()
        try:
            assert c._started is True
        finally:
            c.shutdown()

    def test_start_pulls_route_before_the_first_heartbeat(self):
        # 心跳只发给「路由表里已知的 broker」，所以刷路由必须排在那次同步心跳之前；
        # 反了的话首轮心跳没有目标，broker 侧看不到本实例，多实例 rebalance 会
        # 各自独占全部队列（真实集群上表现为重复消费）。
        c = _Lite("LitePG_UT", _store=STORE)
        c.set_namesrv_addr("127.0.0.1:9876")
        c.subscribe("T", "*")
        c.start()
        try:
            events = c._mock.events
            assert "register:T" in events and "route:T" in events, events
            assert events.index("route:T") < events.index("brokers"), events
        finally:
            c.shutdown()

        assigned = _Lite("LitePG_UT", _store=STORE)
        assigned.set_namesrv_addr("127.0.0.1:9876")
        assigned.assign([MessageQueue("T", "broker-a", 0)])
        assigned.start()
        try:
            # assign 模式下 subscription 是空的，路由得从已指派队列反推出来
            assert "register:T" in assigned._mock.events, assigned._mock.events
        finally:
            assigned.shutdown()


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


class TestCommitOffsetTable:
    """commit() 的三个重载与 Java 的三张位点表（拉取游标 / 已消费游标 / 提交落点）。

    对位 Java ``DefaultLitePullConsumerImpl``：``commit()`` → ``commitAll()``（取
    ``assignedMessageQueue.getConsumerOffset``）、``commit(Map, persist)``（调用方指定位点，
    只写 offsetStore）、``commit(Set, persist)``（只提交给定队列）。
    本端口没有 Java 那份 MQClientInstance 5s 的 persistConsumerOffset 定时器，
    所以 persist=True 就地发给 broker——这条偏离在 commit() 的文档里。
    """

    def _two_queue_store(self):
        return {("broker-a", 0): ["A0", "B1", "A2"], ("broker-a", 1): ["C0", "D1"]}

    def _consumer(self, store=None):
        c = _Lite("LitePG_UT", _store=store or self._two_queue_store())
        c.set_namesrv_addr("127.0.0.1:9876")
        c.set_auto_commit(False)          # 提交时机由用例自己控制
        return c

    def test_commit_map_persists_only_the_given_queues(self):
        c = self._consumer()
        q0 = MessageQueue("T", "broker-a", 0)
        q1 = MessageQueue("T", "broker-a", 1)
        c.assign([q0, q1])
        c.start()
        try:
            c.commit({q0: 2, q1: 7}, persist=False)
            # persist=False：内存表两处都有，broker 一条都没收到
            assert c._offset_table == {q0: 2, q1: 7}
            assert c._mock.updates == []
            c.commit({q0: 9}, persist=True)
            # 只发给定那条队列，并且 Java 的 persistAll 会把其余条目从表里清掉
            assert c._mock.updates == [("broker-a", 0, 9)]
            assert list(c._offset_table) == [q0]
        finally:
            c.shutdown()

    def test_committed_reads_memory_first_then_broker(self):
        c = self._consumer()
        q0 = MessageQueue("T", "broker-a", 0)
        c.assign([q0])
        c.start()
        try:
            # 表里没有 → 回落 broker 并回填（Java readOffset 的 MEMORY_FIRST_THEN_STORE）
            c._mock.committed[("broker-a", 0)] = 4
            assert c.committed(q0) == 4
            queries = len(c._mock.queries)
            assert c.committed(q0) == 4
            assert len(c._mock.queries) == queries, "回填之后不该再问 broker"
            # persist=False 的提交还没落 broker，committed() 也必须看得见
            c.commit({q0: 6}, persist=False)
            assert c.committed(q0) == 6
            assert c._mock.committed[("broker-a", 0)] == 4
        finally:
            c.shutdown()

    def test_commit_map_does_not_move_the_pull_cursor(self):
        c = self._consumer()
        c.set_pull_batch_size(2)
        q0 = MessageQueue("T", "broker-a", 0)
        c.assign([q0])
        c.start()
        try:
            first = c.poll(timeout=2000)
            assert [m.body.decode() for m in first] == ["A0", "B1"]
            assert c._next_offset[q0] == 2
            c.commit({q0: 0})
            # 提交位点是"别人看的账本"，不是"我下次从哪拉"：游标不动，缓冲照旧
            assert c._next_offset[q0] == 2
            again = c.poll(timeout=2000)
            assert [m.body.decode() for m in again] == ["A2"]
        finally:
            c.shutdown()

    def test_commit_uses_the_polled_cursor_not_the_pull_cursor(self):
        # 缓冲里还压着没交付的消息时，提交只能停在 poll 交出去的那一格
        c = self._consumer({("broker-a", 0): ["A0", "B1", "A2", "B3", "A4"]})
        c.set_pull_batch_size(5)
        q0 = MessageQueue("T", "broker-a", 0)
        c.assign([q0])
        c.start()
        try:
            got = c.poll(timeout=2000)
            assert [m.body.decode() for m in got] == ["A0", "B1", "A2", "B3", "A4"]
            assert c._next_offset[q0] == 5
            assert c._consume_offset[q0] == 5
            c.commit()
            assert c._mock.updates == [("broker-a", 0, 5)]
        finally:
            c.shutdown()

    def test_auto_commit_never_commits_what_poll_never_handed_out(self):
        # 后台提交线程与 poll 抢的是同一份"已消费游标"，所以哪一轮真正触发提交
        # 取决于线程时序，"停在第几格"却是不变量：**每次发出的位点都不许跑过 poll
        # 交给调用方的那一格**。把拉取游标当提交源（Java 之外四语言的共同写法）会
        # 在这里红：缓冲一满就提交到队列末尾，调用方崩了那段消息永久丢失。
        c = self._consumer({("broker-a", 0): ["A0", "B1", "A2", "B3", "A4"]})
        c.set_pull_batch_size(2)
        c.set_auto_commit(True)
        c.set_auto_commit_interval_millis(0)
        q0 = MessageQueue("T", "broker-a", 0)
        c.assign([q0])
        c.start()
        try:
            # 先确认后台确实拉过（否则 updates == [] 只是"还没来得及"的假阴性）
            for _ in range(200):
                if c._next_offset.get(q0, -1) >= 2:
                    break
                time.sleep(0.01)
            assert c._next_offset.get(q0, -1) >= 2, "后台没拉过，这条用例失去意义"
            assert c._mock.updates == [], "一条都没交付 ⇒ 没有位点可提交"

            handed = 0
            for _ in range(5):
                got = c.poll(timeout=2000)
                if got:
                    handed = max(handed, max(m.queue_offset + 1 for m in got))
                for _, _, off in c._mock.updates:
                    assert off <= handed, \
                        "提交了 poll 还没交出去的消息: %d > %d" % (off, handed)
            assert handed == 5, handed
            # 手动把截止时刻拨到过去，逼那道闸门走一次：落点就是已交付的那一格
            c._next_auto_commit_deadline = -1
            c._maybe_auto_commit()
            assert c._mock.updates[-1] == ("broker-a", 0, 5)
            assert c._next_auto_commit_deadline > 0, "提交完要把截止时刻推到下一个周期"
        finally:
            c.shutdown()

    def test_commit_set_form_only_touches_given_queues(self):
        c = self._consumer()
        c.set_pull_batch_size(2)
        q0 = MessageQueue("T", "broker-a", 0)
        q1 = MessageQueue("T", "broker-a", 1)
        c.assign([q0, q1])
        c.start()
        try:
            drained = []
            for _ in range(4):
                drained.extend(c.poll(timeout=1000))
            assert len(drained) == 5, drained
            c.commit([q1], persist=True)
            assert [(b, q, o) for b, q, o in c._mock.updates] == [("broker-a", 1, 2)]
        finally:
            c.shutdown()

    def test_commit_skips_minus_one_and_unassigned_queues(self):
        c = self._consumer()
        q0 = MessageQueue("T", "broker-a", 0)
        stranger = MessageQueue("T", "broker-a", 7)
        c.assign([q0])
        c.start()
        try:
            c.commit({q0: -1, stranger: 3})
            # -1：Java 原话 consumerOffset is -1 in messageQueue [...]，跳过；
            # 不是本实例持有的队列：Java 的 processQueue 守卫，静默跳过
            assert c._offset_table == {}
            assert c._mock.updates == []
        finally:
            c.shutdown()

    def test_empty_map_and_empty_set_are_ignored(self):
        c = self._consumer()
        q0 = MessageQueue("T", "broker-a", 0)
        c.assign([q0])
        c.start()
        try:
            c.commit({q0: 2}, persist=False)
            c.commit({})
            c.commit([])
            # 空集合直接 return：既不发消息，也不触发 persistAll 的清表
            assert c._offset_table == {q0: 2}
            assert c._mock.updates == []
        finally:
            c.shutdown()

    def test_seek_moves_the_consume_cursor_too(self):
        # Java nextPullOffset：吃掉 seekOffset 时连同 consumeOffset 一起改写，
        # 否则重放的那一段会被上一格的已提交位点盖过去
        c = self._consumer()
        c.set_pull_batch_size(2)
        q0 = MessageQueue("T", "broker-a", 0)
        c.assign([q0])
        c.start()
        try:
            c.poll(timeout=2000)
            assert c._consume_offset[q0] == 2
            c.seek(q0, 0)
            assert c._consume_offset[q0] == 0
            c.commit()
            assert c._mock.updates == [("broker-a", 0, 0)]
        finally:
            c.shutdown()

    def test_revoked_queue_is_persisted_then_dropped(self):
        # Java RebalanceLitePullImpl#removeUnnecessaryMessageQueue：先 persist(mq) 再 removeOffset(mq)。
        # 这条清理挂在 **subscribe 模式**的 rebalance 上：队列是 broker 撤的，客户端必须
        # 把最后那次提交补发出去，否则新持有者从上一格起重投一遍。
        store = {("broker-a", 0): ["A0", "B1", "A2"], ("broker-a", 1): ["C0", "D1"]}
        c = _Lite("LitePG_UT", _store=store)
        c.set_namesrv_addr("127.0.0.1:9876")
        c.set_auto_commit(False)
        c.subscribe("T", "*")
        c.start()
        try:
            q0 = MessageQueue("T", "broker-a", 0)
            q1 = MessageQueue("T", "broker-a", 1)
            assert {q1} <= set(c.assignment()), c.assignment()
            c.commit({q1: 1})
            assert c._mock.updates == [("broker-a", 1, 1)]
            c._mock.updates.clear()
            # broker 侧撤走 q1（本机模拟 rebalance 少了一条队列）
            del store[("broker-a", 1)]
            c._rebalance()
            assert c._mock.updates == [("broker-a", 1, 1)], \
                "撤队列之前要把最后那次提交补发出去"
            assert q1 not in c._offset_table and q1 not in c._consume_offset
            assert q1 not in c._next_offset and q1 not in set(c.assignment())
            # 留下的那条队列不能顺手被清掉
            assert q0 in c._next_offset or q0 in c._offset_table
        finally:
            c.shutdown()

    def test_assign_mode_revoke_does_not_persist(self):
        # Java updateAssignedMessageQueue（assign 模式）：只把 MessageQueueState 丢掉，
        # 既不发 persist 也不碰 offsetStore——位点是调用方自己管的。
        c = self._consumer()
        q0 = MessageQueue("T", "broker-a", 0)
        q1 = MessageQueue("T", "broker-a", 1)
        c.assign([q0, q1])
        c.start()
        try:
            c.commit({q1: 1}, persist=False)
            assert c._offset_table[q1] == 1
            c._mock.updates.clear()
            c.assign([q0])
            assert c._mock.updates == [], "assign 模式不替调用方补发提交"
            # 两条游标都跟着状态一起消失，offsetTable 的条目留给 persistAll 去清
            assert q1 not in c._next_offset and q1 not in c._consume_offset
            assert c._offset_table[q1] == 1
        finally:
            c.shutdown()

    def test_three_cursors_stay_three_separate_numbers(self):
        # 真机 S8 的离线版：一条队列灌 1200 条，poll 单次上限 1024 ⇒
        # 「拉了多少」「交了多少」「提交到哪」三个数字必须各不相同。
        # 把拉取游标当提交源（#68 修掉的那条）在这里就红：一次提交把 1200 全推给 broker，
        # 调用方崩在那 176 条之前 ⇒ 静默丢消息。
        c = self._consumer({("broker-a", 0): ["A%d" % i for i in range(1200)]})
        c.set_pull_batch_size(1200)
        q0 = MessageQueue("T", "broker-a", 0)
        stranger = MessageQueue("T", "broker-a", 9)
        # 三个观测点：队列还没进表时一律 -1，不是 0（0 是"队首之前没有消息"的有效位点）
        assert (c.pull_cursor_of(q0), c.consume_cursor_of(q0), c.pending_commit_of(q0)) \
            == (-1, -1, -1)
        c.assign([q0])
        c.start()
        try:
            # 后台拉取线程与这条用例抢的是同一次拉取，所以只等结果、不_ASSERT_谁拉的
            pull_deadline = time.time() + 10
            while c.pull_cursor_of(q0) < 1200 and time.time() < pull_deadline:
                c._pull_one(q0)
            assert c.pull_cursor_of(q0) == 1200, c.pull_cursor_of(q0)
            assert c.consume_cursor_of(q0) == -1, "一条都没交付"
            c.commit()
            assert c._mock.updates == [], "没交付过就没有位点可提交"

            first = c.poll(timeout=2000)
            assert len(first) == 1024, len(first)
            assert c.consume_cursor_of(q0) == 1024
            assert c.pull_cursor_of(q0) == 1200, "尾巴那 176 条还压在缓冲里"
            c.commit()
            assert c._mock.updates == [("broker-a", 0, 1024)], \
                "提交的是已消费游标，不是拉取游标"
            # persist=False 只进内存表：committed() 看得见，broker 侧还是 1024
            c.commit({q0: 777}, persist=False)
            assert c.pending_commit_of(q0) == 777
            assert c.committed(q0) == 777
            assert c._mock.committed[("broker-a", 0)] == 1024
            # 点名提交会把没点名的内存值清掉（Java persistAll 的 remove unused mq）：
            # 这里 scope 只有一条 foreign，它自己没过 -1 守卫所以什么都没发，
            # 但 q0 那格 777 在扫表时被丢掉，回读只剩 broker 的 1024
            updates_before = len(c._mock.updates)
            c.commit([stranger], persist=True)
            assert len(c._mock.updates) == updates_before, "没点名的队列不该被发出去"
            assert c.pending_commit_of(q0) == -1
            assert c.committed(q0) == 1024
        finally:
            c.shutdown()
