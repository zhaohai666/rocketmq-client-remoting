# -*- coding: utf-8 -*-
"""队列分配策略单测（不需要集群）。

期望结果由静态分区表逐值钉死：同一张表、同一组入参，结果必须逐值相同。

覆盖：AVG / AVG_BY_CIRCLE 的不整除切分、守卫（非法入参返回空列表而不是抛
``IllegalArgumentException``）、CONFIG 忽略守卫且返回副本、``get_name()`` 与经典
remoting 协议里的策略名逐字一致（AVG / AVG_BY_CIRCLE / CONFIG）、分区完整性与确定性、
以及 push / lite 两个消费者对策略的暴露（默认 AVG、可替换、置 None 时 ``start()``
本地校验拒绝）。
"""
from __future__ import annotations

import itertools
from typing import List

import pytest

from client.consumer import (
    AllocateMessageQueueAveragely,
    AllocateMessageQueueAveragelyByCircle,
    AllocateMachineRoomNearby,
    AllocateMessageQueueByConfig,
    AllocateMessageQueueByMachineRoom,
    AllocateMessageQueueConsistentHash,
    AllocateMessageQueueStrategy,
    DefaultLitePullConsumer,
    DefaultMQPullConsumer,
    DefaultMQPushConsumer,
    MD5Hash,
    _split_dropping_trailing_empty,
    _strategy_name,
)
from client.consumer_result import ConsumeConcurrentlyStatus
from client.exception import MQClientException
from common.message import MessageQueue


def _queues(size: int) -> List[MessageQueue]:
    """构造 size 条固定队列：topic / brokerName / 0..size-1。"""
    return [MessageQueue("topic", "brokerName", i) for i in range(size)]


def _cids(size: int) -> List[str]:
    """构造 size 个固定消费者 id：CID_PREFIX + i。"""
    return ["CID_PREFIX%d" % i for i in range(size)]


def _ids(mqs: List[MessageQueue]) -> List[int]:
    return [mq.queue_id for mq in mqs]


def _allocate(strategy: AllocateMessageQueueStrategy, mq_size: int, cid_size: int,
              index: int) -> List[int]:
    mq_all, cid_all = _queues(mq_size), _cids(cid_size)
    return _ids(strategy.allocate("ConsumerGroupTest", cid_all[index], mq_all, cid_all))


# (队列数, 消费者数, 逐个消费者按 CID_PREFIX0..N 顺序的期望队列下标)
AVERAGELY_CASES = [
    # 10 / 4 → size {3,3,2,2}
    (10, 4, [[0, 1, 2], [3, 4, 5], [6, 7], [8, 9]]),
    (8, 3, [[0, 1, 2], [3, 4, 5], [6, 7]]),
    (9, 3, [[0, 1, 2], [3, 4, 5], [6, 7, 8]]),
    (4, 4, [[0], [1], [2], [3]]),
    # 队列比消费者少：只有前 N 个各 1 条，其余空
    (2, 4, [[0], [1], [], []]),
    (1, 3, [[0], [], []]),
    (3, 1, [[0, 1, 2]]),
    (0, 2, [[], []]),
]

CIRCLE_CASES = [
    # 10 / 4 → {0,4,8} {1,5,9} {2,6} {3,7}
    (10, 4, [[0, 4, 8], [1, 5, 9], [2, 6], [3, 7]]),
    (8, 3, [[0, 3, 6], [1, 4, 7], [2, 5]]),
    (9, 3, [[0, 3, 6], [1, 4, 7], [2, 5, 8]]),
    (4, 4, [[0], [1], [2], [3]]),
    (2, 4, [[0], [1], [], []]),
    (1, 3, [[0], [], []]),
    (3, 1, [[0, 1, 2]]),
    (0, 2, [[], []]),
]


@pytest.mark.parametrize("strategy,cases", [
    (AllocateMessageQueueAveragely(), AVERAGELY_CASES),
    (AllocateMessageQueueAveragelyByCircle(), CIRCLE_CASES),
])
def test_partition_table(strategy, cases):
    """分区表逐值钉死：同表同入参，结果必须逐值相同。"""
    for mq_size, cid_size, expect in cases:
        assert len(expect) == cid_size
        for index, expected in enumerate(expect):
            got = _allocate(strategy, mq_size, cid_size, index)
            assert got == expected, "%s mq=%d cid=%d index=%d → %s" % (
                strategy.get_name(), mq_size, cid_size, index, got)


def test_averagely_produces_the_expected_sizes():
    """AVG 的 size 断言：10 / 4 → {3,3,2,2}。"""
    strategy = AllocateMessageQueueAveragely()
    assert [len(_allocate(strategy, 10, 4, i)) for i in range(4)] == [3, 3, 2, 2]


def test_circle_not_in_cid_all():
    """AVG_BY_CIRCLE：currentCID 不在 cidAll → 空。"""
    got = AllocateMessageQueueAveragelyByCircle().allocate(
        "G", "CID_PREFIX", _queues(10), _cids(4))
    assert got == []


@pytest.mark.parametrize("current_cid,mq_size,cid_size", [
    ("", 4, 2),
    ("CID_PREFIX0", 0, 2),
    ("CID_PREFIX0", 4, 0),
    ("CID_NOT_IN_LIST", 4, 2),
])
def test_guards_return_empty_result(current_cid, mq_size, cid_size):
    """守卫：非法入参一律返回空列表，不抛 ``IllegalArgumentException``。"""
    mq_all, cid_all = _queues(mq_size), _cids(cid_size)
    for strategy in (AllocateMessageQueueAveragely(), AllocateMessageQueueAveragelyByCircle()):
        assert strategy.allocate("G", current_cid, mq_all, cid_all) == [], strategy.get_name()


def test_by_config_produces_the_expected_assignment():
    """CONFIG：4 个队列，2 个消费者都拿到 [0,1,2,3]。"""
    mq_all, cid_all = _queues(4), _cids(2)
    strategy = AllocateMessageQueueByConfig()
    strategy.message_queue_list = list(mq_all)
    for cid in cid_all:
        assert _ids(strategy.allocate("G", cid, mq_all, cid_all)) == [0, 1, 2, 3]


@pytest.mark.parametrize("current_cid,mq_size,cid_size", [
    ("", 0, 0),
    ("anyCID", 5, 0),
    ("CID_NOT_IN_LIST", 4, 2),
])
def test_by_config_ignores_all_guards(current_cid, mq_size, cid_size):
    """ByConfig **不**做入参检查：守卫场景照样返回配置值。"""
    strategy = AllocateMessageQueueByConfig(_queues(2))
    got = strategy.allocate("G", current_cid, _queues(mq_size), _cids(cid_size))
    assert _ids(got) == [0, 1]


def test_by_config_defaults_empty_and_returns_copy():
    """未配置 = 空列表；allocate 给副本，改配置不影响已拿到的结果。"""
    strategy = AllocateMessageQueueByConfig()
    assert strategy.allocate("G", "c", _queues(2), ["c"]) == []
    mq_all = _queues(2)
    strategy = AllocateMessageQueueByConfig(mq_all)
    got = strategy.allocate("G", "c", _queues(2), ["c"])
    got.append(MessageQueue("topic", "brokerName", 99))
    assert _ids(strategy.allocate("G", "c", mq_all, ["c"])) == [0, 1]
    # 构造时同样取副本：外部列表后来被改，不影响策略
    mq_all.append(MessageQueue("topic", "brokerName", 99))
    assert _ids(strategy.allocate("G", "c", mq_all, ["c"])) == [0, 1]


def test_strategy_names_match_the_wire_values():
    """``get_name()`` 返回经典 remoting 协议里的固定策略名，逐字一致。"""
    assert AllocateMessageQueueAveragely().get_name() == "AVG"
    assert AllocateMessageQueueAveragelyByCircle().get_name() == "AVG_BY_CIRCLE"
    assert AllocateMessageQueueByConfig().get_name() == "CONFIG"
    assert _strategy_name(AllocateMessageQueueAveragely()) == "AVG"


def test_interface_methods_not_implemented():
    """接口本身只声明契约：``allocate`` / ``get_name`` 都抛 NotImplementedError。"""
    strategy = AllocateMessageQueueStrategy()
    with pytest.raises(NotImplementedError):
        strategy.allocate("G", "c", _queues(1), ["c"])
    with pytest.raises(NotImplementedError):
        strategy.get_name()


def test_strategy_name_falls_back_for_duck_typed_strategy():
    """业务方可以只实现 ``allocate``（Python 是鸭子类型）：日志取名时退化成类名。"""
    class _Custom(object):
        def allocate(self, consumer_group, current_cid, mq_all, cid_all):
            return list(mq_all)

    assert _strategy_name(_Custom()) == "_Custom"


@pytest.mark.parametrize("mq_size,cid_size", list(itertools.product(range(0, 8), range(1, 6))))
def test_partition_covers_every_queue_exactly_once(mq_size, cid_size):
    """两个策略都必须划分全集：并集 = 全部队列且互不重叠（除 CONFIG）。"""
    for strategy in (AllocateMessageQueueAveragely(), AllocateMessageQueueAveragelyByCircle()):
        all_ids = list(range(mq_size))
        parts = [_allocate(strategy, mq_size, cid_size, i) for i in range(cid_size)]
        flat = [q for part in parts for q in part]
        assert sorted(flat) == all_ids, "%s mq=%d cid=%d → %s" % (
            strategy.get_name(), mq_size, cid_size, flat)


def test_allocate_is_order_preserving_and_deterministic():
    """输出是输入的一个有序子序列，且同一入参重复调用结果相同（rebalance 依赖这一点）。"""
    mq_all, cid_all = _queues(7), _cids(3)
    for strategy in (AllocateMessageQueueAveragely(), AllocateMessageQueueAveragelyByCircle()):
        for index in range(3):
            first = strategy.allocate("G", cid_all[index], mq_all, cid_all)
            second = strategy.allocate("G", cid_all[index], mq_all, cid_all)
            assert _ids(first) == _ids(second)
            assert _ids(first) == sorted(_ids(first))
            assert all(mq.topic == "topic" for mq in first)


def test_allocate_real_queues_from_multiple_brokers():
    """真实队列（同 topic 挂多个 brokerName）也一律按下标切分。"""
    layout = [("broker-a", 0), ("broker-a", 1), ("broker-b", 0), ("broker-b", 1), ("broker-c", 0)]
    mq_all = [MessageQueue("TopicTest", broker, qid) for broker, qid in layout]
    cid_all = _cids(2)
    strategy = AllocateMessageQueueAveragely()
    first = strategy.allocate("G", cid_all[0], mq_all, cid_all)
    second = strategy.allocate("G", cid_all[1], mq_all, cid_all)
    assert [(m.broker_name, m.queue_id) for m in first] == [("broker-a", 0), ("broker-a", 1), ("broker-b", 0)]
    assert [(m.broker_name, m.queue_id) for m in second] == [("broker-b", 1), ("broker-c", 0)]


def test_push_consumer_exposes_strategy():
    """push：默认 AVG、可替换；置 None 由 start() 本地校验拒绝。"""
    consumer = DefaultMQPushConsumer("G_test")
    assert consumer.allocate_strategy.get_name() == "AVG"
    consumer.set_allocate_message_queue_strategy(AllocateMessageQueueAveragelyByCircle())
    assert consumer.allocate_strategy.get_name() == "AVG_BY_CIRCLE"
    consumer.allocate_strategy = None
    consumer.set_namesrv_addr("127.0.0.1:1")
    consumer.subscribe("TopicTest", "*")
    # start() 的守卫是顺序式的：先订阅 / 监听器，再到策略（校验全在本地，不建连）
    consumer.set_message_listener(lambda msgs: ConsumeConcurrentlyStatus.CONSUME_SUCCESS)
    with pytest.raises(MQClientException) as info:
        consumer.start()
    assert "allocateMessageQueueStrategy is null" in str(info.value)


def test_lite_consumer_exposes_strategy():
    """lite：默认 AVG、可替换；置 None 由 start() 本地校验拒绝。"""
    consumer = DefaultLitePullConsumer("G_test")
    assert consumer.allocate_message_queue_strategy.get_name() == "AVG"
    by_config = AllocateMessageQueueByConfig(_queues(2))
    consumer.set_allocate_message_queue_strategy(by_config)
    assert consumer.allocate_message_queue_strategy.get_name() == "CONFIG"
    assert _ids(by_config.allocate("G", "whoever", _queues(9), _cids(3))) == [0, 1]
    consumer.allocate_message_queue_strategy = None
    consumer.set_namesrv_addr("127.0.0.1:1")
    consumer.subscribe("TopicTest", "*")
    with pytest.raises(MQClientException) as info:
        consumer.start()
    assert "allocateMessageQueueStrategy is null" in str(info.value)


def test_lite_rebalance_keeps_current_assignment_when_strategy_raises():
    """策略抛异常 → 本轮跳过该 topic（保留当前分配），不因瞬时异常撤掉队列。"""
    consumer = DefaultLitePullConsumer("G_test")
    held = MessageQueue("TopicTest", "broker-a", 3)
    consumer._assigned = {held}

    class _Boom(object):
        def allocate(self, consumer_group, current_cid, mq_all, cid_all):
            raise RuntimeError("boom")

        def get_name(self):
            return "BOOM"

    class _Client(object):
        def get_topic_publish_info(self, topic):
            class _Info(object):
                msg_queue_list = _queues(4)
            return _Info()

        def get_topic_subscribe_info(self, topic):
            # rebalance 取的是订阅信息
            return _queues(4)

        def get_consumer_id_list_by_group(self, topic, consumer_group, timeout_millis=5000):
            return ["CID_PREFIX0"]

    consumer.client_id = "CID_PREFIX0"
    consumer.subscription = {"TopicTest": None}
    consumer._mq_client = _Client()
    consumer.allocate_message_queue_strategy = _Boom()

    consumer._rebalance()

    assert consumer._assigned == {held}


def test_pull_consumer_exposes_strategy():
    """拉模式同样带这份配置：默认策略字段 + 读写口，start() 校验拒绝 null。
    本端口拉模式不做 rebalance，所以它只是配置面。"""
    consumer = DefaultMQPullConsumer("G_test")
    assert consumer.allocate_message_queue_strategy.get_name() == "AVG"
    by_config = AllocateMessageQueueByConfig(_queues(2))
    consumer.set_allocate_message_queue_strategy(by_config)
    assert consumer.allocate_message_queue_strategy.get_name() == "CONFIG"
    assert _ids(by_config.allocate("G", "whoever", _queues(9), _cids(3))) == [0, 1]
    consumer.set_allocate_message_queue_strategy(None)
    assert consumer.allocate_message_queue_strategy is None
    consumer.set_namesrv_addr("127.0.0.1:1")
    with pytest.raises(MQClientException) as info:
        consumer.start()
    assert "allocateMessageQueueStrategy is null" in str(info.value)


# ------------------------------------------------- 一致性哈希（CONSISTENT_HASH）
#
# 用 ``AllocateMessageQueueConsistentHash(3)`` 覆盖固定规模与随机规模，断言三件事：
# ①全员分配 = 全集，②摘掉一个消费者后**别人的**队列不换主，③加一个消费者后
# 只有新来者抢到的那些换主。

# (队列数, 消费者数, 逐个 CID-0..N 的期望队列下标)：由 MD5 环算出，逐值钉死
CONSISTENT_HASH_CASES = [
    (6, 2, [[2], [0, 1, 3, 4, 5]]),
    (6, 3, [[2], [1, 5], [0, 3, 4]]),
    (10, 4, [[2], [], [0, 3, 4, 8], [1, 5, 6, 7, 9]]),
    (20, 10, [[2, 14, 15], [17], [8, 11], [], [], [1, 5, 9, 10], [6, 7, 12],
               [13, 18], [16], [0, 3, 4, 19]]),
]

# 默认 virtualNodeCnt=10（无参构造）
CONSISTENT_HASH_DEFAULT_VC_CASES = [
    (4, 2, [[0, 2], [1, 3]]),
    (8, 3, [[0, 2, 4], [3, 5, 6, 7], [1]]),
]


def _ch_queues(size: int) -> List[MessageQueue]:
    """固定队列集：topic / brokerName / 0..N。"""
    return [MessageQueue("topic", "brokerName", i) for i in range(size)]


def _ch_cids(size: int) -> List[str]:
    """固定消费者 id：``CID-`` + i（与 AVG 用例的 CID_PREFIX 不同）。"""
    return ["CID-%d" % i for i in range(size)]


@pytest.mark.parametrize("mq_size,cid_size,expect",
                         CONSISTENT_HASH_CASES + CONSISTENT_HASH_DEFAULT_VC_CASES)
def test_consistent_hash_matches_the_virtual_node_ring(mq_size, cid_size, expect):
    """落点由 MD5(摘要前 4 字节、大端拼数) 的一致性哈希环决定：虚拟节点数 3 与默认 10 各钉一张表。"""
    vc = 3 if (mq_size, cid_size) in [(c[0], c[1]) for c in CONSISTENT_HASH_CASES] else 10
    strategy = AllocateMessageQueueConsistentHash(vc)
    mq_all, cid_all = _ch_queues(mq_size), _ch_cids(cid_size)
    for index, expected in enumerate(expect):
        got = _ids(strategy.allocate("testConsumerGroup", cid_all[index], mq_all, cid_all))
        assert got == expected, "vc=%d mq=%d cid=%d index=%d → %s" % (
            vc, mq_size, cid_size, index, got)


def test_md5_hash_takes_the_first_four_bytes():
    """``MD5Hash`` 只取摘要前 4 字节按大端拼数：换取法就不是同一个环。"""
    # md5("") = d41d8cd9...，md5("abc") = 90015098...
    assert MD5Hash().hash("") == 0xD41D8CD9
    assert MD5Hash().hash("abc") == 0x90015098


def test_consistent_hash_allocates_every_queue_once():
    """任意规模下并集 = 全集且不重叠。"""
    strategy = AllocateMessageQueueConsistentHash(3)
    for mq_size in range(1, 12):
        for cid_size in range(1, 8):
            mq_all, cid_all = _ch_queues(mq_size), _ch_cids(cid_size)
            flat = [m.queue_id for cid in cid_all
                    for m in strategy.allocate("g", cid, mq_all, cid_all)]
            assert sorted(flat) == list(range(mq_size)), (mq_size, cid_size, flat)


def test_consistent_hash_is_stable_when_membership_changes():
    """一致性哈希的全部意义：成员变化只动涉及的那段弧，别的消费者队列不换主。"""
    strategy = AllocateMessageQueueConsistentHash(3)
    mq_all = _ch_queues(9)
    cid_all = _ch_cids(4)
    owner_of = {m.queue_id: cid for cid in cid_all
                for m in strategy.allocate("g", cid, mq_all, cid_all)}
    assert sorted(owner_of) == list(range(9))

    # 摘掉 CID-0：原先属于别人的队列必须还在别人手里
    remaining = cid_all[1:]
    after_remove = {m.queue_id: cid for cid in remaining
                    for m in strategy.allocate("g", cid, mq_all, remaining)}
    for qid, owner in owner_of.items():
        if owner != "CID-0":
            assert after_remove[qid] == owner, "摘队时 qid=%d 不该换主" % qid

    # 加回一个新消费者：只有分给它的队列是新增的
    joined = remaining + ["CID-NEW"]
    after_add = {m.queue_id: cid for cid in joined
                 for m in strategy.allocate("g", cid, mq_all, joined)}
    for qid, owner in after_add.items():
        if owner != "CID-NEW":
            assert owner_of[qid] == owner, "加人时 qid=%d 不该换主" % qid


def test_consistent_hash_rejects_negative_virtual_node_cnt():
    """构造函数就拒绝负数虚拟节点数，抛 IllegalArgumentException("illegal virtualNodeCnt :")。"""
    with pytest.raises(ValueError) as info:
        AllocateMessageQueueConsistentHash(-1)
    assert "illegal virtualNodeCnt" in str(info.value)
    # 0 是合法的（只挡 <0）：环上没有虚拟节点 ⇒ 一条都分不到
    assert AllocateMessageQueueConsistentHash(0).allocate(
        "g", "CID-0", _ch_queues(4), _ch_cids(2)) == []


def test_custom_hash_function_is_injected():
    """支持注入自定义哈希函数；策略必须真的用它建环。"""
    class _FirstByte(object):
        def __init__(self):
            self.calls = 0

        def hash(self, key):
            self.calls += 1
            return ord(key[0]) if key else 0

    fn = _FirstByte()
    strategy = AllocateMessageQueueConsistentHash(2, fn)
    strategy.allocate("g", "CID-0", _ch_queues(4), _ch_cids(2))
    assert fn.calls > 0
    # 同场景真值：两个 cid 的虚拟节点 key（"CID-0-0"…）
    # 首字符都是 'C' ⇒ 哈希相同 ⇒ 建环时同 key 后者覆盖前者，
    # 环上只剩 CID-1，所以全部队列都归它。用默认 MD5Hash 绝不会是这个形状。
    assert _ids(strategy.allocate("g", "CID-0", _ch_queues(4), _ch_cids(2))) == []
    assert _ids(strategy.allocate("g", "CID-1", _ch_queues(4), _ch_cids(2))) == [0, 1, 2, 3]


def test_consistent_hash_guard_returns_empty_like_other_strategies():
    """守卫口径与其它策略一致：非法入参返回空，不抛 IllegalArgumentException。"""
    strategy = AllocateMessageQueueConsistentHash()
    assert strategy.allocate("g", "CID-NOT-HERE", _ch_queues(4), _ch_cids(2)) == []
    assert strategy.allocate("g", "", _ch_queues(4), _ch_cids(2)) == []
    assert strategy.allocate("g", "CID-0", [], _ch_cids(2)) == []
    assert strategy.allocate("g", "CID-0", _ch_queues(4), []) == []


# ----------------------------------------------------- 机房策略（MACHINE_ROOM）

def test_by_machine_room_produces_the_expected_assignment():
    """MACHINE_ROOM：10 队列（0..4 在 room1）+
    consumeridcs={room1} + 2 消费者 → {0,1,4} / {2,3}。

    余数队列给**前 rem 个**消费者（``rem > currentIndex``），所以第 3 条落 CID_PREFIX0。
    """
    mq_all = [MessageQueue("topic", "room1@broker-a" if i < 5 else "room2@broker-b", i)
              for i in range(10)]
    cid_all = _cids(2)
    strategy = AllocateMessageQueueByMachineRoom({"room1"})
    got = {cid: _ids(strategy.allocate("G", cid, mq_all, cid_all)) for cid in cid_all}
    assert got["CID_PREFIX0"] == [0, 1, 4]
    assert got["CID_PREFIX1"] == [2, 3]


def test_by_machine_room_only_accepts_the_two_segment_broker_name():
    """broker 名必须是 ``机房@名字``：没有 @ 或多一段都不参与分配。"""
    cid_all = _cids(1)
    strategy = AllocateMessageQueueByMachineRoom({"room1"})
    assert strategy.allocate("G", "CID_PREFIX0",
                             [MessageQueue("topic", "room1@broker-a", 0)], cid_all) != []
    assert strategy.allocate("G", "CID_PREFIX0",
                             [MessageQueue("topic", "broker-a", 0)], cid_all) == []
    assert strategy.allocate("G", "CID_PREFIX0",
                             [MessageQueue("topic", "room1@broker@a", 0)], cid_all) == []
    # 机房不在白名单里也不分
    assert _ids(strategy.allocate("G", "CID_PREFIX0",
                                  [MessageQueue("topic", "room9@b", 0)], cid_all)) == []
    # 默认没配机房 = 一条都不分
    assert AllocateMessageQueueByMachineRoom().allocate(
        "G", "CID_PREFIX0", [MessageQueue("topic", "room1@b", 0)], cid_all) == []


@pytest.mark.parametrize("broker_name,expected_parts", [
    ("room1@broker-a", ["room1", "broker-a"]),
    ("room1@", ["room1"]),           # 尾空段丢掉 → 1 段，不参与分配
    ("room1@b@", ["room1", "b"]),    # 丢掉尾空段后仍是 2 段 → 参与
    ("@room1", ["", "room1"]),       # 前导空段保留
    ("room1@broker@a", ["room1", "broker", "a"]),
    ("@", []),
    ("broker-a", ["broker-a"]),      # 无分隔符命中：原样返回整串
    ("", [""]),                      # 同上，哪怕整串是空
])
def test_split_drops_trailing_empty_segments(broker_name, expected_parts):
    """按分隔符切分会丢掉尾部空段（``a@b@`` → 2 段），Python 原生 split 不丢。

    ``AllocateMessageQueueByMachineRoom`` 按 ``length == 2`` 判合法，所以这条差异会直接
    改变一条队列参不参与分配 —— 必须按丢尾空段的口径来。
    """
    assert _split_dropping_trailing_empty(broker_name, "@") == expected_parts


def test_by_machine_room_uses_the_trailing_empty_split_on_the_broker_name():
    """``room1@`` 不算两段（不参与）、``room1@b@`` 算两段（参与）。"""
    cid_all = _cids(1)
    strategy = AllocateMessageQueueByMachineRoom({"room1"})
    assert _ids(strategy.allocate("G", "CID_PREFIX0",
                                  [MessageQueue("topic", "room1@", 0)], cid_all)) == []
    assert _ids(strategy.allocate("G", "CID_PREFIX0",
                                  [MessageQueue("topic", "room1@b@", 0)], cid_all)) == [0]
    # "@room1" 是两段，但机房是空串、不在白名单
    assert _ids(strategy.allocate("G", "CID_PREFIX0",
                                  [MessageQueue("topic", "@room1", 0)], cid_all)) == []
    # "@" 切分后是 0 段
    assert _ids(strategy.allocate("G", "CID_PREFIX0",
                                  [MessageQueue("topic", "@", 0)], cid_all)) == []


# --------------------------------------------- 机房就近代理（MACHINE_ROOM_NEARBY）

class _DashRoom(object):
    """测试用 resolver：broker ``IDCx-brokerName`` / 消费者 ``IDCx-CID-i`` 按 '-' 取前段。"""

    def broker_deploy_in(self, message_queue):
        return message_queue.broker_name.split("-")[0]

    def consumer_deploy_in(self, client_id):
        return client_id.split("-")[0]


def _nearby_mq(idc_size, queue_size):
    return [m for i in range(1, idc_size + 1)
            for m in [MessageQueue("topic", "IDC%d-brokerName" % i, q)
                      for q in range(queue_size)]]


def _nearby_cids(idc_size, consumer_size):
    return ["IDC%d-CID-%d" % (i, q) for i in range(1, idc_size + 1)
            for q in range(consumer_size)]


@pytest.mark.parametrize("idc_size,queue_size,consumer_size", [
    (5, 20, 10), (5, 20, 20), (5, 20, 30), (5, 20, 1),
])
def test_nearby_allocates_same_room_only_and_covers_everything(idc_size, queue_size,
                                                               consumer_size):
    """机房数相等时，每个消费者只能拿到**同机房**的队列，
    且全员并集恰好是全集（不重不漏）。"""
    strategy = AllocateMachineRoomNearby(AllocateMessageQueueAveragely(), _DashRoom())
    mq_all, cid_all = _nearby_mq(idc_size, queue_size), _nearby_cids(idc_size, consumer_size)
    flat = []
    for cid in cid_all:
        res = strategy.allocate("Test-C-G", cid, mq_all, cid_all)
        for mq in res:
            assert _DashRoom().broker_deploy_in(mq) == _DashRoom().consumer_deploy_in(cid)
        flat += res
    assert sorted(_ids(flat)) == sorted(_ids(mq_all))
    assert len(flat) == len(mq_all)


def test_nearby_lets_other_rooms_share_queues_with_no_consumer():
    """broker 机房比消费者机房多时，那些**没有活消费者**
    的机房队列要交给全部消费者共享，否则没人消费；有消费者的机房仍然只给自己的消费者。"""
    strategy = AllocateMachineRoomNearby(AllocateMessageQueueAveragely(), _DashRoom())
    mq_all = _nearby_mq(5, 4)                       # IDC1..IDC5 各 4 条
    cid_all = _nearby_cids(2, 3)                    # 只有 IDC1、IDC2 有消费者
    healthy = {"IDC1", "IDC2"}
    # 队列 id 在各机房之间会重名，所以用 (broker, qid) 当身份
    key = lambda mq: (mq.broker_name, mq.queue_id)  # noqa: E731
    allocated = {key(mq): cid for cid in cid_all
                 for mq in strategy.allocate("Test-C-G", cid, mq_all, cid_all)}
    assert set(allocated) == {key(mq) for mq in mq_all}, "每条队列都得有人消费"
    for mq in mq_all:
        room = _DashRoom().broker_deploy_in(mq)
        if room in healthy:  # 健康机房不共享：只分给同机房的消费者
            assert _DashRoom().consumer_deploy_in(allocated[key(mq)]) == room, mq
        else:                # 空机房的队列落在两个机房之间
            assert _DashRoom().consumer_deploy_in(allocated[key(mq)]) in healthy, mq
    # 本机房消费者把自己机房的队列收全
    for room in healthy:
        mine = {key(mq) for mq, cid in
                ((m, c) for c in cid_all
                 for m in strategy.allocate("Test-C-G", c, mq_all, cid_all))
                if _DashRoom().consumer_deploy_in(cid) == room}
        assert mine >= {key(mq) for mq in mq_all if mq.broker_name.startswith(room)}


def test_nearby_shares_a_dead_room_queues_in_a_stable_order():
    """两机房小场景：``IDC2`` 4 条 + ``IDC1`` 2 条、``IDC1`` 两个消费者 →
    CID-0 拿 ``[IDC1#0, IDC2#0, IDC2#1]``、CID-1 拿 ``[IDC1#1, IDC2#2, IDC2#3]``。

    顺序也要钉住：先分配**本机房**的队列，再把
    "没有活消费者"的机房按机房名字典序补上，所以同机房的队列一定排在前面。
    """
    strategy = AllocateMachineRoomNearby(AllocateMessageQueueAveragely(), _DashRoom())
    mq_all = [MessageQueue("topic", "IDC2-brokerName", q) for q in range(4)] \
        + [MessageQueue("topic", "IDC1-brokerName", q) for q in range(2)]
    cid_all = ["IDC1-CID-0", "IDC1-CID-1"]
    got = {cid: [(m.broker_name, m.queue_id)
                 for m in strategy.allocate("G", cid, mq_all, cid_all)] for cid in cid_all}
    assert got["IDC1-CID-0"] == [("IDC1-brokerName", 0),
                                 ("IDC2-brokerName", 0),
                                 ("IDC2-brokerName", 1)]
    assert got["IDC1-CID-1"] == [("IDC1-brokerName", 1),
                                 ("IDC2-brokerName", 2),
                                 ("IDC2-brokerName", 3)]


def test_nearby_name_exposes_the_inner_strategy():
    """``get_name()`` = ``MACHINE_ROOM_NEARBY-<内层策略名>``。"""
    assert AllocateMachineRoomNearby(
        AllocateMessageQueueAveragely(), _DashRoom()).get_name() == "MACHINE_ROOM_NEARBY-AVG"
    assert AllocateMachineRoomNearby(
        AllocateMessageQueueAveragelyByCircle(), _DashRoom()).get_name(
    ) == "MACHINE_ROOM_NEARBY-AVG_BY_CIRCLE"
    assert AllocateMachineRoomNearby(
        AllocateMessageQueueByConfig(_queues(1)), _DashRoom()).get_name(
    ) == "MACHINE_ROOM_NEARBY-CONFIG"


@pytest.mark.parametrize("inner,resolver", [(None, _DashRoom()), (AllocateMessageQueueAveragely(), None)])
def test_nearby_rejects_null_arguments(inner, resolver):
    """非法参数在构造期就抛 NullPointerException（不是 rebalance 期的守卫，照抛）。"""
    with pytest.raises(ValueError):
        AllocateMachineRoomNearby(inner, resolver)


def test_nearby_raises_when_a_room_is_unknown():
    """resolver 给出空机房时必须抛 IllegalArgumentException：**不能**静默返回空列表，
    那等于把整个 topic 的队列撤走；抛出去由 rebalance 兜住并保持现有分配。"""
    class _BlankRoom(object):
        def broker_deploy_in(self, mq):
            return ""

        def consumer_deploy_in(self, cid):
            return "IDC1"

    strategy = AllocateMachineRoomNearby(AllocateMessageQueueAveragely(), _BlankRoom())
    with pytest.raises(ValueError) as info:
        strategy.allocate("G", "CID-0", _ch_queues(2), ["CID-0"])
    assert "Machine room is null for mq" in str(info.value)


def test_new_strategy_names_match_the_wire_values():
    assert AllocateMessageQueueConsistentHash().get_name() == "CONSISTENT_HASH"
    assert AllocateMessageQueueByMachineRoom().get_name() == "MACHINE_ROOM"


def test_consistent_hash_drives_rebalance():
    """换上一致性哈希后，消费者的 rebalance 必须真的用它（策略不是摆设）。"""
    class _Client(object):
        def get_topic_publish_info(self, topic):
            class _Info(object):
                msg_queue_list = _queues(4)
            return _Info()

        def get_topic_subscribe_info(self, topic):
            # rebalance 取的是订阅信息
            return _queues(4)

        def get_consumer_id_list_by_group(self, topic, consumer_group, timeout_millis=5000):
            return ["CID_PREFIX0"]

    consumer = DefaultLitePullConsumer("G_test")
    consumer.client_id = "CID_PREFIX0"
    consumer.subscription = {"TopicTest": None}
    consumer._mq_client = _Client()
    consumer.allocate_message_queue_strategy = AllocateMessageQueueConsistentHash(3)
    consumer._rebalance()
    # 单消费者 + 哈希环：4 条队列全落在自己身上
    assert _ids(sorted(consumer._assigned, key=lambda m: m.queue_id)) == [0, 1, 2, 3]
    assert consumer.allocate_message_queue_strategy.get_name() == "CONSISTENT_HASH"
