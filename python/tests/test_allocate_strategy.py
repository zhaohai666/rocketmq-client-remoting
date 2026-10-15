# -*- coding: utf-8 -*-
"""Unit tests for queue allocation strategies (no cluster needed).

Expected results are pinned value-by-value against static partition tables: the same
table with the same inputs must produce identical results.

Covers: AVG / AVG_BY_CIRCLE uneven splits, guards (invalid inputs return an empty list
instead of raising ``IllegalArgumentException``), CONFIG ignoring guards and returning a
copy, ``get_name()`` matching the strategy names in the classic remoting protocol
verbatim (AVG / AVG_BY_CIRCLE / CONFIG), partition completeness and determinism,
and the strategy exposure of the push / lite consumers (default AVG, replaceable,
rejected by ``start()`` local validation when set to None).
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
    """Build `size` fixed queues: topic / brokerName / 0..size-1."""
    return [MessageQueue("topic", "brokerName", i) for i in range(size)]


def _cids(size: int) -> List[str]:
    """Build `size` fixed consumer ids: CID_PREFIX + i."""
    return ["CID_PREFIX%d" % i for i in range(size)]


def _ids(mqs: List[MessageQueue]) -> List[int]:
    return [mq.queue_id for mq in mqs]


def _allocate(strategy: AllocateMessageQueueStrategy, mq_size: int, cid_size: int,
              index: int) -> List[int]:
    mq_all, cid_all = _queues(mq_size), _cids(cid_size)
    return _ids(strategy.allocate("ConsumerGroupTest", cid_all[index], mq_all, cid_all))


# (queue count, consumer count, expected queue indexes per consumer in CID_PREFIX0..N order)
AVERAGELY_CASES = [
    # 10 / 4 → sizes {3,3,2,2}
    (10, 4, [[0, 1, 2], [3, 4, 5], [6, 7], [8, 9]]),
    (8, 3, [[0, 1, 2], [3, 4, 5], [6, 7]]),
    (9, 3, [[0, 1, 2], [3, 4, 5], [6, 7, 8]]),
    (4, 4, [[0], [1], [2], [3]]),
    # Fewer queues than consumers: only the first N get one each, the rest are empty
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
    """Partition table pinned value-by-value: same table, same inputs, results must match exactly."""
    for mq_size, cid_size, expect in cases:
        assert len(expect) == cid_size
        for index, expected in enumerate(expect):
            got = _allocate(strategy, mq_size, cid_size, index)
            assert got == expected, "%s mq=%d cid=%d index=%d → %s" % (
                strategy.get_name(), mq_size, cid_size, index, got)


def test_averagely_produces_the_expected_sizes():
    """AVG size assertion: 10 / 4 → {3,3,2,2}."""
    strategy = AllocateMessageQueueAveragely()
    assert [len(_allocate(strategy, 10, 4, i)) for i in range(4)] == [3, 3, 2, 2]


def test_circle_not_in_cid_all():
    """AVG_BY_CIRCLE: currentCID not in cidAll → empty."""
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
    """Guards: invalid inputs always return an empty list, never raise ``IllegalArgumentException``."""
    mq_all, cid_all = _queues(mq_size), _cids(cid_size)
    for strategy in (AllocateMessageQueueAveragely(), AllocateMessageQueueAveragelyByCircle()):
        assert strategy.allocate("G", current_cid, mq_all, cid_all) == [], strategy.get_name()


def test_by_config_produces_the_expected_assignment():
    """CONFIG: 4 queues, both consumers get [0,1,2,3]."""
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
    """ByConfig does **not** validate inputs: guard scenarios still return the configured values."""
    strategy = AllocateMessageQueueByConfig(_queues(2))
    got = strategy.allocate("G", current_cid, _queues(mq_size), _cids(cid_size))
    assert _ids(got) == [0, 1]


def test_by_config_defaults_empty_and_returns_copy():
    """Unconfigured = empty list; allocate returns a copy, so changing the config does not affect already-obtained results."""
    strategy = AllocateMessageQueueByConfig()
    assert strategy.allocate("G", "c", _queues(2), ["c"]) == []
    mq_all = _queues(2)
    strategy = AllocateMessageQueueByConfig(mq_all)
    got = strategy.allocate("G", "c", _queues(2), ["c"])
    got.append(MessageQueue("topic", "brokerName", 99))
    assert _ids(strategy.allocate("G", "c", mq_all, ["c"])) == [0, 1]
    # The copy is taken at construction time too: later mutation of the external list does not affect the strategy
    mq_all.append(MessageQueue("topic", "brokerName", 99))
    assert _ids(strategy.allocate("G", "c", mq_all, ["c"])) == [0, 1]


def test_strategy_names_match_the_wire_values():
    """``get_name()`` returns the fixed strategy names from the classic remoting protocol, verbatim."""
    assert AllocateMessageQueueAveragely().get_name() == "AVG"
    assert AllocateMessageQueueAveragelyByCircle().get_name() == "AVG_BY_CIRCLE"
    assert AllocateMessageQueueByConfig().get_name() == "CONFIG"
    assert _strategy_name(AllocateMessageQueueAveragely()) == "AVG"


def test_interface_methods_not_implemented():
    """The interface itself only declares the contract: both ``allocate`` and ``get_name`` raise NotImplementedError."""
    strategy = AllocateMessageQueueStrategy()
    with pytest.raises(NotImplementedError):
        strategy.allocate("G", "c", _queues(1), ["c"])
    with pytest.raises(NotImplementedError):
        strategy.get_name()


def test_strategy_name_falls_back_for_duck_typed_strategy():
    """Callers may implement only ``allocate`` (Python is duck-typed): the log name falls back to the class name."""
    class _Custom(object):
        def allocate(self, consumer_group, current_cid, mq_all, cid_all):
            return list(mq_all)

    assert _strategy_name(_Custom()) == "_Custom"


@pytest.mark.parametrize("mq_size,cid_size", list(itertools.product(range(0, 8), range(1, 6))))
def test_partition_covers_every_queue_exactly_once(mq_size, cid_size):
    """Both strategies must partition the full set: union = all queues and no overlap (except CONFIG)."""
    for strategy in (AllocateMessageQueueAveragely(), AllocateMessageQueueAveragelyByCircle()):
        all_ids = list(range(mq_size))
        parts = [_allocate(strategy, mq_size, cid_size, i) for i in range(cid_size)]
        flat = [q for part in parts for q in part]
        assert sorted(flat) == all_ids, "%s mq=%d cid=%d → %s" % (
            strategy.get_name(), mq_size, cid_size, flat)


def test_allocate_is_order_preserving_and_deterministic():
    """The output is an ordered subsequence of the input, and repeated calls with the same inputs return the same result (rebalance depends on this)."""
    mq_all, cid_all = _queues(7), _cids(3)
    for strategy in (AllocateMessageQueueAveragely(), AllocateMessageQueueAveragelyByCircle()):
        for index in range(3):
            first = strategy.allocate("G", cid_all[index], mq_all, cid_all)
            second = strategy.allocate("G", cid_all[index], mq_all, cid_all)
            assert _ids(first) == _ids(second)
            assert _ids(first) == sorted(_ids(first))
            assert all(mq.topic == "topic" for mq in first)


def test_allocate_real_queues_from_multiple_brokers():
    """Real queues (same topic spread across multiple brokerNames) are also split purely by index."""
    layout = [("broker-a", 0), ("broker-a", 1), ("broker-b", 0), ("broker-b", 1), ("broker-c", 0)]
    mq_all = [MessageQueue("TopicTest", broker, qid) for broker, qid in layout]
    cid_all = _cids(2)
    strategy = AllocateMessageQueueAveragely()
    first = strategy.allocate("G", cid_all[0], mq_all, cid_all)
    second = strategy.allocate("G", cid_all[1], mq_all, cid_all)
    assert [(m.broker_name, m.queue_id) for m in first] == [("broker-a", 0), ("broker-a", 1), ("broker-b", 0)]
    assert [(m.broker_name, m.queue_id) for m in second] == [("broker-b", 1), ("broker-c", 0)]


def test_push_consumer_exposes_strategy():
    """push: default AVG, replaceable; None is rejected by start() local validation."""
    consumer = DefaultMQPushConsumer("G_test")
    assert consumer.allocate_strategy.get_name() == "AVG"
    consumer.set_allocate_message_queue_strategy(AllocateMessageQueueAveragelyByCircle())
    assert consumer.allocate_strategy.get_name() == "AVG_BY_CIRCLE"
    consumer.allocate_strategy = None
    consumer.set_namesrv_addr("127.0.0.1:1")
    consumer.subscribe("TopicTest", "*")
    # start()'s guards are ordered: subscription / listener first, then the strategy (all validation is local, no connection is made)
    consumer.set_message_listener(lambda msgs: ConsumeConcurrentlyStatus.CONSUME_SUCCESS)
    with pytest.raises(MQClientException) as info:
        consumer.start()
    assert "allocateMessageQueueStrategy is null" in str(info.value)


def test_lite_consumer_exposes_strategy():
    """lite: default AVG, replaceable; None is rejected by start() local validation."""
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
    """Strategy raises → this round skips the topic (keeping the current assignment); queues are not dropped due to a transient error."""
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
            # rebalance takes the subscription info
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
    """The pull mode carries the same configuration: a default strategy field plus read/write accessors, and start() rejects null.
    This port's pull mode does not rebalance, so it is only a configuration surface."""
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


# ------------------------------------------------- Consistent hashing (CONSISTENT_HASH)
#
# Use ``AllocateMessageQueueConsistentHash(3)`` to cover fixed and random scales, asserting three things:
# ① the full assignment = the whole set, ② after removing one consumer the **others'** queues do not change owner, ③ after adding one consumer only the queues won by the newcomer change owner.

# (queue count, consumer count, expected queue indexes per CID-0..N): computed from the MD5 ring, pinned value-by-value
CONSISTENT_HASH_CASES = [
    (6, 2, [[2], [0, 1, 3, 4, 5]]),
    (6, 3, [[2], [1, 5], [0, 3, 4]]),
    (10, 4, [[2], [], [0, 3, 4, 8], [1, 5, 6, 7, 9]]),
    (20, 10, [[2, 14, 15], [17], [8, 11], [], [], [1, 5, 9, 10], [6, 7, 12],
               [13, 18], [16], [0, 3, 4, 19]]),
]

# Default virtualNodeCnt=10 (no-arg constructor)
CONSISTENT_HASH_DEFAULT_VC_CASES = [
    (4, 2, [[0, 2], [1, 3]]),
    (8, 3, [[0, 2, 4], [3, 5, 6, 7], [1]]),
]


def _ch_queues(size: int) -> List[MessageQueue]:
    """Fixed queue set: topic / brokerName / 0..N."""
    return [MessageQueue("topic", "brokerName", i) for i in range(size)]


def _ch_cids(size: int) -> List[str]:
    """Fixed consumer ids: ``CID-`` + i (different from the CID_PREFIX used in the AVG cases)."""
    return ["CID-%d" % i for i in range(size)]


@pytest.mark.parametrize("mq_size,cid_size,expect",
                         CONSISTENT_HASH_CASES + CONSISTENT_HASH_DEFAULT_VC_CASES)
def test_consistent_hash_matches_the_virtual_node_ring(mq_size, cid_size, expect):
    """Placement is decided by the MD5 consistent-hash ring (first 4 digest bytes, big-endian): virtual node counts 3 and the default 10 each pin one table."""
    vc = 3 if (mq_size, cid_size) in [(c[0], c[1]) for c in CONSISTENT_HASH_CASES] else 10
    strategy = AllocateMessageQueueConsistentHash(vc)
    mq_all, cid_all = _ch_queues(mq_size), _ch_cids(cid_size)
    for index, expected in enumerate(expect):
        got = _ids(strategy.allocate("testConsumerGroup", cid_all[index], mq_all, cid_all))
        assert got == expected, "vc=%d mq=%d cid=%d index=%d → %s" % (
            vc, mq_size, cid_size, index, got)


def test_md5_hash_takes_the_first_four_bytes():
    """``MD5Hash`` takes only the first 4 digest bytes as a big-endian number: a different extraction would not be the same ring."""
    # md5("") = d41d8cd9..., md5("abc") = 90015098...
    assert MD5Hash().hash("") == 0xD41D8CD9
    assert MD5Hash().hash("abc") == 0x90015098


def test_consistent_hash_allocates_every_queue_once():
    """At any scale, union = the whole set with no overlap."""
    strategy = AllocateMessageQueueConsistentHash(3)
    for mq_size in range(1, 12):
        for cid_size in range(1, 8):
            mq_all, cid_all = _ch_queues(mq_size), _ch_cids(cid_size)
            flat = [m.queue_id for cid in cid_all
                    for m in strategy.allocate("g", cid, mq_all, cid_all)]
            assert sorted(flat) == list(range(mq_size)), (mq_size, cid_size, flat)


def test_consistent_hash_is_stable_when_membership_changes():
    """The whole point of consistent hashing: membership changes only move the affected arc; other consumers' queues do not change owner."""
    strategy = AllocateMessageQueueConsistentHash(3)
    mq_all = _ch_queues(9)
    cid_all = _ch_cids(4)
    owner_of = {m.queue_id: cid for cid in cid_all
                for m in strategy.allocate("g", cid, mq_all, cid_all)}
    assert sorted(owner_of) == list(range(9))

    # Remove CID-0: queues that belonged to others must still be with others
    remaining = cid_all[1:]
    after_remove = {m.queue_id: cid for cid in remaining
                    for m in strategy.allocate("g", cid, mq_all, remaining)}
    for qid, owner in owner_of.items():
        if owner != "CID-0":
            assert after_remove[qid] == owner, "qid=%d must not change owner on removal" % qid

    # Add a new consumer back: only the queues assigned to it are new
    joined = remaining + ["CID-NEW"]
    after_add = {m.queue_id: cid for cid in joined
                 for m in strategy.allocate("g", cid, mq_all, joined)}
    for qid, owner in after_add.items():
        if owner != "CID-NEW":
            assert owner_of[qid] == owner, "qid=%d must not change owner on join" % qid


def test_consistent_hash_rejects_negative_virtual_node_cnt():
    """The constructor rejects a negative virtual node count with IllegalArgumentException("illegal virtualNodeCnt :")."""
    with pytest.raises(ValueError) as info:
        AllocateMessageQueueConsistentHash(-1)
    assert "illegal virtualNodeCnt" in str(info.value)
    # 0 is legal (only <0 is blocked): no virtual nodes on the ring => nothing can be assigned
    assert AllocateMessageQueueConsistentHash(0).allocate(
        "g", "CID-0", _ch_queues(4), _ch_cids(2)) == []


def test_custom_hash_function_is_injected():
    """Supports injecting a custom hash function; the strategy must really use it to build the ring."""
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
    # Same-scenario ground truth: the virtual node keys of the two cids ("CID-0-0"...)
    # all start with 'C' => same hash => when building the ring, the later one overwrites the former for the same key,
    # leaving only CID-1 on the ring, so all queues go to it. The default MD5Hash would never produce this shape.
    assert _ids(strategy.allocate("g", "CID-0", _ch_queues(4), _ch_cids(2))) == []
    assert _ids(strategy.allocate("g", "CID-1", _ch_queues(4), _ch_cids(2))) == [0, 1, 2, 3]


def test_consistent_hash_guard_returns_empty_like_other_strategies():
    """Guard semantics consistent with the other strategies: invalid inputs return empty, never raise IllegalArgumentException."""
    strategy = AllocateMessageQueueConsistentHash()
    assert strategy.allocate("g", "CID-NOT-HERE", _ch_queues(4), _ch_cids(2)) == []
    assert strategy.allocate("g", "", _ch_queues(4), _ch_cids(2)) == []
    assert strategy.allocate("g", "CID-0", [], _ch_cids(2)) == []
    assert strategy.allocate("g", "CID-0", _ch_queues(4), []) == []


# ----------------------------------------------------- Machine room strategy (MACHINE_ROOM)

def test_by_machine_room_produces_the_expected_assignment():
    """MACHINE_ROOM: 10 queues (0..4 in room1) +
    consumeridcs={room1} + 2 consumers → {0,1,4} / {2,3}.

    Remainder queues go to the **first rem** consumers (``rem > currentIndex``), so the third one lands on CID_PREFIX0.
    """
    mq_all = [MessageQueue("topic", "room1@broker-a" if i < 5 else "room2@broker-b", i)
              for i in range(10)]
    cid_all = _cids(2)
    strategy = AllocateMessageQueueByMachineRoom({"room1"})
    got = {cid: _ids(strategy.allocate("G", cid, mq_all, cid_all)) for cid in cid_all}
    assert got["CID_PREFIX0"] == [0, 1, 4]
    assert got["CID_PREFIX1"] == [2, 3]


def test_by_machine_room_only_accepts_the_two_segment_broker_name():
    """The broker name must be ``room@name``: no @ or an extra segment means no allocation."""
    cid_all = _cids(1)
    strategy = AllocateMessageQueueByMachineRoom({"room1"})
    assert strategy.allocate("G", "CID_PREFIX0",
                             [MessageQueue("topic", "room1@broker-a", 0)], cid_all) != []
    assert strategy.allocate("G", "CID_PREFIX0",
                             [MessageQueue("topic", "broker-a", 0)], cid_all) == []
    assert strategy.allocate("G", "CID_PREFIX0",
                             [MessageQueue("topic", "room1@broker@a", 0)], cid_all) == []
    # A room not in the whitelist is not allocated either
    assert _ids(strategy.allocate("G", "CID_PREFIX0",
                                  [MessageQueue("topic", "room9@b", 0)], cid_all)) == []
    # No room configured by default = nothing is allocated
    assert AllocateMessageQueueByMachineRoom().allocate(
        "G", "CID_PREFIX0", [MessageQueue("topic", "room1@b", 0)], cid_all) == []


@pytest.mark.parametrize("broker_name,expected_parts", [
    ("room1@broker-a", ["room1", "broker-a"]),
    ("room1@", ["room1"]),           # trailing empty segment dropped → 1 segment, no allocation
    ("room1@b@", ["room1", "b"]),    # still 2 segments after dropping the trailing empty → participates
    ("@room1", ["", "room1"]),       # leading empty segment kept
    ("room1@broker@a", ["room1", "broker", "a"]),
    ("@", []),
    ("broker-a", ["broker-a"]),      # no separator hit: the whole string returned as-is
    ("", [""]),                      # same as above, even when the whole string is empty
])
def test_split_drops_trailing_empty_segments(broker_name, expected_parts):
    """Splitting on the separator drops trailing empty segments (``a@b@`` → 2 segments), which native Python split does not.

    ``AllocateMessageQueueByMachineRoom`` judges validity by ``length == 2``, so this difference directly
    changes whether a queue participates in allocation — it must follow the drop-trailing-empty rule.
    """
    assert _split_dropping_trailing_empty(broker_name, "@") == expected_parts


def test_by_machine_room_uses_the_trailing_empty_split_on_the_broker_name():
    """``room1@`` does not count as two segments (no allocation); ``room1@b@`` counts as two (participates)."""
    cid_all = _cids(1)
    strategy = AllocateMessageQueueByMachineRoom({"room1"})
    assert _ids(strategy.allocate("G", "CID_PREFIX0",
                                  [MessageQueue("topic", "room1@", 0)], cid_all)) == []
    assert _ids(strategy.allocate("G", "CID_PREFIX0",
                                  [MessageQueue("topic", "room1@b@", 0)], cid_all)) == [0]
    # "@room1" is two segments, but the room is an empty string and not in the whitelist
    assert _ids(strategy.allocate("G", "CID_PREFIX0",
                                  [MessageQueue("topic", "@room1", 0)], cid_all)) == []
    # "@" splits into 0 segments
    assert _ids(strategy.allocate("G", "CID_PREFIX0",
                                  [MessageQueue("topic", "@", 0)], cid_all)) == []


# --------------------------------------------- Machine room nearby proxy (MACHINE_ROOM_NEARBY)

class _DashRoom(object):
    """Test resolver: broker ``IDCx-brokerName`` / consumer ``IDCx-CID-i`` take the segment before '-'."""

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
    """With equal room counts, each consumer can only get queues in the **same room**,
    and the union of everyone is exactly the whole set (no overlap, no omission)."""
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
    """When broker rooms outnumber consumer rooms, the queues of rooms with **no live consumer**
    must be shared among all consumers, otherwise nobody consumes them; rooms with consumers still only serve their own consumers."""
    strategy = AllocateMachineRoomNearby(AllocateMessageQueueAveragely(), _DashRoom())
    mq_all = _nearby_mq(5, 4)                       # 4 queues each in IDC1..IDC5
    cid_all = _nearby_cids(2, 3)                    # consumers only in IDC1 and IDC2
    healthy = {"IDC1", "IDC2"}
    # Queue ids repeat across rooms, so use (broker, qid) as the identity
    key = lambda mq: (mq.broker_name, mq.queue_id)  # noqa: E731
    allocated = {key(mq): cid for cid in cid_all
                 for mq in strategy.allocate("Test-C-G", cid, mq_all, cid_all)}
    assert set(allocated) == {key(mq) for mq in mq_all}, "every queue must have a consumer"
    for mq in mq_all:
        room = _DashRoom().broker_deploy_in(mq)
        if room in healthy:  # healthy rooms do not share: only same-room consumers
            assert _DashRoom().consumer_deploy_in(allocated[key(mq)]) == room, mq
        else:                # queues of empty rooms fall between the two rooms
            assert _DashRoom().consumer_deploy_in(allocated[key(mq)]) in healthy, mq
    # Same-room consumers collect all queues of their own room
    for room in healthy:
        mine = {key(mq) for mq, cid in
                ((m, c) for c in cid_all
                 for m in strategy.allocate("Test-C-G", c, mq_all, cid_all))
                if _DashRoom().consumer_deploy_in(cid) == room}
        assert mine >= {key(mq) for mq in mq_all if mq.broker_name.startswith(room)}


def test_nearby_shares_a_dead_room_queues_in_a_stable_order():
    """Two-room small scenario: 4 queues in ``IDC2`` + 2 in ``IDC1``, two consumers in ``IDC1`` →
    CID-0 gets ``[IDC1#0, IDC2#0, IDC2#1]``, CID-1 gets ``[IDC1#1, IDC2#2, IDC2#3]``.

    The order is pinned too: **same-room** queues are allocated first, then rooms
    with "no live consumers" are appended in lexicographic room-name order, so same-room queues always come first.
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
    """``get_name()`` = ``MACHINE_ROOM_NEARBY-<inner strategy name>``."""
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
    """Illegal arguments raise NullPointerException at construction time (not a rebalance-time guard; they are raised as-is)."""
    with pytest.raises(ValueError):
        AllocateMachineRoomNearby(inner, resolver)


def test_nearby_raises_when_a_room_is_unknown():
    """When the resolver reports an empty room, IllegalArgumentException must be raised: it must **not** silently return an empty list,
    which would amount to revoking the entire topic's queues; the throw is caught by rebalance, which keeps the existing assignment."""
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
    """After switching to consistent hashing, the consumer's rebalance must really use it (the strategy is not a decoration)."""
    class _Client(object):
        def get_topic_publish_info(self, topic):
            class _Info(object):
                msg_queue_list = _queues(4)
            return _Info()

        def get_topic_subscribe_info(self, topic):
            # rebalance takes the subscription info
            return _queues(4)

        def get_consumer_id_list_by_group(self, topic, consumer_group, timeout_millis=5000):
            return ["CID_PREFIX0"]

    consumer = DefaultLitePullConsumer("G_test")
    consumer.client_id = "CID_PREFIX0"
    consumer.subscription = {"TopicTest": None}
    consumer._mq_client = _Client()
    consumer.allocate_message_queue_strategy = AllocateMessageQueueConsistentHash(3)
    consumer._rebalance()
    # Single consumer + hash ring: all 4 queues land on itself
    assert _ids(sorted(consumer._assigned, key=lambda m: m.queue_id)) == [0, 1, 2, 3]
    assert consumer.allocate_message_queue_strategy.get_name() == "CONSISTENT_HASH"
