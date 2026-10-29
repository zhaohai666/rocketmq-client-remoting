# What a rebalance round owes the broker and the application depends on whether the
# assignment MOVED: the topics whose queue set changed re-share their subscription
# version, ONE immediate heartbeat round carries those versions to every broker, the
# topic cache budgets are divided over the queues this round now owns, and last the
# application's queue listener hears about exactly the topics that moved.
# A round that re-pulled the same queues owes none of that -- including the round that
# only self-heals a stalled pull loop, where the queue stays assigned so the set did
# not move.
#
# Every positive leg sits next to a control that goes red when the version is not
# re-shared, when the beat or the notification runs on every round, when the restamp is
# applied to the whole subscription table instead of the moved topics, or when the
# change detector only compares queue COUNTS (a swap that keeps the count would pay
# nothing). The assertions go through the decoded HEART_BEAT(34) body, because a stale
# version on the wire is what a real broker reacts to.
from __future__ import annotations

import time

from client.consumer import (DefaultMQPushConsumer, MessageQueueListener)
from client.mq_client import MQClientInstance
from common.message import MessageQueue
from common.subscription_data import SubscriptionData
from remoting.protocol.codes import RequestCode, ResponseCode
from remoting.protocol.heartbeat import HeartbeatData, MessageModel
from remoting.protocol.remoting_command import RemotingCommand

NS = ["127.0.0.1:9876"]
CLIENT_ID = "python-rebalance-change@1"
BROKER_A = "127.0.0.1:10911"
BROKER_B = "127.0.0.1:10921"
TOPIC_A = "PyRebalanceChangeTopicA"
TOPIC_B = "PyRebalanceChangeTopicB"
BROKER = "broker-a"
OTHER_BROKER = "broker-b"

# A wall-clock value from the past: a re-shared version is then a comparison, not a
# race against millisecond resolution, and a round that must NOT touch it leaves the
# sentinel in place.
STALE = 1_700_000_000_000


# ------------------------------------------------------------- fake broker / route


class _BeatTransport(object):
    """Answers HEART_BEAT and keeps every decoded body, so beats are counted per round."""

    def __init__(self):
        self.rounds = []

    def __call__(self, addr, request, timeout_millis=None):
        assert request.code == RequestCode.HEART_BEAT, \
            "this harness only exercises the heartbeat path: %s" % request.code
        self.rounds.append((addr, HeartbeatData.decode(request.body)))
        response = RemotingCommand()
        response.code = ResponseCode.SUCCESS
        response.version = 401
        response.ext_fields = {}
        return response

    def beat_count(self) -> int:
        return len(self.rounds)

    def sub_version(self, topic: str) -> int:
        """The version the most recent beat carried for ``topic`` (-1 when absent)."""
        for _addr, hb in reversed(self.rounds):
            for cd in hb.consumer_data_set:
                for sub in cd.subscription_data_set:
                    if sub.topic == topic:
                        return sub.sub_version
        return -1


class _Route(object):
    """The route view a rebalance round reads: broadcasting takes every queue of a topic."""

    def __init__(self):
        self.queues = {}

    def set(self, topic: str, count: int, broker_name: str = BROKER) -> None:
        self.queues[topic] = [MessageQueue(topic=topic, broker_name=broker_name, queue_id=i)
                              for i in range(count)]


def _consumer(group: str, pop: bool = False):
    """A started push consumer whose rebalance runs for real except for the network."""
    c = DefaultMQPushConsumer(group)
    transport = _BeatTransport()
    mqc = MQClientInstance(CLIENT_ID, list(NS))
    mqc.remoting_client.invoke_sync = transport
    mqc.get_all_broker_addrs = lambda: [BROKER_A, BROKER_B]
    route = _Route()
    mqc.get_topic_subscribe_info = lambda topic: list(route.queues.get(topic, []))

    c._started = True
    c.use_heartbeat_v2 = False
    c.message_model = MessageModel.BROADCASTING
    c.pop_mode = pop
    c._mq_client = mqc
    c.subscription_data = {TOPIC_A: SubscriptionData(topic=TOPIC_A, sub_string="*"),
                           TOPIC_B: SubscriptionData(topic=TOPIC_B, sub_string="*")}
    # The pull loops and the start-offset RPC are covered by other suites; the table
    # rebuild they drive is the part the change package's ORDER depends on, so it runs.
    c._queue_pull_loop = lambda mq: c._stop.wait(30)
    c._queue_pop_loop = lambda mq: c._stop.wait(30)
    c._on_queues_revoked = lambda revoked: None
    c._resolve_initial_offset = lambda client, mq: 0
    return c, transport, route


class _Listener(MessageQueueListener):
    def __init__(self):
        self.calls = []

    def message_queue_changed(self, topic, mq_all, mq_divided):
        self.calls.append((topic, len(mq_all), len(mq_divided)))

    def topics_since(self, n: int):
        return [topic for topic, _a, _d in self.calls[n:]]


def _pin(c, *topics) -> int:
    for topic in topics:
        c.subscription_data[topic].sub_version = STALE
    return STALE


def _version(c, topic: str) -> int:
    return c.subscription_data[topic].sub_version


# ------------------------------------------------------------------------ rounds


def test_unchanged_round_restamps_nothing_beats_nothing_and_notifies_nothing():
    """The 20 s rebalance re-runs on the same queues; telling the broker about a
    subscription that did not move every round is pure wire noise, so an unchanged
    round pays no version restamp, no beat and no listener call."""
    c, transport, route = _consumer("G_py_rc_unchanged")
    try:
        route.set(TOPIC_A, 2)
        route.set(TOPIC_B, 1)
        c._do_rebalance()
        stale = _pin(c, TOPIC_A, TOPIC_B)
        beats = transport.beat_count()
        listener = _Listener()
        c.message_queue_listener = listener

        c._do_rebalance()

        assert _version(c, TOPIC_A) == stale and _version(c, TOPIC_B) == stale, \
            "an unchanged assignment must not re-share any subscription version"
        assert transport.beat_count() == beats, "an unchanged round must not beat the brokers"
        assert listener.calls == [], "an unchanged round tells the listener nothing new"

        # Control for the wire assertion below: the pinned version is what the beat
        # builder reads, so a beat asked for out of band still carries it -- which
        # means a changed round has to change it.
        c._send_heartbeat_to_all_broker()
        assert transport.sub_version(TOPIC_A) == stale
    finally:
        c._stop.set()


def test_changed_round_restamps_divides_beats_once_on_every_broker():
    """One moved queue buys the whole package: the version is re-shared, the topic
    budget is divided over the queues this round now owns (2 -> 1000/2 = 500, not the
    stale 1000/1), and exactly one beat round reaches every broker of the table
    carrying that fresh version."""
    c, transport, route = _consumer("G_py_rc_changed")
    try:
        route.set(TOPIC_A, 1)
        c.pull_threshold_for_topic = 1000
        c.pull_threshold_size_for_topic = -1
        c.pull_threshold_for_queue = 1000
        c._do_rebalance()
        stale = _pin(c, TOPIC_A, TOPIC_B)
        beats = transport.beat_count()

        route.set(TOPIC_A, 2)
        c._do_rebalance()

        grown = _version(c, TOPIC_A)
        assert grown > stale, "a moved topic must re-share its subscription version"
        assert c.pull_threshold_for_queue == 500, \
            "the division is part of the same package: 1000 over the 2 queues this round owns"
        # 2 brokers x 1 round. A beat that also ran on the periodic path, or a round
        # that beat only the master, lands here as a different count.
        assert transport.beat_count() == beats + 2, \
            "the changed round owes exactly one beat per broker, got %s" % (
                [addr for addr, _hb in transport.rounds],)
        assert transport.sub_version(TOPIC_A) == grown, \
            "the immediate beat must carry the version the next pull sends"
        for _addr, hb in transport.rounds[beats:]:
            topics = {sub.topic for cd in hb.consumer_data_set
                      for sub in cd.subscription_data_set}
            assert topics == {TOPIC_A, TOPIC_B}, \
                "the beat reports the whole subscription table, on every broker"
        assert {addr for addr, _hb in transport.rounds[beats:]} == {BROKER_A, BROKER_B}, \
            "both brokers must be told about the moved topic"
        # The sibling that did not move is still on the wire -- with its OLD version,
        # which is what separates a per-topic restamp from a blanket one.
        assert transport.sub_version(TOPIC_B) == stale
    finally:
        c._stop.set()


def test_only_the_topic_that_moved_is_restamped():
    """The version is re-shared per moved topic, never over the whole subscription
    table: a sibling whose queues did not move keeps its pinned version, and the beat
    that goes out for the moved topic carries the sibling's OLD version untouched."""
    c, transport, route = _consumer("G_py_rc_per_topic")
    try:
        route.set(TOPIC_A, 1)
        route.set(TOPIC_B, 1)
        c._do_rebalance()
        stale = _pin(c, TOPIC_A, TOPIC_B)

        route.set(TOPIC_A, 2)
        c._do_rebalance()

        assert _version(c, TOPIC_A) > stale, "the moved topic must re-share its version"
        assert _version(c, TOPIC_B) == stale, \
            "an untouched topic must keep the pinned version: a blanket restamp would re-share it"
        assert transport.sub_version(TOPIC_B) == stale, \
            "the beat must carry the untouched topic's pinned version"
    finally:
        c._stop.set()


def test_listener_fires_once_per_moved_topic_with_this_rounds_share():
    """The application's queue listener is a change callback: one call per moved topic
    with the route's full queue set and this instance's share, and silence for a
    sibling that did not move."""
    c, _transport, route = _consumer("G_py_rc_notify")
    try:
        route.set(TOPIC_A, 2)
        route.set(TOPIC_B, 1)
        listener = _Listener()
        c.message_queue_listener = listener
        c._do_rebalance()

        assert [call[0] for call in listener.calls] == [TOPIC_A, TOPIC_B], \
            "the round that first assigns both topics notifies both, in topic order"
        assert listener.calls[0][1:] == (2, 2) and listener.calls[1][1:] == (1, 1), \
            "each notification carries the route's queues and this instance's share"

        n = len(listener.calls)
        c._do_rebalance()
        assert listener.topics_since(n) == [], "an unchanged round notifies nothing"

        route.set(TOPIC_A, 3)
        n = len(listener.calls)
        c._do_rebalance()
        assert listener.topics_since(n) == [TOPIC_A], \
            "only the topic whose queue set moved may call back"
        assert listener.calls[-1][1:] == (3, 3), "the callback must show the new share"
    finally:
        c._stop.set()


def test_a_same_count_swap_is_a_change():
    """Losing one queue and gaining another keeps the totals identical. The set moved,
    so which buffers exist changed: the count comparison is not enough."""
    c, transport, route = _consumer("G_py_rc_swap")
    try:
        route.set(TOPIC_A, 2)
        c._do_rebalance()
        stale = _pin(c, TOPIC_A)
        beats = transport.beat_count()

        route.set(TOPIC_A, 2, broker_name=OTHER_BROKER)
        c._do_rebalance()

        assert _version(c, TOPIC_A) > stale, "a broker-name swap of the same size moved the set"
        assert transport.beat_count() == beats + 2, "the swapped round owes one beat per broker"
    finally:
        c._stop.set()


def test_self_heal_round_spends_no_beat_and_no_notification():
    """A stalled loop is healed by the next round, but the queue was assigned all
    along: the round rebuilds the pull state and stops there. A broker beat per
    repaired loop would put the subscriptions of an unchanged consumer on the wire
    every few seconds."""
    c, transport, route = _consumer("G_py_rc_selfheal")
    try:
        route.set(TOPIC_A, 1)
        listener = _Listener()
        c.message_queue_listener = listener
        c._do_rebalance()
        key = c._mq_key(MessageQueue(topic=TOPIC_A, broker_name=BROKER, queue_id=0))
        assert key in c._mq_map, "the fixture needs live pull state to have something to heal"
        c._last_pull_table[key] = time.time() - 200.0
        stale = _pin(c, TOPIC_A)
        beats = transport.beat_count()
        calls = len(listener.calls)

        c._do_rebalance()

        assert key in c._mq_map, "the stalled queue must be rebuilt, or the round never healed it"
        assert transport.beat_count() == beats, "a self-heal round must not beat the brokers"
        assert _version(c, TOPIC_A) == stale, "the queue set did not move, so nothing re-shares"
        assert len(listener.calls) == calls, "a self-heal round notifies nothing"
    finally:
        c._stop.set()


def test_pop_round_pays_the_package_once():
    """POP keeps its per-queue state in another table and syncs it in its own loop, but
    the debt is the same and is paid once: an unchanged POP round pays nothing, a moved
    one restamps, beats and notifies exactly once."""
    c, transport, route = _consumer("G_py_rc_pop", pop=True)
    try:
        route.set(TOPIC_A, 1)
        listener = _Listener()
        c.message_queue_listener = listener
        c._do_rebalance()
        stale = _pin(c, TOPIC_A)
        beats = transport.beat_count()
        calls = len(listener.calls)

        c._do_rebalance()
        assert transport.beat_count() == beats, "an unchanged POP round must not beat"
        assert _version(c, TOPIC_A) == stale, "an unchanged POP round must not restamp"
        assert len(listener.calls) == calls, "an unchanged POP round must not notify"

        route.set(TOPIC_A, 2)
        beats = transport.beat_count()
        c._do_rebalance()
        assert _version(c, TOPIC_A) > stale, "the moved POP round re-shares the version"
        assert transport.beat_count() == beats + 2, "the moved POP round beats once per broker"
        assert listener.topics_since(calls) == [TOPIC_A], "and notifies the moved topic once"
    finally:
        c._stop.set()


def test_no_listener_installed_still_pays_the_broker_side():
    """The broker's view is not optional: without an application listener the round
    still re-shares the version and beats, so a missing callback must not short-circuit
    the package."""
    c, transport, route = _consumer("G_py_rc_no_listener")
    try:
        route.set(TOPIC_A, 1)
        c._do_rebalance()
        stale = _pin(c, TOPIC_A)
        beats = transport.beat_count()
        assert c.message_queue_listener is None

        route.set(TOPIC_A, 2)
        c._do_rebalance()

        assert _version(c, TOPIC_A) > stale
        assert transport.beat_count() == beats + 2
    finally:
        c._stop.set()


def test_pop_round_never_waits_on_a_pull_offset():
    """POP names no start offset -- initMode on the request carries it -- so a queue
    whose offset store cannot be read still gets popped this round. Dropping it would
    park the queue until the store recovers, even though POP never asked it anything."""
    c, _transport, route = _consumer("G_py_rc_pop_offset", pop=True)
    try:
        route.set(TOPIC_A, 2)

        def unreadable(_client, mq):
            raise RuntimeError("offset store unreadable for %s" % mq)

        c._resolve_initial_offset = unreadable
        c._do_rebalance()

        keys = {c._mq_key(mq) for mq in c._assigned_queues()}
        assert keys == {c._mq_key(MessageQueue(topic=TOPIC_A, broker_name=BROKER, queue_id=i))
                        for i in range(2)}, "a POP round keeps every queue it was assigned"
        assert c._pop_queues, "and starts a POP loop for each of them"
    finally:
        c._stop.set()


def test_pull_round_drops_a_queue_it_cannot_place():
    """Control for the POP leg: on the pull path the first cursor is the start
    position, so a queue whose store read fails is skipped for the round instead of
    being pulled from a guessed offset."""
    c, _transport, route = _consumer("G_py_rc_pull_offset")
    try:
        route.set(TOPIC_A, 2)

        def unreadable(_client, mq):
            raise RuntimeError("offset store unreadable for %s" % mq)

        c._resolve_initial_offset = unreadable
        c._do_rebalance()

        assert c._assigned_queues() == [], "an unplaceable queue must not be pulled"
        assert not c._queue_threads, "and must get no pull loop"
    finally:
        c._stop.set()
