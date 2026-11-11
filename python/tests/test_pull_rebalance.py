# -*- coding: utf-8 -*-
"""Pull-side rebalance (Java `DefaultMQPullConsumer.enableRebalance`).

Java's pull consumer is not the "no rebalance" shape this port used to have: the shared
`RebalanceService` thread calls `mqClientFactory.doRebalance()` every
`rocketmq.client.rebalance.waitInterval` (20s), which walks the registered consumers and
calls `tryRebalance()`. That method gates on `enableRebalance` (default **true**) and then
runs `rebalanceImpl.doRebalance(false)` -> allocate -> `messageQueueChanged(topic, mqAll,
mqDivided)`.

The assertions below pin the three things that are easy to get wrong:

* `enableRebalance == false` must still report **balanced** (`tryRebalance` returns true at
  DefaultMQPullConsumerImpl:398-402) -- reporting "unbalanced" would make the loop retry at
  minInterval forever and burn a round-trip per second for nothing.
* the listener fires **only when the allocation changed**, and receives
  `(topic, mqAll, mqDivided)` -- the same three arguments RebalancePullImpl:44-53 passes.
* a throwing listener must not escape: Java logs and swallows it (RebalancePullImpl:49-51)
  so one bad callback cannot kill the loop and freeze every allocation.
"""
from __future__ import annotations

from common.message import MessageQueue
from remoting.protocol.heartbeat import MessageModel
from client.consumer import DefaultMQPullConsumer


TOPIC = "TopicPullReb"


def _queues(n, topic=TOPIC):
    return [MessageQueue(topic, "broker-a", i) for i in range(n)]


class _Client:
    """Minimal stand-in for MQClientInstance: subscribe info + consumer id list."""

    def __init__(self, subscribe, cid_list):
        self._subscribe = list(subscribe or [])
        self._cid_list = cid_list

    def get_topic_subscribe_info(self, topic):
        return [q for q in self._subscribe if q.topic == topic]

    def get_consumer_id_list_by_group(self, topic, consumer_group, timeout_millis=5000):
        return None if self._cid_list is None else list(self._cid_list)


class _Recorder:
    def __init__(self):
        self.calls = []

    def message_queue_changed(self, topic, mq_all, mq_divided):
        self.calls.append((topic, {(q.broker_name, q.queue_id) for q in mq_all},
                           {(q.broker_name, q.queue_id) for q in mq_divided}))


class _Boom:
    def message_queue_changed(self, topic, mq_all, mq_divided):
        raise RuntimeError("listener exploded")


class _CountingStrategy:
    """Wraps the real strategy so a test can prove it was (not) consulted."""

    def __init__(self, inner):
        self.inner = inner
        self.calls = 0

    @property
    def name(self):
        return getattr(self.inner, "name", "counting")

    def allocate(self, consumer_group, current_cid, mq_all, cid_all):
        self.calls += 1
        return self.inner.allocate(consumer_group, current_cid, mq_all, cid_all)


def _consumer(mq_all, cid_all, model=MessageModel.CLUSTERING, client_id="cid-A",
              listener=None):
    c = DefaultMQPullConsumer("PG_PullReb")
    c.client_id = client_id
    c.message_model = model
    c._mq_client = _Client(mq_all, cid_all)
    c._started = True
    c.register_topics.add(TOPIC)
    if listener is not None:
        c.set_message_queue_listener(listener)
    return c


class TestSwitch:
    def test_enable_rebalance_defaults_to_java_true(self):
        # Java DefaultMQPullConsumer.java:97 `private boolean enableRebalance = true`.
        assert DefaultMQPullConsumer("PG_PullReb").is_enable_rebalance() is True

    def test_setter_round_trips(self):
        c = DefaultMQPullConsumer("PG_PullReb")
        c.set_enable_rebalance(False)
        assert c.is_enable_rebalance() is False
        c.set_enable_rebalance(True)
        assert c.is_enable_rebalance() is True

    def test_wait_and_min_interval_are_configurable_and_clamped(self):
        c = DefaultMQPullConsumer("PG_PullReb")
        assert c.rebalance_wait_millis == 20000
        assert c.rebalance_min_interval_millis == 1000
        c.set_rebalance_wait_millis(0)
        c.set_rebalance_min_interval_millis(-5)
        assert c.rebalance_wait_millis == 1, "a zero period would spin the loop"
        assert c.rebalance_min_interval_millis == 1


class TestDisabledRebalance:
    def test_disabled_reports_balanced_and_never_allocates(self):
        """Java tryRebalance returns **true** when the switch is off (:398-402)."""
        c = _consumer(_queues(4), ["cid-A", "cid-B"])
        strategy = _CountingStrategy(c.allocate_message_queue_strategy)
        c.allocate_message_queue_strategy = strategy
        c.set_enable_rebalance(False)

        assert c._do_rebalance() is True, "off must look balanced, not endless-unbalanced"
        assert strategy.calls == 0, "no allocation may run while the switch is off"
        assert c.get_rebalance_table() == {}, "nothing may be cached either"

    def test_disabled_keeps_fetch_in_balance_on_the_spot(self):
        c = _consumer(_queues(4), ["cid-A", "cid-B"])
        c._rebalance_table[TOPIC] = []  # a stale table must be ignored while off
        c.set_enable_rebalance(False)
        got = c.fetch_message_queues_in_balance(TOPIC)
        assert [q.queue_id for q in got] == [0, 1], "falls back to on-the-spot averaging"


class TestBroadcasting:
    def test_first_round_notifies_with_all_queues(self):
        rec = _Recorder()
        c = _consumer(_queues(3), ["cid-A"], model=MessageModel.BROADCASTING,
                      listener=rec)
        assert c._do_rebalance() is False, "the first round changes the empty table"
        assert len(rec.calls) == 1
        topic, mq_all, divided = rec.calls[0]
        assert topic == TOPIC
        assert mq_all == divided == {(("broker-a"), 0), (("broker-a"), 1), (("broker-a"), 2)}

    def test_second_round_is_balanced_and_silent(self):
        rec = _Recorder()
        c = _consumer(_queues(3), ["cid-A"], model=MessageModel.BROADCASTING,
                      listener=rec)
        c._do_rebalance()
        assert c._do_rebalance() is True, "nothing changed, so the topic is balanced"
        assert len(rec.calls) == 1, "messageQueueChanged fires only on a real change"


class TestClustering:
    def test_this_instance_takes_its_share(self):
        c = _consumer(_queues(4), ["cid-A", "cid-B"])
        c._do_rebalance()
        assert sorted(q.queue_id for q in c.get_rebalance_table()[TOPIC]) == [0, 1], \
            "averagely: A (sorted first) takes the first half"

    def test_two_instances_get_disjoint_shares(self):
        a = _consumer(_queues(5), ["cid-A", "cid-B"], client_id="cid-A")
        b = _consumer(_queues(5), ["cid-A", "cid-B"], client_id="cid-B")
        a._do_rebalance()
        b._do_rebalance()
        sa = {(q.broker_name, q.queue_id) for q in a.get_rebalance_table()[TOPIC]}
        sb = {(q.broker_name, q.queue_id) for q in b.get_rebalance_table()[TOPIC]}
        assert sa & sb == set(), "the same queue must never be owned twice"
        assert sa | sb == {("broker-a", i) for i in range(5)}, "together they cover all"

    def test_new_queue_triggers_exactly_one_notification(self):
        rec = _Recorder()
        c = _consumer(_queues(2), ["cid-A"], listener=rec)
        c._do_rebalance()
        assert len(rec.calls) == 1

        c._mq_client._subscribe = _queues(3)  # a queue was added
        assert c._do_rebalance() is False
        assert len(rec.calls) == 2
        _, mq_all, divided = rec.calls[1]
        assert mq_all == {("broker-a", 0), ("broker-a", 1), ("broker-a", 2)}
        assert divided == {("broker-a", 0), ("broker-a", 1), ("broker-a", 2)}

    def test_missing_consumer_id_list_is_balanced_and_silent(self):
        """Java (:300-302) only warns; it does not notify and leaves balanced at true."""
        rec = _Recorder()
        c = _consumer(_queues(3), None, listener=rec)
        assert c._do_rebalance() is True
        assert rec.calls == [], "no consumer list -> say nothing, keep the old allocation"
        assert c.get_rebalance_table() == {}

    def test_first_round_with_an_empty_share_is_silent_and_balanced(self):
        """Java's ``updateProcessQueueTableInRebalance`` counts only table additions/removals
        as a change: "nothing -> nothing" is no change, so an empty opening share fires no
        callback and the round counts as balanced (the cpp port pins the same rule)."""
        class _EmptyStrategy:
            name = "empty"

            def allocate(self, consumer_group, current_cid, mq_all, cid_all):
                return []

        rec = _Recorder()
        c = _consumer(_queues(3), ["cid-A", "cid-B"], listener=rec)
        real = c.allocate_message_queue_strategy
        c.allocate_message_queue_strategy = _EmptyStrategy()

        assert c._do_rebalance() is True, "nothing -> nothing is a balanced round"
        assert rec.calls == [], "an empty opening share must not fire the listener"
        assert c.get_rebalance_table()[TOPIC] == []

        # The share appearing later IS a change (the table grew from nothing): fires once.
        c.allocate_message_queue_strategy = real
        assert c._do_rebalance() is False
        assert len(rec.calls) == 1
        _, mq_all, divided = rec.calls[0]
        assert len(mq_all) == 3 and len(divided) == 2, \
            "averagely: A (sorted first) takes the first half of 3 queues among 2 clients"

    def test_vanished_topic_clears_the_table_and_notifies_empty(self):
        """Java (:286-294) notifies with two empty sets when the route disappears.

        It then ``break``s, so ``balanced`` stays **true** -- the round must not report
        "unbalanced" and spin at minInterval while the topic is merely absent.
        """
        rec = _Recorder()
        c = _consumer(_queues(2), ["cid-A"], listener=rec)
        c._do_rebalance()
        assert len(rec.calls) == 1

        c._mq_client._subscribe = []
        assert c._do_rebalance() is True, "an absent topic is not 'unbalanced'"
        assert c.get_rebalance_table()[TOPIC] == []
        _, mq_all, divided = rec.calls[1]
        assert mq_all == set() and divided == set()


class TestListenerSafety:
    def test_a_throwing_listener_does_not_escape(self):
        c = _consumer(_queues(2), ["cid-A"], listener=_Boom())
        # Java swallows it (RebalancePullImpl:49-51); the call must simply return.
        assert c._do_rebalance() is False
        assert len(c.get_rebalance_table()[TOPIC]) == 2, "the table is still updated"

    def test_same_queues_in_another_order_do_not_notify(self):
        rec = _Recorder()
        c = _consumer(_queues(3), ["cid-A"], listener=rec)
        c._do_rebalance()
        assert len(rec.calls) == 1

        c._mq_client._subscribe = list(reversed(_queues(3)))
        assert c._do_rebalance() is True, "same set, different order is not a change"
        assert len(rec.calls) == 1


class TestFetchInBalanceUsesTheTable:
    def test_table_wins_over_on_the_spot_computation(self):
        c = _consumer(_queues(4), ["cid-A", "cid-B"])
        c._rebalance_table[TOPIC] = [MessageQueue(TOPIC, "broker-a", 3)]
        got = c.fetch_message_queues_in_balance(TOPIC)
        assert [q.queue_id for q in got] == [3], \
            "the background rebalance's product is authoritative"

    def test_table_answer_is_sorted_and_isolated_from_the_caller(self):
        c = _consumer(_queues(4), ["cid-A"])
        c._rebalance_table[TOPIC] = [MessageQueue(TOPIC, "broker-b", 1),
                                     MessageQueue(TOPIC, "broker-a", 2)]
        got = c.fetch_message_queues_in_balance(TOPIC)
        assert [(q.broker_name, q.queue_id) for q in got] == [
            ("broker-a", 2), ("broker-b", 1)]
        got.clear()
        assert len(c.get_rebalance_table()[TOPIC]) == 2, "callers must not mutate the table"

    def test_get_rebalance_table_returns_a_copy(self):
        c = _consumer(_queues(2), ["cid-A"])
        c._do_rebalance()
        snapshot = c.get_rebalance_table()
        snapshot[TOPIC].clear()
        assert len(c.get_rebalance_table()[TOPIC]) == 2

    def test_unfilled_topic_still_computes_on_the_spot(self):
        c = _consumer(_queues(4), ["cid-A", "cid-B"])
        assert c.get_rebalance_table() == {}
        assert [q.queue_id for q in c.fetch_message_queues_in_balance(TOPIC)] == [0, 1]


class TestRegisterMessageQueueListener:
    """Java DefaultMQPullConsumer.java:328-335 registers topic + listener in one call.

    Without the topic in ``register_topics`` the rebalance loop never visits it, so a
    listener registered by the setter alone would silently never fire -- that is the whole
    reason Java has this combined method.
    """

    def test_registers_the_topic_and_the_listener(self):
        c = DefaultMQPullConsumer("PG_PullReb")
        rec = _Recorder()
        c.register_message_queue_listener(TOPIC, rec)
        assert TOPIC in c.get_register_topics()
        assert c.message_queue_listener is rec

    def test_null_listener_keeps_the_current_one(self):
        c = DefaultMQPullConsumer("PG_PullReb")
        rec = _Recorder()
        c.set_message_queue_listener(rec)
        c.register_message_queue_listener(TOPIC, None)
        assert TOPIC in c.get_register_topics(), "the topic is still registered"
        assert c.message_queue_listener is rec, "Java only overwrites when non-null"

    def test_blank_topic_is_ignored(self):
        c = DefaultMQPullConsumer("PG_PullReb")
        rec = _Recorder()
        c.register_message_queue_listener("   ", rec)
        assert c.get_register_topics() == set()
        assert c.message_queue_listener is None, "Java ignores a blank topic entirely"
