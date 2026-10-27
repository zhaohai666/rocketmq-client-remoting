# -*- coding: utf-8 -*-
"""Offline regression for the pre-delivery and re-schedule rules of the concurrent/POP paths.

Three rules are locked here, each with the paired control that goes red when the rule is
reverted, moved, or gated on the wrong field:

* retry-budget pre-drop (POP delivery only): a row whose reconsume count is **strictly greater**
  than the effective budget (the knob when it is not -1, else 16) is dropped inside the POP
  pre-delivery step and acked like every other filtered row. The pull path must **never**
  pre-drop on the budget: it advances its offset, so a pre-dropped pull row would vanish
  without a send-back.
* retry-topic unwrapping before delivery: ``%RETRY%<group>+Topic`` / ``%RETRY%<group>_Topic``
  are rewritten to ``Topic`` at the concurrent delivery entries only; a bare ``%RETRY%<group>``,
  an ordinary topic, and the orderly delivery path must not be touched.
* the POP round's re-schedule is judged per status **after** the pre-delivery step: FOUND-with-
  remaining waits ``pull_interval`` only when it is positive, a FOUND batch emptied by filtering
  re-pops immediately, NO_NEW_MSG/POLLING_NOT_FOUND re-pop immediately, POLLING_FULL and any
  other status park 3000 ms -- and TPS/wait-ack count the kept rows, not the popped ones.

The transport is an in-process double (same style as ``test_pop_consumer``); the loop clock is
swapped so every park is observed as a recorded value instead of a wall-clock window.
"""
from __future__ import annotations

import threading
import time

import pytest

from client import consumer as consumer_mod
from client.consumer import (DefaultMQPushConsumer, PopProcessQueue,
                             PULL_TIME_DELAY_WHEN_BROKER_FLOW_CONTROL,
                             filter_messages_for_delivery)
from client.consumer_result import (ConsumeConcurrentlyStatus, ConsumeOrderlyStatus,
                                    MessageListenerOrderly,
                                    PopResult, PopStatus, PullResult, PullStatus)
from client.consumer_stats import ConsumerStatsManager
from client.exception import MQBrokerException
from common.message import MessageExt, MessageQueue
from common.message_const import MessageConst
from common.subscription_data import FilterAPI, SubscriptionData
from remoting.protocol.codes import ResponseCode
from remoting.protocol.namespace_util import NamespaceUtil

GROUP = "GID_GuardUnitTest"
TOPIC = "GuardUnitTestTopic"
BROKER = "broker-a"


def ck(queue_id: int = 0, offset: int = 0, retry: str = "0",
       pop_time: int = None, invisible: int = 60000, revive_qid: int = 0,
       ck_offset: int = 0) -> str:
    """Assemble a POP_CK; the retry segment decides the ack target rebuilt by getRealTopic."""
    if pop_time is None:
        pop_time = int(time.time() * 1000)
    seg = [str(ck_offset), str(pop_time), str(invisible), str(revive_qid),
           retry, BROKER, str(queue_id), str(offset)]
    return " ".join(seg)


def msg(topic: str = TOPIC, queue_id: int = 0, queue_offset: int = 0,
        pop_ck: str = None, reconsume_times: int = 0, tag: str = None) -> MessageExt:
    m = MessageExt(topic=topic, body=b"body")
    m.queue_id = queue_id
    m.queue_offset = queue_offset
    m.reconsume_times = reconsume_times
    if pop_ck is not None:
        m.properties[MessageConst.PROPERTY_POP_CK] = pop_ck
    if tag is not None:
        m.properties[MessageConst.PROPERTY_TAGS] = tag
    return m


def push_consumer(**kw) -> DefaultMQPushConsumer:
    c = DefaultMQPushConsumer(GROUP)
    for k, v in kw.items():
        setattr(c, k, v)
    return c


class Transport:
    """MQClientInstance double: scripts POP/pull rounds and records everything the client
    would send to the broker (offline tests cannot network)."""

    def __init__(self, pop_script=(), pull_script=()):
        self.acks = []           # (topic, queue_id, queue_offset) of every ACK_MESSAGE
        self.invisible_ms = []   # (topic, offset, invisible_time) of every CHANGE_INVISIBLE
        self._pop_script = list(pop_script)
        self._pull_script = list(pull_script)
        # Replaced by the consumer's own stop event by the round drivers below: once the
        # script is consumed the loop must end instead of hot-spinning on empty rounds.
        self.stop = None

    # ---- round scripting -------------------------------------------------
    def pop_message(self, group, topic, queue_id, **kw):
        # A real network leg takes ms; keeping a small real sleep here makes "RT recorded"
        # and "RT recorded as a rounded 0" distinguishable through the swapped clock.
        time.sleep(0.02)
        if self._pop_script:
            return self._pop_script.pop(0)
        self.stop.set()
        return PopResult(PopStatus.POLLING_NOT_FOUND)

    def pull_message(self, group, mq, offset, *args, **kw):
        if self._pull_script:
            return self._pull_script.pop(0)
        self.stop.set()
        return PullResult(PullStatus.NO_NEW_MSG, next_begin_offset=offset + 10)

    # ---- outgoing ack / invisible-time ------------------------------------
    def pop_ack_addr(self, broker_name, topic, queue_id):
        return "127.0.0.1:10911"

    def ack_message(self, group, topic, queue_id, extra_info, offset, **kw):
        self.acks.append((topic, queue_id, offset))

    def change_invisible_time(self, group, topic, queue_id, extra_info, offset,
                              invisible_time, **kw):
        self.invisible_ms.append((topic, offset, invisible_time))


class SwappedClock:
    """Module-level clock swap for the consumer module: every ``time.sleep`` in the loops is
    recorded instead of waited, everything else passes through to the real module."""

    def __init__(self):
        self.delays = []

    def __getattr__(self, name):
        return getattr(time, name)

    def sleep(self, seconds):
        self.delays.append(seconds)


def _run_pop_rounds(monkeypatch, c, pop_script, sub=None):
    """Drive the real ``_queue_pop_loop`` one queue; returns (pq, delivered, transport, clock)."""
    mq = MessageQueue(TOPIC, BROKER, 0)
    key = c._mq_key(mq)
    c.pop_mode = True
    c._started = True
    c._mq_map[key] = mq
    c._pop_queues[key] = PopProcessQueue()
    c.subscription_data[mq.topic] = sub or SubscriptionData(topic=mq.topic, sub_string="*")
    if c._stats_manager is None:
        c._stats_manager = ConsumerStatsManager()
    transport = Transport(pop_script=pop_script)
    transport.stop = c._stop
    c._mq_client = transport
    delivered = []
    c._submit_pop_consume_request = lambda msgs, pq, m: delivered.append(msgs)
    clock = SwappedClock()
    monkeypatch.setattr(consumer_mod, "time", clock)
    thread = threading.Thread(target=c._queue_pop_loop, args=(mq,))
    c._queue_threads[key] = thread
    thread.start()
    thread.join(10)
    assert not thread.is_alive(), "the POP loop did not exit after the script was consumed"
    return c._pop_queues[key], delivered, transport, clock


def _run_pull_rounds(monkeypatch, c, pull_script, sub=None):
    """Drive the real ``_queue_pull_loop`` one queue; returns (pending, transport, clock)."""
    mq = MessageQueue(TOPIC, BROKER, 0)
    key = c._mq_key(mq)
    c._started = True
    c._mq_map[key] = mq
    c._offset_table[key] = 0
    c.subscription_data[mq.topic] = sub or SubscriptionData(topic=mq.topic, sub_string="*")
    if c._stats_manager is None:
        c._stats_manager = ConsumerStatsManager()
    transport = Transport(pull_script=pull_script)
    transport.stop = c._stop
    c._mq_client = transport
    clock = SwappedClock()
    monkeypatch.setattr(consumer_mod, "time", clock)
    thread = threading.Thread(target=c._queue_pull_loop, args=(mq,))
    c._queue_threads[key] = thread
    thread.start()
    thread.join(10)
    assert not thread.is_alive(), "the pull loop did not exit after the script was consumed"
    return list(c._pending.get(key) or ()), transport, clock


def _pull_result(msgs):
    return PullResult(PullStatus.FOUND, next_begin_offset=10, min_offset=0, max_offset=10,
                      msg_found_list=msgs)


def _stats_items(c):
    stats = c._stats_manager
    key = "%s@%s" % (TOPIC, GROUP)
    return (stats.topic_and_group_pull_rt.find(key),
            stats.topic_and_group_pull_tps.find(key))


# ============================================================ #117: retry-budget pre-drop, POP only


class TestPopRetryBudgetPreDrop:
    def test_budget_drops_strictly_past_the_limit_and_acks_the_drop(self, monkeypatch):
        """knob=2: reconsumeTimes 2 is delivered, 3 is dropped AND acked on the transport."""
        c = push_consumer(max_reconsume_times=2)
        at_limit = msg(queue_offset=2, reconsume_times=2, pop_ck=ck(offset=2))
        over = msg(queue_offset=3, reconsume_times=3, pop_ck=ck(offset=3))
        fresh = msg(queue_offset=1, reconsume_times=0, pop_ck=ck(offset=1))
        pq, delivered, transport, _clock = _run_pop_rounds(
            monkeypatch, c, [PopResult(PopStatus.FOUND, [fresh, at_limit, over])])
        offsets = sorted(m.queue_offset for batch in delivered for m in batch)
        assert offsets == [1, 2], "the row exactly at the budget must be delivered"
        assert transport.acks == [(TOPIC, 0, 3)], "one past the budget must reach the fake transport as an ack"
        assert pq.wait_ack_count() == 2, "only the delivered rows count as wait-ack debt"

    def test_budget_is_not_triggered_at_or_under_the_knob(self, monkeypatch):
        """Paired control for the case above: raising the knob to 3 must deliver the same rows
        and send no ack -- red if the comparison becomes ">=", the knob is ignored, or the ack
        fires unconditionally."""
        c = push_consumer(max_reconsume_times=3)
        rows = [msg(queue_offset=i, reconsume_times=i, pop_ck=ck(offset=i)) for i in (1, 2, 3)]
        pq, delivered, transport, _clock = _run_pop_rounds(
            monkeypatch, c, [PopResult(PopStatus.FOUND, rows)])
        assert sorted(m.queue_offset for batch in delivered for m in batch) == [1, 2, 3]
        assert transport.acks == []
        assert pq.wait_ack_count() == 3

    def test_knob_minus_one_falls_back_to_sixteen(self, monkeypatch):
        """The effective budget with the knob at its -1 default is 16: 16 delivered, 17 dropped."""
        c = push_consumer()  # max_reconsume_times default -1
        rows = [msg(queue_offset=16, reconsume_times=16, pop_ck=ck(offset=16)),
                msg(queue_offset=17, reconsume_times=17, pop_ck=ck(offset=17))]
        _pq, delivered, transport, _clock = _run_pop_rounds(
            monkeypatch, c, [PopResult(PopStatus.FOUND, rows)])
        assert [m.queue_offset for batch in delivered for m in batch] == [16]
        assert transport.acks == [(TOPIC, 0, 17)]

    def test_budget_drop_acks_alongside_filter_drop_in_the_same_round(self, monkeypatch):
        """A tag-filter drop and a budget drop in one round are acked identically; TPS counts
        what remains (1 of 3), and the surviving row reaches the submit step."""
        c = push_consumer(max_reconsume_times=1)
        sub = FilterAPI.build_subscription_data(TOPIC, "TagA")
        kept = msg(queue_offset=1, reconsume_times=0, tag="TagA", pop_ck=ck(offset=1))
        tag_dropped = msg(queue_offset=2, reconsume_times=0, tag="TagB", pop_ck=ck(offset=2))
        budget_dropped = msg(queue_offset=3, reconsume_times=9, tag="TagA", pop_ck=ck(offset=3))
        pq, delivered, transport, _clock = _run_pop_rounds(
            monkeypatch, c, [PopResult(PopStatus.FOUND, [kept, tag_dropped, budget_dropped])],
            sub=sub)
        assert [m.queue_offset for batch in delivered for m in batch] == [1]
        assert sorted(offset for _t, _q, offset in transport.acks) == [2, 3]
        assert pq.wait_ack_count() == 1
        _rt, tps = _stats_items(c)
        assert tps is not None and tps.times == 1 and tps.value == 1

    def test_pull_path_never_pre_drops_on_the_budget(self, monkeypatch):
        """The identical rows on the pull path are delivered unchanged: a pre-dropped pull row
        would vanish without a send-back. One row sits past even the default 16 budget, so red on
        any mutation that makes the budget pass non-opt-in or move it into the shared helper."""
        c = push_consumer(max_reconsume_times=2)
        rows = [msg(queue_offset=1, reconsume_times=0),
                msg(queue_offset=2, reconsume_times=2),
                msg(queue_offset=3, reconsume_times=3),
                msg(queue_offset=17, reconsume_times=17)]
        pending, transport, _clock = _run_pull_rounds(
            monkeypatch, c, [_pull_result(list(rows))])
        assert [m.queue_offset for m in pending] == [1, 2, 3, 17]
        assert [m.reconsume_times for m in pending] == [0, 2, 3, 17]
        assert transport.acks == [], "the pull path has no ack step and must not gain one"

    def test_shared_helper_budget_is_opt_in(self):
        """The helper drops over-budget rows only when the budget argument is supplied."""
        rows = [msg(queue_offset=1, reconsume_times=1), msg(queue_offset=2, reconsume_times=2),
                msg(queue_offset=17, reconsume_times=17)]
        assert [m.queue_offset for m in filter_messages_for_delivery(
            GROUP, [], MessageQueue(TOPIC, BROKER, 0), None, rows)] == [1, 2, 17]
        assert [m.queue_offset for m in filter_messages_for_delivery(
            GROUP, [], MessageQueue(TOPIC, BROKER, 0), None, list(rows),
            retry_budget=1)] == [1]


# ============================================================ #118: retry-topic normalisation


class TestRetryTopicNormalisation:
    def _normalize(self, topics, group=GROUP, namespace=""):
        c = DefaultMQPushConsumer(group)
        c.namespace = namespace
        if namespace:
            c.consumer_group = NamespaceUtil.wrap_namespace(namespace, group)
        rows = [MessageExt(topic=t, body=b"x") for t in topics]
        c._normalize_retry_topic(rows)
        return [m.topic for m in rows]

    def test_both_wire_spellings_of_this_group_are_rewritten(self):
        """`_` and `+` wrappers are unwrapped; the bare retry queue, an ordinary topic, another
        group's wrapper and a wrapper with nothing behind the separator are left exactly as they
        arrived -- cutting an empty tail would hand the listener an empty topic."""
        got = self._normalize(["%%RETRY%%%s+Topic" % GROUP,
                               "%%RETRY%%%s_Topic" % GROUP,
                               "%%RETRY%%%s" % GROUP,
                               TOPIC,
                               "%%RETRY%%%s_Topic" % "OtherGID",
                               "%%RETRY%%%s+" % GROUP,
                               "%%RETRY%%%s_" % GROUP])
        assert got == ["Topic", "Topic",
                       "%%RETRY%%%s" % GROUP, TOPIC,
                       "%%RETRY%%%s_Topic" % "OtherGID",
                       "%%RETRY%%%s+" % GROUP,
                       "%%RETRY%%%s_" % GROUP]

    def test_wrapper_keeps_everything_after_the_separator(self):
        """The rewrite takes the whole tail, so a topic that itself contains the other separator
        survives; and the `+` form is read first, which is the only unambiguous boundary when a
        topic contains both."""
        assert self._normalize(["%%RETRY%%%s+Topic_Extra" % GROUP]) == ["Topic_Extra"]
        assert self._normalize(["%%RETRY%%%s_Topic_Extra" % GROUP]) == ["Topic_Extra"]

    def test_the_group_part_decides_which_wrapper_is_cut(self):
        """A wrapper is only recognised when its group segment names this consumer -- with or
        without the namespace prefix -- so the namespaced wrapper, the bare-group wrapper and an
        ordinary topic resolve, while a wrapper naming neither and a bare retry queue pass through.
        The namespace left on a cut remainder is stripped one step later by
        ``_reset_retry_topic_and_namespace``, which is not this step's business."""
        got = self._normalize(["%%RETRY%%NS%%%s_Topic" % GROUP,
                               "%%RETRY%%%s_NS%%Topic" % GROUP,
                               "%%RETRY%%NS%%%s" % GROUP,
                               "%%RETRY%%%s_Topic" % "OtherGID",
                               "NS%%Topic"], namespace="NS")
        assert got == ["Topic", "NS%Topic",
                       "%%RETRY%%NS%%%s" % GROUP,
                       "%%RETRY%%%s_Topic" % "OtherGID",
                       "NS%%Topic"]

    def test_pop_concurrent_delivery_entry_normalizes_before_the_listener(self):
        """The POP concurrent entry rewrites the wire topic and the settled ack is rebuilt
        from the POP_CK retry flag, so the ack still targets the retry queue."""
        c = push_consumer()
        c._started = True
        transport = Transport()
        c._mq_client = transport
        seen = []
        c.message_listener = _CaptureListener(seen, ConsumeConcurrentlyStatus.CONSUME_SUCCESS)
        m = msg(topic="%%RETRY%%%s+Topic" % GROUP, pop_ck=ck(offset=5, retry="2"))
        pq = PopProcessQueue()
        pq.inc_found_msg(1)
        c._consume_pop_batch([m], pq, MessageQueue(TOPIC, BROKER, 0))
        assert [t for t in seen[0]] == ["Topic"], "the listener must see the business topic"
        assert transport.acks == [("%%RETRY%%%s+Topic" % GROUP, 0, 5)], \
            "the ack target is rebuilt from the CK retry flag, not the rewritten topic"

    def test_classic_concurrent_delivery_entry_normalizes_pull_rows_untouched(self):
        """The classic concurrent entry rewrites a wrapper row and passes a bare
        ``%RETRY%<group>`` row plus an ordinary topic through unchanged."""
        c = push_consumer()
        seen = []
        listener = _CaptureListener(seen, ConsumeConcurrentlyStatus.CONSUME_SUCCESS)
        batch = [msg(topic="%%RETRY%%%s_Topic" % GROUP, queue_offset=1),
                 msg(topic="%%RETRY%%%s" % GROUP, queue_offset=2),
                 msg(topic=TOPIC, queue_offset=3)]
        c._consume_concurrent_batch("k", MessageQueue(TOPIC, BROKER, 0), batch, None,
                                    listener, False)
        assert seen == [["Topic", "%%RETRY%%%s" % GROUP, TOPIC]]

    def test_orderly_delivery_path_does_not_normalize(self):
        """The control that pins the placement: the orderly branch runs the property-based
        restore but never the wrapper unwrap, even for the shape the concurrent entry cuts."""
        c = push_consumer()
        seen = []
        c.message_listener = _Orderly(seen, ConsumeOrderlyStatus.SUCCESS)
        wrapper = "%%RETRY%%%s_Topic" % GROUP
        batch = [msg(topic=wrapper, queue_offset=1)]
        c._consume_batch_held("k", MessageQueue(TOPIC, BROKER, 0), batch, None)
        assert seen == [[wrapper]], "the orderly path must hand the wrapper topic through as-is"


class _CaptureListener:
    """Duck-typed listener sink: records the topics each delivery hands over and returns a fixed status."""

    def __init__(self, sink, status):
        self.sink = sink
        self.status = status

    def consume_message(self, msgs, context):
        self.sink.append([m.topic for m in msgs])
        return self.status


class _Orderly(_CaptureListener, MessageListenerOrderly):
    """A real MessageListenerOrderly so ``_is_orderly()`` flips for the orderly delivery path."""


# ============================================================ #119: per-status POP re-schedule


class TestPopReschedulePerStatus:
    def test_polling_full_parks_the_configured_error_backoff(self, monkeypatch):
        c = push_consumer()
        _pq, delivered, _t, clock = _run_pop_rounds(
            monkeypatch, c, [PopResult(PopStatus.POLLING_FULL)])
        assert delivered == []
        assert clock.delays == [1.0], \
            "POLLING_FULL waits the generic error backoff, whose default is 1000 ms"

    def test_other_status_parks_the_configured_error_backoff_too(self, monkeypatch):
        """A status the round handling does not recognize shares the POLLING_FULL wait, and that
        wait follows the knob rather than a private constant."""
        c = push_consumer()
        c.set_pull_time_delay_when_exception_millis(250)
        weird = PopResult(PopStatus.POLLING_FULL)
        weird.status = "NOT_A_STATUS"
        _pq, _d, _t, clock = _run_pop_rounds(monkeypatch, c, [weird])
        assert clock.delays == [0.25]

    def test_polling_not_found_and_no_new_msg_repop_immediately(self, monkeypatch):
        """Control for the case above: the idle statuses must not park at all -- red if the
        generic 200 ms guard still wraps the empty-result branch."""
        c = push_consumer()
        _pq, _d, _t, clock = _run_pop_rounds(
            monkeypatch, c, [PopResult(PopStatus.POLLING_NOT_FOUND)])
        assert clock.delays == []
        c2 = push_consumer()
        _pq2, _d2, _t2, clock2 = _run_pop_rounds(
            monkeypatch, c2, [PopResult(PopStatus.NO_NEW_MSG)])
        assert clock2.delays == []

    def test_found_emptied_by_the_filter_repops_immediately_without_tps(self, monkeypatch):
        """Every kept row was filtered out: ack each drop, record RT, record **no** TPS, and
        re-pop immediately (the idle guard must not swallow this case)."""
        c = push_consumer()
        sub = FilterAPI.build_subscription_data(TOPIC, "TagA")
        rows = [msg(queue_offset=i, tag="TagB", pop_ck=ck(offset=i)) for i in (1, 2, 3)]
        pq, delivered, transport, clock = _run_pop_rounds(
            monkeypatch, c, [PopResult(PopStatus.FOUND, rows)], sub=sub)
        assert delivered == []
        assert sorted(offset for _t_, _q, offset in transport.acks) == [1, 2, 3]
        assert clock.delays == []
        rt, tps = _stats_items(c)
        assert rt is not None and rt.times == 1
        assert tps is None, "a round that delivered nothing records no TPS"
        assert pq.wait_ack_count() == 0, "the acked drops never entered the wait-ack ledger"

    def test_found_tps_counts_kept_rows_not_popped_rows(self, monkeypatch):
        """Popped 3, kept 2: TPS value is 2."""
        c = push_consumer()
        sub = FilterAPI.build_subscription_data(TOPIC, "TagA")
        rows = [msg(queue_offset=1, tag="TagA", pop_ck=ck(offset=1)),
                msg(queue_offset=2, tag="TagA", pop_ck=ck(offset=2)),
                msg(queue_offset=3, tag="TagB", pop_ck=ck(offset=3))]
        pq, delivered, transport, _clock = _run_pop_rounds(
            monkeypatch, c, [PopResult(PopStatus.FOUND, rows)], sub=sub)
        _rt, tps = _stats_items(c)
        assert tps is not None and tps.times == 1 and tps.value == 2
        assert [len(b) for b in delivered] == [2]
        assert transport.acks == [(TOPIC, 0, 3)]
        assert pq.wait_ack_count() == 2, "the wait-ack accumulator is fed with the remaining rows"

    def test_found_waits_pull_interval_only_when_positive(self, monkeypatch):
        """pull_interval=500 with a delivered round: exactly one 0.5 s park. Control: the same
        round with pull_interval=0 re-pops immediately (the long-poll paces the rounds)."""
        rows = [msg(queue_offset=1, pop_ck=ck(offset=1))]
        c = push_consumer(pull_interval=500)
        _pq, delivered, _t, clock = _run_pop_rounds(
            monkeypatch, c, [PopResult(PopStatus.FOUND, list(rows))])
        assert len(delivered) == 1
        assert clock.delays == [0.5], "the delivered round waits pull_interval milliseconds"
        c2 = push_consumer(pull_interval=0)
        _pq2, delivered2, _t2, clock2 = _run_pop_rounds(
            monkeypatch, c2, [PopResult(PopStatus.FOUND, list(rows))])
        assert len(delivered2) == 1
        assert clock2.delays == [], "pull_interval == 0 re-pops immediately"

    def test_empty_found_records_rt_and_repops_immediately(self, monkeypatch):
        """FOUND carrying an empty list: RT for the round, no TPS, no submit, no park."""
        c = push_consumer()
        _pq, delivered, _t, clock = _run_pop_rounds(
            monkeypatch, c, [PopResult(PopStatus.FOUND, [])])
        assert delivered == []
        assert clock.delays == []
        rt, tps = _stats_items(c)
        assert rt is not None and rt.times == 1
        assert tps is None

    def test_error_parks_are_unchanged(self, monkeypatch):
        """The error path keeps its split (broker flow control parks 20 ms, anything else falls
        back to the generic pull/pop error backoff) -- the per-status re-schedule is about the
        *response statuses* and must not have moved it."""
        c = push_consumer()
        mq = MessageQueue(TOPIC, BROKER, 0)
        key = c._mq_key(mq)
        c.pop_mode = True
        c._started = True
        c._mq_map[key] = mq
        c._pop_queues[key] = PopProcessQueue()
        c.subscription_data[mq.topic] = SubscriptionData(topic=mq.topic, sub_string="*")
        clock = SwappedClock()

        def _record_and_stop(seconds):
            clock.delays.append(seconds)
            c._stop.set()

        clock.sleep = _record_and_stop
        monkeypatch.setattr(consumer_mod, "time", clock)

        class _FlowControlled:
            def pop_message(self, *args, **kw):
                raise MQBrokerException(ResponseCode.FLOW_CONTROL, "throttled")

        c._mq_client = _FlowControlled()
        th = threading.Thread(target=c._queue_pop_loop, args=(mq,))
        c._queue_threads[key] = th
        th.start()
        th.join(10)
        assert not th.is_alive()
        assert clock.delays == [PULL_TIME_DELAY_WHEN_BROKER_FLOW_CONTROL]
        assert PULL_TIME_DELAY_WHEN_BROKER_FLOW_CONTROL == 0.02
        # The generic non-215 error branch and a POLLING_FULL round take the same wait on this port:
        # the consumer's knob, default 1000 ms. That number must stay distinct from the broker's
        # FLOW_CONTROL(215) pause, which keeps its own 20 ms arm.
        assert c.get_pull_time_delay_when_exception_millis() == 1000
        assert (c.get_pull_time_delay_when_exception_millis() / 1000.0
                != PULL_TIME_DELAY_WHEN_BROKER_FLOW_CONTROL)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
