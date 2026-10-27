# -*- coding: utf-8 -*-
"""Offline regression for what one pull round of the push-consumer path is allowed to record.

Two rules of the pull round's accounting, each pinned by a case that goes red when the rule is
reverted, moved, or gated on the wrong field:

* ``PULL_RT`` charges the round trip of a **FOUND** round only. An idle round (``NO_NEW_MSG`` /
  ``NO_MATCHED_MSG``) spends its time parked in the broker's suspend window, so charging it would
  report that window as this client's pull latency; a FOUND round charges the trip even when the
  round carried no row at all.
* ``PULL_TPS`` counts the rows that **survived** the pre-delivery step -- the second-pass tag
  check plus the registered filter hooks -- i.e. what the round hands on, not what the broker
  returned. A FOUND round whose whole batch the pre-delivery step removed therefore records RT and
  contributes nothing to TPS; a round that keeps 2 of 3 records 2.
* the round's **re-schedule** is judged on what survived pre-delivery: a round that delivered rows
  waits ``pull_interval`` (and nothing when that knob is 0), while a round that delivered nothing --
  a FOUND batch that came back empty or was emptied by the filter, ``NO_NEW_MSG``,
  ``NO_MATCHED_MSG`` -- re-pulls immediately, because such a round is itself a long poll and an extra
  idle guard would only delay the next fetch.
  The error arms (the broker's FLOW_CONTROL code and the generic backoff knob) are unchanged.

The transport is an in-process double and the loop clock is swapped, so every park the loop takes
is observed as a recorded value (same style as ``test_pop_delivery_guard``). The transport keeps
**real** sleeps: a FOUND trip is measurably longer than zero (so "recorded" and "recorded as a
rounded 0" differ), and an idle round is measurably longer than a FOUND one (so a leaked idle round
shows up in the recorded RT magnitude, not only in its count).
"""
from __future__ import annotations

import threading
import time

import pytest

from client import consumer as consumer_mod
from client.consumer import (DefaultMQPushConsumer, PULL_TIME_DELAY_WHEN_BROKER_FLOW_CONTROL,
                             filter_messages_for_delivery)
from client.consumer_result import PullResult, PullStatus
from client.consumer_stats import ConsumerStatsManager
from client.exception import MQBrokerException
from client.hook import FilterMessageContext, FilterMessageHook
from common.message import MessageExt, MessageQueue
from common.message_const import MessageConst
from common.subscription_data import FilterAPI, SubscriptionData
from remoting.protocol.codes import ResponseCode

GROUP = "GID_PullRoundStats"
TOPIC = "PullRoundStatsTopic"
BROKER = "broker-a"

# The transport parks this long (real time) on a round that answers "nothing to hand over" -- the
# local stand-in for the broker's suspend window -- and only FOUND_ROUND_TRIP on a FOUND round.
# The gap between the two is what makes an idle round charged to PULL_RT visible in the value.
IDLE_ROUND_WINDOW = 0.25
FOUND_ROUND_TRIP = 0.02


def msg(queue_offset: int = 0, tag: str = None, body: bytes = b"body") -> MessageExt:
    m = MessageExt(topic=TOPIC, body=body)
    m.queue_id = 0
    m.queue_offset = queue_offset
    if tag is not None:
        m.properties[MessageConst.PROPERTY_TAGS] = tag
    return m


def found(*msgs: MessageExt) -> PullResult:
    """A FOUND round carrying exactly these rows."""
    return PullResult(PullStatus.FOUND, next_begin_offset=10, min_offset=0, max_offset=10,
                      msg_found_list=list(msgs))


def push_consumer(**kw) -> DefaultMQPushConsumer:
    c = DefaultMQPushConsumer(GROUP)
    for k, v in kw.items():
        setattr(c, k, v)
    return c


class Transport:
    """MQClientInstance double: replays scripted pull rounds and, once the script is spent, answers
    an idle round after raising the consumer's stop event, so the loop always terminates.

    A scripted item that is an exception instance is raised on that round instead of returned, so
    the same harness can drive the error arms of the loop."""

    def __init__(self, script=()):
        self._script = list(script)
        self.statuses = []      # every round's status, in the order the loop asked
        self.stop = None

    def pull_message(self, group, mq, offset, *args, **kw):
        if self._script:
            result = self._script.pop(0)
            if isinstance(result, BaseException):
                raise result
        else:
            if self.stop is not None:
                self.stop.set()
            result = PullResult(PullStatus.NO_NEW_MSG, next_begin_offset=offset + 10)
        self.statuses.append(result.status)
        time.sleep(FOUND_ROUND_TRIP if result.status == PullStatus.FOUND else IDLE_ROUND_WINDOW)
        return result


class SwappedClock:
    """Module-level clock swap for the consumer module: ``sleep`` is recorded instead of waited,
    everything else (notably ``time()``) passes through to the real module."""

    def __init__(self):
        self.delays = []

    def __getattr__(self, name):
        return getattr(time, name)

    def sleep(self, seconds):
        self.delays.append(seconds)


class _DropByBody(FilterMessageHook):
    """A filter hook that removes the rows carrying one of the listed bodies and counts its calls."""

    def __init__(self, drop_bodies):
        self.drop_bodies = set(drop_bodies)
        self.calls = 0
        self.seen = []

    def hook_name(self) -> str:
        return "drop-by-body"

    def filter_message(self, context: FilterMessageContext) -> None:
        self.calls += 1
        self.seen.append([m.body for m in context.msg_list])
        context.msg_list = [m for m in context.msg_list if m.body not in self.drop_bodies]


class Round:
    """Everything one scripted run of the real loop left behind."""

    def __init__(self, c, key, transport, clock):
        self.c = c
        self.transport = transport
        self.clock = clock
        self.pending = list(c._pending.get(key) or ())
        stats_key = "%s@%s" % (TOPIC, GROUP)
        self.rt = c._stats_manager.topic_and_group_pull_rt.find(stats_key)
        self.tps = c._stats_manager.topic_and_group_pull_tps.find(stats_key)


def _run_pull_rounds(monkeypatch, c, script, sub=None, hooks=()):
    """Drive the real ``_queue_pull_loop`` over a single queue with ``script`` as the answer rounds."""
    mq = MessageQueue(TOPIC, BROKER, 0)
    key = c._mq_key(mq)
    c._started = True
    c._mq_map[key] = mq
    c._offset_table[key] = 0
    c.subscription_data[mq.topic] = sub or SubscriptionData(topic=mq.topic, sub_string="*")
    for hook in hooks:
        c.register_filter_message_hook(hook)
    if c._stats_manager is None:
        c._stats_manager = ConsumerStatsManager()
    transport = Transport(script)
    transport.stop = c._stop
    c._mq_client = transport
    clock = SwappedClock()
    monkeypatch.setattr(consumer_mod, "time", clock)
    thread = threading.Thread(target=c._queue_pull_loop, args=(mq,))
    c._queue_threads[key] = thread
    thread.start()
    thread.join(15)
    assert not thread.is_alive(), "the pull loop did not exit after the script was consumed"
    return Round(c, key, transport, clock)


# ================================================== PULL_RT belongs to FOUND rounds only


class TestPullRtIsFoundRoundOnly:
    @pytest.mark.parametrize("idle_status", [PullStatus.NO_NEW_MSG, PullStatus.NO_MATCHED_MSG])
    def test_idle_round_records_no_rt_and_no_tps(self, monkeypatch, idle_status):
        """One idle round, with no FOUND round to hide behind: neither a PULL_RT item nor a PULL_TPS
        item is created at all. An implementation that charges every round creates the RT item here
        and goes red, and so does one that feeds TPS from the (empty) popped list of an idle round."""
        got = _run_pull_rounds(monkeypatch, push_consumer(),
                               [PullResult(idle_status, next_begin_offset=7)])
        assert got.transport.statuses == [idle_status, PullStatus.NO_NEW_MSG], \
            "the single scripted round plus the loop-closing one: exactly this round is scored"
        assert got.pending == []
        assert got.rt is None, "an idle long-poll round must record no pull RT"
        assert got.tps is None
        # The round is idle, not failed: it must not be routed through an error backoff either, so
        # "recorded nothing" cannot be quietly earned by the round having been abandoned. The
        # generic error wait is the consumer's own knob, so the value checked here is the one this
        # consumer is configured with.
        assert (got.c.get_pull_time_delay_when_exception_millis() / 1000.0
                not in got.clock.delays)
        assert PULL_TIME_DELAY_WHEN_BROKER_FLOW_CONTROL not in got.clock.delays

    def test_idle_long_poll_window_never_reaches_the_recorded_rt(self, monkeypatch):
        """Idle, FOUND, idle: RT is charged exactly once, and its value is the FOUND trip -- far
        below the suspend window the two idle rounds each spent. Red for any implementation that
        folds an idle round into PULL_RT: the average/sum would jump past the window size."""
        got = _run_pull_rounds(
            monkeypatch, push_consumer(),
            [PullResult(PullStatus.NO_MATCHED_MSG, next_begin_offset=7),
             found(msg(queue_offset=1, body=b"only")),
             PullResult(PullStatus.NO_NEW_MSG, next_begin_offset=9)])
        assert got.transport.statuses == [PullStatus.NO_MATCHED_MSG, PullStatus.FOUND,
                                          PullStatus.NO_NEW_MSG, PullStatus.NO_NEW_MSG]
        assert got.rt is not None and got.rt.times == 1, "one FOUND round, one RT sample"
        assert got.rt.value >= 10, "the FOUND trip is real time, not a rounded 0"
        assert got.rt.value < IDLE_ROUND_WINDOW * 1000.0, \
            "the idle rounds' suspend window must not be inside the recorded RT"
        assert got.tps is not None and got.tps.value == 1

    def test_empty_found_round_still_records_its_rt(self, monkeypatch):
        """FOUND carrying no row: the round trip is charged (the status decides, not the batch
        size), and TPS stays untouched."""
        got = _run_pull_rounds(monkeypatch, push_consumer(),
                               [found()])
        assert got.pending == []
        assert got.rt is not None and got.rt.times == 1 and got.rt.value >= 10
        assert got.tps is None

    def test_rt_counts_every_found_round_of_the_run(self, monkeypatch):
        """Two FOUND rounds split by an idle one: RT has two samples while the idle round adds
        none -- the per-round pairing on the RT side."""
        got = _run_pull_rounds(
            monkeypatch, push_consumer(),
            [found(msg(queue_offset=1, body=b"a")),
             PullResult(PullStatus.NO_NEW_MSG, next_begin_offset=5),
             found(msg(queue_offset=2, body=b"b"), msg(queue_offset=3, body=b"c"))])
        assert got.rt is not None and got.rt.times == 2
        assert got.tps is not None and got.tps.times == 2 and got.tps.value == 3


# ================================================== PULL_TPS counts the post-filter rows


class TestPullTpsCountsDeliveredRows:
    def test_round_that_keeps_everything_counts_the_whole_batch(self, monkeypatch):
        """Paired control for the cases below: subscribe "*" with no hook, so the pre-delivery step
        removes nothing and TPS is exactly the popped count. Red if the count call moved out of the
        round or reads something other than the delivered rows."""
        got = _run_pull_rounds(monkeypatch, push_consumer(),
                               [found(msg(queue_offset=1, body=b"a"),
                                      msg(queue_offset=2, body=b"b"),
                                      msg(queue_offset=3, body=b"c"))])
        assert [m.queue_offset for m in got.pending] == [1, 2, 3]
        assert got.tps is not None and got.tps.times == 1 and got.tps.value == 3
        assert got.rt is not None and got.rt.times == 1

    def test_batch_emptied_by_the_tag_pass_records_rt_and_zero_tps(self, monkeypatch):
        """Subscribe TagA, every returned row is TagB: the pre-delivery step empties the round, so
        RT is charged and TPS gets nothing. An implementation counting the raw popped list records 3
        here; one that also gated RT on the batch would lose the RT sample."""
        sub = FilterAPI.build_subscription_data(TOPIC, "TagA")
        rows = [found(msg(queue_offset=1, tag="TagB", body=b"x"),
                      msg(queue_offset=2, tag="TagB", body=b"y"),
                      msg(queue_offset=3, tag="TagB", body=b"z"))]
        got = _run_pull_rounds(monkeypatch, push_consumer(), rows, sub=sub)
        assert got.pending == [], "the tag pass must keep nothing out of the buffer"
        assert got.rt is not None and got.rt.times == 1
        assert got.tps is None, "a round that hands on no row contributes zero to PULL_TPS"

    def test_batch_emptied_by_a_filter_hook_records_rt_and_zero_tps(self, monkeypatch):
        """Same rule on the other stage of the pre-delivery step: the subscription is "*" (so the
        tag pass is inert) and the hook drops every row. The hook call count proves the round really
        went through the filter -- otherwise a zero TPS would be vacuous."""
        hook = _DropByBody([b"a", b"b"])
        got = _run_pull_rounds(monkeypatch, push_consumer(),
                               [found(msg(queue_offset=1, body=b"a"),
                                      msg(queue_offset=2, body=b"b"))],
                               hooks=[hook])
        assert hook.calls == 1
        assert hook.seen == [[b"a", b"b"]]
        assert got.pending == []
        assert got.rt is not None and got.rt.times == 1
        assert got.tps is None

    def test_partial_filter_records_the_kept_count_not_the_popped_count(self, monkeypatch):
        """The round pops 3 rows and the pre-delivery step removes 1: PULL_TPS is 2, one sample."""
        got = _run_pull_rounds(
            monkeypatch, push_consumer(),
            [found(msg(queue_offset=1, body=b"keep"),
                   msg(queue_offset=2, body=b"drop"),
                   msg(queue_offset=3, body=b"keep"))],
            hooks=[_DropByBody([b"drop"])])
        assert [m.queue_offset for m in got.pending] == [1, 3]
        assert got.tps is not None and got.tps.times == 1 and got.tps.value == 2
        assert got.rt is not None and got.rt.times == 1

    def test_both_filter_stages_in_one_round_count_the_survivors_once(self, monkeypatch):
        """4 rows returned: the second tag pass removes 1 (wrong tag) and the hook removes another,
        so TPS is 2 -- and the hook only ever saw the 3 rows the tag pass left, which is why the
        counted number is neither the popped nor the tag-passed count."""
        sub = FilterAPI.build_subscription_data(TOPIC, "TagA")
        hook = _DropByBody([b"hook-drop"])
        got = _run_pull_rounds(
            monkeypatch, push_consumer(),
            [found(msg(queue_offset=1, tag="TagA", body=b"kept-1"),
                   msg(queue_offset=2, tag="TagB", body=b"tag-drop"),
                   msg(queue_offset=3, tag="TagA", body=b"hook-drop"),
                   msg(queue_offset=4, tag="TagA", body=b"kept-2"))],
            sub=sub, hooks=[hook])
        assert hook.seen == [[b"kept-1", b"hook-drop", b"kept-2"]]
        assert [m.queue_offset for m in got.pending] == [1, 4]
        assert got.tps is not None and got.tps.times == 1 and got.tps.value == 2
        assert got.rt is not None and got.rt.times == 1

    def test_tps_and_rt_stay_paired_across_a_mixed_run(self, monkeypatch):
        """Idle, fully-filtered, partial: RT has two samples (the FOUND rounds), TPS one sample of 2.
        Red if the idle round joined RT, if a filtered-out batch joined TPS, or if the two counters
        drifted apart."""
        sub = FilterAPI.build_subscription_data(TOPIC, "TagA")
        got = _run_pull_rounds(
            monkeypatch, push_consumer(),
            [PullResult(PullStatus.NO_NEW_MSG, next_begin_offset=5),
             found(msg(queue_offset=1, tag="TagB", body=b"gone")),
             found(msg(queue_offset=2, tag="TagA", body=b"one"),
                   msg(queue_offset=3, tag="TagA", body=b"two"),
                   msg(queue_offset=4, tag="TagB", body=b"gone"))],
            sub=sub)
        assert [m.queue_offset for m in got.pending] == [2, 3]
        assert got.rt is not None and got.rt.times == 2
        assert got.tps is not None and got.tps.times == 1 and got.tps.value == 2

    def test_both_rules_hold_in_one_run(self, monkeypatch):
        """Both legs at once, in the order they would arrive live: a FOUND round that returns 3 rows
        and delivers 2 (one sample of TPS=2, one RT sample of the FOUND trip), then an idle round
        that leaves both items exactly where they were. An implementation that charges the idle round
        or counts the raw list cannot pass, and neither can one that fixes only half."""
        sub = FilterAPI.build_subscription_data(TOPIC, "TagA")
        got = _run_pull_rounds(
            monkeypatch, push_consumer(),
            [found(msg(queue_offset=1, tag="TagA", body=b"a"),
                   msg(queue_offset=2, tag="TagA", body=b"b"),
                   msg(queue_offset=3, tag="TagB", body=b"c")),
             PullResult(PullStatus.NO_MATCHED_MSG, next_begin_offset=8)], sub=sub)
        assert [m.queue_offset for m in got.pending] == [1, 2]
        assert got.transport.statuses == [PullStatus.FOUND, PullStatus.NO_MATCHED_MSG,
                                          PullStatus.NO_NEW_MSG]
        assert got.rt is not None and got.rt.times == 1
        assert got.rt.value < IDLE_ROUND_WINDOW * 1000.0
        assert got.tps is not None and got.tps.times == 1 and got.tps.value == 2


# ============================================================ the round's re-schedule


class TestPullRoundReschedule:
    """Which pull round parks before the next one, and for how long.

    The whole table hangs on one distinction -- did this round hand a row to the buffer -- so every
    leg here runs with ``pull_interval`` set to a value no other arm of the loop uses (50 ms,
    against the broker flow-control 20 ms and the error knob's scripted 250 ms), except the leg that
    pins the 0 meaning "re-pull immediately"."""

    def test_delivered_round_waits_the_pull_interval_once(self, monkeypatch):
        """pull_interval=50 and a round that delivers one row: exactly one 0.05 s park, and it lands
        between the two rounds. Red when the wait is missing (the bug this file pins), when it is
        taken on a round that delivered nothing, or when a whole run parks only once."""
        got = _run_pull_rounds(monkeypatch, push_consumer(pull_interval=50),
                               [found(msg(queue_offset=1, body=b"a"))])
        assert [m.queue_offset for m in got.pending] == [1]
        assert got.clock.delays == [0.05], \
            "the delivered round waits pull_interval milliseconds, once"
        # One wait per delivered round -- the cadence is per round, not once per loop.
        two = _run_pull_rounds(monkeypatch, push_consumer(pull_interval=50),
                               [found(msg(queue_offset=1, body=b"a")),
                                found(msg(queue_offset=2, body=b"b"))])
        assert [m.queue_offset for m in two.pending] == [1, 2]
        assert two.clock.delays == [0.05, 0.05], two.clock.delays

    def test_delivered_round_with_pull_interval_zero_parks_nothing(self, monkeypatch):
        """The paired control on the other side of the same leg: the same delivered round with
        pull_interval == 0 (the shipped default) re-pulls immediately."""
        got = _run_pull_rounds(monkeypatch, push_consumer(pull_interval=0),
                               [found(msg(queue_offset=1, body=b"a"))])
        assert [m.queue_offset for m in got.pending] == [1]
        assert got.clock.delays == [], "pull_interval == 0 means re-pull immediately"

    def test_found_batch_emptied_by_the_tag_pass_parks_nothing(self, monkeypatch):
        """The leg that decides **where** the wait sits: FOUND with a full batch that the second-pass
        tag check removes entirely delivered nothing, so it re-pulls immediately. Red for an
        implementation that parks on the raw response status or on the popped list instead of on the
        rows that survive pre-delivery."""
        sub = FilterAPI.build_subscription_data(TOPIC, "TagA")
        rows = [found(msg(queue_offset=1, tag="TagB", body=b"x"),
                      msg(queue_offset=2, tag="TagB", body=b"y"))]
        got = _run_pull_rounds(monkeypatch, push_consumer(pull_interval=50), rows, sub=sub)
        assert got.pending == [], "the tag pass must keep nothing out of the buffer"
        assert got.clock.delays == []

    def test_found_batch_emptied_by_a_filter_hook_parks_nothing(self, monkeypatch):
        """Same rule on the other stage of pre-delivery: the hook drops every row, so this is a
        round that delivered nothing and the cadence does not apply."""
        hook = _DropByBody([b"a"])
        got = _run_pull_rounds(monkeypatch, push_consumer(pull_interval=50),
                               [found(msg(queue_offset=1, body=b"a"))], hooks=[hook])
        assert hook.calls == 1
        assert got.pending == []
        assert got.clock.delays == []

    @pytest.mark.parametrize("idle_status", [PullStatus.NO_NEW_MSG, PullStatus.NO_MATCHED_MSG])
    def test_idle_round_parks_nothing_even_with_a_pull_interval(self, monkeypatch, idle_status):
        """The cadence paces rounds that delivered, so an idle round re-pulls at once even with the
        interval configured: a further local wait there would only delay the next fetch.
        A FOUND round whose batch came back empty is the third idle shape."""
        got = _run_pull_rounds(monkeypatch, push_consumer(pull_interval=50),
                               [PullResult(idle_status, next_begin_offset=7)])
        assert got.transport.statuses == [idle_status, PullStatus.NO_NEW_MSG]
        assert got.clock.delays == []
        empty_found = _run_pull_rounds(monkeypatch, push_consumer(pull_interval=50), [found()])
        assert empty_found.clock.delays == []

    def test_a_delivered_round_does_not_reuse_the_error_arms(self, monkeypatch):
        """The pacing change must not have swallowed either error arm: the broker's FLOW_CONTROL
        answer still waits its own 20 ms and any other failure still waits the generic backoff this
        loop snapshotted at setup -- and neither arm adds the cadence on top, because no row was
        delivered."""
        c = push_consumer(pull_interval=50)
        c.set_pull_time_delay_when_exception_millis(250)
        got = _run_pull_rounds(monkeypatch, c,
                               [MQBrokerException(ResponseCode.FLOW_CONTROL, "system busy"),
                                RuntimeError("socket reset")])
        assert got.clock.delays == [PULL_TIME_DELAY_WHEN_BROKER_FLOW_CONTROL, 0.25], \
            got.clock.delays
        assert PULL_TIME_DELAY_WHEN_BROKER_FLOW_CONTROL == 0.02
        assert got.pending == []


# ============================================================ the shared pre-delivery helper


class TestFilterHelperIsWhatTheRoundUses:
    """The round records the list the shared helper returns, so the helper's own shape is part of
    the contract: the hook runs after the tag pass and receives its survivors."""

    def test_hook_runs_after_the_tag_pass_and_can_empty_the_list(self):
        mq = MessageQueue(TOPIC, BROKER, 0)
        sub = FilterAPI.build_subscription_data(TOPIC, "TagA")
        rows = [msg(queue_offset=1, tag="TagB", body=b"x"),
                msg(queue_offset=2, tag="TagA", body=b"y")]
        hook = _DropByBody([b"y"])
        assert filter_messages_for_delivery(GROUP, [hook], mq, sub, rows) == []
        assert hook.seen == [[b"y"]], "the hook must see only the tag pass's survivors"

    def test_no_tag_pass_and_no_hook_keeps_the_batch_intact(self):
        mq = MessageQueue(TOPIC, BROKER, 0)
        rows = [msg(queue_offset=1, tag="TagB", body=b"x")]
        assert filter_messages_for_delivery(GROUP, [], mq, None, rows) == rows


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
