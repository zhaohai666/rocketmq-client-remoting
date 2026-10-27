# -*- coding: utf-8 -*-
"""Offline regression for the push-consumer pull loop's FOUND-round offset sanity check.

On a FOUND round that has already advanced its cursor, the loop compares three numbers it holds
about that round and warns, at WARN level, when they do not hang together:

* ``prev_request_offset`` -- the offset this round asked to pull from,
* ``next_begin_offset``  -- the offset the response says comes next,
* ``first_msg_offset``   -- the offset of the earliest row the round hands to the consume side: the
  **first row of the list left after this client's second-pass tag check and filter hook ran**. A
  round that hands over no row uses the largest representable offset, so that half can never fire.

The check is purely a client-side arithmetic diagnostic. It changes no control flow -- no retry, no
backoff, no offset rewrite, no cursor rollback, no dropped batch -- and it runs after the rows are
handed over and the stats booked. Every case below pins both halves at once: the warning this round
must (or must not) emit, and the fact the round still behaved normally.

The transport is an in-process double that drives the real loop over one queue (same shape as
``test_pull_round_stats``); it raises the stop flag as the last scripted round is handed over, so
exactly that round is scored and nothing after it rewrites the cursor. The warning is observed by
capturing the client logger with ``caplog`` (the way ``test_flow_control`` / ``test_orderly_reconsume``
assert on logs), not by reading a file.
"""
from __future__ import annotations

import threading
import time

import pytest

from client import consumer as consumer_mod
from client.consumer import DefaultMQPushConsumer
from client.consumer_result import PullResult, PullStatus
from client.consumer_stats import ConsumerStatsManager
from common.message import MessageExt, MessageQueue
from common.message_const import MessageConst
from common.subscription_data import FilterAPI, SubscriptionData

GROUP = "GID_PullOffsetSanity"
TOPIC = "PullOffsetSanityTopic"
BROKER = "broker-a"

WARN_MARKER = "pull round offsets do not add up"


def msg(queue_offset: int, tag: str = None, body: bytes = b"body") -> MessageExt:
    m = MessageExt(topic=TOPIC, body=body)
    m.queue_id = 0
    m.queue_offset = queue_offset
    if tag is not None:
        m.properties[MessageConst.PROPERTY_TAGS] = tag
    return m


def found(next_begin_offset: int, *msgs: MessageExt) -> PullResult:
    """A FOUND round whose response says ``next_begin_offset`` comes next and that carries exactly
    these rows (in list order; the first that survives filtering is the row the check reads)."""
    return PullResult(PullStatus.FOUND, next_begin_offset=next_begin_offset,
                      min_offset=0, max_offset=next_begin_offset, msg_found_list=list(msgs))


class Transport:
    """MQClientInstance double: replays the scripted pull rounds and raises the consumer's stop
    event the instant the last scripted round is handed back, so exactly that round is scored and no
    follow-on idle round rewrites the cursor it left behind.

    It records, per round, the status and the offset the loop asked to pull from -- the latter is how
    a test reads back ``prev_request_offset``, the cursor value handed to ``pull_message``."""

    def __init__(self, script):
        self._script = list(script)
        self.statuses = []
        self.requested_offsets = []
        self.stop = None

    def pull_message(self, group, mq, offset, *args, **kw):
        result = self._script.pop(0)
        self.statuses.append(result.status)
        self.requested_offsets.append(offset)
        if not self._script and self.stop is not None:
            self.stop.set()
        return result


class SilentClock:
    """Module-level clock swap: ``sleep`` is dropped, everything else (notably ``time()``) falls
    through to the real module, so a round never really parks."""

    def __getattr__(self, name):
        return getattr(time, name)

    def sleep(self, seconds):
        pass


class Round:
    """Everything one scripted run of the real loop left behind."""

    def __init__(self, c, key, transport):
        self.pending = [m.queue_offset for m in (c._pending.get(key) or ())]
        self.cursor = c._offset_table.get(key)
        self.transport = transport


def _run_rounds(monkeypatch, c, script, start_offset, sub=None):
    """Drive the real ``_queue_pull_loop`` over a single queue, first asking it to pull from
    ``start_offset``, then replaying ``script`` (the loop stops once the script is spent)."""
    mq = MessageQueue(TOPIC, BROKER, 0)
    key = c._mq_key(mq)
    c._started = True
    c._mq_map[key] = mq
    c._offset_table[key] = start_offset
    c.subscription_data[mq.topic] = sub or SubscriptionData(topic=mq.topic, sub_string="*")
    if c._stats_manager is None:
        c._stats_manager = ConsumerStatsManager()
    transport = Transport(script)
    transport.stop = c._stop
    c._mq_client = transport
    monkeypatch.setattr(consumer_mod, "time", SilentClock())
    thread = threading.Thread(target=c._queue_pull_loop, args=(mq,))
    c._queue_threads[key] = thread
    thread.start()
    thread.join(15)
    assert not thread.is_alive(), "the pull loop did not exit after the script was consumed"
    return Round(c, key, transport)


def _warns(caplog):
    return [r.getMessage() for r in caplog.records
            if r.levelname == "WARNING" and WARN_MARKER in r.getMessage()]


# ============================ the two failing legs ============================


def test_next_begin_below_requested_warns_once_without_swallowing_the_round(monkeypatch, caplog):
    """The round asked to pull from 100 but the response says next is 10: the next-begin half does
    not hang together, so exactly one warning is emitted naming all three numbers. The warning is a
    pure diagnostic -- the rows (at 150 / 151, so the first-row half stays silent) are still
    delivered and the cursor is still advanced to the bogus 10, not rolled back to the 100 it asked
    with."""
    with caplog.at_level("WARNING", logger="rocketmq.client"):
        got = _run_rounds(monkeypatch, DefaultMQPushConsumer(GROUP),
                          [found(10, msg(150), msg(151))], start_offset=100)
    warns = _warns(caplog)
    assert len(warns) == 1, "a FOUND round with an out-of-line next-begin offset warns exactly once"
    line = warns[0]
    assert "requested offset 100" in line, line
    assert "next begin offset 10" in line, line
    assert "first message offset 150" in line, line
    assert got.pending == [150, 151], "the round still delivers its rows"
    assert got.cursor == 10, "the cursor advanced to the response's next-begin offset -- no rollback"
    assert got.transport.requested_offsets == [100]


def test_first_dispatched_row_below_requested_warns(monkeypatch, caplog):
    """The round asked to pull from 100 and the earliest row it dispatches sits at 50: the first-row
    half does not hang together, so it warns naming all three numbers, while the next-begin offset
    (200) is forward and keeps that half silent."""
    with caplog.at_level("WARNING", logger="rocketmq.client"):
        got = _run_rounds(monkeypatch, DefaultMQPushConsumer(GROUP),
                          [found(200, msg(50), msg(150))], start_offset=100)
    warns = _warns(caplog)
    assert len(warns) == 1, warns
    line = warns[0]
    assert "requested offset 100" in line, line
    assert "next begin offset 200" in line, line
    assert "first message offset 50" in line, line
    assert got.pending == [50, 150]
    assert got.cursor == 200


# ============================ the legs that must stay quiet ============================


def test_forward_found_round_does_not_warn(monkeypatch, caplog):
    """The leg that goes red if the comparison is inverted or made always-true: a normal forward
    round, the rows starting exactly at the requested offset and next-begin ahead of it, hangs
    together, so it warns nothing -- yet still delivers and advances."""
    with caplog.at_level("WARNING", logger="rocketmq.client"):
        got = _run_rounds(monkeypatch, DefaultMQPushConsumer(GROUP),
                          [found(200, msg(100), msg(101))], start_offset=100)
    assert _warns(caplog) == [], "a coherent forward FOUND round must not warn"
    assert got.pending == [100, 101]
    assert got.cursor == 200


def test_empty_found_round_does_not_warn(monkeypatch, caplog):
    """The sentinel leg: a FOUND round that brought back no row keeps the largest representable
    first-message offset, so the first-row half can never fire, and its forward next-begin keeps the
    other half silent. Nothing warns, nothing is dropped."""
    with caplog.at_level("WARNING", logger="rocketmq.client"):
        got = _run_rounds(monkeypatch, DefaultMQPushConsumer(GROUP),
                          [found(150)], start_offset=100)
    assert _warns(caplog) == [], "an empty FOUND round's sentinel offset must not warn"
    assert got.pending == []
    assert got.transport.statuses == [PullStatus.FOUND]


def test_no_new_msg_round_does_not_warn(monkeypatch, caplog):
    """The check is scoped to FOUND rounds: an idle round whose next-begin happens to sit below the
    requested offset is not a FOUND round, so the check is skipped entirely and nothing warns."""
    with caplog.at_level("WARNING", logger="rocketmq.client"):
        got = _run_rounds(monkeypatch, DefaultMQPushConsumer(GROUP),
                          [PullResult(PullStatus.NO_NEW_MSG, next_begin_offset=50)],
                          start_offset=100)
    assert _warns(caplog) == [], "the offset check is scoped to FOUND rounds, not idle ones"
    assert got.transport.statuses == [PullStatus.NO_NEW_MSG]


# ============================ WHICH list the first-row leg reads ============================


def test_first_msg_offset_is_the_dispatched_row_not_one_the_filter_removed(monkeypatch, caplog):
    """The discriminator that pins WHICH list the check reads. The raw batch's earliest row sits at
    50, below the requested 100, but the subscription only wants TagA, so the second-pass tag check
    removes that row and the round dispatches just the 150 row. The row this client never handed over
    is not this round's first message, so nothing warns -- and reading the raw list instead would
    invent a warning for a row that was dropped on purpose. The drop itself is unchanged: only 150
    reaches the buffer."""
    sub = FilterAPI.build_subscription_data(TOPIC, "TagA")
    with caplog.at_level("WARNING", logger="rocketmq.client"):
        got = _run_rounds(monkeypatch, DefaultMQPushConsumer(GROUP),
                          [found(200, msg(50, tag="TagB"), msg(150, tag="TagA"))],
                          start_offset=100, sub=sub)
    assert _warns(caplog) == [], "a row the filter removed is not the round's first dispatched row"
    assert got.pending == [150], "only the surviving row reaches the buffer -- the drop is unchanged"
    assert got.cursor == 200


def test_dispatched_row_below_requested_warns_even_though_the_raw_head_is_ahead(monkeypatch, caplog):
    """The other side of the same pin: the raw head sits at 100 (exactly the requested offset, so the
    raw list alone would stay quiet), the TagB row in front of it in OFFSET order is the one that
    survives, and it sits below the request. The check reads the dispatched list, so it warns with
    first message offset 50."""
    sub = FilterAPI.build_subscription_data(TOPIC, "TagB")
    with caplog.at_level("WARNING", logger="rocketmq.client"):
        got = _run_rounds(monkeypatch, DefaultMQPushConsumer(GROUP),
                          [found(200, msg(100, tag="TagA"), msg(50, tag="TagB"))],
                          start_offset=100, sub=sub)
    warns = _warns(caplog)
    assert len(warns) == 1, warns
    assert "requested offset 100" in warns[0], warns[0]
    assert "first message offset 50" in warns[0], warns[0]
    assert got.pending == [50]
    assert got.cursor == 200


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
