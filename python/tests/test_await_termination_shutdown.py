# -*- coding: utf-8 -*-
# Offline lock for the shutdown drain ceiling (awaitTerminationMillisWhenShutdown): the
# assertions gate on recorded call state (what shutdown handed to the executors / thread joins),
# never on a wall-clock window.
from __future__ import annotations

import threading

from client.consume_executor import ConsumeExecutor
from client.consumer import DefaultMQPushConsumer


class _RecordingExecutor:
    """Stand-in for the POP consume executor: records every (wait, timeout) shutdown call."""

    def __init__(self):
        self.calls = []

    def shutdown(self, wait=False, timeout=None):
        self.calls.append((bool(wait), timeout))


class _RecordingThread:
    """Stand-in for a live worker thread: records every join timeout it is given."""

    def __init__(self):
        self.joins = []

    def is_alive(self):
        return True

    def join(self, timeout=None):
        self.joins.append(timeout)


def _started_consumer(group):
    c = DefaultMQPushConsumer(group)
    c._started = True
    return c


def test_default_is_zero_and_setter_roundtrips():
    c = DefaultMQPushConsumer("G_await_default")
    assert c.await_termination_millis_when_shutdown == 0
    c.set_await_termination_millis_when_shutdown(3000)
    assert c.get_await_termination_millis_when_shutdown() == 3000


def test_pop_executor_drain_bounded_by_ceiling():
    c = _started_consumer("G_await_pop")
    c.pop_mode = True
    ex = _RecordingExecutor()
    c._pop_executor = ex
    c.set_await_termination_millis_when_shutdown(3000)
    c.shutdown()
    # First stop accepting tasks (never under the consumer lock), then the bounded drain
    assert ex.calls[0] == (False, None), ex.calls
    assert (True, 3.0) in ex.calls, ex.calls


def test_pop_executor_keeps_fire_and_forget_when_ceiling_not_positive():
    # The control: <= 0 must add **no** extra ceiling -- reverting the wired drain keeps this
    # green while the positive-ceiling test above goes red, pinning both directions.
    for millis in (0, -1):
        c = _started_consumer("G_await_pop0")
        c.pop_mode = True
        ex = _RecordingExecutor()
        c._pop_executor = ex
        c.set_await_termination_millis_when_shutdown(millis)
        c.shutdown()
        assert ex.calls == [(False, None)], (millis, ex.calls)


def test_dispatch_thread_join_bounded_by_ceiling():
    # The non-POP consume path runs on the dispatch thread: its join picks up the ceiling.
    c = _started_consumer("G_await_dispatch")
    th = _RecordingThread()
    c._dispatch_thread = th
    c.set_await_termination_millis_when_shutdown(5000)
    c.shutdown()
    assert th.joins == [5.0], th.joins


def test_dispatch_thread_join_keeps_default_without_ceiling():
    c = _started_consumer("G_await_dispatch0")
    th = _RecordingThread()
    c._dispatch_thread = th
    c.shutdown()
    assert th.joins == [2], th.joins


def test_real_executor_bounded_join_returns_before_the_task_finishes():
    ex = ConsumeExecutor(1, 1, thread_name_prefix="rmq-await-unit")
    started = threading.Event()
    finished = threading.Event()
    release = threading.Event()

    def _task():
        started.set()
        release.wait(30)
        finished.set()

    ex.submit(_task)
    assert started.wait(5), "the worker must pick the task up"
    ex.shutdown(wait=True, timeout=0.05)
    assert not finished.is_set(), "a bounded join returns while the task still runs"
    release.set()
    assert finished.wait(5)
