# -*- coding: utf-8 -*-
from __future__ import annotations

import threading
import time
from collections import deque

from client.consumer import DefaultMQPushConsumer
from client.consumer_result import (ConsumeConcurrentlyStatus,
                                              MessageListenerOrderly)
from common.message import MessageExt, MessageQueue
from common.message_accessor import MessageAccessor

GROUP = "GID_CleanExpiredUnitTest"
TOPIC = "CleanExpiredUnitTestTopic"
BROKER = "broker-a"


def msg(queue_offset: int, topic: str = TOPIC) -> MessageExt:
    m = MessageExt(topic=topic, body=b"body")
    m.queue_id = 0
    m.broker_name = BROKER
    m.queue_offset = queue_offset
    return m


def stamp(m: MessageExt, ts_ms: int) -> MessageExt:
    MessageAccessor.set_consume_start_timestamp(m, ts_ms)
    return m


class RecordingStop:
    """Fake ``threading.Event``: records the timeout of every ``wait()`` and returns per script."""

    def __init__(self, results=()):
        self.results = list(results)
        self.timeouts = []

    def wait(self, timeout=None):
        self.timeouts.append(timeout)
        if self.results:
            return bool(self.results.pop(0))
        return True


class Clock:
    """Controllable clock: replaces the ``time`` in the ``client.consumer`` module.

    The expiry criterion for sweeping (``time.time()*1000 - stamp > consumeTimeout*60*1000``) must be
    pushable exactly to the boundary, otherwise the "strictly greater" case is either untestable or made flaky by sleep.
    """

    def __init__(self, now_ms: float = 1_800_000_000_000.0):
        self.now_ms = float(now_ms)

    def time(self) -> float:
        return self.now_ms / 1000.0


class Harness:
    """A consumer that never touches the network: queues / in-flight registration set up manually, send-back only recorded."""

    def __init__(self, consume_timeout=1, orderly=False):
        self.c = DefaultMQPushConsumer(GROUP)
        self.c.consume_timeout = consume_timeout
        self.c._started = True
        self.mq = MessageQueue(TOPIC, BROKER, 0)
        self.key = self.c._mq_key(self.mq)
        self.c._mq_map[self.key] = self.mq
        self.c._pending[self.key] = deque()
        self.c._consume_offsets[self.key] = 0
        self.backed = []
        self.fail_offsets = set()
        if orderly:
            class _Orderly(MessageListenerOrderly):
                def consume_message(self, msgs, context):
                    return None
            self.c.message_listener = _Orderly()

        def _back(m, delay_level, broker_name=None):
            if m.queue_offset in self.fail_offsets:
                raise RuntimeError("simulated send-back failure")
            self.backed.append((m, delay_level))

        self.c.send_message_back = _back  # type: ignore[assignment]

    @property
    def timeout_ms(self):
        return self.c.consume_timeout * 60 * 1000

    def inflight(self, msgs):
        self.c._inflight_msgs.setdefault(self.key, []).extend(msgs)
        return msgs

    def buffered(self, msgs):
        self.c._pending[self.key].extend(msgs)
        return msgs

    def sweep(self):
        self.c._clean_expired_queue(self.key)

    @property
    def backed_offsets(self):
        return sorted(m.queue_offset for m, _ in self.backed)

    @property
    def record_offsets(self):
        return sorted(m.queue_offset for m in self.c._process_queue_entries_locked(self.key))


# ---------------------------------------------------------------- Scheduling cadence
def test_sweep_schedule_initial_delay_equals_period(monkeypatch):
    pass
    c = DefaultMQPushConsumer(GROUP)
    c._started = True
    swept = []
    c._clean_expired_msg_once = lambda: swept.append(True)  # type: ignore[method-assign]
    stop = RecordingStop([False])
    c._stop = stop  # type: ignore[assignment]
    c._clean_expire_loop()
    assert stop.timeouts == [900.0, 900.0]   # the first round also waits a full period
    assert len(swept) == 1

    c2 = DefaultMQPushConsumer(GROUP + "_fast")
    c2._started = True
    c2.consume_timeout = 2
    swept2 = []
    c2._clean_expired_msg_once = lambda: swept2.append(True)  # type: ignore[method-assign]
    stop2 = RecordingStop([False, False])
    c2._stop = stop2  # type: ignore[assignment]
    c2._clean_expire_loop()
    assert stop2.timeouts == [120.0, 120.0, 120.0]
    assert len(swept2) == 2


def test_sweep_loop_exits_when_stopped_before_first_tick():
    """Stopped during shutdown: the first wait is woken immediately, with no sweep at all."""
    c = DefaultMQPushConsumer(GROUP + "_stopped")
    c._started = True
    swept = []
    c._clean_expired_msg_once = lambda: swept.append(True)  # type: ignore[method-assign]
    stop = RecordingStop([True])
    c._stop = stop  # type: ignore[assignment]
    c._clean_expire_loop()
    assert stop.timeouts == [900.0]
    assert swept == []


def test_sweep_loop_survives_a_failing_pass():
    pass
    c = DefaultMQPushConsumer(GROUP + "_resilient")
    c._started = True
    calls = []

    def _boom():
        calls.append(True)
        raise RuntimeError("sweep blew up")

    c._clean_expired_msg_once = _boom  # type: ignore[method-assign]
    stop = RecordingStop([False, False])
    c._stop = stop  # type: ignore[assignment]
    c._clean_expire_loop()
    assert len(calls) == 2, "the next round must continue after the first round raises"


def test_sweep_thread_is_daemon_and_stops_with_the_flag():
    c = DefaultMQPushConsumer(GROUP + "_thread")
    c._started = True
    c.consume_timeout = 1
    c._start_clean_expire_loop()
    t = c._clean_expire_thread
    assert t is not None and t.is_alive() and t.daemon
    c._stop.set()
    t.join(timeout=3)
    assert not t.is_alive()


# ---------------------------------------------------------------- Core sweep semantics
def test_expired_first_entry_is_sent_back_at_delay_3_then_removed(monkeypatch):
    h = Harness()
    clock = Clock()
    monkeypatch.setattr("client.consumer.time", clock)
    m = stamp(msg(0), int(clock.now_ms) - h.timeout_ms - 1)
    h.inflight([m])

    h.sweep()

    assert h.backed == [(m, 3)], "must send back with the fixed delayLevel 3"
    assert h.record_offsets == [], "send-back succeeded and still at the head → removed"

    h.sweep()
    assert len(h.backed) == 1, "an already-removed one is not seen again in the next round"


def test_unstamped_first_entry_blocks_the_whole_pass(monkeypatch):
    pass
    h = Harness()
    clock = Clock()
    monkeypatch.setattr("client.consumer.time", clock)
    head = msg(0)                                   # unstamped
    tail = stamp(msg(1), int(clock.now_ms) - h.timeout_ms - 1)
    h.inflight([head, tail])

    h.sweep()

    assert h.backed == []
    assert h.record_offsets == [0, 1]


def test_expiry_is_strictly_greater_than_timeout(monkeypatch):
    pass
    h = Harness()
    clock = Clock()
    monkeypatch.setattr("client.consumer.time", clock)
    boundary = stamp(msg(0), int(clock.now_ms) - h.timeout_ms)
    h.inflight([boundary])

    h.sweep()
    assert h.backed == [], "exactly at the threshold must not act"

    clock.now_ms += 1
    h.sweep()
    assert h.backed_offsets == [0], "1ms past the threshold should trigger send-back"


def test_orderly_consumer_is_never_swept():
    pass
    h = Harness(orderly=True)
    m = stamp(msg(0), int(time.time() * 1000) - 3600_000)
    h.inflight([m])

    h.sweep()

    assert h.backed == []
    assert h.record_offsets == [0]


def test_at_most_sixteen_messages_per_pass():
    pass
    h = Harness()
    old = int(time.time() * 1000) - h.timeout_ms - 1
    h.inflight([stamp(msg(i), old) for i in range(20)])

    h.sweep()
    assert h.backed_offsets == list(range(16))
    assert h.record_offsets == list(range(16, 20))

    h.sweep()
    assert h.backed_offsets == list(range(20))
    assert h.record_offsets == []


def test_failed_send_back_keeps_the_message_for_the_next_pass():
    pass
    h = Harness()
    old = int(time.time() * 1000) - h.timeout_ms - 1
    m = stamp(msg(0), old)
    h.inflight([m])
    h.fail_offsets = {0}

    h.sweep()
    assert h.backed == []
    assert h.record_offsets == [0], "a message whose send-back failed must stay put for the next round"

    h.fail_offsets = set()
    h.sweep()
    assert h.backed_offsets == [0]
    assert h.record_offsets == []


def test_sweep_only_looks_at_messages_registered_for_held_queues(monkeypatch):
    pass
    h = Harness()
    clock = Clock()
    monkeypatch.setattr("client.consumer.time", clock)
    m = stamp(msg(0), int(clock.now_ms) - h.timeout_ms - 1)
    h.inflight([m])

    h.c._retire_queue_locked(h.key, None, [])
    h.c._clean_expired_msg_once()

    assert h.backed == [], "once the queue is retired (registration cleared), its messages must not be sent back again"


# ---------------------------------------------------------------- Interaction with the consume path
def test_concurrent_batch_registers_inflight_and_send_back_proceeds(monkeypatch):
    """Control leg: while the message is still registered (not swept), an unacknowledged send-back proceeds normally."""
    h = Harness()
    monkeypatch.setattr("client.consumer.time", Clock())

    class Listener:
        def consume_message(self, msgs, context):
            assert h.c._inflight_msgs[h.key], "this batch must be registered in-flight while the listener runs"
            return ConsumeConcurrentlyStatus.RECONSUME_LATER

    h.c.message_listener = Listener()
    m = msg(0)
    assert h.c._consume_batch(h.key, h.mq, [m]) is True
    assert h.backed == [(m, 3)]
    assert h.c._inflight_msgs.get(h.key) is None, "must be fully deregistered after wrap-up"
    assert h.c._consume_offsets[h.key] == 1


def test_send_back_skips_a_message_the_sweep_already_reclaimed(monkeypatch):
    pass
    h = Harness()
    clock = Clock()
    monkeypatch.setattr("client.consumer.time", clock)
    swept = []

    class Listener:
        def consume_message(self, msgs, context):
            # Simulate "the listener hangs without returning and the sweep acts first": push the clock past the threshold, then trigger the sweep
            clock.now_ms += h.timeout_ms + 1
            h.c._clean_expired_queue(h.key)
            swept.append(h.backed_offsets)
            return ConsumeConcurrentlyStatus.RECONSUME_LATER

    h.c.message_listener = Listener()
    m = msg(0)
    assert h.c._consume_batch(h.key, h.mq, [m]) is True
    assert swept == [[0]], "the sweep during the listener must have already sent it back"
    assert h.backed == [(m, 3)], "only one send-back is allowed along the whole chain"
    assert h.c._consume_offsets[h.key] == 1
    assert h.c._inflight_msgs.get(h.key) is None


def test_sweep_can_reclaim_a_requeued_buffered_message(monkeypatch):
    pass
    h = Harness()
    clock = Clock()
    monkeypatch.setattr("client.consumer.time", clock)
    m = stamp(msg(0), int(clock.now_ms) - h.timeout_ms - 1)
    h.buffered([m])

    h.sweep()

    assert h.backed == [(m, 3)]
    assert list(h.c._pending[h.key]) == []


def test_dispatch_loop_registers_inflight_and_deregisters_on_return():
    """Registration/deregistration of the real dispatch loop: the message is visible while the listener hangs; after return, registration is cleared and the offset advances."""
    c = DefaultMQPushConsumer(GROUP + "_dispatch")
    c._started = True
    mq = MessageQueue(TOPIC, BROKER, 0)
    key = c._mq_key(mq)
    c._mq_map[key] = mq
    c._pending[key] = deque([msg(0)])
    backed = []
    c.send_message_back = lambda m, delay_level, broker_name=None: backed.append(m)  # type: ignore[assignment]
    inside = threading.Event()
    release = threading.Event()

    class Listener:
        def consume_message(self, msgs, context):
            inside.set()
            release.wait(5)
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS

    c.message_listener = Listener()
    t = threading.Thread(target=c._dispatch_loop, daemon=True)
    t.start()
    try:
        assert inside.wait(5), "the listener must be running in the dispatch thread"
        assert [m.queue_offset for m in c._inflight_msgs.get(key) or []] == [0]
    finally:
        release.set()
        deadline = time.time() + 5
        while time.time() < deadline and c._inflight_msgs.get(key):
            time.sleep(0.02)
        c._stop.set()
        t.join(timeout=5)
    assert c._inflight_msgs.get(key) is None
    assert c._consume_offsets[key] == 1
    assert backed == []
