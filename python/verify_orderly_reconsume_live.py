#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import annotations

import sys
import threading
import time

sys.path.insert(0, ".")

from client.consumer import (ConsumeOrderlyStatus,
                                      DefaultLitePullConsumer,
                                      DefaultMQPushConsumer,
                                      MessageListenerOrderly)
from client.mq_client import MQClientInstance
from client.producer import DefaultMQProducer
from common.message import Message
from common.mix_all import MixAll
from remoting.protocol.heartbeat import ConsumeFromWhere

NAMESRV = sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1:9876"
PREFIX = "OrdPy_%d" % int(time.time() * 1000)

PASS = 0
FAIL = 0


def check(name: str, ok: bool, detail: str = "") -> None:
    global PASS, FAIL
    if ok:
        PASS += 1
        print("  [PASS] %s%s" % (name, ("  " + detail) if detail else ""))
    else:
        FAIL += 1
        print("  [FAIL] %s%s" % (name, ("  " + detail) if detail else ""))


def median(xs):
    s = sorted(xs)
    n = len(s)
    if not n:
        return 0.0
    return s[n // 2] if n % 2 else (s[n // 2 - 1] + s[n // 2]) / 2.0


class SleepRecorder:
    """Record the suspend-sleep duration that the client **itself requested** on the dispatch
    thread (a live observation with no timing noise).

    Why intercept ``time.sleep`` instead of computing the delivery interval: after ``_consume_batch``
    returns False the dispatch loop still sleeps a fixed 0.05 before going back to the head of the
    queue, so "the interval between two deliveries" = the suspend duration + ~50ms of polling. The
    measured median is 63ms with a jitter of +-10ms, which means the difference between a 10ms and a
    1ms suspend does not fall inside that noise (this is exactly why an earlier version's O3 clamping
    criterion reported falsely). Recording the duration the client requested makes whether the
    clamping takes effect a deterministic fact.

    The thread name filter (``rmq-dispatch-``) keeps only the dispatch thread; the sleeps of the
    other threads pass through untouched. ``cap`` is only used for cases such as "asks for 30s", where
    it squeezes the real wait down to 20ms -- what is asserted is **how long the client asked for**,
    which is unrelated to whether it really slept that long (really sleeping 30s would only make the
    test case 30s slower).
    """

    def __init__(self, cap: float = None):
        self.durations = []
        self.cap = cap
        self._real = time.sleep

    def __enter__(self):
        rec = self

        def patched(sec):
            if threading.current_thread().name.startswith("rmq-dispatch-"):
                rec.durations.append(sec)
                if rec.cap is not None and sec > rec.cap:
                    sec = rec.cap
            return rec._real(sec)

        time.sleep = patched
        return self

    def __exit__(self, *exc):
        time.sleep = self._real
        return False

    def asked(self, seconds: float) -> bool:
        """Whether the client ever requested a sleep of exactly ``seconds`` seconds."""
        return any(abs(d - seconds) < 1e-9 for d in self.durations)

    def others(self):
        """The requested values other than the fixed ticks of the dispatch loop (0.05 polling / 0.1 retry / 0.2 send-back cooldown)."""
        return sorted({round(d, 4) for d in self.durations
                       if all(abs(d - x) > 1e-9 for x in (0.05, 0.1, 0.2))})


def prepare_topic(topic: str, queues: int = 1) -> None:
    """Create the topic. The queue count is fixed at 1: orderly consumption is after "a
    deterministic order inside one queue", and with multiple queues the poison message and the
    ordinary message after it would scatter into different queues, making the difference between
    holding and releasing impossible to see."""
    c = MQClientInstance("ord-setup-%d" % int(time.time() * 1000), [NAMESRV])
    c.start()
    try:
        c.create_topic_in_route(topic, queues, queues)
    finally:
        c.shutdown()


def read_dlq(group: str, timeout: float = 25.0):
    """Read %DLQ%<group> from the head using lite pull; returns (body, reconsumeTimes, topic, RETRY_TOPIC).

    It must read from FIRST_OFFSET: the DLQ topic is only created by the broker at the moment it
    delivers the dead letter, and the LAST_OFFSET that a new group defaults to starts at the tail as
    of the moment of subscribing, which skips exactly the entry that just went in => a false failure."""
    dlq = MixAll.get_dlq_topic(group)
    c = DefaultLitePullConsumer(PREFIX + "_dlqreader")
    c.set_namesrv_addr(NAMESRV)
    c.set_consume_from_where(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET)
    c.subscribe(dlq, "*")
    c.start()
    out = []
    try:
        deadline = time.time() + timeout
        while time.time() < deadline:
            for m in c.poll(3000) or []:
                out.append((bytes(m.body), m.get_reconsume_times(), m.topic,
                            (m.properties or {}).get("RETRY_TOPIC")))
            if out:
                break
            time.sleep(1)
    finally:
        c.shutdown()
    return dlq, out


def broker_offsets(group: str, topic: str):
    """[(mq, committed or None)]: the consumed offsets of that group over the queues of this topic
    on the broker side.

    The offsets are flushed asynchronously on the client side (the consumer's own persistence loop
    flushes once, and shutdown flushes once), so the caller has to wait with a bounded poll instead
    of querying once and asserting (``committed=None`` means the broker has no record at all, which
    differs from the record itself being 0)."""
    c = MQClientInstance("ord-off-%d" % int(time.time() * 1000), [NAMESRV])
    c.start()
    try:
        pub = c.get_topic_publish_info(topic)
        if pub is None or not pub.msg_queue_list:
            return None
        rows = []
        for mq in pub.msg_queue_list:
            try:
                rows.append((mq, c.query_consumer_offset(
                    group, mq, set_zero_if_not_found=False)))
            except Exception:  # noqa: BLE001
                rows.append((mq, None))
        return rows
    finally:
        c.shutdown()


def wait_offsets(group: str, topic: str, want, timeout: float = 20.0):
    """Poll the broker offsets with a bound until ``want(rows)`` holds; returns the last observation."""
    deadline = time.time() + timeout
    rows = None
    while time.time() < deadline:
        rows = broker_offsets(group, topic)
        if rows and want(rows):
            return rows
        time.sleep(1)
    return rows


class PoisonListener(MessageListenerOrderly):
    """The poison message keeps being suspended (it returns SUSPEND every time and never
    "succeeds"), while the rest succeed as usual.

    Only body/reconsumeTimes are recorded: orderly consumption is single-threaded and batch by
    batch, so no lock is needed, but after the consumer restarts the listener instance is reused,
    hence this accumulates the whole round of observations in a list."""

    def __init__(self, poison: bytes):
        self.poison = poison
        self.records = []
        self._lock = threading.Lock()

    def consume_message(self, msgs, context):
        with self._lock:
            for m in msgs:
                self.records.append((bytes(m.body), m.get_reconsume_times(), m.topic))
        if any(bytes(m.body) == self.poison for m in msgs):
            return ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT
        return ConsumeOrderlyStatus.SUCCESS

    def deliveries(self, poison: bytes):
        with self._lock:
            return [r for r in self.records if r[0] == poison]

    def bodies(self):
        with self._lock:
            return [r[0] for r in self.records]


class TimedSuspendListener(MessageListenerOrderly):
    """The poison message is suspended forever and the suspend duration in the context is changed to
    ``asked_ms``; the moment of every delivery is recorded.

    The criterion is the **interval between two adjacent deliveries**: the suspension on this port is
    exactly putting this batch back at the head of the queue and sleeping for that duration, so the
    interval directly reflects the number of milliseconds that was parsed (reading the consumer
    configuration by mistake or missing the clamp would both show up in the interval)."""

    def __init__(self, poison: bytes, asked_ms):
        self.poison = poison
        self.asked_ms = asked_ms
        self.times = []

    def consume_message(self, msgs, context):
        if self.asked_ms is not None:
            context.suspend_current_queue_time_millis = self.asked_ms
        if any(bytes(m.body) == self.poison for m in msgs):
            self.times.append(time.time())
            return ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT
        return ConsumeOrderlyStatus.SUCCESS

    def gaps(self):
        return [b - a for a, b in zip(self.times, self.times[1:])]


class ManualHoldListener(MessageListenerOrderly):
    pass

    def __init__(self, hold_calls: int):
        self.hold_calls = hold_calls
        self.calls = 0
        self.records = []          # [(body, auto_commit)]

    def consume_message(self, msgs, context):
        self.calls += 1
        auto = self.calls > self.hold_calls
        context.auto_commit = auto
        for m in msgs:
            self.records.append((bytes(m.body), auto))
        return ConsumeOrderlyStatus.SUCCESS

    def first_index(self, body: bytes):
        for i, r in enumerate(self.records):
            if r[0] == body:
                return i
        return -1


class ExplicitCommitListener(MessageListenerOrderly):
    pass

    def __init__(self):
        self.batches = []          # [[(body, reconsumeTimes, topic), ...], ...]

    def consume_message(self, msgs, context):
        context.auto_commit = False
        self.batches.append([(bytes(m.body), m.get_reconsume_times(), m.topic)
                             for m in msgs])
        return ConsumeOrderlyStatus.COMMIT


class ExplicitRollbackListener(MessageListenerOrderly):
    pass

    def __init__(self, rollback_calls: int, head: bytes):
        self.rollback_calls = rollback_calls
        self.head = head
        self.records = []          # [(body, reconsumeTimes, topic, time)]
        self.head_calls = 0
        self._lock = threading.Lock()

    def consume_message(self, msgs, context):
        context.auto_commit = False
        now = time.time()
        hit = any(bytes(m.body) == self.head for m in msgs)
        with self._lock:
            for m in msgs:
                self.records.append((bytes(m.body), m.get_reconsume_times(),
                                     m.topic, now))
            if hit:
                self.head_calls += 1
            n = self.head_calls
        if hit and n <= self.rollback_calls:
            return ConsumeOrderlyStatus.ROLLBACK
        return ConsumeOrderlyStatus.COMMIT

    def head_records(self):
        with self._lock:
            return [r for r in self.records if r[0] == self.head]

    def first_index(self, body: bytes):
        with self._lock:
            for i, r in enumerate(self.records):
                if r[0] == body:
                    return i
            return -1

    def count(self, body: bytes):
        with self._lock:
            return len([r for r in self.records if r[0] == body])

    def nth_index(self, body: bytes, n: int):
        """The position of the n-th (0-based) delivery in the whole delivery stream; returns -1 when there are fewer than n+1."""
        with self._lock:
            hits = [i for i, r in enumerate(self.records) if r[0] == body]
        return hits[n] if len(hits) > n else -1


class IllegalRollbackListener(MessageListenerOrderly):
    pass

    def __init__(self):
        self.records = []
        self._lock = threading.Lock()

    def consume_message(self, msgs, context):
        with self._lock:
            for m in msgs:
                self.records.append((bytes(m.body), m.get_reconsume_times(), m.topic))
        return ConsumeOrderlyStatus.ROLLBACK

    def count(self, body: bytes):
        with self._lock:
            return len([r for r in self.records if r[0] == body])

    def first_index(self, body: bytes):
        with self._lock:
            for i, r in enumerate(self.records):
                if r[0] == body:
                    return i
            return -1


def main() -> int:
    producer = DefaultMQProducer(PREFIX + "_pg")
    producer.set_namesrv_addr(NAMESRV)
    producer.start()
    time.sleep(1)

    # ---------- O1 once the count is reached it is handed to the broker and the business queue moves on ----------
    topic1 = PREFIX + "_OrdDlq"
    group1 = PREFIX + "_g1"
    prepare_topic(topic1)
    listener1 = PoisonListener(b"ord-poison")
    c1 = DefaultMQPushConsumer(group1)
    c1.set_namesrv_addr(NAMESRV)
    c1.set_message_listener(listener1)
    c1.consume_message_batch_max_size = 1
    c1.max_reconsume_times = 2      # an explicit small value: the default is unlimited, which would never settle it
    c1.suspend_current_queue_time_millis = 500
    c1.subscribe(topic1, "*")
    c1.start()
    time.sleep(5)   # wait for the first LOCK_BATCH_MQ; only then does the broker side count that "this group holds an unexpired lock"
    sent1 = producer.send(Message(topic1, b"ord-poison"))
    producer.send(Message(topic1, b"ord-after"))
    print("O1: the poison message was sent msgId=%s, waiting for the local count to reach its limit..." % sent1.msg_id)

    # A local retry only sleeps 500ms per round (it does not go through the broker), so three
    # deliveries take about 1.5s; the ordinary message after it is only pulled once the offset
    # advances. The 90s window is the contention margin for running four live test cases in parallel
    # on the same machine.
    deadline = time.time() + 90
    while time.time() < deadline and b"ord-after" not in listener1.bodies():
        time.sleep(1)
    time.sleep(3)   # the counter-proof window: the poison message must not come back any more
    poison1 = listener1.deliveries(b"ord-poison")
    times1 = [p[1] for p in poison1]
    check("O1 with maxReconsumeTimes=2 the poison message is delivered 3 times locally (reconsumeTimes 0/1/2)",
          times1 == [0, 1, 2], "times=%s" % times1)
    check("O1 after the send-back succeeds the business queue moves on (the ordinary message after it is consumed)",
          b"ord-after" in listener1.bodies(), "bodies=%s" % listener1.bodies())
    check("O1 the poison message is not sent back any more (only 3 deliveries inside the observation window)", len(poison1) == 3,
          "arrivals=%d" % len(poison1))
    check("O1 the topic stays the business topic throughout the deliveries (orderly retry does not go through %RETRY%)",
          all(p[2] == topic1 for p in poison1),
          "topics=%s" % sorted({p[2] for p in poison1}))
    lock_ok = len(c1._lock_ok)
    check("O1 the group holds the broker queue lock (the criterion for a direct dead-letter delivery)", lock_ok > 0, "lockOK=%d" % lock_ok)
    c1.shutdown()

    dlq1, got1 = read_dlq(group1)
    check("O1 the poison message landed in %DLQ%<group>", [g[0] for g in got1] == [b"ord-poison"],
          "dlq=%s got=%s" % (dlq1, [(g[0].decode(), g[1]) for g in got1]))
    # The client bumps the reconsumeTimes in the header to 3 (+1 on send-back) and the broker stores it as is
    check("O1 the DLQ message has reconsumeTimes=3", len(got1) == 1 and got1[0][1] == 3,
          "got=%s" % got1)
    check("O1 the DLQ message keeps RETRY_TOPIC = the business topic",
          len(got1) == 1 and got1[0][3] == topic1, "got=%s" % got1)
    check("O1 the topic of the DLQ message is exactly %DLQ%<group>",
          len(got1) == 1 and got1[0][2] == dlq1, "got=%s" % got1)
    print("O1: the original msgId=%s, the DLQ observation=%s" % (sent1.msg_id, got1))

    topic2 = PREFIX + "_OrdUnlimited"
    group2 = PREFIX + "_g2"
    prepare_topic(topic2)
    listener2 = PoisonListener(b"ord-keep")
    c2 = DefaultMQPushConsumer(group2)
    c2.set_namesrv_addr(NAMESRV)
    c2.set_message_listener(listener2)
    c2.consume_message_batch_max_size = 1
    # max_reconsume_times keeps the default -1
    c2.suspend_current_queue_time_millis = 200
    c2.subscribe(topic2, "*")
    c2.start()
    time.sleep(4)
    producer.send(Message(topic2, b"ord-keep"))
    print("O2: the poison message with the default (-1) configuration was sent, watching whether it is declared dead early...")
    deadline = time.time() + 30
    while time.time() < deadline and len(listener2.deliveries(b"ord-keep")) < 25:
        time.sleep(1)
    n2 = len(listener2.deliveries(b"ord-keep"))
    check("O2 with the default configuration it keeps retrying in place (far beyond the 16 of the concurrent side)", n2 > 16, "deliveries=%d" % n2)
    check("O2 the local count keeps advancing (0..n continuous)",
          [p[1] for p in listener2.deliveries(b"ord-keep")][:20] == list(range(20)),
          "times=%s" % [p[1] for p in listener2.deliveries(b"ord-keep")][:20])
    c2.shutdown()
    dlq2, got2 = read_dlq(group2, timeout=12)
    check("O2 there should be no dead letter before the threshold is reached (%s is empty)" % dlq2, got2 == [], "got=%s" % got2)

    topic3 = PREFIX + "_OrdSuspendCtx"
    group3 = PREFIX + "_g3"
    prepare_topic(topic3)
    listener3 = TimedSuspendListener(b"ord-slow", asked_ms=70)   # 70 differs from the polling ticks 50/100/200
    c3 = DefaultMQPushConsumer(group3)
    c3.set_namesrv_addr(NAMESRV)
    c3.set_message_listener(listener3)
    c3.consume_message_batch_max_size = 1
    c3.max_reconsume_times = -1
    c3.suspend_current_queue_time_millis = 900
    c3.subscribe(topic3, "*")
    rec3 = SleepRecorder()
    with rec3:
        c3.start()
        time.sleep(4)
        producer.send(Message(topic3, b"ord-slow"))
        deadline = time.time() + 25
        while time.time() < deadline and len(listener3.times) < 9:
            time.sleep(0.5)
    gaps3 = listener3.gaps()
    med3 = median(gaps3)
    check("O3 the 70ms from the context takes effect (not the 900ms configured on the consumer)",
          len(gaps3) >= 6 and 0.03 <= med3 <= 0.4,
          "n=%d median=%.3fs gaps=%s" % (len(gaps3), med3, [round(g, 3) for g in gaps3[:5]]))
    check("O3 what the client requested is the 0.07s of the context, not the configured 0.9s",
          rec3.asked(0.07), "requested=%s" % rec3.others())
    c3.shutdown()

    topic3b = PREFIX + "_OrdSuspendFloor"
    group3b = PREFIX + "_g3b"
    prepare_topic(topic3b)
    listener3b = TimedSuspendListener(b"ord-fast", asked_ms=1)   # 1ms -> clamped to 10ms
    c3b = DefaultMQPushConsumer(group3b)
    c3b.set_namesrv_addr(NAMESRV)
    c3b.set_message_listener(listener3b)
    c3b.consume_message_batch_max_size = 1
    c3b.suspend_current_queue_time_millis = 0
    c3b.subscribe(topic3b, "*")
    rec3b = SleepRecorder()
    with rec3b:
        c3b.start()
        time.sleep(4)
        producer.send(Message(topic3b, b"ord-fast"))
        deadline = time.time() + 20
        while time.time() < deadline and len(listener3b.times) < 9:
            time.sleep(0.5)
    gaps3b = listener3b.gaps()
    med3b = median(gaps3b)
    check("O3 the clamp floor: a context of 1ms and a configuration of 0 both run as 10ms (a missing clamp means a busy wait)",
          rec3b.asked(0.01), "requested=%s" % rec3b.others())
    check("O3 the clamp floor really waited (the interval cannot be shorter than 10ms)",
          len(gaps3b) >= 6 and med3b >= 0.009,
          "n=%d median=%.4fs gaps=%s"
          % (len(gaps3b), med3b, [round(g, 4) for g in gaps3b[:5]]))
    c3b.shutdown()

    # ---------- O3c the clamp ceiling: asking for 40s -> only waiting 30s ----------
    # The criterion takes the duration the client requested, as above: really waiting 30s would only make
    # the test case half a minute slower, so the real wait is squeezed down to 20ms here.
    topic3c = PREFIX + "_OrdSuspendCeil"
    group3c = PREFIX + "_g3c"
    prepare_topic(topic3c)
    listener3c = TimedSuspendListener(b"ord-ceil", asked_ms=40000)
    c3c = DefaultMQPushConsumer(group3c)
    c3c.set_namesrv_addr(NAMESRV)
    c3c.set_message_listener(listener3c)
    c3c.consume_message_batch_max_size = 1
    c3c.subscribe(topic3c, "*")
    rec3c = SleepRecorder(cap=0.02)
    with rec3c:
        c3c.start()
        time.sleep(4)
        producer.send(Message(topic3c, b"ord-ceil"))
        deadline = time.time() + 20
        while time.time() < deadline and len(listener3c.times) < 5:
            time.sleep(0.5)
    check("O3 the clamp ceiling: asking for 40s runs as 30s only",
          len(listener3c.times) >= 2 and rec3c.asked(30.0),
          "n=%d requested=%s" % (len(listener3c.times), rec3c.others()))
    c3c.shutdown()

    topic4 = PREFIX + "_OrdManual"
    group4 = PREFIX + "_g4"
    prepare_topic(topic4)
    listener4 = ManualHoldListener(hold_calls=3)
    c4 = DefaultMQPushConsumer(group4)
    c4.set_namesrv_addr(NAMESRV)
    c4.set_message_listener(listener4)
    c4.consume_message_batch_max_size = 1
    c4.suspend_current_queue_time_millis = 200
    c4.subscribe(topic4, "*")
    c4.start()
    time.sleep(4)
    producer.send(Message(topic4, b"ord-hold"))
    producer.send(Message(topic4, b"ord-after-hold"))
    deadline = time.time() + 40
    while time.time() < deadline and listener4.first_index(b"ord-after-hold") < 0:
        time.sleep(0.5)
    held = [r for r in listener4.records if not r[1]]
    after_idx = listener4.first_index(b"ord-after-hold")
    check("O4 with autoCommit=false a SUCCESS does not ack back (the same message is delivered repeatedly)",
          len(held) >= 2 and all(r[0] == b"ord-hold" for r in held),
          "held=%d bodies=%s" % (len(held), [r[0].decode() for r in listener4.records]))
    check("O4 after being let through the offset advances: the next one is consumed and the order stays intact",
          after_idx >= 0 and after_idx > listener4.first_index(b"ord-hold")
          and listener4.records[after_idx][1] is True,
          "afterIdx=%d auto=%s" % (after_idx,
                                   listener4.records[after_idx][1] if after_idx >= 0 else None))
    c4.shutdown()

    # ---------- O5 autoCommit=false + COMMIT = an explicit batch ack ----------
    # The three messages are **sent before the consumer starts** (a new group explicitly from the
    # head, the same convention as the ackIndex test cases) and the batch limit is 3: the first round
    # is necessarily the complete three, which gives "acknowledge the whole batch at once" a
    # deterministic surface to observe. Three criteria: the first round has exactly three elements,
    # the whole window delivers only once (if the COMMIT branch were missing, the suspend rhythm would
    # redeliver dozens of times) and the broker offset advances straight to 3.
    topic5 = PREFIX + "_OrdCommit"
    group5 = PREFIX + "_g5"
    prepare_topic(topic5)
    listener5 = ExplicitCommitListener()
    c5 = DefaultMQPushConsumer(group5)
    c5.set_namesrv_addr(NAMESRV)
    c5.set_message_listener(listener5)
    c5.consume_message_batch_max_size = 3
    c5.set_consume_from_where(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET)
    c5.suspend_current_queue_time_millis = 200
    c5.subscribe(topic5, "*")
    producer.send(Message(topic5, b"ack-1"))
    producer.send(Message(topic5, b"ack-2"))
    producer.send(Message(topic5, b"ack-3"))
    c5.start()
    deadline = time.time() + 30
    while time.time() < deadline and len(listener5.batches) < 1:
        time.sleep(0.5)
    time.sleep(6)      # the counter-proof window: an implementation that treats COMMIT as "do not commit" would redeliver ~30 times in these 6s
    c5.shutdown()
    offs5 = wait_offsets(group5, topic5, lambda rows: all(o == 3 for _, o in rows))
    got5 = [[b[0] for b in batch] for batch in listener5.batches]
    check("O5 the first round is a complete three-element batch (sent before starting + batch limit 3)",
          len(listener5.batches) == 1 and len(listener5.batches[0]) == 3,
          "batches=%s" % got5)
    check("O5 the explicit COMMIT acknowledges the whole batch: delivered once, none of the three redelivered",
          got5 == [[b"ack-1", b"ack-2", b"ack-3"]], "batches=%s" % got5)
    check("O5 the broker offset advances straight to 3 (a whole-batch ack, not stuck at 0)",
          offs5 is not None and all(o == 3 for _, o in offs5),
          "offsets=%s" % [o for _, o in offs5] if offs5 else "routes=none")

    # ---------- O6 autoCommit=false + ROLLBACK = an explicit rollback with an immediate local redelivery ----------
    # Roll back 6 times and then commit explicitly. Each of the first 6 times pushes this entry
    # back and it **does not go through the broker**: the interval between adjacent deliveries hugs
    # the 200ms suspend (an implementation that goes through %RETRY% could at best wait for the
    # broker's 10s level), reconsumeTimes stays 0 and the topic stays the business topic; only the
    # 7th commit lets the next one through (it does not overtake even once during the rollbacks).
    topic6 = PREFIX + "_OrdRollback"
    group6 = PREFIX + "_g6"
    prepare_topic(topic6)
    listener6 = ExplicitRollbackListener(rollback_calls=6, head=b"rb-head")
    c6 = DefaultMQPushConsumer(group6)
    c6.set_namesrv_addr(NAMESRV)
    c6.set_message_listener(listener6)
    c6.consume_message_batch_max_size = 1
    c6.suspend_current_queue_time_millis = 200
    c6.subscribe(topic6, "*")
    c6.start()
    time.sleep(4)
    producer.send(Message(topic6, b"rb-head"))
    producer.send(Message(topic6, b"rb-next"))
    deadline = time.time() + 45
    while time.time() < deadline and (listener6.head_calls < 7
                                      or listener6.first_index(b"rb-next") < 0):
        time.sleep(0.5)
    head6 = listener6.head_records()
    gaps6 = [b[3] - a[3] for a, b in zip(head6, head6[1:])]
    med6 = median(gaps6)
    idx7 = listener6.nth_index(b"rb-head", 6)
    idx_next = listener6.first_index(b"rb-next")
    check("O6 the explicit rollback pushes the same batch back for redelivery (head delivered 7 times = 6 rollbacks + 1 commit)",
          len(head6) >= 7, "deliveries=%d" % len(head6))
    check("O6 the local redelivery does not go through the broker: the adjacent intervals hug the 200ms suspend (%RETRY% is at best the 10s level)",
          len(gaps6) >= 5 and med6 < 1.0 and max(gaps6) < 5.0,
          "n=%d median=%.3fs max=%.3fs gaps=%s"
          % (len(gaps6), med6, max(gaps6) if gaps6 else -1.0,
             [round(g, 3) for g in gaps6[:6]]))
    check("O6 the local redelivery does not touch reconsumeTimes (all 0) and does not change the topic",
          bool(head6) and all(r[1] == 0 for r in head6)
          and all(r[2] == topic6 for r in head6),
          "times=%s topics=%s" % ([r[1] for r in head6[:8]],
                                  sorted({r[2] for r in head6})))
    check("O6 the following message does not overtake during the rollbacks (next first appears after the 7th delivery)",
          idx7 >= 0 and idx_next > idx7, "idx7=%d idxNext=%d" % (idx7, idx_next))
    time.sleep(2)      # the counter-proof window: after the commit the head must not come back
    check("O6 after the explicit commit each is delivered once (the commit really released the head)",
          listener6.count(b"rb-head") == 7 and listener6.count(b"rb-next") == 1,
          "head=%d next=%d" % (listener6.count(b"rb-head"),
                               listener6.count(b"rb-next")))
    c6.shutdown()
    offs6 = wait_offsets(group6, topic6, lambda rows: all(o == 2 for _, o in rows))
    check("O6 the broker offset advances to 2 (both messages acked)",
          offs6 is not None and all(o == 2 for _, o in offs6),
          "offsets=%s" % [o for _, o in offs6] if offs6 else "routes=none")

    topic7 = PREFIX + "_OrdIllegal"
    group7 = PREFIX + "_g7"
    prepare_topic(topic7)
    listener7 = IllegalRollbackListener()
    c7 = DefaultMQPushConsumer(group7)
    c7.set_namesrv_addr(NAMESRV)
    c7.set_message_listener(listener7)
    c7.consume_message_batch_max_size = 1
    c7.suspend_current_queue_time_millis = 200
    c7.subscribe(topic7, "*")
    c7.start()
    time.sleep(4)
    producer.send(Message(topic7, b"il-head"))
    producer.send(Message(topic7, b"il-next"))
    deadline = time.time() + 30
    while time.time() < deadline and listener7.first_index(b"il-next") < 0:
        time.sleep(0.5)
    time.sleep(2.5)    # the counter-proof window: if ROLLBACK really were honoured, the head would be redelivered ~10 times in these 2.5s
    check("O7 with autoCommit=true ROLLBACK is treated as an ack: the head is delivered only once",
          listener7.count(b"il-head") == 1, "head=%d" % listener7.count(b"il-head"))
    check("O7 an illegal ROLLBACK does not block the queue: next is consumed immediately",
          listener7.first_index(b"il-next") >= 0,
          "idxNext=%d next=%d" % (listener7.first_index(b"il-next"),
                                  listener7.count(b"il-next")))
    c7.shutdown()
    offs7 = wait_offsets(group7, topic7, lambda rows: all(o == 2 for _, o in rows))
    check("O7 the broker offset advances to 2 (the illegal usage counts as a successful ack)",
          offs7 is not None and all(o == 2 for _, o in offs7),
          "offsets=%s" % [o for _, o in offs7] if offs7 else "routes=none")

    producer.shutdown()
    print("\nPASS=%d FAIL=%d" % (PASS, FAIL))
    return 0 if FAIL == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
