#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Live verification of topic queue-set change listening (register_topic_message_queue_change_listener).

Usage (requires a local RocketMQ 5.5.1 cluster):
    .venv/bin/python verify_lite_topic_queue_change_live.py 127.0.0.1:9876

Why test this separately: queue-change listening relies on "asking nameserver for the subscribed queue set on every pass",
while the ordinary route cache only refreshes every 30s. A unit test with a fake nameserver cannot prove this --
this script squeezes the check interval down to 1s, keeps the route poll at its default 30s, then really scales the topic up and measures two things:
  L1 silence: when the queues have not moved, the listener must not be bothered repeatedly;
  L2 live query: after the background passes its first-check delay and enters the stable 1s-per-pass phase, scale up and require that from "nameserver has
      reported the new queue count" to "the listener receives the callback" only one or two checks pass (≤5s). If the comparison pass read the
      30s route cache, this window would drag out past half a minute;
  L3 convergence: after the callback the snapshot advances, and the same queue set does not trigger another callback;
  L4 scale down: putting the queue count back also reports only once, and reports the real set as it stands;
  L5 a never-registered topic: failing to get any queues always counts as "not found", never disguised as having shrunk to 0 queues.
"""
from __future__ import annotations

import sys
import threading
import time

sys.path.insert(0, ".")

from client.consumer import DefaultLitePullConsumer, DefaultMQPullConsumer
from client.exception import MQClientException
from client.producer import DefaultMQProducer

NAMESRV = sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1:9876"
STAMP = int(time.time() * 1000)
TOPIC = "LiteQcLive_%d" % STAMP
GROUP = "LiteQcG_%d" % STAMP
GHOST = "LiteQcGhost_%d" % STAMP
QUEUE_NUM = 2
SCALED_NUM = 4
CHECK_INTERVAL_MS = 1000          # squeezed to the floor: so "live route queries" and "cache eating" are separable in time
FIRST_CHECK_DELAY_S = 10.0        # the background comparison's first-check delay (matches the default in the implementation)
CACHE_DISCRIMINATOR_S = 5.0       # at most a few seconds from nameserver seeing the change to the callback (the stable phase is one pass per 1s)
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


class Recorder:
    """Records only (topic, queueId list) and does not mutate state; background threads call it too, so it carries its own lock."""

    def __init__(self):
        self.events = []
        self._lock = threading.Lock()

    def on_changed(self, topic, message_queues):
        with self._lock:
            self.events.append((topic, sorted(mq.queue_id for mq in message_queues)))

    @property
    def events_snapshot(self):
        with self._lock:
            return list(self.events)


def scale_read_queues(group_tag: str, queue_num: int) -> None:
    """Send another create-topic request for the same topic to change the read/write queue counts to queue_num."""
    prod = DefaultMQProducer("PG_Qc_%s_%d" % (group_tag, STAMP))
    prod.set_namesrv_addr(NAMESRV)
    prod.start()
    try:
        prod.create_topic("TBW102", TOPIC, queue_num)
    finally:
        prod.shutdown()


def routable_queue_num(timeout: int = 30) -> int:
    """Poll until the topic really has queues on nameserver, returning the queue count at that point."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        probe = DefaultMQPullConsumer("PG_QcProbe_%d" % STAMP)
        probe.set_namesrv_addr(NAMESRV)
        probe.start()
        try:
            qs = probe.fetch_subscribe_message_queues(TOPIC)
            if qs:
                return len(qs)
        except BaseException:
            pass
        finally:
            probe.shutdown()
        time.sleep(1)
    raise RuntimeError("topic %s not routable after %ds" % (TOPIC, timeout))


def wait_queue_num(consumer: DefaultLitePullConsumer, expect: int,
                   timeout: float = 45.0) -> tuple:
    """Wait until nameserver reports expect queues (fetch_message_queues queries live on every pass)."""
    start = time.monotonic()
    deadline = start + timeout
    while time.monotonic() < deadline:
        try:
            if len(consumer.fetch_message_queues(TOPIC)) == expect:
                return True, time.monotonic() - start
        except MQClientException:
            pass
        time.sleep(0.25)
    return False, time.monotonic() - start


def wait_callback(rec: Recorder, expect: list, timeout: float = 45.0) -> tuple:
    start = time.monotonic()
    deadline = start + timeout
    while time.monotonic() < deadline:
        if rec.events_snapshot == expect:
            return True, time.monotonic() - start
        time.sleep(0.25)
    return False, time.monotonic() - start


def main() -> int:
    print("nameserver = %s, topic = %s" % (NAMESRV, TOPIC))

    # ---- create the topic (2 queues) and wait for the route
    prod = DefaultMQProducer("PG_QcCreate_%d" % STAMP)
    prod.set_namesrv_addr(NAMESRV)
    prod.start()
    try:
        prod.create_topic("TBW102", TOPIC, QUEUE_NUM)
    finally:
        prod.shutdown()
    seen = routable_queue_num()
    check("L0 the topic is created and its route is visible", seen == QUEUE_NUM, "queueNum=%d" % seen)

    c1 = DefaultLitePullConsumer(GROUP)
    c1.set_namesrv_addr(NAMESRV)
    c1.instance_name = "qc-live"
    c1.set_topic_metadata_check_interval_millis(CHECK_INTERVAL_MS)
    check("L0 the check interval reaches the 1s floor",
          c1.get_topic_metadata_check_interval_millis() == CHECK_INTERVAL_MS)
    c1.subscribe(TOPIC, "*")
    c1.start()
    loop_start = time.monotonic()

    try:
        # Registering at runtime ⇒ a snapshot is taken immediately ⇒ the first round should not callback
        rec = Recorder()
        c1.register_topic_message_queue_change_listener(TOPIC, rec)
        time.sleep(3.0)
        check("L1 the queues have not moved ⇒ silent (the snapshot eats the first round)",
              rec.events_snapshot == [], "events=%s" % rec.events_snapshot)

        # The first check has a fixed 10s delay: wait for it to pass so the background enters the stable 1s-per-pass phase.
        # After this pass it is still silent, which also proves that "registering at runtime = having a snapshot" holds on live hardware.
        wait = FIRST_CHECK_DELAY_S + 2.0 - (time.monotonic() - loop_start)
        if wait > 0:
            time.sleep(wait)
        check("L1b the first-check pass really ran ⇒ still silent (the snapshot is not treated as a change)",
              rec.events_snapshot == [], "events=%s" % rec.events_snapshot)

        # ---- L2 scale up 2 → 4: first wait for nameserver to acknowledge, then require the listener to be only a few checks away
        scale_read_queues("up", SCALED_NUM)
        seen_ns, t_ns = wait_queue_num(c1, SCALED_NUM)
        check("L2a nameserver reports 4 queues", seen_ns, "after=%.1fs" % t_ns)
        if seen_ns:
            got_cb, t_cb = wait_callback(rec, [(TOPIC, list(range(SCALED_NUM)))])
            check("L2b the comparison pass queries the route live (the callback follows nameserver, no waiting for the 30s cache)",
                  got_cb and t_cb <= CACHE_DISCRIMINATOR_S,
                  "callback=%.1fs after nameserver" % t_cb)

        # ---- L3 convergence: the same queue set no longer bothers us again
        time.sleep(3.0)
        check("L3 the snapshot advances after the callback ⇒ no repeated callbacks",
              len(rec.events_snapshot) == 1, "events=%s" % rec.events_snapshot)

        # ---- L4 scale down 4 → 2
        scale_read_queues("down", QUEUE_NUM)
        seen_ns, t_ns = wait_queue_num(c1, QUEUE_NUM)
        check("L4a nameserver reports back 2 queues", seen_ns, "after=%.1fs" % t_ns)
        if seen_ns:
            got_cb, t_cb = wait_callback(
                rec, [(TOPIC, list(range(SCALED_NUM))), (TOPIC, list(range(QUEUE_NUM)))])
            check("L4b the scale down is also seen via a live route query",
                  got_cb and t_cb <= CACHE_DISCRIMINATOR_S,
                  "callback=%.1fs after nameserver" % t_cb)

        # ---- L5 a never-created topic: an empty queue set counts as "not found"
        raised = ""
        try:
            c1.fetch_message_queues(GHOST)
        except MQClientException as e:
            raised = str(e)
        check('L5 fetching queues for an unknown topic throws "not found" instead of returning empty',
              "Namesrv return empty" in raised, raised)
    finally:
        c1.shutdown()

    print("\nLitePull queue-change live: PASS=%d FAIL=%d" % (PASS, FAIL))
    return 0 if FAIL == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
