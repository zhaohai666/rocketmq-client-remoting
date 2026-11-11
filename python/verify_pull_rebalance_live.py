#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Live verification of the pull-side rebalance (Java `DefaultMQPullConsumer.enableRebalance`).

Java's pull consumer rebalances: `DefaultMQPullConsumer.java:97` defaults
`enableRebalance` to **true**, and the shared `RebalanceService` thread drives
`tryRebalance()` every `rocketmq.client.rebalance.waitInterval` (20s). That allocates
`mq_all` among the group's clients and calls
`MessageQueueListener.messageQueueChanged(topic, mqAll, mqDivided)` **when the allocation
changed**.

What a live cluster can prove that a unit test cannot:
  L1  two instances of one group really split the topic's queues -- disjoint sets whose
      union is the whole topic -- which only holds if both read the same broker-side
      consumer list and run the same allocation formula;
  L2  the listener really fires with `mq_all` = the topic's full queue set (the allocation
      is not invented locally);
  L3  `enableRebalance=false` really stops the loop: no callbacks, empty table, while
      `fetchMessageQueuesInBalance` keeps answering by computing on the spot.

Single-broker cluster, one topic with several queues -- enough for all three: the split is
across *queues* within one broker, not across brokers.
"""
from __future__ import annotations

import sys
import threading
import time

sys.path.insert(0, ".")

from client.consumer import DefaultMQPullConsumer, MessageQueueListener
from client.mq_client import MQClientInstance

NAMESRV = sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1:9876"
PREFIX = "PullReb_%d" % int(time.time() * 1000)
N_QUEUE = 8
# Shorter than Java's 20s so the probe finishes quickly; the loop shape is what matters.
WAIT_MILLIS = 3000
MIN_MILLIS = 500

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


def prepare_topic(topic: str, queues: int) -> None:
    """Create the topic up front: a consumer gets no route for an uncreated topic
    (TBW102 is a producer-side fallback only)."""
    c = MQClientInstance("%s-setup-%d" % (PREFIX, int(time.time() * 1000)), [NAMESRV])
    c.start()
    try:
        c.create_topic_in_route(topic, queues, queues)
    finally:
        c.shutdown()


def until(pred, timeout: float) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if pred():
            return True
        time.sleep(0.2)
    return pred()


class Recorder(MessageQueueListener):
    """Records `messageQueueChanged(topic, mqAll, mqDivided)` calls."""

    def __init__(self) -> None:
        self.calls = []
        self._lock = threading.Lock()

    def message_queue_changed(self, topic, mq_all, mq_divided):
        with self._lock:
            self.calls.append((topic, set(mq_all), set(mq_divided)))

    def count(self) -> int:
        with self._lock:
            return len(self.calls)

    def last(self):
        with self._lock:
            return self.calls[-1] if self.calls else None


def make_consumer(instance_name: str, group: str, topic: str,
                  recorder: Recorder = None, enable: bool = True) -> DefaultMQPullConsumer:
    c = DefaultMQPullConsumer(group)
    # A distinct instanceName gives a distinct clientId; the clientId is what the broker's
    # GET_CONSUMER_LIST_BY_GROUP returns, so without this both facades would be one client.
    c.instance_name = instance_name
    c.set_namesrv_addr(NAMESRV)
    c.set_rebalance_wait_millis(WAIT_MILLIS)
    c.set_rebalance_min_interval_millis(MIN_MILLIS)
    if recorder is not None:
        c.register_message_queue_listener(topic, recorder)
    else:
        c.register_topics.add(topic)
    c.set_enable_rebalance(enable)
    return c


def main() -> int:
    topic = PREFIX + "Topic"
    group = PREFIX + "Group"
    print("%s: pull-side rebalance live (ns=%s, topic=%s, %d queues)"
          % (PREFIX, NAMESRV, topic, N_QUEUE))
    prepare_topic(topic, N_QUEUE)
    print("  created %s with %d queues" % (topic, N_QUEUE))

    rec_a, rec_b = Recorder(), Recorder()
    a = make_consumer(PREFIX + "InstA", group, topic, rec_a)
    b = make_consumer(PREFIX + "InstB", group, topic, rec_b)
    c = None
    try:
        a.start()
        b.start()
        print("  started two instances, waiting for the first rebalance rounds...")

        # L1: both must end up with a non-empty share of the topic (each instance owns
        # half of 8 queues under the averagely strategy).
        got = until(lambda: bool(a.get_rebalance_table().get(topic))
                    and bool(b.get_rebalance_table().get(topic)), 30)
        share_a = a.get_rebalance_table().get(topic) or []
        share_b = b.get_rebalance_table().get(topic) or []
        print("  INFO share A=%d queues, share B=%d queues"
              % (len(share_a), len(share_b)))
        check("L1-both instances ended up with a share of the topic", got,
              "A=%d B=%d" % (len(share_a), len(share_b)))

        ids_a = {(q.broker_name, q.queue_id) for q in share_a}
        ids_b = {(q.broker_name, q.queue_id) for q in share_b}
        check("L1-the two shares are disjoint (one queue is never owned twice)",
              ids_a.isdisjoint(ids_b), "overlap=%d" % len(ids_a & ids_b))
        check("L1-the two shares cover every queue of the topic exactly once",
              ids_a | ids_b == {("broker-a", i) for i in range(N_QUEUE)},
              "union=%d of %d" % (len(ids_a | ids_b), N_QUEUE))

        # L2: the listener really ran, and mq_all was the topic's full set -- proof that the
        # allocation was computed against the real route, not invented locally.
        check("L2-the allocation change was reported to the listener",
              rec_a.count() >= 1 and rec_b.count() >= 1,
              "A calls=%d B calls=%d" % (rec_a.count(), rec_b.count()))
        last_a = rec_a.last()
        check("L2-messageQueueChanged carried the topic's full queue set as mqAll",
              last_a is not None and len(last_a[1]) == N_QUEUE,
              "mqAll=%d" % (len(last_a[1]) if last_a else -1))
        # The recorder keeps MessageQueue objects; compare by identity, not by object.
        divided_ids = ({(q.broker_name, q.queue_id) for q in last_a[2]}
                       if last_a is not None else set())
        check("L2-mqDivided matches what this instance reports as its share",
              divided_ids == ids_a,
              "mqDivided=%d table=%d" % (len(divided_ids), len(ids_a)))

        # The table is the background rebalance's product, so fetch_message_queues_in_balance
        # must now answer from it (Java reads the process-queue keys).
        in_balance = a.fetch_message_queues_in_balance(topic)
        check("L1-fetch_message_queues_in_balance answers from the rebalance table",
              {(q.broker_name, q.queue_id) for q in in_balance} == ids_a,
              "got=%d expect=%d" % (len(in_balance), len(ids_a)))

        # L3: the switch really gates the loop.
        rec_c = Recorder()
        c = make_consumer(PREFIX + "InstC", group, topic, rec_c, enable=False)
        c.start()
        print("  started a third instance with enableRebalance=false, waiting 2 rounds...")
        time.sleep(WAIT_MILLIS * 2 / 1000.0 + 4)
        check("L3-enableRebalance=false stops the loop: no callback at all",
              rec_c.count() == 0, "calls=%d" % rec_c.count())
        check("L3-enableRebalance=false leaves the rebalance table empty",
              c.get_rebalance_table() == {}, "table=%r" % (c.get_rebalance_table(),))
        on_the_spot = c.fetch_message_queues_in_balance(topic)
        check("L3-with the loop off, fetch_message_queues_in_balance still computes on the spot",
              len(on_the_spot) > 0, "got=%d queues" % len(on_the_spot))
    finally:
        for consumer in (c, b, a):
            if consumer is not None:
                try:
                    consumer.shutdown()
                except Exception as e:  # noqa: BLE001
                    print("  (shutdown ignored: %s)" % e)

    print("\n%s: %d passed, %d failed" % (PREFIX, PASS, FAIL))
    return 0 if FAIL == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
