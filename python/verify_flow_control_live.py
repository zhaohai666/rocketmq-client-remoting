#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import sys
import threading
import time

sys.path.insert(0, ".")

from client.consumer import (ConsumeConcurrentlyStatus,
                                      DefaultMQPushConsumer,
                                      MessageListenerConcurrently)
from client.exception import MQBrokerException, MQClientException
from client.mq_client import MQClientInstance
from client.producer import DefaultMQProducer
from common.message import Message

NAMESRV = sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1:9876"
PREFIX = "FcPy_%d" % int(time.time() * 1000)

OFF_COUNT = 65535        # upper bound of pullThresholdForQueue / consumeConcurrentlyMaxSpan
OFF_SIZE_MB = 1024       # upper bound of pullThresholdSizeForQueue (in MiB)

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


def prepare_topic(topic: str, queues: int = 4) -> None:
    """Create the topic first as in real usage: the consumer has no default topic fallback, so with no route it assigns no queues."""
    c = MQClientInstance("fc-setup-%d" % int(time.time() * 1000), [NAMESRV])
    c.start()
    try:
        c.create_topic_in_route(topic, queues, queues)
    finally:
        c.shutdown()


class Recorder(MessageListenerConcurrently):
    """Record the arriving message bodies; optionally sleep slow seconds per batch (creating a "pulled-but-not-consumed" backlog)."""

    def __init__(self, slow: float = 0.0):
        self.slow = slow
        self.bodies = []
        self._lock = threading.Lock()

    def consume_message(self, msgs, context):
        if self.slow > 0:
            time.sleep(self.slow)
        with self._lock:
            self.bodies.extend(bytes(m.body) for m in msgs)
        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS

    def count(self, prefix: bytes) -> int:
        with self._lock:
            return sum(1 for b in self.bodies if b.startswith(prefix))

    def snapshot(self):
        with self._lock:
            return list(self.bodies)


def make_consumer(group: str, topic: str, listener: Recorder,
                  **thresholds) -> DefaultMQPushConsumer:
    c = DefaultMQPushConsumer(group)
    c.set_namesrv_addr(NAMESRV)
    c.set_message_listener(listener)
    for k, v in thresholds.items():
        setattr(c, k, v)
    c.subscribe(topic, "*")
    c.start()
    return c


def wait_until(pred, timeout: float, interval: float = 0.5) -> bool:
    end = time.time() + timeout
    while time.time() < end:
        if pred():
            return True
        time.sleep(interval)
    return False


def run_gate_case(label: str, producer: DefaultMQProducer, topic: str, group: str,
                  msgs: list, listener_slow: float, thresholds: dict,
                  settle: float = 25.0) -> DefaultMQPushConsumer:
    """Run one gate scenario: return the consumer; the caller is responsible for assertions and shutdown."""
    rec = Recorder(listener_slow)
    c = make_consumer(group, topic, rec, **thresholds)
    # Wait for the assignment to settle (the consumer must first be registered with the broker; the rebalance cycle is 20s, shortened to 2s during startup)
    time.sleep(3)
    for m in msgs:
        producer.send(m)
    # Hitting the gate needs the backlog to build up; consuming everything needs the gate to release. Leave settle seconds
    time.sleep(settle)
    c.recorder = rec
    return c


def big_body(tag: int) -> bytes:
    """400KB of incompressible message body.

    It must be random bytes: for a body above the compression threshold the producer first tries to compress, and an all-same-byte payload compresses down to
    a few hundred bytes, so the broker's on-disk storeSize becomes a few hundred bytes too (the first time this verification ran, a 400KB
    b"F"*… measured only 640B, so the size gate "never hitting" was caused by the fixture being compressible, not an implementation bug).
    The prefix is kept so messages can be identified individually.
    """
    return b"FC%04d" % tag + os.urandom(400 * 1024 - 6)


def main() -> int:
    producer = DefaultMQProducer(PREFIX + "_pg")
    producer.set_namesrv_addr(NAMESRV)
    producer.start()
    time.sleep(1)

    MiB = 1024 * 1024
    big = big_body

    # ---------- S0 default gates + fast consumption: should not hit ----------
    t0 = PREFIX + "_Defaults"
    prepare_topic(t0)
    c0 = run_gate_case(
        "S0", producer, t0, PREFIX + "_g0",
        [Message(t0, (b"ok-%d" % i)) for i in range(12)],
        listener_slow=0.0, thresholds={})
    got0 = c0.recorder.snapshot()
    check("S0-default gates do not hit", c0._flow_control_triggered == 0,
          "triggered=%d" % c0._flow_control_triggered)
    check("S0-all arrived under the default gates", len(got0) == 12, "got=%d" % len(got0))
    check("S0-none duplicated or lost", sorted(got0) == sorted(b"ok-%d" % i for i in range(12)),
          "distinct=%d" % len(set(got0)))
    c0.shutdown()

    t1 = PREFIX + "_Size"
    prepare_topic(t1, queues=1)
    c1 = run_gate_case(
        "S1", producer, t1, PREFIX + "_g1",
        [Message(t1, big(i)) for i in range(8)],
        listener_slow=0.3,
        thresholds={"pull_threshold_for_queue": OFF_COUNT,
                    "pull_threshold_size_for_queue": 1,
                    "consume_concurrently_max_span": OFF_COUNT})
    got1 = c1.recorder.snapshot()
    check("S1-the queue-level byte gate really hits live", c1._flow_control_triggered > 0,
          "triggered=%d" % c1._flow_control_triggered)
    check("S1-no large message lost", len(got1) == 8, "got=%d" % len(got1))
    check("S1-all 8 400KB messages are distinct", len(set(got1)) == 8, "distinct=%d" % len(set(got1)))
    c1.shutdown()

    # ---------- S2 offset span gate ----------
    # Both the count and byte gates are pushed to not hit (their upper bounds), leaving only the span.
    t2 = PREFIX + "_Span"
    prepare_topic(t2)
    c2 = run_gate_case(
        "S2", producer, t2, PREFIX + "_g2",
        [Message(t2, (b"s-%d" % i)) for i in range(14)],
        listener_slow=0.3,
        thresholds={"pull_threshold_for_queue": OFF_COUNT,
                    "pull_threshold_size_for_queue": OFF_SIZE_MB,
                    "consume_concurrently_max_span": 2})
    got2 = c2.recorder.snapshot()
    check("S2-the span gate really hits live", c2._flow_control_triggered > 0,
          "triggered=%d" % c2._flow_control_triggered)
    check("S2-everything is still consumed after the span is exceeded", len(got2) == 14, "got=%d" % len(got2))
    c2.shutdown()

    # ---------- S3 topic-level count knob: divided into the per-queue quota ----------
    # The queue-level count knob is set to its upper bound, so no queue could pause on its own config: only the
    # rebalance division of the topic knob (topic threshold // queues this instance owns) can bring the count gate
    # down to a value the slow consumer actually exceeds. Asserting the divided value plus the hit proves the
    # division runs live and that the gate enforces the quotient rather than any cross-queue sum.
    t3 = PREFIX + "_Topic"
    prepare_topic(t3, queues=4)
    topic_count_threshold = 8
    c3 = run_gate_case(
        "S3", producer, t3, PREFIX + "_g3",
        [Message(t3, (b"t-%d" % i)) for i in range(16)],
        listener_slow=0.3,
        thresholds={"pull_threshold_for_queue": OFF_COUNT,
                    "pull_threshold_size_for_queue": OFF_SIZE_MB,
                    "consume_concurrently_max_span": OFF_COUNT,
                    "pull_threshold_for_topic": topic_count_threshold})
    got3 = c3.recorder.snapshot()
    owned = len(c3._mq_map)
    expected_per_queue = max(1, topic_count_threshold // owned) if owned else OFF_COUNT
    check("S3-the topic knob was divided into the per-queue quota on assignment change",
          owned > 0 and c3.pull_threshold_for_queue == expected_per_queue,
          "owned=%d topic=%d perQueue=%d expected=%d"
          % (owned, topic_count_threshold, c3.pull_threshold_for_queue, expected_per_queue))
    check("S3-the divided per-queue quota really hits live", c3._flow_control_triggered > 0,
          "triggered=%d" % c3._flow_control_triggered)
    check("S3-everything is still consumed after the divided quota pauses the pull", len(got3) == 16, "got=%d" % len(got3))
    c3.shutdown()

    # ---------- S4 a queue that hit flow control resumes consuming after recovery ----------
    # Reuse S1's group and topic (the offsets were already committed to the broker's tail by S1): keep the gates as they were and send a fresh batch of messages.
    # This locks in the bug where "pause 100ms" was written as "exit the pull loop" -- that queue would stall permanently,
    # while the messages already consumed in S1 would show no difference at all.
    rec4 = Recorder(0.3)
    c4 = make_consumer(PREFIX + "_g1", t1, rec4,
                       pull_threshold_for_queue=OFF_COUNT,
                       pull_threshold_size_for_queue=1,
                       consume_concurrently_max_span=OFF_COUNT)
    time.sleep(3)
    for i in range(6):
        producer.send(Message(t1, big(100 + i)))
    time.sleep(25)
    got4 = rec4.snapshot()
    check("S4-the queue that triggered flow control resumes consuming after recovery", len(got4) == 6, "got=%d" % len(got4))
    check("S4-the recovery batch still hits flow control (the gate does not fail after one hit)",
          c4._flow_control_triggered > 0, "triggered=%d" % c4._flow_control_triggered)
    check("S4-the recovery batch has no duplicates", len(set(got4)) == 6, "distinct=%d" % len(set(got4)))
    c4.shutdown()

    t5 = PREFIX + "_Config"
    prepare_topic(t5)
    legal = {
        "consume_thread_min": 1, "consume_thread_max": 2,
        "consume_concurrently_max_span": OFF_COUNT,
        "pull_threshold_for_queue": OFF_COUNT,
        "pull_threshold_for_topic": -1,
        "pull_threshold_size_for_queue": OFF_SIZE_MB,
        "pull_threshold_size_for_topic": -1,
        "pull_interval": 0,
        "consume_message_batch_max_size": 1,
        "pull_batch_size": 1024,
        "pop_invisible_time": 300000,     # MAX_POP_INVISIBLE_TIME
        "pop_batch_nums": 32,
    }
    rec5 = Recorder(0.0)
    c5 = make_consumer(PREFIX + "_g5", t5, rec5, **legal)
    check("S5-the boundary config can start", c5._started is True)
    time.sleep(3)
    for i in range(10):
        producer.send(Message(t5, b"c-%d" % i))
    wait_until(lambda: len(rec5.snapshot()) >= 10, 20)
    got5 = rec5.snapshot()
    check("S5-all 10 arrive under the boundary config",
          sorted(got5) == sorted(b"c-%d" % i for i in range(10)),
          "got=%d distinct=%d" % (len(got5), len(set(got5))))

    # 2) out-of-range config: rejected locally + this group is not found on the broker side
    rejected = [
        ({"pull_threshold_size_for_queue": 0},
         "pullThresholdSizeForQueue Out of range [1, 1024]"),
        ({"pull_batch_size": 1025}, "pullBatchSize Out of range [1, 1024]"),
        ({"pop_invisible_time": 4999}, "popInvisibleTime Out of range [5000, 300000]"),
        ({"pop_batch_nums": 33}, "popBatchNums Out of range [1, 32]"),
        ({"consume_thread_min": 8, "consume_thread_max": 4},
         "consumeThreadMin (8) is larger than consumeThreadMax (4)"),
    ]
    bad_group = PREFIX + "_g6"
    for overrides, want in rejected:
        c6 = DefaultMQPushConsumer(bad_group)
        c6.set_namesrv_addr(NAMESRV)
        c6.set_message_listener(Recorder(0.0))
        for k, v in overrides.items():
            setattr(c6, k, v)
        c6.subscribe(t5, "*")
        try:
            c6.start()
            check("S5-out-of-range config rejected: %s" % want, False, "start() actually succeeded")
            c6.shutdown()
            continue
        except MQClientException as e:
            check("S5-out-of-range config rejected: %s" % want, str(e) == want, "actual=%s" % str(e))
        check("S5-out-of-range config leaves no half-started instance: %s" % want,
              c6._started is False and c6._mq_client is None)

    probe = MQClientInstance("fc-probe-%d" % int(time.time() * 1000), [NAMESRV])
    probe.start()
    try:
        addr = probe.find_broker_addr_by_topic(t5)
        check("S5-got the broker address for querying the consume group", addr is not None, "addr=%s" % addr)
        if addr:
            # A never-registered group: the broker's GET_CONSUMER_LIST_BY_GROUP does not return an empty list, but instead
            # throws GROUP_NOT_EXIST directly -- which is equally evidence that "the broker does not know this group". Both shapes are
            # accepted, but it must be "no such group" and must never return any clientId.
            try:
                bad_ids = probe.get_consumer_list_by_group(bad_group, addr=addr).consumer_id_list
                bad_absent = not bad_ids
                detail = "ids=%s" % bad_ids
            except MQBrokerException as e:
                bad_absent = True
                detail = "broker rejected directly: code=%d %s" % (e.response_code, e.error_message)
            check("S5-the broker side does not know the rejected consume group", bad_absent, detail)
            ok_ids = probe.get_consumer_list_by_group(PREFIX + "_g5", addr=addr)
            check("S5-the broker side accepted the boundary-value consumer",
                  len(ok_ids.consumer_id_list) == 1, "ids=%s" % ok_ids.consumer_id_list)
    finally:
        c5.shutdown()
        probe.shutdown()

    producer.shutdown()
    print("flow control live: %d PASS / %d FAIL" % (PASS, FAIL))
    return 0 if FAIL == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
