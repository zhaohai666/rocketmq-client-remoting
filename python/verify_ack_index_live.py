#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import annotations

import sys
import threading
import time

sys.path.insert(0, ".")

from client.consumer import (ConsumeConcurrentlyStatus,
                                      DefaultMQPushConsumer,
                                      MessageListenerConcurrently)
from client.mq_client import MQClientInstance
from client.producer import DefaultMQProducer
from common.message import Message
from remoting.protocol.heartbeat import ConsumeFromWhere, MessageModel

NAMESRV = sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1:9876"
PREFIX = "AckPy_%d" % int(time.time() * 1000)

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


def new_client(tag: str) -> MQClientInstance:
    c = MQClientInstance("%s-%d" % (tag, int(time.time() * 1000)), [NAMESRV])
    c.start()
    return c


def prepare_topic(topic: str, queues: int = 1) -> None:
    """Create the topic. Queue count defaults to 1: some ack cases require "3 messages in one batch on the same queue and contiguous",
    with multiple queues the messages scatter and batch membership becomes indeterminate (in real usage the topic is pre-created by an admin)."""
    c = new_client("setup")
    try:
        c.create_topic_in_route(topic, queues, queues)
    finally:
        c.shutdown()


def wait_until(pred, timeout: float, interval: float = 1.0) -> bool:
    """Poll until the condition holds (all broker-side reconciliation is polling, never a fixed sleep: new topic registration is second-scale)."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        if pred():
            return True
        time.sleep(interval)
    return pred()


def committed_offset(group: str, topic: str) -> int:
    """Read the sum of committed offsets for this group across the topic's queues on the broker.

    A single query timing out must not crash the whole verification: treat it here as "not read yet" (-1), and let the caller's
    wait_until keep polling."""
    c = new_client("reader")
    try:
        publish = c.get_topic_publish_info(topic)
        if publish is None or not publish.msg_queue_list:
            return -1
        total = 0
        for mq in publish.msg_queue_list:
            off = c.query_consumer_offset(group, mq)
            total += off or 0
        return total
    except Exception:
        return -1
    finally:
        c.shutdown()


class RecordingListener(MessageListenerConcurrently):
    """Record per batch, and (optionally) write ackIndex into the context."""

    def __init__(self, ack_index=None, status=ConsumeConcurrentlyStatus.CONSUME_SUCCESS,
                 ack_first_batch_only=False):
        self.ack_index = ack_index
        self.status = status
        self.ack_first_batch_only = ack_first_batch_only
        self.batches = []
        self._lock = threading.Lock()

    def consume_message(self, msgs, context):
        with self._lock:
            self.batches.append([(bytes(m.body), m.topic, m.get_reconsume_times(),
                                  time.time()) for m in msgs])
            index = len(self.batches)
        if self.ack_index is not None:
            # Only narrow down the first batch: the batch that comes back from redelivery must be acknowledged as a whole, otherwise it never converges
            if not self.ack_first_batch_only or index == 1:
                context.ack_index = self.ack_index
        return self.status

    def arrivals(self):
        with self._lock:
            return [r for batch in self.batches for r in batch]


def start(group: str, topic: str, listener, batch_max: int = 3,
          model: str = None, from_first: bool = False) -> DefaultMQPushConsumer:
    """`from_first=True` is for the "messages first, consumer later" cases: under LAST_OFFSET a new group starts from
    the latest offset at assignment time, so the messages sent earlier are skipped outright."""
    c = DefaultMQPushConsumer(group)
    c.set_namesrv_addr(NAMESRV)
    c.set_message_listener(listener)
    c.set_consume_thread_nums(1)
    c.consume_message_batch_max_size = batch_max
    if model is not None:
        c.set_message_model(model)
    if from_first:
        c.set_consume_from_where(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET)
    c.subscribe(topic, "*")
    c.start()
    return c


def main() -> int:
    producer = DefaultMQProducer(PREFIX + "_pg")
    producer.set_namesrv_addr(NAMESRV)
    producer.start()
    time.sleep(1)

    bodies = [b"ack-%d" % i for i in range(3)]

    # ---------- A1 control group: whole-batch acknowledgement by default ----------
    t1 = PREFIX + "_Full"
    g1 = PREFIX + "_g1"
    prepare_topic(t1)
    l1 = RecordingListener()
    c1 = start(g1, t1, l1)
    time.sleep(3)
    for b in bodies:
        producer.send(Message(t1, b))
    print("A1: control group sent, waiting for consumption...")
    wait_until(lambda: len(l1.arrivals()) >= 3, 30)
    time.sleep(3)
    arrivals1 = l1.arrivals()
    check("A1-all 3 delivered once each", sorted(r[0] for r in arrivals1) == sorted(bodies),
          "arrivals=%d" % len(arrivals1))
    check("A1-no redeliveries at all", all(r[2] == 0 for r in arrivals1),
          "times=%s" % sorted(r[2] for r in arrivals1))
    check("A1-offset committed to 3 (equivalent to a whole-batch ack)",
          wait_until(lambda: committed_offset(g1, t1) == 3, 20),
          "offset=%d" % committed_offset(g1, t1))
    c1.shutdown()

    # ---------- A2 partial ack: only the first message of the first batch is acknowledged ----------
    t2 = PREFIX + "_Partial"
    g2 = PREFIX + "_g2"
    prepare_topic(t2)
    l2 = RecordingListener(ack_index=0, ack_first_batch_only=True)
    # Send the 3 first, then start the consumer: how the batch is split depends on pull timing, so sending first necessarily yields "one whole batch of 3";
    # otherwise the first batch could be 1 or 2 messages and the number of tail messages withheld by ackIndex=0 would be indeterminate.
    for b in bodies:
        producer.send(Message(t2, b))
    c2 = start(g2, t2, l2, from_first=True)
    print("A2: sent, waiting for the send-back after partial ack (delay level3≈10s)...")
    # First batch of 3 -> the 2-message tail is sent back to %RETRY% -> redelivered after the broker revives;
    # the %RETRY% topic is only created on the first send-back, and the consumer must wait another rebalance round, so give it a generous window.
    wait_until(lambda: len(l2.arrivals()) >= 5, 90)
    time.sleep(3)
    arrivals2 = l2.arrivals()
    first_batch = [r[0] for r in l2.batches[0]] if l2.batches else []
    acked = first_batch[:1]
    tail = first_batch[1:]
    check("A2-first batch really got 3 messages", len(first_batch) == 3, "batch=%s" % first_batch)
    check("A2-the acknowledged message was delivered only once across the whole window",
          bool(acked) and [r[0] for r in arrivals2].count(acked[0]) == 1,
          "acked=%s" % acked)
    redelivered = [r for r in arrivals2 if r[0] in tail and r[2] >= 1]
    check("A2-the two unacknowledged messages arrived a second time via %RETRY%",
          len(tail) == 2 and {r[0] for r in redelivered} == set(tail),
          "tail=%s redelivered=%d" % (tail, len(redelivered)))
    check("A2-redelivered messages have reconsumeTimes>=1 and the topic is restored to the business topic",
          bool(redelivered) and all(r[1] == t2 for r in redelivered),
          "topics=%s" % sorted({r[1] for r in redelivered}))
    check("A2-all 3 were eventually consumed (none lost)",
          {r[0] for r in arrivals2} == set(bodies),
          "distinct=%d" % len({r[0] for r in arrivals2}))
    check("A2-business queue offset still commits to 3 (the tail is left to broker redelivery, no need to pin it)",
          wait_until(lambda: committed_offset(g2, t2) == 3, 20),
          "offset=%d" % committed_offset(g2, t2))
    c2.shutdown()

    # ---------- A3 RECONSUME_LATER overrides ackIndex ----------
    t3 = PREFIX + "_LaterWins"
    g3 = PREFIX + "_g3"
    prepare_topic(t3)
    l3 = RecordingListener(ack_index=2, status=ConsumeConcurrentlyStatus.RECONSUME_LATER)
    for b in bodies:
        producer.send(Message(t3, b))
    c3 = start(g3, t3, l3, from_first=True)
    print("A3: sent, waiting for whole-batch redelivery...")
    # Each message must be redelivered at least once; a whole-batch RECONSUME_LATER keeps redelivering,
    # so only assert that "all 3 have appeared with reconsumeTimes>=1" and do not compare total counts.
    check("A3-ackIndex=2 cannot block RECONSUME_LATER either (all 3 redelivered)",
          wait_until(lambda: {r[0] for r in l3.arrivals() if r[2] >= 1} == set(bodies), 60),
          "redelivered=%d/%d" % (len({r[0] for r in l3.arrivals() if r[2] >= 1}), len(bodies)))
    c3.shutdown()

    # ---------- A4 broadcast mode: the tail is not sent back ----------
    t4 = PREFIX + "_Broadcast"
    g4 = PREFIX + "_g4"
    prepare_topic(t4)
    l4 = RecordingListener(ack_index=0, ack_first_batch_only=True)
    for b in bodies:
        producer.send(Message(t4, b))
    c4 = start(g4, t4, l4, model=MessageModel.BROADCASTING, from_first=True)
    print("A4: broadcast mode sent, waiting for consumption...")
    wait_until(lambda: len(l4.arrivals()) >= 3, 30)
    time.sleep(12)  # If there were a send-back it would arrive after level3≈10s; give a generous window here to confirm it does not
    arrivals4 = l4.arrivals()
    check("A4-broadcast: the unacknowledged tail is not sent back (each arrives only once)",
          sorted(r[0] for r in arrivals4) == sorted(bodies)
          and all(r[2] == 0 for r in arrivals4),
          "arrivals=%d times=%s" % (len(arrivals4), sorted({r[2] for r in arrivals4})))
    c4.shutdown()

    producer.shutdown()
    print("\nRESULT: %d PASS / %d FAIL" % (PASS, FAIL))
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(main())
