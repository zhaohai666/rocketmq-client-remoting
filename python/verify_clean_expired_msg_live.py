#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import annotations

import sys
import threading
import time

sys.path.insert(0, ".")

from client.consumer import DefaultMQPushConsumer, MessageListenerConcurrently
from client.consumer_result import ConsumeConcurrentlyStatus
from client.mq_client import MQClientInstance
from client.producer import DefaultMQProducer
from common.message import Message, MessageQueue

NAMESRV = sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1:9876"
PREFIX = "CleanExpPy_%d" % int(time.time() * 1000)

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


def committed_offset(group: str, topic: str) -> int:
    c = new_client("reader")
    try:
        publish = c.get_topic_publish_info(topic)
        if publish is None or not publish.msg_queue_list:
            return -1
        total = 0
        for mq in publish.msg_queue_list:
            total += c.query_consumer_offset(group, mq) or 0
        return total
    except Exception:
        return -1
    finally:
        c.shutdown()


def wait_until(pred, timeout: float, interval: float = 1.0) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if pred():
            return True
        time.sleep(interval)
    return pred()


class HungListener(MessageListenerConcurrently):
    """Hang on the first delivery without returning; after release, return CONSUME_SUCCESS normally."""

    def __init__(self):
        self.lock = threading.Lock()
        self.calls = 0
        self.arrivals = []          # [(body, reconsume_times, moment)]
        self.first_seen = threading.Event()
        self.release = threading.Event()

    def consume_message(self, msgs, context):
        with self.lock:
            self.calls += 1
            first = self.calls == 1
            for m in msgs:
                self.arrivals.append((bytes(m.body), m.get_reconsume_times(), time.time()))
        if first:
            self.first_seen.set()
            self.release.wait(300)   # hang window: wait for the sweeper to act + this script to release
        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS

    def times(self, body: bytes):
        with self.lock:
            return [(t, when) for b, t, when in self.arrivals if b == body]


def business_queue(c: DefaultMQPushConsumer, topic: str):
    """Pick the business queue out of the assignment (exclude the auto-subscribed %RETRY%<group>)."""
    return next((q for q in c._assigned_queues() if q.topic == topic), None)


def retry_buffered(c: DefaultMQPushConsumer, body: bytes) -> bool:
    """Whether this body sits in the %RETRY% queue's local buffer (the pull loop is independent of the hung consume thread)."""
    for key, dq in list(c._pending.items()):
        if not key.startswith("%RETRY%"):
            continue
        if any(bytes(m.body) == body for m in dq):
            return True
    return False


def main() -> int:
    topic = PREFIX + "_T"
    group = PREFIX + "_g"
    setup = new_client("setup")
    try:
        setup.create_topic_in_route(topic, 1, 1)
    finally:
        setup.shutdown()

    producer = DefaultMQProducer(PREFIX + "_pg")
    producer.set_namesrv_addr(NAMESRV)
    producer.start()
    time.sleep(1)

    listener = HungListener()
    c = DefaultMQPushConsumer(group)
    c.set_namesrv_addr(NAMESRV)
    c.set_message_listener(listener)
    c.consume_timeout = 1
    c.subscribe(topic, "*")
    body = b"ce-1"
    c.start()

    try:
        mq = business_queue(c, topic) if wait_until(
            lambda: business_queue(c, topic) is not None, 30) else None
        check("A0-business queue assigned (%RETRY% queue excluded)", mq is not None, "mq=%s" % mq)
        if mq is None:
            return 1
        key = c._mq_key(mq)

        # ---------- A1 baseline: the first delivery hangs ----------
        producer.send(Message(topic, body))
        check("A1-first delivery arrived and hung within 30s", listener.first_seen.wait(30),
              "calls=%d" % listener.calls)
        first = listener.times(body)
        check("A1-first delivery reconsumeTimes=0", first and first[0][0] == 0,
              "times=%s" % first)
        first_at = first[0][1] if first else time.time()
        check("A1-message registered as in-flight while hung (in the listener's hands)",
              any(bytes(m.body) == body for m in c._inflight_msgs.get(key) or []),
              "inflight=%s" % [bytes(m.body) for m in c._inflight_msgs.get(key) or []])

        # ---------- A2 sweep hit: the listener is still hung, the entry is already gone from the registry ----------
        t0 = time.time()
        swept = wait_until(
            lambda: not [m for m in c._inflight_msgs.get(key) or []], 210, 1.0)
        elapsed = time.time() - t0
        check("A2-sweep took this message while the listener was still hung (~start+120s)",
              swept and not listener.release.is_set(),
              "elapsed=%.1fs calls=%d" % (elapsed, listener.calls))
        check("A2-taken later than one sweep threshold (>60s, rules out other paths acting)", elapsed > 60,
              "elapsed=%.1fs" % elapsed)

        # ---------- A3 the send-back really reached the broker: it appears in the %RETRY% buffer ----------
        check("A3-send-back message appears in the %RETRY% queue's local buffer (the broker really received sendMessageBack)",
              wait_until(lambda: retry_buffered(c, body), 40),
              "retry_keys=%s" % [k for k in c._pending if k.startswith("%RETRY%")])

        # ---------- A4 release: the redelivery is consumed again ----------
        listener.release.set()
        check("A4-reconsumed after release (reconsumeTimes=1)",
              wait_until(lambda: len(listener.times(body)) >= 2, 60),
              "times=%s" % [t for t, _ in listener.times(body)])
        times = listener.times(body)
        check("A4-second delivery reconsumeTimes=1 (broker-side redelivery count)",
              len(times) >= 2 and times[1][0] == 1, "times=%s" % [t for t, _ in times])
        check("A4-the same body arrived exactly twice overall (one send-back, no duplicate delivery)",
              len(times) == 2, "times=%s" % [t for t, _ in times])
        check("A4-second delivery >60s after the first (not a redelivery caused by the listener itself)",
              len(times) >= 2 and times[1][1] - first_at > 60,
              "gap=%.1fs" % ((times[1][1] - first_at) if len(times) >= 2 else -1))

        # ---------- A5 offset wrap-up ----------
        check("A5-after the hung listener returns, the business queue offset reaches 1 (removeMessage includes swept entries)",
              wait_until(lambda: committed_offset(group, topic) == 1, 30),
              "offset=%d" % committed_offset(group, topic))
    finally:
        listener.release.set()
        try:
            c.shutdown()
        except Exception:  # noqa: BLE001
            pass
        try:
            producer.shutdown()
        except Exception:  # noqa: BLE001
            pass

    print("\nCleanExpiredMsg: PASS=%d FAIL=%d" % (PASS, FAIL))
    return 0 if FAIL == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
