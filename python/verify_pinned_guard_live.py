#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import annotations

import sys
import time

sys.path.insert(0, ".")

from client.consumer import DefaultMQPushConsumer, SimpleMessageListener
from client.consumer_result import ConsumeConcurrentlyStatus
from client.exception import MQClientException
from client.producer import DefaultMQProducer, SendCallbackImpl
from client.send_result import SendStatus
from common.message import Message, MessageQueue
from remoting.protocol.heartbeat import ConsumeFromWhere

NAMESRV = sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1:9876"
STAMP = int(time.time() * 1000)
TOPIC = "PinnedGuardLive_%d" % STAMP
OTHER = "PinnedGuardLiveOther_%d" % STAMP
GROUP = "GID_PinnedGuardLive_%d" % STAMP
NS = "ns1"
WTOPIC = "%s%%%s" % (NS, TOPIC)
# The upper bound on the duration of the negative leg: the guard is a pure string comparison, so
# 50ms on a real machine already leaves two orders of magnitude of margin
LOCAL_BUDGET_MS = 50.0
SYNC_WORDING = "message's topic not equal mq's topic"
ASYNC_WORDING = "Topic of the message does not match its target message queue"

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


def wait_route(prod: DefaultMQProducer, topic: str, min_queues: int,
               timeout: int = 20) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            if len(prod.fetch_publish_message_queues(topic)) >= min_queues:
                return True
        except MQClientException:
            pass
        time.sleep(0.5)
    return False


def wait_until(fn, timeout: float = 10.0, step: float = 0.05) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if fn():
            return True
        time.sleep(step)
    return fn()


def wait_offset(prod: DefaultMQProducer, mq: MessageQueue, want: int,
                timeout: float = 5.0) -> int:
    """Wait until maxOffset reaches want and then return (the broker dispatches the ConsumeQueue a few milliseconds slower than the SEND reply)."""
    wait_until(lambda: prod.max_offset(mq) >= want, timeout)
    return prod.max_offset(mq)


def queue_zero(prod: DefaultMQProducer, topic: str) -> MessageQueue:
    for q in prod.fetch_publish_message_queues(topic):
        if q.queue_id == 0:
            return q
    raise AssertionError("topic %s has no queue 0" % topic)


def timed_send(prod: DefaultMQProducer, msg, mq: MessageQueue):
    """Run one pinned synchronous send and return (the exception or None, the elapsed milliseconds)."""
    began = time.monotonic()
    try:
        prod.send(msg, 3000, mq)
        return None, (time.monotonic() - began) * 1000.0
    except MQClientException as e:
        return e, (time.monotonic() - began) * 1000.0


def main() -> int:
    print("namesrv=%s topic=%s other=%s" % (NAMESRV, TOPIC, OTHER))
    prod = DefaultMQProducer("PG_PinnedGuardLive_%d" % STAMP)
    prod.set_namesrv_addr(NAMESRV)
    prod.start()
    nsprod = None
    push = None
    try:
        prod.create_topic("TBW102", TOPIC, 4)
        prod.create_topic("TBW102", OTHER, 4)
        if not (wait_route(prod, TOPIC, 4) and wait_route(prod, OTHER, 4)):
            check("S0 the routes of both topics are registered", False)
            return 1
        mq0 = queue_zero(prod, TOPIC)
        broker = mq0.broker_name
        # The target queue of the negative leg: a **real** queue (queue 0 of another topic on the
        # same broker). A non-existent broker/queue would die on the route first, and then it would
        # be impossible to tell whether the topic or the address was what got rejected.
        mq_other = queue_zero(prod, OTHER)

        # Bring the consumer up first: the default CONSUME_FROM_FIRST_OFFSET, so that it does not race
        # the sends for the rebalance timing
        received = []
        push = DefaultMQPushConsumer(GROUP)
        push.set_namesrv_addr(NAMESRV)
        push.subscribe(TOPIC, "*")
        push.set_consume_from_where(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET)

        def _collect(msgs):
            received.extend(m.get_keys() for m in msgs)
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS

        push.set_message_listener(SimpleMessageListener(_collect))
        push.start()

        # ---------------- S1 the positive leg: a pinned send onto a real routed queue ----------------
        print("=== S1 pinned send onto a real routed queue ===")
        k_single = "pinned-live-single"
        r = prod.send(Message(TOPIC, b"s1-single", keys=k_single), 3000, mq0)
        check("S1a the synchronous single pinned send got SEND_OK and landed in queue %d" % mq0.queue_id,
              r.send_status == SendStatus.SEND_OK
              and r.message_queue is not None
              and r.message_queue.queue_id == mq0.queue_id
              and r.message_queue.topic == TOPIC,
              "status=%s mq=%s" % (r.send_status, r.message_queue))
        k_b1, k_b2 = "pinned-live-b1", "pinned-live-b2"
        rb = prod.send([Message(TOPIC, b"s1-b1", keys=k_b1),
                        Message(TOPIC, b"s1-b2", keys=k_b2)], 3000, mq0)
        check("S1b the synchronous batch pinned send got SEND_OK and the same queue",
              rb.send_status == SendStatus.SEND_OK
              and rb.message_queue is not None
              and rb.message_queue.queue_id == mq0.queue_id,
              "status=%s mq=%s" % (rb.send_status, rb.message_queue))
        off_after_s1 = wait_offset(prod, mq0, 3)
        # On the consume queue a batch is written per **sub-message** (the broker splits the inner batch
        # apart after receiving it) => 2 sub-messages advance the offset by 2
        check("S1c all three sub-messages really landed (maxOffset = 1 single + 2 batch sub-messages)",
              off_after_s1 == 3, "maxOffset=%d" % off_after_s1)

        # ---------------- S2 the negative leg: rejected synchronously, locally and without a trace ----------------
        print("=== S2 the topic does not match: rejected in sub-millisecond time locally, with no trace on the broker ===")
        e, elapsed = timed_send(prod, Message(TOPIC, b"refused", keys="pinned-live-refused"),
                                mq_other)
        check("S2a the synchronous single send is rejected (with the protocol wording)",
              e is not None and str(e) == SYNC_WORDING,
              "%.2fms %s" % (elapsed, str(e) if e else "no exception was raised"))
        check("S2b the rejection happens locally: sub-millisecond and with no broker code (not a timeout / not a broker remark)",
              e is not None and elapsed < LOCAL_BUDGET_MS and e.response_code is None,
              "%.2fms code=%r" % (elapsed, None if e is None else e.response_code))
        e2, elapsed2 = timed_send(prod, [Message(TOPIC, b"r1"), Message(TOPIC, b"r2")],
                                  mq_other)
        check("S2c the synchronous batch shares the same guard and the same wording",
              e2 is not None and str(e2) == SYNC_WORDING and elapsed2 < LOCAL_BUDGET_MS,
              "%.2fms %s" % (elapsed2, str(e2) if e2 else "no exception was raised"))
        check("S2d wire-level counter-proof: the maxOffset of A did not move at all and B has not a single message",
              prod.max_offset(mq0) == off_after_s1 and prod.max_offset(mq_other) == 0,
              "A=%d B=%d" % (prod.max_offset(mq0), prod.max_offset(mq_other)))

        # ---------------- S3 the namespace: wrap is idempotent and only a genuinely different name is rejected ----------------
        print("=== S3 the comparison under a namespace (neither ns%%topic nor a bare topic is falsely rejected) ===")
        nsprod = DefaultMQProducer("PG_PinnedGuardLive_%d_ns" % STAMP, namespace=NS)
        nsprod.set_namesrv_addr(NAMESRV)
        nsprod.start()
        nsprod.create_topic("TBW102", WTOPIC, 4)
        check("S3a the route of the prefixed topic is usable", wait_route(nsprod, WTOPIC, 4), "topic=%s" % WTOPIC)
        mqw = queue_zero(nsprod, WTOPIC)
        k_ns1, k_ns2 = "pinned-live-ns-q", "pinned-live-ns-m"
        r1 = nsprod.send(Message(TOPIC, b"ns-wrapped-queue", keys=k_ns1), 3000, mqw)
        check("S3b the queue topic already carries the ns prefix: wrap is idempotent and nothing is falsely rejected",
              r1.send_status == SendStatus.SEND_OK, "status=%s" % r1.send_status)
        r2 = nsprod.send(Message(WTOPIC, b"ns-wrapped-message", keys=k_ns2), 3000, mqw)
        check("S3c a message topic that carries the prefix itself is let through as well",
              r2.send_status == SendStatus.SEND_OK, "status=%s" % r2.send_status)
        e3, elapsed3 = timed_send(nsprod, Message(TOPIC, b"ns2-refused"),
                                  MessageQueue("ns2%%%s" % TOPIC, mqw.broker_name, 0))
        check('S3d only switching to the ns2%% prefix gets it rejected (the control leg: it is the name that is rejected, not "having a prefix")',
              e3 is not None and str(e3) == SYNC_WORDING and elapsed3 < LOCAL_BUDGET_MS,
              "%.2fms %s" % (elapsed3, str(e3) if e3 else "no exception was raised"))
        check("S3e the two legs that were let through really landed in ns1%%topic (maxOffset=2)",
              wait_offset(nsprod, mqw, 2) == 2, "maxOffset=%d" % nsprod.max_offset(mqw))

        # ---------------- S4 asynchronous: the callback carries the wording of the asynchronous site ----------------
        print("=== S4 asynchronous single/batch: a rejection goes through the callback, a pass goes through the core ===")
        events = []

        def _cb(tag):
            return SendCallbackImpl(
                success_fn=lambda r, t=tag: events.append(("ok", t, r)),
                exception_fn=lambda e, t=tag: events.append(("err", t, e)))

        def event_of(tag):
            for kind, t, payload in events:
                if t == tag:
                    return kind, payload
            return None, None

        prod.send_async(Message(TOPIC, b"async-refused", keys="pinned-live-async-refused"),
                        _cb("single-refused"), 3000, mq_other)
        ok_ev = wait_until(lambda: event_of("single-refused")[0] is not None, 10)
        kind, payload = event_of("single-refused")
        check("S4a the asynchronous single rejection goes through the callback, with the wording of the asynchronous site",
              ok_ev and kind == "err" and str(payload) == ASYNC_WORDING,
              "%s %s" % (kind, payload))
        check("S4b after the rejection the maxOffset still does not move (the asynchronous path did not sneak a send through)",
              prod.max_offset(mq0) == off_after_s1, "maxOffset=%d" % prod.max_offset(mq0))
        prod.send_async([Message(TOPIC, b"abr1"), Message(TOPIC, b"abr2")],
                        _cb("batch-refused"), 3000, mq_other)
        wait_until(lambda: event_of("batch-refused")[0] is not None, 10)
        kind_b, payload_b = event_of("batch-refused")
        check("S4c the asynchronous batch shares the same wording and the same rejection path",
              kind_b == "err" and str(payload_b) == ASYNC_WORDING,
              "%s %s" % (kind_b, payload_b))
        k_as = "pinned-live-async-single"
        k_ab1, k_ab2 = "pinned-live-async-b1", "pinned-live-async-b2"
        prod.send_async(Message(TOPIC, b"async-ok", keys=k_as), _cb("single-ok"),
                        3000, mq0)
        prod.send_async([Message(TOPIC, b"ab1", keys=k_ab1),
                         Message(TOPIC, b"ab2", keys=k_ab2)], _cb("batch-ok"), 3000, mq0)
        wait_until(lambda: event_of("single-ok")[0] is not None
                   and event_of("batch-ok")[0] is not None, 10)
        ok_s, res_s = event_of("single-ok")
        ok_b, res_b = event_of("batch-ok")
        check("S4d both legs that were let through got SEND_OK",
              ok_s == "ok" and res_s.send_status == SendStatus.SEND_OK
              and ok_b == "ok" and res_b.send_status == SendStatus.SEND_OK,
              "single=%s batch=%s" % (res_s, res_b))
        off_after_async = wait_offset(prod, mq0, off_after_s1 + 3)
        check("S4e the asynchronous legs really landed too (1 single + 2 batch sub-messages, maxOffset advances by 3 again)",
              off_after_async == off_after_s1 + 3, "maxOffset=%d" % off_after_async)

        print("=== S5 a oneway pinned send has no guard: the topic of the msg itself decides ===")
        k_ow = "pinned-live-oneway"
        prod.send_oneway(Message(TOPIC, b"oneway", keys=k_ow), mq_other)
        landed = wait_until(lambda: prod.max_offset(mq0) == off_after_async + 1, 10)
        check("S5a the oneway pinned send has no guard: the message landed in A according to its own topic",
              landed, "A maxOffset=%d" % prod.max_offset(mq0))
        check("S5b B, where the target queue lives, has not a single message (this is a deliberate hole by design, not a missed send)",
              prod.max_offset(mq_other) == 0, "B maxOffset=%d" % prod.max_offset(mq_other))

        # ---------------- S6 the end of the positive leg: not a single message is missing ----------------
        print("=== S6 the push consumer collects every positive-leg message ===")
        expected = [k_single, k_b1, k_b2, k_as, k_ab1, k_ab2, k_ow]
        deadline = time.time() + 40
        while time.time() < deadline and not all(k in received for k in expected):
            time.sleep(0.5)
        missing = [k for k in expected if k not in received]
        check("S6 every positive-leg message was consumed, none missing", not missing,
              "received=%s missing=%s" % (received, missing))
    finally:
        for c in (push, nsprod):
            if c is not None:
                try:
                    c.shutdown()
                except Exception:  # noqa: BLE001 - a failing teardown must not mask the main assertions
                    pass
        prod.shutdown()

    print("############ PASS=%d FAIL=%d ############" % (PASS, FAIL))
    return 0 if FAIL == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
