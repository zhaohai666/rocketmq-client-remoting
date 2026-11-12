# -*- coding: utf-8 -*-
"""Live verification: consume-side trace for FAILED consumption (RECONSUME_LATER).

Prerequisites: real cluster, broker config traceTopicEnable=true.

Usage: .venv/bin/python verify_trace_failure_live.py [127.0.0.1:9876]

Scenarios:
  F1  the first delivery happens and the listener returns RECONSUME_LATER
  F2  SubBefore(retryTimes=0) arrives for that message
  F3  SubAfter(success=false, contextCode=4 == ConsumeReturnType.FAILED) arrives
  F4  the retry delivery happens after the broker delay (~10s) and the listener succeeds
  F5  SubBefore(retryTimes=1) arrives for the same message id
  F6  SubAfter(success=true, contextCode=0) arrives for the retry
  F7  every SubAfter references the requestId of a SubBefore that was also seen
"""
from __future__ import annotations

import sys
import threading
import time

from client.consumer import DefaultMQPushConsumer
from client.consumer_result import ConsumeConcurrentlyStatus, MessageListenerConcurrently
from client.producer import DefaultMQProducer
from client.trace import TraceDataEncoder, TraceType
from common.message import Message
from common.mix_all import MixAll
from remoting.protocol.heartbeat import ConsumeFromWhere

NAMESRV = sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1:9876"
STAMP = int(time.time() * 1000)
TOPIC = "TraceFailTopic_%d" % STAMP
PRODUCER_GROUP = "GID_fail_trace_prod"
CONSUMER_GROUP = "GID_fail_trace_%d" % STAMP
TRACE_READER_GROUP = "GID_fail_trace_reader_%d" % STAMP

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


class _TraceCollector(MessageListenerConcurrently):
    def __init__(self) -> None:
        self.records = []
        self.lock = threading.Lock()

    def consume_message(self, msgs, context=None):
        with self.lock:
            for msg in msgs:
                try:
                    self.records.extend(
                        TraceDataEncoder.decoder_from_trace_data_string(
                            (msg.get_body() or b"").decode("utf-8", errors="replace")))
                except Exception as e:  # noqa: BLE001
                    print("  [warn] decode trace failed: %s" % e)
        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS

    def snapshot(self):
        with self.lock:
            return list(self.records)


class _FlakyListener(MessageListenerConcurrently):
    """First delivery of the marked body -> RECONSUME_LATER; retry -> CONSUME_SUCCESS."""

    def __init__(self) -> None:
        self.deliveries = []  # (body, reconsume_times)
        self.lock = threading.Lock()

    def consume_message(self, msgs, context=None):
        with self.lock:
            self.deliveries.extend(
                [(bytes(m.get_body()), m.reconsume_times) for m in msgs])
        for m in msgs:
            if bytes(m.get_body()).startswith(b"fail-me") and m.reconsume_times == 0:
                return ConsumeConcurrentlyStatus.RECONSUME_LATER
        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS

    def retry_seen(self):
        with self.lock:
            return any(t >= 1 for _b, t in self.deliveries)


def _wait_until(pred, timeout_sec: float, interval: float = 0.5):
    deadline = time.time() + timeout_sec
    while time.time() < deadline:
        if pred():
            return True
        time.sleep(interval)
    return False


def main() -> int:
    print("=== RocketMQ consume-failure trace live verify (python) ===")
    print("namesrv=%s topic=%s" % (NAMESRV, TOPIC))

    collector = _TraceCollector()
    trace_reader = DefaultMQPushConsumer(TRACE_READER_GROUP)
    trace_reader.set_namesrv_addr(NAMESRV)
    trace_reader.set_consume_from_where(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET)
    trace_reader.subscribe(MixAll.TRACE_TOPIC, "*")
    trace_reader.set_message_listener(collector)
    trace_reader.start()

    warm = DefaultMQProducer(PRODUCER_GROUP + "_warm")
    warm.set_namesrv_addr(NAMESRV)
    warm.start()
    warm.send(Message(TOPIC, b"warm-up"), 5000)
    time.sleep(1.5)

    listener = _FlakyListener()
    consumer = DefaultMQPushConsumer(CONSUMER_GROUP)
    consumer.set_namesrv_addr(NAMESRV)
    consumer.set_consume_from_where(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET)
    consumer.subscribe(TOPIC, "*")
    consumer.set_message_listener(listener)
    consumer.set_enable_msg_trace(True)
    consumer.start()

    producer = DefaultMQProducer(PRODUCER_GROUP)
    producer.set_namesrv_addr(NAMESRV)
    producer.set_enable_trace(True)
    producer.start()

    body = ("fail-me-%d" % STAMP).encode()
    result = producer.send(Message(TOPIC, body, "TagA", "KeyFail"), 5000)
    uniq = result.msg_id
    print("  [info] sent uniq_key=%s offset=%s" % (uniq, result.offset_msg_id))

    def _of_uniq(r):
        return (r.trace_beans and r.trace_beans[0].msg_id == uniq)

    # simple wait: first delivery
    first_delivered = _wait_until(lambda: any(b.startswith(b"fail-me")
                                              for b, _t in _snapshot_deliveries(listener)), 25)
    # The trace dispatcher flushes on a ~5s batch cycle and the trace reader needs its own
    # consume cycle, so poll for the failed SubAfter instead of sleeping a fixed window.
    failed_after_seen = _wait_until(lambda: any(
        r.trace_type == TraceType.SUB_AFTER and r.trace_beans
        and r.trace_beans[0].msg_id == uniq
        and r.is_success is False and r.context_code == 4
        for r in collector.snapshot()), 25)

    records_now = [r for r in collector.snapshot()
                   if r.trace_beans and r.trace_beans[0].msg_id == uniq]
    befores1 = [r for r in records_now if r.trace_type == TraceType.SUB_BEFORE]
    afters1 = [r for r in records_now if r.trace_type == TraceType.SUB_AFTER]
    check("F1 the first delivery was received by the listener", first_delivered,
          "deliveries=%s" % _snapshot_deliveries(listener))
    check("F2 SubBefore(retryTimes=0) for the failed message", any(
        b.trace_beans[0].retry_times == 0 for b in befores1),
        "before=%d" % len(befores1))
    check("F3 SubAfter(success=false, contextCode=4 FAILED)", failed_after_seen,
          "afters=%s" % [(a.is_success, a.context_code) for a in afters1])

    # ---------- F4 the retry delivery (broker delayLevel ~10s) ----------
    retried = _wait_until(listener.retry_seen, 60, interval=1.0)
    consumer.shutdown()
    time.sleep(1.0)
    producer.shutdown()
    time.sleep(0.5)

    records = [r for r in collector.snapshot() if _of_uniq(r)]
    befores = [r for r in records if r.trace_type == TraceType.SUB_BEFORE]
    afters = [r for r in records if r.trace_type == TraceType.SUB_AFTER]
    check("F4 the retry delivery happened and succeeded", retried,
          "deliveries=%s" % _snapshot_deliveries(listener))
    check("F5 SubBefore(retryTimes=1) for the same message id", any(
        b.trace_beans[0].retry_times == 1 for b in befores),
        "befores=%s" % [b.trace_beans[0].retry_times for b in befores])
    check("F6 SubAfter(success=true, contextCode=0) for the retry", any(
        a.is_success is True and a.context_code == 0 for a in afters),
        "afters=%s" % [(a.is_success, a.context_code) for a in afters])

    req_ids = {b.request_id for b in befores}
    check("F7 every SubAfter requestId was preceded by a SubBefore",
          all(a.request_id in req_ids for a in afters),
          "befores=%d afters=%d" % (len(befores), len(afters)))

    try:
        trace_reader.shutdown()
    except Exception:  # noqa: BLE001
        pass
    warm.shutdown()

    print("############ PASS=%d FAIL=%d ############" % (PASS, FAIL))
    return 0 if FAIL == 0 else 1


def _snapshot_deliveries(listener):
    with listener.lock:
        return list(listener.deliveries)


if __name__ == "__main__":
    sys.exit(main())
