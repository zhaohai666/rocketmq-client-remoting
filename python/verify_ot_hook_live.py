#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""OpenTracing-style hook trio live spot check against a real cluster.

The offline suite (tests/test_open_tracing_hook.py) proves the hook semantics;
this script proves they really fire on the real send/consume/transaction paths
and that the injected propagation context rides the wire end to end:

  T1  a normal send opens a ``To_<topic>`` producer span with msg_type
      NORMAL_MSG; with a region id on the result it is closed with
      success/msg_id/region_id (Java parity: no result / no region id -> the
      span stays open);
  T1  the tracer's inject() wrote into the live property map, i.e. the context
      was put on the outgoing message;
  T2  a timer message is tagged DELAY_MSG;
  T3  a transactional send opens a TRANS_MSG_HALF send span, and the commit
      fires an ``EndTransaction`` span reporting Trans_msg_Commit;
  T4  the consumer leg opens one ``From_<topic>`` span per message, parents it
      onto the upstream context extracted from the received properties (the
      injected key really came back off the wire), and closes it with success.

Prerequisite: NameServer + Broker already up.
Usage: python3 verify_ot_hook_live.py 127.0.0.1:9876
"""
from __future__ import annotations

import sys
import threading
import time

sys.path.insert(0, ".")

from client.consumer import (ConsumeConcurrentlyStatus, DefaultMQPushConsumer,
                             MessageListenerConcurrently)
from client.open_tracing_hook import (MESSAGE_BUS_DESTINATION,
                                      SPAN_KIND, SPAN_KIND_CONSUMER,
                                      SPAN_KIND_PRODUCER, TRANS_MSG_COMMIT,
                                      ConsumeMessageOpenTracingHook,
                                      EndTransactionOpenTracingHook,
                                      SendMessageOpenTracingHook)
from client.producer import (DefaultMQProducer, LocalTransactionState,
                             TransactionListener)
from client.trace import TraceConstants
from common.message import Message, MessageConst
from remoting.protocol.heartbeat import ConsumeFromWhere

NAMESRV = sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1:9876"
PREFIX = "OTLivePy_%d" % int(time.time() * 1000)
TOPIC = PREFIX + "_Topic"
GROUP = PREFIX + "_Group"
# The propagation key our fake tracer uses; the consumer leg proves it came back.
INJECTED_KEY = "ot_live_injected"

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


class FakeSpanContext:
    """Opaque upstream identity."""


class FakeSpan:
    def __init__(self, operation, tags, refs):
        self.operation = operation
        self.tags = dict(tags or {})
        self.refs = list(refs or [])
        self.finished = False

    def set_tag(self, key, value):
        self.tags[key] = value

    def log(self, fields):
        pass

    def finish(self):
        self.finished = True

    def context(self):
        return FakeSpanContext()


class FakeTracer:
    def __init__(self):
        self._lock = threading.Lock()
        self.spans = []

    def start_span(self, operation, tags=None, refs=None):
        span = FakeSpan(operation, tags, refs)
        with self._lock:
            self.spans.append(span)
        return span

    def extract(self, carrier):
        if carrier and carrier.get(INJECTED_KEY) == "1":
            return FakeSpanContext()
        return None

    def inject(self, span_context, carrier):
        carrier[INJECTED_KEY] = "1"

    def of(self, prefix):
        with self._lock:
            return [s for s in self.spans if s.operation.startswith(prefix)]


class Collector(MessageListenerConcurrently):
    def __init__(self):
        self.bodies = []
        self._lock = threading.Lock()

    def consume_message(self, msgs, context):
        with self._lock:
            self.bodies.extend(m.get_body() for m in msgs)
        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS


class CommitListener(TransactionListener):
    def execute_local_transaction(self, msg, arg):
        return LocalTransactionState.COMMIT_MESSAGE

    def check_local_transaction(self, msg):
        return LocalTransactionState.COMMIT_MESSAGE


def main() -> int:
    tracer = FakeTracer()

    producer = DefaultMQProducer(producer_group=GROUP + "_prod")
    producer.set_namesrv_addr(NAMESRV)
    producer.set_send_msg_timeout(5000)
    producer.register_send_message_hook(SendMessageOpenTracingHook(tracer))
    producer.register_end_transaction_hook(EndTransactionOpenTracingHook(tracer))
    producer.start()

    received = Collector()
    consumer = DefaultMQPushConsumer(GROUP + "_c")
    consumer.set_namesrv_addr(NAMESRV)
    consumer.set_consume_from_where(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET)
    consumer.register_consume_message_hook(ConsumeMessageOpenTracingHook(tracer))
    consumer.set_message_listener(received)
    consumer.subscribe(TOPIC, "*")
    consumer.pull_timeout_millis = 3000
    consumer.pull_suspend_timeout_millis = 1000

    try:
        # ---- T1: the normal send leg ----
        normal = Message(TOPIC, b"normal")
        normal.put_property(MessageConst.PROPERTY_KEYS, "ot-normal")
        r_normal = producer.send(normal)
        to_spans = tracer.of(TraceConstants.TO_PREFIX + TOPIC)
        check("T1 the send opened a To_<topic> span", len(to_spans) >= 1,
              "spans=%d" % len(to_spans))
        span = to_spans[-1]
        check("T1 span.kind=producer", span.tags.get(SPAN_KIND) == SPAN_KIND_PRODUCER,
              str(span.tags.get(SPAN_KIND)))
        check("T1 message_bus.destination=topic",
              span.tags.get(MESSAGE_BUS_DESTINATION) == TOPIC)
        check("T1 msg_type=NORMAL_MSG",
              span.tags.get(TraceConstants.ROCKETMQ_MSG_TYPE) == "NORMAL_MSG",
              str(span.tags.get(TraceConstants.ROCKETMQ_MSG_TYPE)))
        check("T1 the injected context was put on the outgoing message properties",
              normal.get_properties().get(INJECTED_KEY) == "1")
        if r_normal.region_id is not None:
            check("T1 the span is closed with success/msg_id/region_id",
                  span.finished
                  and span.tags.get(TraceConstants.ROCKETMQ_SUCCESS) is True
                  and span.tags.get(TraceConstants.ROCKETMQ_MSG_ID) == r_normal.msg_id
                  and span.tags.get(TraceConstants.ROCKETMQ_REGION_ID) == r_normal.region_id,
                  "msg_id=%s region=%s finished=%s" % (r_normal.msg_id, r_normal.region_id, span.finished))
        else:
            check("T1 no region id on the result: the span stays open (Java parity)",
                  not span.finished)

        # ---- T2: a timer message is tagged DELAY_MSG ----
        delay = Message(TOPIC, b"delay")
        delay.put_property("TIMER_DELAY_SEC", "1")
        r_delay = producer.send(delay)
        to_all = tracer.of(TraceConstants.TO_PREFIX)
        delay_spans = [s for s in to_all
                       if s.tags.get(TraceConstants.ROCKETMQ_MSG_TYPE) == "DELAY_MSG"]
        check("T2 the timer send span is tagged DELAY_MSG", len(delay_spans) >= 1)

        # ---- T3: the transaction legs (send half + EndTransaction commit) ----
        tx = Message(TOPIC, b"tx")
        r_tx = producer.send_message_in_transaction(tx, CommitListener())
        # Re-snapshot AFTER the send: tracer.of() returns the spans known at call
        # time, and the tx span was appended by the send just above.
        to_all = tracer.of(TraceConstants.TO_PREFIX)
        half_spans = [s for s in to_all
                      if s.tags.get(TraceConstants.ROCKETMQ_MSG_TYPE) == "TRANS_MSG_HALF"]
        check("T3 the transactional send span is tagged TRANS_MSG_HALF", len(half_spans) >= 1)
        end_spans = tracer.of(TraceConstants.END_TRANSACTION)
        check("T3 the commit fired an EndTransaction span", len(end_spans) >= 1,
              "spans=%d" % len(end_spans))
        if end_spans:
            es = end_spans[-1]
            check("T3 the EndTransaction span reports Trans_msg_Commit",
                  es.tags.get(TraceConstants.ROCKETMQ_MSG_TYPE) == TRANS_MSG_COMMIT,
                  str(es.tags.get(TraceConstants.ROCKETMQ_MSG_TYPE)))
            check("T3 the EndTransaction span carries the commit state and is closed",
                  es.tags.get(TraceConstants.ROCKETMQ_TRANSACTION_STATE) == "COMMIT_MESSAGE"
                  and es.tags.get(TraceConstants.ROCKETMQ_IS_FROM_TRANSACTION_CHECK) is False
                  and es.finished,
                  "state=%s finished=%s" % (
                      es.tags.get(TraceConstants.ROCKETMQ_TRANSACTION_STATE), es.finished))

        # ---- T4: the consume leg, parents off the wire-borne context ----
        consumer.start()
        want = {"normal", "delay", "tx"}
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline and not want.issubset({b.decode() for b in received.bodies}):
            time.sleep(0.5)
        bodies = {b.decode() for b in received.bodies}
        check("T4 the consumer received all three messages", want.issubset(bodies),
              "got=%s" % sorted(bodies))

        from_spans = tracer.of(TraceConstants.FROM_PREFIX + TOPIC)
        check("T4 one From_<topic> span per consumed message", len(from_spans) >= 3,
              "spans=%d" % len(from_spans))
        parented = [s for s in from_spans if s.refs]
        check("T4 consume spans parent onto the upstream context extracted from the wire",
              len(parented) >= 3, "parented=%d" % len(parented))
        check("T4 consume spans are kind=consumer",
              all(s.tags.get(SPAN_KIND) == SPAN_KIND_CONSUMER for s in from_spans))
        check("T4 consume spans carry the msg_id of the sends",
              any(s.tags.get(TraceConstants.ROCKETMQ_MSG_ID) == r_normal.msg_id for s in from_spans),
              "send=%s" % r_normal.msg_id)
        check("T4 consume spans are closed with success=True",
              all(s.finished and s.tags.get(TraceConstants.ROCKETMQ_SUCCESS) is True
                  for s in from_spans),
              "all_finished=%s" % all(s.finished for s in from_spans))
    finally:
        try:
            consumer.shutdown()
        except Exception:  # noqa: BLE001
            pass
        producer.shutdown()

    print("\nPASS=%d FAIL=%d" % (PASS, FAIL))
    return 0 if FAIL == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
