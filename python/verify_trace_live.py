# -*- coding: utf-8 -*-
"""Verification of the message trace against a real machine.

Prerequisites: NameServer + Broker are running and the broker configuration has
**traceTopicEnable=true** (otherwise RMQ_SYS_TRACE_TOPIC is not pre-created, and it cannot be
created through admin either -- it is a system topic and `validateSystemTopicWhenUpdateTopic`,
true by default, refuses to create it).

Usage: .venv/bin/python verify_trace_live.py [127.0.0.1:9876]

The scenarios S1-S17 are in main() below; it only returns 0 when all of them are hit.
"""
from __future__ import annotations

import sys
import threading
import time

from client.consumer import DefaultMQPushConsumer
from client.consumer_result import ConsumeConcurrentlyStatus, MessageListenerConcurrently
from client.producer import DefaultMQProducer
from client.trace import TraceConstants, TraceDataEncoder, TraceType
from common.message import Message
from common.message_client_id_setter import get_uniq_id
from common.message_const import MessageConst
from common.mix_all import MixAll
from remoting.protocol.heartbeat import ConsumeFromWhere

NAMESRV = sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1:9876"
STAMP = int(time.time() * 1000)
# Both the topic and the business consumer group carry the STAMP: `run_trace_live.sh all` runs
# several rounds of verification **one after another** on the same broker, and fixed names would
# make each round inherit the offsets and leftover messages of the previous one, so the assertion
# results would depend on the run order. Only a brand-new topic plus a brand-new group makes every
# run start from the same point (the real robustness is guaranteed separately, see the body filter
# in S6).
TOPIC = "TraceTopicLive_%d" % STAMP
NOTRACE_TOPIC = "TraceNoTraceTopic_%d" % STAMP
PRODUCER_GROUP = "GID_trace_producer_live"
CONSUMER_GROUP = "GID_trace_live_%d" % STAMP
TRACE_READER_GROUP = "GID_trace_reader_live_%d" % STAMP

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
    """Consume RMQ_SYS_TRACE_TOPIC and accumulate the TraceContext objects decoded from the trace text."""

    def __init__(self) -> None:
        self.records = []
        self.raw = []           # (the trace message keys, the topic of the trace message itself, body)
        self.lock = threading.Lock()

    def consume_message(self, msgs, context=None):
        with self.lock:
            for msg in msgs:
                body = msg.get_body() or b""
                text = body.decode("utf-8", errors="replace")
                self.raw.append((msg.get_keys() or "", msg.topic, text))
                try:
                    self.records.extend(TraceDataEncoder.decoder_from_trace_data_string(text))
                except Exception as e:  # noqa: BLE001
                    print("  [warn] decode trace failed: %s" % e)
        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS

    def snapshot(self):
        with self.lock:
            return list(self.records), list(self.raw)


class _MsgCollector(MessageListenerConcurrently):
    def __init__(self) -> None:
        self.msgs = []
        self.lock = threading.Lock()

    def consume_message(self, msgs, context=None):
        with self.lock:
            self.msgs.extend(msgs)
        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS

    def snapshot(self):
        with self.lock:
            return list(self.msgs)


def _wait_until(pred, timeout_sec: float, interval: float = 0.5):
    deadline = time.time() + timeout_sec
    while time.time() < deadline:
        if pred():
            return True
        time.sleep(interval)
    return False


def main() -> int:
    print("=== RocketMQ message trace live verify (python) ===")
    print("namesrv=%s topic=%s" % (NAMESRV, TOPIC))

    collector = _TraceCollector()
    trace_reader = DefaultMQPushConsumer(TRACE_READER_GROUP)
    trace_reader.set_namesrv_addr(NAMESRV)
    trace_reader.set_consume_from_where(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET)
    trace_reader.subscribe(MixAll.TRACE_TOPIC, "*")
    trace_reader.set_message_listener(collector)
    trace_reader.start()
    print("trace reader started (group=%s, topic=%s)" % (TRACE_READER_GROUP, MixAll.TRACE_TOPIC))

    # ---------- S1 warm up to create the topic ----------
    warm = DefaultMQProducer(PRODUCER_GROUP + "_warm")
    warm.set_namesrv_addr(NAMESRV)
    warm.start()
    try:
        warm.send(Message(TOPIC, b"warm-up"), 5000)
        check("S1 the warm-up message was sent successfully (triggering the broker to auto-create the topic)", True)
    except Exception as e:  # noqa: BLE001
        check("S1 the warm-up message was sent successfully (triggering the broker to auto-create the topic)", False, str(e))
        return 1

    # Wait a bit after the warm-up before starting the consumer, to reduce the race between the
    # "offset resolution" and the "asynchronous broker consumequeue dispatch".
    # This step only gives the timing more slack, it is **not** a correctness guarantee -- the
    # warm-up message may legitimately be delivered; see the body filter in S6.
    time.sleep(1.5)

    # ---------- S2/S3 the producer with tracing turned on ----------
    msg_collector = _MsgCollector()
    consumer = DefaultMQPushConsumer(CONSUMER_GROUP)
    consumer.set_namesrv_addr(NAMESRV)
    consumer.set_consume_from_where(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET)
    consumer.subscribe(TOPIC, "*")
    consumer.set_message_listener(msg_collector)
    consumer.set_enable_msg_trace(True)          # <- tracing on the consumer side
    consumer.start()
    # The consumer must be started before the messages are sent (CONSUME_FROM_LAST_OFFSET semantics
    # + the queues must be assigned first)
    assigned = _wait_until(lambda: consumer.assigned_queue_count() > 0, 30)
    check("S2 the consumer was assigned queues (start the consumer before sending the messages)", assigned,
          "assigned=%d" % consumer.assigned_queue_count())

    producer = DefaultMQProducer(PRODUCER_GROUP)
    producer.set_namesrv_addr(NAMESRV)
    producer.set_enable_trace(True)              # <- tracing on the producer side
    producer.start()

    body = ("trace-live-%d" % STAMP).encode()
    keys = "KeyA KeyB %d" % STAMP
    result = producer.send(Message(TOPIC, body, "TagA", keys), 5000)

    check("S3 SendResult.trace_on == True (the broker defaults traceOn=true)",
          result.trace_on is True, "trace_on=%s" % result.trace_on)
    check("S4 SendResult.msg_id is the client-side UNIQ_KEY and differs from offset_msg_id",
          bool(result.msg_id) and bool(result.offset_msg_id)
          and result.msg_id != result.offset_msg_id,
          "msgId=%s offsetMsgId=%s" % (result.msg_id, result.offset_msg_id))
    check("S5 SendResult.region_id was resolved (DefaultRegion by default)",
          result.region_id == MixAll.DEFAULT_TRACE_REGION_ID, "region=%s" % result.region_id)

    # ---------- S6/S7 the message is consumed ----------
    # CAUTION: only count the messages sent **this time**: the topic still necessarily holds the
    #   warm-up message from S1, and the starting offset of "the group's first consumption" is taken
    #   from the maxOffset at that moment -- the broker's consumequeue is dispatched
    #   **asynchronously**, so the offset may land before the warm-up message and the warm-up
    #   message would then be delivered too (this was actually hit in live testing). What the
    #   assertion really means is "the business message of this round was consumed", so it filters
    #   by body instead of betting that the topic only holds one message.
    def _is_primary(m) -> bool:
        return bytes(m.body).startswith(b"trace-live-")

    got = _wait_until(lambda: any(_is_primary(m) for m in msg_collector.snapshot()), 25)
    msgs = [m for m in msg_collector.snapshot() if _is_primary(m)]
    check("S6 the business consumer received that one message of this round (the warm-up message is filtered out by body)",
          got and len(msgs) == 1, "count=%d" % len(msgs))
    recv = msgs[0] if msgs else None
    recv_msg_id = recv.msg_id if recv else None
    check("S7 the consumer-side msg_id == SendResult.offset_msg_id and the UNIQ_KEY arrives on the consumer side through the properties",
          recv is not None and recv_msg_id == result.offset_msg_id
          and get_uniq_id(recv) == result.msg_id,
          "recv=%s offset=%s uniq=%s" % (recv_msg_id, result.offset_msg_id,
                                         get_uniq_id(recv) if recv else None))

    # ---------- a message without keys (used by S17; live data pins the robustness of the decoder) ----------
    keyless = producer.send(Message(TOPIC, b"no-keys-here"), 5000)
    keyless_ok = _wait_until(
        lambda: any(bytes(m.body) == b"no-keys-here" for m in msg_collector.snapshot()), 25)
    keyless_id = keyless.offset_msg_id

    # Shut the consumer down -> this triggers the trace dispatcher to flush, guaranteeing that SubBefore/SubAfter land on disk
    consumer.shutdown()
    time.sleep(1.0)

    # ---------- S8 wait for the traces to land ----------
    def _found_pub():
        recs, _ = collector.snapshot()
        return any(r.trace_type == TraceType.PUB and r.trace_beans
                   and r.trace_beans[0].msg_id == result.msg_id for r in recs)

    pub_ok = _wait_until(_found_pub, 35)
    producer.shutdown()      # flush the producer-side traces
    time.sleep(0.5)
    _wait_until(_found_pub, 10)

    records, raw = collector.snapshot()
    print("  [info] received %d trace messages / decoded %d trace records" % (len(raw), len(records)))
    for r in records:
        print("         - %s topic=%s msgId=%s group=%s success=%s code=%s" % (
            r.trace_type.value, r.trace_beans[0].topic if r.trace_beans else "-",
            r.trace_beans[0].msg_id if r.trace_beans else "-", r.group_name,
            r.is_success, r.context_code))

    pubs = [r for r in records if r.trace_type == TraceType.PUB and r.trace_beans
            and r.trace_beans[0].msg_id == result.msg_id]
    check("S8 a Pub record shows up in the traces and its msgId matches SendResult.msg_id", pub_ok and len(pubs) >= 1,
          "count=%d" % len(pubs))

    pub = pubs[0] if pubs else None
    if pub is not None:
        check("S9 the topic / groupName of the Pub trace are correct",
              pub.trace_beans[0].topic == TOPIC and pub.group_name == PRODUCER_GROUP,
              "topic=%s group=%s" % (pub.trace_beans[0].topic, pub.group_name))
    else:
        check("S9 the topic / groupName of the Pub trace are correct", False, "no pub record")

    check("S10 the keys of the trace message carrying the Pub trace contain that msgId (the console looks it up by keys)",
          any(result.msg_id in (k or "") for k, _t, _b in raw),
          "raw_keys=%s" % [k for k, _t, _b in raw][:3])

    subs_before = [r for r in records if r.trace_type == TraceType.SUB_BEFORE and r.trace_beans
                   and r.trace_beans[0].msg_id == recv_msg_id]
    check("S11 a SubBefore shows up in the traces and its msgId matches the consumer side", len(subs_before) >= 1,
          "count=%d" % len(subs_before))

    subs_after = [r for r in records if r.trace_type == TraceType.SUB_AFTER and r.trace_beans
                  and r.trace_beans[0].msg_id == recv_msg_id]
    if subs_before and subs_after:
        before, after = subs_before[0], subs_after[0]
        check("S12 SubBefore/SubAfter are paired with the same requestId, success=true, contextCode=0",
              after.request_id == before.request_id and after.is_success is True
              and after.context_code == 0,
              "req=%s success=%s code=%s" % (after.request_id, after.is_success,
                                             after.context_code))
        check("S13 the retryTimes of SubBefore match the consumed message",
              before.trace_beans[0].retry_times == msgs[0].reconsume_times,
              "trace=%s msg=%s" % (before.trace_beans[0].retry_times, msgs[0].reconsume_times))
    else:
        check("S12 SubBefore/SubAfter are paired with the same requestId, success=true, contextCode=0",
              False, "before=%d after=%d" % (len(subs_before), len(subs_after)))
        check("S13 the retryTimes of SubBefore match the consumed message", False, "no sub traces")

    # ---------- S14 recursion guard: the traces themselves are not traced any more ----------
    check("S14 no trace record has the trace topic itself as its topic (recursion guard)",
          all(r.trace_beans[0].topic != MixAll.TRACE_TOPIC for r in records if r.trace_beans),
          "topics=%s" % sorted({r.trace_beans[0].topic for r in records if r.trace_beans}))

    # ---------- S15 tracing off produces no traces ----------
    quiet = DefaultMQProducer(PRODUCER_GROUP + "_quiet")
    quiet.set_namesrv_addr(NAMESRV)
    quiet.set_enable_trace(False)                # <- explicitly off
    quiet.start()
    quiet.send(Message(NOTRACE_TOPIC, b"no-trace"), 5000)
    quiet.shutdown()
    time.sleep(6.5)
    records2, _ = collector.snapshot()
    leaked = [r for r in records2 if r.trace_type == TraceType.PUB and r.trace_beans
              and r.trace_beans[0].topic == NOTRACE_TOPIC]
    check("S15 a producer with enable_trace=false produces no Pub trace", len(leaked) == 0,
          "leaked=%d" % len(leaked))

    all_segments = sum(text.count(TraceConstants.FIELD_SPLITOR)
                       for _k, t, text in raw if t == MixAll.TRACE_TOPIC)
    check("S16 the record count of the trace text (STX count) == the number of decoded records (split semantics)",
          all_segments == len(records) and len(records) > 0,
          "segments=%d decoded=%d" % (all_segments, len(records)))

    # ---------- S17 the trace of a message without keys (the last segment of SubBefore is empty) must decode too ----------
    kl_before = [r for r in records if r.trace_type == TraceType.SUB_BEFORE and r.trace_beans
                 and r.trace_beans[0].msg_id == keyless_id]
    check("S17 a message without keys still yields a decodable SubBefore trace (an empty keys segment does not crash it)",
          keyless_ok and len(kl_before) >= 1 and kl_before[0].trace_beans[0].keys == "",
          "consumed=%s before=%d keys=%r" % (
              keyless_ok, len(kl_before),
              kl_before[0].trace_beans[0].keys if kl_before else None))

    try:
        trace_reader.shutdown()
    except Exception:  # noqa: BLE001
        pass
    warm.shutdown()

    print("############ PASS=%d FAIL=%d ############" % (PASS, FAIL))
    return 0 if FAIL == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
