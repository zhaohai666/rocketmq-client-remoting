# -*- coding: utf-8 -*-
"""Live verification of the AccessChannel=CLOUD trace routing.

Java reference: ClientConfig.setAccessChannel -> dispatcher.start(nameSrv,
accessChannel) (DefaultMQProducer.java:401 / DefaultMQPushConsumer.java:781);
AsyncTraceDispatcher.sendTraceData (:305-322) routes a record whose channel is
CLOUD to "rmq_sys_TRACE_DATA_" + regionId instead of RMQ_SYS_TRACE_TOPIC,
falling back to the dispatcher's own channel when the context carries none.
The broker stamps MSG_REGION=DefaultRegion into the message properties
(AbstractSendMessageProcessor:374) and into the send response header
(SendMessageProcessor:716), so both the Pub and the Sub records carry
regionId=DefaultRegion on a self-built cluster.

Prerequisites: NameServer + Broker with traceTopicEnable=true (same as
verify_trace_live.py).  rmq_sys_TRACE_DATA_DefaultRegion is NOT part of the
broker's system topic set (TopicValidator.java:59-70 lists RMQ_SYS_TRACE_TOPIC
only), so it can be created through admin / auto-create.

Usage: .venv/bin/python verify_trace_cloud_live.py [127.0.0.1:9876]
"""
from __future__ import annotations

import sys
import threading
import time

from client.admin import DefaultMQAdminExt
from client.consumer import DefaultMQPushConsumer
from client.consumer_result import ConsumeConcurrentlyStatus, MessageListenerConcurrently
from client.producer import DefaultMQProducer
from client.trace import AccessChannel, TraceDataEncoder, TraceType
from common.message import Message
from common.message_client_id_setter import get_uniq_id
from common.mix_all import MixAll
from remoting.protocol.heartbeat import ConsumeFromWhere

NAMESRV = sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1:9876"
STAMP = int(time.time() * 1000)
TOPIC = "TraceCloudLive_%d" % STAMP
REGION_TOPIC = "rmq_sys_TRACE_DATA_" + MixAll.DEFAULT_TRACE_REGION_ID
CLOUD_PRODUCER_GROUP = "GID_trace_cloud_p_%d" % STAMP
LOCAL_PRODUCER_GROUP = "GID_trace_local_p_%d" % STAMP
CLOUD_CONSUMER_GROUP = "GID_trace_cloud_c_%d" % STAMP
LOCAL_CONSUMER_GROUP = "GID_trace_local_c_%d" % STAMP
REGION_READER_GROUP = "GID_trace_region_reader_%d" % STAMP
TRACE_READER_GROUP = "GID_trace_topic_reader_%d" % STAMP

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
    """Decode every trace record of one trace topic into TraceContext objects."""

    def __init__(self) -> None:
        self.records = []
        self.raw = []
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
            return list(self.records)


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
    print("=== RocketMQ AccessChannel=CLOUD trace live verify (python) ===")
    print("namesrv=%s topic=%s regionTopic=%s" % (NAMESRV, TOPIC, REGION_TOPIC))

    # ---------- C1 create the region topic on the broker ----------
    # The nameserver-side route creation refuses rmq_sys_* names, so this goes
    # through the broker's UPDATE_AND_CREATE_TOPIC; the broker only rejects
    # exact names in TopicValidator.SYSTEM_TOPIC_SET (RMQ_SYS_TRACE_TOPIC is in
    # it, rmq_sys_TRACE_DATA_DefaultRegion is not).
    admin = DefaultMQAdminExt()
    admin.set_namesrv_addr(NAMESRV)
    admin.start()
    try:
        warm = DefaultMQProducer("GID_trace_cloud_warm_%d" % STAMP)
        warm.set_namesrv_addr(NAMESRV)
        warm.start()
        warm.send(Message(TOPIC, b"warm-up"), 5000)
        warm.shutdown()
        time.sleep(1.0)
        route = admin.examine_topic_route(TOPIC)
        broker_addr = next(iter(route.broker_datas[0].broker_addrs.values()))
        admin.create_topic_in_broker(broker_addr, REGION_TOPIC, 8, 8)
        check("C1 rmq_sys_TRACE_DATA_DefaultRegion created on the broker (not in the system topic set)", True)
    except Exception as e:  # noqa: BLE001
        check("C1 rmq_sys_TRACE_DATA_DefaultRegion created on the broker", False, repr(e))
        return 1
    time.sleep(1.5)   # let the route register before the region reader subscribes

    # Two trace readers: one on the region topic, one on the classic topic.
    region_reader = _TraceCollector()
    region_consumer = DefaultMQPushConsumer(REGION_READER_GROUP)
    region_consumer.set_namesrv_addr(NAMESRV)
    region_consumer.set_consume_from_where(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET)
    region_consumer.subscribe(REGION_TOPIC, "*")
    region_consumer.set_message_listener(region_reader)
    region_consumer.set_enable_msg_trace(False)
    region_consumer.start()

    classic_reader = _TraceCollector()
    classic_consumer = DefaultMQPushConsumer(TRACE_READER_GROUP)
    classic_consumer.set_namesrv_addr(NAMESRV)
    classic_consumer.set_consume_from_where(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET)
    classic_consumer.subscribe(MixAll.TRACE_TOPIC, "*")
    classic_consumer.set_message_listener(classic_reader)
    classic_consumer.set_enable_msg_trace(False)
    classic_consumer.start()

    # ---------- C2 a CLOUD producer sends one message ----------
    business = _MsgCollector()
    cloud_consumer = DefaultMQPushConsumer(CLOUD_CONSUMER_GROUP)
    cloud_consumer.set_namesrv_addr(NAMESRV)
    cloud_consumer.set_consume_from_where(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET)
    cloud_consumer.subscribe(TOPIC, "*")
    cloud_consumer.set_message_listener(business)
    cloud_consumer.set_enable_msg_trace(True)
    cloud_consumer.set_access_channel(AccessChannel.CLOUD)   # <- the new entry point
    cloud_consumer.start()
    assigned = _wait_until(lambda: cloud_consumer.assigned_queue_count() > 0, 30)
    check("C2 the CLOUD consumer was assigned queues", assigned,
          "assigned=%d" % cloud_consumer.assigned_queue_count())

    cloud_producer = DefaultMQProducer(CLOUD_PRODUCER_GROUP)
    cloud_producer.set_namesrv_addr(NAMESRV)
    cloud_producer.set_enable_trace(True)
    cloud_producer.set_access_channel(AccessChannel.CLOUD)   # <- the new entry point
    cloud_producer.start()

    cloud_body = ("trace-cloud-%d" % STAMP).encode()
    cloud_keys = "CloudKey %d" % STAMP
    cloud_result = cloud_producer.send(Message(TOPIC, cloud_body, "TagA", cloud_keys), 5000)
    check("C2 the CLOUD producer sent one message", cloud_result is not None
          and bool(cloud_result.msg_id), "msgId=%s" % (cloud_result.msg_id if cloud_result else None))
    check("C2 SendResult.region_id == DefaultRegion (broker response header)",
          cloud_result.region_id == MixAll.DEFAULT_TRACE_REGION_ID,
          "region=%s" % cloud_result.region_id)

    # ---------- C3 the Pub record lands on the region topic ----------
    def _is_pub(records, msg_id):
        return any(r.trace_type == TraceType.PUB and r.trace_beans
                   and r.trace_beans[0].msg_id == msg_id for r in records)

    cloud_msg_id = cloud_result.msg_id
    pub_in_region = _wait_until(
        lambda: _is_pub(region_reader.snapshot(), cloud_msg_id), 30)
    cloud_producer.shutdown()   # flush the producer-side dispatcher, then wait once more
    time.sleep(0.5)
    _wait_until(lambda: _is_pub(region_reader.snapshot(), cloud_msg_id), 10)
    region_recs = region_reader.snapshot()
    print("  [info] region topic decoded %d records:" % len(region_recs))
    for r in region_recs:
        print("         - %s topic=%s msgId=%s region=%s" % (
            r.trace_type.value, r.trace_beans[0].topic if r.trace_beans else "-",
            r.trace_beans[0].msg_id if r.trace_beans else "-", r.region_id))
    check("C3 the CLOUD producer's Pub record shows up in rmq_sys_TRACE_DATA_DefaultRegion",
          pub_in_region, "records=%d" % len(region_recs))
    if pub_in_region:
        pubs = [t for t in region_recs
                if t.trace_type == TraceType.PUB and t.trace_beans
                and t.trace_beans[0].msg_id == cloud_msg_id]
        check("C3 the region-topic Pub record carries regionId=DefaultRegion and the business topic",
              pubs[0].region_id == MixAll.DEFAULT_TRACE_REGION_ID
              and any(b.topic == TOPIC for b in pubs[0].trace_beans),
              "region=%s topic=%s" % (pubs[0].region_id,
                                      pubs[0].trace_beans[0].topic if pubs[0].trace_beans else None))

    # ---------- C4 the default LOCAL producer does NOT route to the region topic ----------
    local_producer = DefaultMQProducer(LOCAL_PRODUCER_GROUP)
    local_producer.set_namesrv_addr(NAMESRV)
    local_producer.set_enable_trace(True)   # accessChannel stays LOCAL by default
    local_producer.start()
    local_body = ("trace-local-%d" % STAMP).encode()
    local_result = local_producer.send(Message(TOPIC, local_body, "TagA", "LocalKey %d" % STAMP), 5000)
    local_msg_id = local_result.msg_id
    pub_in_classic = _wait_until(
        lambda: _is_pub(classic_reader.snapshot(), local_msg_id), 30)
    local_producer.shutdown()
    time.sleep(0.5)
    _wait_until(lambda: _is_pub(classic_reader.snapshot(), local_msg_id), 10)
    check("C4 the default LOCAL producer's Pub record lands in RMQ_SYS_TRACE_TOPIC",
          pub_in_classic, "records=%d" % len(classic_reader.snapshot()))
    leaked = [t for t in region_reader.snapshot()
              if t.trace_type == TraceType.PUB and t.trace_beans
              and t.trace_beans[0].msg_id == local_msg_id]
    check("C4 the LOCAL producer's Pub record does NOT leak into the region topic", len(leaked) == 0,
          "leaked=%d" % len(leaked))

    # ---------- C5 the CLOUD consumer's Sub records land on the region topic ----------
    got = _wait_until(lambda: any(bytes(m.body) == cloud_body for m in business.snapshot()), 25)
    check("C5 the CLOUD consumer received the business message", got,
          "count=%d" % len(business.snapshot()))
    cloud_consumer.shutdown()   # shutdown flushes the consumer-side dispatcher
    time.sleep(1.0)
    sub_in_region = any(
        t.trace_type == TraceType.SUB_BEFORE and t.trace_beans
        and t.trace_beans[0].msg_id == cloud_msg_id
        for t in region_reader.snapshot())
    check("C5 the CLOUD consumer's SubBefore shows up in the region topic (regionId from the broker-stamped MSG_REGION)",
          sub_in_region,
          "records=%d" % len(region_reader.snapshot()))

    # ---------- C6 the default LOCAL consumer's Sub records stay classic ----------
    # This uses its own message: the CLOUD consumer (still subscribed during C4/C5)
    # also consumes local_body, and its region-topic Sub record for that message is
    # correct routing, not a leak.
    local_consumer = DefaultMQPushConsumer(LOCAL_CONSUMER_GROUP)
    local_consumer.set_namesrv_addr(NAMESRV)
    local_consumer.set_consume_from_where(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET)
    local_consumer.subscribe(TOPIC, "*")
    local_consumer.set_message_listener(_MsgCollector())
    local_consumer.set_enable_msg_trace(True)   # accessChannel stays LOCAL by default
    local_consumer.start()
    _wait_until(lambda: local_consumer.assigned_queue_count() > 0, 30)
    local2_producer = DefaultMQProducer("GID_trace_local2_p_%d" % STAMP)
    local2_producer.set_namesrv_addr(NAMESRV)
    local2_producer.set_enable_trace(True)      # accessChannel stays LOCAL by default
    local2_producer.start()
    local2_result = local2_producer.send(Message(TOPIC, ("trace-local2-%d" % STAMP).encode()), 5000)
    local2_msg_id = local2_result.msg_id
    time.sleep(8)   # allow delivery + both dispatchers to batch out
    local_consumer.shutdown()
    local2_producer.shutdown()
    time.sleep(1.0)
    local_sub_classic = any(
        t.trace_type == TraceType.SUB_BEFORE and t.trace_beans
        and t.trace_beans[0].msg_id == local2_msg_id
        for t in classic_reader.snapshot())
    check("C6 the default LOCAL consumer's SubBefore lands in RMQ_SYS_TRACE_TOPIC",
          local_sub_classic, "classicRecords=%d" % len(classic_reader.snapshot()))
    local_sub_leak = [t for t in region_reader.snapshot()
                      if t.trace_type == TraceType.SUB_BEFORE and t.trace_beans
                      and t.trace_beans[0].msg_id == local2_msg_id]
    check("C6 the LOCAL consumer's SubBefore does NOT leak into the region topic", len(local_sub_leak) == 0,
          "leaked=%d" % len(local_sub_leak))

    for c in (cloud_producer, local_producer, local2_producer, local_consumer,
              region_consumer, classic_consumer, admin):
        try:
            c.shutdown()
        except Exception:  # noqa: BLE001
            pass
    time.sleep(0.5)

    print("\nPASS=%d FAIL=%d" % (PASS, FAIL))
    return 0 if FAIL == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
