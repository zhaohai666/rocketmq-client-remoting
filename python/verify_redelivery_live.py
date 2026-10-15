#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Verification of the consumer-side completion against a real machine (send-back / offset
persistence / orderly lock / broadcast / flow control).

Usage (requires a local RocketMQ 5.5.1 cluster):
    .venv/bin/python verify_redelivery_live.py 127.0.0.1:9876

Scenarios:
  S1 send-back: the listener returns RECONSUME_LATER for retry-me the first time -> it should be
     delivered a second time through %RETRY%topic (delay level3=10s) with RECONSUME_TIMES=1; an
     ordinary message is delivered only once.
  S2 offset persistence: restart a consumer of the same group after consuming 3 messages -> the
     old messages are not repeated and the new messages keep being delivered.
  S3 orderly consumption: the orderly listener consumes normally and the broker lock
     (LOCK_BATCH_MQ) takes effect.
  S4 broadcast mode: two consumers of the same group each collect all the messages, without
     affecting each other.
  S5 flow control: threshold 2 + slow consumption -> the flow control counter becomes > 0 and in
     the end all the messages are consumed.
  S9 dead-letter terminal state: maxReconsumeTimes=2 => after 3 deliveries (reconsumeTimes
     0/1/2) the broker forwards it to %DLQ%<group> (in the DLQ reconsumeTimes=3 and RETRY_TOPIC
     keeps the original topic), and there is no 4th delivery.
"""
from __future__ import annotations

import sys
import threading
import time

sys.path.insert(0, ".")

from client.consumer import (ConsumeConcurrentlyStatus,
                                      ConsumeOrderlyStatus,
                                      DefaultLitePullConsumer,
                                      DefaultMQPushConsumer,
                                      MessageListenerConcurrently,
                                      MessageListenerOrderly,
                                      SimpleMessageListener)
from client.mq_client import MQClientInstance
from client.producer import DefaultMQProducer
from common.message import Message
from common.mix_all import MixAll
from remoting.protocol.heartbeat import ConsumeFromWhere

NAMESRV = sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1:9876"
PREFIX = "GapPy_%d" % int(time.time() * 1000)

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


class Collector:
    """A generic collector: records (body, topic, properties, ts)."""

    def __init__(self, consumer: DefaultMQPushConsumer):
        self.consumer = consumer
        self.records = []
        self._lock = threading.Lock()
        consumer.set_message_listener(SimpleMessageListener(self._on_msg))

    def _on_msg(self, msgs):
        with self._lock:
            for m in msgs:
                self.records.append((bytes(m.body), m.topic,
                                     dict(m.properties or {}), time.time()))
        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS

    def bodies(self):
        with self._lock:
            return [r[0] for r in self.records]


def start_consumer(group: str, topic: str, collector: Collector,
                   model: str = None, thread_nums: int = 1) -> DefaultMQPushConsumer:
    c = collector.consumer
    c.set_namesrv_addr(NAMESRV)
    if model is not None:
        c.set_message_model(model)
    c.set_consume_thread_nums(thread_nums)
    c.subscribe(topic, "*")
    c.start()
    return c


def prepare_topic(topic: str, queues: int = 4) -> None:
    pass
    c = MQClientInstance("setup-%d" % int(time.time() * 1000), [NAMESRV])
    c.start()
    try:
        c.create_topic_in_route(topic, queues, queues)
    finally:
        c.shutdown()


def main() -> int:
    producer = DefaultMQProducer(PREFIX + "_pg")
    producer.set_namesrv_addr(NAMESRV)
    producer.start()
    time.sleep(1)

    # ---------- S1 send-back ----------
    topic1 = PREFIX + "_Retry"
    prepare_topic(topic1)
    group1 = PREFIX + "_g1"
    c1 = DefaultMQPushConsumer(group1)
    seen1 = []
    lock1 = threading.Lock()

    class RetryListener(MessageListenerConcurrently):
        def consume_message(self, msgs, context):
            for m in msgs:
                with lock1:
                    seen1.append((bytes(m.body), m.topic, m.get_reconsume_times(), time.time()))
            # The first delivery of retry-me fails, the redelivery succeeds (the retry count travels in the
            # on-wire MessageExt field)
            if any(bytes(m.body) == b"retry-me" and m.get_reconsume_times() == 0 for m in msgs):
                return ConsumeConcurrentlyStatus.RECONSUME_LATER
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS

    c1.set_namesrv_addr(NAMESRV)
    c1.set_message_listener(RetryListener())
    c1.subscribe(topic1, "*")
    c1.start()
    time.sleep(3)
    producer.send(Message(topic1, b"retry-me"))
    producer.send(Message(topic1, b"normal-1"))
    print("S1: sent, waiting for the send-back (delay level3 is about 10s)...")
    time.sleep(30)
    c1.shutdown()
    retry_arrivals = [r for r in seen1 if r[0] == b"retry-me"]
    normal_arrivals = [r for r in seen1 if r[0] == b"normal-1"]
    retry_times_seen = [r[2] for r in retry_arrivals]
    check("S1 retry-me was delivered several times", len(retry_arrivals) >= 2,
          "arrivals=%d" % len(retry_arrivals))
    check("S1 the redelivery came from %RETRY%/RECONSUME_TIMES",
          any(r[2] >= 1 or r[1].startswith("%RETRY%") for r in retry_arrivals),
          "times=%s" % retry_times_seen)
    if len(retry_arrivals) >= 2:
        gap = retry_arrivals[-1][3] - retry_arrivals[0][3]
        check("S1 the send-back has a delay gradient (>=8s)", gap >= 8.0, "gap=%.1fs" % gap)
    else:
        check("S1 the send-back has a delay gradient (>=8s)", False, "fewer than two deliveries")
    check("S1 the ordinary message was delivered only once", len(normal_arrivals) == 1,
          "arrivals=%d" % len(normal_arrivals))
    retried = [r for r in retry_arrivals if r[2] >= 1]
    check("S1 the topic of the redelivered message is restored to the original topic",
          bool(retried) and all(r[1] == topic1 for r in retried),
          "topics=%s" % sorted({r[1] for r in retried}))

    # ---------- S2 offset persistence ----------
    topic2 = PREFIX + "_Offset"
    prepare_topic(topic2)
    group2 = PREFIX + "_g2"
    c2 = DefaultMQPushConsumer(group2)
    col2 = Collector(c2)
    start_consumer(group2, topic2, col2)
    time.sleep(3)
    old_bodies = [b"persist-%d" % i for i in range(3)]
    for b in old_bodies:
        producer.send(Message(topic2, b))
    time.sleep(8)
    c2.shutdown()  # shutdown persists the offsets
    got_first_round = [b for b in old_bodies if b in set(col2.bodies())]
    check("S2 the first round consumed 3 messages", len(got_first_round) == 3,
          "got=%d" % len(got_first_round))
    # Restart a consumer of the same group and only send 1 new message
    c2b = DefaultMQPushConsumer(group2)
    col2b = Collector(c2b)
    start_consumer(group2, topic2, col2b)
    time.sleep(3)
    producer.send(Message(topic2, b"persist-new"))
    time.sleep(8)
    c2b.shutdown()
    new_seen = b"persist-new" in set(col2b.bodies())
    old_resent = [b for b in old_bodies if b in set(col2b.bodies())]
    check("S2 after the restart the new messages keep being delivered", new_seen, "" if new_seen else "not received")
    check("S2 the restart does not reconsume the old messages", len(old_resent) == 0,
          "duplicated=%s" % old_resent)

    # ---------- S3 orderly consumption + the broker lock ----------
    topic3 = PREFIX + "_Orderly"
    prepare_topic(topic3)
    group3 = PREFIX + "_g3"
    c3 = DefaultMQPushConsumer(group3)
    got3 = []
    lock3 = threading.Lock()

    class OrderlyListener(MessageListenerOrderly):
        def consume_message(self, msgs, context):
            with lock3:
                got3.extend(bytes(m.body) for m in msgs)
            return ConsumeOrderlyStatus.SUCCESS

    c3.set_namesrv_addr(NAMESRV)
    c3.set_message_listener(OrderlyListener())
    c3.subscribe(topic3, "*")
    c3.start()
    time.sleep(5)  # wait for the first LOCK_BATCH_MQ to take effect
    for i in range(4):
        producer.send(Message(topic3, b"orderly-%d" % i))
    time.sleep(8)
    lock_ok_n = len(c3._lock_ok)
    c3.shutdown()
    check("S3 the orderly consumption collected all 4", len(got3) == 4, "got=%d" % len(got3))
    check("S3 the broker queue lock took effect", lock_ok_n > 0, "lockOK=%d" % lock_ok_n)

    # ---------- S4 broadcast mode ----------
    topic4 = PREFIX + "_Bc"
    prepare_topic(topic4)
    group4 = PREFIX + "_g4"

    def make_broadcast_consumer(inst_name: str) -> Collector:
        c = DefaultMQPushConsumer(group4)
        c.set_instance_name(inst_name)  # make sure the two consumers have different clientIds
        col = Collector(c)
        start_consumer(group4, topic4, col, model="BROADCASTING")
        return col

    col4a = make_broadcast_consumer("bc-a")
    col4b = make_broadcast_consumer("bc-b")
    time.sleep(3)
    for i in range(3):
        producer.send(Message(topic4, b"bc-%d" % i))
    time.sleep(8)
    na, nb = len(col4a.bodies()), len(col4b.bodies())
    col4a.consumer.shutdown()
    col4b.consumer.shutdown()
    check("S4 broadcast consumer A collected all 3", na == 3, "got=%d" % na)
    check("S4 broadcast consumer B collected all 3", nb == 3, "got=%d" % nb)

    # ---------- S5 flow control ----------
    topic5 = PREFIX + "_Flow"
    prepare_topic(topic5)
    group5 = PREFIX + "_g5"
    c5 = DefaultMQPushConsumer(group5)
    got5 = []
    lock5 = threading.Lock()

    class SlowListener(MessageListenerConcurrently):
        def consume_message(self, msgs, context):
            time.sleep(0.3)
            with lock5:
                got5.extend(bytes(m.body) for m in msgs)
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS

    c5.set_namesrv_addr(NAMESRV)
    c5.set_message_listener(SlowListener())
    c5.pull_threshold_for_queue = 2
    c5.subscribe(topic5, "*")
    c5.start()
    time.sleep(3)
    for i in range(10):
        producer.send(Message(topic5, b"flow-%d" % i))
    time.sleep(12)
    fc = c5._flow_control_triggered
    c5.shutdown()
    check("S5 under slow consumption all the messages arrived", len(got5) == 10, "got=%d" % len(got5))
    check("S5 the flow control trigger counter > 0", fc > 0, "triggered=%d" % fc)

    # ---------- S6 the multi-instance cluster rebalance (queue assignment) ----------
    # Two instances of the same group subscribe to the same topic: the broker-side consumer list
    # should have 2 clientIds, the queues are split by AllocateMessageQueueAveragely, and every
    # message is **consumed exactly once**.
    topic6 = PREFIX + "_Rebalance"
    group6 = PREFIX + "_g6"
    # Create the topic first (8 queues) and wait for the route to propagate, otherwise the consumer
    # has no route when it starts and the assignment only becomes stable after the next 20s
    # rebalance (the test would read an intermediate state)
    created = False
    try:
        setup = MQClientInstance("setup-%d" % int(time.time() * 1000), [NAMESRV])
        setup.start()
        setup.create_topic_in_route(topic6, 8, 8)
        setup.shutdown()
        created = True
    except Exception as e:  # noqa: BLE001
        print("S6: pre-creating the topic failed (falling back to auto-creation): %s" % e)
    ca = DefaultMQPushConsumer(group6)
    ca.set_instance_name("inst-a")
    ca.set_namesrv_addr(NAMESRV)
    rec_a = []
    rec_b = []
    lk6 = threading.Lock()

    def make_listener(bucket):
        class L(MessageListenerConcurrently):
            def consume_message(self, msgs, context):
                with lk6:
                    bucket.extend(bytes(m.body) for m in msgs)
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS

        return L()

    ca.set_message_listener(make_listener(rec_a))
    ca.subscribe(topic6, "*")
    ca.start()

    cb = DefaultMQPushConsumer(group6)
    cb.set_instance_name("inst-b")
    cb.set_namesrv_addr(NAMESRV)
    cb.set_message_listener(make_listener(rec_b))
    cb.subscribe(topic6, "*")
    cb.start()
    # Wait for the assignment to stabilize: the intersection is empty + both sides are non-empty +
    # the queues of the main topic are fully covered (wait at most 45s to cover the 20s rebalance
    # period; the assignment set also contains the %RETRY% queues, so it is validated against the
    # main topic)
    deadline = time.time() + 45
    asg_a = asg_b = 0
    keys_a, keys_b = [], []
    expected_keys = set()
    while time.time() < deadline:
        keys_a = set(ca.assigned_queue_keys())
        keys_b = set(cb.assigned_queue_keys())
        asg_a, asg_b = len(keys_a), len(keys_b)
        expected_keys = {ca._mq_key(mq) for mq in ca._all_queues_of_topic(topic6)}
        covered = expected_keys and (keys_a | keys_b) >= expected_keys
        if asg_a > 0 and asg_b > 0 and not (keys_a & keys_b) and covered:
            break
        time.sleep(2)
    hb_a = ca.heartbeat_count()
    # The broker pushes 40 back along the long connection when the group membership changes; this is
    # the only observation point in the whole chain (a reverse request cannot be injected by the test
    # case), so it only asserts that "this side really received and handled it", not the count.
    notified_a = ca._mq_client.consumer_ids_changed_count
    notified_b = cb._mq_client.consumer_ids_changed_count
    cid_list = ca._require_client().get_consumer_id_list_by_group(topic6, group6) or []
    n6 = 40
    for i in range(n6):
        producer.send(Message(topic6, b"rb-%d" % i))
    time.sleep(15)
    total6 = len(rec_a) + len(rec_b)
    all6 = rec_a + rec_b
    dup6 = len(all6) - len(set(all6))
    covered_n = len((keys_a | keys_b) & expected_keys)
    ca.shutdown()
    cb.shutdown()
    check("S6 the consumers registered through the heartbeat", hb_a > 0 and len(cid_list) == 2,
          "heartbeats=%d brokerCids=%d" % (hb_a, len(cid_list)))
    check("S6 the queues have neither overlap nor gaps (a=%d,b=%d,intersection=%d,coverage=%d/%d)"
          % (asg_a, asg_b, len(keys_a & keys_b), covered_n, len(expected_keys)),
          asg_a > 0 and asg_b > 0 and not (keys_a & keys_b)
          and expected_keys and (keys_a | keys_b) >= expected_keys)
    check("S6 no message was consumed twice", dup6 == 0 and total6 == n6,
          "got=%d/%d dup=%d" % (total6, n6, dup6))
    # Only the broker can send the reverse push and the test case cannot inject it, so this counter
    # is the one place that can prove "the instance-level 40 handler ran" (it stays 0 when the handler
    # is not registered).
    check("S6 on a membership change the broker NOTIFY_CONSUMER_IDS_CHANGED(40) was received",
          notified_a > 0 and notified_b > 0,
          "a=%d b=%d" % (notified_a, notified_b))

    # ---------- S7 graceful unregistration ----------
    # On shutdown it should send UNREGISTER_CLIENT and the broker side removes it immediately,
    # without waiting for the heartbeat to time out (~120s).
    # The query uses an independent probe client (after the consumer shuts down, its internal client
    # is closed too).
    probe = MQClientInstance("probe-%d" % int(time.time() * 1000), [NAMESRV])
    probe.start()
    group7 = PREFIX + "_g7"
    qc = DefaultMQPushConsumer(group7)
    qc.set_instance_name("inst-c")
    qc.set_namesrv_addr(NAMESRV)
    qc.set_message_listener(make_listener([]))
    qc.subscribe(topic6, "*")
    qc.start()
    time.sleep(3)
    cid7 = qc.client_id
    list_before = probe.get_consumer_id_list_by_group(topic6, group7) or []
    qc.shutdown()
    time.sleep(2)
    list_after = probe.get_consumer_id_list_by_group(topic6, group7) or []
    probe.shutdown()
    check("S7 the shutdown unregistered the clientId",
          cid7 in list_before and cid7 not in list_after,
          "before=%d after=%d" % (len(list_before), len(list_after)))

    ns = PREFIX + "_NS"
    raw_topic = PREFIX + "_NsTopic"
    ns_topic = ns + "%" + raw_topic          # the real topic name on the broker side
    prepare_topic(ns_topic)
    group8 = PREFIX + "_g8"

    p8 = DefaultMQProducer(PREFIX + "_pg8", namespace=ns)
    p8.set_namesrv_addr(NAMESRV)

    n8 = []
    n8_plain = []
    seen8_topics = []
    lk8 = threading.Lock()

    class NsListener(MessageListenerConcurrently):
        def __init__(self, bucket):
            self.bucket = bucket

        def consume_message(self, msgs, context):
            with lk8:
                self.bucket.extend(bytes(m.body) for m in msgs)
                seen8_topics.extend(m.topic for m in msgs)
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS

    c8 = DefaultMQPushConsumer(group8, namespace=ns)
    c8.set_namesrv_addr(NAMESRV)
    c8.set_message_listener(NsListener(n8))
    c8.subscribe(raw_topic, "*")
    c8.start()
    # The counter-proof: a consumer without a namespace subscribes to the same raw_topic, reads a different topic and must not receive anything
    c8p = DefaultMQPushConsumer(PREFIX + "_g8p")
    c8p.set_namesrv_addr(NAMESRV)
    c8p.set_message_listener(NsListener(n8_plain))
    c8p.subscribe(raw_topic, "*")
    c8p.start()

    time.sleep(5)
    p8.start()
    p8.send(Message(raw_topic, b"ns-1"))

    deadline = time.time() + 25
    while time.time() < deadline and not any(b == b"ns-1" for b in n8):
        time.sleep(1)
    time.sleep(3)
    c8.shutdown()
    c8p.shutdown()
    p8.shutdown()
    check("S8 the namespaced consumer received the messages", any(b == b"ns-1" for b in n8),
          "got=%s" % [b.decode() for b in n8])
    check("S8 the consumer without a namespace receives nothing (isolation)", not any(b == b"ns-1" for b in n8_plain),
          "got=%s" % [b.decode() for b in n8_plain])
    check("S8 the topic is restored to the original business name", bool(seen8_topics) and all(t == raw_topic for t in seen8_topics),
          "topics=%s" % sorted(set(seen8_topics)))

    topic9 = PREFIX + "_Dlq"
    prepare_topic(topic9)
    group9 = PREFIX + "_g9"
    attempts = []
    lk9 = threading.Lock()
    # Record the delay of every delivery relative to the send moment: under a heavy load the
    # send-back is much slower than the theoretical value (the 10s/30s levels); with the delay in
    # hand it is possible to tell "the count is wrong" apart from "it simply arrived late" -- when
    # running four live test cases in parallel on the same machine, the 3rd delivery was measured
    # taking more than 100s, which is a slow environment and not an implementation bug.
    clock = {"sent": 0.0}

    class AlwaysFailListener(MessageListenerConcurrently):
        def consume_message(self, msgs, context):
            with lk9:
                for m in msgs:
                    attempts.append((m.get_reconsume_times(), bytes(m.body), m.topic,
                                     time.time() - clock["sent"]))
            return ConsumeConcurrentlyStatus.RECONSUME_LATER

    c9 = DefaultMQPushConsumer(group9)
    c9.set_namesrv_addr(NAMESRV)
    c9.set_message_listener(AlwaysFailListener())
    c9.max_reconsume_times = 2   # an explicit small value: the default of 16 would take hours
    c9.subscribe(topic9, "*")
    c9.start()
    time.sleep(5)
    # Start the consumer before sending (same as S8: CONSUME_FROM_LAST_OFFSET skips the messages sent before startup)
    clock["sent"] = time.time()
    sent9 = producer.send(Message(topic9, b"dlq-me"))
    # The send-back delay levels level3=10s and level4=30s => three deliveries take about 40s; 150s
    # leaves the margin for contention on the same machine
    deadline = time.time() + 150
    while time.time() < deadline and len(attempts) < 3:
        time.sleep(2)
    first_three = list(attempts)
    time.sleep(15)   # the counter-proof window: there must not be a 4th delivery
    final = list(attempts)
    c9.shutdown()
    times_seen = ["%d@%.0fs" % (a[0], a[3]) for a in first_three]
    check("S9 with maxReconsumeTimes=2 there are 3 deliveries (0/1/2 once each)",
          [a[0] for a in first_three][:3] == [0, 1, 2], "times=%s" % times_seen)
    check("S9 once exhausted there are no more deliveries (only 3 inside the observation window)", len(final) == 3,
          "arrivals=%d times=%s" % (len(final), [a[0] for a in final]))
    check("S9 during the redelivery the topic is restored to the business topic",
          all(a[2] == topic9 for a in first_three), "topics=%s" % sorted({a[2] for a in first_three}))

    # %DLQ%<group> is only created and route-registered by the broker when it delivers into the DLQ, so wait for the route first
    dlq_topic = MixAll.get_dlq_topic(group9)
    probe9 = MQClientInstance("dlq-probe-%d" % int(time.time() * 1000), [NAMESRV])
    probe9.start()
    deadline = time.time() + 30
    dlq_route = None
    while time.time() < deadline:
        dlq_route = probe9.get_topic_route_data(dlq_topic)
        if dlq_route is not None and dlq_route.broker_datas:
            break
        time.sleep(2)
    check("S9 the broker automatically created and registered the %DLQ%<group> route", dlq_route is not None and bool(dlq_route.broker_datas),
          "dlq=%s" % dlq_topic)
    probe9.shutdown()

    # Read the DLQ from the head using lite pull, to confirm that it is "exactly that message" rather than just seeing the offset move
    dlq_msgs = []
    if dlq_route is not None and dlq_route.broker_datas:
        lc9 = DefaultLitePullConsumer(PREFIX + "_g9dlq")
        lc9.set_namesrv_addr(NAMESRV)
        # A new consumer group plus CONSUME_FROM_LAST_OFFSET would start at the tail and skip the entry already in the DLQ => a false failure
        lc9.set_consume_from_where(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET)
        lc9.subscribe(dlq_topic, "*")
        lc9.start()
        deadline = time.time() + 25
        while time.time() < deadline:
            for m in lc9.poll(3000) or []:
                dlq_msgs.append(m)
            if dlq_msgs:
                break
            time.sleep(1)
        lc9.shutdown()
    got9 = [(bytes(m.body), m.get_reconsume_times(), m.topic,
             (m.properties or {}).get("RETRY_TOPIC")) for m in dlq_msgs]
    check("S9 the message landed in %DLQ%<group>", [g[0] for g in got9] == [b"dlq-me"],
          "got=%s" % [(g[0].decode(), g[1]) for g in got9])
    # The broker increments reconsumeTimes when storing it (AbstractSendMessageProcessor:226) => 3
    check("S9 the DLQ message has reconsumeTimes=3 (the 3rd send-back turned it into a dead letter)",
          len(got9) == 1 and got9[0][1] == 3, "got=%s" % got9)
    check("S9 the DLQ message keeps RETRY_TOPIC = the original business topic",
          len(got9) == 1 and got9[0][3] == topic9, "got=%s" % got9)
    check("S9 the topic of the DLQ message is exactly %DLQ%<group>",
          len(got9) == 1 and got9[0][2] == dlq_topic, "got=%s" % got9)
    print("S9: the original msgId=%s, the DLQ observation=%s" % (sent9.msg_id, got9))

    producer.shutdown()
    print("\nPASS=%d FAIL=%d" % (PASS, FAIL))
    return 0 if FAIL == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
