#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import annotations

import sys
import threading
import time

sys.path.insert(0, ".")

from client.admin import DefaultMQAdminExt
from client.consumer import (ConsumeConcurrentlyStatus,
                                      DefaultMQPushConsumer,
                                      SimpleMessageListener)
from client.mq_client import MQClientInstance
from client.producer import DefaultMQProducer
from common.message import Message

NAMESRV = sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1:9876"
PREFIX = "GapSub_%d" % int(time.time() * 1000)

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
    pass
    c = MQClientInstance("setup-%d" % int(time.time() * 1000), [NAMESRV])
    c.start()
    try:
        c.create_topic_in_route(topic, queues, queues)
    finally:
        c.shutdown()


def broker_addr(admin: DefaultMQAdminExt) -> str:
    try:
        return admin.fetch_broker_cluster_info().get_broker_addrs()[0]
    except Exception:  # noqa: BLE001
        return "127.0.0.1:10911"


def who(admin: DefaultMQAdminExt, broker: str, topic: str) -> set:
    pass
    return admin.query_topic_consume_by_who(broker, topic)


def wait_for(pred, timeout_s: float, interval_s: float = 0.5):
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        v = pred()
        if v:
            return v
        time.sleep(interval_s)
    return pred()


def main() -> int:
    admin = DefaultMQAdminExt()
    admin.set_namesrv_addr(NAMESRV)
    admin.start()
    broker = broker_addr(admin)
    print("broker=%s" % broker)

    topic_l = PREFIX + "_Late"
    topic_b = PREFIX + "_Base"
    group = PREFIX + "_g"
    prepare_topic(topic_l)
    prepare_topic(topic_b)

    seen = []
    lock = threading.Lock()

    def on_msg(msgs):
        with lock:
            for m in msgs:
                seen.append((bytes(m.body), m.topic))
        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS

    consumer = DefaultMQPushConsumer(group)
    consumer.set_namesrv_addr(NAMESRV)
    consumer.set_message_listener(SimpleMessageListener(on_msg))
    consumer.subscribe(topic_b, "*")
    consumer.start()

    # ---------- S0 positive control ----------
    t0 = time.time()
    got = wait_for(lambda: who(admin, broker, topic_b) or None, 35)
    check("S0-baseline topic B has this group registered (found via 300)", group in got,
          "groupList=%s elapsed=%.2fs" % (sorted(got), time.time() - t0))

    # ---------- S1 negative control ----------
    got_l = who(admin, broker, topic_l)
    check("S1-negative control: the unsubscribed L does not find this group", group not in got_l,
          "groupList=%s" % sorted(got_l))

    # ---------- S2 a late subscription takes effect immediately ----------
    t1 = time.time()
    consumer.subscribe(topic_l, "*")
    subscribe_ms = (time.time() - t1) * 1000.0
    got_l = who(admin, broker, topic_l)
    elapsed_ms = (time.time() - t1) * 1000.0
    check("S2-the broker registers this group immediately after the late subscription (found via 300)", group in got_l,
          "groupList=%s" % sorted(got_l))
    # The heartbeat cycle defaults to 30s: only the subscription path's synchronous heartbeat round can make the registration appear this fast.
    check("S2-registration time far below the 30s heartbeat cycle (can only be pushed by the subscription path)",
          group in got_l and elapsed_ms < 5000,
          "subscribe return took=%.1fms, query completion took=%.1fms" % (subscribe_ms, elapsed_ms))

    # ---------- S3 a late subscription really gets consumed ----------
    t2 = time.time()
    # _mq_key looks like "<topic><brokerName><queueId>" (no separator), so filtering by topic prefix is enough.
    assigned = wait_for(
        lambda: [k for k in consumer.assigned_queue_keys() if k.startswith(topic_l)] or None,
        45)
    check("S3-new topic L enters this instance's assignment set (rebalance works)", bool(assigned),
          "assigned=%s elapsed=%.1fs" % (assigned, time.time() - t2))

    producer = DefaultMQProducer(PREFIX + "_pg")
    producer.set_namesrv_addr(NAMESRV)
    producer.start()
    time.sleep(0.5)
    producer.send(Message(topic_l, b"late-subscribe-me"))
    got_msg = wait_for(lambda: [s for s in seen if s[0] == b"late-subscribe-me"] or None, 30)
    check("S3-messages on the late-subscribed topic are really consumed", bool(got_msg),
          "seen=%s" % [(b.decode(), t) for b, t in seen])

    # ---------- S4 live subscription table ----------
    consumer.unsubscribe(topic_l)
    live = {s.topic for s in consumer.subscriptions()}
    check("S4-this group's subscription set immediately loses L after unsubscribe (only deletes the entry, no heartbeat)",
          topic_l not in live and topic_b in live, "live=%s" % sorted(live))

    consumer.shutdown()
    producer.shutdown()
    admin.shutdown()
    print("\nPASS=%d FAIL=%d" % (PASS, FAIL))
    return 0 if FAIL == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
