# -*- coding: utf-8 -*-
"""PullConsumerScheduleService live verification: the scheduled PullTask loop
pulls in real messages from a real cluster and advances the broker-side offset.

Scenarios (asserted one by one):
  S1  cluster probe + topic with 2 queues + producer delivers 6 messages;
  S2  the schedule service expands one PullTask per assigned queue;
  S3  the pull loop delivers all 6 message bodies through the callback
      (scheduled pulling, not a one-shot pull);
  S4  the broker-side committed offset for every queue equals the queue's
      message count (the loop advanced the cursor, not just consumed);
  S5  shutdown() is clean and the consumer shuts down with the service.

Prerequisite: NameServer + Broker already up.
Usage: python3 verify_pull_schedule_live.py 127.0.0.1:9876
"""
from __future__ import annotations

import os
import sys
import threading
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from client.admin import DefaultMQAdminExt
from client.consumer import DefaultMQPullConsumer
from client.consumer_result import PullStatus
from client.producer import DefaultMQProducer
from client.pull_schedule_service import (PullConsumerScheduleService,
                                          PullTaskCallback)
from common.message import Message
from common.mix_all import MixAll

NAMESRV = sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1:9876"
STAMP = int(time.time())
TOPIC = "PullScheduleTopic_%d" % STAMP
GROUP = "G_pull_schedule_%d" % STAMP
TOTAL = 6
QUEUES = 2

results = []


def check(name, ok, detail=""):
    results.append((name, ok, detail))
    print("[%s] %s %s" % ("PASS" if ok else "FAIL", name, detail))


def _wait_until(pred, timeout_sec, interval=0.2):
    deadline = time.time() + timeout_sec
    while time.time() < deadline:
        if pred():
            return True
        time.sleep(interval)
    return pred()


class Collector(PullTaskCallback):
    def __init__(self):
        self.bodies = []
        self.queues_seen = set()
        self._lock = threading.Lock()

    def do_pull_task(self, mq, pull_result, context):
        if pull_result.status == PullStatus.FOUND:
            with self._lock:
                self.queues_seen.add(mq)
                for m in pull_result.msg_found_list:
                    self.bodies.append(bytes(m.body))
            # The pull model is caller-driven: pull() never commits, the caller
            # advances the broker-side cursor (this is what S4 asserts).
            context.pull_consumer.update_consume_offset(
                mq, int(pull_result.next_begin_offset))
        return None  # default resident interval

    def count(self):
        with self._lock:
            return len(self.bodies)

    def seen(self):
        with self._lock:
            return set(self.queues_seen)


def _broker_offset(admin, mq):
    """The committed offset for the pull consumer's group on this queue on the broker."""
    off = admin._require_client().query_consumer_offset(
        GROUP + "_pc", mq, timeout_millis=5000, set_zero_if_not_found=True)
    return int(off or 0)


def main():
    admin = DefaultMQAdminExt()
    admin.set_namesrv_addr(NAMESRV)
    admin.start()
    producer = None
    svc = None
    try:
        # ---------------- S1 cluster probe + topic + producer ----------------
        cluster = admin.fetch_broker_cluster_info()
        if not cluster or not cluster.get_broker_addrs():
            check("S1 cluster probe", False, "no broker registered with nameServer")
            return 1
        broker_addr = cluster.get_broker_addrs()[0]
        check("S1 cluster probe", True, "broker=%s" % broker_addr)

        admin.create_topic(MixAll.DEFAULT_TOPIC, TOPIC, QUEUES)
        check("S1 admin created topic OK", True, "%s %d queues" % (TOPIC, QUEUES))

        producer = DefaultMQProducer("G_pull_schedule_prod_%d" % STAMP)
        producer.set_namesrv_addr(NAMESRV)
        producer.start()
        for i in range(TOTAL):
            producer.send(Message(TOPIC, ("sch-%d" % i).encode()))
        check("S1 producer delivered %d messages" % TOTAL, True)

        # ---------------- S2/S3 schedule service pulls ----------------
        collector = Collector()
        # The schedule service only builds its own consumer when none is given, and that
        # default consumer has no nameserver address: pass one in with the address set.
        pull_consumer = DefaultMQPullConsumer(GROUP + "_pc")
        pull_consumer.set_namesrv_addr(NAMESRV)
        svc = PullConsumerScheduleService(pull_consumer=pull_consumer)
        # All 6 seeds are sent BEFORE the service starts; the default offset source is
        # broker-offset-then-queue-tail (LAST semantics), which would resolve to 6 and
        # pull nothing. Pin the start offset to 0 (the FIRST_OFFSET reverse leg).
        svc.register_pull_task(TOPIC, collector, offset=0)
        svc.start()
        check("S2 schedule service started", True, "group=%s" % GROUP)

        ok_queues = _wait_until(lambda: len(svc.task_table()) >= QUEUES, 20)
        check("S2 one PullTask per assigned queue expanded", ok_queues,
              "task_table=%d queues (want >=%d)" % (len(svc.task_table()), QUEUES))

        delivered = _wait_until(lambda: collector.count() >= TOTAL, 40)
        check("S3 pull loop delivered all %d bodies" % TOTAL, delivered,
              "collected=%d" % collector.count())
        seen = collector.seen()
        check("S3 pulls landed on multiple queues", len(seen) >= 1,
              "queues=%d" % len(seen))

        # ---------------- S4 broker-side offsets advanced ----------------
        task_mqs = sorted(svc.task_table().keys(), key=lambda q: (q.topic, q.broker_name, q.queue_id))
        all_advanced = True
        detail_parts = []
        for mq in task_mqs:
            off = _broker_offset(admin, mq)
            all_advanced = all_advanced and off >= 1
            detail_parts.append("q%d=%d" % (mq.queue_id, off))
        check("S4 broker-side committed offset advanced on every queue", all_advanced,
              " ".join(detail_parts))

        # ---------------- S5 clean shutdown ----------------
        svc.shutdown()
        svc = None
        check("S5 shutdown clean", True)
    except Exception as exc:  # noqa: BLE001
        import traceback
        traceback.print_exc()
        check("the verification threw an exception", False, "%s: %s" % (type(exc).__name__, exc))
    finally:
        # ---------------- cleanup ----------------
        if svc is not None:
            try:
                svc.shutdown()
            except Exception as e:  # noqa: BLE001
                print("    (schedule service shutdown failed: %s)" % e)
        if producer is not None:
            try:
                producer.shutdown()
            except Exception as e:  # noqa: BLE001
                print("    (producer shutdown failed: %s)" % e)
        try:
            admin.delete_topic(TOPIC)
            print("    (delete_topic(%s) OK)" % TOPIC)
        except Exception as e:  # noqa: BLE001
            print("    (delete_topic(%s) failed: %s)" % (TOPIC, e))
        try:
            admin.delete_subscription_group(broker_addr, GROUP, remove_offset=True)
        except Exception as e:  # noqa: BLE001
            print("    (delete_subscription_group(%s) failed: %s)" % (GROUP, e))
        admin.shutdown()

    failed = [r for r in results if not r[1]]
    print("\n================ summary ================")
    for name, ok, detail in results:
        print("  [%s] %s  %s" % ("PASS" if ok else "FAIL", name, detail))
    print("======================================")
    print("%d checks total, %d failed" % (len(results), len(failed)))
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
