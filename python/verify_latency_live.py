#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Live verification of send-latency fault tolerance (sendLatencyFaultEnable).

Usage (requires a local RocketMQ 5.5.1 cluster):
    .venv/bin/python verify_latency_live.py 127.0.0.1:9876

Scenarios (single broker broker-a / DefaultCluster):
  S1 off by default: send 10, all OK (behavior unchanged, nothing recorded in the fault table)
  S2 fault avoidance on: send 20, all OK (the broker is healthy, no isolation triggered)
  S3 the fault table has a record after a successful send: broker-a's FaultItem.current_latency > 0 and is_available
  S4 manually inject isolation (broker-a, 10000ms isolation level) -> send again: under a single broker it still goes through
     the available->reachable->plain-poll degraded chain and still sends everything
  S5 isolation expiry recovery: inject a 2000ms isolation, immediately unavailable, available again after ≈2s
"""
from __future__ import annotations

import sys
import time

sys.path.insert(0, ".")

from client.producer import DefaultMQProducer
from common.message import Message

NAMESRV = sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1:9876"
STAMP = int(time.time() * 1000)
TOPIC = "Latency_%d" % STAMP
BROKER = "broker-a"

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


def send_ok(producer: DefaultMQProducer, count: int, key: str) -> int:
    ok = 0
    for i in range(count):
        try:
            producer.send(Message(TOPIC, ("%s-%d" % (key, i)).encode()))
            ok += 1
        except BaseException as e:  # noqa: BLE001
            print("    send %d failed: %s: %s" % (i, type(e).__name__, e))
    return ok


def main() -> int:
    print("== LatencyFaultTolerance live verification  namesrv=%s  topic=%s ==" % (NAMESRV, TOPIC))
    prep = DefaultMQProducer("PG_LatPrep_%d" % STAMP)
    prep.set_namesrv_addr(NAMESRV)
    prep.set_instance_name("LatPrep")
    prep.start()
    prep.create_topic("TBW102", TOPIC, 4)
    time.sleep(3)
    prep.shutdown()

    # ---------------- S1 off by default ----------------
    print("\nS1 off by default: behavior unchanged, nothing recorded in the fault table")
    p1 = DefaultMQProducer("PG_LatOff_%d" % STAMP)
    p1.set_namesrv_addr(NAMESRV)
    p1.set_instance_name("LatOff")
    check("S1 send_latency_fault_enable=False by default", p1._mq_fault_strategy.is_send_latency_fault_enable() is False)
    p1.start()
    n = send_ok(p1, 10, "off")
    check("S1 all 10 sends succeed when off", n == 10, "ok=%d" % n)
    check("S1 nothing is recorded in the fault table when off",
          p1._mq_fault_strategy.latency_fault_tolerance.get_fault_item(BROKER) is None)
    p1.shutdown()

    # ---------------- S2/S3 the healthy path after enabling ----------------
    print("\nS2/S3 fault avoidance on: a healthy broker sends normally and records latency")
    p2 = DefaultMQProducer("PG_LatOn_%d" % STAMP)
    p2.set_namesrv_addr(NAMESRV)
    p2.set_instance_name("LatOn")
    p2.set_send_latency_fault_enable(True)
    check("S2 the switch takes effect", p2._mq_fault_strategy.is_send_latency_fault_enable() is True)
    p2.start()
    n = send_ok(p2, 20, "on")
    check("S2 all 20 sends succeed after enabling", n == 20, "ok=%d" % n)
    item = p2._mq_fault_strategy.latency_fault_tolerance.get_fault_item(BROKER)
    check("S3 the fault table has recorded broker-a", item is not None)
    if item is not None:
        check("S3 the recorded measured latency > 0", item.current_latency > 0.0,
              "latency=%.1fms" % item.current_latency)
        check("S3 a healthy broker is still available", item.is_available() and item.is_reachable())
        check("S3 the latency is below the first threshold (no isolation triggered)", item.start_timestamp == 0.0)

    # ---------------- S4 inject isolation -> degraded chain ----------------
    print("\nS4 inject isolation: the available->reachable->plain-poll degraded chain still sends")
    p2._mq_fault_strategy.update_fault_item(BROKER, 99999.0, True, False)
    check("S4 broker-a is unavailable after injection", not p2._mq_fault_strategy.latency_fault_tolerance.is_available(BROKER))
    n = send_ok(p2, 5, "iso")
    # Single broker: none of the three filters can pick one -> degrade to plain polling, and messages still send (isolation does not cost sending ability)
    check("S4 5 messages still send via degraded polling on a single isolated broker", n == 5, "ok=%d" % n)

    # ---------------- S5 isolation expiry recovery ----------------
    print("\nS5 isolation expiry recovery (inject 2000ms after remove)")
    p2._mq_fault_strategy.latency_fault_tolerance.remove(BROKER)
    p2._mq_fault_strategy.latency_fault_tolerance.update_fault_item(BROKER, 1.0, 2000, True)
    check("S5 unavailable during the isolation window", not p2._mq_fault_strategy.latency_fault_tolerance.is_available(BROKER))
    time.sleep(2.3)
    check("S5 available again after expiry", p2._mq_fault_strategy.latency_fault_tolerance.is_available(BROKER))
    n = send_ok(p2, 3, "after")
    check("S5 sending is normal after recovery", n == 3, "ok=%d" % n)
    p2.shutdown()

    print("\n== result: PASS=%d FAIL=%d ==" % (PASS, FAIL))
    return 0 if FAIL == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
