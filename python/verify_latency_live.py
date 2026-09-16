#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""发送延迟故障容错（sendLatencyFaultEnable）真机验证。

用法（需本地 RocketMQ 5.5.1 集群）：
    .venv/bin/python verify_latency_live.py 127.0.0.1:9876

场景（单 broker broker-a / DefaultCluster）：
  S1 默认关闭：发 10 条全 OK（行为不变，容错表不记录）
  S2 开启故障规避：发 20 条全 OK（broker 健康，不触发隔离）
  S3 成功发送后容错表有记录：broker-a 的 FaultItem.current_latency > 0 且 is_available
  S4 手工注入隔离（broker-a，隔离档位 10000ms）→ 再发送：单 broker 下走
     available→reachable→普通轮询 的退化链仍全部发出
  S5 隔离到期恢复：注入 2000ms 隔离，立即不可用，≈2s 后恢复可用
"""
from __future__ import annotations

import sys
import time

sys.path.insert(0, ".")

from rocketmq.client.producer import DefaultMQProducer
from rocketmq.common.message import Message

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
    print("== LatencyFaultTolerance 真机验证  namesrv=%s  topic=%s ==" % (NAMESRV, TOPIC))
    prep = DefaultMQProducer("PG_LatPrep_%d" % STAMP)
    prep.set_namesrv_addr(NAMESRV)
    prep.set_instance_name("LatPrep")
    prep.start()
    prep.create_topic("TBW102", TOPIC, 4)
    time.sleep(3)
    prep.shutdown()

    # ---------------- S1 默认关闭 ----------------
    print("\nS1 默认关闭：行为不变，不记录容错表")
    p1 = DefaultMQProducer("PG_LatOff_%d" % STAMP)
    p1.set_namesrv_addr(NAMESRV)
    p1.set_instance_name("LatOff")
    check("S1 默认 send_latency_fault_enable=False", p1._mq_fault_strategy.is_send_latency_fault_enable() is False)
    p1.start()
    n = send_ok(p1, 10, "off")
    check("S1 关闭时发 10 条全部成功", n == 10, "ok=%d" % n)
    check("S1 关闭时不记录容错表",
          p1._mq_fault_strategy.latency_fault_tolerance.get_fault_item(BROKER) is None)
    p1.shutdown()

    # ---------------- S2/S3 开启后健康路径 ----------------
    print("\nS2/S3 开启故障规避：健康 broker 正常发送并记录延迟")
    p2 = DefaultMQProducer("PG_LatOn_%d" % STAMP)
    p2.set_namesrv_addr(NAMESRV)
    p2.set_instance_name("LatOn")
    p2.set_send_latency_fault_enable(True)
    check("S2 开关生效", p2._mq_fault_strategy.is_send_latency_fault_enable() is True)
    p2.start()
    n = send_ok(p2, 20, "on")
    check("S2 开启后发 20 条全部成功", n == 20, "ok=%d" % n)
    item = p2._mq_fault_strategy.latency_fault_tolerance.get_fault_item(BROKER)
    check("S3 容错表已记录 broker-a", item is not None)
    if item is not None:
        check("S3 记录的实测延迟 > 0", item.current_latency > 0.0,
              "latency=%.1fms" % item.current_latency)
        check("S3 健康 broker 仍可用", item.is_available() and item.is_reachable())
        check("S3 延迟低于第一档阈值（未触发隔离）", item.start_timestamp == 0.0)

    # ---------------- S4 注入隔离 → 退化链 ----------------
    print("\nS4 注入隔离：available→reachable→普通轮询 退化链仍能发出")
    p2._mq_fault_strategy.update_fault_item(BROKER, 99999.0, True, False)
    check("S4 注入后 broker-a 不可用", not p2._mq_fault_strategy.latency_fault_tolerance.is_available(BROKER))
    n = send_ok(p2, 5, "iso")
    # 单 broker：三个过滤器都选不出 → 退化普通轮询，消息照发（不因隔离而丢发送能力）
    check("S4 隔离中单 broker 退化轮询仍发出 5 条", n == 5, "ok=%d" % n)

    # ---------------- S5 隔离到期恢复 ----------------
    print("\nS5 隔离到期恢复（remove 后注入 2000ms）")
    # 注意：S4 注入的是隔离档位 10000ms，而 updateNotAvailableDuration **只延长不缩短**
    # （Java 语义），必须先 remove 才能注入更短的 2000ms 档。
    p2._mq_fault_strategy.latency_fault_tolerance.remove(BROKER)
    p2._mq_fault_strategy.latency_fault_tolerance.update_fault_item(BROKER, 1.0, 2000, True)
    check("S5 隔离期内不可用", not p2._mq_fault_strategy.latency_fault_tolerance.is_available(BROKER))
    time.sleep(2.3)
    check("S5 到期后恢复可用", p2._mq_fault_strategy.latency_fault_tolerance.is_available(BROKER))
    n = send_ok(p2, 3, "after")
    check("S5 恢复后发送正常", n == 3, "ok=%d" % n)
    p2.shutdown()

    print("\n== 结果: PASS=%d FAIL=%d ==" % (PASS, FAIL))
    return 0 if FAIL == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
