#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""后置订阅真机验证（对齐 Java subscribe 之后的「立即推一轮心跳」）。

用法（需本地 RocketMQ 5.5.1 集群）：
    .venv/bin/python verify_subscribe_live.py 127.0.0.1:9876

Java ``DefaultMQPushConsumerImpl.subscribe:1265-1275`` 只做两件事：
``subscriptionInner.put(...)`` + ``if (this.mQClientFactory != null)
this.mQClientFactory.sendHeartbeatToAllBrokerWithLock();`` —— 允许 start() 之后订阅，
而且**同步**推一轮心跳。这一轮的观测点是 broker 的 topic→group 表（
``ConsumerManager.registerConsumer`` 维护，``QUERY_TOPIC_CONSUME_BY_WHO(300)`` 读取）：
订阅路径不推心跳的话，表里要等下一个 30s 心跳周期才出现本组。

场景：
  S0 正对照：start() 之后基础 topic B 已登记本组（心跳链路与 300 查询本身是通的）。
  S1 负对照：本轮**还没**订阅的 L，300 查不到本组。
  S2 后置订阅立即生效：subscribe(L) 之后不睡直接查 300(L) → 本组已在表里，
     且耗时远小于心跳周期（默认 30s）⇒ 只可能来自订阅路径那一轮同步心跳。
  S3 后置订阅真会被消费：L 进分配集 → 发一条消息 → listener 收到。
  S4 活订阅表：unsubscribe(L) 后本组订阅集立刻少掉 L。
     （Java:1317-1319 只删表项、**不**推心跳，且 broker 的 topicGroupTable 只在整组
     无订阅时才清 —— 所以这里不拿 broker 的表当断言。）
"""
from __future__ import annotations

import sys
import threading
import time

sys.path.insert(0, ".")

from rocketmq.client.admin import DefaultMQAdminExt
from rocketmq.client.consumer import (ConsumeConcurrentlyStatus,
                                      DefaultMQPushConsumer,
                                      SimpleMessageListener)
from rocketmq.client.mq_client import MQClientInstance
from rocketmq.client.producer import DefaultMQProducer
from rocketmq.common.message import Message

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
    """先建 topic：消费者不做默认 topic 兜底（对齐 Java），topic 不存在就拿不到路由。"""
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
    """QUERY_TOPIC_CONSUME_BY_WHO(300)：broker 侧 topic→group 表（Java 同名查询）。"""
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

    # ---------- S0 正对照 ----------
    t0 = time.time()
    got = wait_for(lambda: who(admin, broker, topic_b) or None, 35)
    check("S0-基础 topic B 已登记本组（300 查得到）", group in got,
          "groupList=%s elapsed=%.2fs" % (sorted(got), time.time() - t0))

    # ---------- S1 负对照 ----------
    got_l = who(admin, broker, topic_l)
    check("S1-负对照：未订阅的 L 查不到本组", group not in got_l,
          "groupList=%s" % sorted(got_l))

    # ---------- S2 后置订阅立即生效 ----------
    t1 = time.time()
    consumer.subscribe(topic_l, "*")
    subscribe_ms = (time.time() - t1) * 1000.0
    got_l = who(admin, broker, topic_l)
    elapsed_ms = (time.time() - t1) * 1000.0
    check("S2-后置订阅后 broker 立刻登记本组（300 查得到）", group in got_l,
          "groupList=%s" % sorted(got_l))
    # 心跳周期默认 30s：只有订阅路径那一轮**同步**心跳才能让登记这么快出现。
    check("S2-登记耗时远小于 30s 心跳周期（只可能是订阅路径推的）",
          group in got_l and elapsed_ms < 5000,
          "subscribe 返回耗时=%.1fms，查询完成耗时=%.1fms" % (subscribe_ms, elapsed_ms))

    # ---------- S3 后置订阅真会被消费 ----------
    t2 = time.time()
    # _mq_key 形如 "<topic><brokerName><queueId>"（无分隔符），按 topic 前缀筛即可。
    assigned = wait_for(
        lambda: [k for k in consumer.assigned_queue_keys() if k.startswith(topic_l)] or None,
        45)
    check("S3-新 topic L 进入本实例分配集（rebalance 生效）", bool(assigned),
          "assigned=%s elapsed=%.1fs" % (assigned, time.time() - t2))

    producer = DefaultMQProducer(PREFIX + "_pg")
    producer.set_namesrv_addr(NAMESRV)
    producer.start()
    time.sleep(0.5)
    producer.send(Message(topic_l, b"late-subscribe-me"))
    got_msg = wait_for(lambda: [s for s in seen if s[0] == b"late-subscribe-me"] or None, 30)
    check("S3-后置订阅的 topic 上的消息真的被消费", bool(got_msg),
          "seen=%s" % [(b.decode(), t) for b, t in seen])

    # ---------- S4 活订阅表 ----------
    consumer.unsubscribe(topic_l)
    live = {s.topic for s in consumer.subscriptions()}
    check("S4-unsubscribe 后本组订阅集立刻少掉 L（只删表项，不发心跳）",
          topic_l not in live and topic_b in live, "live=%s" % sorted(live))

    consumer.shutdown()
    producer.shutdown()
    admin.shutdown()
    print("\nPASS=%d FAIL=%d" % (PASS, FAIL))
    return 0 if FAIL == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
