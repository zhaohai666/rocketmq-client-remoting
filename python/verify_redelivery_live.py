#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""消费侧补齐真机验证（回投 / 位点持久化 / 顺序锁 / 广播 / 流控）。

用法（需本地 RocketMQ 5.5.1 集群）：
    .venv/bin/python verify_redelivery_live.py 127.0.0.1:9876

场景：
  S1 回投：listener 对 retry-me 首次返回 RECONSUME_LATER → 应经 %RETRY%topic
     （延迟梯度 level3=10s）二次投递，且 RECONSUME_TIMES=1；正常消息只投一次。
  S2 位点持久化：消费 3 条后重启同组消费者 → 旧消息不重复，新消息继续投递。
  S3 顺序消费：orderly listener 消费正常，且 broker 锁（LOCK_BATCH_MQ）生效。
  S4 广播模式：同组两个消费者各自收全所有消息，互不影响。
  S5 流控：阈值 2 + 慢消费 → 触发流控计数 >0，最终消息全部消费。
"""
from __future__ import annotations

import sys
import threading
import time

sys.path.insert(0, ".")

from rocketmq.client.consumer import (ConsumeConcurrentlyStatus,
                                      ConsumeOrderlyStatus,
                                      DefaultMQPushConsumer,
                                      MessageListenerConcurrently,
                                      MessageListenerOrderly,
                                      SimpleMessageListener)
from rocketmq.client.producer import DefaultMQProducer
from rocketmq.common.message import Message

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
    """通用收集器：记录 (body, topic, properties, ts)。"""

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


def main() -> int:
    producer = DefaultMQProducer(PREFIX + "_pg")
    producer.set_namesrv_addr(NAMESRV)
    producer.start()
    time.sleep(1)

    # ---------- S1 回投 ----------
    topic1 = PREFIX + "_Retry"
    group1 = PREFIX + "_g1"
    c1 = DefaultMQPushConsumer(group1)
    seen1 = []
    lock1 = threading.Lock()

    class RetryListener(MessageListenerConcurrently):
        def consume_message(self, msgs, context):
            for m in msgs:
                with lock1:
                    seen1.append((bytes(m.body), m.topic, m.get_reconsume_times(), time.time()))
            # retry-me 首次投递失败，重投后成功（重投次数走 MessageExt 线上字段）
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
    print("S1: 已发送，等待回投（延迟梯度 level3≈10s）...")
    time.sleep(22)
    c1.shutdown()
    retry_arrivals = [r for r in seen1 if r[0] == b"retry-me"]
    normal_arrivals = [r for r in seen1 if r[0] == b"normal-1"]
    retry_times_seen = [r[2] for r in retry_arrivals]
    check("S1-retry-me 被投递多次", len(retry_arrivals) >= 2,
          "arrivals=%d" % len(retry_arrivals))
    check("S1-重投来自 %RETRY%/RECONSUME_TIMES",
          any(r[2] >= 1 or r[1].startswith("%RETRY%") for r in retry_arrivals),
          "times=%s" % retry_times_seen)
    if len(retry_arrivals) >= 2:
        gap = retry_arrivals[-1][3] - retry_arrivals[0][3]
        check("S1-回投有延迟梯度(>=8s)", gap >= 8.0, "gap=%.1fs" % gap)
    else:
        check("S1-回投有延迟梯度(>=8s)", False, "不足两次投递")
    check("S1-正常消息只投一次", len(normal_arrivals) == 1,
          "arrivals=%d" % len(normal_arrivals))

    # ---------- S2 位点持久化 ----------
    topic2 = PREFIX + "_Offset"
    group2 = PREFIX + "_g2"
    c2 = DefaultMQPushConsumer(group2)
    col2 = Collector(c2)
    start_consumer(group2, topic2, col2)
    time.sleep(3)
    old_bodies = [b"persist-%d" % i for i in range(3)]
    for b in old_bodies:
        producer.send(Message(topic2, b))
    time.sleep(8)
    c2.shutdown()  # shutdown 持久化位点
    got_first_round = [b for b in old_bodies if b in set(col2.bodies())]
    check("S2-首轮消费 3 条", len(got_first_round) == 3,
          "got=%d" % len(got_first_round))
    # 重启同组消费者，只发 1 条新消息
    c2b = DefaultMQPushConsumer(group2)
    col2b = Collector(c2b)
    start_consumer(group2, topic2, col2b)
    time.sleep(3)
    producer.send(Message(topic2, b"persist-new"))
    time.sleep(8)
    c2b.shutdown()
    new_seen = b"persist-new" in set(col2b.bodies())
    old_resent = [b for b in old_bodies if b in set(col2b.bodies())]
    check("S2-重启后新消息继续投递", new_seen, "" if new_seen else "未收到")
    check("S2-重启不重复消费旧消息", len(old_resent) == 0,
          "重复=%s" % old_resent)

    # ---------- S3 顺序消费 + broker 锁 ----------
    topic3 = PREFIX + "_Orderly"
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
    time.sleep(5)  # 等 LOCK_BATCH_MQ 首轮生效
    for i in range(4):
        producer.send(Message(topic3, b"orderly-%d" % i))
    time.sleep(8)
    lock_ok_n = len(c3._lock_ok)
    c3.shutdown()
    check("S3-顺序消费收全", len(got3) == 4, "got=%d" % len(got3))
    check("S3-broker 队列锁生效", lock_ok_n > 0, "lockOK=%d" % lock_ok_n)

    # ---------- S4 广播模式 ----------
    topic4 = PREFIX + "_Bc"
    group4 = PREFIX + "_g4"

    def make_broadcast_consumer(inst_name: str) -> Collector:
        c = DefaultMQPushConsumer(group4)
        c.set_instance_name(inst_name)  # 保证两个消费者 clientId 不同
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
    check("S4-广播消费者 A 收全", na == 3, "got=%d" % na)
    check("S4-广播消费者 B 收全", nb == 3, "got=%d" % nb)

    # ---------- S5 流控 ----------
    topic5 = PREFIX + "_Flow"
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
    check("S5-慢消费下消息全部到达", len(got5) == 10, "got=%d" % len(got5))
    check("S5-流控触发计数>0", fc > 0, "triggered=%d" % fc)

    producer.shutdown()
    print("\nPASS=%d FAIL=%d" % (PASS, FAIL))
    return 0 if FAIL == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
