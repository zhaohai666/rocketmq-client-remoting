#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""顺序消费的重投闸门真机验证（Java ConsumeMessageOrderlyService#checkReconsumeTimes）。

用法（需本地 RocketMQ 5.5.1 集群）：
    .venv/bin/python verify_orderly_reconsume_live.py 127.0.0.1:9876

为什么必须真机：闸门本身（本地 reconsumeTimes 计数 + 到点交给 broker）离线能测，
但「交给 broker 之后发生了什么」离线一点都测不出来 —— 未 start 的消费者压根没有内部
生产者，回投必定失败，于是只锁得住「失败分支」。而这条链路的三个真实故障全在成功分支
上，并且**全都是静默的**：

  - 抬进请求头：broker 判死信读的是 requestHeader.reconsumeTimes / maxReconsumeTimes
    （SendMessageProcessor#handleRetryAndDLQ:197-210），不看报文属性。没抬就等于
    消费者配的 maxReconsumeTimes 形同虚设。
  - 回投成功后必须前进位点：Java :266-296 这时 commit 位点让毒消息走人。写错就是
    一条毒消息永久占住整条队列，表现和「消费者死了」一模一样。
  - 顺序组的锁没过期 ⇒ broker 直接把这条 %RETRY% 投递改投 %DLQ%
    （handleRetryAndDLQ:202-207 `isLockAllExpired`）。这是顺序消费独有的终态，
    只有真broker 会做。

场景：
  O1 到点交给 broker：maxReconsumeTimes=2 的顺序消费者遇毒消息 ⇒ 本地投递 3 次
     （reconsumeTimes 0/1/2），第 3 次回投成功后业务队列**继续往前**（后面的正常消息
     照样消费），且消息落在 %DLQ%<group>（reconsumeTimes=3、RETRY_TOPIC=业务 topic）。
  O2 -1 是不设限：默认（-1）配置下同一条毒消息投递远超 3 次仍不进 %DLQ%
     （Java OrderlyService#getMaxReconsumeTimes:313-320 把 -1 读成 Integer.MAX_VALUE，
     与并发侧的 16 不是一套）。写错成 16 就会凭空造死信。
"""
from __future__ import annotations

import sys
import threading
import time

sys.path.insert(0, ".")

from rocketmq.client.consumer import (ConsumeOrderlyStatus,
                                      DefaultLitePullConsumer,
                                      DefaultMQPushConsumer,
                                      MessageListenerOrderly)
from rocketmq.client.mq_client import MQClientInstance
from rocketmq.client.producer import DefaultMQProducer
from rocketmq.common.message import Message
from rocketmq.common.mix_all import MixAll
from rocketmq.remoting.protocol.heartbeat import ConsumeFromWhere

NAMESRV = sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1:9876"
PREFIX = "OrdPy_%d" % int(time.time() * 1000)

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


def prepare_topic(topic: str, queues: int = 1) -> None:
    """建 topic。队列数固定 1：顺序消费要的是「一条队列里的确定顺序」，多队列时毒消息
    和后面那条正常消息会散到不同队列，占位/放行就看不出差别了。"""
    c = MQClientInstance("ord-setup-%d" % int(time.time() * 1000), [NAMESRV])
    c.start()
    try:
        c.create_topic_in_route(topic, queues, queues)
    finally:
        c.shutdown()


def read_dlq(group: str, timeout: float = 25.0):
    """用 lite pull 从队首读 %DLQ%<group>，返回 (body, reconsumeTimes, topic, RETRY_TOPIC)。

    必须从 FIRST_OFFSET 读：DLQ topic 是 broker 在投死信那一刻才建出来的，新组默认的
    LAST_OFFSET 会从「订阅时刻」的队尾开始，正好把刚进去的那条跳过 ⇒ 假失败。"""
    dlq = MixAll.get_dlq_topic(group)
    c = DefaultLitePullConsumer(PREFIX + "_dlqreader")
    c.set_namesrv_addr(NAMESRV)
    c.set_consume_from_where(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET)
    c.subscribe(dlq, "*")
    c.start()
    out = []
    try:
        deadline = time.time() + timeout
        while time.time() < deadline:
            for m in c.poll(3000) or []:
                out.append((bytes(m.body), m.get_reconsume_times(), m.topic,
                            (m.properties or {}).get("RETRY_TOPIC")))
            if out:
                break
            time.sleep(1)
    finally:
        c.shutdown()
    return dlq, out


class PoisonListener(MessageListenerOrderly):
    """毒消息一直挂起（每次都返回 SUSPEND，永不「成功」），其余照常成功。

    只记 body/reconsumeTimes：顺序消费是单线程逐批的，不需要锁，但消费者重启后
    listener 实例会被复用，这里用列表累积整轮观测。"""

    def __init__(self, poison: bytes):
        self.poison = poison
        self.records = []
        self._lock = threading.Lock()

    def consume_message(self, msgs, context):
        with self._lock:
            for m in msgs:
                self.records.append((bytes(m.body), m.get_reconsume_times(), m.topic))
        if any(bytes(m.body) == self.poison for m in msgs):
            return ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT
        return ConsumeOrderlyStatus.SUCCESS

    def deliveries(self, poison: bytes):
        with self._lock:
            return [r for r in self.records if r[0] == poison]

    def bodies(self):
        with self._lock:
            return [r[0] for r in self.records]


def main() -> int:
    producer = DefaultMQProducer(PREFIX + "_pg")
    producer.set_namesrv_addr(NAMESRV)
    producer.start()
    time.sleep(1)

    # ---------- O1 到点交给 broker，业务队列继续往前 ----------
    topic1 = PREFIX + "_OrdDlq"
    group1 = PREFIX + "_g1"
    prepare_topic(topic1)
    listener1 = PoisonListener(b"ord-poison")
    c1 = DefaultMQPushConsumer(group1)
    c1.set_namesrv_addr(NAMESRV)
    c1.set_message_listener(listener1)
    c1.consume_message_batch_max_size = 1
    c1.max_reconsume_times = 2      # 显式小值：默认不设限，等到天荒地老
    c1.suspend_current_queue_time_millis = 500
    c1.subscribe(topic1, "*")
    c1.start()
    time.sleep(5)   # 等首轮 LOCK_BATCH_MQ，broker 侧才算「该组持有未过期锁」
    sent1 = producer.send(Message(topic1, b"ord-poison"))
    producer.send(Message(topic1, b"ord-after"))
    print("O1: 毒消息已发送 msgId=%s，等待本地计数到点..." % sent1.msg_id)

    # 本地重试只 sleep 500ms/轮（不过 broker），3 次投递约 1.5s；后面那条正常消息要等
    # 位点前进才会被拉到。窗口给 90s 是同机并跑四套真机用例时的竞争余量。
    deadline = time.time() + 90
    while time.time() < deadline and b"ord-after" not in listener1.bodies():
        time.sleep(1)
    time.sleep(3)   # 反证窗口：毒消息不该再被投回来
    poison1 = listener1.deliveries(b"ord-poison")
    times1 = [p[1] for p in poison1]
    check("O1-maxReconsumeTimes=2 时毒消息本地投递 3 次（reconsumeTimes 0/1/2）",
          times1 == [0, 1, 2], "times=%s" % times1)
    check("O1-回投成功后业务队列继续往前（后面的正常消息被消费）",
          b"ord-after" in listener1.bodies(), "bodies=%s" % listener1.bodies())
    check("O1-毒消息不再投回（观察窗口内只有 3 次投递）", len(poison1) == 3,
          "arrivals=%d" % len(poison1))
    check("O1-投递期间 topic 一直是业务 topic（顺序重试不过 %RETRY%）",
          all(p[2] == topic1 for p in poison1),
          "topics=%s" % sorted({p[2] for p in poison1}))
    lock_ok = len(c1._lock_ok)
    check("O1-该组持有 broker 队列锁（死信直投的判据）", lock_ok > 0, "lockOK=%d" % lock_ok)
    c1.shutdown()

    dlq1, got1 = read_dlq(group1)
    check("O1-毒消息落在 %DLQ%<group>", [g[0] for g in got1] == [b"ord-poison"],
          "dlq=%s got=%s" % (dlq1, [(g[0].decode(), g[1]) for g in got1]))
    # 客户端抬进头里的 reconsumeTimes=3（回投时 +1），broker 原样存储
    check("O1-DLQ 消息 reconsumeTimes=3", len(got1) == 1 and got1[0][1] == 3,
          "got=%s" % got1)
    check("O1-DLQ 消息保留 RETRY_TOPIC=业务 topic",
          len(got1) == 1 and got1[0][3] == topic1, "got=%s" % got1)
    check("O1-DLQ 消息 topic 就是 %DLQ%<group>",
          len(got1) == 1 and got1[0][2] == dlq1, "got=%s" % got1)
    print("O1: 原始 msgId=%s，DLQ 观测=%s" % (sent1.msg_id, got1))

    # ---------- O2 -1 在顺序侧是「不设限」 ----------
    # Java 顺序侧 -1 → Integer.MAX_VALUE（OrderlyService#getMaxReconsumeTimes:313-320），
    # 并发侧 -1 → 16（DefaultMQPushConsumerImpl#getMaxReconsumeTimes:890）。并成一个常量
    # 的话，这条用默认配置的正常路径会在第 16 次凭空造出一条死信。
    topic2 = PREFIX + "_OrdUnlimited"
    group2 = PREFIX + "_g2"
    prepare_topic(topic2)
    listener2 = PoisonListener(b"ord-keep")
    c2 = DefaultMQPushConsumer(group2)
    c2.set_namesrv_addr(NAMESRV)
    c2.set_message_listener(listener2)
    c2.consume_message_batch_max_size = 1
    # max_reconsume_times 保持默认 -1
    c2.suspend_current_queue_time_millis = 200
    c2.subscribe(topic2, "*")
    c2.start()
    time.sleep(4)
    producer.send(Message(topic2, b"ord-keep"))
    print("O2: 默认（-1）配置的毒消息已发送，观察是否会被提前判死...")
    deadline = time.time() + 30
    while time.time() < deadline and len(listener2.deliveries(b"ord-keep")) < 25:
        time.sleep(1)
    n2 = len(listener2.deliveries(b"ord-keep"))
    check("O2-默认配置下持续原地重试（远超并发侧的 16）", n2 > 16, "deliveries=%d" % n2)
    check("O2-本地计数一直前进（0..n 连续）",
          [p[1] for p in listener2.deliveries(b"ord-keep")][:20] == list(range(20)),
          "times=%s" % [p[1] for p in listener2.deliveries(b"ord-keep")][:20])
    c2.shutdown()
    dlq2, got2 = read_dlq(group2, timeout=12)
    check("O2-没到阈值就不该有死信（%s 为空）" % dlq2, got2 == [], "got=%s" % got2)

    producer.shutdown()
    print("\nPASS=%d FAIL=%d" % (PASS, FAIL))
    return 0 if FAIL == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
