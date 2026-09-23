#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""``ackIndex``（部分 ack）真机验证 —— 并发消费 classic 路径。

用法（需本地 RocketMQ 5.5.1 集群）：
    .venv/bin/python verify_ack_index_live.py 127.0.0.1:9876

为什么单独一个脚本：Java ``ConsumeMessageConcurrentlyService#processConsumeResult:207-254``
里，``ConsumeConcurrentlyContext.ackIndex``（默认 ``Integer.MAX_VALUE``）是 listener
表达「这批我只认可到第几条」的唯一手段：认可前缀提交位点，其后的条目逐条
``sendMessageBack`` 回投 ``%RETRY%``。本端口过去只在 POP 路径用它，classic 路径整段忽略 ——
listener 设了 ackIndex 之后尾巴**既不回投、位点又照样越过**，是静默丢消息。
「丢了几条」这种错必须看 broker：光看客户端收到的条数，全 ack 和过度回投都能伪装成通过。

场景：
  A1 对照组（默认整批认可）：3 条各投一次，位点提交到 3，没有任何回投。
  A2 部分 ack（ackIndex=0）：首批 3 条只认可第 1 条 →
     第 1 条整个窗口只投一次；后 2 条经 %RETRY% 二次到达（reconsumeTimes>=1、
     topic 还原成业务 topic）；业务队列位点仍提交到 3；3 条最终全部被消费，无丢失。
  A3 RECONSUME_LATER 压过 ackIndex：listener 设 ackIndex=2 却返回 RECONSUME_LATER
     → 3 条全部重投（Java:222-226 强制 ackIndex=-1）。
  A4 广播模式：ackIndex=0 时尾巴不回投（%RETRY% 上不出现它），本地整批前进。
"""
from __future__ import annotations

import sys
import threading
import time

sys.path.insert(0, ".")

from rocketmq.client.consumer import (ConsumeConcurrentlyStatus,
                                      DefaultMQPushConsumer,
                                      MessageListenerConcurrently)
from rocketmq.client.mq_client import MQClientInstance
from rocketmq.client.producer import DefaultMQProducer
from rocketmq.common.message import Message
from rocketmq.remoting.protocol.heartbeat import ConsumeFromWhere, MessageModel

NAMESRV = sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1:9876"
PREFIX = "AckPy_%d" % int(time.time() * 1000)

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


def new_client(tag: str) -> MQClientInstance:
    c = MQClientInstance("%s-%d" % (tag, int(time.time() * 1000)), [NAMESRV])
    c.start()
    return c


def prepare_topic(topic: str, queues: int = 1) -> None:
    """建 topic。队列数默认 1：部分 ack 要求「一批里 3 条同一队列且连续」，
    多队列时消息会散开，批次归属就不确定了（真实用法里 topic 由管理员预先建好）。"""
    c = new_client("setup")
    try:
        c.create_topic_in_route(topic, queues, queues)
    finally:
        c.shutdown()


def wait_until(pred, timeout: float, interval: float = 1.0) -> bool:
    """轮询到条件成立（broker 侧对账一律轮询，不要固定 sleep：新 topic 注册是秒级的）。"""
    deadline = time.time() + timeout
    while time.time() < deadline:
        if pred():
            return True
        time.sleep(interval)
    return pred()


def committed_offset(group: str, topic: str) -> int:
    """读 broker 上该 group 在这个 topic 各队列的已提交位点之和。

    一次查询超时不能让整轮验证崩掉：这里把它当成「还没读到」（-1），由调用方的
    wait_until 继续轮询。"""
    c = new_client("reader")
    try:
        publish = c.get_topic_publish_info(topic)
        if publish is None or not publish.msg_queue_list:
            return -1
        total = 0
        for mq in publish.msg_queue_list:
            off = c.query_consumer_offset(group, mq)
            total += off or 0
        return total
    except Exception:
        return -1
    finally:
        c.shutdown()


class RecordingListener(MessageListenerConcurrently):
    """按批次记录，并（可选）把 ackIndex 写进 context。"""

    def __init__(self, ack_index=None, status=ConsumeConcurrentlyStatus.CONSUME_SUCCESS,
                 ack_first_batch_only=False):
        self.ack_index = ack_index
        self.status = status
        self.ack_first_batch_only = ack_first_batch_only
        self.batches = []
        self._lock = threading.Lock()

    def consume_message(self, msgs, context):
        with self._lock:
            self.batches.append([(bytes(m.body), m.topic, m.get_reconsume_times(),
                                  time.time()) for m in msgs])
            index = len(self.batches)
        if self.ack_index is not None:
            # 只收窄**首批**：重投回来的那一批必须整批认可，否则永远收敛不了
            if not self.ack_first_batch_only or index == 1:
                context.ack_index = self.ack_index
        return self.status

    def arrivals(self):
        with self._lock:
            return [r for batch in self.batches for r in batch]


def start(group: str, topic: str, listener, batch_max: int = 3,
          model: str = None, from_first: bool = False) -> DefaultMQPushConsumer:
    """`from_first=True` 给「消息先发、消费者后起」的用例：新组在 LAST_OFFSET 下会从
    分配时刻的最新位点开始，先发的那几条会被直接跳过。"""
    c = DefaultMQPushConsumer(group)
    c.set_namesrv_addr(NAMESRV)
    c.set_message_listener(listener)
    c.set_consume_thread_nums(1)
    c.consume_message_batch_max_size = batch_max
    if model is not None:
        c.set_message_model(model)
    if from_first:
        c.set_consume_from_where(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET)
    c.subscribe(topic, "*")
    c.start()
    return c


def main() -> int:
    producer = DefaultMQProducer(PREFIX + "_pg")
    producer.set_namesrv_addr(NAMESRV)
    producer.start()
    time.sleep(1)

    bodies = [b"ack-%d" % i for i in range(3)]

    # ---------- A1 对照组：默认整批认可 ----------
    t1 = PREFIX + "_Full"
    g1 = PREFIX + "_g1"
    prepare_topic(t1)
    l1 = RecordingListener()
    c1 = start(g1, t1, l1)
    time.sleep(3)
    for b in bodies:
        producer.send(Message(t1, b))
    print("A1: 对照组已发送，等待消费...")
    wait_until(lambda: len(l1.arrivals()) >= 3, 30)
    time.sleep(3)
    arrivals1 = l1.arrivals()
    check("A1-3 条各投递一次", sorted(r[0] for r in arrivals1) == sorted(bodies),
          "arrivals=%d" % len(arrivals1))
    check("A1-没有任何重投", all(r[2] == 0 for r in arrivals1),
          "times=%s" % sorted(r[2] for r in arrivals1))
    check("A1-位点提交到 3（等价于整批 ack）",
          wait_until(lambda: committed_offset(g1, t1) == 3, 20),
          "offset=%d" % committed_offset(g1, t1))
    c1.shutdown()

    # ---------- A2 部分 ack：只认可首批的第一条 ----------
    t2 = PREFIX + "_Partial"
    g2 = PREFIX + "_g2"
    prepare_topic(t2)
    l2 = RecordingListener(ack_index=0, ack_first_batch_only=True)
    # 先把 3 条放上去再起消费者：批次怎么切由拉取时机决定，先发就必然是「一整批 3 条」，
    # 否则首批可能是 1 条或 2 条，ackIndex=0 扣下的尾巴数量就不确定了。
    for b in bodies:
        producer.send(Message(t2, b))
    c2 = start(g2, t2, l2, from_first=True)
    print("A2: 已发送，等待部分 ack 后的回投（延迟梯度 level3≈10s）...")
    # 首批 3 条 → 尾巴 2 条回投 %RETRY% → broker  revive 后重新投递；
    # %RETRY% topic 首次回投才建出来，消费者还要等一轮 rebalance，窗口给足。
    wait_until(lambda: len(l2.arrivals()) >= 5, 90)
    time.sleep(3)
    arrivals2 = l2.arrivals()
    first_batch = [r[0] for r in l2.batches[0]] if l2.batches else []
    acked = first_batch[:1]
    tail = first_batch[1:]
    check("A2-首批确实拿到 3 条", len(first_batch) == 3, "batch=%s" % first_batch)
    check("A2-被认可的那条整个窗口只投一次",
          bool(acked) and [r[0] for r in arrivals2].count(acked[0]) == 1,
          "acked=%s" % acked)
    redelivered = [r for r in arrivals2 if r[0] in tail and r[2] >= 1]
    check("A2-未认可的两条经 %RETRY% 二次到达",
          len(tail) == 2 and {r[0] for r in redelivered} == set(tail),
          "tail=%s redelivered=%d" % (tail, len(redelivered)))
    check("A2-重投消息 reconsumeTimes>=1 且 topic 还原成业务 topic",
          bool(redelivered) and all(r[1] == t2 for r in redelivered),
          "topics=%s" % sorted({r[1] for r in redelivered}))
    check("A2-3 条最终全部被消费（没丢）",
          {r[0] for r in arrivals2} == set(bodies),
          "distinct=%d" % len({r[0] for r in arrivals2}))
    check("A2-业务队列位点仍提交到 3（尾巴交给 broker 重投，不必钉住）",
          wait_until(lambda: committed_offset(g2, t2) == 3, 20),
          "offset=%d" % committed_offset(g2, t2))
    c2.shutdown()

    # ---------- A3 RECONSUME_LATER 压过 ackIndex ----------
    t3 = PREFIX + "_LaterWins"
    g3 = PREFIX + "_g3"
    prepare_topic(t3)
    l3 = RecordingListener(ack_index=2, status=ConsumeConcurrentlyStatus.RECONSUME_LATER)
    for b in bodies:
        producer.send(Message(t3, b))
    c3 = start(g3, t3, l3, from_first=True)
    print("A3: 已发送，等待整批重投...")
    # 每条都要重投一次以上；每次全批 RECONSUME_LATER 会一直重投，
    # 所以只断言「3 条都出现过 reconsumeTimes>=1」，不比总条数。
    check("A3-ackIndex=2 也拦不住 RECONSUME_LATER（3 条全部重投）",
          wait_until(lambda: {r[0] for r in l3.arrivals() if r[2] >= 1} == set(bodies), 60),
          "redelivered=%d/%d" % (len({r[0] for r in l3.arrivals() if r[2] >= 1}), len(bodies)))
    c3.shutdown()

    # ---------- A4 广播模式：尾巴不回投 ----------
    t4 = PREFIX + "_Broadcast"
    g4 = PREFIX + "_g4"
    prepare_topic(t4)
    l4 = RecordingListener(ack_index=0, ack_first_batch_only=True)
    for b in bodies:
        producer.send(Message(t4, b))
    c4 = start(g4, t4, l4, model=MessageModel.BROADCASTING, from_first=True)
    print("A4: 广播模式已发送，等待消费...")
    wait_until(lambda: len(l4.arrivals()) >= 3, 30)
    time.sleep(12)  # 若有回投，level3≈10s 后会到；这里给足窗口确认「不会到」
    arrivals4 = l4.arrivals()
    check("A4-广播：未认可的尾巴不回投（每条只到一次）",
          sorted(r[0] for r in arrivals4) == sorted(bodies)
          and all(r[2] == 0 for r in arrivals4),
          "arrivals=%d times=%s" % (len(arrivals4), sorted({r[2] for r in arrivals4})))
    c4.shutdown()

    producer.shutdown()
    print("\nRESULT: %d PASS / %d FAIL" % (PASS, FAIL))
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(main())
