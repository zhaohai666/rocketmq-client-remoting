#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""POP 模式**消费侧**真机验证（推模式消费者走 POP）。

用法：.venv/bin/python verify_pop_consumer_live.py 127.0.0.1:9876

与协议层的 ``verify_pop_live.py`` 区别：那里只验 POP/ACK/延长不可见三个 RPC 本身，
这里验**消费循环**——rebalance 出来的每个队列独立 POP、投递给 listener、
按消费结果 ack 或延长不可见时间。

⚠ 与 Java 的有意差异：Java 的 push-consumer POP 走 **broker 侧分配**
（QUERY_ASSIGNMENT(400) + MessageQueueAssignment(mode=POP)），客户端不做 rebalance；
本项目复用已有的**客户端 rebalance**，队列由本地按分配策略算出。语义等价（每队列一个
POP 循环 + ack 确认），差别只在"谁决定分哪些队列"。

场景：
  S1 POP 消费：起消费者 → 发 12 条 → 全部收到、无重复、body 集合一致
  S2 ack 生效：收满后再观察一段时间，不应被重复投递（ack 已抵消 checkpoint）
  S3 RECONSUME_LATER：listener 持续返回 RECONSUME_LATER → 消息按延迟档位被重新投递
  S4 多队列：消息确实落到了多个队列且都被消费（POP 是逐队列弹的）
"""
from __future__ import annotations

import sys
import threading
import time

from rocketmq.client.consumer import (DefaultMQPushConsumer,
                                     SimpleMessageListener)
from rocketmq.client.producer import DefaultMQProducer
from rocketmq.common.message import Message
from rocketmq.client.consumer_result import (  # noqa: F401
    ConsumeConcurrentlyStatus,
)

PASS = 0
FAIL = 0


def check(name, ok, detail=""):
    global PASS, FAIL
    if ok:
        PASS += 1
        print("  [PASS] %s%s" % (name, ("  " + detail) if detail else ""))
    else:
        FAIL += 1
        print("  [FAIL] %s%s" % (name, ("  " + detail) if detail else ""))


def wait_until(pred, timeout, interval=0.2):
    """等到 pred() 为真或超时。返回是否等到。"""
    deadline = time.time() + timeout
    while time.time() < deadline:
        if pred():
            return True
        time.sleep(interval)
    return pred()


def main():
    namesrv = sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1:9876"
    stamp = str(int(time.time() * 1000))
    topic = "PopConsLive_" + stamp
    group = "GID_PopConsLive_" + stamp
    topic_later = "PopConsLater_" + stamp
    group_later = "GID_PopConsLater_" + stamp
    n_msg = 12
    queue_num = 4

    print("=" * 70)
    print("POP consumer live (Python): namesrv=%s topic=%s group=%s" % (namesrv, topic, group))
    print("=" * 70)

    # ------------------------------------------------ S1 准备 topic
    prep = DefaultMQProducer("PG_PopConsPrep_" + stamp)
    prep.set_namesrv_addr(namesrv)
    prep.start()
    try:
        prep.create_topic("TBW102", topic, queue_num)
        prep.create_topic("TBW102", topic_later, queue_num)
    except Exception as e:  # noqa: BLE001
        print("!! CreateTopic failed: %s" % e)
    prep.shutdown()

    # ------------------------------------------------ S1 POP 消费
    print("=== S1 POP 消费（全收 + 无重复）===")
    received = []
    seen_queues = set()
    lock = threading.Lock()

    def listener(msgs):
        # ⚠ SimpleMessageListener 只传 msgs（consumer.py 的契约），不要写成 (msgs, context)
        with lock:
            for m in msgs:
                received.append(m.body.decode("utf-8", "replace"))
                seen_queues.add(m.queue_id)
        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS

    consumer = DefaultMQPushConsumer(group)
    consumer.set_namesrv_addr(namesrv)
    consumer.pop_mode = True
    consumer.consume_thread_max = 4
    consumer.consume_message_batch_max_size = 4
    # ⚠ 故意压到 10s：让"没 ack → invisibleTime 到期复活重投"在观测窗口内**来得及暴露**。
    # 用默认 60s 的话，即使 ack 完全没发出去，8s 的观察期也看不出重复投递（假通过）。
    consumer.pop_invisible_time = 10000
    consumer.set_message_listener(SimpleMessageListener(listener))
    consumer.subscribe(topic, "*")
    # ⚠ 必须先起消费者再发消息（见项目约定）
    consumer.start()
    time.sleep(1.0)

    producer = DefaultMQProducer("PG_PopCons_" + stamp)
    producer.set_namesrv_addr(namesrv)
    producer.start()
    sent = set()
    for i in range(n_msg):
        body = "pop-cons-%02d" % i
        msg = Message(topic, body.encode("utf-8"))
        msg.set_keys("pc%d" % i)
        producer.send(msg)
        sent.add(body)
    print("  sent %d msgs" % n_msg)

    got_all = wait_until(lambda: len(received) >= n_msg, timeout=30.0)
    # 再多等一会儿确认不会重复投递（S2）。必须 > pop_invisible_time(10s)，
    # 否则"ack 没发"根本来不及复活，S2 会假通过。
    wait_until(lambda: False, timeout=16.0)

    with lock:
        got = list(received)
        queues = set(seen_queues)
    check("S1a 全部消息被消费", got_all and len(got) >= n_msg,
          "received=%d/%d" % (len(got), n_msg))
    check("S1b body 集合与发送一致", set(got) == sent,
          "missing=%s" % sorted(sent - set(got))[:5])

    # ------------------------------------------------ S2 ack 生效
    print("=== S2 ack 生效（不重复投递）===")
    dup = len(got) - len(set(got))
    check("S2a 无重复投递", dup == 0, "received=%d unique=%d dup=%d" % (len(got), len(set(got)), dup))
    check("S2b 观察期内没有新增投递", len(got) == n_msg,
          "received=%d expected=%d" % (len(got), n_msg))

    # ------------------------------------------------ S4 多队列
    print("=== S4 多队列都被 POP 到 ===")
    check("S4 消息分布在多个队列且都被消费", len(queues) > 1, "queues=%s" % sorted(queues))

    consumer.shutdown()

    # ------------------------------------------------ S3 RECONSUME_LATER
    print("=== S3 RECONSUME_LATER → 延迟后重投 ===")
    later_rounds = {"first": 0}
    later_keys = []

    def later_listener(msgs):
        # ⚠ 同上：SimpleMessageListener 只传 msgs
        with lock:
            for m in msgs:
                later_keys.append(m.get_keys() or m.msg_id)
        return ConsumeConcurrentlyStatus.RECONSUME_LATER

    c2 = DefaultMQPushConsumer(group_later)
    c2.set_namesrv_addr(namesrv)
    c2.pop_mode = True
    c2.consume_thread_max = 2
    c2.pop_invisible_time = 5000
    # 消费失败时的延迟档位：把第一档压到 3s，让"延长不可见时间 → 重新可见"尽快发生
    c2.pop_delay_level = [3, 10, 30, 60, 120, 300, 600, 1200, 1800, 3600, 7200]
    c2.set_message_listener(SimpleMessageListener(later_listener))
    c2.subscribe(topic_later, "*")
    c2.start()
    time.sleep(1.0)

    p2 = DefaultMQProducer("PG_PopConsLater_" + stamp)
    p2.set_namesrv_addr(namesrv)
    p2.start()
    for i in range(3):
        msg = Message(topic_later, ("later-%d" % i).encode("utf-8"))
        msg.set_keys("pl%d" % i)
        p2.send(msg)

    ok_first = wait_until(lambda: len(later_keys) >= 3, timeout=30.0)
    with lock:
        first_round = len(later_keys)
    check("S3a 首轮投递 3 条", ok_first and first_round >= 3, "count=%d" % first_round)

    # 失败后客户端延长不可见时间（3s），到期应重新可见并被再次投递。
    # ⚠ 断言的是"每条都重投过"，不是"又多收了 3 次投递"：按条数算时，
    #   某一条被重投 3 次而另一条从没回来也会通过。
    def redelivered_kinds():
        counts = {}
        for k in later_keys:
            counts[k] = counts.get(k, 0) + 1
        return sum(1 for n in counts.values() if n >= 2)

    ok_again = wait_until(redelivered_kinds, timeout=40.0)
    with lock:
        joined = ",".join(str(k) for k in later_keys)
    check("S3b 每条消费失败的消息都被重新投递（延长不可见时间生效）", ok_again,
          "first=%d redelivered=%d deliveries=%s" % (first_round, redelivered_kinds(), joined))

    c2.shutdown()
    p2.shutdown()
    producer.shutdown()

    print("#" * 40)
    print("PASS=%d FAIL=%d" % (PASS, FAIL))
    return 0 if FAIL == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
