#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import annotations

import sys
import threading
import time

sys.path.insert(0, ".")

from client.consumer import DefaultMQPushConsumer, MessageListenerConcurrently
from client.consumer_result import ConsumeConcurrentlyStatus
from client.mq_client import MQClientInstance
from client.producer import DefaultMQProducer
from common.message import Message, MessageQueue

NAMESRV = sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1:9876"
PREFIX = "CleanExpPy_%d" % int(time.time() * 1000)

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


def committed_offset(group: str, topic: str) -> int:
    c = new_client("reader")
    try:
        publish = c.get_topic_publish_info(topic)
        if publish is None or not publish.msg_queue_list:
            return -1
        total = 0
        for mq in publish.msg_queue_list:
            total += c.query_consumer_offset(group, mq) or 0
        return total
    except Exception:
        return -1
    finally:
        c.shutdown()


def wait_until(pred, timeout: float, interval: float = 1.0) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if pred():
            return True
        time.sleep(interval)
    return pred()


class HungListener(MessageListenerConcurrently):
    """第一次投递就挂住不返回；放行后恢复正常返回 CONSUME_SUCCESS。"""

    def __init__(self):
        self.lock = threading.Lock()
        self.calls = 0
        self.arrivals = []          # [(body, reconsume_times, moment)]
        self.first_seen = threading.Event()
        self.release = threading.Event()

    def consume_message(self, msgs, context):
        with self.lock:
            self.calls += 1
            first = self.calls == 1
            for m in msgs:
                self.arrivals.append((bytes(m.body), m.get_reconsume_times(), time.time()))
        if first:
            self.first_seen.set()
            self.release.wait(300)   # 挂起窗口：等清扫动手 + 验证脚本放行
        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS

    def times(self, body: bytes):
        with self.lock:
            return [(t, when) for b, t, when in self.arrivals if b == body]


def business_queue(c: DefaultMQPushConsumer, topic: str):
    """分配结果里挑出业务队列（排除自动订阅的 %RETRY%<group>）。"""
    return next((q for q in c._assigned_queues() if q.topic == topic), None)


def retry_buffered(c: DefaultMQPushConsumer, body: bytes) -> bool:
    """%RETRY% 队列的本地缓冲里有没有这条 body（拉取循环独立于挂住的消费线程）。"""
    for key, dq in list(c._pending.items()):
        if not key.startswith("%RETRY%"):
            continue
        if any(bytes(m.body) == body for m in dq):
            return True
    return False


def main() -> int:
    topic = PREFIX + "_T"
    group = PREFIX + "_g"
    setup = new_client("setup")
    try:
        setup.create_topic_in_route(topic, 1, 1)
    finally:
        setup.shutdown()

    producer = DefaultMQProducer(PREFIX + "_pg")
    producer.set_namesrv_addr(NAMESRV)
    producer.start()
    time.sleep(1)

    listener = HungListener()
    c = DefaultMQPushConsumer(group)
    c.set_namesrv_addr(NAMESRV)
    c.set_message_listener(listener)
    c.consume_timeout = 1
    c.subscribe(topic, "*")
    body = b"ce-1"
    c.start()

    try:
        mq = business_queue(c, topic) if wait_until(
            lambda: business_queue(c, topic) is not None, 30) else None
        check("A0-业务队列已分配（%RETRY% 队列不算）", mq is not None, "mq=%s" % mq)
        if mq is None:
            return 1
        key = c._mq_key(mq)

        # ---------- A1 基线：第一投挂住 ----------
        producer.send(Message(topic, body))
        check("A1-首投在 30s 内到达并挂住", listener.first_seen.wait(30),
              "calls=%d" % listener.calls)
        first = listener.times(body)
        check("A1-首投 reconsumeTimes=0", first and first[0][0] == 0,
              "times=%s" % first)
        first_at = first[0][1] if first else time.time()
        check("A1-挂住期间消息登记在途（在 listener 手里）",
              any(bytes(m.body) == body for m in c._inflight_msgs.get(key) or []),
              "inflight=%s" % [bytes(m.body) for m in c._inflight_msgs.get(key) or []])

        # ---------- A2 清扫命中：listener 还挂着，登记里已经没了 ----------
        t0 = time.time()
        swept = wait_until(
            lambda: not [m for m in c._inflight_msgs.get(key) or []], 210, 1.0)
        elapsed = time.time() - t0
        check("A2-清扫在 listener 仍挂起时收走了这条消息（约 start+120s）",
              swept and not listener.release.is_set(),
              "elapsed=%.1fs calls=%d" % (elapsed, listener.calls))
        check("A2-收走时间晚于一个清扫阈值（>60s，排除别的路径动手）", elapsed > 60,
              "elapsed=%.1fs" % elapsed)

        # ---------- A3 回投真的到了 broker：%RETRY% 缓冲里出现 ----------
        check("A3-回投消息出现在 %RETRY% 队列的本地缓冲（broker 真收到了 sendMessageBack）",
              wait_until(lambda: retry_buffered(c, body), 40),
              "retry_keys=%s" % [k for k in c._pending if k.startswith("%RETRY%")])

        # ---------- A4 放行：重投被重新消费 ----------
        listener.release.set()
        check("A4-放行后重新消费到（reconsumeTimes=1）",
              wait_until(lambda: len(listener.times(body)) >= 2, 60),
              "times=%s" % [t for t, _ in listener.times(body)])
        times = listener.times(body)
        check("A4-第二次投递 reconsumeTimes=1（broker 侧重投计数）",
              len(times) >= 2 and times[1][0] == 1, "times=%s" % [t for t, _ in times])
        check("A4-同一条 body 全程只到两次（清算一次回投，无重复投递）",
              len(times) == 2, "times=%s" % [t for t, _ in times])
        check("A4-第二次投递与首投相隔 >60s（不是 listener 自己造成的重投）",
              len(times) >= 2 and times[1][1] - first_at > 60,
              "gap=%.1fs" % ((times[1][1] - first_at) if len(times) >= 2 else -1))

        # ---------- A5 位点收尾 ----------
        check("A5-挂住的 listener 返回后业务队列位点走到 1（Java removeMessage 含已清扫条目）",
              wait_until(lambda: committed_offset(group, topic) == 1, 30),
              "offset=%d" % committed_offset(group, topic))
    finally:
        listener.release.set()
        try:
            c.shutdown()
        except Exception:  # noqa: BLE001
            pass
        try:
            producer.shutdown()
        except Exception:  # noqa: BLE001
            pass

    print("\nCleanExpiredMsg: PASS=%d FAIL=%d" % (PASS, FAIL))
    return 0 if FAIL == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
