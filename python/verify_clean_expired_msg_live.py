#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""cleanExpiredMsg 挂起逃生口真机验证（Java ConsumeMessageConcurrentlyService:68-88/192-200
＋ ProcessQueue.cleanExpiredMsg:75-127）。

用法（需本地 RocketMQ 5.5.1 集群）：
    .venv/bin/python verify_clean_expired_msg_live.py 127.0.0.1:9876

要验的东西：listener **挂着不返回**超过 ``consumeTimeout`` 分钟时，清扫线程必须把这条消息
发回 broker（delayLevel 3）、由 %RETRY% 重新投递 —— 整条链路上客户端不能丢它、也不能投
两次。这条路径坏掉的样子是静默的：卡住的消息把该队列位点与分发循环一起钉死，没有任何异常
或超时可见；光靠"客户端不报错"什么都证明不了，必须真机跑出「挂起 → 清扫 → 重投到达」。

场景（``consume_timeout=1``，清扫周期与阈值都是 1 分钟；Java 的过期判据是**严格大于**，
所以清扫会在第二个 tick 命中，约 start+120s）：
  A1 基线：1 队列 topic 投 1 条；listener 第一次拿到就挂住（不返回），客户端登记里能看到
     这条消息在途、reconsumeTimes=0。
  A2 清扫命中（**核心判据**）：listener 仍然挂着的同时，轮询在途登记 —— 一旦这条消息从
     登记里消失（且 listener 还没放行），说明清扫线程把它回投并摘除了。距今必须 ≥60s，
     否则说明是别的路径动的手。少了清扫，这里会一直等到超时。
  A3 重投真的到了 broker 侧：清扫后 ≤40s 内，%RETRY% 队列的本地缓冲里必须出现这条消息
     （拉取循环是每队列独立的，不受挂住的消费线程影响）——证明是真回投，不是本地假装。
  A4 放行后被重新消费：恢复到第二次投递，reconsumeTimes=1（broker 侧的重投计数），
     同一条 body 从头到尾**只到两次**（清扫一次回投，没有重复投递）。
  A5 位点收尾：挂住的 listener 返回 CONSUME_SUCCESS 后，业务队列已提交位点走到 1
     （Java removeMessage 的列表仍含这条已被清扫的消息）。

已知副作用：``consume_timeout=1`` 会让 RT 判据（Java :387）把这次 120s+ 的消费记为
TIME_OUT —— 与本用例无关，Java 同样如此。

⚠ 本用例要跑约 4 分钟（等两个清扫周期）；负向对照：把 ``_start_clean_expire_loop()``
的调用注释掉重跑 → 真机实测 ``PASS=6 FAIL=6``（A2 的第一条、A3、A4 四条全红；
仍绿的是不依赖清扫的 A0/A1×3/A5 与 A2 的「收走时刻 >60s」——它只测挂起时长，
清单详见 README 表格）。
"""
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
    c.consume_timeout = 1          # 清扫周期与阈值都变成 1 分钟（Java 同字段同语义）
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
