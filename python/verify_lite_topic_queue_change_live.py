#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""topic 队列集合变更监听（register_topic_message_queue_change_listener）真机验证。

用法（需本地 RocketMQ 5.5.1 集群）：
    .venv/bin/python verify_lite_topic_queue_change_live.py 127.0.0.1:9876

为什么单独测：队列变更监听靠的是「每一趟都现问 nameserver 拿订阅队列集」，
而普通路由缓存是 30s 才刷一次。只跑假 nameserver 的单测证不了这件事——
本脚本把检查周期压到 1s、路由轮询保持默认 30s，再把 topic 真的扩容，量两件事：
  L1 静默：队列没动时监听器不得被反复打扰；
  L2 现查：等后台过了首查延迟、进入 1s 一趟的稳定期后再扩容，要求「nameserver 已经
      报出新队列数」到「监听器收到回调」只隔一两趟检查（≤5s）。如果比对趟次读的是
      30s 路由缓存，这个窗口会拖到半分钟以上；
  L3 收敛：回调之后快照推进，同一套队列不再重复回调；
  L4 缩容：把队列数改回去同样只报一次，且报的是当下真实的那一套；
  L5 未注册过的 topic：取不到队列一律算「查不到」，不会伪装成缩到 0 队列。
"""
from __future__ import annotations

import sys
import threading
import time

sys.path.insert(0, ".")

from client.consumer import DefaultLitePullConsumer, DefaultMQPullConsumer
from client.exception import MQClientException
from client.producer import DefaultMQProducer

NAMESRV = sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1:9876"
STAMP = int(time.time() * 1000)
TOPIC = "LiteQcLive_%d" % STAMP
GROUP = "LiteQcG_%d" % STAMP
GHOST = "LiteQcGhost_%d" % STAMP
QUEUE_NUM = 2
SCALED_NUM = 4
CHECK_INTERVAL_MS = 1000          # 压到下限：让「现查路由」和「吃缓存」在时间上分得开
FIRST_CHECK_DELAY_S = 10.0        # 后台比对的首查延迟（与实现里的默认值一致）
CACHE_DISCRIMINATOR_S = 5.0       # nameserver 见到变化后，最多几秒内必须回调（稳定期是 1s 一趟）
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


class Recorder:
    """只记 (topic, queueId 列表)，不改状态；后台线程也会调它，所以自带锁。"""

    def __init__(self):
        self.events = []
        self._lock = threading.Lock()

    def on_changed(self, topic, message_queues):
        with self._lock:
            self.events.append((topic, sorted(mq.queue_id for mq in message_queues)))

    @property
    def events_snapshot(self):
        with self._lock:
            return list(self.events)


def scale_read_queues(group_tag: str, queue_num: int) -> None:
    """对同一 topic 再下发一次建 topic 请求，把读写队列数改成 queue_num。"""
    prod = DefaultMQProducer("PG_Qc_%s_%d" % (group_tag, STAMP))
    prod.set_namesrv_addr(NAMESRV)
    prod.start()
    try:
        prod.create_topic("TBW102", TOPIC, queue_num)
    finally:
        prod.shutdown()


def routable_queue_num(timeout: int = 30) -> int:
    """轮询到 topic 在 nameserver 上真的有队列为止，返回当时的队列数。"""
    deadline = time.time() + timeout
    while time.time() < deadline:
        probe = DefaultMQPullConsumer("PG_QcProbe_%d" % STAMP)
        probe.set_namesrv_addr(NAMESRV)
        probe.start()
        try:
            qs = probe.fetch_subscribe_message_queues(TOPIC)
            if qs:
                return len(qs)
        except BaseException:
            pass
        finally:
            probe.shutdown()
        time.sleep(1)
    raise RuntimeError("topic %s not routable after %ds" % (TOPIC, timeout))


def wait_queue_num(consumer: DefaultLitePullConsumer, expect: int,
                   timeout: float = 45.0) -> tuple:
    """等 nameserver 报出 expect 个队列（fetch_message_queues 每趟都现查）。"""
    start = time.monotonic()
    deadline = start + timeout
    while time.monotonic() < deadline:
        try:
            if len(consumer.fetch_message_queues(TOPIC)) == expect:
                return True, time.monotonic() - start
        except MQClientException:
            pass
        time.sleep(0.25)
    return False, time.monotonic() - start


def wait_callback(rec: Recorder, expect: list, timeout: float = 45.0) -> tuple:
    start = time.monotonic()
    deadline = start + timeout
    while time.monotonic() < deadline:
        if rec.events_snapshot == expect:
            return True, time.monotonic() - start
        time.sleep(0.25)
    return False, time.monotonic() - start


def main() -> int:
    print("nameserver = %s, topic = %s" % (NAMESRV, TOPIC))

    # ---- 建 topic（2 队列）并等路由
    prod = DefaultMQProducer("PG_QcCreate_%d" % STAMP)
    prod.set_namesrv_addr(NAMESRV)
    prod.start()
    try:
        prod.create_topic("TBW102", TOPIC, QUEUE_NUM)
    finally:
        prod.shutdown()
    seen = routable_queue_num()
    check("L0 topic 建成且路由可见", seen == QUEUE_NUM, "queueNum=%d" % seen)

    c1 = DefaultLitePullConsumer(GROUP)
    c1.set_namesrv_addr(NAMESRV)
    c1.instance_name = "qc-live"
    c1.set_topic_metadata_check_interval_millis(CHECK_INTERVAL_MS)
    check("L0 检查周期落到 1s 下限",
          c1.get_topic_metadata_check_interval_millis() == CHECK_INTERVAL_MS)
    c1.subscribe(TOPIC, "*")
    c1.start()
    loop_start = time.monotonic()

    try:
        # 运行中注册 ⇒ 立刻记快照 ⇒ 首轮不该回调
        rec = Recorder()
        c1.register_topic_message_queue_change_listener(TOPIC, rec)
        time.sleep(3.0)
        check("L1 队列没动 ⇒ 静默（快照吃掉首轮）",
              rec.events_snapshot == [], "events=%s" % rec.events_snapshot)

        # 首查有固定 10s 延迟：等到它过去，后台才进入 1s 一趟的稳定期。
        # 这一趟跑过之后仍然静默，同时证明「运行中注册 = 有快照」在真机上成立。
        wait = FIRST_CHECK_DELAY_S + 2.0 - (time.monotonic() - loop_start)
        if wait > 0:
            time.sleep(wait)
        check("L1b 首查那趟真的跑过 ⇒ 依旧静默（快照没被当成变化）",
              rec.events_snapshot == [], "events=%s" % rec.events_snapshot)

        # ---- L2 扩容 2 → 4：先等 nameserver 认，再要求监听器只隔几趟检查
        scale_read_queues("up", SCALED_NUM)
        seen_ns, t_ns = wait_queue_num(c1, SCALED_NUM)
        check("L2a nameserver 报出 4 个队列", seen_ns, "after=%.1fs" % t_ns)
        if seen_ns:
            got_cb, t_cb = wait_callback(rec, [(TOPIC, list(range(SCALED_NUM)))])
            check("L2b 比对趟次现查路由（回调紧跟 nameserver，不等 30s 缓存）",
                  got_cb and t_cb <= CACHE_DISCRIMINATOR_S,
                  "callback=%.1fs after nameserver" % t_cb)

        # ---- L3 收敛：同一套队列不再重复打扰
        time.sleep(3.0)
        check("L3 回调后快照推进 ⇒ 不重复回调",
              len(rec.events_snapshot) == 1, "events=%s" % rec.events_snapshot)

        # ---- L4 缩容 4 → 2
        scale_read_queues("down", QUEUE_NUM)
        seen_ns, t_ns = wait_queue_num(c1, QUEUE_NUM)
        check("L4a nameserver 报回 2 个队列", seen_ns, "after=%.1fs" % t_ns)
        if seen_ns:
            got_cb, t_cb = wait_callback(
                rec, [(TOPIC, list(range(SCALED_NUM))), (TOPIC, list(range(QUEUE_NUM)))])
            check("L4b 缩容同样靠现查路由看到",
                  got_cb and t_cb <= CACHE_DISCRIMINATOR_S,
                  "callback=%.1fs after nameserver" % t_cb)

        # ---- L5 没建过的 topic：空队列集算「查不到」
        raised = ""
        try:
            c1.fetch_message_queues(GHOST)
        except MQClientException as e:
            raised = str(e)
        check("L5 未知 topic 取队列抛「查不到」而不是返回空",
              "Namesrv return empty" in raised, raised)
    finally:
        c1.shutdown()

    print("\nLitePull queue-change live: PASS=%d FAIL=%d" % (PASS, FAIL))
    return 0 if FAIL == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
