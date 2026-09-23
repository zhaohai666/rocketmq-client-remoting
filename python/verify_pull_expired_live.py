#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""拉取循环停摆**自愈**（Java isPullExpired / PULL_MAX_IDLE_TIME）真机验证。

用法（需本地 RocketMQ 5.5.1 集群）：
    .venv/bin/python verify_pull_expired_live.py 127.0.0.1:9876

要验的东西：Java ``RebalanceImpl.updateProcessQueueTableInRebalance:438-461`` 在每一趟
rebalance 里，对**仍归本实例**的队列问一句 ``pq.isPullExpired()``（阈值
``ProcessQueue.PULL_MAX_IDLE_TIME``=120s，读 ``rocketmq.client.pull.pullMaxIdleTime``）；
过期就按撤走收尾（setDropped → removeUnnecessaryMessageQueue：持久化位点 + 丢 ProcessQueue
+ 顺序模式 UNLOCK），紧接着的 add 分支换一具新的 ProcessQueue 重建拉取。

这条路径坏掉的样子是**静默的**：某条队列的循环一旦死了又没有自愈，那个队列从此不再消费，
但客户端不报任何错、心跳照发、其他队列照常推进——真机上只能从"某个组的位点卡住不动"反推。
所以必须用真机验证「注入停摆 → 恢复消费」这个闭环，光看客户端不报错说明不了任何事。

场景（故障用注入模拟，Python 没有"杀掉一条线程"的合法手段）：
  A1 基线：1 队列 topic 投 3 条，位点 3；运行信息里 lastPullTimestamp 是**真时刻**且在前进。
  A2 线程死掉：把该队列的线程表条目换成一条已退出的线程（等价于循环被异常打穿）→
     下一趟 rebalance 必须换掉它，之后新发的 3 条**照样被消费**（位点走到 6）。
  A3 盖章过期：线程还活着，但把 lastPull 时刻倒拨 121s（> 120s 阈值）→ 同样被撤并重建，
     再发的 3 条照样被消费（位点走到 9）。
  A4 自愈不回退位点：A2/A3 撤走前必须持久化已消费位点，重建后从 broker 位点续拉 →
     前 6 条整个窗口各只到一次（没有从头重投）。
"""
from __future__ import annotations

import sys
import threading
import time

sys.path.insert(0, ".")

from rocketmq.client.consumer import (DefaultMQPushConsumer, PULL_MAX_IDLE_TIME,
                                      MessageListenerConcurrently)
from rocketmq.client.consumer_result import ConsumeConcurrentlyStatus
from rocketmq.client.mq_client import MQClientInstance
from rocketmq.client.producer import DefaultMQProducer
from rocketmq.common.message import Message, MessageQueue
from rocketmq.remoting.protocol.body import ConsumerRunningInfo

NAMESRV = sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1:9876"
PREFIX = "PullExpPy_%d" % int(time.time() * 1000)

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
    """broker 上该组在这个 topic 各队列的已提交位点之和；单次查询超时按"还没读到"处理。"""
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


class CountingListener(MessageListenerConcurrently):
    def __init__(self):
        self.lock = threading.Lock()
        self.arrivals = []          # [(body, reconsumeTimes)]

    def consume_message(self, msgs, context):
        with self.lock:
            for m in msgs:
                self.arrivals.append((bytes(m.body), m.get_reconsume_times()))
        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS

    def bodies(self):
        with self.lock:
            return [b for b, _ in self.arrivals]

    def times(self, body: bytes):
        with self.lock:
            return [t for b, t in self.arrivals if b == body]


def wait_arrivals(l: CountingListener, n: int, timeout: float = 30) -> bool:
    return wait_until(lambda: len(l.bodies()) >= n, timeout, 0.5)


def pull_thread_of(c: DefaultMQPushConsumer, key: str):
    with c._lock:
        return c._queue_threads.get(key)


def dead_thread():
    """一条跑完就退出的线程：状态与"拉取循环被异常打穿"后的条目完全一致。"""
    t = threading.Thread(target=lambda: None, daemon=True)
    t.start()
    t.join()
    return t


_PARKS = []


def parked_thread():
    """一条活着但**永远不再盖章**的线程：状态与"循环卡死在别处"完全一致。

    为什么注入而不真等 120s：真跑的话原循环每轮都会自己刷新时刻，判据到底走了哪一支
    就不确定了；卡住的循环正是"线程还在、时刻不动"这个形状。
    """
    ev = threading.Event()
    t = threading.Thread(target=ev.wait, daemon=True)
    t.start()
    _PARKS.append(ev)            # 保持引用，别被 GC
    return t


def inject_and_rebalance(c: DefaultMQPushConsumer, key: str, mode: str):
    """注入故障并走**生产路径**触发 rebalance（_rebalance_now 就是 40 通知用的那颗事件）。

    两种模式分别打中 Java isPullExpired 的两个判据：``dead`` = 线程已退出，
    ``stale`` = 线程活着但 lastPull 时刻超出 PULL_MAX_IDLE_TIME。
    返回 (原线程, 注入的假线程)——断言"换新"时必须两者都不等于当前条目，
    否则刚注入还没被撤走也会被判成通过（第一版就栽在这里）。
    """
    old = pull_thread_of(c, key)
    assert old is not None, "队列没分到，注入无意义"
    fake = dead_thread() if mode == "dead" else parked_thread()
    with c._lock:
        c._queue_threads[key] = fake
        if mode == "stale":
            c._last_pull_table[key] = time.time() - (PULL_MAX_IDLE_TIME + 1.0)
    c._rebalance_now.set()
    return old, fake


def wait_rebuilt(c: DefaultMQPushConsumer, key: str, old, fake,
                 timeout: float = 30) -> bool:
    """等这一路被换成**另一条活着的**循环线程（既不是原来的，也不是注入的那条）。"""
    def ok():
        t = pull_thread_of(c, key)
        return t is not None and t is not old and t is not fake and t.is_alive()

    return wait_until(ok, timeout, 0.5)


def pqi_of(info: ConsumerRunningInfo, mq: MessageQueue) -> dict:
    """取**指定队列**的运行信息，而不是"表里第一条"。

    自营队列和 %RETRY%<group> 会在同一张 mq_table 里（Java processQueueTable 就是
    按分配队列逐条填的），values()[0] 会随机命中另一条，判据就失效了。
    """
    for k, v in info.mq_table.items():
        if k.topic == mq.topic and k.queue_id == mq.queue_id:
            return v
    return {}


def business_queue(c: DefaultMQPushConsumer, topic: str):
    """分配结果里挑出业务队列（排除自动订阅的 %RETRY%<group>）。"""
    return next((q for q in c._assigned_queues() if q.topic == topic), None)


def main() -> int:
    topic = PREFIX + "_T"
    group = PREFIX + "_g"
    setup = new_client("setup")
    try:
        # 1 队列：只有一路循环，注入点唯一，位点判据也唯一
        setup.create_topic_in_route(topic, 1, 1)
    finally:
        setup.shutdown()

    producer = DefaultMQProducer(PREFIX + "_pg")
    producer.set_namesrv_addr(NAMESRV)
    producer.start()
    time.sleep(1)

    listener = CountingListener()
    c = DefaultMQPushConsumer(group)
    c.set_namesrv_addr(NAMESRV)
    c.set_message_listener(listener)
    c.set_consume_thread_nums(1)
    c.subscribe(topic, "*")
    c.start()

    try:
        mq = business_queue(c, topic) if wait_until(
            lambda: business_queue(c, topic) is not None, 30) else None
        check("A0-业务队列已分配（%RETRY% 队列不算）", mq is not None, "mq=%s" % mq)
        if mq is None:
            return 1
        key = c._mq_key(mq)

        # ---------- A1 基线 ----------
        bodies1 = [b"pe-1-%d" % i for i in range(3)]
        for b in bodies1:
            producer.send(Message(topic, b))
        check("A1-首批 3 条被消费", wait_arrivals(listener, 3),
              "arrivals=%d" % len(listener.bodies()))
        check("A1-位点提交到 3", wait_until(lambda: committed_offset(group, topic) == 3, 20),
              "offset=%d" % committed_offset(group, topic))
        pqi = pqi_of(c.consumer_running_info(), mq)
        stamp = pqi.get("lastPullTimestamp", 0)
        age = time.time() - stamp / 1000.0
        check("A1-运行信息报真实 lastPullTimestamp（不是写死的 0）",
              stamp > 0 and 0 <= age < 30,
              "lastPullTimestamp=%d age=%.1fs" % (stamp, age))

        # ---------- A2 线程死掉 → 自愈 ----------
        t_old, f_old = inject_and_rebalance(c, key, "dead")
        rebuilt = wait_rebuilt(c, key, t_old, f_old)
        check("A2-停摆线程被换掉（isPullExpired 的线程死亡分支）", rebuilt,
              "new=%s" % pull_thread_of(c, key))
        bodies2 = [b"pe-2-%d" % i for i in range(3)]
        for b in bodies2:
            producer.send(Message(topic, b))
        check("A2-自愈后同一个队列继续消费（位点走到 6）",
              wait_arrivals(listener, 6)
              and wait_until(lambda: committed_offset(group, topic) == 6, 25),
              "arrivals=%d offset=%d" % (len(listener.bodies()),
                                         committed_offset(group, topic)))

        # ---------- A3 盖章过期 → 自愈 ----------
        t_old3, f_old3 = inject_and_rebalance(c, key, "stale")
        rebuilt3 = wait_rebuilt(c, key, t_old3, f_old3)
        check("A3-超过 120s 没发起拉取的队列被撤并重建（线程还活着也要换）", rebuilt3,
              "new=%s" % pull_thread_of(c, key))
        bodies3 = [b"pe-3-%d" % i for i in range(3)]
        for b in bodies3:
            producer.send(Message(topic, b))
        check("A3-重建后继续消费（位点走到 9）",
              wait_arrivals(listener, 9)
              and wait_until(lambda: committed_offset(group, topic) == 9, 25),
              "arrivals=%d offset=%d" % (len(listener.bodies()),
                                         committed_offset(group, topic)))

        # ---------- A4 自愈不许回退位点 ----------
        time.sleep(5)   # 留窗口给"重建时从 0 重投"这类错误显形
        all_bodies = bodies1 + bodies2 + bodies3
        check("A4-9 条各只投一次（撤走前持久化了位点，重建后从 broker 位点续拉）",
              sorted(listener.bodies()) == sorted(all_bodies),
              "arrivals=%d distinct=%d" % (len(listener.bodies()),
                                           len(set(listener.bodies()))))
        check("A4-没有任何一条被重投（reconsumeTimes 全 0）",
              all(t == 0 for b in all_bodies for t in listener.times(b)),
              "times=%s" % sorted({t for b in all_bodies for t in listener.times(b)}))
        pqi2 = pqi_of(c.consumer_running_info(), mq)
        check("A4-自愈后 lastPullTimestamp 恢复新鲜",
              0 <= time.time() - pqi2["lastPullTimestamp"] / 1000.0 < 30,
              "age=%.1fs" % (time.time() - pqi2["lastPullTimestamp"] / 1000.0))
    finally:
        c.shutdown()
        producer.shutdown()

    print("\nRESULT: %d PASS / %d FAIL" % (PASS, FAIL))
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(main())
