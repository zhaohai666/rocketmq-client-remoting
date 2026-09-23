#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""拉取前流控（Java ProcessQueue 五个阈值）真机验证。

用法（需本地 RocketMQ 5.5.1 集群）：
    .venv/bin/python verify_flow_control_live.py 127.0.0.1:9876

离线单测（tests/test_flow_control.py）锁的是**判据本身**；这里锁的是真机上两件
离线永远锁不住的事：
  A. 闸门在真实 broker 上**确实会命中**（单位错一位、阈值读错一个字段，离线用
     mock 缓冲照样"能命中"，真机上却永远不命中或永远命中）；
  B. 命中之后**一条消息都不许丢**：流控只是"暂停拉取"，不是"丢弃/跳过"。
     暂停期间位点不许越过还没消费完的消息，恢复后同一个队列必须继续消费到末尾。
     这条是流控最危险的错误方向 —— 实现写成"命中就丢批/退出循环"在 10 秒窗口里
     完全看不出来，只有把全部消息数完才发现。

场景（每条都同时验 A 和 B）：
  S0 默认闸门 + 快消费：不命中（triggered==0），消息全部到达 —— 防"闸门误触发把正常流量也停了"。
  S1 队列级字节闸门：条数闸门放到不可能命中，size=1MiB + 400KB 大消息 + 慢消费。
  S2 位点跨度闸门：size/条数都压到不命中，consume_concurrently_max_span=2 + 慢消费。
  S3 topic 级条数闸门：队列级三条全关掉，pull_threshold_for_topic=4 + 4 队列 + 慢消费。
  S4 命中后恢复：S1 用的队列继续投新消息，仍被正常消费（暂停不是停摆）。
  S5 配置数值闸门（Java checkConfig :1099-1209）：区间**边界值**在真集群上能启动并
     全部消费；越界的配置在本地就被拒，broker 侧根本不知道有这个消费组（离线单测
     只能证明"抛了异常"，证不了"没把半套配置发到 broker 上"）。

关于 S1~S4 里"关掉某道闸门"的写法：这里用的是 Java 允许的**极值**（65535 / 1024 /
-1），不是 0。Java 的 checkConfig 把 0 判成非法值（``pullThresholdSizeForQueue``
区间是 [1, 1024]），本仓库运行期另有 ``max(1, n)`` 兜底，但那道兜底只服务运行期
热改，不能拿来越过启动期闸门 —— 用极值"关掉"闸门既符合 Java，也不依赖兜底。
"""
from __future__ import annotations

import os
import sys
import threading
import time

sys.path.insert(0, ".")

from rocketmq.client.consumer import (ConsumeConcurrentlyStatus,
                                      DefaultMQPushConsumer,
                                      MessageListenerConcurrently)
from rocketmq.client.exception import MQBrokerException, MQClientException
from rocketmq.client.mq_client import MQClientInstance
from rocketmq.client.producer import DefaultMQProducer
from rocketmq.common.message import Message

NAMESRV = sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1:9876"
PREFIX = "FcPy_%d" % int(time.time() * 1000)

# "关掉这道闸门"的合法写法：Java checkConfig 的**上界**（[1, 65535] / [1, 1024]），
# 而不是 0。0 会被启动期闸门拒（见 S5），且它依赖的是运行期 max(1,n) 兜底。
OFF_COUNT = 65535        # pullThresholdForQueue / consumeConcurrentlyMaxSpan 的上界
OFF_SIZE_MB = 1024       # pullThresholdSizeForQueue 的上界（单位 MiB）

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
    """按真实用法先建 topic：消费者不做默认 topic 兜底，没路由就不分配队列。"""
    c = MQClientInstance("fc-setup-%d" % int(time.time() * 1000), [NAMESRV])
    c.start()
    try:
        c.create_topic_in_route(topic, queues, queues)
    finally:
        c.shutdown()


class Recorder(MessageListenerConcurrently):
    """记录到达的消息体；可选地每批睡 slow 秒（制造"已拉未消费"的堆积）。"""

    def __init__(self, slow: float = 0.0):
        self.slow = slow
        self.bodies = []
        self._lock = threading.Lock()

    def consume_message(self, msgs, context):
        if self.slow > 0:
            time.sleep(self.slow)
        with self._lock:
            self.bodies.extend(bytes(m.body) for m in msgs)
        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS

    def count(self, prefix: bytes) -> int:
        with self._lock:
            return sum(1 for b in self.bodies if b.startswith(prefix))

    def snapshot(self):
        with self._lock:
            return list(self.bodies)


def make_consumer(group: str, topic: str, listener: Recorder,
                  **thresholds) -> DefaultMQPushConsumer:
    c = DefaultMQPushConsumer(group)
    c.set_namesrv_addr(NAMESRV)
    c.set_message_listener(listener)
    for k, v in thresholds.items():
        setattr(c, k, v)
    c.subscribe(topic, "*")
    c.start()
    return c


def wait_until(pred, timeout: float, interval: float = 0.5) -> bool:
    end = time.time() + timeout
    while time.time() < end:
        if pred():
            return True
        time.sleep(interval)
    return False


def run_gate_case(label: str, producer: DefaultMQProducer, topic: str, group: str,
                  msgs: list, listener_slow: float, thresholds: dict,
                  settle: float = 25.0) -> DefaultMQPushConsumer:
    """跑一个闸门场景：返回消费者，调用方负责断言与 shutdown。"""
    rec = Recorder(listener_slow)
    c = make_consumer(group, topic, rec, **thresholds)
    # 等分配稳定（消费者要先被 broker 登记，rebalance 周期 20s，启动期缩短为 2s）
    time.sleep(3)
    for m in msgs:
        producer.send(m)
    # 命中闸门要等堆起来；全部消费完要等闸门松开。留足 settle 秒
    time.sleep(settle)
    c.recorder = rec
    return c


def big_body(tag: int) -> bytes:
    """400KB **不可压缩** 的消息体。

    必须是随机字节：生产者对超过压缩阈值的 body 会先试压，全同字节的 payload 会被压到
    几百字节，broker 落盘的 storeSize 就跟着变成几百字节（第一次跑这个验证时 400KB 的
    b"F"*… 只量出 640B，size 闸门"永不命中"其实是夹具可压缩造成的，不是实现错）。
    前缀留着以便按条辨认。
    """
    return b"FC%04d" % tag + os.urandom(400 * 1024 - 6)


def main() -> int:
    producer = DefaultMQProducer(PREFIX + "_pg")
    producer.set_namesrv_addr(NAMESRV)
    producer.start()
    time.sleep(1)

    MiB = 1024 * 1024
    big = big_body

    # ---------- S0 默认闸门 + 快消费：不该命中 ----------
    t0 = PREFIX + "_Defaults"
    prepare_topic(t0)
    c0 = run_gate_case(
        "S0", producer, t0, PREFIX + "_g0",
        [Message(t0, (b"ok-%d" % i)) for i in range(12)],
        listener_slow=0.0, thresholds={})
    got0 = c0.recorder.snapshot()
    check("S0-默认闸门不命中", c0._flow_control_triggered == 0,
          "triggered=%d" % c0._flow_control_triggered)
    check("S0-默认闸门下全部到达", len(got0) == 12, "got=%d" % len(got0))
    check("S0-一条不重不丢", sorted(got0) == sorted(b"ok-%d" % i for i in range(12)),
          "distinct=%d" % len(set(got0)))
    c0.shutdown()

    # ---------- S1 队列级字节闸门 ----------
    # 条数闸门放到 Java 允许的上界：这条场景里**只有** size 闸门可能命中。
    # topic 必须只有 1 个队列：8 条 400KB 摊到 4 个队列上每队才 800KB，永远够不到
    # 1MiB 这道队列级闸门（第一次跑就是这么"闸门不命中"的，不是实现错）。
    t1 = PREFIX + "_Size"
    prepare_topic(t1, queues=1)
    c1 = run_gate_case(
        "S1", producer, t1, PREFIX + "_g1",
        [Message(t1, big(i)) for i in range(8)],
        listener_slow=0.3,
        thresholds={"pull_threshold_for_queue": OFF_COUNT,
                    "pull_threshold_size_for_queue": 1,
                    "consume_concurrently_max_span": OFF_COUNT})
    got1 = c1.recorder.snapshot()
    check("S1-队列级字节闸门真机命中", c1._flow_control_triggered > 0,
          "triggered=%d" % c1._flow_control_triggered)
    check("S1-大消息一条不丢", len(got1) == 8, "got=%d" % len(got1))
    check("S1-8 条 400KB 消息全不重复", len(set(got1)) == 8, "distinct=%d" % len(set(got1)))
    c1.shutdown()

    # ---------- S2 位点跨度闸门 ----------
    # 条数与字节两道都压到不命中（各自上界），只剩跨度。
    t2 = PREFIX + "_Span"
    prepare_topic(t2)
    c2 = run_gate_case(
        "S2", producer, t2, PREFIX + "_g2",
        [Message(t2, (b"s-%d" % i)) for i in range(14)],
        listener_slow=0.3,
        thresholds={"pull_threshold_for_queue": OFF_COUNT,
                    "pull_threshold_size_for_queue": OFF_SIZE_MB,
                    "consume_concurrently_max_span": 2})
    got2 = c2.recorder.snapshot()
    check("S2-跨度闸门真机命中", c2._flow_control_triggered > 0,
          "triggered=%d" % c2._flow_control_triggered)
    check("S2-跨度过限后仍全部消费", len(got2) == 14, "got=%d" % len(got2))
    c2.shutdown()

    # ---------- S3 topic 级条数闸门 ----------
    # 队列级三条全压到不命中，只留 topic 级：必须跨队列累计才可能命中（单队列各自为政则永不命中）。
    t3 = PREFIX + "_Topic"
    prepare_topic(t3, queues=4)
    c3 = run_gate_case(
        "S3", producer, t3, PREFIX + "_g3",
        [Message(t3, (b"t-%d" % i)) for i in range(16)],
        listener_slow=0.3,
        thresholds={"pull_threshold_for_queue": OFF_COUNT,
                    "pull_threshold_size_for_queue": OFF_SIZE_MB,
                    "consume_concurrently_max_span": OFF_COUNT,
                    "pull_threshold_for_topic": 4})
    got3 = c3.recorder.snapshot()
    check("S3-topic 级条数闸门真机命中", c3._flow_control_triggered > 0,
          "triggered=%d" % c3._flow_control_triggered)
    check("S3-跨队列累计后仍全部消费", len(got3) == 16, "got=%d" % len(got3))
    c3.shutdown()

    # ---------- S4 命中过流控的队列恢复后继续消费 ----------
    # 复用 S1 的组与 topic（位点已由 S1 提交到 broker 末尾）：闸门保持原样，投一批新消息。
    # 这一条锁的是"暂停 100ms"被写成"退出拉取循环"的错误 —— 那条队列会永久停摆，
    # 而 S1 里已经消费完的消息看不出任何差别。
    rec4 = Recorder(0.3)
    c4 = make_consumer(PREFIX + "_g1", t1, rec4,
                       pull_threshold_for_queue=OFF_COUNT,
                       pull_threshold_size_for_queue=1,
                       consume_concurrently_max_span=OFF_COUNT)
    time.sleep(3)
    for i in range(6):
        producer.send(Message(t1, big(100 + i)))
    time.sleep(25)
    got4 = rec4.snapshot()
    check("S4-触发过流控的队列恢复后继续消费", len(got4) == 6, "got=%d" % len(got4))
    check("S4-恢复批次仍然命中流控（闸门不会命中一次后失效）",
          c4._flow_control_triggered > 0, "triggered=%d" % c4._flow_control_triggered)
    check("S4-恢复批次不重复", len(set(got4)) == 6, "distinct=%d" % len(set(got4)))
    c4.shutdown()

    # ---------- S5 配置数值闸门（Java checkConfig :1099-1209）----------
    # 离线单测（tests/test_consumer_check_config.py）锁的是区间与文案；这里补两件
    # 只有真集群能锁死的事：
    #   1. 落在 Java 区间**边界**上的配置在 broker 上真能把消费者跑起来并收全消息 ——
    #      闸门写歪最常见的方式是"比 Java 还严"，把合法配置也拒了，用户直接起不来；
    #   2. 越界的配置**没有打到 broker 上**。写成"先注册再校验"的话，broker 的
    #      ConsumerManager 会留下一堆永不心跳的僵尸 clientId，把 rebalance 用的
    #      cidAll 撑歪（真机表现为队列分配不均），而客户端日志里只有启动失败那一条。
    t5 = PREFIX + "_Config"
    prepare_topic(t5)
    legal = {
        "consume_thread_min": 1, "consume_thread_max": 2,
        "consume_concurrently_max_span": OFF_COUNT,
        "pull_threshold_for_queue": OFF_COUNT,
        "pull_threshold_for_topic": -1,
        "pull_threshold_size_for_queue": OFF_SIZE_MB,
        "pull_threshold_size_for_topic": -1,
        "pull_interval": 0,
        "consume_message_batch_max_size": 1,
        "pull_batch_size": 1024,          # Java 区间上界
        "pop_invisible_time": 300000,     # MAX_POP_INVISIBLE_TIME
        "pop_batch_nums": 32,
    }
    rec5 = Recorder(0.0)
    c5 = make_consumer(PREFIX + "_g5", t5, rec5, **legal)
    check("S5-边界值配置能启动", c5._started is True)
    time.sleep(3)
    for i in range(10):
        producer.send(Message(t5, b"c-%d" % i))
    wait_until(lambda: len(rec5.snapshot()) >= 10, 20)
    got5 = rec5.snapshot()
    check("S5-边界值配置下 10 条全到达",
          sorted(got5) == sorted(b"c-%d" % i for i in range(10)),
          "got=%d distinct=%d" % (len(got5), len(set(got5))))

    # 2) 越界配置：本地拒 + broker 侧查不到这个组
    rejected = [
        ({"pull_threshold_size_for_queue": 0},
         "pullThresholdSizeForQueue Out of range [1, 1024]"),
        ({"pull_batch_size": 1025}, "pullBatchSize Out of range [1, 1024]"),
        ({"pop_invisible_time": 4999}, "popInvisibleTime Out of range [5000, 300000]"),
        ({"pop_batch_nums": 33}, "popBatchNums Out of range [1, 32]"),
        ({"consume_thread_min": 8, "consume_thread_max": 4},
         "consumeThreadMin (8) is larger than consumeThreadMax (4)"),
    ]
    bad_group = PREFIX + "_g6"
    for overrides, want in rejected:
        c6 = DefaultMQPushConsumer(bad_group)
        c6.set_namesrv_addr(NAMESRV)
        c6.set_message_listener(Recorder(0.0))
        for k, v in overrides.items():
            setattr(c6, k, v)
        c6.subscribe(t5, "*")
        try:
            c6.start()
            check("S5-越界配置被拒: %s" % want, False, "start() 居然成功了")
            c6.shutdown()
            continue
        except MQClientException as e:
            check("S5-越界配置被拒: %s" % want, str(e) == want, "实际=%s" % str(e))
        check("S5-越界配置没留下半启动实例: %s" % want,
              c6._started is False and c6._mq_client is None)

    probe = MQClientInstance("fc-probe-%d" % int(time.time() * 1000), [NAMESRV])
    probe.start()
    try:
        addr = probe.find_broker_addr_by_topic(t5)
        check("S5-拿到 broker 地址用于查消费组", addr is not None, "addr=%s" % addr)
        if addr:
            # 从未注册过的组：broker 的 GET_CONSUMER_LIST_BY_GROUP 不会回空列表，而是
            # 直接甩 GROUP_NOT_EXIST —— 它同样是"broker 不认识这个组"的证据。两种形态都
            # 收下，但**必须**是"查无此组"，绝不能返回任何 clientId。
            try:
                bad_ids = probe.get_consumer_list_by_group(bad_group, addr=addr).consumer_id_list
                bad_absent = not bad_ids
                detail = "ids=%s" % bad_ids
            except MQBrokerException as e:
                bad_absent = True
                detail = "broker 直接拒绝: code=%d %s" % (e.response_code, e.error_message)
            check("S5-broker 侧不知道被拒的消费组", bad_absent, detail)
            ok_ids = probe.get_consumer_list_by_group(PREFIX + "_g5", addr=addr)
            check("S5-broker 侧认下了边界值消费者",
                  len(ok_ids.consumer_id_list) == 1, "ids=%s" % ok_ids.consumer_id_list)
    finally:
        c5.shutdown()
        probe.shutdown()

    producer.shutdown()
    print("flow control live: %d PASS / %d FAIL" % (PASS, FAIL))
    return 0 if FAIL == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
