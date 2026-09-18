#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""轻量拉取消费者（DefaultLitePullConsumer）真机验证。

用法（需本地 RocketMQ 5.5.1 集群）：
    .venv/bin/python verify_lite_pull_live.py 127.0.0.1:9876

为什么单独测：DefaultLitePullConsumer 是三个语言都缺失的能力（Java 有，本仓库此前三语言都只有
手动的 DefaultMQPullConsumer）。本脚本把它的核心——「subscribe/assign + 后台灌本地缓冲 + poll 取批量」——
在真实集群上跑通。

场景（围绕 Lite 相对 Pull 的本质区别：调用方不用管位点，poll 从本地缓冲拿消息）：
  S1 建 topic + subscribe 模式等待 rebalance 分到位点
  S2 先起消费者、再发 12 条（交替 TagA/TagB）→ subscribe + poll 收全 12 条且内容一致
  S3 auto-commit：消费后 committed 位点 > 0，且 commit() 后可回读
  S4 assign 模式：显式 assign 全部队列 + seek 到队首 → poll 重新收全 12 条（验证 assign/seek/poll）
  S5 订阅 TagA：subscribe(T, "TagA") 只收 TagA 的 6 条（验证订阅级 tag 过滤）
  S6 CONSUME_FROM_TIMESTAMP：consumeTimestamp 按 Java 的 14 位本地墙钟解释
     S6a 新组 + 起点=30 分钟前 → 收全 12 条
     S6b 墙钟→队列位置映射：30 分钟前 → 各队列队首（Σ=0）；10 分钟后 → 越过全部消息（Σ=12）
     旧实现把墙钟串当 epoch 解析会把两个方向同时翻转。
"""
from __future__ import annotations

import sys
import time

sys.path.insert(0, ".")

from rocketmq.client.consumer import DefaultLitePullConsumer, DefaultMQPullConsumer
from rocketmq.client.producer import DefaultMQProducer
from rocketmq.common.message import Message, MessageQueue
from rocketmq.remoting.protocol.heartbeat import ConsumeFromWhere

NAMESRV = sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1:9876"
STAMP = int(time.time() * 1000)
TOPIC = "LiteLive_%d" % STAMP
GROUP1 = "LitePG1_%d" % STAMP
GROUP2 = "LitePG2_%d" % STAMP
GROUP3 = "LitePG3_%d" % STAMP
GROUP4 = "LitePG4_%d" % STAMP
GROUP5 = "LitePG5_%d" % STAMP
N_MSG = 12
QUEUE_NUM = 4
TAG_A = {("lite-%02d" % i).encode() for i in range(0, N_MSG, 2)}   # 偶数下标 → TagA，共 6 条
TAG_B = {("lite-%02d" % i).encode() for i in range(1, N_MSG, 2)}   # 奇数下标 → TagB，共 6 条

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


def prepare_topic() -> None:
    prod = DefaultMQProducer("PG_Prepare_%d" % STAMP)
    prod.set_namesrv_addr(NAMESRV)
    prod.start()
    try:
        prod.create_topic("TBW102", TOPIC, QUEUE_NUM)
    finally:
        prod.shutdown()
    # 等路由注册（用 DefaultMQPullConsumer 探测即可，它不要求先订阅）
    deadline = time.time() + 20
    while time.time() < deadline:
        c = DefaultMQPullConsumer("PG_Probe_%d" % STAMP)
        c.set_namesrv_addr(NAMESRV)
        c.start()
        try:
            qs = c.fetch_subscribe_message_queues(TOPIC)
            if len(qs) >= QUEUE_NUM:
                return
        except BaseException:
            pass
        finally:
            c.shutdown()
        time.sleep(1)
    raise RuntimeError("topic not routable after 20s")


def produce() -> set:
    print("\nS2 生产 %d 条（交替 TagA/TagB）" % N_MSG)
    prod = DefaultMQProducer("PG_LiteLive_%d" % STAMP)
    prod.set_namesrv_addr(NAMESRV)
    prod.start()
    sent = set()
    try:
        for i in range(N_MSG):
            body = ("lite-%02d" % i).encode()
            msg = Message(TOPIC, body)
            msg.set_keys("lite-key-%02d" % i)
            msg.set_tags("TagA" if i % 2 == 0 else "TagB")
            prod.send(msg)
            sent.add(bytes(body))
    finally:
        prod.shutdown()
    check("S2 生产 %d 条成功" % N_MSG, len(sent) == N_MSG, "sent=%d" % len(sent))
    return sent


def wait_assignment(consumer: DefaultLitePullConsumer, timeout: int = 20) -> list:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if consumer.assignment():
            return consumer.assignment()
        time.sleep(0.5)
    return []


def drain(consumer: DefaultLitePullConsumer, expect: int, timeout: int = 30) -> list:
    collected = []
    deadline = time.time() + timeout
    while len(collected) < expect and time.time() < deadline:
        batch = consumer.poll(timeout=1000)
        collected.extend(batch)
    return collected


def main() -> int:
    print("LitePullConsumer 真机验证 topic=%s group=%s" % (TOPIC, GROUP1))
    prepare_topic()

    # ===== S2 subscribe 模式：先起消费者再发消息 =====
    print("\nS1 subscribe 模式启动 + 等待 rebalance 分到位点")
    c1 = DefaultLitePullConsumer(GROUP1)
    c1.set_namesrv_addr(NAMESRV)
    c1.set_poll_timeout_millis(1000)
    c1.subscribe(TOPIC, "*")
    c1.start()
    assigned = wait_assignment(c1)
    check("S1 rebalance 分配到队列", len(assigned) == QUEUE_NUM, "assigned=%d" % len(assigned))
    if not assigned:
        print("!! 未分配到队列，后续跳过")
        c1.shutdown()
        return 1

    sent = produce()
    got = drain(c1, N_MSG)
    got_bytes = {bytes(m.body) for m in got}
    check("S2 subscribe+poll 收全 %d 条且内容一致" % N_MSG, got_bytes == sent,
          "got=%d missing=%s extra=%s" % (len(got), sorted(sent - got_bytes),
                                          sorted(got_bytes - sent)))

    # ===== S3 auto-commit =====
    print("\nS3 auto-commit 位点")
    before = {mq: c1.committed(mq) for mq in assigned}
    c1.commit()
    after = {mq: c1.committed(mq) for mq in assigned}
    check("S3 各队列 committed 位点 > 0", all(v is not None and v > 0 for v in after.values()),
          "before=%s after=%s" % (before, after))

    # ===== S4 assign 模式：显式分配 + seek 到队首重新收全 =====
    print("\nS4 assign 模式：assign 全部队列 + seek 到队首重新收全")
    c2 = DefaultLitePullConsumer(GROUP2)
    c2.set_namesrv_addr(NAMESRV)
    c2.set_poll_timeout_millis(1000)
    c2.set_consume_from_where(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET)
    all_queues = c1.fetch_message_queues(TOPIC)
    c2.assign(all_queues)
    c2.start()
    for mq in all_queues:
        c2.seek_to_begin(mq)
    got2 = drain(c2, N_MSG)
    got2_bytes = {bytes(m.body) for m in got2}
    check("S4 assign+seek+poll 重新收全 %d 条" % N_MSG, got2_bytes == sent,
          "got=%d" % len(got2))
    c2.shutdown()

    # ===== S5 订阅级 tag 过滤 =====
    print("\nS5 订阅 TagA：只收 TagA 的 6 条")
    c3 = DefaultLitePullConsumer(GROUP3)
    c3.set_namesrv_addr(NAMESRV)
    c3.set_poll_timeout_millis(1000)
    c3.set_consume_from_where(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET)
    c3.subscribe(TOPIC, "TagA")
    c3.start()
    wait_assignment(c3)
    got3 = drain(c3, 6)
    got3_bytes = {bytes(m.body) for m in got3}
    print("    [diag] S5 got=%d bodies=%s tags=%s" % (
        len(got3), sorted(bytes(m.body).decode() for m in got3),
        sorted((m.get_tags() or "?") for m in got3)))
    only_a = got3_bytes.issubset(TAG_A) and len(got3_bytes) == 6
    check("S5 仅收 TagA 且恰好 6 条", only_a,
          "got=%d set_ok=%s" % (len(got3), got3_bytes <= TAG_A))
    c3.shutdown()

    # ===== S6 CONSUME_FROM_TIMESTAMP：14 位本地墙钟 =====
    print("\nS6 CONSUME_FROM_TIMESTAMP：墙钟起点收全 + 时间戳→位点映射")
    wall = int(time.time() * 1000)

    def wall_clock(delta_ms: int) -> str:
        return time.strftime("%Y%m%d%H%M%S", time.localtime((wall + delta_ms) / 1000))

    c4 = DefaultLitePullConsumer(GROUP4)
    c4.set_namesrv_addr(NAMESRV)
    c4.set_poll_timeout_millis(1000)
    c4.consume_from_where = ConsumeFromWhere.CONSUME_FROM_TIMESTAMP
    c4.consume_timestamp = wall_clock(-30 * 60 * 1000)
    c4.subscribe(TOPIC, "*")
    c4.start()
    wait_assignment(c4)
    got4 = drain(c4, N_MSG)
    got4_bytes = {bytes(m.body) for m in got4}
    check("S6a 起点=%s 早于全部消息 → 收全 %d 条" % (c4.consume_timestamp, N_MSG),
          got4_bytes == sent, "got=%d" % len(got4_bytes))
    c4.shutdown()

    c5 = DefaultLitePullConsumer(GROUP5)
    c5.set_namesrv_addr(NAMESRV)
    c5.set_poll_timeout_millis(1000)
    c5.consume_from_where = ConsumeFromWhere.CONSUME_FROM_TIMESTAMP
    c5.consume_timestamp = wall_clock(10 * 60 * 1000)
    c5.subscribe(TOPIC, "*")
    c5.start()
    q5 = wait_assignment(c5)
    # 不断言「未来时间戳收不到消息」：Java 的 RebalanceLitePullImpl 先读已提交位点，
    # 新组只要队首仍在 commitlog 内，broker 就直接回 0，consume_from_where 不参与。
    # 墙钟真正影响的是「时间戳 → 队列位置」的映射，所以断言这个量。
    sum_past = sum(c5.offset_for_timestamp(mq, wall - 30 * 60 * 1000) for mq in q5)
    sum_future = sum(c5.offset_for_timestamp(mq, wall + 10 * 60 * 1000) for mq in q5)
    check("S6b 30 分钟前 → 各队列队首", sum_past == 0, "sumOffset=%d" % sum_past)
    check("S6b 10 分钟后 → 越过全部 %d 条" % N_MSG, sum_future == N_MSG,
          "sumOffset=%d" % sum_future)
    c5.shutdown()

    c1.shutdown()
    print("\nLitePullConsumer: PASS=%d FAIL=%d" % (PASS, FAIL))
    return 0 if FAIL == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
