#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""主动拉取消费者（DefaultMQPullConsumer）真机验证。

用法（需本地 RocketMQ 5.5.1 集群）：
    .venv/bin/python verify_pull_live.py 127.0.0.1:9876

为什么单独测：`DefaultMQPullConsumer` 在项目里存在已久，但**从未被任何测试或联调脚本
调用过**（grep 全仓 0 命中）——也就是说它是"纸面能力"。本脚本把它真正跑起来。

场景（拉模式的核心是「调用方自己拉、自己管位点」，所以断言都围绕这一点）：
  S1 建 topic + fetch_subscribe_message_queues → 拿到 4 个队列
  S2 生产 12 条 → 每队列 min/max offset 差值 = 3（消息均匀落到 4 队列）
  S3 **手动拉取**：逐队列从 min offset 拉到 max offset → 收全 12 条且 body 与发送集合一致
  S4 **手动提交位点**：update_consume_offset → fetch_consume_offset 回读一致（broker 往返）
  S5 **位点由调用方掌控**：从已提交位点再拉 → NO_NEW_MSG；把位点退回 min 再拉 → FOUND
     （push 模式做不到这一点，这正是 pull 模式的存在意义）
  S6 search_offset(now) / earliest_msg_store_time → 均 > 0
  S7 send_message_back → 消息落到 %RETRY%group，可被拉取到（回投链路真实可用）
"""
from __future__ import annotations

import sys
import time

sys.path.insert(0, ".")

from rocketmq.client.consumer import DefaultMQPullConsumer
from rocketmq.client.consumer_result import PullStatus
from rocketmq.client.producer import DefaultMQProducer
from rocketmq.common.message import Message, MessageExt, MessageQueue
from rocketmq.remoting.exception import RemotingException

NAMESRV = sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1:9876"
STAMP = int(time.time() * 1000)
TOPIC = "PullLive_%d" % STAMP
GROUP = "PG_PullLive_%d" % STAMP
RETRY_TOPIC = "%RETRY%" + GROUP
N_MSG = 12
QUEUE_NUM = 4
PER_QUEUE = N_MSG // QUEUE_NUM

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


def make_consumer() -> DefaultMQPullConsumer:
    c = DefaultMQPullConsumer(GROUP)
    c.set_namesrv_addr(NAMESRV)
    c.start()
    return c


def prepare_topic() -> None:
    """先建 topic 再起消费者：消费者不做默认 topic 兜底，topic 不存在就拿不到路由。"""
    prod = DefaultMQProducer("PG_Prepare_%d" % STAMP)
    prod.set_namesrv_addr(NAMESRV)
    prod.start()
    try:
        prod.create_topic("TBW102", TOPIC, QUEUE_NUM)
    finally:
        prod.shutdown()
    deadline = time.time() + 20
    last = ""
    while time.time() < deadline:
        c = make_consumer()
        try:
            qs = c.fetch_subscribe_message_queues(TOPIC)
            if len(qs) >= QUEUE_NUM:
                return
            last = "%d queues" % len(qs)
        except BaseException as e:  # noqa: BLE001
            last = "%s: %s" % (type(e).__name__, e)
        finally:
            c.shutdown()
        time.sleep(1)
    raise RuntimeError("topic not routable after 20s: " + last)


def safe_pull(consumer: DefaultMQPullConsumer, q: MessageQueue, expr: str, offset: int,
              max_nums: int = 32, timeout: int = 5000):
    """包一层 pull：超时/网络异常返回 (None, detail)，让断言能报 FAIL 而不是抛栈中断脚本。"""
    try:
        return consumer.pull(q, expr, offset, max_nums, timeout), ""
    except RemotingException as e:
        return None, "%s: %s" % (type(e).__name__, e)


def scenario_queues(consumer: DefaultMQPullConsumer):
    print("\nS1 建 topic + fetch_subscribe_message_queues")
    queues = consumer.fetch_subscribe_message_queues(TOPIC)
    check("S1 拿到 %d 个队列" % QUEUE_NUM, len(queues) == QUEUE_NUM,
          "got=%d" % len(queues))
    check("S1 队列 topic 与 broker 名非空",
          bool(queues) and all(q.topic == TOPIC and q.broker_name for q in queues),
          "sample=%s/%s" % ((queues[0].broker_name, queues[0].queue_id) if queues else "n/a",
                            len(queues)))
    return queues


def scenario_produce() -> list:
    print("\nS2 生产 %d 条" % N_MSG)
    prod = DefaultMQProducer("PG_PullLive_%d" % STAMP)
    prod.set_namesrv_addr(NAMESRV)
    prod.start()
    sent = []
    try:
        for i in range(N_MSG):
            body = ("pull-%02d" % i).encode()
            msg = Message(TOPIC, body)
            msg.set_keys("pull-key-%02d" % i)
            prod.send(msg)
            sent.append(body)
    finally:
        prod.shutdown()
    check("S2 生产 %d 条成功" % N_MSG, len(sent) == N_MSG, "sent=%d" % len(sent))
    return sent


def wait_offsets(consumer: DefaultMQPullConsumer, routes, expect: int):
    """等 consumequeue 异步分发落地：轮询到各队列 max-min 之和 >= expect。

    ⚠ 不能"刚发完立刻查 maxOffset"——broker 是异步分发的，会读到 0。
    """
    deadline = time.time() + 25
    lo = {}
    hi = {}
    while time.time() < deadline:
        lo = {}
        hi = {}
        for q in routes:
            lo[q] = consumer.min_offset(q)
            hi[q] = consumer.max_offset(q)
        if sum(max(0, hi[q] - lo[q]) for q in routes) >= expect:
            break
        time.sleep(0.5)
    return lo, hi


def scenario_pull(consumer: DefaultMQPullConsumer, routes, lo, hi, sent_set) -> set:
    print("\nS3 手动拉取（逐队列 min -> max）")
    got = set()
    for q in routes:
        offset = lo[q]
        guard = 0
        while offset < hi[q] and guard < 64:
            guard += 1
            result, err = safe_pull(consumer, q, "*", offset)
            if result is None:
                check("S3 拉取 %s:%d 异常" % (q.broker_name, q.queue_id), False, err)
                break
            if result.status == PullStatus.FOUND:
                for m in result.msg_found_list:
                    got.add(bytes(m.body))
                if result.next_begin_offset <= offset:
                    break
                offset = result.next_begin_offset
            elif result.status == PullStatus.NO_NEW_MSG:
                break
            elif result.status == PullStatus.OFFSET_ILLEGAL:
                break
            else:
                check("S3 拉取 %s:%d 状态异常" % (q.broker_name, q.queue_id), False,
                      "status=%s" % result.status)
                break
    check("S3 手动拉取收全 %d 条且内容一致" % N_MSG, got == sent_set,
          "got=%d missing=%s extra=%s" % (len(got), sorted(sent_set - got),
                                          sorted(got - sent_set)))
    return got


def scenario_commit(consumer: DefaultMQPullConsumer, q, target: int) -> int:
    print("\nS4 手动提交位点并回读")
    consumer.update_consume_offset(q, target)
    back = consumer.fetch_consume_offset(q)
    check("S4 位点提交后回读一致", back == target,
          "committed=%s target=%d" % (back, target))
    return target


def scenario_own_offset(consumer: DefaultMQPullConsumer, q, committed: int, lo: int) -> None:
    print("\nS5 位点由调用方掌控")
    r1, err1 = safe_pull(consumer, q, "*", committed)
    if r1 is None:
        check("S5 从已提交位点再拉 = NO_NEW_MSG", False, err1)
    else:
        check("S5 从已提交位点再拉 = NO_NEW_MSG",
              r1.status == PullStatus.NO_NEW_MSG and len(r1.msg_found_list) == 0,
              "status=%s n=%d" % (r1.status, len(r1.msg_found_list)))

    # 关键能力：把位点退回 min，就能把同一批消息**再拉一次**（push 模式做不到）
    r2, err2 = safe_pull(consumer, q, "*", lo)
    if r2 is None:
        check("S5 位点退回 min 后可重拉（pull 模式的核心能力）", False, err2)
    else:
        check("S5 位点退回 min 后可重拉（pull 模式的核心能力）",
              r2.status == PullStatus.FOUND and len(r2.msg_found_list) > 0,
              "status=%s n=%d" % (r2.status, len(r2.msg_found_list)))


def scenario_offset_query(consumer: DefaultMQPullConsumer, q, lo: int, hi: int) -> None:
    print("\nS6 search_offset / earliest_msg_store_time / min/max")
    now = int(time.time() * 1000)
    so = consumer.search_offset(q, now)
    check("S6 search_offset(now) > 0", so > 0, "searchOffset=%d" % so)
    emst = consumer.earliest_msg_store_time(q)
    check("S6 earliest_msg_store_time > 0", emst > 0, "earliest=%d" % emst)
    check("S6 min_offset <= max_offset", lo <= hi, "min=%d max=%d" % (lo, hi))


def scenario_send_back(consumer: DefaultMQPullConsumer, sample: MessageExt,
                       expect_body: bytes) -> None:
    """S7 回投。

    ⚠ 必须复用**已跑过 S1–S6 的那个 consumer**：send_message_back 要用
    `broker_addr_of(msg.broker_name)` 反查 broker 地址，而该表来自路由表——
    新起的 consumer 路由表是空的，会报 "broker broker-a not found"。
    这与 Java 一致（Java 也走 findBrokerAddressInPublish 读 brokerAddrTable）。
    """
    print("\nS7 send_message_back -> %%RETRY%%group 可拉取")
    try:
        # delayLevel=0 时 broker 改写成 3 + reconsumeTimes（默认 10s 延迟）
        consumer.send_message_back(sample, 0)
        check("S7 回投请求被 broker 接受", True,
              "offset=%d body=%s" % (sample.commit_log_offset, bytes(sample.body)))
    except BaseException as e:  # noqa: BLE001
        check("S7 回投请求被 broker 接受", False, "%s: %s" % (type(e).__name__, e))
        return

    # 等延迟消息落地后拉 %RETRY%group
    found = False
    detail = "not tried"
    deadline = time.time() + 40
    while time.time() < deadline and not found:
        try:
            retry_queues = consumer.fetch_subscribe_message_queues(RETRY_TOPIC)
        except BaseException as e:  # noqa: BLE001
            detail = "retry topic not routable yet: %s" % e
            time.sleep(1)
            continue
        for rq in retry_queues:
            try:
                rlo = consumer.min_offset(rq)
                rhi = consumer.max_offset(rq)
            except BaseException as e:  # noqa: BLE001
                detail = "%s: %s" % (type(e).__name__, e)
                continue
            if rhi <= rlo:
                continue
            r, rerr = safe_pull(consumer, rq, "*", rlo)
            if r is None:
                detail = rerr
                continue
            for m in r.msg_found_list:
                if bytes(m.body) == expect_body:
                    found = True
                    detail = ("queue=%d reconsumeTimes=%d body=%s"
                              % (rq.queue_id, m.get_reconsume_times(), bytes(m.body)))
                    break
            if found:
                break
        if not found:
            time.sleep(1)
    check("S7 %%RETRY%% 拉到了被回投的消息", found, detail)


def main() -> int:
    print("拉模式消费者真机验证 topic=%s group=%s" % (TOPIC, GROUP))
    prepare_topic()

    consumer = make_consumer()
    try:
        routes = scenario_queues(consumer)
        if len(routes) != QUEUE_NUM:
            print("!! 队列数不对，后续场景跳过")
            return 1

        sent = scenario_produce()
        lo, hi = wait_offsets(consumer, routes, N_MSG)
        for q in routes:
            check("S2 队列 %s:%d 有 %d 条" % (q.broker_name, q.queue_id, PER_QUEUE),
                  hi[q] - lo[q] == PER_QUEUE, "min=%d max=%d" % (lo[q], hi[q]))

        got = scenario_pull(consumer, routes, lo, hi, set(sent))
        if not got:
            print("!! 一条都没拉到，后续场景跳过")
            return 1

        q0 = routes[0]
        committed = scenario_commit(consumer, q0, hi[q0])
        scenario_own_offset(consumer, q0, committed, lo[q0])
        scenario_offset_query(consumer, q0, lo[q0], hi[q0])

        # 回投需要一条真实拉取到的 MessageExt（带 commitLogOffset）
        sample = None
        for q in routes:
            r, _err = safe_pull(consumer, q, "*", lo[q], max_nums=1)
            if r is not None and r.msg_found_list:
                sample = r.msg_found_list[0]
                break
        if sample is None:
            check("S7 取样本消息", False, "no message available")
        else:
            scenario_send_back(consumer, sample, bytes(sample.body))
    finally:
        consumer.shutdown()

    print("\nPullConsumer: PASS=%d FAIL=%d" % (PASS, FAIL))
    return 0 if FAIL == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
