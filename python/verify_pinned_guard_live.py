#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""定点发送 topic 一致性守卫真机验证（Java DefaultMQProducerImpl:1234-1236 / :1277-1278）。

用法（需本地 RocketMQ 5.5.1 集群）：
    .venv/bin/python verify_pinned_guard_live.py 127.0.0.1:9876

离线假集群证明的是「拒了、且报文没上线」；真机这一趟要证明的是**另一面**：
  * 守卫不能误伤真业务 —— 从真实路由取出的队列（broker 名与队列号都是集群给的）
    在同步单条/同步批量/异步单条/异步批量四条入口上照常 SEND_OK，消息一条不少地被消费到；
  * 拒的时候要**守在本端** —— 亚毫秒、无 broker 码，而且**broker 上的 maxOffset 一动不动**：
    这是真机版的 wire 反证（拒绝不会留下任何痕迹，也不会事后偷发）；
  * 命名空间腿看 Java ``queueWithNamespace`` 的幂等：``ns%topic`` 与裸 topic 都不误拒，
    ``ns2%topic`` 才拒；
  * 单向定点**没有**守卫（Java ``:1303-1310`` 有意留的口子）：报文按 msg 自己的 topic
    落库 —— 在真机上用「A 收到、B 的 maxOffset 还是 0」把这条语义钉死。

场景：
  S1 正腿：真实路由队列上的同步单条/批量定点发送 SEND_OK，且落在指定队列上
  S2 反腿：同步单条/批量 topic 不符 ⇒ 本端亚毫秒拒（Java 原文案、无 broker 码），
         broker 的 maxOffset 不动
  S3 命名空间：ns%topic 与裸 topic 都放行（且真落库）；ns2%topic 拒（对照腿）
  S4 异步：单条/批量都在回调里拿到**异步那处文案**；放行腿 SEND_OK
  S5 单向：无守卫，msg 自己的 topic 说了算（A 收到、B 的 maxOffset 保持 0）
  S6 收尾：push 消费者把前面所有正腿消息一条不少地收齐
"""
from __future__ import annotations

import sys
import time

sys.path.insert(0, ".")

from client.consumer import DefaultMQPushConsumer, SimpleMessageListener
from client.consumer_result import ConsumeConcurrentlyStatus
from client.exception import MQClientException
from client.producer import DefaultMQProducer, SendCallbackImpl
from client.send_result import SendStatus
from common.message import Message, MessageQueue
from remoting.protocol.heartbeat import ConsumeFromWhere

NAMESRV = sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1:9876"
STAMP = int(time.time() * 1000)
TOPIC = "PinnedGuardLive_%d" % STAMP
OTHER = "PinnedGuardLiveOther_%d" % STAMP
GROUP = "GID_PinnedGuardLive_%d" % STAMP
NS = "ns1"
WTOPIC = "%s%%%s" % (NS, TOPIC)
# 反腿的耗时上界：守卫是纯字符串比较，真机给 50ms 已留两个数量级余量
LOCAL_BUDGET_MS = 50.0
SYNC_WORDING = "message's topic not equal mq's topic"
ASYNC_WORDING = "Topic of the message does not match its target message queue"

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


def wait_route(prod: DefaultMQProducer, topic: str, min_queues: int,
               timeout: int = 20) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            if len(prod.fetch_publish_message_queues(topic)) >= min_queues:
                return True
        except MQClientException:
            pass
        time.sleep(0.5)
    return False


def wait_until(fn, timeout: float = 10.0, step: float = 0.05) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if fn():
            return True
        time.sleep(step)
    return fn()


def wait_offset(prod: DefaultMQProducer, mq: MessageQueue, want: int,
                timeout: float = 5.0) -> int:
    """等 maxOffset 涨到 want 再返回（broker 的 ConsumeQueue 派发比 SEND 回包慢几毫秒）。"""
    wait_until(lambda: prod.max_offset(mq) >= want, timeout)
    return prod.max_offset(mq)


def queue_zero(prod: DefaultMQProducer, topic: str) -> MessageQueue:
    for q in prod.fetch_publish_message_queues(topic):
        if q.queue_id == 0:
            return q
    raise AssertionError("topic %s has no queue 0" % topic)


def timed_send(prod: DefaultMQProducer, msg, mq: MessageQueue):
    """跑一次定点同步发送，返回 (异常或 None, 耗时毫秒)。"""
    began = time.monotonic()
    try:
        prod.send(msg, 3000, mq)
        return None, (time.monotonic() - began) * 1000.0
    except MQClientException as e:
        return e, (time.monotonic() - began) * 1000.0


def main() -> int:
    print("namesrv=%s topic=%s other=%s" % (NAMESRV, TOPIC, OTHER))
    prod = DefaultMQProducer("PG_PinnedGuardLive_%d" % STAMP)
    prod.set_namesrv_addr(NAMESRV)
    prod.start()
    nsprod = None
    push = None
    try:
        prod.create_topic("TBW102", TOPIC, 4)
        prod.create_topic("TBW102", OTHER, 4)
        if not (wait_route(prod, TOPIC, 4) and wait_route(prod, OTHER, 4)):
            check("S0 两条 topic 的路由都注册好了", False)
            return 1
        mq0 = queue_zero(prod, TOPIC)
        broker = mq0.broker_name
        # 反腿的目标队列：**真**队列（同 broker 上的另一条 topic queue 0）。
        # 不存在的 broker/队列会先死在路由上，就分不清拒的是 topic 还是地址了。
        mq_other = queue_zero(prod, OTHER)

        # 消费者先起：默认 CONSUME_FROM_FIRST_OFFSET，免得和发送抢 rebalance 的时间点
        received = []
        push = DefaultMQPushConsumer(GROUP)
        push.set_namesrv_addr(NAMESRV)
        push.subscribe(TOPIC, "*")
        push.set_consume_from_where(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET)

        def _collect(msgs):
            received.extend(m.get_keys() for m in msgs)
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS

        push.set_message_listener(SimpleMessageListener(_collect))
        push.start()

        # ---------------- S1 正腿：真路由队列上的定点发送 ----------------
        print("=== S1 真实路由队列上的定点发送 ===")
        k_single = "pinned-live-single"
        r = prod.send(Message(TOPIC, b"s1-single", keys=k_single), 3000, mq0)
        check("S1a 同步单条定点 SEND_OK 且落在队列 %d" % mq0.queue_id,
              r.send_status == SendStatus.SEND_OK
              and r.message_queue is not None
              and r.message_queue.queue_id == mq0.queue_id
              and r.message_queue.topic == TOPIC,
              "status=%s mq=%s" % (r.send_status, r.message_queue))
        k_b1, k_b2 = "pinned-live-b1", "pinned-live-b2"
        rb = prod.send([Message(TOPIC, b"s1-b1", keys=k_b1),
                        Message(TOPIC, b"s1-b2", keys=k_b2)], 3000, mq0)
        check("S1b 同步批量定点 SEND_OK 且同一队列",
              rb.send_status == SendStatus.SEND_OK
              and rb.message_queue is not None
              and rb.message_queue.queue_id == mq0.queue_id,
              "status=%s mq=%s" % (rb.send_status, rb.message_queue))
        off_after_s1 = wait_offset(prod, mq0, 3)
        # 批量在消费队列上按**子消息**逐条落位（broker 收到 inner-batch 后拆开写）⇒ 2 条子消息涨 2
        check("S1c 三笔子消息都真落库（maxOffset = 单条 1 + 批量子消息 2）",
              off_after_s1 == 3, "maxOffset=%d" % off_after_s1)

        # ---------------- S2 反腿：同步拒绝，本端、无痕 ----------------
        print("=== S2 topic 不符：本端亚毫秒拒，broker 上无痕 ===")
        e, elapsed = timed_send(prod, Message(TOPIC, b"refused", keys="pinned-live-refused"),
                                mq_other)
        check("S2a 同步单条拒（Java 原文案）",
              e is not None and str(e) == SYNC_WORDING,
              "%.2fms %s" % (elapsed, str(e) if e else "没有抛异常"))
        check("S2b 拒在本端：亚毫秒且无 broker 码（不是超时/不是 broker remark）",
              e is not None and elapsed < LOCAL_BUDGET_MS and e.response_code is None,
              "%.2fms code=%r" % (elapsed, None if e is None else e.response_code))
        e2, elapsed2 = timed_send(prod, [Message(TOPIC, b"r1"), Message(TOPIC, b"r2")],
                                  mq_other)
        check("S2c 同步批量共用同一处守卫与同一句文案",
              e2 is not None and str(e2) == SYNC_WORDING and elapsed2 < LOCAL_BUDGET_MS,
              "%.2fms %s" % (elapsed2, str(e2) if e2 else "没有抛异常"))
        check("S2d wire 反证：A 的 maxOffset 一动没动，B 上一条都没有",
              prod.max_offset(mq0) == off_after_s1 and prod.max_offset(mq_other) == 0,
              "A=%d B=%d" % (prod.max_offset(mq0), prod.max_offset(mq_other)))

        # ---------------- S3 命名空间：wrap 幂等，只拒真的不同名 ----------------
        print("=== S3 命名空间下的比较（ns%%topic 与裸 topic 都不误拒）===")
        nsprod = DefaultMQProducer("PG_PinnedGuardLive_%d_ns" % STAMP, namespace=NS)
        nsprod.set_namesrv_addr(NAMESRV)
        nsprod.start()
        nsprod.create_topic("TBW102", WTOPIC, 4)
        check("S3a 带前缀 topic 的路由可用", wait_route(nsprod, WTOPIC, 4), "topic=%s" % WTOPIC)
        mqw = queue_zero(nsprod, WTOPIC)
        k_ns1, k_ns2 = "pinned-live-ns-q", "pinned-live-ns-m"
        r1 = nsprod.send(Message(TOPIC, b"ns-wrapped-queue", keys=k_ns1), 3000, mqw)
        check("S3b 队列 topic 已带 ns 前缀：wrap 幂等，不误拒",
              r1.send_status == SendStatus.SEND_OK, "status=%s" % r1.send_status)
        r2 = nsprod.send(Message(WTOPIC, b"ns-wrapped-message", keys=k_ns2), 3000, mqw)
        check("S3c 消息 topic 自己已带前缀同样放行",
              r2.send_status == SendStatus.SEND_OK, "status=%s" % r2.send_status)
        e3, elapsed3 = timed_send(nsprod, Message(TOPIC, b"ns2-refused"),
                                  MessageQueue("ns2%%%s" % TOPIC, mqw.broker_name, 0))
        check("S3d 换成 ns2%% 前缀才拒（对照腿：拒的是名字，不是「有前缀」）",
              e3 is not None and str(e3) == SYNC_WORDING and elapsed3 < LOCAL_BUDGET_MS,
              "%.2fms %s" % (elapsed3, str(e3) if e3 else "没有抛异常"))
        check("S3e 两条放行腿真落进 ns1%%topic（maxOffset=2）",
              wait_offset(nsprod, mqw, 2) == 2, "maxOffset=%d" % nsprod.max_offset(mqw))

        # ---------------- S4 异步：回调里是异步那处文案 ----------------
        print("=== S4 异步单条/批量：拒绝走回调，放行走内核 ===")
        events = []

        def _cb(tag):
            return SendCallbackImpl(
                success_fn=lambda r, t=tag: events.append(("ok", t, r)),
                exception_fn=lambda e, t=tag: events.append(("err", t, e)))

        def event_of(tag):
            for kind, t, payload in events:
                if t == tag:
                    return kind, payload
            return None, None

        prod.send_async(Message(TOPIC, b"async-refused", keys="pinned-live-async-refused"),
                        _cb("single-refused"), 3000, mq_other)
        ok_ev = wait_until(lambda: event_of("single-refused")[0] is not None, 10)
        kind, payload = event_of("single-refused")
        check("S4a 单条异步拒绝走回调、文案是异步那处",
              ok_ev and kind == "err" and str(payload) == ASYNC_WORDING,
              "%s %s" % (kind, payload))
        check("S4b 拒后 maxOffset 仍不动（异步也没偷发）",
              prod.max_offset(mq0) == off_after_s1, "maxOffset=%d" % prod.max_offset(mq0))
        prod.send_async([Message(TOPIC, b"abr1"), Message(TOPIC, b"abr2")],
                        _cb("batch-refused"), 3000, mq_other)
        wait_until(lambda: event_of("batch-refused")[0] is not None, 10)
        kind_b, payload_b = event_of("batch-refused")
        check("S4c 批量异步共用同一处文案与同一条拒绝路径",
              kind_b == "err" and str(payload_b) == ASYNC_WORDING,
              "%s %s" % (kind_b, payload_b))
        k_as = "pinned-live-async-single"
        k_ab1, k_ab2 = "pinned-live-async-b1", "pinned-live-async-b2"
        prod.send_async(Message(TOPIC, b"async-ok", keys=k_as), _cb("single-ok"),
                        3000, mq0)
        prod.send_async([Message(TOPIC, b"ab1", keys=k_ab1),
                         Message(TOPIC, b"ab2", keys=k_ab2)], _cb("batch-ok"), 3000, mq0)
        wait_until(lambda: event_of("single-ok")[0] is not None
                   and event_of("batch-ok")[0] is not None, 10)
        ok_s, res_s = event_of("single-ok")
        ok_b, res_b = event_of("batch-ok")
        check("S4d 两条放行腿都 SEND_OK",
              ok_s == "ok" and res_s.send_status == SendStatus.SEND_OK
              and ok_b == "ok" and res_b.send_status == SendStatus.SEND_OK,
              "single=%s batch=%s" % (res_s, res_b))
        off_after_async = wait_offset(prod, mq0, off_after_s1 + 3)
        check("S4e 异步放行腿同样真落库（单条 1 + 批量子消息 2，maxOffset 再涨 3）",
              off_after_async == off_after_s1 + 3, "maxOffset=%d" % off_after_async)

        # ---------------- S5 单向：Java 有意没有守卫 ----------------
        print("=== S5 单向定点没有守卫：msg 自己的 topic 说了算 ===")
        k_ow = "pinned-live-oneway"
        prod.send_oneway(Message(TOPIC, b"oneway", keys=k_ow), mq_other)
        landed = wait_until(lambda: prod.max_offset(mq0) == off_after_async + 1, 10)
        check("S5a 单向定点没有守卫：报文按 msg 自己的 topic 落进 A",
              landed, "A maxOffset=%d" % prod.max_offset(mq0))
        check("S5b 目标队列所在的 B 一条都没有（是 Java 的口子，不是漏发）",
              prod.max_offset(mq_other) == 0, "B maxOffset=%d" % prod.max_offset(mq_other))

        # ---------------- S6 正腿收尾：消息一条不少 ----------------
        print("=== S6 push 消费者收齐正腿消息 ===")
        expected = [k_single, k_b1, k_b2, k_as, k_ab1, k_ab2, k_ow]
        deadline = time.time() + 40
        while time.time() < deadline and not all(k in received for k in expected):
            time.sleep(0.5)
        missing = [k for k in expected if k not in received]
        check("S6 正腿消息一条不少地被消费到", not missing,
              "received=%s missing=%s" % (received, missing))
    finally:
        for c in (push, nsprod):
            if c is not None:
                try:
                    c.shutdown()
                except Exception:  # noqa: BLE001 - 收尾失败不掩盖主断言
                    pass
        prod.shutdown()

    print("############ PASS=%d FAIL=%d ############" % (PASS, FAIL))
    return 0 if FAIL == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
