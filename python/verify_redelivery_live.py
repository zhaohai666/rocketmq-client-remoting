#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""消费侧补齐真机验证（回投 / 位点持久化 / 顺序锁 / 广播 / 流控）。

用法（需本地 RocketMQ 5.5.1 集群）：
    .venv/bin/python verify_redelivery_live.py 127.0.0.1:9876

场景：
  S1 回投：listener 对 retry-me 首次返回 RECONSUME_LATER → 应经 %RETRY%topic
     （延迟梯度 level3=10s）二次投递，且 RECONSUME_TIMES=1；正常消息只投一次。
  S2 位点持久化：消费 3 条后重启同组消费者 → 旧消息不重复，新消息继续投递。
  S3 顺序消费：orderly listener 消费正常，且 broker 锁（LOCK_BATCH_MQ）生效。
  S4 广播模式：同组两个消费者各自收全所有消息，互不影响。
  S5 流控：阈值 2 + 慢消费 → 触发流控计数 >0，最终消息全部消费。
"""
from __future__ import annotations

import sys
import threading
import time

sys.path.insert(0, ".")

from rocketmq.client.consumer import (ConsumeConcurrentlyStatus,
                                      ConsumeOrderlyStatus,
                                      DefaultMQPushConsumer,
                                      MessageListenerConcurrently,
                                      MessageListenerOrderly,
                                      SimpleMessageListener)
from rocketmq.client.mq_client import MQClientInstance
from rocketmq.client.producer import DefaultMQProducer
from rocketmq.common.message import Message

NAMESRV = sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1:9876"
PREFIX = "GapPy_%d" % int(time.time() * 1000)

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


class Collector:
    """通用收集器：记录 (body, topic, properties, ts)。"""

    def __init__(self, consumer: DefaultMQPushConsumer):
        self.consumer = consumer
        self.records = []
        self._lock = threading.Lock()
        consumer.set_message_listener(SimpleMessageListener(self._on_msg))

    def _on_msg(self, msgs):
        with self._lock:
            for m in msgs:
                self.records.append((bytes(m.body), m.topic,
                                     dict(m.properties or {}), time.time()))
        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS

    def bodies(self):
        with self._lock:
            return [r[0] for r in self.records]


def start_consumer(group: str, topic: str, collector: Collector,
                   model: str = None, thread_nums: int = 1) -> DefaultMQPushConsumer:
    c = collector.consumer
    c.set_namesrv_addr(NAMESRV)
    if model is not None:
        c.set_message_model(model)
    c.set_consume_thread_nums(thread_nums)
    c.subscribe(topic, "*")
    c.start()
    return c


def prepare_topic(topic: str, queues: int = 4) -> None:
    """按真实用法先把 topic 建出来，再启动消费者。

    消费者**不做默认 topic 兜底**（对齐 Java：只有生产者才会用 TBW102 为新 topic 合成
    发布信息）。topic 不存在时消费者拿不到路由 → 不分配队列 → 不消费，而且消费者要等
    下一次 30s 心跳才会被 broker 登记（Java 同样），分配还要再往后。真实环境里 topic
    由管理员或首次发送预先创建，这里显式建出来，避免测出"实现没问题但等超时"的假失败。
    """
    c = MQClientInstance("setup-%d" % int(time.time() * 1000), [NAMESRV])
    c.start()
    try:
        c.create_topic_in_route(topic, queues, queues)
    finally:
        c.shutdown()


def main() -> int:
    producer = DefaultMQProducer(PREFIX + "_pg")
    producer.set_namesrv_addr(NAMESRV)
    producer.start()
    time.sleep(1)

    # ---------- S1 回投 ----------
    topic1 = PREFIX + "_Retry"
    prepare_topic(topic1)
    group1 = PREFIX + "_g1"
    c1 = DefaultMQPushConsumer(group1)
    seen1 = []
    lock1 = threading.Lock()

    class RetryListener(MessageListenerConcurrently):
        def consume_message(self, msgs, context):
            for m in msgs:
                with lock1:
                    seen1.append((bytes(m.body), m.topic, m.get_reconsume_times(), time.time()))
            # retry-me 首次投递失败，重投后成功（重投次数走 MessageExt 线上字段）
            if any(bytes(m.body) == b"retry-me" and m.get_reconsume_times() == 0 for m in msgs):
                return ConsumeConcurrentlyStatus.RECONSUME_LATER
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS

    c1.set_namesrv_addr(NAMESRV)
    c1.set_message_listener(RetryListener())
    c1.subscribe(topic1, "*")
    c1.start()
    time.sleep(3)
    producer.send(Message(topic1, b"retry-me"))
    producer.send(Message(topic1, b"normal-1"))
    print("S1: 已发送，等待回投（延迟梯度 level3≈10s）...")
    # 回投消息落在 %RETRY%group 主题，该主题由 broker 在首次回投时才创建；
    # 消费者要等下一轮 rebalance（对齐 Java 的 20s）才会分配它的队列，故窗口给足。
    time.sleep(30)
    c1.shutdown()
    retry_arrivals = [r for r in seen1 if r[0] == b"retry-me"]
    normal_arrivals = [r for r in seen1 if r[0] == b"normal-1"]
    retry_times_seen = [r[2] for r in retry_arrivals]
    check("S1-retry-me 被投递多次", len(retry_arrivals) >= 2,
          "arrivals=%d" % len(retry_arrivals))
    check("S1-重投来自 %RETRY%/RECONSUME_TIMES",
          any(r[2] >= 1 or r[1].startswith("%RETRY%") for r in retry_arrivals),
          "times=%s" % retry_times_seen)
    if len(retry_arrivals) >= 2:
        gap = retry_arrivals[-1][3] - retry_arrivals[0][3]
        check("S1-回投有延迟梯度(>=8s)", gap >= 8.0, "gap=%.1fs" % gap)
    else:
        check("S1-回投有延迟梯度(>=8s)", False, "不足两次投递")
    check("S1-正常消息只投一次", len(normal_arrivals) == 1,
          "arrivals=%d" % len(normal_arrivals))
    # 重投消息实际存在 %RETRY%group 下；对齐 Java resetRetryAndNamespace 后，
    # listener 看到的 topic 应被还原成业务原始 topic（否则用户按 topic 分支会走错）
    retried = [r for r in retry_arrivals if r[2] >= 1]
    check("S1-重投消息 topic 还原为原始 topic",
          bool(retried) and all(r[1] == topic1 for r in retried),
          "topics=%s" % sorted({r[1] for r in retried}))

    # ---------- S2 位点持久化 ----------
    topic2 = PREFIX + "_Offset"
    prepare_topic(topic2)
    group2 = PREFIX + "_g2"
    c2 = DefaultMQPushConsumer(group2)
    col2 = Collector(c2)
    start_consumer(group2, topic2, col2)
    time.sleep(3)
    old_bodies = [b"persist-%d" % i for i in range(3)]
    for b in old_bodies:
        producer.send(Message(topic2, b))
    time.sleep(8)
    c2.shutdown()  # shutdown 持久化位点
    got_first_round = [b for b in old_bodies if b in set(col2.bodies())]
    check("S2-首轮消费 3 条", len(got_first_round) == 3,
          "got=%d" % len(got_first_round))
    # 重启同组消费者，只发 1 条新消息
    c2b = DefaultMQPushConsumer(group2)
    col2b = Collector(c2b)
    start_consumer(group2, topic2, col2b)
    time.sleep(3)
    producer.send(Message(topic2, b"persist-new"))
    time.sleep(8)
    c2b.shutdown()
    new_seen = b"persist-new" in set(col2b.bodies())
    old_resent = [b for b in old_bodies if b in set(col2b.bodies())]
    check("S2-重启后新消息继续投递", new_seen, "" if new_seen else "未收到")
    check("S2-重启不重复消费旧消息", len(old_resent) == 0,
          "重复=%s" % old_resent)

    # ---------- S3 顺序消费 + broker 锁 ----------
    topic3 = PREFIX + "_Orderly"
    prepare_topic(topic3)
    group3 = PREFIX + "_g3"
    c3 = DefaultMQPushConsumer(group3)
    got3 = []
    lock3 = threading.Lock()

    class OrderlyListener(MessageListenerOrderly):
        def consume_message(self, msgs, context):
            with lock3:
                got3.extend(bytes(m.body) for m in msgs)
            return ConsumeOrderlyStatus.SUCCESS

    c3.set_namesrv_addr(NAMESRV)
    c3.set_message_listener(OrderlyListener())
    c3.subscribe(topic3, "*")
    c3.start()
    time.sleep(5)  # 等 LOCK_BATCH_MQ 首轮生效
    for i in range(4):
        producer.send(Message(topic3, b"orderly-%d" % i))
    time.sleep(8)
    lock_ok_n = len(c3._lock_ok)
    c3.shutdown()
    check("S3-顺序消费收全", len(got3) == 4, "got=%d" % len(got3))
    check("S3-broker 队列锁生效", lock_ok_n > 0, "lockOK=%d" % lock_ok_n)

    # ---------- S4 广播模式 ----------
    topic4 = PREFIX + "_Bc"
    prepare_topic(topic4)
    group4 = PREFIX + "_g4"

    def make_broadcast_consumer(inst_name: str) -> Collector:
        c = DefaultMQPushConsumer(group4)
        c.set_instance_name(inst_name)  # 保证两个消费者 clientId 不同
        col = Collector(c)
        start_consumer(group4, topic4, col, model="BROADCASTING")
        return col

    col4a = make_broadcast_consumer("bc-a")
    col4b = make_broadcast_consumer("bc-b")
    time.sleep(3)
    for i in range(3):
        producer.send(Message(topic4, b"bc-%d" % i))
    time.sleep(8)
    na, nb = len(col4a.bodies()), len(col4b.bodies())
    col4a.consumer.shutdown()
    col4b.consumer.shutdown()
    check("S4-广播消费者 A 收全", na == 3, "got=%d" % na)
    check("S4-广播消费者 B 收全", nb == 3, "got=%d" % nb)

    # ---------- S5 流控 ----------
    topic5 = PREFIX + "_Flow"
    prepare_topic(topic5)
    group5 = PREFIX + "_g5"
    c5 = DefaultMQPushConsumer(group5)
    got5 = []
    lock5 = threading.Lock()

    class SlowListener(MessageListenerConcurrently):
        def consume_message(self, msgs, context):
            time.sleep(0.3)
            with lock5:
                got5.extend(bytes(m.body) for m in msgs)
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS

    c5.set_namesrv_addr(NAMESRV)
    c5.set_message_listener(SlowListener())
    c5.pull_threshold_for_queue = 2
    c5.subscribe(topic5, "*")
    c5.start()
    time.sleep(3)
    for i in range(10):
        producer.send(Message(topic5, b"flow-%d" % i))
    time.sleep(12)
    fc = c5._flow_control_triggered
    c5.shutdown()
    check("S5-慢消费下消息全部到达", len(got5) == 10, "got=%d" % len(got5))
    check("S5-流控触发计数>0", fc > 0, "triggered=%d" % fc)

    # ---------- S6 集群多实例 rebalance（队列分配） ----------
    # 两个同组实例订阅同一 topic：broker 端消费者列表应有 2 个 clientId，
    # 队列按 AllocateMessageQueueAveragely 拆分；每条消息**只被消费一次**。
    topic6 = PREFIX + "_Rebalance"
    group6 = PREFIX + "_g6"
    # 先把 topic 建出来（8 队列）并等路由传播，否则消费者启动时无路由，
    # 分配要等到下一轮 20s rebalance 才稳定（测试会读到中间态）
    created = False
    try:
        setup = MQClientInstance("setup-%d" % int(time.time() * 1000), [NAMESRV])
        setup.start()
        setup.create_topic_in_route(topic6, 8, 8)
        setup.shutdown()
        created = True
    except Exception as e:  # noqa: BLE001
        print("S6: 预建 topic 失败（改用自动创建）: %s" % e)
    ca = DefaultMQPushConsumer(group6)
    ca.set_instance_name("inst-a")
    ca.set_namesrv_addr(NAMESRV)
    rec_a = []
    rec_b = []
    lk6 = threading.Lock()

    def make_listener(bucket):
        class L(MessageListenerConcurrently):
            def consume_message(self, msgs, context):
                with lk6:
                    bucket.extend(bytes(m.body) for m in msgs)
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS

        return L()

    ca.set_message_listener(make_listener(rec_a))
    ca.subscribe(topic6, "*")
    ca.start()

    cb = DefaultMQPushConsumer(group6)
    cb.set_instance_name("inst-b")
    cb.set_namesrv_addr(NAMESRV)
    cb.set_message_listener(make_listener(rec_b))
    cb.subscribe(topic6, "*")
    cb.start()
    # 等分配稳定：交集为空 + 两边都非空 + 主 topic 队列被完整覆盖
    # （最多等 45s，覆盖 20s 的 rebalance 周期；分配集合里还含 %RETRY% 队列，故按主 topic 校验）
    deadline = time.time() + 45
    asg_a = asg_b = 0
    keys_a, keys_b = [], []
    expected_keys = set()
    while time.time() < deadline:
        keys_a = set(ca.assigned_queue_keys())
        keys_b = set(cb.assigned_queue_keys())
        asg_a, asg_b = len(keys_a), len(keys_b)
        expected_keys = {ca._mq_key(mq) for mq in ca._all_queues_of_topic(topic6)}
        covered = expected_keys and (keys_a | keys_b) >= expected_keys
        if asg_a > 0 and asg_b > 0 and not (keys_a & keys_b) and covered:
            break
        time.sleep(2)
    hb_a = ca.heartbeat_count()
    # broker 在组成员变化时沿长连接反向推 40；这是全链路唯一的观测点（反向请求
    # 无法由用例注入），只断言「本端确实收到并处理过」，不比次数。
    notified_a = ca._mq_client.consumer_ids_changed_count
    notified_b = cb._mq_client.consumer_ids_changed_count
    cid_list = ca._require_client().get_consumer_id_list_by_group(topic6, group6) or []
    n6 = 40
    for i in range(n6):
        producer.send(Message(topic6, b"rb-%d" % i))
    time.sleep(15)
    total6 = len(rec_a) + len(rec_b)
    all6 = rec_a + rec_b
    dup6 = len(all6) - len(set(all6))
    covered_n = len((keys_a | keys_b) & expected_keys)
    ca.shutdown()
    cb.shutdown()
    check("S6-消费者已心跳注册", hb_a > 0 and len(cid_list) == 2,
          "heartbeats=%d brokerCids=%d" % (hb_a, len(cid_list)))
    check("S6-队列不重不漏(a=%d,b=%d,交集=%d,覆盖=%d/%d)"
          % (asg_a, asg_b, len(keys_a & keys_b), covered_n, len(expected_keys)),
          asg_a > 0 and asg_b > 0 and not (keys_a & keys_b)
          and expected_keys and (keys_a | keys_b) >= expected_keys)
    check("S6-消息无重复消费", dup6 == 0 and total6 == n6,
          "got=%d/%d dup=%d" % (total6, n6, dup6))
    # 反向推送只有 broker 发得出来，用例无法注入，所以计数是唯一能证明
    # 「实例级 40 处理器跑过」的落点（处理器没注册时这里恒为 0）。
    check("S6-成员变化时收到 broker 的 NOTIFY_CONSUMER_IDS_CHANGED(40)",
          notified_a > 0 and notified_b > 0,
          "a=%d b=%d" % (notified_a, notified_b))

    # ---------- S7 优雅注销 ----------
    # shutdown 时应发 UNREGISTER_CLIENT，broker 端立刻摘除，不必等心跳超时（~120s）。
    # 查询用独立的探针客户端（消费者 shutdown 后其内部客户端也已关闭）。
    probe = MQClientInstance("probe-%d" % int(time.time() * 1000), [NAMESRV])
    probe.start()
    group7 = PREFIX + "_g7"
    qc = DefaultMQPushConsumer(group7)
    qc.set_instance_name("inst-c")
    qc.set_namesrv_addr(NAMESRV)
    qc.set_message_listener(make_listener([]))
    qc.subscribe(topic6, "*")
    qc.start()
    time.sleep(3)
    cid7 = qc.client_id
    list_before = probe.get_consumer_id_list_by_group(topic6, group7) or []
    qc.shutdown()
    time.sleep(2)
    list_after = probe.get_consumer_id_list_by_group(topic6, group7) or []
    probe.shutdown()
    check("S7-shutdown 已注销 clientId",
          cid7 in list_before and cid7 not in list_after,
          "before=%d after=%d" % (len(list_before), len(list_after)))

    # ---------- S8 命名空间隔离 ----------
    # 带 namespace 的客户端把资源名拼成 "<ns>%<topic>" 再发给 broker（Java NamespaceUtil），
    # listener 拿到的 topic 应还原成业务原始 topic；不带 namespace 的客户端读不到该消息。
    ns = PREFIX + "_NS"
    raw_topic = PREFIX + "_NsTopic"
    ns_topic = ns + "%" + raw_topic          # broker 侧真实主题名
    prepare_topic(ns_topic)
    group8 = PREFIX + "_g8"

    p8 = DefaultMQProducer(PREFIX + "_pg8", namespace=ns)
    p8.set_namesrv_addr(NAMESRV)

    n8 = []
    n8_plain = []
    seen8_topics = []
    lk8 = threading.Lock()

    class NsListener(MessageListenerConcurrently):
        def __init__(self, bucket):
            self.bucket = bucket

        def consume_message(self, msgs, context):
            with lk8:
                self.bucket.extend(bytes(m.body) for m in msgs)
                seen8_topics.extend(m.topic for m in msgs)
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS

    c8 = DefaultMQPushConsumer(group8, namespace=ns)
    c8.set_namesrv_addr(NAMESRV)
    c8.set_message_listener(NsListener(n8))
    c8.subscribe(raw_topic, "*")
    c8.start()
    # 反证：不带 namespace 的消费者订阅同名 raw_topic，读的是另一个主题，不该收到
    c8p = DefaultMQPushConsumer(PREFIX + "_g8p")
    c8p.set_namesrv_addr(NAMESRV)
    c8p.set_message_listener(NsListener(n8_plain))
    c8p.subscribe(raw_topic, "*")
    c8p.start()

    # 消费者必须先于发送启动：CONSUME_FROM_LAST_OFFSET 从「消费者启动时刻」的最新位点开始
    # （Java 同样），先发后起会把消息跳过。等分配就绪（位点已在分配时解析）再发。
    time.sleep(5)
    p8.start()
    p8.send(Message(raw_topic, b"ns-1"))

    deadline = time.time() + 25
    while time.time() < deadline and not any(b == b"ns-1" for b in n8):
        time.sleep(1)
    time.sleep(3)
    c8.shutdown()
    c8p.shutdown()
    p8.shutdown()
    check("S8-命名空间消费者收到消息", any(b == b"ns-1" for b in n8),
          "got=%s" % [b.decode() for b in n8])
    check("S8-无命名空间消费者收不到(隔离)", not any(b == b"ns-1" for b in n8_plain),
          "got=%s" % [b.decode() for b in n8_plain])
    check("S8-topic 还原为业务原始名", bool(seen8_topics) and all(t == raw_topic for t in seen8_topics),
          "topics=%s" % sorted(set(seen8_topics)))

    producer.shutdown()
    print("\nPASS=%d FAIL=%d" % (PASS, FAIL))
    return 0 if FAIL == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
