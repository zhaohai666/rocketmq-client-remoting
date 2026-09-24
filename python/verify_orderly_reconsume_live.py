#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""顺序消费的重投闸门真机验证（Java ConsumeMessageOrderlyService#checkReconsumeTimes）。

用法（需本地 RocketMQ 5.5.1 集群）：
    .venv/bin/python verify_orderly_reconsume_live.py 127.0.0.1:9876

为什么必须真机：闸门本身（本地 reconsumeTimes 计数 + 到点交给 broker）离线能测，
但「交给 broker 之后发生了什么」离线一点都测不出来 —— 未 start 的消费者压根没有内部
生产者，回投必定失败，于是只锁得住「失败分支」。而这条链路的三个真实故障全在成功分支
上，并且**全都是静默的**：

  - 抬进请求头：broker 判死信读的是 requestHeader.reconsumeTimes / maxReconsumeTimes
    （SendMessageProcessor#handleRetryAndDLQ:197-210），不看报文属性。没抬就等于
    消费者配的 maxReconsumeTimes 形同虚设。
  - 回投成功后必须前进位点：Java :266-296 这时 commit 位点让毒消息走人。写错就是
    一条毒消息永久占住整条队列，表现和「消费者死了」一模一样。
  - 顺序组的锁没过期 ⇒ broker 直接把这条 %RETRY% 投递改投 %DLQ%
    （handleRetryAndDLQ:202-207 `isLockAllExpired`）。这是顺序消费独有的终态，
    只有真broker 会做。

场景：
  O1 到点交给 broker：maxReconsumeTimes=2 的顺序消费者遇毒消息 ⇒ 本地投递 3 次
     （reconsumeTimes 0/1/2），第 3 次回投成功后业务队列**继续往前**（后面的正常消息
     照样消费），且消息落在 %DLQ%<group>（reconsumeTimes=3、RETRY_TOPIC=业务 topic）。
  O2 -1 是不设限：默认（-1）配置下同一条毒消息投递远超 3 次仍不进 %DLQ%
     （Java OrderlyService#getMaxReconsumeTimes:313-320 把 -1 读成 Integer.MAX_VALUE，
     与并发侧的 16 不是一套）。写错成 16 就会凭空造死信。
  O3 挂起时长：context 上的 ``suspendCurrentQueueTimeMillis`` 优先于消费者配置，且解析
     ``-1`` 回落到配置、结果钳到 [10, 30000]（Java submitConsumeRequestLater:211-234）。
     配置 900ms / context 70ms 时中位间隔必须贴着 70ms（端到端），**并且**分发线程上客户端
     请求的睡眠时长必须正好是 0.07s —— 后者没有计时噪声，钳位的两个边界也照此验证：
     context 1ms / 配置 0 → 请求 0.01s；context 40s → 请求 30s。
     为什么两个判据都要：分发循环在挂起之后还有 50ms 固定轮询节拍，10ms 与 1ms 的差别
     落在间隔上根本分不出来（见 SleepRecorder 的注释）。
  O4 autoCommit=false：SUCCESS 只记 TPS **不提交**（Java processConsumeResult:272-274），
     本端口没有把 ProcessQueue 交给 listener，等价地塞回队首等一个挂起周期 —— 被持有的
     那条要投递多次，后一条在「放行」（listener 把 autoCommit 拨回 true）之前不出现。
     忽略 autoCommit 直接当成功 ack 的话，毒消息第一轮就被吞掉、后一条立刻被消费。
"""
from __future__ import annotations

import sys
import threading
import time

sys.path.insert(0, ".")

from rocketmq.client.consumer import (ConsumeOrderlyStatus,
                                      DefaultLitePullConsumer,
                                      DefaultMQPushConsumer,
                                      MessageListenerOrderly)
from rocketmq.client.mq_client import MQClientInstance
from rocketmq.client.producer import DefaultMQProducer
from rocketmq.common.message import Message
from rocketmq.common.mix_all import MixAll
from rocketmq.remoting.protocol.heartbeat import ConsumeFromWhere

NAMESRV = sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1:9876"
PREFIX = "OrdPy_%d" % int(time.time() * 1000)

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


def median(xs):
    s = sorted(xs)
    n = len(s)
    if not n:
        return 0.0
    return s[n // 2] if n % 2 else (s[n // 2 - 1] + s[n // 2]) / 2.0


class SleepRecorder:
    """记录分发线程上客户端**自己请求**的挂起睡眠时长（真机观测，无计时噪声）。

    为什么要拦 ``time.sleep`` 而不是算投递间隔：分发循环在 ``_consume_batch`` 返回 False
    之后还会固定 sleep 0.05 才回取队首，所以「两次投递的间隔」= 挂起时长 + ~50ms 轮询，
    实测中位数 63ms、抖动 ±10ms —— 挂起 10ms 与 1ms 的差别落不进这个噪声里（这正是上一版
    O3 钳位判据误报的原因）。直接记下客户端请求的时长，钳位是否生效就是确定性事实。

    线程名过滤（``rmq-dispatch-``）只留分发线程，其它线程的 sleep 原样穿过；``cap`` 只用于
    「请求 30s」这种用例，把真实等待压到 20ms —— 断言的是**客户端请求了多久**，与是否真的
    睡满无关（真睡 30s 只会让用例慢 30s）。
    """

    def __init__(self, cap: float = None):
        self.durations = []
        self.cap = cap
        self._real = time.sleep

    def __enter__(self):
        rec = self

        def patched(sec):
            if threading.current_thread().name.startswith("rmq-dispatch-"):
                rec.durations.append(sec)
                if rec.cap is not None and sec > rec.cap:
                    sec = rec.cap
            return rec._real(sec)

        time.sleep = patched
        return self

    def __exit__(self, *exc):
        time.sleep = self._real
        return False

    def asked(self, seconds: float) -> bool:
        """客户端是否请求过整整 ``seconds`` 秒的睡眠。"""
        return any(abs(d - seconds) < 1e-9 for d in self.durations)

    def others(self):
        """除了分发循环固定节拍（0.05 轮询 / 0.1 重试 / 0.2 回投冷却）之外的请求值。"""
        return sorted({round(d, 4) for d in self.durations
                       if all(abs(d - x) > 1e-9 for x in (0.05, 0.1, 0.2))})


def prepare_topic(topic: str, queues: int = 1) -> None:
    """建 topic。队列数固定 1：顺序消费要的是「一条队列里的确定顺序」，多队列时毒消息
    和后面那条正常消息会散到不同队列，占位/放行就看不出差别了。"""
    c = MQClientInstance("ord-setup-%d" % int(time.time() * 1000), [NAMESRV])
    c.start()
    try:
        c.create_topic_in_route(topic, queues, queues)
    finally:
        c.shutdown()


def read_dlq(group: str, timeout: float = 25.0):
    """用 lite pull 从队首读 %DLQ%<group>，返回 (body, reconsumeTimes, topic, RETRY_TOPIC)。

    必须从 FIRST_OFFSET 读：DLQ topic 是 broker 在投死信那一刻才建出来的，新组默认的
    LAST_OFFSET 会从「订阅时刻」的队尾开始，正好把刚进去的那条跳过 ⇒ 假失败。"""
    dlq = MixAll.get_dlq_topic(group)
    c = DefaultLitePullConsumer(PREFIX + "_dlqreader")
    c.set_namesrv_addr(NAMESRV)
    c.set_consume_from_where(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET)
    c.subscribe(dlq, "*")
    c.start()
    out = []
    try:
        deadline = time.time() + timeout
        while time.time() < deadline:
            for m in c.poll(3000) or []:
                out.append((bytes(m.body), m.get_reconsume_times(), m.topic,
                            (m.properties or {}).get("RETRY_TOPIC")))
            if out:
                break
            time.sleep(1)
    finally:
        c.shutdown()
    return dlq, out


class PoisonListener(MessageListenerOrderly):
    """毒消息一直挂起（每次都返回 SUSPEND，永不「成功」），其余照常成功。

    只记 body/reconsumeTimes：顺序消费是单线程逐批的，不需要锁，但消费者重启后
    listener 实例会被复用，这里用列表累积整轮观测。"""

    def __init__(self, poison: bytes):
        self.poison = poison
        self.records = []
        self._lock = threading.Lock()

    def consume_message(self, msgs, context):
        with self._lock:
            for m in msgs:
                self.records.append((bytes(m.body), m.get_reconsume_times(), m.topic))
        if any(bytes(m.body) == self.poison for m in msgs):
            return ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT
        return ConsumeOrderlyStatus.SUCCESS

    def deliveries(self, poison: bytes):
        with self._lock:
            return [r for r in self.records if r[0] == poison]

    def bodies(self):
        with self._lock:
            return [r[0] for r in self.records]


class TimedSuspendListener(MessageListenerOrderly):
    """毒消息永远挂起，并把 context 的挂起时长改成 ``asked_ms``；记录每次投递的时刻。

    判据是**相邻两次投递的间隔**：本端口的挂起就是把这一批塞回队首再 sleep 那个时长，
    所以间隔直接反映解析出来的毫秒数（读错成消费者配置 / 漏掉钳位都会在间隔上露出来）。
    """

    def __init__(self, poison: bytes, asked_ms):
        self.poison = poison
        self.asked_ms = asked_ms
        self.times = []

    def consume_message(self, msgs, context):
        if self.asked_ms is not None:
            context.suspend_current_queue_time_millis = self.asked_ms
        if any(bytes(m.body) == self.poison for m in msgs):
            self.times.append(time.time())
            return ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT
        return ConsumeOrderlyStatus.SUCCESS

    def gaps(self):
        return [b - a for a, b in zip(self.times, self.times[1:])]


class ManualHoldListener(MessageListenerOrderly):
    """头 ``hold_calls`` 次把 autoCommit 关掉（模拟 binlog 消费方自己决定何时提交）。

    状态一直返回 SUCCESS：关着的时候只该记 TPS 不提交（Java:272-274），所以这一条会被
    反复投递；拨回 true 之后下一轮才提交位点、放行后面的消息。
    """

    def __init__(self, hold_calls: int):
        self.hold_calls = hold_calls
        self.calls = 0
        self.records = []          # [(body, auto_commit)]

    def consume_message(self, msgs, context):
        self.calls += 1
        auto = self.calls > self.hold_calls
        context.auto_commit = auto
        for m in msgs:
            self.records.append((bytes(m.body), auto))
        return ConsumeOrderlyStatus.SUCCESS

    def first_index(self, body: bytes):
        for i, r in enumerate(self.records):
            if r[0] == body:
                return i
        return -1


def main() -> int:
    producer = DefaultMQProducer(PREFIX + "_pg")
    producer.set_namesrv_addr(NAMESRV)
    producer.start()
    time.sleep(1)

    # ---------- O1 到点交给 broker，业务队列继续往前 ----------
    topic1 = PREFIX + "_OrdDlq"
    group1 = PREFIX + "_g1"
    prepare_topic(topic1)
    listener1 = PoisonListener(b"ord-poison")
    c1 = DefaultMQPushConsumer(group1)
    c1.set_namesrv_addr(NAMESRV)
    c1.set_message_listener(listener1)
    c1.consume_message_batch_max_size = 1
    c1.max_reconsume_times = 2      # 显式小值：默认不设限，等到天荒地老
    c1.suspend_current_queue_time_millis = 500
    c1.subscribe(topic1, "*")
    c1.start()
    time.sleep(5)   # 等首轮 LOCK_BATCH_MQ，broker 侧才算「该组持有未过期锁」
    sent1 = producer.send(Message(topic1, b"ord-poison"))
    producer.send(Message(topic1, b"ord-after"))
    print("O1: 毒消息已发送 msgId=%s，等待本地计数到点..." % sent1.msg_id)

    # 本地重试只 sleep 500ms/轮（不过 broker），3 次投递约 1.5s；后面那条正常消息要等
    # 位点前进才会被拉到。窗口给 90s 是同机并跑四套真机用例时的竞争余量。
    deadline = time.time() + 90
    while time.time() < deadline and b"ord-after" not in listener1.bodies():
        time.sleep(1)
    time.sleep(3)   # 反证窗口：毒消息不该再被投回来
    poison1 = listener1.deliveries(b"ord-poison")
    times1 = [p[1] for p in poison1]
    check("O1-maxReconsumeTimes=2 时毒消息本地投递 3 次（reconsumeTimes 0/1/2）",
          times1 == [0, 1, 2], "times=%s" % times1)
    check("O1-回投成功后业务队列继续往前（后面的正常消息被消费）",
          b"ord-after" in listener1.bodies(), "bodies=%s" % listener1.bodies())
    check("O1-毒消息不再投回（观察窗口内只有 3 次投递）", len(poison1) == 3,
          "arrivals=%d" % len(poison1))
    check("O1-投递期间 topic 一直是业务 topic（顺序重试不过 %RETRY%）",
          all(p[2] == topic1 for p in poison1),
          "topics=%s" % sorted({p[2] for p in poison1}))
    lock_ok = len(c1._lock_ok)
    check("O1-该组持有 broker 队列锁（死信直投的判据）", lock_ok > 0, "lockOK=%d" % lock_ok)
    c1.shutdown()

    dlq1, got1 = read_dlq(group1)
    check("O1-毒消息落在 %DLQ%<group>", [g[0] for g in got1] == [b"ord-poison"],
          "dlq=%s got=%s" % (dlq1, [(g[0].decode(), g[1]) for g in got1]))
    # 客户端抬进头里的 reconsumeTimes=3（回投时 +1），broker 原样存储
    check("O1-DLQ 消息 reconsumeTimes=3", len(got1) == 1 and got1[0][1] == 3,
          "got=%s" % got1)
    check("O1-DLQ 消息保留 RETRY_TOPIC=业务 topic",
          len(got1) == 1 and got1[0][3] == topic1, "got=%s" % got1)
    check("O1-DLQ 消息 topic 就是 %DLQ%<group>",
          len(got1) == 1 and got1[0][2] == dlq1, "got=%s" % got1)
    print("O1: 原始 msgId=%s，DLQ 观测=%s" % (sent1.msg_id, got1))

    # ---------- O2 -1 在顺序侧是「不设限」 ----------
    # Java 顺序侧 -1 → Integer.MAX_VALUE（OrderlyService#getMaxReconsumeTimes:313-320），
    # 并发侧 -1 → 16（DefaultMQPushConsumerImpl#getMaxReconsumeTimes:890）。并成一个常量
    # 的话，这条用默认配置的正常路径会在第 16 次凭空造出一条死信。
    topic2 = PREFIX + "_OrdUnlimited"
    group2 = PREFIX + "_g2"
    prepare_topic(topic2)
    listener2 = PoisonListener(b"ord-keep")
    c2 = DefaultMQPushConsumer(group2)
    c2.set_namesrv_addr(NAMESRV)
    c2.set_message_listener(listener2)
    c2.consume_message_batch_max_size = 1
    # max_reconsume_times 保持默认 -1
    c2.suspend_current_queue_time_millis = 200
    c2.subscribe(topic2, "*")
    c2.start()
    time.sleep(4)
    producer.send(Message(topic2, b"ord-keep"))
    print("O2: 默认（-1）配置的毒消息已发送，观察是否会被提前判死...")
    deadline = time.time() + 30
    while time.time() < deadline and len(listener2.deliveries(b"ord-keep")) < 25:
        time.sleep(1)
    n2 = len(listener2.deliveries(b"ord-keep"))
    check("O2-默认配置下持续原地重试（远超并发侧的 16）", n2 > 16, "deliveries=%d" % n2)
    check("O2-本地计数一直前进（0..n 连续）",
          [p[1] for p in listener2.deliveries(b"ord-keep")][:20] == list(range(20)),
          "times=%s" % [p[1] for p in listener2.deliveries(b"ord-keep")][:20])
    c2.shutdown()
    dlq2, got2 = read_dlq(group2, timeout=12)
    check("O2-没到阈值就不该有死信（%s 为空）" % dlq2, got2 == [], "got=%s" % got2)

    # ---------- O3 挂起时长：context 优先 + 钳到 [10, 30000] ----------
    # Java submitConsumeRequestLater:211-234：-1（默认）→ 消费者配置；给值就用给的值；
    # 最后钳到 [10, 30000]。配置故意设 900ms：读错成配置时中位间隔会落到 0.9s。
    # 两个判据：间隔（端到端真的等了这么久）+ 客户端请求的时长（精确，不受轮询节拍影响）。
    topic3 = PREFIX + "_OrdSuspendCtx"
    group3 = PREFIX + "_g3"
    prepare_topic(topic3)
    listener3 = TimedSuspendListener(b"ord-slow", asked_ms=70)   # 70 ≠ 轮询节拍 50/100/200
    c3 = DefaultMQPushConsumer(group3)
    c3.set_namesrv_addr(NAMESRV)
    c3.set_message_listener(listener3)
    c3.consume_message_batch_max_size = 1
    c3.max_reconsume_times = -1
    c3.suspend_current_queue_time_millis = 900
    c3.subscribe(topic3, "*")
    rec3 = SleepRecorder()
    with rec3:
        c3.start()
        time.sleep(4)
        producer.send(Message(topic3, b"ord-slow"))
        deadline = time.time() + 25
        while time.time() < deadline and len(listener3.times) < 9:
            time.sleep(0.5)
    gaps3 = listener3.gaps()
    med3 = median(gaps3)
    check("O3-context 上的 70ms 生效（不是消费者配置的 900ms）",
          len(gaps3) >= 6 and 0.03 <= med3 <= 0.4,
          "n=%d median=%.3fs gaps=%s" % (len(gaps3), med3, [round(g, 3) for g in gaps3[:5]]))
    check("O3-客户端请求的就是 context 的 0.07s，不是配置的 0.9s",
          rec3.asked(0.07), "requested=%s" % rec3.others())
    c3.shutdown()

    topic3b = PREFIX + "_OrdSuspendFloor"
    group3b = PREFIX + "_g3b"
    prepare_topic(topic3b)
    listener3b = TimedSuspendListener(b"ord-fast", asked_ms=1)   # 1ms → 钳到 10ms
    c3b = DefaultMQPushConsumer(group3b)
    c3b.set_namesrv_addr(NAMESRV)
    c3b.set_message_listener(listener3b)
    c3b.consume_message_batch_max_size = 1
    c3b.suspend_current_queue_time_millis = 0    # 配置侧非法值，Java 同样钳到 10ms
    c3b.subscribe(topic3b, "*")
    rec3b = SleepRecorder()
    with rec3b:
        c3b.start()
        time.sleep(4)
        producer.send(Message(topic3b, b"ord-fast"))
        deadline = time.time() + 20
        while time.time() < deadline and len(listener3b.times) < 9:
            time.sleep(0.5)
    gaps3b = listener3b.gaps()
    med3b = median(gaps3b)
    check("O3-钳位下限：context 1ms / 配置 0 都按 10ms 走（漏钳就是忙等）",
          rec3b.asked(0.01), "requested=%s" % rec3b.others())
    check("O3-钳位下限真的等了（间隔不可能小于 10ms）",
          len(gaps3b) >= 6 and med3b >= 0.009,
          "n=%d median=%.4fs gaps=%s"
          % (len(gaps3b), med3b, [round(g, 4) for g in gaps3b[:5]]))
    c3b.shutdown()

    # ---------- O3c 钳位上限：请求 40s → 只等 30s ----------
    # 判据同上取客户端请求的时长：真等 30s 只会让用例慢半分钟，这里把真实等待压到 20ms。
    topic3c = PREFIX + "_OrdSuspendCeil"
    group3c = PREFIX + "_g3c"
    prepare_topic(topic3c)
    listener3c = TimedSuspendListener(b"ord-ceil", asked_ms=40000)
    c3c = DefaultMQPushConsumer(group3c)
    c3c.set_namesrv_addr(NAMESRV)
    c3c.set_message_listener(listener3c)
    c3c.consume_message_batch_max_size = 1
    c3c.subscribe(topic3c, "*")
    rec3c = SleepRecorder(cap=0.02)
    with rec3c:
        c3c.start()
        time.sleep(4)
        producer.send(Message(topic3c, b"ord-ceil"))
        deadline = time.time() + 20
        while time.time() < deadline and len(listener3c.times) < 5:
            time.sleep(0.5)
    check("O3-钳位上限：请求 40s 只按 30s 走",
          len(listener3c.times) >= 2 and rec3c.asked(30.0),
          "n=%d requested=%s" % (len(listener3c.times), rec3c.others()))
    c3c.shutdown()

    # ---------- O4 autoCommit=false 时 SUCCESS 不提交（Java:272-274）----------
    # 前 3 次关着 autoCommit：那条消息必须被反复投递（位点不动、也没被 ack）；第 4 次
    # 拨回 true 之后提交位点，后面那条才被消费。忽略 autoCommit 的实现会在第 1 次就
    # 把它 ack 掉，于是「重复投递」和「后一条在前 3 次之内不出现」两条都不成立。
    topic4 = PREFIX + "_OrdManual"
    group4 = PREFIX + "_g4"
    prepare_topic(topic4)
    listener4 = ManualHoldListener(hold_calls=3)
    c4 = DefaultMQPushConsumer(group4)
    c4.set_namesrv_addr(NAMESRV)
    c4.set_message_listener(listener4)
    c4.consume_message_batch_max_size = 1
    c4.suspend_current_queue_time_millis = 200
    c4.subscribe(topic4, "*")
    c4.start()
    time.sleep(4)
    producer.send(Message(topic4, b"ord-hold"))
    producer.send(Message(topic4, b"ord-after-hold"))
    deadline = time.time() + 40
    while time.time() < deadline and listener4.first_index(b"ord-after-hold") < 0:
        time.sleep(0.5)
    held = [r for r in listener4.records if not r[1]]
    after_idx = listener4.first_index(b"ord-after-hold")
    check("O4-autoCommit=false 的 SUCCESS 不回 ack（同一条被反复投递）",
          len(held) >= 2 and all(r[0] == b"ord-hold" for r in held),
          "held=%d bodies=%s" % (len(held), [r[0].decode() for r in listener4.records]))
    check("O4-放行后位点前进：后一条被消费且顺序不乱",
          after_idx >= 0 and after_idx > listener4.first_index(b"ord-hold")
          and listener4.records[after_idx][1] is True,
          "afterIdx=%d auto=%s" % (after_idx,
                                   listener4.records[after_idx][1] if after_idx >= 0 else None))
    c4.shutdown()

    producer.shutdown()
    print("\nPASS=%d FAIL=%d" % (PASS, FAIL))
    return 0 if FAIL == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
