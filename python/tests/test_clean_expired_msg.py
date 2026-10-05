# -*- coding: utf-8 -*-
"""cleanExpiredMsg 挂起逃生口（离线，无集群）。

Java 锚点（5.5.1 逐条核对）：

  * ``ConsumeMessageConcurrentlyService.java:68-88``：并发服务的构造器建
    ``CleanExpireMsgScheduledThread_<group>`` 单线程调度，``start()`` 里
    ``scheduleAtFixedRate(cleanExpireMsg, consumeTimeout, consumeTimeout, MINUTES)``
    —— **initialDelay 与 period 同值**（都是 ``DefaultMQPushConsumer.getConsumeTimeout()``
    分钟，默认 15，见 ``DefaultMQPushConsumer.java:267``）；``shutdown`` 关掉它。
  * ``:192-200``：``cleanExpireMsg()`` 遍历 rebalance 的 processQueueTable，逐个
    ``pq.cleanExpiredMsg(pushConsumer)``。只有并发服务有这条调度（POP 的
    ConsumeMessagePopConcurrentlyService 没有）。
  * ``ProcessQueue.java:75-127``：顺序消费直接返回；``loop = min(size, 16)``；只看
    ``msgTreeMap.firstEntry()``（最小位点）；没盖过 ``CONSUME_START_TIME`` 就停；
    过期判据是**严格大于** ``consumeTimeout * 60 * 1000``；过期就
    ``sendMessageBack(msg, 3)``，成功后再看它**仍是**队首才 ``removeMessage``。
  * ``:366-370``：时间戳在 ConsumeRequest.run 里、正是调 listener **之前**逐条盖
    （每次投递重盖）；POP 并发在 ``ConsumeMessagePopConcurrentlyService.java:379-385``
    同样盖（但 POP 没有清扫）。
  * ``:243-248``：listener 返回后回投未认可消息前先 ``containsMessage`` —— 已被清扫掉
    的条目跳过回投，否则会重复投递。

为什么必须离线锁死：这条清扫是"listener 卡死不返回"时唯一的回收路径。少了它，
一条卡住的消息会让该队列位点永久停在原地，且没有任何异常、日志或超时可见 ——
真机上只能靠"消息发了却永远不来第二次"这种间接现象暴露。清扫本身的窗口（分钟级）
与 16 条上限也只有在离线用假时钟才能在秒级验证。

与 C++/Rust/C# 的同名用例一一对应；真机验证见 ../verify_clean_expired_msg_live.py。
"""
from __future__ import annotations

import threading
import time
from collections import deque

from client.consumer import DefaultMQPushConsumer
from client.consumer_result import (ConsumeConcurrentlyStatus,
                                              MessageListenerOrderly)
from common.message import MessageExt, MessageQueue
from common.message_accessor import MessageAccessor

GROUP = "GID_CleanExpiredUnitTest"
TOPIC = "CleanExpiredUnitTestTopic"
BROKER = "broker-a"


def msg(queue_offset: int, topic: str = TOPIC) -> MessageExt:
    m = MessageExt(topic=topic, body=b"body")
    m.queue_id = 0
    m.broker_name = BROKER
    m.queue_offset = queue_offset
    return m


def stamp(m: MessageExt, ts_ms: int) -> MessageExt:
    MessageAccessor.set_consume_start_timestamp(m, ts_ms)
    return m


class RecordingStop:
    """假的 ``threading.Event``：记下每次 ``wait()`` 的超时，按剧本返回。"""

    def __init__(self, results=()):
        self.results = list(results)
        self.timeouts = []

    def wait(self, timeout=None):
        self.timeouts.append(timeout)
        if self.results:
            return bool(self.results.pop(0))
        return True


class Clock:
    """可控时钟：替换 ``client.consumer`` 模块里的 ``time``。

    清扫的过期判据（``time.time()*1000 - stamp > consumeTimeout*60*1000``）必须能被
    精确推到边界上，否则"严格大于"这条要么测不出来、要么靠 sleep 变得不稳定。
    """

    def __init__(self, now_ms: float = 1_800_000_000_000.0):
        self.now_ms = float(now_ms)

    def time(self) -> float:
        return self.now_ms / 1000.0


class Harness:
    """一个不碰网络的消费者：队列/在途登记手动搭好，回投只记录。"""

    def __init__(self, consume_timeout=1, orderly=False):
        self.c = DefaultMQPushConsumer(GROUP)
        self.c.consume_timeout = consume_timeout
        self.c._started = True
        self.mq = MessageQueue(TOPIC, BROKER, 0)
        self.key = self.c._mq_key(self.mq)
        self.c._mq_map[self.key] = self.mq
        self.c._pending[self.key] = deque()
        self.c._consume_offsets[self.key] = 0
        self.backed = []
        self.fail_offsets = set()
        if orderly:
            class _Orderly(MessageListenerOrderly):
                def consume_message(self, msgs, context):
                    return None
            self.c.message_listener = _Orderly()

        def _back(m, delay_level, broker_name=None):
            if m.queue_offset in self.fail_offsets:
                raise RuntimeError("simulated send-back failure")
            self.backed.append((m, delay_level))

        self.c.send_message_back = _back  # type: ignore[assignment]

    @property
    def timeout_ms(self):
        return self.c.consume_timeout * 60 * 1000

    def inflight(self, msgs):
        self.c._inflight_msgs.setdefault(self.key, []).extend(msgs)
        return msgs

    def buffered(self, msgs):
        self.c._pending[self.key].extend(msgs)
        return msgs

    def sweep(self):
        self.c._clean_expired_queue(self.key)

    @property
    def backed_offsets(self):
        return sorted(m.queue_offset for m, _ in self.backed)

    @property
    def record_offsets(self):
        return sorted(m.queue_offset for m in self.c._process_queue_entries_locked(self.key))


# ---------------------------------------------------------------- 调度节奏
def test_sweep_schedule_initial_delay_equals_period(monkeypatch):
    """Java scheduleAtFixedRate(..., consumeTimeout, consumeTimeout, MINUTES)：
    initialDelay == period == consumeTimeout 分钟（默认 15 → 900s）。"""
    c = DefaultMQPushConsumer(GROUP)
    c._started = True
    swept = []
    c._clean_expired_msg_once = lambda: swept.append(True)  # type: ignore[method-assign]
    stop = RecordingStop([False])
    c._stop = stop  # type: ignore[assignment]
    c._clean_expire_loop()
    assert stop.timeouts == [900.0, 900.0]   # 首轮也要等满一个周期
    assert len(swept) == 1

    c2 = DefaultMQPushConsumer(GROUP + "_fast")
    c2._started = True
    c2.consume_timeout = 2
    swept2 = []
    c2._clean_expired_msg_once = lambda: swept2.append(True)  # type: ignore[method-assign]
    stop2 = RecordingStop([False, False])
    c2._stop = stop2  # type: ignore[assignment]
    c2._clean_expire_loop()
    assert stop2.timeouts == [120.0, 120.0, 120.0]
    assert len(swept2) == 2


def test_sweep_loop_exits_when_stopped_before_first_tick():
    """shutdown 期间停住：首轮等待就被唤醒，不打任何清扫。"""
    c = DefaultMQPushConsumer(GROUP + "_stopped")
    c._started = True
    swept = []
    c._clean_expired_msg_once = lambda: swept.append(True)  # type: ignore[method-assign]
    stop = RecordingStop([True])
    c._stop = stop  # type: ignore[assignment]
    c._clean_expire_loop()
    assert stop.timeouts == [900.0]
    assert swept == []


def test_sweep_loop_survives_a_failing_pass():
    """Java 的调度壳子 catch (Throwable)：单轮出错不能打死调度（:77-81）。"""
    c = DefaultMQPushConsumer(GROUP + "_resilient")
    c._started = True
    calls = []

    def _boom():
        calls.append(True)
        raise RuntimeError("sweep blew up")

    c._clean_expired_msg_once = _boom  # type: ignore[method-assign]
    stop = RecordingStop([False, False])
    c._stop = stop  # type: ignore[assignment]
    c._clean_expire_loop()
    assert len(calls) == 2, "首轮抛异常后必须继续下一轮"


def test_sweep_thread_is_daemon_and_stops_with_the_flag():
    c = DefaultMQPushConsumer(GROUP + "_thread")
    c._started = True
    c.consume_timeout = 1
    c._start_clean_expire_loop()
    t = c._clean_expire_thread
    assert t is not None and t.is_alive() and t.daemon
    c._stop.set()
    t.join(timeout=3)
    assert not t.is_alive()


# ---------------------------------------------------------------- 核心清扫语义
def test_expired_first_entry_is_sent_back_at_delay_3_then_removed(monkeypatch):
    h = Harness()
    clock = Clock()
    monkeypatch.setattr("client.consumer.time", clock)
    m = stamp(msg(0), int(clock.now_ms) - h.timeout_ms - 1)
    h.inflight([m])

    h.sweep()

    assert h.backed == [(m, 3)], "必须用固定 delayLevel 3 回投（Java:103）"
    assert h.record_offsets == [], "回投成功且仍是队首 → 摘除（Java:106-115）"

    h.sweep()
    assert len(h.backed) == 1, "已摘除的不会在下一轮再被看见"


def test_unstamped_first_entry_blocks_the_whole_pass(monkeypatch):
    """只看队首：队首没盖过章（还没进过 listener）就停，后面过期也轮不到（Java:87-90）。"""
    h = Harness()
    clock = Clock()
    monkeypatch.setattr("client.consumer.time", clock)
    head = msg(0)                                   # 未盖章
    tail = stamp(msg(1), int(clock.now_ms) - h.timeout_ms - 1)
    h.inflight([head, tail])

    h.sweep()

    assert h.backed == []
    assert h.record_offsets == [0, 1]


def test_expiry_is_strictly_greater_than_timeout(monkeypatch):
    """等于阈值不算过期，多 1ms 才算（Java:89 的 ``>``）。"""
    h = Harness()
    clock = Clock()
    monkeypatch.setattr("client.consumer.time", clock)
    boundary = stamp(msg(0), int(clock.now_ms) - h.timeout_ms)
    h.inflight([boundary])

    h.sweep()
    assert h.backed == [], "恰好等于阈值不许动手"

    clock.now_ms += 1
    h.sweep()
    assert h.backed_offsets == [0], "超过阈值 1ms 就该回投"


def test_orderly_consumer_is_never_swept():
    """Java ProcessQueue.cleanExpiredMsg:76-78：顺序消费直接返回（回投会乱序）。"""
    h = Harness(orderly=True)
    m = stamp(msg(0), int(time.time() * 1000) - 3600_000)
    h.inflight([m])

    h.sweep()

    assert h.backed == []
    assert h.record_offsets == [0]


def test_at_most_sixteen_messages_per_pass():
    """Java:80 的 ``min(size, 16)``：一轮最多 16 条，剩下的下一轮继续。"""
    h = Harness()
    old = int(time.time() * 1000) - h.timeout_ms - 1
    h.inflight([stamp(msg(i), old) for i in range(20)])

    h.sweep()
    assert h.backed_offsets == list(range(16))
    assert h.record_offsets == list(range(16, 20))

    h.sweep()
    assert h.backed_offsets == list(range(20))
    assert h.record_offsets == []


def test_failed_send_back_keeps_the_message_for_the_next_pass():
    """Java:122-125：回投失败只记日志、绝不摘除（摘了就真丢了）。"""
    h = Harness()
    old = int(time.time() * 1000) - h.timeout_ms - 1
    m = stamp(msg(0), old)
    h.inflight([m])
    h.fail_offsets = {0}

    h.sweep()
    assert h.backed == []
    assert h.record_offsets == [0], "回投失败的消息必须留在原地等下一轮"

    h.fail_offsets = set()
    h.sweep()
    assert h.backed_offsets == [0]
    assert h.record_offsets == []


def test_sweep_only_looks_at_messages_registered_for_held_queues(monkeypatch):
    """清扫范围 = 当前持有的队列（Java 遍历 processQueueTable）：撤销过的队列不再被扫。"""
    h = Harness()
    clock = Clock()
    monkeypatch.setattr("client.consumer.time", clock)
    m = stamp(msg(0), int(clock.now_ms) - h.timeout_ms - 1)
    h.inflight([m])

    h.c._retire_queue_locked(h.key, None, [])
    h.c._clean_expired_msg_once()

    assert h.backed == [], "队列已撤（登记一并清空）就不该再回投它上面的消息"


# ---------------------------------------------------------------- 与消费路径的配合
def test_concurrent_batch_registers_inflight_and_send_back_proceeds(monkeypatch):
    """对照腿：消息还在登记里（没被清扫）时，未认可的回投照常发生。"""
    h = Harness()
    monkeypatch.setattr("client.consumer.time", Clock())

    class Listener:
        def consume_message(self, msgs, context):
            assert h.c._inflight_msgs[h.key], "listener 运行时这批必须已登记在途"
            return ConsumeConcurrentlyStatus.RECONSUME_LATER

    h.c.message_listener = Listener()
    m = msg(0)
    assert h.c._consume_batch(h.key, h.mq, [m]) is True
    assert h.backed == [(m, 3)]
    assert h.c._inflight_msgs.get(h.key) is None, "收尾后必须摘干净"
    assert h.c._consume_offsets[h.key] == 1


def test_send_back_skips_a_message_the_sweep_already_reclaimed(monkeypatch):
    """Java:243-248 的 containsMessage 闸门：listener 挂着期间被清扫回投的消息，
    listener 事后返回 RECONSUME_LATER 时**不能再回投一次**。"""
    h = Harness()
    clock = Clock()
    monkeypatch.setattr("client.consumer.time", clock)
    swept = []

    class Listener:
        def consume_message(self, msgs, context):
            # 模拟"listener 挂着不返回、清扫先动手"：把时钟推过阈值再触发清扫
            clock.now_ms += h.timeout_ms + 1
            h.c._clean_expired_queue(h.key)
            swept.append(h.backed_offsets)
            return ConsumeConcurrentlyStatus.RECONSUME_LATER

    h.c.message_listener = Listener()
    m = msg(0)
    assert h.c._consume_batch(h.key, h.mq, [m]) is True
    assert swept == [[0]], "listener 期间清扫必须已把它回投"
    assert h.backed == [(m, 3)], "整条链路上只允许一次回投"
    # Java removeMessage(msgs) 里含这条已摘除的消息：位点照样推到它后面
    assert h.c._consume_offsets[h.key] == 1
    assert h.c._inflight_msgs.get(h.key) is None


def test_sweep_can_reclaim_a_requeued_buffered_message(monkeypatch):
    """回塞到本地缓冲的批次保留旧时间戳（Java makeMessageToConsumeAgain 同理），
    队首又是它时清扫照样能回收，摘除要落到缓冲上。"""
    h = Harness()
    clock = Clock()
    monkeypatch.setattr("client.consumer.time", clock)
    m = stamp(msg(0), int(clock.now_ms) - h.timeout_ms - 1)
    h.buffered([m])

    h.sweep()

    assert h.backed == [(m, 3)]
    assert list(h.c._pending[h.key]) == []


def test_dispatch_loop_registers_inflight_and_deregisters_on_return():
    """真分发循环的登记/摘除：listener 挂着时消息可见，返回后登记清空、位点前进。"""
    c = DefaultMQPushConsumer(GROUP + "_dispatch")
    c._started = True
    mq = MessageQueue(TOPIC, BROKER, 0)
    key = c._mq_key(mq)
    c._mq_map[key] = mq
    c._pending[key] = deque([msg(0)])
    backed = []
    c.send_message_back = lambda m, delay_level, broker_name=None: backed.append(m)  # type: ignore[assignment]
    inside = threading.Event()
    release = threading.Event()

    class Listener:
        def consume_message(self, msgs, context):
            inside.set()
            release.wait(5)
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS

    c.message_listener = Listener()
    t = threading.Thread(target=c._dispatch_loop, daemon=True)
    t.start()
    try:
        assert inside.wait(5), "listener 必须在分发线程里跑起来"
        assert [m.queue_offset for m in c._inflight_msgs.get(key) or []] == [0]
    finally:
        release.set()
        deadline = time.time() + 5
        while time.time() < deadline and c._inflight_msgs.get(key):
            time.sleep(0.02)
        c._stop.set()
        t.join(timeout=5)
    assert c._inflight_msgs.get(key) is None
    assert c._consume_offsets[key] == 1
    assert backed == []
