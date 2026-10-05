# -*- coding: utf-8 -*-
"""``correctTagsOffset`` 单测（Java ``DefaultMQPushConsumerImpl:713-717``），不需要集群。

为什么必须离线锁死：这条路径错了是**静默**的两种极端 ——

  - 漏了它：tag 长期不匹配 / 客户端二次过滤把一批全摘掉的队列，broker 侧"已消费位点"
    原地不动，每次重投/重启都把这批没人要的消息从头再扫一遍；
  - 闸门没守住（进程队列里还有消息就抬位点）：崩溃恢复时**静默跳过**在途的那批，消息真丢。

两个方向在真机短期窗口里都看不出差别，所以断言全部放离线。

判据来源：Java 在拉取回调里（``:394-401``）对 ``NO_NEW_MSG`` / ``NO_MATCHED_MSG`` 两态调
``correctTagsOffset``。后者是 broker 侧按订阅表达式过滤后一条没匹配上
（``MQClientAPIImpl:1095-1097`` 把 ``PULL_RETRY_IMMEDIATELY`` 映射成它），前者覆盖
"客户端二次 tag 过滤把一批全摘掉之后，下一轮长轮询空手而归"。闸门是
``0L == processQueue.getMsgCount()``，而 ``msgCount`` 数的是**仍在 ProcessQueue 里**的消息
（在途批次要等 listener 返回之后才 ``removeMessage``，见
``ConsumeMessageConcurrentlyService:266``），所以本端口的「进程队列为空」=
``_pending`` 为空 **且** ``_msg_queue_inflight`` 为 0。
"""
from __future__ import annotations

import threading
from collections import deque

from client.consumer import DefaultMQPushConsumer
from client.consumer_result import ConsumeConcurrentlyStatus, PullStatus
from common.message import MessageExt, MessageQueue

GROUP = "GID_CorrectTagsOffsetUnitTest"
TOPIC = "CorrectTagsOffsetUnitTestTopic"
BROKER = "broker-a"


def msg(queue_offset: int) -> MessageExt:
    m = MessageExt(topic=TOPIC, body=b"body")
    m.queue_id = 0
    m.broker_name = BROKER
    m.queue_offset = queue_offset
    return m


class Harness:
    """一个不碰网络的 push consumer：缓冲/位点/在途计数都手动搭。"""

    def __init__(self):
        self.c = DefaultMQPushConsumer(GROUP)
        self.mq = MessageQueue(TOPIC, BROKER, 0)
        self.key = self.c._mq_key(self.mq)

    def seed_pending(self, offsets) -> None:
        self.c._pending[self.key] = deque(msg(o) for o in offsets)

    def seed_consume_offset(self, off: int) -> None:
        self.c._consume_offsets[self.key] = off

    def seed_inflight(self, n: int) -> None:
        self.c._msg_queue_inflight[self.key] = n

    def correct(self, status, next_off) -> None:
        with self.c._lock:
            self.c._correct_tags_offset_locked(self.key, status, next_off)

    @property
    def offset(self):
        return self.c._consume_offsets.get(self.key)


# ------------------------------------------------- 空应答把位点抬到 nextBeginOffset


class TestAdvancesOnEmptyResponse:
    def test_no_new_msg_advances_when_queue_empty(self):
        h = Harness()
        h.correct(PullStatus.NO_NEW_MSG, 110)
        assert h.offset == 110

    def test_no_matched_msg_advances_when_queue_empty(self):
        # broker 侧过滤把整批都挡掉了（PULL_RETRY_IMMEDIATELY → NO_MATCHED_MSG）：
        # 这些消息永远不会进 _pending，位点只能靠这一步跟上
        h = Harness()
        h.correct(PullStatus.NO_MATCHED_MSG, 110)
        assert h.offset == 110

    def test_advances_from_the_recorded_offset(self):
        h = Harness()
        h.seed_consume_offset(50)
        h.correct(PullStatus.NO_NEW_MSG, 110)
        assert h.offset == 110

    def test_none_next_offset_is_ignored(self):
        # 应答头缺 nextBeginOffset（本端口用 None 表达）：宁可不抬，也不能写个 0 把
        # broker 上已提交的位点打回去
        h = Harness()
        h.seed_consume_offset(50)
        h.correct(PullStatus.NO_NEW_MSG, None)
        assert h.offset == 50


# ------------------------------------------------------------- 只前进，不回退


class TestIncreaseOnly:
    def test_never_regresses(self):
        # Java updateOffset(..., increaseOnly=true)：应答里的 nextBeginOffset 比本地
        # 已记录的还小（位点被别处置动过）时不动
        h = Harness()
        h.seed_consume_offset(200)
        h.correct(PullStatus.NO_NEW_MSG, 110)
        assert h.offset == 200

    def test_equal_offset_is_noop(self):
        h = Harness()
        h.seed_consume_offset(110)
        h.correct(PullStatus.NO_NEW_MSG, 110)
        assert h.offset == 110


# ---------------------------------------------------------- 闸门：进程队列非空


class TestProcessQueueGuard:
    def test_pending_blocks(self):
        # 还有消息躺在缓冲里（Java msgCount > 0）：这批迟早会消费，位点不能越过它
        h = Harness()
        h.seed_pending([7])
        h.correct(PullStatus.NO_NEW_MSG, 110)
        assert h.offset is None

    def test_in_flight_batch_blocks(self):
        # 缓冲已被分发线程取走、listener 还在跑：Java 那边 removeMessage 尚未调用，
        # msgCount 仍 > 0。不等它落定就抬位点 = 崩溃时静默跳过在途消息
        h = Harness()
        h.seed_inflight(1)
        h.correct(PullStatus.NO_MATCHED_MSG, 110)
        assert h.offset is None

    def test_advances_once_queue_drains(self):
        h = Harness()
        h.seed_inflight(1)
        h.correct(PullStatus.NO_NEW_MSG, 110)
        assert h.offset is None
        h.seed_inflight(0)
        h.correct(PullStatus.NO_NEW_MSG, 110)
        assert h.offset == 110


# ------------------------------------------------- 只有这两个状态才修正位点


class TestStatusGate:
    def test_found_is_ignored(self):
        # FOUND 走的是 putMessage + 消费那条路：没人会"空手而归"，位点由 _advance 推进。
        # 在这里顺手抬一把等于把还没消费的消息跳过。
        h = Harness()
        h.seed_consume_offset(50)
        h.correct(PullStatus.FOUND, 110)
        assert h.offset == 50

    def test_offset_illegal_is_ignored(self):
        # OFFSET_ILLEGAL 是 Java 的另一条分支（drop + 冻结位点后重平衡），与这里无关
        h = Harness()
        h.seed_consume_offset(50)
        h.correct(PullStatus.OFFSET_ILLEGAL, 110)
        assert h.offset == 50


# ------------------------------- dispatch 循环登记在途（闸门不能只是"能算出来"）


class BlockingListener:
    """进 listener 就挂住，直到测试放行 —— 用来把"在途窗口"变成确定性的。"""

    def __init__(self):
        self.entered = threading.Event()
        self.release = threading.Event()
        self.seen = []

    def consume_message(self, msgs, context):
        self.seen.append([m.queue_offset for m in msgs])
        self.entered.set()
        assert self.release.wait(5), "listener never released"
        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS


class TestDispatchLoopWiring:
    def test_inflight_window_blocks_correction(self):
        c = DefaultMQPushConsumer(GROUP)
        mq = MessageQueue(TOPIC, BROKER, 0)
        key = c._mq_key(mq)
        c._started = True
        c.consume_message_batch_max_size = 3
        c._pending[key] = deque([msg(0), msg(1)])
        c._mq_map[key] = mq
        listener = BlockingListener()
        c.message_listener = listener
        c._start_dispatch_loop()
        try:
            assert listener.entered.wait(5), "dispatch loop never reached the listener"
            # 缓冲已被取空、批次在 listener 手里：correctTagsOffset 必须不动
            assert c._msg_queue_inflight.get(key) == 1
            with c._lock:
                c._correct_tags_offset_locked(key, PullStatus.NO_NEW_MSG, 110)
            assert c._consume_offsets.get(key) is None
        finally:
            listener.release.set()
            # 等消费收尾（成功 → 位点到 2）并把在途计数注销
            for _ in range(100):
                if not c._msg_queue_inflight.get(key):
                    break
                threading.Event().wait(0.02)
            c._stop.set()
            c._dispatch_thread.join(timeout=5)
        assert c._msg_queue_inflight.get(key) is None, "在途计数没有注销，闸门会永远关着"
        assert c._consume_offsets.get(key) == 2, "批次没被消费，位点不是 2"
        # 注销之后同一条应答就能抬位点了（真机上就是"消费完 + 队列没消息"那一刻）
        with c._lock:
            c._correct_tags_offset_locked(key, PullStatus.NO_NEW_MSG, 110)
        assert c._consume_offsets.get(key) == 110
