# -*- coding: utf-8 -*-
"""``OFFSET_ILLEGAL`` 纠错分支单测（Java ``DefaultMQPushConsumerImpl:402-427``），不需要集群。

为什么必须离线锁死：这条路径错了是**静默**的两种极端 ——

  - 只把拉取游标拨到 ``nextBeginOffset`` 而不丢队列/不冻结：broker 刚把位点纠正到
    合法区间，这条队列上**已经取回还没 ack** 的旧批次一 ack 又把位点推回非法值，
    下一轮拉取再被 broker 拒一次 —— 客户端与 broker 之间来回弹跳，永不停歇；
  - 纠错后的位点没立刻落盘：进程在下一轮周期落盘（默认 5s）之前崩掉，broker 上留着的
    还是非法位点，重启后从非法位点起拉 —— 这条纠错等于没做。

两个方向在真机短期窗口里都看不出差别（消息照消费、只是一直在弹/一次崩溃才暴露），
所以断言全部放离线；真机另有一条链路证明（见 README 的 `live_*` 一节）。

判据来源：``PullMessageProcessor`` 对 OFFSET_OVERFLOW_BADLY / NO_MESSAGE_IN_QUEUE /
OFFSET_RESET / OFFSET_TOO_SMALL 一律回 ``PULL_OFFSET_MOVED``，``MQClientAPIImpl:1099``
把它映射成 ``PullStatus.OFFSET_ILLEGAL``，修正值在应答头 ``nextBeginOffset``。Java 的处理
是 ``setNextOffset`` → ``ProcessQueue.setDropped(true)`` → 异步
``{ updateAndFreezeOffset; persist; removeProcessQueue; rebalanceImmediately }``。
"""
from __future__ import annotations

import threading
from collections import deque

from client.consumer import DefaultMQPushConsumer
from client.consumer_result import ConsumeConcurrentlyStatus, PullStatus
from common.message import MessageExt, MessageQueue
from common.subscription_data import SubscriptionData

GROUP = "GID_OffsetIllegalUnitTest"
TOPIC = "OffsetIllegalUnitTestTopic"
BROKER = "broker-a"


def msg(queue_offset: int) -> MessageExt:
    m = MessageExt(topic=TOPIC, body=b"body")
    m.queue_id = 0
    m.broker_name = BROKER
    m.queue_offset = queue_offset
    return m


class FakeClient:
    """只记 RPC，不碰网络（``_on_queues_revoked`` 的落盘走它）。"""

    def __init__(self):
        self.offsets = []

    def update_consumer_offset(self, group, mq, off):
        self.offsets.append((group, mq, off))


class FakeListener:
    def __init__(self):
        self.calls = []

    def consume_message(self, msgs, context):
        self.calls.append(list(msgs))
        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS


class Harness:
    """一个不碰网络的 push consumer：缓冲/位点/代号都手动搭。"""

    def __init__(self, assigned: bool = True, epoch: int = 0):
        self.c = DefaultMQPushConsumer(GROUP)
        self.mq = MessageQueue(TOPIC, BROKER, 0)
        self.key = self.c._mq_key(self.mq)
        self.client = FakeClient()
        self.c._mq_client = self.client
        if assigned:
            with self.c._lock:
                self.c._mq_map[self.key] = self.mq
                self.c._queue_threads[self.key] = threading.current_thread()
                self.c._offset_table[self.key] = 3
                self.c._consume_offsets[self.key] = 3
                self.c._queue_epoch[self.key] = epoch

    def freeze(self, off: int) -> None:
        """模拟拉取回调里 OFFSET_ILLEGAL 的锁内动作（游标 + 冻结 + 修正值）。"""
        with self.c._lock:
            self.c._offset_table[self.key] = off
            self.c._frozen_offsets.add(self.key)
            self.c._consume_offsets[self.key] = off

    def recover(self) -> None:
        self.c._offset_illegal_recover(self.key)

    @property
    def offset(self):
        return self.c._consume_offsets.get(self.key)

    @property
    def epoch(self):
        return self.c._queue_epoch.get(self.key)

    def ack(self, offsets, epoch=None):
        self.c._advance_consume_offset(self.key, [msg(o) for o in offsets], epoch=epoch)


# ------------------------------------------------- 纠错 = 冻结 + 丢队列 + 立刻落盘


class TestRecover:
    def test_recover_drops_everything_for_the_queue(self):
        h = Harness()
        h.c._pending[h.key] = deque([msg(1), msg(2)])
        h.c._msg_queue_inflight[h.key] = 1
        h.freeze(0)
        h.recover()
        # 队列的本地状态全部作废：缓冲、游标、已消费位点、线程表、分配表
        assert h.key not in h.c._pending
        assert h.key not in h.c._offset_table
        assert h.key not in h.c._consume_offsets
        assert h.key not in h.c._queue_threads
        assert h.key not in h.c._mq_map

    def test_recover_persists_the_corrected_offset_immediately(self):
        # Java 的显式 persist：不等周期落盘，纠错当场写回 broker
        h = Harness()
        h.freeze(0)
        h.recover()
        assert h.client.offsets == [(GROUP, h.mq, 0)]

    def test_recover_wakes_the_rebalance_loop(self):
        # removeProcessQueue 之后靠 rebalanceImmediately 重建这条队列
        h = Harness()
        h.freeze(0)
        assert not h.c._rebalance_now.is_set()
        h.recover()
        assert h.c._rebalance_now.is_set()

    def test_recover_bumps_the_queue_epoch(self):
        # 代号 +1 是「旧批次作废」的依据（Java setDropped(true)）
        h = Harness(epoch=0)
        h.freeze(0)
        h.recover()
        assert h.epoch == 1

    def test_recover_without_a_queue_is_a_noop(self):
        # 队列已经不在本实例名下（并发撤销）：没有 mq 就没有可落盘的对象，不能凭空造一条
        h = Harness(assigned=False)
        h.recover()
        assert h.client.offsets == []
        assert h.c._rebalance_now.is_set()   # 重建请求照样发（Java 的 rebalanceImmediately 无条件）


# ------------------------------------------------- 冻结：修正值不被在途 ack 推翻


class TestFreeze:
    def test_frozen_offset_ignores_ack(self):
        h = Harness()
        h.freeze(0)
        # 旧批次（offset 2 已消费）迟到的 ack：把位点推回 3 就是 java 的弹跳现场
        h.ack([0, 1, 2], epoch=0)
        assert h.offset == 0

    def test_frozen_offset_ignores_correct_tags_offset(self):
        h = Harness()
        h.freeze(0)
        with h.c._lock:
            h.c._correct_tags_offset_locked(h.key, PullStatus.NO_NEW_MSG, 110)
        assert h.offset == 0

    def test_freeze_survives_the_drop_until_the_queue_is_rebuilt(self):
        # 有意偏差：Java 在 removeOffset 时解冻、靠 ProcessQueue.isDropped() 兜底；
        # 本端口没有 per-batch 的 ProcessQueue 对象，冻结一直留到队列重建（更严）
        h = Harness()
        h.freeze(0)
        h.recover()
        assert h.key in h.c._frozen_offsets

    def test_rebuild_clears_the_freeze(self):
        h = Harness()
        h.freeze(0)
        h.recover()
        # 重建：把队列重新划给自己（_rebalance_pull_threads 的 add 分支）
        h.c._queue_pull_loop = lambda mq: None     # 线程里什么都不做
        h.c._assigned = [h.mq]
        h.c._started = True
        h.c.subscription_data[TOPIC] = SubscriptionData(topic=TOPIC, sub_string="*")
        h.c._rebalance_pull_threads()
        assert h.key not in h.c._frozen_offsets
        # 重建后 ack 恢复正常
        h.ack([0, 1])
        assert h.offset == 2


# ------------------------------------------------- 旧代号的批次整批作废（Java :267/:339）


class TestDroppedBatch:
    def test_stale_epoch_ack_is_dropped(self):
        # 队列被撤销/重建（代号 0 → 1）后，旧批次迟到的 ack 不能再动位点
        h = Harness(epoch=1)
        h.c._consume_offsets[h.key] = 0
        h.ack([0, 1, 2], epoch=0)
        assert h.offset == 0

    def test_current_epoch_ack_advances(self):
        h = Harness(epoch=1)
        h.c._consume_offsets[h.key] = 0
        h.ack([0, 1, 2], epoch=1)
        assert h.offset == 3

    def test_stale_epoch_batch_is_not_consumed(self):
        # Java ConsumeMessageConcurrentlyService:339 —— 排队期间被丢弃的批次连 listener
        # 都不该进（消息已由新属主/重建后的队列接管）
        h = Harness(epoch=1)
        listener = FakeListener()
        h.c.message_listener = listener
        done = h.c._consume_batch(h.key, h.mq, [msg(0), msg(1)], epoch=0)
        assert done is False
        assert listener.calls == []

    def test_current_epoch_batch_is_consumed(self):
        h = Harness(epoch=1)
        listener = FakeListener()
        h.c.message_listener = listener
        h.c._consume_offsets[h.key] = 0
        done = h.c._consume_batch(h.key, h.mq, [msg(0), msg(1)], epoch=1)
        assert done is True
        assert len(listener.calls) == 1
        assert h.offset == 2
