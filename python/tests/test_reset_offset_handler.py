# -*- coding: utf-8 -*-
"""220 ``RESET_CONSUMER_CLIENT_OFFSET`` 处理逻辑单测（Java ``MQClientInstance:1403-1450``），
不需要集群。

为什么必须离线锁死：这条路径错了全是**静默**的三种表现 ——

  - 只把缓冲与游标清掉、不撤队列（不 +1 代号）：在途批次的 ack 照样落地，把刚重置的
    位点又推回旧位置（Java 靠 ``ProcessQueue.setDropped(true)`` 挡住，见
    ``ConsumeMessageConcurrentlyService:267``）；长轮询在途的应答也会把重置前那一批
    消息重新塞进缓冲，照常消费 + ack；
  - 新位点没立刻落盘（撤销收尾里传 None）：进程在下一个周期落盘前崩掉，broker 上还是旧
    位点 —— 「重置」只活到本次进程结束；
  - 撤销收尾把队列从位点表里摘掉却没写回文件（广播模式）：重建后的队列读不到旧位点，
    按 consumeFromWhere 从头/从尾重扫（Java 是 persist(mq) 在前、removeOffset(mq) 在后）。

真机短期窗口里前两条最多表现为"重置后消息又冒出来一批"或"崩一次才暴露"，所以判据放离线；
真机另有链路证明（见 README 的 ``verify_reset_offset_live`` 一节）。
"""
from __future__ import annotations

import threading
from collections import deque

from rocketmq.client.consumer import DefaultMQPushConsumer
from rocketmq.client.consumer_result import ConsumeConcurrentlyStatus
from rocketmq.common.message import MessageExt, MessageQueue
from rocketmq.common.subscription_data import SubscriptionData
from rocketmq.remoting.protocol.heartbeat import MessageModel

GROUP = "GID_ResetOffsetUnitTest"
TOPIC = "ResetOffsetUnitTestTopic"
BROKER = "broker-a"
CLIENT_ID = "127.0.0.1@resetOffsetUnitTest"


def msg(queue_offset: int) -> MessageExt:
    m = MessageExt(topic=TOPIC, body=b"body")
    m.queue_id = 0
    m.broker_name = BROKER
    m.queue_offset = queue_offset
    return m


class FakePublishInfo:
    def __init__(self, queues):
        self.msg_queue_list = queues


class FakeClient:
    """只记 RPC、不碰网络；``broker_offsets`` 模拟 broker 上的已提交位点表。"""

    def __init__(self, mqs):
        self.mqs = list(mqs)
        self.offsets = []            # 每次 update_consumer_offset 的 (group, mq, off)
        self.broker_offsets = {}     # MessageQueue -> off

    def update_consumer_offset(self, group, mq, off):
        self.offsets.append((group, mq, off))
        self.broker_offsets[mq] = off

    def get_topic_publish_info(self, topic):
        return FakePublishInfo([mq for mq in self.mqs if mq.topic == topic])

    def get_topic_subscribe_info(self, topic):
        # rebalance 取的是订阅信息（Java RebalanceImpl.topicSubscribeInfoTable）
        return [mq for mq in self.mqs if mq.topic == topic]

    def get_consumer_id_list_by_group(self, topic, group):
        return [CLIENT_ID]

    def query_consumer_offset(self, group, mq, set_zero_if_not_found=False):
        return self.broker_offsets.get(mq)

    def unlock_batch_mq(self, group, client_id, mqs):
        return mqs


class Harness:
    """不碰网络的 push consumer：分配、缓冲、位点、代号都手动搭。

    ``_do_rebalance`` 默认换成计数器（否则重置里会真的起拉取线程）；要跑真实重建就
    显式调 ``rebuild()``。
    """

    def __init__(self, queues=1, cluster_offset=7):
        self.c = DefaultMQPushConsumer(GROUP)
        self.c.client_id = CLIENT_ID
        self.c._started = True
        self.c.subscription_data[TOPIC] = SubscriptionData(topic=TOPIC, sub_string="*")
        self.mqs = [MessageQueue(TOPIC, BROKER, i) for i in range(queues)]
        self.keys = [self.c._mq_key(mq) for mq in self.mqs]
        self.client = FakeClient(self.mqs)
        self.c._mq_client = self.client
        self.c._assigned = list(self.mqs)     # 真实状态：队列已分给本实例
        self.rebalances = 0
        self._real_rebalance = self.c._do_rebalance
        self.c._do_rebalance = self._count_rebalance
        self.c._queue_pull_loop = lambda mq: None   # 重建起的循环不打网络
        for mq, key in zip(self.mqs, self.keys):
            self.client.broker_offsets[mq] = cluster_offset
            with self.c._lock:
                self.c._mq_map[key] = mq
                self.c._queue_threads[key] = threading.current_thread()
                self.c._offset_table[key] = cluster_offset
                self.c._consume_offsets[key] = cluster_offset

    def _count_rebalance(self):
        self.rebalances += 1

    @property
    def mq(self):
        return self.mqs[0]

    @property
    def key(self):
        return self.keys[0]

    def reset(self, offsets):
        self.c.reset_offset(TOPIC, {self.mq: off for off in offsets})

    def epoch(self, key=None):
        with self.c._lock:
            return self.c._queue_epoch.get(key or self.key)

    def offset(self, key=None):
        with self.c._lock:
            return self.c._consume_offsets.get(key or self.key)

    def ack(self, offsets, epoch=None, key=None):
        self.c._advance_consume_offset(key or self.key, [msg(o) for o in offsets], epoch=epoch)

    def rebuild(self):
        """重置之后的下一趟 rebalance（真实路径：_do_rebalance → _rebalance_pull_threads）。"""
        self._real_rebalance()


# ------------------------------------------------- 撤队列：在途/缓冲一起作废


class TestRetire:
    def test_reset_drops_everything_for_the_queue(self):
        h = Harness()
        h.c._pending[h.key] = deque([msg(1), msg(2)])
        h.c._msg_queue_inflight[h.key] = 1
        h.reset([3])
        assert h.key not in h.c._pending
        assert h.key not in h.c._offset_table
        assert h.key not in h.c._consume_offsets
        assert h.key not in h.c._queue_threads
        assert h.key not in h.c._mq_map

    def test_reset_bumps_the_queue_epoch(self):
        h = Harness()
        assert h.epoch() in (None, 0)
        h.reset([3])
        assert h.epoch() == 1

    def test_stale_ack_cannot_undo_the_reset(self):
        # 在途批次拿的是重置前的代号（0）：它的 ack 必须整批作废。修好之前这里会写回 8 ——
        # 位点被判"重置过又退回旧位置"，重置只活到下一个周期落盘。
        h = Harness()
        h.reset([3])
        h.ack([7], epoch=0)
        assert h.offset() is None

    def test_ack_after_rebuild_still_works(self):
        # 负向对照：不是把这条队列永久冻死 —— 重建后的队列拿新代号，ack 恢复正常
        h = Harness()
        h.reset([3])
        h.rebuild()
        assert h.key in h.c._mq_map
        h.ack([3, 4], epoch=1)
        assert h.offset() == 5

    def test_reset_only_touches_queues_in_the_offset_table(self):
        # 负向对照：不在 220 队列表里的队列（同 topic 也）一根手指都不许碰
        h = Harness(queues=2)
        h.c._pending[h.keys[1]] = deque([msg(1)])
        h.reset([3])
        other = h.keys[1]
        assert other in h.c._mq_map
        assert other in h.c._offset_table
        assert other in h.c._consume_offsets
        assert other in h.c._pending
        assert h.epoch(other) in (None, 0)
        assert h.client.offsets == [(GROUP, h.mq, 3)]

    def test_reset_with_unknown_topic_is_a_noop(self):
        h = Harness()
        h.c.reset_offset("NoSuchTopic", {h.mq: 0})
        assert h.client.offsets == []
        assert h.key in h.c._mq_map
        assert h.epoch() in (None, 0)
        assert h.rebalances == 0

    def test_reset_with_empty_table_is_a_noop(self):
        h = Harness()
        h.c.reset_offset(TOPIC, {})
        assert h.key in h.c._mq_map
        assert h.epoch() in (None, 0)


# ------------------------------------------------- 立刻落盘 + 重建后从新位点拉


class TestPersistAndRebuild:
    def test_reset_persists_the_new_offset_immediately(self):
        # Java 的显式 persist：不等周期落盘，重置当场写回 broker（写的是**新**值）
        h = Harness()
        h.reset([3])
        assert h.client.offsets == [(GROUP, h.mq, 3)]

    def test_reset_schedules_a_rebalance(self):
        h = Harness()
        h.reset([3])
        assert h.rebalances == 1

    def test_rebuilt_queue_repulls_from_the_new_offset(self):
        # 撤销收尾落盘 → 重建时本地游标已被摘掉，新循环按 broker 上的（新）位点起拉：
        # 这就是"重置真的生效"（Java computePullFromWhere 的 READ_FROM_STORE 一支）。
        h = Harness(cluster_offset=7)
        h.reset([3])
        h.rebuild()
        assert h.key in h.c._mq_map
        assert h.key in h.c._queue_threads
        assert h.key not in h.c._offset_table           # 旧游标随撤销一起没了
        sub = h.c.subscription_data[TOPIC]
        assert h.c._resolve_initial_offset(h.client, h.mq, sub) == 3

    def test_backward_reset_repulls_from_the_new_position(self):
        h = Harness(cluster_offset=7)
        h.reset([0])
        assert h.client.broker_offsets[h.mq] == 0
        h.rebuild()
        sub = h.c.subscription_data[TOPIC]
        assert h.c._resolve_initial_offset(h.client, h.mq, sub) == 0


# ------------------------------------------------- 广播：位点只存本地，文件不能被抹掉


class TestBroadcast:
    def _local_saved(self, h):
        from rocketmq.client import consumer as consumer_mod
        with open(h.c._local_offset_path(), encoding="utf-8") as f:
            return consumer_mod._parse_local_offsets_json(f.read())

    def test_reset_keeps_the_new_offset_in_the_local_file(self, tmp_path, monkeypatch):
        monkeypatch.setenv("HOME", str(tmp_path))
        h = Harness()
        h.c.message_model = MessageModel.BROADCASTING
        h.reset([3])
        assert self._local_saved(h) == {h.key: 3}

    def test_plain_revoke_keeps_the_persisted_offset(self, tmp_path, monkeypatch):
        # 一般情形（不是重置）：普通 rebalance 撤队列也要把位点留在文件里，
        # 否则同名队列下次分回来时读不到位点、按 consumeFromWhere 重扫
        monkeypatch.setenv("HOME", str(tmp_path))
        h = Harness()
        h.c.message_model = MessageModel.BROADCASTING
        revoked = []
        with h.c._lock:
            h.c._retire_queue_locked(h.key, h.mq, revoked)
        assert revoked == [(h.mq, 7)]
        h.c._on_queues_revoked(revoked)
        assert self._local_saved(h) == {h.key: 7}
