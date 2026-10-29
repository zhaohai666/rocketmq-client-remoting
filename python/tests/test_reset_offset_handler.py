# -*- coding: utf-8 -*-
from __future__ import annotations

import threading
from collections import deque

from client.consumer import DefaultMQPushConsumer
from client.consumer_result import ConsumeConcurrentlyStatus
from common.message import MessageExt, MessageQueue
from common.subscription_data import SubscriptionData
from remoting.protocol.heartbeat import MessageModel

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
    """Only records RPCs, never touches the network; ``broker_offsets`` simulates the committed offset table on the broker."""

    def __init__(self, mqs):
        self.mqs = list(mqs)
        self.offsets = []            # (group, mq, off) of each update_consumer_offset
        self.broker_offsets = {}     # MessageQueue -> off

    def update_consumer_offset(self, group, mq, off):
        self.offsets.append((group, mq, off))
        self.broker_offsets[mq] = off

    def get_topic_publish_info(self, topic):
        return FakePublishInfo([mq for mq in self.mqs if mq.topic == topic])

    def get_topic_subscribe_info(self, topic):
        return [mq for mq in self.mqs if mq.topic == topic]

    def get_consumer_id_list_by_group(self, topic, group):
        return [CLIENT_ID]

    def query_consumer_offset(self, group, mq, set_zero_if_not_found=False):
        return self.broker_offsets.get(mq)

    def unlock_batch_mq(self, group, client_id, mqs):
        return mqs


class Harness:
    """A push consumer that never touches the network: assignment, buffer, offsets, and epochs are all set up manually.

    ``_do_rebalance`` is replaced with a counter by default (otherwise the reset would really start pull threads); to run the real rebuild,
    call ``rebuild()`` explicitly.
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
        self.c._assigned = list(self.mqs)     # real state: the queues have been assigned to this instance
        self.rebalances = 0
        self._real_rebalance = self.c._do_rebalance
        self.c._do_rebalance = self._count_rebalance
        self.c._queue_pull_loop = lambda mq: None   # the restarted loop does not touch the network
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
        """The next rebalance after the reset (real path: _do_rebalance -> _rebalance_pull_threads)."""
        self._real_rebalance()


# ------------------------------------------------- Revoking queues: in-flight and buffered messages are voided together


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
        # the in-flight batch carries the pre-reset epoch (0): its ack must be voided as a whole. Before the fix this wrote back 8 --
        # the offset was judged "reset then fell back to the old position", so the reset only survived until the next periodic persist.
        h = Harness()
        h.reset([3])
        h.ack([7], epoch=0)
        assert h.offset() is None

    def test_ack_after_rebuild_still_works(self):
        # negative control: the queue is not permanently frozen -- after the rebuild the queue gets a new epoch and ack works normally
        h = Harness()
        h.reset([3])
        h.rebuild()
        assert h.key in h.c._mq_map
        h.ack([3, 4], epoch=1)
        assert h.offset() == 5

    def test_reset_only_touches_queues_in_the_offset_table(self):
        # negative control: queues not in the 220 queue table (even on the same topic) must not be touched at all
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


# ------------------------------------------------- Persist immediately + pull from the new offset after rebuild


class TestPersistAndRebuild:
    def test_reset_persists_the_new_offset_immediately(self):
        h = Harness()
        h.reset([3])
        assert h.client.offsets == [(GROUP, h.mq, 3)]

    def test_reset_schedules_a_rebalance(self):
        h = Harness()
        h.reset([3])
        assert h.rebalances == 1

    def test_rebuilt_queue_repulls_from_the_new_offset(self):
        h = Harness(cluster_offset=7)
        h.reset([3])
        h.rebuild()
        assert h.key in h.c._mq_map
        assert h.key in h.c._queue_threads
        assert h.key not in h.c._offset_table           # the old cursor is gone along with the revocation
        # the initial offset never depends on the subscription type
        assert h.c._resolve_initial_offset(h.client, h.mq) == 3

    def test_backward_reset_repulls_from_the_new_position(self):
        h = Harness(cluster_offset=7)
        h.reset([0])
        assert h.client.broker_offsets[h.mq] == 0
        h.rebuild()
        assert h.c._resolve_initial_offset(h.client, h.mq) == 0


# ------------------------------------------------- Broadcasting: offsets are stored locally only, the file must not be wiped


class TestBroadcast:
    def _local_saved(self, h):
        from client import consumer as consumer_mod
        with open(h.c._local_offset_path(), encoding="utf-8") as f:
            return consumer_mod._parse_local_offsets_json(f.read())

    def test_reset_keeps_the_new_offset_in_the_local_file(self, tmp_path, monkeypatch):
        monkeypatch.setenv("HOME", str(tmp_path))
        h = Harness()
        h.c.message_model = MessageModel.BROADCASTING
        h.reset([3])
        assert self._local_saved(h) == {h.key: 3}

    def test_plain_revoke_keeps_the_persisted_offset(self, tmp_path, monkeypatch):
        # the general case (not a reset): a plain rebalance revoking queues must also keep the offsets in the file,
        # otherwise when the same-named queue is assigned back next time the offset cannot be read and it rescans per consumeFromWhere
        monkeypatch.setenv("HOME", str(tmp_path))
        h = Harness()
        h.c.message_model = MessageModel.BROADCASTING
        revoked = []
        with h.c._lock:
            h.c._retire_queue_locked(h.key, h.mq, revoked)
        assert revoked == [(h.mq, 7)]
        h.c._on_queues_revoked(revoked)
        assert self._local_saved(h) == {h.key: 7}
