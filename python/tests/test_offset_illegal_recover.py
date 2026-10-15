# -*- coding: utf-8 -*-
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
    """Only records RPC, never touches the network (``_on_queues_revoked``'s persistence goes through it)."""

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
    """A push consumer that never touches the network: buffer / offset / key are all set up manually."""

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
        """Simulate the in-lock actions of OFFSET_ILLEGAL in the pull callback (cursor + freeze + corrected value)."""
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


# ------------------------------------------------- Correction = freeze + drop queue + persist immediately


class TestRecover:
    def test_recover_drops_everything_for_the_queue(self):
        h = Harness()
        h.c._pending[h.key] = deque([msg(1), msg(2)])
        h.c._msg_queue_inflight[h.key] = 1
        h.freeze(0)
        h.recover()
        # All local state of the queue is voided: buffer, cursor, consumed offset, thread table, assignment table
        assert h.key not in h.c._pending
        assert h.key not in h.c._offset_table
        assert h.key not in h.c._consume_offsets
        assert h.key not in h.c._queue_threads
        assert h.key not in h.c._mq_map

    def test_recover_persists_the_corrected_offset_immediately(self):
        h = Harness()
        h.freeze(0)
        h.recover()
        assert h.client.offsets == [(GROUP, h.mq, 0)]

    def test_recover_wakes_the_rebalance_loop(self):
        # After removeProcessQueue, the queue is rebuilt via rebalanceImmediately
        h = Harness()
        h.freeze(0)
        assert not h.c._rebalance_now.is_set()
        h.recover()
        assert h.c._rebalance_now.is_set()

    def test_recover_bumps_the_queue_epoch(self):
        h = Harness(epoch=0)
        h.freeze(0)
        h.recover()
        assert h.epoch == 1

    def test_recover_without_a_queue_is_a_noop(self):
        # The queue is no longer under this instance (concurrent revoke): with no mq there is nothing to persist, and none can be conjured up
        h = Harness(assigned=False)
        h.recover()
        assert h.client.offsets == []
        assert h.c._rebalance_now.is_set()


# ------------------------------------------------- Freeze: the corrected value is not overturned by in-flight acks


class TestFreeze:
    def test_frozen_offset_ignores_ack(self):
        h = Harness()
        h.freeze(0)
        # A late ack from the old batch (offset 2 already consumed): pushing the offset back to 3 is the bounce scenario
        h.ack([0, 1, 2], epoch=0)
        assert h.offset == 0

    def test_frozen_offset_ignores_correct_tags_offset(self):
        h = Harness()
        h.freeze(0)
        with h.c._lock:
            h.c._correct_tags_offset_locked(h.key, PullStatus.NO_NEW_MSG, 110)
        assert h.offset == 0

    def test_freeze_survives_the_drop_until_the_queue_is_rebuilt(self):
        h = Harness()
        h.freeze(0)
        h.recover()
        assert h.key in h.c._frozen_offsets

    def test_rebuild_clears_the_freeze(self):
        h = Harness()
        h.freeze(0)
        h.recover()
        # Rebuild: reassign the queue to oneself (the add branch of _rebalance_pull_threads)
        h.c._queue_pull_loop = lambda mq: None     # the thread does nothing
        h.c._assigned = [h.mq]
        h.c._started = True
        h.c.subscription_data[TOPIC] = SubscriptionData(topic=TOPIC, sub_string="*")
        h.c._rebalance_pull_threads()
        assert h.key not in h.c._frozen_offsets
        # After the rebuild, acks work normally
        h.ack([0, 1])
        assert h.offset == 2




class TestDroppedBatch:
    def test_stale_epoch_ack_is_dropped(self):
        # After the queue is revoked/rebuilt (epoch 0 → 1), a late ack from the old batch must not move the offset again
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
