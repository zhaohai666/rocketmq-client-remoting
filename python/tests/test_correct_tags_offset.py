# -*- coding: utf-8 -*-
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
    """A push consumer that never touches the network: buffer / offset / in-flight count are all set up manually."""

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


# ------------------------------------------------- Empty response advances the offset to nextBeginOffset


class TestAdvancesOnEmptyResponse:
    def test_no_new_msg_advances_when_queue_empty(self):
        h = Harness()
        h.correct(PullStatus.NO_NEW_MSG, 110)
        assert h.offset == 110

    def test_no_matched_msg_advances_when_queue_empty(self):
        # Broker-side filtering blocked the whole batch (PULL_RETRY_IMMEDIATELY → NO_MATCHED_MSG):
        # those messages never enter _pending, so the offset can only catch up via this step
        h = Harness()
        h.correct(PullStatus.NO_MATCHED_MSG, 110)
        assert h.offset == 110

    def test_advances_from_the_recorded_offset(self):
        h = Harness()
        h.seed_consume_offset(50)
        h.correct(PullStatus.NO_NEW_MSG, 110)
        assert h.offset == 110

    def test_none_next_offset_is_ignored(self):
        # The response header lacks nextBeginOffset (expressed as None in this port): better not to advance than to write a 0 that
        # knocks the committed offset on the broker back down
        h = Harness()
        h.seed_consume_offset(50)
        h.correct(PullStatus.NO_NEW_MSG, None)
        assert h.offset == 50


# ------------------------------------------------------------- Advance only, never regress


class TestIncreaseOnly:
    def test_never_regresses(self):
        h = Harness()
        h.seed_consume_offset(200)
        h.correct(PullStatus.NO_NEW_MSG, 110)
        assert h.offset == 200

    def test_equal_offset_is_noop(self):
        h = Harness()
        h.seed_consume_offset(110)
        h.correct(PullStatus.NO_NEW_MSG, 110)
        assert h.offset == 110


# ---------------------------------------------------------- Gate: process queue non-empty


class TestProcessQueueGuard:
    def test_pending_blocks(self):
        h = Harness()
        h.seed_pending([7])
        h.correct(PullStatus.NO_NEW_MSG, 110)
        assert h.offset is None

    def test_in_flight_batch_blocks(self):
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


# ------------------------------------------------- Only these two statuses correct the offset


class TestStatusGate:
    def test_found_is_ignored(self):
        # FOUND goes through the putMessage + consume path: nobody "comes back empty-handed", and the offset is advanced by _advance.
        # Casually advancing it here would amount to skipping messages that have not been consumed yet.
        h = Harness()
        h.seed_consume_offset(50)
        h.correct(PullStatus.FOUND, 110)
        assert h.offset == 50

    def test_offset_illegal_is_ignored(self):
        h = Harness()
        h.seed_consume_offset(50)
        h.correct(PullStatus.OFFSET_ILLEGAL, 110)
        assert h.offset == 50


# ------------------------------- The dispatch loop registers in-flight (the gate must not merely be "computable")


class BlockingListener:
    """Blocks on entering the listener until the test releases it — used to make the "in-flight window" deterministic."""

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
            # The buffer has been drained and the batch is in the listener's hands: correctTagsOffset must not move
            assert c._msg_queue_inflight.get(key) == 1
            with c._lock:
                c._correct_tags_offset_locked(key, PullStatus.NO_NEW_MSG, 110)
            assert c._consume_offsets.get(key) is None
        finally:
            listener.release.set()
            # Wait for consumption to finish (success → offset to 2) and deregister the in-flight count
            for _ in range(100):
                if not c._msg_queue_inflight.get(key):
                    break
                threading.Event().wait(0.02)
            c._stop.set()
            c._dispatch_thread.join(timeout=5)
        assert c._msg_queue_inflight.get(key) is None, "the in-flight count was not deregistered; the gate would stay closed forever"
        assert c._consume_offsets.get(key) == 2, "the batch was not consumed; the offset is not 2"
        # After deregistration, the same response can advance the offset (on a real machine: the moment "consumed + queue empty")
        with c._lock:
            c._correct_tags_offset_locked(key, PullStatus.NO_NEW_MSG, 110)
        assert c._consume_offsets.get(key) == 110
