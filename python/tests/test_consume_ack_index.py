# -*- coding: utf-8 -*-
from __future__ import annotations

from collections import deque

from client.consumer import DefaultMQPushConsumer
from client.consumer_result import ConsumeConcurrentlyStatus
from client.hook import ConsumeMessageHook
from common.message import MessageExt, MessageQueue
from remoting.protocol.heartbeat import MessageModel

GROUP = "GID_AckIndexUnitTest"
TOPIC = "AckIndexUnitTestTopic"
BROKER = "broker-a"
KEY = "key"


class _RecordingConsumeHook(ConsumeMessageHook):
    """Only records the (status, success, ConsumeContextType) seen by the after hook."""

    def __init__(self):
        self.after = []

    def hook_name(self):
        return "RecordingConsumeHook"

    def consume_message_before(self, context):
        pass

    def consume_message_after(self, context):
        self.after.append((context.status, context.success,
                           context.props.get("ConsumeContextType")))


def msg(queue_offset: int, reconsume_times: int = 0) -> MessageExt:
    m = MessageExt(topic=TOPIC, body=b"body")
    m.queue_id = 0
    m.broker_name = BROKER
    m.queue_offset = queue_offset
    m.reconsume_times = reconsume_times
    return m


class FixedListener:
    """Returns a fixed status and (optionally) writes ack_index into the context."""

    def __init__(self, status, ack_index=None):
        self.status = status
        self.ack_index = ack_index

    def consume_message(self, msgs, context):
        if self.ack_index is not None:
            context.ack_index = self.ack_index
        return self.status


class Harness:
    """A push consumer that never touches the network: offsets / pending queue set up manually, send-back only recorded."""

    def __init__(self, model=None, batch_max=3):
        self.c = DefaultMQPushConsumer(GROUP)
        if model is not None:
            self.c.message_model = model
        self.c.consume_message_batch_max_size = batch_max
        self.c._pending[KEY] = deque()
        self.c._consume_offsets[KEY] = 0
        self.backed = []
        self.fail_for = set()

        def _back(m, delay_level, broker_name=None):
            if m.queue_offset in self.fail_for:
                raise RuntimeError("simulated send-back failure")
            self.backed.append((m, delay_level))

        self.c.send_message_back = _back  # type: ignore[assignment]
        self.mq = MessageQueue(TOPIC, BROKER, 0)

    def consume(self, msgs, listener):
        self.c.message_listener = listener
        return self.c._consume_batch(KEY, self.mq, msgs)

    @property
    def offset(self):
        return self.c._consume_offsets[KEY]

    @property
    def pending_offsets(self):
        return [m.queue_offset for m in self.c._pending[KEY]]

    @property
    def backed_offsets(self):
        return sorted(m.queue_offset for m, _ in self.backed)


# --------------------------------------------------------------- Defaults


class TestContextDefault:
    def test_default_is_the_int32_max_sentinel(self):
        from client.consumer_result import ConsumeConcurrentlyContext
        assert ConsumeConcurrentlyContext().ack_index == (1 << 31) - 1


# ------------------------------------------------------- CONSUME_SUCCESS


class TestConsumeSuccess:
    def test_full_ack_sends_nothing_back(self):
        h = Harness()
        batch = [msg(0), msg(1), msg(2)]
        assert h.consume(batch, FixedListener(ConsumeConcurrentlyStatus.CONSUME_SUCCESS)) is True
        assert h.backed_offsets == []
        assert h.offset == 3

    def test_ack_index_beyond_size_is_clamped(self):
        h = Harness()
        listener = FixedListener(ConsumeConcurrentlyStatus.CONSUME_SUCCESS, ack_index=99)
        assert h.consume([msg(0), msg(1)], listener) is True
        assert h.backed_offsets == []
        assert h.offset == 2

    def test_partial_ack_commits_prefix_and_redelivers_tail(self):
        """Core scenario: the listener acks only the first message → the last two must be sent back, not skipped over by the offset."""
        h = Harness()
        listener = FixedListener(ConsumeConcurrentlyStatus.CONSUME_SUCCESS, ack_index=0)
        assert h.consume([msg(0), msg(1), msg(2)], listener) is True
        assert h.backed_offsets == [1, 2]
        assert h.offset == 3

    def test_negative_ack_index_on_success_redelivers_everything(self):
        h = Harness()
        listener = FixedListener(ConsumeConcurrentlyStatus.CONSUME_SUCCESS, ack_index=-1)
        assert h.consume([msg(0), msg(1)], listener) is True
        assert h.backed_offsets == [0, 1]
        assert h.offset == 2

    def test_send_back_uses_the_delay_level_gradient(self):
        h = Harness()
        batch = [msg(0), msg(1, reconsume_times=2), msg(2)]
        h.consume(batch, FixedListener(ConsumeConcurrentlyStatus.CONSUME_SUCCESS, ack_index=0))
        assert [(m.queue_offset, dl) for m, dl in h.backed] == [(1, 5), (2, 3)]

    def test_send_back_failure_requeues_that_message_only(self):
        pass
        h = Harness()
        h.fail_for = {1}
        batch = [msg(0), msg(1), msg(2)]
        ok = h.consume(batch, FixedListener(ConsumeConcurrentlyStatus.CONSUME_SUCCESS,
                                           ack_index=0))
        assert ok is False
        assert h.pending_offsets == [1]
        assert batch[1].reconsume_times == 1
        # Acked prefix 0 + successfully sent-back 2 → would commit to 3, but 1 is still in the queue, so it stops at 1
        assert h.offset == 1

    def test_all_send_backs_failing_keeps_offset_frozen(self):
        h = Harness()
        h.fail_for = {0, 1}
        ok = h.consume([msg(0), msg(1)],
                       FixedListener(ConsumeConcurrentlyStatus.RECONSUME_LATER))
        assert ok is False
        assert h.pending_offsets == [0, 1]
        assert h.offset == 0


# ------------------------------------------------------- RECONSUME_LATER


class TestReconsumeLater:
    def test_ignores_listener_ack_index_and_redelivers_whole_batch(self):
        h = Harness()
        listener = FixedListener(ConsumeConcurrentlyStatus.RECONSUME_LATER, ack_index=1)
        assert h.consume([msg(0), msg(1), msg(2)], listener) is True
        assert h.backed_offsets == [0, 1, 2]
        assert h.offset == 3

    def test_exception_is_treated_as_reconsume_later(self):
        h = Harness()

        class Boom:
            def consume_message(self, msgs, context):
                raise RuntimeError("listener blew up")

        assert h.consume([msg(0), msg(1)], Boom()) is True
        assert h.backed_offsets == [0, 1]

    def test_null_return_is_reconsume_later_not_a_silent_ack(self):
        pass
        h = Harness()
        hook = _RecordingConsumeHook()
        h.c.register_consume_message_hook(hook)
        assert h.consume([msg(0), msg(1)], FixedListener(None)) is True
        assert h.backed_offsets == [0, 1], "the whole batch is sent back"
        assert hook.after == [("RECONSUME_LATER", False, "RETURNNULL")], (
            "the status is the bare member name, not a class-qualified enum string")

    def test_hook_status_is_the_uppercase_enum_name_on_the_success_path_too(self):
        pass
        h = Harness()
        hook = _RecordingConsumeHook()
        h.c.register_consume_message_hook(hook)
        assert h.consume([msg(0)], FixedListener(ConsumeConcurrentlyStatus.CONSUME_SUCCESS)) is True
        status, success, ctx_type = hook.after[0]
        assert (status, success, ctx_type) == ("CONSUME_SUCCESS", True, "SUCCESS")
        assert status != str(ConsumeConcurrentlyStatus.CONSUME_SUCCESS)


# ---------------------------------------------------------- BROADCASTING


class TestBroadcasting:
    def test_tail_is_dropped_without_send_back(self):
        pass
        h = Harness(model=MessageModel.BROADCASTING)
        listener = FixedListener(ConsumeConcurrentlyStatus.CONSUME_SUCCESS, ack_index=0)
        assert h.consume([msg(0), msg(1), msg(2)], listener) is True
        assert h.backed_offsets == []
        assert h.pending_offsets == []
        assert h.offset == 3

    def test_reconsume_later_broadcast_still_advances(self):
        h = Harness(model=MessageModel.BROADCASTING)
        ok = h.consume([msg(0), msg(1)], FixedListener(ConsumeConcurrentlyStatus.RECONSUME_LATER))
        assert ok is True
        assert h.backed_offsets == []
        assert h.offset == 2
