# -*- coding: utf-8 -*-
from __future__ import annotations

import time
from collections import deque

import pytest

from client import consumer
from client.consumer import (_INT32_MAX, DefaultMQPushConsumer,
                                     MessageListenerOrderly)
from client.consumer_result import (ConsumeConcurrentlyStatus,
                                             ConsumeOrderlyContext, ConsumeOrderlyStatus)
from client.hook import ConsumeMessageHook
from client.mq_client import MQClientInstance
from common.message import MessageExt, MessageQueue
from common.message_const import MessageConst
from remoting.protocol.headers import ConsumerSendMsgBackRequestHeader

GROUP = "GID_OrderlyReconsumeUnitTest"
TOPIC = "OrderlyReconsumeUnitTestTopic"
BROKER = "broker-a"
KEY = "key"
RETRY_TOPIC = "%RETRY%" + GROUP


def msg(queue_offset: int = 0, reconsume_times: int = 0) -> MessageExt:
    m = MessageExt(topic=TOPIC, body=b"body-%d" % queue_offset)
    m.queue_id = 0
    m.broker_name = BROKER
    m.queue_offset = queue_offset
    m.reconsume_times = reconsume_times
    m.msg_id = "offset-msg-id-%d" % queue_offset
    return m


class OrderlyListener(MessageListenerOrderly):
    """An orderly listener that always fails (in a real scenario, this is the poison message)."""

    def __init__(self, status=ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT):
        self.status = status
        self.calls = 0

    def consume_message(self, msgs, context):
        self.calls += 1
        return self.status


class ScriptedListener(MessageListenerOrderly):
    """Return status per the script, and rewrite context at the call site (autoCommit / suspend duration).

    This is how a real listener changes these two: after getting context, set it right there, then return a status.
    """

    def __init__(self, status, mutate=None, raise_exc=False):
        self.status = status
        self.mutate = mutate
        self.raise_exc = raise_exc
        self.contexts = []

    def consume_message(self, msgs, context):
        self.contexts.append(context)
        if self.mutate is not None:
            self.mutate(context)
        if self.raise_exc:
            raise RuntimeError("listener blew up")
        return self.status


class RecordingStats:
    """Replaces ConsumerStatsManager on the instance, recording only the three counting entry points."""

    def __init__(self):
        self.ok = []
        self.failed = []
        self.rt = []

    def inc_consume_ok_tps(self, group, topic, count):
        self.ok.append(count)

    def inc_consume_failed_tps(self, group, topic, count):
        self.failed.append(count)

    def inc_consume_rt(self, group, topic, rt):
        self.rt.append(rt)


class RecordingHook(ConsumeMessageHook):
    """Record the (status, success, ConsumeContextType) seen by the after hook."""

    def __init__(self):
        self.after = []

    def hook_name(self):
        return "RecordingHook"

    def consume_message_before(self, context):
        pass

    def consume_message_after(self, context):
        self.after.append((context.status, context.success,
                           context.props.get("ConsumeContextType")))


class _FakeTime:
    """Replace only time.sleep in the consumer module; everything else (time.time) forwards to the real module."""

    def __init__(self, slept):
        self.slept = slept

    def sleep(self, seconds):
        self.slept.append(seconds)

    def time(self):
        return time.time()


class Harness:
    """An orderly consumer that never touches the network: the internal producer only records the message sent out."""

    def __init__(self, max_reconsume_times: int = -1, stats=None) -> None:
        self.c = DefaultMQPushConsumer(GROUP)
        self.c.max_reconsume_times = max_reconsume_times
        self.c.suspend_current_queue_time_millis = 0
        self.c._pending[KEY] = deque()
        self.c._consume_offsets[KEY] = 0
        self.c._stats_manager = stats
        self.sent = []
        self.fail_send = False
        self.mq = MessageQueue(TOPIC, BROKER, 0)
        self.c._require_client = lambda: self          # orderly send-back takes the "instance" right here

    def get_topic_publish_info(self, topic, is_default=False):
        # the route is given as usual: what needs simulating for a send-back failure is the **send** step, not failing to select a queue,
        # otherwise the exception would be thrown before send_message and the criterion would not reach the real branch.
        return _PublishInfo(self.mq)

    def send_message(self, producer_group, message, mq, timeout_millis=3000,
                     sys_flag=0, unit_mode=False, **kw):
        # record the calling criteria: both the inner producer group name and unitMode must follow the consumer
        self.sent.append({"group": producer_group, "msg": message, "mq": mq,
                          "unit_mode": unit_mode})
        if self.fail_send:
            raise RuntimeError("simulated inner-producer failure")
        return None


class _PublishInfo:
    def __init__(self, mq):
        self._mq = mq

    def select_one_message_queue(self, *filters):
        return self._mq


def _consume(h: Harness, batch, listener=None):
    h.c.message_listener = listener if listener is not None else OrderlyListener()
    return h.c._consume_batch(KEY, h.mq, batch)


# ------------------------------------------------ The two meanings of -1 (orderly vs concurrent)


def test_minus_one_is_unlimited_for_orderly_consumption():
    h = Harness(max_reconsume_times=-1)
    assert h.c._orderly_max_reconsume_times() == _INT32_MAX
    # under the default config a poison message must not be judged "exhausted": the local count can never catch up with MAX_VALUE
    batch = [msg(0, reconsume_times=_INT32_MAX - 1)]
    assert h.c._check_orderly_reconsume_times(batch) is True
    assert batch[0].get_reconsume_times() == _INT32_MAX
    assert h.sent == []


def test_explicit_cap_is_used_as_is():
    h = Harness(max_reconsume_times=3)
    assert h.c._orderly_max_reconsume_times() == 3


def test_orderly_cap_does_not_reuse_the_concurrent_16_default():
    """``-1`` means "not configured", and the two consume modes resolve it differently: concurrent
    send-back lands on 16, orderly consumption lands on ``Integer.MAX_VALUE``. Merging them into one
    constant would make orderly consumption produce dead letters out of thin air, or make concurrent
    messages retry forever -- so each side is tested once for the value that really lands in the
    request header."""
    captured = {}

    class FakeClient:
        def find_broker_address_in_publish(self, broker_name):
            return "127.0.0.1:10911"

        def vip_addr(self, addr):
            # The VIP knob defaults off, so the address goes out unchanged.
            return addr

        def _invoke_sync(self, addr, request, timeout):
            captured["ext"] = dict(request.ext_fields)
            captured["header"] = request.custom_header
            captured["addr"] = addr
            return _OkResponse()

        def _check_response(self, response):
            return None

    c = DefaultMQPushConsumer(GROUP)
    c._require_client = lambda: FakeClient()
    m = MessageExt(topic=TOPIC, body=b"x")
    m.broker_name = BROKER
    m.commit_log_offset = 1234
    c.send_message_back(m, 3)
    header: ConsumerSendMsgBackRequestHeader = captured["header"]
    assert header.max_reconsume_times == 16, "concurrent send-back still follows -1 -> 16"
    assert captured["addr"] == "127.0.0.1:10911", "the plain broker address while the VIP channel is off"
    assert c._orderly_max_reconsume_times() == _INT32_MAX, "the orderly side must be" \
        " Integer.MAX_VALUE; the two must not share one parse result"


class _OkResponse:
    code = 0
    remark = ""
    ext_fields: dict = {}
    custom_header = None
    body = b""


# ------------------------------------------------ checkReconsumeTimes's three branches


def test_below_cap_counts_locally_and_suspends():
    h = Harness(max_reconsume_times=3)
    batch = [msg(0, 0), msg(1, 2)]
    assert h.c._check_orderly_reconsume_times(batch) is True
    assert [m.get_reconsume_times() for m in batch] == [1, 3], \
        "the broker side did not record this failure; without a local +1 the threshold can never be reached"
    assert h.sent == [], "not a single send-back while the times are not exhausted"


def test_at_cap_sends_back_and_lets_the_queue_move_on():
    h = Harness(max_reconsume_times=2)
    poison = msg(7, 2)
    poison.put_property(MessageConst.PROPERTY_TRANSACTION_PREPARED, "true")
    poison.put_property(MessageConst.PROPERTY_KEYS, "k7")
    assert h.c._check_orderly_reconsume_times([poison]) is False, \
        "a successful send-back = this message has been handed to the broker; at this point commit the offset instead of retrying in place"
    assert len(h.sent) == 1
    sent = h.sent[0]["msg"]
    assert sent.topic == RETRY_TOPIC, "orderly send-back goes via %RETRY%<group>, not CONSUMER_SEND_MSG_BACK"
    assert sent.body == poison.body
    assert sent.properties[MessageConst.PROPERTY_KEYS] == "k7", "the original properties are carried whole"
    assert sent.properties[MessageConst.PROPERTY_RETRY_TOPIC] == TOPIC
    assert sent.properties[MessageConst.PROPERTY_RECONSUME_TIME] == "3", "carrying the +1 count"
    assert sent.properties[MessageConst.PROPERTY_MAX_RECONSUME_TIMES] == "2"
    assert sent.properties["DELAY"] == str(3 + 2), "delayLevel = 3 + reconsumeTimes"
    assert MessageConst.PROPERTY_TRANSACTION_PREPARED not in sent.properties, \
        "the half-message flag must be cleared, otherwise the broker would treat it as a check-back message"
    assert sent.properties[MessageConst.PROPERTY_ORIGIN_MESSAGE_ID] == poison.msg_id
    assert h.sent[0]["group"] == "CLIENT_INNER_PRODUCER"
    assert poison.get_reconsume_times() == 2, "no more local increments after handing it over"


def test_send_back_failure_keeps_the_queue_suspended():
    h = Harness(max_reconsume_times=2)
    h.fail_send = True
    poison = msg(7, 2)
    assert h.c._check_orderly_reconsume_times([poison]) is True, \
        "when the send-back fails the offset must not advance: this message has gone nowhere and can only stay suspended"
    assert poison.get_reconsume_times() == 3, "increment even on failure, so it can come again next time"
    # WARNING: only one send-back was made: writing the failure criterion backwards (return True without counting) would turn into infinitely re-sending the same message
    assert len(h.sent) == 1


def test_empty_batch_does_not_suspend():
    h = Harness(max_reconsume_times=0)
    assert h.c._check_orderly_reconsume_times([]) is False
    assert h.c._check_orderly_reconsume_times(None) is False


# ------------------------------------------------ Wiring in the consume loop


def test_suspend_requeues_batch_while_below_cap():
    h = Harness(max_reconsume_times=3)
    batch = [msg(0, 0), msg(1, 1)]
    h.c._pending[KEY].append(msg(2, 0))          # there is one more at the queue tail
    assert _consume(h, batch) is False
    assert h.c._consume_offsets[KEY] == 0, "still retrying, the offset must not go past it"
    assert [m.queue_offset for m in h.c._pending[KEY]] == [0, 1, 2], \
        "the whole batch must be pushed back to the queue head in the original order (orderly consumption must not reorder)"


def test_suspend_advances_offset_once_the_poison_message_is_handed_over():
    h = Harness(max_reconsume_times=1)
    batch = [msg(5, 1)]
    assert _consume(h, batch) is True
    assert len(h.sent) == 1
    assert h.c._consume_offsets[KEY] == 6, "after handing it over the offset must advance, or the queue gets stuck"
    assert list(h.c._pending[KEY]) == [], "the queue head must no longer hold it"


def test_suspend_keeps_offset_when_hand_over_fails():
    h = Harness(max_reconsume_times=1)
    h.fail_send = True
    batch = [msg(5, 1)]
    assert _consume(h, batch) is False
    assert h.c._consume_offsets[KEY] == 0
    assert [m.queue_offset for m in h.c._pending[KEY]] == [5]


def test_success_path_is_untouched_by_the_reconsume_gate():
    h = Harness(max_reconsume_times=0)   # threshold 0: once the criterion is reached it will be sent back
    batch = [msg(0, 0)]
    h.c.message_listener = OrderlyListener(ConsumeOrderlyStatus.SUCCESS)
    assert h.c._consume_batch(KEY, h.mq, batch) is True
    assert h.sent == [], "success must not trigger a send-back"
    assert h.c._consume_offsets[KEY] == 1


# ------------------------------------------------ Lifted into the request header (sendKernelImpl:1004-1018)


def _build(topic, properties):
    m = MessageExt(topic=topic, body=b"b")
    m.properties = dict(properties)
    mq = MessageQueue(topic, BROKER, 0)
    inst = MQClientInstance.__new__(MQClientInstance)     # only this header-building stretch is used
    return m, inst._build_send_request("PG", m, mq)


def test_retry_topic_properties_are_lifted_into_the_send_header():
    m, request = _build(RETRY_TOPIC, {
        MessageConst.PROPERTY_RECONSUME_TIME: "4",
        MessageConst.PROPERTY_MAX_RECONSUME_TIMES: "6",
    })
    # ``create_request_command`` only attaches custom_header; ext_fields is not filled until the encoding step,
    # so to_ext_fields() is called explicitly here to see what it really looks like on the wire.
    ext = request.custom_header.to_ext_fields()
    assert ext["j"] == 4, "V2's reconsumeTimes key is j"
    assert ext["l"] == 6, "V2's maxReconsumeTimes key is l"
    # what the broker judges dead letters by is the request header; without this step it would fall back to the subscription group's default retryMaxTimes(16)
    assert request.custom_header.reconsume_times == 4
    assert request.custom_header.max_reconsume_times == 6
    assert m.properties.get(MessageConst.PROPERTY_RECONSUME_TIME) is None, \
        "after lifting, the local property must be cleared (only after clearProperty is this msg reused)"
    assert "RECONSUME_TIME" in ext["i"], "V2's properties key is i"


def test_ordinary_topic_send_is_not_lifted():
    _, request = _build(TOPIC, {
        MessageConst.PROPERTY_RECONSUME_TIME: "4",
        MessageConst.PROPERTY_MAX_RECONSUME_TIMES: "6",
    })
    ext = request.custom_header.to_ext_fields()
    assert ext["j"] == 0
    assert "l" not in ext, \
        "a non-%RETRY% send does not carry maxReconsumeTimes down (always sending 0 would make the first delivery judged over the limit)"


# ------------------------------------------------ Suspend duration: -1 -> config -> [10, 30000]


def test_orderly_context_defaults_are_the_protocol_defaults():
    h = Harness()
    ctx = ConsumeOrderlyContext(h.mq)
    assert ctx.suspend_current_queue_time_millis == -1, "the default -1 = unspecified"
    assert ctx.auto_commit is True


def test_minus_one_falls_back_to_the_consumer_config():
    h = Harness()
    h.c.suspend_current_queue_time_millis = 1000
    ctx = ConsumeOrderlyContext(h.mq)          # default -1 -> falls back to the config
    assert h.c._orderly_suspend_millis(ctx) == 1000


def test_context_value_wins_over_the_consumer_config():
    h = Harness()
    h.c.suspend_current_queue_time_millis = 1000
    ctx = ConsumeOrderlyContext(h.mq)
    ctx.suspend_current_queue_time_millis = 250
    assert h.c._orderly_suspend_millis(ctx) == 250


def test_suspend_millis_is_clamped_to_the_protocol_bounds():
    h = Harness()
    h.c.suspend_current_queue_time_millis = 1000
    ctx = ConsumeOrderlyContext(h.mq)
    for asked, expected in ((0, 10), (9, 10), (10, 10), (250, 250),
                            (30000, 30000), (30001, 30000), (1 << 40, 30000)):
        ctx.suspend_current_queue_time_millis = asked
        assert h.c._orderly_suspend_millis(ctx) == expected, asked
    # the config side (where -1 lands) must also go through clamping
    ctx.suspend_current_queue_time_millis = -1
    h.c.suspend_current_queue_time_millis = 0
    assert h.c._orderly_suspend_millis(ctx) == 10
    h.c.suspend_current_queue_time_millis = 60000
    assert h.c._orderly_suspend_millis(ctx) == 30000


def test_suspend_sleep_uses_the_resolved_millis(monkeypatch):
    h = Harness(max_reconsume_times=3)
    h.c.suspend_current_queue_time_millis = 1000
    slept = []
    monkeypatch.setattr(consumer, "time", _FakeTime(slept))
    assert _consume(h, [msg(0, 0)]) is False
    assert slept == [1.0], "the -1 -> consumer config (1000ms) chain must really land on sleep"


def test_listener_suspend_override_reaches_the_sleep(monkeypatch):
    h = Harness(max_reconsume_times=3)
    h.c.suspend_current_queue_time_millis = 1000
    slept = []
    monkeypatch.setattr(consumer, "time", _FakeTime(slept))
    listener = ScriptedListener(
        ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT,
        mutate=lambda ctx: setattr(ctx, "suspend_current_queue_time_millis", 50))
    assert _consume(h, [msg(0, 0)], listener) is False
    assert slept == [0.05], "the value set by listener on context takes priority over the consumer config"


# ------------------------------------------------ Illegal COMMIT/ROLLBACK under autoCommit=true


@pytest.mark.parametrize("status", [ConsumeOrderlyStatus.COMMIT, ConsumeOrderlyStatus.ROLLBACK])
def test_commit_and_rollback_are_illegal_under_auto_commit(status, caplog):
    pass
    h = Harness(max_reconsume_times=0)   # threshold 0: reaching the send-back criterion sends it out
    batch = [msg(9, 0)]
    with caplog.at_level("WARNING"):
        assert _consume(h, batch, ScriptedListener(status)) is True
    assert h.c._consume_offsets[KEY] == 10, "an illegal status is handled as SUCCESS: the offset advances"
    assert list(h.c._pending[KEY]) == [], "neither sent back nor retried in place"
    assert h.sent == [], "COMMIT/ROLLBACK are not send-back signals"
    assert "the message queue consume result is illegal" in caplog.text


# ------------------------------------------------ autoCommit=false (binlog consumption)


def _manual(status):
    return ScriptedListener(status,
                            mutate=lambda ctx: setattr(ctx, "auto_commit", False))


def test_manual_commit_advances_without_tps():
    stats = RecordingStats()
    h = Harness(stats=stats)
    assert _consume(h, [msg(7, 0)], _manual(ConsumeOrderlyStatus.COMMIT)) is True
    assert h.c._consume_offsets[KEY] == 8, "after commit() the local offset moves forward"
    assert list(h.c._pending[KEY]) == []
    assert stats.ok == [] and stats.failed == [], "the commit branch records not a single TPS"
    assert len(stats.rt) == 1, "RT is recorded as usual, being outside processConsumeResult"


def test_manual_rollback_requeues_without_tps():
    stats = RecordingStats()
    h = Harness(stats=stats)
    assert _consume(h, [msg(7, 0)], _manual(ConsumeOrderlyStatus.ROLLBACK)) is False
    assert h.c._consume_offsets[KEY] == 0
    assert [m.queue_offset for m in h.c._pending[KEY]] == [7]
    assert h.sent == [], "rollback is not a send-back: the message stays in the local queue"
    assert stats.ok == [] and stats.failed == []
    assert len(stats.rt) == 1


def test_manual_success_holds_the_batch_without_committing(monkeypatch):
    pass
    stats = RecordingStats()
    h = Harness(stats=stats)
    h.c.suspend_current_queue_time_millis = 1000
    slept = []
    monkeypatch.setattr(consumer, "time", _FakeTime(slept))
    assert _consume(h, [msg(3, 0)], _manual(ConsumeOrderlyStatus.SUCCESS)) is False
    assert h.c._consume_offsets[KEY] == 0
    assert [m.queue_offset for m in h.c._pending[KEY]] == [3]
    assert slept == [1.0], "without sleeping the next round would immediately get the same batch again: 100% CPU spinning"
    assert stats.ok == [1], "only OK TPS is recorded"
    assert stats.failed == []


def test_manual_suspend_below_cap_requeues():
    stats = RecordingStats()
    h = Harness(max_reconsume_times=3, stats=stats)
    assert _consume(h, [msg(4, 0)],
                    _manual(ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT)) is False
    assert [m.queue_offset for m in h.c._pending[KEY]] == [4]
    assert h.c._consume_offsets[KEY] == 0
    assert stats.failed == [1]
    assert h.sent == [], "times not exhausted yet: no send-back"


def test_manual_suspend_at_cap_does_not_commit_after_hand_over():
    pass
    h = Harness(max_reconsume_times=1)
    assert _consume(h, [msg(5, 1)],
                    _manual(ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT)) is False
    assert len(h.sent) == 1, "times exhausted: hand it to the broker"
    assert h.c._consume_offsets[KEY] == 0, "no commit even after handing it over"
    assert list(h.c._pending[KEY]) == []


# ------------------------------------------------ the listener does not return a valid status


@pytest.mark.parametrize("returned", [None, "SUCCESS", 0])
def test_bogus_listener_return_is_treated_as_suspend(returned):
    pass
    h = Harness(max_reconsume_times=3)
    batch = [msg(2, 0)]
    assert _consume(h, batch, ScriptedListener(returned)) is False
    assert h.c._consume_offsets[KEY] == 0, "it must not be treated as a successful ack"
    assert [m.queue_offset for m in h.c._pending[KEY]] == [2]
    assert h.sent == [], "the threshold has not been reached: retry in place"


def test_listener_exception_suspends_instead_of_acking():
    h = Harness(max_reconsume_times=3)
    assert _consume(h, [msg(2, 0)], ScriptedListener(None, raise_exc=True)) is False
    assert h.c._consume_offsets[KEY] == 0
    assert [m.queue_offset for m in h.c._pending[KEY]] == [2]


# ------------------------------------------------ the status / success / returnType seen by the hook


def _hooked(h, listener):
    hook = RecordingHook()
    h.c.register_consume_message_hook(hook)
    _consume(h, [msg(0, 0)], listener)
    assert len(hook.after) == 1
    return hook.after[0]


def test_orderly_hook_marks_commit_as_success():
    h = Harness()
    status, success, ctx_type = _hooked(h, _manual(ConsumeOrderlyStatus.COMMIT))
    assert status == "COMMIT", "the hook gets the raw status, whose shape is the bare member name"
    assert status != str(ConsumeOrderlyStatus.COMMIT), "it must not carry the Python class-name prefix"
    assert success is True, "the success criterion is SUCCESS || COMMIT"
    assert ctx_type == "SUCCESS"


def test_orderly_hook_marks_suspend_as_failure():
    h = Harness(max_reconsume_times=0)
    status, success, ctx_type = _hooked(h, OrderlyListener())
    assert status == "SUSPEND_CURRENT_QUEUE_A_MOMENT"
    assert success is False
    assert ctx_type == "FAILED"


def test_orderly_hook_sees_success_for_success():
    h = Harness()
    _, success, ctx_type = _hooked(h, ScriptedListener(ConsumeOrderlyStatus.SUCCESS))
    assert success is True
    assert ctx_type == "SUCCESS"


def test_orderly_hook_sees_returnnull_for_a_null_return():
    pass
    h = Harness(max_reconsume_times=3)
    status, success, ctx_type = _hooked(h, ScriptedListener(None))
    assert status == "SUSPEND_CURRENT_QUEUE_A_MOMENT"
    assert success is False
    assert ctx_type == "RETURNNULL", "it must not be recorded as FAILED"


def test_orderly_hook_sees_exception_for_a_raising_listener():
    h = Harness(max_reconsume_times=3)
    status, _, ctx_type = _hooked(h, ScriptedListener(None, raise_exc=True))
    assert status == "SUSPEND_CURRENT_QUEUE_A_MOMENT"
    assert ctx_type == "EXCEPTION"


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-v"]))
