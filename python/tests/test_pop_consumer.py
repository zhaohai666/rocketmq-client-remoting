# -*- coding: utf-8 -*-
from __future__ import annotations

import threading
import time

import pytest

from client.consumer import (MAX_POP_INVISIBLE_TIME,
                                      MIN_POP_INVISIBLE_TIME, POP_DELAY_LEVEL,
                                      DefaultMQPushConsumer, PopProcessQueue)
from client.consumer_result import (ConsumeConcurrentlyContext,
                                             ConsumeConcurrentlyStatus, PopResult,
                                             PopStatus)
from client.consumer_stats import ConsumerStatsManager
from common.message import MessageExt, MessageQueue
from common.message_const import MessageConst
from common.subscription_data import SubscriptionData
from remoting.protocol.extra_info import build_extra_info

GROUP = "GID_PopUnitTest"
TOPIC = "PopUnitTestTopic"
BROKER = "broker-a"


def ck(queue_id: int = 0, offset: int = 0, retry: str = "0",
       pop_time: int = 1000, invisible: int = 60000, revive_qid: int = 0,
       ck_offset: int = 0) -> str:
    """Assemble a POP_CK. The retry segment determines the result of getRealTopic."""
    seg = [str(ck_offset), str(pop_time), str(invisible), str(revive_qid),
           retry, BROKER, str(queue_id), str(offset)]
    return " ".join(seg)


def msg(topic: str = TOPIC, queue_id: int = 0, queue_offset: int = 0,
        pop_ck: str = None, reconsume_times: int = 0,
        born_timestamp: int = 0) -> MessageExt:
    m = MessageExt(topic=topic, body=b"body")
    m.queue_id = queue_id
    m.queue_offset = queue_offset
    m.reconsume_times = reconsume_times
    m.born_timestamp = born_timestamp
    if pop_ck is not None:
        m.properties[MessageConst.PROPERTY_POP_CK] = pop_ck
    return m


def consumer(**kw) -> DefaultMQPushConsumer:
    c = DefaultMQPushConsumer(GROUP)
    c.pop_mode = True
    for k, v in kw.items():
        setattr(c, k, v)
    return c


class Recording:
    """Replace the two methods that actually send requests and record the calls (offline unit tests cannot network)."""

    def __init__(self, c: DefaultMQPushConsumer):
        self.acked = []
        self.changed = []
        c._ack_pop_msg = self.acked.append  # type: ignore[method-assign]
        c._change_pop_invisible_time = (  # type: ignore[method-assign]
            lambda m, level: self.changed.append((m, level)))


# ---------------------------------------------------------------- Configuration defaults


class TestDefaults:
    def test_pop_mode_off_by_default(self):
        """When off, it must fully follow the original pull path, behaving the same as before the change."""
        assert DefaultMQPushConsumer(GROUP).pop_mode is False

    def test_pop_defaults_match_the_protocol_defaults(self):
        c = DefaultMQPushConsumer(GROUP)
        assert c.pop_invisible_time == 60000
        assert c.pop_batch_nums == 32
        assert c.pop_threshold_for_queue == 96
        assert c.pop_batch_nums <= 32  # the broker side replies INVALID_PARAMETER for >32

    def test_poll_time_must_be_shorter_than_request_timeout(self):
        """The long-polling suspension must be < the request timeout, otherwise the client times out first and spins empty every time."""
        c = DefaultMQPushConsumer(GROUP)
        assert c.pop_poll_time_millis < c.pop_timeout_millis

    def test_delay_level_starts_at_10s(self):
        assert POP_DELAY_LEVEL[0] == 10
        assert list(POP_DELAY_LEVEL) == sorted(POP_DELAY_LEVEL)

    def test_invisible_time_bounds(self):
        assert MIN_POP_INVISIBLE_TIME == 5000
        assert MAX_POP_INVISIBLE_TIME == 300000


# ---------------------------------------------------------------- PopProcessQueue


class TestPopProcessQueue:
    def test_counter_up_and_down(self):
        pq = PopProcessQueue()
        assert pq.wait_ack_count() == 0
        pq.inc_found_msg(3)
        assert pq.wait_ack_count() == 3
        pq.ack()
        pq.ack()
        assert pq.wait_ack_count() == 1

    def test_dec_found_msg_takes_a_negative_invisible_time(self):
        pass
        pq = PopProcessQueue()
        pq.inc_found_msg(5)
        pq.dec_found_msg(-5)
        assert pq.wait_ack_count() == 0

    def test_dropped_flag(self):
        pq = PopProcessQueue()
        assert pq.is_dropped() is False
        pq.set_dropped(True)
        assert pq.is_dropped() is True


# ---------------------------------------------------------------- ackIndex semantics


class TestProcessPopConsumeResult:
    def test_success_acks_everything(self):
        pass
        c = consumer()
        rec = Recording(c)
        msgs = [msg(queue_offset=i, pop_ck=ck(queue_id=0, offset=100 + i))
                for i in range(3)]
        pq = PopProcessQueue()
        pq.inc_found_msg(3)
        ctx = ConsumeConcurrentlyContext()
        ctx.ack_index = len(msgs) - 1
        c._process_pop_consume_result(ConsumeConcurrentlyStatus.CONSUME_SUCCESS,
                                      ctx, msgs, pq, None)
        assert len(rec.acked) == 3
        assert rec.changed == []
        assert pq.wait_ack_count() == 0

    def test_success_with_partial_ack_index(self):
        c = consumer()
        rec = Recording(c)
        msgs = [msg(queue_offset=i, pop_ck=ck(queue_id=0, offset=i))
                for i in range(4)]
        pq = PopProcessQueue()
        pq.inc_found_msg(4)
        ctx = ConsumeConcurrentlyContext()
        ctx.ack_index = 1  # only ack the first two
        c._process_pop_consume_result(ConsumeConcurrentlyStatus.CONSUME_SUCCESS,
                                      ctx, msgs, pq, None)
        assert len(rec.acked) == 2
        # the last two go through invisible-time extension
        assert len(rec.changed) == 2
        assert [m.queue_offset for m, _ in rec.changed] == [2, 3]
        assert pq.wait_ack_count() == 0

    def test_ack_index_beyond_size_is_clamped(self):
        c = consumer()
        rec = Recording(c)
        msgs = [msg(queue_offset=i, pop_ck=ck(queue_id=0, offset=i))
                for i in range(2)]
        pq = PopProcessQueue()
        pq.inc_found_msg(2)
        ctx = ConsumeConcurrentlyContext()
        ctx.ack_index = 99
        c._process_pop_consume_result(ConsumeConcurrentlyStatus.CONSUME_SUCCESS,
                                      ctx, msgs, pq, None)
        assert len(rec.acked) == 2

    def test_reconsume_later_acks_nothing(self):
        c = consumer()
        rec = Recording(c)
        msgs = [msg(queue_offset=i, pop_ck=ck(queue_id=0, offset=i))
                for i in range(3)]
        pq = PopProcessQueue()
        pq.inc_found_msg(3)
        ctx = ConsumeConcurrentlyContext()
        ctx.ack_index = 2  # ineffective even if the listener sets it
        c._process_pop_consume_result(ConsumeConcurrentlyStatus.RECONSUME_LATER,
                                      ctx, msgs, pq, None)
        assert rec.acked == []
        assert len(rec.changed) == 3
        assert pq.wait_ack_count() == 0

    def test_reconsume_later_uses_context_delay_level(self):
        c = consumer()
        rec = Recording(c)
        msgs = [msg(pop_ck=ck())]
        ctx = ConsumeConcurrentlyContext()
        ctx.delay_level_when_next_consume = 2
        c._process_pop_consume_result(ConsumeConcurrentlyStatus.RECONSUME_LATER,
                                      ctx, msgs, PopProcessQueue(), None)
        assert rec.changed == [(msgs[0], 2)]

    def test_max_reconsume_times_reached_routes_to_check_need_ack_or_delay(self):
        """Retries exhausted → no longer the ordinary extension, but checkNeedAckOrDelay.

        Alive for 30s, not exceeding twice the max level → after picking the 30s level, +1 → 60s.
        (Careful not to use born_timestamp=0: then the "alive time" would be decades, and it would be acked and dropped directly.)
        """
        c = wired(max_reconsume_times=2)
        born = int(time.time() * 1000) - 30 * 1000
        msgs = [msg(pop_ck=ck(offset=11), reconsume_times=5, born_timestamp=born)]
        c._process_pop_consume_result(ConsumeConcurrentlyStatus.RECONSUME_LATER,
                                      ConsumeConcurrentlyContext(), msgs,
                                      PopProcessQueue(), None)
        assert c._mq_client.acks == []
        assert c._mq_client.invisible_ms == [(TOPIC, 11, 60 * 1000)]

    def test_over_max_reconsume_and_too_old_is_acked_and_dropped(self):
        """Both exceeding the retry count and being alive more than twice the max level → acked and dropped directly."""
        c = wired(max_reconsume_times=2)
        born = int(time.time() * 1000) - (POP_DELAY_LEVEL[-1] * 2 + 10) * 1000
        msgs = [msg(pop_ck=ck(offset=12), reconsume_times=5, born_timestamp=born)]
        c._process_pop_consume_result(ConsumeConcurrentlyStatus.RECONSUME_LATER,
                                      ConsumeConcurrentlyContext(), msgs,
                                      PopProcessQueue(), None)
        assert c._mq_client.acks == [(TOPIC, 0, 12)]
        assert c._mq_client.invisible_ms == []


# ---------------------------------------------------------------- POP_CK restoration


class TestPopCkTarget:
    def test_normal_topic_roundtrip(self):
        c = consumer()
        m = msg(topic=TOPIC, queue_id=3, pop_ck=ck(queue_id=3, offset=7))
        topic, broker, qid, offset, extra = c._pop_ck_target(m)
        assert (topic, broker, qid, offset) == (TOPIC, BROKER, 3, 7)
        assert extra == m.properties[MessageConst.PROPERTY_POP_CK]

    def test_retry_flag_one_rebuilds_retry_topic(self):
        """retryFlag=1 → the real topic is %RETRY%<group>_<topic> (V1 underscore).

        Even if the topic on the message has already been restored by resetRetryAndNamespace, as long as the CK says 1,
        the ack's target topic must be the one with the %RETRY% prefix.
        """
        c = consumer()
        m = msg(topic=TOPIC, queue_id=1, pop_ck=ck(queue_id=1, offset=2, retry="1"))
        topic, broker, qid, offset, _ = c._pop_ck_target(m)
        assert topic == "%%RETRY%%%s_%s" % (GROUP, TOPIC)
        assert (broker, qid, offset) == (BROKER, 1, 2)

    def test_retry_flag_two_uses_plus_separator(self):
        """V2 uses '+' as the separator (when enableRetryTopicV2=true)."""
        c = consumer()
        m = msg(topic=TOPIC, pop_ck=ck(retry="2"))
        topic, _, _, _, _ = c._pop_ck_target(m)
        assert topic == "%%RETRY%%%s+%s" % (GROUP, TOPIC)

    def test_missing_ck_returns_none(self):
        c = consumer()
        assert c._pop_ck_target(msg()) is None

    def test_short_ck_returns_none(self):
        """With too few segments (<8) it must not throw; the only option is to give up the ack — leave revival to the broker."""
        c = consumer()
        m = msg(pop_ck=build_extra_info(0, 1, 2, 3, TOPIC, BROKER, 4))  # 7 segments
        assert c._pop_ck_target(m) is None

    def test_truncated_garbage_ck_returns_none(self):
        c = consumer()
        m = msg(pop_ck="only two")
        assert c._pop_ck_target(m) is None


# ---------------------------------------------------------------- Delay levels


class FakeClient:
    """MQClientInstance double: records the parameters actually to be sent to the broker (offline cannot network)."""

    def __init__(self):
        self.invisible_ms = []
        self.acks = []

    def pop_ack_addr(self, broker_name, topic, queue_id):
        pass
        return "127.0.0.1:10911"

    def change_invisible_time(self, group, topic, queue_id, extra_info, offset,
                              invisible_time, **kw):
        self.invisible_ms.append((topic, offset, invisible_time))

    def ack_message(self, group, topic, queue_id, extra_info, offset, **kw):
        self.acks.append((topic, queue_id, offset))


def wired(**kw):
    """Build a "started" consumer whose client is a double, so the real outgoing parameters are observable."""
    c = consumer(**kw)
    c._started = True
    c._mq_client = FakeClient()
    return c


class TestDelayLevel:
    def test_delay_level_is_seconds_converted_to_millis(self):
        """The level table's unit is **seconds**, while CHANGE_MESSAGE_INVISIBLETIME wants milliseconds."""
        c = wired()
        c._change_pop_invisible_time(msg(pop_ck=ck(offset=9)), 3)
        # POP_DELAY_LEVEL = (10, 30, 60, 120, ...) → index 3 is 120 seconds
        assert c._mq_client.invisible_ms == [(TOPIC, 9, 120 * 1000)]

    def test_level_zero_falls_back_to_reconsume_times(self):
        pass
        c = wired()
        c._change_pop_invisible_time(msg(pop_ck=ck(offset=1), reconsume_times=3), 0)
        # delayLevel 0 → reconsumeTimes=3 → POP_DELAY_LEVEL[3] = 120 seconds
        assert c._mq_client.invisible_ms == [(TOPIC, 1, 120 * 1000)]

    def test_level_beyond_table_clamps_to_last(self):
        c = wired()
        c._change_pop_invisible_time(msg(pop_ck=ck(offset=2)), 999)
        assert c._mq_client.invisible_ms == [(TOPIC, 2, POP_DELAY_LEVEL[-1] * 1000)]

    def test_check_need_ack_or_delay_drops_when_too_old(self):
        """Alive time > max level * 2 → acked and dropped directly, no more infinite retries."""
        c = consumer()
        rec = Recording(c)
        born = int(time.time() * 1000) - (7200 * 2 + 10) * 1000
        m = msg(pop_ck=ck(), born_timestamp=born)
        c._check_need_ack_or_delay(m)
        assert len(rec.acked) == 1
        assert rec.changed == []

    def test_check_need_ack_or_delay_picks_next_level_up(self):
        """Alive 30s → after finding the 30s level top-down, +1 → the next level 60s."""
        c = wired()
        born = int(time.time() * 1000) - 30 * 1000
        c._check_need_ack_or_delay(msg(pop_ck=ck(offset=5), born_timestamp=born))
        assert c._mq_client.acks == []
        assert c._mq_client.invisible_ms == [(TOPIC, 5, 60 * 1000)]

    def test_check_need_ack_or_delay_very_fresh_message_never_negative(self):
        pass
        c = wired()
        c._check_need_ack_or_delay(
            msg(pop_ck=ck(offset=6), born_timestamp=int(time.time() * 1000)))
        assert c._mq_client.invisible_ms == [(TOPIC, 6, POP_DELAY_LEVEL[0] * 1000)]

    def test_check_need_ack_or_delay_acks_and_drops_when_too_old(self):
        c = wired()
        born = int(time.time() * 1000) - (POP_DELAY_LEVEL[-1] * 2 + 10) * 1000
        c._check_need_ack_or_delay(msg(pop_ck=ck(offset=7), born_timestamp=born))
        assert c._mq_client.acks == [(TOPIC, 0, 7)]
        assert c._mq_client.invisible_ms == []


# ---------------------------------------------------------------- Timeout judgment


class TestIsPopTimeout:
    def test_unparsable_ck_counts_as_timeout(self):
        pass
        assert DefaultMQPushConsumer._is_pop_timeout([msg()], 0, 0) is True
        assert DefaultMQPushConsumer._is_pop_timeout([msg()], 0, 60000) is True
        assert DefaultMQPushConsumer._is_pop_timeout([msg()], 1000, 0) is True

    def test_empty_batch_is_timeout(self):
        assert DefaultMQPushConsumer._is_pop_timeout([], 1000, 60000) is True

    def test_within_window_not_timeout(self):
        now = int(time.time() * 1000)
        assert DefaultMQPushConsumer._is_pop_timeout([msg()], now, 60000) is False

    def test_past_window_is_timeout(self):
        now = int(time.time() * 1000)
        assert DefaultMQPushConsumer._is_pop_timeout([msg()], now - 60001, 60000) is True


# ---------------------------------------------------------------- Batch splitting


class TestSubmitPopConsumeRequest:
    def test_splits_by_batch_max_size(self):
        """consume_message_batch_max_size=4, 10 messages → 4+4+2 three batches."""
        c = consumer(consume_message_batch_max_size=4)
        batches = []
        c._consume_pop_batch = lambda b, pq, mq: batches.append(list(b))
        pq = PopProcessQueue()
        msgs = [msg(queue_offset=i, pop_ck=ck(offset=i)) for i in range(10)]
        # _pop_executor is None → synchronous execution
        c._submit_pop_consume_request(msgs, pq, None)
        assert [len(b) for b in batches] == [4, 4, 2]
        assert [m.queue_offset for b in batches for m in b] == list(range(10))

    def test_single_batch_when_size_exceeds(self):
        c = consumer(consume_message_batch_max_size=32)
        batches = []
        c._consume_pop_batch = lambda b, pq, mq: batches.append(list(b))
        msgs = [msg(queue_offset=i, pop_ck=ck(offset=i)) for i in range(3)]
        c._submit_pop_consume_request(msgs, PopProcessQueue(), None)
        assert len(batches) == 1 and len(batches[0]) == 3

    def test_batch_max_size_zero_is_treated_as_one(self):
        c = consumer(consume_message_batch_max_size=0)
        batches = []
        c._consume_pop_batch = lambda b, pq, mq: batches.append(list(b))
        msgs = [msg(queue_offset=i, pop_ck=ck(offset=i)) for i in range(3)]
        c._submit_pop_consume_request(msgs, PopProcessQueue(), None)
        assert [len(b) for b in batches] == [1, 1, 1]


# ---------------------------------------------------------------- Timeout batches dropped directly


class TestTimeoutBatchDropped:
    def test_expired_batch_is_not_consumed(self):
        """A batch past invisibleTime: not delivered to the listener, and the counter is returned as well."""
        c = consumer()
        seen = []
        c.message_listener = type("L", (), {
            "consume_message": staticmethod(
                lambda msgs, ctx: seen.append(msgs) or ConsumeConcurrentlyStatus.CONSUME_SUCCESS)
        })()
        pq = PopProcessQueue()
        pq.inc_found_msg(2)
        old = int(time.time() * 1000) - 60001
        msgs = [msg(queue_offset=i, pop_ck=ck(pop_time=old, invisible=60000, offset=i))
                for i in range(2)]
        c._consume_pop_batch(msgs, pq, None)
        assert seen == []
        assert pq.wait_ack_count() == 0

    def test_dropped_queue_batch_is_not_consumed(self):
        c = consumer()
        seen = []
        c.message_listener = type("L", (), {
            "consume_message": staticmethod(
                lambda msgs, ctx: seen.append(msgs) or ConsumeConcurrentlyStatus.CONSUME_SUCCESS)
        })()
        pq = PopProcessQueue()
        pq.set_dropped(True)
        now = int(time.time() * 1000)
        msgs = [msg(pop_ck=ck(pop_time=now, invisible=60000))]
        c._consume_pop_batch(msgs, pq, None)
        assert seen == []


# ------------------------------------------------ pullRT / pullTPS of the POP loop


class TestPopLoopPullStats:
    pass

    @staticmethod
    def _run_rounds(results):
        """Run the real `_queue_pop_loop`, feeding the given PopResult round by round, and collect the thread automatically when done."""
        c = consumer()
        c._started = True
        mq = MessageQueue(TOPIC, BROKER, 0)
        key = c._mq_key(mq)
        c._pop_queues[key] = PopProcessQueue()
        c.subscription_data[TOPIC] = SubscriptionData(topic=TOPIC, sub_string="*")
        c._stats_manager = ConsumerStatsManager()
        delivered = []
        c._submit_pop_consume_request = lambda msgs, pq, m: delivered.append(len(msgs))
        pending = list(results)

        class PopClient:
            def pop_message(self, group, topic, queue_id, **kw):
                # At least 20ms per round, otherwise RT rounds to 0 and "recorded" and "recorded a 0" are indistinguishable
                time.sleep(0.02)
                if not pending:
                    c._stop.set()
                    return PopResult(PopStatus.POLLING_NOT_FOUND)
                return pending.pop(0)

        c._mq_client = PopClient()
        thread = threading.Thread(target=c._queue_pop_loop, args=(mq,))
        c._queue_threads[key] = thread
        thread.start()
        thread.join(10)
        assert not thread.is_alive(), "the POP loop did not exit after the results were fed"
        return c, delivered

    @staticmethod
    def _items(c):
        stats = c._stats_manager
        key = "%s@%s" % (TOPIC, GROUP)
        return (stats.topic_and_group_pull_rt.find(key),
                stats.topic_and_group_pull_tps.find(key))

    def test_found_round_records_rt_and_tps(self):
        c, delivered = self._run_rounds(
            [PopResult(PopStatus.FOUND, [msg(queue_offset=1), msg(queue_offset=2)])])
        assert delivered == [2]
        rt, tps = self._items(c)
        assert rt is not None and rt.times == 1 and rt.value > 0, "RT is recorded once for every FOUND round"
        assert tps is not None and tps.times == 1 and tps.value == 2, "TPS is recorded by the number of messages popped"

    def test_found_with_empty_list_records_rt_only(self):
        pass
        c, delivered = self._run_rounds([PopResult(PopStatus.FOUND, [])])
        assert delivered == []
        rt, tps = self._items(c)
        assert rt is not None and rt.times == 1
        assert tps is None, "an empty result produces no TPS entry"

    def test_polling_not_found_records_nothing(self):
        """Long-polling coming back empty-handed (the norm in POP mode) must not enter RT, otherwise the average RT is diluted to a fake low."""
        c, _ = self._run_rounds([PopResult(PopStatus.POLLING_NOT_FOUND)])
        rt, tps = self._items(c)
        assert rt is None and tps is None


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
