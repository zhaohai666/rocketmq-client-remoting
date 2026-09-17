# -*- coding: utf-8 -*-
"""POP **消费侧**（DefaultMQPushConsumer.pop_mode）本地单测，不需要集群。

为什么单独一个文件：协议管道（`test_pop.py`）过了 ≠ 消费循环对。消费侧有自己一套
容易错的语义，而且错得**很安静** —— 比如 ackIndex 默认值不对，CONSUME_SUCCESS 会
一条都不 ack，消息在 invisibleTime 到期后被 broker 复活重投；如果观测窗口比
invisibleTime 短，真机看起来还是"全过"。所以这些必须离线锁死。

覆盖：
  - PopProcessQueue 计数与 dropped 语义
  - ackIndex 语义（全 ack / 部分 ack / RECONSUME_LATER 不 ack）
  - POP_CK → (topic, brokerName, queueId, offset) 的还原，含 retry topic 反解
  - 延迟档位选择（Java checkNeedAckOrDelay / changePopInvisibleTime）
  - isPopTimeout 判定
  - 切批（consume_message_batch_max_size）
"""
from __future__ import annotations

import time

import pytest

from rocketmq.client.consumer import (MAX_POP_INVISIBLE_TIME,
                                      MIN_POP_INVISIBLE_TIME, POP_DELAY_LEVEL,
                                      DefaultMQPushConsumer, PopProcessQueue)
from rocketmq.client.consumer_result import (ConsumeConcurrentlyContext,
                                             ConsumeConcurrentlyStatus)
from rocketmq.common.message import MessageExt
from rocketmq.common.message_const import MessageConst
from rocketmq.remoting.protocol.extra_info import build_extra_info

GROUP = "GID_PopUnitTest"
TOPIC = "PopUnitTestTopic"
BROKER = "broker-a"


def ck(queue_id: int = 0, offset: int = 0, retry: str = "0",
       pop_time: int = 1000, invisible: int = 60000, revive_qid: int = 0,
       ck_offset: int = 0) -> str:
    """拼一条 POP_CK。retry 段决定 getRealTopic 的结果。"""
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
    """替换掉真正发请求的两个方法，记录调用（离线单测不能联网）。"""

    def __init__(self, c: DefaultMQPushConsumer):
        self.acked = []
        self.changed = []
        c._ack_pop_msg = self.acked.append  # type: ignore[method-assign]
        c._change_pop_invisible_time = (  # type: ignore[method-assign]
            lambda m, level: self.changed.append((m, level)))


# ---------------------------------------------------------------- 配置默认值


class TestDefaults:
    def test_pop_mode_off_by_default(self):
        """关掉时必须完全走原来的 pull 路径，行为与改动前一致。"""
        assert DefaultMQPushConsumer(GROUP).pop_mode is False

    def test_pop_defaults_match_java(self):
        c = DefaultMQPushConsumer(GROUP)
        # Java popInvisibleTime=60000 / popBatchNums=32 / popThresholdForQueue=96
        assert c.pop_invisible_time == 60000
        assert c.pop_batch_nums == 32
        assert c.pop_threshold_for_queue == 96
        assert c.pop_batch_nums <= 32  # broker 侧 >32 会回 INVALID_PARAMETER

    def test_poll_time_must_be_shorter_than_request_timeout(self):
        """长轮询挂起时长必须 < 请求超时，否则客户端先超时、每次都空转。"""
        c = DefaultMQPushConsumer(GROUP)
        assert c.pop_poll_time_millis < c.pop_timeout_millis

    def test_delay_level_starts_at_10s(self):
        # Java POP_DELAY_LEVEL 首档 10s（send 的延迟档位首档是 1s，别混）
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

    def test_dec_found_msg_takes_negative_like_java(self):
        """Java 是 decFoundMsg(-msgs.size())，这里按"减多少"理解。"""
        pq = PopProcessQueue()
        pq.inc_found_msg(5)
        pq.dec_found_msg(-5)
        assert pq.wait_ack_count() == 0

    def test_dropped_flag(self):
        pq = PopProcessQueue()
        assert pq.is_dropped() is False
        pq.set_dropped(True)
        assert pq.is_dropped() is True


# ---------------------------------------------------------------- ackIndex 语义


class TestProcessPopConsumeResult:
    def test_success_acks_everything(self):
        """回归守卫：Java ackIndex 默认 Integer.MAX_VALUE = 全 ack。

        本项目 ConsumeConcurrentlyContext 默认 -1（push 回投语义），POP 路径若不改成
        size-1，这里会一条都不 ack —— 真机上表现为 invisibleTime 后整批复活重投。
        """
        c = consumer()
        rec = Recording(c)
        msgs = [msg(queue_offset=i, pop_ck=ck(queue_id=0, offset=100 + i))
                for i in range(3)]
        pq = PopProcessQueue()
        pq.inc_found_msg(3)
        ctx = ConsumeConcurrentlyContext()
        ctx.ack_index = len(msgs) - 1  # Java 默认 MAX_VALUE 钳到这里
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
        ctx.ack_index = 1  # 只 ack 前两条
        c._process_pop_consume_result(ConsumeConcurrentlyStatus.CONSUME_SUCCESS,
                                      ctx, msgs, pq, None)
        assert len(rec.acked) == 2
        # 后两条走延长不可见时间
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
        ctx.ack_index = 2  # 即使 listener 设了也无效
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
        """重试次数用尽 → 不再走普通延长，改走 checkNeedAckOrDelay。

        存活 30s、未超过最大档 2 倍 → 选中 30s 那一档后 +1 → 60s。
        （注意别用 born_timestamp=0：那样"存活时间"是几十年，会直接 ack 丢弃。）
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
        """既要超重试次数、又要存活超过最大档 2 倍 → 直接 ack 丢弃。"""
        c = wired(max_reconsume_times=2)
        born = int(time.time() * 1000) - (POP_DELAY_LEVEL[-1] * 2 + 10) * 1000
        msgs = [msg(pop_ck=ck(offset=12), reconsume_times=5, born_timestamp=born)]
        c._process_pop_consume_result(ConsumeConcurrentlyStatus.RECONSUME_LATER,
                                      ConsumeConcurrentlyContext(), msgs,
                                      PopProcessQueue(), None)
        assert c._mq_client.acks == [(TOPIC, 0, 12)]
        assert c._mq_client.invisible_ms == []


# ---------------------------------------------------------------- POP_CK 还原


class TestPopCkTarget:
    def test_normal_topic_roundtrip(self):
        c = consumer()
        m = msg(topic=TOPIC, queue_id=3, pop_ck=ck(queue_id=3, offset=7))
        topic, broker, qid, offset, extra = c._pop_ck_target(m)
        assert (topic, broker, qid, offset) == (TOPIC, BROKER, 3, 7)
        assert extra == m.properties[MessageConst.PROPERTY_POP_CK]

    def test_retry_flag_one_rebuilds_retry_topic(self):
        """retryFlag=1 → 真实 topic 是 %RETRY%<group>_<topic>（V1 下划线）。

        即使消息上的 topic 已经被 resetRetryAndNamespace 还原过，只要 CK 里是 1，
        ack 的目标 topic 也必须是带 %RETRY% 前缀的那个。
        """
        c = consumer()
        m = msg(topic=TOPIC, queue_id=1, pop_ck=ck(queue_id=1, offset=2, retry="1"))
        topic, broker, qid, offset, _ = c._pop_ck_target(m)
        assert topic == "%%RETRY%%%s_%s" % (GROUP, TOPIC)
        assert (broker, qid, offset) == (BROKER, 1, 2)

    def test_retry_flag_two_uses_plus_separator(self):
        """V2 用 '+' 分隔（enableRetryTopicV2=true 时）。"""
        c = consumer()
        m = msg(topic=TOPIC, pop_ck=ck(retry="2"))
        topic, _, _, _, _ = c._pop_ck_target(m)
        assert topic == "%%RETRY%%%s+%s" % (GROUP, TOPIC)

    def test_missing_ck_returns_none(self):
        c = consumer()
        assert c._pop_ck_target(msg()) is None

    def test_short_ck_returns_none(self):
        """段数不够（<8）时不能抛，只能放弃 ack —— 交给 broker 复活。"""
        c = consumer()
        m = msg(pop_ck=build_extra_info(0, 1, 2, 3, TOPIC, BROKER, 4))  # 7 段
        assert c._pop_ck_target(m) is None

    def test_truncated_garbage_ck_returns_none(self):
        c = consumer()
        m = msg(pop_ck="only two")
        assert c._pop_ck_target(m) is None


# ---------------------------------------------------------------- 延迟档位


class FakeClient:
    """替身 MQClientInstance：记录真正要发到 broker 的参数（离线不能联网）。"""

    def __init__(self):
        self.invisible_ms = []
        self.acks = []

    def broker_addr_of(self, broker_name):
        return "127.0.0.1:10911"

    def change_invisible_time(self, group, topic, queue_id, extra_info, offset,
                              invisible_time, **kw):
        self.invisible_ms.append((topic, offset, invisible_time))

    def ack_message(self, group, topic, queue_id, extra_info, offset, **kw):
        self.acks.append((topic, queue_id, offset))


def wired(**kw):
    """造一个"已启动"的消费者，client 是替身，可观测真实出参。"""
    c = consumer(**kw)
    c._started = True
    c._mq_client = FakeClient()
    return c


class TestDelayLevel:
    def test_delay_level_is_seconds_converted_to_millis(self):
        """档位表单位是**秒**，CHANGE_MESSAGE_INVISIBLETIME 要的是毫秒。"""
        c = wired()
        c._change_pop_invisible_time(msg(pop_ck=ck(offset=9)), 3)
        # POP_DELAY_LEVEL = (10, 30, 60, 120, ...) → 索引 3 是 120 秒
        assert c._mq_client.invisible_ms == [(TOPIC, 9, 120 * 1000)]

    def test_level_zero_falls_back_to_reconsume_times(self):
        """Java changePopInvisibleTime：0 == delayLevel → 用 msg.reconsumeTimes。"""
        c = wired()
        c._change_pop_invisible_time(msg(pop_ck=ck(offset=1), reconsume_times=3), 0)
        # delayLevel 0 → reconsumeTimes=3 → POP_DELAY_LEVEL[3] = 120 秒
        assert c._mq_client.invisible_ms == [(TOPIC, 1, 120 * 1000)]

    def test_level_beyond_table_clamps_to_last(self):
        c = wired()
        c._change_pop_invisible_time(msg(pop_ck=ck(offset=2)), 999)
        assert c._mq_client.invisible_ms == [(TOPIC, 2, POP_DELAY_LEVEL[-1] * 1000)]

    def test_check_need_ack_or_delay_drops_when_too_old(self):
        """存活时间 > 最大档 * 2 → 直接 ack 丢弃，不再无限重试。"""
        c = consumer()
        rec = Recording(c)
        born = int(time.time() * 1000) - (7200 * 2 + 10) * 1000
        m = msg(pop_ck=ck(), born_timestamp=born)
        c._check_need_ack_or_delay(m)
        assert len(rec.acked) == 1
        assert rec.changed == []

    def test_check_need_ack_or_delay_picks_next_level_up(self):
        """存活 30s → 自顶向下找到 30s 那一档后 +1 → 下一档 60s。"""
        c = wired()
        born = int(time.time() * 1000) - 30 * 1000
        c._check_need_ack_or_delay(msg(pop_ck=ck(offset=5), born_timestamp=born))
        assert c._mq_client.acks == []
        assert c._mq_client.invisible_ms == [(TOPIC, 5, 60 * 1000)]

    def test_check_need_ack_or_delay_very_fresh_message_never_negative(self):
        """存活时间小于首档时 Java 会算出 delayLevel=-1 并 ArrayIndexOutOfBounds。

        这里钳到首档（10s），是**有意偏离 Java**：不为了"对齐"而去索引 table[-1]。
        """
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


# ---------------------------------------------------------------- 超时判定


class TestIsPopTimeout:
    def test_unparsable_ck_counts_as_timeout(self):
        """Java isPopTimeout：解析不出 popTime/invisibleTime 就按超时处理。"""
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


# ---------------------------------------------------------------- 切批


class TestSubmitPopConsumeRequest:
    def test_splits_by_batch_max_size(self):
        """consume_message_batch_max_size=4，10 条 → 4+4+2 三批。"""
        c = consumer(consume_message_batch_max_size=4)
        batches = []
        c._consume_pop_batch = lambda b, pq, mq: batches.append(list(b))
        pq = PopProcessQueue()
        msgs = [msg(queue_offset=i, pop_ck=ck(offset=i)) for i in range(10)]
        # _pop_executor 为 None → 同步执行
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


# ---------------------------------------------------------------- 超时批直接丢弃


class TestTimeoutBatchDropped:
    def test_expired_batch_is_not_consumed(self):
        """已超过 invisibleTime 的批次：不投递 listener，计数器也要归还。"""
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


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
