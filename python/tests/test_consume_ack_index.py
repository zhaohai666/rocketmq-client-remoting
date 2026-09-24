# -*- coding: utf-8 -*-
"""并发消费（非 POP 的 classic 路径）的 ``ackIndex`` 语义单测，不需要集群。

为什么单独一个文件：``ConsumeConcurrentlyContext.ack_index`` 早在本端口实现出来了，
但只在 **POP** 路径生效过；classic 路径（``_consume_batch``）整段把它忽略了 ——
listener 设 ``ack_index=0`` 表达「这批只认可第一条」时，尾巴既不会回投 ``%RETRY%``，
位点又照样越过它，等于**静默丢消息**。这类 bug 真机也很难看出来（收条数对得上），
所以全部断言放离线锁死。

对齐 Java ``ConsumeMessageConcurrentlyService#processConsumeResult:202-270``：

  - ``ackIndex`` 默认 ``Integer.MAX_VALUE``（``ConsumeConcurrentlyContext:33``）
  - CONSUME_SUCCESS 钳到 ``size-1``；RECONSUME_LATER 强制 ``-1``（:212-229）
  - CLUSTERING 只回投 ``[ackIndex+1, size)``（:238-254），回投失败的 ``reconsumeTimes+1``
    后重新提交消费（:250-260）
  - BROADCASTING 不回投，未认可的尾巴打 warn 后丢弃（:232-237）
  - 位点提交 = 已处理条目的最大 ``queueOffset+1``，且不越过仍在队列里的那条（:266-269）
"""
from __future__ import annotations

from collections import deque

from rocketmq.client.consumer import DefaultMQPushConsumer
from rocketmq.client.consumer_result import ConsumeConcurrentlyStatus
from rocketmq.client.hook import ConsumeMessageHook
from rocketmq.common.message import MessageExt, MessageQueue
from rocketmq.remoting.protocol.heartbeat import MessageModel

GROUP = "GID_AckIndexUnitTest"
TOPIC = "AckIndexUnitTestTopic"
BROKER = "broker-a"
KEY = "key"


class _RecordingConsumeHook(ConsumeMessageHook):
    """只记 after 钩子看到的 (status, success, ConsumeContextType)。"""

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
    """返回固定状态，并（可选）把 ack_index 写进 context。"""

    def __init__(self, status, ack_index=None):
        self.status = status
        self.ack_index = ack_index

    def consume_message(self, msgs, context):
        if self.ack_index is not None:
            context.ack_index = self.ack_index
        return self.status


class Harness:
    """一个不碰网络的 push consumer：位点/待发队列手动搭好，回投只记录。"""

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


# --------------------------------------------------------------- 默认值


class TestContextDefault:
    def test_default_is_java_max_value(self):
        from rocketmq.client.consumer_result import ConsumeConcurrentlyContext
        # Java ConsumeConcurrentlyContext:33 —— 默认「整批认可」，不是「一条都不认可」
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
        """核心场景：listener 只认可第一条 → 后两条必须回投，而不是被位点越过。"""
        h = Harness()
        listener = FixedListener(ConsumeConcurrentlyStatus.CONSUME_SUCCESS, ack_index=0)
        assert h.consume([msg(0), msg(1), msg(2)], listener) is True
        assert h.backed_offsets == [1, 2]
        # 尾巴已经交给 broker 重投，位点可以整批前进（Java:266 removeMessage(整批)）
        assert h.offset == 3

    def test_negative_ack_index_on_success_redelivers_everything(self):
        h = Harness()
        listener = FixedListener(ConsumeConcurrentlyStatus.CONSUME_SUCCESS, ack_index=-1)
        assert h.consume([msg(0), msg(1)], listener) is True
        assert h.backed_offsets == [0, 1]
        assert h.offset == 2

    def test_send_back_uses_java_delay_gradient(self):
        # Java sendMessageBack:277-283 —— delayLevel 为 0 时取 3 + reconsumeTimes
        h = Harness()
        batch = [msg(0), msg(1, reconsume_times=2), msg(2)]
        h.consume(batch, FixedListener(ConsumeConcurrentlyStatus.CONSUME_SUCCESS, ack_index=0))
        assert [(m.queue_offset, dl) for m, dl in h.backed] == [(1, 5), (2, 3)]

    def test_send_back_failure_requeues_that_message_only(self):
        """回投失败的条目：reconsumeTimes+1、塞回队首、位点不能越过它（Java:250-260,266）。"""
        h = Harness()
        h.fail_for = {1}
        batch = [msg(0), msg(1), msg(2)]
        ok = h.consume(batch, FixedListener(ConsumeConcurrentlyStatus.CONSUME_SUCCESS,
                                           ack_index=0))
        assert ok is False
        assert h.pending_offsets == [1]
        assert batch[1].reconsume_times == 1
        # 认可前缀 0 + 回投成功的 2 → 本来会提交到 3，但 1 还在队里，只能停在 1
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
        # Java:222-226 —— RECONSUME_LATER 把 ackIndex 强制成 -1
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
        """Java:399-405 —— listener 返回 null 按 RECONSUME_LATER 处理。

        钩子里的 ConsumeContextType 用的是归一化**前**的 status（null → RETURNNULL），
        而写进上下文的 status 是归一化后的 RECONSUME_LATER —— 两个值在 Java 里就不同。
        """
        h = Harness()
        hook = _RecordingConsumeHook()
        h.c.register_consume_message_hook(hook)
        assert h.consume([msg(0), msg(1)], FixedListener(None)) is True
        assert h.backed_offsets == [0, 1], "整批回投"
        assert hook.after == [("RECONSUME_LATER", False, "RETURNNULL")], (
            "status 的形态按 Java 的 Enum.toString()（裸成员名），不是 str(枚举) 那种带类名的")

    def test_hook_status_is_java_enum_name_on_the_success_path_too(self):
        """成功路径同样：``ConsumeConcurrentlyService:408`` 写的是 ``CONSUME_SUCCESS``。

        反证「没写错形态」得同时钉住两件事：等于 Java 的裸成员名，且**不等于** Python
        的 ``str(枚举)`` —— 只断言"以 C 开头"之类的模糊判据会让 `"ConsumeConcurrentlyStatus.CONSUME_SUCCESS"`
        这种带类名的形态蒙过去。
        """
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
        """Java:232-237 —— 广播模式一条都不回投，未认可的尾巴只 warn 后丢掉。"""
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
