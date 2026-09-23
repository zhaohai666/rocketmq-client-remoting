# -*- coding: utf-8 -*-
"""顺序消费的 ``checkReconsumeTimes`` / 回投语义单测，不需要集群。

对齐 Java ``ConsumeMessageOrderlyService``（:236-360）：

  - ``processConsumeResult``:254-266 —— ``SUSPEND_CURRENT_QUEUE_A_MOMENT`` 先过
    ``checkReconsumeTimes``；返回 true 才 ``makeMessageToConsumeAgain`` + 延后重试，
    返回 false 走 ``commit()``，位点前进
  - ``getMaxReconsumeTimes``:313-320 —— 顺序侧 ``-1`` 读成 ``Integer.MAX_VALUE``
    （**不是**并发侧的 16）
  - ``checkReconsumeTimes``:322-336 —— 没用尽就本地 ``reconsumeTimes+1`` 并挂起；
    用尽则回投，**只有回投失败**才算挂起
  - ``sendMessageBack``:338-360 —— 走内部生产者把消息当**普通消息**发到
    ``%RETRY%<group>``（属性置法见下），不是 ``CONSUMER_SEND_MSG_BACK(3)``

外加 Java ``DefaultMQProducerImpl.sendKernelImpl:1004-1018`` 的那次"抬进请求头"：
broker 的 ``handleRetryAndDLQ``（``SendMessageProcessor:197-210``）读的是
``requestHeader.reconsumeTimes`` / ``maxReconsumeTimes``，不是报文里的属性。

为什么全部放离线：这三处坏掉都是**静默**的。少 +1 就永远到不了阈值；抬错字段
broker 就拿订阅组默认的 16 判定；回投成功后还挂起，则一条毒消息永久占住那条队列
（顺序消费的 head-of-line blocking 在真机上表现为"这个组就停在第 N 条不动了"，
和"消费者挂了"看起来一模一样）。
"""
from __future__ import annotations

from collections import deque

import pytest

from rocketmq.client.consumer import (_JAVA_INT_MAX, DefaultMQPushConsumer,
                                     MessageListenerOrderly)
from rocketmq.client.consumer_result import ConsumeOrderlyStatus
from rocketmq.client.mq_client import MQClientInstance
from rocketmq.common.message import MessageExt, MessageQueue
from rocketmq.common.message_const import MessageConst
from rocketmq.remoting.protocol.headers import ConsumerSendMsgBackRequestHeader

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
    """永远失败的顺序监听器（真实场景里就是那条毒消息）。"""

    def __init__(self, status=ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT):
        self.status = status
        self.calls = 0

    def consume_message(self, msgs, context):
        self.calls += 1
        return self.status


class Harness:
    """不碰网络的顺序消费者：内部生产者只记录发出去的那条消息。"""

    def __init__(self, max_reconsume_times: int = -1) -> None:
        self.c = DefaultMQPushConsumer(GROUP)
        self.c.max_reconsume_times = max_reconsume_times
        self.c.suspend_current_queue_time_millis = 0   # 单测不干等 1s
        self.c._pending[KEY] = deque()
        self.c._consume_offsets[KEY] = 0
        self.sent = []
        self.fail_send = False
        self.mq = MessageQueue(TOPIC, BROKER, 0)
        self.c._require_client = lambda: self          # 顺序回投拿"实例"就走这里

    def get_topic_publish_info(self, topic, is_default=False):
        # 路由照常给出：回投失败要模拟的是**发送**那一步，不是选不到队列，
        # 否则异常在 send_message 之前就抛出，判据就测不到真正的那条分支。
        return _PublishInfo(self.mq)

    def send_message(self, producer_group, message, mq, timeout_millis=3000,
                     sys_flag=0, unit_mode=False, **kw):
        # 记下调用口径：内部生产者组名与 unitMode 都要跟着消费者
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


def _consume(h: Harness, batch):
    listener = OrderlyListener()
    h.c.message_listener = listener
    return h.c._consume_batch(KEY, h.mq, batch)


# ------------------------------------------------ -1 的两套含义（顺序 vs 并发）


def test_minus_one_is_unlimited_for_orderly_consumption():
    h = Harness(max_reconsume_times=-1)
    assert h.c._orderly_max_reconsume_times() == _JAVA_INT_MAX
    # 默认配置下毒消息不该被判定"用尽"：本地计数永远追不上 MAX_VALUE
    batch = [msg(0, reconsume_times=_JAVA_INT_MAX - 1)]
    assert h.c._check_orderly_reconsume_times(batch) is True
    assert batch[0].get_reconsume_times() == _JAVA_INT_MAX
    assert h.sent == []


def test_explicit_cap_is_used_as_is():
    h = Harness(max_reconsume_times=3)
    assert h.c._orderly_max_reconsume_times() == 3


def test_orderly_cap_does_not_reuse_the_concurrent_16_default():
    """并发侧 ``-1 → 16``（``DefaultMQPushConsumerImpl#getMaxReconsumeTimes:890``）与顺序侧
    ``-1 → Integer.MAX_VALUE`` 是**两套**约定。并成一个常量会让顺序消费凭空造死信，
    或让并发消息无限重投 —— 所以两边各测一次真实落到请求头上的值。"""
    captured = {}

    class FakeClient:
        def broker_addr_of(self, broker_name):
            return "127.0.0.1:10911"

        def _invoke_sync(self, addr, request, timeout):
            captured["ext"] = dict(request.ext_fields)
            captured["header"] = request.custom_header
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
    assert header.max_reconsume_times == 16, "并发回投仍按 Java 的 -1 → 16"
    assert c._orderly_max_reconsume_times() == _JAVA_INT_MAX, "顺序侧必须是" \
        " Integer.MAX_VALUE，两者不能共用一个解析结果"


class _OkResponse:
    code = 0
    remark = ""
    ext_fields: dict = {}
    custom_header = None
    body = b""


# ------------------------------------------------ checkReconsumeTimes 三条分支


def test_below_cap_counts_locally_and_suspends():
    h = Harness(max_reconsume_times=3)
    batch = [msg(0, 0), msg(1, 2)]
    assert h.c._check_orderly_reconsume_times(batch) is True
    assert [m.get_reconsume_times() for m in batch] == [1, 3], \
        "broker 侧没记这次失败，客户端不就地 +1 就永远到不了阈值"
    assert h.sent == [], "没用尽时一条都不该回投"


def test_at_cap_sends_back_and_lets_the_queue_move_on():
    h = Harness(max_reconsume_times=2)
    poison = msg(7, 2)
    poison.put_property(MessageConst.PROPERTY_TRANSACTION_PREPARED, "true")
    poison.put_property(MessageConst.PROPERTY_KEYS, "k7")
    assert h.c._check_orderly_reconsume_times([poison]) is False, \
        "回投成功 = 这条已经交给 broker，Java 这时 commit 位点而不是继续原地重试"
    assert len(h.sent) == 1
    sent = h.sent[0]["msg"]
    assert sent.topic == RETRY_TOPIC, "顺序回投走%RETRY%<group>，不是 CONSUMER_SEND_MSG_BACK"
    assert sent.body == poison.body
    assert sent.properties[MessageConst.PROPERTY_KEYS] == "k7", "原属性整份带上"
    assert sent.properties[MessageConst.PROPERTY_RETRY_TOPIC] == TOPIC
    assert sent.properties[MessageConst.PROPERTY_RECONSUME_TIME] == "3", "带 +1 的次数"
    assert sent.properties[MessageConst.PROPERTY_MAX_RECONSUME_TIMES] == "2"
    assert sent.properties["DELAY"] == str(3 + 2), "delayLevel = 3 + reconsumeTimes"
    assert MessageConst.PROPERTY_TRANSACTION_PREPARED not in sent.properties, \
        "半消息标记必须清掉，否则 broker 会把它当回查消息"
    assert sent.properties[MessageConst.PROPERTY_ORIGIN_MESSAGE_ID] == poison.msg_id
    assert h.sent[0]["group"] == "CLIENT_INNER_PRODUCER"
    assert poison.get_reconsume_times() == 2, "交出去之后不再本地累加"


def test_send_back_failure_keeps_the_queue_suspended():
    h = Harness(max_reconsume_times=2)
    h.fail_send = True
    poison = msg(7, 2)
    assert h.c._check_orderly_reconsume_times([poison]) is True, \
        "回投失败时不能前进位点：这条消息哪儿也没去，只能继续挂着"
    assert poison.get_reconsume_times() == 3, "Java :330 失败也 +1，下次再来"
    # ⚠ 只回投了一次：把失败判据写反（return True 而不计数）会变成无限重复投同一条
    assert len(h.sent) == 1


def test_empty_batch_does_not_suspend():
    h = Harness(max_reconsume_times=0)
    assert h.c._check_orderly_reconsume_times([]) is False
    assert h.c._check_orderly_reconsume_times(None) is False


# ------------------------------------------------ 消费循环里的接线


def test_suspend_requeues_batch_while_below_cap():
    h = Harness(max_reconsume_times=3)
    batch = [msg(0, 0), msg(1, 1)]
    h.c._pending[KEY].append(msg(2, 0))          # 队尾还有下一条
    assert _consume(h, batch) is False
    assert h.c._consume_offsets[KEY] == 0, "还在重试，位点不能越过它"
    assert [m.queue_offset for m in h.c._pending[KEY]] == [0, 1, 2], \
        "整批要按原顺序塞回队首（顺序消费不许乱序）"


def test_suspend_advances_offset_once_the_poison_message_is_handed_over():
    h = Harness(max_reconsume_times=1)
    batch = [msg(5, 1)]
    assert _consume(h, batch) is True
    assert len(h.sent) == 1
    assert h.c._consume_offsets[KEY] == 6, "交出去之后位点必须前进，队列才不被堵住"
    assert list(h.c._pending[KEY]) == [], "队首不能还留着它"


def test_suspend_keeps_offset_when_hand_over_fails():
    h = Harness(max_reconsume_times=1)
    h.fail_send = True
    batch = [msg(5, 1)]
    assert _consume(h, batch) is False
    assert h.c._consume_offsets[KEY] == 0
    assert [m.queue_offset for m in h.c._pending[KEY]] == [5]


def test_success_path_is_untouched_by_the_reconsume_gate():
    h = Harness(max_reconsume_times=0)   # 阈值 0：一旦走到判据就会回投
    batch = [msg(0, 0)]
    h.c.message_listener = OrderlyListener(ConsumeOrderlyStatus.SUCCESS)
    assert h.c._consume_batch(KEY, h.mq, batch) is True
    assert h.sent == [], "成功不该触发回投"
    assert h.c._consume_offsets[KEY] == 1


# ------------------------------------------------ 抬进请求头（sendKernelImpl:1004-1018）


def _build(topic, properties):
    m = MessageExt(topic=topic, body=b"b")
    m.properties = dict(properties)
    mq = MessageQueue(topic, BROKER, 0)
    inst = MQClientInstance.__new__(MQClientInstance)     # 只用建头这一段
    return m, inst._build_send_request("PG", m, mq)


def test_retry_topic_properties_are_lifted_into_the_send_header():
    m, request = _build(RETRY_TOPIC, {
        MessageConst.PROPERTY_RECONSUME_TIME: "4",
        MessageConst.PROPERTY_MAX_RECONSUME_TIMES: "6",
    })
    # ``create_request_command`` 只挂 custom_header，ext_fields 要到编码那一步才填，
    # 所以这里显式调 to_ext_fields() 看线上真正长什么样。
    ext = request.custom_header.to_ext_fields()
    assert ext["j"] == 4, "V2 的 reconsumeTimes 键是 j"
    assert ext["l"] == 6, "V2 的 maxReconsumeTimes 键是 l"
    # broker 判死信看的是请求头；少了这一步它会退回订阅组默认的 retryMaxTimes(16)
    assert request.custom_header.reconsume_times == 4
    assert request.custom_header.max_reconsume_times == 6
    assert m.properties.get(MessageConst.PROPERTY_RECONSUME_TIME) is None, \
        "抬完要清掉本地属性（Java 在 clearProperty 之后才复用这条 msg）"
    # ⚠ 线上属性里 RECONSUME_TIME **仍在**：Java 先序列化 properties 再抬字段（顺序照抄，
    #    改成"先抬后编"会让消费端读不到 broker 写的重试次数）
    assert "RECONSUME_TIME" in ext["i"], "V2 的 properties 键是 i"


def test_ordinary_topic_send_is_not_lifted():
    _, request = _build(TOPIC, {
        MessageConst.PROPERTY_RECONSUME_TIME: "4",
        MessageConst.PROPERTY_MAX_RECONSUME_TIMES: "6",
    })
    ext = request.custom_header.to_ext_fields()
    assert ext["j"] == 0
    assert "l" not in ext, \
        "非 %RETRY% 发送不下发 maxReconsumeTimes（固定发 0 会让首投就判超限）"


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-v"]))
