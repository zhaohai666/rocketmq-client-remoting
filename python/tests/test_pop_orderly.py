# -*- coding: utf-8 -*-
"""POP + 顺序监听器（Java ConsumeMessagePopOrderlyService，5.5.0 未完成骨架）本地单测。

Java DefaultMQPushConsumerImpl:960-990 按 listener 类型选服务：顺序监听器 + POP 走
ConsumeMessagePopOrderlyService。上游 5.5.0 那是个未完成骨架（:533 POPTODO）：请求
去重入队后 run() 拿到队列锁就返回 —— 消息**不消费、不 ack**，invisibleTime 到期由
broker 复活重投，宏观表现是「顺序 + POP 收不到消息且积压不消」。

覆盖（对齐 ConsumeMessagePopOrderlyService.java）：
  - submitPopConsumeRequest:161-166 —— 分派进顺序骨架，listener 不被调、不 ack
  - submitConsumeRequest:178-191 —— 去重集按 (pq 引用, mq) 判等
  - ConsumeRequest.run:315-324 —— pq 被撤销才摘请求；活队列上是 no-op
  - 并发分支不受分流影响（回归护栏）

离线锁不住的部分：POP 模式下不发 LOCK/UNLOCK_BATCH_MQ（Java 的 lockAll/unlockAll
只读 processQueueTable），需要真机抓包证明。
"""
from __future__ import annotations

import time

import pytest

from rocketmq.client.consumer import DefaultMQPushConsumer, PopProcessQueue
from rocketmq.client.consumer_result import (ConsumeConcurrentlyStatus,
                                             ConsumeConcurrentlyContext,
                                             ConsumeOrderlyStatus,
                                             MessageListenerConcurrently,
                                             MessageListenerOrderly)
from rocketmq.common.message import MessageExt, MessageQueue
from rocketmq.common.message_const import MessageConst
from rocketmq.remoting.protocol.extra_info import build_extra_info

GROUP = "GID_PopOrderlyUnitTest"
TOPIC = "PopOrderlyUnitTestTopic"
BROKER = "broker-a"


def mq(queue_id: int = 0) -> MessageQueue:
    return MessageQueue(TOPIC, BROKER, queue_id)


def msg(queue_offset: int, with_ck: bool = False) -> MessageExt:
    m = MessageExt(topic=TOPIC, body=b"body-%d" % queue_offset)
    m.broker_name = BROKER
    m.queue_id = 0
    m.queue_offset = queue_offset
    m.msg_id = "pop-orderly-%d" % queue_offset
    if with_ck:
        # pop_time 必须是"刚弹出"（当前时刻），否则 isPopTimeout 会把本批按超时丢弃
        m.properties[MessageConst.PROPERTY_POP_CK] = build_extra_info(
            queue_offset, int(time.time() * 1000), 60000, 0, TOPIC, BROKER, 0, queue_offset)
    return m


class CountingOrderly(MessageListenerOrderly):
    def __init__(self):
        self.calls = 0

    def consume_message(self, msgs, context):
        self.calls += 1
        return ConsumeOrderlyStatus.SUCCESS


class CountingConcurrent(MessageListenerConcurrently):
    def __init__(self):
        self.calls = 0

    def consume_message(self, msgs, context):
        self.calls += 1
        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS


def fresh_consumer(listener) -> DefaultMQPushConsumer:
    c = DefaultMQPushConsumer(GROUP)
    c.pop_mode = True
    c.message_listener = listener
    return c


def test_orderly_dispatch_never_invokes_listener():
    listener = CountingOrderly()
    c = fresh_consumer(listener)
    pq = PopProcessQueue()
    pq.inc_found_msg(2)
    c._submit_pop_consume_request([msg(0), msg(1)], pq, mq())
    assert listener.calls == 0, "listener must never be invoked by the skeleton"
    assert pq.wait_ack_count() == 2, "skeleton must not ack or extend invisible time"
    assert c.pop_orderly_request_count() == 1, "non-dropped request stays in the set"


def test_concurrent_dispatch_still_consumes():
    listener = CountingConcurrent()
    c = fresh_consumer(listener)
    pq = PopProcessQueue()
    pq.inc_found_msg(1)
    c._submit_pop_consume_request([msg(0, with_ck=True)], pq, mq())
    assert listener.calls == 1
    assert pq.wait_ack_count() == 0, "CONSUME_SUCCESS acks the whole batch"


def test_request_dedup():
    c = fresh_consumer(CountingOrderly())
    pq = PopProcessQueue()
    c._submit_pop_orderly_request(pq, mq())
    c._submit_pop_orderly_request(pq, mq())
    assert c.pop_orderly_request_count() == 1
    c._submit_pop_orderly_request(pq, mq(1))
    assert c.pop_orderly_request_count() == 2, "different mq is a new request"
    fresh_pq = PopProcessQueue()
    c._submit_pop_orderly_request(fresh_pq, mq())
    assert c.pop_orderly_request_count() == 3, "new pq (rebalance) is a new request"
    c._submit_pop_orderly_request(pq, mq(), force=True)
    assert c.pop_orderly_request_count() == 3, "force re-runs but the set stays single"


def test_dropped_request_is_removed():
    listener = CountingOrderly()
    c = fresh_consumer(listener)
    pq = PopProcessQueue()
    c._submit_pop_orderly_request(pq, mq())
    assert c.pop_orderly_request_count() == 1
    pq.set_dropped(True)
    c._run_pop_orderly_request(pq, mq())
    assert c.pop_orderly_request_count() == 0, "dropped pq removes its request"
    assert listener.calls == 0
    c._submit_pop_orderly_request(pq, mq())
    assert c.pop_orderly_request_count() == 0, "submit on dropped pq self-cleans"


def test_run_on_live_queue_is_noop():
    listener = CountingOrderly()
    c = fresh_consumer(listener)
    pq = PopProcessQueue()
    pq.inc_found_msg(3)
    c._submit_pop_orderly_request(pq, mq())
    c._run_pop_orderly_request(pq, mq())
    assert c.pop_orderly_request_count() == 1
    assert pq.wait_ack_count() == 3
    assert listener.calls == 0
