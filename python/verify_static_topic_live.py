#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""真机静态主题腿（七端口径的 python 参考版）。

S1 createStaticTopic(513)：单 broker，totalQueues=8，物理 4 队列
   （currIdMap 0-3 → 物理 0-3，4-7 无承载者）。
S2 路由回读：TopicQueueMappingByBroker 非空。
S3 topicRouteData2EndpointsForStaticTopic：8 条端点（0-3 → broker-a，
   4-7 → __syslo__none__）。
S4 实例重写：getBrokerNameFromMessageQueue 对 mock 队列名解析到真实
   broker；表外 id 与非静态主题透传。
S5 端到端：普通生产者发 4 条、PUSH 消费者收齐 4 条（静态主题经物理
   队列可正常收发）。

用法：python verify_static_topic_live.py [namesrv]
"""
from __future__ import annotations

import sys
import threading
import time

sys.path.insert(0, ".")

from client.admin import DefaultMQAdminExt
from client.consumer import DefaultMQPushConsumer
from client.consumer_result import (ConsumeConcurrentlyStatus,
                                    MessageListenerConcurrently)
from client.producer import DefaultMQProducer
from client.static_topic import (LOGICAL_QUEUE_MOCK_NOT_EXIST,
                                 get_mock_broker_name,
                                 topic_route_data_2_endpoints_for_static_topic)
from common.message import Message, MessageQueue
from common.topic_config import TopicConfig

NAMESRV = sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1:9876"
STAMP = int(time.time() * 1000)
TOPIC = "StaticLive_%d" % STAMP
GROUP = "StaticPG_%d" % STAMP
PHYSICAL_QUEUES = 4
TOTAL_QUEUES = 8

PASS = 0
FAIL = 0


def check(name: str, ok: bool, detail: str = "") -> None:
    global PASS, FAIL
    if ok:
        PASS += 1
        print("  [PASS] %s%s" % (name, ("  " + detail) if detail else ""))
    else:
        FAIL += 1
        print("  [FAIL] %s%s" % (name, ("  " + detail) if detail else ""))


class Recorder(MessageListenerConcurrently):
    def __init__(self) -> None:
        self.bodies: list = []
        self._lock = threading.Lock()

    def consume_message(self, msgs, context):
        with self._lock:
            self.bodies.extend(bytes(m.body) for m in msgs)
        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS


def mapping_detail(broker_name: str) -> dict:
    """单 broker 的 TopicQueueMappingDetail 文档（513 body，Java
    TopicQueueMappingDetail 的 fastjson2 字段名）。"""
    hosted = {}
    for gid in range(PHYSICAL_QUEUES):
        hosted[str(gid)] = [{
            "gen": 0,
            "queueId": gid,
            "bname": broker_name,
            "logicOffset": 0,
            "startOffset": 0,
            "endOffset": -1,
            "timeOfStart": -1,
            "timeOfEnd": -1,
        }]
    return {
        "topic": TOPIC,
        "scope": "__global__",
        "totalQueues": TOTAL_QUEUES,
        "bname": broker_name,
        "epoch": 0,
        "dirty": False,
        "currIdMap": {str(g): g for g in range(PHYSICAL_QUEUES)},
        "hostedQueues": hosted,
    }


def main() -> int:
    print("\nS1 起管理端，createStaticTopic(513)")
    admin = DefaultMQAdminExt()
    admin.set_namesrv_addr(NAMESRV)
    admin.set_timeout_millis(10000)
    admin.start()
    try:
        info = admin.fetch_broker_cluster_info()
        broker_name = sorted(info.broker_addr_table.keys())[0]
        broker_addr = info.broker_addr_table[broker_name][0]
        check("S1 broker 地址解析", bool(broker_addr), "%s(%s)" % (broker_name, broker_addr))

        config = TopicConfig(TOPIC, PHYSICAL_QUEUES, PHYSICAL_QUEUES)
        admin.create_static_topic(broker_addr, "TBW102", config,
                                  mapping_detail(broker_name), True)
        check("S1 createStaticTopic(513) 被接受", True, "totalQueues=%d" % TOTAL_QUEUES)
    finally:
        admin.shutdown()

    print("\nS2/S3 路由回读 + 端点集推导")
    route = None
    deadline = time.time() + 30
    while time.time() < deadline:
        probe = DefaultMQAdminExt()
        probe.set_namesrv_addr(NAMESRV)
        probe.set_timeout_millis(10000)
        probe.start()
        try:
            route = probe.examine_topic_route(TOPIC)
            if route.topic_queue_mapping_by_broker:
                break
            route = None
        except BaseException:
            route = None
        finally:
            probe.shutdown()
        time.sleep(2)
    check("S2 路由携带 TopicQueueMappingByBroker", route is not None)
    if route is None:
        return 1

    endpoints = topic_route_data_2_endpoints_for_static_topic(TOPIC, route)
    check("S3 端点集非空", endpoints is not None)
    if endpoints is None:
        return 1
    mock_name = get_mock_broker_name("__global__")
    check("S3 端点总数 = totalQueues", len(endpoints) == TOTAL_QUEUES,
          "got %d want %d" % (len(endpoints), TOTAL_QUEUES))
    hosted_ok = all(
        endpoints.get(MessageQueue(TOPIC, mock_name, gid)) == broker_name
        for gid in range(PHYSICAL_QUEUES))
    check("S3 承载 id 0-%d → broker" % (PHYSICAL_QUEUES - 1), hosted_ok)
    unhosted_ok = all(
        endpoints.get(MessageQueue(TOPIC, mock_name, gid)) == LOGICAL_QUEUE_MOCK_NOT_EXIST
        for gid in range(PHYSICAL_QUEUES, TOTAL_QUEUES))
    check("S3 无承载 id %d-%d → __syslo__none__" % (PHYSICAL_QUEUES, TOTAL_QUEUES - 1),
          unhosted_ok)

    print("\nS4/S5 实例重写 + 端到端收发")
    rec = Recorder()
    consumer = DefaultMQPushConsumer(GROUP)
    consumer.set_namesrv_addr(NAMESRV)
    consumer.set_message_listener(rec)
    consumer.subscribe(TOPIC, "*")
    consumer.start()
    time.sleep(4)

    inst = consumer._mq_client
    rewrite_ok = inst is not None and all(
        inst.get_broker_name_from_message_queue(
            MessageQueue(TOPIC, mock_name, gid)) == broker_name
        for gid in range(PHYSICAL_QUEUES))
    check("S4 实例把 mock 队列重写到真实 broker", rewrite_ok)
    # id TOTAL-1 在端点表内，值就是 __syslo__none__（Java 语义：返回端点值）；
    # 透传用例要用表外 id（Go 单测用 99）。
    unhosted_rewrite = inst.get_broker_name_from_message_queue(
        MessageQueue(TOPIC, mock_name, TOTAL_QUEUES - 1)) \
        if inst is not None else None
    check("S4 无承载 id 返回哨兵值", unhosted_rewrite == LOGICAL_QUEUE_MOCK_NOT_EXIST,
          str(unhosted_rewrite))
    outside_rewrite = inst.get_broker_name_from_message_queue(
        MessageQueue(TOPIC, mock_name, 99)) if inst is not None else None
    check("S4 表外 id 透传 mock 名", outside_rewrite == mock_name,
          str(outside_rewrite))
    plain_rewrite = inst.get_broker_name_from_message_queue(
        MessageQueue("Plain_%d" % STAMP, broker_name, 0)) \
        if inst is not None else None
    check("S4 非静态主题原样透传", plain_rewrite == broker_name, str(plain_rewrite))

    prod = DefaultMQProducer("StaticProd_%d" % STAMP)
    prod.set_namesrv_addr(NAMESRV)
    prod.start()
    sent = 0
    try:
        for i in range(PHYSICAL_QUEUES):
            prod.send(Message(TOPIC, ("static-%d" % i).encode()))
            sent += 1
    finally:
        prod.shutdown()
    check("S5 发送 %d 条" % PHYSICAL_QUEUES, sent == PHYSICAL_QUEUES)

    ok = False
    deadline = time.time() + 40
    while time.time() < deadline:
        with rec._lock:
            if len(rec.bodies) >= PHYSICAL_QUEUES:
                ok = True
                break
        time.sleep(1)
    check("S5 消费收齐 %d 条" % PHYSICAL_QUEUES, ok,
          "got %d" % len(rec.bodies))
    consumer.shutdown()
    return 0 if FAIL == 0 else 1


if __name__ == "__main__":
    code = 1
    try:
        code = main()
    finally:
        print("\n=== RESULT pass=%d fail=%d ===" % (PASS, FAIL))
        sys.exit(code)
