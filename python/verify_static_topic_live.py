#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""The static-topic leg against a real machine.

S1 createStaticTopic(513): a single broker, totalQueues=8, 4 physical queues
   (currIdMap 0-3 -> physical 0-3, 4-7 have no carrier).
S2 Reading the route back: TopicQueueMappingByBroker is not empty.
S3 topicRouteData2EndpointsForStaticTopic: 8 endpoints (0-3 -> broker-a,
   4-7 -> __syslo__none__).
S4 Instance rewriting: getBrokerNameFromMessageQueue resolves a mock queue name to the
   real broker; ids outside the table and non-static topics are passed through.
S5 End to end: an ordinary producer sends 4 messages and a PUSH consumer collects all 4
   (a static topic sends and receives normally through its physical queues).

Usage: python verify_static_topic_live.py [namesrv]
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
    """The TopicQueueMappingDetail document for a single broker (the 513 body; the fastjson2
    field names of the server-side TopicQueueMappingDetail)."""
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
    print("\nS1 starting the admin client, createStaticTopic(513)")
    admin = DefaultMQAdminExt()
    admin.set_namesrv_addr(NAMESRV)
    admin.set_timeout_millis(10000)
    admin.start()
    try:
        info = admin.fetch_broker_cluster_info()
        broker_name = sorted(info.broker_addr_table.keys())[0]
        broker_addr = info.broker_addr_table[broker_name][0]
        check("S1 resolved the broker address", bool(broker_addr), "%s(%s)" % (broker_name, broker_addr))

        config = TopicConfig(TOPIC, PHYSICAL_QUEUES, PHYSICAL_QUEUES)
        admin.create_static_topic(broker_addr, "TBW102", config,
                                  mapping_detail(broker_name), True)
        check("S1 createStaticTopic(513) was accepted", True, "totalQueues=%d" % TOTAL_QUEUES)
    finally:
        admin.shutdown()

    print("\nS2/S3 reading the route back + deriving the endpoint set")
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
    check("S2 the route carries TopicQueueMappingByBroker", route is not None)
    if route is None:
        return 1

    endpoints = topic_route_data_2_endpoints_for_static_topic(TOPIC, route)
    check("S3 the endpoint set is not empty", endpoints is not None)
    if endpoints is None:
        return 1
    mock_name = get_mock_broker_name("__global__")
    check("S3 the total number of endpoints = totalQueues", len(endpoints) == TOTAL_QUEUES,
          "got %d want %d" % (len(endpoints), TOTAL_QUEUES))
    hosted_ok = all(
        endpoints.get(MessageQueue(TOPIC, mock_name, gid)) == broker_name
        for gid in range(PHYSICAL_QUEUES))
    check("S3 hosted ids 0-%d -> broker" % (PHYSICAL_QUEUES - 1), hosted_ok)
    unhosted_ok = all(
        endpoints.get(MessageQueue(TOPIC, mock_name, gid)) == LOGICAL_QUEUE_MOCK_NOT_EXIST
        for gid in range(PHYSICAL_QUEUES, TOTAL_QUEUES))
    check("S3 unhosted ids %d-%d -> __syslo__none__" % (PHYSICAL_QUEUES, TOTAL_QUEUES - 1),
          unhosted_ok)

    print("\nS4/S5 instance rewriting + end-to-end send and receive")
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
    check("S4 the instance rewrites the mock queue to the real broker", rewrite_ok)
    # id TOTAL-1 is inside the endpoint table and its value is __syslo__none__ (a sentinel
    # value: the endpoint value itself is returned); the pass-through case needs an id outside
    # the table (the Go unit test uses 99).
    unhosted_rewrite = inst.get_broker_name_from_message_queue(
        MessageQueue(TOPIC, mock_name, TOTAL_QUEUES - 1)) \
        if inst is not None else None
    check("S4 an unhosted id returns the sentinel value", unhosted_rewrite == LOGICAL_QUEUE_MOCK_NOT_EXIST,
          str(unhosted_rewrite))
    outside_rewrite = inst.get_broker_name_from_message_queue(
        MessageQueue(TOPIC, mock_name, 99)) if inst is not None else None
    check("S4 an id outside the table passes the mock name through", outside_rewrite == mock_name,
          str(outside_rewrite))
    plain_rewrite = inst.get_broker_name_from_message_queue(
        MessageQueue("Plain_%d" % STAMP, broker_name, 0)) \
        if inst is not None else None
    check("S4 a non-static topic is passed through unchanged", plain_rewrite == broker_name, str(plain_rewrite))

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
    check("S5 sent %d messages" % PHYSICAL_QUEUES, sent == PHYSICAL_QUEUES)

    ok = False
    deadline = time.time() + 40
    while time.time() < deadline:
        with rec._lock:
            if len(rec.bodies) >= PHYSICAL_QUEUES:
                ok = True
                break
        time.sleep(1)
    check("S5 the consumer collected all %d messages" % PHYSICAL_QUEUES, ok,
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
