# -*- coding: utf-8 -*-
"""Multi-NameServer failover reproduction: after namesrv[0] is killed the producer should be able to
switch to the surviving node, and consumer start() should not raise RemotingConnectException."""
import os
import sys
import time
import traceback

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from client.consumer import DefaultMQPushConsumer
from client.producer import DefaultMQProducer
from common.message import Message
from remoting.exception import RemotingException
from remoting.protocol.heartbeat import ConsumeFromWhere

# ns[0] is a port with nothing listening on it (equivalent to "killing nameServ[0]"), ns[1] is the real running 9876
NS = "127.0.0.1:19876;127.0.0.1:9876"
STAMP = int(time.time())
TOPIC = "NsFailover_%d" % STAMP


def step(name, fn):
    try:
        fn()
        print("STEP-OK   %s" % name)
        return True
    except BaseException as e:  # noqa: BLE001
        print("STEP-FAIL %s: %s: %s" % (name, type(e).__name__, e))
        traceback.print_exc()
        return False


def produce(tag):
    p = DefaultMQProducer("PG_NsFail_%d" % STAMP)
    p.set_namesrv_addr(NS)
    p.start()
    try:
        res = p.send(Message(TOPIC, ("ns-failover-%s" % tag).encode()))
        print("   sent msgId=%s" % res.msg_id)
    finally:
        p.shutdown()


def create_topic():
    p = DefaultMQProducer("PG_NsFailCreate_%d" % STAMP)
    p.set_namesrv_addr(NS)
    p.start()
    try:
        p.create_topic("TBW102", TOPIC, 4)
    finally:
        p.shutdown()


def consume():
    print("   [debug] consumer ns list passed in:", NS.split(";"))
    got = []
    c = DefaultMQPushConsumer("GID_NsFail_%d" % STAMP)
    c.set_namesrv_addr(NS)
    c.subscribe(TOPIC, "*")

    class L:
        def consume_message(self, msgs, ctx):
            got.extend(msgs)
            from client.consumer_result import ConsumeConcurrentlyStatus
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS

    c.set_message_listener(L())
    c.set_consume_from_where(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET)
    c.set_consume_thread_min(1)
    c.set_consume_thread_max(2)
    try:
        c.start()
    except BaseException as e:
        inst = c._mq_client
        if inst is not None:
            print("   [debug] instance.name_server_addrs =", inst.name_server_addrs)
        raise
    print("   consumer started, waiting for messages...")
    deadline = time.time() + 30
    while time.time() < deadline and len(got) < 3:
        time.sleep(0.25)
    c.shutdown()
    print("   consumed=%d" % len(got))
    if not got:
        raise AssertionError("consumed nothing")


ok = True
ok &= step("create topic (both ns up)", create_topic)
ok &= step("produce before kill", lambda: produce("before"))
ok &= step("consume after kill (ns[0] dead)", consume)
ok &= step("produce after kill", lambda: produce("after"))
print("REPRO-RESULT: %s" % ("PASS" if ok else "FAIL"))
sys.exit(0 if ok else 1)
