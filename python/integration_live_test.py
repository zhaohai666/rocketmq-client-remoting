# -*- coding: utf-8 -*-
"""rocketmq-client-remoting integration test against a real cluster.

Prerequisites: a local nameServer(9876) + broker(10911) is already running, with
autoCreateTopic enabled on the broker.
Run it like this in the python venv: `python integration_live_test.py`
"""
import sys
import time
import threading

from client.producer import DefaultMQProducer
from client.consumer import DefaultMQPushConsumer, SimpleMessageListener
from common.message import Message, MessageBatch
from remoting.protocol.heartbeat import ConsumeFromWhere
from client.consumer_result import ConsumeConcurrentlyStatus

NAMESRV = "127.0.0.1:9876"
TOPIC = "PythonTestTopic"
GROUP = "PythonTestConsumerGroup"
N = 10

results = []


def check(name, ok, detail=""):
    results.append((name, ok, detail))
    print("[%s] %s %s" % ("PASS" if ok else "FAIL", name, detail))


def main():
    # ---------- 1. cluster liveness check ----------
    prod = DefaultMQProducer(producer_group="PythonTestProducerGroup")
    prod.set_namesrv_addr(NAMESRV)
    prod.set_send_msg_timeout(5000)
    prod.start()
    try:
        info = prod._mq_client.get_broker_cluster_info()
        brokers = list(info.broker_addr_table.keys()) if info and info.broker_addr_table else []
        check("cluster liveness: obtained the broker route", bool(brokers), "brokers=%s" % brokers)
    except Exception as e:  # noqa: BLE001
        check("cluster liveness: obtained the broker route", False, "exception=%s" % e)
        prod.shutdown()
        return 1

    # ---------- 2. send N messages synchronously ----------
    sent_bodies = []
    send_ok = 0
    send_ids = []
    try:
        for i in range(N):
            body = ("hello-rocketmq-%d" % i).encode("utf-8")
            msg = Message(TOPIC, body)
            msg.put_property("a", str(i))
            sr = prod.send(msg)
            sent_bodies.append(body)
            send_ids.append(sr.msg_id)
            if sr.send_status.name == "SEND_OK":
                send_ok += 1
        check("sent %d messages synchronously" % N, send_ok == N,
              "send_ok=%d/%d, the first msg_id=%s" % (send_ok, N, send_ids[0] if send_ids else ""))
    except Exception as e:  # noqa: BLE001
        check("sent %d messages synchronously" % N, False, "exception=%s" % e)

    # ---------- 3. batch send ----------
    try:
        batch = MessageBatch.generate_from_list([
            Message(TOPIC, ("batch-%d" % i).encode("utf-8")) for i in range(3)
        ])
        bsr = prod.send(batch)
        check("batch-sent 3 messages", bsr.send_status.name == "SEND_OK", "msg_id=%s" % bsr.msg_id)
    except Exception as e:  # noqa: BLE001
        check("batch-sent 3 messages", False, "exception=%s" % e)

    # ---------- 4. oneway send (no persistence guarantee, only checks that it does not throw) ----------
    try:
        prod.send_oneway(Message(TOPIC, b"oneway-ping"))
        check("oneway send", True, "returned without an exception")
    except Exception as e:  # noqa: BLE001
        check("oneway send", False, "exception=%s" % e)

    # ---------- 5. push consumption (starting from the earliest offset) ----------
    received = []
    lock = threading.Lock()

    def on_msg(msgs):
        with lock:
            for m in msgs:
                received.append(m.body)
        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS

    cons = DefaultMQPushConsumer(consumer_group=GROUP)
    cons.set_namesrv_addr(NAMESRV)
    cons.set_consume_from_where(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET)
    cons.subscribe(TOPIC, "*")
    cons.set_message_listener(SimpleMessageListener(on_msg))
    cons.start()

    # Wait for the consumption (10 synchronous + 3 batch + 1 oneway)
    time.sleep(12)
    cons.shutdown()

    recv_set = set(b.decode("utf-8", "ignore") for b in received)
    expected_sync = set("hello-rocketmq-%d" % i for i in range(N))
    expected_batch = set("batch-%d" % i for i in range(3))
    missing_sync = expected_sync - recv_set
    missing_batch = expected_batch - recv_set
    check("consumed count (%d)" % len(received), len(received) >= N,
          "received=%d (expected >= %d)" % (len(received), N))
    check("the synchronous messages are all intact", not missing_sync,
          "missing=%s" % (list(missing_sync) if missing_sync else "none"))
    check("the batch messages are all intact", not missing_batch,
          "missing=%s" % (list(missing_batch) if missing_batch else "none"))

    prod.shutdown()

    # ---------- summary ----------
    failed = [r for r in results if not r[1]]
    print("\n================ summary ================")
    for name, ok, detail in results:
        print("  [%s] %s" % ("PASS" if ok else "FAIL", name))
    print("======================================")
    if failed:
        print("Result: %d check(s) failed" % len(failed))
        return 1
    print("Result: all checks passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
