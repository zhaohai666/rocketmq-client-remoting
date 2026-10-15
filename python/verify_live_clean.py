# -*- coding: utf-8 -*-
"""Clean-environment end-to-end verification for rocketmq-client-remoting.

Difference from integration_live_test.py: this script uses a "brand-new, timestamp-unique
topic + unique consumer group + brand-new store directory", which rules out the inflated message counts caused by "/tmp/rmqstore accumulating messages across multiple
runs + CONSUME_FROM_FIRST_OFFSET re-reading from 0".

This script does not start the cluster itself; the caller is responsible for starting nameServer(9876)+broker(10911) first, with
broker autoCreateTopic enabled. Run: `python verify_live_clean.py` in the python venv
"""
import os
import sys
import time
import threading

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from client.producer import DefaultMQProducer
from client.consumer import DefaultMQPushConsumer, SimpleMessageListener
from common.message import Message, MessageBatch
from remoting.protocol.heartbeat import ConsumeFromWhere
from client.consumer_result import ConsumeConcurrentlyStatus

NAMESRV = "127.0.0.1:9876"
STAMP = int(time.time())
TOPIC = "VerifyCleanTopic_%d" % STAMP
GROUP = "VerifyCleanGroup_%d" % STAMP
N_SYNC = 10
N_BATCH = 3

results = []


def check(name, ok, detail=""):
    results.append((name, ok, detail))
    print("[%s] %s %s" % ("PASS" if ok else "FAIL", name, detail))


def main():
    prod = DefaultMQProducer(producer_group="VerifyCleanProducer_%d" % STAMP)
    prod.set_namesrv_addr(NAMESRV)
    prod.set_send_msg_timeout(5000)
    prod.start()
    # Poll-wait for the broker to finish registering with nameServer (avoids the race where the port is open but registration has not landed)
    brokers = []
    last_err = None
    for attempt in range(40):
        try:
            info = prod._mq_client.get_broker_cluster_info()
            if info and info.broker_addr_table:
                brokers = list(info.broker_addr_table.keys())
                break
        except Exception as e:  # noqa: BLE001
            last_err = e
        time.sleep(1)
    if brokers:
        check("cluster probe", True, "brokers=%s" % brokers)
    else:
        check("cluster probe", False, "no broker registered with nameServer last_err=%s" % last_err)
        prod.shutdown()
        return 1

    # ---------- produce: exactly 14 messages (10 sync + 3 batch + 1 oneway) ----------
    produced_bodies = []
    expected = set()
    send_ok = 0
    for i in range(N_SYNC):
        body = ("hello-rocketmq-%d" % i).encode("utf-8")
        produced_bodies.append(body)
        expected.add(body.decode("utf-8"))
        try:
            sr = prod.send(Message(TOPIC, body))
            if sr.send_status.name == "SEND_OK":
                send_ok += 1
        except Exception as e:  # noqa: BLE001
            print("sync send %d error: %s" % (i, e))
    check("sync send %d" % N_SYNC, send_ok == N_SYNC, "send_ok=%d/%d" % (send_ok, N_SYNC))

    try:
        batch = MessageBatch.generate_from_list([
            Message(TOPIC, ("batch-%d" % i).encode("utf-8")) for i in range(N_BATCH)
        ])
        bsr = prod.send(batch)
        bok = bsr.send_status.name == "SEND_OK"
    except Exception as e:  # noqa: BLE001
        bok = False
        print("batch send error: %s" % e)
    for i in range(N_BATCH):
        expected.add("batch-%d" % i)
    check("batch send %d" % N_BATCH, bok, "status=%s" % ("SEND_OK" if bok else "ERR"))

    try:
        prod.send_oneway(Message(TOPIC, b"oneway-ping"))
        ow_ok = True
    except Exception as e:  # noqa: BLE001
        ow_ok = False
        print("oneway error: %s" % e)
    expected.add("oneway-ping")
    check("oneway send", ow_ok, "returned without exception")

    total_produced = N_SYNC + N_BATCH + 1
    print("produced %d messages (topic=%s group=%s)" % (total_produced, TOPIC, GROUP))

    # ---------- consume: unique group + from the earliest offset ----------
    received = []          # list of (queue_id, offset, body_str, msg_id)
    lock = threading.Lock()

    def on_msg(msgs):
        with lock:
            for m in msgs:
                received.append((m.queue_id, m.queue_offset, m.body.decode("utf-8", "ignore"), m.msg_id))
        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS

    cons = DefaultMQPushConsumer(consumer_group=GROUP)
    cons.set_namesrv_addr(NAMESRV)
    cons.set_consume_from_where(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET)
    cons.subscribe(TOPIC, "*")
    cons.set_message_listener(SimpleMessageListener(on_msg))
    cons.start()

    time.sleep(15)
    cons.shutdown()
    prod.shutdown()

    recv_bodies = [r[2] for r in received]
    recv_set = set(recv_bodies)
    distinct_qo = set((r[0], r[1]) for r in received)

    dup_count = len(recv_bodies) - len(recv_set)
    extra = recv_set - expected
    missing = expected - recv_set
    received_n = len(received)

    print("\n--- count reconciliation ---")
    print("total produced        : %d" % total_produced)
    print("total delivered       : %d" % received_n)
    print("distinct bodies       : %d" % len(recv_set))
    print("distinct (queue,offset): %d" % len(distinct_qo))
    print("duplicate bodies      : %d" % dup_count)
    print("extra bodies(not produced): %s" % (list(extra) if extra else "none"))
    print("missing bodies(not consumed): %s" % (list(missing) if missing else "none"))

    check("consume count == produce count (14)", received_n == total_produced,
          "received=%d expected=%d" % (received_n, total_produced))
    check("no duplicate delivery", dup_count == 0, "dup=%d" % dup_count)
    check("no extra messages", not extra, "extra=%s" % (list(extra) if extra else "none"))
    check("no lost messages", not missing, "missing=%s" % (list(missing) if missing else "none"))

    failed = [r for r in results if not r[1]]
    print("\n================ summary ================")
    for name, ok, detail in results:
        print("  [%s] %s" % ("PASS" if ok else "FAIL", name))
    print("======================================")
    if failed:
        print("result: %d item(s) failed" % len(failed))
        return 1
    print("result: all passed (count reconciliation consistent, proving the earlier 19/36 was a store-accumulation artifact)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
