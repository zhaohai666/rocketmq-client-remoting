#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import subprocess
import sys
import threading
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from client.admin import DefaultMQAdminExt
from client.consumer import DefaultMQPushConsumer, SimpleMessageListener
from client.consumer_result import ConsumeConcurrentlyStatus
from client.exception import ClientErrorCode, MQClientException
from client.mq_client import MQClientInstance
from client.producer import DefaultMQProducer
from client.send_result import SendStatus
from common.message import Message, MessageQueue
from remoting.protocol.body import LockBatchRequestBody, LockBatchResponseBody
from remoting.protocol.codes import RequestCode
from remoting.protocol.heartbeat import ConsumeFromWhere
from remoting.protocol.remoting_command import RemotingCommand

NAMESRV = sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1:9876"
MASTER_ARG = sys.argv[2] if len(sys.argv) > 2 else "127.0.0.1:10911"
SLAVE_ARG = sys.argv[3] if len(sys.argv) > 3 else "127.0.0.1:10931"
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BROKER_CTL = os.path.join(ROOT, "scripts", "rmq_test_broker.sh")

STAMP = int(time.time() * 1000)
TOPIC = "PrMasterLive_%d" % STAMP
GROUP = "GID_PrMasterLive_%d" % STAMP
BROKER_NAME = "broker-a"
# The upper bound for a local failure: when the publish info is empty there is no wire call at
# all, so 500ms on a real machine already leaves an order of magnitude of margin
LOCAL_BUDGET_MS = 500.0

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


def broker_ctl(action: str) -> tuple:
    """Run scripts/rmq_test_broker.sh and return (returncode, output).

    The output goes through a **file** rather than a pipe: start brings the broker up as a
    long-running process and the write end of the pipe is inherited by it, so ``subprocess`` would
    only return once every write end is closed -- which shows up as the script having printed its
    result while the caller is still stuck.
    """
    out_path = "/tmp/rmq_pr_master_broker_ctl.%s.log" % action
    with open(out_path, "w") as fh:
        proc = subprocess.run(["sh", BROKER_CTL, action], stdout=fh, stderr=fh,
                              timeout=600, start_new_session=True)
    with open(out_path) as fh:
        return proc.returncode, fh.read().strip()


def broker_addrs_of(inst: MQClientInstance, topic: str):
    """Force a refresh and then take the brokerAddrs of broker-a (returns None when the refresh fails)."""
    try:
        inst.update_topic_route_info_from_name_server(topic, 5000)
    except Exception as e:  # noqa: BLE001
        print("    (route refresh failed: %s)" % e)
    route = inst.get_topic_route_data(topic)
    if route is None:
        return None
    bd = next((b for b in route.get_broker_datas() if b.broker_name == BROKER_NAME), None)
    return dict(bd.broker_addrs) if bd is not None else None


def wait_for(pred, timeout_s: float, what: str):
    deadline = time.time() + timeout_s
    last = None
    while time.time() < deadline:
        last = pred()
        if last:
            return last
        time.sleep(1)
    print("    (did not get %s within the timeout, last observation: %s)" % (what, last))
    return None


def main() -> int:
    admin = DefaultMQAdminExt()
    admin.set_namesrv_addr(NAMESRV)
    admin.set_timeout_millis(10000)
    admin.start()

    inst = MQClientInstance("pr-master-live-%d" % STAMP, [NAMESRV])
    inst.start()
    producer = DefaultMQProducer("PID_PrMasterLive_%d" % STAMP)
    producer.set_namesrv_addr(NAMESRV)
    producer.start()

    consumer = None
    got = []
    got_lock = threading.Lock()
    try:
        # ---------- S0 the control leg ----------
        print("S0 control leg (the master is up): route {0: master, 1: slave}, 4 queues each for publish/subscribe")
        inst.create_topic_in_route(TOPIC, 4, 4)
        try:
            # Create one on the slave directly as well, rather than betting on the 5s SlaveSynchronize period
            admin.create_topic_in_broker(SLAVE_ARG, TOPIC, 4, 4)
        except Exception as e:  # noqa: BLE001
            print("    (creating the topic on the slave failed: %s)" % e)

        def route_ok():
            addrs = broker_addrs_of(inst, TOPIC)
            return addrs if addrs and set(addrs) >= {0, 1} else None

        addrs = wait_for(route_ok, 30, "the route {0, 1}")
        check("S0 the route contains {0: master, 1: slave}", addrs is not None,
              "broker_addrs=%s" % (addrs or {}))
        if addrs is None:
            print("== the precondition is not met: no usable master/slave address ==")
            return 1
        check("S0 the slave address matches the argument", addrs.get(1) == SLAVE_ARG,
              "route=%s argv=%s" % (addrs.get(1), SLAVE_ARG))

        def publish_queues():
            info = inst.get_topic_publish_info(TOPIC)
            return list(info.msg_queue_list)

        queues = wait_for(lambda: publish_queues() or None, 15, "the publish queues")
        check("S0 4 publish queues (control)", bool(queues) and len(queues) == 4,
              "queues=%s" % ([(q.broker_name, q.queue_id) for q in queues or []]))
        subs = inst.get_topic_subscribe_info(TOPIC)
        check("S0 4 subscribe queues (control)", len(subs) == 4,
              "queues=%s" % ([(q.broker_name, q.queue_id) for q in subs]))
        if not queues:
            return 1

        # ---------- S1 seed the messages ----------
        print("\nS1 seeding 4 messages (one pinned per queue) and waiting for the slave store to catch up")
        seeded = []
        for i, q in enumerate(sorted(queues, key=lambda x: x.queue_id)):
            body = ("pr-master-%d" % i).encode()
            r = producer.send(Message(TOPIC, body),
                              mq=MessageQueue(TOPIC, q.broker_name, q.queue_id))
            seeded.append(body)
            if r.send_status != SendStatus.SEND_OK:
                check("S1 the %d-th seeded message got SEND_OK" % i, False, "status=%s" % r.send_status)
        check("S1 all 4 seeded messages got SEND_OK", len(seeded) == 4)

        master_max = {q.queue_id: inst.get_max_offset(q) for q in queues}

        def slave_caught_up():
            if not master_max:
                return None
            try:
                slave_max = {q.queue_id: inst.get_max_offset(q, addr=SLAVE_ARG)
                             for q in queues}
            except Exception as e:  # noqa: BLE001
                print("    (reading maxOffset from the slave failed: %s)" % e)
                return None
            return slave_max if all(slave_max[k] >= v for k, v in master_max.items()) else None

        slave_max = wait_for(slave_caught_up, 30, "the slave replication catching up")
        check("S1 the 4 messages were replicated to the slave store", slave_max is not None,
              "master=%s slave=%s" % (master_max, slave_max or {}))

        # ---------- S2 stop the master ----------
        print("\nS2 stopping the master (scripts/rmq_test_broker.sh stop) and waiting until the route only has the slave")
        code, out = broker_ctl("stop")
        check("S2 the master shut down gracefully", code == 0, out)

        def masterless():
            addrs_now = broker_addrs_of(inst, TOPIC)
            return addrs_now if addrs_now and 0 not in addrs_now and 1 in addrs_now else None

        down_addrs = wait_for(masterless, 60, "the masterless route")
        check("S2 the route of broker-a only has {1: slave} left", down_addrs is not None,
              "broker_addrs=%s" % (down_addrs or {}))
        if down_addrs is None:
            return 1

        # ---------- S3 (A) the publish queues ----------
        print("\nS3 (A) the publish info skips a broker that has no master")
        info = inst.topic_publish_info_table.get(TOPIC)
        n_pub = len(info.msg_queue_list) if info is not None else -1
        check("S3 after stopping the master the publish queues == 0", n_pub == 0, "msg_queue_list=%d" % n_pub)
        acc_exc = None
        try:
            inst.get_topic_publish_info(TOPIC)
        except MQClientException as e:
            acc_exc = e
        check("S3b the publish info accessor throws locally with 'no queue can be selected'",
              acc_exc is not None and "Can not find Message Queue for topic" in str(acc_exc),
              "%s" % acc_exc)

        # ---------- S4 (C) the subscribe queues ----------
        subs_down = inst.get_topic_subscribe_info(TOPIC)
        check("S4 (C) the subscribe queues are still 4 and all in broker-a (the consumer side does not look at the master)",
              len(subs_down) == 4 and all(q.broker_name == BROKER_NAME for q in subs_down),
              "queues=%s" % ([(q.broker_name, q.queue_id) for q in subs_down]))

        # ---------- S5 (A settled) the send fails fast ----------
        # It is looked at in two stages: while the **cache has not been refreshed** yet (in
        # reality the 30s periodic task has not fired), the sending instance still holds the old
        # pre-shutdown route, so the address resolution lands on the dead master and the send fails
        # fast, never silently switching to the slave; once **the route has been refreshed into the
        # post-shutdown shape** (the periodic task / an explicit refresh), (A) takes effect: the
        # publish queues are empty and the send does not put a single message on the wire.
        print("\nS5 a synchronous send without a specified queue: fails fast on the stale cache -> fails fast locally after the refresh")
        began = time.monotonic()
        exc = None
        try:
            producer.send(Message(TOPIC, b"must-not-send"))
        except Exception as e:  # noqa: BLE001
            exc = e
        stale_ms = (time.monotonic() - began) * 1000
        # This leg is an opportunistic one: whether the periodic refresh has already run is not
        # decided by this script. The criterion shared by both legs is "fails fast + never lands on
        # the slave address" (the stale-cache leg targets the dead master).
        check("S5 the send fails fast and the error contains no slave address (it never switches to the slave)",
              exc is not None and stale_ms < LOCAL_BUDGET_MS and SLAVE_ARG not in str(exc),
              "%.0fms %s: %s" % (stale_ms, type(exc).__name__, exc))

        # Let the sending instance's own route cache be refreshed into the post-shutdown shape (the same
        # code path as the 30s periodic task)
        pcli = producer._require_client()
        pcli.update_topic_route_info_from_name_server(TOPIC, 5000)
        p_info = pcli.topic_publish_info_table.get(TOPIC)
        n_pub_send = len(p_info.msg_queue_list) if p_info is not None else -1
        check("S5b the publish queues of the sending instance are == 0 as well (this is where (A) takes effect)", n_pub_send == 0,
              "msg_queue_list=%d" % n_pub_send)

        began = time.monotonic()
        exc2 = None
        try:
            producer.send(Message(TOPIC, b"must-not-send-2"))
        except Exception as e:  # noqa: BLE001
            exc2 = e
        fresh_ms = (time.monotonic() - began) * 1000
        check("S5c after the refresh: the local 10005 throws 'no queue can be selected', with no wire call (no BrokersSent)",
              isinstance(exc2, MQClientException)
              and exc2.response_code == ClientErrorCode.NOT_FOUND_TOPIC_EXCEPTION
              and "Can not find Message Queue for topic" in str(exc2)
              and "BrokersSent" not in str(exc2)
              and fresh_ms < LOCAL_BUDGET_MS,
              "%.0fms code=%s: %s" % (fresh_ms, getattr(exc2, "response_code", None), exc2))

        print("\nS5d (B) the control leg: a pinned send to that queue -> locally reports the broker does not exist (no wire)"),
        pinned_exc = None
        began = time.monotonic()
        try:
            producer.send(Message(TOPIC, b"pinned-to-slave"),
                          mq=MessageQueue(TOPIC, BROKER_NAME, 0))
        except Exception as e:  # noqa: BLE001
            pinned_exc = e
        pinned_ms = (time.monotonic() - began) * 1000
        check("S5d the pinned send locally reports \"The broker[broker-a] not exist\", with no wire"
              "(not the SYSTEM_BUSY(2) returned by the slave)",
              isinstance(pinned_exc, MQClientException)
              and str(pinned_exc) == "The broker[%s] not exist" % BROKER_NAME
              and pinned_ms < LOCAL_BUDGET_MS,
              "%.0fms %s: %s" % (pinned_ms, type(pinned_exc).__name__, pinned_exc))

        print("\nS5e (D) the orderly lock: inside the outage window the whole machine is skipped (no route refresh, no wire)"),
        client_id = "pr-master-live-%d" % STAMP
        mqs = [MessageQueue(TOPIC, BROKER_NAME, 0)]
        began = time.monotonic()
        locks = inst.lock_batch_mq(GROUP, client_id, mqs)
        lock_ms = (time.monotonic() - began) * 1000
        check("S5e when only the slave is left not a single machine can be locked (the old behaviour would fall back to locking on the slave)",
              locks == [] and lock_ms < LOCAL_BUDGET_MS, "%.0fms locked=%s" % (lock_ms, locks))

        # The control leg: the very same request names the slave directly -- the slave **does** hand out
    # the lock as it always does
        req = RemotingCommand.create_request_command(RequestCode.LOCK_BATCH_MQ, None)
        body = LockBatchRequestBody()
        body.consumer_group = GROUP
        body.client_id = client_id
        body.mq_set = [{"topic": TOPIC, "brokerName": BROKER_NAME, "queueId": 0}]
        req.body = body.encode()
        slave_ok = len(LockBatchResponseBody.decode(
            inst._invoke_sync(SLAVE_ARG, req, 3000).body).lock_ok_mq_set)
        check("S5e2 control: when the slave is named directly the lock is handed out (the empty set is not a broker rejection)", slave_ok == 1,
              "slave lockOKMQSet=%d" % slave_ok)

        began = time.monotonic()
        inst.unlock_batch_mq(GROUP, client_id, mqs)
        unlock_ms = (time.monotonic() - began) * 1000
        check("S5e3 the unlock is silently skipped in the same way (no throw, nothing sent)", unlock_ms < LOCAL_BUDGET_MS,
              "%.0fms" % unlock_ms)

        print("\nS5f (E) the POP pull: locally reports \"The broker[broker-a] not exist\" (no wire)")
        pop_exc = None
        began = time.monotonic()
        try:
            inst.pop_message(GROUP, TOPIC, queue_id=0, max_msg_nums=1,
                             invisible_time=30000, poll_time=100)
        except Exception as e:  # noqa: BLE001
            pop_exc = e
        pop_ms = (time.monotonic() - began) * 1000
        check("S5f inside the outage window the POP locally reports \"The broker[broker-a] not exist\" (not an error returned by the slave)",
              isinstance(pop_exc, MQClientException)
              and str(pop_exc) == "The broker[%s] not exist" % BROKER_NAME
              and pop_ms < LOCAL_BUDGET_MS,
              "%.0fms %s: %s" % (pop_ms, type(pop_exc).__name__, pop_exc))

        print("\nS5g (F) reading the offset: the cold instance refreshes the route once and then widens to the slave")
        cold = MQClientInstance("pr-master-live-cold-%d" % STAMP, [NAMESRV])
        cold_exc = None
        off = None
        try:
            off = cold.query_consumer_offset(GROUP, MessageQueue(TOPIC, BROKER_NAME, 0))
        except Exception as e:  # noqa: BLE001
            cold_exc = e
        finally:
            cold.shutdown()
        check("S5g reading the offset from the cold instance raises no error: refresh the route -> fall back to the slave and let the broker answer",
              cold_exc is None and (off is None or isinstance(off, int)),
              "%s: %s" % (type(cold_exc).__name__ if cold_exc else "ok",
                          cold_exc if cold_exc else "offset=%s" % off))

        # ---------- S6 (C end to end) consuming inside the outage window ----------
        print("\nS6 a push consumer started inside the outage window: 4 queues + collecting all 4 seeded messages from the slave")

        def on_msg(msgs):
            with got_lock:
                got.extend(bytes(m.body) for m in msgs)
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS

        consumer = DefaultMQPushConsumer(GROUP)
        consumer.set_namesrv_addr(NAMESRV)
        consumer.set_consume_from_where(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET)
        consumer.set_message_listener(SimpleMessageListener(on_msg))
        consumer.subscribe(TOPIC, "*")
        consumer.start()
        subs_in_window = consumer._require_client().get_topic_subscribe_info(TOPIC)
        check("S6a the subscribe info of the consumer inside the window is 4 as well",
              len(subs_in_window) == 4,
              "queues=%s" % ([(q.broker_name, q.queue_id) for q in subs_in_window]))
        wait_for(lambda: sorted(got) == sorted(seeded), 60, "the 4 seeded messages")
        check("S6 while the master was down all 4 were collected from the slave",
              sorted(got) == sorted(seeded), "got=%s" % sorted(got))

        # ---------- S7 the negative control (bring the master back first) ----------
        print("\nS7 negative control: after the master comes back the publish queues and the send recover")
        code, out = broker_ctl("start")
        if code != 0:
            print("  [FAIL] restoring the master failed: %s" % out)

        def publish_back():
            try:
                return list(inst.get_topic_publish_info(TOPIC).msg_queue_list) or None
            except Exception:  # noqa: BLE001
                return None

        queues_back = wait_for(publish_back, 60, "the publish queues recovering")
        check("S7 the publish queues recovered to 4", bool(queues_back) and len(queues_back) == 4,
              "queues=%s" % ([(q.broker_name, q.queue_id) for q in queues_back or []]))
        # Neither of the two failed sends left a message on the broker: the maxOffset of every seeded queue is still 1
        after = {q.queue_id: inst.get_max_offset(q) for q in (queues_back or queues or [])}
        check("S7b the two failed sends left no messages behind (maxOffset is still 1)",
              bool(after) and all(v == 1 for v in after.values()), "maxOffset=%s" % after)
        r = producer.send(Message(TOPIC, b"pr-master-back"))
        check("S7c the send recovered with SEND_OK", r.send_status == SendStatus.SEND_OK,
              "status=%s msgId=%s" % (r.send_status, r.msg_id))
    finally:
        # The master has to come back (start is idempotent and this is also reached on the exception path)
        broker_ctl("start")
        for c in (consumer, producer):
            if c is not None:
                try:
                    c.shutdown()
                except Exception as e:  # noqa: BLE001
                    print("    (shutdown failed: %s)" % e)
        try:
            inst.shutdown()
        except Exception as e:  # noqa: BLE001
            print("    (instance shutdown failed: %s)" % e)
        try:
            admin.delete_topic(TOPIC)
        except Exception as e:  # noqa: BLE001
            print("    (delete_topic(%s) failed: %s)" % (TOPIC, e))
        admin.shutdown()

    print("\n== Result: %d/%d passed ==" % (PASS, PASS + FAIL))
    return 0 if FAIL == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
