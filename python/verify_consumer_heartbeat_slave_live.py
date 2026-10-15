#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import annotations

import sys
import threading
import time

sys.path.insert(0, ".")

from client.admin import DefaultMQAdminExt
from client.consumer import (ConsumeConcurrentlyStatus,
                                      DefaultMQPushConsumer,
                                      SimpleMessageListener)
from client.exception import MQBrokerException
from client.mq_client import MQClientInstance
from client.producer import DefaultMQProducer
from common.message import Message, MessageQueue
from common.sysflag import PullSysFlag
from remoting.protocol.codes import ResponseCode

NAMESRV = sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1:9876"
SLAVE_ARG = sys.argv[2] if len(sys.argv) > 2 else "127.0.0.1:10931"
STAMP = int(time.time() * 1000)
TOPIC = "HbSlave_%d" % STAMP
GROUP = "GID_HbSlave_%d" % STAMP
GHOST_GROUP = "GID_HbSlaveGhost_%d" % STAMP
PRODUCER_GROUP = "PID_HbSlave_%d" % STAMP

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


def main() -> int:
    admin = DefaultMQAdminExt()
    admin.set_namesrv_addr(NAMESRV)
    admin.set_timeout_millis(10000)
    admin.start()

    consumer = None
    producer = None
    inst = None
    try:
        # ---------- Create topic (route to master, plus a separate copy on the slave) ----------
        # The slave's own TopicConfigManager is synced by SlaveSynchronize (first round ~10s, then every 60s),
        # so do not wait for it: PullMessageProcessor's topic check (:347) runs before the subscription check (:420),
        # and when the slave does not know the topic it replies TOPIC_NOT_EXIST(17) first, masking the very gate under test.
        print("create topic (master via route + slave direct, one each)")
        inst = MQClientInstance("hb-slave-setup-%d" % STAMP, [NAMESRV])
        inst.start()
        inst.create_topic_in_route(TOPIC, 4, 4)

        # ---------- S0 route has both master and slave ----------
        print("\nS0 route: broker-a {0: master, 1: slave}")
        route = None
        deadline = time.time() + 30
        while time.time() < deadline:
            route = inst.get_topic_route_data(TOPIC)
            if route and route.broker_datas:
                bd = route.broker_datas[0]
                if len(bd.broker_addrs) >= 2:
                    break
            time.sleep(1)
        bd = route.broker_datas[0] if route and route.broker_datas else None
        addrs = dict(bd.broker_addrs) if bd else {}
        check("S0 route has {0: master, 1: slave}", set(addrs) >= {0, 1},
              "broker_addrs=%s" % addrs)
        master = addrs.get(0, "127.0.0.1:10911")
        slave = addrs.get(1, SLAVE_ARG)
        check("S0 slave address matches argument", slave == SLAVE_ARG,
              "route=%s argv=%s" % (slave, SLAVE_ARG))
        # The slave must also know the topic: connect directly to it and create one in its own TopicConfigManager
        try:
            admin.create_topic_in_broker(slave, TOPIC, 4, 4)
            print("    (created topic %s on slave %s)" % (TOPIC, slave))
        except Exception as e:  # noqa: BLE001
            print("    (failed to create topic on slave: %s)" % e)
        check("S0 getAllBrokerAddrs has two, getRouteOfAllBrokers only master",
              sorted(inst.get_all_broker_addrs()) == sorted([master, slave])
              and inst.get_route_of_all_brokers() == [master],
              "all=%s probe=%s" % (inst.get_all_broker_addrs(),
                                   inst.get_route_of_all_brokers()))
        if not (master and slave and master != slave):
            print("\n== precondition not met: no usable master/slave addresses ==")
            return 1

        # ---------- S1 bring up consumer and register ----------
        print("\nS1 start push consumer and wait for first heartbeat")
        got = []
        lock = threading.Lock()

        def on_msg(msgs):
            with lock:
                got.extend(bytes(m.body) for m in msgs)
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS

        consumer = DefaultMQPushConsumer(GROUP)
        consumer.set_namesrv_addr(NAMESRV)
        consumer.set_message_listener(SimpleMessageListener(on_msg))
        consumer.subscribe(TOPIC, "*")
        consumer.start()
        client = consumer._require_client()
        client_id = consumer.client_id
        wait = time.time() + 20
        while time.time() < wait and consumer.heartbeat_count() < 1:
            time.sleep(0.5)
        check("S1 first heartbeat sent", consumer.heartbeat_count() >= 1,
              "heartbeat_count=%d clientId=%s" % (consumer.heartbeat_count(), client_id))

        def ids_at(addr, group):
            """Query 38 directly against the given address; the broker replies SUCCESS+empty list for "group not exist" too."""
            try:
                body = client.get_consumer_list_by_group(group, 5000, addr=addr)
            except Exception as e:  # noqa: BLE001
                return None, "%s: %s" % (type(e).__name__, e)
            return list(body.consumer_id_list or []), ""

        # ---------- S2 slave also knows this group ----------
        print("\nS2 GET_CONSUMER_LIST_BY_GROUP(38) on the slave can see this clientId")
        seen, err = ids_at(slave, GROUP)
        check("S2 slave 38 finds this clientId (heartbeat truly fans out to the slave)",
              bool(seen) and client_id in seen,
              "slave=%s seen=%s %s" % (slave, seen, err))
        # ---------- S2b negative control ----------
        ghost, err2 = ids_at(slave, GHOST_GROUP)
        check("S2b control: a never-heartbeated group on the slave -> empty",
              ghost == [] or ghost is None,
              "ghost=%s %s" % (ghost, err2))
        # ---------- S3 master also knows this group ----------
        seen_m, err3 = ids_at(master, GROUP)
        check("S3 master 38 also finds this group (proves it really is registered)",
              bool(seen_m) and client_id in seen_m,
              "master=%s seen=%s %s" % (master, seen_m, err3))

        # ---------- S4 pull against the slave with the "no subscription flag" ----------
        # push consumer default shape: subscription=False (postSubscriptionWhenPull=false)
        print("\nS4 no-subscription-flag pull against the slave (push default shape)")
        mq = MessageQueue(TOPIC, bd.broker_name, 0)
        sys_flag = PullSysFlag.build_sys_flag(commit_offset=False, suspend=False,
                                              subscription=False, class_filter=False)

        def pull_at(addr, group):
            try:
                client.pull_message(group, mq, 0, 32, sys_flag, 0, None, 0, "TAG",
                                    timeout_millis=5000, max_msg_bytes=-1,
                                    suspend_timeout_millis=15000, addr=addr)
                return "OK", ""
            except MQBrokerException as e:
                return None, "code=%s %s" % (e.response_code, e.error_message)
            except Exception as e:  # noqa: BLE001
                return None, "%s: %s" % (type(e).__name__, e)

        status, err4 = pull_at(slave, GROUP)
        check("S4 slave no longer returns SUBSCRIPTION_NOT_EXIST(24) for a registered group",
              status == "OK",
              "slave=%s -> %s %s" % (slave, status or "rejected", err4))
        status_g, err5 = pull_at(slave, GHOST_GROUP)
        check("S4b control: the same slave still returns 24 for an unregistered group (that gate really judges by each node's own table)",
              "code=%d" % ResponseCode.SUBSCRIPTION_NOT_EXIST in err5,
              "ghost -> %s" % err5)

        # ---------- S5 producer heartbeat still only goes to master ----------
        print("\nS5 producer heartbeat only hits master: 204 cannot find the group on the slave, master can")
        producer = DefaultMQProducer(PRODUCER_GROUP)
        producer.set_namesrv_addr(NAMESRV)
        producer.start()
        # The producer's first heartbeat at start() may fire before the route table is filled (known broker list empty then, nothing sent),
        # so send one message first to pull up the route, then wait for the next heartbeat cycle (default 30s) to register the group.
        producer.send(Message(TOPIC, b"hb-slave-probe"))
        wait = time.time() + 70
        pc_master = None
        while time.time() < wait:
            pc_master = _connection_client_ids(admin, master, PRODUCER_GROUP)
            if pc_master:
                break
            time.sleep(1)
        check("S5 master 204 can see this producer (heartbeat path works)",
              bool(pc_master), "master=%s seen=%s" % (master, pc_master))
        pc_slave = _connection_client_ids(admin, slave, PRODUCER_GROUP)
        check("S5b slave 204 cannot find this producer group (producer heartbeat still only hits master)",
              not pc_slave, "slave=%s seen=%s" % (slave, pc_slave))
    finally:
        for c in (consumer, producer):
            if c is not None:
                try:
                    c.shutdown()
                except Exception as e:  # noqa: BLE001
                    print("    (shutdown failed: %s)" % e)
        if inst is not None:
            try:
                inst.shutdown()
            except Exception as e:  # noqa: BLE001
                print("    (setup instance shutdown failed: %s)" % e)
        try:
            admin.delete_topic(TOPIC)
        except Exception as e:  # noqa: BLE001
            print("    (delete_topic(%s) failed: %s)" % (TOPIC, e))
        admin.shutdown()

    print("\n== result: %d/%d passed ==" % (PASS, PASS + FAIL))
    return 0 if FAIL == 0 else 1


def _connection_client_ids(admin: DefaultMQAdminExt, broker_addr: str, group: str):
    """clientId list as seen via 204; returns None when the broker says "group not exist"."""
    try:
        pc = admin.examine_producer_connection_info(group, broker_addr)
    except Exception as e:  # noqa: BLE001
        if "not exist" in str(e):
            return None
        raise
    return [c.client_id for c in pc.connection_set]


if __name__ == "__main__":
    sys.exit(main())
