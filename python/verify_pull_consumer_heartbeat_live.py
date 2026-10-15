#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import annotations

import sys
import time

sys.path.insert(0, ".")

from client.admin import DefaultMQAdminExt
from client.consumer import DefaultMQPullConsumer
from client.exception import MQBrokerException, MQClientException

NAMESRV = sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1:9876"
MASTER = sys.argv[2] if len(sys.argv) > 2 else "127.0.0.1:10911"
SLAVE = sys.argv[3] if len(sys.argv) > 3 else None
STAMP = int(time.time() * 1000)
TOPIC = "PullHb_%d" % STAMP
GROUP = "PG_PullHb_%d" % STAMP
GHOST_GROUP = "PG_PullHbGhost_%d" % STAMP

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


def group_is_online(admin: DefaultMQAdminExt, group: str, addr: str):
    """The raw answer from 203: online returns ConsumerConnection, offline returns None (any exception is treated as offline)."""
    try:
        return admin.examine_consumer_connection_info(group, broker_addr=addr)
    except (MQBrokerException, MQClientException):
        return None


def consumer_ids(client, group: str, addr: str):
    """The raw answer from 38: when offline the broker throws `no consumer for this group`, treated as an empty list."""
    try:
        return client.get_consumer_list_by_group(group, 5000, addr=addr).consumer_id_list
    except MQBrokerException:
        return []


def main() -> int:
    admin = DefaultMQAdminExt()
    admin.set_namesrv_addr(NAMESRV)
    admin.set_timeout_millis(10000)
    admin.start()
    consumer = None
    try:
        # ---- A0 create topic ----
        admin.create_topic_in_broker(MASTER, TOPIC, 4, 4)
        print("topic=%s group=%s" % (TOPIC, GROUP))

        # ---- A1 start the pull consumer and pull once ----
        consumer = DefaultMQPullConsumer(GROUP)
        consumer.set_namesrv_addr(NAMESRV)
        consumer.register_topics.add(TOPIC)
        consumer.start()
        mqs = consumer.fetch_subscribe_message_queues(TOPIC)
        pulled = consumer.pull(mqs[0], "*", 0, 32) if mqs else None
        check("A1 pull consumer started and pulled one round successfully",
              consumer.heartbeat_count() >= 1 and pulled is not None,
              "queues=%d status=%s heartbeats=%d"
              % (len(mqs), pulled.status if pulled else "-", consumer.heartbeat_count()))

        # ---- A2 master 203: this group is in the broker's consumerTable, and the mode is pull ----
        conn = group_is_online(admin, GROUP, MASTER)
        check("A2 master 203 finds this group (heartbeat registered)", conn is not None,
              "connections=%d" % (len(conn.connection_set) if conn else 0))
        check("A2 consume type is CONSUME_ACTIVELY",
              conn is not None and conn.consume_type == "CONSUME_ACTIVELY",
              "consumeType=%s" % (conn.consume_type if conn else None))
        check("A2 consume-from-where is CONSUME_FROM_LAST_OFFSET (:353)",
              conn is not None and conn.consume_from_where == "CONSUME_FROM_LAST_OFFSET",
              "consumeFromWhere=%s" % (conn.consume_from_where if conn else None))
        check("A2 broadcast/cluster mode is CLUSTERING",
              conn is not None and conn.message_model == "CLUSTERING",
              "messageModel=%s" % (conn.message_model if conn else None))

        subs = (conn.subscription_table or {}) if conn else {}
        entry = subs.get(TOPIC)
        sub_string = None
        if isinstance(entry, dict):
            sub_string = entry.get("subString") or entry.get("sub_string")
        check("A2b 203's subscription table carries the registerTopics topic and subString=*",
              sub_string == "*", "subscriptionTable=%s" % subs)

        # ---- A3 38 master: the clientId list in the consumerTable ----
        client = consumer._mq_client
        ids = consumer_ids(client, GROUP, MASTER)
        check("A3 master 38 finds this clientId", consumer.client_id in ids, "ids=%s" % ids)

        # ---- A4 slave: the heartbeat also reaches every node ----
        if SLAVE:
            slave_conn = group_is_online(admin, GROUP, SLAVE)
            check("A4 slave 203 also finds this group (heartbeat fans out to the slave)", slave_conn is not None,
                  "slave=%s connections=%d"
                  % (SLAVE, len(slave_conn.connection_set) if slave_conn else 0))
            slave_ids = consumer_ids(client, GROUP, SLAVE)
            check("A4 slave 38 also finds this clientId", consumer.client_id in slave_ids,
                  "ids=%s" % slave_ids)

        # ---- A5 control: the ghost group (never heartbeats) must not be found ----
        ghost_conn = group_is_online(admin, GHOST_GROUP, MASTER)
        ghost_ids = consumer_ids(client, GHOST_GROUP, MASTER)
        check("A5 control: the never-heartbeated ghost group is not found via 203", ghost_conn is None,
              "ghost connections=%d" % (len(ghost_conn.connection_set) if ghost_conn else 0))
        check("A5 control: the never-heartbeated ghost group is an empty list via 38", ghost_ids == [], "ids=%s" % ghost_ids)

        # ---- A6 shutdown unregisters immediately (35), no need to wait for the ~120s channel scan ----
        client_id = consumer.client_id
        consumer.shutdown()
        consumer = None
        deadline = time.time() + 10
        gone = False
        while time.time() < deadline:
            if group_is_online(admin, GROUP, MASTER) is None:
                gone = True
                break
            time.sleep(0.5)
        check("A6 after shutdown, 203 immediately cannot find this group (a 35 unregister was sent)", gone, "clientId=%s" % client_id)
    finally:
        if consumer is not None:
            try:
                consumer.shutdown()
            except Exception:  # noqa: BLE001
                pass
        try:
            admin.delete_topic_in_broker(MASTER, TOPIC)
        except Exception:  # noqa: BLE001
            pass
        admin.shutdown()

    print("\n%d PASS / %d FAIL" % (PASS, FAIL))
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(main())
