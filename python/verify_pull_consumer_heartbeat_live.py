#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""拉模式消费者（DefaultMQPullConsumer）的心跳必须把消费组注册进 broker（真机验证）。

用法（需本地 RocketMQ 5.5.1 集群，broker-a 主 10911）：
    .venv/bin/python verify_pull_consumer_heartbeat_live.py 127.0.0.1:9876 127.0.0.1:10911 [127.0.0.1:10931]

第三个参数是从节点地址；给了就顺带断言心跳同样落到从节点（承 #96 的扇出规则）。

Java 依据：``DefaultMQPullConsumerImpl.start``:746 把本组 ``registerConsumer`` 进
``MQClientInstance``，实例的 ``sendHeartbeatToAllBrokerWithLock`` 周期任务据此发出一份
``consumeType=CONSUME_ACTIVELY`` 的 ConsumerData（``MQClientInstance#prepareHeartbeatData``
:1031-1045 遍历 consumerTable；``consumeType():348`` 与 ``consumeFromWhere():353`` 是拉模式的
两个固定口径，订阅集来自 ``subscriptions():357-385`` 的 registerTopics）。

为什么必须真机锁死：少了这份心跳，broker 的 ``ConsumerManager.consumerTable`` 里根本没有
这个组（拉取本身仍能work —— 拉模式请求带 subscription 标志，走
``PullMessageProcessor``:397-412 的补偿分支），于是：
  * ``GET_CONSUMER_CONNECTION_LIST(203)``（mqadmin consumerConnection）看不到本组
    —— Java 的拉模式消费者在管理端**可见**；
  * ``GET_CONSUMER_LIST_BY_GROUP(38)`` 查不到本 clientId；
  * ``rejectPullConsumerEnabled=true`` 的 broker 会把每次拉取回
    ``SUBSCRIPTION_NOT_EXIST``（``PullMessageProcessor``:493-505）。
这些全是**静默**的：消费照常、一条日志都不报。

场景：
  A0 建 topic（主节点一份配置）
  A1 起拉模式消费者（register_topics 带上该 topic），拉一轮 → 拉取本身正常
  A2 【核心】203 在**主节点**查到本组，且 consumeType=CONSUME_ACTIVELY、
     messageModel=CLUSTERING、consumeFromWhere=CONSUME_FROM_LAST_OFFSET
  A2b 203 的订阅表里带 registerTopics 那个 topic（subString="*"）
  A3 38 在**主节点**查到本 clientId
  A4 从节点上 203 也看得到本组（心跳扇出到每一台）
  A5 对照：从未心跳过的幽灵组 → 203 报错、38 空列表（这条判据本身有效）
  A6 shutdown() 立刻发 35 注销 → 203 随即查不到（不必等 ~120s 通道扫描）
"""
from __future__ import annotations

import sys
import time

sys.path.insert(0, ".")

from rocketmq.client.admin import DefaultMQAdminExt
from rocketmq.client.consumer import DefaultMQPullConsumer
from rocketmq.client.exception import MQBrokerException, MQClientException

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
    """203 的原始答案：在线返回 ConsumerConnection，不在线返回 None（异常一律当不在线）。"""
    try:
        return admin.examine_consumer_connection_info(group, broker_addr=addr)
    except (MQBrokerException, MQClientException):
        return None


def consumer_ids(client, group: str, addr: str):
    """38 的原始答案：不在线时 broker 直接抛 `no consumer for this group`，当空列表处理。"""
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
        # ---- A0 建 topic ----
        admin.create_topic_in_broker(MASTER, TOPIC, 4, 4)
        print("topic=%s group=%s" % (TOPIC, GROUP))

        # ---- A1 起拉模式消费者并拉一轮 ----
        consumer = DefaultMQPullConsumer(GROUP)
        consumer.set_namesrv_addr(NAMESRV)
        consumer.register_topics.add(TOPIC)
        consumer.start()
        mqs = consumer.fetch_subscribe_message_queues(TOPIC)
        pulled = consumer.pull(mqs[0], "*", 0, 32) if mqs else None
        check("A1 拉模式消费者启动并成功拉取一轮",
              consumer.heartbeat_count() >= 1 and pulled is not None,
              "queues=%d status=%s heartbeats=%d"
              % (len(mqs), pulled.status if pulled else "-", consumer.heartbeat_count()))

        # ---- A2 主节点 203：本组在 broker 的 consumerTable 里，且口径是拉模式 ----
        conn = group_is_online(admin, GROUP, MASTER)
        check("A2 主节点 203 查到本组（心跳已注册）", conn is not None,
              "connections=%d" % (len(conn.connection_set) if conn else 0))
        check("A2 消费类型是 CONSUME_ACTIVELY（Java DefaultMQPullConsumerImpl:348）",
              conn is not None and conn.consume_type == "CONSUME_ACTIVELY",
              "consumeType=%s" % (conn.consume_type if conn else None))
        check("A2 消费位点是 CONSUME_FROM_LAST_OFFSET（:353）",
              conn is not None and conn.consume_from_where == "CONSUME_FROM_LAST_OFFSET",
              "consumeFromWhere=%s" % (conn.consume_from_where if conn else None))
        check("A2 广播/集群口径是 CLUSTERING",
              conn is not None and conn.message_model == "CLUSTERING",
              "messageModel=%s" % (conn.message_model if conn else None))

        # ---- A2b 订阅集来自 registerTopics（Java subscriptions():357-385）----
        subs = (conn.subscription_table or {}) if conn else {}
        entry = subs.get(TOPIC)
        sub_string = None
        if isinstance(entry, dict):
            sub_string = entry.get("subString") or entry.get("sub_string")
        check("A2b 203 的订阅表带 registerTopics 的 topic 且 subString=*",
              sub_string == "*", "subscriptionTable=%s" % subs)

        # ---- A3 38 主节点：consumerTable 里的 clientId 列表 ----
        client = consumer._mq_client
        ids = consumer_ids(client, GROUP, MASTER)
        check("A3 主节点 38 查到本 clientId", consumer.client_id in ids, "ids=%s" % ids)

        # ---- A4 从节点：心跳同样落到每一台 ----
        if SLAVE:
            slave_conn = group_is_online(admin, GROUP, SLAVE)
            check("A4 从节点 203 也查到本组（心跳扇出到从节点）", slave_conn is not None,
                  "slave=%s connections=%d"
                  % (SLAVE, len(slave_conn.connection_set) if slave_conn else 0))
            slave_ids = consumer_ids(client, GROUP, SLAVE)
            check("A4 从节点 38 也查到本 clientId", consumer.client_id in slave_ids,
                  "ids=%s" % slave_ids)

        # ---- A5 对照：幽灵组（从未心跳）必须查不到 ----
        ghost_conn = group_is_online(admin, GHOST_GROUP, MASTER)
        ghost_ids = consumer_ids(client, GHOST_GROUP, MASTER)
        check("A5 对照：未心跳的幽灵组 203 查不到", ghost_conn is None,
              "ghost connections=%d" % (len(ghost_conn.connection_set) if ghost_conn else 0))
        check("A5 对照：未心跳的幽灵组 38 空列表", ghost_ids == [], "ids=%s" % ghost_ids)

        # ---- A6 shutdown 立刻注销（35），不必等 ~120s 通道扫描 ----
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
        check("A6 shutdown 后 203 立刻查不到本组（发过 35 注销）", gone, "clientId=%s" % client_id)
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
