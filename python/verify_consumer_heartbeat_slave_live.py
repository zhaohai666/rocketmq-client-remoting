#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""消费者心跳必须扇出到**从节点**（真机验证，Java sendHeartbeatToAllBroker 的分流规则）。

用法（需本地 RocketMQ 5.5.1 集群，且 broker-a 有一台从节点）：
    .venv/bin/python verify_consumer_heartbeat_slave_live.py 127.0.0.1:9876 127.0.0.1:10931

从节点（brokerId=1）没起时本脚本 S0 就失败 —— 这是刻意的：本机默认集群只有一台 master，
不满足前置条件时宁可红，也不要静默退化成"只测了 master"。

Java 依据：``MQClientInstance#sendHeartbeatToAllBroker``:732-750 遍历 ``brokerAddrTable``
的**每个 brokerId**，只在 ``consumerEmpty && MixAll.MASTER_ID != id`` 时跳过从节点。
带 ConsumerData 的消费者心跳因此必须打到每一台；只带 ProducerData 的生产者心跳
（``consumerEmpty=true``）仍然只打 master。

为什么漏发从节点不是"少一发冗余"：broker 的 ``ConsumerManager`` 是**每台各自一份**状态，
从节点没见过这个组时：
  * ``GET_CONSUMER_LIST_BY_GROUP(38)`` 在从节点上查不到本组（本脚本 S2/S2b）；
  * 指向从节点的拉取被 ``PullMessageProcessor``:420-427 回 ``SUBSCRIPTION_NOT_EXIST(24)``
    —— push 消费者默认的拉取**不带**订阅标志（``postSubscriptionWhenPull=false``，
    ``DefaultMQPushConsumerImpl.pullMessage``:458-468），走的正是那条 else 分支（S4/S4b）。

场景：
  S0 路由里 broker-a 有 {0: master, 1: slave}；client 侧 ``get_all_broker_addrs`` 两台、
     ``get_route_of_all_brokers`` 只有 master（两个 helper 的分工）
  S1 起 push consumer（订阅 topic）→ 等首轮心跳
  S2 【核心】从节点上 38 查到本 clientId
  S2b 对照：同一个从节点查一个**从未心跳过**的组 → 查不到（38 这条判据本身有效）
  S3 主节点上 38 也查到本组（组确实注册进了集群）
  S4 无订阅标志的拉取打**从节点** → 不再是 24
  S4b 对照：同一个从节点的无标志拉取、未注册组 → 仍是 24（那道门真按每台自己的表判）
  S5 生产者心跳仍只打 master：从节点 204 ``GET_PRODUCER_CONNECTION_LIST`` 查不到本组，
     主节点查得到 —— 扇出只对消费者打开
"""
from __future__ import annotations

import sys
import threading
import time

sys.path.insert(0, ".")

from rocketmq.client.admin import DefaultMQAdminExt
from rocketmq.client.consumer import (ConsumeConcurrentlyStatus,
                                      DefaultMQPushConsumer,
                                      SimpleMessageListener)
from rocketmq.client.exception import MQBrokerException
from rocketmq.client.mq_client import MQClientInstance
from rocketmq.client.producer import DefaultMQProducer
from rocketmq.common.message import Message, MessageQueue
from rocketmq.common.sysflag import PullSysFlag
from rocketmq.remoting.protocol.codes import ResponseCode

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
        # ---------- 建 topic（路由给 master，另在从节点上各建一份） ----------
        # 从节点自己的 TopicConfigManager 靠 SlaveSynchronize 同步（首轮 ~10s、之后 60s 一次），
        # 不等它同步：PullMessageProcessor 的 topic 检查（:347）排在订阅检查（:420）之前，
        # 从节点不认识 topic 时会先回 TOPIC_NOT_EXIST(17) 把要验的那道门整个遮住。
        print("建 topic（master 走 route + slave 直连各一份）")
        inst = MQClientInstance("hb-slave-setup-%d" % STAMP, [NAMESRV])
        inst.start()
        inst.create_topic_in_route(TOPIC, 4, 4)

        # ---------- S0 路由里主从都在 ----------
        print("\nS0 路由：broker-a 的 {0: master, 1: slave}")
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
        check("S0 路由含 {0: master, 1: slave}", set(addrs) >= {0, 1},
              "broker_addrs=%s" % addrs)
        master = addrs.get(0, "127.0.0.1:10911")
        slave = addrs.get(1, SLAVE_ARG)
        check("S0 从节点地址与参数一致", slave == SLAVE_ARG,
              "route=%s argv=%s" % (slave, SLAVE_ARG))
        # 从节点也要认识 topic：直连它在它自己的 TopicConfigManager 里建一份
        try:
            admin.create_topic_in_broker(slave, TOPIC, 4, 4)
            print("    (已在从节点 %s 建 topic %s)" % (slave, TOPIC))
        except Exception as e:  # noqa: BLE001
            print("    (从节点建 topic 失败: %s)" % e)
        check("S0 getAllBrokerAddrs 两台、getRouteOfAllBrokers 只有 master",
              sorted(inst.get_all_broker_addrs()) == sorted([master, slave])
              and inst.get_route_of_all_brokers() == [master],
              "all=%s probe=%s" % (inst.get_all_broker_addrs(),
                                   inst.get_route_of_all_brokers()))
        if not (master and slave and master != slave):
            print("\n== 前置不满足：没有可用的主从地址 ==")
            return 1

        # ---------- S1 消费者起来并注册 ----------
        print("\nS1 push consumer 起并等首轮心跳")
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
        check("S1 首轮心跳已发出", consumer.heartbeat_count() >= 1,
              "heartbeat_count=%d clientId=%s" % (consumer.heartbeat_count(), client_id))

        def ids_at(addr, group):
            """直接打指定地址查 38；broker 回「组不存在」也走 SUCCESS+空表。"""
            try:
                body = client.get_consumer_list_by_group(group, 5000, addr=addr)
            except Exception as e:  # noqa: BLE001
                return None, "%s: %s" % (type(e).__name__, e)
            return list(body.consumer_id_list or []), ""

        # ---------- S2 从节点也认识本组 ----------
        print("\nS2 从节点上 GET_CONSUMER_LIST_BY_GROUP(38) 能看到本 clientId")
        seen, err = ids_at(slave, GROUP)
        check("S2 从节点 38 查到本 clientId（心跳真扇出到从节点）",
              bool(seen) and client_id in seen,
              "slave=%s seen=%s %s" % (slave, seen, err))
        # ---------- S2b 负向对照 ----------
        ghost, err2 = ids_at(slave, GHOST_GROUP)
        check("S2b 对照组：从节点查一个从未心跳过的组 → 空",
              ghost == [] or ghost is None,
              "ghost=%s %s" % (ghost, err2))
        # ---------- S3 主节点同样认识本组 ----------
        seen_m, err3 = ids_at(master, GROUP)
        check("S3 主节点 38 也查到本组（说明该组确实注册成了）",
              bool(seen_m) and client_id in seen_m,
              "master=%s seen=%s %s" % (master, seen_m, err3))

        # ---------- S4 指向从节点的「无订阅标志」拉取 ----------
        # push 消费者的默认形状：subscription=False（postSubscriptionWhenPull=false）
        print("\nS4 无订阅标志拉取打从节点（push 默认形状）")
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
        check("S4 从节点对上已注册组不再回 SUBSCRIPTION_NOT_EXIST(24)",
              status == "OK",
              "slave=%s -> %s %s" % (slave, status or "拒绝", err4))
        status_g, err5 = pull_at(slave, GHOST_GROUP)
        check("S4b 对照组：同一个从节点对未注册组仍回 24（那道门真按每台自己的表判）",
              "code=%d" % ResponseCode.SUBSCRIPTION_NOT_EXIST in err5,
              "ghost -> %s" % err5)

        # ---------- S5 生产者心跳仍只打 master ----------
        print("\nS5 生产者心跳只打 master：从节点 204 查不到本组，主节点查得到")
        producer = DefaultMQProducer(PRODUCER_GROUP)
        producer.set_namesrv_addr(NAMESRV)
        producer.start()
        # 生产者 start() 时的首轮心跳可能赶在路由表填好之前（那时已知 broker 为空、发 0 发），
        # 先发一笔把路由拉起来，再等下一轮心跳周期（默认 30s）把组登记上去。
        producer.send(Message(TOPIC, b"hb-slave-probe"))
        wait = time.time() + 70
        pc_master = None
        while time.time() < wait:
            pc_master = _connection_client_ids(admin, master, PRODUCER_GROUP)
            if pc_master:
                break
            time.sleep(1)
        check("S5 主节点 204 能看到本生产者（心跳链路通）",
              bool(pc_master), "master=%s seen=%s" % (master, pc_master))
        pc_slave = _connection_client_ids(admin, slave, PRODUCER_GROUP)
        check("S5b 从节点 204 查不到本生产者组（生产者心跳仍只打 master）",
              not pc_slave, "slave=%s seen=%s" % (slave, pc_slave))
    finally:
        for c in (consumer, producer):
            if c is not None:
                try:
                    c.shutdown()
                except Exception as e:  # noqa: BLE001
                    print("    (shutdown 失败: %s)" % e)
        if inst is not None:
            try:
                inst.shutdown()
            except Exception as e:  # noqa: BLE001
                print("    (setup instance shutdown 失败: %s)" % e)
        try:
            admin.delete_topic(TOPIC)
        except Exception as e:  # noqa: BLE001
            print("    (delete_topic(%s) 失败: %s)" % (TOPIC, e))
        admin.shutdown()

    print("\n== 结果: %d/%d 通过 ==" % (PASS, PASS + FAIL))
    return 0 if FAIL == 0 else 1


def _connection_client_ids(admin: DefaultMQAdminExt, broker_addr: str, group: str):
    """204 看到的 clientId 列表；broker 说「组不存在」时回 None。"""
    try:
        pc = admin.examine_producer_connection_info(group, broker_addr)
    except Exception as e:  # noqa: BLE001
        if "not exist" in str(e):
            return None
        raise
    return [c.client_id for c in pc.connection_set]


if __name__ == "__main__":
    sys.exit(main())
