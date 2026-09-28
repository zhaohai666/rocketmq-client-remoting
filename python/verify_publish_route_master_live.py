#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""发布路由必须跳过「没有 master 的 broker」真机验证（Java MQClientInstance:294-303）。

用法（需本地 RocketMQ 5.5.1 集群，broker-a 带一台从节点；脚本会**停一次再拉起 master**）：
    .venv/bin/python verify_publish_route_master_live.py 127.0.0.1:9876 127.0.0.1:10911 127.0.0.1:10931

前置：从节点必须在跑（S0 起就红，不静默退化）；master 由 ``scripts/rmq_test_broker.sh``
（**只碰 master**）停/起，脚本用 try/finally 保证 master 一定被拉回来。

Java 依据：``MQClientInstance.topicRouteData2TopicPublishInfo:294-303`` 组装发布信息时，
brokerDatas 里没有同名 broker、或它的 brokerAddrs 没有 MASTER_ID，整条 QueueData 跳过。
从节点自己也会注册进 namesrv，且默认配置下照样带写位（``RouteInfoManager`` 只在
「prime slave 且 enableActingMaster」时才抹掉 WRITE，本机 broker.conf 是 false），
所以 master 一掉线，路由里同一个 brokerName 只剩 brokerId=1 —— 漏判这条，生产者就会
把消息发到从节点上，而从节点对发送请求一律 reject（SendMessageProcessor ⇒
SYSTEM_BUSY(2)，**还是可重试码**），白烧重试。
消费侧是另一份口径（``topicRouteData2TopicSubscribeInfo:318-332``：读位 + readQueueNums、
**不要求有 master**），停窗口内消费者仍要看得见队列、还得能从从节点拉。

场景（同一停窗口里做完）：
  S0 控制腿（master 在）：路由 {0: master, 1: slave}；发布队列 4、订阅队列 4
  S1 预埋：每队列定点一条共 4 条，等从节点 store 追上（HA 复制确认，不然 S6 无从消费）
  S2 停 master → 刷新路由直到 broker-a 只剩 {1: slave}
  S3 (A) 发布队列 == 0；访问器本端抛「选不到队列」（不再给出会把消息发到从节点的假队列）
  S4 (C) 订阅队列仍是 4（消费侧不看 master）
  S5 发送快速失败、报错里没有从节点地址（旧缓存腿打的是死掉的 master；周期刷新恰好
     已跑过则是本端 10005）；再显式把发送实例刷成停后形状：(A) 生效 —— 发布队列为空、
     发送本端 10005 且**一条 wire 都不发**
  S5d 对照：定点发到该队列 → 从节点回 SYSTEM_BUSY(2)（可重试）—— S5c 若漏做，
      不指定队列的发送就是 3 次 wire 全被拒的下场（一条也落不了库，S7b 用 maxOffset 钉死）
  S6 (C 端到端) 停窗口内新起的 push 消费者仍看到 4 条队列，并从**从节点**把 S1 的 4 条收齐
  S7 负控：master 拉回 → 发布队列恢复 4、两条失败发送都没在 broker 上留下消息、发送 SEND_OK
"""
from __future__ import annotations

import os
import subprocess
import sys
import threading
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from rocketmq.client.admin import DefaultMQAdminExt
from rocketmq.client.consumer import DefaultMQPushConsumer, SimpleMessageListener
from rocketmq.client.consumer_result import ConsumeConcurrentlyStatus
from rocketmq.client.exception import ClientErrorCode, MQClientException
from rocketmq.client.mq_client import MQClientInstance
from rocketmq.client.producer import DefaultMQProducer
from rocketmq.client.send_result import SendStatus
from rocketmq.common.message import Message, MessageQueue
from rocketmq.remoting.protocol.codes import ResponseCode
from rocketmq.remoting.protocol.heartbeat import ConsumeFromWhere

NAMESRV = sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1:9876"
MASTER_ARG = sys.argv[2] if len(sys.argv) > 2 else "127.0.0.1:10911"
SLAVE_ARG = sys.argv[3] if len(sys.argv) > 3 else "127.0.0.1:10931"
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BROKER_CTL = os.path.join(ROOT, "scripts", "rmq_test_broker.sh")

STAMP = int(time.time() * 1000)
TOPIC = "PrMasterLive_%d" % STAMP
GROUP = "GID_PrMasterLive_%d" % STAMP
BROKER_NAME = "broker-a"
# 本端失败上界：发布信息为空时压根没有 wire 调用，真机给 500ms 已留量级余量
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
    """跑 scripts/rmq_test_broker.sh，返回 (returncode, 输出)。

    输出走**文件**而不是管道：start 会把 broker 拉成常驻进程，管道的写端被它继承，
    ``subprocess`` 就要等到写端全部关闭才返回 —— 表现为脚本已经打印结果，调用方还卡着。
    """
    out_path = "/tmp/rmq_pr_master_broker_ctl.%s.log" % action
    with open(out_path, "w") as fh:
        proc = subprocess.run(["sh", BROKER_CTL, action], stdout=fh, stderr=fh,
                              timeout=600, start_new_session=True)
    with open(out_path) as fh:
        return proc.returncode, fh.read().strip()


def broker_addrs_of(inst: MQClientInstance, topic: str):
    """强制刷新后取 broker-a 的 brokerAddrs（刷不到返回 None）。"""
    try:
        inst.update_topic_route_info_from_name_server(topic, 5000)
    except Exception as e:  # noqa: BLE001
        print("    (路由刷新失败: %s)" % e)
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
    print("    (等不到 %s，最后一次: %s)" % (what, last))
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
        # ---------- S0 控制腿 ----------
        print("S0 控制腿（master 在）：路由 {0: master, 1: slave}、发布/订阅各 4 条")
        inst.create_topic_in_route(TOPIC, 4, 4)
        try:
            # 从节点也直建一份，不赌 SlaveSynchronize 的 5s 周期
            admin.create_topic_in_broker(SLAVE_ARG, TOPIC, 4, 4)
        except Exception as e:  # noqa: BLE001
            print("    (从节点建 topic 失败: %s)" % e)

        def route_ok():
            addrs = broker_addrs_of(inst, TOPIC)
            return addrs if addrs and set(addrs) >= {0, 1} else None

        addrs = wait_for(route_ok, 30, "路由 {0, 1}")
        check("S0 路由含 {0: master, 1: slave}", addrs is not None,
              "broker_addrs=%s" % (addrs or {}))
        if addrs is None:
            print("== 前置不满足：没有可用的主从地址 ==")
            return 1
        check("S0 从节点地址与参数一致", addrs.get(1) == SLAVE_ARG,
              "route=%s argv=%s" % (addrs.get(1), SLAVE_ARG))

        def publish_queues():
            info = inst.get_topic_publish_info(TOPIC)
            return list(info.msg_queue_list)

        queues = wait_for(lambda: publish_queues() or None, 15, "发布队列")
        check("S0 发布队列 4（控制）", bool(queues) and len(queues) == 4,
              "queues=%s" % ([(q.broker_name, q.queue_id) for q in queues or []]))
        subs = inst.get_topic_subscribe_info(TOPIC)
        check("S0 订阅队列 4（控制）", len(subs) == 4,
              "queues=%s" % ([(q.broker_name, q.queue_id) for q in subs]))
        if not queues:
            return 1

        # ---------- S1 预埋 ----------
        print("\nS1 预埋 4 条（每队列定点一条）并等从节点 store 追上")
        seeded = []
        for i, q in enumerate(sorted(queues, key=lambda x: x.queue_id)):
            body = ("pr-master-%d" % i).encode()
            r = producer.send(Message(TOPIC, body),
                              mq=MessageQueue(TOPIC, q.broker_name, q.queue_id))
            seeded.append(body)
            if r.send_status != SendStatus.SEND_OK:
                check("S1 第 %d 条预埋 SEND_OK" % i, False, "status=%s" % r.send_status)
        check("S1 4 条预埋全部 SEND_OK", len(seeded) == 4)

        master_max = {q.queue_id: inst.get_max_offset(q) for q in queues}

        def slave_caught_up():
            if not master_max:
                return None
            try:
                slave_max = {q.queue_id: inst.get_max_offset(q, addr=SLAVE_ARG)
                             for q in queues}
            except Exception as e:  # noqa: BLE001
                print("    (从节点取 maxOffset 失败: %s)" % e)
                return None
            return slave_max if all(slave_max[k] >= v for k, v in master_max.items()) else None

        slave_max = wait_for(slave_caught_up, 30, "从节点复制追上")
        check("S1 4 条已复制到从节点 store", slave_max is not None,
              "master=%s slave=%s" % (master_max, slave_max or {}))

        # ---------- S2 停 master ----------
        print("\nS2 停 master（scripts/rmq_test_broker.sh stop），等路由只剩从节点")
        code, out = broker_ctl("stop")
        check("S2 master 已优雅停机", code == 0, out)

        def masterless():
            addrs_now = broker_addrs_of(inst, TOPIC)
            return addrs_now if addrs_now and 0 not in addrs_now and 1 in addrs_now else None

        down_addrs = wait_for(masterless, 60, "masterless 路由")
        check("S2 路由里 broker-a 只剩 {1: slave}", down_addrs is not None,
              "broker_addrs=%s" % (down_addrs or {}))
        if down_addrs is None:
            return 1

        # ---------- S3 (A) 发布队列 ----------
        print("\nS3 (A) 发布信息跳过没有 master 的 broker")
        info = inst.topic_publish_info_table.get(TOPIC)
        n_pub = len(info.msg_queue_list) if info is not None else -1
        check("S3 停 master 后发布队列 == 0", n_pub == 0, "msg_queue_list=%d" % n_pub)
        acc_exc = None
        try:
            inst.get_topic_publish_info(TOPIC)
        except MQClientException as e:
            acc_exc = e
        check("S3b 发布信息访问器本端抛「选不到队列」",
              acc_exc is not None and "Can not find Message Queue for topic" in str(acc_exc),
              "%s" % acc_exc)

        # ---------- S4 (C) 订阅队列 ----------
        subs_down = inst.get_topic_subscribe_info(TOPIC)
        check("S4 (C) 订阅队列仍是 4、且都在 broker-a（消费侧不看 master）",
              len(subs_down) == 4 and all(q.broker_name == BROKER_NAME for q in subs_down),
              "queues=%s" % ([(q.broker_name, q.queue_id) for q in subs_down]))

        # ---------- S5 (A 定型) 发送快速失败 ----------
        # 分两段看：**缓存还没刷**时（现实中 30s 周期任务未到）发送实例手里还是停前的旧
        # 路由，地址解析落在死掉的 master 上，快速失败、绝不静默改发从节点；**路由刷成停后
        # 形状**后（周期任务 / 显式刷新），(A) 生效：发布队列为空，发送连一条 wire 都不发。
        print("\nS5 不指定队列的同步发送：旧缓存快速失败 → 刷新后本端快速失败")
        began = time.monotonic()
        exc = None
        try:
            producer.send(Message(TOPIC, b"must-not-send"))
        except Exception as e:  # noqa: BLE001
            exc = e
        stale_ms = (time.monotonic() - began) * 1000
        # 这一腿是机会腿：周期刷新是否已经跑过不由本脚本定。两条腿的共同判据是
        # "快速失败 + 绝不落到从节点地址上"（旧缓存腿打的是死掉的 master）。
        check("S5 发送快速失败，且报错里没有从节点地址（绝不改发从节点）",
              exc is not None and stale_ms < LOCAL_BUDGET_MS and SLAVE_ARG not in str(exc),
              "%.0fms %s: %s" % (stale_ms, type(exc).__name__, exc))

        # 让发送实例自己的路由缓存刷成停后形状（与 30s 周期任务同一条代码路径）
        pcli = producer._require_client()
        pcli.update_topic_route_info_from_name_server(TOPIC, 5000)
        p_info = pcli.topic_publish_info_table.get(TOPIC)
        n_pub_send = len(p_info.msg_queue_list) if p_info is not None else -1
        check("S5b 发送实例的发布队列也 == 0（(A) 就作用在这里）", n_pub_send == 0,
              "msg_queue_list=%d" % n_pub_send)

        began = time.monotonic()
        exc2 = None
        try:
            producer.send(Message(TOPIC, b"must-not-send-2"))
        except Exception as e:  # noqa: BLE001
            exc2 = e
        fresh_ms = (time.monotonic() - began) * 1000
        check("S5c 刷新后：本端 10005 抛「选不到队列」，无 wire 调用（无 BrokersSent）",
              isinstance(exc2, MQClientException)
              and exc2.response_code == ClientErrorCode.NOT_FOUND_TOPIC_EXCEPTION
              and "Can not find Message Queue for topic" in str(exc2)
              and "BrokersSent" not in str(exc2)
              and fresh_ms < LOCAL_BUDGET_MS,
              "%.0fms code=%s: %s" % (fresh_ms, getattr(exc2, "response_code", None), exc2))

        # ---------- S5d 对照：定点发送的下场 ----------
        print("\nS5d 对照：定点发到该队列 → 从节点拒收（SYSTEM_BUSY=2，可重试码）")
        pinned_exc = None
        try:
            producer.send(Message(TOPIC, b"pinned-to-slave"),
                          mq=MessageQueue(TOPIC, BROKER_NAME, 0))
        except Exception as e:  # noqa: BLE001
            pinned_exc = e
        check("S5d 从节点回 SYSTEM_BUSY(2)（S5c 若漏做，不指定队列的发送就是这个下场）",
              getattr(pinned_exc, "response_code", None) == ResponseCode.SYSTEM_BUSY,
              "%s: %s" % (type(pinned_exc).__name__, pinned_exc))

        # ---------- S6 (C 端到端) 停窗口内消费 ----------
        print("\nS6 停窗口内新起的 push 消费者：4 条队列 + 从从节点收齐预埋的 4 条")

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
        check("S6a 窗口内消费者自己的订阅信息也是 4 条",
              len(subs_in_window) == 4,
              "queues=%s" % ([(q.broker_name, q.queue_id) for q in subs_in_window]))
        wait_for(lambda: sorted(got) == sorted(seeded), 60, "4 条预埋消息")
        check("S6 停 master 期间从从节点收齐 4 条",
              sorted(got) == sorted(seeded), "got=%s" % sorted(got))

        # ---------- S7 负控（先把 master 拉回来） ----------
        print("\nS7 负控：master 拉回后发布队列恢复、发送恢复")
        code, out = broker_ctl("start")
        if code != 0:
            print("  [FAIL] master 复位失败: %s" % out)

        def publish_back():
            try:
                return list(inst.get_topic_publish_info(TOPIC).msg_queue_list) or None
            except Exception:  # noqa: BLE001
                return None

        queues_back = wait_for(publish_back, 60, "发布队列恢复")
        check("S7 发布队列恢复 4", bool(queues_back) and len(queues_back) == 4,
              "queues=%s" % ([(q.broker_name, q.queue_id) for q in queues_back or []]))
        # 两条失败发送都没在 broker 上留下消息：每条预埋队列的 maxOffset 仍是 1
        after = {q.queue_id: inst.get_max_offset(q) for q in (queues_back or queues or [])}
        check("S7b 两条失败发送没留下消息（maxOffset 仍是 1）",
              bool(after) and all(v == 1 for v in after.values()), "maxOffset=%s" % after)
        r = producer.send(Message(TOPIC, b"pr-master-back"))
        check("S7c 发送恢复 SEND_OK", r.send_status == SendStatus.SEND_OK,
              "status=%s msgId=%s" % (r.send_status, r.msg_id))
    finally:
        # master 必须回来（start 幂等，异常路径也走到这里）
        broker_ctl("start")
        for c in (consumer, producer):
            if c is not None:
                try:
                    c.shutdown()
                except Exception as e:  # noqa: BLE001
                    print("    (shutdown 失败: %s)" % e)
        try:
            inst.shutdown()
        except Exception as e:  # noqa: BLE001
            print("    (instance shutdown 失败: %s)" % e)
        try:
            admin.delete_topic(TOPIC)
        except Exception as e:  # noqa: BLE001
            print("    (delete_topic(%s) 失败: %s)" % (TOPIC, e))
        admin.shutdown()

    print("\n== 结果: %d/%d 通过 ==" % (PASS, PASS + FAIL))
    return 0 if FAIL == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
