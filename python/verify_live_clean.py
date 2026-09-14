# -*- coding: utf-8 -*-
"""rocketmq-client-remoting 干净环境端到端验证。

与 integration_live_test.py 的区别：本脚本使用一个「全新的、带时间戳的唯一
topic + 唯一 consumer group + 全新 store 目录」，从而排除「/tmp/rmqstore 跨多次
运行累积消息 + CONSUME_FROM_FIRST_OFFSET 从 0 重读」导致的消息计数虚高问题。

本脚本自身不启动集群；调用方负责先启动 nameServer(9876)+broker(10911) 且
broker 开启 autoCreateTopic。运行：在 python venv 中 `python verify_live_clean.py`
"""
import os
import sys
import time
import threading

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from rocketmq.client.producer import DefaultMQProducer
from rocketmq.client.consumer import DefaultMQPushConsumer, SimpleMessageListener
from rocketmq.common.message import Message, MessageBatch
from rocketmq.remoting.protocol.heartbeat import ConsumeFromWhere
from rocketmq.client.consumer_result import ConsumeConcurrentlyStatus

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
    # 轮询等待 broker 在 nameServer 完成注册（避免端口刚开、注册尚未落地的竞态）
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
        check("集群探活", True, "brokers=%s" % brokers)
    else:
        check("集群探活", False, "nameServer 无 broker 注册 last_err=%s" % last_err)
        prod.shutdown()
        return 1

    # ---------- 生产：恰好 14 条（10 同步 + 3 批量 + 1 单向）----------
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
    check("同步发送 %d 条" % N_SYNC, send_ok == N_SYNC, "send_ok=%d/%d" % (send_ok, N_SYNC))

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
    check("批量发送 %d 条" % N_BATCH, bok, "status=%s" % ("SEND_OK" if bok else "ERR"))

    try:
        prod.send_oneway(Message(TOPIC, b"oneway-ping"))
        ow_ok = True
    except Exception as e:  # noqa: BLE001
        ow_ok = False
        print("oneway error: %s" % e)
    expected.add("oneway-ping")
    check("单向发送", ow_ok, "无异常返回")

    total_produced = N_SYNC + N_BATCH + 1
    print("produced %d messages (topic=%s group=%s)" % (total_produced, TOPIC, GROUP))

    # ---------- 消费：唯一 group + 从最早 offset ----------
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

    print("\n--- 计数对账 ---")
    print("生产总数            : %d" % total_produced)
    print("消费投递总数        : %d" % received_n)
    print("消费去重 body 数    : %d" % len(recv_set))
    print("去重 (queue,offset) : %d" % len(distinct_qo))
    print("重复 body 数        : %d" % dup_count)
    print("多余 body(非生产)   : %s" % (list(extra) if extra else "无"))
    print("缺失 body(未消费)   : %s" % (list(missing) if missing else "无"))

    check("消费计数==生产计数(14)", received_n == total_produced,
          "received=%d expected=%d" % (received_n, total_produced))
    check("无重复投递", dup_count == 0, "dup=%d" % dup_count)
    check("无多余消息", not extra, "extra=%s" % (list(extra) if extra else "无"))
    check("无丢失消息", not missing, "missing=%s" % (list(missing) if missing else "无"))

    failed = [r for r in results if not r[1]]
    print("\n================ 汇总 ================")
    for name, ok, detail in results:
        print("  [%s] %s" % ("PASS" if ok else "FAIL", name))
    print("======================================")
    if failed:
        print("结果: %d 项失败" % len(failed))
        return 1
    print("结果: 全部通过（计数对账一致，证明此前 19/36 为 store 累积假象）")
    return 0


if __name__ == "__main__":
    sys.exit(main())
