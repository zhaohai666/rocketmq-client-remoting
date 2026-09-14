# -*- coding: utf-8 -*-
"""rocketmq-client-remoting 真实集群联调测试。

前置：本地已启动 nameServer(9876) + broker(10911)，broker 开启 autoCreateTopic。
运行：在 python venv 中 `python integration_live_test.py`
"""
import sys
import time
import threading

from rocketmq.client.producer import DefaultMQProducer
from rocketmq.client.consumer import DefaultMQPushConsumer, SimpleMessageListener
from rocketmq.common.message import Message, MessageBatch
from rocketmq.remoting.protocol.heartbeat import ConsumeFromWhere
from rocketmq.client.consumer_result import ConsumeConcurrentlyStatus

NAMESRV = "127.0.0.1:9876"
TOPIC = "PythonTestTopic"
GROUP = "PythonTestConsumerGroup"
N = 10

results = []


def check(name, ok, detail=""):
    results.append((name, ok, detail))
    print("[%s] %s %s" % ("PASS" if ok else "FAIL", name, detail))


def main():
    # ---------- 1. 集群探活 ----------
    prod = DefaultMQProducer(producer_group="PythonTestProducerGroup")
    prod.set_namesrv_addr(NAMESRV)
    prod.set_send_msg_timeout(5000)
    prod.start()
    try:
        info = prod._mq_client.get_broker_cluster_info()
        brokers = list(info.broker_addr_table.keys()) if info and info.broker_addr_table else []
        check("集群探活: 获取 broker 路由", bool(brokers), "brokers=%s" % brokers)
    except Exception as e:  # noqa: BLE001
        check("集群探活: 获取 broker 路由", False, "exception=%s" % e)
        prod.shutdown()
        return 1

    # ---------- 2. 同步发送 N 条 ----------
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
        check("同步发送 %d 条" % N, send_ok == N,
              "send_ok=%d/%d, 首条 msg_id=%s" % (send_ok, N, send_ids[0] if send_ids else ""))
    except Exception as e:  # noqa: BLE001
        check("同步发送 %d 条" % N, False, "exception=%s" % e)

    # ---------- 3. 批量发送 ----------
    try:
        batch = MessageBatch.generate_from_list([
            Message(TOPIC, ("batch-%d" % i).encode("utf-8")) for i in range(3)
        ])
        bsr = prod.send(batch)
        check("批量发送 3 条", bsr.send_status.name == "SEND_OK", "msg_id=%s" % bsr.msg_id)
    except Exception as e:  # noqa: BLE001
        check("批量发送 3 条", False, "exception=%s" % e)

    # ---------- 4. 单向发送（不保证落库，仅验证不抛异常）----------
    try:
        prod.send_oneway(Message(TOPIC, b"oneway-ping"))
        check("单向发送", True, "无异常返回")
    except Exception as e:  # noqa: BLE001
        check("单向发送", False, "exception=%s" % e)

    # ---------- 5. 推送消费（从最早 offset 消费）----------
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

    # 等待消费（含同步 10 + 批量 3 + 单向 1）
    time.sleep(12)
    cons.shutdown()

    recv_set = set(b.decode("utf-8", "ignore") for b in received)
    expected_sync = set("hello-rocketmq-%d" % i for i in range(N))
    expected_batch = set("batch-%d" % i for i in range(3))
    missing_sync = expected_sync - recv_set
    missing_batch = expected_batch - recv_set
    check("消费数量(%d)" % len(received), len(received) >= N,
          "received=%d (期望>=%d)" % (len(received), N))
    check("同步消息内容完整", not missing_sync,
          "missing=%s" % (list(missing_sync) if missing_sync else "无"))
    check("批量消息内容完整", not missing_batch,
          "missing=%s" % (list(missing_batch) if missing_batch else "无"))

    prod.shutdown()

    # ---------- 汇总 ----------
    failed = [r for r in results if not r[1]]
    print("\n================ 汇总 ================")
    for name, ok, detail in results:
        print("  [%s] %s" % ("PASS" if ok else "FAIL", name))
    print("======================================")
    if failed:
        print("结果: %d 项失败" % len(failed))
        return 1
    print("结果: 全部通过")
    return 0


if __name__ == "__main__":
    sys.exit(main())
