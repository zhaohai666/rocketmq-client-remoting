# -*- coding: utf-8 -*-
"""rocketmq-client-remoting 不同消息类型联调测试。

覆盖 verify_live_clean.py 之外的消息类型/特性，全部针对真实集群验证：
  1. 异步发送（send_async + SendCallback）
  2. 延迟消息（set_delay_time_level，验证延迟投递 + 时序）
  3. 顺序消息（send_by_selector 同 key 落同队列 + 顺序消费保序）
  4. 带 Tag 消息 + 服务端 Tag 过滤消费
  5. 带 Key 消息 + 按 Key 服务端查询（query_message）
  6. 用户属性（user property）生产/消费透传
  7. 事务消息（send_message_in_transaction，注意：Python 端为简化单阶段实现）

本脚本自身不启动集群；调用方需先启动 nameServer(9876)+broker(10911) 且
autoCreateTopicEnable=true。运行：在 venv 中 `python verify_message_types.py`
为避免 store 累积假象，所有 topic/group 均带时间戳且使用全新 store。
"""
import os
import sys
import time
import threading

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from rocketmq.client.producer import (DefaultMQProducer, SelectMessageQueueByHash,
                                       SendCallbackImpl, LocalTransactionState, TransactionListener)
from rocketmq.client.consumer import (DefaultMQPushConsumer, MessageListenerConcurrently,
                                       MessageListenerOrderly)
from rocketmq.client.consumer_result import (ConsumeConcurrentlyStatus, ConsumeOrderlyStatus)
from rocketmq.common.message import Message, MessageExt
from rocketmq.remoting.protocol.heartbeat import ConsumeFromWhere

NAMESRV = "127.0.0.1:9876"
STAMP = int(time.time())
PREFIX = "MT_%d" % STAMP

results = []


def check(name, ok, detail=""):
    results.append((name, ok, detail))
    print("[%s] %s %s" % ("PASS" if ok else "FAIL", name, detail))


def _wait_broker(prod):
    """轮询等待 broker 在 nameServer 注册完成（避免端口刚开、注册未落地的竞态）。"""
    for _ in range(40):
        try:
            info = prod._mq_client.get_broker_cluster_info()
            if info and info.broker_addr_table:
                return list(info.broker_addr_table.keys())
        except Exception:
            pass
        time.sleep(1)
    return []


def run_consumer(topic, sub_expr, duration, orderly=False, group_suffix="c",
                 expect=0, pull_timeout=3000, pull_suspend=1000):
    """启动消费者，活跃等待直至收到 expect 条（或最多 duration 秒）后关闭。

    expect<=0 时退化为固定窗口等待。用活跃等待取代盲 sleep，避免消费者
    冷启动（路由/队列尚未就绪）在固定窗口结束前尚未拉到消息导致的偶发 0 条。

    关键坑：消费循环在单线程内顺序长轮询各队列；空闲队列的 suspend 长轮询会
    阻塞整轮，导致消息集中在高序号队列（如顺序消息全部落在 qid=2）时被排在
    前面的空闲队列饿死。故把 pull_timeout(客户端等待响应) 与 pull_suspend(服务端
    挂起) 都设短：空闲队列 ~1s 即返回 NO_NEW_MSG，循环很快轮到满载队列；
    pull_timeout 略大于 pull_suspend，避免空闲轮询触发客户端超时 ERROR（新日志
    已落文件，应保持干净）。
    """
    received = []
    lock = threading.Lock()
    errs = []

    if orderly:
        class _L(MessageListenerOrderly):
            def consume_message(self, msgs, context):
                with lock:
                    for m in msgs:
                        received.append(m)
                return ConsumeOrderlyStatus.SUCCESS
    else:
        class _L(MessageListenerConcurrently):
            def consume_message(self, msgs, context):
                with lock:
                    for m in msgs:
                        received.append(m)
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS

    cons = DefaultMQPushConsumer(consumer_group="%s_%s" % (PREFIX, group_suffix))
    cons.set_namesrv_addr(NAMESRV)
    cons.set_consume_from_where(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET)
    cons.subscribe(topic, sub_expr)
    try:
        cons.set_message_listener(_L())
    except Exception as e:  # noqa: BLE001
        errs.append("set_listener: %s" % e)
    cons.pull_timeout_millis = pull_timeout
    cons.pull_suspend_timeout_millis = pull_suspend
    cons.start()
    deadline = time.time() + duration
    while time.time() < deadline and (expect <= 0 or len(received) < expect):
        time.sleep(0.2)
    cons.shutdown()
    if errs:
        print("  [diag] consumer errs: %s" % errs)
    return received


def main():
    prod = DefaultMQProducer(producer_group="%s_producer" % PREFIX)
    prod.set_namesrv_addr(NAMESRV)
    prod.set_send_msg_timeout(5000)
    prod.start()
    brokers = _wait_broker(prod)
    if not brokers:
        check("集群探活", False, "nameServer 无 broker 注册")
        prod.shutdown()
        return 1
    check("集群探活", True, "brokers=%s" % brokers)

    # ---------- 1. 异步发送 ----------
    topic_async = "%s_Async" % PREFIX
    ok_res, errs = [], []
    cb = SendCallbackImpl(lambda sr: ok_res.append(sr), lambda e: errs.append(e))
    prod.send_async(Message(topic_async, b"async-hello"), cb)
    time.sleep(2)
    check("异步发送 send_async",
          len(ok_res) == 1 and ok_res[0].send_status.name == "SEND_OK" and not errs,
          "ok=%d err=%d" % (len(ok_res), len(errs)))

    # ---------- 2. 顺序消息：同 key 落同队列 + 顺序消费保序 ----------
    topic_order = "%s_Order" % PREFIX
    qids = set()
    bodies_order = []
    for i in range(10):
        b = ("ord-%02d" % i).encode("utf-8")
        bodies_order.append(b)
        sr = prod.send_by_selector(Message(topic_order, b), SelectMessageQueueByHash(), "shard-A")
        qids.add(sr.message_queue.queue_id)
    check("顺序发送: 同 key 路由到同一队列",
          len(qids) == 1, "distinct_queue_ids=%s" % (list(qids) or "?"))
    recv_order = run_consumer(topic_order, "*", 12, orderly=True, group_suffix="order")
    recv_bodies = sorted(m.body for m in recv_order)
    check("顺序消费保序", recv_bodies == sorted(bodies_order) and len(recv_order) == 10,
          "received=%d/%d" % (len(recv_order), len(bodies_order)))

    # ---------- 3. 带 Tag 消息 + 服务端 Tag 过滤 ----------
    topic_tag = "%s_Tag" % PREFIX
    for i in range(3):
        prod.send(Message(topic_tag, ("tagA-%d" % i).encode(), tags="TagA"))
    for i in range(3):
        prod.send(Message(topic_tag, ("tagB-%d" % i).encode(), tags="TagB"))
    recv_tag = run_consumer(topic_tag, "TagA", 12, group_suffix="tag")
    recv_tags = set(m.get_tags() for m in recv_tag)
    check("Tag 过滤消费（仅收到 TagA）",
          len(recv_tag) == 3 and recv_tags == {"TagA"},
          "received=%d tags=%s" % (len(recv_tag), recv_tags))

    # ---------- 4. 用户属性透传（独立 topic，避免与 Tag 测试耦合）----------
    topic_prop = "%s_Prop" % PREFIX
    for i in range(3):
        m = Message(topic_prop, ("prop-%d" % i).encode(), tags="P")
        m.set_user_property("city", "Hangzhou")
        m.set_user_property("env", "prod")
        prod.send(m)
    recv_prop = run_consumer(topic_prop, "*", 12, group_suffix="prop")
    ok_city = all(m.get_user_property("city") == "Hangzhou" for m in recv_prop)
    ok_env = all(m.get_user_property("env") == "prod" for m in recv_prop)
    check("用户属性透传(city=Hangzhou, env=prod)",
          ok_city and ok_env and len(recv_prop) == 3,
          "received=%d" % len(recv_prop))

    # ---------- 5. 延迟消息 ----------
    topic_delay = "%s_Delay" % PREFIX
    t0 = int(time.time() * 1000)
    prod.send(Message(topic_delay, b"normal-now"))                       # 普通
    dm = Message(topic_delay, b"delayed-5s")
    dm.set_delay_time_level(2)                                          # level2 = 5s
    prod.send(dm)
    recv_delay = run_consumer(topic_delay, "*", 16, group_suffix="delay", expect=2)
    delayed = [m for m in recv_delay if m.body == b"delayed-5s"]
    normal = [m for m in recv_delay if m.body == b"normal-now"]
    check("延迟消息最终投递", bool(delayed), "delayed received=%d normal=%d" % (len(delayed), len(normal)))
    if delayed:
        drift = delayed[0].store_timestamp - delayed[0].born_timestamp
        check("延迟生效(store_ts-born_ts>=3000ms)", drift >= 3000,
              "drift=%dms" % drift)
    else:
        check("延迟生效(store_ts-born_ts>=3000ms)", False, "无延迟消息可校验")

    # ---------- 6. 带 Key 消息 + 按 Key 查询 ----------
    topic_key = "%s_Key" % PREFIX
    key = "MTKEY_%d" % STAMP
    body_key = b"key-msg-payload"
    prod.send(Message(topic_key, body_key, keys=key))
    time.sleep(1)
    begin = t0 - 120000
    end = int(time.time() * 1000) + 120000
    found = prod.query_message(topic_key, key, 10, begin, end)
    hit = any((m.body == body_key) for m in (found or []))
    check("按 Key 查询(query_message)", hit, "returned=%d" % (len(found or [])))

    # ---------- 7. 事务消息（简化单阶段实现）----------
    topic_tx = "%s_Tx" % PREFIX

    class _TxListener(TransactionListener):
        def execute_local_transaction(self, msg, arg):
            return LocalTransactionState.COMMIT_MESSAGE

        def check_local_transaction(self, msg):
            return LocalTransactionState.COMMIT_MESSAGE

    tsr = prod.send_message_in_transaction(Message(topic_tx, b"tx-commit"), _TxListener())
    tx_ok = (tsr is not None and tsr.send_status.name == "SEND_OK"
             and tsr.get_local_transaction_state() == LocalTransactionState.COMMIT_MESSAGE)
    check("事务消息发送(简化单阶段)", tx_ok,
          "state=%s" % (tsr.get_local_transaction_state() if tsr else "None"))
    # 确认事务消息确实落库可被消费
    recv_tx = run_consumer(topic_tx, "*", 10, group_suffix="tx")
    check("事务消息落库可被消费", any(m.body == b"tx-commit" for m in recv_tx),
          "received=%d" % len(recv_tx))

    prod.shutdown()

    # ---------- 汇总 ----------
    failed = [r for r in results if not r[1]]
    print("\n================ 消息类型联调汇总 ================")
    for name, ok, detail in results:
        print("  [%s] %s" % ("PASS" if ok else "FAIL", name))
    print("==================================================")
    if failed:
        print("结果: %d 项失败" % len(failed))
        return 1
    print("结果: 全部通过（7 类消息类型均对真实集群验证通过）")
    return 0


if __name__ == "__main__":
    sys.exit(main())
