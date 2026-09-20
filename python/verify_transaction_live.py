#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""事务消息真机验证（对齐 Java 两阶段）。

用法（需本地 RocketMQ 5.5.1 集群；broker 不要求改配置，走 Java 默认值
      transactionTimeOut=6s / transactionCheckInterval=30s / transactionCheckMax=15，
      想要更快的回查可在 broker.conf 里把前两项调成 3000）：
    .venv/bin/python verify_transaction_live.py 127.0.0.1:9876
"""
from __future__ import annotations

import sys
import threading
import time

sys.path.insert(0, ".")

from rocketmq.client.admin import DefaultMQAdminExt
from rocketmq.client.consumer import (DefaultMQPushConsumer,
                                      SimpleMessageListener)
from rocketmq.client.producer import (DefaultMQProducer, LocalTransactionState,
                                      TransactionListener)
from rocketmq.common.message import Message
from rocketmq.common.mix_all import MixAll
from rocketmq.remoting.protocol.heartbeat import ConsumeFromWhere

NAMESRV = sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1:9876"
PREFIX = "TxPy_%d" % int(time.time() * 1000)

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


class ConsumeCollector:
    """先 start 再发消息：消费者若晚于投递启动（默认从最新位点消费）会整个错过消息。"""

    def __init__(self, topic: str, group: str):
        self.msgs = []
        self._consumer = DefaultMQPushConsumer(group)
        self._consumer.set_namesrv_addr(NAMESRV)
        # 全新消费组 + 默认 LAST_OFFSET：broker 会把位点直接种到队尾，消费者拿到队列
        # 之前发的那几条就永远读不到了。这里必须显式从 0 开始（与 verify_message_types 一致）。
        self._consumer.set_consume_from_where(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET)
        self._consumer.subscribe(topic, "*")

        def listener(msgs, context):
            self.msgs.extend(msgs)
            from rocketmq.client.consumer import ConsumeConcurrentlyStatus
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS

        # 必须是 SimpleMessageListener；回调参数是**消息列表**（不是单条）
        self._consumer.set_message_listener(SimpleMessageListener(self._on_msg))

    def _on_msg(self, msgs):
        if isinstance(msgs, list):
            self.msgs.extend(msgs)
        else:
            self.msgs.append(msgs)

    def start(self):
        self._consumer.start()
        # 等消费者完成路由/队列就绪，避免刚启动时的空窗
        time.sleep(2)

    def wait_and_stop(self, seconds: int):
        time.sleep(seconds)
        self._consumer.shutdown()


class CommitListener(TransactionListener):
    def execute_local_transaction(self, msg, arg):
        return LocalTransactionState.COMMIT_MESSAGE

    def check_local_transaction(self, msg):
        return LocalTransactionState.COMMIT_MESSAGE


class RollbackListener(TransactionListener):
    def execute_local_transaction(self, msg, arg):
        return LocalTransactionState.ROLLBACK_MESSAGE

    def check_local_transaction(self, msg):
        return LocalTransactionState.ROLLBACK_MESSAGE


class UnknownThenCommitListener(TransactionListener):
    """本地事务返回 UNKNOW，等 broker 回查时才判 COMMIT。"""

    def __init__(self):
        self.check_calls = 0
        self._lock = threading.Lock()

    def execute_local_transaction(self, msg, arg):
        return LocalTransactionState.UNKNOW

    def check_local_transaction(self, msg):
        with self._lock:
            self.check_calls += 1
        return LocalTransactionState.COMMIT_MESSAGE


def main() -> int:
    producer = DefaultMQProducer(PREFIX + "_pg")
    producer.set_namesrv_addr(NAMESRV)
    producer.start()

    topic_plain = PREFIX + "_Plain"
    topic = PREFIX + "_Tx"
    topic_rb = PREFIX + "_TxRollback"
    topic_ck = PREFIX + "_TxCheck"

    # topic 必须先建好，再启动消费者。靠 broker 自动建 topic 的话，新 topic 要到下一次
    # broker 注册（registerNameServerPeriod=30s）才进 NameServer 路由；而消费者心跳又只发往
    # 路由里已有的 broker（get_route_of_all_brokers 读的是缓存路由）。两者叠加，"消费者先于
    # topic 启动"要白等 30s 以上才分得到队列，远超本脚本 8~25s 的观测窗。
    # admin 建 topic 会让 broker 立刻重新注册，NameServer 当场就有路由。
    admin = DefaultMQAdminExt()
    admin.set_namesrv_addr(NAMESRV)
    admin.start()
    try:
        for t in (topic_plain, topic, topic_rb, topic_ck):
            admin.create_topic(MixAll.DEFAULT_TOPIC, t, 4)
        time.sleep(1)
    finally:
        admin.shutdown()

    # ---------- 0. 对照组：普通（非事务）消息 ----------
    # 先证明消费者链路本身是通的，否则"事务消息没被消费"可能只是脚本/消费者用法问题
    plain = ConsumeCollector(topic_plain, PREFIX + "_cg_plain")
    plain.start()
    producer.send(Message(topic_plain, b"plain-msg"))
    plain.wait_and_stop(8)
    check("对照组-普通消息可被消费",
          any(m.body == b"plain-msg" for m in plain.msgs),
          "received=%d" % len(plain.msgs))

    # ---------- 1. COMMIT ----------
    listener = CommitListener()
    run = ConsumeCollector(topic, PREFIX + "_cg")
    run.start()
    try:
        tsr = producer.send_message_in_transaction(
            Message(topic, b"tx-commit"), listener)
        check("事务-COMMIT 发送状态",
              tsr.send_status.name == "SEND_OK"
              and tsr.get_local_transaction_state() == LocalTransactionState.COMMIT_MESSAGE,
              "state=%s" % tsr.get_local_transaction_state())
    except Exception as e:  # noqa: BLE001
        check("事务-COMMIT 发送状态", False, "throw=%s" % e)

    run.wait_and_stop(10)
    check("事务-COMMIT 落库可被消费",
          any(m.body == b"tx-commit" for m in run.msgs),
          "received=%d" % len(run.msgs))

    # ---------- 2. ROLLBACK ----------
    rb_listener = RollbackListener()
    run2 = ConsumeCollector(topic_rb, PREFIX + "_cg_rb")
    run2.start()
    try:
        tsr = producer.send_message_in_transaction(
            Message(topic_rb, b"tx-rollback"), rb_listener)
        check("事务-ROLLBACK 发送状态",
              tsr.get_local_transaction_state() == LocalTransactionState.ROLLBACK_MESSAGE,
              "state=%s" % tsr.get_local_transaction_state())
    except Exception as e:  # noqa: BLE001
        check("事务-ROLLBACK 发送状态", False, "throw=%s" % e)

    run2.wait_and_stop(10)
    check("事务-ROLLBACK 不被投递",
          not any(m.body == b"tx-rollback" for m in run2.msgs),
          "received=%d" % len(run2.msgs))

    # ---------- 3. UNKNOW + broker 回查 ----------
    ck_listener = UnknownThenCommitListener()
    run3 = ConsumeCollector(topic_ck, PREFIX + "_cg_ck")
    run3.start()
    try:
        tsr = producer.send_message_in_transaction(
            Message(topic_ck, b"tx-check"), ck_listener)
        check("事务-UNKNOW 发送状态",
              tsr.get_local_transaction_state() == LocalTransactionState.UNKNOW,
              "state=%s" % tsr.get_local_transaction_state())
    except Exception as e:  # noqa: BLE001
        check("事务-UNKNOW 发送状态", False, "throw=%s" % e)

    # broker 巡检半消息的周期是 Java BrokerConfig#transactionCheckInterval，**默认 30s**
    # （不是本文件头注释里说的 3s，除非 broker.conf 显式覆写），所以半消息最快也要等一轮
    # 巡检才被回查。旧代码固定 sleep(25) 会稳定地早于回查 → "最终投递"必然失败。
    # 这里改成等"回查发生"这一事件（最多 90s，兼容 3s/30s 两种配置），
    # 再额外观察 10s 让 COMMIT 后的消息真正投递到消费者。
    deadline = time.time() + 90
    while ck_listener.check_calls == 0 and time.time() < deadline:
        time.sleep(1)
    run3.wait_and_stop(10)
    check("事务-UNKNOW 触发 broker 回查", ck_listener.check_calls > 0,
          "check_local_transaction_calls=%d" % ck_listener.check_calls)
    check("事务-UNKNOW 回查后最终投递",
          any(m.body == b"tx-check" for m in run3.msgs),
          "received=%d" % len(run3.msgs))

    producer.shutdown()
    print("\nPASS=%d FAIL=%d" % (PASS, FAIL))
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(main())
