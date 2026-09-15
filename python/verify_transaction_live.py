#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""事务消息真机验证（对齐 Java 两阶段）。

用法（需本地 RocketMQ 5.5.1 集群，broker 需配：
      transactionCheckInterval=3000 / transactionTimeOut=3000 / transactionCheckMax=5）：
    .venv/bin/python verify_transaction_live.py 127.0.0.1:9876
"""
from __future__ import annotations

import sys
import threading
import time

sys.path.insert(0, ".")

from rocketmq.client.consumer import (DefaultMQPushConsumer,
                                      SimpleMessageListener)
from rocketmq.client.producer import (DefaultMQProducer, LocalTransactionState,
                                      TransactionListener)
from rocketmq.common.message import Message

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

    # ---------- 0. 对照组：普通（非事务）消息 ----------
    # 先证明消费者链路本身是通的，否则"事务消息没被消费"可能只是脚本/消费者用法问题
    topic_plain = PREFIX + "_Plain"
    plain = ConsumeCollector(topic_plain, PREFIX + "_cg_plain")
    plain.start()
    producer.send(Message(topic_plain, b"plain-msg"))
    plain.wait_and_stop(8)
    check("对照组-普通消息可被消费",
          any(m.body == b"plain-msg" for m in plain.msgs),
          "received=%d" % len(plain.msgs))

    # ---------- 1. COMMIT ----------
    topic = PREFIX + "_Tx"
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
    topic_rb = PREFIX + "_TxRollback"
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
    topic_ck = PREFIX + "_TxCheck"
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

    # 回查默认 60s 一轮；联调 broker 已配 3s，这里给足窗口
    run3.wait_and_stop(25)
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
