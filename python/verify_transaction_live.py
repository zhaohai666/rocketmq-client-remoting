#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import annotations

import sys
import threading
import time

sys.path.insert(0, ".")

from client.admin import DefaultMQAdminExt
from client.consumer import (DefaultMQPushConsumer,
                                      SimpleMessageListener)
from client.producer import (DefaultMQProducer, LocalTransactionState,
                                      TransactionListener)
from common.message import Message
from common.mix_all import MixAll
from remoting.protocol.heartbeat import ConsumeFromWhere

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
    """Start first and only then send the messages: a consumer that comes up after the delivery started (the default consumes from the latest offset) would miss the messages entirely."""

    def __init__(self, topic: str, group: str):
        self.msgs = []
        self._consumer = DefaultMQPushConsumer(group)
        self._consumer.set_namesrv_addr(NAMESRV)
        # A brand-new consumer group plus the default LAST_OFFSET: the broker seeds the offset
        # straight to the tail, so the few messages sent before the consumer got its queue could
        # never be read. It has to explicitly start from 0 here (consistent with
        # verify_message_types).
        self._consumer.set_consume_from_where(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET)
        self._consumer.subscribe(topic, "*")

        def listener(msgs, context):
            self.msgs.extend(msgs)
            from client.consumer import ConsumeConcurrentlyStatus
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS

        # It has to be a SimpleMessageListener; the callback parameter is a **list of messages** (not a single one)
        self._consumer.set_message_listener(SimpleMessageListener(self._on_msg))

    def _on_msg(self, msgs):
        if isinstance(msgs, list):
            self.msgs.extend(msgs)
        else:
            self.msgs.append(msgs)

    def start(self):
        self._consumer.start()
        # Wait for the consumer to finish resolving its route/queues, to avoid the blank window right after startup
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
    """The local transaction returns UNKNOW; COMMIT is only decided when the broker checks back."""

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

    # The topic has to be created first, and only then the consumer started. If the topic were
    # auto-created by the broker, it would only enter the NameServer route on the next broker
    # registration (registerNameServerPeriod=30s); and the consumer heartbeat only goes to the
    # brokers already present in the route (get_route_of_all_brokers reads the cached route).
    # Combined, "the consumer starts before the topic exists" means waiting over 30s before it
    # gets any queue assigned, far beyond the 8~25s observation window of this script.
    # Creating the topic via admin makes the broker re-register immediately, so the NameServer
    # has the route on the spot.
    admin = DefaultMQAdminExt()
    admin.set_namesrv_addr(NAMESRV)
    admin.start()
    try:
        for t in (topic_plain, topic, topic_rb, topic_ck):
            admin.create_topic(MixAll.DEFAULT_TOPIC, t, 4)
        time.sleep(1)
    finally:
        admin.shutdown()

    # ---------- 0. control group: an ordinary (non-transactional) message ----------
    # First prove that the consumer path itself works, otherwise "the transactional message was
    # not consumed" might just be a problem with this script or with how the consumer is used
    plain = ConsumeCollector(topic_plain, PREFIX + "_cg_plain")
    plain.start()
    producer.send(Message(topic_plain, b"plain-msg"))
    plain.wait_and_stop(8)
    check("control group - the ordinary message can be consumed",
          any(m.body == b"plain-msg" for m in plain.msgs),
          "received=%d" % len(plain.msgs))

    # ---------- 1. COMMIT ----------
    listener = CommitListener()
    run = ConsumeCollector(topic, PREFIX + "_cg")
    run.start()
    try:
        tsr = producer.send_message_in_transaction(
            Message(topic, b"tx-commit"), listener)
        check("transaction - COMMIT send status",
              tsr.send_status.name == "SEND_OK"
              and tsr.get_local_transaction_state() == LocalTransactionState.COMMIT_MESSAGE,
              "state=%s" % tsr.get_local_transaction_state())
    except Exception as e:  # noqa: BLE001
        check("transaction - COMMIT send status", False, "throw=%s" % e)

    run.wait_and_stop(10)
    check("transaction - the COMMIT message is persisted and can be consumed",
          any(m.body == b"tx-commit" for m in run.msgs),
          "received=%d" % len(run.msgs))

    # ---------- 2. ROLLBACK ----------
    rb_listener = RollbackListener()
    run2 = ConsumeCollector(topic_rb, PREFIX + "_cg_rb")
    run2.start()
    try:
        tsr = producer.send_message_in_transaction(
            Message(topic_rb, b"tx-rollback"), rb_listener)
        check("transaction - ROLLBACK send status",
              tsr.get_local_transaction_state() == LocalTransactionState.ROLLBACK_MESSAGE,
              "state=%s" % tsr.get_local_transaction_state())
    except Exception as e:  # noqa: BLE001
        check("transaction - ROLLBACK send status", False, "throw=%s" % e)

    run2.wait_and_stop(10)
    check("transaction - ROLLBACK is not delivered",
          not any(m.body == b"tx-rollback" for m in run2.msgs),
          "received=%d" % len(run2.msgs))

    # ---------- 3. UNKNOW + broker check-back ----------
    ck_listener = UnknownThenCommitListener()
    run3 = ConsumeCollector(topic_ck, PREFIX + "_cg_ck")
    run3.start()
    try:
        tsr = producer.send_message_in_transaction(
            Message(topic_ck, b"tx-check"), ck_listener)
        check("transaction - UNKNOW send status",
              tsr.get_local_transaction_state() == LocalTransactionState.UNKNOW,
              "state=%s" % tsr.get_local_transaction_state())
    except Exception as e:  # noqa: BLE001
        check("transaction - UNKNOW send status", False, "throw=%s" % e)

    deadline = time.time() + 90
    while ck_listener.check_calls == 0 and time.time() < deadline:
        time.sleep(1)
    run3.wait_and_stop(10)
    check("transaction - UNKNOW triggered a broker check-back", ck_listener.check_calls > 0,
          "check_local_transaction_calls=%d" % ck_listener.check_calls)
    check("transaction - the UNKNOW message is finally delivered after the check-back",
          any(m.body == b"tx-check" for m in run3.msgs),
          "received=%d" % len(run3.msgs))

    producer.shutdown()
    print("\nPASS=%d FAIL=%d" % (PASS, FAIL))
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(main())
