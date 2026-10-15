#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import annotations

import sys
import threading
import time

sys.path.insert(0, ".")

from client.consumer import (ConsumeConcurrentlyStatus,
                                      DefaultMQPushConsumer,
                                      MessageListenerConcurrently)
from client.mq_client import MQClientInstance
from client.producer import DefaultMQProducer
from common.message import Message
from remoting.protocol.heartbeat import ConsumeFromWhere

NAMESRV = sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1:9876"
STAMP = int(time.time() * 1000)
TOPIC = "CorrectTagsLive_%d" % STAMP
GROUP_CTRL = "GID_CorrectTagsCtrl_%d" % STAMP
GROUP_TEST = "GID_CorrectTagsTest_%d" % STAMP
QUEUES = 4
MSGS = 5

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


def new_client(tag: str) -> MQClientInstance:
    c = MQClientInstance("%s-%d" % (tag, STAMP), [NAMESRV])
    c.start()
    return c


def wait_until(pred, timeout: float, interval: float = 1.0) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if pred():
            return True
        time.sleep(interval)
    return pred()


def queue_snapshot(client: MQClientInstance, topic: str):
    """[(MessageQueue, maxOffset, committedOffset or None)]; returns None when the route is not ready."""
    publish = client.get_topic_publish_info(topic)
    if publish is None or not publish.msg_queue_list:
        return None
    out = []
    for mq in publish.msg_queue_list:
        max_off = client.get_max_offset(mq)
        out.append((mq, max_off, None))
    return out


def committed(client: MQClientInstance, group: str, topic: str):
    """[(mq, maxOffset, committed or None)], committed=None means no record on the broker."""
    publish = client.get_topic_publish_info(topic)
    if publish is None or not publish.msg_queue_list:
        return None
    out = []
    for mq in publish.msg_queue_list:
        try:
            off = client.query_consumer_offset(group, mq, set_zero_if_not_found=False)
        except Exception:  # noqa: BLE001
            off = None
        try:
            max_off = client.get_max_offset(mq)
        except Exception:  # noqa: BLE001
            max_off = -1
        out.append((mq, max_off, off))
    return out


class RecordingListener(MessageListenerConcurrently):
    def __init__(self):
        self.batches = []
        self._lock = threading.Lock()

    def consume_message(self, msgs, context):
        with self._lock:
            self.batches.append([(bytes(m.body), m.get_tags()) for m in msgs])
        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS

    def arrivals(self):
        with self._lock:
            return [r for batch in self.batches for r in batch]


def start(group: str, topic: str, listener: RecordingListener,
          expression: str = "*", from_first: bool = False) -> DefaultMQPushConsumer:
    c = DefaultMQPushConsumer(group)
    c.set_namesrv_addr(NAMESRV)
    c.set_message_listener(listener)
    c.set_consume_thread_nums(1)
    c.consume_message_batch_max_size = 3
    if from_first:
        c.set_consume_from_where(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET)
    c.subscribe(topic, expression)
    c.start()
    return c


def main() -> int:
    setup = new_client("setup")
    setup.create_topic_in_route(TOPIC, QUEUES, QUEUES)
    print("topic=%s queues=%d" % (TOPIC, QUEUES))

    producer = DefaultMQProducer("CorrectTagsLive_pg_%d" % STAMP)
    producer.set_namesrv_addr(NAMESRV)
    producer.start()
    time.sleep(1)

    # ---------- S1 control group: the messages really exist, and normal consumption lands at maxOffset ----------
    ctrl_listener = RecordingListener()
    ctrl = start(GROUP_CTRL, TOPIC, ctrl_listener, expression="TagA", from_first=True)
    time.sleep(2)
    for i in range(MSGS):
        msg = Message(TOPIC, b"cto-%d" % i)
        msg.set_tags("TagA")
        producer.send(msg)
    print("S1: sent %d TagA, waiting for control group TagA consumption..." % MSGS)
    ok_ctrl = wait_until(lambda: len(ctrl_listener.arrivals()) >= MSGS, 30)
    check("S1-control (TagA) received all %d -- the messages really are in the queue" % MSGS,
          ok_ctrl and len(ctrl_listener.arrivals()) == MSGS,
          "arrivals=%d" % len(ctrl_listener.arrivals()))

    def ctrl_committed_matches():
        rows = committed(setup, GROUP_CTRL, TOPIC)
        return bool(rows) and all(o is not None and o == m for _, m, o in rows)

    ok = wait_until(ctrl_committed_matches, 25)
    rows = committed(setup, GROUP_CTRL, TOPIC) or []
    check("S1-control committed offset == per-queue maxOffset (numeric check)", ok,
          " ".join("q%d:%s/%d" % (mq.queue_id, o, m) for mq, m, o in rows))
    check("S1-control really pushed messages into the queue (maxOffset sum > 0)",
          sum(m for _, m, _ in rows) > 0,
          "maxSum=%d" % sum(m for _, m, _ in rows))
    ctrl.shutdown()

    # ---------- S2 NO_MATCHED_MSG: never-matching subscription, zero delivery but the offset must advance ----------
    test_listener = RecordingListener()
    test = start(GROUP_TEST, TOPIC, test_listener, expression="TagB")
    print("S2: consumer (TagB, never matches) started, waiting for the empty-reply offset correction (first hop 10s + cycle 5s)...")

    def test_committed_matches():
        rows = committed(setup, GROUP_TEST, TOPIC)
        return bool(rows) and all(o is not None and o == m for _, m, o in rows)

    ok = wait_until(test_committed_matches, 40)
    rows = committed(setup, GROUP_TEST, TOPIC) or []
    check("S2-zero delivery (listener received none)", len(test_listener.arrivals()) == 0,
          "arrivals=%d" % len(test_listener.arrivals()))
    check("S2-every queue's committed offset == that queue's maxOffset (empty-reply correction works)", ok,
          " ".join("q%d:%s/%d" % (mq.queue_id, o, m) for mq, m, o in rows))
    check("S2-corrected offset sum == control group (same queue's max offset)",
          sum(o for _, _, o in rows) == sum(m for _, m, _ in rows),
          "test=%d ctrl=%d" % (sum(o for _, _, o in rows), sum(m for _, m, _ in rows)))

    # ---------- S3 NO_NEW_MSG: the %RETRY%<group> empty queue must still leave an offset record ----------
    retry_topic = "%RETRY%" + GROUP_TEST
    print("S3: waiting for the offset record on %s (empty queue NO_NEW_MSG)..." % retry_topic)

    def retry_committed_present():
        rows_ = committed(setup, GROUP_TEST, retry_topic)
        return bool(rows_) and all(o is not None and o == m for _, m, o in rows_)

    ok = wait_until(retry_committed_present, 40)
    rows_retry = committed(setup, GROUP_TEST, retry_topic) or []
    check("S3-an offset record appears on %s and equals maxOffset" % retry_topic, ok,
          " ".join("q%d:%s/%d" % (mq.queue_id, o, m) for mq, m, o in rows_retry))
    check("S3-that offset is really 0 (the empty queue's nextBeginOffset)",
          bool(rows_retry) and all(o == 0 for _, _, o in rows_retry),
          "offsets=%s" % [o for _, _, o in rows_retry])

    # Wait one more quiet window: the correction only advances the offset and must not deliver anything.
    time.sleep(6)
    check("S3-listener still received 0 all along (the correction never fabricates delivery)",
          len(test_listener.arrivals()) == 0,
          "arrivals=%d" % len(test_listener.arrivals()))

    test.shutdown()
    producer.shutdown()
    setup.shutdown()

    print("\ncorrectTagsOffset live: %d passed, %d failed" % (PASS, FAIL))
    return 0 if FAIL == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
