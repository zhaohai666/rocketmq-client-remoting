#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import annotations

import sys
import time

sys.path.insert(0, ".")

from client.consumer import DefaultLitePullConsumer
from client.mq_client import MQClientInstance
from client.producer import DefaultMQProducer
from common.message import Message
from remoting.protocol.heartbeat import ConsumeFromWhere

NAMESRV = sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1:9876"
STAMP = int(time.time() * 1000)
TOPIC = "LiteCursorLive_%d" % STAMP
GROUP = "GID_LiteCursorLive_%d" % STAMP
QUEUES = 4
NEVER_MATCH = "TagLiteCursorNeverMatch"
BIG_AHEAD = 1000

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


def wait_until(pred, timeout: float, interval: float = 0.2) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if pred():
            return True
        time.sleep(interval)
    return pred()


def queues_of(client: MQClientInstance, topic: str) -> list:
    publish = client.get_topic_publish_info(topic)
    if publish is None or not publish.msg_queue_list:
        return []
    return list(publish.msg_queue_list)


def cursor_row(c: DefaultLitePullConsumer, mqs: list) -> str:
    return " ".join("q%d:%s" % (mq.queue_id, c.pull_cursor_of(mq)) for mq in mqs)


def drain(c: DefaultLitePullConsumer, expect: int, timeout: float = 30.0) -> list:
    """Poll repeatedly until expect messages are collected (or timeout)."""
    out: list = []
    deadline = time.time() + timeout
    while len(out) < expect and time.time() < deadline:
        out.extend(c.poll(500))
    return out


def poll_quiet(c: DefaultLitePullConsumer, seconds: float = 1.0) -> list:
    """Watch the buffer over the given window: return all messages polled during it (used to assert "should be none")."""
    out: list = []
    deadline = time.time() + seconds
    while time.time() < deadline:
        out.extend(c.poll(200))
    return out


def main() -> int:
    setup = MQClientInstance("lite-cursor-setup-%d" % STAMP, [NAMESRV])
    setup.start()
    setup.create_topic_in_route(TOPIC, QUEUES, QUEUES)
    print("topic=%s queues=%d group=%s" % (TOPIC, QUEUES, GROUP))

    producer = DefaultMQProducer("LiteCursorLive_pg_%d" % STAMP)
    producer.set_namesrv_addr(NAMESRV)
    producer.start()
    time.sleep(1)

    mqs = queues_of(setup, TOPIC)
    check("route visible: %d queues" % QUEUES, len(mqs) == QUEUES, "got=%d" % len(mqs))
    if len(mqs) != QUEUES:
        return 1

    # ---------- S1 control group: * subscription + seek(0), receive the one message on each queue ----------
    for i, mq in enumerate(mqs):
        msg = Message(TOPIC, b"lc-s1-%d" % i)
        msg.set_tags("TagA")
        producer.send(msg, mq=mq)

    c = DefaultLitePullConsumer(GROUP)
    c.set_namesrv_addr(NAMESRV)
    c.set_consume_from_where(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET)
    # The out-of-range offset leg must never commit the out-of-range value: turn off auto-commit and only watch the pull/delivery cursors.
    c.set_auto_commit(False)
    c.set_pull_interval_millis(200)
    c.assign(mqs)
    c.start()
    time.sleep(1)

    for mq in mqs:
        c.seek(mq, 0)

    got1 = drain(c, QUEUES)
    check("S1-control: all 4 messages pinned to queues received under * subscription", len(got1) == QUEUES,
          "got=%d bodies=%s" % (len(got1), sorted(bytes(m.body) for m in got1)[:6]))

    def _s1_max_is_one():
        return all(setup.get_max_offset(mq) == 1 for mq in mqs)

    check("S1-per-queue maxOffset == 1 (the yardstick for the next two legs)",
          wait_until(_s1_max_is_one, 10),
          " ".join("q%d:%d" % (mq.queue_id, setup.get_max_offset(mq)) for mq in mqs))

    # ---------- S2 NO_MATCHED_MSG: the whole span is filtered by the expression, the cursor must span it ----------
    c.set_sub_expression_for_assign(TOPIC, NEVER_MATCH)
    for mq in mqs:
        c.seek(mq, 0)

    ok2 = wait_until(lambda: all(c.pull_cursor_of(mq) == 1 for mq in mqs), 20)
    check("S2-pull cursor spans the whole non-matching span after NO_MATCHED_MSG (== maxOffset=1)", ok2,
          cursor_row(c, mqs))
    check("S2-zero delivery during the empty replies", not poll_quiet(c, 1.5), "no messages should be delivered")

    # ---------- S3 OFFSET_ILLEGAL: the out-of-range offset is corrected by the broker + the messages really come back ----------
    for mq in mqs:
        c.seek(mq, setup.get_max_offset(mq) + BIG_AHEAD)

    ok3 = wait_until(lambda: all(c.pull_cursor_of(mq) == 1 for mq in mqs), 20)
    check("S3-after the broker corrects the out-of-range offset, the cursor returns to maxOffset (self-heal)", ok3,
          cursor_row(c, mqs))

    # The never-match expression from S2 must be swapped back, otherwise the 4 TagA below will be filtered out by the broker as-is.
    c.set_sub_expression_for_assign(TOPIC, "*")

    # The start point must also be accepted by the broker via a path other than heartbeat/commit: here we only send messages, never touch the offset.
    for i, mq in enumerate(mqs):
        msg = Message(TOPIC, b"lc-s3-%d" % i)
        msg.set_tags("TagA")
        producer.send(msg, mq=mq)

    got3 = drain(c, QUEUES, timeout=40.0)
    check("S3-after self-heal, all new messages are delivered (old impl: cursor stuck at +1000, nothing visible)",
          len(got3) == QUEUES,
          "got=%d bodies=%s" % (len(got3), sorted(bytes(m.body) for m in got3)[:6]))
    check("S3-received exactly the 4 pinned in after the out-of-range point",
          sorted(bytes(m.body) for m in got3)[:QUEUES]
          == sorted(b"lc-s3-%d" % i for i in range(QUEUES)),
          "bodies=%s" % sorted(bytes(m.body) for m in got3)[:6])

    c.shutdown()
    producer.shutdown()
    setup.shutdown()

    print("\nlite pull cursor live: %d passed, %d failed" % (PASS, FAIL))
    return 0 if FAIL == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
