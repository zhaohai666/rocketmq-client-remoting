# -*- coding: utf-8 -*-
"""Read back, with the **already verified Python client**, the messages that Go autoBatch
(ProduceAccumulator) wrote into a real broker.

Why it is needed: after autoBatch accumulates a batch it is sent as a SEND_BATCH_MESSAGE
frame, and when the frame is built wrongly the broker still replies SEND_OK -- batch
splitting errors (mismatched member bodies, lost TAGS/KEYS, missing UNIQ_KEY) can only be
picked up by a consumer written in another language. This script validates them one by one:
the count, the body prefix of every message, TAGS, KEYS and UNIQ_KEY.

Usage: python go_autobatch_consume_check.py <topic> [expect] [group]

expect is taken by the caller from the `SENT=<n>` line on the Go side (see
scripts/run_go_autobatch_live.sh); do not hardcode it.
"""
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from client.consumer import DefaultMQPushConsumer, SimpleMessageListener
from client.consumer_result import ConsumeConcurrentlyStatus
from remoting.protocol.heartbeat import ConsumeFromWhere

NAMESRV = os.environ.get("ROCKETMQ_NAMESRV", "127.0.0.1:9876")

results = []


def check(name, ok, detail=""):
    results.append(ok)
    print(("PASS  " if ok else "FAIL  ") + name + ((" " + detail) if detail else ""))


def consume(topic, group, expect, timeout_sec=40):
    got = []

    def on_msg(msgs):
        got.extend(m for m in msgs if m.topic == topic)
        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS

    cons = DefaultMQPushConsumer(consumer_group=group)
    cons.set_namesrv_addr(NAMESRV)
    cons.set_consume_from_where(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET)
    cons.subscribe(topic, "*")
    cons.set_message_listener(SimpleMessageListener(on_msg))
    cons.start()
    deadline = time.time() + timeout_sec
    while time.time() < deadline and len(got) < expect:
        time.sleep(0.25)
    cons.shutdown()
    return got


def main():
    if len(sys.argv) < 2:
        print("usage: go_autobatch_consume_check.py <topic> [expect] [group]", file=sys.stderr)
        return 2
    topic = sys.argv[1]
    expect = int(sys.argv[2]) if len(sys.argv) > 2 else 12
    group = sys.argv[3] if len(sys.argv) > 3 else "GID_PyReadGoAutoBatch_%d" % int(time.time())

    msgs = consume(topic, group, expect)
    check("read back %d messages" % expect, len(msgs) >= expect, "got=%d" % len(msgs))

    # The member attributes are what batching most easily loses: TAGS/KEYS/UNIQ_KEY must survive every single message.
    by_body = {}
    for m in msgs:
        by_body[m.body] = m

    prefixes = (b"autobatch-sync-", b"autobatch-tail-", b"autobatch-async-")
    matched = sum(1 for body in by_body if any(body.startswith(p) for p in prefixes))
    check("every message has a body from the autoBatch sample", matched == min(expect, len(by_body)),
          "matched=%d" % matched)

    sync = by_body.get(b"autobatch-sync-00-0123456789abcdef")
    if sync is not None:
        check("batched member TAGS=autoBatch", sync.get_property("TAGS") == "autoBatch",
              "tags=%r" % sync.get_property("TAGS"))
        check("batched member KEYS=auto-batch-key-0", sync.get_property("KEYS") == "auto-batch-key-0",
              "keys=%r" % sync.get_property("KEYS"))
        check("batched member carries UNIQ_KEY", bool(sync.get_property("UNIQ_KEY")),
              "uniq=%r" % sync.get_property("UNIQ_KEY"))
        check("batched member msg_id is not empty", bool(sync.msg_id), "msg_id=%r" % sync.msg_id)
    else:
        check("received the batched member autobatch-sync-00", False)

    # The UNIQ_KEY of the 12 messages must be pairwise different (batching never shares member ids)
    uniqs = [m.get_property("UNIQ_KEY") for m in msgs]
    check("member UNIQ_KEY values are all different", len(set(uniqs)) == len(uniqs),
          "distinct=%d total=%d" % (len(set(uniqs)), len(uniqs)))

    print("PASS=%d FAIL=%d" % (sum(1 for r in results if r), sum(1 for r in results if not r)))
    return 0 if all(results) else 1


if __name__ == "__main__":
    sys.exit(main())
