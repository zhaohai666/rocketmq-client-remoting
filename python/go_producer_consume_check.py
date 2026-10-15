# -*- coding: utf-8 -*-
"""Read back, with the **already verified Python client**, the messages that the Go producer
wrote into a real broker.

Why it is needed: the Go side's live_producer can only prove that "the broker accepted it"
(SEND_OK); it cannot prove that the message encoding is correct -- the V2 short-key header
(a..n), the 6-segment lightweight batch frame, the zlib compression bit, the transactional
half-message attributes, the async chain (DEFAULT ASYNC has its own separate build path):
if any one of them is written wrongly the broker may still reply SUCCESS while the consumer
ends up with broken data. Consuming it with an implementation written in another language is
what makes it a real cross-language cross-check.

Usage: python go_producer_consume_check.py <topic> [expect_count] [group]

expect_count is taken by the caller from the `SENT=<n>` line on the Go side (see
scripts/run_go_producer_live.sh); do not hardcode it -- adding one more check to
live_producer would silently invalidate a hardcoded expectation.
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
        print("usage: go_producer_consume_check.py <topic> [expect] [group]", file=sys.stderr)
        return 2
    topic = sys.argv[1]
    expect = int(sys.argv[2]) if len(sys.argv) > 2 else 8
    group = sys.argv[3] if len(sys.argv) > 3 else "GID_PyReadGo_%d" % int(time.time())

    msgs = consume(topic, group, expect)
    check("read back %d messages" % expect, len(msgs) >= expect, "got=%d" % len(msgs))

    by_body = {}
    for m in msgs:
        by_body[m.body] = m

    expected_bodies = [
        b"go-live-sync",
        b"go-live-oneway",
        b"go-live-tx-commit",
        b"go-live-tx-unknown",
        b"go-live-batch-1",
        b"go-live-batch-2",
        b"go-live-batch-3",
    ]
    for body in expected_bodies:
        check("received %r" % body.decode(), body in by_body)

    # The tags / keys of a synchronous message must survive untouched (the property segment is encoded correctly)
    sync = by_body.get(b"go-live-sync")
    if sync is not None:
        check("synchronous message TAGS=goLive", sync.get_property("TAGS") == "goLive",
              "tags=%r" % sync.get_property("TAGS"))
        check("synchronous message KEYS=go-live-key-1", sync.get_property("KEYS") == "go-live-key-1",
              "keys=%r" % sync.get_property("KEYS"))
        # Every message must carry a client-side UNIQ_KEY (written by setUniqID on the Go
        # side); it is what links the msgId and the trace together.
        check("synchronous message carries UNIQ_KEY", bool(sync.get_property("UNIQ_KEY")),
              "uniq=%r" % sync.get_property("UNIQ_KEY"))
        check("synchronous message msg_id is not empty", bool(sync.msg_id), "msg_id=%r" % sync.msg_id)
        # The PGROUP attribute of a transactional message must be kept (the broker check-back uses it to locate the producer)
    tx = by_body.get(b"go-live-tx-commit")
    if tx is not None:
        check("transactional message carries PGROUP", bool(tx.get_property("PGROUP")),
              "pgroup=%r" % tx.get_property("PGROUP"))
        check("transactional message keeps TRAN_MSG=true", tx.get_property("TRAN_MSG") == "true",
              "TRAN_MSG=%r" % tx.get_property("TRAN_MSG"))
        check("transactional message carries __transactionId__", bool(tx.get_property("__transactionId__")),
              "tid=%r" % tx.get_property("__transactionId__"))

    # Compressed message: after the consumer decompresses it the length must equal the
    # original (12 bytes x 500 = 6000 on the Go side), and the COMPRESSED bit must be cleared
    big = [m for m in msgs if m.body and m.body.startswith(b"compress-me-")]
    if big:
        m = big[0]
        check("decompressed length of the compressed message = 6000", len(m.body) == 6000, "len=%d" % len(m.body))
        check("the compressed message content is intact", m.body == (b"compress-me-" * 500),
              "head=%r" % m.body[:24])
        check("the COMPRESSED bit of the compressed message is cleared", not (m.sys_flag & 0x1),
              "sys_flag=%#x" % m.sys_flag)
    else:
        check("received the large compressed message", False, "(no message starting with compress-me- was found)")

    # ---- the async chain (Go DEFAULT ASYNC) -------------------------------------------
    # An asynchronous send goes through a different build path (build once + retry from the
    # callback), so the property segment / uniqID / compression bit may all differ from the
    # synchronous path; that is why they are verified separately here instead of taking
    # "it replied SEND_OK, so it must be fine" as good enough.
    async_single = by_body.get(b"go-live-async")
    if async_single is None:
        check("received the asynchronous message go-live-async", False)
    else:
        check("received the asynchronous message go-live-async", True)
        check("asynchronous message TAGS=goAsync", async_single.get_property("TAGS") == "goAsync",
              "tags=%r" % async_single.get_property("TAGS"))
        check("asynchronous message KEYS=go-live-async-key",
              async_single.get_property("KEYS") == "go-live-async-key",
              "keys=%r" % async_single.get_property("KEYS"))
        check("asynchronous message carries UNIQ_KEY", bool(async_single.get_property("UNIQ_KEY")),
              "uniq=%r" % async_single.get_property("UNIQ_KEY"))

    burst = sorted(b for b in by_body if b.startswith(b"go-live-async-burst-"))
    check("all 16 asynchronous concurrent messages are present", len(burst) == 16, "got=%d" % len(burst))
    bp = sorted(b for b in by_body if b.startswith(b"go-live-async-bp-"))
    check("all 16 messages are present under back pressure", len(bp) == 16, "got=%d" % len(bp))

    for i in (1, 2, 3):
        body = b"go-live-async-batch-%d" % i
        check("received asynchronous batch %d" % i, body in by_body)

    # Pinned queue / selector: the Go side checks the **queueId in the response header**, while
    # here the **queue the consumer really landed in** is checked -- only when two independent
    # pieces of evidence point at the same queue did it really land in the requested queue.
    pinned = by_body.get(b"go-live-async-pinned")
    if pinned is None:
        check("received the pinned-queue asynchronous message", False)
    else:
        check("the pinned-queue message landed in queue_id=1", pinned.queue_id == 1,
              "queue_id=%r" % pinned.queue_id)
    selected = by_body.get(b"go-live-async-selected")
    if selected is None:
        check("received the selector asynchronous message", False)
    else:
        check("the selector message landed in queue_id=2", selected.queue_id == 2,
              "queue_id=%r" % selected.queue_id)

    print("\nPASS=%d FAIL=%d" % (sum(1 for r in results if r), sum(1 for r in results if not r)))
    return 0 if all(results) else 1


if __name__ == "__main__":
    sys.exit(main())
