#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Verification of the active-pull consumer (DefaultMQPullConsumer) against a real machine.

Usage (requires a local RocketMQ 5.5.1 cluster):
    .venv/bin/python verify_pull_live.py 127.0.0.1:9876

Why it is tested on its own: `DefaultMQPullConsumer` has existed in this project for a long
time but has **never been called by any test or integration script** (grep finds 0 hits across
the whole repository) -- in other words, it was a "paper capability". This script actually runs
it.

Scenarios (the core of the pull mode is that "the caller pulls and owns the offsets", so every
assertion revolves around that):
  S1 create topic + fetch_subscribe_message_queues -> obtain 4 queues
  S2 produce 12 messages -> the min/max offset difference of each queue = 3 (the messages are
     spread evenly over the 4 queues)
  S3 **manual pulling**: per queue, pull from the min offset to the max offset -> collect all 12
     messages and their bodies match the sent set
  S4 **manual offset commit**: update_consume_offset -> fetch_consume_offset reads back the same
     value (a real round trip to the broker)
  S5 **the caller owns the offsets**: pulling again from the committed offset -> NO_NEW_MSG; moving
     the offset back to min and pulling again -> FOUND (the push mode cannot do this, which is
     exactly the reason the pull mode exists)
  S6 search_offset(now) / earliest_msg_store_time -> both > 0
  S7 send_message_back -> the message lands in %RETRY%group and can be pulled (the send-back chain
     really works)
  S8 fetch_message_queues_in_balance -> each instance only gets its own share; the two shares do
     not overlap and together cover the whole group (the criterion is "an instance that never pulled
     also gets a non-empty result": the fallback when the computation fails is to keep the existing
     assignment, that is, an empty set)
"""
from __future__ import annotations

import sys
import time

sys.path.insert(0, ".")

from client.consumer import DefaultMQPullConsumer
from client.consumer_result import PullStatus
from client.producer import DefaultMQProducer
from common.message import Message, MessageExt, MessageQueue
from remoting.exception import RemotingException

NAMESRV = sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1:9876"
STAMP = int(time.time() * 1000)
TOPIC = "PullLive_%d" % STAMP
GROUP = "PG_PullLive_%d" % STAMP
RETRY_TOPIC = "%RETRY%" + GROUP
N_MSG = 12
QUEUE_NUM = 4
PER_QUEUE = N_MSG // QUEUE_NUM

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


def make_consumer() -> DefaultMQPullConsumer:
    c = DefaultMQPullConsumer(GROUP)
    c.set_namesrv_addr(NAMESRV)
    c.register_topics.add(TOPIC)
    c.start()
    return c


def prepare_topic() -> None:
    """Create the topic first and only then start the consumer: the consumer does not fall back to a default topic, so without the topic it cannot get a route."""
    prod = DefaultMQProducer("PG_Prepare_%d" % STAMP)
    prod.set_namesrv_addr(NAMESRV)
    prod.start()
    try:
        prod.create_topic("TBW102", TOPIC, QUEUE_NUM)
    finally:
        prod.shutdown()
    deadline = time.time() + 20
    last = ""
    while time.time() < deadline:
        c = make_consumer()
        try:
            qs = c.fetch_subscribe_message_queues(TOPIC)
            if len(qs) >= QUEUE_NUM:
                return
            last = "%d queues" % len(qs)
        except BaseException as e:  # noqa: BLE001
            last = "%s: %s" % (type(e).__name__, e)
        finally:
            c.shutdown()
        time.sleep(1)
    raise RuntimeError("topic not routable after 20s: " + last)


def safe_pull(consumer: DefaultMQPullConsumer, q: MessageQueue, expr: str, offset: int,
              max_nums: int = 32, timeout: int = 5000):
    """Wrap a pull: a timeout / network exception returns (None, detail), so that an assertion can report FAIL instead of a stack trace aborting the script."""
    try:
        return consumer.pull(q, expr, offset, max_nums, timeout), ""
    except RemotingException as e:
        return None, "%s: %s" % (type(e).__name__, e)


def scenario_queues(consumer: DefaultMQPullConsumer):
    print("\nS1 create topic + fetch_subscribe_message_queues")
    queues = consumer.fetch_subscribe_message_queues(TOPIC)
    check("S1 obtained %d queues" % QUEUE_NUM, len(queues) == QUEUE_NUM,
          "got=%d" % len(queues))
    check("S1 the queue topic and the broker name are not empty",
          bool(queues) and all(q.topic == TOPIC and q.broker_name for q in queues),
          "sample=%s/%s" % ((queues[0].broker_name, queues[0].queue_id) if queues else "n/a",
                            len(queues)))
    return queues


def scenario_produce() -> list:
    print("\nS2 produce %d messages" % N_MSG)
    prod = DefaultMQProducer("PG_PullLive_%d" % STAMP)
    prod.set_namesrv_addr(NAMESRV)
    prod.start()
    sent = []
    try:
        for i in range(N_MSG):
            body = ("pull-%02d" % i).encode()
            msg = Message(TOPIC, body)
            msg.set_keys("pull-key-%02d" % i)
            prod.send(msg)
            sent.append(body)
    finally:
        prod.shutdown()
    check("S2 producing %d messages succeeded" % N_MSG, len(sent) == N_MSG, "sent=%d" % len(sent))
    return sent


def wait_offsets(consumer: DefaultMQPullConsumer, routes, expect: int):
    """Wait for the asynchronous consumequeue dispatch to land: poll until the sum of max-min over
    all queues is >= expect.

    CAUTION: you must not "query maxOffset right after sending" -- the broker dispatches
    asynchronously, so you would read 0.
    """
    deadline = time.time() + 25
    lo = {}
    hi = {}
    while time.time() < deadline:
        lo = {}
        hi = {}
        for q in routes:
            lo[q] = consumer.min_offset(q)
            hi[q] = consumer.max_offset(q)
        if sum(max(0, hi[q] - lo[q]) for q in routes) >= expect:
            break
        time.sleep(0.5)
    return lo, hi


def scenario_pull(consumer: DefaultMQPullConsumer, routes, lo, hi, sent_set) -> set:
    print("\nS3 manual pulling (per queue, min -> max)")
    got = set()
    for q in routes:
        offset = lo[q]
        guard = 0
        while offset < hi[q] and guard < 64:
            guard += 1
            result, err = safe_pull(consumer, q, "*", offset)
            if result is None:
                check("S3 pulling %s:%d raised an exception" % (q.broker_name, q.queue_id), False, err)
                break
            if result.status == PullStatus.FOUND:
                for m in result.msg_found_list:
                    got.add(bytes(m.body))
                if result.next_begin_offset <= offset:
                    break
                offset = result.next_begin_offset
            elif result.status == PullStatus.NO_NEW_MSG:
                break
            elif result.status == PullStatus.OFFSET_ILLEGAL:
                break
            else:
                check("S3 the status of the pull from %s:%d is unexpected" % (q.broker_name, q.queue_id), False,
                      "status=%s" % result.status)
                break
    check("S3 manual pulling collected all %d messages with matching content" % N_MSG, got == sent_set,
          "got=%d missing=%s extra=%s" % (len(got), sorted(sent_set - got),
                                          sorted(got - sent_set)))
    return got


def scenario_commit(consumer: DefaultMQPullConsumer, q, target: int) -> int:
    print("\nS4 manually commit the offset and read it back")
    consumer.update_consume_offset(q, target)
    back = consumer.fetch_consume_offset(q)
    check("S4 the offset read back after the commit matches", back == target,
          "committed=%s target=%d" % (back, target))
    return target


def scenario_own_offset(consumer: DefaultMQPullConsumer, q, committed: int, lo: int) -> None:
    print("\nS5 the caller owns the offset")
    r1, err1 = safe_pull(consumer, q, "*", committed)
    if r1 is None:
        check("S5 pulling again from the committed offset = NO_NEW_MSG", False, err1)
    else:
        check("S5 pulling again from the committed offset = NO_NEW_MSG",
              r1.status == PullStatus.NO_NEW_MSG and len(r1.msg_found_list) == 0,
              "status=%s n=%d" % (r1.status, len(r1.msg_found_list)))

    # The key capability: moving the offset back to min lets you **pull the same messages again**
    # (which the push mode cannot do)
    r2, err2 = safe_pull(consumer, q, "*", lo)
    if r2 is None:
        check("S5 after moving the offset back to min they can be pulled again (the core capability of the pull mode)", False, err2)
    else:
        check("S5 after moving the offset back to min they can be pulled again (the core capability of the pull mode)",
              r2.status == PullStatus.FOUND and len(r2.msg_found_list) > 0,
              "status=%s n=%d" % (r2.status, len(r2.msg_found_list)))


def scenario_offset_query(consumer: DefaultMQPullConsumer, q, lo: int, hi: int) -> None:
    print("\nS6 search_offset / earliest_msg_store_time / min/max")
    now = int(time.time() * 1000)
    so = consumer.search_offset(q, now)
    check("S6 search_offset(now) > 0", so > 0, "searchOffset=%d" % so)
    emst = consumer.earliest_msg_store_time(q)
    check("S6 earliest_msg_store_time > 0", emst > 0, "earliest=%d" % emst)
    check("S6 min_offset <= max_offset", lo <= hi, "min=%d max=%d" % (lo, hi))


def scenario_send_back(consumer: DefaultMQPullConsumer, sample: MessageExt,
                       expect_body: bytes) -> None:
    pass
    print("\nS7 send_message_back -> %%RETRY%%group can be pulled")
    try:
        # When delayLevel=0 the broker rewrites it to 3 + reconsumeTimes (the default 10s delay)
        consumer.send_message_back(sample, 0)
        check("S7 the send-back request was accepted by the broker", True,
              "offset=%d body=%s" % (sample.commit_log_offset, bytes(sample.body)))
    except BaseException as e:  # noqa: BLE001
        check("S7 the send-back request was accepted by the broker", False, "%s: %s" % (type(e).__name__, e))
        return

    # After the delayed message lands, pull %RETRY%group
    found = False
    detail = "not tried"
    deadline = time.time() + 40
    while time.time() < deadline and not found:
        try:
            retry_queues = consumer.fetch_subscribe_message_queues(RETRY_TOPIC)
        except BaseException as e:  # noqa: BLE001
            detail = "retry topic not routable yet: %s" % e
            time.sleep(1)
            continue
        for rq in retry_queues:
            try:
                rlo = consumer.min_offset(rq)
                rhi = consumer.max_offset(rq)
            except BaseException as e:  # noqa: BLE001
                detail = "%s: %s" % (type(e).__name__, e)
                continue
            if rhi <= rlo:
                continue
            r, rerr = safe_pull(consumer, rq, "*", rlo)
            if r is None:
                detail = rerr
                continue
            for m in r.msg_found_list:
                if bytes(m.body) == expect_body:
                    found = True
                    detail = ("queue=%d reconsumeTimes=%d body=%s"
                              % (rq.queue_id, m.get_reconsume_times(), bytes(m.body)))
                    break
            if found:
                break
        if not found:
            time.sleep(1)
    check("S7 %%RETRY%% pulled the message that was sent back", found, detail)


def scenario_balance_view(consumer: DefaultMQPullConsumer) -> None:
    pass
    print("\nS8 fetch_message_queues_in_balance")
    whole = consumer.fetch_subscribe_message_queues(TOPIC)
    key = lambda qs: sorted((q.broker_name, q.queue_id) for q in qs)  # noqa: E731

    b = make_consumer()
    try:
        cid_all: list = []
        deadline = time.time() + 30
        while time.time() < deadline:
            try:
                cid_all = b._mq_client.get_consumer_id_list_by_group(TOPIC, GROUP) or []
            except BaseException:  # noqa: BLE001
                cid_all = []
            if len(cid_all) >= 2:
                break
            time.sleep(1)
        check("S8 the broker's 38 counts two instances of the group (the heartbeat really registered into consumerTable)",
              len(cid_all) >= 2, "cid_all=%s" % sorted(cid_all))

        mine_a = consumer.fetch_message_queues_in_balance(TOPIC)
        mine_b = b.fetch_message_queues_in_balance(TOPIC)
        check("S8 the instance that never pulled also computed its own share (proving it went through the route + 38, not the fallback)",
              len(mine_b) > 0, "b pulled nothing, mine_b=%d" % len(mine_b))
        check("S8 the shares of both instances are contained in the subscription view",
              set(key(mine_a)) <= set(key(whole)) and set(key(mine_b)) <= set(key(whole)),
              "a=%s b=%s" % (key(mine_a), key(mine_b)))
        overlap = set(key(mine_a)) & set(key(mine_b))
        check("S8 the shares of the two instances do not overlap (an overlap means the same message is consumed twice)",
              len(overlap) == 0, "a=%s b=%s overlap=%s"
              % (key(mine_a), key(mine_b), sorted(overlap)))
        check("S8 the two instances together cover the whole topic",
              set(key(mine_a)) | set(key(mine_b)) == set(key(whole)),
              "a=%d b=%d whole=%d" % (len(mine_a), len(mine_b), len(whole)))
    finally:
        b.shutdown()


def main() -> int:
    print("Pull-mode consumer verification against a real machine, topic=%s group=%s" % (TOPIC, GROUP))
    prepare_topic()

    consumer = make_consumer()
    try:
        routes = scenario_queues(consumer)
        if len(routes) != QUEUE_NUM:
            print("!! the queue count is wrong, skipping the remaining scenarios")
            return 1

        sent = scenario_produce()
        lo, hi = wait_offsets(consumer, routes, N_MSG)
        for q in routes:
            check("S2 queue %s:%d holds %d messages" % (q.broker_name, q.queue_id, PER_QUEUE),
                  hi[q] - lo[q] == PER_QUEUE, "min=%d max=%d" % (lo[q], hi[q]))

        got = scenario_pull(consumer, routes, lo, hi, set(sent))
        if not got:
            print("!! not a single message was pulled, skipping the remaining scenarios")
            return 1

        q0 = routes[0]
        committed = scenario_commit(consumer, q0, hi[q0])
        scenario_own_offset(consumer, q0, committed, lo[q0])
        scenario_offset_query(consumer, q0, lo[q0], hi[q0])

        # Sending a message back needs a genuinely pulled MessageExt (carrying commitLogOffset)
        sample = None
        for q in routes:
            r, _err = safe_pull(consumer, q, "*", lo[q], max_nums=1)
            if r is not None and r.msg_found_list:
                sample = r.msg_found_list[0]
                break
        if sample is None:
            check("S7 obtained the sample message", False, "no message available")
        else:
            scenario_send_back(consumer, sample, bytes(sample.body))

        scenario_balance_view(consumer)
    finally:
        consumer.shutdown()

    print("\nPullConsumer: PASS=%d FAIL=%d" % (PASS, FAIL))
    return 0 if FAIL == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
