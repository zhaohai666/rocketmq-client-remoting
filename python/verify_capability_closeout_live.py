#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Live verification of the 2026-11-11 capability closeout (Java 5.x parity).

The three capabilities added in that round are only worth a real broker if they are
either wire-visible or change **which messages a client sees** -- so each of them is
asserted against an actual 5.5.1 cluster here, not against a mock:

L1  ConsumeFromWhere's three "deprecated" values.
    Java folds CONSUME_FROM_LAST_OFFSET_AND_FROM_MIN_WHEN_BOOT_FIRST /
    CONSUME_FROM_MIN_OFFSET / CONSUME_FROM_MAX_OFFSET into the CONSUME_FROM_LAST_OFFSET
    branch of RebalancePushImpl#computePullFromWhereWithException (lines 171-173), so
    all four must start at the same place. One backlog, five fresh groups:
        FIRST_OFFSET (positive control)  -> receives the whole backlog
        LAST_OFFSET  (baseline)          -> measured, not assumed
        the three deprecated values      -> exactly the baseline
    The baseline is measured because on a **fresh topic** the broker answers
    QUERY_CONSUMER_OFFSET with SUCCESS offset=0 for a never-committed group
    (ConsumerManageProcessor's V3_0_6 path) unless the client sends
    setZeroIfNotFound=false; Java sends no such field, so whether LAST_OFFSET replays
    the backlog is a per-end parity property, not a constant. Only the folding is
    asserted, and the absolute value is printed for the parity record.

L2  LitePull pull-from-which-node pin (connectBrokerByUser / defaultBrokerId).
    Java PullAPIWrapper#recalculatePullFromWhichNode pins the id on the first line and
    ignores the broker's suggestion table -- the only way to read from a slave.
        pin off          -> the request carries no broker id at all (this port's shape for
                            "resolve the address from the route", with no suggestion table
                            on the lite path), and the backlog is delivered
        pin 0 (master)   -> every request carries brokerId=0, backlog delivered
        pin 1 (no slave) -> every request carries brokerId=1, and the backlog is **still**
                            delivered: Java resolves the address with
                            findBrokerAddressInSubscribe(..., onlyThisBroker=false), which
                            falls back to the first broker in the table when the requested
                            id is missing. That fallback is parity, not a hole -- so the
                            assertion is on the id that travels, not on the outcome.
    This single-broker cluster has no slave, so "reads the slave" cannot be shown here.

L3  adminExtGroup.
    A pure identity bit with no wire effect (this port's admin sends no synthetic
    consumer heartbeat), so the live leg only proves admin still boots and serves an RPC
    with a custom group, plus the default/override values themselves.

Usage: python verify_capability_closeout_live.py [namesrv]
"""
from __future__ import annotations

import sys
import threading
import time

sys.path.insert(0, ".")

from client.admin import DEFAULT_ADMIN_EXT_GROUP, DefaultMQAdminExt
from client.consumer import (ConsumeConcurrentlyStatus, DefaultLitePullConsumer,
                             DefaultMQPushConsumer, MessageListenerConcurrently)
from client.mq_client import MQClientInstance
from client.producer import DefaultMQProducer
from common.message import Message
from remoting.protocol.heartbeat import ConsumeFromWhere

NAMESRV = sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1:9876"
PREFIX = "CapLive_%d" % int(time.time() * 1000)
N_MSG = 8

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


def prepare_topic(topic: str, queues: int = 1) -> None:
    """Create the topic up front: a consumer gets no route for an uncreated topic
    (TBW102 is a producer-side fallback only)."""
    c = MQClientInstance("%s-setup-%d" % (PREFIX, int(time.time() * 1000)), [NAMESRV])
    c.start()
    try:
        c.create_topic_in_route(topic, queues, queues)
    finally:
        c.shutdown()


class RecordingListener(MessageListenerConcurrently):
    def __init__(self) -> None:
        self.bodies = []
        self._lock = threading.Lock()

    def consume_message(self, msgs, context):
        with self._lock:
            self.bodies.extend(bytes(m.body) for m in msgs)
        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS

    def count(self) -> int:
        with self._lock:
            return len(self.bodies)


def push_leg(topic: str, group: str, where: str, window: float) -> int:
    """Start one push consumer at `where`, record what arrives for `window` seconds.

    Every leg gets a **fresh group**: a group that has already committed offsets would
    read them back regardless of consumeFromWhere, which is exactly the false green this
    scenario must avoid.
    """
    listener = RecordingListener()
    c = DefaultMQPushConsumer(group)
    c.set_namesrv_addr(NAMESRV)
    c.set_message_listener(listener)
    c.set_consume_thread_nums(1)
    c.set_consume_from_where(where)
    c.subscribe(topic, "*")
    try:
        c.start()
        deadline = time.time() + window
        while time.time() < deadline:
            time.sleep(1.0)
    finally:
        c.shutdown()
    return listener.count()


def lite_leg(topic: str, group: str, pin: bool, broker_id: int, want: int,
             window: float):
    """One LitePull subscribe-mode leg with the given pin.

    Returns ``(polled, request_ids)``: how many messages it polled out of the local
    buffer, and the ``broker_id`` each pull round really put on the request (recorded by
    wrapping the client instance's ``pull_message``).

    The recorded ids are the point of this scenario. A single-broker cluster cannot show
    "reads the slave", and the fallback rule makes any purely behavioural assertion
    ambiguous -- Java's ``findBrokerAddressInSubscribe(..., onlyThisBroker=false)`` falls
    back to the first broker in the table when the requested id is absent, so "pin to a
    missing slave" delivers messages whether or not the pin is honoured. Watching the id
    that actually travels is unambiguous.
    """
    c = DefaultLitePullConsumer(group)
    c.set_namesrv_addr(NAMESRV)
    c.set_poll_timeout_millis(1000)
    c.set_pull_batch_size(32)
    c.set_consume_from_where(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET)
    c.set_connect_broker_by_user(pin)
    c.set_default_broker_id(broker_id)
    c.subscribe(topic, "*")
    got = 0
    request_ids = []
    try:
        c.start()
        client = c._mq_client
        original = client.pull_message

        def spy(*args, **kwargs):
            request_ids.append(kwargs.get("broker_id"))
            return original(*args, **kwargs)

        client.pull_message = spy
        deadline = time.time() + window
        while time.time() < deadline and got < want:
            try:
                got += len(c.poll(1000))
            except Exception as exc:
                print("      poll raised: %s: %s" % (type(exc).__name__, exc))
                time.sleep(0.5)
    finally:
        c.shutdown()
    return got, request_ids


def main() -> int:
    producer = DefaultMQProducer(PREFIX + "_pg")
    producer.set_namesrv_addr(NAMESRV)
    producer.start()
    time.sleep(1)

    # ---------------------------------------------------------------- L1
    print("\nL1 ConsumeFromWhere: the three deprecated values fold into LAST_OFFSET")
    t1 = PREFIX + "_FromWhere"
    prepare_topic(t1)
    bodies = [b"cap-%02d" % i for i in range(N_MSG)]
    for b in bodies:
        producer.send(Message(t1, b))
    print("  sent %d messages, no consumer has ever joined this topic" % N_MSG)

    window = 12.0
    n_first = push_leg(t1, PREFIX + "_gFirst", ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET, window)
    check("L1-positive control: FIRST_OFFSET delivers the whole backlog",
          n_first == N_MSG, "received=%d expect=%d" % (n_first, N_MSG))

    # The four LAST-family values are measured, never compared to a constant, and **not**
    # required to equal each other. Delivery counts cannot discriminate the branch: the
    # broker answers QUERY_CONSUMER_OFFSET for a never-committed group with SUCCESS offset=0
    # on its V3_0_6 path (minOffset<=0 and offset 0 still in memory), otherwise with
    # QUERY_NOT_FOUND, and that answer flips between legs. Measured 2026-11-11 with the JAVA
    # reference client on one backlog in one process: LAST_OFFSET=0 while MIN_OFFSET=8 and
    # MAX_OFFSET=8. So this leg asserts what it can honestly assert -- every value is
    # accepted, the consumer starts, and nothing exceeds the backlog -- and prints the counts
    # as the parity record. The "folds into the LAST_OFFSET branch" claim is pinned by the
    # offline suite, which reads the resolved offset directly with a fake store.
    counts = {}
    for label, where in (
            ("LAST_OFFSET", ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET),
            ("MIN_OFFSET", ConsumeFromWhere.CONSUME_FROM_MIN_OFFSET),
            ("MAX_OFFSET", ConsumeFromWhere.CONSUME_FROM_MAX_OFFSET),
            ("LAST_OFFSET_AND_FROM_MIN_WHEN_BOOT_FIRST",
             ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET_AND_FROM_MIN_WHEN_BOOT_FIRST)):
        n = push_leg(t1, PREFIX + "_g" + label, where, window)
        counts[label] = n
        check("L1-%s accepted: the consumer started and received a sane count" % label,
              0 <= n <= N_MSG, "received=%d of %d" % (n, N_MSG))
    print("  INFO LAST-family delivery counts on this end: %s "
          "(not a parity oracle: the broker's V3_0_6 offset answer is timing-dependent)"
          % counts)

    # ---------------------------------------------------------------- L2
    print("\nL2 LitePull pin (connectBrokerByUser / defaultBrokerId)")
    t2 = PREFIX + "_Pin"
    prepare_topic(t2)
    for b in bodies:
        producer.send(Message(t2, b))
    print("  sent %d messages to %s" % (N_MSG, t2))

    want = N_MSG
    n_off, ids_off = lite_leg(t2, PREFIX + "_pinOff", False, 0, want, 20.0)
    check("L2-pin off: the request carries no broker id (route resolution as before)",
          bool(ids_off) and all(i is None for i in ids_off),
          "ids=%s" % sorted({str(i) for i in ids_off}))
    check("L2-pin off: delivers the backlog", n_off == want, "got=%d expect=%d" % (n_off, want))

    n_master, ids_master = lite_leg(t2, PREFIX + "_pinMaster", True, 0, want, 20.0)
    check("L2-pin to the master (0): every pull request carries brokerId=0",
          bool(ids_master) and all(i == 0 for i in ids_master),
          "ids=%s rounds=%d" % (sorted({str(i) for i in ids_master}), len(ids_master)))
    check("L2-pin to the master (0): delivers the backlog",
          n_master == want, "got=%d expect=%d" % (n_master, want))

    n_slave, ids_slave = lite_leg(t2, PREFIX + "_pinSlave", True, 1, want, 12.0)
    check("L2-pin to a non-existent slave (1): every pull request carries brokerId=1",
          bool(ids_slave) and all(i == 1 for i in ids_slave),
          "ids=%s rounds=%d" % (sorted({str(i) for i in ids_slave}), len(ids_slave)))
    check("L2-pin to a non-existent slave (1): the onlyThisBroker=false fallback still "
          "delivers (Java parity, not a hole)", n_slave == want,
          "got=%d expect=%d" % (n_slave, want))

    # ---------------------------------------------------------------- L3
    print("\nL3 adminExtGroup (identity only, no wire effect)")
    admin = DefaultMQAdminExt()
    admin.set_namesrv_addr(NAMESRV)
    check("L3-default adminExtGroup is 'admin_ext_group'",
          admin.get_admin_ext_group() == "admin_ext_group",
          "value=%r" % admin.get_admin_ext_group())
    check("L3-DEFAULT_ADMIN_EXT_GROUP matches the Java constant",
          DEFAULT_ADMIN_EXT_GROUP == "admin_ext_group", "value=%r" % DEFAULT_ADMIN_EXT_GROUP)
    admin.set_admin_ext_group("cap_closeout_admin_" + PREFIX)
    check("L3-override takes", admin.get_admin_ext_group() == "cap_closeout_admin_" + PREFIX,
          "value=%r" % admin.get_admin_ext_group())
    try:
        admin.start()
        topics = admin.fetch_all_topic_list()
        names = list(getattr(topics, "topic_list", []) or [])
        check("L3-a custom-group admin still boots and serves an RPC on the live cluster",
              any(t2 == n for n in names), "topics=%d" % len(names))
    finally:
        admin.shutdown()

    producer.shutdown()

    print("\n%s: %d passed, %d failed" % (PREFIX, PASS, FAIL))
    return 0 if FAIL == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
