#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import annotations

import sys
import time

sys.path.insert(0, ".")

from client.consumer import (AllocateMachineRoomNearby,
                                      AllocateMessageQueueAveragely,
                                      AllocateMessageQueueAveragelyByCircle,
                                      AllocateMessageQueueByConfig,
                                      AllocateMessageQueueByMachineRoom,
                                      AllocateMessageQueueConsistentHash,
                                      DefaultLitePullConsumer, DefaultMQPullConsumer,
                                      MachineRoomResolver)
from client.exception import MQClientException
from client.producer import DefaultMQProducer
from client.send_result import SendStatus
from common.message import Message, MessageQueue
from remoting.protocol.heartbeat import ConsumeFromWhere

NAMESRV = sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1:9876"
STAMP = int(time.time() * 1000)
TOPIC = "LiteLive_%d" % STAMP
GROUP1 = "LitePG1_%d" % STAMP
GROUP2 = "LitePG2_%d" % STAMP
GROUP3 = "LitePG3_%d" % STAMP
GROUP4 = "LitePG4_%d" % STAMP
GROUP5 = "LitePG5_%d" % STAMP
GROUP6 = "LitePG6_%d" % STAMP
GROUP7 = "LitePG7_%d" % STAMP
GROUP8 = "LitePG8_%d" % STAMP
GROUP9 = "LitePG9_%d" % STAMP
GROUP10 = "LitePG10_%d" % STAMP   # S7d consistent hash ring
GROUP11 = "LitePG11_%d" % STAMP   # S7e same-machine-room nearby allocation
GROUP12 = "LitePG12_%d" % STAMP   # S7f machine room whitelist misconfigured
N_MSG = 12
QUEUE_NUM = 4
TAG_A = {("lite-%02d" % i).encode() for i in range(0, N_MSG, 2)}   # even indices -> TagA, 6 messages in total
TAG_B = {("lite-%02d" % i).encode() for i in range(1, N_MSG, 2)}   # odd indices -> TagB, 6 messages in total

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


def prepare_topic() -> None:
    prod = DefaultMQProducer("PG_Prepare_%d" % STAMP)
    prod.set_namesrv_addr(NAMESRV)
    prod.start()
    try:
        prod.create_topic("TBW102", TOPIC, QUEUE_NUM)
    finally:
        prod.shutdown()
    # Wait for the route to register (probing with DefaultMQPullConsumer is enough, it does not require subscribing first)
    deadline = time.time() + 20
    while time.time() < deadline:
        c = DefaultMQPullConsumer("PG_Probe_%d" % STAMP)
        c.set_namesrv_addr(NAMESRV)
        c.start()
        try:
            qs = c.fetch_subscribe_message_queues(TOPIC)
            if len(qs) >= QUEUE_NUM:
                return
        except BaseException:
            pass
        finally:
            c.shutdown()
        time.sleep(1)
    raise RuntimeError("topic not routable after 20s")


def produce() -> set:
    print("\nS2 produce %d messages (alternating TagA/TagB)" % N_MSG)
    prod = DefaultMQProducer("PG_LiteLive_%d" % STAMP)
    prod.set_namesrv_addr(NAMESRV)
    prod.start()
    sent = set()
    try:
        for i in range(N_MSG):
            body = ("lite-%02d" % i).encode()
            msg = Message(TOPIC, body)
            msg.set_keys("lite-key-%02d" % i)
            msg.set_tags("TagA" if i % 2 == 0 else "TagB")
            prod.send(msg)
            sent.add(bytes(body))
    finally:
        prod.shutdown()
    check("S2 produced %d messages successfully" % N_MSG, len(sent) == N_MSG, "sent=%d" % len(sent))
    return sent


def wait_assignment(consumer: DefaultLitePullConsumer, timeout: int = 20) -> list:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if consumer.assignment():
            return consumer.assignment()
        time.sleep(0.5)
    return []


def drain(consumer: DefaultLitePullConsumer, expect: int, timeout: int = 30) -> list:
    collected = []
    deadline = time.time() + timeout
    while len(collected) < expect and time.time() < deadline:
        batch = consumer.poll(timeout=1000)
        collected.extend(batch)
    return collected


def drain_for(consumer: DefaultLitePullConsumer, seconds: float) -> list:
    """Drain the buffer by time: when a strategy only assigns part of the queues the expected count is unknown, so you cannot wait by count."""
    collected = []
    deadline = time.time() + seconds
    while time.time() < deadline:
        collected.extend(consumer.poll(timeout=1000))
    return collected


def queue_keys(queues: list) -> set:
    """Queue list -> key set ("brokerName#queueId"), used to compare assignment results."""
    return {"%s#%d" % (mq.broker_name, mq.queue_id) for mq in queues}


def wait_split_assignment(a: DefaultLitePullConsumer, b: DefaultLitePullConsumer,
                          total: int, timeout: int = 25) -> tuple:
    """Wait for the two instances to split all queues (assignment convergence needs one heartbeat + rebalance round on each side)."""
    deadline = time.time() + timeout
    va, vb = [], []
    while time.time() < deadline:
        va, vb = a.assignment(), b.assignment()
        ka, kb = queue_keys(va), queue_keys(vb)
        if ka and kb and not (ka & kb) and len(ka | kb) == total:
            return va, vb
        time.sleep(0.5)
    return va, vb


def make_pair(group: str, names: tuple, strategies: tuple, *subscribe: str) -> tuple:
    """Build two instances of the same group with one strategy each (not started).

    Python's client_id is generated per second by default, so starting two instances in the same second would collide into the same clientId (the broker
    would only see one consumer), so here we explicitly give an id with a name suffix, following the S7b pattern.
    """
    out = []
    for name, strategy in zip(names, strategies):
        c = DefaultLitePullConsumer(group)
        c.set_namesrv_addr(NAMESRV)
        c.set_poll_timeout_millis(1000)
        c.instance_name = name
        c.client_id = "%s@%d#%s" % (group, STAMP, name)
        c.set_consume_from_where(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET)
        c.allocate_message_queue_strategy = strategy
        if subscribe:
            c.subscribe(*subscribe)
        out.append(c)
    return tuple(out)


def wait_until_prediction_converged(group: str, mq_all: list, cid_members: tuple,
                                   asserted: tuple, strategies: tuple,
                                   timeout: int = 45) -> tuple:
    pass
    cid_all = sorted(c.client_id for c in cid_members)
    expected = []
    for c, strategy in zip(asserted, strategies):
        predicted = strategy.allocate(group, c.client_id, mq_all, cid_all)
        expected.append(queue_keys(predicted))
    deadline = time.time() + timeout
    while True:
        live = [queue_keys(c.assignment()) for c in asserted]
        if live == expected:
            return True, _snapshot(asserted, live, expected, cid_all)
        if time.time() >= deadline:
            return False, _snapshot(asserted, live, expected, cid_all)
        time.sleep(0.5)


def _snapshot(asserted: tuple, live: list, expected: list, cid_all: list) -> str:
    return " | ".join(
        ["cid=%s live=[%s] predict=[%s]" % (c.client_id, " ".join(sorted(l)), " ".join(sorted(e)))
         for c, l, e in zip(asserted, live, expected)]
        + ["cidAll=%s" % cid_all])


# Where NEARBY lands: the real cluster has only one broker, so record both the queues and the clients into the same machine room,
# so NEARBY necessarily takes the "own machine room" branch, equivalent to the inner strategy.
# Keep the call records too -- used to prove rebalance really asked about the machine room of every queue and client,
# rather than the strategy object being renamed without participating in the assignment.
ROOM = "room1"


class OneRoomResolver(MachineRoomResolver):
    """A resolver that puts every queue / client into ROOM, and records each call along the way."""

    def __init__(self) -> None:
        self.broker_calls: list = []
        self.consumer_calls: list = []

    def broker_deploy_in(self, message_queue: MessageQueue) -> str:
        self.broker_calls.append(message_queue.broker_name)
        return ROOM

    def consumer_deploy_in(self, client_id: str) -> str:
        self.consumer_calls.append(client_id)
        return ROOM


def broker_offset(probe: DefaultMQPullConsumer, mq: MessageQueue) -> int:
    """An independent probe reads the offset of this slot on the broker; never committed (QUERY_NOT_FOUND) is recorded as -1."""
    value = probe.fetch_consume_offset(mq)
    return -1 if value is None else value


def s8_three_offset_tables(foreign_pool: list) -> None:
    pass
    off_topic = "LiteOff_%d" % STAMP
    off_group = "LiteOffPG_%d" % STAMP
    n_big = 1200          # > the 1024 single-poll cap ⇒ one delivery necessarily leaves an undelivered tail

    queues: list = []
    landed = 0
    prod = DefaultMQProducer("PG_PrepareOff_%d" % STAMP)
    prod.set_namesrv_addr(NAMESRV)
    prod.start()
    try:
        try:
            prod.create_topic("TBW102", off_topic, 1)
        except Exception as e:  # noqa: BLE001
            print("!! create_topic(%s) failed: %s" % (off_topic, e))
        route_deadline = time.time() + 20
        while not queues and time.time() < route_deadline:
            try:
                queues = prod.fetch_publish_message_queues(off_topic)
            except Exception:  # noqa: BLE001
                queues = []
            if not queues:
                time.sleep(0.5)
        check("S8 prepare the topic (1 queue)", len(queues) == 1, "queues=%d" % len(queues))
        if len(queues) == 1:
            for frm in range(0, n_big, 300):
                chunk = [Message(off_topic, ("off-%04d" % i).encode())
                         for i in range(frm, min(n_big, frm + 300))]
                try:
                    r = prod.send(chunk, timeout_millis=5000, mq=queues[0])
                    if r.send_status == SendStatus.SEND_OK:
                        landed += len(chunk)
                except Exception as e:  # noqa: BLE001
                    print("   send batch failed: %s" % e)
    finally:
        prod.shutdown()
    check("S8 produced %d messages successfully" % n_big, landed == n_big, "landed=%d" % landed)

    # An independent probe that reads the broker offset (its own group, its own connection, not touching any memory of the instance under test)
    probe = DefaultMQPullConsumer(off_group)
    probe.set_namesrv_addr(NAMESRV)
    probe.start()

    q0 = queues[0] if queues else MessageQueue(off_topic, "broker-a", 0)
    o = DefaultLitePullConsumer(off_group)
    o.set_namesrv_addr(NAMESRV)
    o.instance_name = "liteoff"
    o.client_id = "%s@%d#liteoff" % (off_group, STAMP)
    o.set_poll_timeout_millis(1000)
    o.set_pull_batch_size(32)
    # The four cache gates would otherwise stop the background pull once this queue holds more than
    # pull_threshold_for_queue (default 1000) messages, and 1200 > 1000 -- that is the gate working, not
    # a defect. This scenario is about the three offset tables, so lift every gate above the 1200 backlog.
    o.set_pull_threshold_for_all(n_big * 32)
    o.set_pull_threshold_for_queue(n_big + 100)
    o.set_pull_threshold_size_for_queue(1024)
    o.set_consume_max_span(n_big + 1000)
    o.auto_commit = False            # the commit timing is entirely controlled by the scenario
    o.set_consume_from_where(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET)
    o.assign([q0])
    o.start()
    try:
        # S8a pull-only without delivering: the pull cursor runs to 1200, and the consumed cursor must not move a single step
        pull_deadline = time.time() + 30
        while o.pull_cursor_of(q0) < n_big and time.time() < pull_deadline:
            time.sleep(0.2)
        check("S8a the background pulls all %d into the local buffer (pull cursor=%d)" % (n_big, o.pull_cursor_of(q0)),
              o.pull_cursor_of(q0) == n_big, "pullCursor=%d" % o.pull_cursor_of(q0))
        check("S8a nothing delivered ⇒ the consumed cursor stays at -1", o.consume_cursor_of(q0) == -1,
              "consumeCursor=%d" % o.consume_cursor_of(q0))
        # This is the heart of #68: the old impl's commit() iterated the pull cursor, which would send 1200 out here,
        # and if the caller crashed before that, all 1200 messages would never be delivered even though none had been consumed.
        o.commit()
        after_empty = broker_offset(probe, q0)
        check("S8a nothing delivered ⇒ the broker offset is not pushed away (the old impl commits %d here)" % n_big,
              after_empty != n_big, "brokerOffset=%d" % after_empty)

        # S8b the 1024 single-poll cap < the 1200 in the buffer ⇒ the last 176 are not counted as consumed
        first = o.poll(timeout=3000)
        check("S8b one poll hands out 1024 messages (the single-delivery cap)", len(first) == 1024,
              "got=%d" % len(first))
        check("S8b the consumed cursor = the slot handed out", o.consume_cursor_of(q0) == 1024,
              "consumeCursor=%d" % o.consume_cursor_of(q0))
        check("S8b the buffer still holds %d undelivered messages" % (n_big - 1024),
              o.pull_cursor_of(q0) == n_big and o.consume_cursor_of(q0) < o.pull_cursor_of(q0),
              "pull=%d" % o.pull_cursor_of(q0))
        o.commit()
        broker_at = broker_offset(probe, q0)
        check("S8b what is committed to the broker is exactly 1024 (not %d)" % n_big, broker_at == 1024,
              "brokerOffset=%d" % broker_at)

        # S8c specify an earlier offset: only the commit landing point changes, neither cursor may move
        pull_before = o.pull_cursor_of(q0)
        consume_before = o.consume_cursor_of(q0)
        o.commit({q0: 5}, persist=True)
        rewound = broker_offset(probe, q0)
        check("S8c commit(map) changes the broker offset to the caller-specified 5", rewound == 5,
              "brokerOffset=%d" % rewound)
        check("S8c committing the offset does not change the pull cursor", o.pull_cursor_of(q0) == pull_before,
              "pullCursor=%d" % o.pull_cursor_of(q0))
        check("S8c committing the offset does not change the consumed cursor", o.consume_cursor_of(q0) == consume_before,
              "consumeCursor=%d" % o.consume_cursor_of(q0))
        # After the offset is wound back to 5, the last 176 are still delivered (the buffer is unrelated to the broker offset)
        tail = o.poll(timeout=3000)
        check("S8c after winding back to 5, the remaining %d in the buffer are still delivered" % (n_big - 1024),
              len(tail) == n_big - 1024, "got=%d" % len(tail))
        bodies = {bytes(m.body) for m in first} | {bytes(m.body) for m in tail}
        check("S8 all %d are received across the whole run, none duplicated or missing" % n_big, len(bodies) == n_big,
              "distinct=%d" % len(bodies))

        # S8d not a single byte goes on the wire: committed() can see it, the broker cannot
        o.commit({q0: 777}, persist=False)
        check("S8d persist=False: committed() reads the in-memory table's 777", o.committed(q0) == 777,
              "committed=%s" % o.committed(q0))
        still = broker_offset(probe, q0)
        check("S8d persist=False: the broker side is still the previous round's 5", still == 5,
              "brokerOffset=%d" % still)

        # S8e a new instance (same group) continues consuming from that slot on the broker: the in-memory table does not cross instances
        o2 = DefaultLitePullConsumer(off_group)
        o2.set_namesrv_addr(NAMESRV)
        o2.instance_name = "liteoff2"
        o2.client_id = "%s@%d#liteoff2" % (off_group, STAMP)
        o2.set_poll_timeout_millis(1000)
        o2.set_pull_batch_size(32)
        o2.auto_commit = False
        o2.set_consume_from_where(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET)
        o2.assign([q0])
        o2.start()
        start2 = o2.pull_cursor_of(q0)
        # The upper bound only requires "not eating the other instance's in-memory 777": the starting pull cursor is pushed forward by background
        # continued pulling, and deadlocking at 5 would be gambling on timing.
        check("S8e the new instance's starting point is the broker's 5 (not the other instance's in-memory 777)",
              5 <= start2 < 777, "pullCursor=%d" % start2)
        o2.shutdown()

        # S8f seek rewrites both cursors at once: the replayed segment must not be skipped by an old offset
        # Pause this queue first: background continued pulling would push the pull cursor past 60, and without stopping this assertion gambles on timing.
        o.pause([q0])
        time.sleep(0.3)
        o.seek(q0, 60)
        check("S8f seek changes the pull cursor", o.pull_cursor_of(q0) == 60,
              "pullCursor=%d" % o.pull_cursor_of(q0))
        check("S8f seek also changes the consumed cursor", o.consume_cursor_of(q0) == 60,
              "consumeCursor=%d" % o.consume_cursor_of(q0))
        o.commit()
        seeked = broker_offset(probe, q0)
        check("S8f commit lands on 60 after seek", seeked == 60, "brokerOffset=%d" % seeked)
        o.resume([q0])

        if len(foreign_pool) < 2:
            check("S8g the main topic needs at least 2 queues", False, "queues=%d" % len(foreign_pool))
            return
        foreign = foreign_pool[1]      # another topic's queue, which off_group has never touched
        o.assign([q0, foreign])        # just one extra cursor, does not affect the comparison above
        o.commit({q0: 300}, persist=False)
        check("S8g the unpersisted in-memory value is visible first", o.pending_commit_of(q0) == 300,
              "pending=%d" % o.pending_commit_of(q0))
        o.commit([], persist=True)
        check("S8g an empty set neither clears the table nor sends messages", o.pending_commit_of(q0) == 300,
              "pending=%d" % o.pending_commit_of(q0))
        still2 = broker_offset(probe, q0)
        check("S8g the empty set does not touch the broker", still2 == 60, "brokerOffset=%d" % still2)
        # Naming one foreign queue: it has no consume record (-1 guard blocks the write),
        # but persistAll scanning the table clears q0's unpersisted value -- this is the cost of "committing part of the queues".
        o.commit([foreign], persist=True)
        check("S8g naming a commit clears the unnamed in-memory value (remove unused mq)",
              o.pending_commit_of(q0) == -1, "pending=%d" % o.pending_commit_of(q0))
        check("S8g after clearing, the read-back is the broker's slot (and it is filled back into the table)",
              o.committed(q0) == 60 and o.pending_commit_of(q0) == 60,
              "committed=%s pending=%d" % (o.committed(q0), o.pending_commit_of(q0)))
        after_prune = broker_offset(probe, q0)
        check("S8g the cleanup only drops the in-memory value, and does not write 300 to the broker", after_prune == 60,
              "brokerOffset=%d" % after_prune)
        # commit(Set) takes the currently consumed cursor, not the in-memory slot
        o.commit({q0: 300}, persist=False)
        o.commit([q0], persist=True)
        set_committed = broker_offset(probe, q0)
        check("S8g commit(Set) commits the consumed cursor (60), not the in-memory slot 300",
              set_committed == 60, "brokerOffset=%d" % set_committed)
        check("S8g after persistAll the in-memory table returns to the consumed cursor (updateConsumeOffset)",
              o.pending_commit_of(q0) == 60, "pending=%d" % o.pending_commit_of(q0))
    finally:
        o.shutdown()
        probe.shutdown()


def s9_cache_gate_bounded_pull() -> None:
    """The cached-count gate on a real broker: the pull stops on its own while the buffer is over budget and resumes when it drains."""
    gate_topic = "LiteGate_%d" % STAMP
    gate_group = "LiteGatePG_%d" % STAMP
    ctl_group = "LiteGateCtl_%d" % STAMP
    n_gate = 200
    batch = 32
    queue_threshold = 64      # the gate trips once this queue caches *more* than this many messages

    queues: list = []
    landed = 0
    prod = DefaultMQProducer("PG_PrepareGate_%d" % STAMP)
    prod.set_namesrv_addr(NAMESRV)
    prod.start()
    try:
        prod.create_topic("TBW102", gate_topic, 1)
        route_deadline = time.time() + 20
        while not queues and time.time() < route_deadline:
            try:
                queues = prod.fetch_publish_message_queues(gate_topic)
            except Exception:  # noqa: BLE001
                queues = []
            if not queues:
                time.sleep(0.5)
        check("S9 prepare the topic (1 queue)", len(queues) == 1, "queues=%d" % len(queues))
        if len(queues) != 1:
            return
        q0 = queues[0]
        for frm in range(0, n_gate, 50):
            chunk = [Message(gate_topic, ("gate-%04d" % i).encode())
                     for i in range(frm, min(n_gate, frm + 50))]
            r = prod.send(chunk, timeout_millis=5000, mq=q0)
            if r.send_status == SendStatus.SEND_OK:
                landed += len(chunk)
    finally:
        prod.shutdown()
    check("S9 produced %d messages successfully" % n_gate, landed == n_gate, "landed=%d" % landed)

    def make(group: str, name: str, threshold: int) -> DefaultLitePullConsumer:
        c = DefaultLitePullConsumer(group)
        c.set_namesrv_addr(NAMESRV)
        c.instance_name = name
        c.client_id = "%s@%d#%s" % (group, STAMP, name)
        c.set_poll_timeout_millis(1000)
        c.set_pull_batch_size(batch)
        # Only the count gate is under test: the size gate (100 MiB) and the span gate (2000) cannot
        # trip on 200 tiny messages, and the whole-buffer gate would need threshold*batch to stay clear.
        c.set_pull_threshold_for_queue(threshold)
        c.set_pull_threshold_for_all(n_gate * batch)
        c.auto_commit = False
        c.set_consume_from_where(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET)
        c.assign([q0])
        c.start()
        return c

    gated = make(gate_group, "gated", queue_threshold)
    control = make(ctl_group, "control", n_gate + 100)
    try:
        # The gate has to be reached first: without the pull cursor passing the threshold, "it stopped early" says nothing.
        passed = time.time() + 20
        while gated.pull_cursor_of(q0) <= queue_threshold and time.time() < passed:
            time.sleep(0.2)
        parked = gated.pull_cursor_of(q0)
        check("S9a the gate lets the queue cache past its threshold (%d)" % queue_threshold,
              parked > queue_threshold, "pullCursor=%d" % parked)

        # Then it must refuse every further round while nothing is consumed: the cursor stays put.
        settled = gated.pull_cursor_of(q0)
        hold = time.time() + 5
        while time.time() < hold:
            time.sleep(0.5)
        held = gated.pull_cursor_of(q0)
        check("S9a with the buffer over budget the pull cursor stops on its own (never reaches %d)" % n_gate,
              held == settled and held < n_gate, "pullCursor=%d" % held)

        # Control: same topic, same queue, only the threshold is high enough to clear -- the pull runs to the tail.
        run = time.time() + 20
        while control.pull_cursor_of(q0) < n_gate and time.time() < run:
            time.sleep(0.3)
        check("S9-control the same queue pulls all %d once the threshold clears" % n_gate,
              control.pull_cursor_of(q0) == n_gate, "pullCursor=%d" % control.pull_cursor_of(q0))

        # Draining the buffer releases the gate: the rest arrives without restarting anything.
        got = drain(gated, n_gate, timeout=60)
        bodies = {bytes(m.body) for m in got}
        check("S9b after the buffer drains the remaining messages all arrive (%d)" % n_gate,
              len(got) == n_gate and len(bodies) == n_gate, "got=%d distinct=%d" % (len(got), len(bodies)))
        check("S9b the pull cursor runs to the tail once the queue is consumed",
              gated.pull_cursor_of(q0) == n_gate, "pullCursor=%d" % gated.pull_cursor_of(q0))
    finally:
        gated.shutdown()
        control.shutdown()


def main() -> int:
    print("LitePullConsumer verification against a real machine, topic=%s group=%s" % (TOPIC, GROUP1))
    prepare_topic()

    # ===== S2 the subscribe mode: start the consumer before sending the messages =====
    print("\nS1 starting in subscribe mode + waiting for the rebalance to assign the offsets")
    c1 = DefaultLitePullConsumer(GROUP1)
    c1.set_namesrv_addr(NAMESRV)
    c1.set_poll_timeout_millis(1000)
    c1.subscribe(TOPIC, "*")
    c1.start()
    assigned = wait_assignment(c1)
    check("S1 the rebalance assigned the queues", len(assigned) == QUEUE_NUM, "assigned=%d" % len(assigned))
    if not assigned:
        print("!! no queues were assigned, skipping the rest")
        c1.shutdown()
        return 1

    sent = produce()
    got = drain(c1, N_MSG)
    got_bytes = {bytes(m.body) for m in got}
    check("S2 subscribe+poll collected all %d messages with matching content" % N_MSG, got_bytes == sent,
          "got=%d missing=%s extra=%s" % (len(got), sorted(sent - got_bytes),
                                          sorted(got_bytes - sent)))

    # ===== S3 auto-commit =====
    print("\nS3 the auto-commit offset")
    before = {mq: c1.committed(mq) for mq in assigned}
    deadline = time.time() + 20
    after = before
    while time.time() < deadline:
        c1.poll(timeout=500)          # an idle poll counts as a poll too: the gate is checked inside poll
        after = {mq: c1.committed(mq) for mq in assigned}
        if all(v is not None and v > 0 for v in after.values()):
            break
    check("S3 after polling past the auto-commit period the offset persisted on its own (nobody called commit)",
          all(v is not None and v > 0 for v in after.values()),
          "before=%s after=%s" % (before, after))
    # The manual commit path must persist as well: the offset is already correct, and committing once
    # more must not move it back
    c1.commit()
    after2 = {mq: c1.committed(mq) for mq in assigned}
    check("S3 the offset does not go backwards after commit()",
          all(after2[mq] is not None and after2[mq] >= (after[mq] or 0) for mq in assigned),
          "after2=%s" % after2)

    # ===== S4 the assign mode: explicit assignment + seek back to the head to collect everything again =====
    print("\nS4 the assign mode: assign all queues + seek to the head to collect everything again")
    c2 = DefaultLitePullConsumer(GROUP2)
    c2.set_namesrv_addr(NAMESRV)
    c2.set_poll_timeout_millis(1000)
    c2.set_consume_from_where(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET)
    all_queues = c1.fetch_message_queues(TOPIC)
    c2.assign(all_queues)
    c2.start()
    for mq in all_queues:
        c2.seek_to_begin(mq)
    got2 = drain(c2, N_MSG)
    got2_bytes = {bytes(m.body) for m in got2}
    check("S4 assign+seek+poll collected all %d messages again" % N_MSG, got2_bytes == sent,
          "got=%d" % len(got2))
    c2.shutdown()

    # ===== S5 subscription-level tag filtering =====
    print("\nS5 subscribing with TagA: only the 6 TagA messages are collected")
    c3 = DefaultLitePullConsumer(GROUP3)
    c3.set_namesrv_addr(NAMESRV)
    c3.set_poll_timeout_millis(1000)
    c3.set_consume_from_where(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET)
    c3.subscribe(TOPIC, "TagA")
    c3.start()
    wait_assignment(c3)
    got3 = drain(c3, 6)
    got3_bytes = {bytes(m.body) for m in got3}
    print("    [diag] S5 got=%d bodies=%s tags=%s" % (
        len(got3), sorted(bytes(m.body).decode() for m in got3),
        sorted((m.get_tags() or "?") for m in got3)))
    only_a = got3_bytes.issubset(TAG_A) and len(got3_bytes) == 6
    check("S5 only TagA was collected and there are exactly 6", only_a,
          "got=%d set_ok=%s" % (len(got3), got3_bytes <= TAG_A))
    c3.shutdown()

    # ===== S6 CONSUME_FROM_TIMESTAMP: a 14-digit local wall clock =====
    print("\nS6 CONSUME_FROM_TIMESTAMP: collect everything from the wall-clock start + timestamp->offset mapping")
    wall = int(time.time() * 1000)

    def wall_clock(delta_ms: int) -> str:
        return time.strftime("%Y%m%d%H%M%S", time.localtime((wall + delta_ms) / 1000))

    c4 = DefaultLitePullConsumer(GROUP4)
    c4.set_namesrv_addr(NAMESRV)
    c4.set_poll_timeout_millis(1000)
    c4.consume_from_where = ConsumeFromWhere.CONSUME_FROM_TIMESTAMP
    c4.consume_timestamp = wall_clock(-30 * 60 * 1000)
    c4.subscribe(TOPIC, "*")
    c4.start()
    wait_assignment(c4)
    got4 = drain(c4, N_MSG)
    got4_bytes = {bytes(m.body) for m in got4}
    check("S6a the start=%s precedes all the messages -> all %d messages are collected" % (c4.consume_timestamp, N_MSG),
          got4_bytes == sent, "got=%d" % len(got4_bytes))
    c4.shutdown()

    c5 = DefaultLitePullConsumer(GROUP5)
    c5.set_namesrv_addr(NAMESRV)
    c5.set_poll_timeout_millis(1000)
    c5.consume_from_where = ConsumeFromWhere.CONSUME_FROM_TIMESTAMP
    c5.consume_timestamp = wall_clock(10 * 60 * 1000)
    c5.subscribe(TOPIC, "*")
    c5.start()
    q5 = wait_assignment(c5)
    sum_past = sum(c5.offset_for_timestamp(mq, wall - 30 * 60 * 1000) for mq in q5)
    sum_future = sum(c5.offset_for_timestamp(mq, wall + 10 * 60 * 1000) for mq in q5)
    check("S6b 30 minutes ago -> the head of each queue", sum_past == 0, "sumOffset=%d" % sum_past)
    check("S6b 10 minutes later -> it skips all %d messages" % N_MSG, sum_future == N_MSG,
          "sumOffset=%d" % sum_future)
    c5.shutdown()

    # ===== S7 a pluggable queue allocation strategy =====
    print("\nS7 a pluggable queue allocation strategy (setAllocateMessageQueueStrategy)")
    all_queues = sorted(c1.fetch_message_queues(TOPIC),
                        key=lambda mq: (mq.topic, mq.broker_name, mq.queue_id))
    total_queues = len(all_queues)

    probe = DefaultLitePullConsumer(GROUP6)
    probe.set_namesrv_addr(NAMESRV)
    check("S7a the default strategy name = AVG",
          probe.allocate_message_queue_strategy.get_name() == "AVG",
          "name=%s" % probe.allocate_message_queue_strategy.get_name())
    probe.allocate_message_queue_strategy = None
    probe.subscribe(TOPIC, "*")
    try:
        probe.start()
        check("S7a when the strategy is None start() reports the same wording", False, "no exception was raised")
        probe.shutdown()
    except MQClientException as e:
        check("S7a when the strategy is None start() reports the same wording",
              "allocateMessageQueueStrategy is null" in str(e), str(e))

    # S7b AVG_BY_CIRCLE: the two instances of the same group split it in an interleaved way, with neither overlap nor gaps
    circle = AllocateMessageQueueAveragelyByCircle()
    ca, cb = DefaultLitePullConsumer(GROUP7), DefaultLitePullConsumer(GROUP7)
    for c, name in ((ca, "s7ca"), (cb, "s7cb")):
        c.set_namesrv_addr(NAMESRV)
        c.set_poll_timeout_millis(1000)
        c.instance_name = name
        # The two instances in the same process must have different clientIds, otherwise the broker side only sees one consumer
        c.client_id = "%s@%s#%s" % (GROUP7, STAMP, name)
        c.allocate_message_queue_strategy = circle
        c.subscribe(TOPIC, "*")
    check("S7b after the replacement the strategy name = AVG_BY_CIRCLE",
          ca.allocate_message_queue_strategy.get_name() == "AVG_BY_CIRCLE")
    ca.start()
    cb.start()
    qa, qb = wait_split_assignment(ca, cb, total_queues)
    ka, kb = queue_keys(qa), queue_keys(qb)
    check("S7b the assignments of the two instances have no intersection", not (ka & kb), "overlap=%s" % sorted(ka & kb))
    check("S7b the union covers all %d queues" % total_queues,
          (ka | kb) == queue_keys(all_queues), "a=%d b=%d" % (len(ka), len(kb)))
    # The signature of a circular allocation: what you get are the queues picked by "index modulo",
    # not contiguous segments (4 queues / 2 instances -> 2 each with an index step of 2, whereas AVG
    # gives two contiguous segments).
    pos_a = sorted(i for i, mq in enumerate(all_queues)
                   if "%s#%d" % (mq.broker_name, mq.queue_id) in ka)
    circle_shape = len(pos_a) == 2 and all((y - x) % 2 == 0 for x, y in zip(pos_a, pos_a[1:]))
    check("S7b the assignment shape is interleaved (step 2), not the contiguous segments of AVG",
          total_queues != 4 or circle_shape, "posA=%s" % pos_a)
    ca.shutdown()
    cb.shutdown()

    # S7c CONFIG: only the half of the queues that went into the configuration are assigned, and the
    # messages obtained by poll can only come from those queues
    half_a, half_b = all_queues[:total_queues // 2], all_queues[total_queues // 2:]
    cfg_a, cfg_b = AllocateMessageQueueByConfig(half_a), AllocateMessageQueueByConfig(half_b)
    c6, c7 = DefaultLitePullConsumer(GROUP8), DefaultLitePullConsumer(GROUP9)
    for c, strategy in ((c6, cfg_a), (c7, cfg_b)):
        c.set_namesrv_addr(NAMESRV)
        c.set_poll_timeout_millis(1000)
        c.set_consume_from_where(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET)
        c.allocate_message_queue_strategy = strategy
        c.subscribe(TOPIC, "*")
    c6.start()
    c7.start()
    a6, a7 = wait_assignment(c6), wait_assignment(c7)
    check("S7c CONFIG only gives the configured queues (ignoring mqAll/cidAll)",
          queue_keys(a6) == queue_keys(half_a) and queue_keys(a7) == queue_keys(half_b),
          "a=%s b=%s" % (sorted(queue_keys(a6)), sorted(queue_keys(a7))))
    keys_a, keys_b = queue_keys(half_a), queue_keys(half_b)
    g6, g7 = drain_for(c6, 8), drain_for(c7, 8)
    b6 = {bytes(m.body) for m in g6}
    b7 = {bytes(m.body) for m in g7}
    on_cfg_a = {bytes(m.body) for m in g6 if "%s#%d" % (m.broker_name, m.queue_id) in keys_a}
    on_cfg_b = {bytes(m.body) for m in g7 if "%s#%d" % (m.broker_name, m.queue_id) in keys_b}
    check("S7c the CONFIG consumer only receives messages from its own configured queues",
          b6 == on_cfg_a and b7 == on_cfg_b,
          "a=%d aOnCfg=%d b=%d bOnCfg=%d" % (len(b6), len(on_cfg_a), len(b7), len(on_cfg_b)))
    check("S7c the two halves together cover exactly all %d messages without overlapping" % N_MSG,
          (b6 | b7) == sent and not (b6 & b7),
          "union=%d inter=%d" % (len(b6 | b7), len(b6 & b7)))
    c6.shutdown()
    c7.shutdown()

    ch = AllocateMessageQueueConsistentHash()
    d1, d2 = make_pair(GROUP10, ("s7da", "s7db"), (ch, ch), TOPIC, "*")
    check("S7d after the replacement the strategy name = CONSISTENT_HASH",
          d1.allocate_message_queue_strategy.get_name() == "CONSISTENT_HASH",
          d1.allocate_message_queue_strategy.get_name())
    d1.start()
    d2.start()
    ok, snap = wait_until_prediction_converged(GROUP10, all_queues, (d1, d2), (d1, d2), (ch, ch))
    check("S7d the live assignment converges to the offline prediction of the consistent-hash ring", ok, snap)
    kd1, kd2 = queue_keys(d1.assignment()), queue_keys(d2.assignment())
    check("S7d the assignments of the two instances have no intersection", not (kd1 & kd2), "overlap=%s" % sorted(kd1 & kd2))
    check("S7d the union covers all %d queues" % total_queues,
          (kd1 | kd2) == queue_keys(all_queues), "a=%d b=%d" % (len(kd1), len(kd2)))
    bd1 = {bytes(m.body) for m in drain_for(d1, 8)}
    bd2 = {bytes(m.body) for m in drain_for(d2, 8)}
    check("S7d the two instances together received all %d messages (the ring really drives the send/receive)" % N_MSG,
          (bd1 | bd2) == sent, "union=%d" % len(bd1 | bd2))
    d1.shutdown()
    d2.shutdown()

    # S7e MACHINE_ROOM_NEARBY: a real cluster has only one machine room => the decorator must pass the
    # inner strategy through unchanged.
    inner = AllocateMessageQueueConsistentHash()
    resolver = OneRoomResolver()
    nearby = AllocateMachineRoomNearby(inner, resolver)
    e1, e2 = make_pair(GROUP11, ("s7ea", "s7eb"), (nearby, nearby), TOPIC, "*")
    check("S7e the decorated strategy name = MACHINE_ROOM_NEARBY-CONSISTENT_HASH",
          e1.allocate_message_queue_strategy.get_name() == "MACHINE_ROOM_NEARBY-CONSISTENT_HASH",
          e1.allocate_message_queue_strategy.get_name())
    e1.start()
    e2.start()
    ok, snap = wait_until_prediction_converged(GROUP11, all_queues, (e1, e2), (e1, e2),
                                              (inner, inner))
    check("S7e the live assignment of NEARBY == the offline prediction of the inner ring", ok, snap)
    real_brokers = {mq.broker_name for mq in all_queues}
    check("S7e the resolver was asked about the machine room per queue (%d times)" % len(resolver.broker_calls),
          bool(resolver.broker_calls) and set(resolver.broker_calls) == real_brokers,
          "brokers=%s" % sorted(set(resolver.broker_calls)))
    check("S7e the resolver was asked about two real clientIds",
          {e1.client_id, e2.client_id} <= set(resolver.consumer_calls),
          "calls=%s" % sorted(set(resolver.consumer_calls)))
    e1.shutdown()
    e2.shutdown()

    room = AllocateMessageQueueByMachineRoom({ROOM})
    check("S7f the strategy name = MACHINE_ROOM and the whitelist can be read back",
          room.get_name() == "MACHINE_ROOM" and room.consumeridcs == {ROOM},
          "idcs=%s" % sorted(room.consumeridcs))
    avg = AllocateMessageQueueAveragely()
    f1, f2 = make_pair(GROUP12, ("s7fa", "s7fb"), (room, avg), TOPIC, "*")
    f1.start()
    f2.start()
    ctrl = wait_assignment(f2)
    check("S7f the control instance of the same group (AVG) is assigned queues normally", bool(ctrl), "ctrl=%s" % sorted(queue_keys(ctrl)))
    check("S7f the machine room does not match the real brokerName -> not a single queue is assigned (no error and no wrongly eaten queue)",
          not f1.assignment(), "assignment=%s" % sorted(queue_keys(f1.assignment())))
    ok, snap = wait_until_prediction_converged(GROUP12, all_queues, (f1, f2), (f1, f2),
                                              (room, avg))
    check("S7f the live assignment on each side converges to the offline prediction of its own strategy", ok, snap)
    starved = drain_for(f1, 5)
    check("S7f the starved side polls no messages and throws no error", not starved, "got=%d" % len(starved))
    f1.shutdown()
    f2.shutdown()

    c1.shutdown()

    # ===== S8 three offset tables =====
    print("\nS8 three offset tables: the pull cursor / the consumed cursor / the committed landing point")
    s8_three_offset_tables(assigned)

    # ===== S9 the cached-count gate on a real broker =====
    print("\nS9 the local buffer's cached-count gate: the pull parks itself and resumes after draining")
    s9_cache_gate_bounded_pull()

    print("\nLitePullConsumer: PASS=%d FAIL=%d" % (PASS, FAIL))
    return 0 if FAIL == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
