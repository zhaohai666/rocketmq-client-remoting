# -*- coding: utf-8 -*-
from __future__ import annotations

import sys
import threading
import time

sys.path.insert(0, ".")

from client.admin import DefaultMQAdminExt
from client.consumer import (ConsumeConcurrentlyStatus,
                                      DefaultMQPushConsumer,
                                      MessageListenerConcurrently)
from client.mq_client import MQClientInstance
from client.producer import DefaultMQProducer
from common.message import Message
from remoting.protocol.heartbeat import ConsumeFromWhere

NAMESRV = sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1:9876"
STAMP = int(time.time() * 1000)
TOPIC1 = "ResetOffsetBack_%d" % STAMP
GROUP1 = "GID_ResetOffsetBack_%d" % STAMP
TOPIC2 = "ResetOffsetSkip_%d" % STAMP
GROUP2 = "GID_ResetOffsetSkip_%d" % STAMP
MSGS = 10
BACK_TARGET = 3

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


def committed(client: MQClientInstance, group: str, topic: str):
    """[(MessageQueue, maxOffset, committed or None)], where committed=None means the broker has no record."""
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


def fmt_rows(rows) -> str:
    return " ".join("q%d:%s/max%d" % (mq.queue_id, o, m) for mq, m, o in rows)


class SteppingListener(MessageListenerConcurrently):
    """Every batch is held at the gate and the test releases them one by one, so that the local
    state between batches can be sampled.

    The key sampling point of S1 (the old batch has been released, the first batch of the new queue
    has not been acked yet) only exists when the batches are controllable one by one; a listener
    that releases everything at once mixes the 11 (the old batch ack) and the following redelivery
    into a single instant.
    """

    def __init__(self):
        self._lock = threading.Lock()
        self._batches = []
        self._release = threading.Event()

    def consume_message(self, msgs, context):
        with self._lock:
            self._batches.append([m.queue_offset for m in msgs])
        # The upper bound only guards against a deadlock: on the normal path the test calls release() batch by batch
        self._release.wait(90)
        self._release.clear()
        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS

    def wait_batch(self, n: int, timeout: float = 20.0) -> bool:
        return wait_until(lambda: len(self.batches()) >= n, timeout, 0.05)

    def release(self) -> None:
        self._release.set()

    def batches(self):
        with self._lock:
            return [list(b) for b in self._batches]

    def offsets(self):
        with self._lock:
            return [o for batch in self._batches for o in batch]


class RecordingListener(MessageListenerConcurrently):
    def __init__(self):
        self._offsets = []
        self._lock = threading.Lock()

    def consume_message(self, msgs, context):
        with self._lock:
            self._offsets.extend(m.queue_offset for m in msgs)
        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS

    def offsets(self):
        with self._lock:
            return list(self._offsets)


def consumer_key(c: DefaultMQPushConsumer, topic: str):
    """The queue key of that topic from the consumer's point of view (uses the key of _mq_map instead of assembling brokerName by hand)."""
    with c._lock:
        for key, mq in c._mq_map.items():
            if mq.topic == topic:
                return key
    return None


def local_offset(c: DefaultMQPushConsumer, topic: str):
    pass
    status = c.get_consumer_status(topic)
    for mq, off in status.items():
        if mq.queue_id == 0:
            return off
    return None


def wait_broker_addr(admin: DefaultMQAdminExt, timeout: float = 40.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            ci = admin.fetch_broker_cluster_info()
            if ci and ci.broker_addr_table:
                return ci.get_broker_addrs()[0]
        except Exception:  # noqa: BLE001
            pass
        time.sleep(1)
    return None


def set_server_side_reset(admin: DefaultMQAdminExt, broker_addr: str, value: str) -> bool:
    admin.update_broker_config(broker_addr, {"useServerSideResetOffset": value})
    time.sleep(1)
    cfg = admin.get_broker_config(broker_addr)
    return cfg.get("useServerSideResetOffset") == value


def start_consumer(group: str, topic: str, listener, batch_size: int, persist_ms: int):
    c = DefaultMQPushConsumer(group)
    c.set_namesrv_addr(NAMESRV)
    c.set_message_listener(listener)
    c.consume_message_batch_max_size = batch_size
    if persist_ms > 0:
        c.persist_consumer_offset_interval = persist_ms
    c.set_consume_from_where(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET)
    c.subscribe(topic, "*")
    c.start()
    return c


# ------------------------------------------------ S1: the backward reset + voiding the in-flight batch


def s1_backward_reset(setup: MQClientInstance, admin: DefaultMQAdminExt,
                      broker_addr: str, producer: DefaultMQProducer) -> None:
    print("\n--- S1: the backward reset (10 -> 3) persists immediately + the in-flight batch is voided as a whole ---")
    # Precondition: use an ordinary listener to bring the offset on the broker to 10 (the reset requires that the
    # group has a record on the broker)
    l0 = RecordingListener()
    c0 = start_consumer(GROUP1, TOPIC1, l0, 1, 0)
    time.sleep(1)
    t_mid = 0
    for i in range(MSGS):
        producer.send(Message(TOPIC1, b"rb-%d" % i))
        if i == 2:
            t_mid = int(time.time() * 1000)
            # Give the 3rd entry (offset 3) some distance in store time from the previous three, so that the
    # timestamp-based reset later reliably hits 3
            time.sleep(2)
    ok = wait_until(lambda: len(l0.offsets()) == MSGS, 30, 0.2)
    check("S1-precondition: the consumer consumed %d messages" % MSGS, ok, "arrivals=%s" % l0.offsets())

    def broker_is(n):
        rows = committed(setup, GROUP1, TOPIC1)
        return bool(rows) and all(o == n for _, _, o in rows)

    ok = wait_until(lambda: broker_is(MSGS), 20, 0.5)
    check("S1-precondition: the broker offset was periodically persisted at %d (the reset requires the group to have a record on the broker)" % MSGS, ok,
          fmt_rows(committed(setup, GROUP1, TOPIC1) or []))
    c0.shutdown()
    time.sleep(1)

    # The main consumer: gated batch by batch + a 60s periodic persist => the only path inside the window
    # that can change the broker offset is the persist that the reset itself brings along
    listener = SteppingListener()
    c = start_consumer(GROUP1, TOPIC1, listener, 1, 60000)
    t_start = time.time()
    time.sleep(2)
    for i in range(MSGS, MSGS + 5):
        producer.send(Message(TOPIC1, b"rb-%d" % i))
    key = None

    def arranged():
        nonlocal key
        k = consumer_key(c, TOPIC1)
        if k is None:
            return False
        key = k
        return len(listener.batches()) >= 1 and len(c._pending.get(k, ())) == 4

    ok = wait_until(arranged, 30, 0.05)
    check("S1-window ready: 1 in-flight (offset 10 stuck in the listener) + 4 left in the buffer",
          ok and listener.offsets()[:1] == [10],
          "arrivals=%s pending=%s" % (listener.offsets(),
                                      len(c._pending.get(key, ())) if key else "?"))
    if not ok or key is None:
        listener.release()
        c.shutdown()
        return

    wait_until(lambda: time.time() - t_start > 12, 15, 0.5)
    check("S1 the first periodic persist has already happened (no periodic writes for the next 60s)",
          time.time() - t_start > 12 and broker_is(MSGS),
          fmt_rows(committed(setup, GROUP1, TOPIC1) or []))

    # Reset by timestamp to 3: t_mid falls between the 3rd and the 4th entry, so
    # getOffsetInQueueByTime hits 3, and since 3 < consumerOffset(10) with isForce=true the broker
    # pushes {mq: 3}
    reset_ts = t_mid + 500
    t0 = time.time()
    table = admin.reset_offset_by_timestamp(TOPIC1, GROUP1, reset_ts, True)
    check("S1 the target offset in the 222 response is exactly 3", [v for v in table.values()] == [BACK_TARGET],
          "table=%s" % {str(k): v for k, v in table.items()})

    ok = wait_before(lambda: broker_is(BACK_TARGET), 2.0)
    elapsed = time.time() - t0
    check("S1 the broker offset becomes %d within %.1fs (the persist that the reset path brings along, periodic persist=60s)"
          % (elapsed, BACK_TARGET), ok, fmt_rows(committed(setup, GROUP1, TOPIC1) or []))

    off = local_offset(c, TOPIC1)
    check("S1 after the reset the local table has no old offset (removeOffset; the new offset leaves through the revoked tail)",
          off is None, "local_offset=%s epoch=%d" % (off, c._queue_epoch.get(key, 0)))

    # Release the old batch: its ack belongs to a ProcessQueue that has already been revoked, so it must be voided
    listener.release()
    ok = listener.wait_batch(2, 20.0)
    off = local_offset(c, TOPIC1)
    check("S1 the old batch ack is voided (after the release, while the first batch of the new queue at offset %d is in-flight, the local offset is still not past 3)"
          % (listener.batches()[1][0] if ok and len(listener.batches()) > 1 else -1),
          ok and (off is None or off <= BACK_TARGET),
          "local_offset=%s arrivals=%s" % (off, listener.offsets()))

    # Releasing batch by batch walks through the redelivery: 3..14 are each redelivered once (the 11..14 in the
    # old buffer have already been voided and can only appear as redeliveries)
    expected = [10] + list(range(BACK_TARGET, MSGS + 5))
    for n in range(2, len(expected) + 1):
        listener.release()
        if n + 1 <= len(expected):
            listener.wait_batch(n + 1, 15.0)
    arr = listener.offsets()
    check("S1 the queue is really rebuilt: the redelivery sequence is %s" % expected, arr == expected, "arrivals=%s" % arr)
    wait_until(lambda: local_offset(c, TOPIC1) == MSGS + 5, 10, 0.05)

    rows = committed(setup, GROUP1, TOPIC1) or []
    check("S1 inside the window only the reset wrote to the broker (the offset is still parked at %d, periodic persist=60s)" % BACK_TARGET,
          bool(rows) and all(o == BACK_TARGET for _, _, o in rows), fmt_rows(rows))

    # Shutting down persists once synchronously: the ack of the redelivery is the final value (15 = the last entry 14 plus one)
    c.shutdown()
    ok = wait_until(lambda: broker_is(MSGS + 5), 15, 0.5)
    check("S1 the persist on shutdown writes the ack of the redelivery back to the broker (the offset advances to %d)" % (MSGS + 5), ok,
          fmt_rows(committed(setup, GROUP1, TOPIC1) or []))


def wait_before(pred, seconds: float, interval: float = 0.05) -> bool:
    """Poll for seconds seconds (used by assertions about something that "is about to happen", sampling more densely than wait_until)."""
    deadline = time.time() + seconds
    while time.time() < deadline:
        if pred():
            return True
        time.sleep(interval)
    return pred()


# ------------------------------------------------ S2: the forward reset + recovery


def s2_forward_skip(setup: MQClientInstance, admin: DefaultMQAdminExt,
                    broker_addr: str, producer: DefaultMQProducer) -> None:
    print("\n--- S2: the forward reset (timestamp=-1 -> maxOffset) does not redeliver + new messages are still consumed ---")
    listener = SteppingListener()
    # The periodic persist uses the default 5s: in this scenario the first flush (10s after start) has to
    # first establish the record of this group on the broker (Broker2Client.resetOffset replies
    # SYSTEM_ERROR directly for a group with queryOffset==-1), and only then do we wait for a window
    # longer than 5s to do the reset -- the persist of the reset happens at a different moment from
    # the periodic write, so the criterion stays clean.
    c = start_consumer(GROUP2, TOPIC2, listener, 1, 0)
    t_start = time.time()
    time.sleep(2)
    for i in range(MSGS):
        producer.send(Message(TOPIC2, b"rs-%d" % i))

    key = None

    def arranged():
        nonlocal key
        k = consumer_key(c, TOPIC2)
        if k is None:
            return False
        key = k
        return len(listener.batches()) >= 4

    # Release the first 3 entries batch by batch: the 4th entry (offset 3) stays in the listener as the "in-flight batch"
    for n in range(1, 5):
        if not listener.wait_batch(n, 30.0):
            break
        if n < 4:
            listener.release()
    ok = arranged()
    check("S2-window ready: the first 3 are acked and the 4th is stuck in the listener",
          ok and listener.offsets()[:4] == [0, 1, 2, 3], "arrivals=%s" % listener.offsets())
    if not ok or key is None:
        listener.release()
        c.shutdown()
        return

    def broker_is(n):
        rows = committed(setup, GROUP2, TOPIC2)
        return bool(rows) and all(o == n for _, _, o in rows)

    # Wait for the first flush to establish the record of this group (the locally consumed offset 3 = the acks of
    # the first three) and leave a silent window longer than 5s
    ok = wait_until(lambda: time.time() - t_start > 12 and broker_is(3), 20, 0.5)
    check("S2-precondition: the group has a record on the broker (q0:3) and the next periodic write is still more than 5s away", ok,
          fmt_rows(committed(setup, GROUP2, TOPIC2) or []))

    t0 = time.time()
    table = admin.reset_offset_by_timestamp(TOPIC2, GROUP2, -1, True)
    check("S2 the target offset in the 222 response is exactly maxOffset(%d)" % MSGS,
          [v for v in table.values()] == [MSGS],
          "table=%s" % {str(k): v for k, v in table.items()})

    ok = wait_before(lambda: broker_is(MSGS), 2.0)
    elapsed = time.time() - t0
    check("S2 the broker offset jumps forward to %d within %.1fs (the persist that the reset path brings along)" % (elapsed, MSGS), ok,
          fmt_rows(committed(setup, GROUP2, TOPIC2) or []))

    listener.release()
    time.sleep(3)
    check("S2 not one of the skipped 4..%d is delivered (the ack of the in-flight entry is voided too)" % (MSGS - 1),
          listener.offsets() == [0, 1, 2, 3], "arrivals=%s" % listener.offsets())

    producer.send(Message(TOPIC2, b"rs-new"))
    ok = listener.wait_batch(5, 20.0)
    listener.release()
    check("S2 the rebuilt queue continues from the tail (the new message at offset %d is delivered normally)" % MSGS,
          ok and listener.offsets() == [0, 1, 2, 3, MSGS], "arrivals=%s" % listener.offsets())

    # release() only lets the listener return; the ack is written afterwards by the consume thread.
    # Shutting down without waiting for the local offset to really reach 11 means the persist on
    # shutdown may run before the ack (writing back 10 again) -- that is a race in the assertion, not
    # a semantic problem.
    wait_until(lambda: local_offset(c, TOPIC2) == MSGS + 1, 10, 0.05)
    c.shutdown()
    ok = wait_until(lambda: broker_is(MSGS + 1), 15, 0.5)
    check("S2 the persist on shutdown writes the ack of the new message back to the broker (the offset advances to %d)" % (MSGS + 1), ok,
          fmt_rows(committed(setup, GROUP2, TOPIC2) or []))


def cleanup(admin: DefaultMQAdminExt, broker_addr: str) -> None:
    for topic in (TOPIC1, TOPIC2):
        try:
            admin.delete_topic(topic)
        except Exception as e:  # noqa: BLE001
            print("  (cleanup) delete topic %s failed: %s" % (topic, e))
    for group in (GROUP1, GROUP2):
        try:
            admin.delete_subscription_group(broker_addr, group, True)
        except Exception as e:  # noqa: BLE001
            print("  (cleanup) delete group %s failed: %s" % (group, e))


def main() -> int:
    setup = new_client("setup")
    setup.create_topic_in_route(TOPIC1, 1, 1)
    setup.create_topic_in_route(TOPIC2, 1, 1)
    print("TOPIC1=%s TOPIC2=%s (1 queue and %d messages each)" % (TOPIC1, TOPIC2, MSGS))

    admin = DefaultMQAdminExt()
    admin.set_namesrv_addr(NAMESRV)
    admin.set_timeout_millis(10000)
    admin.start()
    broker_addr = wait_broker_addr(admin)
    if broker_addr is None:
        check("cluster liveness", False, "no broker registered on the nameServer")
        admin.shutdown()
        setup.shutdown()
        return 1
    print("broker_addr=%s" % broker_addr)

    producer = DefaultMQProducer("ResetOffsetLive_pg_%d" % STAMP)
    producer.set_namesrv_addr(NAMESRV)
    producer.start()
    time.sleep(1)

    try:
        check("hot-switch the flag: useServerSideResetOffset=false (a precondition of the 220 push path)",
              set_server_side_reset(admin, broker_addr, "false"),
              "read back useServerSideResetOffset=%s"
              % admin.get_broker_config(broker_addr).get("useServerSideResetOffset"))
        s1_backward_reset(setup, admin, broker_addr, producer)
        s2_forward_skip(setup, admin, broker_addr, producer)
        cleanup(admin, broker_addr)
    finally:
        try:
            ok = set_server_side_reset(admin, broker_addr, "true")
            check("restored useServerSideResetOffset=true", ok)
        except Exception as e:  # noqa: BLE001
            check("restored useServerSideResetOffset=true", False, str(e))
        producer.shutdown()
        admin.shutdown()
        setup.shutdown()

    print("\nreset-offset live: %d passed, %d failed" % (PASS, FAIL))
    return 0 if FAIL == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
