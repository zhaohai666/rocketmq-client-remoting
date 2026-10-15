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
TOPIC1 = "OffsetIllegalDrop_%d" % STAMP
GROUP1 = "GID_OffsetIllegalDrop_%d" % STAMP
TOPIC2 = "OffsetIllegalPersist_%d" % STAMP
GROUP2 = "GID_OffsetIllegalPersist_%d" % STAMP
MSGS = 3
ILLEGAL_TARGET = 103

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


class GatedListener(MessageListenerConcurrently):
    """The first batch is held at the gate (keeping up the "1 in-flight + the rest buffered" window); it returns as usual once released."""

    def __init__(self):
        self.batches = []
        self._lock = threading.Lock()
        self.release = threading.Event()

    def consume_message(self, msgs, context):
        with self._lock:
            self.batches.append([m.queue_offset for m in msgs])
        # The upper bound only guards against a deadlock: on the normal path release lets it through explicitly,
        # and it must happen after the correction (see the timing notes in s1)
        self.release.wait(90)
        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS

    def offsets(self):
        with self._lock:
            return [o for batch in self.batches for o in batch]


class RecordingListener(MessageListenerConcurrently):
    def __init__(self):
        self.offsets_ = []
        self._lock = threading.Lock()

    def consume_message(self, msgs, context):
        with self._lock:
            self.offsets_.extend(m.queue_offset for m in msgs)
        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS

    def offsets(self):
        with self._lock:
            return list(self.offsets_)


def consumer_key(c: DefaultMQPushConsumer, topic: str):
    """The queue key of that topic from the consumer's point of view (uses the key of _mq_map instead of assembling brokerName by hand)."""
    with c._lock:
        for key, mq in c._mq_map.items():
            if mq.topic == topic:
                return key
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


def s1_drop_and_rebuild(setup: MQClientInstance, admin: DefaultMQAdminExt,
                        broker_addr: str, producer: DefaultMQProducer) -> None:
    print("\n--- S1: OFFSET_ILLEGAL voids the whole batch of in-flight/buffered messages and rebuilds from the corrected offset ---")
    c = DefaultMQPushConsumer(GROUP1)
    c.set_namesrv_addr(NAMESRV)
    listener = GatedListener()
    c.set_message_listener(listener)
    c.consume_message_batch_max_size = 1
    c.set_consume_from_where(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET)
    c.subscribe(TOPIC1, "*")
    c.start()
    time.sleep(1)
    for i in range(MSGS):
        msg = Message(TOPIC1, b"ilo-%d" % i)
        producer.send(msg)
    print("S1: sent %d messages (single queue), waiting for the listener to hold up the 1st one..." % MSGS)

    state = {}

    def arranged():
        key = consumer_key(c, TOPIC1)
        if key is None:
            return False
        state["key"] = key
        state["pending"] = len(c._pending.get(key, ()))
        state["inflight"] = c._msg_queue_inflight.get(key, 0)
        return state["pending"] == MSGS - 1 and state["inflight"] == 1

    ok = wait_until(arranged, 30, 0.2)
    key = state.get("key")
    check("S1-window ready: 1 in-flight (the listener is held at the gate) + %d left in the buffer" % (MSGS - 1), ok,
          "pending=%s inflight=%s arrivals=%s" % (state.get("pending"), state.get("inflight"),
                                                  listener.offsets()))
    if key is None:
        listener.release.set()
        c.shutdown()
        return

    # The server resets to maxOffset: RPC2 writes resetOffsetTable, and the next pull hits
    # PullMessageProcessor:539-545 where it is short-circuited into OFFSET_RESET
    # (nextBeginOffset = the corrected value)
    admin.reset_offset_by_queue_id(broker_addr, GROUP1, TOPIC1, 0, MSGS)
    print("S1: issued resetOffsetByQueueId(->%d), waiting for the next pull to pick up the server-side reset..." % MSGS)

    ok = wait_until(lambda: c._queue_epoch.get(key, 0) >= 1, 45, 0.2)
    check("S1-after the broker judged the offset illegal this side drops that queue (ProcessQueue.setDropped: the epoch +1)", ok,
          "epoch=%s arrivals=%s" % (c._queue_epoch.get(key, 0), listener.offsets()))

    def committed_is(n):
        rows = committed(setup, GROUP1, TOPIC1)
        return bool(rows) and all(o == n for _, _, o in rows)

    ok = wait_until(lambda: committed_is(MSGS), 20, 0.5)
    rows = committed(setup, GROUP1, TOPIC1) or []
    check("S1-the offset on the broker stays at the corrected value %d (in this scenario the two reset RPCs already wrote it once, so this is a weak assertion)" % MSGS,
          ok, " ".join("q%d:%s" % (mq.queue_id, o) for mq, _, o in rows))

    listener.release.set()
    time.sleep(6)
    arr = listener.offsets()
    check("S1-the %d messages already fetched into the buffer are voided as a whole (messages 1 and 2 are never delivered)" % (MSGS - 1),
          arr and 1 not in arr and 2 not in arr, "arrivals=%s" % arr)

    msg = Message(TOPIC1, b"ilo-after")
    producer.send(msg)
    ok = wait_until(lambda: MSGS in listener.offsets(), 20, 0.5)
    arr = listener.offsets()
    check("S1-the rebuilt queue resumes pulling from the corrected offset (the new message number %d is delivered normally, and the ones already fetched are not redelivered)"
          % MSGS, ok and 1 not in arr and 2 not in arr, "arrivals=%s" % arr)

    ok = wait_until(lambda: committed_is(MSGS + 1), 25, 1.0)
    rows = committed(setup, GROUP1, TOPIC1) or []
    check("S1-the freeze is lifted by the rebuild (the ack of the new message lets the broker offset advance to %d)" % (MSGS + 1),
          ok, "committed=%s" % [o for _, _, o in rows])
    c.shutdown()


def s2_immediate_persist(setup: MQClientInstance, admin: DefaultMQAdminExt,
                         broker_addr: str, producer: DefaultMQProducer) -> None:
    print("\n--- S2: the correction persists the corrected offset immediately (without waiting for the periodic persist) ---")
    listener = RecordingListener()
    c = DefaultMQPushConsumer(GROUP2)
    c.set_namesrv_addr(NAMESRV)
    c.set_message_listener(listener)
    c.set_consume_from_where(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET)
    c.subscribe(TOPIC2, "*")
    c.start()
    time.sleep(1)
    for i in range(MSGS):
        msg = Message(TOPIC2, b"ilo2-%d" % i)
        producer.send(msg)
    ok = wait_until(lambda: len(listener.offsets()) == MSGS, 30, 0.5)
    check("S2-precondition: the consumer first consumes %d messages normally" % MSGS, ok, "arrivals=%s" % listener.offsets())
    # shutdown persists once synchronously (consumer.py:1522); after that the offset on the
    # broker is 3. It has to be stopped first, otherwise its periodic/on-shutdown persist would
    # overwrite the illegal value planted below.
    c.shutdown()
    time.sleep(1)

    rejected = False
    remark = ""
    try:
        admin.reset_offset_by_queue_id(broker_addr, GROUP2, TOPIC2, 0, ILLEGAL_TARGET)
    except Exception as e:  # noqa: BLE001
        rejected = True
        remark = "%s: %s" % (type(e).__name__, str(e)[:140])
    check("S2-precondition: the out-of-range target is rejected by resetOffsetInner (the 2nd RPC)", rejected, remark)

    rows = committed(setup, GROUP2, TOPIC2) or []
    check("S2-precondition: the 1st commitOffset already persisted the illegal offset %d (the two RPCs are not atomic)"
          % ILLEGAL_TARGET,
          bool(rows) and all(o == ILLEGAL_TARGET for _, _, o in rows),
          " ".join("q%d:%s/max%d" % (mq.queue_id, o, m) for mq, m, o in rows))

    c2 = DefaultMQPushConsumer(GROUP2)
    c2.set_namesrv_addr(NAMESRV)
    l2 = RecordingListener()
    c2.set_message_listener(l2)
    # Stretch the periodic persist out to 60s: the only path inside that window that can rewrite the
    # broker offset is the immediate persist that the correction branch brings along
    c2.persist_consumer_offset_interval = 60000
    c2.set_consume_from_where(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET)
    c2.subscribe(TOPIC2, "*")
    t0 = time.time()
    c2.start()
    print("S2: the new consumer pulls from the illegal offset %d, waiting for the correction to write the broker offset back to %d (periodic persist=60s)..."
          % (ILLEGAL_TARGET, MSGS))

    def back_to_max():
        rows_ = committed(setup, GROUP2, TOPIC2)
        return bool(rows_) and all(o == MSGS for _, _, o in rows_)

    ok = wait_until(back_to_max, 20, 0.5)
    elapsed = time.time() - t0
    rows = committed(setup, GROUP2, TOPIC2) or []
    check("S2-the broker offset is corrected from %d back to %d (the persist that the correction branch brings along)" % (ILLEGAL_TARGET, MSGS),
          ok, "elapsed=%.1fs committed=%s" % (elapsed, [o for _, _, o in rows]))

    # Then wait out one silent window: once the offset has been corrected back it neither redelivers
    # 0..2 nor gets rewritten again
    time.sleep(6)
    check("S2-zero deliveries throughout (the corrected offset lands after the historical messages, so not a single one is dispatched)",
          len(l2.offsets()) == 0, "arrivals=%s" % l2.offsets())
    check("S2-the offset is still parked at %d after the silent window" % MSGS, back_to_max(),
          "committed=%s" % [o for _, _, o in committed(setup, GROUP2, TOPIC2) or []])
    c2.shutdown()


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
    print("TOPIC1=%s TOPIC2=%s (1 queue each, %d messages)" % (TOPIC1, TOPIC2, MSGS))

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

    producer = DefaultMQProducer("OffsetIllegalLive_pg_%d" % STAMP)
    producer.set_namesrv_addr(NAMESRV)
    producer.start()
    time.sleep(1)

    try:
        s1_drop_and_rebuild(setup, admin, broker_addr, producer)
        s2_immediate_persist(setup, admin, broker_addr, producer)
        cleanup(admin, broker_addr)
    finally:
        producer.shutdown()
        admin.shutdown()
        setup.shutdown()

    print("\noffset-illegal live: %d passed, %d failed" % (PASS, FAIL))
    return 0 if FAIL == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
