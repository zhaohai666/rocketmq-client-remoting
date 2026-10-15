# -*- coding: utf-8 -*-
from __future__ import annotations

import threading
import time
from typing import List, Optional

import pytest

from client.exception import MQClientException
from client.produce_accumulator import (AggregateKey, ProduceAccumulator,
                                                get_or_create_produce_accumulator)
from client.producer import DefaultMQProducer, SendCallback
from client.send_result import SendResult, SendStatus
from common.message import Message, MessageBatch, MessageQueue

TOPIC = "AccumTestTopic"


# ---------------------------------------------------------------- Fixtures

class FakeProducer:
    pass

    def __init__(self, result: Optional[SendResult] = None):
        self.sent: List[tuple] = []
        self._result = result
        self._lock = threading.Lock()

    def send_direct(self, msg, mq, send_callback):
        with self._lock:
            self.sent.append((msg, mq, send_callback))
        result = self._result
        if result is None:
            result = SendResult(SendStatus.SEND_OK, "123", mq, 0)
        if send_callback is not None:
            send_callback.on_success(result)
        return result


class CollectingCallback(SendCallback):
    def __init__(self):
        self.results: List[SendResult] = []
        self.errors: List[BaseException] = []
        self._lock = threading.Lock()

    def on_success(self, send_result: SendResult) -> None:
        with self._lock:
            self.results.append(send_result)

    def on_exception(self, e: BaseException) -> None:
        with self._lock:
            self.errors.append(e)

    def total(self) -> int:
        with self._lock:
            return len(self.results) + len(self.errors)


def make_messages(n: int = 5) -> List[Message]:
    return [Message(TOPIC, b"1" * (i + 1)) for i in range(n)]


def reference_batch_body(messages: List[Message]) -> bytes:
    return MessageBatch.generate_from_list(messages).encode()


def wait_until(predicate, timeout: float = 5.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.01)
    return predicate()


def start_daemon(target) -> threading.Thread:
    """Start a **daemon** thread.

    ⚠ Must be daemon: many tests are "multiple messages added concurrently, gathered in the same batch, with the send manually triggered by the main thread";
    if an assertion fails early, worker threads still blocked in ``Condition.wait()`` would hang the pytest process
    (non-daemon threads block interpreter exit).
    """
    thread = threading.Thread(target=target, daemon=True)
    thread.start()
    return thread


# ---------------------------------------------------------------- Parameters

def test_default_params_match_the_producer_contract():
    acc = ProduceAccumulator("params")
    assert acc.get_batch_max_delay_ms() == 10
    assert acc.get_batch_max_bytes() == 32 * 1024
    assert acc.total_hold_size == 32 * 1024 * 1024
    assert acc.get_total_batch_max_bytes() == 32 * 1024
    assert acc.currently_hold_size == 0


def test_param_guards_use_the_documented_ranges():
    acc = ProduceAccumulator("guards")
    acc.batch_max_delay_ms(1)
    acc.batch_max_delay_ms(30 * 1000)
    with pytest.raises(ValueError):
        acc.batch_max_delay_ms(0)
    with pytest.raises(ValueError):
        acc.batch_max_delay_ms(30 * 1000 + 1)

    acc.batch_max_bytes(1)
    acc.batch_max_bytes(2 * 1024 * 1024)
    with pytest.raises(ValueError):
        acc.batch_max_bytes(0)
    with pytest.raises(ValueError):
        acc.batch_max_bytes(2 * 1024 * 1024 + 1)

    acc.total_batch_max_bytes(1)
    with pytest.raises(ValueError):
        acc.total_batch_max_bytes(0)


def test_producer_getters_return_zero_before_accumulator_exists():
    pass
    producer = DefaultMQProducer("PG_AccumGetter")
    assert producer.get_batch_max_delay_ms() == 0
    assert producer.get_batch_max_bytes() == 0
    assert producer.get_total_batch_max_bytes() == 0
    assert producer.get_auto_batch() is False
    producer.set_auto_batch(True)
    assert producer.get_auto_batch() is False


def test_init_produce_accumulator_syncs_thresholds_and_reuses_by_client_id():
    client_id = "accum-reuse-%d" % time.time_ns()
    producer = DefaultMQProducer("PG_AccumInit")
    producer.client_id = client_id
    producer.set_batch_max_delay_ms(50)
    producer.set_batch_max_bytes(1024)
    producer.set_total_batch_max_bytes(4096)
    producer.init_produce_accumulator()

    acc = producer.produce_accumulator
    assert acc is get_or_create_produce_accumulator(client_id)   # reused by clientId
    assert acc.get_batch_max_delay_ms() == 50
    assert acc.get_batch_max_bytes() == 1024
    assert acc.total_hold_size == 4096
    producer.set_batch_max_delay_ms(80)
    assert acc.get_batch_max_delay_ms() == 80
    # Another producer with the same clientId → reuses the same accumulator
    second = DefaultMQProducer("PG_AccumInit2")
    second.client_id = client_id
    second.init_produce_accumulator()
    assert second.produce_accumulator is acc


def test_try_add_message_gate_and_release():
    acc = ProduceAccumulator("gate")
    acc.total_batch_max_bytes(10)
    msg = Message(TOPIC, b"1234567890")          # 10 bytes
    assert acc.try_add_message(msg) is True
    assert acc.currently_hold_size == 10
    assert acc.try_add_message(Message(TOPIC, b"x")) is False
    assert acc.currently_hold_size == 10
    acc.release_hold(10)
    assert acc.currently_hold_size == 0
    assert acc.try_add_message(Message(TOPIC, b"x")) is True
    # An empty body is not accounted for but is still let through
    acc.release_hold(1)
    acc.total_batch_max_bytes(5)
    assert acc.try_add_message(Message(TOPIC, b"")) is True
    assert acc.currently_hold_size == 0


# ---------------------------------------------------------------- Merging

def test_sync_batch_merges_messages_and_returns_per_message_results():
    """Sync: 5 same-key messages gather into 1 MessageBatch, each getting its own split SendResult."""
    producer = FakeProducer(SendResult(
        SendStatus.SEND_OK, "id-0,id-1,id-2,id-3,id-4", MessageQueue(TOPIC, "broker-a", 0),
        100, None, "off-0,off-1,off-2,off-3,off-4"))
    acc = ProduceAccumulator("sync-batch")
    # holdMs stretched to 3s: ensures all 5 enter the same batch (the test triggers the send manually, without waiting the 3s)
    acc.batch_max_delay_ms(3000)

    messages = make_messages(5)
    # In the real call chain, ``producer._can_batch`` first calls ``try_add_message`` to record the byte quota,
    # and the accumulator returns it after the batch is sent; calling the accumulator directly here means the accounting must be done manually, otherwise
    # ``release_hold`` would drive the quota negative.
    for m in messages:
        assert acc.try_add_message(m) is True
    collected: List[SendResult] = []
    lock = threading.Lock()

    def sender(msg: Message) -> None:
        result = acc.send(msg, producer)
        with lock:
            collected.append(result)

    threads = [start_daemon(lambda m=m: sender(m)) for m in messages]
    # Wait for all 5 to enter the batch (they are all blocked in add waiting for the threshold)
    batch = wait_until(lambda: len(acc.sync_send_batches_snapshot()) == 1 and
                       acc.sync_send_batches_snapshot()[0].count == 5, timeout=3.0)
    assert batch is True
    the_batch = acc.sync_send_batches_snapshot()[0]
    assert the_batch.send_callbacks == []
    assert the_batch.keys == set()

    # Manually trigger (equivalent to the guard thread waking one waiter to send after holdMs elapses)
    with the_batch.cond:
        the_batch._send_sync()
    for t in threads:
        t.join(timeout=5)
        assert not t.is_alive()

    assert len(producer.sent) == 1
    sent_msg, sent_mq, sent_cb = producer.sent[0]
    assert isinstance(sent_msg, MessageBatch)
    assert sent_mq is None                      # no mq specified → selected by the producer's round-robin
    assert sent_cb is None                      # the sync path has no callback
    assert len(sent_msg.body) == len(reference_batch_body(messages))

    assert len(collected) == 5
    assert sorted(r.msg_id for r in collected) == ["id-%d" % i for i in range(5)]
    for result in collected:
        position = int(result.msg_id.split("-")[1])
        assert result.offset_msg_id == "off-%d" % position
        assert result.queue_offset == 100 + position
    # return the global quota after sending
    assert acc.currently_hold_size == 0


def test_async_batch_merges_and_fires_every_callback():
    """Async: 5 same-key messages are gathered into 1 batch by a guard thread, and each callback gets its own split result."""
    producer = FakeProducer(SendResult(
        SendStatus.SEND_OK, "a,b,c,d,e", MessageQueue(TOPIC, "broker-a", 0), 7, None, "p,q,r,s,t"))
    acc = ProduceAccumulator("async-batch")
    acc.start()
    callbacks = [CollectingCallback() for _ in range(5)]
    messages = make_messages(5)
    try:
        for m in messages:
            assert acc.try_add_message(m) is True
        for msg, cb in zip(messages, callbacks):
            acc.send_async(msg, cb, producer)

        # Async batches are woken by the **guard thread**: it scans every max(1, holdMs/2) ms and sends when readyToSend
        assert wait_until(lambda: all(cb.total() == 1 for cb in callbacks), timeout=5.0)
    finally:
        acc.shutdown()

    assert all(not cb.errors for cb in callbacks)
    assert [cb.results[0].msg_id for cb in callbacks] == ["a", "b", "c", "d", "e"]
    assert [cb.results[0].queue_offset for cb in callbacks] == [7, 8, 9, 10, 11]

    assert len(producer.sent) == 1
    sent_msg, _, sent_cb = producer.sent[0]
    assert isinstance(sent_msg, MessageBatch)
    assert sent_cb is not None                  # the async path's callback is not None
    # Single-threaded sequential add -> the in-batch order is deterministic, so a byte-by-byte comparison is possible
    assert sent_msg.body == reference_batch_body(messages)
    assert sent_msg.get_keys() == ""
    assert acc.currently_hold_size == 0


def test_skip_results_are_shared_when_msg_id_has_no_comma():
    """Old broker / single-message reply: when msgId contains no comma, all indexes share the same SendResult object."""
    shared = SendResult(SendStatus.SEND_OK, "single-id", MessageQueue(TOPIC, "b", 0), 3)
    producer = FakeProducer(shared)
    acc = ProduceAccumulator("shared-result")
    acc.batch_max_delay_ms(3000)
    messages = make_messages(3)
    results: List[SendResult] = []
    lock = threading.Lock()

    def sender(msg):
        result = acc.send(msg, producer)
        with lock:
            results.append(result)

    threads = [start_daemon(lambda m=m: sender(m)) for m in messages]
    assert wait_until(lambda: acc.sync_send_batches_snapshot() and
                      acc.sync_send_batches_snapshot()[0].count == 3, timeout=3.0)
    the_batch = acc.sync_send_batches_snapshot()[0]
    with the_batch.cond:
        the_batch._send_sync()
    for t in threads:
        t.join(timeout=5)

    assert len(results) == 3
    assert all(r is shared for r in results)


def test_send_with_message_queue_pins_the_batch():
    pass
    mq = MessageQueue(TOPIC, "broker-pinned", 2)
    producer = FakeProducer()
    acc = ProduceAccumulator("pinned")
    acc.batch_max_delay_ms(3000)

    messages = make_messages(2)
    threads = [start_daemon(lambda m=m: acc.send_with_mq(m, mq, producer))
               for m in messages]
    assert wait_until(lambda: acc.sync_send_batches_snapshot() and
                      acc.sync_send_batches_snapshot()[0].count == 2, timeout=3.0)
    the_batch = acc.sync_send_batches_snapshot()[0]
    with the_batch.cond:
        the_batch._send_sync()
    for t in threads:
        t.join(timeout=5)

    assert len(producer.sent) == 1
    assert producer.sent[0][1] == mq

    # async + specified mq
    producer2 = FakeProducer()
    acc2 = ProduceAccumulator("pinned-async")
    acc2.start()
    cb = CollectingCallback()
    msg = make_messages(1)[0]
    try:
        assert acc2.try_add_message(msg) is True
        acc2.send_async_with_mq(msg, mq, cb, producer2)
        assert wait_until(lambda: cb.total() == 1, timeout=5.0)
    finally:
        acc2.shutdown()
    assert not cb.errors
    assert len(producer2.sent) == 1
    assert producer2.sent[0][1] == mq


def test_batch_merges_keys_with_space_separator():
    """The sync batch's KEYS = the union of all sub-message keys, space-joined (MessageConst.KEY_SEPARATOR)."""
    producer = FakeProducer()
    acc = ProduceAccumulator("keys")
    acc.batch_max_delay_ms(3000)
    m1 = Message(TOPIC, b"aa", keys="k1 k2")
    m2 = Message(TOPIC, b"bbb", keys="k2 k3")
    threads = [start_daemon(lambda m=m: acc.send(m, producer)) for m in (m1, m2)]
    assert wait_until(lambda: acc.sync_send_batches_snapshot() and
                      acc.sync_send_batches_snapshot()[0].count == 2, timeout=3.0)
    the_batch = acc.sync_send_batches_snapshot()[0]
    with the_batch.cond:
        the_batch._send_sync()
    for t in threads:
        t.join(timeout=5)

    keys = producer.sent[0][0].get_keys()
    assert set(keys.split(" ")) == {"k1", "k2", "k3"}
    # Only the length is compared here too (the two messages are added concurrently by two threads, so the in-batch order is indeterminate)
    assert len(producer.sent[0][0].body) == len(reference_batch_body([m1, m2]))


# ---------------------------------------------------------------- Partition key

def test_aggregate_key_partitions_by_topic_mq_wait_and_tag():
    mq = MessageQueue(TOPIC, "b", 0)
    base = Message(TOPIC, b"x", tags="TagA")
    assert AggregateKey.of_message(base) == AggregateKey.of_message(Message(TOPIC, b"y", tags="TagA"))
    assert AggregateKey.of_message(base) != AggregateKey.of_message(Message(TOPIC, b"y", tags="TagB"))
    assert AggregateKey.of_message(base) != AggregateKey.of_message(Message("Other", b"y", tags="TagA"))
    assert AggregateKey.of_message(base) != AggregateKey.of_message_with_mq(base, mq)
    assert AggregateKey.of_message_with_mq(base, mq) == AggregateKey.of_message_with_mq(
        Message(TOPIC, b"z", tags="TagA"), mq)
    # waitStoreMsgOK participates in partitioning
    other_wait = Message(TOPIC, b"y", tags="TagA")
    other_wait.set_wait_store_msg_ok(False)
    assert AggregateKey.of_message(base) != AggregateKey.of_message(other_wait)
    plain = Message(TOPIC, b"y", tags="TagA")
    assert plain.get_wait_store_msg_ok() is None
    assert AggregateKey.of_message(plain).wait_store_msg_ok is True
    assert AggregateKey.of_message_with_mq(plain, mq).wait_store_msg_ok is True


def test_different_tags_do_not_merge():
    producer = FakeProducer()
    acc = ProduceAccumulator("tags")
    acc.batch_max_delay_ms(3000)
    m1 = Message(TOPIC, b"aa", tags="TagA")
    m2 = Message(TOPIC, b"bb", tags="TagB")
    t1 = start_daemon(lambda: acc.send(m1, producer))
    t2 = start_daemon(lambda: acc.send(m2, producer))
    assert wait_until(lambda: len(acc.sync_send_batches_snapshot()) == 2
                      and all(b.count == 1 for b in acc.sync_send_batches_snapshot()),
                      timeout=3.0)
    for batch in acc.sync_send_batches_snapshot():
        with batch.cond:
            batch._send_sync()
    t1.join(timeout=5)
    t2.join(timeout=5)
    assert len(producer.sent) == 2
    tags = sorted(m.get_tags() for m, _, _ in producer.sent)
    assert tags == ["TagA", "TagB"]


# ---------------------------------------------------------------- Guard thread

def test_guard_keeps_closed_batch_until_next_send():
    pass
    producer = FakeProducer()
    acc = ProduceAccumulator("guard-keep")
    acc.start()
    try:
        first = make_messages(1)[0]
        assert acc.try_add_message(first) is True
        acc.send(first, producer)
        snapshot = acc.sync_send_batches_snapshot()
        assert len(snapshot) == 1
        assert snapshot[0].closed is True
        assert snapshot[0].messages_size > 0
        # the global quota has been returned (currentlyHoldSize -= messagesSize in finally)
        assert acc.currently_hold_size == 0

        # The next same-key send: drop the closed batch -> create a new one -> send another batch
        second = make_messages(1)[0]
        assert acc.try_add_message(second) is True
        acc.send(second, producer)
        assert len(producer.sent) == 2
        assert acc.currently_hold_size == 0
    finally:
        acc.shutdown()


def test_accumulator_can_restart_after_shutdown():
    pass
    acc = ProduceAccumulator("restart")
    acc.start()
    acc.shutdown()
    try:
        acc.start()
        producer = FakeProducer()
        msg = make_messages(1)[0]
        assert acc.try_add_message(msg) is True
        acc.send(msg, producer)     # woken and sent by the restarted guard thread
        assert len(producer.sent) == 1
    finally:
        acc.shutdown()


def test_guard_removes_empty_batch_without_sending():
    """An empty batch that nobody actually adds messages to: the guard thread directly sets closed + removes it from the table, sending nothing."""
    producer = FakeProducer()
    acc = ProduceAccumulator("guard-empty")
    from client.produce_accumulator import MessageAccumulation
    key = AggregateKey.of_message(Message(TOPIC, b"x"))
    empty = MessageAccumulation(key, producer, acc)
    with acc._table_lock:
        acc._sync_send_batches[key] = empty
    acc._guard_sync.do_work()
    assert empty.closed is True
    assert acc.sync_send_batches_snapshot() == []
    assert producer.sent == []


# ---------------------------------------------------------------- can_batch

def test_can_batch_rejects_delay_retry_and_pgroup():
    producer = DefaultMQProducer("PG_CanBatch")
    acc = ProduceAccumulator("can-batch")
    acc.total_batch_max_bytes(1024 * 1024)
    producer.produce_accumulator = acc

    assert producer._can_batch(Message(TOPIC, b"hello")) is True

    delayed = Message(TOPIC, b"hello")
    delayed.set_delay_time_level(3)
    assert producer._can_batch(delayed) is False

    timer = Message(TOPIC, b"hello")
    timer.put_property("TIMER_DELAY_MS", "100")
    assert producer._can_batch(timer) is False

    deliver = Message(TOPIC, b"hello")
    deliver.put_property("TIMER_DELIVER_MS", "100")
    assert producer._can_batch(deliver) is False

    retry = Message("%RETRY%GID_x", b"hello")
    assert producer._can_batch(retry) is False

    grouped = Message(TOPIC, b"hello")
    grouped.put_property("PGROUP", "PG_x")
    assert producer._can_batch(grouped) is False

    assert acc.currently_hold_size > 0


class StubProducer(DefaultMQProducer):
    """A real producer overriding ``send_direct``: it goes through ``send_by_accumulator``'s local validation
    without actually sending a network request."""

    def __init__(self, group: str):
        super().__init__(group)
        self.sent: List[tuple] = []

    def send_direct(self, msg, mq, send_callback):
        self.sent.append((msg, mq, send_callback))
        return SendResult(SendStatus.SEND_OK, "stub-id", mq, 0)


def test_send_by_accumulator_stamps_uniq_id_then_accumulates():
    pass
    from common.message_client_id_setter import get_uniq_id

    acc = ProduceAccumulator("send-by-accum")
    acc.batch_max_delay_ms(3000)
    stub = StubProducer("PG_AccumSend")
    stub.produce_accumulator = acc

    msg = Message(TOPIC, b"stamped")
    assert not get_uniq_id(msg)

    thread = start_daemon(lambda: stub.send_by_accumulator(msg, None, None))
    assert wait_until(lambda: acc.sync_send_batches_snapshot() and
                      acc.sync_send_batches_snapshot()[0].count == 1, timeout=3.0)
    # The batching path has already stamped UNIQ_KEY on this message (otherwise every sub-message of the batch body would lack a client ID)
    assert get_uniq_id(msg)

    batch = acc.sync_send_batches_snapshot()[0]
    with batch.cond:
        batch._send_sync()
    thread.join(timeout=5)
    assert not thread.is_alive()
    assert len(stub.sent) == 1
    assert isinstance(stub.sent[0][0], MessageBatch)


def test_message_batch_never_enters_accumulator():
    pass
    producer = DefaultMQProducer("PG_NoRecurse")
    producer.client_id = "accum-norecurse-%d" % time.time_ns()
    producer.init_produce_accumulator()
    producer.set_auto_batch(True)
    assert producer.get_auto_batch() is True

    batch = MessageBatch.generate_from_list([Message(TOPIC, b"a")])
    # Not started -> the direct-send path's first act is _require_client(), throwing "not started";
    # if it entered the accumulator, the exception would become the accumulator's internal behavior (and would not be this wording).
    with pytest.raises(MQClientException) as exc:
        producer.send(batch)
    assert "not started" in str(exc.value)
