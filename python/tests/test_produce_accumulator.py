# -*- coding: utf-8 -*-
"""ProduceAccumulator（自动攒批）单测。

对应 Java ``client/src/test/java/org/apache/rocketmq/client/producer/ProduceAccumulatorTest.java``
的三个场景（sync / async / 指定 MessageQueue），另补：

* 参数三档校验与"累加器未建时 getter 返回 0"的语义；
* ``try_add_message`` 全局字节闸门（放行即记账、归还、拒绝后调用方直发）；
* 批量应答**拆条**（逗号分隔 msgId/offsetMsgId → 每条各自的 SendResult、queueOffset 递增）；
* ``AggregateKey`` 的四维分区（topic / mq / waitStoreMsgOK / tag）；
* 守卫线程把"已发完的空批次"置 closed 并摘表；
* ``can_batch`` 的四条排除项（延时 / 重试 topic / PGROUP）；
* ``MessageBatch`` 不再进累加器（防无限递归）。

⚠ Java 的单测是**直接调累加器**的 ``send``/``send(msg, callback)``，绕过了
``DefaultMQProducer.sendByAccumulator`` 里的 ``MessageClientIDSetter.setUniqID`` ——
所以两边子消息都没有 UNIQ_KEY，``MessageBatch.encode()`` 出来的 body 才可比。
同理 ``MessageBatch.generateFromList`` 只用 topic + waitStoreMsgOK 组装批对象，
**不含** batch 级 KEYS，而累加器的 ``batch()`` 会写 KEYS（空集合即空串）——
batch 级属性不进 body，所以 body 相等仍然成立。
"""
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


# ---------------------------------------------------------------- 夹具

class FakeProducer:
    """只实现累加器需要的那一个方法（Java 单测里的 ``MockMQProducer`` 同款）。"""

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
    # 与 Java 单测同款 body（1/22/333/4444/55555 字节）
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
    """起一个 **daemon** 线程。

    ⚠ 必须 daemon：不少用例是"多条消息并发 add、攒在同一批里、由主线程手动触发发送"，
    一旦断言提前失败，还阻塞在 ``Condition.wait()`` 里的工作线程就会吊住 pytest 进程
    （非 daemon 线程会挡住解释器退出）。
    """
    thread = threading.Thread(target=target, daemon=True)
    thread.start()
    return thread


# ---------------------------------------------------------------- 参数

def test_default_params_match_java():
    acc = ProduceAccumulator("params")
    assert acc.get_batch_max_delay_ms() == 10
    assert acc.get_batch_max_bytes() == 32 * 1024
    assert acc.total_hold_size == 32 * 1024 * 1024
    # Java 的 getTotalBatchMaxBytes 实际返回 holdSize（上游笔误，照抄）
    assert acc.get_total_batch_max_bytes() == 32 * 1024
    assert acc.currently_hold_size == 0


def test_param_guards_copy_java_ranges():
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
    """Java：累加器为 null 时三个 getter 返回 0，getAutoBatch() 返回 false。"""
    producer = DefaultMQProducer("PG_AccumGetter")
    assert producer.get_batch_max_delay_ms() == 0
    assert producer.get_batch_max_bytes() == 0
    assert producer.get_total_batch_max_bytes() == 0
    assert producer.get_auto_batch() is False
    producer.set_auto_batch(True)
    # 仍然没有累加器 → 依然 false（Java 的 getAutoBatch 有 null 短路）
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
    assert acc is get_or_create_produce_accumulator(client_id)   # 按 clientId 复用
    assert acc.get_batch_max_delay_ms() == 50
    assert acc.get_batch_max_bytes() == 1024
    assert acc.total_hold_size == 4096
    # 累加器建好之后 setter 立刻生效（Java 同）
    producer.set_batch_max_delay_ms(80)
    assert acc.get_batch_max_delay_ms() == 80
    # 再来一个同 clientId 的 producer → 复用同一个累加器
    second = DefaultMQProducer("PG_AccumInit2")
    second.client_id = client_id
    second.init_produce_accumulator()
    assert second.produce_accumulator is acc


def test_try_add_message_gate_and_release():
    acc = ProduceAccumulator("gate")
    acc.total_batch_max_bytes(10)
    msg = Message(TOPIC, b"1234567890")          # 10 字节
    assert acc.try_add_message(msg) is True
    assert acc.currently_hold_size == 10
    # 额度已满（Java：`currentlyHoldSize < totalHoldSize` 才放行）
    assert acc.try_add_message(Message(TOPIC, b"x")) is False
    assert acc.currently_hold_size == 10
    acc.release_hold(10)
    assert acc.currently_hold_size == 0
    assert acc.try_add_message(Message(TOPIC, b"x")) is True
    # 空 body 不记账但仍然放行
    acc.release_hold(1)
    acc.total_batch_max_bytes(5)
    assert acc.try_add_message(Message(TOPIC, b"")) is True
    assert acc.currently_hold_size == 0


# ---------------------------------------------------------------- 归并

def test_sync_batch_merges_messages_and_returns_per_message_results():
    """同步：5 条同键消息攒成 1 个 MessageBatch，各自拿到自己的拆条 SendResult。"""
    producer = FakeProducer(SendResult(
        SendStatus.SEND_OK, "id-0,id-1,id-2,id-3,id-4", MessageQueue(TOPIC, "broker-a", 0),
        100, None, "off-0,off-1,off-2,off-3,off-4"))
    acc = ProduceAccumulator("sync-batch")
    # holdMs 拉长到 3s：保证 5 条都进同一批（测试里手动触发发送，不等这 3s）
    acc.batch_max_delay_ms(3000)

    messages = make_messages(5)
    # 真实调用链里是 ``producer._can_batch`` 先 ``try_add_message`` 记下字节额度，
    # 累加器在批次发完后再归还；这里直接调累加器，就得自己补上记账，否则
    # ``release_hold`` 会把额度减成负数。
    for m in messages:
        assert acc.try_add_message(m) is True
    collected: List[SendResult] = []
    lock = threading.Lock()

    def sender(msg: Message) -> None:
        result = acc.send(msg, producer)
        with lock:
            collected.append(result)

    threads = [start_daemon(lambda m=m: sender(m)) for m in messages]
    # 等 5 条都进批（它们此时都阻塞在 add 里等阈值）
    batch = wait_until(lambda: len(acc.sync_send_batches_snapshot()) == 1 and
                       acc.sync_send_batches_snapshot()[0].count == 5, timeout=3.0)
    assert batch is True
    the_batch = acc.sync_send_batches_snapshot()[0]
    assert the_batch.send_callbacks == []
    assert the_batch.keys == set()

    # 手动触发（等价于守卫线程在 holdMs 到点后叫醒某一个等待者去发）
    with the_batch.cond:
        the_batch._send_sync()
    for t in threads:
        t.join(timeout=5)
        assert not t.is_alive()

    assert len(producer.sent) == 1
    sent_msg, sent_mq, sent_cb = producer.sent[0]
    assert isinstance(sent_msg, MessageBatch)
    assert sent_mq is None                      # 没指定 mq → 由 producer 轮询选
    assert sent_cb is None                      # 同步路径没有回调
    # 子消息集合与 reference 一致。⚠ 只比长度不比字节：5 条是**不同线程**并发 add 的，
    # 批内顺序不确定（Java 的同步用例也因此只断言长度，只有单线程的异步用例才全等比较）。
    assert len(sent_msg.body) == len(reference_batch_body(messages))

    assert len(collected) == 5
    # 拆条结果是**按批内位置**下发的，而位置由 add 的先后决定 —— 5 条消息是并发 add 的，
    # 「哪条消息拿哪个下标」不确定（Java 的单测同样只能断言"成套"关系，不能按完成顺序对号）。
    # 所以这里验两件事：① 五条 MsgId 一个不少；② 同一条结果里 id-N ↔ off-N ↔ 100+N 必须
    # 成套（串了就说明 split_send_results 的下标算错了）。
    assert sorted(r.msg_id for r in collected) == ["id-%d" % i for i in range(5)]
    for result in collected:
        position = int(result.msg_id.split("-")[1])
        assert result.offset_msg_id == "off-%d" % position
        assert result.queue_offset == 100 + position
    # 发完归还全局额度
    assert acc.currently_hold_size == 0


def test_async_batch_merges_and_fires_every_callback():
    """异步：5 条同键消息由一个守卫线程攒成 1 批，回调各自拿到拆条结果。"""
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

        # 异步批次靠**守卫线程**唤醒：每 max(1, holdMs/2) ms 扫一遍，readyToSend 就发
        assert wait_until(lambda: all(cb.total() == 1 for cb in callbacks), timeout=5.0)
    finally:
        acc.shutdown()

    assert all(not cb.errors for cb in callbacks)
    assert [cb.results[0].msg_id for cb in callbacks] == ["a", "b", "c", "d", "e"]
    assert [cb.results[0].queue_offset for cb in callbacks] == [7, 8, 9, 10, 11]

    assert len(producer.sent) == 1
    sent_msg, _, sent_cb = producer.sent[0]
    assert isinstance(sent_msg, MessageBatch)
    assert sent_cb is not None                  # 异步路径回调不为 None
    # 单线程依次 add → 批内顺序确定，可以逐字节比
    assert sent_msg.body == reference_batch_body(messages)
    # 异步批次**不**收集 keys（Java 的不对称行为），所以 batch 级 KEYS 为空串
    assert sent_msg.get_keys() == ""
    assert acc.currently_hold_size == 0


def test_skip_results_are_shared_when_msg_id_has_no_comma():
    """老 broker / 单条应答：msgId 不含逗号时所有下标共享同一个 SendResult 对象。"""
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
    """指定 mq（Java 的 send(msg, mq, producer)）：mq 原样透传给 sendDirect。"""
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

    # 异步 + 指定 mq
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
    """同步批次的 KEYS = 全体子消息 keys 的并集，空格 join（MessageConst.KEY_SEPARATOR）。"""
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
    # 同样只比长度（两条消息是两个线程并发 add 的，批内顺序不定）
    assert len(producer.sent[0][0].body) == len(reference_batch_body([m1, m2]))


# ---------------------------------------------------------------- 分区键

def test_aggregate_key_partitions_by_topic_mq_wait_and_tag():
    mq = MessageQueue(TOPIC, "b", 0)
    base = Message(TOPIC, b"x", tags="TagA")
    assert AggregateKey.of_message(base) == AggregateKey.of_message(Message(TOPIC, b"y", tags="TagA"))
    assert AggregateKey.of_message(base) != AggregateKey.of_message(Message(TOPIC, b"y", tags="TagB"))
    assert AggregateKey.of_message(base) != AggregateKey.of_message(Message("Other", b"y", tags="TagA"))
    assert AggregateKey.of_message(base) != AggregateKey.of_message_with_mq(base, mq)
    assert AggregateKey.of_message_with_mq(base, mq) == AggregateKey.of_message_with_mq(
        Message(TOPIC, b"z", tags="TagA"), mq)
    # waitStoreMsgOK 参与分区
    other_wait = Message(TOPIC, b"y", tags="TagA")
    other_wait.set_wait_store_msg_ok(False)
    assert AggregateKey.of_message(base) != AggregateKey.of_message(other_wait)
    # 回归：**没设过 WAIT** 的普通消息要按 Java 的「缺省即 true」归类，不能落进 false 那一档
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


# ---------------------------------------------------------------- 守卫线程

def test_guard_keeps_closed_batch_until_next_send():
    """守卫线程的清理口径（Java ``GuardForSyncSendService.doWork``）：**只**清理
    ``messagesSize == 0`` 的批次。

    ⚠ 发完的批次 ``messagesSize`` 仍 > 0（``send()`` 只置 ``closed``，不重置
    ``messagesSize``）—— 所以它会**留在表里**，直到下一次同键 ``send`` 拿到它、
    ``add`` 返回 -1 才被摘掉重取。别把"发完即摘表"当成 Java 行为。
    """
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
        # 全局额度已归还（finally 里 currentlyHoldSize -= messagesSize）
        assert acc.currently_hold_size == 0

        # 下一次同键发送：摘掉 closed 批次 → 新建 → 再发一批
        second = make_messages(1)[0]
        assert acc.try_add_message(second) is True
        acc.send(second, producer)
        assert len(producer.sent) == 2
        assert acc.currently_hold_size == 0
    finally:
        acc.shutdown()


def test_accumulator_can_restart_after_shutdown():
    """守卫线程必须能**重建**：累加器按 clientId 复用，同一个实例会经历
    ``start → shutdown → start``（生产者重启；Java 的 ``ServiceThread`` 同样可重复 start）。

    ⚠ 回归守卫：守卫原先继承 ``threading.Thread``，而 ``Thread`` 只能 ``start()`` 一次 ——
    第二次会抛 ``RuntimeError: threads can only be started once``。
    """
    acc = ProduceAccumulator("restart")
    acc.start()
    acc.shutdown()
    try:
        acc.start()
        producer = FakeProducer()
        msg = make_messages(1)[0]
        assert acc.try_add_message(msg) is True
        acc.send(msg, producer)     # 靠重启后的守卫线程叫醒并发送
        assert len(producer.sent) == 1
    finally:
        acc.shutdown()


def test_guard_removes_empty_batch_without_sending():
    """没人真的加消息的空批次：守卫线程直接置 closed + 摘表，不发任何东西。"""
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

    # ⚠ 被拒的四条**不归还**已经在闸门里记下的字节数（Java 遗漏，照抄）
    assert acc.currently_hold_size > 0


class StubProducer(DefaultMQProducer):
    """覆写 ``send_direct`` 的真 producer：既能走 ``send_by_accumulator`` 的本地校验，
    又不真的发网络请求。"""

    def __init__(self, group: str):
        super().__init__(group)
        self.sent: List[tuple] = []

    def send_direct(self, msg, mq, send_callback):
        self.sent.append((msg, mq, send_callback))
        return SendResult(SendStatus.SEND_OK, "stub-id", mq, 0)


def test_send_by_accumulator_stamps_uniq_id_then_accumulates():
    """``send_by_accumulator`` 先过本地校验、补 UNIQ_KEY，再交给累加器（Java :778-793）。"""
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
    # 攒批路径已经给这条消息打上 UNIQ_KEY（否则批量体的每条子消息都没有客户端 ID）
    assert get_uniq_id(msg)

    batch = acc.sync_send_batches_snapshot()[0]
    with batch.cond:
        batch._send_sync()
    thread.join(timeout=5)
    assert not thread.is_alive()
    assert len(stub.sent) == 1
    assert isinstance(stub.sent[0][0], MessageBatch)


def test_message_batch_never_enters_accumulator():
    """batch 消息不再攒批（Java ``!(msg instanceof MessageBatch)``）→ 直接进直发路径。"""
    producer = DefaultMQProducer("PG_NoRecurse")
    producer.client_id = "accum-norecurse-%d" % time.time_ns()
    producer.init_produce_accumulator()
    producer.set_auto_batch(True)
    assert producer.get_auto_batch() is True

    batch = MessageBatch.generate_from_list([Message(TOPIC, b"a")])
    # 没 start 过 → 直发路径第一件事就是 _require_client()，抛"not started"；
    # 若它进了累加器，异常会变成累加器内部的行为（且不会是这个文案）。
    with pytest.raises(MQClientException) as exc:
        producer.send(batch)
    assert "not started" in str(exc.value)
