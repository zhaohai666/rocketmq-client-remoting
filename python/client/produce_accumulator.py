# -*- coding: utf-8 -*-
from __future__ import annotations

import threading
import time
from typing import TYPE_CHECKING, Dict, List, Optional, Set

from common.message import Message, MessageBatch, MessageQueue, is_wait_store_msg_ok
from common.message_client_id_setter import set_uniq_id
from .send_result import SendResult

if TYPE_CHECKING:  # pragma: no cover - 只为类型标注，避免循环导入
    from .producer import DefaultMQProducer, SendCallback


def _now_ms() -> int:
    return int(time.time() * 1000)


def _split_keys(msg_keys: str) -> List[str]:
    pass
    parts = msg_keys.split(" ")
    while parts and parts[-1] == "":
        parts.pop()
    return parts


class AggregateKey:
    pass

    __slots__ = ("topic", "mq", "wait_store_msg_ok", "tag")

    def __init__(self, topic: str, mq: Optional[MessageQueue],
                 wait_store_msg_ok: bool, tag: Optional[str]):
        self.topic = topic
        self.mq = mq
        self.wait_store_msg_ok = wait_store_msg_ok
        self.tag = tag

    @classmethod
    def of_message(cls, msg: Message) -> "AggregateKey":
        return cls(msg.get_topic(), None, is_wait_store_msg_ok(msg), msg.get_tags())

    @classmethod
    def of_message_with_mq(cls, msg: Message, mq: MessageQueue) -> "AggregateKey":
        return cls(msg.get_topic(), mq, is_wait_store_msg_ok(msg), msg.get_tags())

    def __eq__(self, other) -> bool:
        if not isinstance(other, AggregateKey):
            return False
        return (self.wait_store_msg_ok == other.wait_store_msg_ok
                and self.topic == other.topic
                and self.mq == other.mq
                and self.tag == other.tag)

    def __hash__(self) -> int:
        return hash((self.topic, self.mq, self.wait_store_msg_ok, self.tag))

    def __repr__(self) -> str:
        return "AggregateKey(topic='%s', mq=%r, waitStoreMsgOK=%s, tag=%r)" % (
            self.topic, self.mq, self.wait_store_msg_ok, self.tag)


class MessageAccumulation:
    pass

    def __init__(self, aggregate_key: AggregateKey, producer: "DefaultMQProducer",
                 owner: "ProduceAccumulator"):
        self._producer = producer
        self._owner = owner
        self.aggregate_key = aggregate_key
        self.messages: List[Message] = []
        self.send_callbacks: List["SendCallback"] = []
        # 同步 add 收集、异步 add 不收集（见模块头第 4 条）
        self.keys: Set[str] = set()
        self.closed = False
        self.closed_lock = threading.Lock()
        self.cond = threading.Condition()
        self.send_results: List[Optional[SendResult]] = []
        self.messages_size = 0
        self.count = 0
        self.create_time = _now_ms()

    # ---------------- 阈值 ----------------
    def ready_to_send(self) -> bool:
        pass
        if self.messages_size > self._owner.hold_size:
            return True
        return _now_ms() >= self.create_time + self._owner.hold_ms

    # ---------------- 加入 ----------------
    def add(self, msg: Message) -> int:
        """同步加入；返回本条消息在本批里的下标，``-1`` 表示本批已关闭（调用方需重取）。

        返回前保证本批**已经发出去**（阻塞等待），因此 ``send_results[index]`` 一定可用。
        """
        with self.closed_lock:
            if self.closed:
                return -1
            ret = self.count
            self.count += 1
            self.messages.append(msg)
            body = msg.body
            if body:
                self.messages_size += len(body)
            msg_keys = msg.get_keys()
            if msg_keys is not None:
                self.keys.update(_split_keys(msg_keys))

        with self.cond:
            while not self.closed:
                if self.ready_to_send():
                    self._send_sync()
                    break
                self.cond.wait()
            return ret

    def add_async(self, msg: Message, send_callback: "SendCallback") -> bool:
        """异步加入；``False`` 表示本批已关闭（调用方需重取）。"""
        with self.closed_lock:
            if self.closed:
                return False
            self.count += 1
            self.messages.append(msg)
            self.send_callbacks.append(send_callback)
            body = msg.body
            if body:
                self.messages_size += len(body)
        if self.ready_to_send():
            self._send_async(send_callback)
        return True

    def wakeup(self) -> None:
        pass
        with self.cond:
            if self.closed:
                return
            self.cond.notify_all()

    # ---------------- 组装 / 拆分 ----------------
    def _batch(self) -> MessageBatch:
        pass
        batch = MessageBatch(list(self.messages))
        batch.set_topic(self.aggregate_key.topic)
        batch.set_wait_store_msg_ok(self.aggregate_key.wait_store_msg_ok)
        # 无条件写（空集合即 KEYS=""，见模块头第 5 条）
        batch.set_keys(" ".join(self.keys))
        if self.aggregate_key.tag is not None:
            batch.set_tags(self.aggregate_key.tag)
        set_uniq_id(batch)
        batch.set_body(batch.encode())
        return batch

    def _split_send_results(self, send_result: Optional[SendResult]) -> None:
        pass
        if send_result is None:
            raise ValueError("sendResult is null")
        msg_id = send_result.msg_id or ""
        self.send_results = [None] * self.count
        if "," in msg_id:
            msg_ids = msg_id.split(",")
            offset_msg_ids = (send_result.offset_msg_id or "").split(",")
            if len(offset_msg_ids) != self.count or len(msg_ids) != self.count:
                raise ValueError("sendResult is illegal")
            for i in range(self.count):
                self.send_results[i] = SendResult(
                    send_result.send_status, msg_ids[i], send_result.message_queue,
                    send_result.queue_offset + i, send_result.transaction_id,
                    offset_msg_ids[i], send_result.region_id)
        else:
            for i in range(self.count):
                self.send_results[i] = send_result

    # ---------------- 发送 ----------------
    def _send_sync(self) -> None:
        pass
        with self.closed_lock:
            if self.closed:
                return
            self.closed = True
        batch = self._batch()
        try:
            result = self._producer.send_direct(batch, self.aggregate_key.mq, None)
            self._split_send_results(result)
        finally:
            self._owner.release_hold(self.messages_size)
            self.cond.notify_all()

    def _send_async(self, send_callback: "SendCallback") -> None:
        pass
        with self.closed_lock:
            if self.closed:
                return
            self.closed = True
        batch = self._batch()
        size = self.messages_size

        def _on_success(send_result: SendResult) -> None:
            try:
                self._split_send_results(send_result)
                i = 0
                for cb in self.send_callbacks:
                    cb.on_success(self.send_results[i])
                    i += 1
                if i != self.count:
                    raise ValueError("sendResult is illegal")
                self._owner.release_hold(size)
            except Exception as e:
                _on_exception(e)

        def _on_exception(e: BaseException) -> None:
            for cb in self.send_callbacks:
                cb.on_exception(e)
            self._owner.release_hold(size)

        try:
            self._producer.send_direct(batch, self.aggregate_key.mq,
                                       _InternalSendCallback(_on_success, _on_exception))
        except Exception as e:  # noqa: BLE001
            for cb in self.send_callbacks:
                cb.on_exception(e)


class _InternalSendCallback:
    pass

    def __init__(self, on_success, on_exception):
        self._on_success = on_success
        self._on_exception = on_exception

    def on_success(self, send_result: SendResult) -> None:
        self._on_success(send_result)

    def on_exception(self, e: BaseException) -> None:
        self._on_exception(e)


class _GuardService:
    pass

    def __init__(self, owner: "ProduceAccumulator", instance_name: str, suffix: str):
        self._owner = owner
        self.name = "Client_%s_%s" % (instance_name, suffix)
        self._stopped = threading.Event()
        self._thread: Optional[threading.Thread] = None

    def start(self) -> None:
        if self._thread is not None:
            return
        self._stopped.clear()
        self._thread = threading.Thread(target=self._run, name=self.name, daemon=True)
        self._thread.start()

    def shutdown(self) -> None:
        self._stopped.set()
        thread, self._thread = self._thread, None
        if thread is not None and thread.is_alive():
            thread.join(timeout=5.0)

    def _run(self) -> None:  # pragma: no cover - 线程体，靠测试间接覆盖
        while not self._stopped.is_set():
            try:
                self.do_work()
            except Exception:
                pass

    def do_work(self) -> None:
        raise NotImplementedError

    def _sleep(self) -> None:
        time.sleep(max(1, self._owner.hold_ms / 2) / 1000.0)


class _GuardForSyncSend(_GuardService):
    pass

    def __init__(self, owner: "ProduceAccumulator"):
        super().__init__(owner, owner.instance_name, "GuardForSyncSend")

    def do_work(self) -> None:
        for v in self._owner.sync_send_batches_snapshot():
            v.wakeup()
            with v.cond:
                with v.closed_lock:
                    if v.messages_size == 0:
                        v.closed = True
                        self._owner.remove_sync_batch(v)
                    else:
                        v.cond.notify_all()
        self._sleep()


class _GuardForAsyncSend(_GuardService):
    pass

    def __init__(self, owner: "ProduceAccumulator"):
        super().__init__(owner, owner.instance_name, "GuardForAsyncSend")

    def do_work(self) -> None:
        for v in self._owner.async_send_batches_snapshot():
            if v.ready_to_send():
                v._send_async(None)
            with v.closed_lock:
                if v.messages_size == 0:
                    v.closed = True
                    self._owner.remove_async_batch(v)
        self._sleep()


class ProduceAccumulator:
    pass

    DEFAULT_TOTAL_HOLD_SIZE = 32 * 1024 * 1024
    DEFAULT_HOLD_SIZE = 32 * 1024
    DEFAULT_HOLD_MS = 10

    def __init__(self, instance_name: str):
        self.instance_name = instance_name
        self.total_hold_size = self.DEFAULT_TOTAL_HOLD_SIZE
        self.hold_size = self.DEFAULT_HOLD_SIZE
        self.hold_ms = self.DEFAULT_HOLD_MS
        self._sync_send_batches: Dict[AggregateKey, MessageAccumulation] = {}
        self._async_send_batches: Dict[AggregateKey, MessageAccumulation] = {}
        self._table_lock = threading.Lock()
        self._hold_lock = threading.Lock()
        self._currently_hold_size = 0
        self._guard_sync = _GuardForSyncSend(self)
        self._guard_async = _GuardForAsyncSend(self)

    # ---------------- 生命周期 ----------------
    # 幂等且可重复：两个守卫各自管自己的线程（见 _GuardService 的说明），
    # 所以 start→shutdown→start（生产者重启）不会撞 "threads can only be started once"。
    def start(self) -> None:
        self._guard_sync.start()
        self._guard_async.start()

    def shutdown(self) -> None:
        self._guard_sync.shutdown()
        self._guard_async.shutdown()

    @property
    def currently_hold_size(self) -> int:
        with self._hold_lock:
            return self._currently_hold_size

    def get_batch_max_delay_ms(self) -> int:
        return self.hold_ms

    def batch_max_delay_ms(self, hold_ms: int) -> None:
        if hold_ms <= 0 or hold_ms > 30 * 1000:
            raise ValueError(
                "batchMaxDelayMs expect between 1ms and 30s, but get %d!" % hold_ms)
        self.hold_ms = hold_ms

    def get_batch_max_bytes(self) -> int:
        return self.hold_size

    def batch_max_bytes(self, hold_size: int) -> None:
        if hold_size <= 0 or hold_size > 2 * 1024 * 1024:
            raise ValueError(
                "batchMaxBytes expect between 1B and 2MB, but get %d!" % hold_size)
        self.hold_size = hold_size

    def get_total_batch_max_bytes(self) -> int:
        return self.hold_size

    def total_batch_max_bytes(self, total_hold_size: int) -> None:
        if total_hold_size <= 0:
            raise ValueError(
                "totalBatchMaxBytes must bigger then 0, but get %d!" % total_hold_size)
        self.total_hold_size = total_hold_size

    # ---------------- 全局字节闸门 ----------------
    def try_add_message(self, message: Message) -> bool:
        pass
        with self._hold_lock:
            if self._currently_hold_size < self.total_hold_size:
                body = message.body
                body_size = len(body) if body else 0
                if body_size > 0:
                    self._currently_hold_size += body_size
                return True
            return False

    def release_hold(self, size: int) -> None:
        pass
        with self._hold_lock:
            self._currently_hold_size -= size

    # ---------------- 表操作 ----------------
    def sync_send_batches_snapshot(self) -> List[MessageAccumulation]:
        with self._table_lock:
            return list(self._sync_send_batches.values())

    def async_send_batches_snapshot(self) -> List[MessageAccumulation]:
        with self._table_lock:
            return list(self._async_send_batches.values())

    def remove_sync_batch(self, batch: MessageAccumulation) -> None:
        pass
        with self._table_lock:
            if self._sync_send_batches.get(batch.aggregate_key) is batch:
                self._sync_send_batches.pop(batch.aggregate_key, None)

    def remove_async_batch(self, batch: MessageAccumulation) -> None:
        with self._table_lock:
            if self._async_send_batches.get(batch.aggregate_key) is batch:
                self._async_send_batches.pop(batch.aggregate_key, None)

    def _get_or_create_sync_batch(self, key: AggregateKey,
                                  producer: "DefaultMQProducer") -> MessageAccumulation:
        with self._table_lock:
            batch = self._sync_send_batches.get(key)
            if batch is not None:
                return batch
            batch = MessageAccumulation(key, producer, self)
            return self._sync_send_batches.setdefault(key, batch)

    def _get_or_create_async_batch(self, key: AggregateKey,
                                   producer: "DefaultMQProducer") -> MessageAccumulation:
        with self._table_lock:
            batch = self._async_send_batches.get(key)
            if batch is not None:
                return batch
            batch = MessageAccumulation(key, producer, self)
            return self._async_send_batches.setdefault(key, batch)

    # ---------------- 对外发送入口 ----------------
    def send(self, msg: Message, producer: "DefaultMQProducer") -> SendResult:
        pass
        key = AggregateKey.of_message(msg)
        while True:
            batch = self._get_or_create_sync_batch(key, producer)
            index = batch.add(msg)
            if index == -1:
                # 本批在本次 add 之前就被别的线程关掉了：摘掉它，重取/新建一个再试
                self.remove_sync_batch(batch)
                continue
            return batch.send_results[index]

    def send_with_mq(self, msg: Message, mq: MessageQueue,
                     producer: "DefaultMQProducer") -> SendResult:
        key = AggregateKey.of_message_with_mq(msg, mq)
        while True:
            batch = self._get_or_create_sync_batch(key, producer)
            index = batch.add(msg)
            if index == -1:
                self.remove_sync_batch(batch)
                continue
            return batch.send_results[index]

    def send_async(self, msg: Message, send_callback: "SendCallback",
                   producer: "DefaultMQProducer") -> None:
        key = AggregateKey.of_message(msg)
        while True:
            batch = self._get_or_create_async_batch(key, producer)
            if not batch.add_async(msg, send_callback):
                self.remove_async_batch(batch)
                continue
            return

    def send_async_with_mq(self, msg: Message, mq: MessageQueue,
                           send_callback: "SendCallback",
                           producer: "DefaultMQProducer") -> None:
        key = AggregateKey.of_message_with_mq(msg, mq)
        while True:
            batch = self._get_or_create_async_batch(key, producer)
            if not batch.add_async(msg, send_callback):
                self.remove_async_batch(batch)
                continue
            return


_ACCUMULATOR_TABLE: Dict[str, ProduceAccumulator] = {}
_ACCUMULATOR_LOCK = threading.Lock()


def get_or_create_produce_accumulator(client_id: str) -> ProduceAccumulator:
    with _ACCUMULATOR_LOCK:
        accumulator = _ACCUMULATOR_TABLE.get(client_id)
        if accumulator is None:
            accumulator = ProduceAccumulator(client_id)
            _ACCUMULATOR_TABLE[client_id] = accumulator
        return accumulator
