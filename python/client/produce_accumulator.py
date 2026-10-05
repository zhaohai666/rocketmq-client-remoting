# -*- coding: utf-8 -*-
"""对应 ``org.apache.rocketmq.client.producer.ProduceAccumulator``（Java 5.5.0）。

生产者打开 ``autoBatch`` 之后，``send(Message)`` 不再一条一条直发，而是先按
``AggregateKey(topic, mq, waitStoreMsgOK, tag)`` 归并进 ``MessageAccumulation``，
攒够 ``holdMs``/``holdSize``（或被守卫线程唤醒）再合成**一个** ``MessageBatch``
发出去，最后把 broker 回的**批量** ``SendResult`` 拆回每条消息各自的 SendResult ——
调用方拿到的东西与直发完全一致（msgId / offsetMsgId / queueOffset 都是这一条自己的）。

契约细节（逐条对齐 Java，别"顺手优化"）：

1. ``AggregateKey`` 是 topic + mq + waitStoreMsgOK + **tag** 四元组：tag 不同不合并
   （一个 MessageBatch 只有一个 TAGS 属性）；指定 mq 与不指定 mq 也不合并。
2. ``try_add_message`` 是全局字节闸门：``currently_hold_size < total_hold_size`` 才放行，
   放行时把本条 body 长度记进 ``currently_hold_size``；**批次真的发完**（同步版在
   ``finally``、异步版在回调里）才扣回。⚠ 上游的真实口径是「先记账，再判延时/重试」——
   也就是说 ``can_batch`` 里因延时消息退回直发的那条消息，它的字节数**已被记进
   currently_hold_size 且永不归还**（Java 遗漏，照抄；如果"修"成归还，就与 Java 对不上了）。
3. 批量应答拆条（``_split_send_results``）：broker 对批量消息回的 msgId/offsetMsgId 是
   **逗号分隔**的逐条 ID；含逗号才拆，条数对不上直接 ``ValueError``（Java
   ``IllegalArgumentException("sendResult is illegal")``）；不含逗号（老 broker /
   单条）时**所有**下标指向同一个 SendResult 对象（就地共享，不复制）。
4. **同步 add 收集 keys，异步 add 不收集**（Java 的不对称行为，照抄）：同步批次会把
   所有子消息的 KEYS 并集挂到 MessageBatch 上，异步批次恒为 ``keys=[]`` →
   ``batch.setKeys("")``。跨语言/跨进程的 KEYS 顺序不保证（Java 是 HashSet 迭代序），
   断言只能用「具体某条 key 在不在」，别比整串。
5. ``Message.setKeys(Collection)`` 是 ``String.join(" ", keys)``：分隔符是**空格**
   （``MessageConst.KEY_SEPARATOR``），且**无条件**写属性 —— 空集合写出 ``KEYS=""``
   （空串不是 null，``messageProperties2String`` 不会跳过它）。同理 tag 为 None 时不写
   TAGS（Java 的 null 值属性会被 ``messageProperties2String`` 跳过，等价）。
6. 守卫线程（sync/async 各一个）每轮 ``max(1, holdMs/2)`` 毫秒：sync 版对每个批次
   ``wakeup()``（叫醒正在 ``add`` 里阻塞的调用方去自查 ``ready_to_send``），再把
   ``messages_size == 0`` 的空批次置 ``closed`` 并摘表；async 版先 ``ready_to_send``
   就 ``send``，再做同样的摘表。**表项的移除只发生在守卫线程 / add 返回 -1 的调用方**，
   别在 send 里顺手 remove。
"""
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
    """``msg.getKeys().split(MessageConst.KEY_SEPARATOR)`` 的 Java 等价物。

    Java 的 ``String.split``（limit=0）**丢弃所有尾部空串**，Python 的 ``str.split``
    会保留 —— 所以这里显式剥掉尾部空段（``"a ".split(" ")`` 在 Java 是 ``["a"]``，
    在 Python 是 ``["a", ""]``；中间的空段两边都保留）。
    """
    parts = msg_keys.split(" ")
    while parts and parts[-1] == "":
        parts.pop()
    return parts


class AggregateKey:
    """归并键：``topic + mq + waitStoreMsgOK + tag``（Java ``ProduceAccumulator.AggregateKey``）。"""

    __slots__ = ("topic", "mq", "wait_store_msg_ok", "tag")

    def __init__(self, topic: str, mq: Optional[MessageQueue],
                 wait_store_msg_ok: bool, tag: Optional[str]):
        self.topic = topic
        self.mq = mq
        self.wait_store_msg_ok = wait_store_msg_ok
        self.tag = tag

    @classmethod
    def of_message(cls, msg: Message) -> "AggregateKey":
        # Java ``AggregateKey(message)`` 用 ``message.isWaitStoreMsgOK()``：**缺省即 true**。
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
        # Java 用的是 Objects.hash(topic, mq, waitStoreMsgOK, tag) —— 只要「相等的键
        # 哈希相同」即可，具体数值不必与 Java 对齐（键只在本进程内用）。
        return hash((self.topic, self.mq, self.wait_store_msg_ok, self.tag))

    def __repr__(self) -> str:
        return "AggregateKey(topic='%s', mq=%r, waitStoreMsgOK=%s, tag=%r)" % (
            self.topic, self.mq, self.wait_store_msg_ok, self.tag)


class MessageAccumulation:
    """一批待归并的消息（Java ``ProduceAccumulator.MessageAccumulation``）。

    两类调用方：同步 ``add()`` 会在批次达到阈值前**阻塞**自己（等到本批真的发出去，
    自己的那条消息才有 SendResult 可返回）；异步 ``add_async()`` 立刻返回，结果走回调。
    """

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
        # 对应 Java 的 `synchronized (this.closed)`：closed 既是锁对象又是状态位
        self.closed_lock = threading.Lock()
        # 对应 Java 的 `synchronized (this)`：add 在这里 wait、守卫线程在这里 notify
        self.cond = threading.Condition()
        self.send_results: List[Optional[SendResult]] = []
        self.messages_size = 0
        self.count = 0
        self.create_time = _now_ms()

    # ---------------- 阈值 ----------------
    def ready_to_send(self) -> bool:
        """Java ``readyToSend()``：按**本批**字节数或本批存活时间（不是全局限额）。"""
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

        # Java 这里只有一个 `while (!closed)` 循环，靠守卫线程的 notify 与自己的 send
        # 两条路跳出；closed 已经为真时直接返回（不管有没有发成功）。
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
        """Java ``wakeup()``：叫醒一个正在 ``add`` 里等阈值的调用方，让它自查 ``ready_to_send``。"""
        with self.cond:
            if self.closed:
                return
            self.cond.notify_all()

    # ---------------- 组装 / 拆分 ----------------
    def _batch(self) -> MessageBatch:
        """Java ``batch()``：把本批组装成一个 MessageBatch。"""
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
        """Java ``splitSendResults``：批量应答拆成逐条 SendResult。"""
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
            # 不含逗号：老 broker / 单条应答，所有下标共享同一个 result 对象（Java 同）
            for i in range(self.count):
                self.send_results[i] = send_result

    # ---------------- 发送 ----------------
    def _send_sync(self) -> None:
        """Java ``MessageAccumulation.send()``（同步）。

        ⚠ 只能在**持有 ``self.cond``** 时调用：Java 的 ``notifyAll()`` 靠调用方
        （``add`` 里的 ``synchronized (this)``）提供的监视器，可重入所以合法。
        """
        with self.closed_lock:
            if self.closed:
                return
            self.closed = True
        batch = self._batch()
        try:
            result = self._producer.send_direct(batch, self.aggregate_key.mq, None)
            self._split_send_results(result)
        finally:
            # 无论成败都归还全局字节额度（Java：finally 里 currentlyHoldSize -= messagesSize）
            self._owner.release_hold(self.messages_size)
            self.cond.notify_all()

    def _send_async(self, send_callback: "SendCallback") -> None:
        """Java ``MessageAccumulation.send(SendCallback)``（异步）。

        参数 ``send_callback`` 与 Java 一样**不参与**逻辑（Java 收了但没用），回调来自
        ``self.send_callbacks`` 列表。批量应答回来后逐条分发给各自的回调。
        """
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
            except Exception as e:  # noqa: BLE001 - 与 Java 一样把内部异常转给全体回调
                _on_exception(e)

        def _on_exception(e: BaseException) -> None:
            for cb in self.send_callbacks:
                cb.on_exception(e)
            self._owner.release_hold(size)

        try:
            self._producer.send_direct(batch, self.aggregate_key.mq,
                                       _InternalSendCallback(_on_success, _on_exception))
        except Exception as e:  # noqa: BLE001
            # ⚠ Java 在这里**没有**归还 currentlyHoldSize（只有回调路径会还）——
            # 即"异步发送在发起阶段就抛异常"会漏掉一份字节额度。照抄，别修。
            for cb in self.send_callbacks:
                cb.on_exception(e)


class _InternalSendCallback:
    """把两个函数包成 ``SendCallback``（对应 Java 的匿名内部类）。"""

    def __init__(self, on_success, on_exception):
        self._on_success = on_success
        self._on_exception = on_exception

    def on_success(self, send_result: SendResult) -> None:
        self._on_success(send_result)

    def on_exception(self, e: BaseException) -> None:
        self._on_exception(e)


class _GuardService:
    """守卫线程基类（对应 Java 的 ``ServiceThread`` 子类）。

    ⚠ **不能**继承 ``threading.Thread``：累加器是按 clientId 复用的，同一个实例会经历
    ``start → shutdown → start``（生产者重启；Java 的 ``ServiceThread`` 同样可重复 start），
    而 ``Thread`` 对象只能 ``start()`` 一次 —— 第二次会抛
    ``RuntimeError: threads can only be started once``。这里改成「持有一根可重建的 Thread」。
    """

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
            except Exception:  # noqa: BLE001 - Java：日志告警后继续跑
                pass

    def do_work(self) -> None:
        raise NotImplementedError

    def _sleep(self) -> None:
        # Java：Math.max(1, holdMs / 2)
        time.sleep(max(1, self._owner.hold_ms / 2) / 1000.0)


class _GuardForSyncSend(_GuardService):
    """同步发送的守卫线程（Java ``GuardForSyncSendService``）。"""

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
    """异步发送的守卫线程（Java ``GuardForAsyncSendService``）。"""

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
    """对应 Java ``ProduceAccumulator``：按 clientId 复用的自动攒批器。"""

    # Java 的三个默认值（totalHoldSize / holdSize / holdMs）
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
        # 保护两张表的所有读改写（Java 用 ConcurrentHashMap；这里用一把表锁给出更强的
        # 一致性，锁序恒为 表锁 → 批次的 cond/closed_lock，不会成环）
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

    # ---------------- 参数（Java 的校验口径逐字照抄）----------------
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
        # Java 这里也返回 holdSize（不是 totalHoldSize）—— 上游笔误，照抄
        return self.hold_size

    def total_batch_max_bytes(self, total_hold_size: int) -> None:
        if total_hold_size <= 0:
            raise ValueError(
                "totalBatchMaxBytes must bigger then 0, but get %d!" % total_hold_size)
        self.total_hold_size = total_hold_size

    # ---------------- 全局字节闸门 ----------------
    def try_add_message(self, message: Message) -> bool:
        """Java ``tryAddMessage``：还有额度就记账放行，否则拒绝（调用方退回直发）。"""
        with self._hold_lock:
            if self._currently_hold_size < self.total_hold_size:
                body = message.body
                body_size = len(body) if body else 0
                if body_size > 0:
                    self._currently_hold_size += body_size
                return True
            return False

    def release_hold(self, size: int) -> None:
        """批次发送完成后的归还（Java 直接 ``currentlyHoldSize.addAndGet(-size)``）。"""
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
        """Java ``syncSendBatchs.remove(key, batch)``：只在值仍是它时才摘。"""
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
        """Java ``send(Message, DefaultMQProducer)``：只返回本条消息自己的 SendResult。"""
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


# ---------------------------------------------------------------- 进程级复用
# 对应 Java ``MQClientManager.getOrCreateProduceAccumulator``：**按 clientId 缓存**，
# 所以同进程里两个 clientId 相同的 producer（比如同一 group 反复 new）共享同一个累加器
# 与同一对守卫线程 —— 这也是「累加器参数要先记在 producer 上、start() 时再同步下去」的
# 原因：第二个 producer 拿到的是别人已经建好的累加器，构造期无从设置阈值。
_ACCUMULATOR_TABLE: Dict[str, ProduceAccumulator] = {}
_ACCUMULATOR_LOCK = threading.Lock()


def get_or_create_produce_accumulator(client_id: str) -> ProduceAccumulator:
    with _ACCUMULATOR_LOCK:
        accumulator = _ACCUMULATOR_TABLE.get(client_id)
        if accumulator is None:
            accumulator = ProduceAccumulator(client_id)
            _ACCUMULATOR_TABLE[client_id] = accumulator
        return accumulator
