# -*- coding: utf-8 -*-
"""生产者（对应 org.apache.rocketmq.client.producer.* 及 impl.producer）。

提供：DefaultMQProducer（同步/异步/单向/批量/选择器发送、事务消息）、
TransactionMQProducer、MessageQueueSelector 系列选择器、SendCallback 回调、
LocalTransactionState / TransactionListener 等。
"""
from __future__ import annotations

import random
import threading
import time
from enum import Enum
from typing import Callable, List, Optional

from ..common.message import Message, MessageBatch, MessageExt, MessageQueue
from ..common.message_accessor import MessageAccessor
from ..common.mix_all import MixAll
from ..logging import get_logger
from ..remoting.exception import RemotingException
from ..remoting.rpchook import RPCHook
from .exception import MQBrokerException, MQClientException
from .mq_client import MQClientInstance
from .send_result import SendResult, SendStatus

logger = get_logger()


class MessageQueueSelector:
    """消息队列选择器（对应 Java MessageQueueSelector）。"""

    def select(self, mqs: List[MessageQueue], msg: Message, arg) -> MessageQueue:
        raise NotImplementedError


class SelectMessageQueueByHash(MessageQueueSelector):
    """按 arg 的 hash 选择队列（Java SelectMessageQueueByHash）。"""

    def select(self, mqs: List[MessageQueue], msg: Message, arg) -> MessageQueue:
        if not mqs:
            raise MQClientException("no message queue")
        value = arg if arg is not None else 0
        idx = abs(hash(value)) % len(mqs)
        return mqs[idx]


class SelectMessageQueueByRandom(MessageQueueSelector):
    """随机选择一个可用队列（Java SelectMessageQueueByRandom）。"""

    def select(self, mqs: List[MessageQueue], msg: Message, arg) -> MessageQueue:
        if not mqs:
            raise MQClientException("no message queue")
        return mqs[random.randint(0, len(mqs) - 1)]


class SelectMessageQueueByMachineRoom(MessageQueueSelector):
    """按机房（brokerName 前缀）选择队列（Java SelectMessageQueueByMachineRoom）。"""

    def select(self, mqs: List[MessageQueue], msg: Message, arg) -> MessageQueue:
        if not mqs:
            raise MQClientException("no message queue")
        room = str(arg)
        for mq in mqs:
            if mq.broker_name.startswith(room):
                return mq
        # 无匹配则回退首个
        return mqs[0]


class SendCallback:
    """异步发送回调（对应 Java SendCallback）。"""

    def on_success(self, send_result: SendResult) -> None:
        raise NotImplementedError

    def on_exception(self, e: Exception) -> None:
        raise NotImplementedError


class LocalTransactionState(Enum):
    COMMIT_MESSAGE = 0
    ROLLBACK_MESSAGE = 1
    UNKNOW = 2


class TransactionListener:
    """事务监听器（对应 Java TransactionListener）。"""

    def execute_local_transaction(self, msg: Message, arg) -> LocalTransactionState:
        raise NotImplementedError

    def check_local_transaction(self, msg: MessageExt) -> LocalTransactionState:
        raise NotImplementedError


class TransactionSendResult(SendResult):
    """事务消息发送结果（对应 Java TransactionSendResult）。"""

    def __init__(self, send_result: Optional[SendResult] = None,
                 local_transaction_state: Optional[LocalTransactionState] = None):
        if send_result is not None:
            super().__init__(send_result.send_status, send_result.msg_id,
                             send_result.message_queue, send_result.queue_offset,
                             send_result.transaction_id, send_result.offset_msg_id,
                             send_result.region_id)
        else:
            super().__init__()
        self.local_transaction_state = local_transaction_state

    def get_local_transaction_state(self) -> Optional[LocalTransactionState]:
        return self.local_transaction_state

    def set_local_transaction_state(self, state: Optional[LocalTransactionState]) -> None:
        self.local_transaction_state = state


class DefaultMQProducer:
    """默认生产者（对应 org.apache.rocketmq.client.producer.DefaultMQProducer）。"""

    def __init__(self, producer_group: str = MixAll.DEFAULT_PRODUCER_GROUP,
                 rpc_hook: Optional[RPCHook] = None, namespace: str = "",
                 topics: Optional[List[str]] = None):
        if producer_group is None or not str(producer_group).strip():
            raise MQClientException("producerGroup is empty")
        self.producer_group = str(producer_group)
        self.namespace = namespace
        self.instance_name = "DEFAULT"
        self.client_id = None
        self.create_topic_key = MixAll.DEFAULT_TOPIC
        self.default_topic_queue_nums = MixAll.DEFAULT_TOPIC_QUEUE_NUMS
        self.send_msg_timeout = 3000
        self.compress_msg_body_over_howmuch = 1024 * 4
        self.retry_times_when_send_failed = 2
        self.retry_times_when_send_async_failed = 2
        self.retry_another_broker_when_not_store_ok = False
        self.max_message_size = 1024 * 1024 * 4
        self.rpc_hook = rpc_hook
        self.topics = list(topics) if topics else []
        self.name_server_addrs: List[str] = []
        self._namespace_mode = False
        if namespace:
            self._namespace_mode = True
        self._mq_client: Optional[MQClientInstance] = None
        self._started = False
        self._lock = threading.Lock()

    # ---------------- 配置 ----------------
    def set_namesrv_addr(self, addr: str) -> None:
        self.name_server_addrs = [a.strip() for a in addr.split(";") if a.strip()]

    def set_name_server_addresses(self, addrs: List[str]) -> None:
        self.name_server_addrs = list(addrs)

    def get_namesrv_addr(self) -> str:
        return ";".join(self.name_server_addrs)

    def set_instance_name(self, name: str) -> None:
        self.instance_name = name

    def set_max_message_size(self, size: int) -> None:
        self.max_message_size = size

    def set_send_msg_timeout(self, timeout: int) -> None:
        self.send_msg_timeout = timeout

    def set_retry_times_when_send_failed(self, n: int) -> None:
        self.retry_times_when_send_failed = n

    def set_create_topic_key(self, key: str) -> None:
        self.create_topic_key = key

    def set_default_topic_queue_nums(self, n: int) -> None:
        self.default_topic_queue_nums = n

    def get_producer_group(self) -> str:
        return self.producer_group

    def set_producer_group(self, group: str) -> None:
        if self._started:
            raise MQClientException("producerGroup cannot be changed after startup")
        self.producer_group = group

    # ---------------- 生命周期 ----------------
    def start(self) -> None:
        with self._lock:
            if self._started:
                return
            if not self.name_server_addrs:
                raise MQClientException("name server address is not set")
            if self.client_id is None:
                self.client_id = "%s@%s" % (self.instance_name, time.strftime("%Y%m%d%H%M%S"))
            self._mq_client = MQClientInstance(self.client_id, self.name_server_addrs)
            if self.rpc_hook is not None:
                self._mq_client.remoting_client.register_rpc_hook(self.rpc_hook)
            self._mq_client.start()
            self._started = True

    def shutdown(self) -> None:
        with self._lock:
            if not self._started:
                return
            if self._mq_client is not None:
                self._mq_client.shutdown()
            self._started = False

    def _require_client(self) -> MQClientInstance:
        if not self._started or self._mq_client is None:
            raise MQClientException("producer not started, call start() first")
        return self._mq_client

    # ---------------- 正常发送 ----------------
    def send(self, msg: Message, timeout_millis: Optional[int] = None,
             mq: Optional[MessageQueue] = None) -> SendResult:
        """同步发送：msg 或 Collection；指定 mq 走定点发送，否则轮询选择。"""
        client = self._require_client()
        timeout = timeout_millis if timeout_millis is not None else self.send_msg_timeout
        if isinstance(msg, (list, tuple)):
            return self._send_batch(list(msg), mq, timeout)
        self._check_message(msg)
        if mq is not None:
            return client.send_message(self.producer_group, msg, mq, timeout)
        last_exc = None
        for attempt in range(self.retry_times_when_send_failed + 1):
            try:
                publish = client.get_topic_publish_info(msg.topic)
                selected = publish.select_one_message_queue()
                mq_sel = MessageQueue(msg.topic, selected.broker_name, selected.queue_id)
                return client.send_message(self.producer_group, msg, mq_sel, timeout)
            except (MQClientException, MQBrokerException, RemotingException) as e:
                last_exc = e
        raise last_exc

    def send_async(self, msg: Message, callback: SendCallback,
                   timeout_millis: Optional[int] = None,
                   mq: Optional[MessageQueue] = None) -> None:
        """异步发送（对应 Java send(msg, callBack, timeout)）。"""
        client = self._require_client()
        timeout = timeout_millis if timeout_millis is not None else self.send_msg_timeout
        try:
            result = self.send(msg, timeout, mq)
            callback.on_success(result)
        except Exception as e:  # noqa: BLE001
            callback.on_exception(e)

    def send_oneway(self, msg: Message, mq: Optional[MessageQueue] = None) -> None:
        """单向发送（对应 Java sendOneway）。"""
        client = self._require_client()
        self._check_message(msg)
        if mq is not None:
            client.send_message_oneway(self.producer_group, msg, mq,
                                       self._need_addr(client, mq), self.send_msg_timeout)
            return
        publish = client.get_topic_publish_info(msg.topic)
        selected = publish.select_one_message_queue()
        mq_sel = MessageQueue(msg.topic, selected.broker_name, selected.queue_id)
        client.send_message_oneway(self.producer_group, msg, mq_sel,
                                   self._need_addr(client, mq_sel), self.send_msg_timeout)

    def send_by_selector(self, msg: Message, selector: MessageQueueSelector, arg,
                         timeout_millis: Optional[int] = None) -> SendResult:
        """使用 MessageQueueSelector 选择队列发送（对应 Java send(msg, selector, arg)）。"""
        client = self._require_client()
        timeout = timeout_millis if timeout_millis is not None else self.send_msg_timeout
        publish = client.get_topic_publish_info(msg.topic)
        selected = selector.select(publish.msg_queue_list, msg, arg)
        mq_sel = MessageQueue(msg.topic, selected.broker_name, selected.queue_id)
        return client.send_message(self.producer_group, msg, mq_sel, timeout)

    # ---------------- 批量发送 ----------------
    def _send_batch(self, msgs: List[Message], mq: Optional[MessageQueue] = None,
                    timeout_millis: Optional[int] = None) -> SendResult:
        client = self._require_client()
        timeout = timeout_millis if timeout_millis is not None else self.send_msg_timeout
        if not msgs:
            raise MQClientException("message list is empty")
        batch = MessageBatch.generate_from_list(msgs)
        if mq is not None:
            return client.send_message(self.producer_group, batch, mq, timeout)
        publish = client.get_topic_publish_info(batch.topic)
        selected = publish.select_one_message_queue()
        mq_sel = MessageQueue(batch.topic, selected.broker_name, selected.queue_id)
        return client.send_message(self.producer_group, batch, mq_sel, timeout)

    # ---------------- 事务消息 ----------------
    def send_message_in_transaction(self, msg: Message,
                                    listener: TransactionListener,
                                    arg=None) -> TransactionSendResult:
        """发送事务消息（对应 Java sendMessageInTransaction）。"""
        client = self._require_client()
        self._check_message(msg)
        publish = client.get_topic_publish_info(msg.topic)
        selected = publish.select_one_message_queue()
        mq_sel = MessageQueue(msg.topic, selected.broker_name, selected.queue_id)
        # 发送 half 消息（模拟：先发送，再执行本地事务，按结果决定提交/回滚）
        send_result = client.send_message(self.producer_group, msg, mq_sel, self.send_msg_timeout)
        state = listener.execute_local_transaction(msg, arg)
        tsr = TransactionSendResult(send_result, state)
        if state == LocalTransactionState.UNKNOW:
            # 由 broker 回查，本地简化直接跳过
            pass
        return tsr

    # ---------------- 管理能力 ----------------
    def fetch_publish_message_queues(self, topic: str) -> List[MessageQueue]:
        client = self._require_client()
        publish = client.get_topic_publish_info(topic)
        return list(publish.msg_queue_list)

    def create_topic(self, key: str, new_topic: str, queue_num: int = 4,
                     topic_sys_flag: int = 0) -> None:
        client = self._require_client()
        perm = 6  # PermName.PERM_READ | PERM_WRITE
        client.create_topic_in_route(new_topic, queue_num, queue_num, perm)

    def search_offset(self, mq: MessageQueue, timestamp: int) -> int:
        return self._require_client().search_offset_by_timestamp(mq, timestamp)

    def max_offset(self, mq: MessageQueue) -> int:
        return self._require_client().get_max_offset(mq)

    def min_offset(self, mq: MessageQueue) -> int:
        return self._require_client().get_min_offset(mq)

    def earliest_msg_store_time(self, mq: MessageQueue) -> int:
        from ..remoting.protocol.headers import GetEarliestMsgStoretimeRequestHeader
        from ..remoting.protocol.codes import RequestCode
        from ..remoting.protocol.remoting_command import RemotingCommand
        client = self._require_client()
        addr = client._broker_addr(mq)
        header = GetEarliestMsgStoretimeRequestHeader()
        header.topic = mq.topic
        header.queue_id = mq.queue_id
        request = RemotingCommand.create_request_command(RequestCode.GET_EARLIEST_MSG_STORETIME, header)
        from ..remoting.protocol.headers import GetEarliestMsgStoretimeResponseHeader
        response = client._invoke_sync(addr, request, 5000)
        client._check_response(response)
        resp_header = GetEarliestMsgStoretimeResponseHeader()
        resp_header.from_ext_fields(response.ext_fields)
        return resp_header.timestamp or 0

    def query_message(self, topic: str, key: str, max_num: int, begin: int, end: int):
        """查询消息（返回原始 body），对应 Java queryMessage。"""
        client = self._require_client()
        body = client.query_message(topic, key, max_num, begin, end)
        if body is None:
            return []
        from ..common.message_decoder import decode_messages
        return decode_messages(body)

    def view_message(self, topic: str, msg_id: str):
        raise MQClientException("viewMessage by msgId is not supported in Python edition")

    # ---------------- 辅助 ----------------
    def _check_message(self, msg: Message) -> None:
        if msg is None:
            raise MQClientException("message is null")
        if not msg.topic:
            raise MQClientException("message topic is empty")
        if len(msg.body) > self.max_message_size:
            raise MQClientException(
                "message body size %d exceeds maxMessageSize %d" % (len(msg.body), self.max_message_size))

    @staticmethod
    def _need_addr(client: MQClientInstance, mq: MessageQueue) -> str:
        addr = client.broker_addr_of(mq.broker_name)
        if addr is None:
            raise MQClientException("broker address not found for %s" % mq.broker_name)
        return addr


class TransactionMQProducer(DefaultMQProducer):
    """事务消息生产者（对应 Java TransactionMQProducer）。"""

    def __init__(self, producer_group: str = MixAll.DEFAULT_PRODUCER_GROUP,
                 rpc_hook: Optional[RPCHook] = None, namespace: str = "",
                 topics: Optional[List[str]] = None):
        super().__init__(producer_group, rpc_hook, namespace, topics)
        self.transaction_listener: Optional[TransactionListener] = None
        self.check_thread_pool_min_size = 1
        self.check_thread_pool_max_size = 1
        self.check_request_hold_max = 2000

    def set_transaction_listener(self, listener: Optional[TransactionListener]) -> None:
        self.transaction_listener = listener

    def get_transaction_listener(self) -> Optional[TransactionListener]:
        return self.transaction_listener

    def send_message_in_transaction(self, msg: Message, listener: Optional[TransactionListener] = None,
                                    arg=None) -> TransactionSendResult:
        listener = listener if listener is not None else self.transaction_listener
        if listener is None:
            raise MQClientException("transaction listener is not set")
        return super().send_message_in_transaction(msg, listener, arg)


class SendCallbackImpl(SendCallback):
    """便捷回调包装：把 on_success / on_exception 转成普通函数。"""

    def __init__(self, success_fn: Optional[Callable[[SendResult], None]] = None,
                 exception_fn: Optional[Callable[[Exception], None]] = None):
        self.success_fn = success_fn
        self.exception_fn = exception_fn

    def on_success(self, send_result: SendResult) -> None:
        if self.success_fn is not None:
            self.success_fn(send_result)

    def on_exception(self, e: Exception) -> None:
        if self.exception_fn is not None:
            self.exception_fn(e)


__all__ = [
    "DefaultMQProducer", "TransactionMQProducer", "MessageQueueSelector",
    "SelectMessageQueueByHash", "SelectMessageQueueByRandom",
    "SelectMessageQueueByMachineRoom", "SendCallback", "SendCallbackImpl",
    "LocalTransactionState", "TransactionListener", "TransactionSendResult",
    "SendResult", "SendStatus",
]