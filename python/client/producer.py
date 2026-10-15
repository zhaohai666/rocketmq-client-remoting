# -*- coding: utf-8 -*-
from __future__ import annotations

import functools
import os
import random
import threading
import time
from collections import deque
from enum import Enum
from typing import Callable, Dict, List, Optional, Tuple

from common.message import Message, MessageBatch, MessageExt, MessageQueue
from common.message_accessor import MessageAccessor
from common.message_const import MessageConst
from common.message_decoder import _compress, decode_message, decode_message_id
from common.message_client_id_setter import get_uniq_id, set_uniq_id
from common.message_type import MessageType
from common import recall_message_handle
from common.mix_all import MixAll
from common.sysflag import MessageSysFlag
from rocketmq_logging import get_logger
from remoting.exception import (RemotingConnectException, RemotingException,
                                  RemotingSendRequestException, RemotingTimeoutException,
                                  RemotingTooMuchRequestException)
from remoting.protocol.codes import RequestCode, ResponseCode
from remoting.protocol.headers import (CheckTransactionStateRequestHeader,
                                         EndTransactionRequestHeader,
                                         RecallMessageRequestHeader)
from remoting.protocol.heartbeat import HeartbeatData, ProducerData
from remoting.protocol.namespace_util import NamespaceUtil
from remoting.protocol.remoting_command import RemotingCommand
from remoting.rpchook import RPCHook
from remoting.socks_proxy import SocksProxyConfig
from .consume_executor import ConsumeExecutor, RejectedExecutionError
from .backpressure import (MIN_ASYNC_SEND_NUM, MIN_ASYNC_SEND_SIZE, FairSemaphore)
from .exception import (ClientErrorCode, MQBrokerException, MQClientException,
                        RequestTimeoutException)
from .hook import (CheckForbiddenContext, CheckForbiddenHook, CommunicationMode,

                   EndTransactionContext, EndTransactionHook, SendMessageContext, SendMessageHook)
from .latency import MQFaultStrategy
from .metrics import ClientMetrics
from .mq_client import MQClientInstance
from .mq_client_manager import MQClientManager
from .top_addressing import DefaultTopAddressing
from .request_reply import (DEFAULT_REQUEST_TIMEOUT_MILLIS, REQUEST_FUTURE_HOLDER,
                            RequestCallback, RequestResponseFuture, create_correlation_id)
from .produce_accumulator import ProduceAccumulator, get_or_create_produce_accumulator
from .send_result import SendResult, SendStatus
from .trace_context import inject_trace_context, trace_context_enabled_from_env
from .trace import AccessChannel
from .trace_dispatcher import AsyncTraceDispatcher, TraceDispatcherType
from .trace_hook import EndTransactionTraceHook, SendMessageTraceHook
from . import validators

logger = get_logger()

_PINNED_TOPIC_MISMATCH_SYNC = "message's topic not equal mq's topic"
_PINNED_TOPIC_MISMATCH_ASYNC = "Topic of the message does not match its target message queue"


def _restores_caller_message(fn):
    pass
    @functools.wraps(fn)
    def wrapper(self, msg, *args, **kwargs):
        if isinstance(msg, (list, tuple)):
            return fn(self, msg, *args, **kwargs)
        prev_body = msg.body
        try:
            return fn(self, msg, *args, **kwargs)
        finally:
            msg.body = prev_body
            msg.topic = NamespaceUtil.without_namespace(msg.topic, self.namespace)
    return wrapper


class _NullSendCallback:
    pass

    def __init__(self, future: Optional[RequestResponseFuture] = None):
        self.future = future

    def on_success(self, send_result: SendResult) -> None:
        if self.future is not None:
            self.future.send_request_ok = True

    def on_exception(self, e: BaseException) -> None:
        if self.future is not None:
            self.future.send_request_ok = False
            self.future.put_response_message(None)
            self.future.cause = e


class _RequestAsyncSendCallback:
    pass

    def __init__(self, future: RequestResponseFuture, correlation_id: str):
        self.future = future
        self.correlation_id = correlation_id

    def on_success(self, send_result: SendResult) -> None:
        if self.future is not None:
            self.future.send_request_ok = True

    def on_exception(self, e: BaseException) -> None:
        future = REQUEST_FUTURE_HOLDER.remove_request(self.correlation_id)
        if future is not None:
            future.cause = e
            future.send_request_ok = False
            future.put_response_message(None)
            future.execute_request_callback()


def _check_request_send_args(mq: Optional[MessageQueue],
                             selector: Optional[MessageQueueSelector]) -> None:
    pass
    if mq is not None and selector is not None:
        raise ValueError("mq and selector are mutually exclusive; pass exactly one")


def _back_pressure_permits(configured: int, floor: int, name: str) -> int:
    pass
    if configured > floor:
        return configured
    logger.info("%s can not be smaller than %d.", name, floor)
    return floor


def _back_pressure_msg_len(msg) -> int:
    pass
    if isinstance(msg, (list, tuple, MessageBatch)):
        messages = list(msg)
        return sum(_back_pressure_msg_len(one) for one in messages) or 1
    body = getattr(msg, "body", None)
    return len(body) if body else 1


class _BackPressureSendCallback:
    pass

    def __init__(self, delegate: "SendCallback", num_semaphore: FairSemaphore,
                 size_semaphore: FairSemaphore, msg_len: int) -> None:
        self._delegate = delegate
        self._num_semaphore = num_semaphore
        self._size_semaphore = size_semaphore
        self.msg_len = msg_len
        self._released = False
        self.num_acquired = False
        self.size_acquired = False

    def on_success(self, send_result: SendResult) -> None:
        self._release_permits()
        self._delegate.on_success(send_result)

    def on_exception(self, e: BaseException) -> None:
        self._release_permits()
        self._delegate.on_exception(e)

    def _release_permits(self) -> None:
        pass
        if self._released:
            return
        self._released = True
        if self.size_acquired:
            self._size_semaphore.release(self.msg_len)
        if self.num_acquired:
            self._num_semaphore.release(1)


_TIMER_DELAY_PROPERTIES = ("DELAY", "TIMER_DELAY_MS", "TIMER_DELAY_SEC", "TIMER_DELIVER_MS")


def _max_delay_value(msg: Message) -> int:
    pass
    result = 0
    for name in _TIMER_DELAY_PROPERTIES:
        raw = msg.properties.get(name)
        if raw is None:
            continue
        value = int(raw)
        if value > result:
            result = value
    return result


def _classify_async_failure(error: BaseException,
                            cost: int) -> Tuple[BaseException, bool]:
    pass
    if isinstance(error, RemotingSendRequestException):
        return MQClientException("send request failed", None, error), True
    if isinstance(error, RemotingTimeoutException):
        return MQClientException("wait response timeout, cost=%d" % cost, None, error), True
    if isinstance(error, RemotingException):
        retry = not isinstance(error, RemotingTooMuchRequestException)
        return MQClientException("unknown reason", None, error), retry
    return error, False


class MessageQueueSelector:
    pass

    def select(self, mqs: List[MessageQueue], msg: Message, arg) -> MessageQueue:
        raise NotImplementedError


class SelectMessageQueueByHash(MessageQueueSelector):
    pass

    def select(self, mqs: List[MessageQueue], msg: Message, arg) -> MessageQueue:
        if not mqs:
            raise MQClientException("no message queue")
        value = arg if arg is not None else 0
        idx = abs(hash(value)) % len(mqs)
        return mqs[idx]


class SelectMessageQueueByRandom(MessageQueueSelector):
    pass

    def select(self, mqs: List[MessageQueue], msg: Message, arg) -> MessageQueue:
        if not mqs:
            raise MQClientException("no message queue")
        return mqs[random.randint(0, len(mqs) - 1)]


class SelectMessageQueueByMachineRoom(MessageQueueSelector):
    pass

    def select(self, mqs: List[MessageQueue], msg: Message, arg) -> MessageQueue:
        if not mqs:
            raise MQClientException("no message queue")
        room = str(arg)
        for mq in mqs:
            if mq.broker_name.startswith(room):
                return mq
        # Fall back to the first when there is no match
        return mqs[0]


class SendCallback:
    pass

    def on_success(self, send_result: SendResult) -> None:
        raise NotImplementedError

    def on_exception(self, e: Exception) -> None:
        raise NotImplementedError


class LocalTransactionState(Enum):
    COMMIT_MESSAGE = 0
    ROLLBACK_MESSAGE = 1
    UNKNOW = 2


class TransactionListener:
    pass

    def execute_local_transaction(self, msg: Message, arg) -> LocalTransactionState:
        raise NotImplementedError

    def check_local_transaction(self, msg: MessageExt) -> LocalTransactionState:
        raise NotImplementedError


class TransactionSendResult(SendResult):
    pass

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
    pass

    def __init__(self, producer_group: str = MixAll.DEFAULT_PRODUCER_GROUP,
                 rpc_hook: Optional[RPCHook] = None, namespace: str = "",
                 topics: Optional[List[str]] = None, tls_enable: Optional[bool] = None,
                 tls_options: Optional[Dict[str, str]] = None,
                 socks_proxy_config: Optional[SocksProxyConfig] = None,
                 enable_trace_context: Optional[bool] = None):
        if producer_group is None or not str(producer_group).strip():
            raise MQClientException("producerGroup is empty")
        self.producer_group = str(producer_group)
        self.tls_enable: Optional[bool] = tls_enable
        # TLS details (caCert = strict CA validation; clientCert/clientKey = mTLS; serverName = hostname override)
        self.tls_options: Optional[Dict[str, str]] = tls_options
        self.socks_proxy_config: Optional[SocksProxyConfig] = socks_proxy_config
        # W3C traceparent pass-through (opt-in; None = defer to the env ROCKETMQ_TRACE_CONTEXT_ENABLE)
        if enable_trace_context is None:
            enable_trace_context = trace_context_enabled_from_env()
        self.enable_trace_context: bool = bool(enable_trace_context)
        self.namespace = namespace
        self.instance_name = MixAll.DEFAULT_INSTANCE_NAME
        self.client_id = None
        self.unit_name: Optional[str] = None
        self.unit_mode = False
        self.enable_stream_request_type = False
        self.namespace_v2 = ""
        self.create_topic_key = MixAll.DEFAULT_TOPIC
        self.default_topic_queue_nums = MixAll.DEFAULT_TOPIC_QUEUE_NUMS
        self.send_msg_timeout = 3000
        self.compress_msg_body_over_howmuch = 1024 * 4
        self.compress_level = 5
        self.compress_type = MessageSysFlag.ZLIB_TYPE
        self.retry_times_when_send_failed = 2
        self.retry_times_when_send_async_failed = 2
        self.retry_another_broker_when_not_store_ok = False
        self.send_msg_max_timeout_per_request = -1
        self.retry_response_codes = {
            ResponseCode.SYSTEM_ERROR, ResponseCode.SYSTEM_BUSY,
            ResponseCode.SERVICE_NOT_AVAILABLE, ResponseCode.NO_PERMISSION,
            ResponseCode.TOPIC_NOT_EXIST, ResponseCode.NO_BUYER_ID,
            ResponseCode.NOT_IN_CURRENT_UNIT, ResponseCode.GO_AWAY,
        }
        self.max_message_size = 1024 * 1024 * 4
        self.rpc_hook = rpc_hook
        self.topics = list(topics) if topics else []
        self.name_server_addrs: List[str] = []
        self._mq_client: Optional[MQClientInstance] = None
        self._started = False
        self._lock = threading.Lock()
        # The current transaction listener (recorded on send_message_in_transaction, for broker check callbacks)
        self._transaction_listener: Optional[TransactionListener] = None
        self._heartbeat_running = False
        self.heartbeat_interval_millis = 30000
        self.poll_name_server_interval = 30000
        self.send_latency_fault_enable = False
        self._mq_fault_strategy = MQFaultStrategy(False)
        self.start_detector_enable = False
        self.detect_timeout_millis = 200
        self.detect_interval_millis = 2000
        self.auto_batch = False
        self.batch_max_delay_ms = -1
        self.batch_max_bytes = -1
        self.total_batch_max_bytes = -1
        self.produce_accumulator: Optional[ProduceAccumulator] = None
        self.enable_trace = False
        self.trace_topic: Optional[str] = None      # None -> use RMQ_SYS_TRACE_TOPIC
        self.trace_msg_batch_num = 10
        self.send_message_hook_list: List["SendMessageHook"] = []
        self.end_transaction_hook_list: List["EndTransactionHook"] = []
        self.check_forbidden_hook_list: List["CheckForbiddenHook"] = []
        self.trace_dispatcher = None
        # Basic client metrics (send/consume RT and counts)
        self.metrics = ClientMetrics()
        self.request_timeout = DEFAULT_REQUEST_TIMEOUT_MILLIS
        self.async_sender_queue_capacity = 50000
        self.client_callback_executor_threads = 0
        self._async_sender_executor: Optional[ConsumeExecutor] = None
        self._callback_executor: Optional[ConsumeExecutor] = None
        self.enable_backpressure_for_async_mode = False
        self.back_pressure_for_async_send_num = 1024
        self.back_pressure_for_async_send_size = 100 * 1024 * 1024
        self._semaphore_async_send_num = FairSemaphore(
            _back_pressure_permits(self.back_pressure_for_async_send_num,
                                   MIN_ASYNC_SEND_NUM, "semaphoreAsyncSendNum"))
        self._semaphore_async_send_size = FairSemaphore(
            _back_pressure_permits(self.back_pressure_for_async_send_size,
                                   MIN_ASYNC_SEND_SIZE, "semaphoreAsyncSendSize"))

    # ---------------- Config ----------------
    def set_namesrv_addr(self, addr: str) -> None:
        self.name_server_addrs = [a.strip() for a in addr.split(";") if a.strip()]

    def set_name_server_addresses(self, addrs: List[str]) -> None:
        self.name_server_addrs = list(addrs)

    def get_namesrv_addr(self) -> str:
        return ";".join(self.name_server_addrs)

    def set_instance_name(self, name: str) -> None:
        self.instance_name = name

    def set_unit_name(self, unit_name: Optional[str]) -> None:
        pass
        self.unit_name = unit_name

    def get_unit_name(self) -> Optional[str]:
        return self.unit_name

    def set_unit_mode(self, unit_mode: bool) -> None:
        pass
        self.unit_mode = bool(unit_mode)

    def is_unit_mode(self) -> bool:
        return self.unit_mode

    def set_enable_stream_request_type(self, enable: bool) -> None:
        pass
        self.enable_stream_request_type = bool(enable)

    def set_namespace_v2(self, namespace_v2: Optional[str]) -> None:
        pass
        self.namespace_v2 = namespace_v2

    def get_namespace_v2(self) -> Optional[str]:
        return self.namespace_v2

    def set_socks_proxy_config(self, socks_proxy_config: Optional[SocksProxyConfig]) -> None:
        pass
        self.socks_proxy_config = socks_proxy_config

    def get_socks_proxy_config(self) -> Optional[SocksProxyConfig]:
        return self.socks_proxy_config

    def set_max_message_size(self, size: int) -> None:
        self.max_message_size = size

    def set_send_msg_timeout(self, timeout: int) -> None:
        self.send_msg_timeout = timeout

    def set_retry_times_when_send_failed(self, n: int) -> None:
        self.retry_times_when_send_failed = n

    def set_send_msg_max_timeout_per_request(self, timeout: int) -> None:
        self.send_msg_max_timeout_per_request = timeout

    def get_send_msg_max_timeout_per_request(self) -> int:
        return self.send_msg_max_timeout_per_request

    def set_retry_another_broker_when_not_store_ok(self, retry: bool) -> None:
        self.retry_another_broker_when_not_store_ok = retry

    def is_retry_another_broker_when_not_store_ok(self) -> bool:
        return self.retry_another_broker_when_not_store_ok

    def set_enable_backpressure_for_async_mode(self, enable: bool) -> None:
        self.enable_backpressure_for_async_mode = bool(enable)

    def is_enable_backpressure_for_async_mode(self) -> bool:
        return self.enable_backpressure_for_async_mode

    def set_back_pressure_for_async_send_num(self, num: int) -> None:
        pass
        num = max(num, MIN_ASYNC_SEND_NUM)
        self.back_pressure_for_async_send_num = num
        self._semaphore_async_send_num.set_total_permits(num)

    def get_back_pressure_for_async_send_num(self) -> int:
        return self.back_pressure_for_async_send_num

    def set_back_pressure_for_async_send_size(self, size: int) -> None:
        """Change the "in-flight bytes" cap at runtime; same semantics as ``set_back_pressure_for_async_send_num``."""
        size = max(size, MIN_ASYNC_SEND_SIZE)
        self.back_pressure_for_async_send_size = size
        self._semaphore_async_send_size.set_total_permits(size)

    def get_back_pressure_for_async_send_size(self) -> int:
        return self.back_pressure_for_async_send_size

    def get_semaphore_async_send_num_available_permits(self) -> int:
        pass
        return self._semaphore_async_send_num.available_permits()

    def get_semaphore_async_send_size_available_permits(self) -> int:
        return self._semaphore_async_send_size.available_permits()

    def add_retry_response_code(self, response_code: int) -> None:
        self.retry_response_codes.add(response_code)

    def is_retry_response_code(self, response_code: Optional[int]) -> bool:
        return response_code in self.retry_response_codes

    def set_compress_msg_body_over_howmuch(self, size: int) -> None:
        self.compress_msg_body_over_howmuch = size

    def set_compress_level(self, level: int) -> None:
        self.compress_level = level

    def set_compress_type(self, compression_type: int) -> None:
        self.compress_type = compression_type

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

    def set_send_latency_fault_enable(self, enable: bool) -> None:
        pass
        self.send_latency_fault_enable = enable
        self._mq_fault_strategy.set_send_latency_fault_enable(enable)

    def set_start_detector_enable(self, enable: bool) -> None:
        pass
        self.start_detector_enable = bool(enable)
        self._mq_fault_strategy.set_start_detector_enable(bool(enable))

    def is_start_detector_enable(self) -> bool:
        return self._mq_fault_strategy.is_start_detector_enable()

    def set_detect_timeout(self, detect_timeout_millis: float) -> None:
        pass
        self.detect_timeout_millis = detect_timeout_millis

    def get_detect_timeout(self) -> float:
        return self.detect_timeout_millis

    def set_detect_interval(self, detect_interval_millis: float) -> None:
        pass
        self.detect_interval_millis = detect_interval_millis

    def get_detect_interval(self) -> float:
        return self.detect_interval_millis

    def set_request_timeout(self, timeout_millis: int) -> None:
        """Set the default Request-Reply timeout (used by request() when no timeout is passed)."""
        self.request_timeout = timeout_millis

    def set_auto_batch(self, auto_batch: bool) -> None:
        pass
        self.auto_batch = auto_batch

    def get_auto_batch(self) -> bool:
        if self.produce_accumulator is None:
            return False
        return self.auto_batch

    def is_auto_batch(self) -> bool:
        return self.get_auto_batch()

    def set_batch_max_delay_ms(self, hold_ms: int) -> None:
        pass
        self.batch_max_delay_ms = hold_ms
        if self.produce_accumulator is not None:
            self.produce_accumulator.batch_max_delay_ms(hold_ms)

    def get_batch_max_delay_ms(self) -> int:
        if self.produce_accumulator is None:
            return 0
        return self.produce_accumulator.get_batch_max_delay_ms()

    def set_batch_max_bytes(self, hold_size: int) -> None:
        pass
        self.batch_max_bytes = hold_size
        if self.produce_accumulator is not None:
            self.produce_accumulator.batch_max_bytes(hold_size)

    def get_batch_max_bytes(self) -> int:
        if self.produce_accumulator is None:
            return 0
        return self.produce_accumulator.get_batch_max_bytes()

    def set_total_batch_max_bytes(self, total_hold_size: int) -> None:
        pass
        self.total_batch_max_bytes = total_hold_size
        if self.produce_accumulator is not None:
            self.produce_accumulator.total_batch_max_bytes(total_hold_size)

    def get_total_batch_max_bytes(self) -> int:
        if self.produce_accumulator is None:
            return 0
        return self.produce_accumulator.get_total_batch_max_bytes()

    def get_metrics(self) -> ClientMetrics:
        """Return this producer's basic metrics counters (send/consume RT and counts)."""
        return self.metrics

    def set_enable_trace(self, enable: bool) -> None:
        pass
        self.enable_trace = enable

    def is_enable_trace(self) -> bool:
        return self.enable_trace

    def set_trace_topic(self, trace_topic: Optional[str]) -> None:
        pass
        self.trace_topic = trace_topic

    def set_trace_msg_batch_num(self, n: int) -> None:
        self.trace_msg_batch_num = n

    def register_send_message_hook(self, hook: "SendMessageHook") -> None:
        pass
        if hook is not None:
            self.send_message_hook_list.append(hook)

    def has_send_message_hook(self) -> bool:
        return len(self.send_message_hook_list) > 0

    def register_check_forbidden_hook(self, hook: "CheckForbiddenHook") -> None:
        pass
        if hook is not None:
            self.check_forbidden_hook_list.append(hook)

    def has_check_forbidden_hook(self) -> bool:
        return len(self.check_forbidden_hook_list) > 0

    def _has_send_interceptors(self) -> bool:
        """Whether the send kernel must run with interceptors/hooks (either being present requires it)."""
        return bool(self.send_message_hook_list) or bool(self.check_forbidden_hook_list)

    def execute_check_forbidden_hook(self, context: "CheckForbiddenContext") -> None:
        pass
        if not self.has_check_forbidden_hook():
            return
        for hook in self.check_forbidden_hook_list:
            hook.check_forbidden(context)

    def register_end_transaction_hook(self, hook: "EndTransactionHook") -> None:
        pass
        if hook is not None:
            self.end_transaction_hook_list.append(hook)

    def execute_end_transaction_hook(self, context: "EndTransactionContext") -> None:
        for hook in self.end_transaction_hook_list:
            try:
                hook.end_transaction(context)
            except Exception as e:  # noqa: BLE001
                logger.warning("failed to executeEndTransactionHook: %s", e)

    def execute_send_message_hook_before(self, context: SendMessageContext) -> None:
        pass
        for hook in self.send_message_hook_list:
            try:
                hook.send_message_before(context)
            except Exception as e:  # noqa: BLE001
                logger.warning("failed to executeSendMessageHookBefore: %s", e)

    def execute_send_message_hook_after(self, context: SendMessageContext) -> None:
        for hook in self.send_message_hook_list:
            try:
                hook.send_message_after(context)
            except Exception as e:  # noqa: BLE001
                logger.warning("failed to executeSendMessageHookAfter: %s", e)

    def _build_send_context(self, msg: Message, mq: MessageQueue,
                            broker_addr: str,
                            communication_mode: str = CommunicationMode.SYNC) -> SendMessageContext:
        pass
        context = SendMessageContext()
        context.producer = self
        context.producer_group = self.producer_group
        context.message = msg
        context.mq = mq
        context.broker_addr = broker_addr
        context.namespace = self.namespace
        context.communication_mode = communication_mode
        if msg.get_property(MessageConst.PROPERTY_TRANSACTION_PREPARED) == "true":
            context.msg_type = MessageType.TRANS_MSG_HALF
        for key in ("__STARTDELIVERTIME", MessageConst.PROPERTY_DELAY_TIME_LEVEL,
                    "TIMER_DELIVER_MS", "TIMER_DELAY_SEC", "TIMER_DELAY_MS"):
            if msg.get_property(key) is not None:
                context.msg_type = MessageType.DELAY_MSG
                break
        return context

    def _execute_check_forbidden(self, msg: Message, mq: MessageQueue, broker_addr: str,
                                 arg=None,
                                 communication_mode: str = CommunicationMode.SYNC) -> None:
        """Build a CheckForbiddenContext and execute it (exceptions are **not** swallowed; see execute_check_forbidden_hook)."""
        context = CheckForbiddenContext()
        context.name_srv_addr = self.get_namesrv_addr()
        context.group = self.producer_group
        context.communication_mode = communication_mode
        context.broker_addr = broker_addr
        context.message = msg
        context.mq = mq
        context.unit_mode = self.unit_mode
        context.arg = arg
        self.execute_check_forbidden_hook(context)

    def _send_header_args(self) -> dict:
        pass
        return {
            "unit_mode": self.unit_mode,
            "default_topic": self.create_topic_key,
            "default_topic_queue_nums": self.default_topic_queue_nums,
        }

    def _send_with_hooks(self, client: MQClientInstance, msg: Message, mq_sel: MessageQueue,
                         timeout: int, sys_flag: int, arg=None,
                         communication_mode: str = CommunicationMode.SYNC) -> SendResult:
        pass
        broker_addr = client.publish_addr_for(mq_sel.broker_name, mq_sel.topic)
        if self.has_check_forbidden_hook():
            self._execute_check_forbidden(msg, mq_sel, broker_addr, arg, communication_mode)
        # W3C traceparent pass-through (opt-in): inject a root context when absent; never overwrite an existing value
        if self.enable_trace_context:
            inject_trace_context(msg)
        if not self.send_message_hook_list:
            return client.send_message(self.producer_group, msg, mq_sel, timeout, sys_flag,
                                       **self._send_header_args())
        context = self._build_send_context(msg, mq_sel, broker_addr, communication_mode)
        self.execute_send_message_hook_before(context)
        try:
            result = client.send_message(self.producer_group, msg, mq_sel, timeout, sys_flag,
                                           **self._send_header_args())
        except Exception as e:  # noqa: BLE001
            context.exception = e
            self.execute_send_message_hook_after(context)
            raise
        context.send_result = result
        self.execute_send_message_hook_after(context)
        return result

    # ---------------- Lifecycle ----------------
    def start(self) -> None:
        with self._lock:
            if self._started:
                return
            if self.namespace:
                self.producer_group = NamespaceUtil.wrap_namespace(self.namespace, self.producer_group)
            validators.check_group(self.producer_group)
            if self.producer_group == MixAll.DEFAULT_PRODUCER_GROUP:
                raise MQClientException(
                    "producerGroup can not equal %s, please specify another one."
                    % MixAll.DEFAULT_PRODUCER_GROUP)
            if not self.name_server_addrs and not DefaultTopAddressing.is_configured():
                raise MQClientException("name server address is not set",
                                        ClientErrorCode.NO_NAME_SERVER_EXCEPTION)
            self.instance_name = MixAll.change_instance_name_to_pid(self.instance_name)
            if self.client_id is None:
                self.client_id = MixAll.client_id_for(self.instance_name, self.unit_name,
                                                      self.enable_stream_request_type)
            def _create_instance() -> MQClientInstance:
                return MQClientInstance(
                    self.client_id, self.name_server_addrs,
                    tls_enable=self.tls_enable,
                    tls_options=self.tls_options,
                    socks_proxy_config=self.socks_proxy_config,
                    enable_stream_request_type=self.enable_stream_request_type,
                    unit_name=self.unit_name,
                    namespace_v2=self.namespace_v2,
                    poll_name_server_interval=self.poll_name_server_interval)

            self._mq_client, created = MQClientManager.get_instance() \
                .get_or_create_mq_client_instance(self.client_id, _create_instance)
            if created and self.rpc_hook is not None:
                self._mq_client.remoting_client.register_rpc_hook(self.rpc_hook)
            manager = MQClientManager.get_instance()
            manager.register_client(self.client_id, self._mq_client)
            try:
                self._mq_client.start()
            except Exception:
                # Startup failure must not leave a dangling reference count (the instance also removes itself from the cache).
                manager.unregister_client(self.client_id, self._mq_client)
                raise
            # Dynamic name server: the instance may already have addresses from the addressing server on startup; back-fill this producer
            if not self.name_server_addrs and self._mq_client.name_server_addrs:
                self.name_server_addrs = list(self._mq_client.name_server_addrs)
            self._mq_client.register_transaction_check_handler(
                self._handle_check_transaction_state)
            self._create_async_executors()
            self._setup_reachability_detector()
            REQUEST_FUTURE_HOLDER.start_scheduled_task(self)
            self.init_produce_accumulator()
            self._started = True
            if self.produce_accumulator is not None:
                self.produce_accumulator.start()
            # Heartbeat thread: periodically register ProducerData with the broker.
            # The broker's transaction check callback reaches the producer through the channel registered here;
            # without a producer heartbeat COMMIT/ROLLBACK still succeed (the client sends END_TRANSACTION itself),
            # but half-messages in UNKNOW state would **never be checked back**.
            self._heartbeat_running = True
            threading.Thread(target=self._heartbeat_loop, name="ProducerHeartbeatThread",
                             daemon=True).start()
        self._start_trace_dispatcher()

    def _setup_reachability_detector(self) -> None:
        pass
        tolerance = self._mq_fault_strategy.latency_fault_tolerance
        tolerance.set_detect_timeout(self.detect_timeout_millis)
        tolerance.set_detect_interval(self.detect_interval_millis)
        tolerance.resolver = self._detector_resolver
        tolerance.service_detector = self._detector_probe
        self._mq_fault_strategy.set_start_detector_enable(self.start_detector_enable)
        self._mq_fault_strategy.start_detector()

    def _start_trace_dispatcher(self) -> None:
        pass
        if self.enable_trace:
            try:
                dispatcher = AsyncTraceDispatcher(
                    self.producer_group, TraceDispatcherType.PRODUCE,
                    self.trace_msg_batch_num, self.trace_topic, self.rpc_hook)
                dispatcher.set_host_producer(self)
                dispatcher.set_namespace_v2(self.namespace_v2)
                self.trace_dispatcher = dispatcher
                self.register_send_message_hook(SendMessageTraceHook(dispatcher))
                self.register_end_transaction_hook(EndTransactionTraceHook(dispatcher))
            except Exception as e:  # noqa: BLE001
                logger.error("system mqtrace hook init failed ,maybe can't send msg trace data: %s", e)
        if self.trace_dispatcher is not None:
            try:
                self.trace_dispatcher.start(self.get_namesrv_addr(), AccessChannel.LOCAL)
            except Exception as e:  # noqa: BLE001
                logger.warning("trace dispatcher start failed: %s", e)

    def _detector_resolver(self, broker_name: str) -> Optional[str]:
        pass
        client = self._mq_client
        if client is None:
            return None
        return client.find_broker_address_in_publish(broker_name)

    def _detector_probe(self, addr: str, timeout_millis: float) -> bool:
        pass
        client = self._mq_client
        if client is None:
            return False
        topic = next(iter(client.topic_route_table), None)
        if topic is None:
            return False
        try:
            client.get_max_offset(MessageQueue(topic, None, 0), int(timeout_millis), addr=addr)
            return True
        except Exception:
            return False

    def shutdown(self) -> None:
        with self._lock:
            if not self._started:
                return
            self._heartbeat_running = False
            self._mq_fault_strategy.shutdown()
            REQUEST_FUTURE_HOLDER.shutdown_scheduled_task(self)
            if self._async_sender_executor is not None:
                self._async_sender_executor.shutdown()
                self._async_sender_executor = None
            if self._callback_executor is not None:
                self._callback_executor.shutdown()
                self._callback_executor = None
            if self._mq_client is not None:
                try:
                    self._mq_client.unregister_client_all_brokers(
                        self.client_id or "", self.producer_group, "")
                except Exception as e:  # noqa: BLE001
                    logger.debug("unregister on shutdown failed: %s", e)
                # Detach this producer's handler for request code 39 (CHECK_TRANSACTION_STATE; skipped when the shadow instance has no such interface).
                unregister_handler = getattr(self._mq_client,
                                             "unregister_transaction_check_handler", None)
                if unregister_handler is not None:
                    unregister_handler(self._handle_check_transaction_state)
                MQClientManager.get_instance().unregister_client(
                    self.client_id or "", self._mq_client)
            self._started = False
            if self.produce_accumulator is not None:
                self.produce_accumulator.shutdown()
        if self.trace_dispatcher is not None:
            try:
                self.trace_dispatcher.shutdown()
            except Exception as e:  # noqa: BLE001
                logger.warning("trace dispatcher shutdown failed: %s", e)

    def _heartbeat_loop(self) -> None:
        pass
        interval_sec = max(1, self.heartbeat_interval_millis // 1000)
        while self._heartbeat_running:
            try:
                self._send_heartbeat_to_all_broker()
            except Exception:  # noqa: BLE001
                logger.debug("producer heartbeat failed", exc_info=True)
            for _ in range(interval_sec):
                if not self._heartbeat_running:
                    return
                time.sleep(1)

    def _send_heartbeat_to_all_broker(self) -> int:
        if self._mq_client is None:
            return 0
        try:
            addrs = self._mq_client.get_route_of_all_brokers()
        except Exception as e:  # noqa: BLE001
            logger.warning("producer heartbeat: gather brokers failed: %s", e)
            return 0
        if not addrs:
            return 0
        hb = HeartbeatData(self.client_id or "")
        pd = ProducerData(self.producer_group)
        # WARNING: the Python HeartbeatData has no heartbeatFingerprint / withoutSub fields
        # (known gap, see project memory). When absent, the broker deserializes them as 0, which is equivalent to the V1 registration path;
        # explicitly setting 0 has the same effect as omitting it, so the status quo is kept.
        hb.producer_data_set.add(pd)
        ok_count = 0
        for addr in addrs:
            try:
                self._mq_client.send_heartbeat(addr, hb, 5000)
                ok_count += 1
            except Exception as e:  # noqa: BLE001
                logger.warning("producer heartbeat to %s failed: %s", addr, e)
        return ok_count

    def _require_client(self) -> MQClientInstance:
        if not self._started or self._mq_client is None:
            raise MQClientException("producer not started, call start() first")
        return self._mq_client

    def try_to_compress_message(self, msg: Message) -> int:
        pass
        if isinstance(msg, MessageBatch):
            return 0
        body = msg.get_body()
        if not body or len(body) < self.compress_msg_body_over_howmuch:
            return 0
        try:
            compressed = _compress(body, self.compress_type, self.compress_level)
        except Exception as e:
            logger.warning("tryToCompressMessage failed, send uncompressed: %s", e)
            return 0
        if not compressed:
            return 0
        msg.set_body(compressed)
        sys_flag = MessageSysFlag.COMPRESSED_FLAG
        return MessageSysFlag.set_compression_type(sys_flag, self.compress_type)

    def _with_namespace(self, topic: str) -> str:
        pass
        if not self.namespace:
            return topic
        return NamespaceUtil.wrap_namespace(self.namespace, topic)

    def _check_pinned_topic(self, topic: str, mq: MessageQueue, message: str) -> None:
        pass
        if self._with_namespace(mq.topic) != topic:
            raise MQClientException(message)

    def _topic_publish_info(self, topic: str) -> "TopicPublishInfo":
        pass
        client = self._require_client()
        client.register_topic_in_use(topic)
        try:
            return client.get_topic_publish_info(topic)
        except MQClientException as e:
            try:
                return client.get_topic_publish_info(topic, is_default=True)
            except MQClientException:
                self._validate_name_server_setting()
                raise

    def _validate_name_server_setting(self) -> None:
        pass
        client = self._mq_client
        if client is None or not client.name_server_addrs:
            raise MQClientException("No name server address, please set it.",
                                    ClientErrorCode.NO_NAME_SERVER_EXCEPTION)

    def _update_fault_item(self, selected, began: float, isolation: bool,
                           reachable: bool) -> None:
        """Record one attempt's latency in the fault table. The latency must be measured with a monotonic clock: a sub-millisecond
        local round trip computed from a millisecond wall-clock difference would be recorded as 0, so the latency threshold would never take effect."""
        if selected is None:
            return
        self._mq_fault_strategy.update_fault_item(
            selected.broker_name, (time.monotonic() - began) * 1000.0, isolation, reachable)

    # ---------------- Normal sending ----------------
    @_restores_caller_message
    def send(self, msg: Message, timeout_millis: Optional[int] = None,
             mq: Optional[MessageQueue] = None) -> SendResult:
        """Synchronous send: a single msg or a Collection; a specified mq goes to a pinned send, otherwise round-robin selection is used."""
        if (timeout_millis is None and self.get_auto_batch()
                and not isinstance(msg, (list, tuple))
                and not isinstance(msg, MessageBatch)):
            return self.send_by_accumulator(msg, mq, None)
        client = self._require_client()
        timeout = timeout_millis if timeout_millis is not None else self.send_msg_timeout
        if isinstance(msg, (list, tuple)):
            return self._send_batch(list(msg), mq, timeout)
        msg.topic = self._with_namespace(msg.topic)
        self._check_message(msg)
        if mq is not None:
            self._check_pinned_topic(msg.topic, mq, _PINNED_TOPIC_MISMATCH_SYNC)
        sys_flag = self.try_to_compress_message(msg)
        if mq is not None:
            if self._has_send_interceptors():
                return self._send_with_hooks(client, msg, mq, timeout, sys_flag)
            return client.send_message(self.producer_group, msg, mq, timeout, sys_flag,
                                   **self._send_header_args())
        try:
            publish = self._topic_publish_info(msg.topic)
        except MQClientException as e:
            raise MQClientException(str(e),
                                    e.response_code or ClientErrorCode.NOT_FOUND_TOPIC_EXCEPTION)
        times_total = self.retry_times_when_send_failed + 1
        begin_first = time.monotonic()
        brokers_sent: List[str] = []
        last_broker_name: Optional[str] = None
        result: Optional[SendResult] = None
        last_exc: Optional[Exception] = None
        call_timeout = False
        for attempt in range(times_total):
            selected = None
            began = time.monotonic()
            try:
                selected = self._mq_fault_strategy.select_one_message_queue(
                    publish, last_broker_name, attempt > 0)
                if selected is None:
                    break
                last_broker_name = selected.broker_name
                brokers_sent.append(selected.broker_name)
                mq_sel = MessageQueue(msg.topic, selected.broker_name, selected.queue_id)
                began = time.monotonic()
                cost_time = int((began - begin_first) * 1000)
                if timeout < cost_time:
                    call_timeout = True
                    break
                cur_timeout = timeout - cost_time
                can_retry_again = attempt + 1 < times_total
                if (self.send_msg_max_timeout_per_request > -1 and can_retry_again
                        and cur_timeout > self.send_msg_max_timeout_per_request):
                    cur_timeout = self.send_msg_max_timeout_per_request
                send_start = self.metrics.record_send_start()
                try:
                    result = self._send_with_hooks(client, msg, mq_sel, cur_timeout, sys_flag)
                except Exception:  # noqa: BLE001 — re-raise as the original exception after metric accounting
                    self.metrics.record_send_failure(send_start)
                    raise
                self.metrics.record_send_success(send_start)
                # Record the send latency; exceeding the threshold isolates that broker for a while
                self._update_fault_item(selected, began, False, True)
                if (result is not None and result.send_status != SendStatus.SEND_OK
                        and self.retry_another_broker_when_not_store_ok):
                    continue
                return result
            except MQBrokerException as e:
                # The broker returned an explicit error code: isolate that broker (reachability unchanged); only a retryable code switches to another broker
                self._update_fault_item(selected, began, True, False)
                last_exc = e
                if self.is_retry_response_code(e.response_code):
                    continue
                if result is not None:
                    return result
                raise
            except RemotingException as e:
                self._update_fault_item(selected, began, True,
                                        not self._mq_fault_strategy.is_start_detector_enable())
                last_exc = e
            except MQClientException as e:
                self._update_fault_item(selected, began, False, True)
                last_exc = e

        if result is not None:
            return result
        if call_timeout:
            raise RemotingTooMuchRequestException("sendDefaultImpl call timeout")
        info = ("Send [%d] times, still failed, cost [%d]ms, Topic: %s, BrokersSent: [%s]"
                ", last error: %s" % (len(brokers_sent),
                                      int((time.monotonic() - begin_first) * 1000),
                                      msg.topic, ", ".join(brokers_sent),
                                      last_exc if last_exc is not None else ""))
        if isinstance(last_exc, MQBrokerException):
            code = last_exc.response_code
        elif isinstance(last_exc, RemotingConnectException):
            code = ClientErrorCode.CONNECT_BROKER_EXCEPTION
        elif isinstance(last_exc, RemotingTimeoutException):
            code = ClientErrorCode.ACCESS_BROKER_TIMEOUT
        elif isinstance(last_exc, MQClientException):
            code = ClientErrorCode.BROKER_NOT_EXIST_EXCEPTION
        else:
            code = None
        raise MQClientException(info, code, last_exc)

    def request(self, msg: Message, timeout_millis: Optional[int] = None,
                mq: Optional[MessageQueue] = None,
                selector: Optional[MessageQueueSelector] = None, arg=None) -> Message:
        pass
        _check_request_send_args(mq, selector)
        timeout = timeout_millis if timeout_millis is not None else self.request_timeout
        msg.topic = self._with_namespace(msg.topic)
        self._check_message(msg)

        correlation_id = create_correlation_id()
        client = self._require_client()
        msg.put_property(MessageConst.PROPERTY_CORRELATION_ID, correlation_id)
        msg.put_property(MessageConst.PROPERTY_MESSAGE_REPLY_TO_CLIENT, client.client_id)
        msg.put_property(MessageConst.PROPERTY_MESSAGE_TTL, str(timeout))

        begin = time.time() * 1000.0
        try:
            self._topic_publish_info(msg.topic)
            self._send_heartbeat_to_all_broker()
        except Exception:  # noqa: BLE001 — when the route is unavailable, let the send path below report the error itself
            logger.debug("request: prepare route/heartbeat failed", exc_info=True)

        future = RequestResponseFuture(correlation_id, timeout)
        REQUEST_FUTURE_HOLDER.put_request(correlation_id, future)
        cost = int(time.time() * 1000.0 - begin)
        try:
            self.send_async(msg, _NullSendCallback(future),
                            timeout - cost if timeout > cost else timeout, mq,
                            selector=selector, arg=arg)
            return self._wait_request_response(msg, timeout, future, cost)
        finally:
            REQUEST_FUTURE_HOLDER.remove_request(correlation_id)

    def _wait_request_response(self, msg: Message, timeout: int,
                               future: RequestResponseFuture, cost: int) -> Message:
        pass
        response = future.wait_response_message(timeout - cost)
        if response is None:
            if future.send_request_ok:
                raise RequestTimeoutException(
                    "send request message to <%s> OK, but wait reply message timeout, %d ms."
                    % (msg.topic, timeout), ClientErrorCode.REQUEST_TIMEOUT_EXCEPTION)
            raise MQClientException(
                "send request message to <%s> fail" % msg.topic, None, future.cause)
        return response

    def request_async(self, msg: Message, callback: RequestCallback,
                      timeout_millis: Optional[int] = None,
                      mq: Optional[MessageQueue] = None,
                      selector: Optional[MessageQueueSelector] = None, arg=None) -> None:
        pass
        _check_request_send_args(mq, selector)
        timeout = timeout_millis if timeout_millis is not None else self.request_timeout
        msg.topic = self._with_namespace(msg.topic)
        self._check_message(msg)

        correlation_id = create_correlation_id()
        client = self._require_client()
        msg.put_property(MessageConst.PROPERTY_CORRELATION_ID, correlation_id)
        msg.put_property(MessageConst.PROPERTY_MESSAGE_REPLY_TO_CLIENT, client.client_id)
        msg.put_property(MessageConst.PROPERTY_MESSAGE_TTL, str(timeout))

        begin = time.time() * 1000.0
        try:
            self._topic_publish_info(msg.topic)
            self._send_heartbeat_to_all_broker()
        except Exception:  # noqa: BLE001 — same as the synchronous request: let the send path report the error itself
            logger.debug("request_async: prepare route/heartbeat failed", exc_info=True)

        future = RequestResponseFuture(correlation_id, timeout, callback)
        REQUEST_FUTURE_HOLDER.put_request(correlation_id, future)
        cost = int(time.time() * 1000.0 - begin)
        try:
            self.send_async(msg, _RequestAsyncSendCallback(future, correlation_id),
                            timeout - cost if timeout > cost else timeout, mq,
                            selector=selector, arg=arg)
        except BaseException as e:
            future.cause = e
            removed = REQUEST_FUTURE_HOLDER.remove_request(correlation_id)
            if removed is not None:
                removed.send_request_ok = False
                removed.put_response_message(None)
                removed.execute_request_callback()

    def _create_async_executors(self) -> None:
        pass
        cores = os.cpu_count() or 1
        self._async_sender_executor = ConsumeExecutor(
            cores, cores, thread_name_prefix="AsyncSenderExecutor",
            max_queue_size=self.async_sender_queue_capacity,
            thread_name_sep="_", thread_index_from=1)
        callback_threads = self.client_callback_executor_threads
        if callback_threads <= 0:
            callback_threads = cores
        self._callback_executor = ConsumeExecutor(
            callback_threads, callback_threads,
            thread_name_prefix="NettyClientPublicExecutor",
            thread_name_sep="_", thread_index_from=1)

    def send_async(self, msg: Message, callback: SendCallback,
                   timeout_millis: Optional[int] = None,
                   mq: Optional[MessageQueue] = None,
                   selector: Optional[MessageQueueSelector] = None, arg=None) -> None:
        pass
        if (timeout_millis is None and self.get_auto_batch()
                and not isinstance(msg, (list, tuple))
                and not isinstance(msg, MessageBatch)):
            try:
                self.send_by_accumulator(msg, mq, callback)
            except BaseException as e:
                callback.on_exception(e)
            return
        self._require_client()
        _check_request_send_args(mq, selector)
        executor = self._async_sender_executor
        if executor is None:
            raise MQClientException("producer already shutdown")
        timeout = timeout_millis if timeout_millis is not None else self.send_msg_timeout
        begin = time.monotonic()
        gated = _BackPressureSendCallback(
            callback, self._semaphore_async_send_num, self._semaphore_async_send_size,
            _back_pressure_msg_len(msg))

        def _run() -> None:
            cost = int((time.monotonic() - begin) * 1000)
            if timeout <= cost:
                self._complete(gated, None,
                               RemotingTooMuchRequestException(
                                   "DEFAULT ASYNC send call timeout"),
                               None)
                return
            try:
                if selector is None:
                    # The regular path keeps the old call shape (selector/arg omitted), so positional
                    # capture of inner by existing test stubs does not break
                    self._send_async_inner(msg, mq, gated, timeout - cost)
                else:
                    self._send_async_inner(msg, mq, gated, timeout - cost,
                                           selector=selector, arg=arg)
            except Exception as e:
                self._complete(gated, None, e, None)

        self._execute_async_message_send(_run, gated, timeout, begin, executor)

    def _execute_async_message_send(self, runnable: Callable[[], None],
                                    gated: _BackPressureSendCallback, timeout: int,
                                    begin: float, executor: ConsumeExecutor) -> None:
        pass
        if self.enable_backpressure_for_async_mode:
            cost = int((time.monotonic() - begin) * 1000)
            gated.num_acquired = timeout - cost > 0 and self._semaphore_async_send_num.try_acquire(
                1, timeout - cost)
            if not gated.num_acquired:
                self._complete(gated, None, RemotingTooMuchRequestException(
                    "send message tryAcquire semaphoreAsyncNum timeout"), None)
                return

            cost = int((time.monotonic() - begin) * 1000)
            gated.size_acquired = (timeout - cost > 0
                                   and self._semaphore_async_send_size.try_acquire(
                                       gated.msg_len, timeout - cost))
            if not gated.size_acquired:
                self._complete(gated, None, RemotingTooMuchRequestException(
                    "send message tryAcquire semaphoreAsyncSize timeout"), None)
                return

        try:
            executor.submit(runnable)
        except RejectedExecutionError as e:
            if self.enable_backpressure_for_async_mode:
                runnable()
            else:
                raise MQClientException("executor rejected", None, e) from e

    @_restores_caller_message
    def _send_async_inner(self, msg: Message, mq: Optional[MessageQueue],
                          callback: SendCallback, timeout: int,
                          selector: Optional[MessageQueueSelector] = None, arg=None) -> None:
        pass
        client = self._require_client()
        if isinstance(msg, (list, tuple)):
            if selector is not None:
                raise MQClientException("selector is not supported for batch messages")
            if mq is None:
                result = self._send_batch(list(msg), mq, timeout)
            else:
                result = self._send_batch(list(msg), mq, timeout, _PINNED_TOPIC_MISMATCH_ASYNC)
            self._execute_on_callback_thread(
                lambda: self._complete(callback, result, None, None))
            return
        msg.topic = self._with_namespace(msg.topic)
        self._check_message(msg)
        if selector is not None:
            # sendSelectImpl ASYNC: the selector receives the **raw** queue list from the route
            publish = self._topic_publish_info(msg.topic)
            selected = selector.select(publish.msg_queue_list, msg, arg)
            if selected is None:
                raise MQClientException("selector selected no queue for topic %s" % msg.topic)
            mq_sel = MessageQueue(msg.topic, selected.broker_name, selected.queue_id)
            self._check_pinned_topic(msg.topic, mq_sel, _PINNED_TOPIC_MISMATCH_ASYNC)
            sys_flag = self.try_to_compress_message(msg)
            self._send_kernel_async(client, msg, mq_sel, callback, timeout, sys_flag, None)
            return
        if mq is not None:
            self._check_pinned_topic(msg.topic, mq, _PINNED_TOPIC_MISMATCH_ASYNC)
        # Same as the synchronous send: compression happens once outside the retry chain, so a retry does not compress an already-compressed body again
        sys_flag = self.try_to_compress_message(msg)
        if mq is not None:
            self._send_kernel_async(client, msg, mq, callback, timeout, sys_flag, None)
            return
        try:
            publish = self._topic_publish_info(msg.topic)
        except MQClientException as e:
            # Same as the synchronous send: 10004 is passed through as-is, everything else is classified as 10005
            raise MQClientException(str(e),
                                    e.response_code or ClientErrorCode.NOT_FOUND_TOPIC_EXCEPTION)
        selected = self._mq_fault_strategy.select_one_message_queue(publish, None, False)
        if selected is None:
            raise MQClientException(
                "Send [0] times, still failed, Topic: %s, BrokersSent: []" % msg.topic)
        self._send_kernel_async(client, msg,
                                MessageQueue(msg.topic, selected.broker_name, selected.queue_id),
                                callback, timeout, sys_flag, publish)

    def _send_kernel_async(self, client: MQClientInstance, msg: Message, mq: MessageQueue,
                           callback: SendCallback, timeout: int, sys_flag: int,
                           publish) -> None:
        pass
        began = time.monotonic()
        broker_addr = client.publish_addr_for(mq.broker_name, mq.topic)
        if self.has_check_forbidden_hook():
            self._execute_check_forbidden(msg, mq, broker_addr, None, CommunicationMode.ASYNC)
        if self.enable_trace_context:
            inject_trace_context(msg)
        request = client.build_send_request(self.producer_group, msg, mq, timeout, sys_flag,
                                            **self._send_header_args())
        context = None
        if self.send_message_hook_list:
            context = self._build_send_context(msg, mq, broker_addr, CommunicationMode.ASYNC)
            self.execute_send_message_hook_before(context)
        cost_async = int((time.monotonic() - began) * 1000)
        if timeout < cost_async:
            self._complete(callback, None,
                           RemotingTooMuchRequestException("sendKernelImpl call timeout"),
                           context)
            return
        self._send_message_async(client, broker_addr, mq.broker_name, mq, msg, request,
                                 timeout - cost_async, callback, publish, context, [0])

    def _send_message_async(self, client: MQClientInstance, addr: str, broker_name: str,
                            mq: MessageQueue, msg: Message, request: RemotingCommand,
                            timeout: int, callback: SendCallback, publish, context,
                            times: List[int]) -> None:
        pass
        began = time.monotonic()

        def _handle(result: Optional[SendResult], error: Optional[BaseException]) -> None:
            cost = int((time.monotonic() - began) * 1000)
            if error is None:
                self._update_fault_item(mq, began, False, True)
                self._complete(callback, result, None, context)
                return
            self._update_fault_item(mq, began, True,
                                    not self._mq_fault_strategy.is_start_detector_enable())
            wrapped, need_retry = _classify_async_failure(error, cost)
            self._on_send_exception(client, broker_name, mq, msg, request, timeout - cost,
                                   callback, publish, context, times, wrapped, need_retry)

        def _on_complete(result: Optional[SendResult],
                         error: Optional[BaseException]) -> None:
            self._execute_on_callback_thread(lambda: _handle(result, error))

        try:
            client.send_message_async(addr, request, msg, mq, timeout, _on_complete)
        except Exception as e:  # noqa: BLE001
            cost = int((time.monotonic() - began) * 1000)
            self._update_fault_item(mq, began, True, False)
            self._on_send_exception(client, broker_name, mq, msg, request, timeout - cost,
                                   callback, publish, context, times, e, True)

    def _on_send_exception(self, client: MQClientInstance, broker_name: str, mq: MessageQueue,
                           msg: Message, request: RemotingCommand, timeout: int,
                           callback: SendCallback, publish, context, times: List[int],
                           error: BaseException, need_retry: bool) -> None:
        pass
        times[0] += 1
        attempt = times[0]
        if need_retry and attempt <= self.retry_times_when_send_async_failed and timeout > 0:
            retry_broker = broker_name
            retry_mq = mq
            if publish is not None:
                selected = self._mq_fault_strategy.select_one_message_queue(publish, broker_name,
                                                                           False)
                if selected is not None:
                    retry_mq = MessageQueue(msg.topic, selected.broker_name, selected.queue_id)
                    retry_broker = selected.broker_name
            addr = client.find_broker_address_in_publish(retry_broker)
            if not addr:
                self._complete(callback, None,
                               MQClientException("The broker[%s] not exist" % retry_broker),
                               context)
                return
            logger.warning("async send msg by retry %d times. topic=%s, brokerAddr=%s, "
                           "brokerName=%s: %s", attempt, msg.topic, addr, retry_broker, error)
            # Use a new opaque: the old request is still sitting in responseTable waiting to time out, and reusing it would cross the two attempts' replies
            request.opaque = RemotingCommand.create_new_request_id()
            self._send_message_async(client, addr, retry_broker, retry_mq, msg, request, timeout,
                                     callback, publish, context, times)
            return
        self._complete(callback, None, error, context)

    def _execute_on_callback_thread(self, fn: Callable[[], None]) -> None:
        pass
        pool = self._callback_executor
        if pool is None:
            fn()
            return
        try:
            pool.submit(fn)
        except RejectedExecutionError:
            fn()

    def _complete(self, callback: SendCallback, result: Optional[SendResult],
                  error: Optional[BaseException], context) -> None:
        pass
        if context is not None:
            if error is None:
                context.send_result = result
            else:
                context.exception = error
            self.execute_send_message_hook_after(context)
        try:
            if error is None:
                callback.on_success(result)
            else:
                callback.on_exception(error)
        except Exception as e:  # noqa: BLE001
            logger.warning("send callback raised: %s", e)

    @_restores_caller_message
    def send_oneway(self, msg: Message, mq: Optional[MessageQueue] = None) -> None:
        pass
        client = self._require_client()
        msg.topic = self._with_namespace(msg.topic)
        self._check_message(msg)
        sys_flag = self.try_to_compress_message(msg)
        if mq is not None:
            if self.has_check_forbidden_hook():
                self._execute_check_forbidden(msg, mq, self._need_addr(client, mq),
                                              None, CommunicationMode.ONEWAY)
            client.send_message_oneway(self.producer_group, msg, mq,
                                       self._need_addr(client, mq), self.send_msg_timeout,
                                       sys_flag, **self._send_header_args())
            return
        publish = self._topic_publish_info(msg.topic)
        selected = self._mq_fault_strategy.select_one_message_queue(publish, None)
        mq_sel = MessageQueue(msg.topic, selected.broker_name, selected.queue_id)
        if self.has_check_forbidden_hook():
            self._execute_check_forbidden(msg, mq_sel, self._need_addr(client, mq_sel),
                                          None, CommunicationMode.ONEWAY)
        client.send_message_oneway(self.producer_group, msg, mq_sel,
                                   self._need_addr(client, mq_sel), self.send_msg_timeout,
                                   sys_flag, **self._send_header_args())

    @_restores_caller_message
    def send_by_selector(self, msg: Message, selector: MessageQueueSelector, arg,
                         timeout_millis: Optional[int] = None) -> SendResult:
        pass
        client = self._require_client()
        timeout = timeout_millis if timeout_millis is not None else self.send_msg_timeout
        msg.topic = self._with_namespace(msg.topic)
        publish = self._topic_publish_info(msg.topic)
        selected = selector.select(publish.msg_queue_list, msg, arg)
        mq_sel = MessageQueue(msg.topic, selected.broker_name, selected.queue_id)
        # The selector uses the original message (topic/business fields); compression only affects the body
        self._check_message(msg)
        sys_flag = self.try_to_compress_message(msg)
        if self._has_send_interceptors():
            return self._send_with_hooks(client, msg, mq_sel, timeout, sys_flag, arg=arg)
        return client.send_message(self.producer_group, msg, mq_sel, timeout, sys_flag,
                                       **self._send_header_args())

    def send_batch_by_selector(self, msgs: List[Message], selector: MessageQueueSelector,
                               arg, timeout_millis: Optional[int] = None) -> SendResult:
        pass
        client = self._require_client()
        msgs = list(msgs)
        if not msgs:
            raise MQClientException("message list is empty")
        topic = msgs[0].topic
        for m in msgs:
            if m.topic != topic:
                raise MQClientException(
                    "all messages in one batch must have the same topic")
        # Same order as send_by_selector: append the namespace first, then look up publish info (wrap_namespace per message
        # inside _send_batch is idempotent, so double-wrapping is harmless)
        topic = self._with_namespace(topic)
        publish = self._topic_publish_info(topic)
        selected = selector.select(publish.msg_queue_list, msgs, arg)
        if selected is None:
            raise MQClientException("selector selected no queue for topic %s" % topic)
        mq_sel = MessageQueue(topic, selected.broker_name, selected.queue_id)
        timeout = timeout_millis if timeout_millis is not None else self.send_msg_timeout
        return self._send_batch(msgs, mq_sel, timeout)

    def recall_message(self, topic: str, recall_handle: str) -> str:
        pass
        client = self._require_client()
        topic = self._with_namespace(topic)
        validators.check_topic(topic)
        if MixAll.is_retry_topic(topic) or MixAll.is_dlq_topic(topic):
            raise MQClientException("topic is not supported")
        handle = recall_message_handle.decode_handle(recall_handle)
        self._topic_publish_info(topic)
        addr = client.find_broker_address_in_publish(handle.broker_name)
        if addr is None:
            addr = client.find_broker_addr_by_topic(topic)
        if addr is None:
            logger.warning("can't find broker service address. %s", handle.broker_name)
            raise MQClientException("The broker service address not found")
        header = RecallMessageRequestHeader()
        header.producer_group = self.producer_group
        header.topic = topic
        header.recall_handle = recall_handle
        header.bname = handle.broker_name
        return client.recall_message(addr, header, self.send_msg_timeout)

    def init_produce_accumulator(self) -> None:
        pass
        self.produce_accumulator = get_or_create_produce_accumulator(
            self.client_id or MixAll.DEFAULT_INSTANCE_NAME)
        if self.batch_max_delay_ms > -1:
            self.produce_accumulator.batch_max_delay_ms(self.batch_max_delay_ms)
        if self.batch_max_bytes > -1:
            self.produce_accumulator.batch_max_bytes(self.batch_max_bytes)
        if self.total_batch_max_bytes > -1:
            self.produce_accumulator.total_batch_max_bytes(self.total_batch_max_bytes)

    def _can_batch(self, msg: Message) -> bool:
        pass
        if not self.produce_accumulator.try_add_message(msg):
            return False
        if _max_delay_value(msg) > 0:
            return False
        if msg.get_topic().startswith(MixAll.RETRY_GROUP_TOPIC_PREFIX):
            return False
        if MessageConst.PROPERTY_PRODUCER_GROUP in msg.properties:
            return False
        return True

    def send_direct(self, msg: Message, mq: Optional[MessageQueue],
                    send_callback: Optional[SendCallback]) -> Optional[SendResult]:
        pass
        if send_callback is None:
            if mq is None:
                return self.send(msg)
            return self.send(msg, mq=mq)
        if mq is None:
            self.send_async(msg, send_callback)
        else:
            self.send_async(msg, send_callback, mq=mq)
        return None

    def send_by_accumulator(self, msg: Message, mq: Optional[MessageQueue],
                            send_callback: Optional[SendCallback]) -> Optional[SendResult]:
        pass
        if not self._can_batch(msg):
            return self.send_direct(msg, mq, send_callback)
        validators.check_message(msg, self.max_message_size)
        set_uniq_id(msg)
        if send_callback is None:
            if mq is None:
                return self.produce_accumulator.send(msg, self)
            return self.produce_accumulator.send_with_mq(msg, mq, self)
        if mq is None:
            self.produce_accumulator.send_async(msg, send_callback, self)
        else:
            self.produce_accumulator.send_async_with_mq(msg, mq, send_callback, self)
        return None

    # ---------------- Batch sending ----------------
    def _send_batch(self, msgs: List[Message], mq: Optional[MessageQueue] = None,
                    timeout_millis: Optional[int] = None,
                    pinned_guard_message: str = _PINNED_TOPIC_MISMATCH_SYNC) -> SendResult:
        client = self._require_client()
        timeout = timeout_millis if timeout_millis is not None else self.send_msg_timeout
        if not msgs:
            raise MQClientException("message list is empty")
        for m in msgs:
            validators.check_message(m, self.max_message_size)
            set_uniq_id(m)
            m.topic = self._with_namespace(m.topic)
        batch = MessageBatch.generate_from_list(msgs)
        set_uniq_id(batch)
        batch.set_body(batch.encode())
        if mq is not None:
            self._check_pinned_topic(batch.topic, mq, pinned_guard_message)
        # A MessageBatch is skipped by try_to_compress_message directly (it returns 0); batch messages are never compressed
        sys_flag = self.try_to_compress_message(batch)
        if mq is not None:
            if self._has_send_interceptors():
                return self._send_with_hooks(client, batch, mq, timeout, sys_flag)
            return client.send_message(self.producer_group, batch, mq, timeout, sys_flag,
                                   **self._send_header_args())
        publish = self._topic_publish_info(batch.topic)
        selected = publish.select_one_message_queue()
        mq_sel = MessageQueue(batch.topic, selected.broker_name, selected.queue_id)
        if self._has_send_interceptors():
            return self._send_with_hooks(client, batch, mq_sel, timeout, sys_flag)
        return client.send_message(self.producer_group, batch, mq_sel, timeout, sys_flag,
                                   **self._send_header_args())


    @staticmethod
    def _transaction_flag(state: LocalTransactionState) -> int:
        pass
        if state == LocalTransactionState.COMMIT_MESSAGE:
            return MessageSysFlag.TRANSACTION_COMMIT_TYPE      # 0x2 << 2 = 8
        if state == LocalTransactionState.ROLLBACK_MESSAGE:
            return MessageSysFlag.TRANSACTION_ROLLBACK_TYPE    # 0x3 << 2 = 12
        return MessageSysFlag.TRANSACTION_NOT_TYPE             # 0 (UNKNOW)

    def send_message_in_transaction(self, msg: Message,
                                    listener: TransactionListener,
                                    arg=None) -> TransactionSendResult:
        pass
        if listener is None:
            raise MQClientException("tranExecutor is null", None)
        prev_body = msg.body
        msg.topic = self._with_namespace(msg.topic)

        for key in (MessageConst.PROPERTY_DELAY_TIME_LEVEL,
                    MessageConst.PROPERTY_DELAY_TIME,
                    getattr(MessageConst, "PROPERTY_TIMER_DELAY_MS", "__none__"),
                    getattr(MessageConst, "PROPERTY_TIMER_DELAY_SEC", "__none__"),
                    getattr(MessageConst, "PROPERTY_TIMER_DELIVER_MS", "__none__")):
            if msg.get_property(key) is not None:
                raise MQClientException(
                    "Transactional messages do not support delayed delivery", None)

        client = self._require_client()
        self._check_message(msg)

        # Half-message marker (the broker writes such messages to RMQ_SYS_TRANS_HALF_TOPIC based on it)
        msg.put_property(MessageConst.PROPERTY_TRANSACTION_PREPARED, "true")
        msg.put_property(MessageConst.PROPERTY_PRODUCER_GROUP, self.producer_group)
        # The callback on check uses this listener (the broker locates this producer via the PGROUP property)
        self._transaction_listener = listener

        publish = self._topic_publish_info(msg.topic)
        selected = publish.select_one_message_queue()
        mq_sel = MessageQueue(msg.topic, selected.broker_name, selected.queue_id)

        sys_flag = self.try_to_compress_message(msg)
        sys_flag = MessageSysFlag.reset_transaction_value(
            sys_flag, MessageSysFlag.TRANSACTION_PREPARED_TYPE)

        try:
            # The transactional send also goes through sendKernelImpl (→ send hooks fire too),
            # so with tracing enabled a transactional message first lands a Pub (Trans_Msg_Half) trace
            send_result = self._send_with_hooks(client, msg, mq_sel,
                                               self.send_msg_timeout, sys_flag)
        except Exception as e:  # noqa: BLE001
            raise MQClientException("send message Exception", e)
        finally:
            msg.body = prev_body
            msg.topic = NamespaceUtil.without_namespace(msg.topic, self.namespace)

        state = LocalTransactionState.UNKNOW
        local_exception = None
        if send_result.send_status == SendStatus.SEND_OK:
            if send_result.transaction_id is not None:
                msg.put_property("__transactionId__", send_result.transaction_id)
            uniq = msg.get_property(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX)
            if uniq:
                msg.set_transaction_id(uniq)
            try:
                ret = listener.execute_local_transaction(msg, arg)
                state = ret if ret is not None else LocalTransactionState.UNKNOW
            except Exception as e:  # noqa: BLE001
                logger.error("executeLocalTransactionBranch exception, topic=%s", msg.topic,
                             exc_info=True)
                local_exception = e
        elif send_result.send_status in (SendStatus.FLUSH_DISK_TIMEOUT,
                                         SendStatus.FLUSH_SLAVE_TIMEOUT,
                                         SendStatus.SLAVE_NOT_AVAILABLE):
            state = LocalTransactionState.ROLLBACK_MESSAGE

        try:
            self._end_transaction(send_result, msg, state, local_exception, False)
        except Exception as e:  # noqa: BLE001
            logger.warning("local transaction execute %s, but end broker transaction failed: %s",
                           state, e)

        return TransactionSendResult(send_result, state)

    def _end_transaction(self, send_result: SendResult, msg: Message,
                         state: LocalTransactionState, local_exception,
                         from_transaction_check: bool,
                         check_header: Optional[CheckTransactionStateRequestHeader] = None,
                         msg_ext: Optional[MessageExt] = None,
                         broker_addr: Optional[str] = None) -> None:
        pass
        client = self._require_client()
        header = EndTransactionRequestHeader()

        if from_transaction_check:
            # On a check, the broker carries COMPRESSED/transaction-related information in the check request
            header.commit_log_offset = check_header.commit_log_offset
            header.tran_state_table_offset = check_header.tran_state_table_offset
            header.transaction_id = check_header.transaction_id
            header.bname = check_header.bname
            header.topic = check_header.topic
            uniq = msg_ext.get_property(
                MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX) if msg_ext else None
            header.msg_id = uniq or (msg_ext.msg_id if msg_ext else None)
        else:
            _ip, _port, offset = decode_message_id(
                send_result.offset_msg_id or send_result.msg_id)
            broker_name = send_result.message_queue.broker_name
            header.commit_log_offset = offset
            header.tran_state_table_offset = send_result.queue_offset
            header.transaction_id = send_result.transaction_id
            header.bname = broker_name
            header.topic = msg.topic
            header.msg_id = send_result.msg_id
            broker_addr = client.find_broker_address_in_publish(broker_name)

        header.producer_group = self.producer_group
        header.commit_or_rollback = self._transaction_flag(state)
        header.from_transaction_check = from_transaction_check

        remark = None
        if local_exception is not None:
            remark = "executeLocalTransactionBranch exception: %s" % local_exception

        cmd = RemotingCommand.create_request_command(RequestCode.END_TRANSACTION, header)
        cmd.remark = remark
        if not broker_addr:
            raise MQClientException("no broker address for end transaction", None)
        client.remoting_client.invoke_oneway(broker_addr, cmd)
        if self.end_transaction_hook_list:
            ctx = EndTransactionContext()
            ctx.producer_group = self.producer_group
            ctx.message = msg_ext if msg_ext is not None else msg
            ctx.broker_addr = broker_addr or ""
            ctx.msg_id = header.msg_id
            ctx.transaction_id = header.transaction_id
            ctx.transaction_state = state
            ctx.from_transaction_check = from_transaction_check
            ctx.namespace = self.namespace
            self.execute_end_transaction_hook(ctx)

    def _handle_check_transaction_state(self, cmd, addr: str) -> None:
        pass
        header = CheckTransactionStateRequestHeader()
        try:
            header.from_ext_fields(cmd.ext_fields or {})
        except Exception:  # noqa: BLE001
            logger.warning("checkTransactionState: decode header failed from %s", addr)
            return

        msg_ext = decode_message(cmd.body) if cmd.body else None
        if msg_ext is None:
            logger.warning("checkTransactionState: decode message failed")
            return

        group = msg_ext.get_property(MessageConst.PROPERTY_PRODUCER_GROUP)
        if group is not None and group != self.producer_group:
            logger.debug("checkTransactionState: group %s not mine (%s)", group,
                         self.producer_group)
            return

        listener = self._transaction_listener
        if listener is None:
            logger.warning("checkTransactionState: no transaction listener for group %s",
                           self.producer_group)
            return

        def _run() -> None:
            try:
                ret = listener.check_local_transaction(msg_ext)
                state = ret if ret is not None else LocalTransactionState.UNKNOW
                exception = None
            except Exception as e:  # noqa: BLE001
                logger.error("Broker call checkTransactionState, but checkLocalTransaction "
                             "exception", exc_info=True)
                state = LocalTransactionState.UNKNOW
                exception = e
            try:
                self._end_transaction(None, None, state, exception, True,
                                      check_header=header, msg_ext=msg_ext, broker_addr=addr)
            except Exception as e:  # noqa: BLE001
                logger.warning("checkTransactionState: end transaction failed: %s", e)

        t = threading.Thread(target=_run, name="TransactionCheckThread", daemon=True)
        t.start()

    # ---------------- Admin capabilities ----------------
    def fetch_publish_message_queues(self, topic: str) -> List[MessageQueue]:
        client = self._require_client()
        publish = client.get_topic_publish_info(topic)
        return list(publish.msg_queue_list)

    def create_topic(self, key: str, new_topic: str, queue_num: int = 4,
                     topic_sys_flag: int = 0) -> None:
        client = self._require_client()
        validators.check_topic(new_topic)
        validators.is_system_topic(new_topic)
        perm = 6  # PermName.PERM_READ | PERM_WRITE
        client.create_topic_in_route(new_topic, queue_num, queue_num, perm)

    def search_offset(self, mq: MessageQueue, timestamp: int) -> int:
        return self._require_client().search_offset_by_timestamp(mq, timestamp)

    def max_offset(self, mq: MessageQueue) -> int:
        return self._require_client().get_max_offset(mq)

    def min_offset(self, mq: MessageQueue) -> int:
        return self._require_client().get_min_offset(mq)

    def earliest_msg_store_time(self, mq: MessageQueue) -> int:
        from remoting.protocol.headers import GetEarliestMsgStoretimeRequestHeader
        from remoting.protocol.codes import RequestCode
        from remoting.protocol.remoting_command import RemotingCommand
        client = self._require_client()
        addr = client._publish_addr_in_admin(mq)
        header = GetEarliestMsgStoretimeRequestHeader()
        header.topic = mq.topic
        header.queue_id = mq.queue_id
        request = RemotingCommand.create_request_command(RequestCode.GET_EARLIEST_MSG_STORETIME, header)
        from remoting.protocol.headers import GetEarliestMsgStoretimeResponseHeader
        response = client._invoke_sync(addr, request, 5000)
        client._check_response(response)
        resp_header = GetEarliestMsgStoretimeResponseHeader()
        resp_header.from_ext_fields(response.ext_fields)
        return resp_header.timestamp or 0

    def query_message(self, topic: str, key: str, max_num: int, begin: int, end: int):
        pass
        client = self._require_client()
        body = client.query_message(topic, key, max_num, begin, end)
        if body is None:
            return []
        from common.message_decoder import decode_messages
        return decode_messages(body)

    def view_message(self, topic: str, msg_id: str):
        raise MQClientException("viewMessage by msgId is not supported in Python edition")

    # ---------------- Helpers ----------------
    def _check_message(self, msg: Message) -> None:
        pass
        validators.check_message(msg, self.max_message_size)

    @staticmethod
    def _need_addr(client: MQClientInstance, mq: MessageQueue) -> str:
        return client.publish_addr_for(mq.broker_name, mq.topic)


class TransactionMQProducer(DefaultMQProducer):
    pass

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
    """Convenience callback wrapper: turn on_success / on_exception into plain functions."""

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
