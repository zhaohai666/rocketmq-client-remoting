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
from ..common.message_const import MessageConst
from ..common.message_decoder import _compress, decode_message, decode_message_id
from ..common.message_type import MessageType
from ..common import recall_message_handle
from ..common.mix_all import MixAll
from ..common.sysflag import MessageSysFlag
from ..logging import get_logger
from ..remoting.exception import (RemotingConnectException, RemotingException,
                                  RemotingTimeoutException, RemotingTooMuchRequestException)
from ..remoting.protocol.codes import RequestCode, ResponseCode
from ..remoting.protocol.headers import (CheckTransactionStateRequestHeader,
                                         EndTransactionRequestHeader,
                                         RecallMessageRequestHeader)
from ..remoting.protocol.heartbeat import HeartbeatData, ProducerData
from ..remoting.protocol.namespace_util import NamespaceUtil
from ..remoting.protocol.remoting_command import RemotingCommand
from ..remoting.rpchook import RPCHook
from .exception import (ClientErrorCode, MQBrokerException, MQClientException,
                        RequestTimeoutException)
from .hook import (CheckForbiddenContext, CheckForbiddenHook, CommunicationMode,

                   EndTransactionContext, EndTransactionHook, SendMessageContext, SendMessageHook)
from .latency import MQFaultStrategy
from .metrics import ClientMetrics
from .mq_client import MQClientInstance
from .top_addressing import DefaultTopAddressing
from .request_reply import (DEFAULT_REQUEST_TIMEOUT_MILLIS, REQUEST_FUTURE_HOLDER,
                            RequestResponseFuture, create_correlation_id)
from .send_result import SendResult, SendStatus
from .trace_context import inject_trace_context, trace_context_enabled_from_env
from .trace import AccessChannel
from .trace_dispatcher import AsyncTraceDispatcher, TraceDispatcherType
from .trace_hook import EndTransactionTraceHook, SendMessageTraceHook
from . import validators

logger = get_logger()


class _NullSendCallback:
    """request() 专用的空发送回调（对齐 Java 里给 sendDefaultImpl 传的那个匿名 SendCallback）。

    它只负责把「发送成功/失败」写回 RequestResponseFuture，应答本身由 broker 的
    326 推送投递，与这个回调无关。
    """

    def __init__(self, future: Optional[RequestResponseFuture] = None):
        self.future = future

    def on_success(self, send_result: SendResult) -> None:
        if self.future is not None:
            self.future.send_request_ok = True

    def on_exception(self, e: BaseException) -> None:
        if self.future is not None:
            # 与 Java 的 onException 完全一致：三件事都要做。少了 put_response_message(None)
            # 的话，等待方会一直阻塞到超时才抛错（明明是发送失败，却要等满 timeout）。
            self.future.send_request_ok = False
            self.future.put_response_message(None)
            self.future.cause = e


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
                 topics: Optional[List[str]] = None, tls_enable: Optional[bool] = None,
                 enable_trace_context: Optional[bool] = None):
        if producer_group is None or not str(producer_group).strip():
            raise MQClientException("producerGroup is empty")
        self.producer_group = str(producer_group)
        # TLS（Java 全局系统属性 tls.enable 的等价物；None = 交给 env ROCKETMQ_TLS_ENABLE）
        self.tls_enable: Optional[bool] = tls_enable
        # W3C traceparent 透传（opt-in；None = 交给 env ROCKETMQ_TRACE_CONTEXT_ENABLE）
        if enable_trace_context is None:
            enable_trace_context = trace_context_enabled_from_env()
        self.enable_trace_context: bool = bool(enable_trace_context)
        self.namespace = namespace
        self.instance_name = "DEFAULT"
        self.client_id = None
        self.create_topic_key = MixAll.DEFAULT_TOPIC
        self.default_topic_queue_nums = MixAll.DEFAULT_TOPIC_QUEUE_NUMS
        self.send_msg_timeout = 3000
        # 压缩配置，默认值与 Java DefaultMQProducer 一致
        self.compress_msg_body_over_howmuch = 1024 * 4
        self.compress_level = 5
        self.compress_type = MessageSysFlag.ZLIB_TYPE
        self.retry_times_when_send_failed = 2
        self.retry_times_when_send_async_failed = 2
        self.retry_another_broker_when_not_store_ok = False
        # Java DefaultMQProducer 默认 -1 = 不限制单次请求超时；开了之后每次重试的单请求
        # 超时被压到该值，慢 broker 才会被换掉而不是把总预算吃光。
        self.send_msg_max_timeout_per_request = -1
        # Java retryResponseCodes：broker 明确回了这些码才值得换一台重试，
        # 其余码（如 MESSAGE_ILLEGAL）重试也是白试，必须原样抛出。
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
        # 当前事务监听器（send_message_in_transaction 时记录，供 broker 回查调用）
        self._transaction_listener: Optional[TransactionListener] = None
        self._heartbeat_running = False
        self.heartbeat_interval_millis = 30000
        # 发送延迟故障容错：默认关闭，与 Java sendLatencyFaultEnable 一致
        self.send_latency_fault_enable = False
        self._mq_fault_strategy = MQFaultStrategy(False)
        # ---- 消息轨迹（对应 Java ClientConfig.enableTrace / traceTopic / traceMsgBatchNum）----
        self.enable_trace = False
        self.trace_topic: Optional[str] = None      # None → 用 RMQ_SYS_TRACE_TOPIC
        self.trace_msg_batch_num = 10
        self.send_message_hook_list: List["SendMessageHook"] = []
        self.end_transaction_hook_list: List["EndTransactionHook"] = []
        # 发送前拦截钩子（Java DefaultMQProducerImpl.checkForbiddenHookList）
        self.check_forbidden_hook_list: List["CheckForbiddenHook"] = []
        self.trace_dispatcher = None
        # 基础客户端指标（send/consume RT 与计数）
        self.metrics = ClientMetrics()
        # Request-Reply 的默认超时。Java 的 request(msg, timeout) 必须显式给 timeout，
        # 这里额外提供一个可设置的默认值，方便脚本调用（语义与显式传参完全一致）。
        self.request_timeout = DEFAULT_REQUEST_TIMEOUT_MILLIS

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

    def set_send_msg_max_timeout_per_request(self, timeout: int) -> None:
        self.send_msg_max_timeout_per_request = timeout

    def get_send_msg_max_timeout_per_request(self) -> int:
        return self.send_msg_max_timeout_per_request

    def set_retry_another_broker_when_not_store_ok(self, retry: bool) -> None:
        self.retry_another_broker_when_not_store_ok = retry

    def is_retry_another_broker_when_not_store_ok(self) -> bool:
        return self.retry_another_broker_when_not_store_ok

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
        """对应 Java DefaultMQProducer.setSendLatencyFaultEnable。默认关闭。"""
        self.send_latency_fault_enable = enable
        self._mq_fault_strategy.set_send_latency_fault_enable(enable)

    def set_request_timeout(self, timeout_millis: int) -> None:
        """设置 Request-Reply 的默认超时（不传 timeout 给 request() 时用它）。"""
        self.request_timeout = timeout_millis

    def get_metrics(self) -> ClientMetrics:
        """返回本生产者的基础指标计数器（send/consume RT 与计数）。"""
        return self.metrics

    # ---------------- 消息轨迹配置（对应 Java ClientConfig / DefaultMQProducer）----------------
    def set_enable_trace(self, enable: bool) -> None:
        """开关消息轨迹（Java ``ClientConfig.setEnableTrace``，默认 false）。

        开启后 ``start()`` 会注册 SendMessageTraceHook，把每条消息的发送轨迹
        异步发到轨迹 topic。**内部轨迹生产者自身必须保持关闭**，否则无限递归。
        """
        self.enable_trace = enable

    def is_enable_trace(self) -> bool:
        return self.enable_trace

    def set_trace_topic(self, trace_topic: Optional[str]) -> None:
        """自定义轨迹 topic（Java ``ClientConfig.setTraceTopic``）；空则用系统默认。"""
        self.trace_topic = trace_topic

    def set_trace_msg_batch_num(self, n: int) -> None:
        self.trace_msg_batch_num = n

    def register_send_message_hook(self, hook: "SendMessageHook") -> None:
        """注册发送钩子（对应 Java DefaultMQProducerImpl.registerSendMessageHook）。"""
        if hook is not None:
            self.send_message_hook_list.append(hook)

    def has_send_message_hook(self) -> bool:
        return len(self.send_message_hook_list) > 0

    # ---------------- 发送前拦截钩子（对应 Java CheckForbiddenHook 三个方法）----------------
    def register_check_forbidden_hook(self, hook: "CheckForbiddenHook") -> None:
        """注册发送前拦截钩子（Java DefaultMQProducerImpl.registerCheckForbiddenHook:186）。"""
        if hook is not None:
            self.check_forbidden_hook_list.append(hook)

    def has_check_forbidden_hook(self) -> bool:
        return len(self.check_forbidden_hook_list) > 0

    def _has_send_interceptors(self) -> bool:
        """是否需要走「带拦截/钩子」的发送内核（两者任一存在就得走）。"""
        return bool(self.send_message_hook_list) or bool(self.check_forbidden_hook_list)

    def execute_check_forbidden_hook(self, context: "CheckForbiddenContext") -> None:
        """⚠ 与 send/consume 钩子不同：这里**不吞异常**（Java 签名 ``throws MQClientException``）。

        钩子抛出的异常会沿 sendDefaultImpl 的重试链向上传播，这正是"禁止发送"的实现方式。
        """
        if not self.has_check_forbidden_hook():
            return
        for hook in self.check_forbidden_hook_list:
            hook.check_forbidden(context)

    def register_end_transaction_hook(self, hook: "EndTransactionHook") -> None:
        """注册事务收尾钩子（对应 Java registerEndTransactionHook）。"""
        if hook is not None:
            self.end_transaction_hook_list.append(hook)

    def execute_end_transaction_hook(self, context: "EndTransactionContext") -> None:
        for hook in self.end_transaction_hook_list:
            try:
                hook.end_transaction(context)
            except Exception as e:  # noqa: BLE001
                logger.warning("failed to executeEndTransactionHook: %s", e)

    # ---------------- 发送钩子调用（对应 Java executeSendMessageHookBefore/After）----------------
    def execute_send_message_hook_before(self, context: SendMessageContext) -> None:
        """钩子异常一律吞掉并记 warn（Java DefaultMQProducerImpl:1159）。"""
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
        """构造 SendMessageContext（对齐 Java DefaultMQProducerImpl:969-989）。

        msgType 的判定顺序也照抄：TRAN_MSG=true → Trans_Msg_Half；
        带任何延迟类属性 → Delay_Msg；否则 Normal_Msg。
        """
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
        """构造 CheckForbiddenContext 并执行（异常**不吞**，见 execute_check_forbidden_hook）。"""
        context = CheckForbiddenContext()
        context.name_srv_addr = self.get_namesrv_addr()
        context.group = self.producer_group
        context.communication_mode = communication_mode
        context.broker_addr = broker_addr
        context.message = msg
        context.mq = mq
        # 本项目无 unit mode（Java 的 isUnitMode() 恒为 false）
        context.unit_mode = False
        context.arg = arg
        self.execute_check_forbidden_hook(context)

    def _send_with_hooks(self, client: MQClientInstance, msg: Message, mq_sel: MessageQueue,
                         timeout: int, sys_flag: int, arg=None,
                         communication_mode: str = CommunicationMode.SYNC) -> SendResult:
        """真正发起请求的那一步（对应 Java sendKernelImpl 内的钩子点）。

        执行顺序严格照抄 Java `sendKernelImpl:956-990`：
          1. **CheckForbiddenHook**（每次尝试都跑；异常**不吞**，直接抛给重试链）
          2. SendMessageHook.before
          3. 发请求
          4. SendMessageHook.after（成功带 sendResult / 失败带 exception）
        重试时每轮都会重建 context，所以钩子会被调用多次 —— 与 Java 一致。
        """
        broker_addr = ""
        try:
            broker_addr = client.broker_addr_of(mq_sel.broker_name) or ""
        except Exception:  # noqa: BLE001
            pass
        if self.has_check_forbidden_hook():
            self._execute_check_forbidden(msg, mq_sel, broker_addr, arg, communication_mode)
        # W3C traceparent 透传（opt-in）：没有就注入根上下文，已有值不覆盖
        if self.enable_trace_context:
            inject_trace_context(msg)
        if not self.send_message_hook_list:
            return client.send_message(self.producer_group, msg, mq_sel, timeout, sys_flag)
        context = self._build_send_context(msg, mq_sel, broker_addr, communication_mode)
        self.execute_send_message_hook_before(context)
        try:
            result = client.send_message(self.producer_group, msg, mq_sel, timeout, sys_flag)
        except Exception as e:  # noqa: BLE001
            context.exception = e
            self.execute_send_message_hook_after(context)
            raise
        context.send_result = result
        self.execute_send_message_hook_after(context)
        return result

    # ---------------- 生命周期 ----------------
    def start(self) -> None:
        with self._lock:
            if self._started:
                return
            # 生产者组也拼命名空间（对齐 Java DefaultMQProducer.start:375
            # setProducerGroup(withNamespace(producerGroup))），broker 侧按带前缀的组名登记
            if self.namespace:
                self.producer_group = NamespaceUtil.wrap_namespace(self.namespace, self.producer_group)
            # 对应 Java DefaultMQProducerImpl.checkConfig（:295）：组名校验排在拼完命名空间之后
            # （Java 也是 start() 先 withNamespace 再 impl.start()），并且要挡住
            # DEFAULT_PRODUCER —— 多进程共用默认组会互相踢下线。checkConfig 是 Java start() 的
            # 第一步，所以这里也领先于 name server 地址检查。
            validators.check_group(self.producer_group)
            if self.producer_group == MixAll.DEFAULT_PRODUCER_GROUP:
                raise MQClientException(
                    "producerGroup can not equal %s, please specify another one."
                    % MixAll.DEFAULT_PRODUCER_GROUP)
            if not self.name_server_addrs and not DefaultTopAddressing.is_configured():
                # 静态地址与动态取址（ROCKETMQ_NAMESRV_DOMAIN）二选一必须可用
                raise MQClientException("name server address is not set")
            # 对应 Java `DefaultMQProducerImpl#start`:250-252 的两步：先
            # `changeInstanceNameToPID`（Java 只对非 CLIENT_INNER_PRODUCER 的生产者做，
            # 本客户端没有内部生产者，所以无条件执行），再由 `ClientConfig#buildMQClientId`
            # 拼 `<本机 IP>@<instanceName>`。instanceName 就地写回，和 Java 一样：
            # 第二次 start() 复用同一个 clientId，而不是每重启一次换一个名字。
            self.instance_name = MixAll.change_instance_name_to_pid(self.instance_name)
            if self.client_id is None:
                self.client_id = MixAll.client_id_for(self.instance_name)
            self._mq_client = MQClientInstance(self.client_id, self.name_server_addrs,
                                               tls_enable=self.tls_enable)
            if self.rpc_hook is not None:
                self._mq_client.remoting_client.register_rpc_hook(self.rpc_hook)
            self._mq_client.start()
            # 动态 name server：实例启动时可能已从地址服务器拿到地址，回填到本生产者
            if not self.name_server_addrs and self._mq_client.name_server_addrs:
                self.name_server_addrs = list(self._mq_client.name_server_addrs)
            # 注册 broker 主动请求处理器：事务回查 CHECK_TRANSACTION_STATE(39)。
            # 按 message_ext 的 PGROUP 属性匹配本生产者，不匹配则丢弃。
            self._mq_client.remoting_client.register_processor(
                RequestCode.CHECK_TRANSACTION_STATE, self._handle_check_transaction_state)
            self._started = True
            # 心跳线程：周期性向 broker 注册 ProducerData。
            # broker 的事务回查正是通过这一步登记的 channel 反向联系生产者的；
            # 生产者不发心跳时 COMMIT/ROLLBACK 仍能成功（客户端主动 END_TRANSACTION），
            # 但 UNKNOW 的半消息会**永远不被回查**。
            self._heartbeat_running = True
            threading.Thread(target=self._heartbeat_loop, name="ProducerHeartbeatThread",
                             daemon=True).start()
        # 轨迹分发器在锁外启动（Java 同样在 defaultMQProducerImpl.start() 之后做）：
        # 它要新建内部生产者并拉路由，属网络操作，不该占着生产者自己的锁。
        self._start_trace_dispatcher()

    def _start_trace_dispatcher(self) -> None:
        """对应 Java DefaultMQProducer.start():380-405。

        enableTrace=true 时建 AsyncTraceDispatcher（Type=PRODUCE）并注册
        SendMessageTraceHook；随后无论新建还是复用，都要 start 它。
        任何异常都只记日志 —— 轨迹挂了不能影响正常发送。
        """
        if self.enable_trace:
            try:
                dispatcher = AsyncTraceDispatcher(
                    self.producer_group, TraceDispatcherType.PRODUCE,
                    self.trace_msg_batch_num, self.trace_topic, self.rpc_hook)
                dispatcher.set_host_producer(self)
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

    def shutdown(self) -> None:
        with self._lock:
            if not self._started:
                return
            self._heartbeat_running = False
            if self._mq_client is not None:
                self._mq_client.shutdown()
            self._started = False
        # 顺序对齐 Java DefaultMQProducer.shutdown()：先关本生产者，再 flush 并关轨迹分发器
        # （分发器用的是**自己的**内部生产者，与本客户端实例无关，所以关掉了照样能发完）
        if self.trace_dispatcher is not None:
            try:
                self.trace_dispatcher.shutdown()
            except Exception as e:  # noqa: BLE001
                logger.warning("trace dispatcher shutdown failed: %s", e)

    def _heartbeat_loop(self) -> None:
        """周期性向所有已知 broker 发心跳（含 ProducerData），对齐 Java 的生产者注册。"""
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
        # ⚠ Python 的 HeartbeatData 没有 heartbeatFingerprint / withoutSub 字段
        # （已知缺陷，见项目记忆）。缺失时 broker 反序列化为 0，等价于走 V1 注册路径，
        # 与 C++ 侧显式置 0 的效果一致，这里保持现状不引入新差异。
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

    # ---------------- 压缩（对应 Java DefaultMQProducerImpl.tryToCompressMessage）----------------
    def try_to_compress_message(self, msg: Message) -> int:
        """满足阈值且非批量时**就地压缩 msg.body**，返回应下发的 sys_flag。

        与 Java 逐条对齐：
          * 批量消息（MessageBatch）**永不压缩**；
          * body 长度 >= compress_msg_body_over_howmuch（默认 4096）才压缩；
          * 压缩失败按 Java 的做法**降级为不压缩**并记日志，而不是让发送失败；
          * 压缩后不比较体积（Java 也不比较）。
        不压缩时返回 0。
        """
        if isinstance(msg, MessageBatch):
            return 0
        body = msg.get_body()
        if not body or len(body) < self.compress_msg_body_over_howmuch:
            return 0
        try:
            compressed = _compress(body, self.compress_type, self.compress_level)
        except Exception as e:  # noqa: BLE001  # 对齐 Java：压缩失败降级为不压缩
            logger.warning("tryToCompressMessage failed, send uncompressed: %s", e)
            return 0
        if not compressed:
            return 0
        msg.set_body(compressed)
        sys_flag = MessageSysFlag.COMPRESSED_FLAG
        return MessageSysFlag.set_compression_type(sys_flag, self.compress_type)

    def _with_namespace(self, topic: str) -> str:
        """给 topic 拼上命名空间前缀（对应 Java ClientConfig.withNamespace）。

        Java 在每个 ``DefaultMQProducer.send*`` 公开入口都做 ``msg.setTopic(withNamespace(...))``，
        broker 侧看到的资源名是 ``<namespace>%<topic>``；生产者组在 ``start()`` 里同样被包装。
        """
        if not self.namespace:
            return topic
        return NamespaceUtil.wrap_namespace(self.namespace, topic)

    def _topic_publish_info(self, topic: str) -> "TopicPublishInfo":
        """对应 Java DefaultMQProducerImpl.tryToFindTopicPublishInfo。

        先拉**真实**路由；只有确实拉不到（新 topic 尚未在 NameServer 注册）时才按 Java 的做法
        回退到默认 topic（TBW102）为该 topic 合成发布信息，否则新 topic 的**首条**消息没队列可选。
        消费者路径不做这个兜底（理由见 MQClientInstance.update_topic_route_info_from_name_server）。
        """
        client = self._require_client()
        client.register_topic_in_use(topic)
        try:
            return client.get_topic_publish_info(topic)
        except MQClientException:
            return client.get_topic_publish_info(topic, is_default=True)

    def _update_fault_item(self, selected, began: float, isolation: bool,
                           reachable: bool) -> None:
        """容错表记一次尝试的延迟。延迟必须用单调钟量：本地亚毫秒往返用毫秒墙钟差
        会记成 0，那样延迟阈值永远不会生效。"""
        if selected is None:
            return
        self._mq_fault_strategy.update_fault_item(
            selected.broker_name, (time.monotonic() - began) * 1000.0, isolation, reachable)

    # ---------------- 正常发送 ----------------
    def send(self, msg: Message, timeout_millis: Optional[int] = None,
             mq: Optional[MessageQueue] = None) -> SendResult:
        """同步发送：msg 或 Collection；指定 mq 走定点发送，否则轮询选择。"""
        client = self._require_client()
        timeout = timeout_millis if timeout_millis is not None else self.send_msg_timeout
        if isinstance(msg, (list, tuple)):
            return self._send_batch(list(msg), mq, timeout)
        msg.topic = self._with_namespace(msg.topic)
        self._check_message(msg)
        # 在重试循环**之外**压缩一次：Java 是在循环内调用 tryToCompressMessage 的，
        # 而它会就地 setBody，重试时会把已压缩的 body 再压一遍（zlib(zlib(x))），
        # 消费端只解一层就拿到压缩流。这里避免该问题。
        sys_flag = self.try_to_compress_message(msg)
        if mq is not None:
            # 定点发送同样要过钩子（Java：目标是 mq 也走 sendKernelImpl）
            if self._has_send_interceptors():
                return self._send_with_hooks(client, msg, mq, timeout, sys_flag)
            return client.send_message(self.producer_group, msg, mq, timeout, sys_flag)
        # 对应 Java sendDefaultImpl：重试分类逐异常类型走，不用"啥都重试"糊过去。
        try:
            publish = self._topic_publish_info(msg.topic)
        except MQClientException as e:
            # Java：路由拿不到时立刻抛 NOT_FOUND_TOPIC_EXCEPTION，不把重试次数空转掉
            raise MQClientException(str(e), ClientErrorCode.NOT_FOUND_TOPIC_EXCEPTION)
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
                # 故障规避：开启时按 broker 延迟/隔离状态选队列（Java MQFaultStrategy）；
                # 关闭时退化为普通轮询（策略内部判断）。重试时 resetIndex 让轮询从头开始，
                # 从而能避开 last_broker_name 选到别的 broker。
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
                except Exception:  # noqa: BLE001 — 指标记账后按原异常分类处理
                    self.metrics.record_send_failure(send_start)
                    raise
                self.metrics.record_send_success(send_start)
                # 记录发送延迟；超出阈值会把该 broker 隔离一段时间
                self._update_fault_item(selected, began, False, True)
                # Java：非 SEND_OK 且开了 retryAnotherBrokerWhenNotStoreOK 才换 broker，
                # 否则把这个"存了但没存好"的结果原样返回
                if (result is not None and result.send_status != SendStatus.SEND_OK
                        and self.retry_another_broker_when_not_store_ok):
                    continue
                return result
            except MQBrokerException as e:
                # broker 明确回了错误码：隔离该 broker（可达性不动），只有可重试码才换一台
                self._update_fault_item(selected, began, True, False)
                last_exc = e
                if self.is_retry_response_code(e.response_code):
                    continue
                if result is not None:
                    return result
                raise
            except RemotingException as e:
                # 连不上/超时/发不出去：隔离该 broker。本项目无后台可达性探测任务，
                # 所以 Java 的 reachable = !isStartDetectorEnable() 恒为 True。
                self._update_fault_item(selected, began, True, True)
                last_exc = e
            except MQClientException as e:
                # 客户端自己的问题（选不到队列、路由没了…）：Java 同样只记延迟、不隔离
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
                mq: Optional[MessageQueue] = None) -> Message:
        """Request-Reply（5.x）：发一条请求消息并**同步等应答**，返回应答消息。

        对应 Java ``DefaultMQProducerImpl#request(msg, mq, timeout)``（:1738-1767）。
        请求方做三件事：
          1. 给请求消息写上 CORRELATION_ID（随机 UUID）、REPLY_TO_CLIENT（**本客户端 clientId**）、
             TTL（= timeout）；后两个是 broker 找回本连接、应答方原样带回的依据。
          2. 把等待槽按 correlationId 登记到进程内的 REQUEST_FUTURE_HOLDER。
          3. 发送后阻塞等待；应答由 broker 经 PUSH_REPLY_MESSAGE_TO_CLIENT(326) 推回，
             由 ``MQClientInstance._process_reply_message`` 投递进等待槽。

        超时抛 ``RequestTimeoutException``（消息已发出但没等到应答）；
        发送本身失败则抛 ``MQClientException``（带着底层 cause），与 Java 一致。

        ``REPLY_TO_CLIENT`` 是 clientId —— broker 要靠它反查 channel，
        所以本生产者必须先发过心跳（``start()`` 已起心跳线程；这里也会补一次，
        对齐 Java ``prepareSendRequest`` 的 ``sendHeartbeatToAllBrokerWithLock``）。
        """
        timeout = timeout_millis if timeout_millis is not None else self.request_timeout
        msg.topic = self._with_namespace(msg.topic)
        self._check_message(msg)

        correlation_id = create_correlation_id()
        client = self._require_client()
        msg.put_property(MessageConst.PROPERTY_CORRELATION_ID, correlation_id)
        msg.put_property(MessageConst.PROPERTY_MESSAGE_REPLY_TO_CLIENT, client.client_id)
        msg.put_property(MessageConst.PROPERTY_MESSAGE_TTL, str(timeout))

        begin = time.time() * 1000.0
        # 对齐 Java prepareSendRequest：确保路由已知，然后补一次心跳 ——
        # 没在 broker 上登记为 producer，broker 就找不到 channel 把应答推回来。
        try:
            self._topic_publish_info(msg.topic)
            self._send_heartbeat_to_all_broker()
        except Exception:  # noqa: BLE001 — 拿不到路由就让下面的发送路径自己报错
            logger.debug("request: prepare route/heartbeat failed", exc_info=True)

        future = RequestResponseFuture(correlation_id, timeout)
        REQUEST_FUTURE_HOLDER.put_request(correlation_id, future)
        cost = int(time.time() * 1000.0 - begin)
        try:
            # 说明：Java 用 ASYNC 发送并等 latch；本实现的 send_async 是
            # 「同步发送 + 立即回调」的包装，所以这里等价于同步发。
            # 协议上无差别 —— 应答是 broker 通过**另一条** 326 通道推回来的，
            # 与本次发送的 CommunicationMode 无关。
            # 发送失败时回调会把 future 标成 !send_request_ok 并主动唤醒等待方。
            self.send_async(msg, _NullSendCallback(future),
                            timeout - cost if timeout > cost else timeout, mq)
            return self._wait_request_response(msg, timeout, future, cost)
        finally:
            REQUEST_FUTURE_HOLDER.remove_request(correlation_id)

    def _wait_request_response(self, msg: Message, timeout: int,
                               future: RequestResponseFuture, cost: int) -> Message:
        """对应 Java ``waitResponse``：超时/发送失败分别抛不同异常。"""
        response = future.wait_response_message(timeout - cost)
        if response is None:
            if future.send_request_ok:
                raise RequestTimeoutException(
                    "send request message to <%s> OK, but wait reply message timeout, %d ms."
                    % (msg.topic, timeout))
            raise MQClientException(
                "send request message to <%s> fail" % msg.topic, None, future.cause)
        return response

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
        msg.topic = self._with_namespace(msg.topic)
        self._check_message(msg)
        sys_flag = self.try_to_compress_message(msg)
        if mq is not None:
            if self.has_check_forbidden_hook():
                # Java sendOneway 同样走 sendKernelImpl → 拦截钩子照跑（communicationMode=ONEWAY）
                self._execute_check_forbidden(msg, mq, self._need_addr(client, mq),
                                              None, CommunicationMode.ONEWAY)
            client.send_message_oneway(self.producer_group, msg, mq,
                                       self._need_addr(client, mq), self.send_msg_timeout, sys_flag)
            return
        publish = self._topic_publish_info(msg.topic)
        selected = self._mq_fault_strategy.select_one_message_queue(publish, None)
        mq_sel = MessageQueue(msg.topic, selected.broker_name, selected.queue_id)
        if self.has_check_forbidden_hook():
            self._execute_check_forbidden(msg, mq_sel, self._need_addr(client, mq_sel),
                                          None, CommunicationMode.ONEWAY)
        client.send_message_oneway(self.producer_group, msg, mq_sel,
                                   self._need_addr(client, mq_sel), self.send_msg_timeout, sys_flag)

    def send_by_selector(self, msg: Message, selector: MessageQueueSelector, arg,
                         timeout_millis: Optional[int] = None) -> SendResult:
        """使用 MessageQueueSelector 选择队列发送（对应 Java send(msg, selector, arg)）。"""
        client = self._require_client()
        timeout = timeout_millis if timeout_millis is not None else self.send_msg_timeout
        msg.topic = self._with_namespace(msg.topic)
        publish = self._topic_publish_info(msg.topic)
        selected = selector.select(publish.msg_queue_list, msg, arg)
        mq_sel = MessageQueue(msg.topic, selected.broker_name, selected.queue_id)
        # 选择器用的是原始消息（topic/业务字段），压缩只影响 body
        self._check_message(msg)
        sys_flag = self.try_to_compress_message(msg)
        if self._has_send_interceptors():
            # arg 要透传给 CheckForbiddenContext（Java sendKernelImpl 的 context.setArg）
            return self._send_with_hooks(client, msg, mq_sel, timeout, sys_flag, arg=arg)
        return client.send_message(self.producer_group, msg, mq_sel, timeout, sys_flag)

    # ---------------- 定时消息撤回（对应 Java recallMessage）----------------
    def recall_message(self, topic: str, recall_handle: str) -> str:
        """撤回一条定时/延迟消息，返回被撤回消息的 uniqKey。

        校验顺序与 Java ``DefaultMQProducerImpl#recallMessage``(:1570-1601) 逐条对齐：
        状态 → checkTopic → 禁 retry/DLQ → 解句柄 → 预热路由 → 定位 broker → 发请求。
        句柄来自定时消息的 ``SendResult.recall_handle``，普通消息没有。

        与 Java 的差异：Java 的 ``findBrokerAddrByTopic`` 返回该 topic 的**全部** broker
        地址再随机取一个，这里直接取路由里的第一个可用地址——单 broker 场景等价，
        多 broker 场景两者都只会命中句柄里那个 broker 之外的地址，最终由 broker 用
        ``ILLEGAL_OPERATION``（brokerName 不匹配）拒绝，语义不变。
        """
        client = self._require_client()
        topic = self._with_namespace(topic)
        validators.check_topic(topic)
        if MixAll.is_retry_topic(topic) or MixAll.is_dlq_topic(topic):
            raise MQClientException("topic is not supported")
        handle = recall_message_handle.decode_handle(recall_handle)
        # Java 只是调用 tryToFindTopicPublishInfo 预热路由，返回值并不使用，但**异常照抛**
        # （DefaultMQProducerImpl:1586）—— 连路由都拿不到时，后面的 broker 定位也没有意义。
        self._topic_publish_info(topic)
        addr = client.broker_addr_of(handle.broker_name)
        if addr is None:
            route = client.get_topic_route_data(topic)
            for broker_data in route.get_broker_datas() if route else []:
                addr = broker_data.select_broker_addr()
                if addr:
                    break
        if addr is None:
            logger.warning("can't find broker service address. %s", handle.broker_name)
            raise MQClientException("The broker service address not found")
        header = RecallMessageRequestHeader()
        header.producer_group = self.producer_group
        header.topic = topic
        header.recall_handle = recall_handle
        header.bname = handle.broker_name
        return client.recall_message(addr, header, self.send_msg_timeout)

    # ---------------- 批量发送 ----------------
    def _send_batch(self, msgs: List[Message], mq: Optional[MessageQueue] = None,
                    timeout_millis: Optional[int] = None) -> SendResult:
        client = self._require_client()
        timeout = timeout_millis if timeout_millis is not None else self.send_msg_timeout
        if not msgs:
            raise MQClientException("message list is empty")
        # 对应 Java DefaultMQProducer.batch()：**每条子消息**都过一遍 Validators.checkMessage
        # （在拼命名空间之前），再 MessageBatch.generateFromList 查同质性。
        # 少这一步等于批量路径绕过了所有本地校验——超长/空 body/非法 topic 都能发出去。
        for m in msgs:
            validators.check_message(m, self.max_message_size)
        for m in msgs:
            m.topic = self._with_namespace(m.topic)
        batch = MessageBatch.generate_from_list(msgs)
        # MessageBatch 会被 try_to_compress_message 直接跳过（返回 0），批量消息永不压缩
        sys_flag = self.try_to_compress_message(batch)
        if mq is not None:
            if self._has_send_interceptors():
                return self._send_with_hooks(client, batch, mq, timeout, sys_flag)
            return client.send_message(self.producer_group, batch, mq, timeout, sys_flag)
        publish = self._topic_publish_info(batch.topic)
        selected = publish.select_one_message_queue()
        mq_sel = MessageQueue(batch.topic, selected.broker_name, selected.queue_id)
        if self._has_send_interceptors():
            return self._send_with_hooks(client, batch, mq_sel, timeout, sys_flag)
        return client.send_message(self.producer_group, batch, mq_sel, timeout, sys_flag)

    # ---------------- 事务消息 ----------------
    # 对齐 Java DefaultMQProducerImpl.sendMessageInTransaction（L1433-1509）的**两阶段**：
    #   1) 半消息：给 msg 打 TRAN_MSG / PGROUP 属性，发送时 sysFlag 置 TRANSACTION_PREPARED_TYPE；
    #   2) 本地事务：仅 SEND_OK 时执行，结果/异常汇总为 LocalTransactionState；
    #   3) endTransaction：以 END_TRANSACTION(37, oneway) 告知 broker 提交/回滚/未知；
    #   4) 若 UNKNOW（或本地事务没执行成功），broker 会回查 CHECK_TRANSACTION_STATE(39)，
    #      由 _handle_check_transaction_state 调 listener.check_local_transaction 后再 END_TRANSACTION。

    @staticmethod
    def _transaction_flag(state: LocalTransactionState) -> int:
        """LocalTransactionState -> Java MessageSysFlag 的 commitOrRollback 值。"""
        if state == LocalTransactionState.COMMIT_MESSAGE:
            return MessageSysFlag.TRANSACTION_COMMIT_TYPE      # 0x2 << 2 = 8
        if state == LocalTransactionState.ROLLBACK_MESSAGE:
            return MessageSysFlag.TRANSACTION_ROLLBACK_TYPE    # 0x3 << 2 = 12
        return MessageSysFlag.TRANSACTION_NOT_TYPE             # 0（UNKNOW）

    def send_message_in_transaction(self, msg: Message,
                                    listener: TransactionListener,
                                    arg=None) -> TransactionSendResult:
        """发送事务消息（对应 Java sendMessageInTransaction）。

        与 Java 一致的两阶段语义；返回 TransactionSendResult，其中
        local_transaction_state 是本地事务的最终状态。
        """
        if listener is None:
            raise MQClientException("tranExecutor is null", None)
        msg.topic = self._with_namespace(msg.topic)

        # Java ensureNotDelayedForTransactional：事务消息不支持任何形式的延迟投递
        # Java ensureNotDelayedForTransactional：事务消息不支持延迟投递。
        # Python 目前只有 DELAY / DELAY_TIME 两个延迟类属性（没有 5.x 的 TIMER_*），
        # 因此按 getattr 取，新增常量时自动生效。
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

        # 半消息标记（broker 侧据此把消息写入 RMQ_SYS_TRANS_HALF_TOPIC）
        msg.put_property(MessageConst.PROPERTY_TRANSACTION_PREPARED, "true")
        msg.put_property(MessageConst.PROPERTY_PRODUCER_GROUP, self.producer_group)
        # 回查时按此 listener 回调（broker 通过 PGROUP 属性定位到本生产者）
        self._transaction_listener = listener

        publish = self._topic_publish_info(msg.topic)
        selected = publish.select_one_message_queue()
        mq_sel = MessageQueue(msg.topic, selected.broker_name, selected.queue_id)

        # 压缩与普通发送一致（Java 的事务发送同样走 sendKernelImpl），
        # 再叠加事务类型位（对应 Java L951-953 检测 TRAN_MSG 后置 TRANSACTION_PREPARED）
        sys_flag = self.try_to_compress_message(msg)
        sys_flag = MessageSysFlag.reset_transaction_value(
            sys_flag, MessageSysFlag.TRANSACTION_PREPARED_TYPE)

        try:
            # 事务发送同样走 sendKernelImpl（→ 同样触发发送钩子），
            # 所以开启轨迹后事务消息会先落一条 Pub（Trans_Msg_Half）轨迹
            send_result = self._send_with_hooks(client, msg, mq_sel,
                                               self.send_msg_timeout, sys_flag)
        except Exception as e:  # noqa: BLE001
            raise MQClientException("send message Exception", e)

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
                # Java：返回 null 视为 UNKNOW
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
            # Java：end broker transaction 失败只 warn，不影响返回结果
            logger.warning("local transaction execute %s, but end broker transaction failed: %s",
                           state, e)

        return TransactionSendResult(send_result, state)

    def _end_transaction(self, send_result: SendResult, msg: Message,
                         state: LocalTransactionState, local_exception,
                         from_transaction_check: bool,
                         check_header: Optional[CheckTransactionStateRequestHeader] = None,
                         msg_ext: Optional[MessageExt] = None,
                         broker_addr: Optional[str] = None) -> None:
        """向 broker 发送 END_TRANSACTION(37, oneway)，对齐 Java endTransaction + checkTransactionState。

        - 普通收尾（from_transaction_check=False）：偏移/事务号取自 send_result；
        - 回查收尾（from_transaction_check=True）：偏移/事务号取自 broker 的回查 header
          （send_result/mq 此时不可用），msgId 取 message_ext 的 UNIQ_KEY。
        """
        client = self._require_client()
        header = EndTransactionRequestHeader()

        if from_transaction_check:
            # 回收时 broker 会把 COMPRESSED/事务相关信息放在回查请求里
            header.commit_log_offset = check_header.commit_log_offset
            header.tran_state_table_offset = check_header.tran_state_table_offset
            header.transaction_id = check_header.transaction_id
            header.bname = check_header.bname
            header.topic = check_header.topic
            uniq = msg_ext.get_property(
                MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX) if msg_ext else None
            header.msg_id = uniq or (msg_ext.msg_id if msg_ext else None)
        else:
            # Java：id = decodeMessageId(offsetMsgId != null ? offsetMsgId : msgId)
            _ip, _port, offset = decode_message_id(
                send_result.offset_msg_id or send_result.msg_id)
            broker_name = send_result.message_queue.broker_name
            header.commit_log_offset = offset
            header.tran_state_table_offset = send_result.queue_offset
            header.transaction_id = send_result.transaction_id
            header.bname = broker_name
            header.topic = msg.topic
            header.msg_id = send_result.msg_id
            broker_addr = client.broker_addr_of(broker_name)

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
        # 对应 Java endTransaction 末尾的 executeEndTransactionHook：无论主动提交还是
        # broker 回查后提交，都会落一条 EndTransaction 轨迹
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
        """处理 broker 主动发来的事务回查（CHECK_TRANSACTION_STATE=39）。

        对齐 Java ClientRemotingProcessor.checkTransactionState + DefaultMQProducerImpl
        .checkTransactionState：broker 是 **oneway** 发来的（body 为整条编码后的
        MessageExt），因此**不回响应**，而是在新线程里调 listener.check_local_transaction，
        再以 END_TRANSACTION(fromTransactionCheck=true) 把最终状态告知 broker。
        """
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

    # ---------------- 管理能力 ----------------
    def fetch_publish_message_queues(self, topic: str) -> List[MessageQueue]:
        client = self._require_client()
        publish = client.get_topic_publish_info(topic)
        return list(publish.msg_queue_list)

    def create_topic(self, key: str, new_topic: str, queue_num: int = 4,
                     topic_sys_flag: int = 0) -> None:
        client = self._require_client()
        # 对应 Java DefaultMQProducerImpl.createTopic：checkTopic + isSystemTopic
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
        """对应 ``Validators.checkMessage(msg, this)``——纯本地、打网络之前就跑完。

        校验项与顺序都跟着 Java 走：topic（blank/长度/字符表）→ 禁发 topic →
        body（null/零长/超过 maxMessageSize）→ ``INNER_MULTI_DISPATCH`` 不能带路径分隔符。
        """
        validators.check_message(msg, self.max_message_size)

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