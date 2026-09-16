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
from ..common.mix_all import MixAll
from ..common.sysflag import MessageSysFlag
from ..logging import get_logger
from ..remoting.exception import RemotingException
from ..remoting.protocol.codes import RequestCode
from ..remoting.protocol.headers import (CheckTransactionStateRequestHeader,
                                         EndTransactionRequestHeader)
from ..remoting.protocol.heartbeat import HeartbeatData, ProducerData
from ..remoting.protocol.namespace_util import NamespaceUtil
from ..remoting.protocol.remoting_command import RemotingCommand
from ..remoting.rpchook import RPCHook
from .exception import MQBrokerException, MQClientException
from .latency import MQFaultStrategy
from .metrics import ClientMetrics
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
        # 压缩配置，默认值与 Java DefaultMQProducer 一致
        self.compress_msg_body_over_howmuch = 1024 * 4
        self.compress_level = 5
        self.compress_type = MessageSysFlag.ZLIB_TYPE
        self.retry_times_when_send_failed = 2
        self.retry_times_when_send_async_failed = 2
        self.retry_another_broker_when_not_store_ok = False
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
        # 基础客户端指标（send/consume RT 与计数）
        self.metrics = ClientMetrics()

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

    def get_metrics(self) -> ClientMetrics:
        """返回本生产者的基础指标计数器（send/consume RT 与计数）。"""
        return self.metrics

    # ---------------- 生命周期 ----------------
    def start(self) -> None:
        with self._lock:
            if self._started:
                return
            if not self.name_server_addrs:
                raise MQClientException("name server address is not set")
            if self.client_id is None:
                self.client_id = "%s@%s" % (self.instance_name, time.strftime("%Y%m%d%H%M%S"))
            # 生产者组也拼命名空间（对齐 Java DefaultMQProducer.start:375
            # setProducerGroup(withNamespace(producerGroup))），broker 侧按带前缀的组名登记
            if self.namespace:
                self.producer_group = NamespaceUtil.wrap_namespace(self.namespace, self.producer_group)
            self._mq_client = MQClientInstance(self.client_id, self.name_server_addrs)
            if self.rpc_hook is not None:
                self._mq_client.remoting_client.register_rpc_hook(self.rpc_hook)
            self._mq_client.start()
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

    def shutdown(self) -> None:
        with self._lock:
            if not self._started:
                return
            self._heartbeat_running = False
            if self._mq_client is not None:
                self._mq_client.shutdown()
            self._started = False

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
            return client.send_message(self.producer_group, msg, mq, timeout, sys_flag)
        last_exc = None
        last_broker_name = None
        for attempt in range(self.retry_times_when_send_failed + 1):
            try:
                publish = self._topic_publish_info(msg.topic)
                # 故障规避：开启时按 broker 延迟/隔离状态选队列（Java MQFaultStrategy）；
                # 关闭时退化为普通轮询（策略内部判断）。
                selected = self._mq_fault_strategy.select_one_message_queue(
                    publish, last_broker_name)
                last_broker_name = selected.broker_name
                mq_sel = MessageQueue(msg.topic, selected.broker_name, selected.queue_id)
                send_start = self.metrics.record_send_start()
                try:
                    result = client.send_message(self.producer_group, msg, mq_sel, timeout, sys_flag)
                except Exception:  # noqa: BLE001 — 记录指标/隔离后按原异常重试
                    self.metrics.record_send_failure(send_start)
                    self._mq_fault_strategy.update_fault_item(
                        selected.broker_name, 0.0, True, False)
                    raise
                self.metrics.record_send_success(send_start)
                # 记录发送延迟；超出阈值会把该 broker 隔离一段时间
                self._mq_fault_strategy.update_fault_item(
                    selected.broker_name, time.time() * 1000.0 - send_start, False, True)
                return result
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
        msg.topic = self._with_namespace(msg.topic)
        self._check_message(msg)
        sys_flag = self.try_to_compress_message(msg)
        if mq is not None:
            client.send_message_oneway(self.producer_group, msg, mq,
                                       self._need_addr(client, mq), self.send_msg_timeout, sys_flag)
            return
        publish = self._topic_publish_info(msg.topic)
        selected = self._mq_fault_strategy.select_one_message_queue(publish, None)
        mq_sel = MessageQueue(msg.topic, selected.broker_name, selected.queue_id)
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
        return client.send_message(self.producer_group, msg, mq_sel, timeout, sys_flag)

    # ---------------- 批量发送 ----------------
    def _send_batch(self, msgs: List[Message], mq: Optional[MessageQueue] = None,
                    timeout_millis: Optional[int] = None) -> SendResult:
        client = self._require_client()
        timeout = timeout_millis if timeout_millis is not None else self.send_msg_timeout
        if not msgs:
            raise MQClientException("message list is empty")
        for m in msgs:
            m.topic = self._with_namespace(m.topic)
        batch = MessageBatch.generate_from_list(msgs)
        # MessageBatch 会被 try_to_compress_message 直接跳过（返回 0），批量消息永不压缩
        sys_flag = self.try_to_compress_message(batch)
        if mq is not None:
            return client.send_message(self.producer_group, batch, mq, timeout, sys_flag)
        publish = self._topic_publish_info(batch.topic)
        selected = publish.select_one_message_queue()
        mq_sel = MessageQueue(batch.topic, selected.broker_name, selected.queue_id)
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
            send_result = client.send_message(self.producer_group, msg, mq_sel,
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