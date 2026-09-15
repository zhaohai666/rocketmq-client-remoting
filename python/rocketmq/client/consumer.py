# -*- coding: utf-8 -*-
"""消费者（对应 org.apache.rocketmq.client.consumer.* 的核心能力）。

提供：DefaultMQPushConsumer（注册监听器 + 拉取循环消费）、DefaultMQPullConsumer
（手动拉取与 offset 管理）、MessageSelector、AllocateMessageQueueStrategy 分配策略、
MessageQueueListener、消费进度管理、消息重投（sendMessageBack）等。
"""
from __future__ import annotations

import json
import os
import queue
import threading
import time
from collections import deque
from typing import Callable, Deque, Dict, List, Optional, Set

from ..common.message import MessageExt, MessageQueue
from ..common.mix_all import MixAll
from ..common.subscription_data import ExpressionType, FilterAPI, SubscriptionData
from ..common.sysflag import MessageSysFlag, PullSysFlag
from ..logging import get_logger
from ..remoting.exception import RemotingException, RemotingTimeoutException
from ..remoting.protocol.heartbeat import (ConsumeFromWhere, ConsumeType,
                                           HeartbeatData, MessageModel)
from ..remoting.rpchook import RPCHook
from .consumer_result import (ConsumeConcurrentlyContext, ConsumeConcurrentlyStatus,
                              ConsumeOrderlyContext, ConsumeOrderlyStatus,
                              MessageListener, MessageListenerConcurrently,
                              MessageListenerOrderly, PullResult, PullStatus)
from .exception import MQBrokerException, MQClientException
from .mq_client import MQClientInstance

logger = get_logger()


class MessageSelector:
    """消息选择器（对应 Java MessageSelector）。"""

    def __init__(self, selector_type: str = ExpressionType.TAG, expression: str = "*"):
        self.type = selector_type
        self.expression = expression

    @staticmethod
    def by_tag(tag: str) -> "MessageSelector":
        return MessageSelector(ExpressionType.TAG, tag)

    @staticmethod
    def by_sql(sql: str) -> "MessageSelector":
        return MessageSelector(ExpressionType.SQL92, sql)


class MessageQueueListener:
    """队列变更监听器（对应 Java MessageQueueListener）。"""

    def message_queue_changed(self, topic: str, mq_all: Set[MessageQueue],
                              mq_divided: Set[MessageQueue]) -> None:
        raise NotImplementedError


class AllocateMessageQueueStrategy:
    """队列分配策略接口。"""

    def allocate(self, consumer_group: str, current_cid: str, mq_all: List[MessageQueue],
                 cid_all: List[str]) -> List[MessageQueue]:
        raise NotImplementedError


class AllocateMessageQueueAveragely(AllocateMessageQueueStrategy):
    """平均分配（对应 Java AllocateMessageQueueAveragely）。"""

    def allocate(self, consumer_group: str, current_cid: str, mq_all: List[MessageQueue],
                 cid_all: List[str]) -> List[MessageQueue]:
        if not mq_all:
            return []
        if not cid_all or current_cid not in cid_all:
            return []
        index = cid_all.index(current_cid)
        mod = len(mq_all) % len(cid_all)
        average_size = len(mq_all) // len(cid_all)
        if average_size == 0:
            return [mq_all[index]] if index < len(mq_all) else []
        start_index = 0
        end_index = 0
        result = []
        if mod > 0 and index < mod:
            start_index = index * (average_size + 1)
            end_index = start_index + average_size + 1
        else:
            start_index = mod * (average_size + 1) + (index - mod) * average_size
            end_index = start_index + average_size
        return mq_all[start_index:end_index]


class AllocateMessageQueueAveragelyByCircle(AllocateMessageQueueStrategy):
    """环形平均分配（对应 Java AllocateMessageQueueAveragelyByCircle）。"""

    def allocate(self, consumer_group: str, current_cid: str, mq_all: List[MessageQueue],
                 cid_all: List[str]) -> List[MessageQueue]:
        if not mq_all:
            return []
        if not cid_all or current_cid not in cid_all:
            return []
        index = cid_all.index(current_cid)
        result = []
        for i in range(index, len(mq_all), len(cid_all)):
            result.append(mq_all[i])
        return result


class AllocateMessageQueueByConfig(AllocateMessageQueueStrategy):
    """按显式配置分配（对应 Java AllocateMessageQueueByConfig）。"""

    def __init__(self, message_queue_list: Optional[List[MessageQueue]] = None):
        self.message_queue_list = list(message_queue_list) if message_queue_list else []

    def allocate(self, consumer_group: str, current_cid: str, mq_all: List[MessageQueue],
                 cid_all: List[str]) -> List[MessageQueue]:
        return list(self.message_queue_list)


class DefaultMQPushConsumer:
    """推模式消费者（对应 org.apache.rocketmq.client.consumer.DefaultMQPushConsumer）。"""

    def __init__(self, consumer_group: str = MixAll.DEFAULT_CONSUMER_GROUP,
                 rpc_hook: Optional[RPCHook] = None, namespace: str = "",
                 message_model: str = MessageModel.CLUSTERING, **kwargs):
        if consumer_group is None or not str(consumer_group).strip():
            raise MQClientException("consumerGroup is empty")
        self.consumer_group = str(consumer_group)
        self.namespace = namespace
        self.instance_name = "DEFAULT"
        self.client_id: Optional[str] = None
        self.message_model = message_model
        self.consume_from_where = ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET
        self.consume_timestamp = time.strftime("%Y%m%d%H%M%S", time.localtime(time.time() - 30 * 60))
        self.consume_thread_min = 1
        self.consume_thread_max = 1
        self.consume_concurrently_max_span = 2000
        self.pull_threshold_for_queue = 1000
        self.pull_threshold_size_for_queue = 100
        self.pull_threshold_for_topic = -1
        self.pull_threshold_size_for_topic = -1
        self.pull_interval = 0
        self.pull_timeout_millis = 30000
        # 长轮询的 suspend 时长（对应 Java brokerSuspendMaxTimeMillis 默认 20000）。
        # 注意：这个属性必须在这里初始化——pull 循环会读它，缺了就会每轮抛
        # AttributeError 并被 _consume_loop 的兜底 except 吞掉，表现为"消费端一条都收不到"。
        self.pull_suspend_timeout_millis = 20000
        self.consume_message_batch_max_size = 1
        self.pull_batch_size = 32
        self.pull_batch_size_in_bytes = 256 * 1024
        self.max_reconsume_times = -1
        self.suspend_current_queue_time_millis = 1000
        self.consume_timeout = 15
        self.client_rebalance = True
        self.name_server_addrs: List[str] = []
        self.rpc_hook = rpc_hook
        self.allocate_strategy: AllocateMessageQueueStrategy = AllocateMessageQueueAveragely()
        # 订阅表 topic -> SubscriptionData
        self.subscription_data: Dict[str, SubscriptionData] = {}
        self.message_listener: Optional[MessageListener] = None
        self._mq_client: Optional[MQClientInstance] = None
        self._started = False
        self._pulling = False
        self._lock = threading.RLock()
        self._consume_threads: List[threading.Thread] = []
        self._msg_queue_inflight: Dict[str, int] = {}
        self._offset_table: Dict[str, int] = {}
        self._stop = threading.Event()
        # ---- 对齐 Java 的消费进度 / 缓冲 / 锁状态 ----
        # _offset_table 是"拉取游标"（nextBeginOffset）；_consume_offsets 是
        # "已消费位点"（Java ProcessQueue.removeMessage 后的 commitOffset），
        # 周期持久化到 broker 的是后者。_pending 是已拉未消费缓冲（Java ProcessQueue）。
        self._pending: Dict[str, Deque[MessageExt]] = {}
        self._mq_map: Dict[str, MessageQueue] = {}
        self._consume_offsets: Dict[str, int] = {}
        # 顺序消费：broker LOCK_BATCH_MQ 确认锁定成功的队列 key 集（Java ConsumeMessageOrderlyService）
        self._lock_ok: Set[str] = set()
        self._flow_control_triggered = 0
        self._dispatch_thread: Optional[threading.Thread] = None
        self._persist_thread: Optional[threading.Thread] = None
        self._lock_thread: Optional[threading.Thread] = None
        self._rebalance_thread: Optional[threading.Thread] = None
        self._queue_threads: Dict[str, threading.Thread] = {}

    # ---------------- 配置 ----------------
    def set_namesrv_addr(self, addr: str) -> None:
        self.name_server_addrs = [a.strip() for a in addr.split(";") if a.strip()]

    def set_name_server_addresses(self, addrs: List[str]) -> None:
        self.name_server_addrs = list(addrs)

    def set_instance_name(self, name: str) -> None:
        self.instance_name = name

    def set_message_model(self, model: str) -> None:
        self.message_model = model

    def set_consume_from_where(self, where: str) -> None:
        self.consume_from_where = where

    def set_consume_thread_nums(self, n: int) -> None:
        self.consume_thread_min = max(1, n)
        self.consume_thread_max = max(1, n)

    def set_pull_suspend_timeout_millis(self, millis: int) -> None:
        self.pull_suspend_timeout_millis = int(millis)

    def set_message_listener(self, listener) -> None:
        self.message_listener = listener

    def set_allocate_message_queue_strategy(self, strategy: AllocateMessageQueueStrategy) -> None:
        self.allocate_strategy = strategy

    def get_consumer_group(self) -> str:
        return self.consumer_group

    # ---------------- 订阅 ----------------
    def subscribe(self, topic: str, sub_expression: str = "*") -> None:
        self._assert_not_started()
        sub = FilterAPI.build_subscription_data(topic, sub_expression)
        if sub is None:
            sub = SubscriptionData(topic=topic, sub_string="*")
            sub.tags_set.add("*")
        with self._lock:
            self.subscription_data[topic] = sub

    def subscribe_with_selector(self, topic: str, selector: MessageSelector) -> None:
        self._assert_not_started()
        sub = SubscriptionData(topic=topic, sub_string=selector.expression)
        sub.expression_type = selector.type
        if selector.type == ExpressionType.TAG:
            FilterAPI.build_subscription_data(topic, selector.expression)
            sub.tags_set = FilterAPI.build_subscription_data(topic, selector.expression).tags_set
        with self._lock:
            self.subscription_data[topic] = sub

    def unsubscribe(self, topic: str) -> None:
        with self._lock:
            self.subscription_data.pop(topic, None)

    # ---------------- 生命周期 ----------------
    def start(self) -> None:
        with self._lock:
            if self._started:
                return
            if not self.name_server_addrs:
                raise MQClientException("name server address is not set")
            if not self.subscription_data:
                raise MQClientException("subscription is not set, call subscribe() first")
            if self.message_listener is None:
                raise MQClientException("message listener is not set")
            if self.client_id is None:
                self.client_id = "%s@%s" % (self.instance_name, time.strftime("%Y%m%d%H%M%S"))
            self._mq_client = MQClientInstance(self.client_id, self.name_server_addrs)
            if self.rpc_hook is not None:
                self._mq_client.remoting_client.register_rpc_hook(self.rpc_hook)
            self._mq_client.start()
            self._started = True
            self._stop.clear()
            # 集群模式自动订阅重试 topic（对齐 Java copySubscription →
            # retryTopic = MixAll.getRetryTopic(consumerGroup)），broker 重投的消息写到这里
            if self.message_model != MessageModel.BROADCASTING:
                retry_topic = MixAll.get_retry_topic(self.consumer_group)
                if retry_topic not in self.subscription_data:
                    sub = SubscriptionData(topic=retry_topic, sub_string="*")
                    sub.tags_set.add("*")
                    self.subscription_data[retry_topic] = sub
        self._start_pull_loop()
        self._start_dispatch_loop()
        self._start_offset_persist_loop()
        self._start_lock_loop()
        t = threading.Thread(target=self._rebalance_loop, daemon=True,
                             name="rmq-rebalance-%s" % self.consumer_group)
        t.start()
        self._rebalance_thread = t

    def shutdown(self) -> None:
        with self._lock:
            if not self._started:
                return
            self._stop.set()
        # 退出前把已消费位点持久化一次（对齐 Java MQClientInstance.shutdown →
        # persistAllConsumerOffset）。注意必须在 _started=False 之前调（_require_client）。
        try:
            self._persist_offsets_once()
        except Exception as e:  # noqa: BLE001
            logger.debug("persist offsets on shutdown failed: %s", e)
        # 顺序消费清退时解锁队列（对齐 Java ConsumeMessageOrderlyService.shutdown → unlockAll）
        if self._is_orderly() and self.message_model != MessageModel.BROADCASTING:
            try:
                mqs = self._assigned_queues()
                if mqs:
                    self._require_client().unlock_batch_mq(self.consumer_group, self.client_id or "", mqs)
            except Exception as e:  # noqa: BLE001
                logger.debug("unlock on shutdown failed: %s", e)
        with self._lock:
            self._started = False
        for t in (self._persist_thread, self._lock_thread, self._dispatch_thread,
                  self._rebalance_thread):
            if t is not None and t.is_alive():
                t.join(timeout=2)
        for t in list(self._queue_threads.values()):
            if t.is_alive():
                t.join(timeout=2)
        self._queue_threads.clear()
        if self._mq_client is not None:
            try:
                self._mq_client.shutdown()
            except Exception:  # noqa: BLE001
                pass
        for t in self._consume_threads:
            if t.is_alive():
                t.join(timeout=2)
        self._consume_threads.clear()

    def _assert_not_started(self) -> None:
        if self._started:
            raise MQClientException("consumer already started, cannot change configuration")

    def _require_client(self) -> MQClientInstance:
        if not self._started or self._mq_client is None:
            raise MQClientException("consumer not started, call start() first")
        return self._mq_client

    # ---------------- 消费循环 ----------------
    def _start_pull_loop(self) -> None:
        self._pulling = True
        # 对齐 Java PullMessageService 的并发长轮询语义：broker 会为每个队列挂起
        # 长轮询请求，消息到达立即返回——因此每个队列必须各有一个拉取线程，
        # 否则一个空队列的长轮询（~15s suspend）会阻塞其余队列的投递。
        self._rebalance_pull_threads()

    def _rebalance_pull_threads(self) -> None:
        """按当前分配的队列同步拉取线程集（简化 rebalance 的线程侧实现）。"""
        current = {self._mq_key(mq): mq for mq in self._assigned_queues()}
        with self._lock:
            for key, mq in current.items():
                if key in self._queue_threads:
                    continue
                t = threading.Thread(target=self._queue_pull_loop, args=(mq,), daemon=True,
                                     name="rmq-pull-%s-%s" % (self.consumer_group, key))
                self._queue_threads[key] = t
                t.start()
            for key in list(self._queue_threads.keys()):
                if key not in current:
                    self._queue_threads.pop(key, None)  # 循环内检测到退出

    @staticmethod
    def _mq_key(mq: MessageQueue) -> str:
        return "%s%s%d" % (mq.topic, mq.broker_name, mq.queue_id)

    def _assigned_queues(self) -> List[MessageQueue]:
        """简化 rebalance：本进程所有订阅 topic 的全部队列。"""
        client = self._require_client()
        result: List[MessageQueue] = []
        with self._lock:
            topics = list(self.subscription_data.keys())
        for topic in topics:
            try:
                publish = client.get_topic_publish_info(topic)
                for mq in publish.msg_queue_list:
                    mq2 = MessageQueue(topic, mq.broker_name, mq.queue_id)
                    if mq2 not in result:
                        result.append(mq2)
            except MQClientException as e:  # noqa: BLE001
                logger.debug("assigned_queues: skip topic %s: %s", topic, e)
        return result

    def _rebalance_loop(self) -> None:
        """周期刷新分配集，为新增队列补拉取线程（对应 Java doRebalance 的简化版）。"""
        while not self._stop.wait(5.0):
            try:
                self._rebalance_pull_threads()
            except Exception as e:  # noqa: BLE001
                logger.debug("rebalance pull threads error: %s", e)

    def _queue_pull_loop(self, mq: MessageQueue) -> None:
        """单队列拉取循环：长轮询拉取 → 推入待消费缓冲（Java PullMessageService+ProcessQueue）。"""
        client = self._require_client()
        orderly = self._is_orderly()
        key = self._mq_key(mq)
        while not self._stop.is_set() and self._started:
            with self._lock:
                still_assigned = key in self._queue_threads and self._queue_threads[key] is threading.current_thread()
                sub = self.subscription_data.get(mq.topic)
            if not still_assigned or sub is None:
                return
            # 顺序消费：broker 未确认锁定（LOCK_BATCH_MQ）的队列不拉取
            if orderly and key not in self._lock_ok:
                time.sleep(0.2)
                continue
            # 流控（对齐 Java ProcessQueue.putMessage 的 pullThresholdForQueue 检查）：
            # 已拉未消费的条数超过阈值就暂停本队列拉取
            with self._lock:
                pending_n = len(self._pending.get(key, ()))
            if pending_n >= max(1, self.pull_threshold_for_queue):
                self._flow_control_triggered += 1
                logger.debug("flow control: queue %s pending=%d >= threshold=%d, pause pull",
                             mq, pending_n, self.pull_threshold_for_queue)
                time.sleep(0.1)
                continue
            offset = self._offset_table.get(key)
            if offset is None:
                try:
                    offset = self._resolve_initial_offset(client, mq, sub)
                except Exception as e:  # noqa: BLE001
                    logger.debug("resolve initial offset failed for %s: %s", mq, e)
                    time.sleep(1.0)
                    continue
                self._offset_table[key] = offset
            try:
                sys_flag = PullSysFlag.build_sys_flag(commit_offset=False,
                                                      suspend=True,
                                                      subscription=True,
                                                      class_filter=False)
                result = client.pull_message(self.consumer_group, mq, offset,
                                             self.pull_batch_size, sys_flag, 0,
                                             sub.sub_string or "*", sub.sub_version,
                                             sub.expression_type,
                                             timeout_millis=self.pull_timeout_millis,
                                             max_msg_bytes=self.pull_batch_size_in_bytes,
                                             suspend_timeout_millis=self.pull_suspend_timeout_millis)
            except RemotingTimeoutException as e:
                # 长轮询在 suspend 期间无新消息触发客户端超时属正常行为：broker 将
                # suspend 时间钳制为其自身 brokerSuspendMaxTimeMillis（默认 ~15s），
                # 忽略客户端下发的 suspend_timeout_millis，故空闲队列会周期性超时。
                # 这不是错误，仅 debug 级别，避免污染客户端运行日志（见 logging.py）。
                logger.debug("pull long-poll timeout for %s (benign, will retry): %s", mq, e)
                continue
            except Exception as e:  # noqa: BLE001
                # broker 侧非 SUCCESS 码（TOPIC_NOT_EXIST / PULL_NOT_FOUND 等）或其他错误：
                # 多为 topic 尚未创建等预期路径，debug + 短暂退避，避免热循环
                logger.debug("pull error for %s: %s: %s", mq, type(e).__name__, e)
                time.sleep(0.5)
                continue

            with self._lock:
                if key not in self._pending:
                    self._pending[key] = deque()
                    self._mq_map[key] = mq
            if result.status == PullStatus.FOUND and result.msg_found_list:
                with self._lock:
                    self._pending[key].extend(result.msg_found_list)
            # 拉取游标推进到 nextBeginOffset；"已消费位点"由 _consume_offsets 单独跟踪并持久化
            if result.next_begin_offset is not None:
                self._offset_table[key] = result.next_begin_offset

    def _resolve_initial_offset(self, client: MQClientInstance, mq: MessageQueue,
                                sub: SubscriptionData) -> int:
        if sub.expression_type == ExpressionType.SQL92:
            # SQL 过滤无 offset 语义，默认最新
            return client.get_max_offset(mq)
        if self.message_model == MessageModel.BROADCASTING:
            # 广播模式：offset 只存本地（对齐 Java LocalFileOffsetStore）
            stored = self._load_local_offsets()
            key = "%s%s%d" % (mq.topic, mq.broker_name, mq.queue_id)
            if key in stored:
                return stored[key]
            if self.consume_from_where == ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET:
                return client.get_min_offset(mq)
            return client.get_max_offset(mq)
        # 集群模式：先查 broker 上已提交的位点（对齐 Java RemoteBrokerOffsetStore.readOffset）
        try:
            stored = client.query_consumer_offset(self.consumer_group, mq, set_zero_if_not_found=False)
            if stored is not None and stored >= 0:
                return stored
        except Exception as e:  # noqa: BLE001
            logger.debug("query consumer offset for %s not found: %s", mq, e)
        if self.consume_from_where == ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET:
            return client.get_min_offset(mq)
        if self.consume_from_where == ConsumeFromWhere.CONSUME_FROM_TIMESTAMP:
            return client.search_offset_by_timestamp(mq, int(time.time() * 1000 - 30 * 60 * 1000))
        # 默认 CONSUME_FROM_LAST_OFFSET
        return client.get_max_offset(mq)

    # ---------------- 分发消费 ----------------
    def _start_dispatch_loop(self) -> None:
        t = threading.Thread(target=self._dispatch_loop, daemon=True,
                             name="rmq-dispatch-%s" % self.consumer_group)
        t.start()
        self._dispatch_thread = t

    def _dispatch_loop(self) -> None:
        while not self._stop.is_set():
            progressed = False
            with self._lock:
                keys = list(self._pending.keys())
            for key in keys:
                if self._stop.is_set() or not self._started:
                    return
                mq = self._mq_map.get(key)
                if mq is None:
                    continue
                with self._lock:
                    dq = self._pending.get(key)
                    batch = [dq.popleft() for _ in range(min(len(dq) if dq else 0,
                                                             max(1, self.consume_message_batch_max_size)))]
                if not batch:
                    continue
                try:
                    done = self._consume_batch(key, mq, batch)
                    progressed = progressed or done
                except Exception as e:  # noqa: BLE001
                    # 分发路径意外异常：批次塞回队首，稍后重试（不要让它杀死分发线程）
                    logger.error("dispatch batch error (will retry): %s: %s", type(e).__name__, e)
                    with self._lock:
                        dq2 = self._pending.get(key)
                        if dq2 is not None:
                            for m in reversed(batch):
                                dq2.appendleft(m)
                    time.sleep(0.1)
            if not progressed:
                time.sleep(0.05)

    def _consume_batch(self, key: str, mq: MessageQueue, batch: List[MessageExt]) -> bool:
        """消费一个批次并处理回投/挂起。返回消费位点是否前进。"""
        listener = self.message_listener
        broadcast = self.message_model == MessageModel.BROADCASTING
        # ---- 顺序消费（Java ConsumeMessageOrderlyService）----
        if self._is_orderly():
            ocontext = ConsumeOrderlyContext(mq)
            try:
                status = listener.consume_message(batch, ocontext)
            except Exception as e:  # noqa: BLE001
                # Java 顺序消费：异常 → 不提交 offset，原地重试
                logger.debug("orderly listener error (retry in place): %s", e)
                status = ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT
            if status == ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT:
                with self._lock:
                    dq = self._pending.get(key)
                    if dq is not None:
                        for m in reversed(batch):
                            dq.appendleft(m)
                time.sleep(self.suspend_current_queue_time_millis / 1000.0)
                return False
            self._advance_consume_offset(key, batch)
            return True
        # ---- 并发消费（Java ConsumeMessageConcurrentlyService$ConsumeRequest.run）----
        context = ConsumeConcurrentlyContext(mq)
        try:
            status = listener.consume_message(batch, context)
        except Exception as e:  # noqa: BLE001
            # Java：消费抛异常按 RECONSUME_LATER 处理
            logger.debug("listener error, treat as RECONSUME_LATER: %s", e)
            status = ConsumeConcurrentlyStatus.RECONSUME_LATER
        if status == ConsumeConcurrentlyStatus.CONSUME_SUCCESS:
            self._advance_consume_offset(key, batch)
            return True
        # RECONSUME_LATER：广播模式不回投（仅告警，位点不前进，重启后重新消费）；
        # 集群模式回投 %RETRY%topic（延迟梯度 3+reconsumeTimes；超过 maxReconsumeTimes
        # 由 broker 自动转 %DLQ%）
        if broadcast:
            logger.warning("BROADCASTING: message consume failed, no redelivery: %d msgs in %s",
                           len(batch), mq)
            self._advance_consume_offset(key, batch)
            return True
        if self._send_back_batch(batch, context):
            self._advance_consume_offset(key, batch)
            return True
        # 回投失败：批次塞回队首稍后重试（Java 中这些消息不从 ProcessQueue 移除）
        with self._lock:
            dq = self._pending.get(key)
            if dq is not None:
                for m in reversed(batch):
                    dq.appendleft(m)
        time.sleep(0.2)
        return False

    def _send_back_batch(self, batch: List[MessageExt],
                         context: ConsumeConcurrentlyContext) -> bool:
        """失败批次逐条回投 broker（对齐 Java processConsumeResult → sendMessageBack）。"""
        ok = True
        for msg in batch:
            try:
                # 重投次数在 MessageExt 线上格式第 13 字段（Java msg.getReconsumeTimes()），
                # broker 重投时会 +1；不是 properties 键（Java 的 PROPERTY_RECONSUME_TIME
                # 实际值是 "RECONSUME_TIME"，仅由 MessageAccessor.setReconsumeTime 写入）
                delay_level = context.delay_level_when_next_consume
                if not delay_level:
                    # Java：delayLevelWhenNextConsume == 0 → 3 + reconsumeTimes
                    delay_level = 3 + msg.get_reconsume_times()
                self.send_message_back(msg, delay_level)
            except Exception as e:  # noqa: BLE001
                logger.debug("send message back failed for msg %s: %s", msg.msg_id, e)
                ok = False
        return ok

    def _advance_consume_offset(self, key: str, batch: List[MessageExt]) -> None:
        next_off = max((m.queue_offset or 0) for m in batch) + 1
        with self._lock:
            cur = self._consume_offsets.get(key)
            self._consume_offsets[key] = max(cur or 0, next_off)

    # ---------------- 位点持久化 ----------------
    def _start_offset_persist_loop(self) -> None:
        t = threading.Thread(target=self._offset_persist_loop, daemon=True,
                             name="rmq-offset-persist-%s" % self.consumer_group)
        t.start()
        self._persist_thread = t

    def _offset_persist_loop(self) -> None:
        # Java MQClientInstance.startScheduledTask：persistAllConsumerOffset 每 5s
        while not self._stop.wait(5.0):
            try:
                self._persist_offsets_once()
            except Exception as e:  # noqa: BLE001
                logger.debug("persist offsets error: %s", e)

    def _persist_offsets_once(self) -> None:
        if self.message_model == MessageModel.BROADCASTING:
            self._save_local_offsets()
            return
        client = self._require_client()
        with self._lock:
            items = list(self._consume_offsets.items())
        for key, off in items:
            mq = self._mq_map.get(key)
            if mq is None:
                continue
            try:
                client.update_consumer_offset(self.consumer_group, mq, off)
            except Exception as e:  # noqa: BLE001
                logger.debug("update consumer offset failed for %s: %s", mq, e)

    def _local_offset_path(self) -> str:
        # Java LocalFileOffsetStore：$HOME/.rocketmq_offsets/<clientId>/<group>/offsets.json
        base = os.path.join(os.path.expanduser("~"), ".rocketmq_offsets",
                            self.client_id or "DEFAULT", self.consumer_group)
        return os.path.join(base, "offsets.json")

    def _save_local_offsets(self) -> None:
        with self._lock:
            items = dict(self._consume_offsets)
        path = self._local_offset_path()
        os.makedirs(os.path.dirname(path), exist_ok=True)
        tmp = path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(items, f)
        os.replace(tmp, path)

    def _load_local_offsets(self) -> Dict[str, int]:
        try:
            with open(self._local_offset_path(), "r", encoding="utf-8") as f:
                return {k: int(v) for k, v in json.load(f).items()}
        except (OSError, ValueError):
            return {}

    # ---------------- 顺序消费队列锁 ----------------
    def _start_lock_loop(self) -> None:
        if not self._is_orderly() or self.message_model == MessageModel.BROADCASTING:
            return
        t = threading.Thread(target=self._lock_loop, daemon=True,
                             name="rmq-lock-%s" % self.consumer_group)
        t.start()
        self._lock_thread = t

    def _lock_loop(self) -> None:
        client = self._require_client()
        # Java ConsumeMessageOrderlyService.lockMQ：每 20s 批量锁分到的队列；
        # 启动时立刻尝试一次，避免首个 20s 空转
        while not self._stop.is_set():
            try:
                mqs = self._assigned_queues()
                if mqs:
                    ok = client.lock_batch_mq(self.consumer_group, self.client_id or "", mqs)
                    ok_keys = {"%s%s%d" % (m.topic, m.broker_name, m.queue_id) for m in ok}
                    with self._lock:
                        self._lock_ok = ok_keys
                    logger.debug("lock_batch_mq: %d/%d queues locked", len(ok_keys), len(mqs))
            except Exception as e:  # noqa: BLE001
                logger.debug("lock mq error: %s", e)
            self._stop.wait(20.0)

    def _is_orderly(self) -> bool:
        return isinstance(self.message_listener, MessageListenerOrderly)

    # ---------------- 管理能力 ----------------
    def fetch_subscribe_message_queues(self, topic: str) -> List[MessageQueue]:
        client = self._require_client()
        publish = client.get_topic_publish_info(topic)
        return [MessageQueue(q.topic, q.broker_name, q.queue_id) for q in publish.msg_queue_list]

    def send_message_back(self, msg: MessageExt, delay_level: int,
                          broker_name: Optional[str] = None) -> None:
        """消息重投（对应 Java sendMessageBack）。"""
        client = self._require_client()
        from ..remoting.protocol.codes import RequestCode
        from ..remoting.protocol.headers import ConsumerSendMsgBackRequestHeader
        from ..remoting.protocol.remoting_command import RemotingCommand
        if broker_name is None:
            broker_name = msg.broker_name
        addr = client.broker_addr_of(broker_name)
        if addr is None:
            raise MQClientException("broker %s not found" % broker_name)
        # Java：maxReconsumeTimes == -1 时按 16 传给 broker（超限由 broker 转 %DLQ%）
        max_reconsume = 16 if self.max_reconsume_times == -1 else self.max_reconsume_times
        header = ConsumerSendMsgBackRequestHeader()
        header.offset = msg.commit_log_offset
        header.group = self.consumer_group
        header.delay_level = delay_level
        header.origin_msg_id = msg.msg_id
        header.origin_topic = msg.topic
        header.unit_mode = False
        header.max_reconsume_times = max_reconsume
        request = RemotingCommand.create_request_command(RequestCode.CONSUMER_SEND_MSG_BACK, header)
        response = client._invoke_sync(addr, request, 5000)
        client._check_response(response)


class DefaultMQPullConsumer:
    """拉模式消费者（对应 org.apache.rocketmq.client.consumer.DefaultMQPullConsumer）。"""

    def __init__(self, consumer_group: str = MixAll.DEFAULT_CONSUMER_GROUP,
                 rpc_hook: Optional[RPCHook] = None, namespace: str = "",
                 message_model: str = MessageModel.CLUSTERING):
        if consumer_group is None or not str(consumer_group).strip():
            raise MQClientException("consumerGroup is empty")
        self.consumer_group = str(consumer_group)
        self.namespace = namespace
        self.instance_name = "DEFAULT"
        self.client_id: Optional[str] = None
        self.message_model = message_model
        self.broker_suspend_max_time_millis = 20000
        self.consumer_pull_timeout_millis = 10000
        self.consumer_timeout_millis_when_suspend = 30000
        self.name_server_addrs: List[str] = []
        self.rpc_hook = rpc_hook
        self.register_topics: Set[str] = set()
        self.message_queue_lists: List[MessageQueue] = []
        self.message_queue_listener: Optional[MessageQueueListener] = None
        self._mq_client: Optional[MQClientInstance] = None
        self._started = False

    # ---------------- 配置 ----------------
    def set_namesrv_addr(self, addr: str) -> None:
        self.name_server_addrs = [a.strip() for a in addr.split(";") if a.strip()]

    def set_name_server_addresses(self, addrs: List[str]) -> None:
        self.name_server_addrs = list(addrs)

    def set_instance_name(self, name: str) -> None:
        self.instance_name = name

    def set_message_model(self, model: str) -> None:
        self.message_model = model

    def set_message_queue_listener(self, listener: MessageQueueListener) -> None:
        self.message_queue_listener = listener

    def get_register_topics(self) -> Set[str]:
        return self.register_topics

    # ---------------- 生命周期 ----------------
    def start(self) -> None:
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
        if not self._started:
            return
        self._started = False
        if self._mq_client is not None:
            self._mq_client.shutdown()

    def _require_client(self) -> MQClientInstance:
        if not self._started or self._mq_client is None:
            raise MQClientException("consumer not started, call start() first")
        return self._mq_client

    # ---------------- 拉取 ----------------
    def fetch_subscribe_message_queues(self, topic: str) -> List[MessageQueue]:
        client = self._require_client()
        publish = client.get_topic_publish_info(topic)
        return [MessageQueue(q.topic, q.broker_name, q.queue_id) for q in publish.msg_queue_list]

    def pull(self, mq: MessageQueue, sub_expression: str = "*", offset: int = 0,
             max_nums: int = 32, timeout_millis: Optional[int] = None) -> PullResult:
        client = self._require_client()
        timeout = timeout_millis if timeout_millis is not None else self.consumer_pull_timeout_millis
        sub = FilterAPI.build_subscription_data(mq.topic, sub_expression)
        sys_flag = PullSysFlag.build_sys_flag(commit_offset=(self.message_model != MessageModel.BROADCASTING),
                                              suspend=True, subscription=True, class_filter=False)
        return client.pull_message(self.consumer_group, mq, offset, max_nums,
                                   sys_flag, 0, sub.sub_string or "*", sub.sub_version,
                                   ExpressionType.TAG, timeout_millis=timeout,
                                   max_msg_bytes=-1, suspend_timeout_millis=15000)

    def pull_block_if_not_found(self, mq: MessageQueue, sub_expression: str, offset: int,
                                max_nums: int) -> PullResult:
        """长轮询拉取（对应 Java pullBlockIfNotFound）。"""
        client = self._require_client()
        sub = FilterAPI.build_subscription_data(mq.topic, sub_expression)
        sys_flag = PullSysFlag.build_sys_flag(commit_offset=(self.message_model != MessageModel.BROADCASTING),
                                              suspend=True, subscription=True, class_filter=False)
        return client.pull_message(self.consumer_group, mq, offset, max_nums,
                                   sys_flag, 0, sub.sub_string or "*", sub.sub_version,
                                   ExpressionType.TAG, timeout_millis=self.consumer_timeout_millis_when_suspend,
                                   max_msg_bytes=-1,
                                   suspend_timeout_millis=self.broker_suspend_max_time_millis)

    # ---------------- Offset 管理 ----------------
    def fetch_consume_offset(self, mq: MessageQueue) -> Optional[int]:
        return self._require_client().query_consumer_offset(self.consumer_group, mq)

    def update_consume_offset(self, mq: MessageQueue, offset: int) -> None:
        self._require_client().update_consumer_offset(self.consumer_group, mq, offset)

    def search_offset(self, mq: MessageQueue, timestamp: int) -> int:
        return self._require_client().search_offset_by_timestamp(mq, timestamp)

    def max_offset(self, mq: MessageQueue) -> int:
        return self._require_client().get_max_offset(mq)

    def min_offset(self, mq: MessageQueue) -> int:
        return self._require_client().get_min_offset(mq)

    def earliest_msg_store_time(self, mq: MessageQueue) -> int:
        client = self._require_client()
        from ..remoting.protocol.codes import RequestCode
        from ..remoting.protocol.headers import (GetEarliestMsgStoretimeRequestHeader,
                                                 GetEarliestMsgStoretimeResponseHeader)
        from ..remoting.protocol.remoting_command import RemotingCommand
        addr = client._broker_addr(mq)
        header = GetEarliestMsgStoretimeRequestHeader()
        header.topic = mq.topic
        header.queue_id = mq.queue_id
        request = RemotingCommand.create_request_command(RequestCode.GET_EARLIEST_MSG_STORETIME, header)
        response = client._invoke_sync(addr, request, 5000)
        client._check_response(response)
        resp_header = GetEarliestMsgStoretimeResponseHeader()
        resp_header.from_ext_fields(response.ext_fields)
        return resp_header.timestamp or 0

    def send_message_back(self, msg: MessageExt, delay_level: int) -> None:
        client = self._require_client()
        from ..remoting.protocol.codes import RequestCode
        from ..remoting.protocol.headers import ConsumerSendMsgBackRequestHeader
        from ..remoting.protocol.remoting_command import RemotingCommand
        addr = client.broker_addr_of(msg.broker_name or "")
        if addr is None:
            raise MQClientException("broker %s not found" % msg.broker_name)
        header = ConsumerSendMsgBackRequestHeader()
        header.offset = msg.commit_log_offset
        header.group = self.consumer_group
        header.delay_level = delay_level
        header.origin_msg_id = msg.msg_id
        header.origin_topic = msg.topic
        header.unit_mode = False
        header.max_reconsume_times = -1
        request = RemotingCommand.create_request_command(RequestCode.CONSUMER_SEND_MSG_BACK, header)
        response = client._invoke_sync(addr, request, 5000)
        client._check_response(response)

    def create_topic(self, key: str, new_topic: str, queue_num: int = 4,
                     topic_sys_flag: int = 0) -> None:
        client = self._require_client()
        client.create_topic_in_route(new_topic, queue_num, queue_num, 6)


class SimpleMessageListener(MessageListenerConcurrently):
    """便捷监听器包装：把消费逻辑转成纯函数。"""

    def __init__(self, fn: Callable[[List[MessageExt]], ConsumeConcurrentlyStatus]):
        self.fn = fn

    def consume_message(self, msgs: List[MessageExt],
                        context: ConsumeConcurrentlyContext) -> ConsumeConcurrentlyStatus:
        return self.fn(msgs)


__all__ = [
    "DefaultMQPushConsumer", "DefaultMQPullConsumer", "MessageSelector",
    "MessageQueueListener", "AllocateMessageQueueStrategy",
    "AllocateMessageQueueAveragely", "AllocateMessageQueueAveragelyByCircle",
    "AllocateMessageQueueByConfig", "SimpleMessageListener",
    "PullResult", "PullStatus", "MessageListener", "MessageListenerConcurrently",
    "MessageListenerOrderly", "ConsumeConcurrentlyStatus", "ConsumeOrderlyStatus",
]