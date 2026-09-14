# -*- coding: utf-8 -*-
"""消费者（对应 org.apache.rocketmq.client.consumer.* 的核心能力）。

提供：DefaultMQPushConsumer（注册监听器 + 拉取循环消费）、DefaultMQPullConsumer
（手动拉取与 offset 管理）、MessageSelector、AllocateMessageQueueStrategy 分配策略、
MessageQueueListener、消费进度管理、消息重投（sendMessageBack）等。
"""
from __future__ import annotations

import queue
import threading
import time
from typing import Callable, Dict, List, Optional, Set

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
        self._start_pull_loop()

    def shutdown(self) -> None:
        with self._lock:
            if not self._started:
                return
            self._started = False
            self._stop.set()
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
        n = max(1, self.consume_thread_min)
        for i in range(n):
            t = threading.Thread(target=self._consume_loop, daemon=True,
                                 name="rmq-consume-%s-%d" % (self.consumer_group, i))
            t.start()
            self._consume_threads.append(t)

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
                logger.warning("assigned_queues: skip topic %s: %s", topic, e)
        return result

    def _consume_loop(self) -> None:
        while not self._stop.is_set():
            try:
                if not self._started:
                    break
                self._pull_and_consume_once()
            except Exception as e:  # noqa: BLE001
                # 带类型名：否则 AttributeError 之类的编码错误只打印消息文本，
                # 很容易被当成"拉取超时"忽略掉（曾因此掩盖 pull_suspend_timeout_millis 未初始化）。
                logger.error("consume loop error: %s: %s", type(e).__name__, e)
            time.sleep(self.pull_interval / 1000.0 if self.pull_interval > 0 else 0.01)

    def _pull_and_consume_once(self) -> None:
        client = self._require_client()
        for mq in self._assigned_queues():
            if self._stop.is_set() or not self._started:
                return
            sub = None
            with self._lock:
                sub = self.subscription_data.get(mq.topic)
            if sub is None:
                continue
            key = "%s%s%d" % (mq.topic, mq.broker_name, mq.queue_id)
            offset = self._offset_table.get(key)
            if offset is None:
                offset = self._resolve_initial_offset(client, mq, sub)
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
            except MQBrokerException as e:
                if e.response_code == MQBrokerException.UNKNOWN:
                    pass
                # PULL_OFFSET_MOVED 等已映射到 PullStatus
                continue
            except RemotingTimeoutException as e:
                # 长轮询在 suspend 期间无新消息触发客户端超时属正常行为：broker 将
                # suspend 时间钳制为其自身 brokerSuspendMaxTimeMillis（默认 ~15s），
                # 忽略客户端下发的 suspend_timeout_millis，故空闲队列会周期性超时。
                # 这不是错误，仅 debug 级别，避免污染客户端运行日志（见 logging.py）。
                logger.debug("pull long-poll timeout for %s (benign, will retry): %s", mq, e)
                continue
            except Exception as e:  # noqa: BLE001
                logger.error("pull error for %s: %s", mq, e)
                continue

            if result.status == PullStatus.FOUND and result.msg_found_list:
                dispatched = self._dispatch_messages(mq, result.msg_found_list)
                self._offset_table[key] = offset + dispatched
            elif result.status == PullStatus.NO_NEW_MSG:
                self._offset_table[key] = result.next_begin_offset
            elif result.status == PullStatus.OFFSET_ILLEGAL:
                self._offset_table[key] = result.next_begin_offset

    def _resolve_initial_offset(self, client: MQClientInstance, mq: MessageQueue,
                                sub: SubscriptionData) -> int:
        if sub.expression_type == ExpressionType.SQL92:
            # SQL 过滤无 offset 语义，默认最新
            return client.get_max_offset(mq)
        try:
            from ..remoting.protocol.codes import ResponseCode
            if self.consume_from_where == ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET:
                return client.get_min_offset(mq)
            if self.consume_from_where == ConsumeFromWhere.CONSUME_FROM_TIMESTAMP:
                return client.search_offset_by_timestamp(mq, int(time.time() * 1000 - 30 * 60 * 1000))
            # 默认 CONSUME_FROM_LAST_OFFSET
            return client.get_max_offset(mq)
        except Exception:  # noqa: BLE001
            return 0

    def _dispatch_messages(self, mq: MessageQueue, msgs: List[MessageExt]) -> int:
        """把一批拉到的消息交给监听器消费，按 consume_message_batch_max_size 分批调用。

        返回实际已成功消费（可推进 offset）的消息条数；RECONSUME_LATER 的批次不前进。
        """
        listener = self.message_listener
        if listener is None:
            return 0
        batch_size = max(1, self.consume_message_batch_max_size)
        consumed = 0
        n = len(msgs)
        i = 0
        while i < n:
            batch = msgs[i:i + batch_size]
            context = ConsumeConcurrentlyContext(mq)
            try:
                if isinstance(listener, MessageListenerOrderly):
                    ocontext = ConsumeOrderlyContext(mq)
                    status = listener.consume_message(batch, ocontext)
                    if status == ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT:
                        time.sleep(self.suspend_current_queue_time_millis / 1000.0)
                        break
                else:
                    status = listener.consume_message(batch, context)
                    if status == ConsumeConcurrentlyStatus.RECONSUME_LATER:
                        # 简化：本批不推进 offset，留给后续重投
                        break
                consumed += len(batch)
                i += len(batch)
            except Exception as e:  # noqa: BLE001
                logger.error("listener error: %s", e)
                break
        return consumed

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
        header = ConsumerSendMsgBackRequestHeader()
        header.offset = msg.commit_log_offset
        header.group = self.consumer_group
        header.delay_level = delay_level
        header.origin_msg_id = msg.msg_id
        header.origin_topic = msg.topic
        header.unit_mode = False
        header.max_reconsume_times = self.max_reconsume_times
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