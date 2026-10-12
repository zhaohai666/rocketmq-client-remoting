# -*- coding: utf-8 -*-
from __future__ import annotations

import random
import threading
import time
from typing import TYPE_CHECKING, Any, Callable, Dict, Iterable, List, Optional

from common.boundary_type import BoundaryType
from common.message import Message, MessageBatch, MessageExt, MessageQueue
from common.message_accessor import MessageAccessor
from common.message_client_id_setter import get_uniq_id, set_uniq_id
from common.message_const import MessageConst
from common.message_decoder import (decode_message, decode_messages, decompress_body,
                                      message_properties_2_string,
                                      string_2_message_properties)
from common.mix_all import MixAll
from common.subscription_data import ExpressionType, SubscriptionData
from common.sysflag import MessageSysFlag, PullSysFlag
from client import static_topic
from common.topic_config import TopicFilterType
from rocketmq_logging import get_logger
from remoting.client import RemotingClient
from remoting.protocol.body import (CheckClientRequestBody, ClusterInfo,
                                      ConsumerRunningInfo, ConsumeMessageDirectlyResult,
                                      GetConsumerStatusBody,
                                      GetConsumerListByGroupResponseBody, ResetOffsetBody,
                                      ResetOffsetBodyForC, TopicList)

if TYPE_CHECKING:
    from .consumer import DefaultMQPushConsumer
from remoting.protocol.codes import RequestCode, ResponseCode, SerializeType
from remoting.protocol.headers import (ConsumeMessageDirectlyResultRequestHeader,
                                         CreateTopicRequestHeader,
                                         GetConsumerListByGroupRequestHeader,
                                         GetConsumerRunningInfoRequestHeader,
                                         GetConsumerStatusRequestHeader,
                                         GetMaxOffsetRequestHeader,
                                         GetMaxOffsetResponseHeader, GetMinOffsetRequestHeader,
                                         GetMinOffsetResponseHeader,
                                         NotifyConsumerIdsChangedRequestHeader,
                                         PullMessageRequestHeader,
                                         PullMessageResponseHeader, QueryConsumerOffsetRequestHeader,
                                         QueryConsumerOffsetResponseHeader, QueryMessageRequestHeader,
                                         QueryMessageResponseHeader,
                                         RecallMessageRequestHeader, RecallMessageResponseHeader,
                                         ReplyMessageRequestHeader,
                                         ResetOffsetRequestHeader,
                                         SearchOffsetRequestHeader,
                                         SearchOffsetResponseHeader, SendMessageRequestHeader,
                                         SendMessageRequestHeaderV2, SendMessageResponseHeader,
                                         UpdateConsumerOffsetRequestHeader)
from remoting.protocol.heartbeat import HeartbeatData
from remoting.protocol.remoting_command import RemotingCommand
from remoting.protocol.route import TopicRouteData
from remoting.rpchook import NamespaceRpcHook, StreamTypeRPCHook
from .consumer_stats import ConsumerStatsManager
from .exception import ClientErrorCode, MQBrokerException, MQClientException
from .mq_client_manager import MQClientManager
from .request_reply import REQUEST_FUTURE_HOLDER, is_reply_message
from .send_result import SendResult, SendStatus
from .top_addressing import DefaultTopAddressing

logger = get_logger()


class TopicPublishInfo:
    pass

    def __init__(self):
        self.order_topic = False
        self.msg_queue_list: List[MessageQueue] = []
        self.topic_route_data: Optional[TopicRouteData] = None
        self._index = 0
        self._lock = threading.Lock()

    def ok(self) -> bool:
        return len(self.msg_queue_list) > 0

    def reset_index(self) -> None:
        self._index = 0

    def select_one_message_queue(self, *filters) -> Optional[MessageQueue]:
        pass
        with self._lock:
            if not self.msg_queue_list:
                raise MQClientException("no message queue for publish info")
            n = len(self.msg_queue_list)
            if not filters:
                mq = self.msg_queue_list[self._index % n]
                self._index += 1
                return mq
            for _ in range(n):
                mq = self.msg_queue_list[self._index % n]
                self._index += 1
                if all(f(mq) for f in filters):
                    return mq
            return None

    def to_dict(self) -> dict:
        return {
            "orderTopic": self.order_topic,
            "messageQueueList": [
                {"topic": q.topic, "brokerName": q.broker_name, "queueId": q.queue_id}
                for q in self.msg_queue_list
            ],
        }


class MQClientInstance:
    INSTANCE_MAP: Dict[str, "MQClientInstance"] = {}
    INSTANCE_LOCK = threading.Lock()

    def __init__(self, client_id: str, name_server_addrs: List[str],
                 connect_timeout_millis: int = 3000, invoke_timeout_millis: int = 15000,
                 tls_enable: Optional[bool] = None,
                 tls_options: Optional[Dict[str, str]] = None,
                 enable_stream_request_type: bool = False,
                 unit_name: Optional[str] = None,
                 namespace_v2: Optional[str] = None,
                 poll_name_server_interval: int = 30000,
                 socks_proxy_config=None):
        self.client_id = client_id
        self.name_server_addrs: List[str] = list(name_server_addrs)
        self.poll_name_server_interval = poll_name_server_interval
        self.namespace_v2 = namespace_v2 or ""
        self.socks_proxy_config = socks_proxy_config
        self.remoting_client = RemotingClient(connect_timeout_millis, invoke_timeout_millis,
                                              tls_enable=tls_enable, tls_options=tls_options,
                                              socks_proxy_config=socks_proxy_config)
        self.remoting_client.register_rpc_hook(NamespaceRpcHook(self.namespace_v2))
        if enable_stream_request_type:
            self.remoting_client.register_rpc_hook(StreamTypeRPCHook())
        self.topic_route_table: Dict[str, TopicRouteData] = {}
        self.topic_publish_info_table: Dict[str, TopicPublishInfo] = {}
        self.broker_addr_table: Dict[str, Dict[int, str]] = {}
        self.topic_end_points_table: Dict[str, Dict[MessageQueue, str]] = {}
        self.topic_route_lock = threading.RLock()
        self._started = False
        self._last_route_fetch = 0.0
        self._topics_in_use: set = set()
        self._route_refresh_thread: Optional[threading.Thread] = None
        self._route_refresh_stop = threading.Event()
        self.remoting_client.register_processor(
            RequestCode.PUSH_REPLY_MESSAGE_TO_CLIENT, self._process_reply_message)
        self.remoting_client.register_processor(
            RequestCode.RESET_CONSUMER_CLIENT_OFFSET, self._process_reset_offset)
        self.remoting_client.register_processor(
            RequestCode.GET_CONSUMER_STATUS_FROM_CLIENT, self._process_get_consumer_status)
        self.remoting_client.register_processor(
            RequestCode.GET_CONSUMER_RUNNING_INFO, self._process_get_consumer_running_info)
        self.remoting_client.register_processor(
            RequestCode.CONSUME_MESSAGE_DIRECTLY, self._process_consume_message_directly)
        self.remoting_client.register_processor(
            RequestCode.NOTIFY_CONSUMER_IDS_CHANGED, self._process_notify_consumer_ids_changed)
        # 40 只能由 broker 发出，用例无法注入反向请求，所以计数是唯一能断言
        # 「本端确实收到并处理过」的落点（见 python/tests/test_broker_requests.py）。
        self._consumer_ids_changed_count = 0
        self.remoting_client.register_processor(
            RequestCode.CHECK_TRANSACTION_STATE, self._dispatch_check_transaction_state)
        self._consumer_table: Dict[str, "DefaultMQPushConsumer"] = {}
        self._transaction_check_handlers: List[Callable] = []
        self.top_addressing = DefaultTopAddressing(unit_name=unit_name or "")
        self.consumer_stats_manager = ConsumerStatsManager()
        self._namesrv_refresh_stop = threading.Event()
        self._namesrv_refresh_thread: Optional[threading.Thread] = None
        self._adjust_pool_stop = threading.Event()
        self._adjust_pool_thread: Optional[threading.Thread] = None
        self._lifecycle_lock = threading.RLock()
        MQClientInstance.INSTANCE_MAP[client_id] = self

    def fetch_name_server_addr(self) -> Optional[str]:
        pass
        if self.top_addressing is None or not self.top_addressing.ws_addr:
            return None
        changed = self.top_addressing.fetch_and_apply()
        if changed:
            addrs = [a.strip() for a in changed.split(";") if a.strip()]
            self.update_name_server_address_list(addrs)
        return changed

    def adjust_thread_pool(self) -> None:
        pass
        for group, consumer in list(self._consumer_table.items()):
            if consumer is None:
                continue
            try:
                consumer.adjust_thread_pool()
            except Exception as e:
                logger.debug("adjustThreadPool failed for group %s: %s", group, e)

    # ---------------- 消费者注册（broker 主动请求按 group 分派） ----------------
    def register_consumer(self, group: str, consumer: "DefaultMQPushConsumer") -> None:
        pass
        self._consumer_table[group] = consumer

    def unregister_consumer(self, group: str) -> None:
        self._consumer_table.pop(group, None)

    def find_consumer(self, group: str) -> Optional["DefaultMQPushConsumer"]:
        return self._consumer_table.get(group)

    # ---------------- 事务回查处理器登记（39 分派的门面侧） ----------------
    def register_transaction_check_handler(self, handler: Callable) -> None:
        pass
        with self._lifecycle_lock:
            if handler not in self._transaction_check_handlers:
                self._transaction_check_handlers.append(handler)

    def unregister_transaction_check_handler(self, handler: Callable) -> None:
        with self._lifecycle_lock:
            try:
                self._transaction_check_handlers.remove(handler)
            except ValueError:
                pass

    def _dispatch_check_transaction_state(self, cmd: RemotingCommand,
                                          addr: str) -> Optional[RemotingCommand]:
        """39 是 oneway，不回响应；把请求交给每个登记的生产者处理器。"""
        for handler in list(self._transaction_check_handlers):
            try:
                handler(cmd, addr)
            except Exception as e:  # noqa: BLE001 —— 单个生产者的异常不影响其他门面
                logger.warning("checkTransactionState dispatch failed: %s", e)
        return None


    # ---------------- CHECK_CLIENT_CONFIG(46)：订阅表达式向 broker 求证 ----------------
    def check_client_in_broker(self) -> None:
        pass
        for group, consumer in list(self._consumer_table.items()):
            subs = consumer.subscriptions() if consumer is not None else None
            if not subs:
                return
            self.check_subscriptions_in_broker(group, subs)

    def check_subscriptions_in_broker(self, group: str,
                                      subs: Iterable[SubscriptionData]) -> None:
        pass
        for sub in subs:
            if sub is None or not sub.expression_type or sub.expression_type == ExpressionType.TAG:
                continue
            addr = self.find_broker_addr_by_topic(sub.topic or "")
            if addr is None:
                continue
            try:
                self.check_client_config(addr, group, self.client_id, sub)
            except MQClientException:
                raise
            except Exception as e:  # noqa: BLE001
                raise MQClientException(
                    "Check client in broker error, maybe because you use %s to filter "
                    "message, but server has not been upgraded to support!This error would "
                    "not affect the launch of consumer, but may has impact on message "
                    "receiving if you have use the new features which are not supported by "
                    "server, please check the log!" % sub.expression_type, cause=e)

    # ---------------- 40 NOTIFY_CONSUMER_IDS_CHANGED ----------------
    @property
    def consumer_ids_changed_count(self) -> int:
        """本实例收到过多少次 broker 的 ``NOTIFY_CONSUMER_IDS_CHANGED(40)``。"""
        return self._consumer_ids_changed_count

    def rebalance_immediately(self) -> None:
        pass
        for consumer in list(self._consumer_table.values()):
            if consumer is None:
                continue
            wake = getattr(consumer, "rebalance_immediately", None)
            if wake is None:
                continue
            try:
                wake()
            except Exception as e:
                logger.warning("rebalance_immediately failed: %s", e)

    def _process_notify_consumer_ids_changed(self, cmd: RemotingCommand,
                                             addr: str) -> Optional[RemotingCommand]:
        pass
        header = NotifyConsumerIdsChangedRequestHeader()
        header.from_ext_fields(cmd.ext_fields)
        self._consumer_ids_changed_count += 1
        logger.info("receive broker's notification[%s], the consumer group: %s changed, "
                    "rebalance immediately", addr, header.consumer_group)
        self.rebalance_immediately()
        return None

    # ---------------- broker 主动请求处理（ClientRemotingProcessor） ----------------
    def _process_reset_offset(self, cmd: RemotingCommand, addr: str) -> Optional[RemotingCommand]:
        """RESET_CONSUMER_CLIENT_OFFSET(220)：broker 用 invokeOneway 发的，无需应答。

        但重置逻辑里会触发 rebalance（lock/unlock/batch 等 invokeSync），不能在读线程上同步
        跑——丢到后台线程，立即返回 None（oneway）。
        """
        header = ResetOffsetRequestHeader()
        header.from_ext_fields(cmd.ext_fields)
        group = header.group
        consumer = self.find_consumer(group) if group else None
        if consumer is None:
            logger.warning("RESET_CONSUMER_CLIENT_OFFSET: no consumer for group=%s", group)
            return None
        topic = header.topic
        offset_table: Dict[MessageQueue, int] = {}
        if cmd.body:
            try:
                offset_table = ResetOffsetBody.decode(cmd.body).offset_table
            except Exception as e:  # noqa: BLE001
                logger.warning("RESET_CONSUMER_CLIENT_OFFSET: not map-form body: %s", e)
            if not offset_table:
                try:
                    for e in ResetOffsetBodyForC.decode(cmd.body).offset_table:
                        offset_table[MessageQueue(e.topic, e.broker_name, e.queue_id)] = e.offset
                except Exception as e:  # noqa: BLE001
                    logger.warning("RESET_CONSUMER_CLIENT_OFFSET: bad body: %s", e)
                    return None

        def _run() -> None:
            try:
                consumer.reset_offset(topic, offset_table)
            except Exception as e:  # noqa: BLE001
                logger.warning("reset offset failed (group=%s topic=%s): %s",
                               group, topic, e)

        t = threading.Thread(target=_run, daemon=True,
                             name="rmq-reset-offset-%s" % (group or "?"))
        t.start()
        return None

    def _process_get_consumer_status(self, cmd: RemotingCommand, addr: str) -> Optional[RemotingCommand]:
        """GET_CONSUMER_STATUS_FROM_CLIENT(221)：返回已消费位点表（Map<MessageQueue,Long>）。"""
        header = GetConsumerStatusRequestHeader()
        header.from_ext_fields(cmd.ext_fields)
        group = header.group
        consumer = self.find_consumer(group) if group else None
        if consumer is None:
            return RemotingCommand.create_response_command(
                ResponseCode.SYSTEM_ERROR, "no consumer for group=%s" % group, None)
        status = consumer.get_consumer_status(header.topic)
        body = GetConsumerStatusBody()
        body.message_queue_table = dict(status)
        resp = RemotingCommand.create_response_command(ResponseCode.SUCCESS, None, None)
        resp.body = body.encode()
        return resp

    def _process_get_consumer_running_info(self, cmd: RemotingCommand, addr: str) -> Optional[RemotingCommand]:
        """GET_CONSUMER_RUNNING_INFO(307)：返回本消费者运行信息。"""
        header = GetConsumerRunningInfoRequestHeader()
        header.from_ext_fields(cmd.ext_fields)
        group = header.consumer_group
        consumer = self.find_consumer(group) if group else None
        if consumer is None:
            return RemotingCommand.create_response_command(
                ResponseCode.SYSTEM_ERROR, "no consumer for group=%s" % group, None)
        info = consumer.consumer_running_info()
        resp = RemotingCommand.create_response_command(ResponseCode.SUCCESS, None, None)
        resp.body = info.encode()
        return resp

    def _process_consume_message_directly(self, cmd: RemotingCommand, addr: str) -> Optional[RemotingCommand]:
        """CONSUME_MESSAGE_DIRECTLY(309)：broker 把一条消息推下来，要求本地真实消费一次。"""
        header = ConsumeMessageDirectlyResultRequestHeader()
        header.from_ext_fields(cmd.ext_fields)
        group = header.consumer_group
        consumer = self.find_consumer(group) if group else None
        if consumer is None:
            return RemotingCommand.create_response_command(
                ResponseCode.SYSTEM_ERROR, "no consumer for group=%s" % group, None)
        if not cmd.body:
            return RemotingCommand.create_response_command(
                ResponseCode.SYSTEM_ERROR, "empty message body", None)
        msg = decode_message(cmd.body, check_crc=False)
        if msg is None:
            return RemotingCommand.create_response_command(
                ResponseCode.SYSTEM_ERROR, "decode message failed", None)
        result = consumer.consume_message_directly(msg, header.broker_name)
        resp = RemotingCommand.create_response_command(ResponseCode.SUCCESS, None, None)
        resp.body = result.encode()
        return resp

    # ---------------- 生命周期 ----------------
    def start(self) -> None:
        pass
        with self._lifecycle_lock:
            if self._started:
                return
            self._started = True
            try:
                self._start_locked()
            except Exception:
                self._started = False
                MQClientManager.get_instance().remove_client_factory(self.client_id, self)
                raise

    def _start_locked(self) -> None:
        self.consumer_stats_manager.start()
        dynamic_ns = (not self.name_server_addrs
                      and self.top_addressing is not None
                      and bool(self.top_addressing.ws_addr))
        if dynamic_ns:
            self.fetch_name_server_addr()
            if not self.name_server_addrs:
                raise MQClientException(
                    "name server address is not set and address server (%s) returned none"
                    % self.top_addressing.ws_addr,
                    ClientErrorCode.NO_NAME_SERVER_EXCEPTION)
            self._namesrv_refresh_stop.clear()
            t = threading.Thread(target=self._namesrv_refresh_loop, daemon=True,
                                 name="rmq-namesrv-refresh-%s" % self.client_id)
            t.start()
            self._namesrv_refresh_thread = t
        if self._route_refresh_thread is None:
            self._route_refresh_stop.clear()
            t = threading.Thread(target=self._route_refresh_loop, daemon=True,
                                 name="rmq-route-refresh-%s" % self.client_id)
            t.start()
            self._route_refresh_thread = t
        if self._adjust_pool_thread is None:
            self._adjust_pool_stop.clear()
            t = threading.Thread(target=self._adjust_thread_pool_loop, daemon=True,
                                 name="rmq-adjust-pool-%s" % self.client_id)
            t.start()
            self._adjust_pool_thread = t

    def shutdown(self) -> None:
        # 真正的关闭由 MQClientManager 在引用计数归零时触发（unregister_client）；
        # 这里与 start() 共用一把锁，避免复用场景下与并发的 start 交错。
        with self._lifecycle_lock:
            self._started = False
            self._route_refresh_stop.set()
            self._adjust_pool_stop.set()
            self._namesrv_refresh_stop.set()
            self.consumer_stats_manager.shutdown()
            self.remoting_client.shutdown()

    def _namesrv_refresh_loop(self) -> None:
        pass
        if self._namesrv_refresh_stop.wait(10.0):
            return
        while True:
            if not self._started:
                return
            try:
                self.fetch_name_server_addr()
            except Exception as e:  # noqa: BLE001
                logger.debug("fetchNameServerAddr exception: %s", e)
            if self._namesrv_refresh_stop.wait(120.0):
                return

    def _adjust_thread_pool_loop(self) -> None:
        pass
        if self._adjust_pool_stop.wait(60.0):
            return
        while True:
            if not self._started:
                return
            self.adjust_thread_pool()
            if self._adjust_pool_stop.wait(60.0):
                return

    def register_topic_in_use(self, topic: str) -> None:
        pass
        if topic:
            self._topics_in_use.add(topic)

    def _route_refresh_loop(self) -> None:
        pass
        if self._route_refresh_stop.wait(0.01):
            return
        period = self.poll_name_server_interval / 1000.0
        while True:
            if not self._started:
                return
            for topic in list(self._topics_in_use):
                try:
                    self.update_topic_route_info_from_name_server(topic)
                except Exception as e:  # noqa: BLE001
                    logger.debug("route refresh failed for %s: %s", topic, e)
            if self._route_refresh_stop.wait(period):
                return

    def update_name_server_address_list(self, addrs: List[str]) -> None:
        if addrs:
            self.name_server_addrs = list(addrs)

    # ---------------- 底层 invoke ----------------
    def _invoke_sync(self, addr: str, request: RemotingCommand,
                     timeout_millis: Optional[int] = None) -> RemotingCommand:
        return self.remoting_client.invoke_sync(addr, request, timeout_millis)

    def _check_response(self, response: RemotingCommand) -> RemotingCommand:
        if response.code == ResponseCode.SUCCESS:
            return response
        raise MQBrokerException(response.code, response.remark or "")

    # ---------------- 路由管理 ----------------
    def update_topic_route_info_from_name_server(self, topic: str, timeout_millis: int = 5000,
                                                 is_default: bool = False) -> bool:
        pass
        if not self.name_server_addrs:
            raise MQClientException("name server address list is empty",
                                    ClientErrorCode.NO_NAME_SERVER_EXCEPTION)

        def _fetch(t: str):
            request = RemotingCommand.create_request_command(RequestCode.GET_ROUTEINFO_BY_TOPIC, None)
            request.ext_fields["topic"] = t
            last_exc = None
            for ns_addr in self.name_server_addrs:
                try:
                    response = self._invoke_sync(ns_addr, request, timeout_millis)
                except Exception as e:  # noqa: BLE001
                    last_exc = e
                    continue
                if response.code == ResponseCode.SUCCESS and response.body:
                    return TopicRouteData.decode(response.body)
                return None
            if last_exc is not None and not isinstance(last_exc, MQBrokerException):
                raise last_exc
            return None

        route = _fetch(topic)
        if route is None and is_default and topic != MixAll.DEFAULT_TOPIC:
            route = _fetch(MixAll.DEFAULT_TOPIC)
            if route is not None:
                # 新 topic 由 broker 用 default_topic_queue_nums 创建队列，而默认 topic 自身
                # 可能配置了更多队列，这里按 broker 实际创建数裁剪，避免选中非法 queueId。
                cap = MixAll.DEFAULT_TOPIC_QUEUE_NUMS
                for qd in route.queue_datas:
                    qd.write_queue_nums = min(qd.write_queue_nums, cap)
                    qd.read_queue_nums = min(qd.read_queue_nums, cap)
        if route is None:
            return False
        with self.topic_route_lock:
            self.topic_route_table[topic] = route
            for bd in route.get_broker_datas():
                self.broker_addr_table[bd.broker_name] = dict(bd.broker_addrs)
            endpoints = static_topic.topic_route_data_2_endpoints_for_static_topic(topic, route)
            if endpoints is not None:
                self.topic_end_points_table[topic] = endpoints
            else:
                self.topic_end_points_table.pop(topic, None)
            publish = self.topic_publish_info_table.setdefault(topic, TopicPublishInfo())
            publish.order_topic = route.order_topic_conf is not None
            publish.topic_route_data = route
            publish.msg_queue_list = route.get_all_message_queue(topic)
        return True

    def get_topic_publish_info(self, topic: str, is_default: bool = False) -> TopicPublishInfo:
        with self.topic_route_lock:
            info = self.topic_publish_info_table.get(topic)
            if info is not None and info.ok():
                return info
        self.update_topic_route_info_from_name_server(topic, is_default=is_default)
        with self.topic_route_lock:
            info = self.topic_publish_info_table.get(topic)
            if info is None or not info.ok():
                raise MQClientException("Can not find Message Queue for topic: %s" % topic)
            return info

    def get_topic_route_data(self, topic: str) -> Optional[TopicRouteData]:
        with self.topic_route_lock:
            route = self.topic_route_table.get(topic)
            if route is not None:
                return route
        try:
            self.update_topic_route_info_from_name_server(topic)
        except Exception:  # noqa: BLE001
            pass
        with self.topic_route_lock:
            return self.topic_route_table.get(topic)

    def get_topic_subscribe_info(self, topic: str) -> List[MessageQueue]:
        pass
        route = self.get_topic_route_data(topic)
        if route is None:
            return []
        return route.get_all_subscribe_message_queue(topic)

    @staticmethod
    def find_broker_addr_in_route(route: TopicRouteData, broker_name: str) -> Optional[str]:
        for broker_data in route.get_broker_datas():
            if broker_data.broker_name == broker_name:
                return broker_data.select_broker_addr()
        return None

    def find_broker_addr_by_topic(self, topic: str) -> Optional[str]:
        pass
        route = self.get_topic_route_data(topic)
        if route is None:
            return None
        brokers = route.get_broker_datas()
        if not brokers:
            return None
        return random.choice(brokers).select_broker_addr()

    # ---------------- 消息发送 ----------------
    def publish_addr_for(self, broker_name: str, topic: str) -> str:
        pass
        addr = self.find_broker_address_in_publish(broker_name)
        if not addr:
            self.update_topic_route_info_from_name_server(topic)
            addr = self.find_broker_address_in_publish(broker_name)
        if not addr:
            raise MQClientException("The broker[%s] not exist" % broker_name)
        return addr

    def get_broker_name_from_message_queue(self, mq: MessageQueue) -> str:
        pass
        with self.topic_route_lock:
            endpoints = self.topic_end_points_table.get(mq.topic)
        if endpoints:
            real = endpoints.get(mq)
            if real is not None:
                return real
        return mq.broker_name

    def pop_ack_addr(self, broker_name: str, topic: str, queue_id: int) -> str:
        pass
        def _resolve_des() -> str:
            if static_topic.is_logical_queue_mock_broker_name(broker_name):
                return self.get_broker_name_from_message_queue(
                    MessageQueue(topic, broker_name, queue_id))
            return broker_name

        des = _resolve_des()
        addr = self.find_broker_address_in_publish(des)
        if not addr:
            self.update_topic_route_info_from_name_server(topic)
            des = _resolve_des()
            addr = self.find_broker_address_in_publish(des)
        if not addr:
            raise MQClientException(
                "The broker[%s] master node does not exist" % des)
        return addr

    def send_message(self, producer_group: str, msg: Message, mq: MessageQueue,
                     timeout_millis: int = 3000, sys_flag: int = 0,
                     unit_mode: bool = False,
                     default_topic: Optional[str] = None,
                     default_topic_queue_nums: Optional[int] = None) -> SendResult:
        return self.send_message_to_addr(producer_group, msg, mq,
                                         self.publish_addr_for(mq.broker_name, mq.topic),
                                         timeout_millis, sys_flag, unit_mode, default_topic,
                                         default_topic_queue_nums)

    def send_message_to_addr(self, producer_group: str, msg: Message, mq: MessageQueue,
                             addr: str, timeout_millis: int = 3000,
                             sys_flag: int = 0, unit_mode: bool = False,
                             default_topic: Optional[str] = None,
                             default_topic_queue_nums: Optional[int] = None) -> SendResult:
        request = self._build_send_request(producer_group, msg, mq, timeout_millis, sys_flag,
                                           unit_mode, default_topic, default_topic_queue_nums)
        response = self._invoke_sync(addr, request, timeout_millis)
        return self._parse_send_response(response, msg, mq)

    def send_message_oneway(self, producer_group: str, msg: Message, mq: MessageQueue,
                            addr: str, timeout_millis: int = 3000,
                            sys_flag: int = 0, unit_mode: bool = False,
                            default_topic: Optional[str] = None,
                            default_topic_queue_nums: Optional[int] = None) -> None:
        request = self._build_send_request(producer_group, msg, mq, timeout_millis, sys_flag,
                                           unit_mode, default_topic, default_topic_queue_nums)
        request.mark_oneway_rpc()
        self.remoting_client.invoke_oneway(addr, request)

    def build_send_request(self, producer_group: str, msg: Message, mq: MessageQueue,
                           timeout_millis: int = 3000, sys_flag: int = 0,
                           unit_mode: bool = False,
                           default_topic: Optional[str] = None,
                           default_topic_queue_nums: Optional[int] = None) -> RemotingCommand:
        pass
        return self._build_send_request(producer_group, msg, mq, timeout_millis, sys_flag,
                                        unit_mode, default_topic, default_topic_queue_nums)

    def send_message_async(self, addr: str, request: RemotingCommand, msg: Message,
                           mq: MessageQueue, timeout_millis: int,
                           on_complete: Callable[[Optional[SendResult], Optional[BaseException]],
                                                 None]) -> None:
        pass
        def _callback(response: Optional[RemotingCommand],
                      error: Optional[BaseException]) -> None:
            if error is not None:
                on_complete(None, error)
                return
            try:
                on_complete(self._parse_send_response(response, msg, mq), None)
            except Exception as parse_error:  # noqa: BLE001 — 解析失败也要交给调用方
                on_complete(None, parse_error)

        self.remoting_client.invoke_async(addr, request, _callback, timeout_millis)

    def _build_send_request(self, producer_group: str, msg: Message, mq: MessageQueue,
                            timeout_millis: int = 3000, sys_flag: int = 0,
                            unit_mode: bool = False,
                            default_topic: Optional[str] = None,
                            default_topic_queue_nums: Optional[int] = None) -> RemotingCommand:
        pass
        if not isinstance(msg, MessageBatch):
            set_uniq_id(msg)
        header = SendMessageRequestHeaderV2()
        header.producer_group = producer_group
        header.topic = msg.topic
        header.default_topic = default_topic or MixAll.DEFAULT_TOPIC
        header.default_topic_queue_nums = (default_topic_queue_nums
                                          if default_topic_queue_nums is not None
                                          else MixAll.DEFAULT_TOPIC_QUEUE_NUMS)
        header.queue_id = mq.queue_id
        header.sys_flag = sys_flag
        header.born_timestamp = int(time.time() * 1000)
        header.flag = msg.flag
        header.properties = message_properties_2_string(msg.properties)
        header.reconsume_times = 0
        header.unit_mode = unit_mode
        header.max_reconsume_times = None
        if header.topic.startswith(MixAll.RETRY_GROUP_TOPIC_PREFIX):
            reconsume_times = MessageAccessor.get_reconsume_time(msg)
            if reconsume_times is not None:
                header.reconsume_times = int(reconsume_times)
                MessageAccessor.clear_property(msg, MessageConst.PROPERTY_RECONSUME_TIME)
            max_reconsume_times = MessageAccessor.get_max_reconsume_times(msg)
            if max_reconsume_times is not None:
                header.max_reconsume_times = int(max_reconsume_times)
                MessageAccessor.clear_property(msg, MessageConst.PROPERTY_MAX_RECONSUME_TIMES)
        header.batch = isinstance(msg, MessageBatch)
        header.broker_name = mq.broker_name or None
        if is_reply_message(msg):
            code = RequestCode.SEND_REPLY_MESSAGE_V2
        elif header.batch:
            code = RequestCode.SEND_BATCH_MESSAGE
        else:
            code = RequestCode.SEND_MESSAGE_V2
        request = RemotingCommand.create_request_command(code, header)
        request.body = self._encode_body(msg)
        return request

    @staticmethod
    def _encode_body(msg: Message) -> bytes:
        pass
        return msg.get_body() or b""

    def _parse_send_response(self, response: RemotingCommand, msg: Message, mq: MessageQueue) -> SendResult:
        status_map = {
            ResponseCode.SUCCESS: SendStatus.SEND_OK,
            ResponseCode.FLUSH_DISK_TIMEOUT: SendStatus.FLUSH_DISK_TIMEOUT,
            ResponseCode.FLUSH_SLAVE_TIMEOUT: SendStatus.FLUSH_SLAVE_TIMEOUT,
            ResponseCode.SLAVE_NOT_AVAILABLE: SendStatus.SLAVE_NOT_AVAILABLE,
        }
        if response.code in status_map:
            header = SendMessageResponseHeader()
            header.from_ext_fields(response.ext_fields)
            uniq_msg_id = get_uniq_id(msg) or header.batch_uniq_id
            result = SendResult(
                send_status=status_map[response.code],
                msg_id=uniq_msg_id or header.msg_id,
                message_queue=MessageQueue(mq.topic, mq.broker_name, header.queue_id if header.queue_id is not None else mq.queue_id),
                queue_offset=header.queue_offset or 0,
                transaction_id=header.transaction_id,
                offset_msg_id=header.msg_id,
            )
            ext = response.ext_fields or {}
            region_id = ext.get(MessageConst.PROPERTY_MSG_REGION)
            result.region_id = region_id if region_id else MixAll.DEFAULT_TRACE_REGION_ID
            result.trace_on = str(ext.get(MessageConst.PROPERTY_TRACE_SWITCH)) != "false"
            result.recall_handle = header.recall_handle
            return result
        raise MQBrokerException(response.code, response.remark or "")

    # ---------------- 定时消息撤回 ----------------
    def recall_message(self, addr: str, header: RecallMessageRequestHeader,
                       timeout_millis: int) -> str:
        pass
        request = RemotingCommand.create_request_command(RequestCode.RECALL_MESSAGE, header)
        response = self._invoke_sync(addr, request, timeout_millis)
        if response.code == ResponseCode.SUCCESS:
            resp_header = RecallMessageResponseHeader()
            resp_header.from_ext_fields(response.ext_fields)
            if not resp_header.msg_id:
                raise MQBrokerException(response.code,
                                        "recall message response has no msgId, addr %s" % addr)
            return resp_header.msg_id
        raise MQBrokerException(response.code, response.remark or "")

    # ---------------- Request-Reply：接收 broker 推回的应答 ----------------
    def _process_reply_message(self, cmd: RemotingCommand, addr: str) -> Optional[RemotingCommand]:
        pass
        header = ReplyMessageRequestHeader()
        header.from_ext_fields(cmd.ext_fields)
        try:
            body = cmd.body or b""
            if MessageSysFlag.is_compressed(header.sys_flag or 0):
                body = decompress_body(body, MessageSysFlag.get_compression_type(header.sys_flag or 0))
            msg = MessageExt(topic=header.topic or "", body=body)
            msg.queue_id = header.queue_id or 0
            msg.store_timestamp = header.store_timestamp or 0
            msg.flag = header.flag or 0
            msg.born_timestamp = header.born_timestamp or 0
            msg.reconsume_times = header.reconsume_times or 0
            if header.born_host:
                msg.born_host = header.born_host
            if header.store_host:
                msg.store_host = header.store_host
            msg.properties = string_2_message_properties(header.properties)
            msg.properties[MessageConst.PROPERTY_REPLY_MESSAGE_ARRIVE_TIME] = str(
                int(time.time() * 1000))
            correlation_id = msg.properties.get(MessageConst.PROPERTY_CORRELATION_ID)
            if REQUEST_FUTURE_HOLDER.put_response(correlation_id, msg) is None:
                logger.warning("receive reply message, but not matched any request, "
                               "CorrelationId: %s, reply from host: %s",
                               correlation_id, header.born_host)
            return RemotingCommand.create_response_command(ResponseCode.SUCCESS, None, None)
        except Exception as e:  # noqa: BLE001
            logger.warning("unknown err when receiveReplyMsg", exc_info=True)
            return RemotingCommand.create_response_command(
                ResponseCode.SYSTEM_ERROR, "process reply message fail: %s" % e, None)

    # ---------------- 消息拉取 ----------------
    def pull_message(self, consumer_group: str, mq: MessageQueue, queue_offset: int,
                     max_msg_nums: int, sys_flag: int, commit_offset: int,
                     subscription: str, sub_version: int, expression_type: str,
                     timeout_millis: int = 30000, max_msg_bytes: int = -1,
                     suspend_timeout_millis: int = 15000, addr: Optional[str] = None,
                     request_source: int = 0,
                     broker_id: Optional[int] = None) -> "PullResult":
        from .consumer_result import PullResult, PullStatus
        slave = False
        if addr is None:
            route = self.get_topic_route_data(mq.topic)
            if route is None:
                raise MQClientException("No route info of this topic: %s" % mq.topic)
            if broker_id is not None:
                broker_data = next((bd for bd in route.broker_datas
                                    if bd.broker_name == mq.broker_name), None)
                if broker_data is None:
                    raise MQClientException("Broker %s not exist" % mq.broker_name)
                addr, slave = MQClientInstance.find_broker_addr_in_subscribe(
                    broker_data.broker_addrs, broker_id)
                if addr is None:
                    raise MQClientException("Broker %s not exist" % mq.broker_name)
            else:
                addr = MQClientInstance.find_broker_addr_in_route(route, mq.broker_name)
                if addr is None:
                    raise MQClientException("Broker %s not found in route of topic %s" % (mq.broker_name, mq.topic))
        if slave:
            sys_flag = PullSysFlag.clear_commit_offset_flag(sys_flag)
        header = PullMessageRequestHeader()
        header.consumer_group = consumer_group
        header.topic = mq.topic
        header.queue_id = mq.queue_id
        header.queue_offset = queue_offset
        header.max_msg_nums = max_msg_nums
        header.sys_flag = sys_flag
        header.commit_offset = commit_offset
        header.suspend_timeout_millis = suspend_timeout_millis
        header.subscription = subscription
        header.sub_version = sub_version
        header.expression_type = expression_type
        header.max_msg_bytes = max_msg_bytes
        header.request_source = request_source
        code = (RequestCode.LITE_PULL_MESSAGE
                if PullSysFlag.has_lite_pull_flag(sys_flag)
                else RequestCode.PULL_MESSAGE)
        request = RemotingCommand.create_request_command(code, header)
        response = self._invoke_sync(addr, request, timeout_millis)

        status = PullStatus.NO_NEW_MSG
        if response.code == ResponseCode.SUCCESS:
            status = PullStatus.FOUND
        elif response.code == ResponseCode.PULL_NOT_FOUND:
            status = PullStatus.NO_NEW_MSG
        elif response.code == ResponseCode.PULL_OFFSET_MOVED:
            status = PullStatus.OFFSET_ILLEGAL
        elif response.code == ResponseCode.PULL_RETRY_IMMEDIATELY:
            status = PullStatus.NO_MATCHED_MSG
        else:
            raise MQBrokerException(response.code, response.remark or "")

        resp_header = PullMessageResponseHeader()
        resp_header.from_ext_fields(response.ext_fields)
        found = []
        if response.body:
            found = decode_messages(response.body)
            for m in found:
                m.broker_name = mq.broker_name
                m.queue_id = mq.queue_id
        return PullResult(status, resp_header.next_begin_offset or 0,
                          resp_header.min_offset or 0, resp_header.max_offset or 0, found,
                          suggest_which_broker_id=resp_header.suggest_which_broker_id)

    @staticmethod
    def find_broker_addr_in_subscribe(broker_addrs: Dict[int, str], broker_id: int,
                                      only_this_broker: bool = False):
        pass
        if not broker_addrs:
            return None, False
        addr = broker_addrs.get(broker_id)
        if addr is not None:
            return addr, broker_id != MixAll.MASTER_ID
        if broker_id != MixAll.MASTER_ID:
            addr = broker_addrs.get(broker_id + 1)
            if addr is not None:
                return addr, True
        if not only_this_broker:
            first_id = min(broker_addrs)
            return broker_addrs[first_id], first_id != MixAll.MASTER_ID
        return None, False

    # ---------------- POP 模式（5.x 轻量消费） ----------------

    def pop_message(self, consumer_group: str, topic: str, queue_id: int = -1,
                    max_msg_nums: int = 32, invisible_time: int = 60000,
                    poll_time: int = 0, init_mode: int = 0,
                    exp: Optional[str] = None, exp_type: Optional[str] = None,
                    order: bool = False, broker_name: Optional[str] = None,
                    timeout_millis: int = 10000, addr: Optional[str] = None) -> "PopResult":
        """POP 弹取消息（``RequestCode.POP_MESSAGE = 200050``）。

        与 pull 的语义差别：
        - **不需要提交位点**，消费完成后用 ``ack_message`` 确认；
        - 不 ack 的消息在 ``invisible_time`` 之后被 broker 复活并重投到
          ``%RETRY%<group>_<topic>``（V1），下次 POP 会再弹回来 —— 至少一次语义；
        - ``queue_id = -1`` 表示弹该 topic 的所有队列。

        ``born_time`` 必须是当前毫秒时间戳：broker 用
        ``now - bornTime - pollTime > 500`` 判定"超时太久"并直接回
        ``POLLING_TIMEOUT(210)``，填 0 会必然失败。
        """
        from .consumer_result import PopResult, PopStatus
        from remoting.protocol import extra_info as ei
        from remoting.protocol.headers import (PopMessageRequestHeader,
                                                 PopMessageResponseHeader)

        if not broker_name:
            route = self.get_topic_route_data(topic)
            if route is None:
                raise MQClientException("No route info of this topic: %s" % topic)
            brokers = route.get_broker_datas()
            if not brokers:
                raise MQClientException("No broker in route of topic: %s" % topic)
            # 调用方连 brokerName 都没给（「弹该 topic 的任意队列」）：先按路由挑一台，
            # 挑完照样只认它的 master。
            broker_name = brokers[0].broker_name
        if addr is None:
            addr = self.publish_addr_for(broker_name, topic)

        header = PopMessageRequestHeader()
        header.consumer_group = consumer_group
        header.topic = topic
        header.queue_id = queue_id
        header.max_msg_nums = max_msg_nums
        header.invisible_time = invisible_time
        header.poll_time = poll_time
        header.born_time = int(time.time() * 1000)
        header.init_mode = init_mode
        header.exp_type = exp_type
        header.exp = exp
        header.order = order
        request = RemotingCommand.create_request_command(RequestCode.POP_MESSAGE, header)
        response = self._invoke_sync(addr, request, timeout_millis)

        if response.code == ResponseCode.SUCCESS:
            status = PopStatus.FOUND
        elif response.code == ResponseCode.POLLING_FULL:
            status = PopStatus.POLLING_FULL
        elif response.code == ResponseCode.POLLING_TIMEOUT:
            status = PopStatus.POLLING_NOT_FOUND
        elif response.code == ResponseCode.PULL_NOT_FOUND:
            status = PopStatus.POLLING_NOT_FOUND
        else:
            raise MQBrokerException(response.code, response.remark or "")

        resp_header = PopMessageResponseHeader()
        resp_header.from_ext_fields(response.ext_fields)

        found: List[MessageExt] = []
        if status == PopStatus.FOUND and response.body:
            found = decode_messages(response.body)
            self._stamp_pop_ck(found, broker_name, resp_header)

        for m in found:
            m.broker_name = broker_name
            m.topic = topic

        return PopResult(status, found,
                         resp_header.rest_num or 0,
                         resp_header.pop_time or 0,
                         resp_header.invisible_time or 0,
                         resp_header.revive_qid or 0,
                         resp_header.start_offset_info,
                         resp_header.msg_offset_info,
                         resp_header.order_count_info)

    @staticmethod
    def _pop_queue_map_key(extra, m: MessageExt) -> str:
        pass
        ck = m.properties.get(MessageConst.PROPERTY_POP_CK)
        if ck:
            return extra.get_retry(extra.split(ck)) + "@" + str(m.queue_id)
        return extra.get_start_offset_info_map_key(m.topic, m.queue_id)

    def _stamp_pop_ck(self, found: List[MessageExt], broker_name: str, resp_header) -> None:
        pass
        from remoting.protocol import extra_info as extra

        pop_time = resp_header.pop_time or 0
        invisible_time = resp_header.invisible_time or 0
        revive_qid = resp_header.revive_qid or 0

        if not resp_header.start_offset_info:
            per_queue: Dict[str, str] = {}
            for m in found:
                key = str(m.topic) + str(m.queue_id)
                if key not in per_queue:
                    per_queue[key] = extra.build_extra_info(
                        m.queue_offset, pop_time, invisible_time, revive_qid,
                        m.topic, broker_name, m.queue_id)
                m.properties[MessageConst.PROPERTY_POP_CK] = (
                    per_queue[key] + extra.KEY_SEPARATOR + str(m.queue_offset))
        else:
            start_map = extra.parse_start_offset_info(resp_header.start_offset_info) or {}
            msg_map = extra.parse_msg_offset_info(resp_header.msg_offset_info) or {}

            sorted_offsets: Dict[str, List[int]] = {}
            for m in found:
                queue_key = self._pop_queue_map_key(extra, m)
                sorted_offsets.setdefault(queue_key, []).append(m.queue_offset)
            for offsets in sorted_offsets.values():
                offsets.sort()

            for m in found:
                # retry topic 弹回来的消息 broker 已经写好 POP_CK，不能覆盖。
                if m.properties.get(MessageConst.PROPERTY_POP_CK) is not None:
                    continue
                queue_key = self._pop_queue_map_key(extra, m)
                start_offset = start_map.get(queue_key)
                offsets = msg_map.get(queue_key)
                if start_offset is None or not offsets:
                    continue
                try:
                    index = sorted_offsets[queue_key].index(m.queue_offset)
                except ValueError:
                    continue
                if index >= len(offsets):
                    continue
                m.properties[MessageConst.PROPERTY_POP_CK] = extra.build_extra_info(
                    start_offset, pop_time, invisible_time, revive_qid,
                    m.topic, broker_name, m.queue_id, offsets[index])

        for m in found:
            m.properties.setdefault(MessageConst.PROPERTY_FIRST_POP_TIME, str(pop_time))

    def _addr_for(self, topic: str, broker_name: Optional[str] = None) -> str:
        """按 topic（可选再按 brokerName）解析 broker 地址。"""
        route = self.get_topic_route_data(topic)
        if route is None:
            raise MQClientException("No route info of this topic: %s" % topic)
        if broker_name:
            addr = MQClientInstance.find_broker_addr_in_route(route, broker_name)
            if addr is None:
                raise MQClientException(
                    "Broker %s not found in route of topic %s" % (broker_name, topic))
            return addr
        brokers = route.get_broker_datas()
        if not brokers:
            raise MQClientException("No broker in route of topic: %s" % topic)
        addr = brokers[0].select_broker_addr()
        if addr is None:
            raise MQClientException("No available broker addr for topic: %s" % topic)
        return addr

    def ack_message(self, consumer_group: str, topic: str, queue_id: int,
                    extra_info: str, offset: int, broker_name: Optional[str] = None,
                    timeout_millis: int = 3000, addr: Optional[str] = None) -> int:
        """确认一条 POP 消息（``RequestCode.ACK_MESSAGE = 200051``）。

        ``extra_info`` 用消息上的 ``POP_CK`` 属性；``offset`` 必须是
        **consumeQueue offset**（即 CK 串第 8 段 / msgQueueOffset），不是 commitlog
        offset —— 传错 broker 会回 ``NO_MESSAGE``。

        返回 broker 响应码，``ResponseCode.SUCCESS`` 即确认成功。
        """
        from remoting.protocol import extra_info as extra
        from remoting.protocol.headers import AckMessageRequestHeader

        if broker_name is None:
            broker_name = extra.get_broker_name(extra.split(extra_info))
        if addr is None:
            addr = self.pop_ack_addr(broker_name, topic, queue_id)

        header = AckMessageRequestHeader()
        header.consumer_group = consumer_group
        header.topic = topic
        header.queue_id = queue_id
        header.extra_info = extra_info
        header.offset = offset
        request = RemotingCommand.create_request_command(RequestCode.ACK_MESSAGE, header)
        response = self._invoke_sync(addr, request, timeout_millis)
        return response.code

    def change_invisible_time(self, consumer_group: str, topic: str, queue_id: int,
                              extra_info: str, offset: int, invisible_time: int,
                              broker_name: Optional[str] = None,
                              timeout_millis: int = 3000,
                              addr: Optional[str] = None) -> "ChangeInvisibleTimeResult":
        """延长 POP 消息的不可见时间（``CHANGE_MESSAGE_INVISIBLETIME = 200053``）。

        用于"还在处理、别让 broker 复活重投"的场景。响应返回的是**新的**
        popTime / invisibleTime / reviveQid，客户端据此重建 8 段 extraInfo
        （返回在结果的 ``extra_info`` 字段里）供后续 ACK 使用 —— 不是原来那个旧串。
        """
        from .consumer_result import ChangeInvisibleTimeResult
        from remoting.protocol import extra_info as extra
        from remoting.protocol.headers import (ChangeInvisibleTimeRequestHeader,
                                                 ChangeInvisibleTimeResponseHeader)

        if broker_name is None:
            broker_name = extra.get_broker_name(extra.split(extra_info))
        if addr is None:
            addr = self.pop_ack_addr(broker_name, topic, queue_id)

        header = ChangeInvisibleTimeRequestHeader()
        header.consumer_group = consumer_group
        header.topic = topic
        header.queue_id = queue_id
        header.extra_info = extra_info
        header.offset = offset
        header.invisible_time = invisible_time
        request = RemotingCommand.create_request_command(
            RequestCode.CHANGE_MESSAGE_INVISIBLETIME, header)
        response = self._invoke_sync(addr, request, timeout_millis)

        resp_header = ChangeInvisibleTimeResponseHeader()
        resp_header.from_ext_fields(response.ext_fields)

        new_extra = None
        if response.code == ResponseCode.SUCCESS:
            new_extra = extra.build_extra_info(
                offset, resp_header.pop_time, resp_header.invisible_time,
                resp_header.revive_qid, topic, broker_name, queue_id,
                offset)
        return ChangeInvisibleTimeResult(response.code,
                                         resp_header.pop_time or 0,
                                         resp_header.invisible_time or 0,
                                         resp_header.revive_qid or 0,
                                         new_extra)

    def query_consumer_offset(self, consumer_group: str, mq: MessageQueue,
                              timeout_millis: int = 5000, addr: Optional[str] = None,
                              set_zero_if_not_found: bool = False) -> Optional[int]:
        if addr is None:
            addr = self._consumer_offset_addr(mq)
        header = QueryConsumerOffsetRequestHeader()
        header.consumer_group = consumer_group
        header.topic = mq.topic
        header.queue_id = mq.queue_id
        header.set_zero_if_not_found = set_zero_if_not_found
        request = RemotingCommand.create_request_command(RequestCode.QUERY_CONSUMER_OFFSET, header)
        response = self._invoke_sync(addr, request, timeout_millis)
        if response.code == ResponseCode.QUERY_NOT_FOUND:
            return None
        self._check_response(response)
        resp_header = QueryConsumerOffsetResponseHeader()
        resp_header.from_ext_fields(response.ext_fields)
        return resp_header.offset

    def update_consumer_offset(self, consumer_group: str, mq: MessageQueue, commit_offset: int,
                               timeout_millis: int = 5000, addr: Optional[str] = None) -> None:
        if addr is None:
            addr = self._broker_addr(mq)
        header = UpdateConsumerOffsetRequestHeader()
        header.consumer_group = consumer_group
        header.topic = mq.topic
        header.queue_id = mq.queue_id
        header.commit_offset = commit_offset
        request = RemotingCommand.create_request_command(RequestCode.UPDATE_CONSUMER_OFFSET, header)
        response = self._invoke_sync(addr, request, timeout_millis)
        self._check_response(response)

    def get_max_offset(self, mq: MessageQueue, timeout_millis: int = 5000, addr: Optional[str] = None) -> int:
        if addr is None:
            addr = self._publish_addr_in_admin(mq)
        header = GetMaxOffsetRequestHeader()
        header.topic = mq.topic
        header.queue_id = mq.queue_id
        request = RemotingCommand.create_request_command(RequestCode.GET_MAX_OFFSET, header)
        response = self._invoke_sync(addr, request, timeout_millis)
        self._check_response(response)
        resp_header = GetMaxOffsetResponseHeader()
        resp_header.from_ext_fields(response.ext_fields)
        return resp_header.offset or 0

    def get_min_offset(self, mq: MessageQueue, timeout_millis: int = 5000, addr: Optional[str] = None) -> int:
        if addr is None:
            addr = self._publish_addr_in_admin(mq)
        header = GetMinOffsetRequestHeader()
        header.topic = mq.topic
        header.queue_id = mq.queue_id
        request = RemotingCommand.create_request_command(RequestCode.GET_MIN_OFFSET, header)
        response = self._invoke_sync(addr, request, timeout_millis)
        self._check_response(response)
        resp_header = GetMinOffsetResponseHeader()
        resp_header.from_ext_fields(response.ext_fields)
        return resp_header.offset or 0

    def search_offset_by_timestamp(self, mq: MessageQueue, timestamp: int,
                                   timeout_millis: int = 5000, addr: Optional[str] = None,
                                   boundary_type: Optional[BoundaryType] = BoundaryType.LOWER) -> int:
        pass
        if addr is None:
            addr = self._publish_addr_in_admin(mq)
        header = SearchOffsetRequestHeader()
        header.topic = mq.topic
        header.queue_id = mq.queue_id
        header.timestamp = timestamp
        header.boundary_type = boundary_type
        request = RemotingCommand.create_request_command(RequestCode.SEARCH_OFFSET_BY_TIMESTAMP, header)
        response = self._invoke_sync(addr, request, timeout_millis)
        self._check_response(response)
        resp_header = SearchOffsetResponseHeader()
        resp_header.from_ext_fields(response.ext_fields)
        return resp_header.offset or 0

    def query_message(self, topic: str, key: str, max_num: int, begin_timestamp: int,
                      end_timestamp: int, timeout_millis: int = 15000,
                      addr: Optional[str] = None,
                      index_type: Optional[str] = None,
                      uniq_key: bool = False) -> Optional[bytes]:
        pass
        if addr is None:
            addr = self._broker_addr_for_topic(topic)
        header = QueryMessageRequestHeader()
        header.topic = topic
        header.key = key
        header.max_num = max_num
        header.begin_timestamp = begin_timestamp
        header.end_timestamp = end_timestamp
        header.index_type = index_type
        request = RemotingCommand.create_request_command(RequestCode.QUERY_MESSAGE, header)
        if uniq_key:
            request.ext_fields[MixAll.UNIQUE_MSG_QUERY_FLAG] = "true"
        response = self._invoke_sync(addr, request, timeout_millis)
        if response.code == ResponseCode.QUERY_NOT_FOUND:
            return None
        self._check_response(response)
        return response.body

    def _broker_addr_for_topic(self, topic: str) -> str:
        route = self.get_topic_route_data(topic)
        if route is None:
            raise MQClientException("No route info of this topic: %s" % topic)
        brokers = route.get_broker_datas()
        if not brokers:
            raise MQClientException("No broker in route of topic: %s" % topic)
        addr = brokers[0].select_broker_addr()
        if addr is None:
            raise MQClientException("No available broker addr for topic: %s" % topic)
        return addr

    def query_message_all_brokers(self, topic: str, key: str, max_num: int,
                                  begin_timestamp: int, end_timestamp: int,
                                  index_type: Optional[str] = None,
                                  uniq_key: bool = False,
                                  timeout_millis: int = 15000) -> List:
        pass
        from common.message_const import MessageConst
        messages: List = []
        route = self.get_topic_route_data(topic)
        if route is None:
            return messages
        for broker_data in route.get_broker_datas():
            addr = broker_data.select_broker_addr()
            if not addr:
                continue
            try:
                body = self.query_message(topic, key, max_num, begin_timestamp,
                                          end_timestamp, timeout_millis, addr,
                                          index_type, uniq_key)
            except Exception:  # noqa: BLE001
                continue
            if not body:
                continue
            for m in decode_messages(body):
                m.broker_name = broker_data.broker_name
                if uniq_key:
                    if m.msg_id == key:
                        messages.append(m)
                else:
                    keys = m.get_keys()
                    if keys:
                        for k in keys.split(MessageConst.KEY_SEPARATOR):
                            if k == key and m.topic == topic:
                                messages.append(m)
                                break
        messages.sort(key=lambda x: (x.queue_offset or 0))
        return messages[:max_num] if max_num > 0 else messages

    # ---------------- 队列锁（顺序消费） ----------------

    def lock_batch_mq(self, consumer_group: str, client_id: str, mqs: List[MessageQueue],
                      timeout_millis: int = 1000) -> List[MessageQueue]:
        pass
        from remoting.protocol.body import LockBatchRequestBody, LockBatchResponseBody
        from remoting.protocol.codes import RequestCode
        from remoting.protocol.headers import LockBatchMqRequestHeader
        lock_ok: List[MessageQueue] = []
        by_broker: Dict[str, List[MessageQueue]] = {}
        for mq in mqs:
            by_broker.setdefault(mq.broker_name, []).append(mq)
        for broker_name, broker_mqs in by_broker.items():
            addr = self.find_broker_address_in_publish(broker_name)
            if addr is None:
                continue
            body = LockBatchRequestBody()
            body.consumer_group = consumer_group
            body.client_id = client_id
            body.mq_set = [{"topic": m.topic, "brokerName": m.broker_name, "queueId": m.queue_id}
                           for m in broker_mqs]
            request = RemotingCommand.create_request_command(RequestCode.LOCK_BATCH_MQ, LockBatchMqRequestHeader())
            request.body = body.encode()
            try:
                response = self._invoke_sync(addr, request, timeout_millis)
                self._check_response(response)
                rb = LockBatchResponseBody.decode(response.body)
                for d in rb.lock_ok_mq_set:
                    lock_ok.append(MessageQueue(d.get("topic"), d.get("brokerName"), int(d.get("queueId") or 0)))
            except Exception as e:  # noqa: BLE001
                logger.warning("lock_batch_mq failed for broker %s: %s", broker_name, e)
        return lock_ok

    def unlock_batch_mq(self, consumer_group: str, client_id: str, mqs: List[MessageQueue],
                        timeout_millis: int = 1000) -> None:
        pass
        from remoting.protocol.body import UnlockBatchRequestBody
        from remoting.protocol.codes import RequestCode
        from remoting.protocol.headers import UnlockBatchMqRequestHeader
        by_broker: Dict[str, List[MessageQueue]] = {}
        for mq in mqs:
            by_broker.setdefault(mq.broker_name, []).append(mq)
        for broker_name, broker_mqs in by_broker.items():
            addr = self.find_broker_address_in_publish(broker_name)
            if addr is None:
                continue
            body = UnlockBatchRequestBody()
            body.consumer_group = consumer_group
            body.client_id = client_id
            body.mq_set = [{"topic": m.topic, "brokerName": m.broker_name, "queueId": m.queue_id}
                           for m in broker_mqs]
            request = RemotingCommand.create_request_command(RequestCode.UNLOCK_BATCH_MQ, UnlockBatchMqRequestHeader())
            request.body = body.encode()
            try:
                response = self._invoke_sync(addr, request, timeout_millis)
                self._check_response(response)
            except Exception as e:  # noqa: BLE001
                logger.warning("unlock_batch_mq failed for broker %s: %s", broker_name, e)

    # ---------------- 心跳 / 注销 ----------------
    def send_heartbeat(self, addr: str, heartbeat_data: HeartbeatData, timeout_millis: int = 5000) -> None:
        request = RemotingCommand.create_request_command(RequestCode.HEART_BEAT, None)
        request.body = heartbeat_data.encode()
        response = self._invoke_sync(addr, request, timeout_millis)
        self._check_response(response)

    def unregister_client(self, addr: str, client_id: str, producer_group: str,
                          consumer_group: str, timeout_millis: int = 3000) -> None:
        pass
        from remoting.protocol.headers import UnregisterClientRequestHeader
        header = UnregisterClientRequestHeader()
        header.client_id = client_id
        header.producer_group = (producer_group or "").strip() or None
        header.consumer_group = (consumer_group or "").strip() or None
        request = RemotingCommand.create_request_command(RequestCode.UNREGISTER_CLIENT, header)
        response = self._invoke_sync(addr, request, timeout_millis)
        self._check_response(response)

    def check_client_config(self, broker_addr: str, consumer_group: str, client_id: str,
                            subscription_data: SubscriptionData,
                            timeout_millis: int = 3000) -> None:
        pass
        request = RemotingCommand.create_request_command(RequestCode.CHECK_CLIENT_CONFIG, None)
        body = CheckClientRequestBody()
        body.client_id = client_id
        body.group = consumer_group
        body.subscription_data = subscription_data
        request.body = body.encode()
        response = self._invoke_sync(broker_addr, request, timeout_millis)
        if response is None:
            raise MQClientException("checkClientConfig got no response from %s" % broker_addr)
        if response.code != ResponseCode.SUCCESS:
            raise MQClientException(response.remark or "", response.code)

    # ---------------- 管理类 API ----------------
    def get_broker_cluster_info(self, timeout_millis: int = 10000) -> ClusterInfo:
        request = RemotingCommand.create_request_command(RequestCode.GET_BROKER_CLUSTER_INFO, None)
        for ns_addr in self.name_server_addrs:
            try:
                response = self._invoke_sync(ns_addr, request, timeout_millis)
                if response.code == ResponseCode.SUCCESS and response.body:
                    return ClusterInfo.decode(response.body)
            except Exception:  # noqa: BLE001
                continue
        raise MQClientException("Failed to get broker cluster info from name server")

    def get_all_topic_list_from_name_server(self, timeout_millis: int = 10000) -> TopicList:
        request = RemotingCommand.create_request_command(RequestCode.GET_ALL_TOPIC_LIST_FROM_NAMESERVER, None)
        for ns_addr in self.name_server_addrs:
            try:
                response = self._invoke_sync(ns_addr, request, timeout_millis)
                if response.code == ResponseCode.SUCCESS and response.body:
                    return TopicList.decode(response.body)
            except Exception:  # noqa: BLE001
                continue
        raise MQClientException("Failed to get all topic list from name server")

    def create_topic_in_broker(self, broker_addr: str, default_topic: str, topic: str,
                               read_queue_nums: int = 4, write_queue_nums: int = 4,
                               perm: int = 6, topic_sys_flag: int = 0,
                               topic_filter_type: str = TopicFilterType.SINGLE_TAG,
                               order: bool = False, attributes: Optional[str] = None,
                               timeout_millis: int = 5000, retry_times: int = 5) -> None:
        pass
        header = CreateTopicRequestHeader()
        header.topic = topic
        header.default_topic = default_topic
        header.read_queue_nums = read_queue_nums
        header.write_queue_nums = write_queue_nums
        header.perm = perm
        header.topic_filter_type = topic_filter_type
        header.topic_sys_flag = topic_sys_flag
        header.order = order
        header.attributes = attributes if attributes is not None else ""
        header.force = False
        request = RemotingCommand.create_request_command(RequestCode.UPDATE_AND_CREATE_TOPIC, header)

        last_exc: Optional[Exception] = None
        for attempt in range(max(1, retry_times)):
            try:
                response = self._invoke_sync(broker_addr, request, timeout_millis)
                self._check_response(response)
                return
            except MQBrokerException:
                raise
            except Exception as e:  # noqa: BLE001
                last_exc = e
                if attempt == retry_times - 1:
                    raise
        if last_exc is not None:
            raise last_exc

    def create_topic_in_route(self, topic: str, read_queue_nums: int = 4, write_queue_nums: int = 4,
                              perm: int = 6, topic_sys_flag: int = 0,
                              attributes: Optional[str] = None,
                              timeout_millis: int = 5000) -> None:
        pass
        route = self.get_topic_route_data(MixAll.DEFAULT_TOPIC)
        if route is None:
            raise MQClientException("No route info of default topic %s" % MixAll.DEFAULT_TOPIC)
        created_at_least_once = False
        last_exc: Optional[Exception] = None
        for broker_data in route.get_broker_datas():
            addr = broker_data.select_broker_addr()
            if not addr:
                continue
            try:
                self.create_topic_in_broker(addr, MixAll.DEFAULT_TOPIC, topic,
                                            read_queue_nums, write_queue_nums, perm,
                                            topic_sys_flag, attributes=attributes,
                                            timeout_millis=timeout_millis)
                created_at_least_once = True
            except Exception as e:  # noqa: BLE001
                last_exc = e
        if not created_at_least_once and last_exc is not None:
            raise MQClientException("create new topic failed", cause=last_exc)

    def delete_topic_in_broker(self, broker_addr: str, topic: str, timeout_millis: int = 5000) -> None:
        request = RemotingCommand.create_request_command(RequestCode.DELETE_TOPIC_IN_BROKER, None)
        request.ext_fields["topic"] = topic
        response = self._invoke_sync(broker_addr, request, timeout_millis)
        self._check_response(response)

    def delete_topic_in_namesrv(self, topic: str, timeout_millis: int = 5000) -> None:
        request = RemotingCommand.create_request_command(RequestCode.DELETE_TOPIC_IN_NAMESRV, None)
        request.ext_fields["topic"] = topic
        for ns_addr in self.name_server_addrs:
            try:
                response = self._invoke_sync(ns_addr, request, timeout_millis)
                self._check_response(response)
                return
            except Exception:  # noqa: BLE001
                continue
        raise MQClientException("Failed to delete topic %s in name server" % topic)

    def get_consumer_list_by_group(self, consumer_group: str, timeout_millis: int = 5000,
                                   addr: Optional[str] = None) -> GetConsumerListByGroupResponseBody:
        from remoting.protocol.headers import GetConsumerListByGroupRequestHeader
        if addr is None:
            raise MQClientException("broker addr required for get consumer list")
        header = GetConsumerListByGroupRequestHeader()
        header.consumer_group = consumer_group
        request = RemotingCommand.create_request_command(RequestCode.GET_CONSUMER_LIST_BY_GROUP, header)
        response = self._invoke_sync(addr, request, timeout_millis)
        self._check_response(response)
        if response.body:
            return GetConsumerListByGroupResponseBody.decode(response.body)
        return GetConsumerListByGroupResponseBody()

    # ---------------- 工具 ----------------
    def _consumer_offset_addr(self, mq: MessageQueue) -> str:
        pass
        addr = self.find_broker_address_in_publish(mq.broker_name)
        if not addr:
            self.update_topic_route_info_from_name_server(mq.topic)
            addr = self.broker_addr_of(mq.broker_name)
        if not addr:
            raise MQClientException("The broker[%s] not exist" % mq.broker_name)
        return addr

    def _broker_addr(self, mq: MessageQueue) -> str:
        pass
        route = self.get_topic_route_data(mq.topic)
        if route is None:
            raise MQClientException("No route info of this topic: %s" % mq.topic)
        addr = MQClientInstance.find_broker_addr_in_route(route, mq.broker_name)
        if addr is None:
            raise MQClientException("Broker %s not found in route of topic %s" % (mq.broker_name, mq.topic))
        return addr

    def _publish_addr_in_admin(self, mq: MessageQueue) -> str:
        pass
        addr = self.find_broker_address_in_publish(mq.broker_name)
        if not addr:
            self.update_topic_route_info_from_name_server(mq.topic)
            addr = self.find_broker_address_in_publish(mq.broker_name)
        if not addr:
            raise MQClientException("The broker[%s] not exist" % mq.broker_name)
        return addr

    def broker_addr_of(self, broker_name: str) -> Optional[str]:
        for route in self.topic_route_table.values():
            addr = MQClientInstance.find_broker_addr_in_route(route, broker_name)
            if addr:
                return addr
        return None

    def find_broker_address_in_publish(self, broker_name: str) -> Optional[str]:
        pass
        with self.topic_route_lock:
            addrs = self.broker_addr_table.get(broker_name)
            if not addrs:
                return None
            return addrs.get(0)

    def get_route_of_all_brokers(self) -> List[str]:
        addrs = []
        for route in self.topic_route_table.values():
            for broker_data in route.get_broker_datas():
                a = broker_data.select_broker_addr()
                if a and a not in addrs:
                    addrs.append(a)
        return addrs

    def get_all_broker_addrs(self) -> List[str]:
        pass
        addrs = []
        for route in self.topic_route_table.values():
            for broker_data in route.get_broker_datas():
                for a in broker_data.broker_addrs.values():
                    if a and a not in addrs:
                        addrs.append(a)
        return addrs

    def get_consumer_id_list_by_group(self, topic: str, consumer_group: str,
                                      timeout_millis: int = 5000) -> Optional[List[str]]:
        pass
        try:
            addr = self._broker_addr_for_topic(topic)
        except MQClientException as e:
            logger.debug("get_consumer_id_list_by_group: no broker for topic %s: %s", topic, e)
            return None
        header = GetConsumerListByGroupRequestHeader()
        header.consumer_group = consumer_group
        request = RemotingCommand.create_request_command(RequestCode.GET_CONSUMER_LIST_BY_GROUP,
                                                         header)
        try:
            response = self._invoke_sync(addr, request, timeout_millis)
        except Exception as e:  # noqa: BLE001
            logger.debug("get_consumer_id_list_by_group failed, %s %s: %s",
                         addr, consumer_group, e)
            return None
        if response.code != ResponseCode.SUCCESS or response.body is None:
            return None
        try:
            body = GetConsumerListByGroupResponseBody.decode(response.body)
        except Exception as e:  # noqa: BLE001
            logger.debug("get_consumer_id_list_by_group decode failed: %s", e)
            return None
        return list(body.consumer_id_list)

    def unregister_client_all_brokers(self, client_id: str, producer_group: str,
                                      consumer_group: str, timeout_millis: int = 3000) -> None:
        pass
        for addr in self.get_all_broker_addrs():
            try:
                self.unregister_client(addr, client_id, producer_group, consumer_group,
                                       timeout_millis)
            except Exception as e:  # noqa: BLE001
                logger.debug("unregister_client failed, addr=%s: %s", addr, e)


__all__ = ["MQClientInstance", "TopicPublishInfo"]
