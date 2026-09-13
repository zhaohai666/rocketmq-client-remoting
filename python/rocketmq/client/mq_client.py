# -*- coding: utf-8 -*-
"""MQClientInstance：RocketMQ 客户端核心编排（对应 org.apache.rocketmq.client.impl.factory.MQClientInstance
与 MQClientAPIImpl 的核心调用面，Python 单机版）。

职责：NameServer 地址管理、Topic 路由获取与缓存、Broker 地址解析、
消息发送（SEND_MESSAGE / SEND_MESSAGE_V2）、拉取（PULL_MESSAGE）、
offset 查询/更新、心跳、管理类 API（创建/删除 Topic、集群信息等）。
"""
from __future__ import annotations

import threading
import time
from typing import Dict, List, Optional

from ..common.message import Message, MessageBatch, MessageExt, MessageQueue
from ..common.message_decoder import decode_messages, message_properties_2_string
from ..common.mix_all import MixAll
from ..common.sysflag import MessageSysFlag
from ..logging import get_logger
from ..remoting.client import RemotingClient
from ..remoting.protocol.body import (ClusterInfo, GetConsumerListByGroupResponseBody,
                                      TopicList)
from ..remoting.protocol.codes import RequestCode, ResponseCode, SerializeType
from ..remoting.protocol.headers import (CreateTopicRequestHeader, GetMaxOffsetRequestHeader,
                                         GetMaxOffsetResponseHeader, GetMinOffsetRequestHeader,
                                         GetMinOffsetResponseHeader, PullMessageRequestHeader,
                                         PullMessageResponseHeader, QueryConsumerOffsetRequestHeader,
                                         QueryConsumerOffsetResponseHeader, QueryMessageRequestHeader,
                                         QueryMessageResponseHeader, SearchOffsetRequestHeader,
                                         SearchOffsetResponseHeader, SendMessageRequestHeader,
                                         SendMessageRequestHeaderV2, SendMessageResponseHeader,
                                         UpdateConsumerOffsetRequestHeader)
from ..remoting.protocol.heartbeat import HeartbeatData
from ..remoting.protocol.remoting_command import RemotingCommand
from ..remoting.protocol.route import TopicRouteData
from .exception import MQBrokerException, MQClientException
from .send_result import SendResult, SendStatus

logger = get_logger()


class TopicPublishInfo:
    """对应 org.apache.rocketmq.client.impl.producer.TopicPublishInfo。"""

    def __init__(self):
        self.order_topic = False
        self.msg_queue_list: List[MessageQueue] = []
        self.topic_route_data: Optional[TopicRouteData] = None
        self._index = 0
        self._lock = threading.Lock()

    def ok(self) -> bool:
        return len(self.msg_queue_list) > 0

    def select_one_message_queue(self, last_broker_name: Optional[str] = None) -> MessageQueue:
        with self._lock:
            if not self.msg_queue_list:
                raise MQClientException("no message queue for publish info")
            idx = self._index % len(self.msg_queue_list)
            self._index += 1
            return self.msg_queue_list[idx]

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
                 connect_timeout_millis: int = 3000, invoke_timeout_millis: int = 15000):
        self.client_id = client_id
        self.name_server_addrs: List[str] = list(name_server_addrs)
        self.remoting_client = RemotingClient(connect_timeout_millis, invoke_timeout_millis)
        self.topic_route_table: Dict[str, TopicRouteData] = {}
        self.topic_publish_info_table: Dict[str, TopicPublishInfo] = {}
        self.topic_route_lock = threading.RLock()
        self._started = False
        self._last_route_fetch = 0.0
        MQClientInstance.INSTANCE_MAP[client_id] = self

    # ---------------- 生命周期 ----------------
    def start(self) -> None:
        self._started = True

    def shutdown(self) -> None:
        self._started = False
        self.remoting_client.shutdown()

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
    def update_topic_route_info_from_name_server(self, topic: str, timeout_millis: int = 5000) -> bool:
        if not self.name_server_addrs:
            raise MQClientException("name server address list is empty")
        request = RemotingCommand.create_request_command(RequestCode.GET_ROUTEINFO_BY_TOPIC, None)
        request.ext_fields["topic"] = topic
        last_exc = None
        for ns_addr in self.name_server_addrs:
            try:
                response = self._invoke_sync(ns_addr, request, timeout_millis)
                if response.code == ResponseCode.SUCCESS and response.body:
                    route = TopicRouteData.decode(response.body)
                    with self.topic_route_lock:
                        self.topic_route_table[topic] = route
                        publish = self.topic_publish_info_table.setdefault(topic, TopicPublishInfo())
                        publish.order_topic = route.order_topic_conf is not None
                        publish.topic_route_data = route
                        publish.msg_queue_list = route.get_all_message_queue()
                    return True
                break
            except Exception as e:  # noqa: BLE001
                last_exc = e
        if last_exc is not None and not (isinstance(last_exc, MQBrokerException)):
            raise last_exc
        return False

    def get_topic_publish_info(self, topic: str) -> TopicPublishInfo:
        with self.topic_route_lock:
            info = self.topic_publish_info_table.get(topic)
            if info is not None and info.ok():
                return info
        self.update_topic_route_info_from_name_server(topic)
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

    @staticmethod
    def find_broker_addr_in_route(route: TopicRouteData, broker_name: str) -> Optional[str]:
        for broker_data in route.get_broker_datas():
            if broker_data.broker_name == broker_name:
                return broker_data.select_broker_addr()
        return None

    # ---------------- 消息发送 ----------------
    def send_message(self, producer_group: str, msg: Message, mq: MessageQueue, timeout_millis: int = 3000) -> SendResult:
        addr = self.find_broker_addr_in_route(self.get_topic_route_data(mq.topic), mq.broker_name) if self.get_topic_route_data(mq.topic) else None
        if addr is None:
            route = self.get_topic_route_data(mq.topic)
            if route is None:
                raise MQClientException("No route info of this topic: %s" % mq.topic)
            addr = MQClientInstance.find_broker_addr_in_route(route, mq.broker_name)
            if addr is None:
                raise MQClientException("Broker %s not found in route of topic %s" % (mq.broker_name, mq.topic))
        request = self._build_send_request(producer_group, msg, mq, timeout_millis)
        response = self._invoke_sync(addr, request, timeout_millis)
        return self._parse_send_response(response, msg, mq)

    def send_message_to_addr(self, producer_group: str, msg: Message, mq: MessageQueue,
                             addr: str, timeout_millis: int = 3000) -> SendResult:
        request = self._build_send_request(producer_group, msg, mq, timeout_millis)
        response = self._invoke_sync(addr, request, timeout_millis)
        return self._parse_send_response(response, msg, mq)

    def send_message_oneway(self, producer_group: str, msg: Message, mq: MessageQueue,
                            addr: str, timeout_millis: int = 3000) -> None:
        request = self._build_send_request(producer_group, msg, mq, timeout_millis)
        request.mark_oneway_rpc()
        self.remoting_client.invoke_oneway(addr, request)

    def _build_send_request(self, producer_group: str, msg: Message, mq: MessageQueue,
                            timeout_millis: int = 3000) -> RemotingCommand:
        header = SendMessageRequestHeaderV2()
        header.producer_group = producer_group
        header.topic = msg.topic
        header.default_topic = MixAll.DEFAULT_TOPIC
        header.default_topic_queue_nums = MixAll.DEFAULT_TOPIC_QUEUE_NUMS
        header.queue_id = mq.queue_id
        header.sys_flag = 0
        header.born_timestamp = int(time.time() * 1000)
        header.flag = msg.flag
        header.properties = message_properties_2_string(msg.properties)
        header.reconsume_times = 0
        header.unit_mode = False
        header.max_reconsume_times = 0
        header.batch = isinstance(msg, MessageBatch)
        request = RemotingCommand.create_request_command(RequestCode.SEND_MESSAGE_V2, header)
        request.body = self._encode_body(msg)
        return request

    @staticmethod
    def _encode_body(msg: Message) -> bytes:
        """对应 Java MQClientAPIImpl.sendMessage：request.setBody(msg.getBody())。

        普通消息的 body 就是原始消息体；批量消息的 body 由 MessageBatch 在构造时
        用 MessageDecoder.encodeMessages 编码好（DefaultMQProducer.batch）。
        """
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
            return SendResult(
                send_status=status_map[response.code],
                msg_id=header.msg_id,
                message_queue=MessageQueue(mq.topic, mq.broker_name, header.queue_id if header.queue_id is not None else mq.queue_id),
                queue_offset=header.queue_offset or 0,
                transaction_id=header.transaction_id,
            )
        raise MQBrokerException(response.code, response.remark or "")

    # ---------------- 消息拉取 ----------------
    def pull_message(self, consumer_group: str, mq: MessageQueue, queue_offset: int,
                     max_msg_nums: int, sys_flag: int, commit_offset: int,
                     subscription: str, sub_version: int, expression_type: str,
                     timeout_millis: int = 30000, max_msg_bytes: int = -1,
                     suspend_timeout_millis: int = 15000, addr: Optional[str] = None,
                     request_source: int = 0) -> "PullResult":
        from .consumer_result import PullResult, PullStatus
        if addr is None:
            route = self.get_topic_route_data(mq.topic)
            if route is None:
                raise MQClientException("No route info of this topic: %s" % mq.topic)
            addr = MQClientInstance.find_broker_addr_in_route(route, mq.broker_name)
            if addr is None:
                raise MQClientException("Broker %s not found in route of topic %s" % (mq.broker_name, mq.topic))
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
        request = RemotingCommand.create_request_command(RequestCode.PULL_MESSAGE, header)
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
                          resp_header.min_offset or 0, resp_header.max_offset or 0, found)

    # ---------------- Offset 查询/更新 ----------------
    def query_consumer_offset(self, consumer_group: str, mq: MessageQueue,
                              timeout_millis: int = 5000, addr: Optional[str] = None,
                              set_zero_if_not_found: bool = True) -> Optional[int]:
        if addr is None:
            addr = self._broker_addr(mq)
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
            addr = self._broker_addr(mq)
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
            addr = self._broker_addr(mq)
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
                                   timeout_millis: int = 5000, addr: Optional[str] = None) -> int:
        if addr is None:
            addr = self._broker_addr(mq)
        header = SearchOffsetRequestHeader()
        header.topic = mq.topic
        header.queue_id = mq.queue_id
        header.timestamp = timestamp
        request = RemotingCommand.create_request_command(RequestCode.SEARCH_OFFSET_BY_TIMESTAMP, header)
        response = self._invoke_sync(addr, request, timeout_millis)
        self._check_response(response)
        resp_header = SearchOffsetResponseHeader()
        resp_header.from_ext_fields(response.ext_fields)
        return resp_header.offset or 0

    def query_message(self, topic: str, key: str, max_num: int, begin_timestamp: int,
                      end_timestamp: int, timeout_millis: int = 15000,
                      addr: Optional[str] = None) -> Optional[bytes]:
        if addr is None:
            route = self.get_topic_route_data(topic)
            if route is None:
                raise MQClientException("No route info of this topic: %s" % topic)
            addr = MQClientInstance.find_broker_addr_in_route(route, route.get_broker_datas()[0].broker_name)
        header = QueryMessageRequestHeader()
        header.topic = topic
        header.key = key
        header.max_num = max_num
        header.begin_timestamp = begin_timestamp
        header.end_timestamp = end_timestamp
        request = RemotingCommand.create_request_command(RequestCode.QUERY_MESSAGE, header)
        response = self._invoke_sync(addr, request, timeout_millis)
        if response.code == ResponseCode.QUERY_NOT_FOUND:
            return None
        self._check_response(response)
        return response.body

    # ---------------- 心跳 / 注销 ----------------
    def send_heartbeat(self, addr: str, heartbeat_data: HeartbeatData, timeout_millis: int = 5000) -> None:
        request = RemotingCommand.create_request_command(RequestCode.HEART_BEAT, None)
        request.body = heartbeat_data.encode()
        response = self._invoke_sync(addr, request, timeout_millis)
        self._check_response(response)

    def unregister_client(self, addr: str, client_id: str, producer_group: str,
                          consumer_group: str, timeout_millis: int = 5000) -> None:
        from ..remoting.protocol.headers import UnregisterClientRequestHeader
        header = UnregisterClientRequestHeader()
        header.client_id = client_id
        header.producer_group = producer_group
        header.consumer_group = consumer_group
        request = RemotingCommand.create_request_command(RequestCode.UNREGISTER_CLIENT, header)
        response = self._invoke_sync(addr, request, timeout_millis)
        self._check_response(response)

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
                               perm: int = 6, timeout_millis: int = 5000) -> None:
        header = CreateTopicRequestHeader()
        header.topic = topic
        header.default_topic = default_topic
        header.read_queue_nums = read_queue_nums
        header.write_queue_nums = write_queue_nums
        header.perm = perm
        request = RemotingCommand.create_request_command(RequestCode.UPDATE_AND_CREATE_TOPIC, header)
        response = self._invoke_sync(broker_addr, request, timeout_millis)
        self._check_response(response)

    def create_topic_in_route(self, topic: str, read_queue_nums: int = 4, write_queue_nums: int = 4,
                              perm: int = 6, timeout_millis: int = 5000) -> None:
        route = self.get_topic_route_data(MixAll.DEFAULT_TOPIC)
        if route is None:
            raise MQClientException("No route info of default topic %s" % MixAll.DEFAULT_TOPIC)
        for broker_data in route.get_broker_datas():
            addr = broker_data.select_broker_addr()
            if addr:
                self.create_topic_in_broker(addr, MixAll.DEFAULT_TOPIC, topic,
                                            read_queue_nums, write_queue_nums, perm, timeout_millis)

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
        from ..remoting.protocol.headers import GetConsumerListByGroupRequestHeader
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
    def _broker_addr(self, mq: MessageQueue) -> str:
        route = self.get_topic_route_data(mq.topic)
        if route is None:
            raise MQClientException("No route info of this topic: %s" % mq.topic)
        addr = MQClientInstance.find_broker_addr_in_route(route, mq.broker_name)
        if addr is None:
            raise MQClientException("Broker %s not found in route of topic %s" % (mq.broker_name, mq.topic))
        return addr

    def broker_addr_of(self, broker_name: str) -> Optional[str]:
        for route in self.topic_route_table.values():
            addr = MQClientInstance.find_broker_addr_in_route(route, broker_name)
            if addr:
                return addr
        return None

    def get_route_of_all_brokers(self) -> List[str]:
        addrs = []
        for route in self.topic_route_table.values():
            for broker_data in route.get_broker_datas():
                a = broker_data.select_broker_addr()
                if a and a not in addrs:
                    addrs.append(a)
        return addrs


__all__ = ["MQClientInstance", "TopicPublishInfo"]