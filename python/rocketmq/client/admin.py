# -*- coding: utf-8 -*-
"""管理端（对应 org.apache.rocketmq.client.admin.DefaultMQAdminExt / MQAdminExt）。

提供：Topic 创建/删除/列举、Broker 集群信息、路由信息、offset 管理、
消费组管理、消息查询（按时间/msgId）、重投、统计等管理能力。
"""
from __future__ import annotations

import time
from typing import Dict, List, Optional, Set

from ..common.message import MessageExt, MessageQueue
from ..common.mix_all import MixAll
from ..logging import get_logger
from ..remoting.exception import RemotingException
from ..remoting.protocol.body import (ClusterInfo, ConsumerConnection,
                                      ConsumerRunningInfo, KVTable,
                                      ProducerConnection, TopicList)
from ..remoting.protocol.codes import RequestCode, ResponseCode
from ..remoting.protocol.heartbeat import HeartbeatData
from ..remoting.protocol.remoting_command import RemotingCommand
from ..remoting.protocol.route import BrokerData, QueueData, TopicRouteData
from ..remoting.rpchook import RPCHook
from .consumer_result import PullResult, PullStatus
from .exception import MQBrokerException, MQClientException
from .mq_client import MQClientInstance

logger = get_logger()


class DefaultMQAdminExt:
    """管理客户端（对应 org.apache.rocketmq.client.admin.DefaultMQAdminExt）。"""

    def __init__(self, rpc_hook: Optional[RPCHook] = None,
                 namespace: str = "", **kwargs):
        self.namespace = namespace
        self.instance_name = "ADMIN"
        self.client_id: Optional[str] = None
        self.name_server_addrs: List[str] = []
        self.rpc_hook = rpc_hook
        self._mq_client: Optional[MQClientInstance] = None
        self._started = False

    # ---------------- 配置与生命周期 ----------------
    def set_namesrv_addr(self, addr: str) -> None:
        self.name_server_addrs = [a.strip() for a in addr.split(";") if a.strip()]

    def set_name_server_addresses(self, addrs: List[str]) -> None:
        self.name_server_addrs = list(addrs)

    def set_instance_name(self, name: str) -> None:
        self.instance_name = name

    def get_name_server_addr(self) -> str:
        return ";".join(self.name_server_addrs)

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
            raise MQClientException("admin not started, call start() first")
        return self._mq_client

    # ---------------- Topic 管理 ----------------
    def create_topic(self, key: str, new_topic: str, queue_num: int = 4,
                     topic_sys_flag: int = 0) -> None:
        client = self._require_client()
        client.create_topic_in_route(new_topic, queue_num, queue_num, 6)

    def create_topic_in_broker(self, broker_addr: str, topic: str,
                               read_queue_nums: int = 4, write_queue_nums: int = 4,
                               perm: int = 6) -> None:
        client = self._require_client()
        client.create_topic_in_broker(broker_addr, MixAll.DEFAULT_TOPIC, topic,
                                      read_queue_nums, write_queue_nums, perm)

    def delete_topic_in_broker(self, broker_addr: str, topic: str) -> None:
        self._require_client().delete_topic_in_broker(broker_addr, topic)

    def delete_topic_in_namesrv(self, topic: str) -> None:
        self._require_client().delete_topic_in_namesrv(topic)

    def delete_topic(self, topic: str) -> None:
        """删除 topic（同时清 Namesrv 路由与各 broker）。"""
        client = self._require_client()
        client.delete_topic_in_namesrv(topic)
        try:
            cluster = client.get_broker_cluster_info()
            for broker in cluster.get_broker_addrs():
                client.delete_topic_in_broker(broker, topic)
        except Exception as e:  # noqa: BLE001
            logger.warning("delete topic %s in brokers failed partially: %s", topic, e)

    def fetch_all_topic_list(self) -> TopicList:
        return self._require_client().get_all_topic_list_from_name_server()

    def fetch_topics_by_cluster(self, cluster_name: str) -> Set[str]:
        client = self._require_client()
        request = RemotingCommand.create_request_command(RequestCode.GET_TOPICS_BY_CLUSTER, None)
        request.ext_fields["clusterName"] = cluster_name
        topics: Set[str] = set()
        for ns in client.name_server_addrs:
            try:
                response = client._invoke_sync(ns, request, 10000)
                if response.code == ResponseCode.SUCCESS:
                    body = response.body or b"{}"
                    from ..remoting.protocol.serialize import RemotingSerializable
                    obj = RemotingSerializable.decode_json(body)
                    topics.update(obj.get("topicList", []))
                    break
            except Exception:  # noqa: BLE001
                continue
        return topics

    # ---------------- 集群 / 路由 ----------------
    def fetch_broker_cluster_info(self) -> ClusterInfo:
        return self._require_client().get_broker_cluster_info()

    def fetch_all_topic_route(self) -> List[TopicRouteData]:
        """拉取全部 topic 的路由（遍历全部 broker 的 topic 列表）。"""
        client = self._require_client()
        result: List[TopicRouteData] = []
        topics = client.get_all_topic_list_from_name_server()
        for topic in topics.get_topic_list():
            try:
                route = client.get_topic_route_data(topic)
                if route is not None:
                    result.append(route)
            except Exception:  # noqa: BLE001
                continue
        return result

    def examine_topic_route(self, topic: str) -> TopicRouteData:
        client = self._require_client()
        route = client.get_topic_route_data(topic)
        if route is None:
            raise MQClientException("topic %s not exist" % topic)
        return route

    def examine_broker_cluster_info(self) -> ClusterInfo:
        return self.fetch_broker_cluster_info()

    # ---------------- 消费组管理 ----------------
    def examine_consumer_connection_info(self, consumer_group: str,
                                         broker_addr: Optional[str] = None) -> ConsumerConnection:
        client = self._require_client()
        from ..remoting.protocol.headers import GetConsumerConnectionListRequestHeader
        addr = broker_addr
        if addr is None:
            addr = self._find_first_broker_addr(client)
        header = GetConsumerConnectionListRequestHeader()
        header.consumer_group = consumer_group
        request = RemotingCommand.create_request_command(RequestCode.GET_CONSUMER_CONNECTION_LIST, header)
        response = client._invoke_sync(addr, request, 10000)
        client._check_response(response)
        body = response.body or b"{}"
        return ConsumerConnection.decode(body)

    def examine_consumer_connection(self, consumer_group: str,
                                    broker_addr: Optional[str] = None) -> ConsumerConnection:
        return self.examine_consumer_connection_info(consumer_group, broker_addr)

    def examine_consumer_running_info(self, consumer_group: str, client_id: str,
                                      broker_addr: Optional[str] = None) -> ConsumerRunningInfo:
        client = self._require_client()
        from ..remoting.protocol.headers import GetConsumerRunningInfoRequestHeader
        addr = broker_addr
        if addr is None:
            addr = self._find_first_broker_addr(client)
        header = GetConsumerRunningInfoRequestHeader()
        header.consumer_group = consumer_group
        header.client_id = client_id
        header.jstack_enabled = False
        request = RemotingCommand.create_request_command(RequestCode.GET_CONSUMER_RUNNING_INFO, header)
        response = client._invoke_sync(addr, request, 10000)
        client._check_response(response)
        body = response.body or b"{}"
        return ConsumerRunningInfo.decode(body)

    def get_consumer_list_by_group(self, consumer_group: str,
                                   broker_addr: Optional[str] = None):
        client = self._require_client()
        if broker_addr is None:
            broker_addr = self._find_first_broker_addr(client)
        return client.get_consumer_list_by_group(consumer_group, addr=broker_addr)

    # ---------------- Offset 管理 ----------------
    def max_offset(self, mq: MessageQueue) -> int:
        return self._require_client().get_max_offset(mq)

    def min_offset(self, mq: MessageQueue) -> int:
        return self._require_client().get_min_offset(mq)

    def search_offset(self, mq: MessageQueue, timestamp: int) -> int:
        return self._require_client().search_offset_by_timestamp(mq, timestamp)

    def examine_consumer_offset(self, consumer_group: str, mq: MessageQueue) -> Optional[int]:
        return self._require_client().query_consumer_offset(consumer_group, mq)

    def update_consumer_offset(self, consumer_group: str, mq: MessageQueue, offset: int) -> None:
        self._require_client().update_consumer_offset(consumer_group, mq, offset)

    def reset_offset_by_timestamp(self, consumer_group: str, topic: str,
                                  timestamp: int, force: bool = True) -> Dict[MessageQueue, int]:
        """重置消费位点：对 topic 全部队列按时间戳计算新 offset。"""
        client = self._require_client()
        from ..remoting.protocol.body import ResetOffsetBody
        from ..remoting.protocol.headers import ResetOffsetRequestHeader
        result: Dict[MessageQueue, int] = {}
        route = client.get_topic_route_data(topic)
        if route is None:
            raise MQClientException("topic %s not exist" % topic)
        mqs = route.get_all_message_queue()
        for mq in mqs:
            mq.topic = topic
            try:
                new_offset = client.search_offset_by_timestamp(mq, timestamp)
            except Exception:  # noqa: BLE001
                new_offset = 0
            client.update_consumer_offset(consumer_group, mq, new_offset)
            result[mq] = new_offset
        # 若已知 broker 地址，同步 broker 端 offset 表（简化跳过）
        return result

    # ---------------- 消息查询 ----------------
    def query_message(self, topic: str, key: str, max_num: int, begin: int, end: int):
        client = self._require_client()
        body = client.query_message(topic, key, max_num, begin, end)
        if body is None:
            return []
        from ..common.message_decoder import decode_messages
        return decode_messages(body)

    def query_message_by_uniq_key(self, topic: str, uniq_key: str) -> Optional[MessageExt]:
        client = self._require_client()
        body = client.query_message(topic, uniq_key, 1, 0, int(time.time() * 1000) + 60 * 60 * 1000)
        if body is None:
            return None
        messages = decode_messages(body)
        return messages[0] if messages else None

    def view_message(self, topic: str, msg_id: str) -> Optional[MessageExt]:
        raise MQClientException("viewMessage by msgId is not supported in Python edition")

    # ---------------- 其他管理 ----------------
    def send_message_back(self, mq: MessageQueue, msg_id: str, consumer_group: str,
                          delay_level: int, origin_topic: str) -> None:
        """重投消息（对应 Java sendMessageBack）。"""
        raise MQClientException("sendMessageBack by msgId needs full MessageExt; use consumer.send_message_back")

    def get_broker_config(self, broker_addr: str) -> KVTable:
        client = self._require_client()
        request = RemotingCommand.create_request_command(RequestCode.GET_BROKER_CONFIG, None)
        response = client._invoke_sync(broker_addr, request, 10000)
        client._check_response(response)
        body = response.body or b"{}"
        return KVTable.decode(body)

    def get_all_topic_config(self, broker_addr: str) -> Dict[str, dict]:
        client = self._require_client()
        request = RemotingCommand.create_request_command(RequestCode.GET_ALL_TOPIC_CONFIG, None)
        response = client._invoke_sync(broker_addr, request, 30000)
        client._check_response(response)
        body = response.body or b"{}"
        from ..remoting.protocol.serialize import RemotingSerializable
        obj = RemotingSerializable.decode_json(body)
        return obj.get("data", {})

    # ---------------- 内部工具 ----------------
    @staticmethod
    def _find_first_broker_addr(client: MQClientInstance) -> str:
        try:
            cluster = client.get_broker_cluster_info()
            addrs = cluster.get_broker_addrs()
            if addrs:
                return addrs[0]
        except Exception:  # noqa: BLE001
            pass
        raise MQClientException("no broker address available")


__all__ = ["DefaultMQAdminExt", "ClusterInfo", "TopicList", "KVTable",
           "ConsumerConnection", "ConsumerRunningInfo", "ProducerConnection",
           "TopicRouteData", "QueueData", "BrokerData", "PullResult", "PullStatus"]