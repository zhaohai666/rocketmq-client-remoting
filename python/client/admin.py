# -*- coding: utf-8 -*-
from __future__ import annotations

import time
from typing import Dict, List, Optional, Set

from common.boundary_type import BoundaryType
from common.message import MessageExt, MessageQueue
from common.message_decoder import decode_message_id, decode_message, decode_messages
from common.mix_all import MixAll
from common.sysflag import PermName
from common.topic_config import TopicConfig
from rocketmq_logging import get_logger
from remoting.protocol.body import (ClusterInfo, ConsumeMessageDirectlyResult,
                                      ConsumerConnection,
                                      ConsumerRunningInfo, KVTable,
                                      ProducerConnection, QueryConsumeTimeSpanBody,
                                      ResetOffsetBody, TopicList)
from remoting.protocol.headers import ConsumeMessageDirectlyResultRequestHeader
from remoting.protocol.admin_body import (ConsumeStats, QueryConsumeQueueResponseBody,
                                            TopicConfigSerializeWrapper, TopicStatsTable)
from remoting.protocol.codes import LanguageCode, RequestCode, ResponseCode
from remoting.protocol.remoting_command import RemotingCommand
from remoting.protocol.route import BrokerData, QueueData, TopicRouteData
from remoting.protocol.serialize import RemotingSerializable, fastjson_loads
from remoting.protocol.subscription import (SubscriptionGroupConfig,
                                              SubscriptionGroupWrapper)
from remoting.rpchook import RPCHook
from remoting.socks_proxy import SocksProxyConfig
from .consumer_result import PullResult, PullStatus
from .exception import MQBrokerException, MQClientException
from .mq_client import MQClientInstance
from .mq_client_manager import MQClientManager

logger = get_logger()

# Default timeout (corresponds to DefaultMQAdminExt.DEFAULT_TIMEOUT = 5000 * 3)
DEFAULT_TIMEOUT = 5000 * 3

NAMESPACE_ORDER_TOPIC_CONFIG = "ORDER_TOPIC_CONFIG"



class TrackType:
    pass

    CONSUMED = "CONSUMED"
    CONSUMED_BUT_FILTERED = "CONSUMED_BUT_FILTERED"
    PULL = "PULL"
    NOT_CONSUME_YET = "NOT_CONSUME_YET"
    NOT_ONLINE = "NOT_ONLINE"
    CONSUME_BROADCASTING = "CONSUME_BROADCASTING"
    UNKNOWN = "UNKNOWN"


class MessageTrack:
    pass

    def __init__(self, consumer_group: Optional[str] = None,
                 track_type: str = TrackType.UNKNOWN,
                 exception_desc: Optional[str] = None):
        self.consumer_group = consumer_group
        self.track_type = track_type
        self.exception_desc = exception_desc

    def to_dict(self) -> dict:
        d = {"consumerGroup": self.consumer_group,
             "trackType": self.track_type}
        if self.exception_desc is not None:
            d["exceptionDesc"] = self.exception_desc
        return d

    @staticmethod
    def from_dict(d: dict) -> "MessageTrack":
        return MessageTrack(
            consumer_group=d.get("consumerGroup"),
            track_type=d.get("trackType") or TrackType.UNKNOWN,
            exception_desc=d.get("exceptionDesc"),
        )

    def encode(self) -> bytes:
        return RemotingSerializable.encode(self.to_dict())

    @staticmethod
    def decode(data: bytes) -> "MessageTrack":
        return MessageTrack.from_dict(fastjson_loads(data.decode("utf-8")))

    def __repr__(self):
        return "MessageTrack [consumerGroup=%s, trackType=%s, exceptionDesc=%s]" % (
            self.consumer_group, self.track_type, self.exception_desc)


class DefaultMQAdminExt:
    pass

    def __init__(self, rpc_hook: Optional[RPCHook] = None,
                 namespace: str = "", **kwargs):
        self.namespace = namespace
        self.instance_name = "ADMIN"
        self.client_id: Optional[str] = None
        self.unit_name: Optional[str] = None
        self.enable_stream_request_type = False
        self.namespace_v2 = ""
        self.vip_channel_enabled = False
        self.socks_proxy_config: Optional[SocksProxyConfig] = None
        self.name_server_addrs: List[str] = []
        self.poll_name_server_interval = 30000
        self.rpc_hook = rpc_hook
        self.kv_namespace_to_delete_list: List[str] = []
        self.timeout_millis = int(kwargs.get("timeout_millis", DEFAULT_TIMEOUT))
        self._mq_client: Optional[MQClientInstance] = None
        self._started = False

    # ---------------- Configuration and lifecycle ----------------
    def set_namesrv_addr(self, addr: str) -> None:
        self.name_server_addrs = [a.strip() for a in addr.split(";") if a.strip()]

    def set_name_server_addresses(self, addrs: List[str]) -> None:
        self.name_server_addrs = list(addrs)

    def set_instance_name(self, name: str) -> None:
        self.instance_name = name

    def set_unit_name(self, unit_name: Optional[str]) -> None:
        pass
        self.unit_name = unit_name

    def get_unit_name(self) -> Optional[str]:
        return self.unit_name

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

    def set_vip_channel_enabled(self, enable: bool) -> None:
        pass
        self.vip_channel_enabled = bool(enable)

    def get_name_server_addr(self) -> str:
        return ";".join(self.name_server_addrs)

    def get_name_server_address_list(self) -> List[str]:
        return list(self.name_server_addrs)

    def set_timeout_millis(self, timeout_millis: int) -> None:
        self.timeout_millis = timeout_millis

    def start(self) -> None:
        if self._started:
            return
        if not self.name_server_addrs:
            raise MQClientException("name server address is not set")
        self.instance_name = MixAll.change_instance_name_to_pid(self.instance_name)
        if self.client_id is None:
            self.client_id = MixAll.client_id_for(self.instance_name, self.unit_name,
                                                  self.enable_stream_request_type)
        def _create_instance() -> MQClientInstance:
            return MQClientInstance(self.client_id, self.name_server_addrs,
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
            manager.unregister_client(self.client_id, self._mq_client)
            raise
        self._started = True

    def shutdown(self) -> None:
        if not self._started:
            return
        self._started = False
        if self._mq_client is not None:
            MQClientManager.get_instance().unregister_client(
                self.client_id or "", self._mq_client)

    def get_mq_client_instance(self) -> MQClientInstance:
        return self._require_client()

    def _require_client(self) -> MQClientInstance:
        if not self._started or self._mq_client is None:
            raise MQClientException("admin not started, call start() first")
        return self._mq_client

    # ---------------- Low-level invocation helpers ----------------
    def _invoke_broker(self, addr: str, code: int,
                       ext_fields: Optional[Dict[str, str]] = None,
                       body: Optional[bytes] = None,
                       timeout_millis: Optional[int] = None,
                       vip: bool = True) -> RemotingCommand:
        """Send to the specified Broker and verify SUCCESS.

        ``vip`` mirrors the per-request behaviour of the broker address rewrite: the RPCs the
        protocol rewrites go to the VIP port (listen port - 2) while the switch is on, and the few
        broker RPCs that are answered on the plain port only -- reading the broker config is one of
        them -- pass ``vip=False``.
        """
        client = self._require_client()
        if vip:
            addr = MixAll.broker_vip_channel(self.vip_channel_enabled, addr)
        request = RemotingCommand.create_request_command(code, None)
        if ext_fields:
            for k, v in ext_fields.items():
                request.ext_fields[k] = str(v)
        if body is not None:
            request.body = body
        response = client._invoke_sync(addr, request,
                                       timeout_millis or self.timeout_millis)
        client._check_response(response)
        return response

    def _invoke_namesrv_all(self, code: int,
                            ext_fields: Optional[Dict[str, str]] = None,
                            timeout_millis: Optional[int] = None) -> Optional[RemotingCommand]:
        pass
        client = self._require_client()
        request = RemotingCommand.create_request_command(code, None)
        if ext_fields:
            for k, v in ext_fields.items():
                request.ext_fields[k] = str(v)
        err_response = None
        for ns_addr in client.name_server_addrs:
            response = client._invoke_sync(ns_addr, request,
                                           timeout_millis or self.timeout_millis)
            if response.code != ResponseCode.SUCCESS:
                err_response = response
        if err_response is not None:
            raise MQClientException(err_response.remark or "put/delete kv config failed",
                                    err_response.code)
        return None

    def _invoke_namesrv_one(self, code: int,
                            ext_fields: Optional[Dict[str, str]] = None,
                            timeout_millis: Optional[int] = None) -> RemotingCommand:
        pass
        client = self._require_client()
        request = RemotingCommand.create_request_command(code, None)
        if ext_fields:
            for k, v in ext_fields.items():
                request.ext_fields[k] = str(v)
        last_exc: Optional[Exception] = None
        for ns_addr in client.name_server_addrs:
            try:
                return client._invoke_sync(ns_addr, request,
                                           timeout_millis or self.timeout_millis)
            except Exception as e:  # noqa: BLE001
                last_exc = e
        raise MQClientException("all name servers unreachable: %s" % last_exc)

    @staticmethod
    def _first_broker_addr(client: MQClientInstance) -> str:
        try:
            cluster = client.get_broker_cluster_info()
            addrs = cluster.get_broker_addrs()
            if addrs:
                return addrs[0]
        except Exception:  # noqa: BLE001
            pass
        raise MQClientException("no broker address available")

    def _find_first_broker_addr(self, client: MQClientInstance) -> str:
        return DefaultMQAdminExt._first_broker_addr(client)

    # ---------------- Topic management ----------------
    def create_topic(self, key: str, new_topic: str, queue_num: int = 4,
                     topic_sys_flag: int = 0) -> None:
        client = self._require_client()
        client.create_topic_in_route(new_topic, queue_num, queue_num, MixAll.READ_PERM_BY_DEFAULT)

    def create_and_update_topic_config(self, addr: str, config: TopicConfig) -> None:
        pass
        client = self._require_client()
        client.create_topic_in_broker(addr, MixAll.DEFAULT_TOPIC, config.topic_name,
                                      config.read_queue_nums, config.write_queue_nums,
                                      config.perm)

    def create_topic_in_broker(self, broker_addr: str, topic: str,
                               read_queue_nums: int = 4, write_queue_nums: int = 4,
                               perm: int = 6) -> None:
        client = self._require_client()
        client.create_topic_in_broker(broker_addr, MixAll.DEFAULT_TOPIC, topic,
                                      read_queue_nums, write_queue_nums, perm)

    def delete_topic_in_broker(self, broker_addr: str, topic: str) -> None:
        self._require_client().delete_topic_in_broker(broker_addr, topic)

    def delete_topic_in_name_server(self, addrs: Optional[Set[str]], topic: str) -> None:
        client = self._require_client()
        targets = list(addrs) if addrs else list(client.name_server_addrs)
        for ns_addr in targets:
            self._invoke_namesrv_addr(ns_addr, RequestCode.DELETE_TOPIC_IN_NAMESRV,
                                      {"topic": topic})

    def delete_topic_in_namesrv(self, topic: str) -> None:
        """Backward-compatible alias: delete the topic route on the NameServer."""
        client = self._require_client()
        client.delete_topic_in_namesrv(topic)

    def delete_topic(self, topic: str, cluster_name: Optional[str] = None) -> None:
        """Delete a topic (clear it on each broker, then the NameServer route, then the KV namespace)."""
        client = self._require_client()
        for broker in self._broker_addrs_of_cluster(client, cluster_name):
            try:
                client.delete_topic_in_broker(broker, topic)
            except Exception as e:  # noqa: BLE001
                logger.warning("delete topic %s in broker %s failed: %s", topic, broker, e)
        try:
            client.delete_topic_in_namesrv(topic)
        except Exception as e:  # noqa: BLE001
            logger.warning("delete topic %s in name server failed: %s", topic, e)
        for ns in self.kv_namespace_to_delete_list:
            try:
                self.delete_kv_config(ns, topic)
            except Exception as e:  # noqa: BLE001
                logger.warning("delete kv config %s/%s failed: %s", ns, topic, e)

    def _broker_addrs_of_cluster(self, client: MQClientInstance,
                                 cluster_name: Optional[str] = None) -> List[str]:
        cluster = client.get_broker_cluster_info()
        if not cluster_name:
            return cluster.get_broker_addrs()
        # cluster_addr_table: {clusterName: [brokerName, ...]}
        # broker_addr_table : {brokerName: {brokerId: addr}}
        broker_names = set(cluster.cluster_addr_table.get(cluster_name) or [])
        result: List[str] = []
        for broker_name in sorted(broker_names):
            for addr in sorted((cluster.broker_addr_table.get(broker_name) or {}).values()):
                if addr:
                    result.append(addr)
        return result or cluster.get_broker_addrs()

    def fetch_all_topic_list(self) -> TopicList:
        return self._require_client().get_all_topic_list_from_name_server()

    def fetch_topics_by_cluster(self, cluster_name: str) -> Set[str]:
        pass
        response = self._invoke_namesrv_one(RequestCode.GET_TOPICS_BY_CLUSTER,
                                            {"cluster": cluster_name})
        topics: Set[str] = set()
        if response.code == ResponseCode.SUCCESS and response.body:
            obj = RemotingSerializable.decode_json(response.body)
            topics.update(obj.get("topicList", []))
        return topics

    def get_cluster_list(self, topic: str) -> Set[str]:
        pass
        client = self._require_client()
        cluster_info = client.get_broker_cluster_info()
        route = self.examine_topic_route(topic)
        broker_names = {bd.broker_name for bd in route.get_broker_datas()}
        clusters: Set[str] = set()
        for cluster_name, names in cluster_info.cluster_addr_table.items():
            if set(names or []) & broker_names:
                clusters.add(cluster_name)
        return clusters

    def get_topic_cluster_list(self, topic: str) -> Set[str]:
        return self.get_cluster_list(topic)

    def fetch_all_topic_route(self) -> List[TopicRouteData]:
        """Fetch the routes of all topics (by iterating the NameServer's topic list)."""
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

    def examine_topic_config(self, addr: str, topic: str) -> TopicConfig:
        pass
        response = self._invoke_broker(addr, RequestCode.GET_TOPIC_CONFIG,
                                       {"topic": topic, "lo": "true"})
        if not response.body:
            raise MQBrokerException(ResponseCode.SYSTEM_ERROR,
                                    "empty topic config for %s" % topic)
        return TopicConfig.from_dict(fastjson_loads(response.body.decode("utf-8")))

    def get_all_topic_config(self, broker_addr: str,
                             timeout_millis: Optional[int] = None) -> TopicConfigSerializeWrapper:
        response = self._invoke_broker(broker_addr, RequestCode.GET_ALL_TOPIC_CONFIG,
                                       timeout_millis=timeout_millis)
        if not response.body:
            return TopicConfigSerializeWrapper()
        return TopicConfigSerializeWrapper.decode(response.body)

    def get_user_topic_config(self, broker_addr: str, special_topic: bool = False,
                              timeout_millis: Optional[int] = None) -> TopicConfigSerializeWrapper:
        pass
        wrapper = self.get_all_topic_config(broker_addr, timeout_millis)
        sys_topics = set(self.get_system_topic_list_from_broker(
            broker_addr, timeout_millis).get_topic_list())
        kept: Dict[str, TopicConfig] = {}
        for name, cfg in wrapper.topic_config_table.items():
            if name in sys_topics or MixAll.is_sys_topic(name):
                continue
            if not special_topic and (name.startswith(MixAll.RETRY_GROUP_TOPIC_PREFIX)
                                      or name.startswith(MixAll.DLQ_GROUP_TOPIC_PREFIX)):
                continue
            kept[name] = cfg
        wrapper.topic_config_table = kept
        return wrapper

    def get_system_topic_list_from_broker(self, broker_addr: str,
                                          timeout_millis: Optional[int] = None) -> TopicList:
        response = self._invoke_broker(broker_addr,
                                       RequestCode.GET_SYSTEM_TOPIC_LIST_FROM_BROKER,
                                       timeout_millis=timeout_millis)
        if not response.body:
            return TopicList()
        return TopicList.decode(response.body)

    def examine_topic_stats(self, topic: str) -> TopicStatsTable:
        pass
        route = self.examine_topic_route(topic)
        merged = TopicStatsTable()
        for bd in route.get_broker_datas():
            addr = bd.select_broker_addr()
            if not addr:
                continue
            try:
                part = self.examine_topic_stats_by_broker(addr, topic)
            except Exception as e:  # noqa: BLE001
                logger.warning("getTopicStatsInfo error. topic=%s broker=%s: %s",
                               topic, addr, e)
                continue
            merged.offset_table.update(part.offset_table)
            merged.topic_put_tps += part.topic_put_tps
        if not merged.offset_table:
            raise MQClientException("Not found the topic stats info")
        return merged

    def examine_topic_stats_by_broker(self, broker_addr: str,
                                      topic: str) -> TopicStatsTable:
        response = self._invoke_broker(broker_addr, RequestCode.GET_TOPIC_STATS_INFO,
                                       {"topic": topic})
        if not response.body:
            return TopicStatsTable()
        return TopicStatsTable.decode(response.body)

    # ---------------- Cluster / Broker ----------------
    def fetch_broker_cluster_info(self) -> ClusterInfo:
        return self._require_client().get_broker_cluster_info()

    def examine_broker_cluster_info(self) -> ClusterInfo:
        return self.fetch_broker_cluster_info()

    def fetch_broker_runtime_stats(self, broker_addr: str,
                                   timeout_millis: Optional[int] = None) -> KVTable:
        response = self._invoke_broker(broker_addr, RequestCode.GET_BROKER_RUNTIME_INFO,
                                       timeout_millis=timeout_millis)
        if not response.body:
            return KVTable()
        return KVTable.decode(response.body)

    def get_broker_runtime_info(self, broker_addr: str,
                                timeout_millis: Optional[int] = None) -> KVTable:
        return self.fetch_broker_runtime_stats(broker_addr, timeout_millis)

    def get_broker_config(self, broker_addr: str,
                          timeout_millis: Optional[int] = None) -> Dict[str, str]:
        pass
        response = self._invoke_broker(broker_addr, RequestCode.GET_BROKER_CONFIG,
                                       timeout_millis=timeout_millis, vip=False)
        text = (response.body or b"").decode("utf-8")
        return MixAll.string2_properties(text)

    def update_broker_config(self, broker_addr: str, properties: Dict[str, str],
                             timeout_millis: Optional[int] = None) -> None:
        pass
        broker_permission = (properties or {}).get("brokerPermission")
        if broker_permission is not None and not _perm_is_valid(broker_permission):
            raise MQClientException(
                "brokerPermission value: %s is invalid." % broker_permission,
                ResponseCode.NO_PERMISSION)
        text = MixAll.properties2_string(properties)
        if not text:
            return
        self._invoke_broker(broker_addr, RequestCode.UPDATE_BROKER_CONFIG,
                            body=text.encode("utf-8"), timeout_millis=timeout_millis)

    def _invoke_namesrv_addr(self, addr: str, code: int,
                             ext_fields: Optional[Dict[str, str]] = None,
                             timeout_millis: Optional[int] = None) -> RemotingCommand:
        """Send to an explicitly given NameServer and verify SUCCESS (does not use the VIP channel)."""
        client = self._require_client()
        request = RemotingCommand.create_request_command(code, None)
        if ext_fields:
            for k, v in ext_fields.items():
                request.ext_fields[k] = str(v)
        response = client._invoke_sync(addr, request, timeout_millis or self.timeout_millis)
        client._check_response(response)
        return response

    def wipe_write_perm_of_broker(self, namesrv_addr: str, broker_name: str) -> int:
        response = self._invoke_namesrv_addr(namesrv_addr, RequestCode.WIPE_WRITE_PERM_OF_BROKER,
                                             {"brokerName": broker_name})
        return int(response.ext_fields.get("wipeTopicCount", 0) or 0)

    def add_write_perm_of_broker(self, namesrv_addr: str, broker_name: str) -> int:
        response = self._invoke_namesrv_addr(namesrv_addr, RequestCode.ADD_WRITE_PERM_OF_BROKER,
                                             {"brokerName": broker_name})
        return int(response.ext_fields.get("addTopicCount", 0) or 0)

    def clean_unused_topic(self, cluster_name: Optional[str] = None,
                           topic: Optional[str] = None) -> bool:
        pass
        client = self._require_client()
        ok = True
        for addr in self._broker_addrs_of_cluster(client, cluster_name):
            try:
                response = self._invoke_broker(addr, RequestCode.CLEAN_UNUSED_TOPIC)
                if response.code != ResponseCode.SUCCESS:
                    ok = False
            except Exception as e:  # noqa: BLE001
                logger.warning("cleanUnusedTopic on %s failed: %s", addr, e)
                ok = False
        return ok

    def view_broker_stats_data(self, broker_addr: str, stats_name: str,
                               stats_key: str) -> dict:
        response = self._invoke_broker(broker_addr, RequestCode.VIEW_BROKER_STATS_DATA,
                                       {"statsName": stats_name, "statsKey": stats_key})
        if not response.body:
            return {}
        return fastjson_loads(response.body.decode("utf-8"))

    # ---------------- NameServer KV config ----------------
    def create_and_update_kv_config(self, namespace: str, key: str, value: str) -> None:
        pass
        self._invoke_namesrv_all(RequestCode.PUT_KV_CONFIG,
                                 {"namespace": namespace, "key": key, "value": value})

    def put_kv_config(self, namespace: str, key: str, value: str) -> None:
        self.create_and_update_kv_config(namespace, key, value)

    def get_kv_config(self, namespace: str, key: str) -> Optional[str]:
        response = self._invoke_namesrv_one(RequestCode.GET_KV_CONFIG,
                                            {"namespace": namespace, "key": key})
        if response.code == ResponseCode.SUCCESS:
            return response.ext_fields.get("value")
        return None

    def delete_kv_config(self, namespace: str, key: str) -> None:
        self._invoke_namesrv_all(RequestCode.DELETE_KV_CONFIG,
                                 {"namespace": namespace, "key": key})

    def get_kv_list_by_namespace(self, namespace: str) -> KVTable:
        response = self._invoke_namesrv_one(RequestCode.GET_KVLIST_BY_NAMESPACE,
                                            {"namespace": namespace})
        if not response.body:
            return KVTable()
        return KVTable.decode(response.body)

    # ---------------- Subscription group management ----------------
    def create_and_update_subscription_group_config(self, addr: str,
                                                    config: SubscriptionGroupConfig) -> None:
        pass
        self._invoke_broker(addr, RequestCode.UPDATE_AND_CREATE_SUBSCRIPTIONGROUP,
                            body=config.encode())

    def examine_subscription_group_config(self, addr: str, group: str) -> Optional[SubscriptionGroupConfig]:
        pass
        wrapper = self.get_all_subscription_group(addr)
        return wrapper.subscription_group_table.get(group)

    def get_subscription_group_config(self, addr: str, group: str) -> Optional[SubscriptionGroupConfig]:
        pass
        response = self._invoke_broker(addr, RequestCode.GET_SUBSCRIPTIONGROUP_CONFIG,
                                       {"group": group})
        if not response.body:
            return None
        return SubscriptionGroupConfig.decode(response.body)

    def get_all_subscription_group(self, broker_addr: str,
                                   timeout_millis: Optional[int] = None) -> SubscriptionGroupWrapper:
        pass
        client = self._require_client()
        timeout = timeout_millis or self.timeout_millis
        broker_addr = MixAll.broker_vip_channel(self.vip_channel_enabled, broker_addr)
        current_data_version: Optional[dict] = None
        group_seq = 0
        table: Dict[str, SubscriptionGroupConfig] = {}
        forbidden: Dict[str, object] = {}
        begin = time.monotonic()
        while True:
            left = timeout - int((time.monotonic() - begin) * 1000)
            if left < 0:
                raise MQClientException("invokeSync call timeout")
            ext = {
                "groupSeq": group_seq,
                "maxGroupNum": 10000,
            }
            if current_data_version is not None:
                ext["dataVersion"] = RemotingSerializable.to_json(current_data_version)
            request = RemotingCommand.create_request_command(
                RequestCode.GET_ALL_SUBSCRIPTIONGROUP_CONFIG, None)
            for k, v in ext.items():
                request.ext_fields[k] = str(v)
            response = client._invoke_sync(broker_addr, request, left)
            if response.code != ResponseCode.SUCCESS:
                raise MQBrokerException(response.code, response.remark or "")
            wrapper = (SubscriptionGroupWrapper.decode(response.body)
                       if response.body else SubscriptionGroupWrapper())
            table.update(wrapper.subscription_group_table)
            forbidden.update(wrapper.forbidden_table)
            new_version = wrapper.data_version
            if current_data_version is None:
                current_data_version = new_version
            group_seq += len(wrapper.subscription_group_table)

            total = response.ext_fields.get("totalGroupNum")
            if total is None:
                # Old broker: returns everything at once
                break
            total = int(total)
            if current_data_version != new_version:
                logger.warning("subscription group dataVersion changed, restart paging")
                current_data_version = new_version
                group_seq = 0
                table.clear()
                forbidden.clear()
                continue
            if group_seq >= total - 1:
                break

        result = SubscriptionGroupWrapper()
        result.subscription_group_table = table
        result.forbidden_table = forbidden
        result.data_version = current_data_version or {}
        return result

    def get_user_subscription_group(self, broker_addr: str,
                                    timeout_millis: Optional[int] = None) -> SubscriptionGroupWrapper:
        wrapper = self.get_all_subscription_group(broker_addr, timeout_millis)
        wrapper.subscription_group_table = {
            k: v for k, v in wrapper.subscription_group_table.items()
            if not MixAll.is_sys_consumer_group(k) and not MixAll.is_predefined_group(k)}
        return wrapper

    def delete_subscription_group(self, addr: str, group_name: str,
                                  remove_offset: bool = False) -> None:
        self._invoke_broker(addr, RequestCode.DELETE_SUBSCRIPTIONGROUP,
                            {"groupName": group_name,
                             "cleanOffset": "true" if remove_offset else "false"})

    # ---------------- Consumer / producer connections ----------------
    def examine_consumer_connection_info(self, consumer_group: str,
                                         broker_addr: Optional[str] = None) -> ConsumerConnection:
        addr = broker_addr or self._find_first_broker_addr(self._require_client())
        response = self._invoke_broker(addr, RequestCode.GET_CONSUMER_CONNECTION_LIST,
                                       {"consumerGroup": consumer_group})
        if not response.body:
            raise MQClientException("consumer group %s not online" % consumer_group)
        return ConsumerConnection.decode(response.body)

    def examine_consumer_connection(self, consumer_group: str,
                                    broker_addr: Optional[str] = None) -> ConsumerConnection:
        return self.examine_consumer_connection_info(consumer_group, broker_addr)

    def examine_producer_connection_info(self, producer_group: str,
                                         broker_addr: Optional[str] = None):
        addr = broker_addr or self._find_first_broker_addr(self._require_client())
        response = self._invoke_broker(addr, RequestCode.GET_PRODUCER_CONNECTION_LIST,
                                       {"producerGroup": producer_group})
        if not response.body:
            return ProducerConnection()
        return ProducerConnection.decode(response.body)

    def examine_consumer_running_info(self, consumer_group: str, client_id: str,
                                      jstack: bool = False,
                                      broker_addr: Optional[str] = None) -> ConsumerRunningInfo:
        addr = broker_addr or self._find_first_broker_addr(self._require_client())
        response = self._invoke_broker(
            addr, RequestCode.GET_CONSUMER_RUNNING_INFO,
            {"consumerGroup": consumer_group, "clientId": client_id,
             "jstackEnable": "true" if jstack else "false"})
        if not response.body:
            raise MQClientException("no running info for client %s" % client_id)
        return ConsumerRunningInfo.decode(response.body)

    def get_consumer_running_info(self, consumer_group: str, client_id: str,
                                  jstack: bool = False,
                                  broker_addr: Optional[str] = None) -> ConsumerRunningInfo:
        return self.examine_consumer_running_info(consumer_group, client_id, jstack,
                                                  broker_addr)

    def get_consumer_list_by_group(self, consumer_group: str,
                                   broker_addr: Optional[str] = None):
        client = self._require_client()
        addr = broker_addr or self._find_first_broker_addr(client)
        return client.get_consumer_list_by_group(consumer_group, addr=addr)

    def consume_message_directly(self, consumer_group: str, client_id: str, topic: str,
                                 msg_id: str,
                                 broker_addr: Optional[str] = None) -> ConsumeMessageDirectlyResult:
        pass
        if not consumer_group or not client_id:
            raise MQClientException("consumeMessageDirectly: consumerGroup/clientId required")
        from common.message_const import MessageConst
        msg = self.view_message(topic, msg_id)
        addr = broker_addr or msg.get_store_host_string()
        if not addr:
            raise MQClientException(
                "consumeMessageDirectly: cannot resolve broker address for message %s"
                % msg_id)
        out_msg_id = msg_id
        if (msg.get_property(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX) is not None
                and msg.get_offset_msg_id()):
            out_msg_id = msg.get_offset_msg_id()

        broker_name = msg.get_broker_name()
        if not broker_name:
            try:
                route = self._require_client().get_topic_route_data(topic)
            except Exception:  # noqa: BLE001 — brokerName is only supplementary info; missing it is not blocking
                route = None
            for bd in (route.get_broker_datas() if route else []):
                if addr in (bd.broker_addrs or {}).values():
                    broker_name = bd.broker_name
                    break

        header = ConsumeMessageDirectlyResultRequestHeader()
        header.consumer_group = consumer_group
        header.client_id = client_id
        header.msg_id = out_msg_id
        header.topic = topic
        header.broker_name = broker_name
        response = self._invoke_broker(addr, RequestCode.CONSUME_MESSAGE_DIRECTLY,
                                       ext_fields=header.to_ext_fields())
        if not response.body:
            raise MQClientException("no consume result for client %s" % client_id)
        return ConsumeMessageDirectlyResult.decode(response.body)

    # ---------------- Consumption stats ----------------
    def examine_consume_stats(self, broker_addr: str, consumer_group: str,
                              topic: Optional[str] = None,
                              topic_list: Optional[List[str]] = None) -> ConsumeStats:
        ext = {"consumerGroup": consumer_group}
        if topic:
            ext["topic"] = topic
        if topic_list:
            ext["topicList"] = ";".join(topic_list)
        response = self._invoke_broker(broker_addr, RequestCode.GET_CONSUME_STATS, ext)
        if not response.body:
            return ConsumeStats()
        return ConsumeStats.decode(response.body)

    def fetch_consume_stats_in_broker(self, broker_addr: str,
                                      is_order: bool = False,
                                      timeout_millis: Optional[int] = None):
        from remoting.protocol.body import ConsumeStatsList
        response = self._invoke_broker(
            broker_addr, RequestCode.GET_BROKER_CONSUME_STATS,
            {"isOrder": "true" if is_order else "false"},
            timeout_millis=timeout_millis)
        if not response.body:
            return ConsumeStatsList()
        return ConsumeStatsList.decode(response.body)

    def query_topic_consume_by_who(self, broker_addr: str, topic: str):
        response = self._invoke_broker(broker_addr, RequestCode.QUERY_TOPIC_CONSUME_BY_WHO,
                                       {"topic": topic})
        if not response.body:
            return set()
        obj = RemotingSerializable.decode_json(response.body)
        return set(obj.get("groupList", []))


    def examine_consume_stats_group(self, consumer_group: str,
                                    topic: Optional[str] = None) -> ConsumeStats:
        pass
        route = self.examine_topic_route(MixAll.get_retry_topic(consumer_group))
        result = ConsumeStats()
        for bd in route.broker_datas:
            addr = bd.select_broker_addr()
            if addr:
                part = self.examine_consume_stats(addr, consumer_group, topic)
                result.offset_table.update(part.offset_table)
                result.consume_tps += part.consume_tps
        if not result.offset_table:
            raise MQClientException("no consume stats for group %s" % consumer_group)
        return result

    def consumed(self, msg: MessageExt, group: str) -> bool:
        pass
        cstats = self.examine_consume_stats_group(group)
        ci = self.examine_broker_cluster_info()
        store_host = msg.get_store_host_string()
        for mq, wrapper in cstats.offset_table.items():
            if mq.topic == msg.topic and mq.queue_id == msg.queue_id:
                broker_addrs = ci.broker_addr_table.get(mq.broker_name)
                if broker_addrs:
                    addr = broker_addrs.get(MixAll.MASTER_ID)
                    if addr and store_host and addr == store_host:
                        if wrapper.consumer_offset > msg.queue_offset:
                            return True
        return False

    def message_track_detail(self, msg: MessageExt) -> List[MessageTrack]:
        pass
        result: List[MessageTrack] = []
        route = self.examine_topic_route(msg.topic)
        broker_addr = None
        for bd in route.broker_datas:
            broker_addr = bd.select_broker_addr()
            if broker_addr:
                break
        if broker_addr is None:
            return result
        groups = self.query_topic_consume_by_who(broker_addr, msg.topic)
        for group in sorted(groups):
            mt = MessageTrack(consumer_group=group)
            try:
                cc = self.examine_consumer_connection_info(group)
            except MQBrokerException as e:
                if e.response_code == ResponseCode.CONSUMER_NOT_ONLINE:
                    mt.track_type = TrackType.NOT_ONLINE
                mt.exception_desc = "CODE:%s DESC:%s" % (e.response_code, e.error_message)
                result.append(mt)
                continue
            except Exception as e:  # noqa: BLE001
                mt.exception_desc = str(e)
                result.append(mt)
                continue

            if cc.consume_type == "CONSUME_ACTIVELY":
                mt.track_type = TrackType.PULL
            elif cc.consume_type == "CONSUME_PASSIVELY":
                try:
                    if_consumed = self.consumed(msg, group)
                except (MQClientException, MQBrokerException) as e:
                    if e.response_code == ResponseCode.CONSUMER_NOT_ONLINE:
                        mt.track_type = TrackType.NOT_ONLINE
                        mt.exception_desc = ("CODE:%s DESC:%s"
                                             % (e.response_code, getattr(e, "error_message", e)))
                    elif e.response_code == ResponseCode.BROADCAST_CONSUMPTION:
                        mt.track_type = TrackType.CONSUME_BROADCASTING
                    result.append(mt)
                    continue
                except Exception as e:  # noqa: BLE001
                    mt.exception_desc = str(e)
                    result.append(mt)
                    continue

                if if_consumed:
                    mt.track_type = TrackType.CONSUMED
                    sub = cc.subscription_table.get(msg.topic)
                    if sub:
                        tags_set = set(sub.get("tagsSet") or [])
                        if tags_set and "*" not in tags_set and msg.get_tags() not in tags_set:
                            mt.track_type = TrackType.CONSUMED_BUT_FILTERED
                else:
                    mt.track_type = TrackType.NOT_CONSUME_YET
            result.append(mt)
        return result

    def query_topics_by_consumer_to_broker(self, broker_addr: str, group: str) -> TopicList:
        pass
        response = self._invoke_broker(broker_addr, RequestCode.QUERY_TOPICS_BY_CONSUMER,
                                       {"group": group})
        if not response.body:
            return TopicList()
        return TopicList.decode(response.body)

    def query_topics_by_consumer(self, group: str) -> TopicList:
        pass
        route = self.examine_topic_route(MixAll.get_retry_topic(group))
        result = TopicList()
        seen: Set[str] = set()
        for bd in route.get_broker_datas():
            addr = bd.select_broker_addr()
            if not addr:
                continue
            for topic in self.query_topics_by_consumer_to_broker(addr, group).get_topic_list():
                if topic not in seen:
                    seen.add(topic)
                    result.topic_list.append(topic)
        return result

    def query_subscription(self, broker_addr: str, group: str, topic: str) -> Optional[dict]:
        response = self._invoke_broker(broker_addr, RequestCode.QUERY_SUBSCRIPTION_BY_CONSUMER,
                                       {"group": group, "topic": topic})
        if not response.body:
            return None
        return fastjson_loads(response.body.decode("utf-8"))

    def get_consume_status(self, broker_addr: str, topic: str, group: str,
                           client_addr: str = "") -> Dict[str, dict]:
        response = self._invoke_broker(
            broker_addr, RequestCode.INVOKE_BROKER_TO_GET_CONSUMER_STATUS,
            {"topic": topic, "group": group, "clientAddr": client_addr})
        if not response.body:
            return {}
        obj = fastjson_loads(response.body.decode("utf-8"))
        return obj.get("consumerTable", {})

    def clone_group_offset(self, broker_addr: str, src_group: str, dest_group: str,
                           topic: str, is_offline: bool = False) -> None:
        self._invoke_broker(broker_addr, RequestCode.CLONE_GROUP_OFFSET,
                            {"srcGroup": src_group, "destGroup": dest_group,
                             "topic": topic,
                             "offline": "true" if is_offline else "false"})

    # ---------------- Offset management ----------------
    def max_offset(self, mq: MessageQueue) -> int:
        return self._require_client().get_max_offset(mq)

    def min_offset(self, mq: MessageQueue) -> int:
        return self._require_client().get_min_offset(mq)

    def search_offset(self, mq: MessageQueue, timestamp: int) -> int:
        pass
        return self._require_client().search_offset_by_timestamp(
            mq, timestamp, boundary_type=BoundaryType.LOWER)

    def search_lower_boundary_offset(self, mq: MessageQueue, timestamp: int) -> int:
        pass
        return self._require_client().search_offset_by_timestamp(
            mq, timestamp, boundary_type=BoundaryType.LOWER)

    def search_upper_boundary_offset(self, mq: MessageQueue, timestamp: int) -> int:
        pass
        return self._require_client().search_offset_by_timestamp(
            mq, timestamp, boundary_type=BoundaryType.UPPER)

    def earliest_msg_store_time(self, mq: MessageQueue) -> int:
        client = self._require_client()
        addr = client._publish_addr_in_admin(mq)
        response = self._invoke_broker(addr, RequestCode.GET_EARLIEST_MSG_STORETIME,
                                       {"topic": mq.topic, "queueId": mq.queue_id,
                                        "brokerName": mq.broker_name})
        return int(response.ext_fields.get("timestamp", 0) or 0)

    def examine_consumer_offset(self, consumer_group: str, mq: MessageQueue) -> Optional[int]:
        return self._require_client().query_consumer_offset(consumer_group, mq)

    def update_consumer_offset(self, consumer_group: str, mq: MessageQueue, offset: int) -> None:
        self._require_client().update_consumer_offset(consumer_group, mq, offset)

    def update_consumer_offset_to_broker(self, broker_addr: str, consumer_group: str,
                                         mq: MessageQueue, offset: int) -> None:
        self._require_client().update_consumer_offset(consumer_group, mq, offset,
                                                      addr=broker_addr)

    def reset_offset_by_timestamp(self, topic: str, group: str, timestamp: int,
                                  is_force: bool = True,
                                  cluster_name: Optional[str] = None,
                                  is_cpp: bool = False) -> Dict[MessageQueue, int]:
        pass
        route_topic = topic
        if topic and (MixAll.is_lmq(topic) or
                      topic == MixAll.SYSTEM_TOPIC_PREFIX + "wheel_timer") and cluster_name:
            route_topic = cluster_name
        route = self.examine_topic_route(route_topic)
        all_offsets: Dict[MessageQueue, int] = {}
        for bd in route.get_broker_datas():
            addr = bd.select_broker_addr()
            if not addr:
                continue
            all_offsets.update(
                self._invoke_broker_reset_offset(addr, topic, group, timestamp,
                                                 is_force, is_cpp))
        if not all_offsets:
            raise MQClientException("reset offset failed, no broker returned offset table")
        return all_offsets

    def _invoke_broker_reset_offset(self, broker_addr: str, topic: str, group: str,
                                    timestamp: int, is_force: bool, is_cpp: bool,
                                    queue_id: Optional[int] = None,
                                    offset: Optional[int] = None) -> Dict[MessageQueue, int]:
        pass
        client = self._require_client()
        request = RemotingCommand.create_request_command(
            RequestCode.INVOKE_BROKER_TO_RESET_OFFSET, None)
        ext = {
            "topic": topic,
            "group": group,
            "timestamp": timestamp,
            "isForce": "true" if is_force else "false",
            "offset": -1 if offset is None else offset,
        }
        if queue_id is not None:
            ext["queueId"] = queue_id
        for k, v in ext.items():
            request.ext_fields[k] = str(v)
        if is_cpp:
            request.language = LanguageCode.CPP
        response = client._invoke_sync(
            MixAll.broker_vip_channel(self.vip_channel_enabled, broker_addr),
            request, self.timeout_millis)
        if response.code != ResponseCode.SUCCESS:
            raise MQClientException(response.remark or "reset offset failed", response.code)
        if not response.body:
            return {}
        return ResetOffsetBody.decode(response.body).offset_table

    def reset_offset_by_queue_id(self, broker_addr: str, consumer_group: str, topic: str,
                                 queue_id: int, reset_offset: int) -> Dict[MessageQueue, int]:
        pass
        self.update_consumer_offset_to_broker(
            broker_addr, consumer_group, MessageQueue(topic, "", queue_id), reset_offset)
        return self._invoke_broker_reset_offset(
            broker_addr, topic, consumer_group, 0, False, False,
            queue_id=queue_id, offset=reset_offset)

    def reset_offset_new(self, consumer_group: str, topic: str, timestamp: int) -> None:
        pass
        try:
            self.reset_offset_by_timestamp(topic, consumer_group, timestamp, True)
        except MQClientException as e:
            if e.response_code == ResponseCode.CONSUMER_NOT_ONLINE:
                self.reset_offset_by_timestamp_old(consumer_group, topic, timestamp, True)
                return
            raise

    def reset_offset_by_timestamp_old(self, consumer_group: str, topic: str, timestamp: int,
                                      force: bool = True) -> Dict[MessageQueue, int]:
        pass
        client = self._require_client()
        route = self.examine_topic_route(topic)
        result: Dict[MessageQueue, int] = {}
        for bd in route.get_broker_datas():
            addr = bd.select_broker_addr()
            if not addr:
                continue
            for qd in route.queue_datas:
                if qd.broker_name != bd.broker_name:
                    continue
                for queue_id in range(qd.read_queue_nums):
                    mq = MessageQueue(topic, bd.broker_name, queue_id)
                    try:
                        consumer_offset = client.query_consumer_offset(
                            consumer_group, mq, addr=addr) or 0
                    except Exception:  # noqa: BLE001
                        consumer_offset = 0
                    if timestamp == -1:
                        reset_offset = client.get_max_offset(mq, addr=addr)
                    else:
                        reset_offset = client.search_offset_by_timestamp(
                            mq, timestamp, addr=addr)
                    if force or reset_offset <= consumer_offset:
                        client.update_consumer_offset(consumer_group, mq, reset_offset,
                                                      addr=addr)
                        result[mq] = reset_offset
        return result

    # ---------------- Message query ----------------
    def query_message(self, topic: str, key: str, max_num: int, begin: int, end: int):
        pass
        from common.message_const import MessageConst
        return self._require_client().query_message_all_brokers(
            topic, key, max_num, begin, end,
            index_type=MessageConst.INDEX_KEY_TYPE, uniq_key=False)

    def query_message_by_uniq_key(self, topic: str, uniq_key: str) -> Optional[MessageExt]:
        pass
        from common.message_const import MessageConst
        messages = self._require_client().query_message_all_brokers(
            topic, uniq_key, 32, 0, int(time.time() * 1000) + 60 * 60 * 1000,
            index_type=MessageConst.INDEX_UNIQUE_TYPE, uniq_key=True)
        return messages[0] if messages else None

    def query_message_by_key(self, topic: str, key: str, max_num: int = 32) -> List[MessageExt]:
        """Query by the normal KEYS index (corresponds to the NORMAL mode of the queryMsgByKey tool)."""
        from common.message_const import MessageConst
        return self._require_client().query_message_all_brokers(
            topic, key, max_num, 0, int(time.time() * 1000) + 60 * 60 * 1000,
            index_type=MessageConst.INDEX_KEY_TYPE, uniq_key=False)

    def view_message(self, topic: str, msg_id: str) -> MessageExt:
        pass
        by_id_error: Optional[Exception] = None
        try:
            ip, port, offset = decode_message_id(msg_id)
            if not 0 < port <= 65535:
                raise MQClientException("not a valid offset msgId: %s" % msg_id)
            response = self._invoke_broker("%s:%d" % (ip, port),
                                           RequestCode.VIEW_MESSAGE_BY_ID,
                                           {"topic": topic, "offset": offset})
            if response.body:
                return decode_message(response.body)
            by_id_error = MQBrokerException(ResponseCode.NO_MESSAGE,
                                            "message not found: %s" % msg_id)
        except Exception as e:
            by_id_error = e

        found = self.query_message_by_uniq_key(topic, msg_id)
        if found is not None:
            return found
        raise MQClientException(
            "viewMessage failed: neither offset msgId nor uniq key matched message %s of %s"
            % (msg_id, topic), ResponseCode.NO_MESSAGE, by_id_error)

    def query_consume_queue(self, broker_addr: str, topic: str, queue_id: int,
                            index: int, count: int = 32,
                            consumer_group: str = "") -> QueryConsumeQueueResponseBody:
        response = self._invoke_broker(
            broker_addr, RequestCode.QUERY_CONSUME_QUEUE,
            {"topic": topic, "queueId": queue_id, "index": index, "count": count,
             "consumerGroup": consumer_group})
        if not response.body:
            return QueryConsumeQueueResponseBody()
        return QueryConsumeQueueResponseBody.decode(response.body)

    def create_and_update_topic_config_list(self, broker_addr: str,
                                            configs: List[TopicConfig]) -> None:
        pass
        if not configs:
            raise MQClientException("createAndUpdateTopicConfigList: empty topicConfigList")
        body = RemotingSerializable.encode(
            {"topicConfigList": [c.to_dict() for c in configs]})
        self._invoke_broker(broker_addr, RequestCode.UPDATE_AND_CREATE_TOPIC_LIST,
                            body=body)

    def create_and_update_subscription_group_config_list(
            self, broker_addr: str, configs: List[SubscriptionGroupConfig]) -> None:
        pass
        if not configs:
            raise MQClientException(
                "createAndUpdateSubscriptionGroupConfigList: empty groupConfigList")
        body = RemotingSerializable.encode(
            {"groupConfigList": [c.to_dict() for c in configs]})
        self._invoke_broker(broker_addr, RequestCode.UPDATE_AND_CREATE_SUBSCRIPTIONGROUP_LIST,
                            body=body)

    def create_static_topic(self, broker_addr: str, default_topic: str,
                            config: TopicConfig, mapping_detail: Dict,
                            force: bool = False) -> None:
        pass
        if config is None:
            raise MQClientException("createStaticTopic: config must not be None")
        if not mapping_detail:
            raise MQClientException("createStaticTopic: mappingDetail must not be empty")
        header = {
            "topic": config.topic_name,
            "defaultTopic": default_topic,
            "readQueueNums": config.read_queue_nums,
            "writeQueueNums": config.write_queue_nums,
            "perm": config.perm,
            "topicFilterType": config.topic_filter_type or "SINGLE_TAG",
            "topicSysFlag": config.topic_sys_flag,
            "order": str(config.order).lower(),
            "force": str(force).lower(),
        }
        self._invoke_broker(broker_addr, RequestCode.UPDATE_AND_CREATE_STATIC_TOPIC,
                            ext_fields=header,
                            body=RemotingSerializable.encode(mapping_detail))

    def update_and_get_group_read_forbidden(self, broker_addr: str, group: str,
                                            topic: str,
                                            readable: Optional[bool] = None) -> Dict:
        pass
        if not group or not topic:
            raise MQClientException("updateAndGetGroupReadForbidden: group/topic required")
        ext = {"group": group, "topic": topic}
        if readable is not None:
            ext["readable"] = str(readable).lower()
        response = self._invoke_broker(broker_addr, RequestCode.UPDATE_AND_GET_GROUP_FORBIDDEN,
                                       ext_fields=ext)
        if not response.body:
            raise MQClientException("updateAndGetGroupReadForbidden: empty response body")
        return fastjson_loads(response.body.decode("utf-8"))

    def resume_check_half_message(self, broker_addr: str, topic: str,
                                  msg_id: str = "") -> bool:
        pass
        if not topic:
            raise MQClientException("resumeCheckHalfMessage: topic required")
        ext = {"topic": topic}
        if msg_id:
            ext["msgId"] = msg_id
        client = self._require_client()
        addr = MixAll.broker_vip_channel(self.vip_channel_enabled, broker_addr)
        request = RemotingCommand.create_request_command(RequestCode.RESUME_CHECK_HALF_MESSAGE,
                                                         None)
        request.ext_fields.update({k: str(v) for k, v in ext.items()})
        response = client._invoke_sync(addr, request, self.timeout_millis)
        return response.code == ResponseCode.SUCCESS

    def create_or_update_order_conf(self, key: str, value: str,
                                    is_cluster: bool = False) -> None:
        pass
        if not key or not value:
            raise MQClientException("createOrUpdateOrderConf: key/value required")
        if is_cluster:
            self.put_kv_config(NAMESPACE_ORDER_TOPIC_CONFIG, key, value)
            return
        try:
            old_confs = self.get_kv_config(NAMESPACE_ORDER_TOPIC_CONFIG, key) or ""
        except Exception:
            old_confs = ""
        entries: Dict[str, str] = {}
        for entry in old_confs.split(";"):
            entry = entry.strip()
            if not entry:
                continue
            entry_key = entry.split(":", 1)[0]
            entries[entry_key] = entry
        new_key = value.split(":", 1)[0]
        if not new_key:
            raise MQClientException("createOrUpdateOrderConf: value must start with a key")
        entries[new_key] = value
        self.put_kv_config(NAMESPACE_ORDER_TOPIC_CONFIG, key,
                           ";".join(entries.values()))

    # ---------------- Operational cleanup ----------------
    def clean_expired_consumer_queue(self, broker_addr: str, time_hours: int) -> None:
        pass
        self._invoke_broker(broker_addr, RequestCode.CLEAN_EXPIRED_CONSUMEQUEUE,
                            {"time": time_hours})

    def clean_expired_consumer_queue_by_addr(self, addrs: List[str],
                                             time_hours: int) -> List[str]:
        pass
        failed = []
        for addr in addrs:
            try:
                self.clean_expired_consumer_queue(addr, time_hours)
            except Exception:  # noqa: BLE001
                failed.append(addr)
        return failed

    def delete_expired_commit_log(self, broker_addr: str, time_hours: int) -> None:
        pass
        self._invoke_broker(broker_addr, RequestCode.DELETE_EXPIRED_COMMITLOG,
                            {"time": time_hours})

    def delete_expired_commit_log_by_addr(self, addrs: List[str],
                                          time_hours: int) -> List[str]:
        pass
        failed = []
        for addr in addrs:
            try:
                self.delete_expired_commit_log(addr, time_hours)
            except Exception:  # noqa: BLE001
                failed.append(addr)
        return failed

    def clean_unused_topic_by_addr(self, broker_addr: str) -> None:
        pass
        self._invoke_broker(broker_addr, RequestCode.CLEAN_UNUSED_TOPIC)

    def query_consume_time_span(self, topic: str, group: str) -> List[Dict]:
        pass
        route = self.examine_topic_route(topic)
        spans: List[Dict] = []
        for bd in route.broker_datas:
            addr = bd.select_broker_addr()
            if not addr:
                continue
            response = self._invoke_broker(addr, RequestCode.QUERY_CONSUME_TIME_SPAN,
                                           {"topic": topic, "group": group})
            if not response.body:
                continue
            body = QueryConsumeTimeSpanBody.decode(response.body)
            spans.extend(body.consume_time_span_set)
        return spans

    # ---------------- NameServer config (318/319) ----------------
    def update_name_server_config(self, properties: Dict[str, str],
                                  timeout_millis: Optional[int] = None) -> None:
        pass
        text = MixAll.properties2_string(properties or {})
        if not text:
            return
        client = self._require_client()
        request = RemotingCommand.create_request_command(RequestCode.UPDATE_NAMESRV_CONFIG,
                                                         None)
        request.body = text.encode("utf-8")
        err_response = None
        for ns_addr in client.name_server_addrs:
            response = client._invoke_sync(ns_addr, request,
                                           timeout_millis or self.timeout_millis)
            if response.code != ResponseCode.SUCCESS:
                err_response = response
        if err_response is not None:
            raise MQClientException(err_response.remark or "update name server config failed",
                                    err_response.code)

    def get_name_server_config(self, namesrv_addrs: Optional[List[str]] = None,
                               timeout_millis: Optional[int] = None) -> Dict[str, Dict[str, str]]:
        pass
        client = self._require_client()
        targets = namesrv_addrs or list(client.name_server_addrs)
        result: Dict[str, Dict[str, str]] = {}
        last_exc: Optional[Exception] = None
        for ns_addr in targets:
            request = RemotingCommand.create_request_command(RequestCode.GET_NAMESRV_CONFIG,
                                                             None)
            try:
                response = client._invoke_sync(ns_addr, request,
                                               timeout_millis or self.timeout_millis)
            except Exception as e:
                last_exc = e
                continue
            if response.code != ResponseCode.SUCCESS:
                last_exc = MQClientException(response.remark or "get name server config failed",
                                             response.code)
                continue
            result[ns_addr] = MixAll.string2_properties(
                (response.body or b"").decode("utf-8"))
        if not result and last_exc is not None:
            raise last_exc
        return result

    def set_message_request_mode(self, broker_addr: str, topic: str,
                                 consumer_group: str, mode: str,
                                 pop_share_queue_num: int = 0) -> None:
        pass
        ext = {"topic": topic, "consumerGroup": consumer_group, "mode": mode}
        if pop_share_queue_num > 0:
            ext["popShareQueueNum"] = pop_share_queue_num
        self._invoke_broker(broker_addr, RequestCode.SET_MESSAGE_REQUEST_MODE,
                            ext_fields=ext)


# ---------------------------------------------------------------- Helpers
def _perm_is_valid(value) -> bool:
    pass
    try:
        return PermName.is_valid(value)
    except (TypeError, ValueError):
        return False


__all__ = ["DefaultMQAdminExt", "DEFAULT_TIMEOUT", "ClusterInfo", "TopicList", "KVTable",
           "ConsumerConnection", "ConsumerRunningInfo", "ProducerConnection",
           "TopicRouteData", "QueueData", "BrokerData", "PullResult", "PullStatus",
           "TopicConfig", "TopicConfigSerializeWrapper", "TopicStatsTable",
           "ConsumeStats", "SubscriptionGroupConfig", "SubscriptionGroupWrapper",
           "QueryConsumeQueueResponseBody"]
