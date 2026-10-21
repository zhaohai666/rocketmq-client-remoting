# -*- coding: utf-8 -*-
from __future__ import annotations

import threading
import time
from concurrent.futures import ThreadPoolExecutor, wait
from typing import Dict, List, Optional, Set, Union

from common.boundary_type import BoundaryType
from common.message import MessageExt, MessageQueue
from common.message_decoder import decode_message_id, decode_message, decode_messages
from common.mix_all import MixAll
from common.sysflag import PermName
from common.topic_config import TopicConfig
from rocketmq_logging import get_logger
from remoting.exception import RemotingException
from remoting.protocol.body import (ClusterInfo, ConsumeMessageDirectlyResult,
                                      ConsumerConnection,
                                      ConsumerRunningInfo, KVTable,
                                      ProducerConnection, QueryConsumeTimeSpanBody,
                                      ResetOffsetBody, TopicList)
from remoting.protocol.headers import (
    AddBrokerRequestHeader, CheckRocksdbCqWriteProgressRequestHeader,
    CleanControllerBrokerDataRequestHeader, ConsumeMessageDirectlyResultRequestHeader,
    CreateTopicListRequestHeader, ExportRocksDBConfigToJsonRequestHeader,
    GetLiteClientInfoRequestHeader, GetLiteGroupInfoRequestHeader,
    GetLiteTopicInfoRequestHeader, GetMetaDataResponseHeader,
    GetParentTopicInfoRequestHeader, RemoveBrokerRequestHeader,
    TriggerLiteDispatchRequestHeader)
from remoting.protocol.admin_body import (
    AclInfo, AdminToolResult, AdminToolsResultCodeEnum, BrokerOperatorResult,
    BrokerReplicasInfo, CheckRocksdbCqWriteResult, ConsumeStats, OffsetWrapper,
    GetBrokerLiteInfoResponseBody, GetLiteClientInfoResponseBody,
    GetLiteGroupInfoResponseBody, GetLiteTopicInfoResponseBody,
    GetParentTopicInfoResponseBody, QueryConsumeQueueResponseBody,
    TopicConfigSerializeWrapper, TopicStatsTable, UserInfo)
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

# Default timeout of the admin RPC families that take the admin-wide budget
# (the admin implementation's own default, not the facade constructor's 5000).
DEFAULT_TIMEOUT = 20000

NAMESPACE_ORDER_TOPIC_CONFIG = "ORDER_TOPIC_CONFIG"

# Hardcoded budgets of the admin RPC families that do not follow the admin default:
# the container add/remove pair and the controller-leader routed reads.
ADMIN_CONTAINER_TIMEOUT = 20000
CONTROLLER_RPC_TIMEOUT = 3000

# The single ext field of the commitlog read-ahead request (no header class exists for
# it, the key is the protocol constant).
READ_AHEAD_MODE_KEY = "READ_AHEAD_MODE"



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
        self.max_page_size_in_get_metadata = int(
            kwargs.get("max_page_size_in_get_metadata", 2000))
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

    def set_max_page_size_in_get_metadata(self, page_size: int) -> None:
        """Page size of the paged metadata reads (topic configs and subscription groups)."""
        self.max_page_size_in_get_metadata = max(1, int(page_size))

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

    # ---------------- Fan-out plumbing ----------------
    @staticmethod
    def _fan_out(targets, task, timeout_millis: int) -> None:
        """Run one task per target concurrently and wait for them like a counted latch:
        the wait is bounded by the timeout, and tasks that miss it still keep running and
        contributing (their results are shared containers). Task exceptions are the
        tasks' own business -- each one handles and logs them instead of propagating."""
        if not targets:
            return
        executor = ThreadPoolExecutor(max_workers=len(targets),
                                      thread_name_prefix="mq-admin-fanout")
        try:
            futures = [executor.submit(task, target) for target in targets]
            wait(futures, timeout=max(0.0, timeout_millis / 1000.0))
        finally:
            executor.shutdown(wait=False)

    def _examine_topic_route_info(self, topic: str) -> Optional[TopicRouteData]:
        """Route fetch for the fan-out tools: None when the topic has no route, transport
        failures raised (the tool envelope maps them to the remote-error code)."""
        client = self._require_client()
        if not client.update_topic_route_info_from_name_server(topic):
            return None
        with client.topic_route_lock:
            return client.topic_route_table.get(topic)

    def _admin_tool_execute(self, handler):
        """Run a fan-out tool body and turn every failure into the tool envelope instead
        of an exception."""
        try:
            return handler()
        except RemotingException as e:
            logger.warning("admin tool remote error: %s", e)
            return AdminToolResult.failure(AdminToolsResultCodeEnum.REMOTING_ERROR,
                                           str(e))
        except MQClientException as e:
            if e.response_code == ResponseCode.TOPIC_NOT_EXIST:
                return AdminToolResult.failure(
                    AdminToolsResultCodeEnum.TOPIC_ROUTE_INFO_NOT_EXIST, str(e))
            return AdminToolResult.failure(AdminToolsResultCodeEnum.MQ_CLIENT_ERROR,
                                           str(e))
        except InterruptedError as e:
            return AdminToolResult.failure(AdminToolsResultCodeEnum.INTERRUPT_ERROR,
                                           str(e))
        except Exception as e:  # noqa: BLE001
            return AdminToolResult.failure(AdminToolsResultCodeEnum.MQ_BROKER_ERROR,
                                           str(e))

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
        client = self._require_client()
        timeout = timeout_millis or self.timeout_millis
        broker_addr = MixAll.broker_vip_channel(self.vip_channel_enabled, broker_addr)
        current_data_version: Optional[dict] = None
        topic_seq = 0
        table: Dict[str, TopicConfig] = {}
        begin = time.monotonic()
        while True:
            left = timeout - int((time.monotonic() - begin) * 1000)
            if left < 0:
                raise MQClientException("invokeSync call timeout")
            data_version = ""
            if current_data_version is not None:
                data_version = RemotingSerializable.to_json(current_data_version)
            ext = {
                "topicSeq": topic_seq,
                "maxTopicNum": self.max_page_size_in_get_metadata,
                "dataVersion": data_version,
            }
            request = RemotingCommand.create_request_command(
                RequestCode.GET_ALL_TOPIC_CONFIG, None)
            for k, v in ext.items():
                request.ext_fields[k] = str(v)
            response = client._invoke_sync(broker_addr, request, left)
            if response.code != ResponseCode.SUCCESS:
                raise MQBrokerException(response.code, response.remark or "")
            wrapper = (TopicConfigSerializeWrapper.decode(response.body)
                       if response.body else TopicConfigSerializeWrapper())
            table.update(wrapper.topic_config_table)
            new_version = wrapper.data_version
            if current_data_version is None:
                current_data_version = new_version
            topic_seq += len(wrapper.topic_config_table)

            total = response.ext_fields.get("totalTopicNum")
            if total is None:
                # Old broker: returns everything at once
                break
            total = int(total)
            if current_data_version != new_version:
                logger.warning("topic config dataVersion changed, restart paging")
                current_data_version = new_version
                topic_seq = 0
                table.clear()
                continue
            if topic_seq >= total - 1:
                break

        result = TopicConfigSerializeWrapper()
        result.topic_config_table = table
        result.data_version = current_data_version or {}
        return result

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
                "maxGroupNum": self.max_page_size_in_get_metadata,
                "dataVersion": (RemotingSerializable.to_json(current_data_version)
                                if current_data_version is not None else ""),
            }
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
            except Exception:  # noqa: BLE001 - brokerName is only supplementary info; missing it is not blocking
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
            result.append(self._track_one_group(msg, group))
        return result

    def _track_one_group(self, msg: MessageExt, group: str) -> MessageTrack:
        """The per-group verdict shared by the serial and the concurrent message track."""
        mt = MessageTrack(consumer_group=group)
        try:
            cc = self.examine_consumer_connection_info(group)
        except MQBrokerException as e:
            if e.response_code == ResponseCode.CONSUMER_NOT_ONLINE:
                mt.track_type = TrackType.NOT_ONLINE
            mt.exception_desc = "CODE:%s DESC:%s" % (e.response_code, e.error_message)
            return mt
        except Exception as e:  # noqa: BLE001
            mt.exception_desc = str(e)
            return mt

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
                return mt
            except Exception as e:  # noqa: BLE001
                mt.exception_desc = str(e)
                return mt

            if if_consumed:
                mt.track_type = TrackType.CONSUMED
                sub = cc.subscription_table.get(msg.topic)
                if sub:
                    tags_set = set(sub.get("tagsSet") or [])
                    if tags_set and "*" not in tags_set and msg.get_tags() not in tags_set:
                        mt.track_type = TrackType.CONSUMED_BUT_FILTERED
            else:
                mt.track_type = TrackType.NOT_CONSUME_YET
        return mt

    def message_track_detail_concurrent(self, msg: MessageExt) -> List[MessageTrack]:
        """Track every consumer group of the message topic in parallel; the per-group
        verdicts are the ones of the serial form, failures encoded into the track."""
        result: List[MessageTrack] = []
        route = self.examine_topic_route(msg.topic)
        broker_addr = None
        for bd in route.broker_datas:
            broker_addr = bd.select_broker_addr()
            if broker_addr:
                break
        if broker_addr is None:
            return result
        groups = sorted(self.query_topic_consume_by_who(broker_addr, msg.topic))
        lock = threading.Lock()

        def _track(group: str) -> None:
            track = self._track_one_group(msg, group)
            with lock:
                result.append(track)

        self._fan_out(groups, _track, self.timeout_millis)
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
        client = self._require_client()
        body = RemotingSerializable.encode(
            {"topicConfigList": [c.to_dict() for c in configs]})
        request = RemotingCommand.create_request_command(
            RequestCode.UPDATE_AND_CREATE_TOPIC_LIST, CreateTopicListRequestHeader())
        request.body = body
        response = client._invoke_sync(
            MixAll.broker_vip_channel(self.vip_channel_enabled, broker_addr),
            request, self.timeout_millis)
        if response.code != ResponseCode.SUCCESS:
            raise MQClientException(response.remark or
                                    "createAndUpdateTopicConfigList failed",
                                    response.code)

    def create_and_update_subscription_group_config_list(
            self, broker_addr: str, configs: List[SubscriptionGroupConfig]) -> None:
        client = self._require_client()
        body = RemotingSerializable.encode(
            {"groupConfigList": [c.to_dict() for c in configs]})
        # No header at all: this request carries no extFields on the wire.
        request = RemotingCommand.create_request_command(
            RequestCode.UPDATE_AND_CREATE_SUBSCRIPTIONGROUP_LIST, None)
        request.body = body
        response = client._invoke_sync(
            MixAll.broker_vip_channel(self.vip_channel_enabled, broker_addr),
            request, self.timeout_millis)
        if response.code != ResponseCode.SUCCESS:
            raise MQClientException(
                response.remark or "createAndUpdateSubscriptionGroupConfigList failed",
                response.code)

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

    # ---------------- Broker container ----------------
    def add_broker_to_container(self, broker_container_addr: str,
                                broker_config_path: str) -> None:
        header = AddBrokerRequestHeader()
        header.config_path = broker_config_path
        self._invoke_broker(broker_container_addr, RequestCode.ADD_BROKER,
                            ext_fields=header.to_ext_fields(),
                            timeout_millis=ADMIN_CONTAINER_TIMEOUT, vip=False)

    def remove_broker_from_container(self, broker_container_addr: str,
                                     cluster_name: str, broker_name: str,
                                     broker_id: int) -> None:
        header = RemoveBrokerRequestHeader()
        header.broker_cluster_name = cluster_name
        header.broker_name = broker_name
        header.broker_id = broker_id
        self._invoke_broker(broker_container_addr, RequestCode.REMOVE_BROKER,
                            ext_fields=header.to_ext_fields(),
                            timeout_millis=ADMIN_CONTAINER_TIMEOUT, vip=False)

    # ---------------- Controller ----------------
    def get_controller_meta_data(self, controller_addr: str) -> GetMetaDataResponseHeader:
        client = self._require_client()
        request = RemotingCommand.create_request_command(
            RequestCode.CONTROLLER_GET_METADATA_INFO, None)
        response = client._invoke_sync(controller_addr, request,
                                       CONTROLLER_RPC_TIMEOUT)
        if response.code != ResponseCode.SUCCESS:
            raise MQBrokerException(response.code, response.remark or "")
        header = GetMetaDataResponseHeader()
        header.from_ext_fields(response.ext_fields)
        return header

    def _controller_leader_address(self, controller_addr: str) -> str:
        return self.get_controller_meta_data(controller_addr).controller_leader_address

    def get_in_sync_state_data(self, controller_address: str,
                               brokers: List[str]) -> BrokerReplicasInfo:
        """Two-phase read: the meta data call names the leader, then the sync state
        request itself goes to the leader address."""
        client = self._require_client()
        leader_address = self._controller_leader_address(controller_address)
        request = RemotingCommand.create_request_command(
            RequestCode.CONTROLLER_GET_SYNC_STATE_DATA, None)
        request.body = RemotingSerializable.encode(list(brokers))
        response = client._invoke_sync(leader_address, request, CONTROLLER_RPC_TIMEOUT)
        if response.code != ResponseCode.SUCCESS:
            raise MQBrokerException(response.code, response.remark or "")
        return BrokerReplicasInfo.decode(response.body)

    def clean_controller_broker_data(self, controller_addr: str, cluster_name: str,
                                     broker_name: str,
                                     broker_controller_ids_to_clean: Optional[str] = None,
                                     is_clean_living_broker: bool = False) -> None:
        client = self._require_client()
        header = CleanControllerBrokerDataRequestHeader(
            cluster_name=cluster_name, broker_name=broker_name,
            broker_controller_ids_to_clean=broker_controller_ids_to_clean,
            is_clean_living_broker=is_clean_living_broker)
        leader_address = self._controller_leader_address(controller_addr)
        request = RemotingCommand.create_request_command(
            RequestCode.CLEAN_BROKER_DATA, header)
        for k, v in header.to_ext_fields().items():
            request.ext_fields[k] = str(v)
        response = client._invoke_sync(leader_address, request, CONTROLLER_RPC_TIMEOUT)
        if response.code != ResponseCode.SUCCESS:
            raise MQBrokerException(response.code, response.remark or "")

    def get_controller_config(self, controller_servers: Optional[List[str]] = None,
                              timeout_millis: Optional[int] = None) -> Optional[Dict[str, Dict[str, str]]]:
        client = self._require_client()
        targets = controller_servers if controller_servers else list(client.name_server_addrs)
        if not targets:
            return None
        request = RemotingCommand.create_request_command(
            RequestCode.GET_CONTROLLER_CONFIG, None)
        config_map: Dict[str, Dict[str, str]] = {}
        for controller in targets:
            response = client._invoke_sync(controller, request,
                                           timeout_millis or self.timeout_millis)
            if response.code != ResponseCode.SUCCESS:
                raise MQClientException(response.remark or "get controller config failed",
                                        response.code)
            config_map[controller] = MixAll.string2_properties(
                (response.body or b"").decode("utf-8"))
        return config_map

    def update_controller_config(self, properties: Dict[str, str],
                                 controllers: Optional[List[str]] = None) -> None:
        client = self._require_client()
        text = MixAll.properties2_string(properties or {})
        if not text or not controllers:
            return
        request = RemotingCommand.create_request_command(
            RequestCode.UPDATE_CONTROLLER_CONFIG, None)
        request.body = text.encode("utf-8")
        err_response = None
        for controller in controllers:
            response = client._invoke_sync(controller, request, self.timeout_millis)
            if response.code != ResponseCode.SUCCESS:
                err_response = response
        if err_response is not None:
            raise MQClientException(
                err_response.remark or "update controller config failed",
                err_response.code)

    # ---------------- Cold data flow control ----------------
    def update_cold_data_flow_ctr_group_config(self, broker_addr: str,
                                               properties: Dict[str, str]) -> None:
        text = MixAll.properties2_string(properties or {})
        if not text:
            return
        self._invoke_broker(broker_addr, RequestCode.UPDATE_COLD_DATA_FLOW_CTR_CONFIG,
                            body=text.encode("utf-8"))

    def remove_cold_data_flow_ctr_group_config(self, broker_addr: str,
                                               consumer_group: str) -> None:
        if not consumer_group:
            return
        # The group travels as a plain string body, not as JSON.
        self._invoke_broker(broker_addr, RequestCode.REMOVE_COLD_DATA_FLOW_CTR_CONFIG,
                            body=consumer_group.encode("utf-8"))

    def get_cold_data_flow_ctr_info(self, broker_addr: str) -> Optional[str]:
        response = self._invoke_broker(broker_addr,
                                       RequestCode.GET_COLD_DATA_FLOW_CTR_INFO,
                                       vip=False)
        body = response.body
        if body:
            return body.decode("utf-8")
        return None

    # ---------------- CommitLog read ahead ----------------
    def set_commit_log_read_ahead_mode(self, broker_addr: str,
                                       mode: str) -> Optional[str]:
        client = self._require_client()
        # Built by hand: this request has no header class, and its single ext field is
        # the protocol constant key.
        request = RemotingCommand.create_request_command(
            RequestCode.SET_COMMITLOG_READ_MODE, None)
        request.ext_fields[READ_AHEAD_MODE_KEY] = mode
        response = client._invoke_sync(broker_addr, request, self.timeout_millis)
        if response.code != ResponseCode.SUCCESS:
            raise MQBrokerException(response.code, response.remark or "")
        return response.remark or None

    # ---------------- Pop record export ----------------
    def export_pop_records(self, broker_addr: str, timeout: int) -> None:
        # The caller's timeout is forwarded verbatim; the admin default does not apply.
        self._invoke_broker(broker_addr, RequestCode.POP_ROLLBACK,
                            timeout_millis=timeout, vip=False)

    # ---------------- RocksDB cold queue ----------------
    def check_rocksdb_cq_write_progress(self, broker_addr: str, topic: str,
                                        check_store_time: int) -> CheckRocksdbCqWriteResult:
        client = self._require_client()
        header = CheckRocksdbCqWriteProgressRequestHeader()
        header.topic = topic
        header.check_store_time = check_store_time
        request = RemotingCommand.create_request_command(
            RequestCode.CHECK_ROCKSDB_CQ_WRITE_PROGRESS, header)
        for k, v in header.to_ext_fields().items():
            request.ext_fields[k] = str(v)
        response = client._invoke_sync(broker_addr, request, self.timeout_millis)
        if response.code != ResponseCode.SUCCESS:
            raise MQClientException(response.remark or "checkRocksdbCqWriteProgress failed",
                                    response.code)
        return CheckRocksdbCqWriteResult.decode(response.body)

    def export_rocksdb_config_to_json(self, broker_addr: str,
                                      config_types: List[str]) -> None:
        client = self._require_client()
        header = ExportRocksDBConfigToJsonRequestHeader()
        header.update_config_type(config_types)
        request = RemotingCommand.create_request_command(
            RequestCode.EXPORT_ROCKSDB_CONFIG_TO_JSON, header)
        for k, v in header.to_ext_fields().items():
            request.ext_fields[k] = str(v)
        response = client._invoke_sync(broker_addr, request, self.timeout_millis)
        if response.code != ResponseCode.SUCCESS:
            raise MQClientException(
                response.remark or "exportRocksDBConfigToJson failed", response.code)

    # ---------------- Lite topic admin ----------------
    def _invoke_broker_method(self, addr: str, code: int,
                              ext_fields: Optional[Dict[str, str]],
                              decode_body) -> object:
        """The shared lite-admin invocation: VIP routing, the body decoded only when the
        broker sent one, and a broker exception carrying the address on failure."""
        client = self._require_client()
        request = RemotingCommand.create_request_command(code, None)
        if ext_fields:
            for k, v in ext_fields.items():
                request.ext_fields[k] = str(v)
        response = client._invoke_sync(
            MixAll.broker_vip_channel(self.vip_channel_enabled, addr), request,
            self.timeout_millis)
        if response.code != ResponseCode.SUCCESS:
            raise MQBrokerException(response.code, response.remark or "", addr)
        if response.body:
            return decode_body(response.body)
        return None

    def get_broker_lite_info(self, broker_addr: str) -> Optional[GetBrokerLiteInfoResponseBody]:
        return self._invoke_broker_method(broker_addr, RequestCode.GET_BROKER_LITE_INFO,
                                          None, GetBrokerLiteInfoResponseBody.decode)

    def get_parent_topic_info(self, broker_addr: str,
                              topic: str) -> Optional[GetParentTopicInfoResponseBody]:
        header = GetParentTopicInfoRequestHeader()
        header.topic = topic
        return self._invoke_broker_method(broker_addr, RequestCode.GET_PARENT_TOPIC_INFO,
                                          header.to_ext_fields(),
                                          GetParentTopicInfoResponseBody.decode)

    def get_lite_topic_info(self, broker_addr: str, parent_topic: str,
                            lite_topic: Optional[str] = None
                            ) -> Optional[GetLiteTopicInfoResponseBody]:
        header = GetLiteTopicInfoRequestHeader()
        header.parent_topic = parent_topic
        header.lite_topic = lite_topic
        return self._invoke_broker_method(broker_addr, RequestCode.GET_LITE_TOPIC_INFO,
                                          header.to_ext_fields(),
                                          GetLiteTopicInfoResponseBody.decode)

    def get_lite_client_info(self, broker_addr: str, parent_topic: str, group: str,
                             client_id: Optional[str] = None
                             ) -> Optional[GetLiteClientInfoResponseBody]:
        header = GetLiteClientInfoRequestHeader()
        header.parent_topic = parent_topic
        header.group = group
        header.client_id = client_id
        return self._invoke_broker_method(broker_addr, RequestCode.GET_LITE_CLIENT_INFO,
                                          header.to_ext_fields(),
                                          GetLiteClientInfoResponseBody.decode)

    def get_lite_group_info(self, broker_addr: str, group: str,
                            lite_topic: Optional[str] = None, top_k: int = 0
                            ) -> Optional[GetLiteGroupInfoResponseBody]:
        header = GetLiteGroupInfoRequestHeader()
        header.group = group
        header.lite_topic = lite_topic
        header.top_k = top_k
        return self._invoke_broker_method(broker_addr, RequestCode.GET_LITE_GROUP_INFO,
                                          header.to_ext_fields(),
                                          GetLiteGroupInfoResponseBody.decode)

    def trigger_lite_dispatch(self, broker_addr: str, group: str,
                              client_id: Optional[str] = None) -> None:
        header = TriggerLiteDispatchRequestHeader()
        header.group = group
        header.client_id = client_id
        self._invoke_broker_method(broker_addr, RequestCode.TRIGGER_LITE_DISPATCH,
                                   header.to_ext_fields(), lambda body: None)

    # ---------------- Concurrent fan-out tools ----------------
    def examine_topic_stats_concurrent(self, topic: str) -> AdminToolResult:
        def _execute() -> AdminToolResult:
            table = TopicStatsTable()
            route = self._examine_topic_route_info(topic)
            if route is None or not route.get_broker_datas():
                return AdminToolResult.success(table)

            lock = threading.Lock()

            def _collect(bd: BrokerData) -> None:
                try:
                    addr = bd.select_broker_addr()
                    if addr is not None:
                        part = self.examine_topic_stats_by_broker(addr, topic)
                        with lock:
                            table.offset_table.update(part.offset_table)
                            table.topic_put_tps += part.topic_put_tps
                except Exception as e:  # noqa: BLE001
                    logger.warning("getTopicStatsInfo error. topic=%s: %s", topic, e)

            self._fan_out(list(route.get_broker_datas()), _collect, self.timeout_millis)
            return AdminToolResult.success(table)

        return self._admin_tool_execute(_execute)

    def examine_consume_stats_concurrent(self, consumer_group: str,
                                         topic: Optional[str] = None) -> AdminToolResult:
        def _execute() -> AdminToolResult:
            route = None
            route_topics = [MixAll.get_retry_topic(consumer_group)]
            if topic is not None:
                route_topics.append(topic)
                route_topics.append(MixAll.build_pop_retry_topic(topic, consumer_group))
            for route_topic in route_topics:
                try:
                    route = self._examine_topic_route_info(route_topic)
                except Exception:  # noqa: BLE001
                    route = None
                if route is not None:
                    break
            if route is None or not route.get_broker_datas():
                return AdminToolResult.failure(
                    AdminToolsResultCodeEnum.TOPIC_ROUTE_INFO_NOT_EXIST,
                    "topic router info not found")

            result = ConsumeStats()
            lock = threading.Lock()
            consume_tps_map: Dict[str, float] = {}

            def _collect(bd: BrokerData) -> None:
                try:
                    addr = bd.select_broker_addr()
                    if addr is not None:
                        part = self.examine_consume_stats(addr, consumer_group, topic)
                        with lock:
                            result.offset_table.update(part.offset_table)
                            consume_tps_map[addr] = part.consume_tps
                except Exception as e:  # noqa: BLE001
                    logger.warning("getConsumeStats error. topic=%s, consumerGroup=%s: %s",
                                   topic, consumer_group, e)

            self._fan_out(list(route.get_broker_datas()), _collect, self.timeout_millis)
            # The TPS of every broker that answered is summed, answer or not otherwise.
            for tps in consume_tps_map.values():
                result.consume_tps += tps

            if not result.offset_table:
                try:
                    connection = self.examine_consumer_connection_info(consumer_group)
                except Exception as e:  # noqa: BLE001
                    logger.warning("examineConsumerConnectionInfo failed: %s", e)
                    return AdminToolResult.failure(
                        AdminToolsResultCodeEnum.CONSUMER_NOT_ONLINE,
                        "Not found the consumer group consume stats, because return "
                        "offset table is empty, maybe the consumer not consume any message")
                if connection.message_model == "BROADCASTING":
                    return AdminToolResult.failure(
                        AdminToolsResultCodeEnum.BROADCAST_CONSUMPTION,
                        "Not found the consumer group consume stats, because return "
                        "offset table is empty, the consumer is under the broadcast mode")
            return AdminToolResult.success(result)

        return self._admin_tool_execute(_execute)

    def delete_topic_in_broker_concurrent(self, addrs: Set[str],
                                          topic: str) -> AdminToolResult:
        success_list: List[str] = []
        failure_list: List[str] = []
        lock = threading.Lock()

        def _delete(addr: str) -> None:
            try:
                self._require_client().delete_topic_in_broker(addr, topic)
                with lock:
                    success_list.append(addr)
            except Exception as e:  # noqa: BLE001
                logger.warning("deleteTopicInBroker error. topic=%s, broker=%s: %s",
                               topic, addr, e)
                with lock:
                    failure_list.append(addr)

        self._fan_out(list(addrs), _delete, self.timeout_millis)
        return AdminToolResult.success(BrokerOperatorResult(success_list, failure_list))

    def reset_offset_new_concurrent(self, group: str, topic: str,
                                    timestamp: int) -> AdminToolResult:
        def _execute() -> AdminToolResult:
            route = self._examine_topic_route_info(topic)
            if route is None or not route.get_broker_datas():
                return AdminToolResult.failure(
                    AdminToolsResultCodeEnum.TOPIC_ROUTE_INFO_NOT_EXIST,
                    "topic router info not found")
            route_map: Dict[str, QueueData] = {}
            for qd in route.queue_datas:
                route_map[qd.broker_name] = qd

            success_list: List[str] = []
            failure_list: List[str] = []
            lock = threading.Lock()

            def _reset(bd: BrokerData) -> None:
                addr = bd.select_broker_addr()
                try:
                    if addr is not None:
                        offset_table = self._invoke_broker_reset_offset(
                            addr, topic, group, timestamp, True, False)
                        if offset_table is not None:
                            with lock:
                                success_list.append(addr)
                        else:
                            with lock:
                                failure_list.append(addr)
                except MQClientException as e:
                    if e.response_code == ResponseCode.CONSUMER_NOT_ONLINE:
                        try:
                            self._reset_offset_old_on_broker(
                                addr, route_map.get(bd.broker_name), group, topic,
                                timestamp, True)
                            with lock:
                                success_list.append(addr)
                        except Exception as e2:  # noqa: BLE001
                            logger.warning("resetOffsetByTimestampOld error. addr=%s, "
                                           "topic=%s, group=%s, timestamp=%s: %s",
                                           addr, topic, group, timestamp, e2)
                            with lock:
                                failure_list.append(addr)
                    elif e.response_code == ResponseCode.SYSTEM_ERROR:
                        # A group that never came online has nothing to reset: counted
                        # as done.
                        with lock:
                            success_list.append(addr)
                    else:
                        logger.warning("resetOffsetNewConcurrent error. addr=%s, "
                                       "topic=%s, group=%s, timestamp=%s: %s",
                                       addr, topic, group, timestamp, e)
                        with lock:
                            failure_list.append(addr)
                except Exception as e:  # noqa: BLE001
                    logger.warning("resetOffsetNewConcurrent error. addr=%s, topic=%s, "
                                   "group=%s, timestamp=%s: %s",
                                   addr, topic, group, timestamp, e)
                    with lock:
                        failure_list.append(addr)

            self._fan_out(list(route.get_broker_datas()), _reset, self.timeout_millis)
            result = BrokerOperatorResult(success_list, failure_list)
            if len(success_list) == len(route.get_broker_datas()):
                return AdminToolResult.success(result)
            return AdminToolResult.failure(AdminToolsResultCodeEnum.MQ_BROKER_ERROR,
                                           "operator failure", result)

        return self._admin_tool_execute(_execute)

    def _reset_offset_old_on_broker(self, broker_addr: str, queue_data: Optional[QueueData],
                                    consumer_group: str, topic: str, timestamp: int,
                                    force: bool) -> List[dict]:
        """The old per-broker reset: reset the queues the group actually consumed (from
        its consume stats), or every read queue when it never consumed the topic."""
        client = self._require_client()
        consume_stats = self.examine_consume_stats(broker_addr, consumer_group)
        rollback_stats_list: List[dict] = []
        has_consumed = False
        for queue, wrapper in consume_stats.offset_table.items():
            if queue.topic != topic:
                continue
            has_consumed = True
            rollback_stats_list.append(
                self._reset_offset_consume_offset(broker_addr, consumer_group, queue,
                                                  wrapper, timestamp, force))
        if not has_consumed and queue_data is not None:
            topic_status = self.examine_topic_stats_by_broker(broker_addr, topic)
            for i in range(queue_data.read_queue_nums):
                queue = MessageQueue(topic, queue_data.broker_name, i)
                offset_wrapper = topic_status.offset_table.get(queue)
                fallback = OffsetWrapper(
                    broker_offset=offset_wrapper.max_offset if offset_wrapper else 0,
                    consumer_offset=offset_wrapper.min_offset if offset_wrapper else 0)
                rollback_stats_list.append(
                    self._reset_offset_consume_offset(broker_addr, consumer_group, queue,
                                                      fallback, timestamp, force))
        return rollback_stats_list

    def _reset_offset_consume_offset(self, broker_addr: str, consumer_group: str,
                                     queue: MessageQueue, offset_wrapper: OffsetWrapper,
                                     timestamp: int, force: bool) -> dict:
        client = self._require_client()
        if timestamp == -1:
            reset_offset = client.get_max_offset(queue, self.timeout_millis, broker_addr)
        else:
            reset_offset = client.search_offset_by_timestamp(
                queue, timestamp, self.timeout_millis, broker_addr)
        stats = {
            "brokerName": queue.broker_name,
            "queueId": queue.queue_id,
            "brokerOffset": offset_wrapper.broker_offset,
            "consumerOffset": offset_wrapper.consumer_offset,
            "timestampOffset": reset_offset,
            "rollbackOffset": offset_wrapper.consumer_offset,
        }
        if force or reset_offset <= offset_wrapper.consumer_offset:
            stats["rollbackOffset"] = reset_offset
            client.update_consumer_offset(consumer_group, queue, reset_offset,
                                          self.timeout_millis, broker_addr)
        return stats

    def query_topics_by_consumer_concurrent(self, group: str) -> AdminToolResult:
        def _execute() -> AdminToolResult:
            retry_topic = MixAll.get_retry_topic(group)
            route = self._examine_topic_route_info(retry_topic)
            if route is None or not route.get_broker_datas():
                return AdminToolResult.failure(
                    AdminToolsResultCodeEnum.TOPIC_ROUTE_INFO_NOT_EXIST,
                    "router info not found.")

            result = TopicList()
            lock = threading.Lock()

            def _collect(bd: BrokerData) -> None:
                try:
                    addr = bd.select_broker_addr()
                    if addr is not None:
                        topic_list = self.query_topics_by_consumer_to_broker(addr, group)
                        with lock:
                            for topic in topic_list.get_topic_list():
                                result.topic_list.append(topic)
                except Exception as e:  # noqa: BLE001
                    logger.warning("queryTopicsByConsumer error. group=%s: %s", group, e)

            self._fan_out(list(route.get_broker_datas()), _collect, self.timeout_millis)
            return AdminToolResult.success(result)

        return self._admin_tool_execute(_execute)

    def query_consume_time_span_concurrent(self, topic: str,
                                           group: str) -> AdminToolResult:
        def _execute() -> AdminToolResult:
            span_set: List[dict] = []
            route = self._examine_topic_route_info(topic)
            if route is None or not route.get_broker_datas():
                return AdminToolResult.success(span_set)
            lock = threading.Lock()

            def _collect(bd: BrokerData) -> None:
                try:
                    addr = bd.select_broker_addr()
                    if addr is not None:
                        response = self._invoke_broker(
                            addr, RequestCode.QUERY_CONSUME_TIME_SPAN,
                            {"topic": topic, "group": group})
                        if response.body:
                            body = QueryConsumeTimeSpanBody.decode(response.body)
                            with lock:
                                span_set.extend(body.consume_time_span_set)
                except Exception as e:  # noqa: BLE001
                    logger.warning("queryConsumeTimeSpan error. topic=%s, group=%s: %s",
                                   topic, group, e)

            self._fan_out(list(route.get_broker_datas()), _collect, self.timeout_millis)
            return AdminToolResult.success(span_set)

        return self._admin_tool_execute(_execute)

    # ---------------- Auth user / ACL management ----------------
    def _invoke_auth(self, broker_addr: str, code: int,
                     ext_fields: Dict[str, str],
                     body: Optional[bytes] = None,
                     timeout_millis: Optional[int] = None) -> RemotingCommand:
        """Send an auth management RPC and verify SUCCESS.

        ``vip=False`` is load-bearing: these RPCs must reach the address the caller gave,
        byte for byte. The VIP-channel rewrite would move them to another port, where the
        auth processor is not listening.
        """
        return self._invoke_broker(broker_addr, code, ext_fields=ext_fields, body=body,
                                   timeout_millis=timeout_millis, vip=False)

    @staticmethod
    def _auth_ext_fields(**fields: Optional[str]) -> Dict[str, str]:
        """Keep only the header fields that are set: an unset one is omitted rather than
        sent as ``""`` or ``"None"``, which the broker would read as a real value."""
        return {k: v for k, v in fields.items() if v is not None}

    @staticmethod
    def _resolve_user(user: Optional[Union[UserInfo, str]], password: Optional[str],
                      user_type: Optional[str]) -> UserInfo:
        if isinstance(user, UserInfo):
            return user
        return UserInfo(username=user, password=password, user_type=user_type)

    @staticmethod
    def _resolve_acl(acl: Optional[Union[AclInfo, str]],
                     resources: Union[str, List[str], None], actions: Optional[List[str]],
                     source_ips: Optional[List[str]],
                     decision: Optional[str]) -> AclInfo:
        if isinstance(acl, AclInfo):
            return acl
        return AclInfo.for_resources(acl, resources, actions, source_ips, decision)

    def create_user(self, broker_addr: str, user: Optional[Union[UserInfo, str]] = None,
                    password: Optional[str] = None, user_type: Optional[str] = None,
                    timeout_millis: Optional[int] = None) -> None:
        """Create an auth user. Pass either a UserInfo or, in the convenience form, the
        username / password / userType trio -- that form leaves userStatus unset, so the key
        stays out of the body."""
        info = self._resolve_user(user, password, user_type)
        self._invoke_auth(broker_addr, RequestCode.AUTH_CREATE_USER,
                          self._auth_ext_fields(username=info.username),
                          body=info.encode(), timeout_millis=timeout_millis)

    def update_user(self, broker_addr: str, user: Optional[Union[UserInfo, str]] = None,
                    password: Optional[str] = None, user_type: Optional[str] = None,
                    timeout_millis: Optional[int] = None) -> None:
        """Update an auth user; same two call shapes as create_user."""
        info = self._resolve_user(user, password, user_type)
        self._invoke_auth(broker_addr, RequestCode.AUTH_UPDATE_USER,
                          self._auth_ext_fields(username=info.username),
                          body=info.encode(), timeout_millis=timeout_millis)

    def delete_user(self, broker_addr: str, username: str,
                    timeout_millis: Optional[int] = None) -> None:
        self._invoke_auth(broker_addr, RequestCode.AUTH_DELETE_USER,
                          self._auth_ext_fields(username=username),
                          timeout_millis=timeout_millis)

    def get_user(self, broker_addr: str, username: str,
                 timeout_millis: Optional[int] = None) -> Optional[UserInfo]:
        response = self._invoke_auth(broker_addr, RequestCode.AUTH_GET_USER,
                                     self._auth_ext_fields(username=username),
                                     timeout_millis=timeout_millis)
        return UserInfo.decode(response.body)

    def list_users(self, broker_addr: str, filter: Optional[str] = None,
                   timeout_millis: Optional[int] = None) -> List[UserInfo]:
        response = self._invoke_auth(broker_addr, RequestCode.AUTH_LIST_USER,
                                     self._auth_ext_fields(filter=filter),
                                     timeout_millis=timeout_millis)
        return UserInfo.decode_list(response.body)

    def create_acl(self, broker_addr: str, acl: Optional[Union[AclInfo, str]] = None,
                   resources: Union[str, List[str], None] = None,
                   actions: Optional[List[str]] = None,
                   source_ips: Optional[List[str]] = None, decision: Optional[str] = None,
                   timeout_millis: Optional[int] = None) -> None:
        """Create an ACL. Pass either an AclInfo or, in the convenience form, the subject /
        resources / actions / sourceIps / decision set -- that form builds one untyped policy
        holding one entry per resource."""
        info = self._resolve_acl(acl, resources, actions, source_ips, decision)
        self._invoke_auth(broker_addr, RequestCode.AUTH_CREATE_ACL,
                          self._auth_ext_fields(subject=info.subject),
                          body=info.encode(), timeout_millis=timeout_millis)

    def update_acl(self, broker_addr: str, acl: Optional[Union[AclInfo, str]] = None,
                   resources: Union[str, List[str], None] = None,
                   actions: Optional[List[str]] = None,
                   source_ips: Optional[List[str]] = None, decision: Optional[str] = None,
                   timeout_millis: Optional[int] = None) -> None:
        """Update an ACL; same two call shapes as create_acl."""
        info = self._resolve_acl(acl, resources, actions, source_ips, decision)
        self._invoke_auth(broker_addr, RequestCode.AUTH_UPDATE_ACL,
                          self._auth_ext_fields(subject=info.subject),
                          body=info.encode(), timeout_millis=timeout_millis)

    def delete_acl(self, broker_addr: str, subject: str, resource: str,
                   timeout_millis: Optional[int] = None) -> None:
        """Delete one resource grant of a subject: this request carries only the subject
        and the resource, no body and no policy shape."""
        self._invoke_auth(broker_addr, RequestCode.AUTH_DELETE_ACL,
                          self._auth_ext_fields(subject=subject, resource=resource),
                          timeout_millis=timeout_millis)

    def get_acl(self, broker_addr: str, subject: str,
                timeout_millis: Optional[int] = None) -> Optional[AclInfo]:
        response = self._invoke_auth(broker_addr, RequestCode.AUTH_GET_ACL,
                                     self._auth_ext_fields(subject=subject),
                                     timeout_millis=timeout_millis)
        return AclInfo.decode(response.body)

    def list_acls(self, broker_addr: str, subject_filter: Optional[str] = None,
                  resource_filter: Optional[str] = None,
                  timeout_millis: Optional[int] = None) -> List[AclInfo]:
        response = self._invoke_auth(
            broker_addr, RequestCode.AUTH_LIST_ACL,
            self._auth_ext_fields(subjectFilter=subject_filter,
                                  resourceFilter=resource_filter),
            timeout_millis=timeout_millis)
        return AclInfo.decode_list(response.body)


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
           "QueryConsumeQueueResponseBody", "UserInfo", "AclInfo",
           "AdminToolResult", "AdminToolsResultCodeEnum", "BrokerOperatorResult",
           "BrokerReplicasInfo", "ReplicasInfo", "ReplicaIdentity",
           "CheckRocksdbCqWriteResult", "GetMetaDataResponseHeader"]
