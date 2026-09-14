# -*- coding: utf-8 -*-
"""管理端（对应 org.apache.rocketmq.client.admin.DefaultMQAdminExt / MQAdminExt
与 org.apache.rocketmq.tools.admin.DefaultMQAdminExtImpl）。

覆盖：Topic 增删查/配置、Broker 集群信息与运行时信息/配置、NameServer KV 配置、
订阅组管理、消费者/生产者连接、消费统计、offset 管理（含真实 broker 位点重置）、
消息查询（key / uniqKey / msgId / ConsumeQueue）。

对齐要点（均为 Java 5.x 探针 + 源码核对结论，勿凭记忆改）：
 - GET_BROKER_CONFIG 的 body 是 **properties 文本**（``k=v\\n``），不是 JSON。
 - UPDATE_AND_CREATE_SUBSCRIPTIONGROUP 的 body 是 SubscriptionGroupConfig **JSON**。
 - GET_TOPIC_CONFIG 请求头带 ``topic`` + ``lo``，响应体是 TopicConfigAndQueueMapping JSON。
 - GET_ALL_SUBSCRIPTIONGROUP_CONFIG 是**分页**接口（groupSeq / maxGroupNum / dataVersion）。
 - ResetOffsetBody.offsetTable 是 Map<MessageQueue, Long>。
 - KV 配置类请求打到 **NameServer**，且 PUT/DELETE 要广播到**每一个** NameServer。
"""
from __future__ import annotations

import time
from typing import Dict, List, Optional, Set

from ..common.message import MessageExt, MessageQueue
from ..common.message_decoder import decode_message_id, decode_message, decode_messages
from ..common.mix_all import MixAll
from ..common.sysflag import PermName
from ..common.topic_config import TopicConfig
from ..logging import get_logger
from ..remoting.protocol.body import (ClusterInfo, ConsumerConnection,
                                      ConsumerRunningInfo, KVTable,
                                      ProducerConnection, ResetOffsetBody, TopicList)
from ..remoting.protocol.admin_body import (ConsumeStats, QueryConsumeQueueResponseBody,
                                            TopicConfigSerializeWrapper, TopicStatsTable)
from ..remoting.protocol.codes import LanguageCode, RequestCode, ResponseCode
from ..remoting.protocol.remoting_command import RemotingCommand
from ..remoting.protocol.route import BrokerData, QueueData, TopicRouteData
from ..remoting.protocol.serialize import RemotingSerializable, fastjson_loads
from ..remoting.protocol.subscription import (SubscriptionGroupConfig,
                                              SubscriptionGroupWrapper)
from ..remoting.rpchook import RPCHook
from .consumer_result import PullResult, PullStatus
from .exception import MQBrokerException, MQClientException
from .mq_client import MQClientInstance

logger = get_logger()

# 默认超时（对应 DefaultMQAdminExt.DEFAULT_TIMEOUT = 5000 * 3）
DEFAULT_TIMEOUT = 5000 * 3


class DefaultMQAdminExt:
    """管理客户端（对应 org.apache.rocketmq.client.admin.DefaultMQAdminExt）。"""

    def __init__(self, rpc_hook: Optional[RPCHook] = None,
                 namespace: str = "", **kwargs):
        self.namespace = namespace
        self.instance_name = "ADMIN"
        self.client_id: Optional[str] = None
        self.name_server_addrs: List[str] = []
        self.rpc_hook = rpc_hook
        # 删除 topic 时一并清理的 KV namespace（Java 的 kvNamespaceToDeleteList）
        self.kv_namespace_to_delete_list: List[str] = []
        self.timeout_millis = int(kwargs.get("timeout_millis", DEFAULT_TIMEOUT))
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

    def get_name_server_address_list(self) -> List[str]:
        return list(self.name_server_addrs)

    def set_timeout_millis(self, timeout_millis: int) -> None:
        self.timeout_millis = timeout_millis

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

    def get_mq_client_instance(self) -> MQClientInstance:
        return self._require_client()

    def _require_client(self) -> MQClientInstance:
        if not self._started or self._mq_client is None:
            raise MQClientException("admin not started, call start() first")
        return self._mq_client

    # ---------------- 底层调用助手 ----------------
    def _invoke_broker(self, addr: str, code: int,
                       ext_fields: Optional[Dict[str, str]] = None,
                       body: Optional[bytes] = None,
                       timeout_millis: Optional[int] = None) -> RemotingCommand:
        """发到指定 Broker 并校验 SUCCESS。"""
        client = self._require_client()
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
        """广播到所有 NameServer（Java putKVConfigValue / deleteKVConfigValue 语义）。"""
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
        """发到第一个可用 NameServer（Java invokeSync(null, ...) 语义）。"""
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

    def _broker_addr_for_mq(self, client: MQClientInstance, mq: MessageQueue) -> str:
        route = client.get_topic_route_data(mq.topic)
        if route is None:
            raise MQClientException("No route info of this topic: %s" % mq.topic)
        addr = MQClientInstance.find_broker_addr_in_route(route, mq.broker_name)
        if addr is None:
            raise MQClientException("Broker %s not found in route of topic %s"
                                    % (mq.broker_name, mq.topic))
        return addr

    # ---------------- Topic 管理 ----------------
    def create_topic(self, key: str, new_topic: str, queue_num: int = 4,
                     topic_sys_flag: int = 0) -> None:
        client = self._require_client()
        client.create_topic_in_route(new_topic, queue_num, queue_num, MixAll.READ_PERM_BY_DEFAULT)

    def create_and_update_topic_config(self, addr: str, config: TopicConfig) -> None:
        """对应 Java DefaultMQAdminExtImpl.createAndUpdateTopicConfig（走 createTopicKey）。"""
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
            self._invoke_broker(ns_addr, RequestCode.DELETE_TOPIC_IN_NAMESRV,
                                {"topic": topic})

    def delete_topic_in_namesrv(self, topic: str) -> None:
        """兼容旧名：删除 NameServer 上的 topic 路由。"""
        client = self._require_client()
        client.delete_topic_in_namesrv(topic)

    def delete_topic(self, topic: str, cluster_name: Optional[str] = None) -> None:
        """删除 topic（先清各 broker，再清 NameServer 路由，最后清 KV namespace）。"""
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
        """对应 Java fetchTopicsByCLuster（GET_TOPICS_BY_CLUSTER 打到 NameServer）。"""
        response = self._invoke_namesrv_one(RequestCode.GET_TOPICS_BY_CLUSTER,
                                            {"clusterName": cluster_name})
        topics: Set[str] = set()
        if response.code == ResponseCode.SUCCESS and response.body:
            obj = RemotingSerializable.decode_json(response.body)
            topics.update(obj.get("topicList", []))
        return topics

    def get_cluster_list(self, topic: str) -> Set[str]:
        """对应 Java getClusterList：包含该 topic 的路由 broker 所在集群名集合。"""
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
        """拉取全部 topic 的路由（遍历 Nameserver 的 topic 列表）。"""
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
        """对应 Java examineTopicConfig（GET_TOPIC_CONFIG，body 为 TopicConfig JSON）。"""
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
        """对应 Java getUserTopicConfig：剔除系统 topic 与 %RETRY%/%DLQ%。"""
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
        """对应 Java examineTopicStats：遍历该 topic 的所有 broker 合并统计。"""
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

    # ---------------- 集群 / Broker ----------------
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

    # Java 旧接口名：getBrokerRuntimeInfo
    def get_broker_runtime_info(self, broker_addr: str,
                                timeout_millis: Optional[int] = None) -> KVTable:
        return self.fetch_broker_runtime_stats(broker_addr, timeout_millis)

    def get_broker_config(self, broker_addr: str,
                          timeout_millis: Optional[int] = None) -> Dict[str, str]:
        """对应 Java getBrokerConfig：响应体是 **properties 文本**，不是 JSON/KVTable。

        历史实现的 bug：把 body 当 KVTable JSON 解析，真实 broker 上必然失败。
        """
        response = self._invoke_broker(broker_addr, RequestCode.GET_BROKER_CONFIG,
                                       timeout_millis=timeout_millis)
        text = (response.body or b"").decode("utf-8")
        return MixAll.string2_properties(text)

    def update_broker_config(self, broker_addr: str, properties: Dict[str, str],
                             timeout_millis: Optional[int] = None) -> None:
        """对应 Java updateBrokerConfig（含 Validators.checkBrokerConfig 的 brokerPermission 校验）。"""
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

    def wipe_write_perm_of_broker(self, namesrv_addr: str, broker_name: str) -> int:
        response = self._invoke_broker(namesrv_addr, RequestCode.WIPE_WRITE_PERM_OF_BROKER,
                                       {"brokerName": broker_name})
        return int(response.ext_fields.get("wipeTopicCount", 0) or 0)

    def add_write_perm_of_broker(self, namesrv_addr: str, broker_name: str) -> int:
        response = self._invoke_broker(namesrv_addr, RequestCode.ADD_WRITE_PERM_OF_BROKER,
                                       {"brokerName": broker_name})
        return int(response.ext_fields.get("addTopicCount", 0) or 0)

    def clean_unused_topic(self, cluster_name: Optional[str] = None,
                           topic: Optional[str] = None) -> bool:
        """对应 Java cleanUnusedTopic：逐 broker 下发 CLEAN_UNUSED_TOPIC。"""
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

    # ---------------- NameServer KV 配置 ----------------
    def create_and_update_kv_config(self, namespace: str, key: str, value: str) -> None:
        """对应 Java createAndUpdateKvConfig → putKVConfigValue（广播所有 NameServer）。"""
        self._invoke_namesrv_all(RequestCode.PUT_KV_CONFIG,
                                 {"namespace": namespace, "key": key, "value": value})

    # Java 接口名（DefaultMQAdminExtImpl.putKVConfig 为空实现，真正的实现在 createAndUpdateKvConfig）
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

    # ---------------- 订阅组管理 ----------------
    def create_and_update_subscription_group_config(self, addr: str,
                                                    config: SubscriptionGroupConfig) -> None:
        """对应 Java createAndUpdateSubscriptionGroupConfig：body 为 SubscriptionGroupConfig JSON。"""
        self._invoke_broker(addr, RequestCode.UPDATE_AND_CREATE_SUBSCRIPTIONGROUP,
                            body=config.encode())

    def examine_subscription_group_config(self, addr: str, group: str) -> Optional[SubscriptionGroupConfig]:
        """对应 Java examineSubscriptionGroupConfig：拉全部订阅组后取目标 group。"""
        wrapper = self.get_all_subscription_group(addr)
        return wrapper.subscription_group_table.get(group)

    def get_subscription_group_config(self, addr: str, group: str) -> Optional[SubscriptionGroupConfig]:
        """对应 Java getSubscriptionGroupConfig（GET_SUBSCRIPTIONGROUP_CONFIG 单查）。"""
        response = self._invoke_broker(addr, RequestCode.GET_SUBSCRIPTIONGROUP_CONFIG,
                                       {"group": group})
        if not response.body:
            return None
        return SubscriptionGroupConfig.decode(response.body)

    def get_all_subscription_group(self, broker_addr: str,
                                   timeout_millis: Optional[int] = None) -> SubscriptionGroupWrapper:
        """对应 Java getAllSubscriptionGroup：**分页**累积，直到 groupSeq >= totalGroupNum-1。

        老版本 broker 不带 totalGroupNum，此时一次性返回全部（单轮即结束）。
        """
        client = self._require_client()
        timeout = timeout_millis or self.timeout_millis
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
                # 老 broker：一次返回全部
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

    # ---------------- 消费者 / 生产者连接 ----------------
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

    # ---------------- 消费统计 ----------------
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
        from ..remoting.protocol.body import ConsumeStatsList
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

    def query_topics_by_consumer(self, broker_addr: str, group: str) -> TopicList:
        response = self._invoke_broker(broker_addr, RequestCode.QUERY_TOPICS_BY_CONSUMER,
                                       {"group": group})
        if not response.body:
            return TopicList()
        return TopicList.decode(response.body)

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

    # ---------------- Offset 管理 ----------------
    def max_offset(self, mq: MessageQueue) -> int:
        return self._require_client().get_max_offset(mq)

    def min_offset(self, mq: MessageQueue) -> int:
        return self._require_client().get_min_offset(mq)

    def search_offset(self, mq: MessageQueue, timestamp: int) -> int:
        return self._require_client().search_offset_by_timestamp(mq, timestamp)

    def earliest_msg_store_time(self, mq: MessageQueue) -> int:
        client = self._require_client()
        addr = self._broker_addr_for_mq(client, mq)
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
                                  is_cpp: bool = True) -> Dict[MessageQueue, int]:
        """对应 Java resetOffsetByTimestamp：

        逐 broker 下发 INVOKE_BROKER_TO_RESET_OFFSET（broker 端按 timestamp 计算新位点，
        并同步在线消费者 + 更新 offset 表），汇总 ``Map<MessageQueue, Long>``。

        注意：这里**不再**走「逐队列 searchOffset + updateConsumerOffset」的旧本地实现——
        那不会同步在线消费者，也不会做 broker 端一致性校验。
        """
        client = self._require_client()
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
            request = RemotingCommand.create_request_command(
                RequestCode.INVOKE_BROKER_TO_RESET_OFFSET, None)
            ext = {
                "topic": topic,
                "group": group,
                "timestamp": timestamp,
                "force": "true" if is_force else "false",
                # Java：offset=-1 表示 offset 为空
                "offset": -1,
            }
            for k, v in ext.items():
                request.ext_fields[k] = str(v)
            if is_cpp:
                request.language = LanguageCode.CPP
            response = client._invoke_sync(addr, request, self.timeout_millis)
            if response.code == ResponseCode.SUCCESS:
                if response.body:
                    all_offsets.update(ResetOffsetBody.decode(response.body).offset_table)
            else:
                raise MQClientException(response.remark or "reset offset failed",
                                        response.code)
        if not all_offsets:
            raise MQClientException("reset offset failed, no broker returned offset table")
        return all_offsets

    def reset_offset_new(self, consumer_group: str, topic: str, timestamp: int) -> None:
        """对应 Java resetOffsetNew：先试新版（broker 端重置），失败再退化到旧版。"""
        try:
            self.reset_offset_by_timestamp(topic, consumer_group, timestamp, True)
        except MQClientException as e:
            if e.response_code == ResponseCode.CONSUMER_NOT_ONLINE:
                self.reset_offset_by_timestamp_old(consumer_group, topic, timestamp, True)
                return
            raise

    def reset_offset_by_timestamp_old(self, consumer_group: str, topic: str, timestamp: int,
                                      force: bool = True) -> Dict[MessageQueue, int]:
        """对应 Java resetOffsetByTimestampOld：逐队列 searchOffset 后按 force 决策写回。"""
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

    # ---------------- 消息查询 ----------------
    def query_message(self, topic: str, key: str, max_num: int, begin: int, end: int):
        """对应 Java MQAdminImpl.queryMessage(正常 key)：查所有 broker + 客户端侧 key 二次校验。"""
        from ..common.message_const import MessageConst
        return self._require_client().query_message_all_brokers(
            topic, key, max_num, begin, end,
            index_type=MessageConst.INDEX_KEY_TYPE, uniq_key=False)

    def query_message_by_uniq_key(self, topic: str, uniq_key: str) -> Optional[MessageExt]:
        """对应 Java queryMessageByUniqKey：indexType="U" + extFields["_UNIQUE_KEY_QUERY"]="true"。

        注意：broker 侧 uniqKey 索引只有 RocksDB 索引实现支持（IndexRocksDBStore）；
        默认的文件索引下该查询可能返回空，这属于 broker 配置差异而非客户端问题。
        """
        from ..common.message_const import MessageConst
        messages = self._require_client().query_message_all_brokers(
            topic, uniq_key, 32, 0, int(time.time() * 1000) + 60 * 60 * 1000,
            index_type=MessageConst.INDEX_UNIQUE_TYPE, uniq_key=True)
        return messages[0] if messages else None

    def query_message_by_key(self, topic: str, key: str, max_num: int = 32) -> List[MessageExt]:
        """按普通 KEYS 索引查询（对应工具 queryMsgByKey 的 NORMAL 模式）。"""
        from ..common.message_const import MessageConst
        return self._require_client().query_message_all_brokers(
            topic, key, max_num, 0, int(time.time() * 1000) + 60 * 60 * 1000,
            index_type=MessageConst.INDEX_KEY_TYPE, uniq_key=False)

    def view_message(self, topic: str, msg_id: str) -> MessageExt:
        """对应 Java MQAdminImpl.viewMessage：从 msgId 自身解出 broker 地址 + commitLog 偏移。"""
        try:
            ip, port, offset = decode_message_id(msg_id)
        except Exception as e:  # noqa: BLE001
            raise MQClientException(
                "query message by id finished, but no message.",
                ResponseCode.NO_MESSAGE) from e
        addr = "%s:%d" % (ip, port)
        response = self._invoke_broker(addr, RequestCode.VIEW_MESSAGE_BY_ID,
                                       {"topic": topic, "offset": offset})
        if not response.body:
            raise MQBrokerException(ResponseCode.NO_MESSAGE, "message not found: %s" % msg_id)
        return decode_message(response.body)

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


# ---------------------------------------------------------------- 辅助
def _perm_is_valid(value) -> bool:
    """对应 Java PermName.isValid(String)（数字解析失败等价于抛 NumberFormatException）。"""
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
