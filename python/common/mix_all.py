# -*- coding: utf-8 -*-
from __future__ import annotations


class MixAll:
    NAMESRV_ADDR_PROPERTY = "rocketmq.namesrv.addr"
    NAMESRV_ADDR_ENV = "NAMESRV_ADDR"
    MESSAGE_COMPRESS_LEVEL = "rocketmq.message.compressLevel"
    DEFAULT_TOPIC = "TBW102"
    BENCHMARK_TOPIC = "BenchmarkTest"
    DEFAULT_PRODUCER_GROUP = "DEFAULT_PRODUCER"
    DEFAULT_CONSUMER_GROUP = "DEFAULT_CONSUMER"
    CLIENT_INNER_PRODUCER_GROUP = "CLIENT_INNER_PRODUCER"
    SELF_TEST_PRODUCER_GROUP = "SELF_TEST_P_GROUP"
    SELF_TEST_CONSUMER_GROUP = "SELF_TEST_C_GROUP"
    SCHEDULE_CONSUMER_GROUP = "SCHEDULE_CONSUMER"
    ONS_HTTP_PROXY_GROUP = "CID_ONS-HTTP-PROXY"
    CID_ONSAPI_PERMISSION_GROUP = "CID_ONSAPI_PERMISSION"
    CID_ONSAPI_OWNER_GROUP = "CID_ONSAPI_OWNER"
    CID_ONSAPI_PULL_GROUP = "CID_ONSAPI_PULL"
    CID_SYS_RMQ_TRANS = "CID_SYS_RMQ_TRANS"
    ONS_ADDR = "ONS_ADDR"
    CID_RMQ_SYS_PREFIX = "CID_RMQ_SYS_"
    CID_ONSAPI_PREFIX = "CID_ONSAPI_"
    CID_SDK_SYNC_PREFIX = "CID_SDK_SYNC_"
    CID_SDK_ASYNC_PREFIX = "CID_SDK_ASYNC_"
    CID_SDK_PROXY_PREFIX = "CID_SDK_PROXY_"
    PROXY_NAME = "MQProxy"
    DEFAULT_PRODUCER_GROUP_AND_STREAM = "DEFAULT_PRODUCER_AND_STREAM"

    RETRY_GROUP_TOPIC_PREFIX = "%RETRY%"
    DLQ_GROUP_TOPIC_PREFIX = "%DLQ%"
    REPLY_TOPIC_PREFIX = "%REPLY%"
    REPLY_TOPIC_POSTFIX = "REPLY_TOPIC"
    REPLY_MESSAGE_FLAG = "reply"
    SYSTEM_TOPIC_PREFIX = "rmq_sys_"
    TOOLS_CONSUMER_GROUP = "TOOLS_CONSUMER"
    FILTERSRV_CONSUMER_GROUP = "FILTERSRV_CONSUMER"
    MONITOR_CONSUMER_GROUP = "__MONITOR_CONSUMER"
    CLIENT_INNER_CONSUMER_GROUP = "CLIENT_INNER_CONSUMER"
    SELF_TEST_CONSUMER_GROUP2 = "SELF_TEST_C_GROUP2"
    ONS_NAMESPACE = "namespace"
    UNIQUE_MSG_QUERY_FLAG = "_UNIQUE_KEY_QUERY"
    TRACE_TOPIC = "RMQ_SYS_TRACE_TOPIC"
    REAL_TRACE_TOPIC = "rmq_sys_TRACE_DATA"
    DEFAULT_TRACE_REGION_ID = "DefaultRegion"
    TRANS_STAT_PROGRESS_TOPIC = "RMQ_SYS_TRANS_OP_HALF_TOPIC"
    RMQ_SYS_TRANS_HALF_TOPIC = "RMQ_SYS_TRANS_HALF_TOPIC"
    RMQ_SYS_TRANS_OP_HALF_TOPIC = "RMQ_SYS_TRANS_OP_HALF_TOPIC"
    RMQ_SYS_TRANS_CHECK_MAX_TIME_TOPIC = "RMQ_SYS_TRANS_CHECK_MAX_TIME_TOPIC"
    RMQ_SYS_TRANS_CHECK_MAX_TIME = 15
    TRANS_CHECK_MAX_TIME = 15
    UNIT_PREFIX = "unit_"
    REQ_T = "ReqT"
    # Heartbeat V2 (MQClientAPIImpl#sendHeartbeatV2 reads both keys out of the HEART_BEAT(34)
    # response extFields; ClientManageProcessor#heartBeatV2 writes them).
    IS_SUPPORT_HEART_BEAT_V2 = "IS_SUPPORT_HEART_BEAT_V2"
    IS_SUB_CHANGE = "IS_SUB_CHANGE"
    RPC_REQUEST_HEADER_NAMESPACED_FIELD = "nsd"
    RPC_REQUEST_HEADER_NAMESPACE_FIELD = "ns"
    STREAM_REQUEST_TYPE = "STREAM"
    LMQ_PREFIX = "%LMQ%"
    LMQ_QUEUE_ID = 0
    DEFAULT_TOPIC_QUEUE_NUMS = 4
    DEFAULT_TOPIC_READ_QUEUE_NUMS = 4
    DEFAULT_TOPIC_WRITE_QUEUE_NUMS = 4
    MAX_TOPIC_LENGTH = 127
    MAX_GROUP_LENGTH = 255
    CHARACTER_MAX_LENGTH = 255
    PULL_THRESHOLD_LEVEL_HIGH = 1
    PULL_THRESHOLD_LEVEL_MEDIUM = 2
    PULL_THRESHOLD_LEVEL_LOW = 3
    PULL_TIMEOUT_MILLIS_HIGH = 30000
    PULL_TIMEOUT_MILLIS_MEDIUM = 20000
    PULL_TIMEOUT_MILLIS_LOW = 10000
    LOG_STATS_TOPIC = "LOG_STATS_TOPIC"

    W_HELPER = "HELPER"
    W_EXPIRY_DATE = "EXPIRY_DATE"
    W_AVATAR = "AVATAR"
    W_REGION_ID = "REGION_ID"

    MASTER_ID = 0
    DEFAULT_CENTER = "DEFAULT_CENTER"

    NAMESPACE_PATTERN = "^[%s]{4}[a-zA-Z0-9_-]+$"

    READ_PERM_BY_DEFAULT = 4 | 2  # PermName.PERM_READ | PERM_WRITE

    @staticmethod
    def get_retry_topic(consumer_group: str) -> str:
        return "%s%s" % (MixAll.RETRY_GROUP_TOPIC_PREFIX, consumer_group)

    @staticmethod
    def is_retry_topic(topic: str) -> bool:
        return topic is not None and topic.startswith(MixAll.RETRY_GROUP_TOPIC_PREFIX)

    @staticmethod
    def get_dlq_topic(consumer_group: str) -> str:
        return "%s%s" % (MixAll.DLQ_GROUP_TOPIC_PREFIX, consumer_group)

    @staticmethod
    def is_dlq_topic(topic: str) -> bool:
        return topic is not None and topic.startswith(MixAll.DLQ_GROUP_TOPIC_PREFIX)

    @staticmethod
    def broker_vip_channel(is_change: bool, broker_addr: str) -> str:
        pass
        if not is_change:
            return broker_addr
        host, _, port = broker_addr.rpartition(":")
        if not host or not port:
            return broker_addr
        try:
            return "%s:%d" % (host, int(port) - 2)
        except ValueError:
            return broker_addr

    @staticmethod
    def get_reply_topic(cluster_name: str) -> str:
        pass
        return "%s_%s" % (cluster_name, MixAll.REPLY_TOPIC_POSTFIX)

    @staticmethod
    def get_broker_circuit_breaker_consume_group() -> str:
        return "BROKER_CIRCUIT_BREAKER"

    @staticmethod
    def get_broker_circuit_breaker_topic() -> str:
        return "BROKER_CIRCUIT_BREAKER_TOPIC"

    @staticmethod
    def is_sys_topic(topic: str) -> bool:
        return topic is not None and topic.startswith(MixAll.SYSTEM_TOPIC_PREFIX)

    @staticmethod
    def is_lmq(lmq_meta_data: str) -> bool:
        pass
        return lmq_meta_data is not None and lmq_meta_data.startswith(MixAll.LMQ_PREFIX)

    @staticmethod
    def is_sys_consumer_group(consumer_group: str) -> bool:
        pass
        return consumer_group is not None and consumer_group.startswith(MixAll.CID_RMQ_SYS_PREFIX)

    @staticmethod
    def is_predefined_group(consumer_group: str) -> bool:
        pass
        return consumer_group in _PREDEFINE_GROUP_SET

    @staticmethod
    def reset_retry_and_dlq_topic(topic: str) -> str:
        if topic is None:
            return None
        if MixAll.is_retry_topic(topic):
            return topic[len(MixAll.RETRY_GROUP_TOPIC_PREFIX):]
        if MixAll.is_dlq_topic(topic):
            return topic[len(MixAll.DLQ_GROUP_TOPIC_PREFIX):]
        return topic

    @staticmethod
    def compare_and_increase_namespace(instance_name: str, namespace: str) -> str:
        if namespace is None or not namespace:
            return instance_name
        if instance_name.startswith(namespace):
            return instance_name
        namespace_prefix = "%%%s%%" % namespace
        if instance_name.startswith(namespace_prefix):
            return instance_name
        return "%%%s%%%%%s" % (namespace, instance_name)

    @staticmethod
    def create_uniq_name(prefix: str):
        import time
        import uuid
        return "%s%s" % (prefix, uuid.uuid4().hex)

    @staticmethod
    def get_ip_str() -> str:
        import socket
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            s.connect(("8.8.8.8", 80))
            return s.getsockname()[0]
        except Exception:
            return "127.0.0.1"
        finally:
            s.close()

    @staticmethod
    def pid() -> int:
        import os
        return os.getpid()

    DEFAULT_INSTANCE_NAME = "DEFAULT"

    @staticmethod
    def cached_ip_str() -> str:
        pass
        global _CACHED_IP_STR
        if _CACHED_IP_STR is None:
            _CACHED_IP_STR = MixAll.get_ip_str()
        return _CACHED_IP_STR

    @staticmethod
    def build_mq_client_id(client_ip: str, instance_name: str,
                           unit_name: Optional[str] = None,
                           enable_stream_request_type: bool = False) -> str:
        pass
        cid = "%s@%s" % (client_ip, instance_name)
        if unit_name is not None and unit_name.strip():
            cid = "%s@%s" % (cid, unit_name)
        if enable_stream_request_type:
            cid = "%s@%s" % (cid, MixAll.STREAM_REQUEST_TYPE)
        return cid

    @staticmethod
    def change_instance_name_to_pid(instance_name: str) -> str:
        pass
        if instance_name == MixAll.DEFAULT_INSTANCE_NAME:
            import time
            return "%d#%d" % (MixAll.pid(), time.monotonic_ns())
        return instance_name

    @staticmethod
    def client_id_for(instance_name: str, unit_name: Optional[str] = None,
                      enable_stream_request_type: bool = False) -> str:
        pass
        return MixAll.build_mq_client_id(MixAll.cached_ip_str(), instance_name,
                                        unit_name, enable_stream_request_type)

    # ---------------- Properties <-> String (corresponds to MixAll.properties2String/string2Properties) ----------------
    @staticmethod
    def properties2_string(properties, is_sort: bool = False) -> str:
        pass
        if properties is None:
            return ""
        items = list(properties.items())
        if is_sort:
            items.sort(key=lambda kv: str(kv[0]))
        buf = []
        for k, v in items:
            if v is not None:
                buf.append("%s=%s\n" % (k, v))
        return "".join(buf)

    @staticmethod
    def string2_properties(text: str) -> dict:
        pass
        if text is None:
            return {}
        result = {}
        # first merge continuation lines into logical lines (.properties continuation format)
        logical_lines = []
        pending = None
        for raw in text.splitlines():
            line = raw
            if pending is not None:
                line = pending + line.lstrip()
                pending = None
            # count trailing backslashes: odd count means continuation line
            trailing = len(line) - len(line.rstrip("\\"))
            if trailing % 2 == 1:
                pending = line[:-1]
                continue
            logical_lines.append(line)
        if pending is not None:
            logical_lines.append(pending)

        for line in logical_lines:
            stripped = line.strip()
            if not stripped or stripped[0] in "#!":
                continue
            n = len(line)
            i = 0
            while i < n and line[i] in _PROP_WS:
                i += 1
            key_start = i
            while i < n and line[i] not in "=:" and line[i] not in _PROP_WS:
                i += 1
            key = line[key_start:i]
            # skip whitespace before the separator
            while i < n and line[i] in _PROP_WS:
                i += 1
            # optional '=' / ':' and the whitespace after it
            if i < n and line[i] in "=:":
                i += 1
                while i < n and line[i] in _PROP_WS:
                    i += 1
            result[key] = line[i:]
        return result


# Whitespace characters recognized by the .properties format (used by string2_properties for separator detection).
# Must be a module-level constant: a bare name lookup inside a staticmethod resolves to the module global,
# not the class scope.
_PROP_WS = " \t\f"

# Cache for `MixAll.cached_ip_str`, also module-level (cannot be assigned inside class scope).
_CACHED_IP_STR = None


_PREDEFINE_GROUP_SET = frozenset([
    MixAll.DEFAULT_CONSUMER_GROUP,
    MixAll.DEFAULT_PRODUCER_GROUP,
    MixAll.TOOLS_CONSUMER_GROUP,
    MixAll.SCHEDULE_CONSUMER_GROUP,
    MixAll.FILTERSRV_CONSUMER_GROUP,
    MixAll.MONITOR_CONSUMER_GROUP,
    MixAll.CLIENT_INNER_PRODUCER_GROUP,
    MixAll.SELF_TEST_PRODUCER_GROUP,
    MixAll.SELF_TEST_CONSUMER_GROUP,
    MixAll.ONS_HTTP_PROXY_GROUP,
    MixAll.CID_ONSAPI_PERMISSION_GROUP,
    MixAll.CID_ONSAPI_OWNER_GROUP,
    MixAll.CID_ONSAPI_PULL_GROUP,
    MixAll.CID_SYS_RMQ_TRANS,
])

class QueryMsgType:
    """Three modes for querying messages by key (corresponds to tools' QueryMsgByKeySubCommand.QueryMsgType).

    Not the same as ``MixAll.UNIQUE_MSG_QUERY_FLAG``: the latter is the **key name** inside extFields.
    """

    ALL_MESSAGE = 0
    UNIQUE_KEY = 1
    NORMAL = 2
