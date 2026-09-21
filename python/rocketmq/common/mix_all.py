# -*- coding: utf-8 -*-
"""MixAll 常量与工具（对应 org.apache.rocketmq.common.MixAll）。"""
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
    # Request-Reply：应答 topic 名 = <cluster>_REPLY_TOPIC（Java MixAll.REPLY_TOPIC_POSTFIX）
    REPLY_TOPIC_POSTFIX = "REPLY_TOPIC"
    # Request-Reply：应答消息的 MSG_TYPE 属性值（Java MixAll.REPLY_MESSAGE_FLAG）
    REPLY_MESSAGE_FLAG = "reply"
    SYSTEM_TOPIC_PREFIX = "rmq_sys_"
    TOOLS_CONSUMER_GROUP = "TOOLS_CONSUMER"
    FILTERSRV_CONSUMER_GROUP = "FILTERSRV_CONSUMER"
    MONITOR_CONSUMER_GROUP = "__MONITOR_CONSUMER"
    CLIENT_INNER_CONSUMER_GROUP = "CLIENT_INNER_CONSUMER"
    SELF_TEST_CONSUMER_GROUP2 = "SELF_TEST_C_GROUP2"
    ONS_NAMESPACE = "namespace"
    # ⚠ Java MixAll.UNIQUE_MSG_QUERY_FLAG 是 extFields 的**键名**（值为 "true"/"false"），
    # 不是数字标志位。broker QueryMessageProcessor 用 request.extFields.get(该键) 判断是否按
    # uniqKey（INDEX_UNIQUE_TYPE）查询。早期 Python 把它误写成 1。
    UNIQUE_MSG_QUERY_FLAG = "_UNIQUE_KEY_QUERY"
    TRACE_TOPIC = "RMQ_SYS_TRACE_TOPIC"
    REAL_TRACE_TOPIC = "rmq_sys_TRACE_DATA"
    # 轨迹里的 region 占位值：SEND 响应头没带 MSG_REGION 时用它（Java MixAll:232）
    DEFAULT_TRACE_REGION_ID = "DefaultRegion"
    TRANS_STAT_PROGRESS_TOPIC = "RMQ_SYS_TRANS_OP_HALF_TOPIC"
    RMQ_SYS_TRANS_HALF_TOPIC = "RMQ_SYS_TRANS_HALF_TOPIC"
    RMQ_SYS_TRANS_OP_HALF_TOPIC = "RMQ_SYS_TRANS_OP_HALF_TOPIC"
    RMQ_SYS_TRANS_CHECK_MAX_TIME_TOPIC = "RMQ_SYS_TRANS_CHECK_MAX_TIME_TOPIC"
    RMQ_SYS_TRANS_CHECK_MAX_TIME = 15
    TRANS_CHECK_MAX_TIME = 15
    UNIT_PREFIX = "unit_"
    # Java `MixAll.REQ_T`（common/MixAll.java:115）：extFields 的键名，值是
    # `RequestType` 的枚举 code。由 StreamTypeRPCHook 在发请求前写入。
    REQ_T = "ReqT"
    # Java `ClientConfig#buildMQClientId` 拼的是 `sb.append(RequestType.STREAM)`，
    # 即枚举**名**（不是 code），所以 clientId 后缀为 "@STREAM"。
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
    def get_reply_topic(cluster_name: str) -> str:
        """对应 Java MixAll.getReplyTopic(clusterName) = clusterName + "_REPLY_TOPIC"。

        Request-Reply 的应答消息就发到这个 topic 上（broker 会把 cluster 名写进
        消息的 CLUSTER 属性，应答方据此拼出该 topic）。注意这**不是**控制台里那个
        `%REPLY%<topic>` 前缀（那是另一套东西），本方法只用于 request-reply。
        """
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
        """对应 Java MixAll.isLmq（LMQ topic 以 %LMQ% 开头）。"""
        return lmq_meta_data is not None and lmq_meta_data.startswith(MixAll.LMQ_PREFIX)

    @staticmethod
    def is_sys_consumer_group(consumer_group: str) -> bool:
        """对应 Java MixAll.isSysConsumerGroup（CID_RMQ_SYS_ 前缀）。"""
        return consumer_group is not None and consumer_group.startswith(MixAll.CID_RMQ_SYS_PREFIX)

    @staticmethod
    def is_predefined_group(consumer_group: str) -> bool:
        """对应 Java MixAll.isPredefinedGroup 的 PREDEFINE_GROUP_SET。"""
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

    # ---------------- clientId（对应 Java ClientConfig）----------------
    # Java `ClientConfig#instanceName` 的默认值（`System.getProperty("rocketmq.client.name", "DEFAULT")`）
    DEFAULT_INSTANCE_NAME = "DEFAULT"

    @staticmethod
    def cached_ip_str() -> str:
        """进程内只探测一次的本机 IP。

        对应 Java `ClientConfig#clientIP` —— 它在 ClientConfig 构造时就定下来了，
        同一个客户端的 clientId 因此稳定；每次都重新探测既慢又可能在网卡变化后
        让重启的客户端换一个 clientId。
        """
        global _CACHED_IP_STR
        if _CACHED_IP_STR is None:
            _CACHED_IP_STR = MixAll.get_ip_str()
        return _CACHED_IP_STR

    @staticmethod
    def build_mq_client_id(client_ip: str, instance_name: str,
                           unit_name: Optional[str] = None,
                           enable_stream_request_type: bool = False) -> str:
        """对应 Java `ClientConfig#buildMQClientId`：`ip@instanceName[@unitName][@STREAM]`。

        两段后缀都是可选的，且顺序固定：先 unitName 再 STREAM。
        - unitName：Java 用 `UtilAll.isBlank(unitName)` 判断，**只在判空时用 trim**，
          拼接用的是原值（所以尾随空格的 unitName 会原样进 clientId）。
        - STREAM：`enableStreamRequestType` 为真时拼 `sb.append(RequestType.STREAM)`，
          即枚举名。它的存在意义见 `client_id_for` 的注释。
        """
        cid = "%s@%s" % (client_ip, instance_name)
        if unit_name is not None and unit_name.strip():
            # Java 只在 isBlank 判断上用了 trim，拼接时用的是原值
            cid = "%s@%s" % (cid, unit_name)
        if enable_stream_request_type:
            cid = "%s@%s" % (cid, MixAll.STREAM_REQUEST_TYPE)
        return cid

    @staticmethod
    def change_instance_name_to_pid(instance_name: str) -> str:
        """对应 Java `ClientConfig#changeInstanceNameToPID`：默认名 `DEFAULT` 换成
        `<pid>#<nanoTime>`，其余原样返回。

        这一步是 clientId 唯一性的来源：不换的话同进程里两个客户端会算出同一个
        clientId（旧实现用秒级时间戳，同一秒内必撞），在 `MQClientInstance.INSTANCE_MAP`
        里互相覆盖。Java 只在生产者（非 `CLIENT_INNER_PRODUCER`）和 CLUSTERING 消费者
        的 `start()` 里调用它，条件由各 facade 把。
        """
        if instance_name == MixAll.DEFAULT_INSTANCE_NAME:
            import time
            # Java 用 System.nanoTime()：单调、原点任意，只用来保证同进程内不重复
            return "%d#%d" % (MixAll.pid(), time.monotonic_ns())
        return instance_name

    @staticmethod
    def client_id_for(instance_name: str, unit_name: Optional[str] = None,
                      enable_stream_request_type: bool = False) -> str:
        """未显式配置 clientId 时的默认口径：`<本机 IP>@<instanceName>[@<unitName>][@STREAM]`。

        调用方要按 Java 的条件先跑过 `change_instance_name_to_pid`
        （生产者无条件，消费者仅 CLUSTERING）。

        `enable_stream_request_type` 这段后缀不只是好看：Java 的注释写明它是为了
        「prevent unexpected reuses of MQClientInstance」—— 拉取/轻量消费者恒为 true，
        所以同一个 instanceName 的它们与推送消费者天然落在不同的 clientId 上。
        """
        return MixAll.build_mq_client_id(MixAll.cached_ip_str(), instance_name,
                                        unit_name, enable_stream_request_type)

    # ---------------- Properties <-> String（对应 MixAll.properties2String/string2Properties）----------------
    @staticmethod
    def properties2_string(properties, is_sort: bool = False) -> str:
        """Java: MixAll.properties2String —— 每条 "key=value\\n"，null 值跳过。"""
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
        """Java: MixAll.string2Properties —— 走 java.util.Properties.load 语义。

        规则（逐条对齐 ``java.util.Properties.load``）：
          - 跳过空行与 ``#`` / ``!`` 注释行；
          - 行尾**未转义**的 ``\\`` 表示续行，下一行的前导空白被丢弃；
          - 键与值以**第一个** ``=``、``:`` 或**空白**分隔（空白也是合法分隔符！）；
          - 分隔符前后的空白被跳过；值的**尾部**空白保留（Java 不去尾空白）。

        注：Java 还会处理 ``\\t \\n \\uXXXX`` 等转义，broker 配置导出里不出现，
        这里不实现（避免把反斜杠语义做错反而不一致）。
        """
        if text is None:
            return {}
        result = {}
        # 先把续行合并成逻辑行（java.util.Properties 语义）
        logical_lines = []
        pending = None
        for raw in text.splitlines():
            line = raw
            if pending is not None:
                line = pending + line.lstrip()
                pending = None
            # 统计行尾反斜杠个数：奇数表示续行
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
            # 跳过分隔符前的空白
            while i < n and line[i] in _PROP_WS:
                i += 1
            # 可选的 '=' / ':' 及其后的空白
            if i < n and line[i] in "=:":
                i += 1
                while i < n and line[i] in _PROP_WS:
                    i += 1
            result[key] = line[i:]
        return result


# java.util.Properties.load 认的空白字符（string2_properties 的分隔判断用）。
# 必须是模块级常量：staticmethod 里裸名查找走的是模块全局，不是类作用域。
_PROP_WS = " \t\f"

# `MixAll.cached_ip_str` 的缓存，同样是模块级（类作用域里不能赋值）。
_CACHED_IP_STR = None


# 对应 Java MixAll.PREDEFINE_GROUP_SET
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
    """按 key 查消息的三种模式（对应 tools 的 QueryMsgByKeySubCommand.QueryMsgType）。

    与 ``MixAll.UNIQUE_MSG_QUERY_FLAG`` 不是一个东西：后者是 extFields 里的**键名**。
    """

    ALL_MESSAGE = 0
    UNIQUE_KEY = 1
    NORMAL = 2
