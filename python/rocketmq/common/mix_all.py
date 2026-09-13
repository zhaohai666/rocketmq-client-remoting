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
    SYSTEM_TOPIC_PREFIX = "rmq_sys_"
    TOOLS_CONSUMER_GROUP = "TOOLS_CONSUMER"
    FILTERSRV_CONSUMER_GROUP = "FILTERSRV_CONSUMER"
    MONITOR_CONSUMER_GROUP = "__MONITOR_CONSUMER"
    CLIENT_INNER_CONSUMER_GROUP = "CLIENT_INNER_CONSUMER"
    SELF_TEST_CONSUMER_GROUP2 = "SELF_TEST_C_GROUP2"
    ONS_NAMESPACE = "namespace"
    ALL_MESSAGE_QUERY_FLAG = 0
    UNIQUE_MSG_QUERY_FLAG = 1
    NORMAL_MSG_QUERY_FLAG = 2
    TRACE_TOPIC = "RMQ_SYS_TRACE_TOPIC"
    REAL_TRACE_TOPIC = "rmq_sys_TRACE_DATA"
    TRANS_STAT_PROGRESS_TOPIC = "RMQ_SYS_TRANS_OP_HALF_TOPIC"
    RMQ_SYS_TRANS_HALF_TOPIC = "RMQ_SYS_TRANS_HALF_TOPIC"
    RMQ_SYS_TRANS_OP_HALF_TOPIC = "RMQ_SYS_TRANS_OP_HALF_TOPIC"
    RMQ_SYS_TRANS_CHECK_MAX_TIME_TOPIC = "RMQ_SYS_TRANS_CHECK_MAX_TIME_TOPIC"
    RMQ_SYS_TRANS_CHECK_MAX_TIME = 15
    TRANS_CHECK_MAX_TIME = 15
    UNIT_PREFIX = "unit_"
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
    def get_reply_topic(topic: str) -> str:
        return "%s%s" % (MixAll.REPLY_TOPIC_PREFIX, topic)

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