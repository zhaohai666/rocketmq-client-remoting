# -*- coding: utf-8 -*-
"""命名空间工具（对应 org.apache.rocketmq.remoting.protocol.NamespaceUtil）。

命名空间用于多租户隔离：客户端把 ``namespace`` 以 ``namespace%`` 前缀拼到
topic / group 上再发给 broker，从 broker 拿到的资源名在交给上层（listener、
admin 结果）之前再剥掉前缀。

对齐要点（勿凭直觉改）：
- 分隔符是 ``%``（不是 ``/`` 也不是 ``:``）。
- ``%RETRY%`` / ``%DLQ%`` 前缀**在**命名空间之外：``%RETRY%NS%GID``。
  因此剥/拼都要先把 retry/DLQ 前缀摘下来处理，再拼回去。
- 系统资源（``rmq_sys_`` 前缀 topic、``CID_RMQ_SYS_`` 前缀 group）**不**加命名空间。
"""
from __future__ import annotations

from ...common.mix_all import MixAll

NAMESPACE_SEPARATOR = "%"


class NamespaceUtil:
    """对应 Java NamespaceUtil（仅客户端用到的部分）。"""

    @staticmethod
    def with_out_retry_and_dlq(resource: str) -> str:
        """摘掉 retry/DLQ 前缀（对应 Java withOutRetryAndDLQ）。"""
        return MixAll.reset_retry_and_dlq_topic(resource)

    @staticmethod
    def is_retry_topic(resource: str) -> bool:
        return MixAll.is_retry_topic(resource)

    @staticmethod
    def is_dlq_topic(resource: str) -> bool:
        return MixAll.is_dlq_topic(resource)

    @staticmethod
    def is_system_resource(resource: str) -> bool:
        if not resource:
            return False
        return MixAll.is_sys_topic(resource) or MixAll.is_sys_consumer_group(resource)

    @staticmethod
    def is_already_with_namespace(resource: str, namespace: str) -> bool:
        if not namespace or not resource or NamespaceUtil.is_system_resource(resource):
            return False
        plain = NamespaceUtil.with_out_retry_and_dlq(resource)
        return plain.startswith(namespace + NAMESPACE_SEPARATOR)

    @staticmethod
    def without_namespace(resource_with_namespace: str, namespace: str = "") -> str:
        """剥掉命名空间前缀（对应 Java 两个重载的 withoutNamespace）。

        ``MQ_INST_XX%Topic`` → ``Topic``；``%RETRY%MQ_INST_XX%GID`` → ``%RETRY%GID``。
        未带该命名空间时原样返回。
        """
        if not resource_with_namespace:
            return resource_with_namespace
        if namespace:
            plain = NamespaceUtil.with_out_retry_and_dlq(resource_with_namespace)
            if not plain.startswith(namespace + NAMESPACE_SEPARATOR):
                return resource_with_namespace
        elif NamespaceUtil.is_system_resource(resource_with_namespace):
            return resource_with_namespace
        prefix = ""
        if NamespaceUtil.is_retry_topic(resource_with_namespace):
            prefix = MixAll.RETRY_GROUP_TOPIC_PREFIX
        if NamespaceUtil.is_dlq_topic(resource_with_namespace):
            prefix = MixAll.DLQ_GROUP_TOPIC_PREFIX
        plain = NamespaceUtil.with_out_retry_and_dlq(resource_with_namespace)
        index = plain.find(NAMESPACE_SEPARATOR)
        if index > 0:
            return prefix + plain[index + 1:]
        return resource_with_namespace

    @staticmethod
    def wrap_namespace(namespace: str, resource_without_namespace: str) -> str:
        """拼上命名空间前缀（对应 Java wrapNamespace）。"""
        if not namespace or not resource_without_namespace:
            return resource_without_namespace
        if NamespaceUtil.is_system_resource(resource_without_namespace):
            return resource_without_namespace
        if NamespaceUtil.is_already_with_namespace(resource_without_namespace, namespace):
            return resource_without_namespace
        prefix = ""
        if NamespaceUtil.is_retry_topic(resource_without_namespace):
            prefix = MixAll.RETRY_GROUP_TOPIC_PREFIX
        if NamespaceUtil.is_dlq_topic(resource_without_namespace):
            prefix = MixAll.DLQ_GROUP_TOPIC_PREFIX
        plain = NamespaceUtil.with_out_retry_and_dlq(resource_without_namespace)
        return "%s%s%s%s" % (prefix, namespace, NAMESPACE_SEPARATOR, plain)

    @staticmethod
    def wrap_namespace_and_retry(namespace: str, consumer_group: str) -> str:
        """``%RETRY%<wrapNamespace(namespace, group)>``（对应 Java wrapNamespaceAndRetry）。"""
        if not consumer_group:
            return consumer_group
        return MixAll.RETRY_GROUP_TOPIC_PREFIX + NamespaceUtil.wrap_namespace(
            namespace, consumer_group)

    @staticmethod
    def get_namespace_from_resource(resource: str) -> str:
        """从资源名里取出命名空间（对应 Java getNamespaceFromResource）。"""
        if not resource or NamespaceUtil.is_system_resource(resource):
            return ""
        plain = NamespaceUtil.with_out_retry_and_dlq(resource)
        index = plain.find(NAMESPACE_SEPARATOR)
        return plain[:index] if index > 0 else ""


__all__ = ["NamespaceUtil", "NAMESPACE_SEPARATOR"]
