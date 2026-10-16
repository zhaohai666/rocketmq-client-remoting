# -*- coding: utf-8 -*-
"""Namespace utilities (handling the namespace prefix of resource names).

A namespace is used for multi-tenant isolation: the client prefixes topic / group with
``namespace%`` before sending them to the broker, and strips the prefix off the
resource names coming back from the broker before handing them to the upper layers
(listeners, admin results).

The on-the-wire contract (do not change it on intuition):
- The separator is ``%`` (neither ``/`` nor ``:``).
- The ``%RETRY%`` / ``%DLQ%`` prefixes sit **outside** the namespace:
  ``%RETRY%NS%GID``. So both stripping and prepending must first take the retry/DLQ
  prefix off, handle it, and then put it back.
- System resources (topics prefixed with ``rmq_sys_``, groups prefixed with
  ``CID_RMQ_SYS_``) get **no** namespace.
"""
from __future__ import annotations

from common.mix_all import MixAll

NAMESPACE_SEPARATOR = "%"


class NamespaceUtil:
    """Utilities for namespaced resource names (only the parts used by the client)."""

    @staticmethod
    def with_out_retry_and_dlq(resource: str) -> str:
        """Strip the retry/DLQ prefix."""
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
        """Strip the namespace prefix (supports both an explicitly given namespace and
        automatic detection).

        ``MQ_INST_XX%Topic`` -> ``Topic``; ``%RETRY%MQ_INST_XX%GID`` -> ``%RETRY%GID``.
        The value is returned unchanged when it does not carry that namespace.
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
        """Prepend the namespace prefix."""
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
        """``%RETRY%<wrapNamespace(namespace, group)>``."""
        if not consumer_group:
            return consumer_group
        return MixAll.RETRY_GROUP_TOPIC_PREFIX + NamespaceUtil.wrap_namespace(
            namespace, consumer_group)

    @staticmethod
    def get_namespace_from_resource(resource: str) -> str:
        """Extract the namespace out of a resource name."""
        if not resource or NamespaceUtil.is_system_resource(resource):
            return ""
        plain = NamespaceUtil.with_out_retry_and_dlq(resource)
        index = plain.find(NAMESPACE_SEPARATOR)
        return plain[:index] if index > 0 else ""


__all__ = ["NamespaceUtil", "NAMESPACE_SEPARATOR"]
