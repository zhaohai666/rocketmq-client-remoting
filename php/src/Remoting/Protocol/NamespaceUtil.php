<?php

declare(strict_types=1);

namespace RocketMQ\Remoting\Protocol;

use RocketMQ\Common\MixAll;

/**
 * Namespace utilities.
 *
 * A namespace is used for multi-tenant isolation: the client prefixes ``namespace`` onto the topic /
 * group as ``namespace%`` before sending to the broker, and the resource names coming back from the
 * broker have the prefix stripped again before being handed up (to the listener, the admin results).
 *
 * Alignment points (do not change them out of intuition):
 * - The separator is ``%`` (not ``/`` and not ``:``).
 * - The ``%RETRY%`` / ``%DLQ%`` prefixes are **outside** the namespace: ``%RETRY%NS%GID``. So both
 *   stripping and wrapping have to take the retry/DLQ prefix off first, process it, then put it back.
 * - System resources (topics with the ``rmq_sys_`` prefix, groups with the ``CID_RMQ_SYS_`` prefix)
 *   **do not** get a namespace.
 */
final class NamespaceUtil
{
    public const NAMESPACE_SEPARATOR = '%';

    /** Strips the ``%RETRY%`` / ``%DLQ%`` prefix, leaving only the namespace + resource name. */
    public static function withOutRetryAndDlq(string $resource): string
    {
        return (string)MixAll::resetRetryAndDlqTopic($resource);
    }

    public static function isRetryTopic(string $resource): bool
    {
        return MixAll::isRetryTopic($resource);
    }

    public static function isDlqTopic(string $resource): bool
    {
        return MixAll::isDlqTopic($resource);
    }

    public static function isSystemResource(string $resource): bool
    {
        if ($resource === '') {
            return false;
        }
        return MixAll::isSysTopic($resource) || MixAll::isSysConsumerGroup($resource);
    }

    public static function isAlreadyWithNamespace(string $resource, string $namespace): bool
    {
        if ($namespace === '' || $resource === '' || self::isSystemResource($resource)) {
            return false;
        }
        $plain = self::withOutRetryAndDlq($resource);
        return str_starts_with($plain, $namespace . self::NAMESPACE_SEPARATOR);
    }

    /**
     * Strips the namespace prefix.
     *
     * ``MQ_INST_XX%Topic`` → ``Topic``; ``%RETRY%MQ_INST_XX%GID`` → ``%RETRY%GID``.
     * Returns the input unchanged when it does not carry that namespace.
     */
    public static function withoutNamespace(string $resourceWithNamespace, string $namespace = ''): string
    {
        if ($resourceWithNamespace === '') {
            return $resourceWithNamespace;
        }
        if ($namespace !== '') {
            $plain = self::withOutRetryAndDlq($resourceWithNamespace);
            if (!str_starts_with($plain, $namespace . self::NAMESPACE_SEPARATOR)) {
                return $resourceWithNamespace;
            }
        } elseif (self::isSystemResource($resourceWithNamespace)) {
            return $resourceWithNamespace;
        }
        $prefix = '';
        if (self::isRetryTopic($resourceWithNamespace)) {
            $prefix = MixAll::RETRY_GROUP_TOPIC_PREFIX;
        }
        if (self::isDlqTopic($resourceWithNamespace)) {
            $prefix = MixAll::DLQ_GROUP_TOPIC_PREFIX;
        }
        $plain = self::withOutRetryAndDlq($resourceWithNamespace);
        $index = strpos($plain, self::NAMESPACE_SEPARATOR);
        if ($index !== false && $index > 0) {
            return $prefix . substr($plain, $index + 1);
        }
        return $resourceWithNamespace;
    }

    /** Prefixes the namespace (idempotent: an already-prefixed value or a system resource is returned as-is). */
    public static function wrapNamespace(string $namespace, string $resourceWithoutNamespace): string
    {
        if ($namespace === '' || $resourceWithoutNamespace === '') {
            return $resourceWithoutNamespace;
        }
        if (self::isSystemResource($resourceWithoutNamespace)) {
            return $resourceWithoutNamespace;
        }
        if (self::isAlreadyWithNamespace($resourceWithoutNamespace, $namespace)) {
            return $resourceWithoutNamespace;
        }
        $prefix = '';
        if (self::isRetryTopic($resourceWithoutNamespace)) {
            $prefix = MixAll::RETRY_GROUP_TOPIC_PREFIX;
        }
        if (self::isDlqTopic($resourceWithoutNamespace)) {
            $prefix = MixAll::DLQ_GROUP_TOPIC_PREFIX;
        }
        $plain = self::withOutRetryAndDlq($resourceWithoutNamespace);
        return sprintf('%s%s%s%s', $prefix, $namespace, self::NAMESPACE_SEPARATOR, $plain);
    }

    /** The resource name wrapping for a retry topic: ``%RETRY%<namespace%group>``. */
    public static function wrapNamespaceAndRetry(string $namespace, string $consumerGroup): string
    {
        if ($consumerGroup === '') {
            return $consumerGroup;
        }
        return MixAll::RETRY_GROUP_TOPIC_PREFIX . self::wrapNamespace($namespace, $consumerGroup);
    }

    /** Extracts the namespace from a resource name; returns an empty string when there is none. */
    public static function getNamespaceFromResource(string $resource): string
    {
        if ($resource === '' || self::isSystemResource($resource)) {
            return '';
        }
        $plain = self::withOutRetryAndDlq($resource);
        $index = strpos($plain, self::NAMESPACE_SEPARATOR);
        return ($index !== false && $index > 0) ? substr($plain, 0, $index) : '';
    }
}
