<?php

declare(strict_types=1);

namespace RocketMQ\Remoting\Protocol;

use RocketMQ\Common\MixAll;

/**
 * 命名空间工具（对应 org.apache.rocketmq.remoting.protocol.NamespaceUtil，移植自 protocol/namespace_util.py）。
 *
 * 命名空间用于多租户隔离：客户端把 ``namespace`` 以 ``namespace%`` 前缀拼到
 * topic / group 上再发给 broker，从 broker 拿到的资源名在交给上层（listener、
 * admin 结果）之前再剥掉前缀。
 *
 * 对齐要点（勿凭直觉改）：
 * - 分隔符是 ``%``（不是 ``/`` 也不是 ``:``）。
 * - ``%RETRY%`` / ``%DLQ%`` 前缀**在**命名空间之外：``%RETRY%NS%GID``。
 *   因此剥/拼都要先把 retry/DLQ 前缀摘下来处理，再拼回去。
 * - 系统资源（``rmq_sys_`` 前缀 topic、``CID_RMQ_SYS_`` 前缀 group）**不**加命名空间。
 */
final class NamespaceUtil
{
    public const NAMESPACE_SEPARATOR = '%';

    /** 摘掉 retry/DLQ 前缀（对应 Java withOutRetryAndDLQ）。 */
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
     * 剥掉命名空间前缀（对应 Java 两个重载的 withoutNamespace）。
     *
     * ``MQ_INST_XX%Topic`` → ``Topic``；``%RETRY%MQ_INST_XX%GID`` → ``%RETRY%GID``。
     * 未带该命名空间时原样返回。
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

    /** 拼上命名空间前缀（对应 Java wrapNamespace）。 */
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

    /** ``%RETRY%<wrapNamespace(namespace, group)>``（对应 Java wrapNamespaceAndRetry）。 */
    public static function wrapNamespaceAndRetry(string $namespace, string $consumerGroup): string
    {
        if ($consumerGroup === '') {
            return $consumerGroup;
        }
        return MixAll::RETRY_GROUP_TOPIC_PREFIX . self::wrapNamespace($namespace, $consumerGroup);
    }

    /** 从资源名里取出命名空间（对应 Java getNamespaceFromResource）。 */
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
