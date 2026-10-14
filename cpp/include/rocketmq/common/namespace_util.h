// The namespace provides multi-tenant isolation: the client prepends ``namespace%`` to the
// topic / group before sending to the broker, and strips the prefix again from resource names
// returned by the broker before handing them to the upper layer (listener, admin results).
//
// Points that must stay exact (do not change them on intuition):
//   - The separator is ``%`` (neither ``/`` nor ``:``).
//   - The ``%RETRY%`` / ``%DLQ%`` prefixes sit **outside** the namespace: ``%RETRY%NS%GID``.
//     So stripping and adding both remove the retry/DLQ prefix first, then reattach it.
//   - System resources (``rmq_sys_`` prefixed topics, ``CID_RMQ_SYS_`` prefixed groups) get **no** namespace.
#ifndef ROCKETMQ_COMMON_NAMESPACE_UTIL_H
#define ROCKETMQ_COMMON_NAMESPACE_UTIL_H

#include <string>

#include "rocketmq/common/mix_all.h"

namespace rocketmq {

struct NamespaceUtil {
    static constexpr const char* NAMESPACE_SEPARATOR = "%";

    static std::string withOutRetryAndDlq(const std::string& resource) {
        return MixAll::resetRetryAndDlqTopic(resource);
    }

    static bool isRetryTopic(const std::string& resource) {
        return MixAll::isRetryTopic(resource);
    }

    static bool isDlqTopic(const std::string& resource) {
        return MixAll::isDlqTopic(resource);
    }

    // System resources (rmq_sys_* topics / CID_RMQ_SYS_* groups) get no namespace.
    static bool isSystemResource(const std::string& resource) {
        if (resource.empty()) return false;
        return MixAll::isSysTopic(resource) || MixAll::isSysConsumerGroup(resource);
    }

    // Whether resource already carries the given namespace prefix (NS%plain).
    static bool isAlreadyWithNamespace(const std::string& resource, const std::string& ns) {
        if (ns.empty() || resource.empty() || isSystemResource(resource)) return false;
        std::string plain = withOutRetryAndDlq(resource);
        return plain.rfind(ns + NAMESPACE_SEPARATOR, 0) == 0;
    }

    static std::string withoutNamespace(const std::string& resourceWithNamespace,
                                         const std::string& ns = std::string()) {
        if (resourceWithNamespace.empty()) return resourceWithNamespace;
        if (!ns.empty()) {
            std::string plain = withOutRetryAndDlq(resourceWithNamespace);
            if (plain.rfind(ns + NAMESPACE_SEPARATOR, 0) != 0) {
                return resourceWithNamespace;
            }
        } else if (isSystemResource(resourceWithNamespace)) {
            return resourceWithNamespace;
        }
        std::string prefix;
        if (isRetryTopic(resourceWithNamespace)) prefix = MixAll::RETRY_GROUP_TOPIC_PREFIX;
        if (isDlqTopic(resourceWithNamespace)) prefix = MixAll::DLQ_GROUP_TOPIC_PREFIX;
        std::string plain = withOutRetryAndDlq(resourceWithNamespace);
        size_t idx = plain.find(NAMESPACE_SEPARATOR);
        if (idx != std::string::npos && idx > 0) {
            return prefix + plain.substr(idx + 1);
        }
        return resourceWithNamespace;
    }

    static std::string wrapNamespace(const std::string& ns,
                                     const std::string& resourceWithoutNamespace) {
        if (ns.empty() || resourceWithoutNamespace.empty()) return resourceWithoutNamespace;
        if (isSystemResource(resourceWithoutNamespace)) return resourceWithoutNamespace;
        if (isAlreadyWithNamespace(resourceWithoutNamespace, ns)) return resourceWithoutNamespace;
        std::string prefix;
        if (isRetryTopic(resourceWithoutNamespace)) prefix = MixAll::RETRY_GROUP_TOPIC_PREFIX;
        if (isDlqTopic(resourceWithoutNamespace)) prefix = MixAll::DLQ_GROUP_TOPIC_PREFIX;
        std::string plain = withOutRetryAndDlq(resourceWithoutNamespace);
        return prefix + ns + NAMESPACE_SEPARATOR + plain;
    }

    static std::string wrapNamespaceAndRetry(const std::string& ns,
                                             const std::string& consumerGroup) {
        if (consumerGroup.empty()) return consumerGroup;
        return std::string(MixAll::RETRY_GROUP_TOPIC_PREFIX) + wrapNamespace(ns, consumerGroup);
    }

    static std::string getNamespaceFromResource(const std::string& resource) {
        if (resource.empty() || isSystemResource(resource)) return std::string();
        std::string plain = withOutRetryAndDlq(resource);
        size_t idx = plain.find(NAMESPACE_SEPARATOR);
        return idx != std::string::npos && idx > 0 ? plain.substr(0, idx) : std::string();
    }
};

}  // namespace rocketmq

#endif  // ROCKETMQ_COMMON_NAMESPACE_UTIL_H
