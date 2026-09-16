// 命名空间工具（对应 org.apache.rocketmq.remoting.protocol.NamespaceUtil）。
//
// 命名空间用于多租户隔离：客户端把 ``namespace`` 以 ``namespace%`` 前缀拼到
// topic / group 上再发给 broker，从 broker 拿到的资源名在交给上层（listener、
// admin 结果）之前再剥掉前缀。
//
// 对齐要点（勿凭直觉改）：
//   - 分隔符是 ``%``（不是 ``/`` 也不是 ``:``）。
//   - ``%RETRY%`` / ``%DLQ%`` 前缀**在**命名空间之外：``%RETRY%NS%GID``。
//     因此剥/拼都要先把 retry/DLQ 前缀摘下来处理，再拼回去。
//   - 系统资源（``rmq_sys_`` 前缀 topic、``CID_RMQ_SYS_`` 前缀 group）**不**加命名空间。
#ifndef ROCKETMQ_COMMON_NAMESPACE_UTIL_H
#define ROCKETMQ_COMMON_NAMESPACE_UTIL_H

#include <string>

#include "rocketmq/common/mix_all.h"

namespace rocketmq {

struct NamespaceUtil {
    static constexpr const char* NAMESPACE_SEPARATOR = "%";

    // 摘掉 retry/DLQ 前缀（对应 Java withOutRetryAndDLQ）。
    static std::string withOutRetryAndDlq(const std::string& resource) {
        return MixAll::resetRetryAndDlqTopic(resource);
    }

    static bool isRetryTopic(const std::string& resource) {
        return MixAll::isRetryTopic(resource);
    }

    static bool isDlqTopic(const std::string& resource) {
        return MixAll::isDlqTopic(resource);
    }

    // 系统资源（rmq_sys_* topic / CID_RMQ_SYS_* group）不加命名空间。
    static bool isSystemResource(const std::string& resource) {
        if (resource.empty()) return false;
        return MixAll::isSysTopic(resource) || MixAll::isSysConsumerGroup(resource);
    }

    // resource 是否已经带上了指定 namespace 前缀（NS%plain）。
    static bool isAlreadyWithNamespace(const std::string& resource, const std::string& ns) {
        if (ns.empty() || resource.empty() || isSystemResource(resource)) return false;
        std::string plain = withOutRetryAndDlq(resource);
        return plain.rfind(ns + NAMESPACE_SEPARATOR, 0) == 0;
    }

    // 剥掉命名空间前缀（对应 Java withoutNamespace 的两个重载）。
    //   "MQ_INST_XX%Topic" -> "Topic"；"%RETRY%MQ_INST_XX%GID" -> "%RETRY%GID"。
    // 未带该命名空间时原样返回。
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

    // 拼上命名空间前缀（对应 Java wrapNamespace）。
    //   plain -> "NS%plain"；"%RETRY%GID" -> "%RETRY%NS%GID"。
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

    // "%RETRY%<wrapNamespace(ns, group)>"（对应 Java wrapNamespaceAndRetry）。
    static std::string wrapNamespaceAndRetry(const std::string& ns,
                                             const std::string& consumerGroup) {
        if (consumerGroup.empty()) return consumerGroup;
        return std::string(MixAll::RETRY_GROUP_TOPIC_PREFIX) + wrapNamespace(ns, consumerGroup);
    }

    // 从资源名里取出命名空间（对应 Java getNamespaceFromResource）。
    static std::string getNamespaceFromResource(const std::string& resource) {
        if (resource.empty() || isSystemResource(resource)) return std::string();
        std::string plain = withOutRetryAndDlq(resource);
        size_t idx = plain.find(NAMESPACE_SEPARATOR);
        return idx != std::string::npos && idx > 0 ? plain.substr(0, idx) : std::string();
    }
};

}  // namespace rocketmq

#endif  // ROCKETMQ_COMMON_NAMESPACE_UTIL_H
