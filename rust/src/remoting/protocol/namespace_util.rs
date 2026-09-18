//! 命名空间工具（对应 Java `org.apache.rocketmq.remoting.protocol.NamespaceUtil`，
//! 参考实现 `python/rocketmq/remoting/protocol/namespace_util.py`）。
//!
//! 命名空间用于多租户隔离：客户端把 `namespace` 以 `namespace%` 前缀拼到
//! topic / group 上再发给 broker，从 broker 拿到的资源名在交给上层之前再剥掉前缀。
//!
//! 对齐要点（勿凭直觉改）：
//! - 分隔符是 `%`（不是 `/` 也不是 `:`）。
//! - `%RETRY%` / `%DLQ%` 前缀**在**命名空间之外：`%RETRY%NS%GID`。
//!   因此剥/拼都要先把 retry/DLQ 前缀摘下来处理，再拼回去。
//! - 系统资源（`rmq_sys_` 前缀 topic、`CID_RMQ_SYS_` 前缀 group）**不**加命名空间。

use crate::common::mix_all::MixAll;

/// 对应 Java `NamespaceUtil.NAMESPACE_SEPARATOR`。
pub const NAMESPACE_SEPARATOR: char = '%';

/// 对应 Java `NamespaceUtil`（纯静态类）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NamespaceUtil;

impl NamespaceUtil {
    /// 对应 Java `NamespaceUtil.withOutRetryAndDLQ`。
    pub fn with_out_retry_and_dlq(resource: &str) -> String {
        if resource.is_empty() {
            return String::new();
        }
        MixAll::reset_retry_and_dlq_topic(Some(resource)).unwrap_or_default()
    }

    /// 对应 Java `NamespaceUtil.isRetryTopic`。
    pub fn is_retry_topic(resource: &str) -> bool {
        MixAll::is_retry_topic(Some(resource))
    }

    /// 对应 Java `NamespaceUtil.isDLQTopic`。
    pub fn is_dlq_topic(resource: &str) -> bool {
        MixAll::is_dlq_topic(Some(resource))
    }

    /// 对应 Java `NamespaceUtil.isSystemResource`。
    ///
    /// ⚠ Python 与 Java 这里不一致：Java 用 `TopicValidator.isSystemTopic`
    /// （固定系统 topic 名单 **或** `rmq_sys_` 前缀），Python 只看 `rmq_sys_` 前缀。
    /// 本实现按 Python，即 `TBW102` / `SCHEDULE_TOPIC_XXXX` 等在 Java 里不加命名空间的
    /// topic 在这里仍会被加上命名空间。
    pub fn is_system_resource(resource: &str) -> bool {
        if resource.is_empty() {
            return false;
        }
        MixAll::is_sys_topic(Some(resource)) || MixAll::is_sys_consumer_group(Some(resource))
    }

    /// 对应 Java `NamespaceUtil.isAlreadyWithNamespace`。
    pub fn is_already_with_namespace(resource: &str, namespace: &str) -> bool {
        if namespace.is_empty() || resource.is_empty() || Self::is_system_resource(resource) {
            return false;
        }
        Self::with_out_retry_and_dlq(resource).starts_with(&format!("{namespace}{NAMESPACE_SEPARATOR}"))
    }

    /// 对应 Java `NamespaceUtil.withoutNamespace` 的两个重载（`namespace` 传空串
    /// 即 Java 的单参版本）。
    ///
    /// `MQ_INST_XX%Topic` → `Topic`；`%RETRY%MQ_INST_XX%GID` → `%RETRY%GID`。
    /// 未带该命名空间时原样返回。
    pub fn without_namespace(resource_with_namespace: &str, namespace: &str) -> String {
        if resource_with_namespace.is_empty() {
            return resource_with_namespace.to_string();
        }
        if namespace.is_empty() {
            if Self::is_system_resource(resource_with_namespace) {
                return resource_with_namespace.to_string();
            }
        } else if !Self::with_out_retry_and_dlq(resource_with_namespace)
            .starts_with(&format!("{namespace}{NAMESPACE_SEPARATOR}"))
        {
            return resource_with_namespace.to_string();
        }

        let mut prefix = String::new();
        if Self::is_retry_topic(resource_with_namespace) {
            prefix.push_str(MixAll::RETRY_GROUP_TOPIC_PREFIX);
        }
        if Self::is_dlq_topic(resource_with_namespace) {
            prefix.clear();
            prefix.push_str(MixAll::DLQ_GROUP_TOPIC_PREFIX);
        }
        let plain = Self::with_out_retry_and_dlq(resource_with_namespace);
        match plain.find(NAMESPACE_SEPARATOR) {
            Some(index) if index > 0 => {
                prefix.push_str(&plain[index + NAMESPACE_SEPARATOR.len_utf8()..]);
                prefix
            }
            _ => resource_with_namespace.to_string(),
        }
    }

    /// 对应 Java `NamespaceUtil.wrapNamespace`。
    pub fn wrap_namespace(namespace: &str, resource_without_namespace: &str) -> String {
        if namespace.is_empty() || resource_without_namespace.is_empty() {
            return resource_without_namespace.to_string();
        }
        if Self::is_system_resource(resource_without_namespace)
            || Self::is_already_with_namespace(resource_without_namespace, namespace)
        {
            return resource_without_namespace.to_string();
        }
        let mut prefix = String::new();
        if Self::is_retry_topic(resource_without_namespace) {
            prefix.push_str(MixAll::RETRY_GROUP_TOPIC_PREFIX);
        }
        if Self::is_dlq_topic(resource_without_namespace) {
            prefix.clear();
            prefix.push_str(MixAll::DLQ_GROUP_TOPIC_PREFIX);
        }
        let plain = Self::with_out_retry_and_dlq(resource_without_namespace);
        prefix.push_str(namespace);
        prefix.push(NAMESPACE_SEPARATOR);
        prefix.push_str(&plain);
        prefix
    }

    /// 对应 Java `NamespaceUtil.wrapNamespaceAndRetry`：
    /// `%RETRY%<wrapNamespace(namespace, group)>`。
    ///
    /// ⚠ Java 在 group 为空时返回 `null`，Python 返回入参（空串）；这里跟 Python。
    pub fn wrap_namespace_and_retry(namespace: &str, consumer_group: &str) -> String {
        if consumer_group.is_empty() {
            return consumer_group.to_string();
        }
        format!(
            "{}{}",
            MixAll::RETRY_GROUP_TOPIC_PREFIX,
            Self::wrap_namespace(namespace, consumer_group)
        )
    }

    /// 对应 Java `NamespaceUtil.getNamespaceFromResource`。
    pub fn get_namespace_from_resource(resource: &str) -> String {
        if resource.is_empty() || Self::is_system_resource(resource) {
            return String::new();
        }
        let plain = Self::with_out_retry_and_dlq(resource);
        match plain.find(NAMESPACE_SEPARATOR) {
            Some(index) if index > 0 => plain[..index].to_string(),
            _ => String::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn separator_and_prefix_helpers() {
        assert_eq!(NAMESPACE_SEPARATOR, '%');
        assert_eq!(NamespaceUtil::with_out_retry_and_dlq("%RETRY%MQ_INST_XX%GID"), "MQ_INST_XX%GID");
        assert_eq!(NamespaceUtil::with_out_retry_and_dlq("%DLQ%G"), "G");
        assert_eq!(NamespaceUtil::with_out_retry_and_dlq("Topic"), "Topic");
        assert_eq!(NamespaceUtil::with_out_retry_and_dlq(""), "");
        assert!(NamespaceUtil::is_retry_topic("%RETRY%G"));
        assert!(NamespaceUtil::is_dlq_topic("%DLQ%G"));
        assert!(!NamespaceUtil::is_dlq_topic("%RETRY%G"));
    }

    #[test]
    fn system_resources_are_never_touched() {
        assert!(NamespaceUtil::is_system_resource("rmq_sys_TRACE_DATA"));
        assert!(NamespaceUtil::is_system_resource("CID_RMQ_SYS_trans"));
        assert!(!NamespaceUtil::is_system_resource("TopicTest"));
        assert!(!NamespaceUtil::is_system_resource(""));
        assert_eq!(
            NamespaceUtil::wrap_namespace("MQ_INST_XX", "rmq_sys_TRACE_DATA"),
            "rmq_sys_TRACE_DATA"
        );
        assert_eq!(
            NamespaceUtil::without_namespace("rmq_sys_TRACE_DATA", ""),
            "rmq_sys_TRACE_DATA"
        );
        assert_eq!(NamespaceUtil::get_namespace_from_resource("CID_RMQ_SYS_x"), "");
    }

    #[test]
    fn wrap_namespace_matches_python_cases() {
        assert_eq!(NamespaceUtil::wrap_namespace("MQ_INST_XX", "Topic"), "MQ_INST_XX%Topic");
        assert_eq!(
            NamespaceUtil::wrap_namespace("MQ_INST_XX", "%RETRY%GID"),
            "%RETRY%MQ_INST_XX%GID"
        );
        assert_eq!(
            NamespaceUtil::wrap_namespace("MQ_INST_XX", "%DLQ%GID"),
            "%DLQ%MQ_INST_XX%GID"
        );
        // 已带命名空间时幂等
        assert_eq!(
            NamespaceUtil::wrap_namespace("MQ_INST_XX", "MQ_INST_XX%Topic"),
            "MQ_INST_XX%Topic"
        );
        // namespace / resource 为空则原样返回
        assert_eq!(NamespaceUtil::wrap_namespace("", "Topic"), "Topic");
        assert_eq!(NamespaceUtil::wrap_namespace("NS", ""), "");
        // 别的命名空间不算「已带」
        assert_eq!(NamespaceUtil::wrap_namespace("NS2", "NS1%Topic"), "NS2%NS1%Topic");
    }

    #[test]
    fn without_namespace_matches_python_cases() {
        assert_eq!(NamespaceUtil::without_namespace("MQ_INST_XX%Topic", ""), "Topic");
        assert_eq!(
            NamespaceUtil::without_namespace("%RETRY%MQ_INST_XX%GID", ""),
            "%RETRY%GID"
        );
        assert_eq!(
            NamespaceUtil::without_namespace("%DLQ%MQ_INST_XX%GID", ""),
            "%DLQ%GID"
        );
        // 没有命名空间时原样返回
        assert_eq!(NamespaceUtil::without_namespace("Topic", ""), "Topic");
        // 显式命名空间：命中才剥
        assert_eq!(NamespaceUtil::without_namespace("MQ_INST_XX1%Topic1", "MQ_INST_XX1"), "Topic1");
        assert_eq!(
            NamespaceUtil::without_namespace("MQ_INST_XX2%Topic2", "MQ_INST_XX1"),
            "MQ_INST_XX2%Topic2"
        );
        assert_eq!(
            NamespaceUtil::without_namespace("%RETRY%MQ_INST_XX1%GID1", "MQ_INST_XX1"),
            "%RETRY%GID1"
        );
        assert_eq!(
            NamespaceUtil::without_namespace("%RETRY%MQ_INST_XX2%GID2", "MQ_INST_XX3"),
            "%RETRY%MQ_INST_XX2%GID2"
        );
        // namespace 传空 + 无 '%' ：原样
        assert_eq!(NamespaceUtil::without_namespace("", ""), "");
        assert_eq!(NamespaceUtil::without_namespace("%RETRY%GID", ""), "%RETRY%GID");
    }

    #[test]
    fn wrap_namespace_and_retry_cases() {
        assert_eq!(
            NamespaceUtil::wrap_namespace_and_retry("MQ_INST_XX", "GID"),
            "%RETRY%MQ_INST_XX%GID"
        );
        assert_eq!(NamespaceUtil::wrap_namespace_and_retry("", "GID"), "%RETRY%GID");
        // Python: group 为空时原样返回（Java 返回 null）
        assert_eq!(NamespaceUtil::wrap_namespace_and_retry("NS", ""), "");
        // 传进来的 group 已带 %RETRY%：Python 同理会得到双前缀（调用方应传裸 group），
        // 这里把这个行为钉住，别让人「顺手优化」成去重。
        assert_eq!(
            NamespaceUtil::wrap_namespace_and_retry("NS", "%RETRY%GID"),
            "%RETRY%%RETRY%NS%GID"
        );
    }

    #[test]
    fn get_namespace_from_resource_cases() {
        assert_eq!(
            NamespaceUtil::get_namespace_from_resource("MQ_INST_XX%Topic"),
            "MQ_INST_XX"
        );
        assert_eq!(
            NamespaceUtil::get_namespace_from_resource("%RETRY%MQ_INST_XX%GID"),
            "MQ_INST_XX"
        );
        assert_eq!(NamespaceUtil::get_namespace_from_resource("Topic"), "");
        assert_eq!(NamespaceUtil::get_namespace_from_resource(""), "");
        assert_eq!(NamespaceUtil::get_namespace_from_resource("%RETRY%GID"), "");
    }

    #[test]
    fn is_already_with_namespace_cases() {
        assert!(NamespaceUtil::is_already_with_namespace("NS%Topic", "NS"));
        assert!(NamespaceUtil::is_already_with_namespace("%RETRY%NS%GID", "NS"));
        assert!(!NamespaceUtil::is_already_with_namespace("NS%Topic", ""));
        assert!(!NamespaceUtil::is_already_with_namespace("", "NS"));
        assert!(!NamespaceUtil::is_already_with_namespace("NS%Topic", "Other"));
        assert!(!NamespaceUtil::is_already_with_namespace("CID_RMQ_SYS_NS%g", "CID_RMQ_SYS_NS"));
    }

    #[test]
    fn wrap_then_without_round_trips() {
        for resource in ["Topic", "%RETRY%GID", "%DLQ%GID", "MQ_INST_1%Topic"] {
            let wrapped = NamespaceUtil::wrap_namespace("MQ_INST_XX", resource);
            let unwrapped = NamespaceUtil::without_namespace(&wrapped, "MQ_INST_XX");
            let expected = if resource == "MQ_INST_1%Topic" {
                "MQ_INST_1%Topic"
            } else {
                resource
            };
            assert_eq!(unwrapped, expected, "resource={resource} wrapped={wrapped}");
        }
    }
}
