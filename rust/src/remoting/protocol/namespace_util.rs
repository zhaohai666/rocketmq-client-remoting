//! Namespaces provide multi-tenant isolation: the client prefixes `namespace` as `namespace%` onto
//! the topic / group before sending to the broker, and strips the prefix off resource names received from the broker before handing them upstream.
//!
//! Alignment points (do not change on intuition):
//! - The separator is `%` (not `/` and not `:`).
//! - The `%RETRY%` / `%DLQ%` prefixes sit **outside** the namespace: `%RETRY%NS%GID`.
//!   So both stripping and wrapping must first remove the retry/DLQ prefix, process, then put it back.
//! - System resources (`rmq_sys_` prefixed topics, `CID_RMQ_SYS_` prefixed groups) are **never** namespaced.

use crate::common::mix_all::MixAll;

pub const NAMESPACE_SEPARATOR: char = '%';

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NamespaceUtil;

impl NamespaceUtil {
    pub fn with_out_retry_and_dlq(resource: &str) -> String {
        if resource.is_empty() {
            return String::new();
        }
        MixAll::reset_retry_and_dlq_topic(Some(resource)).unwrap_or_default()
    }

    pub fn is_retry_topic(resource: &str) -> bool {
        MixAll::is_retry_topic(Some(resource))
    }

    pub fn is_dlq_topic(resource: &str) -> bool {
        MixAll::is_dlq_topic(Some(resource))
    }

    pub fn is_system_resource(resource: &str) -> bool {
        if resource.is_empty() {
            return false;
        }
        MixAll::is_sys_topic(Some(resource)) || MixAll::is_sys_consumer_group(Some(resource))
    }

    pub fn is_already_with_namespace(resource: &str, namespace: &str) -> bool {
        if namespace.is_empty() || resource.is_empty() || Self::is_system_resource(resource) {
            return false;
        }
        Self::with_out_retry_and_dlq(resource).starts_with(&format!("{namespace}{NAMESPACE_SEPARATOR}"))
    }

    /// `MQ_INST_XX%Topic` → `Topic`; `%RETRY%MQ_INST_XX%GID` → `%RETRY%GID`.
    /// Returns the input unchanged when the namespace is absent.
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
    fn wrap_namespace_matches_golden_cases() {
        assert_eq!(NamespaceUtil::wrap_namespace("MQ_INST_XX", "Topic"), "MQ_INST_XX%Topic");
        assert_eq!(
            NamespaceUtil::wrap_namespace("MQ_INST_XX", "%RETRY%GID"),
            "%RETRY%MQ_INST_XX%GID"
        );
        assert_eq!(
            NamespaceUtil::wrap_namespace("MQ_INST_XX", "%DLQ%GID"),
            "%DLQ%MQ_INST_XX%GID"
        );
        // Idempotent when the namespace is already present
        assert_eq!(
            NamespaceUtil::wrap_namespace("MQ_INST_XX", "MQ_INST_XX%Topic"),
            "MQ_INST_XX%Topic"
        );
        // Empty namespace / resource returns the input unchanged
        assert_eq!(NamespaceUtil::wrap_namespace("", "Topic"), "Topic");
        assert_eq!(NamespaceUtil::wrap_namespace("NS", ""), "");
        // A different namespace does not count as "already namespaced"
        assert_eq!(NamespaceUtil::wrap_namespace("NS2", "NS1%Topic"), "NS2%NS1%Topic");
    }

    #[test]
    fn without_namespace_matches_golden_cases() {
        assert_eq!(NamespaceUtil::without_namespace("MQ_INST_XX%Topic", ""), "Topic");
        assert_eq!(
            NamespaceUtil::without_namespace("%RETRY%MQ_INST_XX%GID", ""),
            "%RETRY%GID"
        );
        assert_eq!(
            NamespaceUtil::without_namespace("%DLQ%MQ_INST_XX%GID", ""),
            "%DLQ%GID"
        );
        // Returns unchanged when there is no namespace
        assert_eq!(NamespaceUtil::without_namespace("Topic", ""), "Topic");
        // Explicit namespace: strip only on a match
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
        // Empty namespace + no '%': unchanged
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
        assert_eq!(NamespaceUtil::wrap_namespace_and_retry("NS", ""), "");
        // A group passed in that already carries %RETRY%: a double prefix is produced (callers should pass a bare group);
        // this behavior is pinned here so nobody "helpfully" deduplicates it.
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
