package common

import "strings"

// Namespace helpers.
//
// A namespace gives multi-tenant isolation: the client prefixes resource names
// with `namespace%` before talking to the broker, and strips the prefix again
// before handing names back to callers (listeners, admin results).
//
// Facts that are easy to get wrong — do not "simplify" these away:
//   - the separator is `%` (not `/`, not `:`);
//   - the `%RETRY%` / `%DLQ%` prefix sits OUTSIDE the namespace, so the fully
//     qualified retry topic is `%RETRY%NS%GID`. Both wrap and unwrap must strip
//     the retry/DLQ prefix, work on the remainder, then put it back;
//   - system resources (topics starting with `rmq_sys_`, groups starting with
//     `CID_RMQ_SYS_`) are NEVER namespaced.

// NamespaceSeparator is the namespace delimiter "%" inside resource names.
const NamespaceSeparator = "%"

// SystemConsumerGroupPrefix marks broker-managed system consumer groups.
const SystemConsumerGroupPrefix = "CID_RMQ_SYS_"

// IsSysTopic matches the `rmq_sys_` prefix only (this
// is deliberately weaker than IsSystemTopic, which also matches the fixed set
// of broker topics — the namespace rules use this weaker form).
func IsSysTopic(topic string) bool { return strings.HasPrefix(topic, SystemTopicPrefix) }

// IsSysConsumerGroup matches the CID_RMQ_SYS_ prefix.
func IsSysConsumerGroup(group string) bool {
	return strings.HasPrefix(group, SystemConsumerGroupPrefix)
}

// WithoutRetryAndDLQ strips the %RETRY% / %DLQ% prefix.
func WithoutRetryAndDLQ(resource string) string { return ResetRetryAndDLQTopic(resource) }

// IsSystemResource reports whether the resource is exempt from namespacing.
func IsSystemResource(resource string) bool {
	if resource == "" {
		return false
	}
	return IsSysTopic(resource) || IsSysConsumerGroup(resource)
}

// IsAlreadyWithNamespace reports whether the resource already carries the
// namespace prefix.
func IsAlreadyWithNamespace(resource, namespace string) bool {
	if namespace == "" || resource == "" || IsSystemResource(resource) {
		return false
	}
	plain := WithoutRetryAndDLQ(resource)
	return strings.HasPrefix(plain, namespace+NamespaceSeparator)
}

// WithoutNamespace strips the namespace prefix. `MQ_INST_XX%Topic` -> `Topic`, `%RETRY%MQ_INST_XX%GID` ->
// `%RETRY%GID`. An empty namespace means "drop whatever namespace is there";
// a non-empty one only strips when it actually matches.
func WithoutNamespace(resourceWithNamespace, namespace string) string {
	if resourceWithNamespace == "" {
		return resourceWithNamespace
	}
	if namespace != "" {
		plain := WithoutRetryAndDLQ(resourceWithNamespace)
		if !strings.HasPrefix(plain, namespace+NamespaceSeparator) {
			return resourceWithNamespace
		}
	} else if IsSystemResource(resourceWithNamespace) {
		return resourceWithNamespace
	}
	prefix := ""
	if IsRetryTopic(resourceWithNamespace) {
		prefix = RetryGroupTopicPrefix
	}
	if IsDLQTopic(resourceWithNamespace) {
		prefix = DLQGroupTopicPrefix
	}
	plain := WithoutRetryAndDLQ(resourceWithNamespace)
	index := strings.Index(plain, NamespaceSeparator)
	if index > 0 {
		return prefix + plain[index+1:]
	}
	return resourceWithNamespace
}

// WrapNamespace prefixes a resource with the namespace. It is idempotent: a resource that already
// carries the namespace, or any system resource, comes back unchanged.
func WrapNamespace(namespace, resourceWithoutNamespace string) string {
	if namespace == "" || resourceWithoutNamespace == "" {
		return resourceWithoutNamespace
	}
	if IsSystemResource(resourceWithoutNamespace) {
		return resourceWithoutNamespace
	}
	if IsAlreadyWithNamespace(resourceWithoutNamespace, namespace) {
		return resourceWithoutNamespace
	}
	prefix := ""
	if IsRetryTopic(resourceWithoutNamespace) {
		prefix = RetryGroupTopicPrefix
	}
	if IsDLQTopic(resourceWithoutNamespace) {
		prefix = DLQGroupTopicPrefix
	}
	plain := WithoutRetryAndDLQ(resourceWithoutNamespace)
	return prefix + namespace + NamespaceSeparator + plain
}

// WrapNamespaceAndRetry builds `%RETRY%<wrapNamespace(namespace, group)>`.
func WrapNamespaceAndRetry(namespace, consumerGroup string) string {
	if consumerGroup == "" {
		return consumerGroup
	}
	return RetryGroupTopicPrefix + WrapNamespace(namespace, consumerGroup)
}

// GetNamespaceFromResource extracts the namespace out of a resource name;
// "" means "none".
func GetNamespaceFromResource(resource string) string {
	if resource == "" || IsSystemResource(resource) {
		return ""
	}
	plain := WithoutRetryAndDLQ(resource)
	index := strings.Index(plain, NamespaceSeparator)
	if index > 0 {
		return plain[:index]
	}
	return ""
}
