// Namespaces provide multi-tenant isolation: the client prefixes the namespace as
// "namespace%" onto topic / group before sending to the broker, and strips it off
// resource names received from the broker before handing them to upper layers
// (listeners / admin results).
//
// Alignment notes (do not change by intuition):
// - The separator is '%' (not '/' and not ':').
// - The "%RETRY%" / "%DLQ%" prefixes are **outside** the namespace: %RETRY%NS%GID.
//   So strip/wrap must first detach the retry/DLQ prefix, then re-attach it.
// - System resources (rmq_sys_-prefixed topics, CID_RMQ_SYS_-prefixed groups) get **no** namespace.
using System.Globalization;

namespace RocketMQ.Common;

public static class NamespaceUtil
{
    public const string NamespaceSeparator = "%";

    public static string WithOutRetryAndDlq(string resource) => MixAll.ResetRetryAndDlqTopic(resource);

    public static bool IsRetryTopic(string resource) => MixAll.IsRetryTopic(resource);

    public static bool IsDlqTopic(string resource) => MixAll.IsDlqTopic(resource);

    /// <summary>System resources (rmq_sys_-prefixed topics or CID_RMQ_SYS_-prefixed groups) get no namespace.</summary>
    public static bool IsSystemResource(string resource)
    {
        if (string.IsNullOrEmpty(resource))
        {
            return false;
        }

        return MixAll.IsSysTopic(resource) || MixAll.IsSysConsumerGroup(resource);
    }

    public static bool IsAlreadyWithNamespace(string resource, string ns)
    {
        if (string.IsNullOrEmpty(ns) || string.IsNullOrEmpty(resource) || IsSystemResource(resource))
        {
            return false;
        }

        string plain = WithOutRetryAndDlq(resource);
        return plain.StartsWith(ns + NamespaceSeparator, StringComparison.Ordinal);
    }

    public static string WithoutNamespace(string resourceWithNamespace, string ns = "")
    {
        if (string.IsNullOrEmpty(resourceWithNamespace))
        {
            return resourceWithNamespace;
        }

        if (!string.IsNullOrEmpty(ns))
        {
            string plain = WithOutRetryAndDlq(resourceWithNamespace);
            if (!plain.StartsWith(ns + NamespaceSeparator, StringComparison.Ordinal))
            {
                return resourceWithNamespace;
            }
        }
        else if (IsSystemResource(resourceWithNamespace))
        {
            return resourceWithNamespace;
        }

        string prefix = string.Empty;
        if (IsRetryTopic(resourceWithNamespace))
        {
            prefix = MixAll.RetryGroupTopicPrefix;
        }

        if (IsDlqTopic(resourceWithNamespace))
        {
            prefix = MixAll.DlqGroupTopicPrefix;
        }

        string plainRes = WithOutRetryAndDlq(resourceWithNamespace);
        int index = plainRes.IndexOf(NamespaceSeparator, StringComparison.Ordinal);
        if (index > 0)
        {
            return prefix + plainRes[(index + 1)..];
        }

        return resourceWithNamespace;
    }

    public static string WrapNamespace(string ns, string resourceWithoutNamespace)
    {
        if (string.IsNullOrEmpty(ns) || string.IsNullOrEmpty(resourceWithoutNamespace))
        {
            return resourceWithoutNamespace;
        }

        if (IsSystemResource(resourceWithoutNamespace))
        {
            return resourceWithoutNamespace;
        }

        if (IsAlreadyWithNamespace(resourceWithoutNamespace, ns))
        {
            return resourceWithoutNamespace;
        }

        string prefix = string.Empty;
        if (IsRetryTopic(resourceWithoutNamespace))
        {
            prefix = MixAll.RetryGroupTopicPrefix;
        }

        if (IsDlqTopic(resourceWithoutNamespace))
        {
            prefix = MixAll.DlqGroupTopicPrefix;
        }

        string plain = WithOutRetryAndDlq(resourceWithoutNamespace);
        return prefix + ns + NamespaceSeparator + plain;
    }

    public static string WrapNamespaceAndRetry(string ns, string consumerGroup)
    {
        if (string.IsNullOrEmpty(consumerGroup))
        {
            return consumerGroup;
        }

        return MixAll.RetryGroupTopicPrefix + WrapNamespace(ns, consumerGroup);
    }

    public static string GetNamespaceFromResource(string resource)
    {
        if (string.IsNullOrEmpty(resource) || IsSystemResource(resource))
        {
            return string.Empty;
        }

        string plain = WithOutRetryAndDlq(resource);
        int index = plain.IndexOf(NamespaceSeparator, StringComparison.Ordinal);
        return index > 0 ? plain[..index] : string.Empty;
    }
}
