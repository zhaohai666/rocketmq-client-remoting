// 命名空间工具（对应 org.apache.rocketmq.remoting.protocol.NamespaceUtil，
// 端口自 python/rocketmq/remoting/protocol/namespace_util.py）。
//
// 命名空间用于多租户隔离：客户端把 namespace 以 "namespace%" 前缀拼到 topic / group
// 上再发给 broker，从 broker 拿到的资源名在交给上层（listener / admin 结果）之前再剥掉。
//
// 对齐要点（勿凭直觉改）：
// - 分隔符是 '%'（不是 '/' 也不是 ':'）。
// - "%RETRY%" / "%DLQ%" 前缀**在**命名空间之外：%RETRY%NS%GID。
//   因此剥/拼都要先把 retry/DLQ 前缀摘下来处理，再拼回去。
// - 系统资源（rmq_sys_ 前缀 topic、CID_RMQ_SYS_ 前缀 group）**不**加命名空间。
using System.Globalization;

namespace RocketMQ.Common;

/// <summary>对应 Java NamespaceUtil（仅客户端用到的部分）。</summary>
public static class NamespaceUtil
{
    public const string NamespaceSeparator = "%";

    /// <summary>摘掉 retry/DLQ 前缀（对应 Java withOutRetryAndDLQ）。</summary>
    public static string WithOutRetryAndDlq(string resource) => MixAll.ResetRetryAndDlqTopic(resource);

    public static bool IsRetryTopic(string resource) => MixAll.IsRetryTopic(resource);

    public static bool IsDlqTopic(string resource) => MixAll.IsDlqTopic(resource);

    /// <summary>系统资源（rmq_sys_ 前缀 topic 或 CID_RMQ_SYS_ 前缀 group）不加命名空间。</summary>
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

    /// <summary>
    /// 剥掉命名空间前缀（对应 Java 两个重载的 withoutNamespace）。
    /// "MQ_INST_XX%Topic" → "Topic"；"%RETRY%MQ_INST_XX%GID" → "%RETRY%GID"。
    /// 未带该命名空间时原样返回。
    /// </summary>
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

    /// <summary>拼上命名空间前缀（对应 Java wrapNamespace）。</summary>
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

    /// <summary>"%RETRY%&lt;wrapNamespace(ns, group)&gt;"（对应 Java wrapNamespaceAndRetry）。</summary>
    public static string WrapNamespaceAndRetry(string ns, string consumerGroup)
    {
        if (string.IsNullOrEmpty(consumerGroup))
        {
            return consumerGroup;
        }

        return MixAll.RetryGroupTopicPrefix + WrapNamespace(ns, consumerGroup);
    }

    /// <summary>从资源名里取出命名空间（对应 Java getNamespaceFromResource）。</summary>
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
