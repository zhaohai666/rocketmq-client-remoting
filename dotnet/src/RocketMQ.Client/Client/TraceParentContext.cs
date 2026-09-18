// W3C Trace Context（traceparent）透传 —— OpenTracing/OTel 场景的消息级上下文。
//
// 格式：`00-<trace-id 32hex>-<parent-id 16hex>-<flags 2hex>`；id 不能全 0。
// 生产侧：opt-in（EnableTraceContext / env ROCKETMQ_TRACE_CONTEXT_ENABLE），
// 发送路径注入根上下文；已有值**不覆盖**（上游传播优先）。键名沿用 W3C 小写
// `traceparent`。Java 客户端把这类注入交给外部链路追踪的 SendMessageHook，
// 本实现内建等价能力（有意差异：直接在发送路径注入）。
using System;
using System.Security.Cryptography;
using RocketMQ.Common;
using RocketMQ.Remoting.Protocol;

namespace RocketMQ.Client;

public static class TraceParentContext
{
    public const string TraceContextProperty = "traceparent";
    public const string TraceStateProperty = "tracestate";

    private const string HexDigits = "0123456789abcdef";

    /// <summary>生成合法的根 traceparent：`00-&lt;32hex&gt;-&lt;16hex&gt;-01`（记录采样）。</summary>
    public static string Generate()
    {
        return "00-" + RandomHex(32) + "-" + RandomHex(16) + "-01";
    }

    /// <summary>按 W3C 语法与"不全 0"规则校验。宽松接受大写 hex（转发不重写）。</summary>
    public static bool IsValid(string? value)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            return false;
        }

        string[] parts = value.Trim().Split('-');
        if (parts.Length != 4)
        {
            return false;
        }

        string version = parts[0];
        string traceId = parts[1].ToLowerInvariant();
        string parentId = parts[2].ToLowerInvariant();
        string flags = parts[3].ToLowerInvariant();

        if (version == "ff")
        {
            return false;
        }

        if (version != "00" && (version.Length != 2 || !AllHex(version)))
        {
            return false;
        }

        if (traceId.Length != 32 || !AllHex(traceId) || AllZero(traceId))
        {
            return false;
        }

        if (parentId.Length != 16 || !AllHex(parentId) || AllZero(parentId))
        {
            return false;
        }

        return flags.Length == 2 && AllHex(flags);
    }

    /// <summary>同一 trace-id 下生成子 span（换 parent-id）；parent 非法返回 null。</summary>
    public static string? Child(string? parent)
    {
        if (!IsValid(parent))
        {
            return null;
        }

        string[] parts = parent!.Trim().Split('-');
        return "00-" + parts[1].ToLowerInvariant() + "-" + RandomHex(16) + "-01";
    }

    /// <summary>消息没有 traceparent 属性时注入根上下文；返回（注入后的）值。</summary>
    public static string Inject(Message msg)
    {
        string existing = msg.GetProperty(TraceContextProperty);
        if (!string.IsNullOrEmpty(existing))
        {
            return existing;   // 上游传播优先，不覆盖
        }

        string tp = Generate();
        msg.PutProperty(TraceContextProperty, tp);
        return tp;
    }

    /// <summary>从消息属性里取出 traceparent（未注入/为空返回 null）。</summary>
    public static string? Extract(MessageExt msg)
    {
        string v = msg.GetProperty(TraceContextProperty);
        return string.IsNullOrEmpty(v) ? null : v;
    }

    /// <summary>env `ROCKETMQ_TRACE_CONTEXT_ENABLE`。</summary>
    public static bool EnabledFromEnv()
    {
        string? v = Environment.GetEnvironmentVariable("ROCKETMQ_TRACE_CONTEXT_ENABLE");
        if (v is null)
        {
            return false;
        }

        return v.Trim().ToLowerInvariant() is "1" or "true" or "yes";
    }

    private static string RandomHex(int n)
    {
        var bytes = RandomNumberGenerator.GetBytes(n / 2);
        var sb = new System.Text.StringBuilder(n);
        foreach (byte b in bytes)
        {
            sb.Append(HexDigits[(b >> 4) & 0xF]);
            sb.Append(HexDigits[b & 0xF]);
        }

        return sb.ToString();
    }

    private static bool AllHex(string s)
    {
        foreach (char c in s)
        {
            if (!Uri.IsHexDigit(c))
            {
                return false;
            }
        }

        return s.Length > 0;
    }

    private static bool AllZero(string s)
    {
        foreach (char c in s)
        {
            if (c != '0')
            {
                return false;
            }
        }

        return true;
    }
}
