// W3C Trace Context (traceparent) propagation —— message-level context for OpenTracing/OTel scenarios.
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

    /// <summary>Generate a valid root traceparent: `00-&lt;32hex&gt;-&lt;16hex&gt;-01` (records sampling).</summary>
    public static string Generate()
    {
        return "00-" + RandomHex(32) + "-" + RandomHex(16) + "-01";
    }

    /// <summary>Validate by W3C syntax and the "not all zeros" rule. Case-insensitive hex is accepted leniently (forwarding does not rewrite).</summary>
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

    /// <summary>Generate a child span under the same trace-id (changes parent-id); returns null if parent is invalid.</summary>
    public static string? Child(string? parent)
    {
        if (!IsValid(parent))
        {
            return null;
        }

        string[] parts = parent!.Trim().Split('-');
        return "00-" + parts[1].ToLowerInvariant() + "-" + RandomHex(16) + "-01";
    }

    /// <summary>Inject the root context when the message has no traceparent property; returns the (injected) value.</summary>
    public static string Inject(Message msg)
    {
        string existing = msg.GetProperty(TraceContextProperty);
        if (!string.IsNullOrEmpty(existing))
        {
            return existing;   // upstream propagation takes priority, do not overwrite
        }

        string tp = Generate();
        msg.PutProperty(TraceContextProperty, tp);
        return tp;
    }

    /// <summary>Read the traceparent from the message properties (returns null if not injected / empty).</summary>
    public static string? Extract(MessageExt msg)
    {
        string v = msg.GetProperty(TraceContextProperty);
        return string.IsNullOrEmpty(v) ? null : v;
    }

    /// <summary>env `ROCKETMQ_TRACE_CONTEXT_ENABLE`.</summary>
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
