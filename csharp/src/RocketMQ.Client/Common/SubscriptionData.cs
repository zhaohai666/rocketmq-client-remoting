// 用途：Consumer 注册到 broker 的心跳里携带 SubscriptionData，broker 依据
// tagsSet / codeSet 做 TAG 过滤；subString 为 "||" 分隔的 tag 表达式。
using System;
using System.Collections.Generic;
using System.Globalization;
using RocketMQ.Remoting.Protocol;

namespace RocketMQ.Common;

public static class ExpressionType
{
    public const string TAG = "TAG";
    public const string Sql92 = "SQL92";
    public const string ClassFilter = "CLASS_FILTER";
}

public sealed class SubscriptionData : IComparable<SubscriptionData>
{
    public bool ClassFilterMode { get; set; }
    public string Topic { get; set; } = string.Empty;
    public string SubString { get; set; } = string.Empty;
    public SortedSet<string> TagsSet { get; set; } = new SortedSet<string>(StringComparer.Ordinal);
    public SortedSet<long> CodeSet { get; set; } = new SortedSet<long>();
    public long SubVersion { get; set; }
    public string ExpressionType { get; set; } = RocketMQ.Common.ExpressionType.TAG;
    public string FilterClassSource { get; set; } = string.Empty;

    public SubscriptionData()
    {
        // 默认取当前毫秒时间戳（C++ 默认构造函数行为一致）
        SubVersion = UtilAll.CurrentTimeMillis();
    }

    public SubscriptionData(string topic, string subString)
    {
        Topic = topic;
        SubString = subString;
        SubVersion = UtilAll.CurrentTimeMillis();
    }

    // ---- JSON ----
    // 键按字母序，与 fastjson2 输出一致
    public JsonValue ToJson()
    {
        JsonValue o = JsonValue.MakeObject();
        o.Set("classFilterMode", JsonValue.MakeBool(ClassFilterMode));
        o.Set("codeSet", SetToJson(CodeSet));
        o.Set("expressionType", JsonValue.MakeString(ExpressionType));
        o.Set("subString", JsonValue.MakeString(SubString));
        o.Set("subVersion", JsonValue.MakeInt(SubVersion));
        o.Set("tagsSet", SetToJson(TagsSet));
        o.Set("topic", JsonValue.MakeString(Topic));
        return o;
    }

    public static SubscriptionData FromJson(JsonValue v)
    {
        SubscriptionData sd = new SubscriptionData();
        if (!v.IsObject)
        {
            return sd;
        }

        if (v.TryGetBool("classFilterMode", out bool b))
        {
            sd.ClassFilterMode = b;
        }

        if (v.TryGetString("expressionType", out string s1))
        {
            sd.ExpressionType = s1;
        }

        if (v.TryGetString("subString", out string s2))
        {
            sd.SubString = s2;
        }

        if (v.TryGetString("topic", out string s3))
        {
            sd.Topic = s3;
        }

        if (v.TryGetInt("subVersion", out long n))
        {
            sd.SubVersion = n;
        }

        JsonValue? tags = v.Find("tagsSet");
        if (tags is not null && tags.IsArray)
        {
            for (int i = 0; i < tags.Size(); i++)
            {
                sd.TagsSet.Add(tags.At(i).StringValue());
            }
        }

        JsonValue? codes = v.Find("codeSet");
        if (codes is not null && codes.IsArray)
        {
            for (int i = 0; i < codes.Size(); i++)
            {
                sd.CodeSet.Add(codes.At(i).IntValue());
            }
        }

        return sd;
    }

    public override bool Equals(object? obj)
    {
        if (obj is not SubscriptionData other)
        {
            return false;
        }

        return ClassFilterMode == other.ClassFilterMode
            && CodeSet.SetEquals(other.CodeSet)
            && SubString == other.SubString
            && SubVersion == other.SubVersion
            && TagsSet.SetEquals(other.TagsSet)
            && Topic == other.Topic
            && ExpressionType == other.ExpressionType;
    }

    public int CompareTo(SubscriptionData? other)
    {
        if (other is null)
        {
            return 1;
        }

        string a = Topic + "@" + SubString;
        string b = other.Topic + "@" + other.SubString;
        if (a == b)
        {
            return 0;
        }

        return a.CompareTo(b) < 0 ? -1 : 1;
    }

    public override int GetHashCode()
    {
        long result = 1;
        result = 31 * result + (ClassFilterMode ? 1231 : 1237);
        result = 31 * result + SetHashCode(CodeSet);
        result = 31 * result + LegacyHash.StringHash(SubString);
        result = 31 * result + SetHashCode(TagsSet);
        result = 31 * result + LegacyHash.StringHash(Topic);
        result = 31 * result + LegacyHash.StringHash(ExpressionType);
        result &= 0xFFFFFFFFL;
        return unchecked((int)result);
    }

    public static bool operator ==(SubscriptionData? left, SubscriptionData? right)
    {
        if (left is null)
        {
            return right is null;
        }

        return left.Equals(right);
    }

    public static bool operator !=(SubscriptionData? left, SubscriptionData? right) => !(left == right);

    public override string ToString()
    {
        return "SubscriptionData [classFilterMode=" + (ClassFilterMode ? "true" : "false")
            + ", topic=" + Topic + ", subString=" + SubString
            + ", tagsSet=" + SetToJson(TagsSet).Dump()
            + ", codeSet=" + SetToJson(CodeSet).Dump()
            + ", subVersion=" + SubVersion.ToString(CultureInfo.InvariantCulture)
            + ", expressionType=" + ExpressionType + "]";
    }

    private static int SetHashCode(SortedSet<string> s)
    {
        long h = 0;
        foreach (string e in s)
        {
            h += LegacyHash.StringHash(e);
            h &= 0xFFFFFFFFL;
        }

        return unchecked((int)h);
    }

    private static int SetHashCode(SortedSet<long> s)
    {
        long h = 0;
        foreach (long e in s)
        {
            h += e;
            h &= 0xFFFFFFFFL;
        }

        return unchecked((int)h);
    }

    private static JsonValue SetToJson(SortedSet<string> s)
    {
        JsonValue arr = JsonValue.MakeArray();
        foreach (string e in s)
        {
            arr.PushArray(JsonValue.MakeString(e));
        }

        return arr;
    }

    private static JsonValue SetToJson(SortedSet<long> s)
    {
        JsonValue arr = JsonValue.MakeArray();
        foreach (long e in s)
        {
            arr.PushArray(JsonValue.MakeInt(e));
        }

        return arr;
    }
}

public static class FilterAPI
{
    public const string SubAll = "*";

    public static SubscriptionData BuildSubscriptionData(string topic, string subString)
    {
        SubscriptionData sub = new SubscriptionData(topic, subString);
        if (string.IsNullOrEmpty(subString) || subString == SubAll)
        {
            sub.SubString = SubAll;
            return sub;
        }

        List<string> rawTags = new List<string>();
        int pos = 0;
        while (true)
        {
            int next = subString.IndexOf("||", pos, StringComparison.Ordinal);
            if (next < 0)
            {
                rawTags.Add(subString.Substring(pos));
                break;
            }

            rawTags.Add(subString.Substring(pos, next - pos));
            pos = next + 2;
        }

        while (rawTags.Count > 0 && rawTags[rawTags.Count - 1].Length == 0)
        {
            rawTags.RemoveAt(rawTags.Count - 1);
        }

        if (rawTags.Count == 0)
        {
            throw new ArgumentException("subString split error");
        }

        foreach (string raw in rawTags)
        {
            string tag = TrimTag(raw);
            if (tag.Length > 0)
            {
                sub.TagsSet.Add(tag);
                sub.CodeSet.Add(LegacyHash.StringHash(tag));
            }
        }

        return sub;
    }

    private static string TrimTag(string s)
    {
        int b = 0;
        while (b < s.Length && IsTagWs(s[b]))
        {
            b++;
        }

        int e = s.Length - 1;
        while (e >= b && IsTagWs(s[e]))
        {
            e--;
        }

        return b > e ? string.Empty : s.Substring(b, e - b + 1);
    }

    private static bool IsTagWs(char c) => c == ' ' || c == '\t' || c == '\r' || c == '\n';
}
