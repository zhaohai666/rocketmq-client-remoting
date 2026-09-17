// ExtraInfoUtil 的 .NET 移植（对应 org.apache.rocketmq.remoting.protocol.header.ExtraInfoUtil）。
//
// POP 的 CK（checkpoint）串是 broker 与客户端交换消费进度的载体：
//   ckQueueOffset popTime invisibleTime reviveQid retryFlag brokerName queueId [msgQueueOffset]
//
// ⚠ 普通 topic 直连 POP 时 broker **不在消息上写** POP_CK，它是客户端用响应头的
//   startOffsetInfo/msgOffsetInfo 反构出来的（见 MqClient.StampPopCk）。没有它就发不了 ACK。
//
// 两个最容易错的细节：
//   1) 分段符是**空格**（MessageConst.KEY_SEPARATOR = " "），不是逗号；";" 只用于分隔
//      "多个队列" 的段。
//   2) Java 的 String.split(" ") 会**丢弃末尾空串**（limit=0 语义），C# 的 Split(' ') 不会。
//      这里必须显式丢弃，否则长度下限校验（如 GetQueueOffset 要求 ≥8 段）会与 Java 分歧。
using System.Globalization;
using RocketMQ.Common;

namespace RocketMQ.Remoting.Protocol;

public static class ExtraInfoUtil
{
    public const string NormalTopic = "0";
    public const string RetryTopic = "1";
    public const string RetryTopicV2 = "2";
    public const string QueueOffsetKeyPrefix = "qo";

    /// <summary>MessageConst.KEY_SEPARATOR —— 注意是空格。</summary>
    public const char KeySeparator = ' ';

    /// <summary>分隔「多个队列」的段。</summary>
    private const char QueueSplitter = ';';

    /// <summary>KeyBuilder.POP_ORDER_REVIVE_QUEUE —— reviveQid 为它表示顺序消费。</summary>
    public const int PopOrderReviveQueue = 999;

    /// <summary>
    /// Java ExtraInfoUtil.split：按空格切分并**丢弃末尾空串**（String.split 的 limit=0 语义）。
    /// </summary>
    public static string[] Split(string extraInfo)
    {
        if (extraInfo is null)
        {
            throw new ArgumentException("split extraInfo is null");
        }

        string[] raw = extraInfo.Split(KeySeparator);
        int end = raw.Length;
        while (end > 0 && raw[end - 1].Length == 0)
        {
            --end;
        }

        if (end == raw.Length)
        {
            return raw;
        }

        var trimmed = new string[end];
        Array.Copy(raw, trimmed, end);
        return trimmed;
    }

    public static long GetCkQueueOffset(string[] extraInfoStrs)
    {
        RequireLength(extraInfoStrs, 1, "getCkQueueOffset");
        return long.Parse(extraInfoStrs[0], CultureInfo.InvariantCulture);
    }

    public static long GetPopTime(string[] extraInfoStrs)
    {
        RequireLength(extraInfoStrs, 2, "getPopTime");
        return long.Parse(extraInfoStrs[1], CultureInfo.InvariantCulture);
    }

    public static long GetInvisibleTime(string[] extraInfoStrs)
    {
        RequireLength(extraInfoStrs, 3, "getInvisibleTime");
        return long.Parse(extraInfoStrs[2], CultureInfo.InvariantCulture);
    }

    public static int GetReviveQid(string[] extraInfoStrs)
    {
        RequireLength(extraInfoStrs, 4, "getReviveQid");
        return int.Parse(extraInfoStrs[3], CultureInfo.InvariantCulture);
    }

    public static string GetRetry(string[] extraInfoStrs)
    {
        RequireLength(extraInfoStrs, 5, "getRetry");
        return extraInfoStrs[4];
    }

    public static string GetBrokerName(string[] extraInfoStrs)
    {
        RequireLength(extraInfoStrs, 6, "getBrokerName");
        return extraInfoStrs[5];
    }

    public static int GetQueueId(string[] extraInfoStrs)
    {
        RequireLength(extraInfoStrs, 7, "getQueueId");
        return int.Parse(extraInfoStrs[6], CultureInfo.InvariantCulture);
    }

    public static long GetQueueOffset(string[] extraInfoStrs)
    {
        RequireLength(extraInfoStrs, 8, "getQueueOffset");
        return long.Parse(extraInfoStrs[7], CultureInfo.InvariantCulture);
    }

    /// <summary>Java ExtraInfoUtil.getRealTopic(String[] extraInfoStrs, topic, cid)：按第 5 段判定。</summary>
    public static string GetRealTopic(string[] extraInfoStrs, string topic, string cid)
    {
        RequireLength(extraInfoStrs, 5, "getRealTopic");
        return GetRealTopic(topic, cid, extraInfoStrs[4]);
    }

    /// <summary>
    /// retryFlag → 真实 topic。拼接规则来自 Java KeyBuilder.buildPopRetryTopicV1/V2：
    /// V1 = "%RETRY%{cid}_{topic}"，V2 = "%RETRY%{cid}+{topic}"。
    /// </summary>
    public static string GetRealTopic(string topic, string cid, string retry)
    {
        if (retry == NormalTopic)
        {
            return topic;
        }

        if (retry == RetryTopic)
        {
            return MixAll.RetryGroupTopicPrefix + cid + "_" + topic;
        }

        if (retry == RetryTopicV2)
        {
            return MixAll.RetryGroupTopicPrefix + cid + "+" + topic;
        }

        throw new ArgumentException("getRetry fail, format is wrong");
    }

    /// <summary>
    /// Java KeyBuilder.buildPopRetryTopicV1：POP 的复活/重投 topic 是
    /// "%RETRY%{cid}_{topic}"，**与 push 消费的 "%RETRY%{group}" 是两套不同的 topic**。
    /// </summary>
    public static string BuildPopRetryTopicV1(string topic, string cid) =>
        MixAll.RetryGroupTopicPrefix + cid + "_" + topic;

    /// <summary>Java KeyBuilder.buildPopRetryTopicV2（enableRetryTopicV2=true 时）。</summary>
    public static string BuildPopRetryTopicV2(string topic, string cid) =>
        MixAll.RetryGroupTopicPrefix + cid + "+" + topic;

    public static string BuildExtraInfo(long ckQueueOffset, long popTime, long invisibleTime,
        int reviveQid, string topic, string brokerName, int queueId)
    {
        return string.Join(KeySeparator,
            ckQueueOffset.ToString(CultureInfo.InvariantCulture),
            popTime.ToString(CultureInfo.InvariantCulture),
            invisibleTime.ToString(CultureInfo.InvariantCulture),
            reviveQid.ToString(CultureInfo.InvariantCulture),
            RetryOfTopic(topic),
            brokerName,
            queueId.ToString(CultureInfo.InvariantCulture));
    }

    public static string BuildExtraInfo(long ckQueueOffset, long popTime, long invisibleTime,
        int reviveQid, string topic, string brokerName, int queueId, long msgQueueOffset)
    {
        return BuildExtraInfo(ckQueueOffset, popTime, invisibleTime, reviveQid, topic, brokerName, queueId)
            + KeySeparator + msgQueueOffset.ToString(CultureInfo.InvariantCulture);
    }

    /// <summary>
    /// Java ExtraInfoUtil.parseStartOffsetInfo。空/null → null；每段必须正好 3 列，
    /// key = "{retry}@{queueId}"，重复 key 抛异常。
    /// </summary>
    public static Dictionary<string, long>? ParseStartOffsetInfo(string? startOffsetInfo)
    {
        var map = new Dictionary<string, long>(4);
        if (!ParseThreeColumnSegments(startOffsetInfo, "parse startOffsetInfo error", out string[][]? rows))
        {
            return null;
        }

        foreach (string[] row in rows!)
        {
            string key = row[0] + "@" + row[1];
            if (map.ContainsKey(key))
            {
                throw new ArgumentException(
                    "parse startOffsetInfo error, duplicate, " + ToDebugString(map));
            }

            map[key] = long.Parse(row[2], CultureInfo.InvariantCulture);
        }

        return map;
    }

    /// <summary>Java ExtraInfoUtil.parseMsgOffsetInfo：第 3 列是逗号分隔的 offset 列表。</summary>
    public static Dictionary<string, List<long>>? ParseMsgOffsetInfo(string? msgOffsetInfo)
    {
        var map = new Dictionary<string, List<long>>(4);
        if (!ParseThreeColumnSegments(msgOffsetInfo, "parse msgOffsetMap error", out string[][]? rows))
        {
            return null;
        }

        foreach (string[] row in rows!)
        {
            string key = row[0] + "@" + row[1];
            if (map.ContainsKey(key))
            {
                throw new ArgumentException("parse msgOffsetMap error, duplicate, " + ToDebugString(map));
            }

            var offsets = new List<long>(8);
            foreach (string one in row[2].Split(','))
            {
                offsets.Add(long.Parse(one, CultureInfo.InvariantCulture));
            }

            map[key] = offsets;
        }

        return map;
    }

    /// <summary>Java ExtraInfoUtil.parseOrderCountInfo：第 3 列是该队列顺序消费的计数。</summary>
    public static Dictionary<string, int>? ParseOrderCountInfo(string? orderCountInfo)
    {
        var map = new Dictionary<string, int>(4);
        if (!ParseThreeColumnSegments(orderCountInfo, "parse orderCountInfo error", out string[][]? rows))
        {
            return null;
        }

        foreach (string[] row in rows!)
        {
            string key = row[0] + "@" + row[1];
            if (map.ContainsKey(key))
            {
                throw new ArgumentException(
                    "parse orderCountInfo error, duplicate, " + ToDebugString(map));
            }

            map[key] = int.Parse(row[2], CultureInfo.InvariantCulture);
        }

        return map;
    }

    public static string GetStartOffsetInfoMapKey(string topic, long key) =>
        RetryOfTopic(topic) + "@" + key.ToString(CultureInfo.InvariantCulture);

    /// <summary>Java 的同名重载：popCk 非 null 时用 CK 的第 5 段当 retryFlag，否则退回按 topic 形状判定。</summary>
    public static string GetStartOffsetInfoMapKey(string topic, string? popCk, long key) =>
        (popCk is null ? RetryOfTopic(topic) : GetRetry(Split(popCk))) + "@"
            + key.ToString(CultureInfo.InvariantCulture);

    public static string GetQueueOffsetKeyValueKey(long queueId, long queueOffset) =>
        QueueOffsetKeyPrefix + queueId.ToString(CultureInfo.InvariantCulture) + "%"
            + queueOffset.ToString(CultureInfo.InvariantCulture);

    public static string GetQueueOffsetMapKey(string topic, long queueId, long queueOffset) =>
        RetryOfTopic(topic) + "@" + GetQueueOffsetKeyValueKey(queueId, queueOffset);

    /// <summary>Java ExtraInfoUtil.isOrder：reviveQid == POP_ORDER_REVIVE_QUEUE(999) 即顺序消费。</summary>
    public static bool IsOrder(string[] extraInfo) => GetReviveQid(extraInfo) == PopOrderReviveQueue;

    /// <summary>
    /// Java ExtraInfoUtil.getRetry(topic)。判定顺序很重要：先判 V2（"%RETRY%" 且含 '+'），
    /// 再判 "%RETRY%" 前缀（V1）。
    /// </summary>
    public static string RetryOfTopic(string topic)
    {
        if (IsPopRetryTopicV2(topic))
        {
            return RetryTopicV2;
        }

        return topic.StartsWith(MixAll.RetryGroupTopicPrefix, StringComparison.Ordinal)
            ? RetryTopic
            : NormalTopic;
    }

    /// <summary>Java KeyBuilder.isPopRetryTopicV2。</summary>
    public static bool IsPopRetryTopicV2(string retryTopic) =>
        retryTopic.StartsWith(MixAll.RetryGroupTopicPrefix, StringComparison.Ordinal)
            && retryTopic.Contains('+', StringComparison.Ordinal);

    private static void RequireLength(string[]? extraInfoStrs, int min, string caller)
    {
        if (extraInfoStrs is null || extraInfoStrs.Length < min)
        {
            throw new ArgumentException(
                caller + " fail, extraInfoStrs length " + (extraInfoStrs?.Length ?? 0));
        }
    }

    /// <summary>
    /// parseStartOffsetInfo / parseMsgOffsetInfo / parseOrderCountInfo 共用的切分：
    /// 空输入 → false（对应 Java 返回 null）；否则每段必须正好 3 列。
    /// </summary>
    private static bool ParseThreeColumnSegments(string? info, string errorPrefix, out string[][]? rows)
    {
        rows = null;
        if (string.IsNullOrEmpty(info))
        {
            return false;
        }

        // Java 用 indexOf(";") < 0 判断是否需要切分：这决定 "a b c" 与 "a b c;" 的不同处理。
        string[] segs = info.IndexOf(QueueSplitter) < 0 ? new[] { info } : info.Split(QueueSplitter);
        var parsed = new string[segs.Length][];
        for (int i = 0; i < segs.Length; i++)
        {
            string[] split = segs[i].Split(KeySeparator);
            if (split.Length != 3)
            {
                throw new ArgumentException(errorPrefix + ", " + info);
            }

            parsed[i] = split;
        }

        rows = parsed;
        return true;
    }

    /// <summary>异常信息里的 map 文本（Java 直接 toString 一个空 map，这里复刻语义即可）。</summary>
    private static string ToDebugString<TKey, TValue>(Dictionary<TKey, TValue> map) where TKey : notnull =>
        "{" + string.Join(", ", map.Keys.Select(k => k.ToString())) + "}";
}
