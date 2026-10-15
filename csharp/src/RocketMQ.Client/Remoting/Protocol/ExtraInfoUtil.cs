// The POP CK (checkpoint) string is the carrier through which broker and client exchange
// consume progress:
//   ckQueueOffset popTime invisibleTime reviveQid retryFlag brokerName queueId [msgQueueOffset]
//
// WARNING: on direct POP against a normal topic, the broker does **not** write POP_CK onto
//   messages; the client reconstructs it from the startOffsetInfo/msgOffsetInfo response
//   headers (see MqClient.StampPopCk). Without it no ACK can be sent.
using System.Globalization;
using RocketMQ.Common;

namespace RocketMQ.Remoting.Protocol;

public static class ExtraInfoUtil
{
    public const string NormalTopic = "0";
    public const string RetryTopic = "1";
    public const string RetryTopicV2 = "2";
    public const string QueueOffsetKeyPrefix = "qo";

    /// <summary>MessageConst.KEY_SEPARATOR — note this is a space.</summary>
    public const char KeySeparator = ' ';

    /// <summary>Segment separating "multiple queues".</summary>
    private const char QueueSplitter = ';';

    /// <summary>KeyBuilder.POP_ORDER_REVIVE_QUEUE — a reviveQid equal to it means ordered consumption.</summary>
    public const int PopOrderReviveQueue = 999;

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

    public static string GetRealTopic(string[] extraInfoStrs, string topic, string cid)
    {
        RequireLength(extraInfoStrs, 5, "getRealTopic");
        return GetRealTopic(topic, cid, extraInfoStrs[4]);
    }

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

    public static string BuildPopRetryTopicV1(string topic, string cid) =>
        MixAll.RetryGroupTopicPrefix + cid + "_" + topic;

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

    public static string GetStartOffsetInfoMapKey(string topic, string? popCk, long key) =>
        (popCk is null ? RetryOfTopic(topic) : GetRetry(Split(popCk))) + "@"
            + key.ToString(CultureInfo.InvariantCulture);

    public static string GetQueueOffsetKeyValueKey(long queueId, long queueOffset) =>
        QueueOffsetKeyPrefix + queueId.ToString(CultureInfo.InvariantCulture) + "%"
            + queueOffset.ToString(CultureInfo.InvariantCulture);

    public static string GetQueueOffsetMapKey(string topic, long queueId, long queueOffset) =>
        RetryOfTopic(topic) + "@" + GetQueueOffsetKeyValueKey(queueId, queueOffset);

    public static bool IsOrder(string[] extraInfo) => GetReviveQid(extraInfo) == PopOrderReviveQueue;

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

    private static bool ParseThreeColumnSegments(string? info, string errorPrefix, out string[][]? rows)
    {
        rows = null;
        if (string.IsNullOrEmpty(info))
        {
            return false;
        }

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

    private static string ToDebugString<TKey, TValue>(Dictionary<TKey, TValue> map) where TKey : notnull =>
        "{" + string.Join(", ", map.Keys.Select(k => k.ToString())) + "}";
}
