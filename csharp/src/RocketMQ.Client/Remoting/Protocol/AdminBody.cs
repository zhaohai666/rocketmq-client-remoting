// 管理端专用响应体（对应 org.apache.rocketmq.remoting.protocol.admin.* 与
// body.* 中的管理类）：
//   TopicStatsTable / TopicOffset / ConsumeStats / OffsetWrapper /
//   TopicConfigSerializeWrapper / ConsumeQueueData / QueryConsumeQueueResponseBody
//
// ⚠ 关键坑：这几个类的 Map 键是 **MessageQueue**，fastjson2 会把键直接内联成 JSON 对象，
// 产出**非法 JSON**，例如：
//   {"offsetTable":{{"brokerName":"broker-a","queueId":3,"topic":"MyTopic"}:{...}}}
// 因此 Json 解析器必须容忍"对象作为键"（保留原始 JSON 文本作键名），
// 再由 ParseMessageQueueKey() 还原成 MessageQueue。
// 字段名与结构均由 Java 探针实测确认。
using System.Globalization;
using RocketMQ.Common;

namespace RocketMQ.Remoting.Protocol;

/// <summary>MessageQueue 作为 map 键的序列化/还原工具。</summary>
public static class MessageQueueKeys
{
    /// <summary>把 MessageQueue 序列化成 fastjson2 风格的内联对象键（键按字母序，与 fastjson2 一致）。</summary>
    public static string MessageQueueKey(MessageQueue mq)
    {
        // 键按字母序（fastjson2 行为）：brokerName, queueId, topic
        var v = JsonValue.MakeObject();
        v.Set("brokerName", JsonValue.MakeString(mq.BrokerName));
        v.Set("queueId", JsonValue.MakeInt(mq.QueueId));
        v.Set("topic", JsonValue.MakeString(mq.Topic));
        return v.Dump();
    }

    /// <summary>
    /// 把 fastjson2 写出的 MessageQueue 内联对象键还原成 MessageQueue。
    /// 返回 false 表示这个键不是内联对象（例如 "0"、"G1" 这类普通字符串键）。
    /// </summary>
    public static bool ParseMessageQueueKey(string key, out MessageQueue @out)
    {
        @out = new MessageQueue();
        // 先看是不是内联对象键（以 '{' 起头）；否则是普通字符串键，直接不接受
        int b = 0;
        while (b < key.Length && (key[b] == ' ' || key[b] == '\t'))
        {
            ++b;
        }

        if (b >= key.Length || key[b] != '{')
        {
            return false;
        }

        if (!Json.TryParse(key, out JsonValue v, out _) || !v.IsObject)
        {
            return false;
        }

        @out.Topic = v.Get("topic").StringValue();
        @out.BrokerName = v.Get("brokerName").StringValue();
        @out.QueueId = JavaNumber.ToInt32(v.Get("queueId").IntValue(0));
        return true;
    }

    /// <summary>通用：解析以 MessageQueue 为键的 map，值为原始 JsonValue。</summary>
    public static SortedDictionary<MessageQueue, JsonValue> DecodeMessageQueueMap(JsonValue raw)
    {
        var result = new SortedDictionary<MessageQueue, JsonValue>();
        if (!raw.IsObject)
        {
            return result;
        }

        foreach (var kv in raw.ObjectItems())
        {
            if (ParseMessageQueueKey(kv.Key, out MessageQueue mq))
            {
                result[mq] = kv.Value;
            }
        }

        return result;
    }
}

/// <summary>org.apache.rocketmq.remoting.protocol.admin.TopicOffset。</summary>
public sealed class TopicOffset
{
    public long MinOffset { get; set; }
    public long MaxOffset { get; set; }
    public long LastUpdateTimestamp { get; set; }

    public JsonValue ToJson()
    {
        var v = JsonValue.MakeObject();
        v.Set("minOffset", JsonValue.MakeInt(MinOffset));
        v.Set("maxOffset", JsonValue.MakeInt(MaxOffset));
        v.Set("lastUpdateTimestamp", JsonValue.MakeInt(LastUpdateTimestamp));
        return v;
    }

    public static TopicOffset FromJson(JsonValue v) =>
        new()
        {
            MinOffset = v.Get("minOffset").IntValue(0),
            MaxOffset = v.Get("maxOffset").IntValue(0),
            LastUpdateTimestamp = v.Get("lastUpdateTimestamp").IntValue(0),
        };
}

/// <summary>
/// org.apache.rocketmq.remoting.protocol.admin.TopicStatsTable。
/// 探针输出：{"offsetTable":{MessageQueue:{...}},"topicPutTps":0.0}
/// </summary>
public sealed class TopicStatsTable
{
    public SortedDictionary<MessageQueue, TopicOffset> OffsetTable { get; } = new();
    public double TopicPutTps { get; set; }

    /// <summary>各队列 maxOffset 之和（管理端最常用的收敛判断）。</summary>
    public long TotalMaxOffset() => OffsetTable.Values.Aggregate(0L, (sum, o) => sum + o.MaxOffset);

    public JsonValue ToJson() => AdminJson.EncodeMqKeyedMap(OffsetTable, o => o.ToJson(), "topicPutTps", JsonValue.MakeDouble(TopicPutTps));

    public static TopicStatsTable FromJson(JsonValue v)
    {
        var t = new TopicStatsTable();
        foreach (var kv in MessageQueueKeys.DecodeMessageQueueMap(v.Get("offsetTable")))
        {
            t.OffsetTable[kv.Key] = TopicOffset.FromJson(kv.Value);
        }

        t.TopicPutTps = v.Get("topicPutTps").DoubleValue(0.0);
        return t;
    }

    public byte[] Encode() => RemotingSerializable.Encode(ToJson());

    public static bool Decode(byte[] data, out TopicStatsTable @out)
    {
        @out = new TopicStatsTable();
        if (!RemotingSerializable.Decode(data, out JsonValue v))
        {
            return false;
        }

        @out = FromJson(v);
        return true;
    }
}

/// <summary>org.apache.rocketmq.remoting.protocol.admin.OffsetWrapper。</summary>
public sealed class OffsetWrapper
{
    public long BrokerOffset { get; set; }
    public long ConsumerOffset { get; set; }
    public long LastTimestamp { get; set; }
    public long PullOffset { get; set; }

    /// <summary>与 Java OffsetWrapper.getLag() 一致：brokerOffset - consumerOffset。</summary>
    public long Lag() => BrokerOffset - ConsumerOffset;

    public JsonValue ToJson()
    {
        var v = JsonValue.MakeObject();
        v.Set("brokerOffset", JsonValue.MakeInt(BrokerOffset));
        v.Set("consumerOffset", JsonValue.MakeInt(ConsumerOffset));
        v.Set("lastTimestamp", JsonValue.MakeInt(LastTimestamp));
        v.Set("pullOffset", JsonValue.MakeInt(PullOffset));
        return v;
    }

    public static OffsetWrapper FromJson(JsonValue v) =>
        new()
        {
            BrokerOffset = v.Get("brokerOffset").IntValue(0),
            ConsumerOffset = v.Get("consumerOffset").IntValue(0),
            LastTimestamp = v.Get("lastTimestamp").IntValue(0),
            PullOffset = v.Get("pullOffset").IntValue(0),
        };
}

/// <summary>
/// org.apache.rocketmq.remoting.protocol.admin.ConsumeStats。
/// 探针输出：{"consumeTps":1.5,"offsetTable":{MessageQueue:OffsetWrapper}}
/// </summary>
public sealed class ConsumeStats
{
    public SortedDictionary<MessageQueue, OffsetWrapper> OffsetTable { get; } = new();
    public double ConsumeTps { get; set; }

    public long TotalLag() => OffsetTable.Values.Aggregate(0L, (sum, w) => sum + w.Lag());

    public JsonValue ToJson() => AdminJson.EncodeMqKeyedMap(OffsetTable, w => w.ToJson(), "consumeTps", JsonValue.MakeDouble(ConsumeTps));

    public static ConsumeStats FromJson(JsonValue v)
    {
        var c = new ConsumeStats();
        foreach (var kv in MessageQueueKeys.DecodeMessageQueueMap(v.Get("offsetTable")))
        {
            c.OffsetTable[kv.Key] = OffsetWrapper.FromJson(kv.Value);
        }

        c.ConsumeTps = v.Get("consumeTps").DoubleValue(0.0);
        return c;
    }

    public byte[] Encode() => RemotingSerializable.Encode(ToJson());

    public static bool Decode(byte[] data, out ConsumeStats @out)
    {
        @out = new ConsumeStats();
        if (!RemotingSerializable.Decode(data, out JsonValue v))
        {
            return false;
        }

        @out = FromJson(v);
        return true;
    }
}

/// <summary>
/// org.apache.rocketmq.remoting.protocol.body.TopicConfigSerializeWrapper。
/// 探针输出：{"dataVersion":{...},"topicConfigTable":{"Topic":{"attributes":{},...}}}
/// </summary>
public sealed class TopicConfigSerializeWrapper
{
    public SortedDictionary<string, TopicConfig> TopicConfigTable { get; } = new();

    // 透传
    public JsonValue DataVersion { get; set; } = JsonValue.Null;

    public JsonValue ToJson()
    {
        var v = JsonValue.MakeObject();
        v.Set("dataVersion", DataVersion.IsNull ? JsonValue.MakeObject() : DataVersion);
        var table = JsonValue.MakeObject();
        foreach (var kv in TopicConfigTable)
        {
            table.Set(kv.Key, kv.Value.ToJson());
        }

        v.Set("topicConfigTable", table);
        return v;
    }

    public static TopicConfigSerializeWrapper FromJson(JsonValue v)
    {
        var w = new TopicConfigSerializeWrapper();
        JsonValue? table = v.Find("topicConfigTable");
        if (table is { IsObject: true })
        {
            foreach (var kv in table.ObjectItems())
            {
                w.TopicConfigTable[kv.Key] = TopicConfig.FromJson(kv.Value);
            }
        }

        w.DataVersion = v.Get("dataVersion");
        return w;
    }

    public byte[] Encode() => RemotingSerializable.Encode(ToJson());

    public static bool Decode(byte[] data, out TopicConfigSerializeWrapper @out)
    {
        @out = new TopicConfigSerializeWrapper();
        if (!RemotingSerializable.Decode(data, out JsonValue v))
        {
            return false;
        }

        @out = FromJson(v);
        return true;
    }
}

/// <summary>
/// org.apache.rocketmq.remoting.protocol.body.ConsumeQueueData。
/// 字段：physicOffset, physicSize, tagsCode, extendDataJson, bitMap, eval, msg
/// </summary>
public sealed class ConsumeQueueData
{
    public long PhysicOffset { get; set; }
    public long PhysicSize { get; set; }
    public long TagsCode { get; set; }
    public bool Eval { get; set; }
    public string ExtendDataJson { get; set; } = string.Empty;
    public bool HasExtendDataJson { get; set; }
    public string BitMap { get; set; } = string.Empty;
    public bool HasBitMap { get; set; }
    public string Msg { get; set; } = string.Empty;
    public bool HasMsg { get; set; }

    public JsonValue ToJson()
    {
        var v = JsonValue.MakeObject();
        v.Set("physicOffset", JsonValue.MakeInt(PhysicOffset));
        v.Set("physicSize", JsonValue.MakeInt(PhysicSize));
        v.Set("tagsCode", JsonValue.MakeInt(TagsCode));
        v.Set("eval", JsonValue.MakeBool(Eval));
        if (HasBitMap)
        {
            v.Set("bitMap", JsonValue.MakeString(BitMap));
        }
        else
        {
            v.Set("bitMap", JsonValue.MakeNull());
        }

        // 同 Java：null 字段不序列化（这里用 has* 显式表达 null）
        if (HasExtendDataJson)
        {
            v.Set("extendDataJson", JsonValue.MakeString(ExtendDataJson));
        }

        if (HasMsg)
        {
            v.Set("msg", JsonValue.MakeString(Msg));
        }

        return v;
    }

    public static ConsumeQueueData FromJson(JsonValue v)
    {
        var d = new ConsumeQueueData
        {
            PhysicOffset = v.Get("physicOffset").IntValue(0),
            PhysicSize = v.Get("physicSize").IntValue(0),
            TagsCode = v.Get("tagsCode").IntValue(0),
            Eval = v.Get("eval").BoolValue(false),
        };
        if (v.TryGetString("extendDataJson", out string e))
        {
            d.ExtendDataJson = e;
            d.HasExtendDataJson = true;
        }

        if (v.TryGetString("bitMap", out string bm))
        {
            d.BitMap = bm;
            d.HasBitMap = true;
        }

        if (v.TryGetString("msg", out string m))
        {
            d.Msg = m;
            d.HasMsg = true;
        }

        return d;
    }
}

/// <summary>
/// org.apache.rocketmq.remoting.protocol.body.QueryConsumeQueueResponseBody。
/// 探针输出：{"filterData":"*","maxQueueIndex":88,"minQueueIndex":1,"subscriptionData":{...}}
/// （queueData 为 null 时不出现）
/// </summary>
public sealed class QueryConsumeQueueResponseBody
{
    // 透传（null 表示不出现）
    public JsonValue SubscriptionData { get; set; } = JsonValue.Null;

    public string FilterData { get; set; } = string.Empty;
    public bool HasFilterData { get; set; }
    public List<ConsumeQueueData> QueueData { get; } = new();
    public bool HasQueueData { get; set; }
    public long MaxQueueIndex { get; set; }
    public long MinQueueIndex { get; set; }

    public JsonValue ToJson()
    {
        var v = JsonValue.MakeObject();
        v.Set("maxQueueIndex", JsonValue.MakeInt(MaxQueueIndex));
        v.Set("minQueueIndex", JsonValue.MakeInt(MinQueueIndex));
        if (!SubscriptionData.IsNull)
        {
            v.Set("subscriptionData", SubscriptionData);
        }

        if (HasFilterData)
        {
            v.Set("filterData", JsonValue.MakeString(FilterData));
        }

        if (HasQueueData)
        {
            var arr = JsonValue.MakeArray();
            foreach (ConsumeQueueData q in QueueData)
            {
                arr.PushArray(q.ToJson());
            }

            v.Set("queueData", arr);
        }

        return v;
    }

    public static QueryConsumeQueueResponseBody FromJson(JsonValue v)
    {
        var b = new QueryConsumeQueueResponseBody
        {
            SubscriptionData = v.Get("subscriptionData"),
        };
        if (v.TryGetString("filterData", out string fd))
        {
            b.FilterData = fd;
            b.HasFilterData = true;
        }

        JsonValue? raw = v.Find("queueData");
        if (raw is { IsArray: true })
        {
            b.HasQueueData = true;
            for (int i = 0; i < raw.Size(); ++i)
            {
                b.QueueData.Add(ConsumeQueueData.FromJson(raw.At(i)));
            }
        }

        b.MaxQueueIndex = v.Get("maxQueueIndex").IntValue(0);
        b.MinQueueIndex = v.Get("minQueueIndex").IntValue(0);
        return b;
    }

    public byte[] Encode() => RemotingSerializable.Encode(ToJson());

    public static bool Decode(byte[] data, out QueryConsumeQueueResponseBody @out)
    {
        @out = new QueryConsumeQueueResponseBody();
        if (!RemotingSerializable.Decode(data, out JsonValue v))
        {
            return false;
        }

        @out = FromJson(v);
        return true;
    }
}

/// <summary>admin/body 层共享的 JSON 编码辅助。</summary>
internal static class AdminJson
{
    /// <summary>把 MessageQueue 键的 map 编码成 fastjson2 风格的对象，附带一个标量字段。</summary>
    public static JsonValue EncodeMqKeyedMap<T>(
        SortedDictionary<MessageQueue, T> m, Func<T, JsonValue> toJson, string scalarKey, JsonValue scalarValue)
    {
        var v = JsonValue.MakeObject();
        var table = JsonValue.MakeObject();
        foreach (var kv in m)
        {
            table.Set(MessageQueueKeys.MessageQueueKey(kv.Key), toJson(kv.Value));
        }

        v.Set("offsetTable", table);
        v.Set(scalarKey, scalarValue);
        return v;
    }
}
