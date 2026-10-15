using System.Globalization;
using RocketMQ.Common;

namespace RocketMQ.Remoting.Protocol;

/// <summary>Serialization/restore helper for MessageQueue as a map key.</summary>
public static class MessageQueueKeys
{
    /// <summary>Serializes a MessageQueue into a fastjson2-style inline object key (keys in alphabetical order, matching fastjson2).</summary>
    public static string MessageQueueKey(MessageQueue mq)
    {
        // Keys in alphabetical order (fastjson2 behavior): brokerName, queueId, topic
        var v = JsonValue.MakeObject();
        v.Set("brokerName", JsonValue.MakeString(mq.BrokerName));
        v.Set("queueId", JsonValue.MakeInt(mq.QueueId));
        v.Set("topic", JsonValue.MakeString(mq.Topic));
        return v.Dump();
    }

    /// <summary>
    /// Restores the fastjson2-written MessageQueue inline object key back into a MessageQueue.
    /// Returns false when the key is not an inline object (e.g. a plain string key like "0" or "G1").
    /// </summary>
    public static bool ParseMessageQueueKey(string key, out MessageQueue @out)
    {
        @out = new MessageQueue();
        // First check whether it is an inline object key (starts with '{'); otherwise it is a
        // plain string key and is rejected outright
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
        @out.QueueId = LegacyNumber.ToInt32(v.Get("queueId").IntValue(0));
        return true;
    }

    /// <summary>Generic: parses a MessageQueue-keyed map with raw JsonValue values.</summary>
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

public sealed class TopicStatsTable
{
    public SortedDictionary<MessageQueue, TopicOffset> OffsetTable { get; } = new();
    public double TopicPutTps { get; set; }

    /// <summary>Sum of every queue's maxOffset (the most common convergence check on the admin side).</summary>
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

public sealed class OffsetWrapper
{
    public long BrokerOffset { get; set; }
    public long ConsumerOffset { get; set; }
    public long LastTimestamp { get; set; }
    public long PullOffset { get; set; }

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

public sealed class TopicConfigSerializeWrapper
{
    public SortedDictionary<string, TopicConfig> TopicConfigTable { get; } = new();

    // pass-through
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

public sealed class QueryConsumeQueueResponseBody
{
    // pass-through (null means it does not appear)
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

/// <summary>Shared JSON encoding helper for the admin/body layer.</summary>
internal static class AdminJson
{
    /// <summary>Encodes a MessageQueue-keyed map into a fastjson2-style object with one extra scalar field.</summary>
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
