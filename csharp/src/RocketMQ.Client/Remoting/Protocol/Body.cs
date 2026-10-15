// Composite fields the admin side does not need to interpret (subscriptionTable / mqTable /
// statsList, etc.) are always passed through as JsonValue, avoiding over-modeling that would
// drop fields.
using System.Globalization;
using RocketMQ.Common;

namespace RocketMQ.Remoting.Protocol;

public sealed class KvTable
{
    public PropertyMap Table { get; set; } = new();

    public JsonValue ToJson()
    {
        var v = JsonValue.MakeObject();
        v.Set("table", AdminJsonStrings.MakeStringMap(Table));
        return v;
    }

    public static KvTable FromJson(JsonValue v) => new() { Table = AdminJsonStrings.ReadStringMap(v.Get("table")) };

    public byte[] Encode() => RemotingSerializable.Encode(ToJson());

    public static bool Decode(byte[] data, out KvTable @out)
    {
        @out = new KvTable();
        if (!RemotingSerializable.Decode(data, out JsonValue v))
        {
            return false;
        }

        @out = FromJson(v);
        return true;
    }
}

public sealed class TopicList
{
    // The JSON key is "topicList"; a C# property cannot share the type's name, hence Topics
    public List<string> Topics { get; } = new();
    public string BrokerAddr { get; set; } = string.Empty;
    public bool HasBrokerAddr { get; set; }

    public JsonValue ToJson()
    {
        var v = JsonValue.MakeObject();
        var arr = JsonValue.MakeArray();
        foreach (string t in Topics)
        {
            arr.PushArray(JsonValue.MakeString(t));
        }

        v.Set("topicList", arr);
        if (HasBrokerAddr)
        {
            v.Set("brokerAddr", JsonValue.MakeString(BrokerAddr));
        }

        return v;
    }

    public static TopicList FromJson(JsonValue v)
    {
        var tl = new TopicList();
        JsonValue? arr = v.Find("topicList");
        if (arr is { IsArray: true })
        {
            for (int i = 0; i < arr.Size(); ++i)
            {
                if (arr.At(i).IsString)
                {
                    tl.Topics.Add(arr.At(i).StringValue());
                }
            }
        }

        if (v.TryGetString("brokerAddr", out string s))
        {
            tl.BrokerAddr = s;
            tl.HasBrokerAddr = true;
        }

        return tl;
    }

    public byte[] Encode() => RemotingSerializable.Encode(ToJson());

    public static bool Decode(byte[] data, out TopicList @out)
    {
        @out = new TopicList();
        if (!RemotingSerializable.Decode(data, out JsonValue v))
        {
            return false;
        }

        @out = FromJson(v);
        return true;
    }

    public bool Contains(string topic) => Topics.Contains(topic);
}

public sealed class GetConsumerListByGroupResponseBody
{
    public List<string> ConsumerIdList { get; } = new();

    public JsonValue ToJson()
    {
        var v = JsonValue.MakeObject();
        var arr = JsonValue.MakeArray();
        foreach (string id in ConsumerIdList)
        {
            arr.PushArray(JsonValue.MakeString(id));
        }

        v.Set("consumerIdList", arr);
        return v;
    }

    public static GetConsumerListByGroupResponseBody FromJson(JsonValue v)
    {
        var b = new GetConsumerListByGroupResponseBody();
        JsonValue? arr = v.Find("consumerIdList");
        if (arr is { IsArray: true })
        {
            for (int i = 0; i < arr.Size(); ++i)
            {
                if (arr.At(i).IsString)
                {
                    b.ConsumerIdList.Add(arr.At(i).StringValue());
                }
            }
        }

        return b;
    }

    public byte[] Encode() => RemotingSerializable.Encode(ToJson());

    public static bool Decode(byte[] data, out GetConsumerListByGroupResponseBody @out)
    {
        @out = new GetConsumerListByGroupResponseBody();
        if (!RemotingSerializable.Decode(data, out JsonValue v))
        {
            return false;
        }

        @out = FromJson(v);
        return true;
    }
}

public sealed class CheckClientRequestBody
{
    public string ClientId { get; set; } = string.Empty;
    public string Group { get; set; } = string.Empty;
    public SubscriptionData? SubscriptionData { get; set; }
    public string? Namespace { get; set; }

    public JsonValue ToJson()
    {
        var v = JsonValue.MakeObject();
        v.Set("clientId", JsonValue.MakeString(ClientId));
        v.Set("group", JsonValue.MakeString(Group));
        if (SubscriptionData != null)
        {
            v.Set("subscriptionData", SubscriptionData.ToJson());
        }

        if (Namespace != null)
        {
            v.Set("namespace", JsonValue.MakeString(Namespace));
        }

        return v;
    }

    public static CheckClientRequestBody FromJson(JsonValue v)
    {
        var b = new CheckClientRequestBody
        {
            ClientId = v.Get("clientId").StringValue(string.Empty),
            Group = v.Get("group").StringValue(string.Empty),
        };
        JsonValue? ns = v.Find("namespace");
        if (ns is { IsString: true })
        {
            b.Namespace = ns.StringValue();
        }

        JsonValue? sd = v.Find("subscriptionData");
        if (sd is { IsObject: true })
        {
            b.SubscriptionData = RocketMQ.Common.SubscriptionData.FromJson(sd);
        }

        return b;
    }

    public byte[] Encode() => RemotingSerializable.Encode(ToJson());

    public static bool Decode(byte[] data, out CheckClientRequestBody @out)
    {
        @out = new CheckClientRequestBody();
        if (!RemotingSerializable.Decode(data, out JsonValue v))
        {
            return false;
        }

        @out = FromJson(v);
        return true;
    }
}

public sealed class ClusterInfo
{
    public SortedDictionary<string, BrokerData> BrokerAddrTable { get; } = new();
    public SortedDictionary<string, List<string>> ClusterAddrTable { get; } = new();

    /// <summary>Collects all broker addresses (deduplicated, ordered by brokerName then brokerId).</summary>
    public List<string> GetBrokerAddrs()
    {
        // brokerAddrTable is already sorted by brokerName; the inner brokerAddrs are sorted by brokerId too
        var addrs = new List<string>();
        foreach (var kv in BrokerAddrTable)
        {
            foreach (var addrKv in kv.Value.BrokerAddrs)
            {
                string a = addrKv.Value;
                if (a.Length > 0 && !addrs.Contains(a))
                {
                    addrs.Add(a);
                }
            }
        }

        return addrs;
    }

    /// <summary>Lists all broker addresses under a cluster name (empty clusterName returns all).</summary>
    public List<string> GetBrokerAddrsOfCluster(string clusterName)
    {
        if (clusterName.Length == 0)
        {
            return GetBrokerAddrs();
        }

        if (!ClusterAddrTable.TryGetValue(clusterName, out List<string>? names))
        {
            return new List<string>();
        }

        var addrs = new List<string>();
        foreach (string brokerName in names)
        {
            if (!BrokerAddrTable.TryGetValue(brokerName, out BrokerData? bd))
            {
                continue;
            }

            foreach (var addrKv in bd.BrokerAddrs)
            {
                string a = addrKv.Value;
                if (a.Length > 0 && !addrs.Contains(a))
                {
                    addrs.Add(a);
                }
            }
        }

        return addrs;
    }

    public JsonValue ToJson()
    {
        var v = JsonValue.MakeObject();
        var brokers = JsonValue.MakeObject();
        foreach (var kv in BrokerAddrTable)
        {
            brokers.Set(kv.Key, kv.Value.ToJson());
        }

        v.Set("brokerAddrTable", brokers);

        var clusters = JsonValue.MakeObject();
        foreach (var kv in ClusterAddrTable)
        {
            var arr = JsonValue.MakeArray();
            foreach (string n in kv.Value)
            {
                arr.PushArray(JsonValue.MakeString(n));
            }

            clusters.Set(kv.Key, arr);
        }

        v.Set("clusterAddrTable", clusters);
        return v;
    }

    public static ClusterInfo FromJson(JsonValue v)
    {
        var ci = new ClusterInfo();
        // In real RocketMQ, brokerAddrTable[name] is a BrokerData object, and the real
        // {brokerId: addr} mapping lives in its brokerAddrs field.
        JsonValue? brokers = v.Find("brokerAddrTable");
        if (brokers is { IsObject: true })
        {
            foreach (var kv in brokers.ObjectItems())
            {
                ci.BrokerAddrTable[kv.Key] = BrokerData.FromJson(kv.Value);
            }
        }

        JsonValue? clusters = v.Find("clusterAddrTable");
        if (clusters is { IsObject: true })
        {
            foreach (var kv in clusters.ObjectItems())
            {
                // Support both forms: array (standard) and string (a few old versions)
                if (kv.Value.IsArray)
                {
                    var names = new List<string>();
                    for (int i = 0; i < kv.Value.Size(); ++i)
                    {
                        if (kv.Value.At(i).IsString)
                        {
                            names.Add(kv.Value.At(i).StringValue());
                        }
                    }

                    ci.ClusterAddrTable[kv.Key] = names;
                }
                else if (kv.Value.IsString)
                {
                    if (!ci.ClusterAddrTable.TryGetValue(kv.Key, out List<string>? list))
                    {
                        list = new List<string>();
                        ci.ClusterAddrTable[kv.Key] = list;
                    }

                    list.Add(kv.Value.StringValue());
                }
            }
        }

        return ci;
    }

    public byte[] Encode() => RemotingSerializable.Encode(ToJson());

    public static bool Decode(byte[] data, out ClusterInfo @out)
    {
        @out = new ClusterInfo();
        if (!RemotingSerializable.Decode(data, out JsonValue v))
        {
            return false;
        }

        @out = FromJson(v);
        return true;
    }
}

public sealed class Connection
{
    public string ClientId { get; set; } = string.Empty;
    public string ClientAddr { get; set; } = string.Empty;
    public string Language { get; set; } = string.Empty;
    public int Version { get; set; }

    public JsonValue ToJson()
    {
        var v = JsonValue.MakeObject();
        v.Set("clientId", JsonValue.MakeString(ClientId));
        v.Set("clientAddr", JsonValue.MakeString(ClientAddr));
        v.Set("language", JsonValue.MakeString(Language));
        v.Set("version", JsonValue.MakeInt(Version));
        return v;
    }

    public static Connection FromJson(JsonValue v) =>
        new()
        {
            ClientId = v.Get("clientId").StringValue(),
            ClientAddr = v.Get("clientAddr").StringValue(),
            Language = v.Get("language").StringValue(),
            Version = LegacyNumber.ToInt32(v.Get("version").IntValue(0)),
        };
}

public sealed class ConsumerConnection
{
    public List<Connection> ConnectionSet { get; } = new();

    // pass-through (Map<String, SubscriptionData>)
    public JsonValue SubscriptionTable { get; set; } = JsonValue.Null;

    public string ConsumeType { get; set; } = string.Empty;
    public string MessageModel { get; set; } = string.Empty;
    public string ConsumeFromWhere { get; set; } = string.Empty;

    public JsonValue ToJson()
    {
        var v = JsonValue.MakeObject();
        var arr = JsonValue.MakeArray();
        foreach (Connection c in ConnectionSet)
        {
            arr.PushArray(c.ToJson());
        }

        v.Set("connectionSet", arr);
        v.Set("subscriptionTable", SubscriptionTable.IsNull ? JsonValue.MakeObject() : SubscriptionTable);
        v.Set("consumeType", JsonValue.MakeString(ConsumeType));
        v.Set("messageModel", JsonValue.MakeString(MessageModel));
        v.Set("consumeFromWhere", JsonValue.MakeString(ConsumeFromWhere));
        return v;
    }

    public static ConsumerConnection FromJson(JsonValue v)
    {
        var cc = new ConsumerConnection();
        JsonValue? arr = v.Find("connectionSet");
        if (arr is { IsArray: true })
        {
            for (int i = 0; i < arr.Size(); ++i)
            {
                cc.ConnectionSet.Add(Connection.FromJson(arr.At(i)));
            }
        }

        cc.SubscriptionTable = v.Get("subscriptionTable");
        cc.ConsumeType = v.Get("consumeType").StringValue();
        cc.MessageModel = v.Get("messageModel").StringValue();
        cc.ConsumeFromWhere = v.Get("consumeFromWhere").StringValue();
        return cc;
    }

    public byte[] Encode() => RemotingSerializable.Encode(ToJson());

    public static bool Decode(byte[] data, out ConsumerConnection @out)
    {
        @out = new ConsumerConnection();
        if (!RemotingSerializable.Decode(data, out JsonValue v))
        {
            return false;
        }

        @out = FromJson(v);
        return true;
    }
}

public sealed class ProducerConnection
{
    public List<Connection> ConnectionSet { get; } = new();

    public JsonValue ToJson()
    {
        var v = JsonValue.MakeObject();
        var arr = JsonValue.MakeArray();
        foreach (Connection c in ConnectionSet)
        {
            arr.PushArray(c.ToJson());
        }

        v.Set("connectionSet", arr);
        return v;
    }

    public static ProducerConnection FromJson(JsonValue v)
    {
        var pc = new ProducerConnection();
        JsonValue? arr = v.Find("connectionSet");
        if (arr is { IsArray: true })
        {
            for (int i = 0; i < arr.Size(); ++i)
            {
                pc.ConnectionSet.Add(Connection.FromJson(arr.At(i)));
            }
        }

        return pc;
    }

    public byte[] Encode() => RemotingSerializable.Encode(ToJson());

    public static bool Decode(byte[] data, out ProducerConnection @out)
    {
        @out = new ProducerConnection();
        if (!RemotingSerializable.Decode(data, out JsonValue v))
        {
            return false;
        }

        @out = FromJson(v);
        return true;
    }
}

public sealed class ConsumeStatus
{
    public double PullRT { get; set; }
    public double PullTPS { get; set; }
    public double ConsumeRT { get; set; }
    public double ConsumeOKTPS { get; set; }
    public double ConsumeFailedTPS { get; set; }
    public long ConsumeFailedMsgs { get; set; }

    public JsonValue ToJson()
    {
        var v = JsonValue.MakeObject();
        v.Set("pullRT", JsonValue.MakeDouble(PullRT));
        v.Set("pullTPS", JsonValue.MakeDouble(PullTPS));
        v.Set("consumeRT", JsonValue.MakeDouble(ConsumeRT));
        v.Set("consumeOKTPS", JsonValue.MakeDouble(ConsumeOKTPS));
        v.Set("consumeFailedTPS", JsonValue.MakeDouble(ConsumeFailedTPS));
        v.Set("consumeFailedMsgs", JsonValue.MakeInt(ConsumeFailedMsgs));
        return v;
    }

    public static ConsumeStatus FromJson(JsonValue v)
    {
        return new ConsumeStatus
        {
            PullRT = v.Get("pullRT").DoubleValue(),
            PullTPS = v.Get("pullTPS").DoubleValue(),
            ConsumeRT = v.Get("consumeRT").DoubleValue(),
            ConsumeOKTPS = v.Get("consumeOKTPS").DoubleValue(),
            ConsumeFailedTPS = v.Get("consumeFailedTPS").DoubleValue(),
            ConsumeFailedMsgs = v.Get("consumeFailedMsgs").IntValue()
        };
    }
}

public sealed class ConsumerRunningInfo
{
    public const string PropNameserverAddr = "PROP_NAMESERVER_ADDR";
    public const string PropThreadpoolCoreSize = "PROP_THREADPOOL_CORE_SIZE";
    public const string PropConsumeOrderly = "PROP_CONSUMEORDERLY";
    public const string PropConsumeType = "PROP_CONSUME_TYPE";
    public const string PropClientVersion = "PROP_CLIENT_VERSION";
    public const string PropConsumerStartTimestamp = "PROP_CONSUMER_START_TIMESTAMP";

    public PropertyMap Properties { get; set; } = new();

    // pass-through (List<SubscriptionData>)
    public JsonValue SubscriptionSet { get; set; } = JsonValue.Null;

    // pass-through (Map<MessageQueue, ProcessQueueInfo>)
    public JsonValue MqTable { get; set; } = JsonValue.Null;

    // pass-through (Map<MessageQueue, ProcessQueueInfo>, POP mode)
    public JsonValue MqPopTable { get; set; } = JsonValue.Null;

    // pass-through (Map<String, ConsumeStatus>)
    public JsonValue StatusTable { get; set; } = JsonValue.Null;

    // pass-through (Map<String, String>)
    public JsonValue UserConsumerInfo { get; set; } = JsonValue.Null;

    public string Jstack { get; set; } = string.Empty;
    public bool HasJstack { get; set; }

    public JsonValue ToJson()
    {
        var v = JsonValue.MakeObject();
        v.Set("properties", AdminJsonStrings.MakeStringMap(Properties));
        v.Set("subscriptionSet", SubscriptionSet.IsNull ? JsonValue.MakeArray() : SubscriptionSet);
        v.Set("mqTable", MqTable.IsNull ? JsonValue.MakeObject() : MqTable);
        v.Set("mqPopTable", MqPopTable.IsNull ? JsonValue.MakeObject() : MqPopTable);
        v.Set("statusTable", StatusTable.IsNull ? JsonValue.MakeObject() : StatusTable);
        v.Set("userConsumerInfo", UserConsumerInfo.IsNull ? JsonValue.MakeObject() : UserConsumerInfo);
        if (HasJstack)
        {
            v.Set("jstack", JsonValue.MakeString(Jstack));
        }

        return v;
    }

    public static ConsumerRunningInfo FromJson(JsonValue v)
    {
        var ri = new ConsumerRunningInfo
        {
            Properties = AdminJsonStrings.ReadStringMap(v.Get("properties")),
            SubscriptionSet = v.Get("subscriptionSet"),
            MqTable = v.Get("mqTable"),
            MqPopTable = v.Get("mqPopTable"),
            StatusTable = v.Get("statusTable"),
            UserConsumerInfo = v.Get("userConsumerInfo"),
        };
        if (v.TryGetString("jstack", out string s))
        {
            ri.Jstack = s;
            ri.HasJstack = true;
        }

        return ri;
    }

    public byte[] Encode() => RemotingSerializable.Encode(ToJson());

    public static bool Decode(byte[] data, out ConsumerRunningInfo @out)
    {
        @out = new ConsumerRunningInfo();
        if (!RemotingSerializable.Decode(data, out JsonValue v))
        {
            return false;
        }

        @out = FromJson(v);
        return true;
    }
}

public sealed class ConsumeStatsList
{
    // pass-through
    public JsonValue StatsList { get; set; } = JsonValue.Null;

    public string BrokerAddr { get; set; } = string.Empty;
    public bool HasBrokerAddr { get; set; }

    public long TotalDiff { get; set; }
    public long TotalInflightDiff { get; set; }

    public JsonValue ToJson()
    {
        var v = JsonValue.MakeObject();
        v.Set("consumeStatsList", StatsList.IsNull ? JsonValue.MakeArray() : StatsList);
        if (HasBrokerAddr)
        {
            v.Set("brokerAddr", JsonValue.MakeString(BrokerAddr));
        }

        v.Set("totalDiff", JsonValue.MakeInt(TotalDiff));
        v.Set("totalInflightDiff", JsonValue.MakeInt(TotalInflightDiff));
        return v;
    }

    public static ConsumeStatsList FromJson(JsonValue v)
    {
        var sl = new ConsumeStatsList
        {
            StatsList = v.Get("consumeStatsList"),
        };
        if (v.TryGetString("brokerAddr", out string s))
        {
            sl.BrokerAddr = s;
            sl.HasBrokerAddr = true;
        }

        v.TryGetInt("totalDiff", out long totalDiff);
        sl.TotalDiff = totalDiff;
        v.TryGetInt("totalInflightDiff", out long totalInflight);
        sl.TotalInflightDiff = totalInflight;
        return sl;
    }

    public byte[] Encode() => RemotingSerializable.Encode(ToJson());

    public static bool Decode(byte[] data, out ConsumeStatsList @out)
    {
        @out = new ConsumeStatsList();
        if (!RemotingSerializable.Decode(data, out JsonValue v))
        {
            return false;
        }

        @out = FromJson(v);
        return true;
    }
}

public sealed class ResetOffsetBody
{
    public SortedDictionary<MessageQueue, long> OffsetTable { get; } = new();

    public JsonValue ToJson()
    {
        var v = JsonValue.MakeObject();
        var table = JsonValue.MakeObject();
        foreach (var kv in OffsetTable)
        {
            table.Set(MessageQueueKeys.MessageQueueKey(kv.Key), JsonValue.MakeInt(kv.Value));
        }

        v.Set("offsetTable", table);
        return v;
    }

    public static ResetOffsetBody FromJson(JsonValue v)
    {
        var b = new ResetOffsetBody();
        foreach (var kv in MessageQueueKeys.DecodeMessageQueueMap(v.Get("offsetTable")))
        {
            if (kv.Value.IsNumber)
            {
                b.OffsetTable[kv.Key] = kv.Value.IntValue();
            }
        }

        return b;
    }

    public byte[] Encode() => RemotingSerializable.Encode(ToJson());

    public static bool Decode(byte[] data, out ResetOffsetBody @out)
    {
        @out = new ResetOffsetBody();
        if (!RemotingSerializable.Decode(data, out JsonValue v))
        {
            return false;
        }

        @out = FromJson(v);
        return true;
    }
}

public sealed class MessageQueueForC
{
    public string Topic { get; set; } = string.Empty;

    public string BrokerName { get; set; } = string.Empty;

    public int QueueId { get; set; }

    public long Offset { get; set; }

    public JsonValue ToJson()
    {
        var v = JsonValue.MakeObject();
        v.Set("topic", JsonValue.MakeString(Topic));
        v.Set("brokerName", JsonValue.MakeString(BrokerName));
        v.Set("queueId", JsonValue.MakeInt(QueueId));
        v.Set("offset", JsonValue.MakeInt(Offset));
        return v;
    }

    public static MessageQueueForC FromJson(JsonValue v) => new()
    {
        Topic = v.Get("topic").StringValue(),
        BrokerName = v.Get("brokerName").StringValue(),
        QueueId = (int)v.Get("queueId").IntValue(),
        Offset = v.Get("offset").IntValue(),
    };
}

public sealed class ResetOffsetBodyForC
{
    public List<MessageQueueForC> OffsetTable { get; } = new();

    public JsonValue ToJson()
    {
        var v = JsonValue.MakeObject();
        var arr = JsonValue.MakeArray();
        foreach (var e in OffsetTable)
        {
            arr.PushArray(e.ToJson());
        }

        v.Set("offsetTable", arr);
        return v;
    }

    public static ResetOffsetBodyForC FromJson(JsonValue v)
    {
        var b = new ResetOffsetBodyForC();
        var arr = v.Get("offsetTable");
        if (arr.IsArray)
        {
            for (int i = 0; i < arr.Size(); i++)
            {
                if (arr.At(i).IsObject)
                {
                    b.OffsetTable.Add(MessageQueueForC.FromJson(arr.At(i)));
                }
            }
        }

        return b;
    }

    public byte[] Encode() => RemotingSerializable.Encode(ToJson());

    public static bool Decode(byte[] data, out ResetOffsetBodyForC @out)
    {
        @out = new ResetOffsetBodyForC();
        if (!RemotingSerializable.Decode(data, out JsonValue v))
        {
            return false;
        }

        @out = FromJson(v);
        return true;
    }
}

public sealed class GetConsumerStatusBody
{
    public SortedDictionary<MessageQueue, long> MessageQueueTable { get; } = new();

    public JsonValue ToJson()
    {
        var v = JsonValue.MakeObject();
        var table = JsonValue.MakeObject();
        foreach (var kv in MessageQueueTable)
        {
            table.Set(MessageQueueKeys.MessageQueueKey(kv.Key), JsonValue.MakeInt(kv.Value));
        }

        v.Set("messageQueueTable", table);
        v.Set("consumerTable", JsonValue.MakeObject());
        return v;
    }

    public byte[] Encode() => RemotingSerializable.Encode(ToJson());
}

public sealed class ConsumeMessageDirectlyResult
{
    public bool Order { get; set; }

    public bool AutoCommit { get; set; } = true;

    public string? ConsumeResult { get; set; }

    public string? Remark { get; set; }

    public long SpentTimeMills { get; set; }

    public JsonValue ToJson()
    {
        var v = JsonValue.MakeObject();
        v.Set("order", JsonValue.MakeBool(Order));
        v.Set("autoCommit", JsonValue.MakeBool(AutoCommit));
        v.Set("consumeResult", ConsumeResult is null ? JsonValue.MakeNull() : JsonValue.MakeString(ConsumeResult));
        v.Set("remark", Remark is null ? JsonValue.MakeNull() : JsonValue.MakeString(Remark));
        v.Set("spentTimeMills", JsonValue.MakeInt(SpentTimeMills));
        return v;
    }

    public static ConsumeMessageDirectlyResult FromJson(JsonValue v)
    {
        var r = new ConsumeMessageDirectlyResult
        {
            Order = v.Get("order").BoolValue(),
            AutoCommit = v.Get("autoCommit").BoolValue(),
        };
        if (v.Get("consumeResult").IsString)
        {
            r.ConsumeResult = v.Get("consumeResult").StringValue();
        }

        if (v.Get("remark").IsString)
        {
            r.Remark = v.Get("remark").StringValue();
        }

        r.SpentTimeMills = v.Get("spentTimeMills").IntValue();
        return r;
    }

    public byte[] Encode() => RemotingSerializable.Encode(ToJson());

    public static bool Decode(byte[] data, out ConsumeMessageDirectlyResult @out)
    {
        @out = new ConsumeMessageDirectlyResult();
        if (!RemotingSerializable.Decode(data, out JsonValue v))
        {
            return false;
        }

        @out = FromJson(v);
        return true;
    }
}

/// <summary>String map codec helper for the body layer.</summary>
internal static class AdminJsonStrings
{
    public static JsonValue MakeStringMap(PropertyMap m)
    {
        var o = JsonValue.MakeObject();
        foreach (var kv in m)
        {
            o.Set(kv.Key, JsonValue.MakeString(kv.Value));
        }

        return o;
    }

    public static PropertyMap ReadStringMap(JsonValue v)
    {
        var @out = new PropertyMap();
        if (v.IsObject)
        {
            foreach (var kv in v.ObjectItems())
            {
                if (kv.Value.IsString)
                {
                    @out[kv.Key] = kv.Value.StringValue();
                }
            }
        }

        return @out;
    }
}
