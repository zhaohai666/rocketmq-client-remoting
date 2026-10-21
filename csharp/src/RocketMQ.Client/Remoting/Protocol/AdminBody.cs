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

/// <summary>User management payload. Every field is optional on the wire: one the caller
/// leaves unset does not appear in the object at all (a null-valued key is not sent).</summary>
public sealed class UserInfo
{
    public string? Username { get; set; }
    public string? Password { get; set; }
    public string? UserType { get; set; }
    public string? UserStatus { get; set; }

    public JsonValue ToJson()
    {
        var v = JsonValue.MakeObject();
        AdminJson.PutOptString(v, "username", Username);
        AdminJson.PutOptString(v, "password", Password);
        AdminJson.PutOptString(v, "userType", UserType);
        AdminJson.PutOptString(v, "userStatus", UserStatus);
        return v;
    }

    public static UserInfo FromJson(JsonValue v) => new()
    {
        Username = AdminJson.OptString(v, "username"),
        Password = AdminJson.OptString(v, "password"),
        UserType = AdminJson.OptString(v, "userType"),
        UserStatus = AdminJson.OptString(v, "userStatus"),
    };

    public byte[] Encode() => RemotingSerializable.Encode(ToJson());

    public static bool Decode(byte[] data, out UserInfo @out)
    {
        @out = new UserInfo();
        if (data.Length == 0 || !RemotingSerializable.Decode(data, out JsonValue v) || !v.IsObject)
        {
            return false;
        }

        @out = FromJson(v);
        return true;
    }

    /// <summary>The list form answers with a bare JSON array; anything else decodes to an
    /// empty list.</summary>
    public static List<UserInfo> DecodeList(byte[] data)
    {
        var list = new List<UserInfo>();
        if (data.Length == 0 || !RemotingSerializable.Decode(data, out JsonValue v) || !v.IsArray)
        {
            return list;
        }

        for (int i = 0; i < v.Size(); ++i)
        {
            if (v.At(i).IsObject) list.Add(FromJson(v.At(i)));
        }

        return list;
    }
}

/// <summary>ACL management payload: a subject plus the policies granted to it.</summary>
public sealed class AclInfo
{
    public string? Subject { get; set; }
    public List<AclPolicy>? Policies { get; set; }

    /// <summary>Builds the flat form of a grant: one policy with no type, holding one entry
    /// per resource that repeats the same actions / sourceIps / decision.</summary>
    public static AclInfo OfResources(string subject, IReadOnlyList<string> resources,
        IReadOnlyList<string> actions, IReadOnlyList<string> sourceIps, string decision)
    {
        var entries = new List<AclEntry>();
        foreach (string resource in resources)
        {
            entries.Add(new AclEntry
            {
                Resource = resource,
                Actions = new List<string>(actions),
                SourceIps = new List<string>(sourceIps),
                Decision = decision,
            });
        }

        return new AclInfo
        {
            Subject = subject,
            // The type stays unset here: a flat grant is answered with an untyped policy.
            Policies = new List<AclPolicy> { new() { Entries = entries } },
        };
    }

    public JsonValue ToJson()
    {
        var v = JsonValue.MakeObject();
        AdminJson.PutOptString(v, "subject", Subject);
        if (Policies is not null)
        {
            var arr = JsonValue.MakeArray();
            foreach (AclPolicy p in Policies) arr.PushArray(p.ToJson());
            v.Set("policies", arr);
        }

        return v;
    }

    public static AclInfo FromJson(JsonValue v)
    {
        var acl = new AclInfo { Subject = AdminJson.OptString(v, "subject") };
        JsonValue? arr = v.Find("policies");
        if (arr is { IsArray: true })
        {
            var policies = new List<AclPolicy>();
            for (int i = 0; i < arr.Size(); ++i)
            {
                if (arr.At(i).IsObject) policies.Add(AclPolicy.FromJson(arr.At(i)));
            }

            acl.Policies = policies;
        }

        return acl;
    }

    public byte[] Encode() => RemotingSerializable.Encode(ToJson());

    public static bool Decode(byte[] data, out AclInfo @out)
    {
        @out = new AclInfo();
        if (data.Length == 0 || !RemotingSerializable.Decode(data, out JsonValue v) || !v.IsObject)
        {
            return false;
        }

        @out = FromJson(v);
        return true;
    }

    /// <summary>The list form answers with a bare JSON array; anything else decodes to an
    /// empty list.</summary>
    public static List<AclInfo> DecodeList(byte[] data)
    {
        var list = new List<AclInfo>();
        if (data.Length == 0 || !RemotingSerializable.Decode(data, out JsonValue v) || !v.IsArray)
        {
            return list;
        }

        for (int i = 0; i < v.Size(); ++i)
        {
            if (v.At(i).IsObject) list.Add(FromJson(v.At(i)));
        }

        return list;
    }
}

public sealed class AclPolicy
{
    public string? PolicyType { get; set; }
    public List<AclEntry>? Entries { get; set; }

    public JsonValue ToJson()
    {
        var v = JsonValue.MakeObject();
        AdminJson.PutOptString(v, "policyType", PolicyType);
        if (Entries is not null)
        {
            var arr = JsonValue.MakeArray();
            foreach (AclEntry e in Entries) arr.PushArray(e.ToJson());
            v.Set("entries", arr);
        }

        return v;
    }

    public static AclPolicy FromJson(JsonValue v)
    {
        var policy = new AclPolicy { PolicyType = AdminJson.OptString(v, "policyType") };
        JsonValue? arr = v.Find("entries");
        if (arr is { IsArray: true })
        {
            var entries = new List<AclEntry>();
            for (int i = 0; i < arr.Size(); ++i)
            {
                if (arr.At(i).IsObject) entries.Add(AclEntry.FromJson(arr.At(i)));
            }

            policy.Entries = entries;
        }

        return policy;
    }
}

public sealed class AclEntry
{
    public string? Resource { get; set; }
    public List<string>? Actions { get; set; }
    public List<string>? SourceIps { get; set; }
    public string? Decision { get; set; }

    public JsonValue ToJson()
    {
        var v = JsonValue.MakeObject();
        AdminJson.PutOptString(v, "resource", Resource);
        AdminJson.PutOptStringList(v, "actions", Actions);
        AdminJson.PutOptStringList(v, "sourceIps", SourceIps);
        AdminJson.PutOptString(v, "decision", Decision);
        return v;
    }

    public static AclEntry FromJson(JsonValue v) => new()
    {
        Resource = AdminJson.OptString(v, "resource"),
        Actions = AdminJson.OptStringList(v, "actions"),
        SourceIps = AdminJson.OptStringList(v, "sourceIps"),
        Decision = AdminJson.OptString(v, "decision"),
    };
}

/// <summary>Wire spellings of the user type. The case differs from field to field across the
/// auth protocol, so these constants are the only place the exact text is written down.</summary>
public static class AuthUserType
{
    public const string Super = "Super";
    public const string Normal = "Normal";
}

/// <summary>Wire spellings of the user status (lowercase, unlike the user type).</summary>
public static class AuthUserStatus
{
    public const string Enable = "enable";
    public const string Disable = "disable";
}

/// <summary>Wire spellings of the policy type.</summary>
public static class AuthPolicyType
{
    public const string Custom = "Custom";
    public const string Default = "Default";
}

/// <summary>Wire spellings of an entry decision.</summary>
public static class AuthDecision
{
    public const string Allow = "Allow";
    public const string Deny = "Deny";
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

    /// <summary>Writes a string only when it is set — an unset field is left off the object
    /// rather than sent as a null value.</summary>
    public static void PutOptString(JsonValue o, string key, string? v)
    {
        if (v is not null)
        {
            o.Set(key, JsonValue.MakeString(v));
        }
    }

    public static string? OptString(JsonValue o, string key) =>
        o.Find(key) is { IsString: true } v ? v.StringValue() : null;

    /// <summary>Writes a list of strings only when it is set (an empty list is a set value and
    /// goes out as []).</summary>
    public static void PutOptStringList(JsonValue o, string key, IReadOnlyList<string>? items)
    {
        if (items is null)
        {
            return;
        }

        var arr = JsonValue.MakeArray();
        foreach (string item in items)
        {
            arr.PushArray(JsonValue.MakeString(item));
        }

        o.Set(key, arr);
    }

    public static List<string>? OptStringList(JsonValue o, string key)
    {
        JsonValue? arr = o.Find(key);
        if (arr is not { IsArray: true })
        {
            return null;
        }

        var items = new List<string>();
        for (int i = 0; i < arr.Size(); ++i)
        {
            if (arr.At(i).IsString) items.Add(arr.At(i).StringValue());
        }

        return items;
    }
}

/// <summary>The controller metadata answer (1005): every field rides in the response
/// extFields under its header field name, so the read is a flat ext lookup.</summary>
public sealed class GetMetaDataResponseHeader
{
    public string Group { get; set; } = string.Empty;
    public string ControllerLeaderId { get; set; } = string.Empty;
    public string ControllerLeaderAddress { get; set; } = string.Empty;
    public bool IsLeader { get; set; }
    public string Peers { get; set; } = string.Empty;

    public static GetMetaDataResponseHeader FromExtFields(IReadOnlyDictionary<string, string> ext)
    {
        var h = new GetMetaDataResponseHeader();
        if (ext.TryGetValue("group", out string? group)) h.Group = group;
        if (ext.TryGetValue("controllerLeaderId", out string? leaderId)) h.ControllerLeaderId = leaderId;
        if (ext.TryGetValue("controllerLeaderAddress", out string? leaderAddr)) h.ControllerLeaderAddress = leaderAddr;
        if (ext.TryGetValue("isLeader", out string? isLeader)) h.IsLeader = isLeader == "true";
        if (ext.TryGetValue("peers", out string? peers)) h.Peers = peers;
        return h;
    }
}

/// <summary>Replica identity as the controller writes it; a null alive flag stays off the
/// object rather than being sent as a value.</summary>
public sealed class ReplicaIdentity
{
    public string BrokerName { get; set; } = string.Empty;
    public long BrokerId { get; set; }
    public string BrokerAddress { get; set; } = string.Empty;
    public bool? Alive { get; set; }

    public JsonValue ToJson()
    {
        var v = JsonValue.MakeObject();
        v.Set("brokerName", JsonValue.MakeString(BrokerName));
        v.Set("brokerId", JsonValue.MakeInt(BrokerId));
        v.Set("brokerAddress", JsonValue.MakeString(BrokerAddress));
        if (Alive.HasValue) v.Set("alive", JsonValue.MakeBool(Alive.Value));
        return v;
    }

    public static ReplicaIdentity FromJson(JsonValue v) => new()
    {
        BrokerName = v.Get("brokerName").StringValue(),
        BrokerId = v.Get("brokerId").IntValue(0),
        BrokerAddress = v.Get("brokerAddress").StringValue(),
        Alive = v.Find("alive") is { IsBool: true } a ? a.BoolValue() : null,
    };
}

/// <summary>Per-broker replication state of the controller's sync-state-set view.</summary>
public sealed class ReplicasInfo
{
    public long? MasterBrokerId { get; set; }
    public string MasterAddress { get; set; } = string.Empty;
    public int MasterEpoch { get; set; }
    public int SyncStateSetEpoch { get; set; }
    public List<ReplicaIdentity> InSyncReplicas { get; } = new();
    public List<ReplicaIdentity> NotInSyncReplicas { get; } = new();

    public JsonValue ToJson()
    {
        var v = JsonValue.MakeObject();
        if (MasterBrokerId.HasValue) v.Set("masterBrokerId", JsonValue.MakeInt(MasterBrokerId.Value));
        v.Set("masterAddress", JsonValue.MakeString(MasterAddress));
        v.Set("masterEpoch", JsonValue.MakeInt(MasterEpoch));
        v.Set("syncStateSetEpoch", JsonValue.MakeInt(SyncStateSetEpoch));
        v.Set("inSyncReplicas", ReplicaList(InSyncReplicas));
        v.Set("notInSyncReplicas", ReplicaList(NotInSyncReplicas));
        return v;
    }

    private static JsonValue ReplicaList(List<ReplicaIdentity> list)
    {
        var arr = JsonValue.MakeArray();
        foreach (ReplicaIdentity r in list) arr.PushArray(r.ToJson());
        return arr;
    }

    public static ReplicasInfo FromJson(JsonValue v)
    {
        var info = new ReplicasInfo
        {
            MasterAddress = v.Get("masterAddress").StringValue(),
            MasterEpoch = (int)v.Get("masterEpoch").IntValue(0),
            SyncStateSetEpoch = (int)v.Get("syncStateSetEpoch").IntValue(0),
        };
        if (v.Find("masterBrokerId") is { IsNumber: true } id) info.MasterBrokerId = id.IntValue(0);
        FillReplicas(v.Find("inSyncReplicas"), info.InSyncReplicas);
        FillReplicas(v.Find("notInSyncReplicas"), info.NotInSyncReplicas);
        return info;
    }

    private static void FillReplicas(JsonValue? arr, List<ReplicaIdentity> into)
    {
        if (arr is not { IsArray: true }) return;
        for (int i = 0; i < arr.Size(); ++i)
        {
            if (arr.At(i).IsObject) into.Add(ReplicaIdentity.FromJson(arr.At(i)));
        }
    }
}

/// <summary>The controller's per-broker replicas table (1006 response body).</summary>
public sealed class BrokerReplicasInfo
{
    public Dictionary<string, ReplicasInfo> ReplicasInfoTable { get; } = new();

    public JsonValue ToJson()
    {
        var v = JsonValue.MakeObject();
        var table = JsonValue.MakeObject();
        foreach (var kv in ReplicasInfoTable) table.Set(kv.Key, kv.Value.ToJson());
        v.Set("replicasInfoTable", table);
        return v;
    }

    public static BrokerReplicasInfo FromJson(JsonValue v)
    {
        var info = new BrokerReplicasInfo();
        JsonValue? table = v.Find("replicasInfoTable");
        if (table is { IsObject: true })
        {
            foreach (var kv in table.ObjectItems())
            {
                if (kv.Value.IsObject) info.ReplicasInfoTable[kv.Key] = ReplicasInfo.FromJson(kv.Value);
            }
        }

        return info;
    }

    public byte[] Encode() => RemotingSerializable.Encode(ToJson());

    public static bool Decode(byte[] data, out BrokerReplicasInfo @out)
    {
        @out = new BrokerReplicasInfo();
        if (data.Length == 0 || !RemotingSerializable.Decode(data, out JsonValue v) || !v.IsObject)
        {
            return false;
        }

        @out = FromJson(v);
        return true;
    }
}

/// <summary>RocksDB consume-queue write progress check (354 response body). The status is
/// one of <see cref="CheckRocksdbCheckStatus" />.</summary>
public sealed class CheckRocksdbCqWriteResult
{
    public string? CheckResult { get; set; }
    public int CheckStatus { get; set; }

    public JsonValue ToJson()
    {
        var v = JsonValue.MakeObject();
        AdminJson.PutOptString(v, "checkResult", CheckResult);
        v.Set("checkStatus", JsonValue.MakeInt(CheckStatus));
        return v;
    }

    public static CheckRocksdbCqWriteResult FromJson(JsonValue v) => new()
    {
        CheckResult = AdminJson.OptString(v, "checkResult"),
        CheckStatus = (int)v.Get("checkStatus").IntValue(0),
    };

    public byte[] Encode() => RemotingSerializable.Encode(ToJson());

    public static bool Decode(byte[] data, out CheckRocksdbCqWriteResult @out)
    {
        @out = new CheckRocksdbCqWriteResult();
        if (data.Length == 0 || !RemotingSerializable.Decode(data, out JsonValue v) || !v.IsObject)
        {
            return false;
        }

        @out = FromJson(v);
        return true;
    }
}

/// <summary>Wire values of the 354 check status enum (CHECK_OK .. CHECK_ERROR).</summary>
public static class CheckRocksdbCheckStatus
{
    public const int CheckOk = 0;
    public const int CheckNotOk = 1;
    public const int CheckInProgress = 2;
    public const int CheckError = 3;
}

/// <summary>One queue's consume-time span (the consumeTimeSpanSet entries of 303).</summary>
public sealed class QueueTimeSpan
{
    public MessageQueue MessageQueue { get; set; } = new();
    public long MinTimeStamp { get; set; }
    public long MaxTimeStamp { get; set; }
    public long ConsumeTimeStamp { get; set; }
    public long DelayTime { get; set; }

    public JsonValue ToJson()
    {
        var v = JsonValue.MakeObject();
        v.Set("messageQueue", MessageQueueKeys.MessageQueueKey(MessageQueue) is string key
            && Json.TryParse(key, out JsonValue parsed, out _) ? parsed : JsonValue.MakeObject());
        v.Set("minTimeStamp", JsonValue.MakeInt(MinTimeStamp));
        v.Set("maxTimeStamp", JsonValue.MakeInt(MaxTimeStamp));
        v.Set("consumeTimeStamp", JsonValue.MakeInt(ConsumeTimeStamp));
        v.Set("delayTime", JsonValue.MakeInt(DelayTime));
        return v;
    }

    public static QueueTimeSpan FromJson(JsonValue v)
    {
        var span = new QueueTimeSpan
        {
            MinTimeStamp = v.Get("minTimeStamp").IntValue(0),
            MaxTimeStamp = v.Get("maxTimeStamp").IntValue(0),
            ConsumeTimeStamp = v.Get("consumeTimeStamp").IntValue(0),
            DelayTime = v.Get("delayTime").IntValue(0),
        };
        JsonValue mq = v.Get("messageQueue");
        if (mq.IsObject)
        {
            span.MessageQueue = new MessageQueue(
                mq.Get("topic").StringValue(),
                mq.Get("brokerName").StringValue(),
                LegacyNumber.ToInt32(mq.Get("queueId").IntValue(0)));
        }

        return span;
    }

    /// <summary>The 303 answer wraps the list in a consumeTimeSpanSet object; a bare array
    /// is accepted too.</summary>
    public static List<QueueTimeSpan> DecodeList(byte[] data)
    {
        var list = new List<QueueTimeSpan>();
        if (data.Length == 0 || !RemotingSerializable.Decode(data, out JsonValue v))
        {
            return list;
        }

        JsonValue? arr = v.IsArray ? v : v.Find("consumeTimeSpanSet");
        if (arr is not { IsArray: true })
        {
            return list;
        }

        for (int i = 0; i < arr.Size(); ++i)
        {
            if (arr.At(i).IsObject) list.Add(FromJson(arr.At(i)));
        }

        return list;
    }
}

/// <summary>A (clientId, group) pair as the lite-topic subscriber set writes it.</summary>
public sealed class ClientGroup
{
    public string ClientId { get; set; } = string.Empty;
    public string Group { get; set; } = string.Empty;

    public JsonValue ToJson()
    {
        var v = JsonValue.MakeObject();
        v.Set("clientId", JsonValue.MakeString(ClientId));
        v.Set("group", JsonValue.MakeString(Group));
        return v;
    }

    public static ClientGroup FromJson(JsonValue v) => new()
    {
        ClientId = v.Get("clientId").StringValue(),
        Group = v.Get("group").StringValue(),
    };
}

/// <summary>One row of the lite-topic lag top-K lists.</summary>
public sealed class LiteLagInfo
{
    public string LiteTopic { get; set; } = string.Empty;
    public long LagCount { get; set; }
    public long EarliestUnconsumedTimestamp { get; set; } = -1;

    public JsonValue ToJson()
    {
        var v = JsonValue.MakeObject();
        v.Set("liteTopic", JsonValue.MakeString(LiteTopic));
        v.Set("lagCount", JsonValue.MakeInt(LagCount));
        v.Set("earliestUnconsumedTimestamp", JsonValue.MakeInt(EarliestUnconsumedTimestamp));
        return v;
    }

    public static LiteLagInfo FromJson(JsonValue v) => new()
    {
        LiteTopic = v.Get("liteTopic").StringValue(),
        LagCount = v.Get("lagCount").IntValue(0),
        EarliestUnconsumedTimestamp = v.Get("earliestUnconsumedTimestamp").IntValue(-1),
    };
}

/// <summary>Broker-wide lite-topic store overview (200074 body).</summary>
public sealed class GetBrokerLiteInfoResponseBody
{
    public string StoreType { get; set; } = string.Empty;
    public int MaxLmqNum { get; set; }
    public int CurrentLmqNum { get; set; }
    public int LiteSubscriptionCount { get; set; }
    public int OrderInfoCount { get; set; }
    public int CqTableSize { get; set; }
    public int OffsetTableSize { get; set; }
    public int EventMapSize { get; set; }
    public Dictionary<string, int> TopicMeta { get; } = new();
    public Dictionary<string, List<string>> GroupMeta { get; } = new();

    public static GetBrokerLiteInfoResponseBody FromJson(JsonValue v)
    {
        var b = new GetBrokerLiteInfoResponseBody
        {
            StoreType = v.Get("storeType").StringValue(),
            MaxLmqNum = (int)v.Get("maxLmqNum").IntValue(0),
            CurrentLmqNum = (int)v.Get("currentLmqNum").IntValue(0),
            LiteSubscriptionCount = (int)v.Get("liteSubscriptionCount").IntValue(0),
            OrderInfoCount = (int)v.Get("orderInfoCount").IntValue(0),
            CqTableSize = (int)v.Get("cqTableSize").IntValue(0),
            OffsetTableSize = (int)v.Get("offsetTableSize").IntValue(0),
            EventMapSize = (int)v.Get("eventMapSize").IntValue(0),
        };
        if (v.Get("topicMeta") is { IsObject: true } topicMeta)
        {
            foreach (var kv in topicMeta.ObjectItems())
            {
                b.TopicMeta[kv.Key] = (int)kv.Value.IntValue(0);
            }
        }

        if (v.Get("groupMeta") is { IsObject: true } groupMeta)
        {
            foreach (var kv in groupMeta.ObjectItems())
            {
                var groups = new List<string>();
                if (kv.Value is { IsArray: true } arr)
                {
                    for (int i = 0; i < arr.Size(); ++i)
                    {
                        if (arr.At(i).IsString) groups.Add(arr.At(i).StringValue());
                    }
                }

                b.GroupMeta[kv.Key] = groups;
            }
        }

        return b;
    }
}

/// <summary>Parent-topic lite overview (200075 body).</summary>
public sealed class GetParentTopicInfoResponseBody
{
    public string Topic { get; set; } = string.Empty;
    public int Ttl { get; set; }
    public List<string> Groups { get; } = new();
    public int LmqNum { get; set; }
    public int LiteTopicCount { get; set; }

    public static GetParentTopicInfoResponseBody FromJson(JsonValue v)
    {
        var b = new GetParentTopicInfoResponseBody
        {
            Topic = v.Get("topic").StringValue(),
            Ttl = (int)v.Get("ttl").IntValue(0),
            LmqNum = (int)v.Get("lmqNum").IntValue(0),
            LiteTopicCount = (int)v.Get("liteTopicCount").IntValue(0),
        };
        if (v.Get("groups") is { IsArray: true } arr)
        {
            for (int i = 0; i < arr.Size(); ++i)
            {
                if (arr.At(i).IsString) b.Groups.Add(arr.At(i).StringValue());
            }
        }

        return b;
    }
}

/// <summary>Single lite topic state (200076 body).</summary>
public sealed class GetLiteTopicInfoResponseBody
{
    public string ParentTopic { get; set; } = string.Empty;
    public string LiteTopic { get; set; } = string.Empty;
    public List<ClientGroup> Subscriber { get; } = new();
    public TopicOffset? TopicOffset { get; set; }
    public bool ShardingToBroker { get; set; }

    public static GetLiteTopicInfoResponseBody FromJson(JsonValue v)
    {
        var b = new GetLiteTopicInfoResponseBody
        {
            ParentTopic = v.Get("parentTopic").StringValue(),
            LiteTopic = v.Get("liteTopic").StringValue(),
            ShardingToBroker = v.Get("shardingToBroker").BoolValue(false),
        };
        if (v.Get("topicOffset") is { IsObject: true } offset)
        {
            b.TopicOffset = TopicOffset.FromJson(offset);
        }

        if (v.Get("subscriber") is { IsArray: true } arr)
        {
            for (int i = 0; i < arr.Size(); ++i)
            {
                if (arr.At(i).IsObject) b.Subscriber.Add(ClientGroup.FromJson(arr.At(i)));
            }
        }

        return b;
    }
}

/// <summary>Per-client lite consumption state (200077 body).</summary>
public sealed class GetLiteClientInfoResponseBody
{
    public string ParentTopic { get; set; } = string.Empty;
    public string Group { get; set; } = string.Empty;
    public string ClientId { get; set; } = string.Empty;
    public long LastAccessTime { get; set; }
    public long LastConsumeTime { get; set; }
    public int LiteTopicCount { get; set; }
    public List<string> LiteTopicSet { get; } = new();

    public static GetLiteClientInfoResponseBody FromJson(JsonValue v)
    {
        var b = new GetLiteClientInfoResponseBody
        {
            ParentTopic = v.Get("parentTopic").StringValue(),
            Group = v.Get("group").StringValue(),
            ClientId = v.Get("clientId").StringValue(),
            LastAccessTime = v.Get("lastAccessTime").IntValue(0),
            LastConsumeTime = v.Get("lastConsumeTime").IntValue(0),
            LiteTopicCount = (int)v.Get("liteTopicCount").IntValue(0),
        };
        if (v.Get("liteTopicSet") is { IsArray: true } arr)
        {
            for (int i = 0; i < arr.Size(); ++i)
            {
                if (arr.At(i).IsString) b.LiteTopicSet.Add(arr.At(i).StringValue());
            }
        }

        return b;
    }
}

/// <summary>Per-group lite lag overview (200078 body).</summary>
public sealed class GetLiteGroupInfoResponseBody
{
    public string Group { get; set; } = string.Empty;
    public string ParentTopic { get; set; } = string.Empty;
    public string LiteTopic { get; set; } = string.Empty;
    public long EarliestUnconsumedTimestamp { get; set; } = -1;
    public long TotalLagCount { get; set; }
    public OffsetWrapper? LiteTopicOffsetWrapper { get; set; }
    public List<LiteLagInfo> LagCountTopK { get; } = new();
    public List<LiteLagInfo> LagTimestampTopK { get; } = new();

    public static GetLiteGroupInfoResponseBody FromJson(JsonValue v)
    {
        var b = new GetLiteGroupInfoResponseBody
        {
            Group = v.Get("group").StringValue(),
            ParentTopic = v.Get("parentTopic").StringValue(),
            LiteTopic = v.Get("liteTopic").StringValue(),
            EarliestUnconsumedTimestamp = v.Get("earliestUnconsumedTimestamp").IntValue(-1),
            TotalLagCount = v.Get("totalLagCount").IntValue(0),
        };
        if (v.Get("liteTopicOffsetWrapper") is { IsObject: true } wrapper)
        {
            b.LiteTopicOffsetWrapper = OffsetWrapper.FromJson(wrapper);
        }

        FillTopK(v.Find("lagCountTopK"), b.LagCountTopK);
        FillTopK(v.Find("lagTimestampTopK"), b.LagTimestampTopK);
        return b;
    }

    private static void FillTopK(JsonValue? arr, List<LiteLagInfo> into)
    {
        if (arr is not { IsArray: true }) return;
        for (int i = 0; i < arr.Size(); ++i)
        {
            if (arr.At(i).IsObject) into.Add(LiteLagInfo.FromJson(arr.At(i)));
        }
    }
}

/// <summary>Per-address outcome of a fan-out broker operation.</summary>
public sealed class BrokerOperatorResult
{
    public List<string> SuccessList { get; set; } = new();
    public List<string> FailureList { get; set; } = new();
}

/// <summary>Wire values of the admin tool result codes.</summary>
public static class AdminToolsResultCodeEnum
{
    public const int Success = 200;
    public const int RemotingError = -1001;
    public const int MqBrokerError = -1002;
    public const int MqClientError = -1003;
    public const int InterruptError = -1004;
    public const int TopicRouteInfoNotExist = -2001;
    public const int ConsumerNotOnline = -2002;
    public const int BroadcastConsumption = -2003;
}

/// <summary>Envelope of the concurrent admin operations: every settled outcome is carried
/// in <see cref="Data" />, a failure of the operation itself in <see cref="ErrorMsg" />.</summary>
public sealed class AdminToolResult<T>
{
    public bool Success { get; set; }
    public int Code { get; set; }
    public string? ErrorMsg { get; set; }
    public T? Data { get; set; }

    public static AdminToolResult<T> Ok(T data) => new()
    {
        Success = true,
        Code = AdminToolsResultCodeEnum.Success,
        ErrorMsg = "success",
        Data = data,
    };

    public static AdminToolResult<T> Failure(int code, string? errorMsg, T? data = default) => new()
    {
        Success = false,
        Code = code,
        ErrorMsg = errorMsg,
        Data = data,
    };
}
