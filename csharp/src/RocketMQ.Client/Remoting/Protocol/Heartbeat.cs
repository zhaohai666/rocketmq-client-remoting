// Purpose: producers/consumers send HEART_BEAT to **all** brokers at a fixed interval, with
// the body being the JSON of HeartbeatData. The broker uses it to register/refresh the client
// channel, consume-group subscriptions and producer groups, and to create retry topics.
//
// JSON fields (consistent with the fastjson2 serialization output, keys in alphabetical order):
//   ProducerData  : groupName
//   ConsumerData  : consumeFromWhere | consumeType | groupName | messageModel
//                   | subscriptionDataSet | unitMode
//   HeartbeatData : clientID | consumerDataSet | heartbeatFingerprint
//                   | producerDataSet | withoutSub
//
// Note two differences from the old 4.x version (this repo is 5.x source, confirmed against
// actual fastjson2 output):
//   1. ConsumerData has **no** consumeTimestamp / maxReconsumeTimes fields;
//   2. HeartbeatData has two extra fields: heartbeatFingerprint / withoutSub.
//
// Semantics of heartbeatFingerprint (see the broker's ClientManageProcessor.heartBeat):
//   - fingerprint != 0 -> takes the heartBeatV2 path (subscription changes are detected by
//     fingerprint, and subscription reporting can be skipped via withoutSub);
//   - fingerprint == 0 -> takes the V1 path, registering the consumer with the full
//     subscriptionDataSet.
// A V1 beat therefore keeps the field at 0 (the client's default). Heartbeat V2 is opt-in
// per client (ClientConfig#useHeartbeatV2, on this port MQClientInstance#UseHeartbeatV2):
// when it is on, the fingerprint of the subscription snapshot is computed by
// HeartbeatData#ComputeHeartbeatFingerprint and stamped on both the subscription-carrying
// body and its withoutSub twin, and the broker's IS_SUPPORT_HEART_BEAT_V2 / IS_SUB_CHANGE
// reply tells the client whether it may stop re-reporting subscriptions to that address.
using System;
using System.Collections.Generic;
using RocketMQ.Common;

namespace RocketMQ.Remoting.Protocol;

public static class ConsumeType
{
    public const string ConsumeActively = "CONSUME_ACTIVELY";
    public const string ConsumePassively = "CONSUME_PASSIVELY";
    public const string ConsumePop = "CONSUME_POP";
}

public static class MessageModel
{
    public const string Broadcasting = "BROADCASTING";
    public const string Clustering = "CLUSTERING";
    public const string LiteSelective = "LITE_SELECTIVE";
}

public static class ConsumeFromWhere
{
    public const string ConsumeFromLastOffset = "CONSUME_FROM_LAST_OFFSET";
    public const string ConsumeFromFirstOffset = "CONSUME_FROM_FIRST_OFFSET";
    public const string ConsumeFromTimestamp = "CONSUME_FROM_TIMESTAMP";

    public const string ConsumeFromLastOffsetAndFromMinWhenBootFirst =
        "CONSUME_FROM_LAST_OFFSET_AND_FROM_MIN_WHEN_BOOT_FIRST";
    public const string ConsumeFromMinOffset = "CONSUME_FROM_MIN_OFFSET";
    public const string ConsumeFromMaxOffset = "CONSUME_FROM_MAX_OFFSET";

    public static string NormalizeDeprecated(string where)
    {
        return where is ConsumeFromLastOffsetAndFromMinWhenBootFirst
            or ConsumeFromMinOffset
            or ConsumeFromMaxOffset
            ? ConsumeFromLastOffset
            : where;
    }
}

public sealed class ProducerData
{
    public string GroupName { get; set; } = string.Empty;

    public ProducerData()
    {
    }

    public ProducerData(string groupName) => GroupName = groupName;

    public JsonValue ToJson()
    {
        var o = JsonValue.MakeObject();
        o.Set("groupName", JsonValue.MakeString(GroupName));
        return o;
    }

    public static ProducerData FromJson(JsonValue v)
    {
        var p = new ProducerData();
        if (v.IsObject && v.TryGetString("groupName", out string s))
        {
            p.GroupName = s;
        }

        return p;
    }

    public bool Equals(ProducerData? other) => other is not null && GroupName == other.GroupName;

    public override bool Equals(object? obj) => obj is ProducerData other && Equals(other);

    public override int GetHashCode() => LegacyHash.StringHash(GroupName);

    public override string ToString() => "ProducerData [groupName=" + GroupName + "]";
}

public sealed class ConsumerData
{
    public string GroupName { get; set; } = string.Empty;
    public string ConsumeType { get; set; } = Protocol.ConsumeType.ConsumePassively;
    public string MessageModel { get; set; } = Protocol.MessageModel.Clustering;
    public string ConsumeFromWhere { get; set; } = Protocol.ConsumeFromWhere.ConsumeFromLastOffset;
    public List<SubscriptionData> SubscriptionDataSet { get; set; } = new();
    public bool UnitMode { get; set; }

    public ConsumerData()
    {
    }

    public ConsumerData(string groupName, string consumeType, string messageModel, string consumeFromWhere)
    {
        GroupName = groupName;
        ConsumeType = consumeType;
        MessageModel = messageModel;
        ConsumeFromWhere = consumeFromWhere;
    }

    public void AddSubscriptionData(SubscriptionData sd) => SubscriptionDataSet.Add(sd);

    public JsonValue ToJson()
    {
        var o = JsonValue.MakeObject();
        o.Set("consumeFromWhere", JsonValue.MakeString(ConsumeFromWhere));
        o.Set("consumeType", JsonValue.MakeString(ConsumeType));
        o.Set("groupName", JsonValue.MakeString(GroupName));
        o.Set("messageModel", JsonValue.MakeString(MessageModel));
        var subs = JsonValue.MakeArray();
        foreach (SubscriptionData sd in SubscriptionDataSet)
        {
            subs.PushArray(sd.ToJson());
        }

        o.Set("subscriptionDataSet", subs);
        o.Set("unitMode", JsonValue.MakeBool(UnitMode));
        return o;
    }

    public static ConsumerData FromJson(JsonValue v)
    {
        var c = new ConsumerData();
        if (!v.IsObject)
        {
            return c;
        }

        if (v.TryGetString("consumeFromWhere", out string s))
        {
            c.ConsumeFromWhere = s;
        }

        if (v.TryGetString("consumeType", out s))
        {
            c.ConsumeType = s;
        }

        if (v.TryGetString("groupName", out s))
        {
            c.GroupName = s;
        }

        if (v.TryGetString("messageModel", out s))
        {
            c.MessageModel = s;
        }

        if (v.TryGetBool("unitMode", out bool b))
        {
            c.UnitMode = b;
        }

        JsonValue? subs = v.Find("subscriptionDataSet");
        if (subs is { IsArray: true })
        {
            for (int i = 0; i < subs.Size(); ++i)
            {
                c.SubscriptionDataSet.Add(SubscriptionData.FromJson(subs.At(i)));
            }
        }

        return c;
    }

    public bool Equals(ConsumerData? other) =>
        other is not null && GroupName == other.GroupName && ConsumeType == other.ConsumeType
        && MessageModel == other.MessageModel && ConsumeFromWhere == other.ConsumeFromWhere
        && UnitMode == other.UnitMode && SubscriptionDataSet.SequenceEqual(other.SubscriptionDataSet);

    public override bool Equals(object? obj) => obj is ConsumerData other && Equals(other);

    public override int GetHashCode() => LegacyHash.StringHash(GroupName);

    public override string ToString()
    {
        string subs = "[";
        for (int i = 0; i < SubscriptionDataSet.Count; ++i)
        {
            if (i > 0)
            {
                subs += ", ";
            }

            subs += SubscriptionDataSet[i].ToString();
        }

        subs += "]";
        return "ConsumerData [groupName=" + GroupName + ", consumeType=" + ConsumeType
             + ", messageModel=" + MessageModel + ", consumeFromWhere=" + ConsumeFromWhere
             + ", unitMode=" + (UnitMode ? "true" : "false") + ", subscriptionDataSet=" + subs + "]";
    }
}

public sealed class HeartbeatData
{
    public string ClientId { get; set; } = string.Empty;
    public List<ProducerData> ProducerDataSet { get; set; } = new();
    public List<ConsumerData> ConsumerDataSet { get; set; } = new();

    // 0 = take the broker's V1 registration path (recommended); non-zero triggers the
    // heartBeatV2 fingerprint optimization
    public int HeartbeatFingerprint { get; set; }

    // Only read by the broker when fingerprint != 0 (V2 path)
    public bool WithoutSub { get; set; }

    public HeartbeatData()
    {
    }

    public HeartbeatData(string clientId) => ClientId = clientId;

    public void AddProducerData(ProducerData p) => ProducerDataSet.Add(p);

    public void AddConsumerData(ConsumerData c) => ConsumerDataSet.Add(c);

    public JsonValue ToJson()
    {
        var o = JsonValue.MakeObject();
        o.Set("clientID", JsonValue.MakeString(ClientId));

        var consumers = JsonValue.MakeArray();
        foreach (ConsumerData c in ConsumerDataSet)
        {
            consumers.PushArray(c.ToJson());
        }

        o.Set("consumerDataSet", consumers);

        o.Set("heartbeatFingerprint", JsonValue.MakeInt(HeartbeatFingerprint));

        var producers = JsonValue.MakeArray();
        foreach (ProducerData p in ProducerDataSet)
        {
            producers.PushArray(p.ToJson());
        }

        o.Set("producerDataSet", producers);

        o.Set("withoutSub", JsonValue.MakeBool(WithoutSub));
        return o;
    }

    public static HeartbeatData FromJson(JsonValue v)
    {
        var hb = new HeartbeatData();
        if (!v.IsObject)
        {
            return hb;
        }

        if (v.TryGetString("clientID", out string s))
        {
            hb.ClientId = s;
        }

        if (v.TryGetInt("heartbeatFingerprint", out long n))
        {
            hb.HeartbeatFingerprint = LegacyNumber.ToInt32(n);
        }

        if (v.TryGetBool("withoutSub", out bool b))
        {
            hb.WithoutSub = b;
        }
        else if (v.TryGetBool("isWithoutSub", out b))
        {
            hb.WithoutSub = b;
        }

        JsonValue? producers = v.Find("producerDataSet");
        if (producers is { IsArray: true })
        {
            for (int i = 0; i < producers.Size(); ++i)
            {
                hb.ProducerDataSet.Add(ProducerData.FromJson(producers.At(i)));
            }
        }

        JsonValue? consumers = v.Find("consumerDataSet");
        if (consumers is { IsArray: true })
        {
            for (int i = 0; i < consumers.Size(); ++i)
            {
                hb.ConsumerDataSet.Add(ConsumerData.FromJson(consumers.At(i)));
            }
        }

        return hb;
    }

    /// <summary>
    /// The same body with every subscription set emptied and withoutSub flagged, so a V2
    /// broker can skip registration work it already did (the reference's
    /// prepareHeartbeatData(true)). Nothing shared with the caller is mutated: the
    /// ProducerData / ConsumerData entries are fresh values that keep the group identity
    /// (groupName, consumeType, messageModel, consumeFromWhere, unitMode) and only lose the
    /// subscriptions.
    /// </summary>
    public HeartbeatData CloneWithoutSubscriptions()
    {
        var copy = new HeartbeatData(ClientId)
        {
            HeartbeatFingerprint = HeartbeatFingerprint,
            WithoutSub = true,
        };

        foreach (ProducerData p in ProducerDataSet)
        {
            copy.AddProducerData(new ProducerData(p.GroupName));
        }

        foreach (ConsumerData c in ConsumerDataSet)
        {
            copy.AddConsumerData(new ConsumerData(c.GroupName, c.ConsumeType, c.MessageModel,
                c.ConsumeFromWhere)
            {
                UnitMode = c.UnitMode,
            });
        }

        return copy;
    }

    /// <summary>
    /// Fingerprint of this body's subscription snapshot: the Java String.hashCode of a
    /// NORMALISED rendering, i.e. the fields that change per beat or per client stripped out
    /// (every subscription's subVersion, clientID, withoutSub, heartbeatFingerprint) and
    /// every set put into a defined order.
    ///
    /// subVersion normalisation is the load-bearing part: it is stamped with the registration
    /// time, so a rendering that kept it would report a subscription change on every single
    /// beat and the broker-side skip would never trigger. Ordering matters here even more than
    /// in the reference: that one relies on a set's iteration order being stable, while this
    /// port builds its lists from a dictionary walk and from several facades sharing one
    /// client id, so the sets are sorted explicitly.
    ///
    /// The number only has to be stable for THIS client while its subscriptions do not change
    /// and different as soon as they do -- a broker compares one client's beats to each other,
    /// never across languages.
    /// </summary>
    public int ComputeHeartbeatFingerprint() => LegacyHash.StringHash(NormalisedJson().Dump());

    private JsonValue NormalisedJson()
    {
        var consumers = new List<JsonValue>();
        foreach (ConsumerData c in ConsumerDataSet)
        {
            JsonValue cj = c.ToJson();
            var subs = new List<JsonValue>();
            foreach (SubscriptionData sd in c.SubscriptionDataSet)
            {
                JsonValue sj = sd.ToJson();
                // Registration timestamp: normalise it out or every beat looks like a change.
                sj.Set("subVersion", JsonValue.MakeInt(0));
                subs.Add(sj);
            }

            cj.Set("subscriptionDataSet", SortedArray(subs));
            consumers.Add(cj);
        }

        var producers = new List<JsonValue>();
        foreach (ProducerData p in ProducerDataSet)
        {
            producers.Add(p.ToJson());
        }

        var o = JsonValue.MakeObject();
        o.Set("clientID", JsonValue.MakeString(string.Empty));
        o.Set("consumerDataSet", SortedArray(consumers));
        o.Set("heartbeatFingerprint", JsonValue.MakeInt(0));
        o.Set("producerDataSet", SortedArray(producers));
        o.Set("withoutSub", JsonValue.MakeBool(false));
        return o;
    }

    /// <summary>
    /// An array whose elements are ordered by their own rendered form (code-unit order).
    /// Sorting the rendering rather than the object makes the result independent of the
    /// list's insertion order even for entries that agree on every identity field.
    /// </summary>
    private static JsonValue SortedArray(List<JsonValue> items)
    {
        items.Sort((a, b) => string.CompareOrdinal(a.Dump(), b.Dump()));
        var arr = JsonValue.MakeArray();
        foreach (JsonValue v in items)
        {
            arr.PushArray(v);
        }

        return arr;
    }

    public byte[] Encode() => RemotingSerializable.Encode(ToJson());

    public static bool Decode(byte[] data, out HeartbeatData @out)
    {
        @out = new HeartbeatData();
        if (!RemotingSerializable.Decode(data, out JsonValue v))
        {
            return false;
        }

        @out = FromJson(v);
        return true;
    }

    public override string ToString() =>
        "HeartbeatData [clientID=" + ClientId
        + ", producerDataSet=" + ProducerDataSet.Count.ToString(System.Globalization.CultureInfo.InvariantCulture)
        + ", consumerDataSet=" + ConsumerDataSet.Count.ToString(System.Globalization.CultureInfo.InvariantCulture) + "]";
}

/// <summary>
/// What a HEART_BEAT(34) response carries beyond "SUCCESS": the broker's protocol version
/// plus the two heartbeat-V2 flags. The reference builds the same triple in
/// <c>MQClientAPIImpl#sendHeartbeatV2</c> from <c>response.getVersion()</c> and the
/// <c>MixAll.IS_SUPPORT_HEART_BEAT_V2</c> / <c>MixAll.IS_SUB_CHANGE</c> ext fields.
///
/// Reading is deliberately fail-open: absent ext fields -- every pre-V2 broker, and every
/// test stub that does not know about V2 -- mean "this broker does not speak heartbeat V2",
/// so the client keeps reporting its subscriptions instead of skipping them. The flags are
/// TEXT booleans ("true"/"false"), so a key that is present but says false must not read as
/// support.
/// </summary>
public sealed class HeartbeatV2Result
{
    public static readonly HeartbeatV2Result NoReply = new();

    /// <summary>The broker's protocol version, taken from the response header.</summary>
    public int Version { get; init; }

    /// <summary>Whether this broker answered IS_SUPPORT_HEART_BEAT_V2=true.</summary>
    public bool SupportV2 { get; init; }

    /// <summary>Whether this broker answered IS_SUB_CHANGE=true, i.e. the fingerprint it
    /// holds for the reported consumer groups differs from the one just received.</summary>
    public bool SubChange { get; init; }

    public static HeartbeatV2Result Of(RemotingCommand? response)
    {
        if (response is null)
        {
            return new HeartbeatV2Result();
        }

        return new HeartbeatV2Result
        {
            Version = response.Version,
            SupportV2 = ReadTrue(response, MixAll.IsSupportHeartbeatV2),
            SubChange = ReadTrue(response, MixAll.IsSubChange),
        };
    }

    private static bool ReadTrue(RemotingCommand response, string key)
    {
        if (response.ExtFields is null)
        {
            return false;
        }

        return response.ExtFields.TryGetValue(key, out string? value)
            && string.Equals(value, "true", StringComparison.OrdinalIgnoreCase);
    }

    public override string ToString() =>
        "HeartbeatV2Result [version=" + Version.ToString(System.Globalization.CultureInfo.InvariantCulture)
        + ", supportV2=" + (SupportV2 ? "true" : "false")
        + ", subChange=" + (SubChange ? "true" : "false") + "]";
}
