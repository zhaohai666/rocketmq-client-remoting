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
// So **keeping the default 0** is the safest choice: no need to implement the fingerprint
// algorithm of computeHeartbeatFingerprint() that depends on fastjson2 field order, and the
// broker still registers correctly.
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
