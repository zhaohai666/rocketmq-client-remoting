// 心跳数据（对应 org.apache.rocketmq.remoting.protocol.heartbeat.*）。
//
// 用途：生产者/消费者按固定间隔向**所有** broker 发 HEART_BEAT，body 即
// HeartbeatData 的 JSON。broker 据此注册/刷新 client channel、消费组订阅与
// 生产者组，并创建重试 topic。
//
// JSON 字段（与 fastjson2 序列化结果一致，键按字母序）：
//   ProducerData  : groupName
//   ConsumerData  : consumeFromWhere | consumeType | groupName | messageModel
//                   | subscriptionDataSet | unitMode
//   HeartbeatData : clientID | consumerDataSet | heartbeatFingerprint
//                   | producerDataSet | withoutSub
//
// 注意两点与旧版 4.x 的差异（本仓库为 5.x 源码，已实测 fastjson2 输出确认）：
//   1. ConsumerData **没有** consumeTimestamp / maxReconsumeTimes 字段；
//   2. HeartbeatData 多了 heartbeatFingerprint / withoutSub 两个字段。
//
// heartbeatFingerprint 的语义（见 broker ClientManageProcessor.heartBeat）：
//   - fingerprint != 0 -> 走 heartBeatV2 路径（按指纹判断订阅是否变化，可配合
//     withoutSub 跳过订阅上报）；
//   - fingerprint == 0 -> 走 V1 路径，用完整的 subscriptionDataSet 注册消费者。
// 因此**保持默认 0** 是最稳妥的选择：无需实现 computeHeartbeatFingerprint() 那套
// 依赖 fastjson2 字段序的指纹算法，也能被 broker 正确注册。
using RocketMQ.Common;

namespace RocketMQ.Remoting.Protocol;

/// <summary>org.apache.rocketmq.remoting.protocol.heartbeat.ConsumeType。</summary>
public static class ConsumeType
{
    public const string ConsumeActively = "CONSUME_ACTIVELY";
    public const string ConsumePassively = "CONSUME_PASSIVELY";
    public const string ConsumePop = "CONSUME_POP";
}

/// <summary>org.apache.rocketmq.remoting.protocol.heartbeat.MessageModel。</summary>
public static class MessageModel
{
    public const string Broadcasting = "BROADCASTING";
    public const string Clustering = "CLUSTERING";
    public const string LiteSelective = "LITE_SELECTIVE";
}

/// <summary>org.apache.rocketmq.common.consumer.ConsumeFromWhere。</summary>
public static class ConsumeFromWhere
{
    public const string ConsumeFromLastOffset = "CONSUME_FROM_LAST_OFFSET";
    public const string ConsumeFromFirstOffset = "CONSUME_FROM_FIRST_OFFSET";
    public const string ConsumeFromTimestamp = "CONSUME_FROM_TIMESTAMP";
}

/// <summary>org.apache.rocketmq.remoting.protocol.heartbeat.ProducerData。</summary>
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

    public override int GetHashCode() => JavaHash.JavaStringHash(GroupName);

    public override string ToString() => "ProducerData [groupName=" + GroupName + "]";
}

/// <summary>org.apache.rocketmq.remoting.protocol.heartbeat.ConsumerData。</summary>
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

    // 对应 Java subscriptionDataSet.add(...)
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

    public override int GetHashCode() => JavaHash.JavaStringHash(GroupName);

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

/// <summary>org.apache.rocketmq.remoting.protocol.heartbeat.HeartbeatData。</summary>
public sealed class HeartbeatData
{
    public string ClientId { get; set; } = string.Empty;
    public List<ProducerData> ProducerDataSet { get; set; } = new();
    public List<ConsumerData> ConsumerDataSet { get; set; } = new();

    // 0 = 走 broker 的 V1 注册路径（推荐）；非 0 才会触发 heartBeatV2 指纹优化
    public int HeartbeatFingerprint { get; set; }

    // 仅在 fingerprint != 0（V2 路径）时被 broker 读取
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
            hb.HeartbeatFingerprint = JavaNumber.ToInt32(n);
        }

        // 同时兼容 withoutSub 与 Java 字段名 isWithoutSub 两种写法
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
