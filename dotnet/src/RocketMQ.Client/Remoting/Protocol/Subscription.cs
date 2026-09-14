// 订阅组模型（对应 org.apache.rocketmq.remoting.protocol.subscription 包）：
// SubscriptionGroupConfig / GroupRetryPolicy / SimpleSubscriptionData /
// SubscriptionGroupWrapper。
//
// 字段名与默认值以 Java 5.x 探针实测为准：
//   JSON.toJSONString(new SubscriptionGroupConfig("MyGroup")) ->
//   {"attributes":{},"brokerId":0,"consumeBroadcastEnable":true,"consumeEnable":true,
//    "consumeFromMinEnable":true,"consumeMessageOrderly":false,"consumeTimeoutMinute":15,
//    "groupName":"MyGroup","groupRetryPolicy":{"type":"CUSTOMIZED"},"groupSysFlag":0,
//    "notifyConsumerIdsChangedEnable":true,"retryMaxTimes":16,"retryQueueNums":1,
//    "whichBrokerWhenConsumeSlowly":1}
// fastjson2 **跳过 null 字段**（subscriptionDataSet 为 null 时整个键不出现）。
using System.Globalization;
using RocketMQ.Common;

namespace RocketMQ.Remoting.Protocol;

/// <summary>org.apache.rocketmq.remoting.protocol.subscription.GroupRetryPolicyType。</summary>
public static class GroupRetryPolicyType
{
    public const string Exponential = "EXPONENTIAL";
    public const string Customized = "CUSTOMIZED";
}

/// <summary>
/// 对应 GroupRetryPolicy。两个子策略在 Java 里默认是 new 出来的实例，但 fastjson2
/// 的探针输出里只有 {"type":"CUSTOMIZED"} —— 说明它们实际序列化时被跳过。
/// 因此这里同样只在显式设置时输出（用 JsonValue 透传，避免过度建模）。
/// </summary>
public sealed class GroupRetryPolicy
{
    public string Type { get; set; } = GroupRetryPolicyType.Customized;

    // IsNull 表示不输出
    public JsonValue ExponentialRetryPolicy { get; set; } = JsonValue.Null;

    // IsNull 表示不输出
    public JsonValue CustomizedRetryPolicy { get; set; } = JsonValue.Null;

    public JsonValue ToJson()
    {
        var v = JsonValue.MakeObject();
        v.Set("type", JsonValue.MakeString(Type));
        if (!ExponentialRetryPolicy.IsNull)
        {
            v.Set("exponentialRetryPolicy", ExponentialRetryPolicy);
        }

        if (!CustomizedRetryPolicy.IsNull)
        {
            v.Set("customizedRetryPolicy", CustomizedRetryPolicy);
        }

        return v;
    }

    public static GroupRetryPolicy FromJson(JsonValue v)
    {
        var p = new GroupRetryPolicy();
        if (!v.IsObject)
        {
            return p;
        }

        p.Type = v.TryGetString("type", out string t) ? t : GroupRetryPolicyType.Customized;
        p.ExponentialRetryPolicy = v.Get("exponentialRetryPolicy");
        p.CustomizedRetryPolicy = v.Get("customizedRetryPolicy");
        return p;
    }
}

/// <summary>对应 SimpleSubscriptionData。</summary>
public sealed class SimpleSubscriptionData
{
    public string Topic { get; set; } = string.Empty;
    public string ExpressionType { get; set; } = "TAG";
    public string Expression { get; set; } = "*";
    public long Version { get; set; }

    public JsonValue ToJson()
    {
        var v = JsonValue.MakeObject();
        v.Set("topic", JsonValue.MakeString(Topic));
        v.Set("expressionType", JsonValue.MakeString(ExpressionType));
        v.Set("expression", JsonValue.MakeString(Expression));
        v.Set("version", JsonValue.MakeInt(Version));
        return v;
    }

    public static SimpleSubscriptionData FromJson(JsonValue v) =>
        new()
        {
            Topic = v.Get("topic").StringValue(),
            ExpressionType = v.Get("expressionType").StringValue("TAG"),
            Expression = v.Get("expression").StringValue("*"),
            Version = v.Get("version").IntValue(0),
        };
}

/// <summary>对应 SubscriptionGroupConfig。</summary>
public sealed class SubscriptionGroupConfig
{
    // MixAll.MASTER_ID
    public const int MasterId = 0;

    public string GroupName { get; set; } = string.Empty;
    public bool ConsumeEnable { get; set; } = true;
    public bool ConsumeFromMinEnable { get; set; } = true;
    public bool ConsumeBroadcastEnable { get; set; } = true;
    public bool ConsumeMessageOrderly { get; set; }
    public int RetryQueueNums { get; set; } = 1;
    public int RetryMaxTimes { get; set; } = 16;
    public GroupRetryPolicy GroupRetryPolicy { get; set; } = new();
    public int BrokerId { get; set; } = MasterId;
    public int WhichBrokerWhenConsumeSlowly { get; set; } = 1;
    public bool NotifyConsumerIdsChangedEnable { get; set; } = true;
    public int GroupSysFlag { get; set; }
    public int ConsumeTimeoutMinute { get; set; } = 15;
    public PropertyMap Attributes { get; set; } = new();

    // Java 为 null 时 fastjson2 整个键不输出
    public List<SimpleSubscriptionData> SubscriptionDataSet { get; } = new();
    public bool HasSubscriptionDataSet { get; set; }

    public SubscriptionGroupConfig()
    {
    }

    public SubscriptionGroupConfig(string name) => GroupName = name;

    public JsonValue ToJson()
    {
        var v = JsonValue.MakeObject();
        v.Set("groupName", JsonValue.MakeString(GroupName));
        v.Set("consumeEnable", JsonValue.MakeBool(ConsumeEnable));
        v.Set("consumeFromMinEnable", JsonValue.MakeBool(ConsumeFromMinEnable));
        v.Set("consumeBroadcastEnable", JsonValue.MakeBool(ConsumeBroadcastEnable));
        v.Set("consumeMessageOrderly", JsonValue.MakeBool(ConsumeMessageOrderly));
        v.Set("retryQueueNums", JsonValue.MakeInt(RetryQueueNums));
        v.Set("retryMaxTimes", JsonValue.MakeInt(RetryMaxTimes));
        v.Set("groupRetryPolicy", GroupRetryPolicy.ToJson());
        v.Set("brokerId", JsonValue.MakeInt(BrokerId));
        v.Set("whichBrokerWhenConsumeSlowly", JsonValue.MakeInt(WhichBrokerWhenConsumeSlowly));
        v.Set("notifyConsumerIdsChangedEnable", JsonValue.MakeBool(NotifyConsumerIdsChangedEnable));
        v.Set("groupSysFlag", JsonValue.MakeInt(GroupSysFlag));
        v.Set("consumeTimeoutMinute", JsonValue.MakeInt(ConsumeTimeoutMinute));
        v.Set("attributes", AdminJsonStrings.MakeStringMap(Attributes));
        // fastjson2 默认跳过 null：只有显式设置过才输出该键
        if (HasSubscriptionDataSet)
        {
            var arr = JsonValue.MakeArray();
            foreach (SimpleSubscriptionData s in SubscriptionDataSet)
            {
                arr.PushArray(s.ToJson());
            }

            v.Set("subscriptionDataSet", arr);
        }

        return v;
    }

    public static SubscriptionGroupConfig FromJson(JsonValue v)
    {
        var c = new SubscriptionGroupConfig
        {
            GroupName = v.Get("groupName").StringValue(),
            ConsumeEnable = v.Get("consumeEnable").BoolValue(true),
            ConsumeFromMinEnable = v.Get("consumeFromMinEnable").BoolValue(true),
            ConsumeBroadcastEnable = v.Get("consumeBroadcastEnable").BoolValue(true),
            ConsumeMessageOrderly = v.Get("consumeMessageOrderly").BoolValue(false),
            RetryQueueNums = JavaNumber.ToInt32(v.Get("retryQueueNums").IntValue(1)),
            RetryMaxTimes = JavaNumber.ToInt32(v.Get("retryMaxTimes").IntValue(16)),
            BrokerId = JavaNumber.ToInt32(v.Get("brokerId").IntValue(MasterId)),
            WhichBrokerWhenConsumeSlowly = JavaNumber.ToInt32(v.Get("whichBrokerWhenConsumeSlowly").IntValue(1)),
            NotifyConsumerIdsChangedEnable = v.Get("notifyConsumerIdsChangedEnable").BoolValue(true),
            GroupSysFlag = JavaNumber.ToInt32(v.Get("groupSysFlag").IntValue(0)),
            ConsumeTimeoutMinute = JavaNumber.ToInt32(v.Get("consumeTimeoutMinute").IntValue(15)),
            Attributes = ReadStringMap(v.Get("attributes")),
        };
        JsonValue? policy = v.Find("groupRetryPolicy");
        if (policy is not null)
        {
            c.GroupRetryPolicy = GroupRetryPolicy.FromJson(policy);
        }

        JsonValue? sub = v.Find("subscriptionDataSet");
        if (sub is { IsArray: true } && sub.Size() > 0)
        {
            c.HasSubscriptionDataSet = true;
            c.SubscriptionDataSet.Clear();
            for (int i = 0; i < sub.Size(); ++i)
            {
                c.SubscriptionDataSet.Add(SimpleSubscriptionData.FromJson(sub.At(i)));
            }
        }

        return c;
    }

    private static PropertyMap ReadStringMap(JsonValue v)
    {
        var @out = new PropertyMap();
        JsonValue? p = v.Find("attributes");
        if (p is { IsObject: true })
        {
            foreach (var kv in p.ObjectItems())
            {
                if (kv.Value.IsString)
                {
                    @out[kv.Key] = kv.Value.StringValue();
                }
            }
        }

        return @out;
    }

    public byte[] Encode() => RemotingSerializable.Encode(ToJson());

    public static bool Decode(byte[] data, out SubscriptionGroupConfig @out)
    {
        @out = new SubscriptionGroupConfig();
        if (!RemotingSerializable.Decode(data, out JsonValue v))
        {
            return false;
        }

        @out = FromJson(v);
        return true;
    }

    public override string ToString() =>
        "SubscriptionGroupConfig [groupName=" + GroupName
        + ", consumeEnable=" + (ConsumeEnable ? "true" : "false")
        + ", consumeFromMinEnable=" + (ConsumeFromMinEnable ? "true" : "false")
        + ", consumeBroadcastEnable=" + (ConsumeBroadcastEnable ? "true" : "false")
        + ", consumeMessageOrderly=" + (ConsumeMessageOrderly ? "true" : "false")
        + ", retryQueueNums=" + RetryQueueNums.ToString(CultureInfo.InvariantCulture)
        + ", retryMaxTimes=" + RetryMaxTimes.ToString(CultureInfo.InvariantCulture)
        + ", brokerId=" + BrokerId.ToString(CultureInfo.InvariantCulture)
        + ", whichBrokerWhenConsumeSlowly=" + WhichBrokerWhenConsumeSlowly.ToString(CultureInfo.InvariantCulture) + "]";
}

/// <summary>
/// 对应 org.apache.rocketmq.remoting.protocol.body.SubscriptionGroupWrapper。
/// 探针输出：{"dataVersion":{...},"forbiddenTable":{},"subscriptionGroupTable":{...}}
/// </summary>
public sealed class SubscriptionGroupWrapper
{
    public SortedDictionary<string, SubscriptionGroupConfig> SubscriptionGroupTable { get; } = new();

    // 透传（本实现不解释其内容）
    public JsonValue ForbiddenTable { get; set; } = JsonValue.Null;

    // 透传：翻页请求要原样回传，避免重排/丢精度
    public JsonValue DataVersion { get; set; } = JsonValue.Null;

    public JsonValue ToJson()
    {
        var v = JsonValue.MakeObject();
        v.Set("dataVersion", DataVersion.IsNull ? JsonValue.MakeObject() : DataVersion);
        v.Set("forbiddenTable", ForbiddenTable.IsNull ? JsonValue.MakeObject() : ForbiddenTable);
        var table = JsonValue.MakeObject();
        foreach (var kv in SubscriptionGroupTable)
        {
            table.Set(kv.Key, kv.Value.ToJson());
        }

        v.Set("subscriptionGroupTable", table);
        return v;
    }

    public static SubscriptionGroupWrapper FromJson(JsonValue v)
    {
        var w = new SubscriptionGroupWrapper();
        JsonValue? table = v.Find("subscriptionGroupTable");
        if (table is { IsObject: true })
        {
            foreach (var kv in table.ObjectItems())
            {
                w.SubscriptionGroupTable[kv.Key] = SubscriptionGroupConfig.FromJson(kv.Value);
            }
        }

        w.ForbiddenTable = v.Get("forbiddenTable");
        w.DataVersion = v.Get("dataVersion");
        return w;
    }

    public byte[] Encode() => RemotingSerializable.Encode(ToJson());

    public static bool Decode(byte[] data, out SubscriptionGroupWrapper @out)
    {
        @out = new SubscriptionGroupWrapper();
        if (!RemotingSerializable.Decode(data, out JsonValue v))
        {
            return false;
        }

        @out = FromJson(v);
        return true;
    }

    /// <summary>合并另一页（getAllSubscriptionGroup 分页累积用）。</summary>
    public void MergeFrom(SubscriptionGroupWrapper other)
    {
        foreach (var kv in other.SubscriptionGroupTable)
        {
            SubscriptionGroupTable[kv.Key] = kv.Value;
        }

        if (other.ForbiddenTable.IsObject)
        {
            if (!ForbiddenTable.IsObject)
            {
                ForbiddenTable = JsonValue.MakeObject();
            }

            foreach (var kv in other.ForbiddenTable.ObjectItems())
            {
                ForbiddenTable.Set(kv.Key, kv.Value);
            }
        }

        if (!other.DataVersion.IsNull)
        {
            DataVersion = other.DataVersion;
        }
    }
}
