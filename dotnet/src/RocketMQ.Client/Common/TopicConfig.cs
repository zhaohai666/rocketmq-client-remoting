// TopicConfig（对应 org.apache.rocketmq.common.TopicConfig）。
//
// 字段与默认值以 Java 5.x 为准（探针实测，勿凭记忆改）：
//   new TopicConfig("t") -> readQueueNums=16, writeQueueNums=16, perm=6,
//   topicFilterType=SINGLE_TAG, topicSysFlag=0, order=false, attributes={}。
//   attributes **会被序列化**（Java 的 getAttributes() 没有 serialize=false）。
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Text;
using RocketMQ.Remoting.Protocol;

namespace RocketMQ.Common;

// org.apache.rocketmq.common.TopicFilterType
public static class TopicFilterType
{
    public const string SingleTag = "SINGLE_TAG";
    public const string MultiTag = "MULTI_TAG";
}

public sealed class TopicConfig
{
    // TopicConfig.defaultReadQueueNums / defaultWriteQueueNums
    public const int DefaultReadQueueNums = 16;
    public const int DefaultWriteQueueNums = 16;
    // PermName.PERM_READ | PermName.PERM_WRITE
    public const int DefaultPerm = 6;

    public string TopicName { get; set; } = string.Empty;
    public int ReadQueueNums { get; set; } = DefaultReadQueueNums;
    public int WriteQueueNums { get; set; } = DefaultWriteQueueNums;
    public int Perm { get; set; } = DefaultPerm;
    public string TopicFilterType { get; set; } = RocketMQ.Common.TopicFilterType.SingleTag;
    public int TopicSysFlag { get; set; }
    public bool Order { get; set; }
    public PropertyMap Attributes { get; set; } = new PropertyMap();

    public TopicConfig()
    {
    }

    public TopicConfig(string name)
    {
        TopicName = name;
    }

    // 对应 Java TopicConfig.getPerm() 的可读形式（PermName.permToString）
    public string PermString() => PermName.PermToString(Perm);

    public JsonValue ToJson()
    {
        JsonValue v = JsonValue.MakeObject();
        v.Set("topicName", JsonValue.MakeString(TopicName));
        v.Set("readQueueNums", JsonValue.MakeInt(ReadQueueNums));
        v.Set("writeQueueNums", JsonValue.MakeInt(WriteQueueNums));
        v.Set("perm", JsonValue.MakeInt(Perm));
        v.Set("topicFilterType", JsonValue.MakeString(TopicFilterType));
        v.Set("topicSysFlag", JsonValue.MakeInt(TopicSysFlag));
        v.Set("order", JsonValue.MakeBool(Order));
        // Java：attributes 没有 serialize=false，因此**始终序列化**（即使是空 map）
        JsonValue attrs = JsonValue.MakeObject();
        foreach (var kv in Attributes)
        {
            attrs.Set(kv.Key, JsonValue.MakeString(kv.Value));
        }

        v.Set("attributes", attrs);
        return v;
    }

    public static TopicConfig FromJson(JsonValue v)
    {
        TopicConfig c = new TopicConfig();
        c.TopicName = v.Get("topicName").StringValue();
        JsonValue rq = v.Get("readQueueNums");
        c.ReadQueueNums = rq.IsNumber ? (int)rq.IntValue() : DefaultReadQueueNums;
        JsonValue wq = v.Get("writeQueueNums");
        c.WriteQueueNums = wq.IsNumber ? (int)wq.IntValue() : DefaultWriteQueueNums;
        JsonValue pm = v.Get("perm");
        c.Perm = pm.IsNumber ? (int)pm.IntValue() : DefaultPerm;
        if (v.TryGetString("topicFilterType", out string ft))
        {
            c.TopicFilterType = ft;
        }
        else
        {
            c.TopicFilterType = RocketMQ.Common.TopicFilterType.SingleTag;
        }

        JsonValue sf = v.Get("topicSysFlag");
        c.TopicSysFlag = sf.IsNumber ? (int)sf.IntValue() : 0;
        JsonValue? ord = v.Find("order");
        c.Order = ord is not null && ord.IsBool ? ord.BoolValue() : false;
        JsonValue? attrs = v.Find("attributes");
        if (attrs is not null && attrs.IsObject)
        {
            foreach (var kv in attrs.ObjectItems())
            {
                if (kv.Value.IsString)
                {
                    c.Attributes[kv.Key] = kv.Value.StringValue();
                }
            }
        }

        return c;
    }

    // 对应 C++ TopicConfig::encode：JsonValue -> UTF-8 字节串（RemotingSerializable::encode）。
    // 因 dotnet 侧暂无 RemotingSerializable，这里直接内联其语义（JSON dump -> UTF-8）。
    public byte[] Encode() => Encoding.UTF8.GetBytes(Json.Dump(ToJson()));

    // 解析失败返回 false（不抛异常），对应 C++ TopicConfig::decode。
    public static bool Decode(byte[] data, out TopicConfig outConfig)
    {
        outConfig = new TopicConfig();
        if (data is null || data.Length == 0)
        {
            return false;
        }

        string text = Encoding.UTF8.GetString(data);
        if (!Json.TryParse(text, out JsonValue v, out _))
        {
            return false;
        }

        outConfig = FromJson(v);
        return true;
    }

    public override string ToString()
    {
        return "TopicConfig [topicName=" + TopicName
            + ", readQueueNums=" + ReadQueueNums.ToString(CultureInfo.InvariantCulture)
            + ", writeQueueNums=" + WriteQueueNums.ToString(CultureInfo.InvariantCulture)
            + ", perm=" + PermString()
            + ", topicFilterType=" + TopicFilterType
            + ", topicSysFlag=" + TopicSysFlag.ToString(CultureInfo.InvariantCulture)
            + ", order=" + (Order ? "true" : "false") + "]";
    }
}
