// 请求/响应自定义头（对应 common/protocol/header/*.java）。
//
// C++ 侧用 std::optional 表达「字段未设置」；C# 天然对应可空类型（string? / int? / long? / bool?）。
// ToExtFields() 只写入已设置的字段，与 Java RemotingCommand.makeCustomHeaderToNet
// 的「非空才写」语义等价；FromExtFields() 缺省保持 null。
//
// ⚠ extFields 的键名是**协议的一部分**（Java 字段名），一个字母都不能错——
//   broker 用 fastjson2 按这些名字反序列化，错一个就静默丢字段。
//   特别注意 HeartbeatRequestHeader 的键是 "clientID"（大写 ID），不是 "clientId"。
using System.Globalization;
using RocketMQ.Common;

namespace RocketMQ.Remoting.Protocol;

/// <summary>对应 CommandCustomHeader 接口。</summary>
public interface ICommandCustomHeader
{
    PropertyMap ToExtFields();

    void FromExtFields(PropertyMap ext);
}

/// <summary>optional &lt;-&gt; 字符串 的编解码助手（对应 C++ 的 putOpt*/getOpt* 自由函数）。</summary>
public static class HeaderCodec
{
    public static void PutOptStr(PropertyMap outMap, string key, string? v)
    {
        if (v is not null)
        {
            outMap[key] = v;
        }
    }

    public static void PutOptLong(PropertyMap outMap, string key, long? v)
    {
        if (v.HasValue)
        {
            outMap[key] = v.Value.ToString(CultureInfo.InvariantCulture);
        }
    }

    public static void PutOptInt(PropertyMap outMap, string key, int? v)
    {
        if (v.HasValue)
        {
            outMap[key] = v.Value.ToString(CultureInfo.InvariantCulture);
        }
    }

    /// <summary>Java Boolean.toString(true) == "true"，与 broker 端 Boolean.parseBoolean 互为逆操作。</summary>
    public static void PutOptBool(PropertyMap outMap, string key, bool? v)
    {
        if (v.HasValue)
        {
            outMap[key] = v.Value ? "true" : "false";
        }
    }

    public static string? GetOptStr(PropertyMap ext, string key) =>
        ext.TryGetValue(key, out string? v) ? v : null;

    public static long? GetOptLong(PropertyMap ext, string key)
    {
        if (!ext.TryGetValue(key, out string? s) || s.Length == 0)
        {
            return null;
        }

        return long.TryParse(s, NumberStyles.Integer, CultureInfo.InvariantCulture, out long v)
            ? v
            : null;
    }

    public static int? GetOptInt(PropertyMap ext, string key)
    {
        long? v = GetOptLong(ext, key);
        return v.HasValue ? unchecked((int)v.Value) : null;
    }

    /// <summary>大小写不敏感地识别 "true"，另外接受 "1"；其余（含 "false"）一律返回 null。</summary>
    public static bool? GetOptBool(PropertyMap ext, string key)
    {
        if (!ext.TryGetValue(key, out string? s))
        {
            return null;
        }

        if (string.Equals(s, "true", StringComparison.OrdinalIgnoreCase))
        {
            return true;
        }

        return s == "1" ? true : null;
    }
}

// ------------------------------------------------ 发送消息

/// <summary>SendMessageRequestHeader：长字段名（V1）。</summary>
public sealed class SendMessageRequestHeader : ICommandCustomHeader
{
    public string? ProducerGroup { get; set; }
    public string? Topic { get; set; }
    public string? DefaultTopic { get; set; }
    public int? DefaultTopicQueueNums { get; set; }
    public int? QueueId { get; set; }
    public int? SysFlag { get; set; }
    public long? BornTimestamp { get; set; }
    public int? Flag { get; set; }
    public string? Properties { get; set; }
    public int? ReconsumeTimes { get; set; }
    public bool? UnitMode { get; set; }
    public int? MaxReconsumeTimes { get; set; }
    public bool? Batch { get; set; }
    public string? BrokerName { get; set; }

    public PropertyMap ToExtFields()
    {
        var outMap = new PropertyMap();
        HeaderCodec.PutOptStr(outMap, "producerGroup", ProducerGroup);
        HeaderCodec.PutOptStr(outMap, "topic", Topic);
        HeaderCodec.PutOptStr(outMap, "defaultTopic", DefaultTopic);
        HeaderCodec.PutOptInt(outMap, "defaultTopicQueueNums", DefaultTopicQueueNums);
        HeaderCodec.PutOptInt(outMap, "queueId", QueueId);
        HeaderCodec.PutOptInt(outMap, "sysFlag", SysFlag);
        HeaderCodec.PutOptLong(outMap, "bornTimestamp", BornTimestamp);
        HeaderCodec.PutOptInt(outMap, "flag", Flag);
        HeaderCodec.PutOptStr(outMap, "properties", Properties);
        HeaderCodec.PutOptInt(outMap, "reconsumeTimes", ReconsumeTimes);
        HeaderCodec.PutOptBool(outMap, "unitMode", UnitMode);
        HeaderCodec.PutOptInt(outMap, "maxReconsumeTimes", MaxReconsumeTimes);
        HeaderCodec.PutOptBool(outMap, "batch", Batch);
        HeaderCodec.PutOptStr(outMap, "brokerName", BrokerName);
        return outMap;
    }

    public void FromExtFields(PropertyMap ext)
    {
        ProducerGroup = HeaderCodec.GetOptStr(ext, "producerGroup");
        Topic = HeaderCodec.GetOptStr(ext, "topic");
        DefaultTopic = HeaderCodec.GetOptStr(ext, "defaultTopic");
        DefaultTopicQueueNums = HeaderCodec.GetOptInt(ext, "defaultTopicQueueNums");
        QueueId = HeaderCodec.GetOptInt(ext, "queueId");
        SysFlag = HeaderCodec.GetOptInt(ext, "sysFlag");
        BornTimestamp = HeaderCodec.GetOptLong(ext, "bornTimestamp");
        Flag = HeaderCodec.GetOptInt(ext, "flag");
        Properties = HeaderCodec.GetOptStr(ext, "properties");
        ReconsumeTimes = HeaderCodec.GetOptInt(ext, "reconsumeTimes");
        UnitMode = HeaderCodec.GetOptBool(ext, "unitMode");
        MaxReconsumeTimes = HeaderCodec.GetOptInt(ext, "maxReconsumeTimes");
        Batch = HeaderCodec.GetOptBool(ext, "batch");
        BrokerName = HeaderCodec.GetOptStr(ext, "brokerName");
    }
}

/// <summary>
/// SendMessageRequestHeaderV2：短字段名 a..n，减少每条消息的头部体积。
/// </summary>
public sealed class SendMessageRequestHeaderV2 : ICommandCustomHeader
{
    public string? ProducerGroup { get; set; }            // a
    public string? Topic { get; set; }                    // b
    public string? DefaultTopic { get; set; }             // c
    public int? DefaultTopicQueueNums { get; set; }       // d
    public int? QueueId { get; set; }                     // e
    public int? SysFlag { get; set; }                     // f
    public long? BornTimestamp { get; set; }              // g
    public int? Flag { get; set; }                        // h
    public string? Properties { get; set; }               // i
    public int? ReconsumeTimes { get; set; }              // j
    public bool? UnitMode { get; set; }                   // k
    public int? MaxReconsumeTimes { get; set; }           // l
    public bool? Batch { get; set; }                      // m
    public string? BrokerName { get; set; }               // n

    public PropertyMap ToExtFields()
    {
        var outMap = new PropertyMap();
        HeaderCodec.PutOptStr(outMap, "a", ProducerGroup);
        HeaderCodec.PutOptStr(outMap, "b", Topic);
        HeaderCodec.PutOptStr(outMap, "c", DefaultTopic);
        HeaderCodec.PutOptInt(outMap, "d", DefaultTopicQueueNums);
        HeaderCodec.PutOptInt(outMap, "e", QueueId);
        HeaderCodec.PutOptInt(outMap, "f", SysFlag);
        HeaderCodec.PutOptLong(outMap, "g", BornTimestamp);
        HeaderCodec.PutOptInt(outMap, "h", Flag);
        HeaderCodec.PutOptStr(outMap, "i", Properties);
        HeaderCodec.PutOptInt(outMap, "j", ReconsumeTimes);
        HeaderCodec.PutOptBool(outMap, "k", UnitMode);
        HeaderCodec.PutOptInt(outMap, "l", MaxReconsumeTimes);
        HeaderCodec.PutOptBool(outMap, "m", Batch);
        HeaderCodec.PutOptStr(outMap, "n", BrokerName);
        return outMap;
    }

    public void FromExtFields(PropertyMap ext)
    {
        ProducerGroup = HeaderCodec.GetOptStr(ext, "a");
        Topic = HeaderCodec.GetOptStr(ext, "b");
        DefaultTopic = HeaderCodec.GetOptStr(ext, "c");
        DefaultTopicQueueNums = HeaderCodec.GetOptInt(ext, "d");
        QueueId = HeaderCodec.GetOptInt(ext, "e");
        SysFlag = HeaderCodec.GetOptInt(ext, "f");
        BornTimestamp = HeaderCodec.GetOptLong(ext, "g");
        Flag = HeaderCodec.GetOptInt(ext, "h");
        Properties = HeaderCodec.GetOptStr(ext, "i");
        ReconsumeTimes = HeaderCodec.GetOptInt(ext, "j");
        UnitMode = HeaderCodec.GetOptBool(ext, "k");
        MaxReconsumeTimes = HeaderCodec.GetOptInt(ext, "l");
        Batch = HeaderCodec.GetOptBool(ext, "m");
        BrokerName = HeaderCodec.GetOptStr(ext, "n");
    }

    /// <summary>V1 -&gt; V2（对应 DefaultMQProducerImpl.sendKernelImpl 里的 implicitly 逻辑）。</summary>
    public static SendMessageRequestHeaderV2 FromV1(SendMessageRequestHeader v1) => new()
    {
        ProducerGroup = v1.ProducerGroup,
        Topic = v1.Topic,
        DefaultTopic = v1.DefaultTopic,
        DefaultTopicQueueNums = v1.DefaultTopicQueueNums,
        QueueId = v1.QueueId,
        SysFlag = v1.SysFlag,
        BornTimestamp = v1.BornTimestamp,
        Flag = v1.Flag,
        Properties = v1.Properties,
        ReconsumeTimes = v1.ReconsumeTimes,
        UnitMode = v1.UnitMode,
        MaxReconsumeTimes = v1.MaxReconsumeTimes,
        Batch = v1.Batch,
        BrokerName = v1.BrokerName,
    };

    /// <summary>V2 -&gt; V1。</summary>
    public SendMessageRequestHeader ToV1() => new()
    {
        ProducerGroup = ProducerGroup,
        Topic = Topic,
        DefaultTopic = DefaultTopic,
        DefaultTopicQueueNums = DefaultTopicQueueNums,
        QueueId = QueueId,
        SysFlag = SysFlag,
        BornTimestamp = BornTimestamp,
        Flag = Flag,
        Properties = Properties,
        ReconsumeTimes = ReconsumeTimes,
        UnitMode = UnitMode,
        MaxReconsumeTimes = MaxReconsumeTimes,
        Batch = Batch,
        BrokerName = BrokerName,
    };
}

public sealed class SendMessageResponseHeader : ICommandCustomHeader
{
    public string? MsgId { get; set; }
    public int? QueueId { get; set; }
    public long? QueueOffset { get; set; }
    public string? TransactionId { get; set; }
    public long? MsgRegion { get; set; }
    // 只有 inner-batch（消息落到 batch CQ）时 broker 才回：
    // SendMessageProcessor:630 把批量消息自身的 UNIQ_KEY 原样填进来。
    public string? BatchUniqId { get; set; }
    // 只给定时/延迟消息：broker 的 SendMessageProcessor#attachRecallHandle 看到
    // TIMER_OUT_MS + REAL_TOPIC 才挂上，普通消息恒为 null。
    public string? RecallHandle { get; set; }

    public PropertyMap ToExtFields()
    {
        var outMap = new PropertyMap();
        HeaderCodec.PutOptStr(outMap, "msgId", MsgId);
        HeaderCodec.PutOptInt(outMap, "queueId", QueueId);
        HeaderCodec.PutOptLong(outMap, "queueOffset", QueueOffset);
        HeaderCodec.PutOptStr(outMap, "transactionId", TransactionId);
        HeaderCodec.PutOptLong(outMap, "msgRegion", MsgRegion);
        HeaderCodec.PutOptStr(outMap, "batchUniqId", BatchUniqId);
        HeaderCodec.PutOptStr(outMap, "recallHandle", RecallHandle);
        return outMap;
    }

    public void FromExtFields(PropertyMap ext)
    {
        MsgId = HeaderCodec.GetOptStr(ext, "msgId");
        QueueId = HeaderCodec.GetOptInt(ext, "queueId");
        QueueOffset = HeaderCodec.GetOptLong(ext, "queueOffset");
        TransactionId = HeaderCodec.GetOptStr(ext, "transactionId");
        MsgRegion = HeaderCodec.GetOptLong(ext, "msgRegion");
        BatchUniqId = HeaderCodec.GetOptStr(ext, "batchUniqId");
        RecallHandle = HeaderCodec.GetOptStr(ext, "recallHandle");
    }
}

// 对应 org.apache.rocketmq.remoting.protocol.header.RecallMessageRequestHeader。
// ⚠ Java 侧继承 TopicRequestHeader → RpcRequestHeader，父类字段 bname 的**反射名就是
// bname**（不是 brokerName）：写成 brokerName 会被 broker 静默丢掉。
public sealed class RecallMessageRequestHeader : ICommandCustomHeader
{
    public string? ProducerGroup { get; set; }
    public string? Topic { get; set; }
    public string? RecallHandle { get; set; }
    public string? Bname { get; set; }

    public PropertyMap ToExtFields()
    {
        var outMap = new PropertyMap();
        HeaderCodec.PutOptStr(outMap, "producerGroup", ProducerGroup);
        HeaderCodec.PutOptStr(outMap, "topic", Topic);
        HeaderCodec.PutOptStr(outMap, "recallHandle", RecallHandle);
        HeaderCodec.PutOptStr(outMap, "bname", Bname);
        return outMap;
    }

    public void FromExtFields(PropertyMap ext)
    {
        ProducerGroup = HeaderCodec.GetOptStr(ext, "producerGroup");
        Topic = HeaderCodec.GetOptStr(ext, "topic");
        RecallHandle = HeaderCodec.GetOptStr(ext, "recallHandle");
        Bname = HeaderCodec.GetOptStr(ext, "bname");
    }
}

// 对应 RecallMessageResponseHeader：Java 只有一个字段 msgId（被撤回消息的 uniqKey）。
public sealed class RecallMessageResponseHeader : ICommandCustomHeader
{
    public string? MsgId { get; set; }

    public PropertyMap ToExtFields()
    {
        var outMap = new PropertyMap();
        HeaderCodec.PutOptStr(outMap, "msgId", MsgId);
        return outMap;
    }

    public void FromExtFields(PropertyMap ext)
    {
        MsgId = HeaderCodec.GetOptStr(ext, "msgId");
    }
}

// ------------------------------------------------ 拉取消息

public sealed class PullMessageRequestHeader : ICommandCustomHeader
{
    public string? ConsumerGroup { get; set; }
    public string? Topic { get; set; }
    public string? LiteTopic { get; set; }
    public int? QueueId { get; set; }
    public long? QueueOffset { get; set; }
    public int? MaxMsgNums { get; set; }
    public int? SysFlag { get; set; }
    public long? CommitOffset { get; set; }
    public long? SuspendTimeoutMillis { get; set; }
    public string? Subscription { get; set; }
    public long? SubVersion { get; set; }
    public string? ExpressionType { get; set; }
    public int? MaxMsgBytes { get; set; }
    public int? RequestSource { get; set; }
    public string? ProxyFrowardClientId { get; set; }

    public PropertyMap ToExtFields()
    {
        var outMap = new PropertyMap();
        HeaderCodec.PutOptStr(outMap, "consumerGroup", ConsumerGroup);
        HeaderCodec.PutOptStr(outMap, "topic", Topic);
        HeaderCodec.PutOptStr(outMap, "liteTopic", LiteTopic);
        HeaderCodec.PutOptInt(outMap, "queueId", QueueId);
        HeaderCodec.PutOptLong(outMap, "queueOffset", QueueOffset);
        HeaderCodec.PutOptInt(outMap, "maxMsgNums", MaxMsgNums);
        HeaderCodec.PutOptInt(outMap, "sysFlag", SysFlag);
        HeaderCodec.PutOptLong(outMap, "commitOffset", CommitOffset);
        HeaderCodec.PutOptLong(outMap, "suspendTimeoutMillis", SuspendTimeoutMillis);
        HeaderCodec.PutOptStr(outMap, "subscription", Subscription);
        HeaderCodec.PutOptLong(outMap, "subVersion", SubVersion);
        HeaderCodec.PutOptStr(outMap, "expressionType", ExpressionType);
        HeaderCodec.PutOptInt(outMap, "maxMsgBytes", MaxMsgBytes);
        HeaderCodec.PutOptInt(outMap, "requestSource", RequestSource);
        HeaderCodec.PutOptStr(outMap, "proxyFrowardClientId", ProxyFrowardClientId);
        return outMap;
    }

    public void FromExtFields(PropertyMap ext)
    {
        ConsumerGroup = HeaderCodec.GetOptStr(ext, "consumerGroup");
        Topic = HeaderCodec.GetOptStr(ext, "topic");
        LiteTopic = HeaderCodec.GetOptStr(ext, "liteTopic");
        QueueId = HeaderCodec.GetOptInt(ext, "queueId");
        QueueOffset = HeaderCodec.GetOptLong(ext, "queueOffset");
        MaxMsgNums = HeaderCodec.GetOptInt(ext, "maxMsgNums");
        SysFlag = HeaderCodec.GetOptInt(ext, "sysFlag");
        CommitOffset = HeaderCodec.GetOptLong(ext, "commitOffset");
        SuspendTimeoutMillis = HeaderCodec.GetOptLong(ext, "suspendTimeoutMillis");
        Subscription = HeaderCodec.GetOptStr(ext, "subscription");
        SubVersion = HeaderCodec.GetOptLong(ext, "subVersion");
        ExpressionType = HeaderCodec.GetOptStr(ext, "expressionType");
        MaxMsgBytes = HeaderCodec.GetOptInt(ext, "maxMsgBytes");
        RequestSource = HeaderCodec.GetOptInt(ext, "requestSource");
        ProxyFrowardClientId = HeaderCodec.GetOptStr(ext, "proxyFrowardClientId");
    }
}

public sealed class PullMessageResponseHeader : ICommandCustomHeader
{
    public long? SuggestWhichBrokerId { get; set; }
    public long? NextBeginOffset { get; set; }
    public long? MinOffset { get; set; }
    public long? MaxOffset { get; set; }
    public bool? ForbidCommitOffset { get; set; }

    public PropertyMap ToExtFields()
    {
        var outMap = new PropertyMap();
        HeaderCodec.PutOptLong(outMap, "suggestWhichBrokerId", SuggestWhichBrokerId);
        HeaderCodec.PutOptLong(outMap, "nextBeginOffset", NextBeginOffset);
        HeaderCodec.PutOptLong(outMap, "minOffset", MinOffset);
        HeaderCodec.PutOptLong(outMap, "maxOffset", MaxOffset);
        HeaderCodec.PutOptBool(outMap, "forbidCommitOffset", ForbidCommitOffset);
        return outMap;
    }

    public void FromExtFields(PropertyMap ext)
    {
        SuggestWhichBrokerId = HeaderCodec.GetOptLong(ext, "suggestWhichBrokerId");
        NextBeginOffset = HeaderCodec.GetOptLong(ext, "nextBeginOffset");
        MinOffset = HeaderCodec.GetOptLong(ext, "minOffset");
        MaxOffset = HeaderCodec.GetOptLong(ext, "maxOffset");
        ForbidCommitOffset = HeaderCodec.GetOptBool(ext, "forbidCommitOffset");
    }
}

// ------------------------------------------------ 消费位点

public sealed class QueryConsumerOffsetRequestHeader : ICommandCustomHeader
{
    public string? ConsumerGroup { get; set; }
    public string? Topic { get; set; }
    public int? QueueId { get; set; }

    public PropertyMap ToExtFields()
    {
        var outMap = new PropertyMap();
        HeaderCodec.PutOptStr(outMap, "consumerGroup", ConsumerGroup);
        HeaderCodec.PutOptStr(outMap, "topic", Topic);
        HeaderCodec.PutOptInt(outMap, "queueId", QueueId);
        return outMap;
    }

    public void FromExtFields(PropertyMap ext)
    {
        ConsumerGroup = HeaderCodec.GetOptStr(ext, "consumerGroup");
        Topic = HeaderCodec.GetOptStr(ext, "topic");
        QueueId = HeaderCodec.GetOptInt(ext, "queueId");
    }
}

public sealed class QueryConsumerOffsetResponseHeader : ICommandCustomHeader
{
    public long? Offset { get; set; }

    public PropertyMap ToExtFields()
    {
        var outMap = new PropertyMap();
        HeaderCodec.PutOptLong(outMap, "offset", Offset);
        return outMap;
    }

    public void FromExtFields(PropertyMap ext) => Offset = HeaderCodec.GetOptLong(ext, "offset");
}

public sealed class UpdateConsumerOffsetRequestHeader : ICommandCustomHeader
{
    public string? ConsumerGroup { get; set; }
    public string? Topic { get; set; }
    public int? QueueId { get; set; }
    public long? CommitOffset { get; set; }

    public PropertyMap ToExtFields()
    {
        var outMap = new PropertyMap();
        HeaderCodec.PutOptStr(outMap, "consumerGroup", ConsumerGroup);
        HeaderCodec.PutOptStr(outMap, "topic", Topic);
        HeaderCodec.PutOptInt(outMap, "queueId", QueueId);
        HeaderCodec.PutOptLong(outMap, "commitOffset", CommitOffset);
        return outMap;
    }

    public void FromExtFields(PropertyMap ext)
    {
        ConsumerGroup = HeaderCodec.GetOptStr(ext, "consumerGroup");
        Topic = HeaderCodec.GetOptStr(ext, "topic");
        QueueId = HeaderCodec.GetOptInt(ext, "queueId");
        CommitOffset = HeaderCodec.GetOptLong(ext, "commitOffset");
    }
}

public sealed class UpdateConsumerOffsetResponseHeader : ICommandCustomHeader
{
    public PropertyMap ToExtFields() => new();

    public void FromExtFields(PropertyMap ext)
    {
        // 空头
    }
}

// ------------------------------------------------ offset 查询

public sealed class GetMaxOffsetRequestHeader : ICommandCustomHeader
{
    public string? Topic { get; set; }
    public int? QueueId { get; set; }

    public PropertyMap ToExtFields()
    {
        var outMap = new PropertyMap();
        HeaderCodec.PutOptStr(outMap, "topic", Topic);
        HeaderCodec.PutOptInt(outMap, "queueId", QueueId);
        return outMap;
    }

    public void FromExtFields(PropertyMap ext)
    {
        Topic = HeaderCodec.GetOptStr(ext, "topic");
        QueueId = HeaderCodec.GetOptInt(ext, "queueId");
    }
}

public sealed class GetMaxOffsetResponseHeader : ICommandCustomHeader
{
    public long? Offset { get; set; }

    public PropertyMap ToExtFields()
    {
        var outMap = new PropertyMap();
        HeaderCodec.PutOptLong(outMap, "offset", Offset);
        return outMap;
    }

    public void FromExtFields(PropertyMap ext) => Offset = HeaderCodec.GetOptLong(ext, "offset");
}

public sealed class GetMinOffsetRequestHeader : ICommandCustomHeader
{
    public string? Topic { get; set; }
    public int? QueueId { get; set; }

    public PropertyMap ToExtFields()
    {
        var outMap = new PropertyMap();
        HeaderCodec.PutOptStr(outMap, "topic", Topic);
        HeaderCodec.PutOptInt(outMap, "queueId", QueueId);
        return outMap;
    }

    public void FromExtFields(PropertyMap ext)
    {
        Topic = HeaderCodec.GetOptStr(ext, "topic");
        QueueId = HeaderCodec.GetOptInt(ext, "queueId");
    }
}

public sealed class GetMinOffsetResponseHeader : ICommandCustomHeader
{
    public long? Offset { get; set; }

    public PropertyMap ToExtFields()
    {
        var outMap = new PropertyMap();
        HeaderCodec.PutOptLong(outMap, "offset", Offset);
        return outMap;
    }

    public void FromExtFields(PropertyMap ext) => Offset = HeaderCodec.GetOptLong(ext, "offset");
}

public sealed class SearchOffsetRequestHeader : ICommandCustomHeader
{
    public string? Topic { get; set; }
    public int? QueueId { get; set; }
    public long? Timestamp { get; set; }

    public PropertyMap ToExtFields()
    {
        var outMap = new PropertyMap();
        HeaderCodec.PutOptStr(outMap, "topic", Topic);
        HeaderCodec.PutOptInt(outMap, "queueId", QueueId);
        HeaderCodec.PutOptLong(outMap, "timestamp", Timestamp);
        return outMap;
    }

    public void FromExtFields(PropertyMap ext)
    {
        Topic = HeaderCodec.GetOptStr(ext, "topic");
        QueueId = HeaderCodec.GetOptInt(ext, "queueId");
        Timestamp = HeaderCodec.GetOptLong(ext, "timestamp");
    }
}

public sealed class SearchOffsetResponseHeader : ICommandCustomHeader
{
    public long? Offset { get; set; }

    public PropertyMap ToExtFields()
    {
        var outMap = new PropertyMap();
        HeaderCodec.PutOptLong(outMap, "offset", Offset);
        return outMap;
    }

    public void FromExtFields(PropertyMap ext) => Offset = HeaderCodec.GetOptLong(ext, "offset");
}

// ------------------------------------------------ 其它常用头部

public sealed class ViewMessageRequestHeader : ICommandCustomHeader
{
    public long? Offset { get; set; }

    public PropertyMap ToExtFields()
    {
        var outMap = new PropertyMap();
        HeaderCodec.PutOptLong(outMap, "offset", Offset);
        return outMap;
    }

    public void FromExtFields(PropertyMap ext) => Offset = HeaderCodec.GetOptLong(ext, "offset");
}

public sealed class QueryMessageRequestHeader : ICommandCustomHeader
{
    public string? Topic { get; set; }
    public string? Key { get; set; }
    public int? MaxNum { get; set; }
    public long? BeginTimestamp { get; set; }
    public long? EndTimestamp { get; set; }

    /// <summary>索引类型："K"=普通 KEYS 索引，"U"=uniqKey（需 RocksDB 索引），"T"=tag。空值时 broker 按 "K" 处理。</summary>
    public string? IndexType { get; set; }

    /// <summary>分页游标：broker 每页最多返回 maxNum 条，翻页时带上上一页最后一条的 key。</summary>
    public string? LastKey { get; set; }

    public PropertyMap ToExtFields()
    {
        var outMap = new PropertyMap();
        HeaderCodec.PutOptStr(outMap, "topic", Topic);
        HeaderCodec.PutOptStr(outMap, "key", Key);
        HeaderCodec.PutOptInt(outMap, "maxNum", MaxNum);
        HeaderCodec.PutOptLong(outMap, "beginTimestamp", BeginTimestamp);
        HeaderCodec.PutOptLong(outMap, "endTimestamp", EndTimestamp);
        HeaderCodec.PutOptStr(outMap, "indexType", IndexType);
        HeaderCodec.PutOptStr(outMap, "lastKey", LastKey);
        return outMap;
    }

    public void FromExtFields(PropertyMap ext)
    {
        Topic = HeaderCodec.GetOptStr(ext, "topic");
        Key = HeaderCodec.GetOptStr(ext, "key");
        MaxNum = HeaderCodec.GetOptInt(ext, "maxNum");
        BeginTimestamp = HeaderCodec.GetOptLong(ext, "beginTimestamp");
        EndTimestamp = HeaderCodec.GetOptLong(ext, "endTimestamp");
        IndexType = HeaderCodec.GetOptStr(ext, "indexType");
        LastKey = HeaderCodec.GetOptStr(ext, "lastKey");
    }
}

public sealed class EndTransactionRequestHeader : ICommandCustomHeader
{
    /// <summary>对应 setTopic(msg.getTopic())。</summary>
    public string? Topic { get; set; }

    public string? ProducerGroup { get; set; }
    public long? TranStateTableOffset { get; set; }
    public long? CommitLogOffset { get; set; }

    /// <summary>MessageSysFlag.TRANSACTION_*_TYPE</summary>
    public int? CommitOrRollback { get; set; }

    public bool? FromTransactionCheck { get; set; }
    public string? MsgId { get; set; }
    public string? TransactionId { get; set; }

    // ⚠ 键名必须是 "bname"（RpcRequestHeader 的字段名），不能是 brokerName——
    //   否则 Java broker 用 setBrokerName 反序列化时静默丢字段。
    public string? Bname { get; set; }

    public PropertyMap ToExtFields()
    {
        var outMap = new PropertyMap();
        HeaderCodec.PutOptStr(outMap, "topic", Topic);
        HeaderCodec.PutOptStr(outMap, "producerGroup", ProducerGroup);
        HeaderCodec.PutOptLong(outMap, "tranStateTableOffset", TranStateTableOffset);
        HeaderCodec.PutOptLong(outMap, "commitLogOffset", CommitLogOffset);
        HeaderCodec.PutOptInt(outMap, "commitOrRollback", CommitOrRollback);
        HeaderCodec.PutOptBool(outMap, "fromTransactionCheck", FromTransactionCheck);
        HeaderCodec.PutOptStr(outMap, "msgId", MsgId);
        HeaderCodec.PutOptStr(outMap, "transactionId", TransactionId);
        HeaderCodec.PutOptStr(outMap, "bname", Bname);
        return outMap;
    }

    public void FromExtFields(PropertyMap ext)
    {
        Topic = HeaderCodec.GetOptStr(ext, "topic");
        ProducerGroup = HeaderCodec.GetOptStr(ext, "producerGroup");
        TranStateTableOffset = HeaderCodec.GetOptLong(ext, "tranStateTableOffset");
        CommitLogOffset = HeaderCodec.GetOptLong(ext, "commitLogOffset");
        CommitOrRollback = HeaderCodec.GetOptInt(ext, "commitOrRollback");
        FromTransactionCheck = HeaderCodec.GetOptBool(ext, "fromTransactionCheck");
        MsgId = HeaderCodec.GetOptStr(ext, "msgId");
        TransactionId = HeaderCodec.GetOptStr(ext, "transactionId");
        Bname = HeaderCodec.GetOptStr(ext, "bname");
    }
}

public sealed class ConsumerSendMsgBackRequestHeader : ICommandCustomHeader
{
    public string? Group { get; set; }
    public long? Offset { get; set; }
    public int? DelayLevel { get; set; }
    public string? OriginMsgId { get; set; }
    public string? OriginTopic { get; set; }
    public bool? UnitMode { get; set; }
    public int? MaxReconsumeTimes { get; set; }

    public PropertyMap ToExtFields()
    {
        var outMap = new PropertyMap();
        HeaderCodec.PutOptStr(outMap, "group", Group);
        HeaderCodec.PutOptLong(outMap, "offset", Offset);
        HeaderCodec.PutOptInt(outMap, "delayLevel", DelayLevel);
        HeaderCodec.PutOptStr(outMap, "originMsgId", OriginMsgId);
        HeaderCodec.PutOptStr(outMap, "originTopic", OriginTopic);
        HeaderCodec.PutOptBool(outMap, "unitMode", UnitMode);
        HeaderCodec.PutOptInt(outMap, "maxReconsumeTimes", MaxReconsumeTimes);
        return outMap;
    }

    public void FromExtFields(PropertyMap ext)
    {
        Group = HeaderCodec.GetOptStr(ext, "group");
        Offset = HeaderCodec.GetOptLong(ext, "offset");
        DelayLevel = HeaderCodec.GetOptInt(ext, "delayLevel");
        OriginMsgId = HeaderCodec.GetOptStr(ext, "originMsgId");
        OriginTopic = HeaderCodec.GetOptStr(ext, "originTopic");
        UnitMode = HeaderCodec.GetOptBool(ext, "unitMode");
        MaxReconsumeTimes = HeaderCodec.GetOptInt(ext, "maxReconsumeTimes");
    }
}

public sealed class HeartbeatRequestHeader : ICommandCustomHeader
{
    public string? ClientId { get; set; }

    public PropertyMap ToExtFields()
    {
        var outMap = new PropertyMap();
        // ⚠ 键名是 "clientID"（Java 字段名如此），不是 "clientId"
        HeaderCodec.PutOptStr(outMap, "clientID", ClientId);
        return outMap;
    }

    public void FromExtFields(PropertyMap ext) => ClientId = HeaderCodec.GetOptStr(ext, "clientID");
}

public sealed class UnregisterClientRequestHeader : ICommandCustomHeader
{
    public string? ClientId { get; set; }
    public string? ProducerGroup { get; set; }
    public string? ConsumerGroup { get; set; }

    public PropertyMap ToExtFields()
    {
        var outMap = new PropertyMap();
        HeaderCodec.PutOptStr(outMap, "clientID", ClientId);
        HeaderCodec.PutOptStr(outMap, "producerGroup", ProducerGroup);
        HeaderCodec.PutOptStr(outMap, "consumerGroup", ConsumerGroup);
        return outMap;
    }

    public void FromExtFields(PropertyMap ext)
    {
        ClientId = HeaderCodec.GetOptStr(ext, "clientID");
        ProducerGroup = HeaderCodec.GetOptStr(ext, "producerGroup");
        ConsumerGroup = HeaderCodec.GetOptStr(ext, "consumerGroup");
    }
}

public sealed class GetConsumerListByGroupRequestHeader : ICommandCustomHeader
{
    public string? ConsumerGroup { get; set; }

    // ⚠ 该头继承 RpcRequestHeader，Java 字段名是 "bname"（不是 "brokerName"）。
    // findConsumerIdList 发往 topic 路由里的第一个 broker，bname 填该 brokerName。
    // broker 处理 GET_CONSUMER_LIST_BY_GROUP 时实际只用 consumerGroup，bname 是协议对齐项。
    public string? Bname { get; set; }

    public PropertyMap ToExtFields()
    {
        var outMap = new PropertyMap();
        HeaderCodec.PutOptStr(outMap, "consumerGroup", ConsumerGroup);
        HeaderCodec.PutOptStr(outMap, "bname", Bname);
        return outMap;
    }

    public void FromExtFields(PropertyMap ext)
    {
        ConsumerGroup = HeaderCodec.GetOptStr(ext, "consumerGroup");
        Bname = HeaderCodec.GetOptStr(ext, "bname");
    }
}

public sealed class GetConsumerListByGroupResponseHeader : ICommandCustomHeader
{
    public PropertyMap ToExtFields() => new();

    public void FromExtFields(PropertyMap ext)
    {
        // 空头
    }
}

public sealed class NotifyConsumerIdsChangedRequestHeader : ICommandCustomHeader
{
    public string? ConsumerGroup { get; set; }

    public PropertyMap ToExtFields()
    {
        var outMap = new PropertyMap();
        HeaderCodec.PutOptStr(outMap, "consumerGroup", ConsumerGroup);
        return outMap;
    }

    public void FromExtFields(PropertyMap ext) =>
        ConsumerGroup = HeaderCodec.GetOptStr(ext, "consumerGroup");
}

public sealed class GetRouteInfoRequestHeader : ICommandCustomHeader
{
    public string? Topic { get; set; }

    public PropertyMap ToExtFields()
    {
        var outMap = new PropertyMap();
        HeaderCodec.PutOptStr(outMap, "topic", Topic);
        return outMap;
    }

    public void FromExtFields(PropertyMap ext) => Topic = HeaderCodec.GetOptStr(ext, "topic");
}

public sealed class CheckTransactionStateRequestHeader : ICommandCustomHeader
{
    /// <summary>对应 RpcRequestHeader 基类的 topic 字段（CHECK 请求里 broker 也带了 topic）。</summary>
    public string? Topic { get; set; }

    public long? TranStateTableOffset { get; set; }
    public long? CommitLogOffset { get; set; }
    public string? MsgId { get; set; }
    public string? TransactionId { get; set; }

    /// <summary>⚠ Java 的 offsetMsgId 是 **String**，不是 long。</summary>
    public string? OffsetMsgId { get; set; }

    // ⚠ 键名必须是 "bname"（同 EndTransactionRequestHeader）。
    public string? Bname { get; set; }

    public PropertyMap ToExtFields()
    {
        var outMap = new PropertyMap();
        HeaderCodec.PutOptStr(outMap, "topic", Topic);
        HeaderCodec.PutOptLong(outMap, "tranStateTableOffset", TranStateTableOffset);
        HeaderCodec.PutOptLong(outMap, "commitLogOffset", CommitLogOffset);
        HeaderCodec.PutOptStr(outMap, "msgId", MsgId);
        HeaderCodec.PutOptStr(outMap, "transactionId", TransactionId);
        HeaderCodec.PutOptStr(outMap, "offsetMsgId", OffsetMsgId);
        HeaderCodec.PutOptStr(outMap, "bname", Bname);
        return outMap;
    }

    public void FromExtFields(PropertyMap ext)
    {
        Topic = HeaderCodec.GetOptStr(ext, "topic");
        TranStateTableOffset = HeaderCodec.GetOptLong(ext, "tranStateTableOffset");
        CommitLogOffset = HeaderCodec.GetOptLong(ext, "commitLogOffset");
        MsgId = HeaderCodec.GetOptStr(ext, "msgId");
        TransactionId = HeaderCodec.GetOptStr(ext, "transactionId");
        OffsetMsgId = HeaderCodec.GetOptStr(ext, "offsetMsgId");
        Bname = HeaderCodec.GetOptStr(ext, "bname");
    }
}

/// <summary>
/// 对应 org.apache.rocketmq.remoting.protocol.header.CreateTopicRequestHeader，
/// 用于 UPDATE_AND_CREATE_TOPIC：在 broker 上按指定队列数/权限建 topic。
/// </summary>
public sealed class CreateTopicRequestHeader : ICommandCustomHeader
{
    public string? Topic { get; set; }
    public string? DefaultTopic { get; set; }
    public int? ReadQueueNums { get; set; }
    public int? WriteQueueNums { get; set; }
    public int? Perm { get; set; }
    public string? TopicFilterType { get; set; }   // 默认 SINGLE_TAG
    public int? TopicSysFlag { get; set; }
    public bool? Order { get; set; }               // 默认 false
    public string? Attributes { get; set; }
    public bool? Force { get; set; }               // 默认 false

    public PropertyMap ToExtFields()
    {
        var outMap = new PropertyMap();
        HeaderCodec.PutOptStr(outMap, "topic", Topic);
        HeaderCodec.PutOptStr(outMap, "defaultTopic", DefaultTopic);
        HeaderCodec.PutOptInt(outMap, "readQueueNums", ReadQueueNums);
        HeaderCodec.PutOptInt(outMap, "writeQueueNums", WriteQueueNums);
        HeaderCodec.PutOptInt(outMap, "perm", Perm);
        HeaderCodec.PutOptStr(outMap, "topicFilterType", TopicFilterType);
        HeaderCodec.PutOptInt(outMap, "topicSysFlag", TopicSysFlag);
        HeaderCodec.PutOptBool(outMap, "order", Order);
        HeaderCodec.PutOptStr(outMap, "attributes", Attributes);
        HeaderCodec.PutOptBool(outMap, "force", Force);
        return outMap;
    }

    public void FromExtFields(PropertyMap ext)
    {
        Topic = HeaderCodec.GetOptStr(ext, "topic");
        DefaultTopic = HeaderCodec.GetOptStr(ext, "defaultTopic");
        ReadQueueNums = HeaderCodec.GetOptInt(ext, "readQueueNums");
        WriteQueueNums = HeaderCodec.GetOptInt(ext, "writeQueueNums");
        Perm = HeaderCodec.GetOptInt(ext, "perm");
        TopicFilterType = HeaderCodec.GetOptStr(ext, "topicFilterType");
        TopicSysFlag = HeaderCodec.GetOptInt(ext, "topicSysFlag");
        Order = HeaderCodec.GetOptBool(ext, "order");
        Attributes = HeaderCodec.GetOptStr(ext, "attributes");
        Force = HeaderCodec.GetOptBool(ext, "force");
    }
}

/// <summary>
/// 对应 org.apache.rocketmq.remoting.protocol.header.ReplyMessageRequestHeader：
/// broker → 请求方 的 PUSH_REPLY_MESSAGE_TO_CLIENT(326) 请求头。
///
/// 字段与 SendMessageRequestHeaderV2 高度重合，但多了 bornHost / storeHost /
/// storeTimestamp（broker 的 ReplyMessageProcessor#pushReplyMessage 据此拼出头）。
/// 请求方收到后结合 body 还原出真正的应答 MessageExt。
/// </summary>
public sealed class ReplyMessageRequestHeader : ICommandCustomHeader
{
    public string? ProducerGroup { get; set; }
    public string? Topic { get; set; }
    public string? DefaultTopic { get; set; }
    public int? DefaultTopicQueueNums { get; set; }
    public int? QueueId { get; set; }
    public int? SysFlag { get; set; }
    public long? BornTimestamp { get; set; }
    public int? Flag { get; set; }
    public string? Properties { get; set; }
    public int? ReconsumeTimes { get; set; }
    public bool? UnitMode { get; set; }
    public string? BornHost { get; set; }
    public string? StoreHost { get; set; }
    public long? StoreTimestamp { get; set; }

    public PropertyMap ToExtFields()
    {
        var outMap = new PropertyMap();
        HeaderCodec.PutOptStr(outMap, "producerGroup", ProducerGroup);
        HeaderCodec.PutOptStr(outMap, "topic", Topic);
        HeaderCodec.PutOptStr(outMap, "defaultTopic", DefaultTopic);
        HeaderCodec.PutOptInt(outMap, "defaultTopicQueueNums", DefaultTopicQueueNums);
        HeaderCodec.PutOptInt(outMap, "queueId", QueueId);
        HeaderCodec.PutOptInt(outMap, "sysFlag", SysFlag);
        HeaderCodec.PutOptLong(outMap, "bornTimestamp", BornTimestamp);
        HeaderCodec.PutOptInt(outMap, "flag", Flag);
        HeaderCodec.PutOptStr(outMap, "properties", Properties);
        HeaderCodec.PutOptInt(outMap, "reconsumeTimes", ReconsumeTimes);
        HeaderCodec.PutOptBool(outMap, "unitMode", UnitMode);
        HeaderCodec.PutOptStr(outMap, "bornHost", BornHost);
        HeaderCodec.PutOptStr(outMap, "storeHost", StoreHost);
        HeaderCodec.PutOptLong(outMap, "storeTimestamp", StoreTimestamp);
        return outMap;
    }

    public void FromExtFields(PropertyMap ext)
    {
        ProducerGroup = HeaderCodec.GetOptStr(ext, "producerGroup");
        Topic = HeaderCodec.GetOptStr(ext, "topic");
        DefaultTopic = HeaderCodec.GetOptStr(ext, "defaultTopic");
        DefaultTopicQueueNums = HeaderCodec.GetOptInt(ext, "defaultTopicQueueNums");
        QueueId = HeaderCodec.GetOptInt(ext, "queueId");
        SysFlag = HeaderCodec.GetOptInt(ext, "sysFlag");
        BornTimestamp = HeaderCodec.GetOptLong(ext, "bornTimestamp");
        Flag = HeaderCodec.GetOptInt(ext, "flag");
        Properties = HeaderCodec.GetOptStr(ext, "properties");
        ReconsumeTimes = HeaderCodec.GetOptInt(ext, "reconsumeTimes");
        UnitMode = HeaderCodec.GetOptBool(ext, "unitMode");
        BornHost = HeaderCodec.GetOptStr(ext, "bornHost");
        StoreHost = HeaderCodec.GetOptStr(ext, "storeHost");
        StoreTimestamp = HeaderCodec.GetOptLong(ext, "storeTimestamp");
    }
}

// ------------------------------------------------ POP（5.x 轻量消费）
//
// ⚠ ext 键名必须逐字等于 Java 字段名：broker 用 fastjson2 按 Java 属性名反序列化，
//   错一个字母就**静默丢字段**（不报错，只是那个条件不生效）。
//   例如 maxMsgNums 写成 maxMsgNum 会被 broker 当 0 处理。

/// <summary>org.apache.rocketmq.remoting.protocol.header.PopMessageRequestHeader。</summary>
public sealed class PopMessageRequestHeader : ICommandCustomHeader
{
    public string? ConsumerGroup { get; set; }
    public string? Topic { get; set; }
    public int? QueueId { get; set; }
    public int? MaxMsgNums { get; set; }
    public long? InvisibleTime { get; set; }

    /// <summary>长轮询挂起时长；本客户端只做短轮询，恒为 0。</summary>
    public long? PollTime { get; set; }

    /// <summary>
    /// ⚠ 必须填**当前毫秒时间戳**。broker 校验 now - bornTime - pollTime > 500 直接回
    /// POLLING_TIMEOUT(210)（PopMessageRequestHeader.isTimeoutTooMuch）。
    /// </summary>
    public long? BornTime { get; set; }

    /// <summary>ConsumeInitMode：0=MIN（从最小位点消费历史），1=MAX（只取新消息）。</summary>
    public int? InitMode { get; set; }

    public string? ExpType { get; set; }
    public string? Exp { get; set; }

    /// <summary>Java 是 primitive boolean，**总是在 extFields 里**（与 Python/C++ 侧一致）。</summary>
    public bool Order { get; set; }

    public string? AttemptId { get; set; }

    public PropertyMap ToExtFields()
    {
        var outMap = new PropertyMap();
        HeaderCodec.PutOptStr(outMap, "consumerGroup", ConsumerGroup);
        HeaderCodec.PutOptStr(outMap, "topic", Topic);
        HeaderCodec.PutOptInt(outMap, "queueId", QueueId);
        HeaderCodec.PutOptInt(outMap, "maxMsgNums", MaxMsgNums);
        HeaderCodec.PutOptLong(outMap, "invisibleTime", InvisibleTime);
        HeaderCodec.PutOptLong(outMap, "pollTime", PollTime);
        HeaderCodec.PutOptLong(outMap, "bornTime", BornTime);
        HeaderCodec.PutOptInt(outMap, "initMode", InitMode);
        HeaderCodec.PutOptStr(outMap, "expType", ExpType);
        HeaderCodec.PutOptStr(outMap, "exp", Exp);
        HeaderCodec.PutOptBool(outMap, "order", Order);
        HeaderCodec.PutOptStr(outMap, "attemptId", AttemptId);
        return outMap;
    }

    public void FromExtFields(PropertyMap ext)
    {
        ConsumerGroup = HeaderCodec.GetOptStr(ext, "consumerGroup");
        Topic = HeaderCodec.GetOptStr(ext, "topic");
        QueueId = HeaderCodec.GetOptInt(ext, "queueId");
        MaxMsgNums = HeaderCodec.GetOptInt(ext, "maxMsgNums");
        InvisibleTime = HeaderCodec.GetOptLong(ext, "invisibleTime");
        PollTime = HeaderCodec.GetOptLong(ext, "pollTime");
        BornTime = HeaderCodec.GetOptLong(ext, "bornTime");
        InitMode = HeaderCodec.GetOptInt(ext, "initMode");
        ExpType = HeaderCodec.GetOptStr(ext, "expType");
        Exp = HeaderCodec.GetOptStr(ext, "exp");
        Order = HeaderCodec.GetOptBool(ext, "order") ?? false;
        AttemptId = HeaderCodec.GetOptStr(ext, "attemptId");
    }
}

/// <summary>
/// org.apache.rocketmq.remoting.protocol.header.PopMessageResponseHeader。
/// startOffsetInfo / msgOffsetInfo 是客户端反构 POP_CK 的唯一来源。
/// </summary>
public sealed class PopMessageResponseHeader : ICommandCustomHeader
{
    public long? PopTime { get; set; }
    public long? InvisibleTime { get; set; }
    public int? ReviveQid { get; set; }
    public long? RestNum { get; set; }
    public string? StartOffsetInfo { get; set; }
    public string? MsgOffsetInfo { get; set; }
    public string? OrderCountInfo { get; set; }

    public PropertyMap ToExtFields()
    {
        var outMap = new PropertyMap();
        HeaderCodec.PutOptLong(outMap, "popTime", PopTime);
        HeaderCodec.PutOptLong(outMap, "invisibleTime", InvisibleTime);
        HeaderCodec.PutOptInt(outMap, "reviveQid", ReviveQid);
        HeaderCodec.PutOptLong(outMap, "restNum", RestNum);
        HeaderCodec.PutOptStr(outMap, "startOffsetInfo", StartOffsetInfo);
        HeaderCodec.PutOptStr(outMap, "msgOffsetInfo", MsgOffsetInfo);
        HeaderCodec.PutOptStr(outMap, "orderCountInfo", OrderCountInfo);
        return outMap;
    }

    public void FromExtFields(PropertyMap ext)
    {
        PopTime = HeaderCodec.GetOptLong(ext, "popTime");
        InvisibleTime = HeaderCodec.GetOptLong(ext, "invisibleTime");
        ReviveQid = HeaderCodec.GetOptInt(ext, "reviveQid");
        RestNum = HeaderCodec.GetOptLong(ext, "restNum");
        StartOffsetInfo = HeaderCodec.GetOptStr(ext, "startOffsetInfo");
        MsgOffsetInfo = HeaderCodec.GetOptStr(ext, "msgOffsetInfo");
        OrderCountInfo = HeaderCodec.GetOptStr(ext, "orderCountInfo");
    }
}

/// <summary>
/// org.apache.rocketmq.remoting.protocol.header.AckMessageRequestHeader。
/// ⚠ Offset 是 **consumeQueue offset**（CK 串第 8 段 / msgQueueOffset），不是 commitlog offset。
/// </summary>
public sealed class AckMessageRequestHeader : ICommandCustomHeader
{
    public string? ConsumerGroup { get; set; }
    public string? Topic { get; set; }
    public int? QueueId { get; set; }
    public string? ExtraInfo { get; set; }
    public long? Offset { get; set; }
    public string? LiteTopic { get; set; }

    public PropertyMap ToExtFields()
    {
        var outMap = new PropertyMap();
        HeaderCodec.PutOptStr(outMap, "consumerGroup", ConsumerGroup);
        HeaderCodec.PutOptStr(outMap, "topic", Topic);
        HeaderCodec.PutOptInt(outMap, "queueId", QueueId);
        HeaderCodec.PutOptStr(outMap, "extraInfo", ExtraInfo);
        HeaderCodec.PutOptLong(outMap, "offset", Offset);
        HeaderCodec.PutOptStr(outMap, "liteTopic", LiteTopic);
        return outMap;
    }

    public void FromExtFields(PropertyMap ext)
    {
        ConsumerGroup = HeaderCodec.GetOptStr(ext, "consumerGroup");
        Topic = HeaderCodec.GetOptStr(ext, "topic");
        QueueId = HeaderCodec.GetOptInt(ext, "queueId");
        ExtraInfo = HeaderCodec.GetOptStr(ext, "extraInfo");
        Offset = HeaderCodec.GetOptLong(ext, "offset");
        LiteTopic = HeaderCodec.GetOptStr(ext, "liteTopic");
    }
}

/// <summary>org.apache.rocketmq.remoting.protocol.header.ChangeInvisibleTimeRequestHeader。</summary>
public sealed class ChangeInvisibleTimeRequestHeader : ICommandCustomHeader
{
    public string? ConsumerGroup { get; set; }
    public string? Topic { get; set; }
    public int? QueueId { get; set; }
    public string? ExtraInfo { get; set; }
    public long? Offset { get; set; }
    public long? InvisibleTime { get; set; }
    public string? LiteTopic { get; set; }

    /// <summary>Java 是 primitive boolean（默认 false），**总是在 extFields 里**。</summary>
    public bool Suspend { get; set; }

    public PropertyMap ToExtFields()
    {
        var outMap = new PropertyMap();
        HeaderCodec.PutOptStr(outMap, "consumerGroup", ConsumerGroup);
        HeaderCodec.PutOptStr(outMap, "topic", Topic);
        HeaderCodec.PutOptInt(outMap, "queueId", QueueId);
        HeaderCodec.PutOptStr(outMap, "extraInfo", ExtraInfo);
        HeaderCodec.PutOptLong(outMap, "offset", Offset);
        HeaderCodec.PutOptLong(outMap, "invisibleTime", InvisibleTime);
        HeaderCodec.PutOptStr(outMap, "liteTopic", LiteTopic);
        HeaderCodec.PutOptBool(outMap, "suspend", Suspend);
        return outMap;
    }

    public void FromExtFields(PropertyMap ext)
    {
        ConsumerGroup = HeaderCodec.GetOptStr(ext, "consumerGroup");
        Topic = HeaderCodec.GetOptStr(ext, "topic");
        QueueId = HeaderCodec.GetOptInt(ext, "queueId");
        ExtraInfo = HeaderCodec.GetOptStr(ext, "extraInfo");
        Offset = HeaderCodec.GetOptLong(ext, "offset");
        InvisibleTime = HeaderCodec.GetOptLong(ext, "invisibleTime");
        LiteTopic = HeaderCodec.GetOptStr(ext, "liteTopic");
        Suspend = HeaderCodec.GetOptBool(ext, "suspend") ?? false;
    }
}

/// <summary>
/// org.apache.rocketmq.remoting.protocol.header.ChangeInvisibleTimeResponseHeader。
/// ⚠ 返回的是**新的** invisibleTime/popTime（不是 startTime/nextVisibleTime）。
/// </summary>
public sealed class ChangeInvisibleTimeResponseHeader : ICommandCustomHeader
{
    public long? PopTime { get; set; }
    public long? InvisibleTime { get; set; }
    public int? ReviveQid { get; set; }

    public PropertyMap ToExtFields()
    {
        var outMap = new PropertyMap();
        HeaderCodec.PutOptLong(outMap, "popTime", PopTime);
        HeaderCodec.PutOptLong(outMap, "invisibleTime", InvisibleTime);
        HeaderCodec.PutOptInt(outMap, "reviveQid", ReviveQid);
        return outMap;
    }

    public void FromExtFields(PropertyMap ext)
    {
        PopTime = HeaderCodec.GetOptLong(ext, "popTime");
        InvisibleTime = HeaderCodec.GetOptLong(ext, "invisibleTime");
        ReviveQid = HeaderCodec.GetOptInt(ext, "reviveQid");
    }
}
