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

    public PropertyMap ToExtFields()
    {
        var outMap = new PropertyMap();
        HeaderCodec.PutOptStr(outMap, "msgId", MsgId);
        HeaderCodec.PutOptInt(outMap, "queueId", QueueId);
        HeaderCodec.PutOptLong(outMap, "queueOffset", QueueOffset);
        HeaderCodec.PutOptStr(outMap, "transactionId", TransactionId);
        HeaderCodec.PutOptLong(outMap, "msgRegion", MsgRegion);
        return outMap;
    }

    public void FromExtFields(PropertyMap ext)
    {
        MsgId = HeaderCodec.GetOptStr(ext, "msgId");
        QueueId = HeaderCodec.GetOptInt(ext, "queueId");
        QueueOffset = HeaderCodec.GetOptLong(ext, "queueOffset");
        TransactionId = HeaderCodec.GetOptStr(ext, "transactionId");
        MsgRegion = HeaderCodec.GetOptLong(ext, "msgRegion");
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
    public string? ProducerGroup { get; set; }
    public long? TranStateTableOffset { get; set; }
    public long? CommitLogOffset { get; set; }

    /// <summary>MessageSysFlag.TRANSACTION_*_TYPE</summary>
    public int? CommitOrRollback { get; set; }

    public bool? FromTransactionCheck { get; set; }
    public string? MsgId { get; set; }
    public string? TransactionId { get; set; }

    public PropertyMap ToExtFields()
    {
        var outMap = new PropertyMap();
        HeaderCodec.PutOptStr(outMap, "producerGroup", ProducerGroup);
        HeaderCodec.PutOptLong(outMap, "tranStateTableOffset", TranStateTableOffset);
        HeaderCodec.PutOptLong(outMap, "commitLogOffset", CommitLogOffset);
        HeaderCodec.PutOptInt(outMap, "commitOrRollback", CommitOrRollback);
        HeaderCodec.PutOptBool(outMap, "fromTransactionCheck", FromTransactionCheck);
        HeaderCodec.PutOptStr(outMap, "msgId", MsgId);
        HeaderCodec.PutOptStr(outMap, "transactionId", TransactionId);
        return outMap;
    }

    public void FromExtFields(PropertyMap ext)
    {
        ProducerGroup = HeaderCodec.GetOptStr(ext, "producerGroup");
        TranStateTableOffset = HeaderCodec.GetOptLong(ext, "tranStateTableOffset");
        CommitLogOffset = HeaderCodec.GetOptLong(ext, "commitLogOffset");
        CommitOrRollback = HeaderCodec.GetOptInt(ext, "commitOrRollback");
        FromTransactionCheck = HeaderCodec.GetOptBool(ext, "fromTransactionCheck");
        MsgId = HeaderCodec.GetOptStr(ext, "msgId");
        TransactionId = HeaderCodec.GetOptStr(ext, "transactionId");
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

    public PropertyMap ToExtFields()
    {
        var outMap = new PropertyMap();
        HeaderCodec.PutOptStr(outMap, "consumerGroup", ConsumerGroup);
        return outMap;
    }

    public void FromExtFields(PropertyMap ext) =>
        ConsumerGroup = HeaderCodec.GetOptStr(ext, "consumerGroup");
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
    public long? TranStateTableOffset { get; set; }
    public long? CommitLogOffset { get; set; }
    public string? MsgId { get; set; }
    public string? TransactionId { get; set; }
    public long? OffsetMsgId { get; set; }

    public PropertyMap ToExtFields()
    {
        var outMap = new PropertyMap();
        HeaderCodec.PutOptLong(outMap, "tranStateTableOffset", TranStateTableOffset);
        HeaderCodec.PutOptLong(outMap, "commitLogOffset", CommitLogOffset);
        HeaderCodec.PutOptStr(outMap, "msgId", MsgId);
        HeaderCodec.PutOptStr(outMap, "transactionId", TransactionId);
        HeaderCodec.PutOptLong(outMap, "offsetMsgId", OffsetMsgId);
        return outMap;
    }

    public void FromExtFields(PropertyMap ext)
    {
        TranStateTableOffset = HeaderCodec.GetOptLong(ext, "tranStateTableOffset");
        CommitLogOffset = HeaderCodec.GetOptLong(ext, "commitLogOffset");
        MsgId = HeaderCodec.GetOptStr(ext, "msgId");
        TransactionId = HeaderCodec.GetOptStr(ext, "transactionId");
        OffsetMsgId = HeaderCodec.GetOptLong(ext, "offsetMsgId");
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
