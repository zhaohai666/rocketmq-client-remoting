// 消息轨迹（对应 org.apache.rocketmq.client.trace 包 + org.apache.rocketmq.client.AccessChannel）。
//
// 包含：
//   * TraceConstants     —— 常量（org.apache.rocketmq.client.trace.TraceConstants）
//   * TraceType           —— Pub / Recall / SubBefore / SubAfter / EndTransaction
//   * AccessChannel       —— LOCAL / CLOUD（org.apache.rocketmq.client.AccessChannel）
//   * TraceMessageType    —— 被追踪消息的类型（Normal/Trans/TransCommit/Delay/Order）
//   * TraceBean           —— 轨迹里被追踪的那条消息
//   * TraceContext        —— 一次追踪上下文（Pub/SubBefore/... 各一份）
//   * TraceTransferBean   —— 编码结果：trans_data + trans_key
//   * TraceDataEncoder    —— 文本编解码（与 Java 逐字节一致，对拍向量见 TraceTests.cs）
//
// ⚠ 两个必须保留的 Java 语义差异（否则编解码对不上）：
//   1. CONTENT_SPLITOR = \x01、FIELD_SPLITOR = \x02，编码时**每条记录末尾补 FIELD_SPLITOR**；
//      解码用 Java 的 String.split 语义（**丢弃末尾空串**）切分 —— 见 JavaSplit。
//   2. C# 的 string.Split(char) 会保留末尾空串（与 Java 不同），直接用它切分会把字段整体
//      错位；本模块一律走 JavaSplit。
using System.Globalization;
using RocketMQ.Common;

namespace RocketMQ.Client;

/// <summary>对应 org.apache.rocketmq.client.trace.TraceConstants。</summary>
public static class TraceConstants
{
    public const string GroupNamePrefix = "_INNER_TRACE_PRODUCER";
    public const string ContentSplitor = "\u0001";
    public const string FieldSplitor = "\u0002";
    public const string TraceInstanceName = "PID_CLIENT_INNER_TRACE_PRODUCER";
    public const string TraceTopicPrefix = "rmq_sys_TRACE_DATA_";
    public const string ToPrefix = "To_";
    public const string FromPrefix = "From_";
    public const string EndTransaction = "EndTransaction";

    public const string RocketmqService = "rocketmq";
    public const string RocketmqSuccess = "rocketmq.success";
    public const string RocketmqTags = "rocketmq.tags";
    public const string RocketmqKeys = "rocketmq.keys";
    public const string RocketmqStoreHost = "rocketmq.store_host";
    public const string RocketmqBodyLength = "rocketmq.body_length";
    public const string RocketmqMsgId = "rocketmq.mgs_id";
    public const string RocketmqMsgType = "rocketmq.mgs_type";
    public const string RocketmqRegionId = "rocketmq.region_id";
    public const string RocketmqTransactionId = "rocketmq.transaction_id";
    public const string RocketmqTransactionState = "rocketmq.transaction_state";
    public const string RocketmqIsFromTransactionCheck = "rocketmq.is_from_transaction_check";
    public const string RocketmqRetryTimers = "rocketmq.retry_times";
}

/// <summary>对应 org.apache.rocketmq.client.trace.TraceType（枚举名即线上字段第 1 段）。</summary>
public enum TraceType
{
    Pub = 0,
    Recall = 1,
    SubBefore = 2,
    SubAfter = 3,
    EndTransaction = 4,
}

/// <summary>对应 org.apache.rocketmq.client.AccessChannel。只在 SubAfter 编码时起作用。</summary>
public enum AccessChannel
{
    Local = 0,
    Cloud = 1,
}

/// <summary>对应 org.apache.rocketmq.common.message.MessageType（轨迹字段 msgType 的枚举）。
/// 注意序数必须与 Java 一致：Normal=0、Trans=1、TransCommit=2、Delay=3、Order=4。</summary>
public enum TraceMessageType
{
    Normal = 0,
    Trans = 1,
    TransCommit = 2,
    Delay = 3,
    Order = 4,
}

/// <summary>
/// 复刻 Java String.split：**丢弃末尾的空串**。C# 的 Split(char) 会保留，直接用它切分
/// 多出一个空记录、把字段整体错位（编码结果末尾正好补了 FIELD_SPLITOR）。这条差异是真金
/// 白银的对拍坑，勿改成 string.Split。
/// </summary>
public static class JavaSplit
{
    public static List<string> Split(string value, string sep)
    {
        var parts = new List<string>(value.Split(new[] { sep }, StringSplitOptions.None));
        while (parts.Count > 0 && parts[^1].Length == 0)
        {
            parts.RemoveAt(parts.Count - 1);
        }

        return parts;
    }
}

/// <summary>对应 org.apache.rocketmq.client.trace.TraceBean。</summary>
public sealed class TraceBean
{
    public string Topic { get; set; } = string.Empty;
    public string MsgId { get; set; } = string.Empty;
    public string OffsetMsgId { get; set; } = string.Empty;
    public string Tags { get; set; } = string.Empty;
    public string Keys { get; set; } = string.Empty;
    public string StoreHost { get; set; } = string.Empty;
    public string ClientHost { get; set; } = TraceDataEncoder.LocalAddress;
    public long StoreTime { get; set; }
    public int RetryTimes { get; set; }
    public int BodyLength { get; set; }
    public TraceMessageType MsgType { get; set; } = TraceMessageType.Normal;
    public string? TransactionState { get; set; }   // LocalTransactionState 的名字
    public string? TransactionId { get; set; }
    public bool FromTransactionCheck { get; set; }
}

/// <summary>对应 org.apache.rocketmq.client.trace.TraceContext。
/// RequestId 默认取 UtilAll.CreateUniqId()（与 Java 一致）—— SubBefore 与 SubAfter
/// 共用一个 request_id，是控制台把一次消费前后串起来的关键。</summary>
public sealed class TraceContext
{
    public TraceType? TraceType { get; set; }
    public long TimeStamp { get; set; } = UtilAll.CurrentTimeMillis();
    public string RegionId { get; set; } = string.Empty;
    public string RegionName { get; set; } = string.Empty;
    public string GroupName { get; set; } = string.Empty;
    public int CostTime { get; set; }
    public bool IsSuccess { get; set; } = true;
    public string RequestId { get; set; } = UtilAll.CreateUniqId();
    public int ContextCode { get; set; }
    public AccessChannel? AccessChannel { get; set; }
    public List<TraceBean> TraceBeans { get; set; } = new();
}

/// <summary>对应 org.apache.rocketmq.client.trace.TraceTransferBean。</summary>
public sealed class TraceTransferBean
{
    public string TransData { get; set; } = string.Empty;
    public HashSet<string> TransKey { get; set; } = new();
}

/// <summary>对应 org.apache.rocketmq.client.trace.TraceDataEncoder。
/// 编码结果的字段顺序**逐字节对齐 Java**，回归守卫是 TraceTests.cs 里由 Java 官方实现
/// 打印出来的固定字符串。</summary>
public static class TraceDataEncoder
{
    /// <summary>对应 Java TraceBean 静态块里的 LOCAL_ADDRESS（storeHost/clientHost 默认值）。</summary>
    public static readonly string LocalAddress = MixAll.GetIpStr();

    /// <summary>
    /// 把线上轨迹文本解回 TraceContext 列表（对应 Java decoderFromTraceDataString）。
    /// 按 FIELD_SPLITOR 切分记录，再按 CONTENT_SPLITOR 切分字段；兼容 Pub 的
    /// 13 / 14 / &gt;=15 段、SubAfter 的 &gt;=7 / &gt;=9 段等老版本分支。
    /// </summary>
    public static List<TraceContext> DecoderFromTraceDataString(string? traceData)
    {
        var res = new List<TraceContext>();
        if (string.IsNullOrEmpty(traceData))
        {
            return res;
        }

        foreach (string context in JavaSplit.Split(traceData!, TraceConstants.FieldSplitor))
        {
            if (context.Length == 0)
            {
                continue;
            }

            TraceContext? ctx;
            try
            {
                ctx = DecodeContext(context);
            }
            catch (Exception e) when (e is ArgumentOutOfRangeException or IndexOutOfRangeException
                                      or FormatException or OverflowException)
            {
                // 有意偏离 Java（Java 会把异常抛给调用方，导致**整条**轨迹消息全丢）：
                // 单条坏记录只跳过自己，其余记录照常解出。
                continue;
            }

            if (ctx is not null)
            {
                res.Add(ctx);
            }
        }

        return res;
    }

    /// <summary>解一条轨迹记录（一段 FIELD_SPLITOR 之内）。无法识别时返回 null。</summary>
    private static TraceContext? DecodeContext(string context)
    {
        List<string> line = JavaSplit.Split(context, TraceConstants.ContentSplitor);
        if (line.Count == 0)
        {
            return null;
        }

        string kind = line[0];
            if (kind == "Pub")
            {
                var ctx = new TraceContext
                {
                    TraceType = RocketMQ.Client.TraceType.Pub,
                    TimeStamp = long.Parse(line[1], CultureInfo.InvariantCulture),
                    RegionId = line[2],
                    GroupName = line[3],
                };
                var bean = new TraceBean
                {
                    Topic = line[4],
                    MsgId = line[5],
                    Tags = line[6],
                    Keys = line[7],
                    StoreHost = line[8],
                    BodyLength = int.Parse(line[9], CultureInfo.InvariantCulture),
                };
                ctx.CostTime = int.Parse(line[10], CultureInfo.InvariantCulture);
                bean.MsgType = (TraceMessageType)int.Parse(line[11], CultureInfo.InvariantCulture);
                if (line.Count == 13)
                {
                    ctx.IsSuccess = line[12] == "true";
                }
                else if (line.Count == 14)
                {
                    bean.OffsetMsgId = line[12];
                    ctx.IsSuccess = line[13] == "true";
                }

                if (line.Count >= 15)
                {
                    bean.OffsetMsgId = line[12];
                    ctx.IsSuccess = line[13] == "true";
                    bean.ClientHost = line[14];
                }

                ctx.TraceBeans = new List<TraceBean> { bean };
                return ctx;
            }
            else if (kind == "SubBefore")
            {
                var ctx = new TraceContext
                {
                    TraceType = RocketMQ.Client.TraceType.SubBefore,
                    TimeStamp = long.Parse(line[1], CultureInfo.InvariantCulture),
                    RegionId = line[2],
                    GroupName = line[3],
                    RequestId = line[4],
                };
                var bean = new TraceBean
                {
                    MsgId = line[5],
                    RetryTimes = int.Parse(line[6], CultureInfo.InvariantCulture),
                    // 无 keys 的消息，其 SubBefore 末段为空，被 Java 的 split 语义（丢弃末尾空串）
                    // 一并丢掉 → 只剩 7 段。Java 原生此处 line[7] 抛 AIOOBE（上游真实缺陷），
                    // 我们按空串兜底，否则无 keys 消息的 SubBefore 轨迹全丢。
                    Keys = line.Count > 7 ? line[7] : string.Empty,
                };
                ctx.TraceBeans = new List<TraceBean> { bean };
                return ctx;
            }
            else if (kind == "SubAfter")
            {
                var ctx = new TraceContext
                {
                    TraceType = RocketMQ.Client.TraceType.SubAfter,
                    RequestId = line[1],
                };
                var bean = new TraceBean
                {
                    MsgId = line[2],
                    Keys = line[5],
                };
                ctx.TraceBeans = new List<TraceBean> { bean };
                ctx.CostTime = int.Parse(line[3], CultureInfo.InvariantCulture);
                ctx.IsSuccess = line[4] == "true";
                if (line.Count >= 7)
                {
                    ctx.ContextCode = int.Parse(line[6], CultureInfo.InvariantCulture);
                }

                if (line.Count >= 9)
                {
                    ctx.TimeStamp = long.Parse(line[7], CultureInfo.InvariantCulture);
                    ctx.GroupName = line[8];
                }

                return ctx;
            }
            else if (kind == "EndTransaction")
            {
                var ctx = new TraceContext
                {
                    TraceType = RocketMQ.Client.TraceType.EndTransaction,
                    TimeStamp = long.Parse(line[1], CultureInfo.InvariantCulture),
                    RegionId = line[2],
                    GroupName = line[3],
                };
                var bean = new TraceBean
                {
                    Topic = line[4],
                    MsgId = line[5],
                    Tags = line[6],
                    Keys = line[7],
                    StoreHost = line[8],
                    MsgType = (TraceMessageType)int.Parse(line[9], CultureInfo.InvariantCulture),
                    TransactionId = line[10],
                    TransactionState = line[11],
                    FromTransactionCheck = line[12] == "true",
                };
                ctx.TraceBeans = new List<TraceBean> { bean };
                return ctx;
            }
            else if (kind == "Recall")
            {
                var ctx = new TraceContext
                {
                    TraceType = RocketMQ.Client.TraceType.Recall,
                    TimeStamp = long.Parse(line[1], CultureInfo.InvariantCulture),
                    RegionId = line[2],
                    GroupName = line[3],
                };
                var bean = new TraceBean
                {
                    Topic = line[4],
                    MsgId = line[5],
                };
                ctx.IsSuccess = line[6] == "true";
                ctx.TraceBeans = new List<TraceBean> { bean };
                return ctx;
            }

        return null;
    }

    /// <summary>把 TraceContext 编成可发送的文本（对应 Java encoderFromContextBean）。</summary>
    public static TraceTransferBean? EncoderFromContextBean(TraceContext? ctx)
    {
        if (ctx is null)
        {
            return null;
        }

        string soh = TraceConstants.ContentSplitor;
        string stx = TraceConstants.FieldSplitor;
        var tb = new TraceTransferBean();
        var sb = new List<string>();
        TraceType? t = ctx.TraceType;
        if (t == RocketMQ.Client.TraceType.Pub)
        {
            TraceBean bean = ctx.TraceBeans[0];
            sb.AddRange(new[]
            {
                "Pub", ctx.TimeStamp.ToString(CultureInfo.InvariantCulture), ctx.RegionId, ctx.GroupName,
                bean.Topic, bean.MsgId, bean.Tags, bean.Keys, bean.StoreHost,
                bean.BodyLength.ToString(CultureInfo.InvariantCulture),
                ctx.CostTime.ToString(CultureInfo.InvariantCulture),
                ((int)bean.MsgType).ToString(CultureInfo.InvariantCulture),
                bean.OffsetMsgId, ctx.IsSuccess ? "true" : "false",
            });
            tb.TransData = string.Join(soh, sb) + stx;
        }
        else if (t == RocketMQ.Client.TraceType.SubBefore)
        {
            foreach (TraceBean bean in ctx.TraceBeans)
            {
                sb.AddRange(new[]
                {
                    "SubBefore", ctx.TimeStamp.ToString(CultureInfo.InvariantCulture), ctx.RegionId,
                    ctx.GroupName, ctx.RequestId, bean.MsgId,
                    bean.RetryTimes.ToString(CultureInfo.InvariantCulture), bean.Keys,
                });
                tb.TransData += string.Join(soh, sb) + stx;
                sb.Clear();
            }
        }
        else if (t == RocketMQ.Client.TraceType.SubAfter)
        {
            foreach (TraceBean bean in ctx.TraceBeans)
            {
                sb.AddRange(new[]
                {
                    "SubAfter", ctx.RequestId, bean.MsgId,
                    ctx.CostTime.ToString(CultureInfo.InvariantCulture),
                    ctx.IsSuccess ? "true" : "false", bean.Keys,
                    ctx.ContextCode.ToString(CultureInfo.InvariantCulture),
                });
                // Java：非 CLOUD 才补 timestamp + groupName（accessChannel 为 null 时按 LOCAL）。
                if ((ctx.AccessChannel ?? RocketMQ.Client.AccessChannel.Local) != RocketMQ.Client.AccessChannel.Cloud)
                {
                    sb.Add(ctx.TimeStamp.ToString(CultureInfo.InvariantCulture));
                    sb.Add(ctx.GroupName);
                }

                tb.TransData += string.Join(soh, sb) + stx;
                sb.Clear();
            }
        }
        else if (t == RocketMQ.Client.TraceType.EndTransaction)
        {
            TraceBean bean = ctx.TraceBeans[0];
            string stateName = bean.TransactionState ?? string.Empty;
            sb.AddRange(new[]
            {
                "EndTransaction", ctx.TimeStamp.ToString(CultureInfo.InvariantCulture), ctx.RegionId,
                ctx.GroupName, bean.Topic, bean.MsgId, bean.Tags, bean.Keys, bean.StoreHost,
                ((int)bean.MsgType).ToString(CultureInfo.InvariantCulture),
                bean.TransactionId ?? string.Empty, stateName,
                bean.FromTransactionCheck ? "true" : "false",
            });
            tb.TransData = string.Join(soh, sb) + stx;
        }
        else if (t == RocketMQ.Client.TraceType.Recall)
        {
            TraceBean bean = ctx.TraceBeans[0];
            sb.AddRange(new[]
            {
                "Recall", ctx.TimeStamp.ToString(CultureInfo.InvariantCulture), ctx.RegionId,
                ctx.GroupName, bean.Topic, bean.MsgId, ctx.IsSuccess ? "true" : "false",
            });
            tb.TransData = string.Join(soh, sb) + stx;
        }

        // 收集 keys：msgId + 按空格拆开的业务 keys（Java split(KEY_SEPARATOR)）。
        foreach (TraceBean bean in ctx.TraceBeans)
        {
            tb.TransKey.Add(bean.MsgId);
            if (!string.IsNullOrEmpty(bean.Keys))
            {
                foreach (string k in bean.Keys.Split(new[] { MessageConst.KeySeparator },
                             StringSplitOptions.RemoveEmptyEntries))
                {
                    tb.TransKey.Add(k);
                }
            }
        }

        return tb;
    }
}
