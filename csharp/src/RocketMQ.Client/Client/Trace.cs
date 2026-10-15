using System.Globalization;
using RocketMQ.Common;

namespace RocketMQ.Client;

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

public enum TraceType
{
    Pub = 0,
    Recall = 1,
    SubBefore = 2,
    SubAfter = 3,
    EndTransaction = 4,
}

public enum AccessChannel
{
    Local = 0,
    Cloud = 1,
}

public enum TraceMessageType
{
    Normal = 0,
    Trans = 1,
    TransCommit = 2,
    Delay = 3,
    Order = 4,
}

public static class LegacySplit
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
    public string? TransactionState { get; set; }   // the name of LocalTransactionState
    public string? TransactionId { get; set; }
    public bool FromTransactionCheck { get; set; }
}

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

public sealed class TraceTransferBean
{
    public string TransData { get; set; } = string.Empty;
    public HashSet<string> TransKey { get; set; } = new();
}

public static class TraceDataEncoder
{
    public static readonly string LocalAddress = MixAll.GetIpStr();

    public static List<TraceContext> DecoderFromTraceDataString(string? traceData)
    {
        var res = new List<TraceContext>();
        if (string.IsNullOrEmpty(traceData))
        {
            return res;
        }

        foreach (string context in LegacySplit.Split(traceData!, TraceConstants.FieldSplitor))
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
                continue;
            }

            if (ctx is not null)
            {
                res.Add(ctx);
            }
        }

        return res;
    }

    /// <summary>Decodes one trace record (within a single FIELD_SPLITOR segment). Returns null when unrecognizable.</summary>
    private static TraceContext? DecodeContext(string context)
    {
        List<string> line = LegacySplit.Split(context, TraceConstants.ContentSplitor);
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
