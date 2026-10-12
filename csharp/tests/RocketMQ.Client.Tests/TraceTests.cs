using System.Collections.Generic;
using System.Globalization;
using System.Reflection;

using RocketMQ.Client;
using RocketMQ.Common;

using Xunit;

namespace RocketMQ.Client.Tests;

public class TraceTests
{
    private const string Soh = "\u0001";
    private const string Stx = "\u0002";
    private const string MsgId1 = "AC1400A1F0A018B4AAC2A1B2C3D4E5F6";
    private const string MsgId2 = "AC1400A1F0A018B4AAC2A1B2C3D4E5F7";
    private const string OffsetMsgId = "AC1400A1000027100000000000000001";

    private static readonly string ExpectedPub = string.Join(Soh, new[]
    {
        "Pub", "1700000000000", "DefaultRegion", "GID_test", "TopicTest",
        MsgId1, "TagA", "KeyA KeyB", "127.0.0.1:10911", "42", "7", "0",
        OffsetMsgId, "true",
    }) + Stx;

    private static readonly string ExpectedSubBefore = string.Join(Soh, new[]
    {
        "SubBefore", "1700000000000", "DefaultRegion", "CID_test", "REQ-SUB-001",
        MsgId1, "2", "KeyA KeyB",
    }) + Stx + string.Join(Soh, new[]
    {
        "SubBefore", "1700000000000", "DefaultRegion", "CID_test", "REQ-SUB-001",
        MsgId2, "0", "KeyC",
    }) + Stx;

    private static readonly string ExpectedSubAfter = string.Join(Soh, new[]
    {
        "SubAfter", "REQ-SUB-001", MsgId1, "11", "false", "KeyA KeyB", "2",
        "1700000000000", "CID_test",
    }) + Stx;

    private static readonly string ExpectedEndTransaction = string.Join(Soh, new[]
    {
        "EndTransaction", "1700000000000", "DefaultRegion", "GID_test", "TopicTest",
        MsgId1, "TagA", "KeyA KeyB", "127.0.0.1:10911", "0", "TRAN-001", "COMMIT_MESSAGE",
        "false",
    }) + Stx;

    private static readonly string ExpectedRecall = string.Join(Soh, new[]
    {
        "Recall", "1700000000000", "DefaultRegion", "GID_test", "TopicTest", MsgId1, "true",
    }) + Stx;

    private static TraceBean MakeBean(string msgId = MsgId1, string keys = "KeyA KeyB",
        int retryTimes = 2) => new()
    {
        Topic = "TopicTest",
        MsgId = msgId,
        OffsetMsgId = OffsetMsgId,
        Tags = "TagA",
        Keys = keys,
        StoreHost = "127.0.0.1:10911",
        StoreTime = 1700000000123,
        RetryTimes = retryTimes,
        BodyLength = 42,
        MsgType = TraceMessageType.Normal,
        TransactionId = "TRAN-001",
        TransactionState = "COMMIT_MESSAGE",
    };


    [Fact]
    public void EncodePubMatchesLegacyVector()
    {
        var ctx = new TraceContext
        {
            TraceType = RocketMQ.Client.TraceType.Pub,
            TimeStamp = 1700000000000,
            RegionId = "DefaultRegion",
            GroupName = "GID_test",
            CostTime = 7,
            IsSuccess = true,
            RequestId = "REQ-PUB-001",
            TraceBeans = new List<TraceBean> { MakeBean() },
        };

        TraceTransferBean tb = TraceDataEncoder.EncoderFromContextBean(ctx)!;
        Assert.Equal(ExpectedPub, tb.TransData);
        Assert.Equal(3, tb.TransKey.Count);
        Assert.Contains(MsgId1, tb.TransKey);
        Assert.Contains("KeyA", tb.TransKey);
        Assert.Contains("KeyB", tb.TransKey);
    }

    [Fact]
    public void EncodeSubBeforeMatchesLegacyVector()
    {
        var ctx = new TraceContext
        {
            TraceType = RocketMQ.Client.TraceType.SubBefore,
            TimeStamp = 1700000000000,
            RegionId = "DefaultRegion",
            GroupName = "CID_test",
            RequestId = "REQ-SUB-001",
            TraceBeans = new List<TraceBean>
            {
                MakeBean(MsgId1, "KeyA KeyB", 2),
                MakeBean(MsgId2, "KeyC", 0),
            },
        };

        TraceTransferBean tb = TraceDataEncoder.EncoderFromContextBean(ctx)!;
        Assert.Equal(ExpectedSubBefore, tb.TransData);
    }

    [Fact]
    public void EncodeSubAfterMatchesLegacyVector()
    {
        var ctx = new TraceContext
        {
            TraceType = RocketMQ.Client.TraceType.SubAfter,
            RequestId = "REQ-SUB-001",
            TimeStamp = 1700000000000,
            GroupName = "CID_test",
            CostTime = 11,
            IsSuccess = false,
            ContextCode = 2,
            AccessChannel = RocketMQ.Client.AccessChannel.Local,
            TraceBeans = new List<TraceBean> { MakeBean() },
        };

        TraceTransferBean tb = TraceDataEncoder.EncoderFromContextBean(ctx)!;
        Assert.Equal(ExpectedSubAfter, tb.TransData);
    }

    [Fact]
    public void EncodeSubAfterOnCloudOmitsTimestampAndGroup()
    {
        var ctx = new TraceContext
        {
            TraceType = RocketMQ.Client.TraceType.SubAfter,
            RequestId = "REQ-SUB-001",
            TimeStamp = 1700000000000,
            GroupName = "CID_test",
            CostTime = 11,
            IsSuccess = false,
            ContextCode = 2,
            AccessChannel = RocketMQ.Client.AccessChannel.Cloud,
            TraceBeans = new List<TraceBean> { MakeBean() },
        };

        TraceTransferBean tb = TraceDataEncoder.EncoderFromContextBean(ctx)!;
        string expected = string.Join(Soh, new[]
        {
            "SubAfter", "REQ-SUB-001", MsgId1, "11", "false", "KeyA KeyB", "2",
        }) + Stx;
        Assert.Equal(expected, tb.TransData);
    }

    [Fact]
    public void EncodeEndTransactionMatchesLegacyVector()
    {
        var ctx = new TraceContext
        {
            TraceType = RocketMQ.Client.TraceType.EndTransaction,
            TimeStamp = 1700000000000,
            RegionId = "DefaultRegion",
            GroupName = "GID_test",
            TraceBeans = new List<TraceBean> { MakeBean() },
        };

        TraceTransferBean tb = TraceDataEncoder.EncoderFromContextBean(ctx)!;
        Assert.Equal(ExpectedEndTransaction, tb.TransData);
    }

    [Fact]
    public void EncodeRecallMatchesLegacyVector()
    {
        var ctx = new TraceContext
        {
            TraceType = RocketMQ.Client.TraceType.Recall,
            TimeStamp = 1700000000000,
            RegionId = "DefaultRegion",
            GroupName = "GID_test",
            IsSuccess = true,
            TraceBeans = new List<TraceBean> { MakeBean() },
        };

        TraceTransferBean tb = TraceDataEncoder.EncoderFromContextBean(ctx)!;
        Assert.Equal(ExpectedRecall, tb.TransData);
    }

    [Fact]
    public void EncoderReturnsNullForNullContext() => Assert.Null(TraceDataEncoder.EncoderFromContextBean(null));

    // ---------------------------------------------------------------- 解码

    [Fact]
    public void DecodePubKeepsMsgIdAndOffsetMsgId()
    {
        List<TraceContext> res = TraceDataEncoder.DecoderFromTraceDataString(ExpectedPub);
        TraceContext ctx = Assert.Single(res);
        Assert.Equal(RocketMQ.Client.TraceType.Pub, ctx.TraceType);
        Assert.Equal(1700000000000, ctx.TimeStamp);
        Assert.Equal("DefaultRegion", ctx.RegionId);
        Assert.Equal("GID_test", ctx.GroupName);
        Assert.Equal(7, ctx.CostTime);
        Assert.True(ctx.IsSuccess);
        TraceBean bean = Assert.Single(ctx.TraceBeans);
        Assert.Equal("TopicTest", bean.Topic);
        Assert.Equal(MsgId1, bean.MsgId);
        Assert.Equal(OffsetMsgId, bean.OffsetMsgId);
        Assert.Equal("TagA", bean.Tags);
        Assert.Equal("KeyA KeyB", bean.Keys);
        Assert.Equal("127.0.0.1:10911", bean.StoreHost);
        Assert.Equal(42, bean.BodyLength);
        Assert.Equal(TraceMessageType.Normal, bean.MsgType);
    }

    [Fact]
    public void DecodeSubBeforeKeepsRequestIdAndRetryTimes()
    {
        List<TraceContext> res = TraceDataEncoder.DecoderFromTraceDataString(ExpectedSubBefore);
        Assert.Equal(2, res.Count);
        Assert.Equal("REQ-SUB-001", res[0].RequestId);
        Assert.Equal(2, res[0].TraceBeans[0].RetryTimes);
        Assert.Equal("KeyA KeyB", res[0].TraceBeans[0].Keys);
        Assert.Equal(MsgId2, res[1].TraceBeans[0].MsgId);
        Assert.Equal(0, res[1].TraceBeans[0].RetryTimes);
        Assert.Equal("KeyC", res[1].TraceBeans[0].Keys);
    }

    [Fact]
    public void DecodeSubAfterKeepsContextCode()
    {
        List<TraceContext> res = TraceDataEncoder.DecoderFromTraceDataString(ExpectedSubAfter);
        TraceContext ctx = Assert.Single(res);
        Assert.Equal(RocketMQ.Client.TraceType.SubAfter, ctx.TraceType);
        Assert.Equal("REQ-SUB-001", ctx.RequestId);
        Assert.Equal(11, ctx.CostTime);
        Assert.False(ctx.IsSuccess);
        Assert.Equal(2, ctx.ContextCode);
        Assert.Equal(1700000000000, ctx.TimeStamp);
        Assert.Equal("CID_test", ctx.GroupName);
        Assert.Equal("KeyA KeyB", ctx.TraceBeans[0].Keys);
    }

    [Fact]
    public void DecodeEndTransactionKeepsTransactionFields()
    {
        List<TraceContext> res = TraceDataEncoder.DecoderFromTraceDataString(ExpectedEndTransaction);
        TraceContext ctx = Assert.Single(res);
        Assert.Equal(RocketMQ.Client.TraceType.EndTransaction, ctx.TraceType);
        TraceBean bean = ctx.TraceBeans[0];
        Assert.Equal("TRAN-001", bean.TransactionId);
        Assert.Equal("COMMIT_MESSAGE", bean.TransactionState);
        Assert.False(bean.FromTransactionCheck);
        Assert.Equal(TraceMessageType.Normal, bean.MsgType);
    }

    [Fact]
    public void EncodeDecodeRoundTripKeepsIds()
    {
        var ctx = new TraceContext
        {
            TraceType = RocketMQ.Client.TraceType.Pub,
            TimeStamp = 1700000000000,
            RegionId = "DefaultRegion",
            GroupName = "GID_test",
            CostTime = 7,
            IsSuccess = true,
            TraceBeans = new List<TraceBean> { MakeBean() },
        };

        string text = TraceDataEncoder.EncoderFromContextBean(ctx)!.TransData;
        TraceBean bean = TraceDataEncoder.DecoderFromTraceDataString(text)[0].TraceBeans[0];
        Assert.Equal(MsgId1, bean.MsgId);
        Assert.Equal(OffsetMsgId, bean.OffsetMsgId);
        Assert.Equal(42, bean.BodyLength);
        Assert.Equal("TopicTest", bean.Topic);
    }

    [Fact]
    public void DecodeSubBeforeWithoutKeysDoesNotThrow()
    {
        // 真实线上形态：keys 为空 → java_split 丢掉末尾空串 → 只剩 7 段
        string raw = string.Join(Soh, new[]
        {
            "SubBefore", "1700000000000", "DefaultRegion", "GID_trace_live", "REQ-001",
            "7F00000100002A9F0000000000000001", "0", "",
        }) + Stx;

        List<TraceContext> res = TraceDataEncoder.DecoderFromTraceDataString(raw);
        TraceContext ctx = Assert.Single(res);
        Assert.Equal("REQ-001", ctx.RequestId);
        Assert.Equal(string.Empty, ctx.TraceBeans[0].Keys);
        Assert.Equal(0, ctx.TraceBeans[0].RetryTimes);
        Assert.Equal("7F00000100002A9F0000000000000001", ctx.TraceBeans[0].MsgId);
    }

    [Fact]
    public void DecodeOneBadRecordDoesNotDropTheBatch()
    {
        string bad = string.Join(Soh, new[] { "SubAfter", "REQ-9", "M9" }) + Stx; // 段数不足
        string batch = ExpectedPub + bad + ExpectedSubBefore;

        List<TraceContext> res = TraceDataEncoder.DecoderFromTraceDataString(batch);
        Assert.Equal(3, res.Count);
        Assert.Equal(RocketMQ.Client.TraceType.Pub, res[0].TraceType);
        Assert.Equal(RocketMQ.Client.TraceType.SubBefore, res[1].TraceType);
        Assert.Equal(RocketMQ.Client.TraceType.SubBefore, res[2].TraceType);
    }

    [Fact]
    public void DecodeUnknownKindIsIgnored()
    {
        string raw = string.Join(Soh, new[] { "NoSuchKind", "1", "2" }) + Stx + ExpectedRecall;
        List<TraceContext> res = TraceDataEncoder.DecoderFromTraceDataString(raw);
        TraceContext ctx = Assert.Single(res);
        Assert.Equal(RocketMQ.Client.TraceType.Recall, ctx.TraceType);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    public void DecodeEmptyOrNullReturnsEmpty(string? input) =>
        Assert.Empty(TraceDataEncoder.DecoderFromTraceDataString(input));

    // ---------------------------------------------------------------- LegacySplit 语义

    [Fact]
    public void LegacySplitDropsTrailingEmptyFields()
    {
        Assert.Equal(new List<string> { "a", "b" }, LegacySplit.Split("a\u0001b\u0001", Soh));
        Assert.Equal(new List<string> { "a", "", "b" }, LegacySplit.Split("a\u0001\u0001b", Soh));
        Assert.Empty(LegacySplit.Split("\u0001\u0001", Soh));
    }

    // ---------------------------------------------------------------- 常量

    [Fact]
    public void ConstantsMatchLegacy()
    {
        Assert.Equal("_INNER_TRACE_PRODUCER", TraceConstants.GroupNamePrefix);
        Assert.Equal("PID_CLIENT_INNER_TRACE_PRODUCER", TraceConstants.TraceInstanceName);
        Assert.Equal("rmq_sys_TRACE_DATA_", TraceConstants.TraceTopicPrefix);
        Assert.Equal("\u0001", TraceConstants.ContentSplitor);
        Assert.Equal("\u0002", TraceConstants.FieldSplitor);
        Assert.Equal("DefaultRegion", MixAll.DefaultTraceRegionId);
    }

    [Fact]
    public void TraceTypeAndMessageTypeOrdinalsMatchLegacy()
    {
        Assert.Equal(0, (int)RocketMQ.Client.TraceType.Pub);
        Assert.Equal(1, (int)RocketMQ.Client.TraceType.Recall);
        Assert.Equal(2, (int)RocketMQ.Client.TraceType.SubBefore);
        Assert.Equal(3, (int)RocketMQ.Client.TraceType.SubAfter);
        Assert.Equal(4, (int)RocketMQ.Client.TraceType.EndTransaction);
        Assert.Equal(0, (int)TraceMessageType.Normal);
        Assert.Equal(1, (int)TraceMessageType.Trans);
        Assert.Equal(2, (int)TraceMessageType.TransCommit);
        Assert.Equal(3, (int)TraceMessageType.Delay);
        Assert.Equal(4, (int)TraceMessageType.Order);
    }

    [Fact]
    public void TraceContextDefaultsToUniqueRequestId()
    {
        var a = new TraceContext();
        var b = new TraceContext();
        Assert.NotEqual(a.RequestId, b.RequestId);
        Assert.Equal(32, a.RequestId.Length);
    }

    // ---------------------------------------------------------------- 分发器

    [Fact]
    public void DispatcherDefaultsToSystemTraceTopic()
    {
        var d = new AsyncTraceDispatcher("GID_x", TraceDispatcherType.Produce);
        Assert.Equal(MixAll.TraceTopic, d.GetTraceTopicName());
        Assert.Equal(0, d.QueueSize);
    }

    [Fact]
    public void DispatcherAcceptsCustomTraceTopic()
    {
        var d = new AsyncTraceDispatcher("GID_x", TraceDispatcherType.Consume, 10, "MY_TRACE_TOPIC");
        Assert.Equal("MY_TRACE_TOPIC", d.GetTraceTopicName());
    }

    [Fact]
    public void DispatcherAppendEnqueuesAndFullQueueIsDropped()
    {
        var d = new AsyncTraceDispatcher("GID_x", TraceDispatcherType.Produce);
        Assert.True(d.Append(new TraceContext()));

        int accepted = 1;
        for (int i = 0; i < 4096; ++i)
        {
            if (d.Append(new TraceContext()))
            {
                accepted++;
            }
        }

        Assert.Equal(2048, accepted);
        Assert.Equal(2048, d.QueueSize);
    }

    [Fact]
    public void DispatcherFlushDrainsTheQueue()
    {
        var d = new AsyncTraceDispatcher("GID_x", TraceDispatcherType.Produce);
        for (int i = 0; i < 5; ++i)
        {
            d.Append(new TraceContext());
        }

        Assert.Equal(5, d.QueueSize);
        // 未 Start 时发送必然失败，但 Flush 仍要把队列取空（不能因为发不出去就堆在队列里）
        d.Flush();
        Assert.Equal(0, d.QueueSize);
    }

    // ---------------------------------------------------------------- 发送侧钩子

    [Fact]
    public void SendHookBuildsPubContextAndAppends()
    {
        var d = new AsyncTraceDispatcher("GID_test", TraceDispatcherType.Produce);
        var hook = new SendMessageTraceHook(d);
        SendMessageContext context = SendContext();

        hook.SendMessageBefore(context);
        var trace = Assert.IsType<TraceContext>(context.MqTraceContext);
        Assert.Equal(RocketMQ.Client.TraceType.Pub, trace.TraceType);
        Assert.Equal("GID_test", trace.GroupName);
        Assert.Equal("TopicTest", trace.TraceBeans[0].Topic);
        Assert.Equal("TagA", trace.TraceBeans[0].Tags);
        Assert.Equal("KeyA KeyB", trace.TraceBeans[0].Keys);
        Assert.Equal(42, trace.TraceBeans[0].BodyLength);
        Assert.Equal(0, d.QueueSize);            // before 不入队，after 才发
        Assert.Equal("SendMessageTraceHook", hook.HookName());

        context.SendResult = new SendResult
        {
            SendStatus = SendStatus.SendOk,
            MsgId = MsgId1,
            OffsetMsgId = OffsetMsgId,
            RegionId = "DefaultRegion",
            TraceOn = true,
        };
        hook.SendMessageAfter(context);

        Assert.Equal(1, d.QueueSize);
        Assert.Equal(MsgId1, trace.TraceBeans[0].MsgId);
        Assert.Equal(OffsetMsgId, trace.TraceBeans[0].OffsetMsgId);
        Assert.True(trace.IsSuccess);
        Assert.Equal("DefaultRegion", trace.RegionId);
    }

    [Fact]
    public void SendHookSkipsTheTraceTopicItself()
    {
        var d = new AsyncTraceDispatcher("GID_test", TraceDispatcherType.Produce);
        var hook = new SendMessageTraceHook(d);
        SendMessageContext context = SendContext(topic: MixAll.TraceTopic);

        hook.SendMessageBefore(context);
        context.SendResult = new SendResult
        {
            SendStatus = SendStatus.SendOk,
            MsgId = MsgId1,
            RegionId = "DefaultRegion",
            TraceOn = true,
        };
        hook.SendMessageAfter(context);

        Assert.Null(context.MqTraceContext);
        Assert.Equal(0, d.QueueSize);            // 防递归：轨迹消息自己不再产生轨迹
    }

    [Fact]
    public void SendHookSkipsWhenBrokerTraceOffOrRegionMissing()
    {
        var d = new AsyncTraceDispatcher("GID_test", TraceDispatcherType.Produce);
        var hook = new SendMessageTraceHook(d);

        SendMessageContext off = SendContext();
        hook.SendMessageBefore(off);
        off.SendResult = new SendResult
        {
            SendStatus = SendStatus.SendOk, MsgId = MsgId1, RegionId = "DefaultRegion", TraceOn = false,
        };
        hook.SendMessageAfter(off);

        SendMessageContext noRegion = SendContext();
        hook.SendMessageBefore(noRegion);
        noRegion.SendResult = new SendResult
        {
            SendStatus = SendStatus.SendOk, MsgId = MsgId1, RegionId = string.Empty, TraceOn = true,
        };
        hook.SendMessageAfter(noRegion);

        Assert.Equal(0, d.QueueSize);
    }

    [Fact]
    public void SendHookMarksFailureStatusAndNeedsBeforeToHaveRun()
    {
        var d = new AsyncTraceDispatcher("GID_test", TraceDispatcherType.Produce);
        var hook = new SendMessageTraceHook(d);

        // before 未跑过 → after 什么都不做
        SendMessageContext orphan = SendContext();
        orphan.SendResult = new SendResult
        {
            SendStatus = SendStatus.SendOk, MsgId = MsgId1, RegionId = "DefaultRegion", TraceOn = true,
        };
        hook.SendMessageAfter(orphan);
        Assert.Equal(0, d.QueueSize);

        SendMessageContext context = SendContext();
        hook.SendMessageBefore(context);
        context.SendResult = new SendResult
        {
            SendStatus = SendStatus.FlushDiskTimeout,
            MsgId = MsgId1,
            RegionId = "DefaultRegion",
            TraceOn = true,
        };
        hook.SendMessageAfter(context);

        Assert.Equal(1, d.QueueSize);
        Assert.False(((TraceContext)context.MqTraceContext!).IsSuccess);
    }

    // ---------------------------------------------------------------- 消费侧钩子

    [Fact]
    public void ConsumeHookSubBeforeAndAfterShareRequestId()
    {
        var d = new AsyncTraceDispatcher("CID_test", TraceDispatcherType.Consume, 10, null, null);
        var hook = new ConsumeMessageTraceHook(d);
        var msgs = new List<MessageExt> { MakeMsg(), MakeMsg(msgId: MsgId2) };
        ConsumeMessageContext context = ConsumeContext(msgs);
        context.Props!["ConsumeContextType"] = "SUCCESS";

        hook.ConsumeMessageBefore(context);
        var before = Assert.IsType<TraceContext>(context.MqTraceContext);
        Assert.Equal(RocketMQ.Client.TraceType.SubBefore, before.TraceType);
        Assert.Equal(2, before.TraceBeans.Count);
        Assert.Equal(1, d.QueueSize);

        context.Success = true;
        hook.ConsumeMessageAfter(context);

        // SubBefore / SubAfter 必须共用一个 request_id，控制台才能把一次消费前后串起来
        List<TraceContext> queued = DrainQueue(d);
        Assert.Equal(2, queued.Count);
        Assert.Equal(queued[0].RequestId, queued[1].RequestId);
        Assert.Equal(RocketMQ.Client.TraceType.SubAfter, queued[1].TraceType);
        Assert.Equal(queued[0].RegionId, queued[1].RegionId);
        Assert.True(queued[1].IsSuccess);

        TraceBean bean = before.TraceBeans[0];
        Assert.Equal(MsgId1, bean.MsgId);
        Assert.Equal("TagA", bean.Tags);
    }

    [Theory]
    [InlineData("SUCCESS", 0)]
    [InlineData("TIME_OUT", 1)]
    [InlineData("EXCEPTION", 2)]
    [InlineData("RETURNNULL", 3)]
    [InlineData("FAILED", 4)]
    public void ConsumeHookMapsContextCodeFromLegacyReturnTypeNames(string name, int expected)
    {
        var d = new AsyncTraceDispatcher("CID_test", TraceDispatcherType.Consume);
        var hook = new ConsumeMessageTraceHook(d);
        ConsumeMessageContext context = ConsumeContext(new List<MessageExt> { MakeMsg() });
        context.Props!["ConsumeContextType"] = name;

        hook.ConsumeMessageBefore(context);
        hook.ConsumeMessageAfter(context);

        List<TraceContext> queued = DrainQueue(d);
        Assert.Equal(2, queued.Count);
        TraceContext subBefore = queued[0];
        TraceContext subAfter = queued[1];
        Assert.Equal(RocketMQ.Client.TraceType.SubBefore, subBefore.TraceType);
        Assert.Equal(RocketMQ.Client.TraceType.SubAfter, subAfter.TraceType);
        // SubBefore / SubAfter 共用一个 request_id —— 控制台靠它把一次消费前后串起来
        Assert.Equal(subBefore.RequestId, subAfter.RequestId);
        Assert.Equal(expected, subAfter.ContextCode);

        // 顺带锁住编码：contextCode 落在第 7 段（0-based 索引 6）
        string text = TraceDataEncoder.EncoderFromContextBean(subAfter)!.TransData;
        string[] seg = LegacySplit.Split(text, Soh).ToArray();
        Assert.Equal(expected.ToString(CultureInfo.InvariantCulture), seg[6]);
    }

    [Fact]
    public void ConsumeHookSkipsMessageWithTraceOff()
    {
        var d = new AsyncTraceDispatcher("CID_test", TraceDispatcherType.Consume);
        var hook = new ConsumeMessageTraceHook(d);
        MessageExt m = MakeMsg();
        m.Properties[MessageConst.PropertyTraceSwitch] = "false";

        ConsumeMessageContext context = ConsumeContext(new List<MessageExt> { m });
        hook.ConsumeMessageBefore(context);

        Assert.Equal(0, d.QueueSize);
    }

    [Fact]
    public void ConsumeHookAfterWithoutBeforeAppendsNothing()
    {
        var d = new AsyncTraceDispatcher("CID_test", TraceDispatcherType.Consume);
        var hook = new ConsumeMessageTraceHook(d);
        ConsumeMessageContext context = ConsumeContext(new List<MessageExt> { MakeMsg() });

        hook.ConsumeMessageAfter(context);
        Assert.Equal(0, d.QueueSize);
        Assert.Equal("ConsumeMessageTraceHook", hook.HookName());
    }

    // ---------------------------------------------------------------- 事务收尾钩子

    [Fact]
    public void EndTransactionHookWritesLegacyStateName()
    {
        var d = new AsyncTraceDispatcher("GID_test", TraceDispatcherType.Produce);
        var hook = new EndTransactionTraceHook(d);
        var context = new EndTransactionContext
        {
            ProducerGroup = "GID_test",
            Message = new Message("TopicTest", new byte[42]) { Tags = "TagA", Keys = "KeyA KeyB" },
            BrokerAddr = "127.0.0.1:10911",
            MsgId = MsgId1,
            TransactionId = "TRAN-001",
            TransactionState = LocalTransactionState.CommitMessage,
            FromTransactionCheck = false,
        };

        hook.EndTransaction(context);
        Assert.Equal(1, d.QueueSize);
        Assert.Equal("EndTransactionTraceHook", hook.HookName());
    }

    [Fact]
    public void EndTransactionHookSkipsTraceTopic()
    {
        var d = new AsyncTraceDispatcher("GID_test", TraceDispatcherType.Produce);
        var hook = new EndTransactionTraceHook(d);
        var context = new EndTransactionContext
        {
            ProducerGroup = "GID_test",
            Message = new Message(MixAll.TraceTopic, new byte[1]),
            MsgId = MsgId1,
            TransactionState = LocalTransactionState.CommitMessage,
        };

        hook.EndTransaction(context);
        Assert.Equal(0, d.QueueSize);
    }

    // ---------------------------------------------------------------- UNIQ_KEY

    [Fact]
    public void SetUniqIdIsIdempotentAnd32Hex()
    {
        var msg = new Message("TopicTest", new byte[1]);
        MessageClientIDSetter.SetUniqId(msg);
        string? first = MessageClientIDSetter.GetUniqId(msg);

        Assert.NotNull(first);
        Assert.Equal(32, first!.Length);
        Assert.Matches("^[0-9A-F]{32}$", first);

        MessageClientIDSetter.SetUniqId(msg);
        Assert.Equal(first, MessageClientIDSetter.GetUniqId(msg));
    }

    // ---------------------------------------------------------------- helpers

    private static SendMessageContext SendContext(string topic = "TopicTest") => new()
    {
        ProducerGroup = "GID_test",
        Message = new Message(topic, new byte[42]) { Tags = "TagA", Keys = "KeyA KeyB" },
        Mq = new MessageQueue("TopicTest", "broker-a", 0),
        BrokerAddr = "127.0.0.1:10911",
        MsgType = TraceMessageType.Normal,
    };

    private static ConsumeMessageContext ConsumeContext(List<MessageExt> msgs) => new("CID_test", msgs,
        new MessageQueue("TopicTest", "broker-a", 0))
    {
        // PropertyMap 是客户端项目内部的 global using（SortedDictionary），测试里写全名
        Props = new System.Collections.Generic.SortedDictionary<string, string>(),
        AccessChannel = RocketMQ.Client.AccessChannel.Local,
    };

    private static MessageExt MakeMsg(string msgId = MsgId1)
    {
        var m = new MessageExt
        {
            Topic = "TopicTest",
            MsgId = msgId,
            Tags = "TagA",
            Keys = "KeyA KeyB",
            ReconsumeTimes = 0,
            StoreSize = 42,
        };
        m.Properties[MessageConst.PropertyMsgRegion] = "DefaultRegion";
        return m;
    }

    /// <summary>把 dispatcher 内部队列取空（未 Start 时发不出去，只能直接读队列来验证 Append）。</summary>
    private static List<TraceContext> DrainQueue(AsyncTraceDispatcher d)
    {
        FieldInfo field = typeof(AsyncTraceDispatcher).GetField("_queue",
            BindingFlags.NonPublic | BindingFlags.Instance)!;
        var queue = (System.Collections.Concurrent.BlockingCollection<TraceContext>)field.GetValue(d)!;
        var list = new List<TraceContext>();
        while (queue.TryTake(out TraceContext? ctx, TimeSpan.Zero) && ctx is not null)
        {
            list.Add(ctx);
        }

        return list;
    }
}
