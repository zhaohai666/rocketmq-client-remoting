// 消息轨迹钩子（对应 org.apache.rocketmq.client.trace.hook 包）。
//
//   * SendMessageTraceHook    ← SendMessageTraceHookImpl
//   * ConsumeMessageTraceHook ← ConsumeMessageTraceHookImpl
//   * EndTransactionTraceHook ← EndTransactionTraceHookImpl
//
// 两条硬性约定：
//   1. 轨迹消息本身不再被追踪：before/after 都先看 topic 是否以轨迹 topic 开头，是则直接
//      return（否则轨迹会自我复制）。
//   2. 是否落轨迹由 broker 说了算：发送侧看 SendResult 的 RegionId / TraceOn（由 SEND 响应头
//      的 MSG_REGION / TRACE_ON 解析而来，broker 默认 traceOn=true）；消费侧看消息属性
//      TRACE_ON 是否为 "false"。
using RocketMQ.Common;

namespace RocketMQ.Client;

/// <summary>发送侧轨迹钩子（对应 SendMessageTraceHookImpl）。</summary>
public sealed class SendMessageTraceHook : ISendMessageHook
{
    private readonly AsyncTraceDispatcher _localDispatcher;

    public SendMessageTraceHook(AsyncTraceDispatcher localDispatcher) => _localDispatcher = localDispatcher;

    public string HookName() => "SendMessageTraceHook";

    public void SendMessageBefore(SendMessageContext context)
    {
        if (context is null || context.Message is null)
        {
            return;
        }

        string topic = context.Message.Topic ?? string.Empty;
        if (topic.StartsWith(_localDispatcher.GetTraceTopicName(), StringComparison.Ordinal))
        {
            return;
        }

        var traceContext = new TraceContext
        {
            TraceType = RocketMQ.Client.TraceType.Pub,
            GroupName = NamespaceUtil.WithoutNamespace(context.ProducerGroup),
        };
        context.MqTraceContext = traceContext;
        var bean = new TraceBean
        {
            Topic = NamespaceUtil.WithoutNamespace(topic),
            Tags = context.Message.Tags ?? string.Empty,
            Keys = context.Message.Keys ?? string.Empty,
            StoreHost = context.BrokerAddr ?? string.Empty,
            BodyLength = context.Message.Body?.Length ?? 0,
            MsgType = context.MsgType,
        };
        traceContext.TraceBeans = new List<TraceBean> { bean };
    }

    public void SendMessageAfter(SendMessageContext context)
    {
        if (context is null || context.Message is null || context.SendResult is null)
        {
            return;
        }

        string topic = context.Message.Topic ?? string.Empty;
        if (topic.StartsWith(_localDispatcher.GetTraceTopicName(), StringComparison.Ordinal))
        {
            return;
        }

        if (context.MqTraceContext is not TraceContext traceContext || traceContext.TraceBeans.Count == 0)
        {
            return;
        }

        SendResult result = context.SendResult;
        // broker 侧 traceOn=false 或没带回 region 时不落库（对齐 Java）。
        if (string.IsNullOrEmpty(result.RegionId) || !result.TraceOn)
        {
            return;
        }

        TraceBean bean = traceContext.TraceBeans[0];
        int costTime = (int)((UtilAll.CurrentTimeMillis() - traceContext.TimeStamp) / traceContext.TraceBeans.Count);
        traceContext.CostTime = costTime;
        traceContext.IsSuccess = result.SendStatus == SendStatus.SendOk;
        traceContext.RegionId = result.RegionId;
        bean.MsgId = result.MsgId ?? string.Empty;
        bean.OffsetMsgId = result.OffsetMsgId ?? string.Empty;
        bean.StoreTime = traceContext.TimeStamp + (costTime / 2);
        _localDispatcher.Append(traceContext);
    }
}

/// <summary>消费侧轨迹钩子（对应 ConsumeMessageTraceHookImpl，SubBefore / SubAfter 共用 request_id）。</summary>
public sealed class ConsumeMessageTraceHook : IConsumeMessageHook
{
    private readonly AsyncTraceDispatcher _localDispatcher;

    public ConsumeMessageTraceHook(AsyncTraceDispatcher localDispatcher) => _localDispatcher = localDispatcher;

    public string HookName() => "ConsumeMessageTraceHook";

    public void ConsumeMessageBefore(ConsumeMessageContext context)
    {
        if (context is null || context.MsgList.Count == 0)
        {
            return;
        }

        var traceContext = new TraceContext
        {
            TraceType = RocketMQ.Client.TraceType.SubBefore,
            GroupName = NamespaceUtil.WithoutNamespace(context.ConsumerGroup),
        };
        context.MqTraceContext = traceContext;
        var beans = new List<TraceBean>();
        foreach (MessageExt msg in context.MsgList)
        {
            if (msg is null)
            {
                continue;
            }

            string? traceOn = msg.GetProperty(MessageConst.PropertyTraceSwitch);
            if (traceOn is not null && traceOn == "false")
            {
                continue;
            }

            string regionId = msg.GetProperty(MessageConst.PropertyMsgRegion);
            var bean = new TraceBean
            {
                Topic = NamespaceUtil.WithoutNamespace(msg.Topic),
                MsgId = msg.MsgId ?? string.Empty,
                Tags = msg.Tags ?? string.Empty,
                Keys = msg.Keys ?? string.Empty,
                StoreTime = msg.StoreTimestamp,
                BodyLength = msg.StoreSize,
                RetryTimes = msg.ReconsumeTimes,
            };
            traceContext.RegionId = regionId ?? string.Empty;
            beans.Add(bean);
        }

        if (beans.Count > 0)
        {
            traceContext.TraceBeans = beans;
            traceContext.TimeStamp = UtilAll.CurrentTimeMillis();
            _localDispatcher.Append(traceContext);
        }
    }

    public void ConsumeMessageAfter(ConsumeMessageContext context)
    {
        if (context is null || context.MsgList.Count == 0)
        {
            return;
        }

        if (context.MqTraceContext is not TraceContext subBefore || subBefore.TraceBeans.Count == 0)
        {
            return;
        }

        var subAfter = new TraceContext
        {
            TraceType = RocketMQ.Client.TraceType.SubAfter,
            RegionId = subBefore.RegionId,
            GroupName = NamespaceUtil.WithoutNamespace(subBefore.GroupName),
            RequestId = subBefore.RequestId,
            AccessChannel = context.AccessChannel,
            IsSuccess = context.Success,
            TraceBeans = subBefore.TraceBeans,
        };
        subAfter.CostTime = (int)((UtilAll.CurrentTimeMillis() - subBefore.TimeStamp) / context.MsgList.Count);

        // ConsumeContextType 是 ConsumeReturnType 的**名字**，其 ordinal 即轨迹里的 contextCode。
        if (context.Props is not null && context.Props.TryGetValue("ConsumeContextType", out string? ct) && ct is not null)
        {
            subAfter.ContextCode = ReturnTypeOrdinal(ct);
        }

        _localDispatcher.Append(subAfter);
    }

    /// <summary>ConsumeReturnType 名字 → ordinal（对齐 Java org.apache.rocketmq.client.consumer.listener.
    /// ConsumeReturnType：SUCCESS=0、TIME_OUT=1、EXCEPTION=2、RETURNNULL=3、FAILED=4）。
    /// ⚠ 这里**不是** ConsumeConcurrentlyStatus（CONSUME_SUCCESS/RECONSUME_LATER），写错会让
    /// 控制台的 contextCode 整体错位。</summary>
    private static int ReturnTypeOrdinal(string name) => name switch
    {
        "SUCCESS" => 0,
        "TIME_OUT" => 1,
        "EXCEPTION" => 2,
        "RETURNNULL" => 3,
        "FAILED" => 4,
        _ => 0,
    };
}

/// <summary>事务收尾轨迹钩子（对应 EndTransactionTraceHookImpl）。</summary>
public sealed class EndTransactionTraceHook : IEndTransactionHook
{
    private readonly AsyncTraceDispatcher _localDispatcher;

    public EndTransactionTraceHook(AsyncTraceDispatcher localDispatcher) => _localDispatcher = localDispatcher;

    public string HookName() => "EndTransactionTraceHook";

    public void EndTransaction(EndTransactionContext context)
    {
        if (context is null || context.Message is null)
        {
            return;
        }

        string topic = context.Message.Topic ?? string.Empty;
        if (topic.StartsWith(_localDispatcher.GetTraceTopicName(), StringComparison.Ordinal))
        {
            return;
        }

        Message msg = context.Message;
        var tuxeContext = new TraceContext
        {
            TraceType = RocketMQ.Client.TraceType.EndTransaction,
            GroupName = NamespaceUtil.WithoutNamespace(context.ProducerGroup),
        };
        var bean = new TraceBean
        {
            Topic = NamespaceUtil.WithoutNamespace(topic),
            Tags = msg.Tags ?? string.Empty,
            Keys = msg.Keys ?? string.Empty,
            StoreHost = context.BrokerAddr ?? string.Empty,
            MsgType = TraceMessageType.TransCommit,
            ClientHost = _localDispatcher.ClientId(),
            MsgId = context.MsgId ?? string.Empty,
            TransactionId = context.TransactionId,
            TransactionState = TransactionStateName(context.TransactionState),
            FromTransactionCheck = context.FromTransactionCheck,
        };
        string? regionId = msg.GetProperty(MessageConst.PropertyMsgRegion);
        tuxeContext.RegionId = regionId ?? MixAll.DefaultTraceRegionId;
        tuxeContext.TraceBeans = new List<TraceBean> { bean };
        tuxeContext.TimeStamp = UtilAll.CurrentTimeMillis();
        _localDispatcher.Append(tuxeContext);
    }

    /// <summary>把 LocalTransactionState 映射成 Java 枚举名（Java 写的是 name()，即
    /// COMMIT_MESSAGE / ROLLBACK_MESSAGE / UNKNOW）。⚠ .NET 枚举成员是 CommitMessage 这种
    /// 帕斯卡命名，直接 ToString() 会写出 "CommitMessage"，控制台认不出来。</summary>
    private static string TransactionStateName(object? state) => state switch
    {
        null => string.Empty,
        string s => s,
        LocalTransactionState st => st switch
        {
            LocalTransactionState.CommitMessage => "COMMIT_MESSAGE",
            LocalTransactionState.RollbackMessage => "ROLLBACK_MESSAGE",
            LocalTransactionState.Unknow => "UNKNOW",
            _ => st.ToString(),
        },
        _ => state.ToString() ?? string.Empty,
    };
}
