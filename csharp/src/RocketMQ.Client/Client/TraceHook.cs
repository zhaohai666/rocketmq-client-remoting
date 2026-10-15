// Two hard rules:
//   1. Trace messages themselves are not traced again: both before/after first check whether
//      the topic starts with the trace topic prefix, and return immediately if so (otherwise
//      traces would self-replicate).
//   2. Whether a trace is emitted is decided by the broker: on the send side it looks at the
//      RegionId / TraceOn of SendResult (parsed from MSG_REGION / TRACE_ON in the SEND response
//      header; the broker defaults to traceOn=true); on the consume side it checks whether the
//      message property TRACE_ON is "false".
using RocketMQ.Common;
using RocketMQ.Remoting;
using RocketMQ.Remoting.Protocol;

namespace RocketMQ.Client;

/// <summary>Send-side trace hook (corresponds to SendMessageTraceHookImpl).</summary>
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

/// <summary>Consume-side trace hook (corresponds to ConsumeMessageTraceHookImpl; SubBefore / SubAfter share request_id).</summary>
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

        // ConsumeContextType is the **name** of ConsumeReturnType; its ordinal is the contextCode in the trace.
        if (context.Props is not null && context.Props.TryGetValue("ConsumeContextType", out string? ct) && ct is not null)
        {
            subAfter.ContextCode = ReturnTypeOrdinal(ct);
        }

        _localDispatcher.Append(subAfter);
    }

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

/// <summary>Transaction-finish trace hook (corresponds to EndTransactionTraceHookImpl).</summary>
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

public sealed class DefaultRecallMessageTraceHook : IRpcHook
{
    public const string RecallTraceEnableKey = "com.rocketmq.recall.default.trace.enable";

    private static readonly string? EnvEnabled = Environment.GetEnvironmentVariable(RecallTraceEnableKey);

    private readonly AsyncTraceDispatcher _traceDispatcher;

    public DefaultRecallMessageTraceHook(AsyncTraceDispatcher traceDispatcher)
    {
        _traceDispatcher = traceDispatcher;
    }

    /// <summary>Defaults to the environment variable ("true"/"1", case-insensitive); can be explicitly overridden on the instance.</summary>
    public bool EnableDefaultTrace { get; set; } =
        string.Equals(EnvEnabled, "true", StringComparison.OrdinalIgnoreCase)
        || string.Equals(EnvEnabled, "1", StringComparison.OrdinalIgnoreCase);

    public void DoBeforeRequest(string remoteAddr, RemotingCommand request)
    {
    }

    public void DoAfterResponse(string remoteAddr, RemotingCommand request, RemotingCommand? response)
    {
        if (request.Code != RequestCode.RecallMessage
            || !EnableDefaultTrace
            || response is null
            || response.ExtFields is null
            || !response.ExtFields.TryGetValue(MessageConst.PropertyMsgRegion, out string? regionId)
            || string.IsNullOrEmpty(regionId)
            || _traceDispatcher is null)
        {
            return;
        }

        try
        {
            var header = new RecallMessageRequestHeader();
            header.FromExtFields(request.ExtFields ?? new PropertyMap());
            string topic = NamespaceUtil.WithoutNamespace(header.Topic ?? string.Empty);
            string group = NamespaceUtil.WithoutNamespace(header.ProducerGroup ?? string.Empty);
            HandleV1 handleV1 = RecallMessageHandle.DecodeHandle(header.RecallHandle);

            var traceBean = new TraceBean
            {
                Topic = topic,
                MsgId = handleV1.MessageId,
            };

            var traceContext = new TraceContext
            {
                RegionId = regionId,
                TraceBeans = new List<TraceBean> { traceBean },
                TraceType = RocketMQ.Client.TraceType.Recall,
                GroupName = group,
                IsSuccess = response.Code == ResponseCode.Success,
            };

            _traceDispatcher.Append(traceContext);
        }
        catch (Exception)
        {
        }
    }
}
