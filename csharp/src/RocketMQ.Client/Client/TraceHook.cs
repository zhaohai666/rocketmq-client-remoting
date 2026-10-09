// 消息轨迹钩子（对应 org.apache.rocketmq.client.trace.hook 包）。
//
//   * SendMessageTraceHook        ← SendMessageTraceHookImpl
//   * ConsumeMessageTraceHook     ← ConsumeMessageTraceHookImpl
//   * EndTransactionTraceHook     ← EndTransactionTraceHookImpl
//   * DefaultRecallMessageTraceHook ← 同名 Java 类（RPCHook 形态，见文件尾）
//
// 两条硬性约定：
//   1. 轨迹消息本身不再被追踪：before/after 都先看 topic 是否以轨迹 topic 开头，是则直接
//      return（否则轨迹会自我复制）。
//   2. 是否落轨迹由 broker 说了算：发送侧看 SendResult 的 RegionId / TraceOn（由 SEND 响应头
//      的 MSG_REGION / TRACE_ON 解析而来，broker 默认 traceOn=true）；消费侧看消息属性
//      TRACE_ON 是否为 "false"。
using RocketMQ.Common;
using RocketMQ.Remoting;
using RocketMQ.Remoting.Protocol;

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
    /// COMMIT_MESSAGE / ROLLBACK_MESSAGE / UNKNOW）。⚠ C# 枚举成员是 CommitMessage 这种
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

/// <summary>
/// 撤回消息的轨迹钩子（对应 org.apache.rocketmq.client.trace.hook.DefaultRecallMessageTraceHook）。
///
/// 与发送/消费钩子不同，Java 把它实现成 <b>RPCHook</b>：RECALL_MESSAGE(370) 没有发送上下文
/// 可挂，轨迹只能在 RPC 的响应侧记 —— DoAfterResponse 里按请求码过滤出 recall 这一笔，
/// 解出句柄里的 topic / msgId，组装 TraceType.Recall 的轨迹丢给分发器（编码器见
/// TraceDataEncoder 的 "Recall" 分支：时间戳/region/group/topic/msgId/成功与否 六段）。
///
/// 三道闸（Java 逐条对应，缺一就直接返回）：
///   1. 请求码必须是 RECALL_MESSAGE；
///   2. <see cref="EnableDefaultTrace"/> 为真 —— Java 读系统属性
///      <c>com.rocketmq.recall.default.trace.enable</c>（默认 false），本端口读同名环境变量
///      （"true"/"1"，与 ROCKETMQ_TLS_ENABLE 同一口径），也允许实例上直接赋值打开；
///   3. 响应 ExtFields 里带 MSG_REGION（broker 没回 region 就不落轨迹）且分发器非空。
///
/// 解句柄/建轨迹的**任何异常都吞掉**（Java 同处 catch Exception 空处理）：
/// 轨迹是旁路观测，绝不能让一条格式坏的句柄把撤回 RPC 本身搞挂。
/// </summary>
public sealed class DefaultRecallMessageTraceHook : IRpcHook
{
    /// <summary>Java 的 RECALL_TRACE_ENABLE_KEY。</summary>
    public const string RecallTraceEnableKey = "com.rocketmq.recall.default.trace.enable";

    private static readonly string? EnvEnabled = Environment.GetEnvironmentVariable(RecallTraceEnableKey);

    private readonly AsyncTraceDispatcher _traceDispatcher;

    public DefaultRecallMessageTraceHook(AsyncTraceDispatcher traceDispatcher)
    {
        _traceDispatcher = traceDispatcher;
    }

    /// <summary>默认取环境变量（"true"/"1"，忽略大小写），实例上可显式覆盖。</summary>
    public bool EnableDefaultTrace { get; set; } =
        string.Equals(EnvEnabled, "true", StringComparison.OrdinalIgnoreCase)
        || string.Equals(EnvEnabled, "1", StringComparison.OrdinalIgnoreCase);

    public void DoBeforeRequest(string remoteAddr, RemotingCommand request)
    {
        // Java 同款空实现：轨迹记在响应侧
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
            // Java 同处：吞掉一切异常（轨迹旁路，不影响撤回本身）
        }
    }
}
