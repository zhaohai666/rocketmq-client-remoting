using RocketMQ.Common;

namespace RocketMQ.Client;

// OpenTracing-style hook trio for the message lifecycle.
//
// Port of the Java client's SendMessageOpenTracingHookImpl /
// ConsumeMessageOpenTracingHookImpl / EndTransactionOpenTracingHookImpl (the
// same trio the PHP port carries as OpenTracingHook.php), on top of a
// duck-typed Tracer SPI with zero third-party deps.
//
// The in-flight span rides in the context's MqTraceContext slot, exactly like
// the Java original (context.setMqTraceContext(span)): before opens the span
// and stashes it, after reads it back and closes it. Registering both the
// trace dispatcher hooks and these hooks on the same producer is not
// supported (same restriction as Java).
//
// Cross-process propagation rides on the message properties themselves: the
// upstream context is extracted from the property map and the just-started
// span's context is injected back into it (Message.Properties is a live
// PropertyMap, so the injector's writes travel in-band with the message).

public static class OpenTracingTags
{
    // OpenTracing standard tags (io.opentracing.tag.Tags).
    public const string SpanKind = "span.kind";
    public const string SpanKindProducer = "producer";
    public const string SpanKindConsumer = "consumer";
    public const string PeerService = "peer.service";
    public const string MessageBusDestination = "message_bus.destination";

    // The rocketmq.* tag keys: the Java TraceConstants literals (the mgs_id /
    // mgs_type spellings are the Java protocol literals -- the typo is
    // load-bearing, do not "fix" it).
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

public static class OpenTracingTraceConstants
{
    public const string ToPrefix = "To_";
    public const string FromPrefix = "From_";
    public const string EndTransaction = "EndTransaction";
    // The transaction leg reports the commit state only.
    public const string TransMsgCommit = "Trans_msg_Commit";
}

// Duck-typed span. Tag values arrive as strings (the hook formats ints and
// bools: "true"/"false"), keeping the SPI trivially implementable without a
// boxed-object surface.
public interface IOpenTracingSpan
{
    void SetTag(string key, string value);

    /// The span context the tracer's inject() understands; may be the span itself.
    object? Context();

    void Finish();
}

// Duck-typed tracer:
//   - StartSpan opens a span; refs carries the upstream context (from
//     Extract) when there is one;
//   - Extract reads the upstream context from the message property map (null
//     means "no upstream");
//   - Inject writes the propagation keys of the just-started span into the
//     carrier, which then travels in-band with the message.
public interface IOpenTracingTracer
{
    IOpenTracingSpan StartSpan(string operation, IReadOnlyDictionary<string, string> tags,
        IReadOnlyList<object>? refs);

    object? Extract(PropertyMap carrier);

    void Inject(object spanContext, PropertyMap carrier);
}

// Java parity: the tag value is the TraceMessageType mapped to the Java
// MessageType enum *name* (NORMAL_MSG...).
public static class OpenTracingMessageTypeName
{
    public static string Of(TraceMessageType type) => type switch
    {
        TraceMessageType.Trans => "TRANS_MSG_HALF",
        TraceMessageType.TransCommit => "TRANS_MSG_COMMIT",
        TraceMessageType.Delay => "DELAY_MSG",
        TraceMessageType.Order => "ORDER_MSG",
        _ => "NORMAL_MSG",
    };
}

// ---------------------------------------------------------------- send leg

// Send leg: span "To_<topic>", kind producer, with the kind + service +
// destination + tags + keys + store_host + msg_type + body_length tags at
// open; success / msg_id / region_id at close. No result, or no region id on
// it: the span is left open, exactly like the Java close-out flow.
public sealed class SendMessageOpenTracingHook : ISendMessageHook
{
    private readonly IOpenTracingTracer _tracer;

    public SendMessageOpenTracingHook(IOpenTracingTracer tracer)
    {
        _tracer = tracer;
    }

    public string HookName() => "SendMessageOpenTracingHook";

    public void SendMessageBefore(SendMessageContext context)
    {
        if (context?.Message == null)
        {
            return;
        }
        var msg = context.Message;
        var tags = new Dictionary<string, string> { [OpenTracingTags.SpanKind] = OpenTracingTags.SpanKindProducer };
        object? upstream = null;
        try
        {
            upstream = _tracer.Extract(msg.Properties);
        }
        catch
        {
            // a failing extract must not break the send
        }
        var refs = upstream != null ? new List<object> { upstream } : null;
        var span = _tracer.StartSpan(OpenTracingTraceConstants.ToPrefix + msg.Topic, tags, refs);
        span.SetTag(OpenTracingTags.PeerService, OpenTracingTags.RocketmqService);
        span.SetTag(OpenTracingTags.MessageBusDestination, msg.Topic);
        span.SetTag(OpenTracingTags.RocketmqTags, msg.Tags ?? string.Empty);
        span.SetTag(OpenTracingTags.RocketmqKeys, msg.Keys ?? string.Empty);
        span.SetTag(OpenTracingTags.RocketmqStoreHost, context.BrokerAddr);
        span.SetTag(OpenTracingTags.RocketmqMsgType, OpenTracingMessageTypeName.Of(context.MsgType));
        span.SetTag(OpenTracingTags.RocketmqBodyLength,
            (msg.Body == null ? 0 : msg.Body.Length).ToString());
        // The just-started span's context is what travels: the downstream leg
        // must parent onto THIS send, not onto whatever upstream produced the
        // incoming context. Properties is a live map, so the injector's writes
        // land on the message directly.
        var sc = span.Context();
        if (sc != null)
        {
            try
            {
                _tracer.Inject(sc, msg.Properties);
            }
            catch
            {
                // propagation must never break the send
            }
        }
        context.MqTraceContext = span;
    }

    public void SendMessageAfter(SendMessageContext context)
    {
        if (context?.MqTraceContext is not IOpenTracingSpan span)
        {
            return;
        }
        // No result, or no region id on it: nothing to report, span stays open.
        if (context.SendResult == null || string.IsNullOrEmpty(context.SendResult.RegionId))
        {
            return;
        }
        span.SetTag(OpenTracingTags.RocketmqSuccess,
            context.SendResult.SendStatus == SendStatus.SendOk ? "true" : "false");
        span.SetTag(OpenTracingTags.RocketmqMsgId, context.SendResult.MsgId ?? string.Empty);
        span.SetTag(OpenTracingTags.RocketmqRegionId, context.SendResult.RegionId);
        span.Finish();
    }
}

// ---------------------------------------------------------------- consume leg

// Consume leg: one span per message, "From_<topic>", kind consumer; msg_id /
// tags / keys / body_length (store size) / retry_times / region_id at open;
// success at close.
public sealed class ConsumeMessageOpenTracingHook : IConsumeMessageHook
{
    private readonly IOpenTracingTracer _tracer;

    public ConsumeMessageOpenTracingHook(IOpenTracingTracer tracer)
    {
        _tracer = tracer;
    }

    public string HookName() => "ConsumeMessageOpenTracingHook";

    public void ConsumeMessageBefore(ConsumeMessageContext context)
    {
        if (context == null || context.MsgList.Count == 0)
        {
            return;
        }
        var spans = new List<IOpenTracingSpan>();
        foreach (var msg in context.MsgList)
        {
            var tags = new Dictionary<string, string> { [OpenTracingTags.SpanKind] = OpenTracingTags.SpanKindConsumer };
            object? upstream = null;
            try
            {
                upstream = _tracer.Extract(msg.Properties);
            }
            catch
            {
                // a failing extract must not break delivery
            }
            var refs = upstream != null ? new List<object> { upstream } : null;
            var span = _tracer.StartSpan(OpenTracingTraceConstants.FromPrefix + msg.Topic, tags, refs);
            span.SetTag(OpenTracingTags.PeerService, OpenTracingTags.RocketmqService);
            span.SetTag(OpenTracingTags.MessageBusDestination,
                NamespaceUtil.WithoutNamespace(msg.Topic));
            span.SetTag(OpenTracingTags.RocketmqMsgId, msg.MsgId ?? string.Empty);
            span.SetTag(OpenTracingTags.RocketmqTags, msg.Tags ?? string.Empty);
            span.SetTag(OpenTracingTags.RocketmqKeys, msg.Keys ?? string.Empty);
            span.SetTag(OpenTracingTags.RocketmqBodyLength, msg.StoreSize.ToString());
            span.SetTag(OpenTracingTags.RocketmqRetryTimers, msg.ReconsumeTimes.ToString());
            span.SetTag(OpenTracingTags.RocketmqRegionId,
                msg.GetProperty(MessageConst.PropertyMsgRegion) ?? string.Empty);
            spans.Add(span);
        }
        context.MqTraceContext = spans;
    }

    public void ConsumeMessageAfter(ConsumeMessageContext context)
    {
        if (context?.MsgList.Count == 0)
        {
            return;
        }
        if (context?.MqTraceContext is not List<IOpenTracingSpan> spans)
        {
            return;
        }
        foreach (var span in spans)
        {
            span.SetTag(OpenTracingTags.RocketmqSuccess, context.Success ? "true" : "false");
            span.Finish();
        }
    }
}

// ------------------------------------------------------------ transaction leg

// Transaction leg: span "EndTransaction" with the transaction identity keys,
// closed immediately. The message is left untouched on this leg (the send
// side already put the context on the properties).
public sealed class EndTransactionOpenTracingHook : IEndTransactionHook
{
    private readonly IOpenTracingTracer _tracer;

    public EndTransactionOpenTracingHook(IOpenTracingTracer tracer)
    {
        _tracer = tracer;
    }

    public string HookName() => "EndTransactionOpenTracingHook";

    public void EndTransaction(EndTransactionContext context)
    {
        if (context == null)
        {
            return;
        }
        var msg = context.Message;
        var tags = new Dictionary<string, string> { [OpenTracingTags.SpanKind] = OpenTracingTags.SpanKindProducer };
        object? upstream = null;
        if (msg != null)
        {
            try
            {
                upstream = _tracer.Extract(msg.Properties);
            }
            catch
            {
                // a failing extract must not break the leg
            }
        }
        var refs = upstream != null ? new List<object> { upstream } : null;
        var span = _tracer.StartSpan(OpenTracingTraceConstants.EndTransaction, tags, refs);
        span.SetTag(OpenTracingTags.PeerService, OpenTracingTags.RocketmqService);
        span.SetTag(OpenTracingTags.MessageBusDestination, msg?.Topic ?? string.Empty);
        span.SetTag(OpenTracingTags.RocketmqTags, msg?.Tags ?? string.Empty);
        span.SetTag(OpenTracingTags.RocketmqKeys, msg?.Keys ?? string.Empty);
        span.SetTag(OpenTracingTags.RocketmqStoreHost, context.BrokerAddr);
        span.SetTag(OpenTracingTags.RocketmqMsgId, context.MsgId ?? string.Empty);
        span.SetTag(OpenTracingTags.RocketmqMsgType, OpenTracingTraceConstants.TransMsgCommit);
        span.SetTag(OpenTracingTags.RocketmqTransactionId, context.TransactionId ?? string.Empty);
        span.SetTag(OpenTracingTags.RocketmqTransactionState,
            context.TransactionState is LocalTransactionState st
                ? LocalTransactionStateNames.Name(st)
                : (context.TransactionState?.ToString() ?? string.Empty));
        span.SetTag(OpenTracingTags.RocketmqIsFromTransactionCheck,
            context.FromTransactionCheck ? "true" : "false");
        span.Finish();
    }
}
