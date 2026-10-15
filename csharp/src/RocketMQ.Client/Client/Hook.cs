using RocketMQ.Common;

namespace RocketMQ.Client;

public sealed class SendMessageContext
{
    public object? Producer { get; set; }
    public string ProducerGroup { get; set; } = string.Empty;
    public Message? Message { get; set; }
    public MessageQueue? Mq { get; set; }
    public string BrokerAddr { get; set; } = string.Empty;
    public string BornHost { get; set; } = string.Empty;
    public object? CommunicationMode { get; set; }
    public SendResult? SendResult { get; set; }
    public Exception? Exception { get; set; }
    public object? MqTraceContext { get; set; }
    public PropertyMap? Props { get; set; }
    public TraceMessageType MsgType { get; set; } = TraceMessageType.Normal;
    public string Namespace { get; set; } = string.Empty;
}

public interface ISendMessageHook
{
    string HookName();

    void SendMessageBefore(SendMessageContext context);

    void SendMessageAfter(SendMessageContext context);
}

public sealed class ConsumeMessageContext
{
    public ConsumeMessageContext(string consumerGroup = "", List<MessageExt>? msgList = null,
        MessageQueue? mq = null)
    {
        ConsumerGroup = consumerGroup;
        MsgList = msgList ?? new List<MessageExt>();
        Mq = mq;
    }

    public string ConsumerGroup { get; set; }
    public List<MessageExt> MsgList { get; set; }
    public MessageQueue? Mq { get; set; }
    public bool Success { get; set; } = true;
    public string? Status { get; set; }
    public object? MqTraceContext { get; set; }
    public PropertyMap? Props { get; set; }
    public AccessChannel? AccessChannel { get; set; }
}

public interface IConsumeMessageHook
{
    string HookName();

    void ConsumeMessageBefore(ConsumeMessageContext context);

    void ConsumeMessageAfter(ConsumeMessageContext context);
}

public sealed class EndTransactionContext
{
    public string ProducerGroup { get; set; } = string.Empty;
    public Message? Message { get; set; }
    public string BrokerAddr { get; set; } = string.Empty;
    public string? MsgId { get; set; }
    public string? TransactionId { get; set; }
    public object? TransactionState { get; set; }
    public bool FromTransactionCheck { get; set; }
    public string Namespace { get; set; } = string.Empty;
}

public interface IEndTransactionHook
{
    string HookName();

    void EndTransaction(EndTransactionContext context);
}

public enum CommunicationMode
{
    Sync = 0,
    Async = 1,
    Oneway = 2,
}

/// The key difference from SendMessageContext: <b>no SendResult</b> (not sent yet), it carries Arg
/// (the business parameter in send(msg, selector, arg)).
/// </summary>
public sealed class CheckForbiddenContext
{
    public string NameSrvAddr { get; set; } = string.Empty;
    public string Group { get; set; } = string.Empty;
    public Message? Message { get; set; }
    public MessageQueue? Mq { get; set; }
    public string BrokerAddr { get; set; } = string.Empty;
    public CommunicationMode CommunicationMode { get; set; } = CommunicationMode.Sync;
    public SendResult? SendResult { get; set; }
    public Exception? Exception { get; set; }
    public object? Arg { get; set; }

    public bool UnitMode { get; set; }
}

public interface ICheckForbiddenHook
{
    string HookName();

    void CheckForbidden(CheckForbiddenContext context);
}

    /// MsgList is <b>mutable</b>: messages the hook replaces/drops are directly discarded by the client
    /// (pull path = silently skipped, offset still advances; POP path = ack immediately).
/// </summary>
public sealed class FilterMessageContext
{
    public FilterMessageContext(string consumerGroup = "", List<MessageExt>? msgList = null,
        MessageQueue? mq = null)
    {
        ConsumerGroup = consumerGroup;
        MsgList = msgList ?? new List<MessageExt>();
        Mq = mq;
    }

    public string ConsumerGroup { get; set; }
    public List<MessageExt> MsgList { get; set; }
    public MessageQueue? Mq { get; set; }
    public object? Arg { get; set; }

    /// <summary>This project has no unit mode.</summary>
    public bool UnitMode { get; set; }
}

public interface IFilterMessageHook
{
    string HookName();

    void FilterMessage(FilterMessageContext context);
}
