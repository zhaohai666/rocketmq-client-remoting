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

/// 与 SendMessageContext 的关键差别：<b>没有 SendResult</b>（此刻还没发），带上 Arg
/// （send(msg, selector, arg) 里的业务参数）。
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

/// MsgList 是<b>可变的</b>：钩子把它替换/裁剪掉的消息会被客户端直接丢弃
/// （拉取路径 = 静默跳过、位点照常推进；POP 路径 = 立刻 ack）。
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

    /// <summary>本项目无 unit mode。</summary>
    public bool UnitMode { get; set; }
}

public interface IFilterMessageHook
{
    string HookName();

    void FilterMessage(FilterMessageContext context);
}
