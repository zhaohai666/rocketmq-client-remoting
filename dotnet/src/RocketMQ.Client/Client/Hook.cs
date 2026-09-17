// 客户端钩子（对应 org.apache.rocketmq.client.hook 包）。
//
// Java 侧钩子是「业务无关的切面」：生产者在 sendKernelImpl 前后各调一次 SendMessageHook，
// 消费者在投递 listener 前后各调一次 ConsumeMessageHook，事务回查收尾调 EndTransactionHook。
// 消息轨迹（trace.hook.*）正是建在这两个接口上的。
//
// 设计约定（与 Java 一致）：
//   * 钩子抛出的异常**必须被吞掉并记 warn**（DefaultMQProducerImpl 的钩子调用点），
//     绝不能因为轨迹出错影响正常收发；
//   * mq_trace_context 是钩子自己的私有状态：before 写入、after 取出。
using RocketMQ.Common;

namespace RocketMQ.Client;

/// <summary>对应 org.apache.rocketmq.client.hook.SendMessageContext。</summary>
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

/// <summary>对应 org.apache.rocketmq.client.hook.SendMessageHook。</summary>
public interface ISendMessageHook
{
    string HookName();

    void SendMessageBefore(SendMessageContext context);

    void SendMessageAfter(SendMessageContext context);
}

/// <summary>对应 org.apache.rocketmq.client.hook.ConsumeMessageContext。</summary>
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

/// <summary>对应 org.apache.rocketmq.client.hook.ConsumeMessageHook。</summary>
public interface IConsumeMessageHook
{
    string HookName();

    void ConsumeMessageBefore(ConsumeMessageContext context);

    void ConsumeMessageAfter(ConsumeMessageContext context);
}

/// <summary>对应 org.apache.rocketmq.client.hook.EndTransactionContext。</summary>
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

/// <summary>对应 org.apache.rocketmq.client.hook.EndTransactionHook。</summary>
public interface IEndTransactionHook
{
    string HookName();

    void EndTransaction(EndTransactionContext context);
}

/// <summary>对应 org.apache.rocketmq.client.impl.CommunicationMode（Java 是枚举，三个常量）。</summary>
public enum CommunicationMode
{
    Sync = 0,
    Async = 1,
    Oneway = 2,
}

/// <summary>对应 org.apache.rocketmq.client.hook.CheckForbiddenContext。
///
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

    /// <summary>本项目无 unit mode（Java 的 isUnitMode() 恒为 false）。</summary>
    public bool UnitMode { get; set; }
}

/// <summary>对应 org.apache.rocketmq.client.hook.CheckForbiddenHook。
///
/// ⚠ 与 Send/Consume 钩子<b>相反</b>：CheckForbidden 抛出的异常<b>不会被吞掉</b>
/// （Java 签名就是 <c>throws MQClientException</c>），而是沿发送重试链向上传播 ——
/// 这正是"拦截"能力的实现方式。
/// </summary>
public interface ICheckForbiddenHook
{
    string HookName();

    void CheckForbidden(CheckForbiddenContext context);
}

/// <summary>对应 org.apache.rocketmq.client.hook.FilterMessageContext。
///
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

/// <summary>对应 org.apache.rocketmq.client.hook.FilterMessageHook。</summary>
public interface IFilterMessageHook
{
    string HookName();

    void FilterMessage(FilterMessageContext context);
}
