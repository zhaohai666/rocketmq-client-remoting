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
