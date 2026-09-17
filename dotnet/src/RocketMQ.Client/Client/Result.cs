// 客户端结果类型与回调/监听器接口。
//
// 对应：
//   org.apache.rocketmq.client.producer.{SendResult, SendStatus, TransactionSendResult,
//                                       MessageQueueSelector, LocalTransactionState,
//                                       TransactionListener, SendCallback}
//   org.apache.rocketmq.client.consumer.listener.{ConsumeConcurrentlyStatus, ...}
//   org.apache.rocketmq.client.consumer.PullResult / PullStatus
using System.Globalization;
using RocketMQ.Common;
using RocketMQ.Remoting.Protocol;

namespace RocketMQ.Client;

// ---------------------------------------------------------------- 发送结果

/// <summary>org.apache.rocketmq.client.producer.SendStatus。</summary>
public enum SendStatus
{
    SendOk = 0,
    FlushDiskTimeout = 1,
    FlushSlaveTimeout = 2,
    SlaveNotAvailable = 3,
}

public static class SendStatusNames
{
    public static string Name(SendStatus s) => s switch
    {
        SendStatus.SendOk => "SEND_OK",
        SendStatus.FlushDiskTimeout => "FLUSH_DISK_TIMEOUT",
        SendStatus.FlushSlaveTimeout => "FLUSH_SLAVE_TIMEOUT",
        SendStatus.SlaveNotAvailable => "SLAVE_NOT_AVAILABLE",
        _ => "UNKNOWN",
    };
}

/// <summary>org.apache.rocketmq.client.producer.SendResult。</summary>
public class SendResult
{
    public SendStatus SendStatus { get; set; } = SendStatus.SendOk;
    public string MsgId { get; set; } = string.Empty;
    public string OffsetMsgId { get; set; } = string.Empty;
    public MessageQueue MessageQueue { get; set; } = new();
    public long QueueOffset { get; set; }
    public string TransactionId { get; set; } = string.Empty;
    public string RegionId { get; set; } = string.Empty;

    public override string ToString() =>
        "SendResult [sendStatus=" + SendStatusNames.Name(SendStatus)
        + ", msgId=" + MsgId + ", offsetMsgId=" + OffsetMsgId
        + ", messageQueue=" + MessageQueue
        + ", queueOffset=" + QueueOffset.ToString(CultureInfo.InvariantCulture)
        + ", transactionId=" + TransactionId + "]";
}

// ---------------------------------------------------------------- 事务

/// <summary>org.apache.rocketmq.client.producer.LocalTransactionState。</summary>
public enum LocalTransactionState
{
    CommitMessage = 0,
    RollbackMessage = 1,
    Unknow = 2,
}

public static class LocalTransactionStateNames
{
    public static string Name(LocalTransactionState s) => s switch
    {
        LocalTransactionState.CommitMessage => "COMMIT_MESSAGE",
        LocalTransactionState.RollbackMessage => "ROLLBACK_MESSAGE",
        LocalTransactionState.Unknow => "UNKNOW",
        _ => "UNKNOWN",
    };
}

/// <summary>org.apache.rocketmq.client.producer.TransactionSendResult。</summary>
public sealed class TransactionSendResult : SendResult
{
    public LocalTransactionState LocalTransactionState { get; set; } = LocalTransactionState.Unknow;
}

/// <summary>org.apache.rocketmq.client.producer.TransactionListener。</summary>
public interface ITransactionListener
{
    /// <summary>执行本地事务，返回提交/回滚/未知。</summary>
    LocalTransactionState ExecuteLocalTransaction(Message msg, string arg);

    /// <summary>broker 回查本地事务状态。</summary>
    LocalTransactionState CheckLocalTransaction(MessageExt msg);
}

// ---------------------------------------------------------------- 异步回调

/// <summary>org.apache.rocketmq.client.producer.SendCallback。</summary>
public interface ISendCallback
{
    void OnSuccess(SendResult sendResult);

    void OnException(string error);
}

// ---------------------------------------------------------------- 拉取结果

/// <summary>org.apache.rocketmq.client.consumer.PullStatus。</summary>
public enum PullStatus
{
    Found = 0,
    NoNewMsg = 1,
    NoMatchedMsg = 2,
    OffsetIllegal = 3,
}

public static class PullStatusNames
{
    public static string Name(PullStatus s) => s switch
    {
        PullStatus.Found => "FOUND",
        PullStatus.NoNewMsg => "NO_NEW_MSG",
        PullStatus.NoMatchedMsg => "NO_MATCHED_MSG",
        PullStatus.OffsetIllegal => "OFFSET_ILLEGAL",
        _ => "UNKNOWN",
    };
}

/// <summary>org.apache.rocketmq.client.consumer.PullResult。</summary>
public sealed class PullResult
{
    public PullStatus Status { get; set; } = PullStatus.NoNewMsg;
    public long NextBeginOffset { get; set; }
    public long MinOffset { get; set; }
    public long MaxOffset { get; set; }
    public List<MessageExt> MsgFoundList { get; set; } = new();

    public bool IsFound => Status == PullStatus.Found;

    public bool IsNoNewMsg => Status == PullStatus.NoNewMsg;
}

// ---------------------------------------------------------------- POP 结果

/// <summary>
/// org.apache.rocketmq.client.consumer.PopStatus。
/// POP 与 pull 的差别：POP 不提交位点，靠 ack 确认；没有新消息时返回 POLLING_NOT_FOUND
/// 而不是 NO_NEW_MSG。
/// </summary>
public enum PopStatus
{
    Found = 0,
    NoNewMsg = 1,
    PollingFull = 2,
    PollingNotFound = 3,
}

public static class PopStatusNames
{
    public static string Name(PopStatus s) => s switch
    {
        PopStatus.Found => "FOUND",
        PopStatus.NoNewMsg => "NO_NEW_MSG",
        PopStatus.PollingFull => "POLLING_FULL",
        PopStatus.PollingNotFound => "POLLING_NOT_FOUND",
        _ => "UNKNOWN",
    };
}

/// <summary>org.apache.rocketmq.client.consumer.PopResult。</summary>
public sealed class PopResult
{
    public PopStatus Status { get; set; } = PopStatus.PollingNotFound;
    public List<MessageExt> MsgFoundList { get; set; } = new();
    public long RestNum { get; set; }
    public long PopTime { get; set; }
    public long InvisibleTime { get; set; }
    public int ReviveQid { get; set; }

    /// <summary>每队列的起始 offset，形如 "0 3 0;0 2 0"；客户端据此反构 POP_CK。</summary>
    public string StartOffsetInfo { get; set; } = string.Empty;

    /// <summary>每队列本批弹出的 offset 列表，形如 "0 3 0,1,2"。</summary>
    public string MsgOffsetInfo { get; set; } = string.Empty;

    public string OrderCountInfo { get; set; } = string.Empty;

    public bool IsFound => Status == PopStatus.Found;
}

/// <summary>
/// CHANGE_MESSAGE_INVISIBLETIME 的结果。ExtraInfo 是用响应里**新的**
/// popTime/invisibleTime/reviveQid 重建的 8 段串，后续 ACK 必须用它。
/// </summary>
public sealed class ChangeInvisibleTimeResult
{
    public int Code { get; set; }
    public long PopTime { get; set; }
    public long InvisibleTime { get; set; }
    public int ReviveQid { get; set; }
    public string ExtraInfo { get; set; } = string.Empty;

    public bool Success => Code == ResponseCode.Success;
}

// ---------------------------------------------------------------- 消费状态

/// <summary>org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus。</summary>
public enum ConsumeConcurrentlyStatus
{
    ConsumeSuccess = 0,
    ReconsumeLater = 1,
}

/// <summary>org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus。</summary>
public enum ConsumeOrderlyStatus
{
    Success = 0,
    SuspendCurrentQueueAMoment = 1,
}

/// <summary>org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext。</summary>
public sealed class ConsumeConcurrentlyContext
{
    public MessageQueue MessageQueue { get; set; }
    public int DelayLevelWhenNextConsume { get; set; }
    public int AckIndex { get; set; } = -1;

    public ConsumeConcurrentlyContext(MessageQueue? mq = null) => MessageQueue = mq ?? new MessageQueue();
}

/// <summary>org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyContext。</summary>
public sealed class ConsumeOrderlyContext
{
    public MessageQueue MessageQueue { get; set; }
    public bool AutoCommit { get; set; } = true;

    public ConsumeOrderlyContext(MessageQueue? mq = null) => MessageQueue = mq ?? new MessageQueue();
}

/// <summary>消费者监听器基接口。Orderly 为 true 表示顺序消费。</summary>
public interface IMessageListener
{
    bool Orderly();
}

/// <summary>org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently。</summary>
public interface IMessageListenerConcurrently : IMessageListener
{
    ConsumeConcurrentlyStatus ConsumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context);
}

/// <summary>org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly。</summary>
public interface IMessageListenerOrderly : IMessageListener
{
    ConsumeOrderlyStatus ConsumeMessage(List<MessageExt> msgs, ConsumeOrderlyContext context);
}

// ---------------------------------------------------------------- 队列选择器

/// <summary>org.apache.rocketmq.client.producer.MessageQueueSelector。</summary>
public interface IMessageQueueSelector
{
    MessageQueue Select(IReadOnlyList<MessageQueue> mqs, Message msg, string arg);
}

/// <summary>对应 Java SelectMessageQueueByHash：arg 的 Java String.hashCode() 取模。</summary>
public sealed class SelectMessageQueueByHash : IMessageQueueSelector
{
    public MessageQueue Select(IReadOnlyList<MessageQueue> mqs, Message msg, string arg)
    {
        if (mqs.Count == 0)
        {
            throw new MQClientException("no message queue");
        }

        // 与 Java 一致：arg.hashCode() 取绝对值后对队列数取模。
        // 这里用 Java String.hashCode() 语义（Python 侧用的是 hash()，对字符串会随
        // PYTHONHASHSEED 变化，不可跨进程复现；Java 语义是确定性的，更适合做分片键）。
        int hashCode = JavaHash.JavaStringHash(arg);
        if (hashCode < 0)
        {
            hashCode = hashCode == int.MinValue ? 0 : -hashCode; // 避免 int.MinValue 取负溢出
        }

        return mqs[(int)((uint)hashCode % (uint)mqs.Count)];
    }
}

/// <summary>对应 Java SelectMessageQueueByRandom。</summary>
public sealed class SelectMessageQueueByRandom : IMessageQueueSelector
{
    public MessageQueue Select(IReadOnlyList<MessageQueue> mqs, Message msg, string arg)
    {
        if (mqs.Count == 0)
        {
            throw new MQClientException("no message queue");
        }

        return mqs[ThreadLocalRandom.Next(mqs.Count)];
    }
}
