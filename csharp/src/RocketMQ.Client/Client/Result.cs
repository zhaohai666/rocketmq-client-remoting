// Client result types and callback/listener interfaces.
using System.Globalization;
using RocketMQ.Common;
using RocketMQ.Remoting.Protocol;

namespace RocketMQ.Client;

// ---------------------------------------------------------------- Send result

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

public class SendResult
{
    public SendStatus SendStatus { get; set; } = SendStatus.SendOk;
    public string MsgId { get; set; } = string.Empty;
    public string OffsetMsgId { get; set; } = string.Empty;
    public MessageQueue MessageQueue { get; set; } = new();
    public long QueueOffset { get; set; }
    public string TransactionId { get; set; } = string.Empty;
    public string RegionId { get; set; } = string.Empty;
    // The broker tells via the TRACE_ON field of the SEND response header whether to record the trace ("false" turns it off; defaults to true).
    public bool TraceOn { get; set; } = true;
    public string? RecallHandle { get; set; }

    public override string ToString() =>
        "SendResult [sendStatus=" + SendStatusNames.Name(SendStatus)
        + ", msgId=" + MsgId + ", offsetMsgId=" + OffsetMsgId
        + ", messageQueue=" + MessageQueue
        + ", queueOffset=" + QueueOffset.ToString(CultureInfo.InvariantCulture)
        + ", transactionId=" + TransactionId + ", regionId=" + RegionId + "]";
}

// ---------------------------------------------------------------- Transaction

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

public sealed class TransactionSendResult : SendResult
{
    public LocalTransactionState LocalTransactionState { get; set; } = LocalTransactionState.Unknow;
}

public interface ITransactionListener
{
    /// <summary>Execute the local transaction and return commit/rollback/unknown.</summary>
    LocalTransactionState ExecuteLocalTransaction(Message msg, string arg);

    /// <summary>The broker checks back the local transaction state.</summary>
    LocalTransactionState CheckLocalTransaction(MessageExt msg);
}

// ---------------------------------------------------------------- Async callbacks

public interface ISendCallback
{
    void OnSuccess(SendResult sendResult);

    void OnException(Exception error);
}

// ---------------------------------------------------------------- Pull result

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

public sealed class PullResult
{
    public PullStatus Status { get; set; } = PullStatus.NoNewMsg;
    public long NextBeginOffset { get; set; }
    public long MinOffset { get; set; }
    public long MaxOffset { get; set; }
    public List<MessageExt> MsgFoundList { get; set; } = new();

    public long? SuggestWhichBrokerId { get; set; }

    public bool IsFound => Status == PullStatus.Found;

    public bool IsNoNewMsg => Status == PullStatus.NoNewMsg;
}

public interface IPullCallback
{
    void OnSuccess(PullResult pullResult);

    void OnException(Exception error);
}

// ---------------------------------------------------------------- POP result

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

public sealed class PopResult
{
    public PopStatus Status { get; set; } = PopStatus.PollingNotFound;
    public List<MessageExt> MsgFoundList { get; set; } = new();
    public long RestNum { get; set; }
    public long PopTime { get; set; }
    public long InvisibleTime { get; set; }
    public int ReviveQid { get; set; }

    /// <summary>Start offset per queue, e.g. "0 3 0;0 2 0"; the client uses it to reconstruct the POP_CK.</summary>
    public string StartOffsetInfo { get; set; } = string.Empty;

    /// <summary>List of offsets popped in this batch per queue, e.g. "0 3 0,1,2".</summary>
    public string MsgOffsetInfo { get; set; } = string.Empty;

    public string OrderCountInfo { get; set; } = string.Empty;

    public bool IsFound => Status == PopStatus.Found;
}

/// <summary>
/// Result of CHANGE_MESSAGE_INVISIBLETIME. ExtraInfo is the 8-segment string rebuilt using the **new**
/// popTime/invisibleTime/reviveQid from the response; subsequent ACKs must use it.
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

// ---------------------------------------------------------------- Consume status

public enum ConsumeConcurrentlyStatus
{
    ConsumeSuccess = 0,
    ReconsumeLater = 1,
}

public enum ConsumeOrderlyStatus
{
    Success = 0,
    Rollback = 1,
    Commit = 2,
    SuspendCurrentQueueAMoment = 3,
}

public static class ConsumeConcurrentlyStatusNames
{
    public static string Name(ConsumeConcurrentlyStatus s) => s switch
    {
        ConsumeConcurrentlyStatus.ConsumeSuccess => "CONSUME_SUCCESS",
        ConsumeConcurrentlyStatus.ReconsumeLater => "RECONSUME_LATER",
        _ => "UNKNOWN",
    };
}

public static class ConsumeOrderlyStatusNames
{
    public static string Name(ConsumeOrderlyStatus s) => s switch
    {
        ConsumeOrderlyStatus.Success => "SUCCESS",
        ConsumeOrderlyStatus.Rollback => "ROLLBACK",
        ConsumeOrderlyStatus.Commit => "COMMIT",
        ConsumeOrderlyStatus.SuspendCurrentQueueAMoment => "SUSPEND_CURRENT_QUEUE_A_MOMENT",
        _ => "UNKNOWN",
    };
}

public sealed class ConsumeConcurrentlyContext
{
    public MessageQueue MessageQueue { get; set; }
    public int DelayLevelWhenNextConsume { get; set; }

    public int AckIndex { get; set; } = int.MaxValue;

    public ConsumeConcurrentlyContext(MessageQueue? mq = null) => MessageQueue = mq ?? new MessageQueue();
}

public sealed class ConsumeOrderlyContext
{
    public MessageQueue MessageQueue { get; set; }

    public bool AutoCommit { get; set; } = true;

    public int SuspendCurrentQueueTimeMillis { get; set; } = -1;

    public ConsumeOrderlyContext(MessageQueue? mq = null) => MessageQueue = mq ?? new MessageQueue();
}

/// <summary>Base interface for consumer listeners. Orderly=true means ordered consumption.</summary>
public interface IMessageListener
{
    bool Orderly();
}

public interface IMessageListenerConcurrently : IMessageListener
{
    ConsumeConcurrentlyStatus ConsumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context);
}

public interface IMessageListenerOrderly : IMessageListener
{
    ConsumeOrderlyStatus ConsumeMessage(List<MessageExt> msgs, ConsumeOrderlyContext context);
}

// ---------------------------------------------------------------- Queue selector

public interface IMessageQueueSelector
{
    MessageQueue Select(IReadOnlyList<MessageQueue> mqs, Message msg, string arg);
}

public sealed class SelectMessageQueueByHash : IMessageQueueSelector
{
    public MessageQueue Select(IReadOnlyList<MessageQueue> mqs, Message msg, string arg)
    {
        if (mqs.Count == 0)
        {
            throw new MQClientException("no message queue");
        }

        int hashCode = LegacyHash.StringHash(arg);
        if (hashCode < 0)
        {
            hashCode = hashCode == int.MinValue ? 0 : -hashCode; // avoid overflow when negating int.MinValue
        }

        return mqs[(int)((uint)hashCode % (uint)mqs.Count)];
    }
}

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
