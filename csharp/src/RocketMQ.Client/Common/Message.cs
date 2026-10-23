using System.Globalization;
using System.Text;

namespace RocketMQ.Common;

// ---------------------------------------------------------------- MessageQueue
public class MessageQueue : IComparable<MessageQueue>
{
    public string Topic { get; set; } = string.Empty;
    public string BrokerName { get; set; } = string.Empty;
    public int QueueId { get; set; }

    public MessageQueue()
    {
    }

    public MessageQueue(string topic, string brokerName, int queueId)
    {
        Topic = topic;
        BrokerName = brokerName;
        QueueId = queueId;
    }

    public string QueueIdStr => QueueId.ToString(CultureInfo.InvariantCulture);

    public int HashCode()
    {
        long brokerHash = LegacyHash.StringHash(BrokerName);
        long topicHash = LegacyHash.StringHash(Topic);
        long v = ((31 + brokerHash) * 31 + QueueId) * 31 + topicHash;
        return LegacyNumber.ToInt32(v);
    }

    public int CompareTo(MessageQueue? other)
    {
        if (other is null)
        {
            return 1;
        }

        if (Topic != other.Topic)
        {
            return string.CompareOrdinal(Topic, other.Topic);
        }

        if (BrokerName != other.BrokerName)
        {
            return string.CompareOrdinal(BrokerName, other.BrokerName);
        }

        if (QueueId != other.QueueId)
        {
            return QueueId < other.QueueId ? -1 : 1;
        }

        return 0;
    }

    public bool Equals(MessageQueue? other) =>
        other is not null && Topic == other.Topic && BrokerName == other.BrokerName && QueueId == other.QueueId;

    public override bool Equals(object? obj) => Equals(obj as MessageQueue);

    public override int GetHashCode() => HashCode();

    public static bool operator ==(MessageQueue left, MessageQueue right) => left.Equals(right);

    public static bool operator !=(MessageQueue left, MessageQueue right) => !left.Equals(right);

    public override string ToString() =>
        "MessageQueue [topic=" + Topic + ", brokerName=" + BrokerName + ", queueId=" + QueueIdStr + "]";
}

// ---------------------------------------------------------------- Message
public class Message
{
    public string Topic { get; set; } = string.Empty;
    public int Flag { get; set; }
    public PropertyMap Properties { get; set; } = new PropertyMap();

    private byte[] _body = Array.Empty<byte>();

    public byte[] Body
    {
        get => _body;
        // setBody semantics: sets the body and also sets hasBody to true.
        set
        {
            _body = value;
            HasBody = true;
        }
    }

    public bool HasBody { get; set; } = true;
    public string TransactionId { get; set; } = string.Empty;

    // Whether this is a batch message body (set by MessageBatch.GenerateFromList).
    // At send time it maps to SendMessageRequestHeader.batch —— on the broker side
    // SendMessageProcessor uses requestHeader.isBatch() to decide whether to go through sendBatchMessage
    // or sendMessage, so this flag is required.
    // Note: type checks are not used because Message is a non-polymorphic type (no virtual methods), so RTTI is unavailable.
    public bool IsBatch { get; set; }

    public Message()
    {
    }

    public Message(string topic, byte[] body)
    {
        Topic = topic;
        Body = body ?? Array.Empty<byte>();
    }

    public Message(string topic, string tags, string keys, byte[] body)
    {
        Topic = topic;
        Body = body ?? Array.Empty<byte>();
        if (!string.IsNullOrEmpty(tags))
        {
            Tags = tags;
        }

        if (!string.IsNullOrEmpty(keys))
        {
            Keys = keys;
        }
    }

    // ---- Property shortcuts ----
    public string Tags
    {
        get => Properties.TryGetValue(MessageConst.PropertyTags, out var v) ? v : string.Empty;
        set => Properties[MessageConst.PropertyTags] = value;
    }

    public string Keys
    {
        get => Properties.TryGetValue(MessageConst.PropertyKeys, out var v) ? v : string.Empty;
        set => Properties[MessageConst.PropertyKeys] = value;
    }

    public int DelayTimeLevel
    {
        get
        {
            if (!Properties.TryGetValue(MessageConst.PropertyDelayTimeLevel, out var s) ||
                string.IsNullOrEmpty(s))
            {
                return 0;
            }

            return int.TryParse(s, NumberStyles.Integer, CultureInfo.InvariantCulture, out var v) ? v : 0;
        }
        set => Properties[MessageConst.PropertyDelayTimeLevel] = value.ToString(CultureInfo.InvariantCulture);
    }

    public bool WaitStoreMsgOk
    {
        get => !Properties.TryGetValue(MessageConst.PropertyWaitStoreMsgOk, out var s)
               || string.Equals(s, "true", StringComparison.OrdinalIgnoreCase);
        set => Properties[MessageConst.PropertyWaitStoreMsgOk] = value ? "true" : "false";
    }

    public string WaitStoreMsgOkStr =>
        Properties.TryGetValue(MessageConst.PropertyWaitStoreMsgOk, out var s) ? s : "true";

    public string GetUserProperty(string name) =>
        Properties.TryGetValue(name, out var v) ? v : string.Empty;

    public void SetUserProperty(string name, string value) => Properties[name] = value;

    public string GetProperty(string name) =>
        Properties.TryGetValue(name, out var v) ? v : string.Empty;

    public void PutProperty(string name, string value) => Properties[name] = value;

    public void RemoveProperty(string name) => Properties.Remove(name);

    public void ClearProperty() => Properties.Clear();

    public void SetDelayTimeSec(long sec) =>
        PutProperty(MessageConst.PropertyTimerDelaySec, sec.ToString(CultureInfo.InvariantCulture));

    public long GetDelayTimeSec() => GetTimerProperty(MessageConst.PropertyTimerDelaySec);

    public void SetDelayTimeMs(long timeMs) =>
        PutProperty(MessageConst.PropertyTimerDelayMs, timeMs.ToString(CultureInfo.InvariantCulture));

    public long GetDelayTimeMs() => GetTimerProperty(MessageConst.PropertyTimerDelayMs);

    public void SetDeliverTimeMs(long timeMs) =>
        PutProperty(MessageConst.PropertyTimerDeliverMs, timeMs.ToString(CultureInfo.InvariantCulture));

    public long GetDeliverTimeMs() => GetTimerProperty(MessageConst.PropertyTimerDeliverMs);

    private long GetTimerProperty(string name)
    {
        return !Properties.TryGetValue(name, out string? t) || t is null
            ? 0
            : long.Parse(t, CultureInfo.InvariantCulture);
    }

    public override string ToString()
    {
        var props = new StringBuilder();
        foreach (var kv in Properties)
        {
            if (props.Length > 0)
            {
                props.Append(',');
            }

            props.Append(kv.Key).Append('=').Append(kv.Value);
        }

        return "Message [topic=" + Topic + ", flag=" + Flag.ToString(CultureInfo.InvariantCulture) +
               ", properties=" + props + ", body=" + Body.Length.ToString(CultureInfo.InvariantCulture) + " bytes]";
    }
}

// ---------------------------------------------------------------- MessageExt
public class MessageExt : Message
{
    public int QueueId { get; set; }
    public int StoreSize { get; set; }
    public long QueueOffset { get; set; }
    public int SysFlag { get; set; }
    public long BornTimestamp { get; set; }
    public string BornHost { get; set; } = string.Empty;
    public int BornHostPort { get; set; }
    public long StoreTimestamp { get; set; }
    public string StoreHost { get; set; } = string.Empty;
    public int StoreHostPort { get; set; }
    public string MsgId { get; set; } = string.Empty;
    public long CommitLogOffset { get; set; }
    public uint BodyCrc { get; set; }
    public int ReconsumeTimes { get; set; }
    public long PreparedTransactionOffset { get; set; }
    public string BrokerName { get; set; } = string.Empty;
    public string OffsetMsgId { get; set; } = string.Empty;
    public string MsgType { get; set; } = string.Empty;

    public MessageExt()
    {
    }

    public MessageExt(Message m)
    {
        Topic = m.Topic;
        Flag = m.Flag;
        Properties = new PropertyMap(m.Properties);
        Body = m.Body;
        HasBody = true;
        TransactionId = m.TransactionId;
        IsBatch = m.IsBatch;
    }

    public string BornHostString
    {
        get
        {
            if (!string.IsNullOrEmpty(BornHost) && BornHostPort != 0)
            {
                return BornHost + ":" + BornHostPort.ToString(CultureInfo.InvariantCulture);
            }

            return BornHost;
        }
    }

    public string StoreHostString
    {
        get
        {
            if (!string.IsNullOrEmpty(StoreHost) && StoreHostPort != 0)
            {
                return StoreHost + ":" + StoreHostPort.ToString(CultureInfo.InvariantCulture);
            }

            return StoreHost;
        }
    }

    public override string ToString() =>
        "MessageExt [queueId=" + QueueId.ToString(CultureInfo.InvariantCulture) +
        ", storeSize=" + StoreSize.ToString(CultureInfo.InvariantCulture) +
        ", queueOffset=" + QueueOffset.ToString(CultureInfo.InvariantCulture) +
        ", sysFlag=" + SysFlag.ToString(CultureInfo.InvariantCulture) +
        ", msgId=" + MsgId + ", topic=" + Topic + "]";
}

public class MessageBatch : Message
{
    public List<Message> Messages { get; set; } = new List<Message>();

    public MessageBatch()
    {
    }

    public MessageBatch(List<Message> msgs)
    {
        Messages = msgs;
    }

    public byte[] Encode() => MessageDecoder.EncodeMessages(Messages);

    public int Size => Messages.Count;

    /// A batch leaves as ONE message with ONE property set, so a member that asks for a later
    /// delivery would put that request on the envelope carrying every other member. "Later
    /// delivery" is the delay level or any of the three timer spellings; a 0 under one of them
    /// means no schedule.
    private static bool ScheduledDeliveryRequested(Message msg) =>
        msg.DelayTimeLevel > 0
        || msg.GetDelayTimeSec() > 0
        || msg.GetDelayTimeMs() > 0
        || msg.GetDeliverTimeMs() > 0;

    public static MessageBatch GenerateFromList(List<Message> msgs)
    {
        if (msgs == null || msgs.Count == 0)
        {
            throw new ArgumentException("messages must not be null or empty");
        }

        Message first = msgs[0];
        foreach (var m in msgs)
        {
            if (ScheduledDeliveryRequested(m))
            {
                throw new ArgumentException("Delayed messages are not supported for batching");
            }

            if (m.Topic.StartsWith(MixAll.RetryGroupTopicPrefix, StringComparison.Ordinal))
            {
                throw new ArgumentException("Retry Group is not supported for batching");
            }

            if (!ReferenceEquals(m, first))
            {
                if (first.Topic != m.Topic)
                {
                    throw new ArgumentException("The topic of the messages in one batch should be the same");
                }

                if (first.WaitStoreMsgOkStr != m.WaitStoreMsgOkStr)
                {
                    throw new ArgumentException("The waitStoreMsgOK of the messages in one batch should be the same");
                }
            }
        }

        var batch = new MessageBatch { Messages = msgs };
        batch.Topic = first.Topic;
        batch.WaitStoreMsgOk = first.WaitStoreMsgOk;
        foreach (Message m in msgs)
        {
            MessageClientIDSetter.SetUniqId(m);
        }

        MessageClientIDSetter.SetUniqId(batch);
        batch.Body = batch.Encode();
        batch.IsBatch = true; // make the send side set SendMessageRequestHeader.batch to true
        return batch;
    }
}

public static class MessageClientIDSetter
{
    /// <summary>When UNIQ_KEY is missing, write a 32-bit hex unique ID (idempotent).</summary>
    public static void SetUniqId(Message msg)
    {
        if (string.IsNullOrEmpty(msg.GetProperty(MessageConst.PropertyUniqClientMessageIdKeyidx)))
        {
            msg.PutProperty(MessageConst.PropertyUniqClientMessageIdKeyidx, UtilAll.CreateUniqId());
        }
    }

    public static string GetUniqId(Message msg) =>
        msg.GetProperty(MessageConst.PropertyUniqClientMessageIdKeyidx);
}
