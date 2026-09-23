// org.apache.rocketmq.common.message 的 C# 对应：消息模型。
//
// 对齐 cpp/include/rocketmq/common/message.h 与 message.cpp：
//   - MessageQueue.hashCode 与 Java 逐位一致（32 位有符号回绕）；
//   - 属性快捷方式（TAGS/KEYS/DELAY/WAIT）读写 Properties；
//   - MessageBatch.generateFromList 的约束：非空 / 同 topic / 同 waitStoreMsgOK / 禁延时 / 禁重试 topic。
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

    // 与 Java 对齐：((31 + brokerHash) * 31 + queueId) * 31 + topicHash，按 32 位有符号回绕。
    public int HashCode()
    {
        long brokerHash = JavaHash.JavaStringHash(BrokerName);
        long topicHash = JavaHash.JavaStringHash(Topic);
        long v = ((31 + brokerHash) * 31 + QueueId) * 31 + topicHash;
        return JavaNumber.ToInt32(v);
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

    // Java MessageQueue 的 hashCode 用上面的 HashCode()，因此 GetHashCode 与之对齐。
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
        // C++ setBody：设置 body 同时把 hasBody 置 true。
        set
        {
            _body = value;
            HasBody = true;
        }
    }

    public bool HasBody { get; set; } = true;
    public string TransactionId { get; set; } = string.Empty;

    // 是否为批量消息体（由 MessageBatch.GenerateFromList 置位）。
    // 发送时映射到 SendMessageRequestHeader.batch —— broker 端
    // SendMessageProcessor 用 requestHeader.isBatch() 决定走 sendBatchMessage
    // 还是 sendMessage，所以这个标志是必须的。
    // 说明：不用类型判断是因为 Message 非多态类型（无虚函数），RTTI 不可用。
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

    // ---- 属性快捷方式 ----
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
        get
        {
            if (!Properties.TryGetValue(MessageConst.PropertyWaitStoreMsgOk, out var s))
            {
                return true;
            }

            return !(s.Length == 5 && (s[0] == 'f' || s[0] == 'F') && (s[1] == 'a' || s[1] == 'A') &&
                     (s[2] == 'l' || s[2] == 'L') && (s[3] == 's' || s[3] == 'S') &&
                     (s[4] == 'e' || s[4] == 'E'));
        }
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

// ---------------------------------------------------------------- MessageBatch
// 对应 org.apache.rocketmq.common.message.MessageBatch：自身不新增序列化字段，
// body 由 Encode() 生成（MessageDecoder 的 6 段轻量格式拼接结果）。
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

    // 对应 Java MessageBatch.generateFromList：
    // 非空 / 同 topic / 同 waitStoreMsgOK / 禁止延时 / 禁止重试 topic。
    public static MessageBatch GenerateFromList(List<Message> msgs)
    {
        if (msgs == null || msgs.Count == 0)
        {
            throw new ArgumentException("messages must not be null or empty");
        }

        Message first = msgs[0];
        foreach (var m in msgs)
        {
            if (m.DelayTimeLevel > 0)
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
        // 对应 Java MessageBatch.generateFromList + DefaultMQProducer.batch():1176-1182 的**顺序**：
        // 每条子消息先写好 UNIQ_KEY，编码后随 body 一起下发给 broker（落地 + 消费侧反查的
        // 轨迹 msgId 来源）。发送侧只对非批量消息补 UNIQ_KEY，所以批量消息的 ID 必须在这里逐条写。
        foreach (Message m in msgs)
        {
            MessageClientIDSetter.SetUniqId(m);
        }

        // Java batch():1179 在 generateFromList 之后给整批那条消息也补一个 ID（inner-batch 时
        // broker 把它原样回在 batchUniqId 里），它就是这个端口的 SendResult.MsgId；
        // 最后才编码 body（对应 batch():1181 setBody(encode())）。
        MessageClientIDSetter.SetUniqId(batch);
        batch.Body = batch.Encode();
        batch.IsBatch = true; // 让发送侧把 SendMessageRequestHeader.batch 置 true
        return batch;
    }
}

/// <summary>对应 org.apache.rocketmq.common.message.MessageClientIDSetter。
/// 生成/读取消息属性里的客户端唯一 ID（UNIQ_KEY，32 位十六进制）。</summary>
public static class MessageClientIDSetter
{
    /// <summary>UNIQ_KEY 缺失时写入一个 32 位十六进制唯一 ID（幂等）。</summary>
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
