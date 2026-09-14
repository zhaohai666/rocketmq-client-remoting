// 生产者（对应 org.apache.rocketmq.client.producer.DefaultMQProducer /
// TransactionMQProducer 与 Python client/producer.py）。
//
// 能力覆盖：同步发送（轮询选队列 / 定点发送）、按选择器发送（顺序消息）、
// 异步发送、单向发送、批量发送、事务消息（简化单阶段）、按 Key 查询、
// offset 查询、建 topic。
//
// 与 C++ producer.cpp 逐函数对齐：重试次数、队列选择、压缩判断阈值 4096、
// 超时与异常映射都是协议行为，下面的中文注释一并保留。
using System.Globalization;
using System.Threading;
using RocketMQ.Common;
using RocketMQ.Remoting;

namespace RocketMQ.Client;

/// <summary>
/// 对应 org.apache.rocketmq.client.producer.DefaultMQProducer。
/// 与 C++ DefaultMQProducer 逐字段、逐函数对齐。
/// </summary>
public class DefaultMQProducer
{
    // 异步发送线程的递增序号，用于线程命名（对齐 Java 线程工厂 "AsyncSenderThread_" + n 的后缀）。
    // 进程级递增，与 Java 的 ThreadFactoryImpl 计数器语义一致（C++ 用匿名命名空间里的
    // nextAsyncSenderSeq()，这里用进程级静态字段 + Interlocked 实现）。
    private static int _nextAsyncSenderSeq;

    private readonly object _lock = new();
    private MQClientInstance? _mqClient;
    private bool _started;

    private string _producerGroup;
    private string _instanceName = "DEFAULT";
    private string _clientId = string.Empty;
    private string _createTopicKey = MixAll.DefaultTopic;
    private int _defaultTopicQueueNums = MixAll.DefaultTopicQueueNums;
    private int _sendMsgTimeout = 3000;
    private int _retryTimesWhenSendFailed = 2;
    private int _maxMessageSize = 1024 * 1024 * 4;
    // 压缩配置，默认值与 Java DefaultMQProducer 一致
    private int _compressMsgBodyOverHowmuch = 1024 * 4;
    private int _compressLevel = 5;
    private int _compressType = CompressionType.ZLIB;
    private List<string> _nameServerAddrs = new();
    // 异步发送线程句柄，shutdown 时统一 join 回收
    private readonly List<Thread> _asyncThreads = new();

    public DefaultMQProducer(string producerGroup = MixAll.DefaultProducerGroup)
    {
        if (UtilAll.IsBlank(producerGroup))
        {
            throw new MQClientException("producerGroup is empty");
        }

        _producerGroup = producerGroup;
    }

    /// <summary>析构：与 C++ ~DefaultMQProducer 一致，释放时兜底 shutdown。</summary>
    ~DefaultMQProducer()
    {
        try
        {
            Shutdown();
        }
        catch (Exception)
        {
            // 析构不抛
        }
    }

    // ---------------- 配置 ----------------
    // 分号分隔的 nameServer 地址串，如 "127.0.0.1:9876"
    public string NamesrvAddr
    {
        get
        {
            var outStr = new System.Text.StringBuilder();
            for (int i = 0; i < _nameServerAddrs.Count; ++i)
            {
                if (i > 0) outStr.Append(';');
                outStr.Append(_nameServerAddrs[i]);
            }

            return outStr.ToString();
        }
        set => _nameServerAddrs = SplitSemicolon(value);
    }

    public List<string> NameServerAddresses
    {
        get => _nameServerAddrs;
        set => _nameServerAddrs = value ?? new List<string>();
    }

    public string InstanceName
    {
        get => _instanceName;
        set => _instanceName = value;
    }

    public int SendMsgTimeout
    {
        get => _sendMsgTimeout;
        set => _sendMsgTimeout = value;
    }

    public int RetryTimesWhenSendFailed
    {
        get => _retryTimesWhenSendFailed;
        set => _retryTimesWhenSendFailed = value;
    }

    public int MaxMessageSize
    {
        get => _maxMessageSize;
        set => _maxMessageSize = value;
    }

    public int DefaultTopicQueueNums
    {
        get => _defaultTopicQueueNums;
        set => _defaultTopicQueueNums = value;
    }

    public string CreateTopicKey
    {
        get => _createTopicKey;
        set => _createTopicKey = value;
    }

    public string ProducerGroup
    {
        get => _producerGroup;
        set
        {
            if (_started)
            {
                throw new MQClientException("producerGroup cannot be changed after startup");
            }

            _producerGroup = value;
        }
    }

    // 压缩配置（对应 Java DefaultMQProducer 同名属性）
    // body 长度 >= 该阈值时自动压缩（默认 4096，与 Java 一致）；批量消息永不压缩。
    public int CompressMsgBodyOverHowmuch
    {
        get => _compressMsgBodyOverHowmuch;
        set => _compressMsgBodyOverHowmuch = value;
    }

    // 压缩级别，仅 ZLIB 有意义（Java 默认 5）
    public int CompressLevel
    {
        get => _compressLevel;
        set => _compressLevel = value;
    }

    // 压缩算法：CompressionType::ZLIB / LZ4 / ZSTD（Java 默认 ZLIB）
    public int CompressType
    {
        get => _compressType;
        set => _compressType = value;
    }

    public string ClientId => _clientId;

    public bool IsStarted => _started;

    // ---------------- 生命周期 ----------------

    public void Start()
    {
        lock (_lock)
        {
            if (_started)
            {
                return;
            }

            if (_nameServerAddrs.Count == 0)
            {
                throw new MQClientException("name server address is not set");
            }

            if (string.IsNullOrEmpty(_clientId))
            {
                _clientId = ClientIds.Build(_instanceName);
            }

            _mqClient = new MQClientInstance(_clientId, _nameServerAddrs);
            _mqClient.Start();
            _started = true;
            ClientLog.Info("DefaultMQProducer[" + _producerGroup + "] started, clientId=" + _clientId);
        }
    }

    public void Shutdown()
    {
        List<Thread> threads;
        lock (_lock)
        {
            if (!_started)
            {
                return;
            }

            _started = false;
            threads = new List<Thread>(_asyncThreads);
            _asyncThreads.Clear();
        }

        // 先回收异步线程（它们内部持有 mqClient_ 引用），再关客户端
        foreach (Thread t in threads)
        {
            try
            {
                if (t.IsAlive) t.Join();
            }
            catch (Exception)
            {
                // 忽略 join 过程中的异常（如线程尚未真正启动）
            }
        }

        if (_mqClient is not null)
        {
            _mqClient.Shutdown();
        }
    }

    /// <summary>返回底层客户端实例（对应 C++ MQClientInstance&amp; client()）。</summary>
    public MQClientInstance Client() => GetClient();

    private MQClientInstance GetClient()
    {
        if (!_started || _mqClient is null)
        {
            throw new MQClientException("producer not started, call start() first");
        }

        return _mqClient;
    }

    // ---------------- 校验 ----------------

    private void CheckMessage(Message msg)
    {
        if (string.IsNullOrEmpty(msg.Topic))
        {
            throw new MQClientException("message topic is empty");
        }

        if (msg.Body.Length > _maxMessageSize)
        {
            throw new MQClientException("message body size " + msg.Body.Length.ToString(CultureInfo.InvariantCulture)
                                        + " exceeds maxMessageSize "
                                        + _maxMessageSize.ToString(CultureInfo.InvariantCulture));
        }
    }

    // 对应 Java DefaultMQProducerImpl.tryToCompressMessage + sendKernelImpl 的 sysFlag 组装：
    // 满足阈值且非批量时**就地压缩 msg.body**，返回应下发的 sysFlag
    // （COMPRESSED_FLAG | 压缩类型位）；不压缩时返回 0。
    private int PrepareForSend(Message msg)
    {
        // 批量消息（MessageBatch）**永不压缩**
        if (msg.IsBatch)
        {
            return 0;
        }

        // body 长度 >= compressMsgBodyOverHowmuch（默认 4096）才压缩
        if (msg.Body.Length < _compressMsgBodyOverHowmuch)
        {
            return 0;
        }

        byte[] compressed;
        try
        {
            compressed = CompressorFactory.Compress(msg.Body, _compressType, _compressLevel);
        }
        catch (Exception e)
        {
            // 压缩失败按 Java 的做法**降级为不压缩**并记日志，而不是让发送失败
            ClientLog.Warn("tryToCompressMessage failed, send uncompressed: " + e.Message);
            return 0;
        }

        // 压缩后不比较体积（Java 也不比较：即使压完更大也照发）
        if (compressed.Length == 0 && msg.Body.Length != 0)
        {
            return 0;
        }

        // 压缩成功：就地替换 body，并打上压缩标志 + 压缩类型位
        msg.Body = compressed;
        int sysFlag = MessageSysFlag.CompressedFlag;
        sysFlag |= CompressionType.GetCompressionFlag(_compressType);
        return sysFlag;
    }

    // C++ 里 Message 是值类型，send 前会拷一份再压缩；C# 的 Message 是引用类型，
    // 这里显式克隆，避免压缩就地改写调用方的原始消息 body。
    private static Message CloneMessage(Message src)
    {
        var copy = new Message
        {
            Topic = src.Topic,
            Flag = src.Flag,
            Properties = new PropertyMap(src.Properties),
            HasBody = src.HasBody,
            TransactionId = src.TransactionId,
            IsBatch = src.IsBatch,
        };
        copy.Body = src.Body;
        return copy;
    }

    // ---------------- 同步发送 ----------------

    // 不指定队列：轮询选择，失败按 retryTimesWhenSendFailed 重试
    public SendResult Send(Message msg, int timeoutMillis = -1)
    {
        MQClientInstance c = GetClient();
        int timeout = timeoutMillis >= 0 ? timeoutMillis : _sendMsgTimeout;
        CheckMessage(msg);
        Message outbound = CloneMessage(msg);
        int sysFlag = PrepareForSend(outbound);

        string lastError = string.Empty;
        for (int attempt = 0; attempt <= _retryTimesWhenSendFailed; ++attempt)
        {
            try
            {
                TopicPublishInfo publish = c.GetTopicPublishInfo(outbound.Topic);
                MessageQueue selected = publish.SelectOneMessageQueue();
                return c.SendMessage(_producerGroup, outbound, selected, timeout, sysFlag);
            }
            catch (MQClientException e)
            {
                lastError = e.Message;
            }
            catch (MQBrokerException e)
            {
                lastError = e.Message;
            }
            catch (RemotingException e)
            {
                lastError = e.Message;
            }
        }

        throw new MQClientException("send failed after "
                                    + (_retryTimesWhenSendFailed + 1).ToString(CultureInfo.InvariantCulture)
                                    + " attempts, last error: " + lastError);
    }

    // 定点发送到指定队列
    public SendResult Send(Message msg, MessageQueue mq, int timeoutMillis = -1)
    {
        MQClientInstance c = GetClient();
        int timeout = timeoutMillis >= 0 ? timeoutMillis : _sendMsgTimeout;
        CheckMessage(msg);
        Message outbound = CloneMessage(msg);
        int sysFlag = PrepareForSend(outbound);
        return c.SendMessage(_producerGroup, outbound, mq, timeout, sysFlag);
    }

    // 按选择器发送（顺序消息：同一 arg 落到同一队列）
    public SendResult SendBySelector(Message msg, IMessageQueueSelector selector,
        string arg, int timeoutMillis = -1)
    {
        MQClientInstance c = GetClient();
        int timeout = timeoutMillis >= 0 ? timeoutMillis : _sendMsgTimeout;
        CheckMessage(msg);
        TopicPublishInfo publish = c.GetTopicPublishInfo(msg.Topic);
        MessageQueue selected = selector.Select(publish.MsgQueueList, msg, arg);
        // 选择器用的是原始消息（topic/业务字段），压缩只影响 body
        Message outbound = CloneMessage(msg);
        int sysFlag = PrepareForSend(outbound);
        return c.SendMessage(_producerGroup, outbound, selected, timeout, sysFlag);
    }

    // ---------------- 异步 / 单向 ----------------

    // 后台线程执行发送并回调；调用方可安全释放 callback。
    public void SendAsync(Message msg, ISendCallback callback, int timeoutMillis = -1)
    {
        // 先确认已启动（与 Python 一致：未启动立即抛，而不是在后台线程里静默失败）
        _ = GetClient();
        int timeout = timeoutMillis >= 0 ? timeoutMillis : _sendMsgTimeout;
        Message captured = CloneMessage(msg);
        ISendCallback? cb = callback;

        var th = new Thread(() =>
        {
            // 线程名对齐 Java 的 ThreadFactoryImpl("AsyncSenderThread_")
            ClientLog.SetThreadName("AsyncSenderThread_"
                                    + (Interlocked.Increment(ref _nextAsyncSenderSeq) - 1)
                                        .ToString(CultureInfo.InvariantCulture));
            try
            {
                SendResult result = Send(captured, timeout);
                cb?.OnSuccess(result);
            }
            catch (Exception e)
            {
                cb?.OnException(e.Message);
            }
        });
        // 设为后台线程，避免宿主进程退出时因未显式 shutdown 而卡住
        th.IsBackground = true;
        th.Start();
        lock (_lock)
        {
            _asyncThreads.Add(th);
        }
    }

    public void SendOneway(Message msg)
    {
        MQClientInstance c = GetClient();
        CheckMessage(msg);
        TopicPublishInfo publish = c.GetTopicPublishInfo(msg.Topic);
        MessageQueue selected = publish.SelectOneMessageQueue();
        Message outbound = CloneMessage(msg);
        int sysFlag = PrepareForSend(outbound);
        c.SendMessageOneway(_producerGroup, outbound, selected, _sendMsgTimeout, sysFlag);
    }

    // ---------------- 批量 ----------------

    public SendResult SendBatch(List<Message> msgs, int timeoutMillis = -1)
    {
        MQClientInstance c = GetClient();
        int timeout = timeoutMillis >= 0 ? timeoutMillis : _sendMsgTimeout;
        if (msgs is null || msgs.Count == 0)
        {
            throw new MQClientException("message list is empty");
        }

        MessageBatch batch = MessageBatch.GenerateFromList(msgs);
        CheckMessage(batch);
        TopicPublishInfo publish = c.GetTopicPublishInfo(batch.Topic);
        MessageQueue selected = publish.SelectOneMessageQueue();
        // MessageBatch 的 isBatch 为 true，prepareForSend 会直接返回 0（不压缩）
        Message outbound = CloneMessage(batch);
        int sysFlag = PrepareForSend(outbound);
        return c.SendMessage(_producerGroup, outbound, selected, timeout, sysFlag);
    }

    // ---------------- 事务消息 ----------------
    // 注意：与 Python 参考实现一致，为**简化单阶段**实现 —— 发送普通消息后执行
    // 本地事务并回填状态，未实现 broker 半消息 / 回查 / END_TRANSACTION 两阶段提交。
    public TransactionSendResult SendMessageInTransaction(Message msg, ITransactionListener listener,
        string arg = "")
    {
        MQClientInstance c = GetClient();
        CheckMessage(msg);
        TopicPublishInfo publish = c.GetTopicPublishInfo(msg.Topic);
        MessageQueue selected = publish.SelectOneMessageQueue();

        // 简化单阶段：先发消息，再执行本地事务，按结果回填状态。
        // 未实现 broker 半消息 + 回查 + END_TRANSACTION 两阶段提交。
        // 压缩与普通发送一致（Java 的事务发送同样走 sendKernelImpl）。
        Message outbound = CloneMessage(msg);
        int sysFlag = PrepareForSend(outbound);
        SendResult sendResult = c.SendMessage(_producerGroup, outbound, selected, _sendMsgTimeout, sysFlag);

        var tsr = new TransactionSendResult
        {
            SendStatus = sendResult.SendStatus,
            MsgId = sendResult.MsgId,
            OffsetMsgId = sendResult.OffsetMsgId,
            MessageQueue = sendResult.MessageQueue,
            QueueOffset = sendResult.QueueOffset,
            TransactionId = sendResult.TransactionId,
            RegionId = sendResult.RegionId,
        };
        tsr.LocalTransactionState = listener.ExecuteLocalTransaction(msg, arg);
        return tsr;
    }

    // ---------------- 查询 / 管理 ----------------

    public List<MessageExt> QueryMessage(string topic, string key, int maxNum, long beginTimestamp,
        long endTimestamp)
    {
        MQClientInstance c = GetClient();
        bool found = c.QueryMessage(topic, key, maxNum, beginTimestamp, endTimestamp, out byte[] body, 15000);
        if (!found || body.Length == 0)
        {
            return new List<MessageExt>();
        }

        return MessageDecoder.DecodeMessages(body);
    }

    public List<MessageQueue> FetchPublishMessageQueues(string topic)
    {
        MQClientInstance c = GetClient();
        TopicPublishInfo publish = c.GetTopicPublishInfo(topic);
        return publish.MsgQueueList;
    }

    public void CreateTopic(string key, string newTopic, int queueNum = 4)
    {
        MQClientInstance c = GetClient();
        const int perm = 6; // PERM_READ | PERM_WRITE
        // C++ 的 createTopic 忽略 key 形参，统一走默认 topic（MixAll.DefaultTopic）建路由
        _ = key;
        c.CreateTopicInRoute(newTopic, queueNum, queueNum, perm);
    }

    public long SearchOffset(MessageQueue mq, long timestamp) =>
        GetClient().SearchOffsetByTimestamp(mq, timestamp);

    public long MaxOffset(MessageQueue mq) => GetClient().GetMaxOffset(mq);

    public long MinOffset(MessageQueue mq) => GetClient().GetMinOffset(mq);

    // ---------------- 内部工具：分号拆分（对齐 C++ splitSemicolon）----------------

    private static List<string> SplitSemicolon(string addr)
    {
        var outList = new List<string>();
        if (string.IsNullOrEmpty(addr))
        {
            return outList;
        }

        int start = 0;
        while (start <= addr.Length)
        {
            int pos = addr.IndexOf(';', start);
            string piece = pos < 0 ? addr[start..] : addr[start..pos];
            string t = piece.Trim();
            if (t.Length > 0) outList.Add(t);
            if (pos < 0) break;
            start = pos + 1;
        }

        return outList;
    }
}

// 事务生产者（对应 Java TransactionMQProducer）：可预设 TransactionListener
public class TransactionMQProducer : DefaultMQProducer
{
    public TransactionMQProducer(string producerGroup = MixAll.DefaultProducerGroup)
        : base(producerGroup)
    {
    }

    public ITransactionListener? TransactionListener { get; set; }

    // 简化单阶段：使用预设 listener 执行本地事务。
    public TransactionSendResult SendMessageInTransaction(Message msg, string arg = "")
    {
        if (TransactionListener is null)
        {
            throw new MQClientException("transaction listener is not set");
        }

        return SendMessageInTransaction(msg, TransactionListener, arg);
    }
}
