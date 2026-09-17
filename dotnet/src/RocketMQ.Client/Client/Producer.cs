// 生产者（对应 org.apache.rocketmq.client.producer.DefaultMQProducer /
// TransactionMQProducer 与 Python client/producer.py）。
//
// 能力覆盖：同步发送（轮询选队列 / 定点发送）、按选择器发送（顺序消息）、
// 异步发送、单向发送、批量发送、事务消息（对齐 Java 的两阶段：半消息 + 回查）、
// 按 Key 查询、offset 查询、建 topic。
//
// 与 C++ producer.cpp 逐函数对齐：重试次数、队列选择、压缩判断阈值 4096、
// 超时与异常映射都是协议行为，下面的中文注释一并保留。
using System.Globalization;
using System.Threading;
using RocketMQ.Common;
using RocketMQ.Remoting;
using RocketMQ.Remoting.Protocol;

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
    // 命名空间（多租户隔离）：非空前，发送时把 topic 拼成 "ns%topic" 发给 broker。
    // 默认空 = 不加命名空间（与裸集群兼容，不破坏现有行为）。
    private string _namespace = string.Empty;
    // ACL 钩子，Start() 时绑定到 MQClientInstance 的传输层
    private IRpcHook? _rpcHook;
    private string _createTopicKey = MixAll.DefaultTopic;
    private int _defaultTopicQueueNums = MixAll.DefaultTopicQueueNums;
    private int _sendMsgTimeout = 3000;
    private int _retryTimesWhenSendFailed = 2;
    // 发送延迟故障规避（默认关闭，对应 Java MQFaultStrategy 的默认开关）
    private readonly MQFaultStrategy _mqFaultStrategy = new(false);
    private int _maxMessageSize = 1024 * 1024 * 4;
    // 压缩配置，默认值与 Java DefaultMQProducer 一致
    private int _compressMsgBodyOverHowmuch = 1024 * 4;
    private int _compressLevel = 5;
    private int _compressType = CompressionType.ZLIB;
    // Request-Reply 默认等待应答超时（ms），与 Java/Python 默认 3000 对齐。
    private int _requestTimeout = 3000;
    private List<string> _nameServerAddrs = new();
    // ---------------- 消息轨迹（对应 Java ClientConfig / DefaultMQProducer）----------------
    // 开启后 Start() 会建 AsyncTraceDispatcher 并注册 Send/EndTransaction 钩子；
    // 内部轨迹生产者自身保持关闭（EnableTrace=false），否则无限递归。
    private bool _enableTrace;
    private string _traceTopic = MixAll.TraceTopic;
    private int _traceMsgBatchNum = 10;
    private readonly List<ISendMessageHook> _sendMessageHooks = new();
    private readonly List<IEndTransactionHook> _endTransactionHooks = new();
    private AsyncTraceDispatcher? _traceDispatcher;
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

    // ---------------- 故障规避（对应 Java sendLatencyFaultEnable，默认关闭）----------------
    // 开启后发送选队列会按 broker 延迟/隔离状态过滤（MQFaultStrategy）；发送结果回写
    // 容错表：成功记实测延迟（超阈值隔离该 broker 一段时间），异常记隔离 10000ms 档。
    public bool SendLatencyFaultEnable
    {
        get => _mqFaultStrategy.IsSendLatencyFaultEnable();
        set => _mqFaultStrategy.SetSendLatencyFaultEnable(value);
    }

    public MQFaultStrategy MqFaultStrategy => _mqFaultStrategy;

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

    // 命名空间（对应 Java DefaultMQProducer.setNamespace）：非空时发送前把 topic
    // 包装成 "ns%topic" 再发给 broker（%RETRY%/%DLQ% 前缀除外，系统资源不包装）。
    public string Namespace
    {
        get => _namespace;
        set => _namespace = value ?? string.Empty;
    }

    // ---------------- ACL 鉴权（对应 Java DefaultMQProducer(group, rpcHook)）----------------
    // 必须在 Start() 之前调用：钩子在 Start() 里绑定到 MQClientInstance（同一 clientId
    // 复用实例时以先注册者为准，与 Java 的绑定时机一致）。
    public void SetRpcHook(IRpcHook hook) => _rpcHook = hook;

    /// <summary>便捷入口：用 accessKey/secretKey（可选 securityToken）构造 AclClientRPCHook。</summary>
    public void SetCredentials(string accessKey, string secretKey, string securityToken = "")
        => _rpcHook = new AclClientRPCHook(new SessionCredentials(accessKey, secretKey, securityToken));

    // ---------------- 消息轨迹配置（对应 Java ClientConfig 的 trace 相关属性）----------------
    // 开启后 Start() 会建 AsyncTraceDispatcher 并注册 Send/EndTransaction 轨迹钩子；
    // 内部分发器用的轨迹生产者自身保持 EnableTrace=false（见 AsyncTraceDispatcher），否则无限递归。
    public bool EnableTrace
    {
        get => _enableTrace;
        set => _enableTrace = value;
    }

    /// <summary>自定义轨迹 topic（Java ClientConfig.setTraceTopic）；空则回落系统默认 RMQ_SYS_TRACE_TOPIC。</summary>
    public string TraceTopic
    {
        get => _traceTopic;
        set => _traceTopic = string.IsNullOrEmpty(value) ? MixAll.TraceTopic : value;
    }

    public int TraceMsgBatchNum
    {
        get => _traceMsgBatchNum;
        set => _traceMsgBatchNum = value;
    }

    /// <summary>注册发送钩子（对应 Java DefaultMQProducerImpl.registerSendMessageHook）。</summary>
    public void RegisterSendMessageHook(ISendMessageHook hook)
    {
        if (hook is not null)
        {
            _sendMessageHooks.Add(hook);
        }
    }

    public bool HasSendMessageHook() => _sendMessageHooks.Count > 0;

    /// <summary>注册事务收尾钩子（对应 Java registerEndTransactionHook）。</summary>
    public void RegisterEndTransactionHook(IEndTransactionHook hook)
    {
        if (hook is not null)
        {
            _endTransactionHooks.Add(hook);
        }
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

    // Request-Reply：等待应答的超时（默认 3000ms，与 Java/Python 对齐）。
    // 任何 <= 0 的值都回退到默认 3000，避免把发送路径的超时设成 0。
    public int RequestTimeout
    {
        get => _requestTimeout;
        set => _requestTimeout = value > 0 ? value : 3000;
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

            // ACL 鉴权钩子：必须在任何请求发出之前绑定（路由拉取、心跳都会带签名）。
            if (_rpcHook is not null && !_mqClient.RegisterRpcHook(_rpcHook))
            {
                ClientLog.Warn("producer rpc hook ignored: MQClientInstance already has one (clientId="
                    + _clientId + ")");
            }

            // 注册 broker 主动请求处理器：事务回查 CHECK_TRANSACTION_STATE(39)。
            // 不注册的话回查会被传输层当成"未知请求"丢弃，事务消息永远停留在 Unknown。
            _mqClient.RemotingClient.RegisterProcessor(RequestCode.CheckTransactionState,
                CheckTransactionState);

            _started = true;
            ClientLog.Info("DefaultMQProducer[" + _producerGroup + "] started, clientId=" + _clientId);

            // 心跳线程：周期性向 broker 注册 ProducerData。broker 的事务回查正是通过
            // 这一步登记的 channel 反向联系生产者的；生产者不发心跳时 Commit/Rollback
            // 仍能成功（客户端主动 END_TRANSACTION），但 Unknown 的半消息**永远不被回查**。
            _heartbeatRunning = true;
            _heartbeatThread = new Thread(HeartbeatLoop)
            { IsBackground = true, Name = "ProducerHeartbeatThread" };
            _heartbeatThread.Start();
        }

        // 轨迹分发器在锁外启动：它会拉起内部生产者（各自加锁），锁内启动容易形成锁嵌套。
        StartTraceDispatcher();
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

            // 先停心跳线程（它内部持有 mqClient 引用）
            _heartbeatRunning = false;
            if (_heartbeatThread is { IsAlive: true })
            {
                _heartbeatThread.Join(2000);
            }

            lock (_txThreadsLock)
            {
                threads.AddRange(_txThreads);
                _txThreads.Clear();
            }
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

        // 顺序对齐 Java DefaultMQProducer.shutdown()：先关本生产者，再 flush 并关轨迹分发器
        //（分发器用的是**自己的**内部生产者，与本客户端实例无关，所以关掉了照样能发完）
        if (_traceDispatcher is not null)
        {
            try
            {
                _traceDispatcher.Shutdown();
            }
            catch (Exception e)
            {
                ClientLog.Warn("trace dispatcher shutdown failed: " + e.Message);
            }
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

    // ---------------- 钩子执行（对应 Java executeSendMessageHookBefore/After、executeEndTransactionHook）----------------
    // 钩子抛出的异常一律吞掉并记 warn（Java DefaultMQProducerImpl:1159）：轨迹出错绝不能影响正常收发。

    private void ExecuteSendMessageHookBefore(SendMessageContext context)
    {
        foreach (ISendMessageHook hook in _sendMessageHooks)
        {
            try
            {
                hook.SendMessageBefore(context);
            }
            catch (Exception e)
            {
                ClientLog.Warn("failed to executeSendMessageHookBefore: " + e.Message);
            }
        }
    }

    private void ExecuteSendMessageHookAfter(SendMessageContext context)
    {
        foreach (ISendMessageHook hook in _sendMessageHooks)
        {
            try
            {
                hook.SendMessageAfter(context);
            }
            catch (Exception e)
            {
                ClientLog.Warn("failed to executeSendMessageHookAfter: " + e.Message);
            }
        }
    }

    private void ExecuteEndTransactionHook(EndTransactionContext context)
    {
        foreach (IEndTransactionHook hook in _endTransactionHooks)
        {
            try
            {
                hook.EndTransaction(context);
            }
            catch (Exception e)
            {
                ClientLog.Warn("failed to executeEndTransactionHook: " + e.Message);
            }
        }
    }

    /// <summary>构造 SendMessageContext（对齐 Java DefaultMQProducerImpl:969-989）。
    /// msgType 判定顺序照抄：TRAN_MSG=true → Trans；带任何延迟类属性 → Delay；否则 Normal。
    /// ⚠ .NET 的 <c>GetProperty</c> 对缺失键返回**空串**而非 null（Java/Python 返回 null），
    /// 所以延迟属性必须用 <c>Properties.ContainsKey</c> 判断，不能靠值判空。</summary>
    private SendMessageContext BuildSendContext(Message msg, MessageQueue mq, string brokerAddr)
    {
        var context = new SendMessageContext
        {
            Producer = this,
            ProducerGroup = _producerGroup,
            Message = msg,
            Mq = mq,
            BrokerAddr = brokerAddr,
            Namespace = _namespace,
        };
        if (msg.GetProperty(MessageConst.PropertyTransactionPrepared) == "true")
        {
            context.MsgType = TraceMessageType.Trans;
        }

        foreach (string key in new[]
                 {
                     "__STARTDELIVERTIME", MessageConst.PropertyDelayTimeLevel,
                     "TIMER_DELIVER_MS", "TIMER_DELAY_SEC", "TIMER_DELAY_MS",
                 })
        {
            if (msg.Properties.ContainsKey(key))
            {
                context.MsgType = TraceMessageType.Delay;
                break;
            }
        }

        return context;
    }

    /// <summary>对应 Java DefaultMQProducerImpl.tryToFindTopicPublishInfo：
    /// 先拉**真实**路由；只有确实拉不到（新 topic 尚未在 NameServer 注册）时，才按 Java 的做法
    /// 用默认 topic（TBW102）为该 topic 合成发布信息 —— 否则新 topic 的**首条**消息没有队列可选，
    /// 直接抛 "Can not find Message Queue"。消费侧**不做**这个兜底（与 Java 一致）。</summary>
    private TopicPublishInfo TryToFindTopicPublishInfo(MQClientInstance c, string topic)
    {
        try
        {
            return c.GetTopicPublishInfo(topic);
        }
        catch (MQClientException)
        {
            return c.GetTopicPublishInfo(topic, true);
        }
    }

    /// <summary>带 before/after 钩子的同步发送（对应 Java sendKernelImpl + sendDefaultImpl 的钩子点）。
    /// 钩子只在**真正发起请求的那一次**执行（Java 重试时每轮都重建 context）。
    /// 无钩子时直通，不引入任何额外开销。</summary>
    private SendResult SendWithHooks(MQClientInstance c, Message msg, MessageQueue mq,
        int timeout, int sysFlag)
    {
        if (_sendMessageHooks.Count == 0)
        {
            return c.SendMessage(_producerGroup, msg, mq, timeout, sysFlag);
        }

        string brokerAddr = string.Empty;
        try
        {
            brokerAddr = c.BrokerAddrForMq(mq) ?? string.Empty;
        }
        catch (Exception)
        {
            // 路由表里查不到 broker 时不影响发送本身，钩子照常跑（brokerAddr 为空）
        }

        SendMessageContext context = BuildSendContext(msg, mq, brokerAddr);
        ExecuteSendMessageHookBefore(context);
        SendResult result;
        try
        {
            result = c.SendMessage(_producerGroup, msg, mq, timeout, sysFlag);
        }
        catch (Exception e)
        {
            context.Exception = e;
            ExecuteSendMessageHookAfter(context);
            throw;
        }

        context.SendResult = result;
        ExecuteSendMessageHookAfter(context);
        return result;
    }

    /// <summary>对应 Java DefaultMQProducer.start():380-405：enableTrace=true 时建分发器
    /// （Type=Produce）并注册 SendMessageTraceHook / EndTransactionTraceHook，随后 start 它。
    /// 任何异常都只记日志 —— 轨迹挂了不能影响正常发送。</summary>
    private void StartTraceDispatcher()
    {
        if (_enableTrace)
        {
            try
            {
                var dispatcher = new AsyncTraceDispatcher(_producerGroup,
                    TraceDispatcherType.Produce, _traceMsgBatchNum, _traceTopic, _rpcHook);
                dispatcher.SetHostProducer(this);
                _traceDispatcher = dispatcher;
                RegisterSendMessageHook(new SendMessageTraceHook(dispatcher));
                RegisterEndTransactionHook(new EndTransactionTraceHook(dispatcher));
            }
            catch (Exception e)
            {
                ClientLog.Error("system mqtrace hook init failed, maybe can't send msg trace data: "
                    + e.Message);
            }
        }

        if (_traceDispatcher is not null)
        {
            try
            {
                _traceDispatcher.Start(_nameServerAddrs, AccessChannel.Local);
            }
            catch (Exception e)
            {
                ClientLog.Warn("trace dispatcher start failed: " + e.Message);
            }
        }
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

    // 发送前给 topic 套上 namespace 前缀（对应 Java DefaultMQProducer.withNamespace）。
    // namespace 为空时仅克隆（不改变 topic）。
    private Message WithNamespace(Message msg)
    {
        Message outMsg = CloneMessage(msg);
        if (_namespace.Length != 0)
        {
            outMsg.Topic = NamespaceUtil.WrapNamespace(_namespace, msg.Topic);
        }

        return outMsg;
    }

    // ---------------- 同步发送 ----------------

    // 不指定队列：轮询选择，失败按 retryTimesWhenSendFailed 重试
    public SendResult Send(Message msg, int timeoutMillis = -1)
    {
        MQClientInstance c = GetClient();
        int timeout = timeoutMillis >= 0 ? timeoutMillis : _sendMsgTimeout;
        CheckMessage(msg);
        Message outbound = WithNamespace(msg);
        int sysFlag = PrepareForSend(outbound);

        string lastError = string.Empty;
        string lastBrokerName = "";
        for (int attempt = 0; attempt <= _retryTimesWhenSendFailed; ++attempt)
        {
            try
            {
                TopicPublishInfo publish = TryToFindTopicPublishInfo(c, outbound.Topic);
                // 故障规避：开启时按 broker 延迟/隔离状态选队列（Java MQFaultStrategy）；
                // 关闭时退化为普通轮询（策略内部判断）。
                MessageQueue selected = _mqFaultStrategy.SelectOneMessageQueue(publish, lastBrokerName);
                lastBrokerName = selected.BrokerName;
                long sendBegin = UtilAll.CurrentTimeMillis();
                SendResult result;
                try
                {
                    result = SendWithHooks(c, outbound, selected, timeout, sysFlag);
                }
                catch (Exception)
                {
                    // 发送异常：按隔离档位记录（latency 固定 10000ms），broker 进入隔离期
                    _mqFaultStrategy.UpdateFaultItem(selected.BrokerName, 0.0, true, false);
                    throw;
                }
                // 记录发送延迟；超出阈值会把该 broker 隔离一段时间
                _mqFaultStrategy.UpdateFaultItem(selected.BrokerName,
                                                 UtilAll.CurrentTimeMillis() - sendBegin, false, true);
                return result;
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
        Message outbound = WithNamespace(msg);
        int sysFlag = PrepareForSend(outbound);
        return SendWithHooks(c, outbound, mq, timeout, sysFlag);
    }

    // 按选择器发送（顺序消息：同一 arg 落到同一队列）
    public SendResult SendBySelector(Message msg, IMessageQueueSelector selector,
        string arg, int timeoutMillis = -1)
    {
        MQClientInstance c = GetClient();
        int timeout = timeoutMillis >= 0 ? timeoutMillis : _sendMsgTimeout;
        CheckMessage(msg);
        Message outbound = WithNamespace(msg);
        TopicPublishInfo publish = TryToFindTopicPublishInfo(c, outbound.Topic);
        MessageQueue selected = selector.Select(publish.MsgQueueList, msg, arg);
        // 选择器用的是原始消息（topic/业务字段），压缩只影响 body
        int sysFlag = PrepareForSend(outbound);
        return SendWithHooks(c, outbound, selected, timeout, sysFlag);
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
        Message outbound = WithNamespace(msg);
        TopicPublishInfo publish = TryToFindTopicPublishInfo(c, outbound.Topic);
        MessageQueue selected = publish.SelectOneMessageQueue();
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
        Message outbound = CloneMessage(batch);
        // 对应 Java MessageBatch.generateFromList + withNamespace：批量 topic 也要套命名空间
        if (_namespace.Length != 0)
        {
            outbound.Topic = NamespaceUtil.WrapNamespace(_namespace, batch.Topic);
        }

        TopicPublishInfo publish = TryToFindTopicPublishInfo(c, outbound.Topic);
        MessageQueue selected = publish.SelectOneMessageQueue();
        // MessageBatch 的 isBatch 为 true，prepareForSend 会直接返回 0（不压缩）
        int sysFlag = PrepareForSend(outbound);
        return SendWithHooks(c, outbound, selected, timeout, sysFlag);
    }

    // ---------------- Request-Reply（5.x）----------------

    /// <summary>
    /// Request-Reply（5.x）：发一条请求消息并**同步等应答**，返回应答消息。
    ///
    /// 对应 Java DefaultMQProducerImpl#request(msg, mq, timeout)（:1738-1767）。
    /// 请求方做三件事：
    ///   1. 给请求消息写上 CORRELATION_ID（随机 UUID）、REPLY_TO_CLIENT（本客户端 clientId）、
    ///      TTL（= timeout）；后两个是 broker 找回本连接、应答方原样带回的依据。
    ///   2. 把等待槽按 correlationId 登记到进程级的 RequestFutureHolder。
    ///   3. 发送后阻塞等待；应答由 broker 经 PUSH_REPLY_MESSAGE_TO_CLIENT(326) 推回，
    ///      由 MQClientInstance.ProcessReplyMessage 投递进等待槽。
    ///
    /// 超时抛 RequestTimeoutException（消息已发出但没等到应答）；发送本身失败则抛
    /// MQClientException（带着底层 cause），与 Java 一致。
    ///
    /// REPLY_TO_CLIENT 是 clientId —— broker 要靠它反查 channel，所以本生产者必须先发过
    /// 心跳（start() 已起心跳线程；这里也会补一次，对齐 Java prepareSendRequest 的
    /// sendHeartbeatToAllBrokerWithLock）。
    /// </summary>
    public Message Request(Message msg, int timeoutMillis = -1)
    {
        int timeout = timeoutMillis >= 0 ? timeoutMillis : _requestTimeout;
        MQClientInstance c = GetClient();
        CheckMessage(msg);
        Message outbound = WithNamespace(msg);
        int sysFlag = PrepareForSend(outbound);

        string correlationId = RequestReply.CreateCorrelationId();
        outbound.PutProperty(MessageConst.PropertyCorrelationId, correlationId);
        outbound.PutProperty(MessageConst.PropertyReplyToClient, c.ClientId);
        outbound.PutProperty(MessageConst.PropertyMessageTTL,
            timeout.ToString(CultureInfo.InvariantCulture));

        // 对齐 Java prepareSendRequest：确保路由已知，然后补一次心跳 ——
        // 没在 broker 上登记为 producer，broker 就找不到 channel 把应答推回来。
        long begin = UtilAll.CurrentTimeMillis();
        try
        {
            c.GetTopicPublishInfo(outbound.Topic);
            SendHeartbeatToAllBroker();
        }
        catch (Exception e)
        {
            ClientLog.Debug("request: prepare route/heartbeat failed: " + e.Message);
        }

        var future = new RequestResponseFuture(correlationId, timeout);
        RequestFutureHolder.Instance.PutRequest(correlationId, future);

        try
        {
            long elapsed = UtilAll.CurrentTimeMillis() - begin;
            int remaining = (int)(timeout > elapsed ? timeout - elapsed : 0);

            // 发送失败时把等待槽标成 !sendRequestOk 并立即唤醒（让 _waitRequestResponse 走失败分支）；
            // 协议上无差别 —— 应答由 broker 经**另一条** 326 通道推回，与本次发送的 mode 无关。
            try
            {
                Send(outbound, remaining);
            }
            catch (Exception e)
            {
                future.SendRequestOk = false;
                future.Cause = e;
                future.PutResponseMessage(null);
            }

            return WaitRequestResponse(outbound, timeout, future);
        }
        finally
        {
            RequestFutureHolder.Instance.RemoveRequest(correlationId);
        }
    }

    /// <summary>对应 Java waitResponse：超时/发送失败分别抛不同异常。</summary>
    private static Message WaitRequestResponse(Message msg, int timeout, RequestResponseFuture future)
    {
        long elapsed = UtilAll.CurrentTimeMillis() - future.BeginTimestamp;
        int waitMillis = (int)(timeout > elapsed ? timeout - elapsed : 0);
        Message? response = future.WaitResponseMessage(waitMillis);
        if (response is null)
        {
            if (future.SendRequestOk)
            {
                throw new RequestTimeoutException(
                    "send request message to <" + msg.Topic + "> OK, but wait reply message timeout, "
                    + timeout.ToString(CultureInfo.InvariantCulture) + " ms.");
            }

            throw new MQClientException(
                "send request message to <" + msg.Topic + "> fail", future.Cause);
        }

        return response;
    }

    // ---------------- 事务消息 ----------------
    // 对齐 Java DefaultMQProducerImpl.sendMessageInTransaction 的**两阶段**：
    //   1) 半消息：给 msg 打 TRAN_MSG / PGROUP 属性，sysFlag 置 TRANSACTION_PREPARED_TYPE；
    //   2) 本地事务：仅 SendOk 时执行；Flush* / SlaveNotAvailable -> Rollback；
    //   3) EndTransaction：以 END_TRANSACTION(37, oneway) 告知 broker 提交 / 回滚 / 未知；
    //   4) Unknow 时由 broker 回查 CHECK_TRANSACTION_STATE(39)，回调
    //      listener.CheckLocalTransaction 后再发 END_TRANSACTION(FromTransactionCheck=true)。
    public TransactionSendResult SendMessageInTransaction(Message msg, ITransactionListener listener,
        string arg = "")
    {
        if (listener is null)
        {
            throw new MQClientException("tranExecutor is null");
        }

        // Java ensureNotDelayedForTransactional：事务消息不支持延迟投递。
        // ⚠ .NET 的 DelayTimeLevel 属性有默认值 0（getter 对缺失键返回 0），
        // 必须判 properties 里是否真的设置了该键，不能只看属性值。
        if (msg.Properties.TryGetValue(MessageConst.PropertyDelayTimeLevel, out string? dl)
            && !string.IsNullOrEmpty(dl))
        {
            throw new MQClientException("Transactional messages do not support delayed delivery");
        }

        MQClientInstance c = GetClient();
        CheckMessage(msg);
        Message outbound = WithNamespace(msg);
        TopicPublishInfo publish = TryToFindTopicPublishInfo(c, outbound.Topic);
        MessageQueue selected = publish.SelectOneMessageQueue();

        // 半消息标记：broker 据此把消息写入 RMQ_SYS_TRANS_HALF_TOPIC，等待 END_TRANSACTION
        outbound.PutProperty(MessageConst.PropertyTransactionPrepared, "true");
        outbound.PutProperty(MessageConst.PropertyProducerGroup, _producerGroup);
        _txListener = listener;

        // 压缩与普通发送一致；再叠加事务类型位（Java sendKernelImpl 检测 TRAN_MSG 后置 PREPARED）
        int sysFlag = PrepareForSend(outbound);
        sysFlag = MessageSysFlag.ResetTransactionValue(sysFlag, MessageSysFlag.TransactionPreparedType);

        SendResult sendResult;
        try
        {
            sendResult = SendWithHooks(c, outbound, selected, _sendMsgTimeout, sysFlag);
        }
        catch (Exception e)
        {
            throw new MQClientException("send message Exception: " + e.Message);
        }

        LocalTransactionState state = LocalTransactionState.Unknow;
        string? localExceptionText = null;
        if (sendResult.SendStatus == SendStatus.SendOk)
        {
            if (!string.IsNullOrEmpty(sendResult.TransactionId))
            {
                outbound.PutProperty("__transactionId__", sendResult.TransactionId!);
            }

            string? uniq = outbound.GetProperty(MessageConst.PropertyUniqClientMessageIdKeyidx);
            if (!string.IsNullOrEmpty(uniq))
            {
                outbound.TransactionId = uniq;
            }

            try
            {
                state = listener.ExecuteLocalTransaction(outbound, arg);
            }
            catch (Exception e)
            {
                ClientLog.Error("executeLocalTransactionBranch exception, topic=" + outbound.Topic
                    + ": " + e.Message);
                localExceptionText = e.Message;
                state = LocalTransactionState.Unknow;
            }
        }
        else if (sendResult.SendStatus == SendStatus.FlushDiskTimeout
                 || sendResult.SendStatus == SendStatus.FlushSlaveTimeout
                 || sendResult.SendStatus == SendStatus.SlaveNotAvailable)
        {
            state = LocalTransactionState.RollbackMessage;
        }

        try
        {
            EndTransaction(outbound, sendResult, state, localExceptionText, false, null, null, "");
        }
        catch (Exception e)
        {
            // Java：end broker transaction 失败只 warn，不影响返回结果
            ClientLog.Warn("local transaction execute " + state
                + ", but end broker transaction failed: " + e.Message);
        }

        return new TransactionSendResult
        {
            SendStatus = sendResult.SendStatus,
            MsgId = sendResult.MsgId,
            OffsetMsgId = sendResult.OffsetMsgId,
            MessageQueue = sendResult.MessageQueue,
            QueueOffset = sendResult.QueueOffset,
            TransactionId = sendResult.TransactionId,
            RegionId = sendResult.RegionId,
            LocalTransactionState = state,
        };
    }

    private static int TransactionFlagOf(LocalTransactionState state) => state switch
    {
        LocalTransactionState.CommitMessage => MessageSysFlag.TransactionCommitType,
        LocalTransactionState.RollbackMessage => MessageSysFlag.TransactionRollbackType,
        _ => MessageSysFlag.TransactionNotType,
    };

    // ---------------- 心跳（broker 事务回查依赖它） ----------------

    private void HeartbeatLoop()
    {
        ClientLog.SetThreadName("ProducerHeartbeatThread");
        int intervalMs = _heartbeatIntervalMillis;
        while (_heartbeatRunning)
        {
            try
            {
                SendHeartbeatToAllBroker();
            }
            catch (Exception e)
            {
                ClientLog.Debug("producer heartbeat failed: " + e.Message);
            }

            for (int i = 0; i < intervalMs / 100 && _heartbeatRunning; ++i)
            {
                Thread.Sleep(100);
            }
        }
    }

    private int SendHeartbeatToAllBroker()
    {
        MQClientInstance? c = _mqClient;
        if (c is null)
        {
            return 0;
        }

        List<string> addrs;
        try
        {
            addrs = c.KnownBrokerAddrs();
        }
        catch (Exception e)
        {
            ClientLog.Warn("producer heartbeat: gather brokers failed: " + e.Message);
            return 0;
        }

        if (addrs.Count == 0)
        {
            return 0;
        }

        // 只带 ProducerData：对齐 Java MQClientInstance 里 producerTable 的注册内容。
        // broker 会把该 group 登记到 ProducerManager（事务回查即通过该 channel 反向联系）。
        var hb = new HeartbeatData(_clientId ?? string.Empty)
        {
            HeartbeatFingerprint = 0,  // 走 V1 注册路径，最稳妥
        };
        hb.AddProducerData(new ProducerData(_producerGroup));

        int okCount = 0;
        foreach (string addr in addrs)
        {
            try
            {
                c.SendHeartbeat(addr, hb, 5000);
                ++okCount;
            }
            catch (Exception e)
            {
                ClientLog.Warn("producer heartbeat to " + addr + " failed: " + e.Message);
            }
        }

        return okCount;
    }

    // 心跳线程（对齐 Java MQClientInstance 的定时心跳；间隔默认 30s）
    private Thread? _heartbeatThread;
    private volatile bool _heartbeatRunning;
    private readonly int _heartbeatIntervalMillis = 30000;

    // 最近一次 SendMessageInTransaction 使用的监听器（broker 回查时回调它）。
    // 引用语义：调用方需保证其生命周期覆盖事务回查（与 Java 的 TransactionListener 一致）。
    private ITransactionListener? _txListener;
    private readonly List<Thread> _txThreads = new();
    private readonly object _txThreadsLock = new();

    /// <summary>
    /// 以 END_TRANSACTION(37, oneway) 告知 broker 事务最终状态（对齐 Java endTransaction /
    /// checkTransactionState）。fromCheck=true 表示这是**回查**的收尾，偏移等字段取自
    /// broker 的回查 header（此时 sendResult 不可用）。
    /// </summary>
    private void EndTransaction(Message msg, SendResult sendResult, LocalTransactionState state,
        string? localExceptionText, bool fromCheck,
        CheckTransactionStateRequestHeader? checkHeader, MessageExt? checkMsg, string brokerAddr)
    {
        MQClientInstance c = GetClient();

        var header = new EndTransactionRequestHeader
        {
            ProducerGroup = _producerGroup,
            CommitOrRollback = TransactionFlagOf(state),
            FromTransactionCheck = fromCheck,
        };

        string addr;
        if (fromCheck)
        {
            header.Topic = checkHeader!.Topic;
            header.CommitLogOffset = checkHeader.CommitLogOffset;
            header.TranStateTableOffset = checkHeader.TranStateTableOffset;
            header.TransactionId = checkHeader.TransactionId;
            header.Bname = checkHeader.Bname;
            // Java: uniqueKey = msg 属性 UNIQ_KEY，取不到才用 msgId
            string? uniqueKey = checkMsg?.GetProperty(MessageConst.PropertyUniqClientMessageIdKeyidx);
            header.MsgId = string.IsNullOrEmpty(uniqueKey) ? checkMsg?.MsgId : uniqueKey;
            addr = brokerAddr;
        }
        else
        {
            // Java: id = decodeMessageId(offsetMsgId != null ? offsetMsgId : msgId)
            string idText = string.IsNullOrEmpty(sendResult.OffsetMsgId)
                ? sendResult.MsgId ?? string.Empty
                : sendResult.OffsetMsgId!;
            MessageDecoder.DecodeMessageId(idText, out _, out _, out long offset);
            header.Topic = msg.Topic;
            header.CommitLogOffset = offset;
            header.TranStateTableOffset = sendResult.QueueOffset;
            header.TransactionId = sendResult.TransactionId;
            header.Bname = sendResult.MessageQueue?.BrokerName;
            header.MsgId = sendResult.MsgId;
            addr = c.BrokerAddrForMq(sendResult.MessageQueue!);
        }

        RemotingCommand request =
            RemotingCommand.CreateRequestCommand(RequestCode.EndTransaction, header);
        if (localExceptionText is not null)
        {
            request.Remark = "executeLocalTransactionBranch exception: " + localExceptionText;
        }

        c.RemotingClient.InvokeOneway(addr, request);

        // 事务收尾轨迹（对应 Java DefaultMQProducerImpl.executeEndTransactionHook）。
        // 必须在真正发出 END_TRANSACTION 之后调用，轨迹里的 transactionState 才是最终状态。
        // 回查路径（fromCheck=true）的 msg 是 broker 带回来的 MessageExt，优先用它。
        if (_endTransactionHooks.Count > 0)
        {
            ExecuteEndTransactionHook(new EndTransactionContext
            {
                ProducerGroup = _producerGroup,
                Message = checkMsg is not null ? checkMsg : msg,
                BrokerAddr = addr,
                MsgId = header.MsgId,
                TransactionId = header.TransactionId,
                TransactionState = state,
                FromTransactionCheck = fromCheck,
                Namespace = _namespace,
            });
        }
    }

    /// <summary>
    /// broker 主动发起的事务回查（CHECK_TRANSACTION_STATE=39）入口，由传输层回调。
    /// 回查是 broker 用 invokeOneway 发的，不期待响应，故返回 null。
    /// </summary>
    private RemotingCommand? CheckTransactionState(RemotingCommand cmd, string addr)
    {
        var header = new CheckTransactionStateRequestHeader();
        try
        {
            header.FromExtFields(cmd.ExtFields ?? new PropertyMap());
        }
        catch (Exception e)
        {
            ClientLog.Warn("checkTransactionState: decode header failed from " + addr + ": " + e.Message);
            return null;
        }

        // broker 把整条 MessageExt 编码后放在 body 里（Java Broker2Client.checkProducerTransactionState）
        MessageExt? msgExt = null;
        if (cmd.Body is { Length: > 0 }
            && MessageDecoder.DecodeMessage(cmd.Body, out MessageExt decoded, true, true, true, false))
        {
            msgExt = decoded;
        }

        if (msgExt is null)
        {
            ClientLog.Warn("checkTransactionState: decode message failed");
            return null;
        }

        string? group = msgExt.GetProperty(MessageConst.PropertyProducerGroup);
        if (group is not null && group != _producerGroup)
        {
            ClientLog.Debug("checkTransactionState: group " + group + " is not mine (" + _producerGroup + ")");
            return null;
        }

        ITransactionListener? listener = _txListener;
        if (listener is null)
        {
            ClientLog.Warn("checkTransactionState: no transaction listener for group " + _producerGroup);
            return null;
        }

        // Java 在独立线程里执行回查回调，避免阻塞读线程
        MessageExt captured = msgExt;
        CheckTransactionStateRequestHeader capturedHeader = header;
        var th = new Thread(() =>
        {
            ClientLog.SetThreadName("TransactionCheckThread");
            LocalTransactionState state = LocalTransactionState.Unknow;
            string? exceptionText = null;
            try
            {
                state = listener.CheckLocalTransaction(captured);
            }
            catch (Exception e)
            {
                ClientLog.Error("Broker call checkTransactionState, but checkLocalTransaction exception: "
                    + e.Message);
                exceptionText = e.Message;
            }

            try
            {
                EndTransaction(new Message(), new SendResult(), state, exceptionText, true,
                    capturedHeader, captured, addr);
            }
            catch (Exception e)
            {
                ClientLog.Warn("checkTransactionState: end transaction failed: " + e.Message);
            }
        })
            { IsBackground = true, Name = "TransactionCheckThread" };
        th.Start();
        lock (_txThreadsLock)
        {
            _txThreads.Add(th);
        }

        return null;
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
        TopicPublishInfo publish = TryToFindTopicPublishInfo(c,
            _namespace.Length == 0 ? topic : NamespaceUtil.WrapNamespace(_namespace, topic));
        return publish.MsgQueueList;
    }

    public void CreateTopic(string key, string newTopic, int queueNum = 4)
    {
        MQClientInstance c = GetClient();
        const int perm = 6; // PERM_READ | PERM_WRITE
        // C++ 的 createTopic 忽略 key 形参，统一走默认 topic（MixAll.DefaultTopic）建路由
        _ = key;
        string realTopic = _namespace.Length == 0
            ? newTopic
            : NamespaceUtil.WrapNamespace(_namespace, newTopic);
        c.CreateTopicInRoute(realTopic, queueNum, queueNum, perm);
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

    // 两阶段事务：使用预设 listener 执行本地事务（半消息 → 本地事务 → END_TRANSACTION → broker 回查）。
    public TransactionSendResult SendMessageInTransaction(Message msg, string arg = "")
    {
        if (TransactionListener is null)
        {
            throw new MQClientException("transaction listener is not set");
        }

        return SendMessageInTransaction(msg, TransactionListener, arg);
    }
}
