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
    private readonly object _lock = new();
    private MQClientInstance? _mqClient;
    private bool _started;

    /// <summary>已经调过 <see cref="Shutdown"/>：用来在「排空在途发送」这段时间里拒绝新请求，
    /// 同时让 <see cref="GetClient"/> 报出与 Java <c>SHUTDOWN_ALREADY</c> 同义的文案。
    /// 对应 Java 的 <c>ServiceState.SHUTDOWN_ALREADY</c>。</summary>
    private volatile bool _shutdownRequested;

    private string _producerGroup;
    private string _instanceName = MixAll.DefaultInstanceName;
    private string _clientId = string.Empty;
    // 命名空间（多租户隔离）：非空前，发送时把 topic 拼成 "ns%topic" 发给 broker。
    // 默认空 = 不加命名空间（与裸集群兼容，不破坏现有行为）。
    private string _namespace = string.Empty;
    // ACL 钩子，Start() 时绑定到 MQClientInstance 的传输层
    private IRpcHook? _rpcHook;
    // ClientConfig 的三个单元化/stream 开关（默认值与 Java DefaultMQProducer 一致）
    private string _unitName = string.Empty;
    private bool _unitMode;
    private bool _enableStreamRequestType;
    private string _createTopicKey = MixAll.DefaultTopic;
    private int _defaultTopicQueueNums = MixAll.DefaultTopicQueueNums;
    private int _sendMsgTimeout = 3000;
    // TLS（对应 Java tls.enable；缺省读 env ROCKETMQ_TLS_ENABLE）
    // 注意必须在字段初始化时就取 env：往 MQClientInstance 传的是 bool（非 bool?），
    // 传 false 会盖掉 RemotingClient 里的 env 兜底。
    private bool _tlsEnable = MQClientInstance.TlsEnabledFromEnv();
    // W3C traceparent 透传（opt-in；缺省读 env ROCKETMQ_TRACE_CONTEXT_ENABLE）
    private bool _enableTraceContext;
    private int _retryTimesWhenSendFailed = 2;
    // 对应 Java sendMsgMaxTimeoutPerRequest（默认 -1 = 不限制）：还有重试机会时，
    // 单次请求超时被压到该值，把总预算的余量留给后面的 broker。
    private int _sendMsgMaxTimeoutPerRequest = -1;
    // 可重试的 broker 响应码，默认集合与 Java DefaultMQProducer#retryResponseCodes 一致
    private readonly HashSet<int> _retryResponseCodes = new()
    {
        ResponseCode.SystemError, ResponseCode.SystemBusy,
        ResponseCode.ServiceNotAvailable, ResponseCode.NoPermission,
        ResponseCode.TopicNotExist, ResponseCode.NoBuyerId,
        ResponseCode.NotInCurrentUnit, ResponseCode.GoAway,
    };
    // 对应 Java retryAnotherBrokerWhenNotStoreOK：状态不是 SEND_OK 时是否也换一台 broker
    private bool _retryAnotherBrokerWhenNotStoreOk;
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
    private readonly List<ICheckForbiddenHook> _checkForbiddenHooks = new();
    private AsyncTraceDispatcher? _traceDispatcher;
    // ---------------- 异步发送池（对应 Java DefaultMQProducerImpl:133-140 + NettyRemotingClient:152-157）----------------
    // AsyncSenderExecutor：把「拉路由、选队列、建请求、换 broker 重试」这些准备工作从调用方
    // 线程挪走。core==max==CPU 核数、队列**有界**（Java 的 LinkedBlockingQueue(50000)）。
    // NettyClientPublicExecutor：用户回调与 SendMessageHook.after 在这里跑，绝不让业务代码
    // 占着连接的读线程或超时清理线程（Java 的 executeInvokeCallback 就是 submit 给 publicExecutor）。
    // 两个池都 keepAlive 60s 且 core==max，所以线程按需创建、创建后不退出。
    private ConsumeExecutor? _asyncSenderExecutor;
    /// <summary>内建池才由 <see cref="Shutdown"/> 负责排空（Java 只关 defaultAsyncSenderExecutor）。</summary>
    private bool _asyncSenderExecutorOwned = true;
    private ConsumeExecutor? _callbackExecutor;
    // Java DefaultMQProducer:140 / :133 —— 异步重试次数与发送队列容量
    private int _retryTimesWhenSendAsyncFailed = 2;
    private int _asyncSenderQueueCapacity = 50000;
    // Java NettyRemotingClient:152 —— 回调池线程数，0 = 用 CPU 核数
    private int _clientCallbackExecutorThreads;
    // ---------------- 异步发送背压（对应 Java DefaultMQProducerImpl:122-153）----------------
    // 开关默认关闭，与 Java 同。两个信号量在**构造**时建好（不是 Start()）：开关允许运行时
    // 才打开，那时闸必须已经在了。
    private bool _enableBackpressureForAsyncMode;
    private int _backPressureForAsyncSendNum = 1024;
    private int _backPressureForAsyncSendSize = 100 * 1024 * 1024;
    private readonly FairSemaphore _semaphoreAsyncSendNum;
    private readonly FairSemaphore _semaphoreAsyncSendSize;

    // 建背压信号量时的初始许可（Java DefaultMQProducerImpl:141-153）：配置**不高于**地板值时
    // 用地板值，并记一条 info —— Java 的分支是 `cfg > 10 ? new Semaphore(max(cfg, 10)) : new
    // Semaphore(10)`，所以恰好等于地板值时也会打这条日志。
    private static long BackPressurePermits(int configured, long floor, string name)
    {
        if (configured > floor)
        {
            return configured;
        }

        ClientLog.Info(name + " can not be smaller than "
                       + floor.ToString(CultureInfo.InvariantCulture) + ".");
        return floor;
    }

    // 扣多少「字节」许可（Java executeAsyncMessageSend:642 的
    // `msg.getBody() == null ? 1 : msg.getBody().length`）：不给空 body 算 1 就等于不限流。
    private static long BackPressureMsgLen(Message msg)
    {
        byte[] body = msg.Body;
        return body.Length == 0 ? 1 : body.Length;
    }

    public DefaultMQProducer(string producerGroup = MixAll.DefaultProducerGroup)
    {
        if (UtilAll.IsBlank(producerGroup))
        {
            throw new MQClientException("producerGroup is empty");
        }

        _producerGroup = producerGroup;
        // Java 在建 impl 时就把两个公平信号量建好（DefaultMQProducerImpl:141-153）
        _semaphoreAsyncSendNum = new FairSemaphore(
            BackPressurePermits(_backPressureForAsyncSendNum, FairSemaphore.MinAsyncSendNum,
                "semaphoreAsyncSendNum"));
        _semaphoreAsyncSendSize = new FairSemaphore(
            BackPressurePermits(_backPressureForAsyncSendSize, FairSemaphore.MinAsyncSendSize,
                "semaphoreAsyncSendSize"));
        // opt-in；缺省读 env ROCKETMQ_TRACE_CONTEXT_ENABLE
        _enableTraceContext = TraceParentContext.EnabledFromEnv();
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

    /// <summary>TLS（对应 Java tls.enable；缺省读 env ROCKETMQ_TLS_ENABLE）。</summary>
    public bool TlsEnable
    {
        get => _tlsEnable;
        set => _tlsEnable = value;
    }

    /// <summary>W3C traceparent 透传（opt-in；缺省读 env ROCKETMQ_TRACE_CONTEXT_ENABLE）。</summary>
    public bool EnableTraceContext
    {
        get => _enableTraceContext;
        set => _enableTraceContext = value;
    }

    public int RetryTimesWhenSendFailed
    {
        get => _retryTimesWhenSendFailed;
        set => _retryTimesWhenSendFailed = value;
    }

    /// <summary>对应 Java <c>DefaultMQProducer.retryTimesWhenSendAsyncFailed</c>（:140，默认 2）：
    /// **异步**发送的重试次数，与同步那份分开配置。只在异步链里生效：换一台 broker、给同一个
    /// 请求换新 opaque 再试，上限就是这个数（首次尝试不计）。异步发送**不看**
    /// <see cref="RetryTimesWhenSendFailed"/>，也**不看** <c>RetryResponseCodes</c>
    /// —— broker 明确回了错误码就不再换 broker（Java <c>onExceptionImpl</c> 的
    /// <c>needRetry</c> 只在「没收到响应」时为真）。</summary>
    public int RetryTimesWhenSendAsyncFailed
    {
        get => _retryTimesWhenSendAsyncFailed;
        set => _retryTimesWhenSendAsyncFailed = value;
    }

    /// <summary>对应 Java <c>DefaultMQProducerImpl:133</c> 写死的 <c>LinkedBlockingQueue(50000)</c>：
    /// 异步发送队列容量。只在 <see cref="Start"/> 读一次（Java 也只在构造时定）。队列满了
    /// <c>SendAsync</c> 向调用方抛 <c>MQClientException("executor rejected")</c>，开了背压则
    /// 就地跑完那一笔（Java <c>:675-681</c>）。</summary>
    public int AsyncSenderQueueCapacity
    {
        get => _asyncSenderQueueCapacity;
        set => _asyncSenderQueueCapacity = value;
    }

    /// <summary>对应 Java <c>DefaultMQProducer.setAsyncSenderExecutor:1157-1161</c> +
    /// <c>DefaultMQProducerImpl.getAsyncSenderExecutor:1608-1613</c>：自带异步发送池时
    /// 内建池不再创建，<see cref="Shutdown"/> 也**不等它、不关它**（Java 只
    /// <c>defaultAsyncSenderExecutor.shutdown()</c>）—— 池的生命周期归调用方。
    /// 必须在 <see cref="Start"/> 之前设（池在 Start 里定）。</summary>
    public ConsumeExecutor? AsyncSenderExecutor { get; set; }

    /// <summary>对应 Java <c>NettyRemotingClient:152-157</c> 的 publicExecutor 线程数：
    /// 用户回调与 after 钩子跑在几根线程上。0（默认）= CPU 核数；必须在
    /// <see cref="Start"/> 之前设，池是启动时建的。</summary>
    public int ClientCallbackExecutorThreads
    {
        get => _clientCallbackExecutorThreads;
        set => _clientCallbackExecutorThreads = value;
    }

    /// <summary>
    /// 对应 Java setSendMsgMaxTimeoutPerRequest。-1（默认）表示单次请求不设上限；
    /// 设成有限值后，**还有重试机会**的那几次单次超时被压到该值 —— 否则一台慢
    /// broker 就能把整个 SendMsgTimeout 预算吃光，剩下的 broker 一次都试不到。
    /// </summary>
    public int SendMsgMaxTimeoutPerRequest
    {
        get => _sendMsgMaxTimeoutPerRequest;
        set => _sendMsgMaxTimeoutPerRequest = value;
    }

    /// <summary>对应 Java retryAnotherBrokerWhenNotStoreOK（默认 false）。</summary>
    public bool RetryAnotherBrokerWhenNotStoreOk
    {
        get => _retryAnotherBrokerWhenNotStoreOk;
        set => _retryAnotherBrokerWhenNotStoreOk = value;
    }

    /// <summary>对应 Java getRetryResponseCodes（返回副本，改集合要走 AddRetryResponseCode）。</summary>
    public HashSet<int> RetryResponseCodes => new(_retryResponseCodes);

    /// <summary>对应 Java addRetryResponseCode：broker 回了这些码才值得换一台重发。</summary>
    public void AddRetryResponseCode(int responseCode)
    {
        lock (_retryResponseCodes)
        {
            _retryResponseCodes.Add(responseCode);
        }
    }

    /// <summary>
    /// 对应 Python is_retry_response_code：null（压根没等到响应码）等于不可重试。
    /// </summary>
    public bool IsRetryResponseCode(int? responseCode)
    {
        if (!responseCode.HasValue)
        {
            return false;
        }

        lock (_retryResponseCodes)
        {
            return _retryResponseCodes.Contains(responseCode.Value);
        }
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

    // ---------------- unitName / unitMode / enableStreamRequestType ----------------
    // 对应 Java ClientConfig 的三个同名开关。⚠ 必须在 Start() 之前设置：unitName 与
    // @STREAM 决定 clientId 的形状，stream 决定请求钩子链（ReqT 要进 ACL 签名内容）。
    /// <summary>单元名：进 clientId 的 <c>@&lt;unitName&gt;</c> 段，也拼进动态取址 URL。</summary>
    public string UnitName
    {
        get => _unitName;
        set => _unitName = value ?? string.Empty;
    }

    /// <summary>
    /// 对应 Java <c>ClientConfig#isUnitMode()</c>。生产者这一路只落在一处：
    /// <c>DefaultMQProducerImpl:1004</c> 的 <c>requestHeader.setUnitMode(...)</c>
    /// → SEND_MESSAGE_V2 的单字母键 <c>k</c>；broker 据此给自动建出来的 topic 打
    /// UNIT(0x1) / UNIT_SUB(0x2) 系统标记（AbstractSendMessageProcessor:487-497）。
    /// Java 的 DefaultMQProducer 从不主动置真，所以默认 false。
    /// </summary>
    public bool UnitMode
    {
        get => _unitMode;
        set => _unitMode = value;
    }

    /// <summary>
    /// true 时每笔请求带 <c>ReqT=0</c>、clientId 末尾多一段 <c>@STREAM</c>。
    /// Java 只有 pull / lite 消费者在构造里置真，生产者默认 false。
    /// </summary>
    public bool EnableStreamRequestType
    {
        get => _enableStreamRequestType;
        set => _enableStreamRequestType = value;
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

    /// <summary>注册发送前拦截钩子（对应 Java DefaultMQProducerImpl.registerCheckForbiddenHook:186）。</summary>
    public void RegisterCheckForbiddenHook(ICheckForbiddenHook hook)
    {
        if (hook is not null)
        {
            _checkForbiddenHooks.Add(hook);
        }
    }

    public bool HasCheckForbiddenHook() => _checkForbiddenHooks.Count > 0;

    public int CheckForbiddenHookCount() => _checkForbiddenHooks.Count;

    /// <summary>是否需要走「带拦截/钩子」的发送内核（两者任一存在就得走）。</summary>
    public bool HasSendInterceptors() => _sendMessageHooks.Count > 0 || _checkForbiddenHooks.Count > 0;

    /// <summary>供单测/联调直接驱动钩子执行（不经过网络）。
    ///
    /// ⚠ 与 Send/Consume/EndTransaction 钩子<b>相反</b>：这里<b>不吞异常</b> ——
    /// 钩子抛出的异常会原样传播出去（Java CheckForbiddenHook 的签名就是
    /// <c>throws MQClientException</c>），这正是"禁止发送"的实现方式。
    /// </summary>
    public void ExecuteCheckForbiddenHook(CheckForbiddenContext context)
    {
        foreach (ICheckForbiddenHook hook in _checkForbiddenHooks)
        {
            hook.CheckForbidden(context);
        }
    }

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

    // ---------------- 异步发送背压（对应 Java DefaultMQProducer:1368-1408）----------------
    // 开关默认关闭（与 Java 同），而且**不是**启动期配置：跑到一半也能打开/关掉，
    // 因为只有闸本身读它（见 SendAsync）。
    public bool EnableBackpressureForAsyncMode
    {
        get => _enableBackpressureForAsyncMode;
        set => _enableBackpressureForAsyncMode = value;
    }

    /// <summary>
    /// 运行时改「在途条数」上限（Java setBackPressureForAsyncSendNum:1383-1391）。语义不是
    /// 「设成 num」而是「总量变成 num、已经在途的那几份原样保留」，所以调小之后
    /// <see cref="SemaphoreAsyncSendNumAvailablePermits" /> 可能为负。低于地板值 10 时夹到 10。
    /// </summary>
    public int BackPressureForAsyncSendNum
    {
        get => _backPressureForAsyncSendNum;
        set
        {
            _backPressureForAsyncSendNum = Math.Max(value, (int)FairSemaphore.MinAsyncSendNum);
            _semaphoreAsyncSendNum.SetTotalPermits(_backPressureForAsyncSendNum);
        }
    }

    /// <summary>在途字节数上限，地板值 1M，语义同 <see cref="BackPressureForAsyncSendNum" />。</summary>
    public int BackPressureForAsyncSendSize
    {
        get => _backPressureForAsyncSendSize;
        set
        {
            _backPressureForAsyncSendSize = Math.Max(value, (int)FairSemaphore.MinAsyncSendSize);
            _semaphoreAsyncSendSize.SetTotalPermits(_backPressureForAsyncSendSize);
        }
    }

    public int GetBackPressureForAsyncSendNum() => _backPressureForAsyncSendNum;

    public int GetBackPressureForAsyncSendSize() => _backPressureForAsyncSendSize;

    /// <summary>观测用（Java DefaultMQProducerImpl:200-206）：当前空闲许可，负数表示在途超额。</summary>
    public long SemaphoreAsyncSendNumAvailablePermits => _semaphoreAsyncSendNum.AvailablePermits();

    public long SemaphoreAsyncSendSizeAvailablePermits => _semaphoreAsyncSendSize.AvailablePermits();

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

            // 对应 Java DefaultMQProducerImpl.checkConfig()：组名合法性 + 保留默认组名。
            // 排在 checkConfig 该在的位置——任何网络动作之前，非法配置在 Start() 当场失败。
            Validators.CheckGroup(_producerGroup);
            if (_producerGroup == MixAll.DefaultProducerGroup)
            {
                throw new MQClientException("producerGroup can not equal " + MixAll.DefaultProducerGroup
                                            + ", please specify another one.");
            }

            // 静态地址与动态取址（ROCKETMQ_NAMESRV_DOMAIN）二选一必须可用
            if (_nameServerAddrs.Count == 0 && !DefaultTopAddressing.IsConfigured())
            {
                throw new MQClientException("name server address is not set");
            }

            // 对应 Java DefaultMQProducerImpl.start()（:251）：非 inner 生产者无条件把默认的
            // instanceName 换成 <pid>#<nanoTime>，再按 ClientConfig#buildMQClientId 拼 clientId。
            // 就地写回字段，所以同一生产者 restart 后 clientId 不变（Java 同样改了不回滚）。
            _instanceName = ClientIds.ChangeInstanceNameToPID(_instanceName);
            if (string.IsNullOrEmpty(_clientId))
            {
                _clientId = ClientIds.Build(_instanceName, _unitName, _enableStreamRequestType);
            }

            // 请求钩子（ACL 签名 / stream 的 ReqT）：绑定必须在 Start() 之前 ——
            // Java 的 rpcHook 是随 MQClientAPIImpl 构造进去的，实例第一笔报文就带着它；
            // 放在 Start() 之后，start 期间的动态取址/首包路由就是裸的。
            // composeRequestHooks 还原 Java 的 stream → 用户钩子顺序（单槽传输层）。
            IRpcHook? requestHook = RequestHooks.Compose(_enableStreamRequestType, _rpcHook);
            _mqClient = new MQClientInstance(_clientId, _nameServerAddrs,
                tlsEnable: _tlsEnable, unitName: _unitName);
            if (requestHook is not null && !_mqClient.RegisterRpcHook(requestHook))
            {
                ClientLog.Warn("producer rpc hook ignored: MQClientInstance already has one (clientId="
                    + _clientId + ")");
            }
            _mqClient.Start();
            // 动态 name server：实例启动时可能已从地址服务器拿到地址，回填到本生产者
            if (_nameServerAddrs.Count == 0 && _mqClient.NameServerAddrs.Count > 0)
            {
                _nameServerAddrs = new List<string>(_mqClient.NameServerAddrs);
            }

            // 注册 broker 主动请求处理器：事务回查 CHECK_TRANSACTION_STATE(39)。
            // 不注册的话回查会被传输层当成"未知请求"丢弃，事务消息永远停留在 Unknown。
            _mqClient.RemotingClient.RegisterProcessor(RequestCode.CheckTransactionState,
                CheckTransactionState);

            _started = true;
            // 允许 Shutdown 之后再 Start（Shutdown 的那半程里 _started 还是 true，靠这个标记
            // 挡住重复 Shutdown；重新 Start 就要把它清掉）
            _shutdownRequested = false;
            // 允许 Shutdown 之后再 Start（Java 也支持重新 start）：这道闸必须跟着复位
            _shutdownRequested = false;
            // 异步发送池（Java 在 DefaultMQProducerImpl 构造时建，这里等价放在 Start 的锁内）：
            // 必须早于 _started=true 之后任何一次 SendAsync —— SendAsync 只在锁里取句柄。
            CreateAsyncExecutors();
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

    /// <summary>建异步发送链的两个池（对应 Java <c>DefaultMQProducerImpl:133-140</c> 与
    /// <c>NettyRemotingClient:152-157</c>），线程名对齐 Java 的 <c>ThreadFactoryImpl</c>
    /// （<c>AsyncSenderExecutor_1…</c> / <c>NettyClientPublicExecutor_1…</c>，序号从 1 起）。</summary>
    private void CreateAsyncExecutors()
    {
        int cores = Math.Max(1, Environment.ProcessorCount);
        // 对应 Java DefaultMQProducerImpl.getAsyncSenderExecutor:1608-1613：
        // 调用方给了自定义池就用它的，内建池只在没给的时候建。
        _asyncSenderExecutorOwned = AsyncSenderExecutor is null;
        _asyncSenderExecutor = AsyncSenderExecutor ?? new ConsumeExecutor(
            cores, cores, 60.0, "AsyncSenderExecutor",
            maxQueueSize: _asyncSenderQueueCapacity, threadNameSep: "_", threadIndexFrom: 1);
        int callbackThreads = _clientCallbackExecutorThreads;
        if (callbackThreads <= 0)
        {
            // Java：默认 availableProcessors；显式配成 <=0 时 NettyRemotingClient 兜到 4
            callbackThreads = cores;
        }

        _callbackExecutor = new ConsumeExecutor(
            callbackThreads, callbackThreads, 60.0, "NettyClientPublicExecutor",
            threadNameSep: "_", threadIndexFrom: 1);
    }

    public void Shutdown()
    {
        List<Thread> threads;
        ConsumeExecutor? sender;
        ConsumeExecutor? callback;
        bool senderOwned;
        lock (_lock)
        {
            if (!_started || _shutdownRequested)
            {
                return;
            }

            // ⚠ 这里**先不**把 _started 翻掉：排空发送池时还要跑准备段，而准备段要用客户端
            //（GetClient / 建连 / 发请求）。提前翻掉会让排在队列里的任务统统变成
            // "producer not started"，排空就成了走个形式。拒绝新请求由 _shutdownRequested
            // 和置空的两个池负责，语义等价于 Java 的 SHUTDOWN_ALREADY。
            _shutdownRequested = true;
            sender = _asyncSenderExecutor;
            callback = _callbackExecutor;
            senderOwned = _asyncSenderExecutorOwned;
            _asyncSenderExecutor = null;
            _callbackExecutor = null;

            // 先停心跳线程（它内部持有 mqClient 引用）
            _heartbeatRunning = false;
            if (_heartbeatThread is { IsAlive: true })
            {
                _heartbeatThread.Join(2000);
            }

            lock (_txThreadsLock)
            {
                threads = new List<Thread>(_txThreads);
                _txThreads.Clear();
            }
        }

        // 先回收异步发送池（它们内部持有 mqClient 引用），再关客户端：
        // 排空（wait=true）而不是只 shutdown()。Java/Python 用的是不等待的
        // <c>defaultAsyncSenderExecutor.shutdown()</c>，于是「send_async 完立刻 shutdown」会
        // 把队列里还没跑到的准备段连同任务一起丢掉；这里等到队列跑完，保证交进来的
        // 每一笔都**跑完准备段并把报文交给传输层**。
        // ⚠ 排空的只是准备段（含换 broker 重试的发起）：网络等待在传输层的线程上，不等。
        // ⚠ 保证止于「交给传输层」：紧接着就要关客户端，broker 还没读走的**尾部**几帧会随
        // 连接一起丢掉（离线跑整套用例时实测 32/36 上线、单跑稳定 36/36），响应没回来的那几笔
        // 也拿不到终态回调 —— 后一条与 Java 同构（关客户端会停掉超时清理并清空在途表，没人再
        // 负责投递）。调用方要确保每一笔都拿到回调，就得自己等回调再 Shutdown。
        // 调用方自带的池（Java setAsyncSenderExecutor）不动它 —— 池归它自己管。
        if (senderOwned)
        {
            sender?.Shutdown(true);
        }
        // 回调池同理：等已排队的回调跑完。响应在此之后才回来的那一笔，
        // CompleteOnCallbackThread 见池已关就地转交用户回调（与 Python 的兜底同一条）。
        callback?.Shutdown(true);

        // 准备段都跑完了才宣布「不再活着」：此后任何路径都拿不到客户端。
        lock (_lock)
        {
            _started = false;
        }

        // 事务回查线程仍按老口径 join（它们也持有 mqClient 引用）
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
            // 分得清「没启动」和「已经关掉」：Java 用 ServiceState 区分 CREATE_JUST 与
            // SHUTDOWN_ALREADY，这里靠 _shutdownRequested 做到同一条判据。
            throw new MQClientException(_shutdownRequested
                ? "producer already shutdown"
                : "producer not started, call start() first");
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
    /// 无钩子时直通，不引入任何额外开销。
    ///
    /// 执行顺序严格照抄 Java sendKernelImpl:956-990：
    ///   1. <b>CheckForbiddenHook</b>（每次尝试都跑；异常<b>不吞</b>，直接抛给重试链）
    ///   2. SendMessageHook.before
    ///   3. 发请求
    ///   4. SendMessageHook.after（成功带 SendResult / 失败带 Exception）</summary>
    private SendResult SendWithHooks(MQClientInstance c, Message msg, MessageQueue mq,
        int timeout, int sysFlag, object? arg = null,
        CommunicationMode mode = CommunicationMode.Sync)
    {
        // W3C traceparent 透传（opt-in）：没有就注入根上下文，已有值不覆盖。
        // ⚠ 必须放在 HasSendInterceptors 早退之前，否则无钩子时注入被跳过。
        if (_enableTraceContext)
        {
            TraceParentContext.Inject(msg);
        }

        if (!HasSendInterceptors())
        {
            return c.SendMessage(_producerGroup, msg, mq, timeout, sysFlag, _unitMode,
                _createTopicKey, _defaultTopicQueueNums);
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

        RunCheckForbidden(msg, mq, brokerAddr, arg, mode);

        if (_sendMessageHooks.Count == 0)
        {
            return c.SendMessage(_producerGroup, msg, mq, timeout, sysFlag, _unitMode,
                _createTopicKey, _defaultTopicQueueNums);
        }

        SendMessageContext context = BuildSendContext(msg, mq, brokerAddr);
        ExecuteSendMessageHookBefore(context);
        SendResult result;
        try
        {
            result = c.SendMessage(_producerGroup, msg, mq, timeout, sysFlag, _unitMode,
                _createTopicKey, _defaultTopicQueueNums);
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

    /// <summary>构造 CheckForbiddenContext 并执行（每次发送尝试都会调一次，含重试）。
    /// 异常不在这里捕获 —— 必须沿发送重试链向上传播。</summary>
    private void RunCheckForbidden(Message msg, MessageQueue mq, string brokerAddr,
        object? arg, CommunicationMode mode)
    {
        if (_checkForbiddenHooks.Count == 0)
        {
            return;
        }

        var context = new CheckForbiddenContext
        {
            NameSrvAddr = _nameServerAddrs.Count > 0 ? _nameServerAddrs[0] : string.Empty,
            Group = _producerGroup,
            Message = msg,
            Mq = mq,
            BrokerAddr = brokerAddr,
            CommunicationMode = mode,
            Arg = arg,
            // Java DefaultMQProducerImpl:964 `checkForbiddenContext.setUnitMode(this.isUnitMode())`
            // —— 钩子据此判断这是不是单元化流量。
            UnitMode = _unitMode,
        };
        ExecuteCheckForbiddenHook(context);
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

    // 对应 Java Validators.checkMessage：topic 合法性、禁发 topic、body 长度阈值、LMQ 路径。
    // 本地先拦下来是因为 TOPIC_NOT_EXIST 属于可重试码，非法名字会把重试预算空转一遍。
    private void CheckMessage(Message msg) => Validators.CheckMessage(msg, _maxMessageSize);

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

    /// <summary>重试耗尽后给最后一次失败定性，决定最终 MQClientException 的错误码。
    /// 等价于 Java 的 lastExceptionType 判定（C++/Rust 同款分档）。</summary>
    private enum SendFailureCause
    {
        None,
        Broker,
        Connect,
        Timeout,
        Client,
        Other,
    }

    // 不指定队列：按 broker 延迟/隔离状态选队列，失败按 retryTimesWhenSendFailed 重试。
    // 逐条对齐 Java DefaultMQProducerImpl#sendDefaultImpl：
    //   * timesTotal = 1 + retryTimesWhenSendFailed（只有同步发送有重试）；
    //   * 路由在循环**之外**只取一次，整条重试链共用；完全取不到时在循环外就按
    //     NOT_FOUND_TOPIC 抛掉，不把重试次数空转掉；
    //   * 每次尝试先算 costTime，总超时已用完则整体放弃（→ RemotingTooMuchRequestException）；
    //     还剩重试机会时，单次请求超时被 SendMsgMaxTimeoutPerRequest 压住，把余量留给后面的 broker；
    //   * 异常按类型分档写容错表，且只有 RetryResponseCodes 里的 broker 响应码才继续重试；
    //   * 全部失败时把原因映射成 ClientErrorCode 塞进 MQClientException。
    public SendResult Send(Message msg, int timeoutMillis = -1)
    {
        MQClientInstance c = GetClient();
        int timeout = timeoutMillis >= 0 ? timeoutMillis : _sendMsgTimeout;
        CheckMessage(msg);
        Message outbound = WithNamespace(msg);
        // 在重试循环之外压缩一次：PrepareForSend 就地改写 body，循环内重复调用会把
        // 已压缩的字节再压一遍（zlib(zlib(x))），消费端只解一层就拿到压缩流。
        int sysFlag = PrepareForSend(outbound);

        TopicPublishInfo publish;
        try
        {
            publish = TryToFindTopicPublishInfo(c, outbound.Topic);
        }
        catch (MQClientException e)
        {
            throw new MQClientException(e.Message, ClientErrorCode.NotFoundTopicException);
        }

        int timesTotal = _retryTimesWhenSendFailed + 1;
        // 耗时用单调高精度钟（对应 Python time.monotonic / C++ steady_clock / Rust monotonic_millis）：
        // 墙钟毫秒粒度会把本地环回的亚毫秒往返量成 0，容错阈值就永远不生效。
        double beginFirst = UtilAll.MonotonicMillis();
        var brokersSent = new List<string>();
        string lastBrokerName = string.Empty;
        // 非 SEND_OK 且开了换 broker 时，把这个"存了但没存好"的结果留着：后面全失败就原样返回。
        SendResult? result = null;
        string lastError = string.Empty;
        var cause = SendFailureCause.None;
        int brokerCode = MQBrokerException.Unknown;
        bool callTimeout = false;

        for (int attempt = 0; attempt < timesTotal; ++attempt)
        {
            // 故障规避：开启时按 broker 延迟/隔离状态选队列（Java MQFaultStrategy）；
            // 关闭时退化为普通轮询（策略内部判断）。重试时 resetIndex 让轮询从头开始，
            // 从而能避开 lastBrokerName 选到别的 broker。
            MessageQueue selected;
            try
            {
                selected = _mqFaultStrategy.SelectOneMessageQueue(publish, lastBrokerName,
                    attempt > 0);
            }
            catch (MQClientException e)
            {
                // 选不到队列属客户端异常：此刻还没有目标 broker，容错表无从记起（Java 同）
                lastError = e.Message;
                cause = SendFailureCause.Client;
                continue;
            }

            lastBrokerName = selected.BrokerName;
            brokersSent.Add(selected.BrokerName);
            double began = UtilAll.MonotonicMillis();

            // 整体预算：timeout 是**这次调用**的总预算，已被前面的尝试吃掉的部分要扣掉，
            // 否则 3 次重试各 3s 会变成最长 9s 才返回。
            long costTime = (long)(began - beginFirst);
            if (timeout < costTime)
            {
                callTimeout = true;
                break;
            }

            // costTime <= timeout（int），差值必在 int 范围内，窄化转换无损
            int curTimeout = (int)(timeout - costTime);
            bool canRetryAgain = attempt + 1 < timesTotal;
            if (_sendMsgMaxTimeoutPerRequest > -1 && canRetryAgain
                && curTimeout > _sendMsgMaxTimeoutPerRequest)
            {
                curTimeout = _sendMsgMaxTimeoutPerRequest;
            }

            try
            {
                SendResult sent = SendWithHooks(c, outbound, selected, curTimeout, sysFlag);
                result = sent;
                // 记录发送延迟；超出阈值会把该 broker 隔离一段时间
                _mqFaultStrategy.UpdateFaultItem(selected.BrokerName,
                    UtilAll.MonotonicMillis() - began, false, true);
                // Java：非 SEND_OK 且开了 retryAnotherBrokerWhenNotStoreOK 才换 broker，
                // 否则把这个"存了但没存好"的结果原样返回
                if (sent.SendStatus != SendStatus.SendOk && _retryAnotherBrokerWhenNotStoreOk)
                {
                    continue;
                }

                return sent;
            }
            catch (MQBrokerException e)
            {
                // broker 明确回了错误码：隔离该 broker（可达性不动），只有可重试码才换一台
                _mqFaultStrategy.UpdateFaultItem(selected.BrokerName,
                    UtilAll.MonotonicMillis() - began, true, false);
                lastError = e.Message;
                cause = SendFailureCause.Broker;
                brokerCode = e.ResponseCode;
                if (IsRetryResponseCode(e.ResponseCode))
                {
                    continue;
                }

                if (result is not null)
                {
                    return result;
                }

                throw;
            }
            catch (RemotingException e)
            {
                // 连不上/超时/发不出去：隔离该 broker。本项目无后台可达性探测线程，
                // 所以 Java 的 reachable=!isStartDetectorEnable() 恒为 true。
                _mqFaultStrategy.UpdateFaultItem(selected.BrokerName,
                    UtilAll.MonotonicMillis() - began, true, true);
                lastError = e.Message;
                cause = e switch
                {
                    RemotingConnectException => SendFailureCause.Connect,
                    RemotingTimeoutException => SendFailureCause.Timeout,
                    _ => SendFailureCause.Other,
                };
            }
            catch (MQClientException e)
            {
                // 客户端自己的问题（钩子拦截、路由没了…）：Java 同样只记延迟、不隔离
                _mqFaultStrategy.UpdateFaultItem(selected.BrokerName,
                    UtilAll.MonotonicMillis() - began, false, true);
                lastError = e.Message;
                cause = SendFailureCause.Client;
            }
        }

        if (result is not null)
        {
            return result;
        }

        if (callTimeout)
        {
            throw new RemotingTooMuchRequestException("sendDefaultImpl call timeout");
        }

        long costTotal = (long)(UtilAll.MonotonicMillis() - beginFirst);
        string info = "Send [" + brokersSent.Count.ToString(CultureInfo.InvariantCulture)
                      + "] times, still failed, cost ["
                      + costTotal.ToString(CultureInfo.InvariantCulture)
                      + "]ms, Topic: " + outbound.Topic + ", BrokersSent: ["
                      + string.Join(", ", brokersSent) + "], last error: " + lastError;
        int responseCode = cause switch
        {
            SendFailureCause.Broker => brokerCode,
            SendFailureCause.Connect => ClientErrorCode.ConnectBrokerException,
            SendFailureCause.Timeout => ClientErrorCode.AccessBrokerTimeout,
            SendFailureCause.Client => ClientErrorCode.BrokerNotExistException,
            _ => MQBrokerException.Unknown,
        };
        throw new MQClientException(info, responseCode);
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
        // arg 透传给 CheckForbiddenHook（Java CheckForbiddenContext.arg 就是它）
        return SendWithHooks(c, outbound, selected, timeout, sysFlag, arg, CommunicationMode.Sync);
    }

    // ---------------- 异步 / 单向 ----------------

    /// <summary>异步发送（对应 Java <c>DefaultMQProducerImpl.send(msg, SendCallback, timeout)</c>
    /// 的整条链，移植自 Python <c>send_async</c>）：<b>调用方立即返回</b>，四段与 Java 逐段对齐：
    ///
    /// 1. 先过背压闸（<see cref="EnableBackpressureForAsyncMode"/>，Java
    ///    <c>executeAsyncMessageSend:635-682</c>）。这道闸<b>在调用方线程上等</b>，与
    ///    Java/Python/C++ 同，所以背压打满时「异步」会退化成「等满 timeout 再报错」。
    /// 2. 任务投进 <c>AsyncSenderExecutor_N</c>（core==max==CPU 核数、队列有界
    ///    <see cref="AsyncSenderQueueCapacity"/>，默认 50000，同 Java 写死的
    ///    <c>LinkedBlockingQueue(50000)</c>）。队满 ≡ Java <c>submit</c> 抛
    ///    <c>RejectedExecutionException</c> → <c>MQClientException("executor rejected")</c>，
    ///    <b>抛给调用方</b>而不走回调；只有开了背压才改走 Java <c>:675-681</c> 的「就地跑完
    ///    这一笔」—— 那边扣许可发生在入队<b>之前</b>，不跑完就要白等超时才归还。
    /// 3. 出队之后才算真实耗时：预算被排队吃掉直接回调
    ///    <c>RemotingTooMuchRequestException("DEFAULT ASYNC send call timeout")</c>，不再发请求。
    /// 4. <c>sendKernelImpl</c> 的 ASYNC 分支（<see cref="SendKernelAsync"/>）：地址解析 →
    ///    拦截钩子 → 建请求（<b>只建一次</b>）→ before 钩子 → <c>invokeAsync</c>。失败进
    ///    <see cref="AsyncSendChain.OnException"/>（Java <c>onExceptionImpl</c>）：换一台 broker
    ///    的队列、给<b>同一个请求</b>换新 opaque 再试，上限
    ///    <see cref="RetryTimesWhenSendAsyncFailed"/>；超时预算是所有尝试<b>共享</b>的剩余时间。
    ///    broker 明确回了错误码<b>不进</b>重试链（异步不看 <c>RetryResponseCodes</c>）。
    ///
    /// 链的终点固定是 <see cref="CompleteAsync"/>：after 钩子 → 归还许可 → 用户回调，用户回调
    /// <b>恰好一次</b>，且跑在 <c>NettyClientPublicExecutor_N</c> 上（Java 的
    /// <c>executeInvokeCallback</c> 就是把回调 submit 给 publicExecutor，为的是不让业务代码占着
    /// 连接的读线程或超时清理线程）。
    ///
    /// 与 Java 的两处有意差别：未 <see cref="Start"/> / 池已关时<b>同步抛</b>（Java 走回调，
    /// 那样问题更难查），与 Python 一致；批量消息没有异步内核可用，于是在池线程里同步发一批、
    /// 回调照样转交（Python/Rust 同构，对调用方语义没差别）。
    /// </summary>
    public void SendAsync(Message msg, ISendCallback callback, int timeoutMillis = -1,
        MessageQueue? mq = null)
    {
        // 链上带的是**克隆**：压缩会就地改 body，不能改调用方那份。克隆在池线程上做（与批量
        // 同一条前段），许可长度仍在**调用方线程**上按压缩前的 body 算 —— Java 就是在那儿算的。
        ExecuteAsyncSend(() => CloneMessage(msg), BackPressureMsgLen(msg), callback, timeoutMillis,
            mq);
    }

    /// <summary>异步链的前段（Python <c>send_async</c> 的整体、Java
    /// <c>executeAsyncMessageSend</c> + <c>AsyncSenderExecutor.submit</c>）：过闸、投池、
    /// 出队后复检预算，再把 <paramref name="prepare"/> 造出来的消息交给
    /// <see cref="SendAsyncInner"/>。</summary>
    /// <param name="prepare">在<b>池线程</b>上造出这一笔要发的消息。放在池线程上是必须的：
    /// 批量入口在那里才会抛「空列表 / 子消息非法 / 不同质」，异常得进回调而不是砸回调用方
    /// 的栈（Python 的批量分支同样在 runnable 里抛、由外层 catch 交给回调）。</param>
    /// <param name="msgLen">扣多少「字节」许可，在调用方线程上算好。</param>
    private void ExecuteAsyncSend(Func<Message> prepare, long msgLen, ISendCallback callback,
        int timeoutMillis, MessageQueue? mq)
    {
        // 先确认已启动（与 Python 一致：未启动立即抛，而不是在后台线程里静默失败）
        _ = GetClient();
        ConsumeExecutor? executor;
        lock (_lock)
        {
            executor = _asyncSenderExecutor;
        }

        if (executor is null)
        {
            throw new MQClientException("producer already shutdown");
        }

        int timeout = timeoutMillis >= 0 ? timeoutMillis : _sendMsgTimeout;
        double began = UtilAll.MonotonicMillis();
        var permits = new AsyncSendPermits(_semaphoreAsyncSendNum, _semaphoreAsyncSendSize, msgLen);

        if (_enableBackpressureForAsyncMode)
        {
            // 两个许可**顺序**申请、都用「从 began 算起的剩余预算」去等（Java :648 / :661
            // 都是 `timeout - costTime`），所以第一个闸就能把预算花光
            int numBudget = timeout - (int)(UtilAll.MonotonicMillis() - began);
            permits.NumAcquired = numBudget > 0 && _semaphoreAsyncSendNum.TryAcquire(1, numBudget);
            if (!permits.NumAcquired)
            {
                CompleteAsync(callback, null,
                    new RemotingTooMuchRequestException(
                        "send message tryAcquire semaphoreAsyncNum timeout"), null, permits,
                    onCallbackPool: false);
                return;
            }

            int sizeBudget = timeout - (int)(UtilAll.MonotonicMillis() - began);
            permits.SizeAcquired = sizeBudget > 0
                                   && _semaphoreAsyncSendSize.TryAcquire(msgLen, sizeBudget);
            if (!permits.SizeAcquired)
            {
                // 已经拿到的条数许可不能留在闸上（Java 靠两个标记做到这一点）
                CompleteAsync(callback, null,
                    new RemotingTooMuchRequestException(
                        "send message tryAcquire semaphoreAsyncSize timeout"), null, permits,
                    onCallbackPool: false);
                return;
            }
        }

        void Run()
        {
            // 出队之后才算真实耗时（Java 的 beginTimestampFirst 也是出队后取的）
            long cost = (long)(UtilAll.MonotonicMillis() - began);
            if (timeout <= cost)
            {
                CompleteAsync(callback, null,
                    new RemotingTooMuchRequestException("DEFAULT ASYNC send call timeout"),
                    null, permits, onCallbackPool: false);
                return;
            }

            try
            {
                SendAsyncInner(prepare(), mq, callback, permits, timeout - (int)cost);
            }
            catch (Exception e)
            {
                // Java：runnable 的 catch → newCallBack.onException(e)
                CompleteAsync(callback, null, e, null, permits, onCallbackPool: false);
            }
        }

        try
        {
            executor.Submit(Run);
        }
        catch (RejectedExecutionException)
        {
            if (_enableBackpressureForAsyncMode)
            {
                // Java :675-681：许可已经扣掉了，就地跑完这一笔（**阻塞调用方**），好让回调
                // 把许可还回来；否则队列一满就直接抛，白扣的容量还得等超时才还得回来。
                Run();
            }
            else
            {
                throw new MQClientException("executor rejected");
            }
        }
    }

    /// <summary>对应 Java <c>sendDefaultImpl(ASYNC)</c> 的准备工作（Python
    /// <c>_send_async_inner</c>）：校验、压缩、选队列。压缩在重试链<b>之外</b>做一次，
    /// 否则每轮把已压缩的 body 再压一遍。<c>timesTotal</c> 固定为 1（Java
    /// <c>sendDefaultImpl:756</c>）：换 broker 的重试全在 <c>onExceptionImpl</c> 里。</summary>
    private void SendAsyncInner(Message msg, MessageQueue? mq, ISendCallback callback,
        AsyncSendPermits permits, int timeout)
    {
        MQClientInstance c = GetClient();
        if (msg.IsBatch)
        {
            // 批量没有异步内核（Java 有，本端口的批量只有同步内核）：在池线程里同步发一批，
            // 结果照样从 CompleteAsync 走回调池转交。与 Python/Rust/C++ 同一处理。
            // mq 非空时必须把它传下去，否则「定点批量异步」会退化成轮询选队列。
            SendResult sent = mq is null ? Send(msg, timeout) : Send(msg, mq, timeout);
            CompleteAsync(callback, sent, null, null, permits);
            return;
        }

        if (_namespace.Length != 0)
        {
            msg.Topic = NamespaceUtil.WrapNamespace(_namespace, msg.Topic);
        }

        CheckMessage(msg);
        int sysFlag = PrepareForSend(msg);
        if (mq is not null)
        {
            // Java send(msg, mq, cb, timeout) → sendKernelImpl 定点发，传下去的
            // topicPublishInfo 是 null，所以失败只会在**同一台 broker** 上换 opaque 重试。
            SendKernelAsync(c, msg, mq, null, callback, permits, timeout, sysFlag);
            return;
        }

        TopicPublishInfo publish;
        try
        {
            publish = TryToFindTopicPublishInfo(c, msg.Topic);
        }
        catch (MQClientException e)
        {
            throw new MQClientException(e.Message, ClientErrorCode.NotFoundTopicException);
        }

        MessageQueue selected;
        try
        {
            selected = _mqFaultStrategy.SelectOneMessageQueue(publish, null, false);
        }
        catch (MQClientException e)
        {
            // Python 这里选不到队列报的是 `Send [0] times, still failed, Topic: …,
            // BrokersSent: []`；.NET 的选队列是抛异常，原始原因拼进同一段文本。
            throw new MQClientException("Send [0] times, still failed, Topic: " + msg.Topic
                                        + ", BrokersSent: [], last error: " + e.Message);
        }

        SendKernelAsync(c, msg, new MessageQueue(msg.Topic, selected.BrokerName, selected.QueueId),
            publish, callback, permits, timeout, sysFlag);
    }

    /// <summary>Java <c>sendKernelImpl</c> 的 ASYNC 分支：地址解析 → 拦截钩子 → traceparent →
    /// 建请求（一次）→ before 钩子 → 交给 <see cref="AsyncSendChain"/>。这一段上每个失败出口都
    /// 要归还许可，所以统一走 <see cref="CompleteAsync"/>。</summary>
    private void SendKernelAsync(MQClientInstance c, Message msg, MessageQueue mq,
        TopicPublishInfo? publish, ISendCallback callback, AsyncSendPermits permits, int timeout,
        int sysFlag)
    {
        double began = UtilAll.MonotonicMillis();
        // 地址解析两步，与 Java sendKernelImpl:919-924 一致：先查已缓存的发布地址，查不到再按
        // topic 刷一次路由重查。定点发送（调用方给了 mq）不会在 sendDefaultImpl 里取发布信息，
        // 这一步是它唯一的路由来源 —— 少了第一次定点发送必然拿到空地址。
        string addr = c.BrokerAddrOf(mq.BrokerName);
        if (addr.Length == 0)
        {
            try
            {
                TopicRouteData? route = c.GetTopicRouteData(mq.Topic);
                if (route is not null)
                {
                    addr = MQClientInstance.FindBrokerAddrInRoute(route, mq.BrokerName);
                }
            }
            catch (Exception)
            {
                // 刷路由失败交给下面统一报「broker 不存在」
            }
        }

        if (addr.Length == 0)
        {
            // Java sendKernelImpl:1100
            CompleteAsync(callback, null,
                new MQClientException("The broker[" + mq.BrokerName + "] not exist"), null, permits,
                onCallbackPool: false);
            return;
        }

        RunCheckForbidden(msg, mq, addr, null, CommunicationMode.Async);
        if (_enableTraceContext)
        {
            TraceParentContext.Inject(msg);
        }

        // 与同步内核同一口径：非批量消息在发请求之前补客户端唯一 ID，它决定
        // SendResult.MsgId，也是重试链里 parseSendResponse 读的那一份。
        if (!msg.IsBatch)
        {
            MessageClientIDSetter.SetUniqId(msg);
        }

        RemotingCommand request = c.BuildSendRequest(_producerGroup, msg, mq, sysFlag, _unitMode,
            _createTopicKey, _defaultTopicQueueNums);
        SendMessageContext? context = null;
        if (HasSendInterceptors())
        {
            context = BuildSendContext(msg, mq, addr);
            ExecuteSendMessageHookBefore(context);
        }

        // Java sendKernelImpl:1043-1046：ASYNC 分支自己的总闸 —— 钩子、压缩、路由都算耗时，
        // 预算被它们吃光就不再发起请求。RemotingTooMuchRequestException 是 RemotingException 的
        // 子类，所以 Java 在 :1088 先跑 hook.after 再抛给回调，这里用 CompleteAsync 复刻同一
        // 顺序（且**不重试**）。
        long cost = (long)(UtilAll.MonotonicMillis() - began);
        if (timeout < cost)
        {
            CompleteAsync(callback, null,
                new RemotingTooMuchRequestException("sendKernelImpl call timeout"), context, permits,
                onCallbackPool: false);
            return;
        }

        new AsyncSendChain(this, c, msg, mq, publish, callback, permits, context, request,
            addr, timeout - (int)cost).Attempt();
    }

    /// <summary>链的终点（Python <c>_complete</c>）：先跑 after 钩子，再归还背压许可，最后把
    /// 用户回调转交出去。<b>每个 AsyncSendChain 只走到一次</b>，所以用户回调恰好一次。
    ///
    /// <paramref name="onCallbackPool"/> 复刻 Python 的分岔：请求交给传输层<b>之前</b>的失败
    /// （闸门、排队超预算、校验、broker 不存在）本来就是 <c>_complete</c> 就地调用，用户回调
    /// 在当下这根线程上跑；只有**传输层带回来**的结果才 submit 给
    /// <c>NettyClientPublicExecutor</c>（Java 的 <c>executeInvokeCallback</c>）。</summary>
    private void CompleteAsync(ISendCallback callback, SendResult? result, Exception? error,
        SendMessageContext? context, AsyncSendPermits permits, bool onCallbackPool = true)
    {
        if (context is not null)
        {
            if (error is not null)
            {
                context.Exception = error;
            }
            else
            {
                context.SendResult = result;
            }

            ExecuteSendMessageHookAfter(context);
        }

        // 先还许可再交给用户（Java 的 BackpressureSendCallBack.semaphoreProcessor:599-610
        // 就是这个顺序，且先 size 后 num），否则用户回调里再发一笔异步消息会多占一格。
        permits.Release();
        if (onCallbackPool)
        {
            CompleteOnCallbackThread(callback, result, error);
            return;
        }

        InvokeSendCallback(callback, result, error);
    }

    /// <summary>真正调用用户回调，并把回调自己抛的异常吞掉（Java 两处都是
    /// <c>catch (Throwable)</c>），否则它会带走回调池的 worker。</summary>
    private static void InvokeSendCallback(ISendCallback callback, SendResult? result,
        Exception? error)
    {
        try
        {
            if (result is not null)
            {
                callback.OnSuccess(result);
            }
            else
            {
                callback.OnException(error?.Message ?? "unknown reason");
            }
        }
        catch (Exception e)
        {
            ClientLog.Warn("send callback raised: " + e);
        }
    }

    /// <summary>把用户回调放到 <c>NettyClientPublicExecutor_N</c> 上跑（Java
    /// <c>NettyRemotingAbstract.executeInvokeCallback</c>）。池已关（正在 Shutdown）或投不进去时
    /// 就地跑 —— 与 Python 的兜底同一条，不能让回调因为关池而凭空消失。</summary>
    private void CompleteOnCallbackThread(ISendCallback callback, SendResult? result,
        Exception? error)
    {
        ConsumeExecutor? pool;
        lock (_lock)
        {
            pool = _callbackExecutor;
        }

        if (pool is null)
        {
            InvokeSendCallback(callback, result, error);
            return;
        }

        try
        {
            pool.Submit(() => InvokeSendCallback(callback, result, error));
        }
        catch (RejectedExecutionException)
        {
            InvokeSendCallback(callback, result, error);
        }
    }

    /// <summary>本次异步发送真正拿到的背压许可（Java 的
    /// <c>isSemaphoreAsyncNumAcquired</c> / <c>isSemaphoreAsyncSizeAcquired</c> 两个标记）。
    /// 归还幂等，所以链上任何一条出口都能安全调用。</summary>
    private sealed class AsyncSendPermits
    {
        private readonly FairSemaphore _num;
        private readonly FairSemaphore _size;
        private readonly long _msgLen;
        private int _numAcquired;
        private int _sizeAcquired;

        public AsyncSendPermits(FairSemaphore num, FairSemaphore size, long msgLen)
        {
            _num = num;
            _size = size;
            _msgLen = msgLen;
        }

        public bool NumAcquired
        {
            get => Volatile.Read(ref _numAcquired) != 0;
            set => Volatile.Write(ref _numAcquired, value ? 1 : 0);
        }

        public bool SizeAcquired
        {
            get => Volatile.Read(ref _sizeAcquired) != 0;
            set => Volatile.Write(ref _sizeAcquired, value ? 1 : 0);
        }

        /// <summary>先还字节再还条数（Java semaphoreProcessor 的顺序）。</summary>
        public void Release()
        {
            if (Interlocked.Exchange(ref _sizeAcquired, 0) != 0)
            {
                _size.Release(_msgLen);
            }

            if (Interlocked.Exchange(ref _numAcquired, 0) != 0)
            {
                _num.Release(1);
            }
        }
    }

    /// <summary>一次异步发送的重试链（Java <c>sendMessageAsync</c> +
    /// <c>onExceptionImpl:683-734</c>）。请求<b>跨尝试复用同一个对象</b>、每轮换新 opaque，
    /// 超时预算是<b>所有尝试共享</b>的剩余时间，与 Java 一致。</summary>
    private sealed class AsyncSendChain
    {
        private readonly DefaultMQProducer _producer;
        private readonly MQClientInstance _client;
        // 只给 ProcessSendResponse 读 UNIQ_KEY：body 已经编进 request 了
        private readonly Message _msg;
        /// <summary>null = 定点发送（调用方给了 mq）：重试不换 broker，只在原地换 opaque。</summary>
        private readonly TopicPublishInfo? _publish;
        private readonly ISendCallback _callback;
        private readonly AsyncSendPermits _permits;
        private readonly SendMessageContext? _context;
        private readonly RemotingCommand _request;
        private MessageQueue _mq;
        private string _addr;
        private int _remaining;
        private double _attemptBegan;
        private int _times;

        public AsyncSendChain(DefaultMQProducer producer, MQClientInstance client, Message msg,
            MessageQueue mq, TopicPublishInfo? publish, ISendCallback callback,
            AsyncSendPermits permits, SendMessageContext? context, RemotingCommand request,
            string addr, int remaining)
        {
            _producer = producer;
            _client = client;
            _msg = msg;
            _mq = mq;
            _publish = publish;
            _callback = callback;
            _permits = permits;
            _context = context;
            _request = request;
            _addr = addr;
            _remaining = remaining;
        }

        /// <summary>Java <c>sendMessageAsync</c>：发出<b>本轮</b>尝试，结果由传输层回调带回。</summary>
        public void Attempt()
        {
            if (_times > 0)
            {
                // Java onExceptionImpl:728-730 `request.setOpaque(createNewRequestId())`：
                // 旧请求还挂在 responseTable 里等超时，复用 opaque 会把两次尝试的应答串台。
                _request.Opaque = RemotingCommand.NextOpaque();
            }

            _attemptBegan = UtilAll.MonotonicMillis();
            try
            {
                _client.InvokeAsyncOnAddr(_addr, _request, _remaining, OnResponse);
            }
            catch (Exception e)
            {
                // Python `producer.py:1274-1278`（Java sendMessageAsync 的外层 catch）：传输层
                // **就地**抛（连不上、写不出去）时异常**原样**传递、needRetry=true，且故障表记
                // 的是 `reachable=false`（这条连接根本没建立，不是"慢"）。.NET 的 InvokeAsync
                // 在 SendRequest 抛掉之前已把 opaque 从在途表摘掉，所以本轮不会再有第二次回调。
                long cost = (long)(UtilAll.MonotonicMillis() - _attemptBegan);
                _producer._mqFaultStrategy.UpdateFaultItem(_mq.BrokerName, cost, true, false);
                OnException(e, true, cost);
            }
        }

        /// <summary>Java <c>operationSucceed</c> / <c>operationFail</c>（Python
        /// <c>_handle</c>）：记故障表，成功就收尾，失败交给重试判断。</summary>
        private void OnResponse(RemotingCommand? response, Exception? error)
        {
            long cost = (long)(UtilAll.MonotonicMillis() - _attemptBegan);
            SendResult? sent = null;
            Exception? failure = error;
            if (failure is null && response is null)
            {
                failure = new RemotingException("unknown reason");
            }
            else if (failure is null)
            {
                try
                {
                    sent = _client.ProcessSendResponse(response!, _msg, _mq);
                }
                catch (Exception e)
                {
                    failure = e;
                }
            }

            if (failure is null)
            {
                _producer._mqFaultStrategy.UpdateFaultItem(_mq.BrokerName, cost, false, true);
                Finish(sent, null);
                return;
            }

            // updateFaultItem(…, isOver=true, …) 排在分类之前，和 Java 一样 —— 哪怕这一笔之后
            // 不重试，这台 broker 也要被记一次失败延迟。
            _producer._mqFaultStrategy.UpdateFaultItem(_mq.BrokerName, cost, true, true);
            (Exception wrapped, bool needRetry) = ClassifyAsyncFailure(failure, cost);
            OnException(wrapped, needRetry, cost);
        }

        /// <summary>Java <c>onExceptionImpl</c>：还能试就换一台 broker、复用请求重试，否则收尾。</summary>
        private void OnException(Exception error, bool needRetry, long cost)
        {
            _times++;
            int remaining = _remaining - (int)cost;
            if (!(needRetry && _times <= _producer._retryTimesWhenSendAsyncFailed && remaining > 0))
            {
                Finish(null, error);
                return;
            }

            // 换目标：Java 用 selectOneMessageQueue(tpInfo, brokerName, false) —— 第三参 false
            // 表示按 lastBrokerName **避开**刚失败的那台；选不到就沿用当前目标（Python 同）。
            string brokerName = _mq.BrokerName;
            if (_publish is not null)
            {
                try
                {
                    MessageQueue next = _producer._mqFaultStrategy.SelectOneMessageQueue(
                        _publish, brokerName, false);
                    _mq = new MessageQueue(_mq.Topic, next.BrokerName, next.QueueId);
                    brokerName = next.BrokerName;
                }
                catch (MQClientException)
                {
                    // 选不到就留在原目标上重试
                }
            }

            // Java onExceptionImpl:725 只查发布地址表、**不刷路由**；查不到就带着 null 撞进
            // invokeAsync。这里就地终止，别让空地址传进传输层。
            string addr = _client.BrokerAddrOf(brokerName);
            if (addr.Length == 0)
            {
                Finish(null, new MQClientException("The broker[" + brokerName + "] not exist"));
                return;
            }

            ClientLog.Warn("async send msg by retry " + _times.ToString(CultureInfo.InvariantCulture)
                           + " times. topic=" + _mq.Topic + ", brokerAddr=" + addr
                           + ", brokerName=" + brokerName + ": " + error.Message);
            _addr = addr;
            _remaining = remaining;
            Attempt();
        }

        /// <summary>链的终点：交给 <see cref="DefaultMQProducer.CompleteAsync"/>。</summary>
        private void Finish(SendResult? result, Exception? error)
        {
            _producer.CompleteAsync(_callback, result, error, _context, _permits);
        }

        /// <summary>Python <c>_classify_async_failure</c>（Java <c>operationFail</c> 的三分支）：
        /// 包装成 MQClientException 并判定能否换 broker 重试。
        ///
        /// ⚠ 只有<b>没收到响应</b>的失败走这里的分类。<b>已经收到响应</b>但
        /// <c>ProcessSendResponse</c> 判失败的 <see cref="MQBrokerException"/> 落在最后的
        /// 分支 —— Java 那条路径传的就是 <c>needRetry=false</c> 且原样抛出。也就是说异步发送
        /// <b>不看</b> RetryResponseCodes：broker 明确回了错就不会换 broker。别与同步语义混了。</summary>
        private static (Exception, bool) ClassifyAsyncFailure(Exception err, long cost)
        {
            switch (err)
            {
                case RemotingSendRequestException:
                    return (new MQClientException("send request failed, last error: " + err.Message),
                        true);
                case RemotingTimeoutException:
                    return (new MQClientException("wait response timeout, cost="
                                                  + cost.ToString(CultureInfo.InvariantCulture)
                                                  + ", last error: " + err.Message), true);
                // 其余 RemotingException 都是「unknown reason」，但 TooMuchRequest 是「自己人太多」，
                // 换 broker 也没用（Python 同一条判据）。
                case RemotingTooMuchRequestException:
                    return (new MQClientException("unknown reason, last error: " + err.Message),
                        false);
                // 连不上（RemotingConnectException）与编解码/命令错也属 RemotingException：
                // 换一台有机会。
                case RemotingConnectException:
                case RemotingCommandException:
                case RemotingException:
                    return (new MQClientException("unknown reason, last error: " + err.Message),
                        true);
                default:
                    return (err, false);
            }
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
        // 单向发送在 Java 里同样走 sendKernelImpl → CheckForbiddenHook 必须生效
        string brokerAddr = string.Empty;
        try
        {
            brokerAddr = c.BrokerAddrForMq(selected) ?? string.Empty;
        }
        catch (Exception)
        {
            // 查不到 broker 地址不影响拦截判定，brokerAddr 留空
        }

        RunCheckForbidden(outbound, selected, brokerAddr, null, CommunicationMode.Oneway);
        // W3C traceparent 透传（opt-in）：单向发送同样注入
        if (_enableTraceContext)
        {
            TraceParentContext.Inject(outbound);
        }

        c.SendMessageOneway(_producerGroup, outbound, selected, _sendMsgTimeout, sysFlag, _unitMode,
            _createTopicKey, _defaultTopicQueueNums);
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

        // 对应 Java DefaultMQProducer.batch()：**每条子消息**都过一遍 Validators.checkMessage，
        // 少这一步等于批量路径绕过了所有本地校验——超长/空 body/非法 topic 都能发出去。
        foreach (Message m in msgs)
        {
            Validators.CheckMessage(m, _maxMessageSize);
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

    /// <summary>
    /// 批量异步发送：对应 Java <c>DefaultMQProducer.send(Collection&lt;Message&gt;, SendCallback,
    /// timeout)</c>（:1121）→ <c>defaultMQProducerImpl.send(batch(msgs), sendCallback, timeout)</c>，
    /// 也就是 Python <c>send_async</c> 收到 list 时走进的那个批量分支。
    ///
    /// 本端口的批量只有同步内核，所以「异步」= 在 <c>AsyncSenderExecutor_N</c> 线程里跑完
    /// 同步批量内核、结果照样从 <c>NettyClientPublicExecutor_N</c> 交付回调。对调用方语义没
    /// 差别：不阻塞提交线程、回调恰好一次、失败照样归还许可。
    /// <see cref="SendAsync(Message,ISendCallback,int,MessageQueue?)"/> 的整段前段（排队、
    /// 预算复检、背压两道闸）在这里全部生效，字节许可按<b>整批每条子消息</b>的 body 长度累加
    /// （空 body 也算 1、空列表算 1，Python <c>_back_pressure_msg_len</c> 的 list 分支同一
    /// 公式）—— 只按一条扣等于一批整体占 1 格，字节闸对批量形同虚设。
    ///
    /// <paramref name="mq"/> 非空时整批定点落到该队列（Java
    /// <c>send(Collection, MessageQueue, SendCallback, timeout)</c>）。
    ///
    /// 与 <see cref="SendBatch"/> 一样，<b>每条子消息</b>先过 <c>Validators.CheckMessage</c>：
    /// 直接把 <see cref="MessageBatch"/> 丢给 <see cref="SendAsync(Message,ISendCallback,int,MessageQueue?)"/>
    /// 会漏掉这一步，等于批量路径绕过本地校验（超长 body、非法 topic 都能发出去）。
    /// 校验/组批的失败（空列表、超长、不同质）一律**进回调**，与 Python 一致：它们发生在
    /// 池线程上的组批步骤里，被异步链的外层 catch 接住。
    /// </summary>
    public void SendBatchAsync(List<Message> msgs, ISendCallback callback, int timeoutMillis = -1,
        MessageQueue? mq = null)
    {
        // 许可份数在调用方线程上算（Python 也在投队列前算好），公式与 _back_pressure_msg_len
        // 的 list 分支一致：逐条累加、空 body 也算 1、空列表算 1
        long msgLen = 0;
        if (msgs is not null)
        {
            foreach (Message m in msgs)
            {
                msgLen += m.Body.Length == 0 ? 1 : m.Body.Length;
            }
        }

        if (msgLen == 0)
        {
            msgLen = 1;
        }

        ExecuteAsyncSend(() => BuildBatchMessage(msgs), msgLen, callback, timeoutMillis, mq);
    }

    /// <summary>批量异步的组批步骤（在池线程上跑）：与 <see cref="SendBatch"/> 开头完全同一串
    /// 动作 —— 逐条 <c>Validators.CheckMessage</c> → <c>GenerateFromList</c>（查同质性、逐条补
    /// UNIQ_KEY、把整批编进 body）→ 整批再查一次。这里的异常由异步链外层交给回调。</summary>
    private Message BuildBatchMessage(List<Message>? msgs)
    {
        if (msgs is null || msgs.Count == 0)
        {
            throw new MQClientException("message list is empty");
        }

        foreach (Message m in msgs)
        {
            Validators.CheckMessage(m, _maxMessageSize);
        }

        MessageBatch batch = MessageBatch.GenerateFromList(msgs);
        CheckMessage(batch);
        return batch;
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

    /// <summary>撤回一条定时/延迟消息，返回被撤回消息的 uniqKey。
    ///
    /// 校验顺序与 Java <c>DefaultMQProducerImpl#recallMessage</c>(:1570-1601) 逐条对齐：
    /// 状态 → checkTopic → 禁 retry/DLQ → 解句柄 → 预热路由 → 定位 broker → 发请求。
    /// 前三步必须在打网络<b>之前</b>跑完，否则一个手滑的句柄就要耗掉一次 RPC 超时。
    /// 句柄来自定时消息的 <see cref="SendResult.RecallHandle"/>，普通消息没有。</summary>
    /// <remarks>
    /// 与 Java 的差异：Java 的 <c>findBrokerAddrByTopic</c> 返回该 topic 的<b>全部</b> broker
    /// 地址再随机取一个，这里直接取路由里的第一个可用地址——单 broker 场景等价，多 broker
    /// 场景两者都只会命中句柄里那个 broker 之外的地址，最终由 broker 用
    /// <c>ILLEGAL_OPERATION</c>（brokerName 不匹配）拒绝，语义不变。
    /// </remarks>
    public string RecallMessage(string topic, string recallHandle)
    {
        MQClientInstance c = GetClient();
        string realTopic = _namespace.Length == 0
            ? topic
            : NamespaceUtil.WrapNamespace(_namespace, topic);
        Validators.CheckTopic(realTopic);
        if (NamespaceUtil.IsRetryTopic(realTopic) || NamespaceUtil.IsDlqTopic(realTopic))
        {
            throw new MQClientException("topic is not supported");
        }

        HandleV1 handle = RecallMessageHandle.DecodeHandle(recallHandle);

        // Java 只是调用 tryToFindTopicPublishInfo 预热路由，返回值并不使用，但**异常照抛**
        // （DefaultMQProducerImpl:1586）—— 连路由都拿不到时，后面的 broker 定位也没有意义。
        TryToFindTopicPublishInfo(c, realTopic);

        // Java findBrokerAddressInPublish(brokerName) → 退化到 findBrokerAddrByTopic(topic)
        string addr = c.BrokerAddrOf(handle.BrokerName);
        if (addr.Length == 0)
        {
            TopicRouteData? route = c.GetTopicRouteData(realTopic);
            if (route is not null)
            {
                foreach (BrokerData bd in route.BrokerDatas)
                {
                    addr = bd.SelectBrokerAddr();
                    if (addr.Length > 0)
                    {
                        break;
                    }
                }
            }
        }

        if (addr.Length == 0)
        {
            ClientLog.Warn("can't find broker service address. " + handle.BrokerName);
            throw new MQClientException("The broker service address not found");
        }

        var header = new RecallMessageRequestHeader
        {
            ProducerGroup = _producerGroup,
            Topic = realTopic,
            RecallHandle = recallHandle,
            // 继承字段在 Java 里反射名就是 bname，写成 brokerName 会被 broker 静默丢掉。
            Bname = handle.BrokerName,
        };
        return c.RecallMessage(addr, header, _sendMsgTimeout);
    }

    public void CreateTopic(string key, string newTopic, int queueNum = 4)
    {
        MQClientInstance c = GetClient();
        const int perm = 6; // PERM_READ | PERM_WRITE
        // 对应 Java DefaultMQProducerImpl.createTopic：先本地校验原始 topic 名，再查系统 topic
        Validators.CheckTopic(newTopic);
        Validators.IsSystemTopic(newTopic);
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
