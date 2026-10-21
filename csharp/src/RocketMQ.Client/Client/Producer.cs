// Capability coverage: synchronous send (round-robin queue selection / pinned send),
// selector-based send (ordered messages), asynchronous send, oneway send, batch send,
// transactional messages (two phases: half message + check-back), query by key,
// offset query, topic creation.
//
// Implemented item by item per the protocol convention: retry counts, queue selection,
// the compression threshold 4096, and timeout/exception mapping are all protocol
// behavior, documented in the comments below.
using System.Globalization;
using System.Threading;
using RocketMQ.Common;
using RocketMQ.Remoting;
using RocketMQ.Remoting.Protocol;

namespace RocketMQ.Client;

public class DefaultMQProducer
{
    private readonly object _lock = new();
    private MQClientInstance? _mqClient;
    private bool _started;

    /// <summary><see cref="Shutdown"/> has already been called: used to reject new requests while
    /// "draining in-flight sends", and to make <see cref="GetClient"/> report the
    /// <c>SHUTDOWN_ALREADY</c> message.</summary>
    private volatile bool _shutdownRequested;

    private string _producerGroup;
    private string _instanceName = MixAll.DefaultInstanceName;
    private string _clientId = string.Empty;
    // Namespace (multi-tenant isolation): when non-empty, sends wrap the topic as
    // "ns%topic" to the broker. Default empty = no namespace (compatible with a bare
    // cluster, does not break existing behavior).
    private string _namespace = string.Empty;
    private string _namespaceV2 = string.Empty;
    // ACL hook, bound to MQClientInstance's transport layer in Start()
    private IRpcHook? _rpcHook;
    // ClientConfig's three unit/stream switches (all off by default)
    private string _unitName = string.Empty;
    private bool _unitMode;
    private bool _enableStreamRequestType;
    // Default value of pollNameServerInterval
    private int _pollNameServerIntervalMillis = 30000;
    // ClientConfig knobs shared by every facade: body materialisation during batch
    // decode, the internal-API timeout, and the concurrent-heartbeat dispatch.
    private bool _decodeReadBody = true;
    private int _mqClientApiTimeout = MQClientInstance.MqClientApiTimeoutMillis;
    private bool _enableConcurrentHeartbeat;
    private int _concurrentHeartbeatThreadPoolSize = Environment.ProcessorCount;
    private string _createTopicKey = MixAll.DefaultTopic;
    private int _defaultTopicQueueNums = MixAll.DefaultTopicQueueNums;
    private int _sendMsgTimeout = 3000;
    // TLS (switch name tls.enable; falls back to env ROCKETMQ_TLS_ENABLE)
    // Note the env must be read at field-initialization time: MQClientInstance takes a bool
    // (not bool?), and passing false would override RemotingClient's env fallback.
    private bool _tlsEnable = MQClientInstance.TlsEnabledFromEnv();
    // W3C traceparent propagation (opt-in; falls back to env ROCKETMQ_TRACE_CONTEXT_ENABLE)
    private bool _enableTraceContext;
    private int _retryTimesWhenSendFailed = 2;
    // sendMsgMaxTimeoutPerRequest (default -1 = unlimited): when retries remain, the
    // per-request timeout is clamped to this value, leaving the rest of the total
    // budget for later brokers.
    private int _sendMsgMaxTimeoutPerRequest = -1;
    // Retryable broker response codes (the default set of retryResponseCodes)
    private readonly HashSet<int> _retryResponseCodes = new()
    {
        ResponseCode.SystemError, ResponseCode.SystemBusy,
        ResponseCode.ServiceNotAvailable, ResponseCode.NoPermission,
        ResponseCode.TopicNotExist, ResponseCode.NoBuyerId,
        ResponseCode.NotInCurrentUnit, ResponseCode.GoAway,
    };
    // retryAnotherBrokerWhenNotStoreOK: whether to switch to another broker when the
    // status is not SEND_OK
    private bool _retryAnotherBrokerWhenNotStoreOk;
    // Send latency fault avoidance (MQFaultStrategy, disabled by default)
    private readonly MQFaultStrategy _mqFaultStrategy = new(false);
    private int _maxMessageSize = 1024 * 1024 * 4;
    // Compression config
    private int _compressMsgBodyOverHowmuch = 1024 * 4;
    private int _compressLevel = 5;
    private int _compressType = CompressionType.ZLIB;
    // Request-Reply default reply wait timeout (ms): 3000.
    private int _requestTimeout = 3000;
    private List<string> _nameServerAddrs = new();
    // ---------------- Message trace config ----------------
    // When enabled, Start() builds an AsyncTraceDispatcher and registers Send/EndTransaction
    // hooks; the internal trace producer itself stays disabled (EnableTrace=false),
    // otherwise infinite recursion.
    private bool _enableTrace;
    private string _traceTopic = MixAll.TraceTopic;
    private int _traceMsgBatchNum = 10;
    private readonly List<ISendMessageHook> _sendMessageHooks = new();
    private readonly List<IEndTransactionHook> _endTransactionHooks = new();
    private readonly List<ICheckForbiddenHook> _checkForbiddenHooks = new();
    private AsyncTraceDispatcher? _traceDispatcher;
    // ---------------- Async send pool ----------------
    // AsyncSenderExecutor: moves the preparation work (fetch routes, select queue, build
    // request, retry on another broker) off the caller thread. core==max==CPU cores, and
    // the queue is **bounded** (LinkedBlockingQueue(50000) spec).
    // NettyClientPublicExecutor: user callbacks and SendMessageHook.after run here; business
    // code must never occupy the connection read thread or the timeout-cleanup thread
    // (executeInvokeCallback always submits to publicExecutor).
    // Both pools keepAlive 60s with core==max, so threads are created on demand and never
    // exit once created.
    private ConsumeExecutor? _asyncSenderExecutor;
    /// <summary>Only built-in pools are drained by <see cref="Shutdown"/> (a caller-supplied
    /// pool's lifecycle belongs to the caller).</summary>
    private bool _asyncSenderExecutorOwned = true;
    private ConsumeExecutor? _callbackExecutor;
    // Async retry count and send queue capacity
    private int _retryTimesWhenSendAsyncFailed = 2;
    private int _asyncSenderQueueCapacity = 50000;
    // Callback pool thread count, 0 = use CPU core count
    private int _clientCallbackExecutorThreads;
    // ---------------- Async send backpressure ----------------
    // The switch is off by default. The two semaphores are built at **construction** time
    // (not Start()): the switch may be turned on at runtime, and by then the gates must
    // already exist.
    private bool _enableBackpressureForAsyncMode;
    private int _backPressureForAsyncSendNum = 1024;
    private int _backPressureForAsyncSendSize = 100 * 1024 * 1024;
    private readonly FairSemaphore _semaphoreAsyncSendNum;
    private readonly FairSemaphore _semaphoreAsyncSendSize;
    // ---------------- Auto-batching (ProduceAccumulator) ----------------
    // When enabled, Send(Message) first merges messages into a MessageBatch by AggregateKey
    // before sending (see the header of ProduceAccumulator.cs).
    // The three -1s are "caller has not spoken" sentinels: InitProduceAccumulator() only
    // feeds values > -1 to the accumulator, so values set in the constructor are not
    // overwritten by defaults.
    private bool _autoBatch;
    private int _batchMaxDelayMs = -1;
    private long _batchMaxBytes = -1;
    private long _totalBatchMaxBytes = -1;
    /// <summary>Accumulator reused by clientId (getOrCreateProduceAccumulator).</summary>
    private ProduceAccumulator? _produceAccumulator;

    // Initial permits when building the backpressure semaphores: when the config is
    // **not higher than** the floor, the floor is used and an info line is logged --
    // the branch is `cfg > 10 ? new Semaphore(max(cfg, 10)) : new
    // Semaphore(10)`, so exactly at the floor the log is also printed.
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

    // How many "bytes" of permits to deduct (the convention of executeAsyncMessageSend
    // `msg.getBody() == null ? 1 : msg.getBody().length`): not counting 1 for an empty
    // body would mean no throttling at all.
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
        // The two fair semaphores are built when the instance is created
        _semaphoreAsyncSendNum = new FairSemaphore(
            BackPressurePermits(_backPressureForAsyncSendNum, FairSemaphore.MinAsyncSendNum,
                "semaphoreAsyncSendNum"));
        _semaphoreAsyncSendSize = new FairSemaphore(
            BackPressurePermits(_backPressureForAsyncSendSize, FairSemaphore.MinAsyncSendSize,
                "semaphoreAsyncSendSize"));
        // opt-in; falls back to env ROCKETMQ_TRACE_CONTEXT_ENABLE
        _enableTraceContext = TraceParentContext.EnabledFromEnv();
    }

    /// <summary>Finalizer: shutdown as a fallback on release.</summary>
    ~DefaultMQProducer()
    {
        try
        {
            Shutdown();
        }
        catch (Exception)
        {
            // finalizer does not throw
        }
    }

    // ---------------- Config ----------------
    // Semicolon-separated nameServer address string, e.g. "127.0.0.1:9876"
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

    /// <summary>TLS (switch name tls.enable; falls back to env ROCKETMQ_TLS_ENABLE).</summary>
    public bool TlsEnable
    {
        get => _tlsEnable;
        set => _tlsEnable = value;
    }

    private TlsOptions? _tlsOptions;

    /// <summary>
    /// TLS details (CaCert=strict CA validation; ClientCert/ClientKey=mTLS; ServerName=hostname
    /// override). Leaving CaCert unset = test-mode (trust self-signed).
    /// </summary>
    public TlsOptions? TlsOptions
    {
        get => _tlsOptions;
        set => _tlsOptions = value;
    }

    private SocksProxyConfig? _socksProxyConfig;

    public SocksProxyConfig? SocksProxyConfig
    {
        get => _socksProxyConfig;
        set => _socksProxyConfig = value;
    }

    /// <summary>W3C traceparent propagation (opt-in; falls back to env ROCKETMQ_TRACE_CONTEXT_ENABLE).</summary>
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

    /// <summary><c>retryTimesWhenSendAsyncFailed</c> (default 2):
    /// the retry count for **async** sends, configured separately from the sync one. Only
    /// effective on the async chain: switch to another broker and retry the same request
    /// with a new opaque, capped at this number (the first attempt is not counted). Async
    /// sends do **not** consult <see cref="RetryTimesWhenSendFailed"/> nor
    /// <c>RetryResponseCodes</c> -- once the broker explicitly returns an error code, no
    /// broker switch happens (<c>needRetry</c> in <c>onExceptionImpl</c> is true only
    /// when "no response was received").</summary>
    public int RetryTimesWhenSendAsyncFailed
    {
        get => _retryTimesWhenSendAsyncFailed;
        set => _retryTimesWhenSendAsyncFailed = value;
    }

    /// <summary>The hard-wired <c>LinkedBlockingQueue(50000)</c>:
    /// async send queue capacity. Read only once in <see cref="Start"/> (fixed at
    /// construction). When the queue is full, <c>SendAsync</c> throws
    /// <c>MQClientException("executor rejected")</c> to the caller; with backpressure
    /// enabled, that one send runs to completion in place.</summary>
    public int AsyncSenderQueueCapacity
    {
        get => _asyncSenderQueueCapacity;
        set => _asyncSenderQueueCapacity = value;
    }

    /// <summary>Semantics of <c>setAsyncSenderExecutor</c> + <c>getAsyncSenderExecutor</c>:
    /// when a caller-supplied async send pool is provided, the built-in pool is not
    /// created, and <see cref="Shutdown"/> does **not** wait for it nor close it (only
    /// <c>defaultAsyncSenderExecutor.shutdown()</c>) -- the pool's lifecycle belongs to
    /// the caller. Must be set before <see cref="Start"/> (the pool is fixed in Start).</summary>
    public ConsumeExecutor? AsyncSenderExecutor { get; set; }

    /// <summary>publicExecutor thread count:
    /// how many threads user callbacks and after hooks run on. 0 (default) = CPU cores;
    /// must be set before <see cref="Start"/>, the pool is built at startup.</summary>
    public int ClientCallbackExecutorThreads
    {
        get => _clientCallbackExecutorThreads;
        set => _clientCallbackExecutorThreads = value;
    }

    /// <summary>
    /// setSendMsgMaxTimeoutPerRequest. -1 (default) means no cap on a single request;
    /// after setting a finite value, the per-request timeouts of the attempts that
    /// **still have retries left** are clamped to this value -- otherwise one slow
    /// broker could eat the entire SendMsgTimeout budget, leaving the remaining
    /// brokers untried.
    /// </summary>
    public int SendMsgMaxTimeoutPerRequest
    {
        get => _sendMsgMaxTimeoutPerRequest;
        set => _sendMsgMaxTimeoutPerRequest = value;
    }

    /// <summary>retryAnotherBrokerWhenNotStoreOK (default false).</summary>
    public bool RetryAnotherBrokerWhenNotStoreOk
    {
        get => _retryAnotherBrokerWhenNotStoreOk;
        set => _retryAnotherBrokerWhenNotStoreOk = value;
    }

    /// <summary>getRetryResponseCodes (returns a copy; to modify the set use
    /// AddRetryResponseCode).</summary>
    public HashSet<int> RetryResponseCodes => new(_retryResponseCodes);

    /// <summary>addRetryResponseCode: only these broker response codes are worth
    /// switching brokers to resend.</summary>
    public void AddRetryResponseCode(int responseCode)
    {
        lock (_retryResponseCodes)
        {
            _retryResponseCodes.Add(responseCode);
        }
    }

    /// <summary>
    /// Retryable response-code check: null (no response code received at all) means
    /// not retryable.
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

    // ---------------- Fault avoidance (sendLatencyFaultEnable, off by default) ----------------
    // When enabled, send queue selection filters by broker latency/isolation state
    // (MQFaultStrategy); send results write back to the fault table: success records the
    // measured latency (over-threshold isolates that broker for a while), exceptions
    // record the 10000ms isolation tier.
    public bool SendLatencyFaultEnable
    {
        get => _mqFaultStrategy.IsSendLatencyFaultEnable();
        set => _mqFaultStrategy.SetSendLatencyFaultEnable(value);
    }

    /// <summary><c>startDetectorEnable</c> (default false):
    /// when enabled, <see cref="Start"/> starts MQFaultStrategy's reachability detector
    /// thread -- TCP connectivity probes at a 2s-per-broker cadence (detectTimeout=200ms);
    /// a recovered failed broker can be pulled back into the fault-tolerance table early
    /// (LatencyFaultTolerance).</summary>
    public bool StartDetectorEnable
    {
        get => _mqFaultStrategy.IsStartDetectorEnable();
        set => _mqFaultStrategy.SetStartDetectorEnable(value);
    }

    /// <summary><c>sendMessageWithVIPChannel</c> (default false):
    /// when enabled, the broker port for sends/requests switches to the VIP port
    /// (normal port -2, see <c>MixAll.BrokerVipChannel</c>).
    /// The consumer side corresponds to <c>ClientConfig#vipChannelEnabled</c> (push
    /// consumers pass it through via <c>ClientConfig</c>, also false by default).</summary>
    public bool SendMessageWithVIPChannel
    {
        get => _sendMessageWithVIPChannel;
        set
        {
            _sendMessageWithVIPChannel = value;
            // The switch semantics require per-message fresh reads and immediate effect;
            // this port's rewrite point is on MQClientInstance's send address, so changing
            // the switch after Start() must also take effect immediately.
            if (_mqClient is not null)
            {
                _mqClient.VipChannelEnabled = value;
            }
        }
    }

    private bool _sendMessageWithVIPChannel;

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

    // Namespace (setNamespace): when non-empty, wrap the topic as "ns%topic" before
    // sending to the broker (except %RETRY%/%DLQ% prefixes; system resources are not
    // wrapped).
    public string Namespace
    {
        get => _namespace;
        set => _namespace = value ?? string.Empty;
    }

    /// <summary>5.x new namespace (namespaceV2): when non-empty,
    /// <see cref="NamespaceRpcHook"/> (head of the hook chain) adds <c>nsd=true</c> /
    /// <c>ns</c> extension headers to every request, and it is propagated to the trace
    /// dispatcher via <see cref="StartTraceDispatcher"/> (the trace producer likewise
    /// gets setNamespaceV2). The hook reads this property fresh per request, so setting
    /// it after Start() takes effect from the next request on.</summary>
    public string NamespaceV2
    {
        get => _namespaceV2;
        set => _namespaceV2 = value ?? string.Empty;
    }

    // ---------------- unitName / unitMode / enableStreamRequestType ----------------
    // Three same-named switches. ⚠ Must be set before Start(): unitName and
    // @STREAM determine the shape of clientId, and stream determines the request hook
    // chain (ReqT goes into the ACL signature content).
    /// <summary>Unit name: goes into the <c>@&lt;unitName&gt;</c> segment of clientId, and is
    /// also concatenated into the dynamic addressing URL.</summary>
    public string UnitName
    {
        get => _unitName;
        set => _unitName = value ?? string.Empty;
    }

    /// <summary>
    /// <c>isUnitMode()</c>. On the producer path it lands in exactly one place:
    /// <c>requestHeader.setUnitMode(...)</c>
    /// → the single-letter key <c>k</c> of SEND_MESSAGE_V2; the broker marks
    /// auto-created topics with the UNIT(0x1) / UNIT_SUB(0x2) system flags accordingly.
    /// The producer never sets it true on its own, so it defaults to false.
    /// </summary>
    public bool UnitMode
    {
        get => _unitMode;
        set => _unitMode = value;
    }

    /// <summary>
    /// When true, every request carries <c>ReqT=0</c> and clientId gains a trailing
    /// <c>@STREAM</c> segment. Only pull / lite consumers set it true in their
    /// constructors; producers default to false.
    /// </summary>
    public bool EnableStreamRequestType
    {
        get => _enableStreamRequestType;
        set => _enableStreamRequestType = value;
    }

    /// <summary>
    /// Default 30000ms: the route refresh period for in-use topics, passed through to
    /// MQClientInstance at Start() (changing it afterwards does not reschedule the
    /// already-started periodic task).
    /// </summary>
    public int PollNameServerIntervalMillis
    {
        get => _pollNameServerIntervalMillis;
        set => _pollNameServerIntervalMillis = value;
    }

    /// <summary>When false, received pull/POP bodies are not materialised: frames are
    /// still walked and messages returned header-only. Default true.</summary>
    public bool DecodeReadBody
    {
        get => _decodeReadBody;
        set
        {
            _decodeReadBody = value;
            if (_mqClient is not null)
            {
                _mqClient.DecodeReadBody = value;
            }
        }
    }

    /// <summary>Internal API timeout (default 3000ms); an explicit per-call timeout wins.</summary>
    public int MqClientApiTimeout
    {
        get => _mqClientApiTimeout;
        set
        {
            _mqClientApiTimeout = value > 0 ? value : MQClientInstance.MqClientApiTimeoutMillis;
            if (_mqClient is not null)
            {
                _mqClient.MqClientApiTimeout = _mqClientApiTimeout;
            }
        }
    }

    /// <summary>Fan heartbeat rounds out over a pool (default off; V2 still wins).</summary>
    public bool EnableConcurrentHeartbeat
    {
        get => _enableConcurrentHeartbeat;
        set
        {
            _enableConcurrentHeartbeat = value;
            if (_mqClient is not null)
            {
                _mqClient.EnableConcurrentHeartbeat = value;
            }
        }
    }

    /// <summary>Pool size for concurrent heartbeats (default: available processors).</summary>
    public int ConcurrentHeartbeatThreadPoolSize
    {
        get => _concurrentHeartbeatThreadPoolSize;
        set
        {
            _concurrentHeartbeatThreadPoolSize = value > 0 ? value : Environment.ProcessorCount;
            if (_mqClient is not null)
            {
                _mqClient.ConcurrentHeartbeatThreadPoolSize = _concurrentHeartbeatThreadPoolSize;
            }
        }
    }

    /// <summary>Paged metadata size (maxTopicNum / maxGroupNum). Default 2000.</summary>
    public int MaxPageSizeInGetMetadata
    {
        get => _maxPageSizeInGetMetadata;
        set
        {
            _maxPageSizeInGetMetadata = value > 0 ? value : MQClientInstance.DefaultMaxPageSizeInGetMetadata;
            if (_mqClient is not null)
            {
                _mqClient.MaxPageSizeInGetMetadata = _maxPageSizeInGetMetadata;
            }
        }
    }

    private int _maxPageSizeInGetMetadata = MQClientInstance.DefaultMaxPageSizeInGetMetadata;

    // ---------------- ACL authentication ----------------
    // Must be called before Start(): the hook is bound to MQClientInstance in Start()
    // (when the instance is reused for the same clientId, first registrant wins).
    public void SetRpcHook(IRpcHook hook) => _rpcHook = hook;

    /// <summary>Convenience entry: build an AclClientRPCHook from accessKey/secretKey
    /// (optional securityToken).</summary>
    public void SetCredentials(string accessKey, string secretKey, string securityToken = "")
        => _rpcHook = new AclClientRPCHook(new SessionCredentials(accessKey, secretKey, securityToken));

    // ---------------- Message trace config (trace-related properties) ----------------
    // When enabled, Start() builds an AsyncTraceDispatcher and registers Send/EndTransaction
    // trace hooks; the trace producer used internally by the dispatcher itself stays
    // EnableTrace=false (see AsyncTraceDispatcher), otherwise infinite recursion.
    public bool EnableTrace
    {
        get => _enableTrace;
        set => _enableTrace = value;
    }

    /// <summary>Custom trace topic (setTraceTopic); empty falls back to the system default
    /// RMQ_SYS_TRACE_TOPIC.</summary>
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

    /// <summary>Register a send hook (registerSendMessageHook).</summary>
    public void RegisterSendMessageHook(ISendMessageHook hook)
    {
        if (hook is not null)
        {
            _sendMessageHooks.Add(hook);
        }
    }

    public bool HasSendMessageHook() => _sendMessageHooks.Count > 0;

    /// <summary>Register a pre-send interception hook (registerCheckForbiddenHook).</summary>
    public void RegisterCheckForbiddenHook(ICheckForbiddenHook hook)
    {
        if (hook is not null)
        {
            _checkForbiddenHooks.Add(hook);
        }
    }

    public bool HasCheckForbiddenHook() => _checkForbiddenHooks.Count > 0;

    public int CheckForbiddenHookCount() => _checkForbiddenHooks.Count;

    /// <summary>Whether the send kernel "with interception/hooks" must be used (either one
    /// existing is enough).</summary>
    public bool HasSendInterceptors() => _sendMessageHooks.Count > 0 || _checkForbiddenHooks.Count > 0;

    /// <summary>For unit tests / integration debugging to drive hook execution directly
    /// (without going through the network).
    ///
    /// ⚠ **Opposite** of the Send/Consume/EndTransaction hooks: exceptions are
    /// <b>not swallowed</b> here -- exceptions thrown by hooks propagate out as-is (the
    /// CheckForbiddenHook signature is <c>throws MQClientException</c>); this is exactly
    /// how "forbidden to send" is implemented.
    /// </summary>
    public void ExecuteCheckForbiddenHook(CheckForbiddenContext context)
    {
        foreach (ICheckForbiddenHook hook in _checkForbiddenHooks)
        {
            hook.CheckForbidden(context);
        }
    }

    /// <summary>Register a transaction-finish hook (registerEndTransactionHook).</summary>
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

    // Compression config
    // Auto-compress when body length >= this threshold (default 4096); batch messages
    // are never compressed.
    public int CompressMsgBodyOverHowmuch
    {
        get => _compressMsgBodyOverHowmuch;
        set => _compressMsgBodyOverHowmuch = value;
    }

    // Compression level, only meaningful for ZLIB (default 5)
    public int CompressLevel
    {
        get => _compressLevel;
        set => _compressLevel = value;
    }

    // Compression algorithm: CompressionType::ZLIB / LZ4 / ZSTD (default ZLIB)
    public int CompressType
    {
        get => _compressType;
        set => _compressType = value;
    }

    // ---------------- Async send backpressure ----------------
    // The switch is off by default and is **not** a startup-time config: it can be
    // toggled mid-run, because only the gates themselves read it (see SendAsync).
    public bool EnableBackpressureForAsyncMode
    {
        get => _enableBackpressureForAsyncMode;
        set => _enableBackpressureForAsyncMode = value;
    }

    /// <summary>
    /// Change the "in-flight count" cap at runtime (setBackPressureForAsyncSendNum). The
    /// semantics are not "set to num" but "the total becomes num, the already in-flight
    /// ones are kept as-is", so after shrinking it
    /// <see cref="SemaphoreAsyncSendNumAvailablePermits" /> may go negative. Clamped to
    /// the floor value 10 when below it.
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

    /// <summary>In-flight byte cap, floor value 1M, same semantics as
    /// <see cref="BackPressureForAsyncSendNum" />.</summary>
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

    /// <summary>For observation: currently available permits; negative means in-flight
    /// overshoot.</summary>
    public long SemaphoreAsyncSendNumAvailablePermits => _semaphoreAsyncSendNum.AvailablePermits();

    public long SemaphoreAsyncSendSizeAvailablePermits => _semaphoreAsyncSendSize.AvailablePermits();

    // Request-Reply: timeout for waiting for a reply (default 3000ms).
    // Any value <= 0 falls back to the default 3000, avoiding setting the send-path
    // timeout to 0.
    public int RequestTimeout
    {
        get => _requestTimeout;
        set => _requestTimeout = value > 0 ? value : 3000;
    }

    /// <summary>This client's identifier (the result of <c>buildMQClientId</c>). Writable:
    /// when non-empty, <see cref="Start"/> no longer builds its own and reuses this
    /// (the accumulator is reused by it, so tests can each have their own).</summary>
    public string ClientId
    {
        get => _clientId;
        set => _clientId = value;
    }

    public bool IsStarted => _started;

    // ---------------- Auto-batching parameters ----------------
    // ⚠ All four getters return the "not enabled" tier when the accumulator has not been
    // built yet (= never Started):
    //   getAutoBatch→false, the three byte/latency getters→0; the semantics are verbatim,
    //   so values read before Start() are **unrelated** to what was set -- do not use
    //   them as config readback.
    public bool AutoBatch
    {
        get => _produceAccumulator is not null && _autoBatch;
        set
        {
            _autoBatch = value;
            ClientLog.Info("Set autoBatch=" + value);
        }
    }

    public bool GetAutoBatch() => AutoBatch;

    /// <summary>Accumulator instance (produceAccumulator). Normally established by
    /// <see cref="InitProduceAccumulator"/> inside <see cref="Start"/>; it is writable so
    /// tests can inject one without Start (unit tests likewise poke the field
    /// directly).</summary>
    public ProduceAccumulator? ProduceAccumulator
    {
        get => _produceAccumulator;
        set => _produceAccumulator = value;
    }

    /// <summary><c>batchMaxDelayMs(int)</c>: first record on this object; if it is an
    /// already-Started object, sync to the accumulator immediately.</summary>
    public int BatchMaxDelayMs
    {
        get => _produceAccumulator is null ? 0 : _produceAccumulator.GetBatchMaxDelayMs();
        set
        {
            _batchMaxDelayMs = value;
            if (_produceAccumulator is not null)
            {
                _produceAccumulator.BatchMaxDelayMs(value);
            }
        }
    }

    public long BatchMaxBytes
    {
        get => _produceAccumulator is null ? 0 : _produceAccumulator.GetBatchMaxBytes();
        set
        {
            _batchMaxBytes = value;
            if (_produceAccumulator is not null)
            {
                _produceAccumulator.BatchMaxBytes(value);
            }
        }
    }

    public long TotalBatchMaxBytes
    {
        get => _produceAccumulator is null ? 0 : _produceAccumulator.GetTotalBatchMaxBytes();
        set
        {
            _totalBatchMaxBytes = value;
            if (_produceAccumulator is not null)
            {
                _produceAccumulator.TotalBatchMaxBytes(value);
            }
        }
    }

    /// <summary><c>initProduceAccumulator</c>: reuse the accumulator by clientId, then feed
    /// in the three <c>&gt; -1</c> thresholds set during construction. Upstream calls it
    /// at <c>DefaultMQProducerImpl.start():256</c> -- it must come before this class's
    /// <c>_started = true</c>, otherwise "send immediately after start" would hit an
    /// empty accumulator.</summary>
    public void InitProduceAccumulator()
    {
        _produceAccumulator = ProduceAccumulatorRegistry.GetOrCreate(
            string.IsNullOrEmpty(_clientId) ? MixAll.DefaultInstanceName : _clientId);
        if (_batchMaxDelayMs > -1)
        {
            _produceAccumulator.BatchMaxDelayMs(_batchMaxDelayMs);
        }

        if (_batchMaxBytes > -1)
        {
            _produceAccumulator.BatchMaxBytes(_batchMaxBytes);
        }

        if (_totalBatchMaxBytes > -1)
        {
            _produceAccumulator.TotalBatchMaxBytes(_totalBatchMaxBytes);
        }
    }

    // ---------------- Lifecycle ----------------

    public void Start()
    {
        lock (_lock)
        {
            if (_started)
            {
                return;
            }

            // checkConfig: group-name legality + reserved default group name.
            // Placed where checkConfig belongs -- before any network action, so invalid
            // config fails on the spot in Start().
            Validators.CheckGroup(_producerGroup);
            if (_producerGroup == MixAll.DefaultProducerGroup)
            {
                throw new MQClientException("producerGroup can not equal " + MixAll.DefaultProducerGroup
                                            + ", please specify another one.");
            }

            // Exactly one of static addresses and dynamic addressing
            // (ROCKETMQ_NAMESRV_DOMAIN) must be usable
            if (_nameServerAddrs.Count == 0 && !DefaultTopAddressing.IsConfigured())
            {
                // Code value 10004 (the code validateNameServerSetting gives for the same
                // fault): the reference behavior does not check in start(), and the fault
                // only surfaces as 10004 on the first send; this port moves it earlier to
                // Start (more predictable), but must not show the caller two different
                // codes.
                throw new MQClientException("name server address is not set",
                    ClientErrorCode.NoNameServerException);
            }

            // Start(): a non-inner producer unconditionally replaces the default
            // instanceName with <pid>#<nanoTime>, then builds clientId per
            // ClientConfig#buildMQClientId. Written back in place, so the same producer's
            // clientId stays the same across restarts (changes are not rolled back).
            _instanceName = ClientIds.ChangeInstanceNameToPID(_instanceName);
            if (string.IsNullOrEmpty(_clientId))
            {
                _clientId = ClientIds.Build(_instanceName, _unitName, _enableStreamRequestType);
            }

            // Request hooks (ACL signing / stream's ReqT): binding must happen before
            // Start() -- rpcHook is constructed into the remoting instance, so the very
            // first packet carries it; placed after Start(), the dynamic addressing /
            // first route packet during start would be bare.
            // RequestHooks.Compose restores the Namespace → stream → user-hook order (single-slot transport).
            IRpcHook? requestHook = RequestHooks.Compose(_enableStreamRequestType, _rpcHook, () => _namespaceV2);
            _mqClient = MQClientManager.Instance.GetOrCreateMQClientInstance(_clientId,
                () => new MQClientInstance(_clientId, _nameServerAddrs,
                    tlsEnable: _tlsEnable, tlsOptions: _tlsOptions, socksProxy: _socksProxyConfig,
                    unitName: _unitName,
                    pollNameServerIntervalMillis: _pollNameServerIntervalMillis,
                    vipChannelEnabled: _sendMessageWithVIPChannel));
            if (requestHook is not null && !_mqClient.RegisterRpcHook(requestHook))
            {
                ClientLog.Warn("producer rpc hook ignored: MQClientInstance already has one (clientId="
                    + _clientId + ")");
            }
            _mqClient.DecodeReadBody = _decodeReadBody;
            _mqClient.MqClientApiTimeout = _mqClientApiTimeout;
            _mqClient.EnableConcurrentHeartbeat = _enableConcurrentHeartbeat;
            _mqClient.ConcurrentHeartbeatThreadPoolSize = _concurrentHeartbeatThreadPoolSize;
            _mqClient.MaxPageSizeInGetMetadata = _maxPageSizeInGetMetadata;
            try
            {
                _mqClient.Start();
            }
            catch
            {
                // If it cannot start, hand the reference back: otherwise the refcount
                // leaks and the shared instance never reaches zero for cleanup
                MQClientManager.Instance.UnregisterMQClient(_clientId, _mqClient);
                _mqClient = null;
                throw;
            }
            // Dynamic name server: the instance may already have fetched addresses from
            // the address server at startup; backfill them into this producer
            if (_nameServerAddrs.Count == 0 && _mqClient.NameServerAddrs.Count > 0)
            {
                _nameServerAddrs = new List<string>(_mqClient.NameServerAddrs);
            }

            // Register broker-initiated request processor: transaction check-back
            // CHECK_TRANSACTION_STATE(39). Without registration, check-backs would be
            // dropped by the transport as "unknown request" and transactional messages
            // would stay Unknown forever. Uses instance-level multicast (under a shared
            // instance each producer registers its own, without clobbering others).
            _mqClient.RegisterFacadeProcessor(RequestCode.CheckTransactionState, CheckTransactionState);

            // The accumulator must be built **before** _started=true: AutoBatch's getter
            // reports "not enabled" when it cannot see the accumulator, so the first send
            // right after start would bypass batching and go direct (same order as
            // impl.start(): this is also the order there). Reused by clientId, so a
            // restart gets the same accumulator.
            InitProduceAccumulator();

            // TTL sweep for Request-Reply waiting slots
            // (RequestHouseKeepingService thread, 3s first scan / 1s period). Shared as a
            // singleton by refcount, stopped only when the last producer exits at Shutdown.
            RequestFutureHolder.Instance.StartScheduledTask(this);

            // MQFaultStrategy.startDetector (startDetectorEnable,
            // default false): the address resolver gives the detector thread broker
            // name → address; the detector uses TCP connectivity probes (the default
            // ServiceDetector is one getMaxOffset RPC, equivalent for connectivity, but
            // saves a protocol-stack path; README difference note explains this).
            _mqFaultStrategy.LatencyFaultTolerance.SetResolver(
                name => _mqClient?.FindBrokerAddressInPublish(name));
            _mqFaultStrategy.LatencyFaultTolerance.SetServiceDetector(new TcpServiceDetector());
            if (_mqFaultStrategy.IsStartDetectorEnable())
            {
                _mqFaultStrategy.StartDetector();
            }

            _started = true;
            // Allow Start after Shutdown: during the first half of Shutdown _started is
            // still true, with _shutdownRequested blocking repeated Shutdown and new
            // requests; this gate must reset on re-Start (supports start→shutdown→start)
            _shutdownRequested = false;
            // Async send pool (built inside Start's lock):
            // must precede any SendAsync after _started=true -- SendAsync only takes the
            // handle under the lock.
            CreateAsyncExecutors();
            // The guard threads likewise go last: they are pure background threads, so
            // starting them earlier or later does not change external semantics, but they
            // must be in place before Send can be entered (otherwise batching would wait
            // for the first guard round).
            _produceAccumulator?.Start();
            ClientLog.Info("DefaultMQProducer[" + _producerGroup + "] started, clientId=" + _clientId);

            // Heartbeat thread: periodically registers ProducerData with brokers. The
            // broker's transaction check-backs reach back to the producer through the
            // channel registered in this step; when a producer sends no heartbeat,
            // Commit/Rollback still succeed (the client actively sends END_TRANSACTION),
            // but Unknown half messages are **never checked back**.
            _heartbeatRunning = true;
            _heartbeatThread = new Thread(HeartbeatLoop)
            { IsBackground = true, Name = "ProducerHeartbeatThread" };
            _heartbeatThread.Start();
        }

        // The trace dispatcher starts outside the lock: it spins up internal producers
        // (each taking their own locks); starting inside the lock risks lock nesting.
        StartTraceDispatcher();
    }

    /// <summary>Build the async send chain's two pools: the async send pool + the client
    /// callback pool. Thread names look like <c>AsyncSenderExecutor_1…</c> /
    /// <c>NettyClientPublicExecutor_1…</c> (indexes start at 1).</summary>
    private void CreateAsyncExecutors()
    {
        int cores = Math.Max(1, Environment.ProcessorCount);
        // getAsyncSenderExecutor's convention:
        // a caller-provided pool is used as-is; the built-in pool is only created when
        // none was given.
        _asyncSenderExecutorOwned = AsyncSenderExecutor is null;
        _asyncSenderExecutor = AsyncSenderExecutor ?? new ConsumeExecutor(
            cores, cores, 60.0, "AsyncSenderExecutor",
            maxQueueSize: _asyncSenderQueueCapacity, threadNameSep: "_", threadIndexFrom: 1);
        int callbackThreads = _clientCallbackExecutorThreads;
        if (callbackThreads <= 0)
        {
            // Default to CPU core count; an explicit <= 0 falls back to 4
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

            // ⚠ _started is deliberately **not** flipped here yet: draining the send pool
            // still runs the preparation stage, which needs the client (GetClient / connect
            // / send request). Flipping it early would turn every queued task into
            // "producer not started", making the drain a formality. Rejecting new
            // requests is handled by _shutdownRequested and the two nulled pools, which
            // is semantically equivalent to SHUTDOWN_ALREADY.
            _shutdownRequested = true;
            sender = _asyncSenderExecutor;
            callback = _callbackExecutor;
            senderOwned = _asyncSenderExecutorOwned;
            _asyncSenderExecutor = null;
            _callbackExecutor = null;

            // Stop the heartbeat thread first (it internally holds an mqClient reference)
            _heartbeatRunning = false;
            if (_heartbeatThread is { IsAlive: true })
            {
                _heartbeatThread.Join(2000);
            }

            // Corresponds to shutdown()'s RequestFutureHolder.shutdown
            // and MQFaultStrategy teardown: the request waiting-slot TTL sweep stops by
            // refcount (only when the last producer exits), and the detector thread is
            // closed along with it.
            RequestFutureHolder.Instance.ShutdownScheduledTask(this);
            _mqFaultStrategy.Shutdown();

            lock (_txThreadsLock)
            {
                threads = new List<Thread>(_txThreads);
                _txThreads.Clear();
            }
        }

        // Reclaim the async send pools first (they internally hold mqClient references),
        // then close the client:
        // drain (wait=true) rather than just shutdown(). A non-waiting
        // <c>defaultAsyncSenderExecutor.shutdown()</c> would drop
        // the queued preparation stages along with their tasks; here we wait for the
        // queue to finish, guaranteeing that every submitted message **completes its
        // preparation stage and hands the packet to the transport**.
        // ⚠ Only the preparation stage is drained (including initiating broker-switch
        // retries): network waits happen on transport threads and are not waited for.
        // ⚠ The guarantee stops at "handed to the transport": the client is about to be
        // closed right after, so the **tail** few frames the broker has not yet read are
        // dropped along with the connection (measured 32/36 up when running the full
        // suite offline, stable 36/36 when run alone), and the few whose responses never
        // came back get no final callback either -- closing the client stops the timeout
        // cleanup and clears the in-flight table, leaving nobody to deliver. Callers
        // wanting a callback for every message must wait for callbacks before Shutdown.
        // A caller-supplied pool (passed via setAsyncSenderExecutor) is left untouched --
        // it manages its own lifecycle.
        if (senderOwned)
        {
            sender?.Shutdown(true);
        }
        // Same for the callback pool: wait for queued callbacks to finish. For a message
        // whose response comes back after this, CompleteOnCallbackThread hands the user
        // callback over in place when it sees the pool is closed.
        callback?.Shutdown(true);

        // Only after all preparation stages finish is "no longer alive" declared: from
        // here on no path can obtain the client.
        lock (_lock)
        {
            _started = false;
        }

        // Guard threads stop with the producer: the accumulator is
        // closed only **after** impl.shutdown(). The accumulator itself stays in the
        // clientId reuse table, and its entries are not cleared: leftover batches are
        // still handled by the new guard thread after the next start.
        _produceAccumulator?.Shutdown();

        // Transaction check-back threads still join by the old convention (they also
        // hold mqClient references)
        foreach (Thread t in threads)
        {
            try
            {
                if (t.IsAlive) t.Join();
            }
            catch (Exception)
            {
                // Ignore exceptions during join (e.g. the thread has not truly started yet)
            }
        }

        if (_mqClient is not null)
        {
            // `shutdown()` → `unregisterProducer` →
            // `unregisterClient(group, null)`: sends UNREGISTER_CLIENT(35) synchronously
            // to **every broker (including slaves)** in brokerAddrTable, timing out at
            // getMqClientApiTimeout() (3000ms), exceptions only log.warn. Without it, the
            // broker-side ProducerManager waits for the channel to drop (or a 120s scan)
            // before reclaiming this group's connection. 35 must travel over the
            // **still-open** connection, so it can only be placed before Shutdown(); the
            // heartbeat thread has already been stopped above, so it will not be
            // re-registered.
            try
            {
                _mqClient.UnregisterClientAllBrokers(_clientId, ProducerGroup, "");
            }
            catch (Exception e)
            {
                ClientLog.Debug("unregister producer on shutdown failed: " + e.Message);
            }

            // The transaction check-back processor is registered by this producer on the
            // **shared instance**: remove it first, so that when the instance is reused
            // by another facade, broker check-backs do not still hit an exited producer.
            _mqClient.UnregisterFacadeProcessor(RequestCode.CheckTransactionState, CheckTransactionState);

            MQClientManager.Instance.UnregisterMQClient(_clientId, _mqClient);
            _mqClient = null;
        }

        // Shutdown order: close this producer first, then flush and close the trace
        // dispatcher (the dispatcher uses its **own** internal producer, unrelated to
        // this client instance, so it can still finish sending after being closed)
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

    /// <summary>Returns the underlying client instance.</summary>
    public MQClientInstance Client() => GetClient();

    private MQClientInstance GetClient()
    {
        if (!_started || _mqClient is null)
        {
            // Distinguishes "not started" from "already shut down": ServiceState tells
            // CREATE_JUST from SHUTDOWN_ALREADY; here _shutdownRequested provides the
            // same single criterion.
            throw new MQClientException(_shutdownRequested
                ? "producer already shutdown"
                : "producer not started, call start() first");
        }

        return _mqClient;
    }

    // ---------------- Hook execution (executeSendMessageHookBefore/After, executeEndTransactionHook) ----------------
    // Exceptions thrown by hooks are always swallowed and logged as warn: trace failures
    // must never affect normal send/receive.

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

    /// <summary>Build the SendMessageContext (all fields of the hook context in the send
    /// kernel). The msgType decision order is copied verbatim: TRAN_MSG=true → Trans; any
    /// delay-type property present → Delay; otherwise Normal.
    /// ⚠ C#'s <c>GetProperty</c> returns an **empty string** rather than null for a
    /// missing key, so delay properties must be checked with <c>Properties.ContainsKey</c>,
    /// not by null-checking the value.</summary>
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

    /// <summary>Corresponds to tryToFindTopicPublishInfo:
    /// first fetch the **real** route; only when it truly cannot be fetched (a new topic
    /// not yet registered with the NameServer) is publish info synthesized for the topic
    /// from the default topic (TBW102) -- otherwise the new topic's **first** message
    /// would have no queue to select and would directly throw "Can not find Message
    /// Queue". The consume side does **not** do this fallback.</summary>
    private TopicPublishInfo TryToFindTopicPublishInfo(MQClientInstance c, string topic)
    {
        try
        {
            return c.GetTopicPublishInfo(topic);
        }
        catch (MQClientException)
        {
            try
            {
                return c.GetTopicPublishInfo(topic, true);
            }
            catch (MQClientException)
            {
                // All three "cannot get route" branches (sendDefaultImpl / sendSelectImpl /
                // invokeMessageQueueSelector:716) call validateNameServerSetting() first
                // before throwing no-route: when there is not a single name server address
                // (an address server configured but returning nothing also counts), 10004
                // is reported instead of blaming "this topic has no route" for an
                // addressing failure.
                ValidateNameServerSetting(c);
                throw;
            }
        }
    }

    /// <summary>
    /// Corresponds to <c>validateNameServerSetting</c>.
    ///
    /// Runs only on the "cannot get route" branch, separating two completely different
    /// faults:
    /// <list type="bullet">
    /// <item>Not a single name server address at all (an address server configured but
    /// returning nothing also counts) → 10004.</item>
    /// <item>Addresses exist, just this topic has no route → let the caller's original
    /// exception be thrown as before (10005 etc.).</item>
    /// </list>
    /// Without this step, a user who misconfigures the address server and gets an empty
    /// list would see "No route info of this topic" -- which means "the topic does not
    /// exist", pointing them toward creating a topic, when what is actually broken is
    /// addressing. The protocol text originally concatenated <c>FAQUrl.suggestTodo(...)</c>;
    /// this repo by convention carries no suffix.
    /// </summary>
    private void ValidateNameServerSetting(MQClientInstance? c)
    {
        if (c is null || c.NameServerAddrs.Count == 0)
        {
            throw new MQClientException("No name server address, please set it.",
                ClientErrorCode.NoNameServerException);
        }
    }

    /// <summary>Synchronous send with before/after hooks (corresponds to the hook points of
    /// sendKernelImpl + sendDefaultImpl). Hooks run only on the attempt that **actually
    /// initiates the request** (each retry rebuilds the context).
    /// With no hooks it passes straight through, adding no extra overhead.
    ///
    /// Execution order strictly follows sendKernelImpl's steps:
    ///   1. <b>CheckForbiddenHook</b> (runs every attempt; exceptions are <b>not
    ///      swallowed</b>, thrown straight into the retry chain)
    ///   2. SendMessageHook.before
    ///   3. send the request
    ///   4. SendMessageHook.after (with SendResult on success / Exception on failure)</summary>
    private SendResult SendWithHooks(MQClientInstance c, Message msg, MessageQueue mq,
        int timeout, int sysFlag, object? arg = null,
        CommunicationMode mode = CommunicationMode.Sync)
    {
        // W3C traceparent propagation (opt-in): inject a root context when absent, never
        // overwrite an existing value.
        // ⚠ Must be placed before the HasSendInterceptors early-return, otherwise the
        // injection is skipped when there are no hooks.
        if (_enableTraceContext)
        {
            TraceParentContext.Inject(msg);
        }

        if (!HasSendInterceptors())
        {
            return c.SendMessage(_producerGroup, msg, mq, timeout, sysFlag, _unitMode,
                _createTopicKey, _defaultTopicQueueNums);
        }

        // Address resolution happens **before** the hooks and is not swallowed on failure:
        // sendKernelImpl's two address steps are outside the hook block, and a failure
        // throws "The broker[X] not exist" per :1100. The old behavior (lenient address
        // lookup + swallowed exception) would let the first pinned send during a master
        // outage hit a slave node and eat a pointless round of SYSTEM_BUSY(2).
        string brokerAddr = c.PublishAddrFor(mq.BrokerName, mq.Topic);

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

    /// <summary>Build the CheckForbiddenContext and execute it (called once per send
    /// attempt, including retries). Exceptions are not caught here -- they must propagate
    /// up along the send retry chain.</summary>
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
            // The auth context's unitMode comes from isUnitMode() (checkForbiddenContext.setUnitMode)
            // -- the hook uses it to judge whether this is unitized traffic.
            UnitMode = _unitMode,
        };
        ExecuteCheckForbiddenHook(context);
    }

    /// <summary>Corresponds to start()'s trace section: when enableTrace=true, build the
    /// dispatcher (Type=Produce) and register SendMessageTraceHook / EndTransactionTraceHook,
    /// then start it. Any exception is only logged -- trace failures must not affect
    /// normal sends.</summary>
    private void StartTraceDispatcher()
    {
        if (_enableTrace)
        {
            try
            {
                var dispatcher = new AsyncTraceDispatcher(_producerGroup,
                    TraceDispatcherType.Produce, _traceMsgBatchNum, _traceTopic, _rpcHook);
                dispatcher.SetHostProducer(this);
                // namespaceV2 propagates to the dispatcher,
                // which then lands it on the internal trace producer at Start.
                dispatcher.NamespaceV2 = _namespaceV2;
                _traceDispatcher = dispatcher;
                RegisterSendMessageHook(new SendMessageTraceHook(dispatcher));
                RegisterEndTransactionHook(new EndTransactionTraceHook(dispatcher));

                // recall RPC's trace hook (IRpcHook,
                // records TraceType.Recall after the response comes back). This port's hook
                // slot is single (first-wins), and Start() has already occupied it with the
                // Namespace/Stream/ACL composite hook -- here it is re-registered as a
                // "original hook + recall" chain; recall is appended last, preserving
                // registration order.
                var recallHook = new DefaultRecallMessageTraceHook(dispatcher);
                IRpcHook? existing = _mqClient?.RemotingClient.CurrentHook();
                if (existing is not null)
                {
                    _mqClient!.RemotingClient.UnregisterRpcHook();
                    _mqClient!.RemotingClient.RegisterRpcHook(existing is ChainedRpcHook chained
                        ? new ChainedRpcHook(chained.Hooks.Append(recallHook).ToList())
                        : new ChainedRpcHook(new[] { existing, recallHook }));
                }
                else if (_mqClient is not null)
                {
                    _mqClient.RemotingClient.RegisterRpcHook(recallHook);
                }
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

    // ---------------- Validation ----------------

    // Corresponds to Validators.checkMessage: topic legality, forbidden topics, body
    // length threshold, LMQ path. Intercepting locally is because TOPIC_NOT_EXIST is a
    // retryable code, and an illegal name would spin the retry budget for nothing.
    private void CheckMessage(Message msg) => Validators.CheckMessage(msg, _maxMessageSize);

    // Corresponds to tryToCompressMessage + sendKernelImpl's sysFlag assembly:
    // when the threshold is met and it is not a batch, **compress msg.body in place**,
    // returning the sysFlag that should be sent (COMPRESSED_FLAG | compression-type
    // bits); returns 0 when not compressing.
    private int PrepareForSend(Message msg)
    {
        // Batch messages (MessageBatch) are **never compressed**
        if (msg.IsBatch)
        {
            return 0;
        }

        // Only compress when body length >= compressMsgBodyOverHowmuch (default 4096)
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
            // Compression failure **degrades to no compression** with a log, rather than
            // failing the send
            ClientLog.Warn("tryToCompressMessage failed, send uncompressed: " + e.Message);
            return 0;
        }

        // Size is not compared after compression (sent as-is even if larger)
        if (compressed.Length == 0 && msg.Body.Length != 0)
        {
            return 0;
        }

        // Compression succeeded: replace the body in place and set the compression flag
        // plus compression-type bits
        msg.Body = compressed;
        int sysFlag = MessageSysFlag.CompressedFlag;
        sysFlag |= CompressionType.GetCompressionFlag(_compressType);
        return sysFlag;
    }

    // Message is a reference type and send semantics require a copy before compression;
    // clone explicitly here, avoiding in-place rewriting of the caller's original
    // message body.
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

    // Wrap the topic with the namespace prefix before sending (corresponds to
    // withNamespace). When the namespace is empty, only clone (topic unchanged).
    private Message WithNamespace(Message msg)
    {
        Message outMsg = CloneMessage(msg);
        if (_namespace.Length != 0)
        {
            outMsg.Topic = NamespaceUtil.WrapNamespace(_namespace, msg.Topic);
        }

        return outMsg;
    }

    // The two pinned-send guard messages (one on the sync path, one on the async path),
    // preserved verbatim: the two texts were originally different, selected by entry point.
    private const string PinnedTopicMismatchSync = "message's topic not equal mq's topic";
    private const string PinnedTopicMismatchAsync =
        "Topic of the message does not match its target message queue";

    /// <summary>Topic-consistency guard for pinned sends (explicit mq given) (one each on the
    /// sync / async paths, with different texts, passed by the caller per entry point).
    /// Compares the names **after each has been namespace-wrapped** -- public entries first
    /// <c>msg.setTopic(withNamespace(...))</c>, then pass mq through
    /// <c>ClientConfig.queueWithNamespace</c>, so what is compared is the two resource
    /// names under the same namespace (WrapNamespace is idempotent, so an already-prefixed
    /// argument is not wrapped twice). Without this guard, a message whose topic does not
    /// match the target queue would still be sent: the broker writes by the queue name in
    /// the request, SendResult looks normal, but the message lands in a partition of
    /// <b>another topic</b>, consumed by nobody and reported by nobody.</summary>
    private void CheckPinnedTopic(string msgTopic, MessageQueue mq, string message)
    {
        if (NamespaceUtil.WrapNamespace(_namespace, msgTopic)
            != NamespaceUtil.WrapNamespace(_namespace, mq.Topic))
        {
            throw new MQClientException(message);
        }
    }

    // ---------------- Synchronous send ----------------

    /// <summary>Qualify the final failure after retries are exhausted, deciding the error
    /// code of the resulting MQClientException. Equivalent to the lastExceptionType
    /// determination (tiered by exception type).</summary>
    private enum SendFailureCause
    {
        None,
        Broker,
        Connect,
        Timeout,
        Client,
        Other,
    }

    // ---------------- Auto-batching forwarding layer (canBatch / sendDirect / sendByAccumulator) ----------------

    /// <summary><c>Message.getDelayTimeMs/Sec()</c> + <c>getDeliverTimeMs()</c>'s maximum
    /// (default 0). Property names align verbatim with <c>MessageConst</c>; on an illegal
    /// value it **throws directly**, like <c>Long.parseLong</c>, rather than silently
    /// treating it as 0.</summary>
    private static long MaxDelayValue(Message msg)
    {
        long result = msg.DelayTimeLevel;
        foreach (string name in TimerDelayProperties)
        {
            if (!msg.Properties.TryGetValue(name, out var raw) || raw is null)
            {
                continue;
            }

            long value = long.Parse(raw, CultureInfo.InvariantCulture);
            if (value > result)
            {
                result = value;
            }
        }

        return result;
    }

    private static readonly string[] TimerDelayProperties =
    {
        "TIMER_DELAY_MS", "TIMER_DELAY_SEC", "TIMER_DELIVER_MS",
    };

    /// <summary><c>canBatch</c>.
    ///
    /// ⚠ The global byte gate <see cref="ProduceAccumulator.TryAddMessage"/> comes first:
    /// **admittance books it**, while the three "cannot batch" checks that follow only
    /// make the caller fall back to direct send -- that message's byte count is **not
    /// refunded** (an upstream omission, copied verbatim; changing it would no longer
    /// match the live convention). The three exclusions:
    ///   1. Delayed/scheduled messages (any of DELAY / TIMER_DELAY_MS / TIMER_DELAY_SEC /
    ///      TIMER_DELIVER_MS) &gt; 0 disables batching -- a MessageBatch can only carry one
    ///      delay property;
    ///   2. Retry topic (<c>%RETRY%</c> prefix): retry messages have special offset
    ///      semantics;
    ///   3. Messages already carrying the PGROUP property (the transactional half-message
    ///      kind): the broker locates the connection by group.
    ///
    /// The test project and this assembly have no InternalsVisibleTo relationship, hence
    /// public; do not use in business code.</summary>
    public bool CanBatch(Message msg)
    {
        if (_produceAccumulator is null || !_produceAccumulator.TryAddMessage(msg))
        {
            return false;
        }

        if (MaxDelayValue(msg) > 0)
        {
            return false;
        }

        if (msg.Topic.StartsWith(MixAll.RetryGroupTopicPrefix, StringComparison.Ordinal))
        {
            return false;
        }

        if (msg.Properties.ContainsKey(MessageConst.PropertyProducerGroup))
        {
            return false;
        }

        return true;
    }

    /// <summary><c>sendDirect</c>: direct send bypassing the accumulator (sync or async).
    /// Note "direct" means **not via the accumulator**; internally it still goes through
    /// the retry chain. ⚠ It must land on the *Impl variants (i.e. the
    /// <c>DefaultMQProducerImpl.send*</c> convention): the public entries judge AutoBatch
    /// again, and a caller arriving here is precisely because batching was rejected --
    /// looping back to the public entry would be infinite recursion.
    /// virtual exists so tests can build a fake producer that "only books without
    /// networking" (the MockMQProducer convention).</summary>
    public virtual SendResult? SendDirect(Message msg, MessageQueue? mq, ISendCallback? sendCallback)
    {
        if (sendCallback is null)
        {
            return mq is null ? SendImpl(msg, -1) : SendToMqImpl(msg, mq, -1);
        }

        SendAsyncImpl(msg, sendCallback, -1, mq);
        return null;
    }

    /// <summary><c>sendByAccumulator</c>.
    /// Cannot batch (see <see cref="CanBatch"/>) → fall back to direct send; otherwise
    /// first <c>Validators.checkMessage</c>, then stamp UNIQ_KEY on this message (the
    /// batch's IDs must be written before the body is encoded), then hand it to the
    /// accumulator (the sync version returns this message's own SendResult; the async
    /// version returns null immediately, results via callback).</summary>
    public SendResult? SendByAccumulator(Message msg, MessageQueue? mq, ISendCallback? sendCallback)
    {
        if (!CanBatch(msg))
        {
            return SendDirect(msg, mq, sendCallback);
        }

        Validators.CheckMessage(msg, _maxMessageSize);
        MessageClientIDSetter.SetUniqId(msg);
        if (sendCallback is null)
        {
            return mq is null
                ? _produceAccumulator!.Send(msg, this)
                : _produceAccumulator!.Send(msg, mq, this);
        }

        if (mq is null)
        {
            _produceAccumulator!.SendAsync(msg, sendCallback, this);
        }
        else
        {
            _produceAccumulator!.SendAsync(msg, mq, sendCallback, this);
        }

        return null;
    }

    // No queue specified: select a queue by broker latency/isolation state, retrying on
    // failure per retryTimesWhenSendFailed. Aligned item by item with sendDefaultImpl:
    //   * timesTotal = 1 + retryTimesWhenSendFailed (only sync sends retry);
    //   * the route is fetched only once **outside** the loop and shared by the whole
    //     retry chain; when it cannot be fetched at all, NOT_FOUND_TOPIC is thrown
    //     outside the loop rather than spinning the retries for nothing;
    //   * each attempt first computes costTime; if the total timeout is used up, give up
    //     entirely (→ RemotingTooMuchRequestException); when retries remain, the
    //     per-request timeout is clamped by SendMsgMaxTimeoutPerRequest, leaving the rest
    //     for later brokers;
    //   * exceptions are tiered into the fault table by type, and only broker response
    //     codes in RetryResponseCodes continue retrying;
    //   * when all fail, the cause is mapped into a ClientErrorCode packed into the
    //     MQClientException.
    public SendResult Send(Message msg, int timeoutMillis = -1)
    {
        // Auto-batching dispatch (the send(Message) facade). Two non-dispatch cases:
        //   * the **explicit timeout** overload (send(msg, timeout)) goes straight to impl,
        //     no batching;
        //   * batch messages are blocked by `msg is not MessageBatch` -- that is exactly
        //     what the accumulator itself sends out; without the block it would recurse
        //     infinitely.
        // Here namespace is applied first and the **copy** is handed to the accumulator
        // (if the entry set the topic in place, each sub-message's topic would then be
        // encoded one by one into the batch body, and the broker and consume side would
        // see the prefixed one; this project leaves the caller's message alone per the
        // existing convention).
        if (timeoutMillis < 0 && AutoBatch && msg is not MessageBatch)
        {
            return SendByAccumulator(WithNamespace(msg), null, null)!;
        }

        return SendImpl(msg, timeoutMillis);
    }

    /// <summary>The whole synchronous send chain of <c>DefaultMQProducerImpl.send(msg, timeout)</c>
    /// (the "bypassing the accumulator" half). <see cref="SendDirect"/> must come here
    /// rather than the public <see cref="Send(Message,int)"/>: the latter judges AutoBatch
    /// again, so when batching fails the two would call each other, becoming infinite
    /// recursion.</summary>
    private SendResult SendImpl(Message msg, int timeoutMillis)
    {
        MQClientInstance c = GetClient();
        int timeout = timeoutMillis >= 0 ? timeoutMillis : _sendMsgTimeout;
        CheckMessage(msg);
        Message outbound = WithNamespace(msg);
        // Compress once outside the retry loop: PrepareForSend rewrites the body in
        // place, and calling it repeatedly inside the loop would re-compress already
        // compressed bytes (zlib(zlib(x))), leaving the consume side with a compressed
        // stream after only one layer of decompression.
        int sysFlag = PrepareForSend(outbound);

        TopicPublishInfo publish;
        try
        {
            publish = TryToFindTopicPublishInfo(c, outbound.Topic);
        }
        catch (MQClientException e)
        {
            // Route lookup failed -> 10005 is thrown outside the loop, so the retry budget is not
            // burned for nothing; but the already-decided 10004 from TryToFindTopicPublishInfo
            // (no name server configured at all) must be propagated as is, otherwise the two distinct
            // failures get blurred into one again.
            throw new MQClientException(e.Message,
                e.ResponseCode == ClientErrorCode.NoNameServerException
                    ? e.ResponseCode
                    : ClientErrorCode.NotFoundTopicException);
        }

        int timesTotal = _retryTimesWhenSendFailed + 1;
        // Elapsed time uses a monotonic high-resolution clock:
        // wall-clock millisecond granularity would measure sub-millisecond local loopback
        // round trips as 0, and the fault threshold would never take effect.
        double beginFirst = UtilAll.MonotonicMillis();
        var brokersSent = new List<string>();
        string lastBrokerName = string.Empty;
        // Non-SEND_OK with broker-switch enabled: keep this "stored but not stored well"
        // result; if everything later fails, return it as-is.
        SendResult? result = null;
        string lastError = string.Empty;
        var cause = SendFailureCause.None;
        int brokerCode = MQBrokerException.Unknown;
        bool callTimeout = false;

        for (int attempt = 0; attempt < timesTotal; ++attempt)
        {
            // Fault avoidance: when enabled, select a queue by broker latency/isolation
            // state (MQFaultStrategy); when disabled, degrade to plain round-robin (the
            // strategy decides internally). On retry, resetIndex restarts round-robin from
            // the beginning, so a different broker can be picked by avoiding
            // lastBrokerName.
            MessageQueue selected;
            try
            {
                selected = _mqFaultStrategy.SelectOneMessageQueue(publish, lastBrokerName,
                    attempt > 0);
            }
            catch (MQClientException e)
            {
                // No selectable queue is a client-side exception: there is no target
                // broker yet, so the fault table has nothing to record
                lastError = e.Message;
                cause = SendFailureCause.Client;
                continue;
            }

            lastBrokerName = selected.BrokerName;
            brokersSent.Add(selected.BrokerName);
            double began = UtilAll.MonotonicMillis();

            // Overall budget: timeout is **this call's** total budget, and the portion
            // already eaten by earlier attempts must be deducted, otherwise 3 retries of
            // 3s each would take up to 9s to return.
            long costTime = (long)(began - beginFirst);
            if (timeout < costTime)
            {
                callTimeout = true;
                break;
            }

            // costTime <= timeout (int), so the difference is within int range; the
            // narrowing conversion is lossless
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
                // Record send latency; over-threshold isolates that broker for a while
                _mqFaultStrategy.UpdateFaultItem(selected.BrokerName,
                    UtilAll.MonotonicMillis() - began, false, true);
                // Non-SEND_OK with retryAnotherBrokerWhenNotStoreOK enabled switches
                // brokers; otherwise return this "stored but not stored well" result as-is
                if (sent.SendStatus != SendStatus.SendOk && _retryAnotherBrokerWhenNotStoreOk)
                {
                    continue;
                }

                return sent;
            }
            catch (MQBrokerException e)
            {
                // The broker explicitly returned an error code: isolate that broker (reachability is left
                // alone), only a retryable code switches to another broker.
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
                // Cannot connect / timeout / cannot send: isolate that broker. This project has no
                // background reachability prober, so reachable=!isStartDetectorEnable() is always true.
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
                // Client-side problems (hook interception, route gone...): record the latency only, no isolation
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

    // Pinned send to the specified queue
    public SendResult Send(Message msg, MessageQueue mq, int timeoutMillis = -1)
    {
        // Same dispatch as the overload without mq: a pinned queue and an unpinned one have
        // different merge keys (AggregateKey.mq), but the criterion is the same.
        if (timeoutMillis < 0 && AutoBatch && msg is not MessageBatch)
        {
            return SendByAccumulator(WithNamespace(msg), mq, null)!;
        }

        return SendToMqImpl(msg, mq, timeoutMillis);
    }

    private SendResult SendToMqImpl(Message msg, MessageQueue mq, int timeoutMillis)
    {
        MQClientInstance c = GetClient();
        int timeout = timeoutMillis >= 0 ? timeoutMillis : _sendMsgTimeout;
        CheckMessage(msg);
        // Order: Validators -> pinned guard -> sendKernelImpl (compression happens in the kernel)
        // -> timeout recheck.
        CheckPinnedTopic(msg.Topic, mq, PinnedTopicMismatchSync);
        Message outbound = WithNamespace(msg);
        int sysFlag = PrepareForSend(outbound);
        return SendWithHooks(c, outbound, mq, timeout, sysFlag);
    }

    // Send with a selector (ordered messages: the same arg maps to the same queue)
    public SendResult SendBySelector(Message msg, IMessageQueueSelector selector,
        string arg, int timeoutMillis = -1)
    {
        MQClientInstance c = GetClient();
        int timeout = timeoutMillis >= 0 ? timeoutMillis : _sendMsgTimeout;
        CheckMessage(msg);
        Message outbound = WithNamespace(msg);
        TopicPublishInfo publish = TryToFindTopicPublishInfo(c, outbound.Topic);
        MessageQueue selected = selector.Select(publish.MsgQueueList, msg, arg);
        // The selector receives the original message (topic / business fields); compression only
        // affects the body
        int sysFlag = PrepareForSend(outbound);
        // arg is passed through to CheckForbiddenHook (CheckForbiddenContext.arg is exactly it)
        return SendWithHooks(c, outbound, selected, timeout, sysFlag, arg, CommunicationMode.Sync);
    }

    // ---------------- Async / Oneway ----------------

    /// <summary>Async send (the whole chain of
    /// <c>DefaultMQProducerImpl.send(msg, SendCallback, timeout)</c>, i.e. <c>send_async</c>):
    ///
    /// 1. First the backpressure gate (<see cref="EnableBackpressureForAsyncMode"/>, i.e.
    ///    <c>executeAsyncMessageSend</c>). This gate <b>waits on the caller's thread</b>, so
    ///    when backpressure is saturated "async" degrades into "wait until the timeout expires
    ///    and then report an error".
    /// 2. The task is submitted to <c>AsyncSenderExecutor_N</c> (core==max==CPU core count,
    ///    bounded queue <see cref="AsyncSenderQueueCapacity"/>, default 50000, matching the
    ///    hard-coded <c>LinkedBlockingQueue(50000)</c>). A full queue means <c>submit</c>
    ///    throws <c>RejectedExecutionException</c> ->
    ///    <c>MQClientException("executor rejected")</c>, <b>thrown at the caller</b> instead of
    ///    going through the callback; only when backpressure is enabled does it switch to "run
    ///    this one inline" -- there the permit is taken <b>before</b> enqueueing, so not running
    ///    it inline would idle until the timeout expires before the permit came back.
    /// 3. Real elapsed time only counts after dequeue: if queueing ate the whole budget, the
    ///    callback gets
    ///    <c>RemotingTooMuchRequestException("DEFAULT ASYNC send call timeout")</c> and no
    ///    request is sent.
    /// 4. The ASYNC branch of <c>sendKernelImpl</c> (<see cref="SendKernelAsync"/>): address
    ///    resolution -> interceptor hooks -> build the request (<b>exactly once</b>) -> before
    ///    hooks -> <c>invokeAsync</c>. Failures enter
    ///    <see cref="AsyncSendChain.OnException"/> (i.e. <c>onExceptionImpl</c>): pick another
    ///    broker's queue, give the <b>same request</b> a new opaque and retry, up to
    ///    <see cref="RetryTimesWhenSendAsyncFailed"/>; the timeout budget is the <b>shared</b>
    ///    remaining time across all attempts. An explicit error code from the broker does
    ///    <b>not</b> enter the retry chain (async never consults <c>RetryResponseCodes</c>).
    ///
    /// The chain always ends at <see cref="CompleteAsync"/>: after hooks -> return the permit
    /// -> the user callback, invoked <b>exactly once</b> and on <c>NettyClientPublicExecutor_N</c>
    /// (<c>executeInvokeCallback</c> simply submits the callback to publicExecutor so business
    /// code does not hold up the connection's reader thread or the timeout cleanup thread).
    ///
    /// Two deliberate differences: when not <see cref="Start"/>ed or the pool is already closed
    /// it throws <b>synchronously</b> (going through the callback would make the problem harder
    /// to diagnose); batch messages have no async kernel available, so a batch is sent
    /// synchronously on a pool thread and the callback is still handed off (no semantic
    /// difference for the caller).
    /// </summary>
    public void SendAsync(Message msg, ISendCallback callback, int timeoutMillis = -1,
        MessageQueue? mq = null)
    {
        // Automatic batching dispatch (the send(msg, sendCallback) facade). Both dispatches'
        // **Throwable** are routed to onException from the same try block -- an "invalid message
        // / illegal accumulator argument" on the batching path must not be thrown back onto the
        // caller's stack.
        if (timeoutMillis < 0 && AutoBatch && msg is not MessageBatch)
        {
            try
            {
                SendByAccumulator(WithNamespace(msg), mq, callback);
            }
            catch (Exception e)
            {
                callback.OnException(e);
            }

            return;
        }

        SendAsyncImpl(msg, callback, timeoutMillis, mq);
    }

    private void SendAsyncImpl(Message msg, ISendCallback callback, int timeoutMillis,
        MessageQueue? mq)
    {
        // The chain carries a **clone**: compression rewrites the body in place and must not touch
        // the caller's copy. The clone is made on the pool thread (the same prologue as the batch
        // path), while the permit length is still computed on the **caller thread** from the
        // pre-compression body -- that is where it belongs.
        ExecuteAsyncSend(() => CloneMessage(msg), BackPressureMsgLen(msg), callback, timeoutMillis,
            mq);
    }

    /// <summary>The prologue of the async chain (the whole of <c>send_async</c>, i.e.
    /// <c>executeAsyncMessageSend</c> + <c>AsyncSenderExecutor.submit</c>): pass the gates,
    /// submit to the pool, recheck the budget after dequeue, then hand the message produced by
    /// <paramref name="prepare"/> to
    /// <see cref="SendAsyncInner"/>.</summary>
    /// <param name="prepare">Produces the message for this one send <b>on the pool thread</b>.
    /// Having it on the pool thread is mandatory: the batch entry point throws "empty list /
    /// illegal sub-message / inhomogeneous" there, and those exceptions must reach the callback
    /// instead of the caller's stack (the batch branch likewise throws inside the runnable and
    /// lets the outer catch hand them to the callback).</param>
    /// <param name="msgLen">How many "byte" permits to take, computed on the caller
    /// thread.</param>
    private void ExecuteAsyncSend(Func<Message> prepare, long msgLen, ISendCallback callback,
        int timeoutMillis, MessageQueue? mq)
    {
        // Confirm started first (throw immediately when not started rather than failing silently
        // on a background thread)
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
            // The two permits are taken **sequentially**, both waiting with the "remaining budget
            // measured from began" (both use `timeout - costTime`), so the first gate alone can
            // already consume the whole budget
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
                // A count permit already acquired must not be left at the gate (two flags achieve this)
                CompleteAsync(callback, null,
                    new RemotingTooMuchRequestException(
                        "send message tryAcquire semaphoreAsyncSize timeout"), null, permits,
                    onCallbackPool: false);
                return;
            }
        }

        void Run()
        {
            // Real elapsed time only counts after dequeue (beginTimestampFirst is also taken after
            // dequeue)
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
                // the runnable's catch -> newCallBack.onException(e)
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
                // The permit is already taken, so run this one inline (**blocks the caller**) to let the
                // callback return the permit; otherwise a full queue throws right away and the wrongly
                // taken capacity would only be returned when the timeout expires.
                Run();
            }
            else
            {
                throw new MQClientException("executor rejected");
            }
        }
    }

    /// <summary>Preparation corresponding to <c>sendDefaultImpl(ASYNC)</c> (i.e.
    /// <c>_send_async_inner</c>): validation, compression, queue selection. Compression is
    /// done once <b>outside</b> the retry chain, otherwise each round would compress the
    /// already compressed body again. <c>timesTotal</c> is fixed at 1: switching broker
    /// retries all happen in <c>onExceptionImpl</c>.</summary>
    private void SendAsyncInner(Message msg, MessageQueue? mq, ISendCallback callback,
        AsyncSendPermits permits, int timeout)
    {
        MQClientInstance c = GetClient();
        if (msg.IsBatch)
        {
            // Batch has no async kernel (in this port batch only has the synchronous kernel): send the
            // batch synchronously on a pool thread; the result still travels from CompleteAsync
            // through the callback pool. When mq is non-null it must be passed down, otherwise "pinned
            // batch async" degrades to round-robin queue selection. Pinned batch async goes through
            // `impl.send(batch, mq, cb, timeout)`, so the guard message is the **async** one (the
            // synchronous text further down in Send -> SendToMqImpl serves only the synchronous entry
            // point).
            if (mq is not null)
            {
                CheckPinnedTopic(msg.Topic, mq, PinnedTopicMismatchAsync);
            }

            SendResult sent = mq is null ? Send(msg, timeout) : Send(msg, mq, timeout);
            CompleteAsync(callback, sent, null, null, permits);
            return;
        }

        if (_namespace.Length != 0)
        {
            msg.Topic = NamespaceUtil.WrapNamespace(_namespace, msg.Topic);
        }

        CheckMessage(msg);
        if (mq is not null)
        {
            // The async branch throws with a different message at the same place; the exception is
            // routed by the runnable's catch to `newCallBack.onException` (here collected by
            // ExecuteAsyncSend's Run).
            CheckPinnedTopic(msg.Topic, mq, PinnedTopicMismatchAsync);
        }

        int sysFlag = PrepareForSend(msg);
        if (mq is not null)
        {
            // send(msg, mq, cb, timeout) -> pinned sendKernelImpl; the topicPublishInfo passed down is
            // null, so a failure retries with a new opaque on **the same broker** only.
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
            // Route lookup failed -> 10005 is thrown outside the loop, so the retry budget is not
            // burned for nothing; but the already-decided 10004 from TryToFindTopicPublishInfo (no name
            // server configured at all) must be propagated as is, otherwise the two distinct failures
            // get blurred into one again.
            throw new MQClientException(e.Message,
                e.ResponseCode == ClientErrorCode.NoNameServerException
                    ? e.ResponseCode
                    : ClientErrorCode.NotFoundTopicException);
        }

        MessageQueue selected;
        try
        {
            selected = _mqFaultStrategy.SelectOneMessageQueue(publish, null, false);
        }
        catch (MQClientException e)
        {
            // When no queue can be selected it reports `Send [0] times, still failed, Topic: ...,
            // BrokersSent: []`; the queue selection here throws and appends the original reason to the
            // same text.
            throw new MQClientException("Send [0] times, still failed, Topic: " + msg.Topic
                                        + ", BrokersSent: [], last error: " + e.Message);
        }

        SendKernelAsync(c, msg, new MessageQueue(msg.Topic, selected.BrokerName, selected.QueueId),
            publish, callback, permits, timeout, sysFlag);
    }

    /// <summary>The ASYNC branch of <c>sendKernelImpl</c>: address resolution -> interceptor
    /// hooks -> traceparent -> build the request (once) -> before hooks -> hand off to
    /// <see cref="AsyncSendChain"/>. Every failure exit on this stage has to return the permit,
    /// so they all go through <see cref="CompleteAsync"/>.</summary>
    private void SendKernelAsync(MQClientInstance c, Message msg, MessageQueue mq,
        TopicPublishInfo? publish, ISendCallback callback, AsyncSendPermits permits, int timeout,
        int sysFlag)
    {
        double began = UtilAll.MonotonicMillis();
        // Address resolution in two steps (the sendKernelImpl rule): first look up the cached
        // **publish** address (master only); on a miss, refresh the route once for the topic and
        // look again, still missing means "The broker[X] not exist". A pinned send (the caller
        // supplied an mq) does not fetch publish info inside sendDefaultImpl, so this is its only
        // source of routing. Slaves do not accept SEND_MESSAGE, so no slave address is used as a
        // fallback (a PublishAddrFor failure throws right away and CompleteAsync hands it to the
        // callback).
        string addr;
        try
        {
            // Same rule as the synchronous kernel: master only, and once the address is obtained apply
            // the VIP rewrite as sendKernelImpl does (the async request is handed to the send chain
            // with the addr from here and is not resolved a second time via SendMessage).
            addr = c.VipAddr(c.PublishAddrFor(mq.BrokerName, mq.Topic));
        }
        catch (Exception e)
        {
            CompleteAsync(callback, null, e, null, permits, onCallbackPool: false);
            return;
        }

        RunCheckForbidden(msg, mq, addr, null, CommunicationMode.Async);
        if (_enableTraceContext)
        {
            TraceParentContext.Inject(msg);
        }

        // Same rule as the synchronous kernel: a non-batch message gets its unique client ID
        // before the request is sent; it determines SendResult.MsgId and is the very value
        // parseSendResponse reads in the retry chain.
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

        // The ASYNC branch's own overall gate -- hooks, compression and routing all count as
        // elapsed time; if they eat the whole budget no request is issued.
        // RemotingTooMuchRequestException is a subclass of RemotingException, so hook.after has to
        // run before it is thrown to the callback; CompleteAsync reproduces the same ordering here
        // (and does **not** retry).
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

    /// <summary>The end of the chain (i.e. <c>_complete</c>): run the after hooks, then return
    /// the backpressure permit, then hand off the user callback.
    /// <b>Every AsyncSendChain reaches this exactly once</b>, so the user callback runs exactly
    /// once.
    ///
    /// <paramref name="onCallbackPool"/> is one fork: failures that happen <b>before</b> the
    /// request reaches the transport layer (gates, queueing over budget, validation, broker not
    /// found) already invoke <c>_complete</c> in place, so the user callback runs on the current
    /// thread; only results brought back <b>by the transport layer</b> are submitted to
    /// <c>NettyClientPublicExecutor</c> (i.e. <c>executeInvokeCallback</c>).</summary>
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

        // Return the permit before handing over to the user (BackpressureSendCallBack.
        // semaphoreProcessor uses this order, size before num), otherwise sending another async
        // message inside the user callback would occupy an extra slot.
        permits.Release();
        if (onCallbackPool)
        {
            CompleteOnCallbackThread(callback, result, error);
            return;
        }

        InvokeSendCallback(callback, result, error);
    }

    /// <summary>Actually invokes the user callback and swallows any exception the callback
    /// itself throws (both places use <c>catch (Throwable)</c>), otherwise it would take down a
    /// callback pool worker.</summary>
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
                // The exception object is handed over as is (`sendCallback.onException(e)`); a null error
                // can only mean the internal contract was broken, so fall back to an MQClientException
                // instead of a null reference.
                callback.OnException(error ?? new MQClientException("unknown reason"));
            }
        }
        catch (Exception e)
        {
            ClientLog.Warn("send callback raised: " + e);
        }
    }

    /// <summary>Runs the user callback on <c>NettyClientPublicExecutor_N</c> (i.e.
    /// <c>NettyRemotingAbstract.executeInvokeCallback</c>). When the pool is already closed
    /// (shutting down) or the submission is rejected it runs inline -- a callback must never
    /// vanish just because the pool is closing.</summary>
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

    /// <summary>The backpressure permits actually acquired for this async send (the
    /// <c>isSemaphoreAsyncNumAcquired</c> / <c>isSemaphoreAsyncSizeAcquired</c> flags).
    /// Returning them is idempotent, so any exit on the chain can safely call it.</summary>
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

        /// <summary>Returns bytes first, then the count (the order semaphoreProcessor uses).</summary>
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

    /// <summary>The retry chain of one async send (<c>sendMessageAsync</c> +
    /// <c>onExceptionImpl</c>). The request <b>reuses the same object across attempts</b> and
    /// gets a fresh opaque each round; the timeout budget is the <b>remaining time shared</b>
    /// by all attempts.</summary>
    private sealed class AsyncSendChain
    {
        private readonly DefaultMQProducer _producer;
        private readonly MQClientInstance _client;
        // Only ProcessSendResponse reads UNIQ_KEY: the body is already encoded into the request
        private readonly Message _msg;
        /// <summary>null = pinned send (the caller supplied an mq): retries do not switch broker,
        /// they only get a new opaque in place.</summary>
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

        /// <summary><c>sendMessageAsync</c>: issues <b>this round's</b> attempt, the result comes
        /// back through the transport callback.</summary>
        public void Attempt()
        {
            if (_times > 0)
            {
                // Before retrying on another broker, `request.setOpaque(createNewRequestId())`: the old
                // request is still sitting in responseTable waiting for its timeout, so reusing the opaque
                // would cross-wire the responses of the two attempts.
                _request.Opaque = RemotingCommand.NextOpaque();
            }

            _attemptBegan = UtilAll.MonotonicMillis();
            try
            {
                _client.InvokeAsyncOnAddr(_addr, _request, _remaining, OnResponse);
            }
            catch (Exception e)
            {
                // The outer catch of sendMessageAsync: when the transport layer throws **in place** (cannot
                // connect, cannot write) the exception is propagated **as is** with needRetry=true, and the
                // fault table records `reachable=false` (the connection was never established, it is not
                // "slow"). C#'s InvokeAsync has already removed the opaque from the in-flight table before
                // SendRequest throws, so this round will not get a second callback.
                long cost = (long)(UtilAll.MonotonicMillis() - _attemptBegan);
                _producer._mqFaultStrategy.UpdateFaultItem(_mq.BrokerName, cost, true, false);
                OnException(e, true, cost);
            }
        }

        /// <summary><c>operationSucceed</c> / <c>operationFail</c> (i.e.
        /// <c>_handle</c>): record the fault table; on success wrap up, on failure hand over to the
        /// retry decision.</summary>
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

            // updateFaultItem(..., isOver=true, ...) is ordered before the classification -- even if
            // this send is not retried, that broker still has to be recorded once with a failure
            // latency.
            _producer._mqFaultStrategy.UpdateFaultItem(_mq.BrokerName, cost, true, true);
            (Exception wrapped, bool needRetry) = ClassifyAsyncFailure(failure, cost);
            OnException(wrapped, needRetry, cost);
        }

        /// <summary><c>onExceptionImpl</c>: if another attempt is possible, switch broker and reuse
        /// the request; otherwise wrap up.</summary>
        private void OnException(Exception error, bool needRetry, long cost)
        {
            _times++;
            int remaining = _remaining - (int)cost;
            if (!(needRetry && _times <= _producer._retryTimesWhenSendAsyncFailed && remaining > 0))
            {
                Finish(null, error);
                return;
            }

            // Pick a new target: selectOneMessageQueue(tpInfo, brokerName, false) -- the third argument
            // false means to **avoid** the broker that just failed according to lastBrokerName; if none
            // is found, keep the current target.
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
                    // If no queue can be selected, stay on the current target and retry
                }
            }

            // Only the publish address table is consulted (findBrokerAddressInPublish, master only),
            // **without refreshing the route**; when not found it goes into invokeAsync carrying null.
            // Terminate right here so a null address never reaches the transport layer.
            string addr = _client.FindBrokerAddressInPublish(brokerName);
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

        /// <summary>The end of the chain: hands over to <see cref="DefaultMQProducer.CompleteAsync"/>.</summary>
        private void Finish(SendResult? result, Exception? error)
        {
            _producer.CompleteAsync(_callback, result, error, _context, _permits);
        }

        /// <summary><c>_classify_async_failure</c> (i.e. the three-way branch of
        /// <c>operationFail</c>): wraps into an MQClientException and decides whether a broker
        /// switch retry is allowed.
        ///
        /// Only failures where <b>no response was received</b> go through this classification. An
        /// <see cref="MQBrokerException"/> that <b>did</b> receive a response but was judged failed
        /// by <c>ProcessSendResponse</c> lands in the last branch -- that path passes
        /// <c>needRetry=false</c> and rethrows as is. In other words an async send <b>ignores</b>
        /// RetryResponseCodes: an explicit broker error never switches broker. Do not confuse this
        /// with the synchronous semantics.</summary>
        private static (Exception, bool) ClassifyAsyncFailure(Exception err, long cost)
        {
            // The message text and cause follow `MQClientAPIImpl#operationFail`:
            // `new MQClientException("send request failed", throwable)` and friends -- **the message is
            // exactly those three phrases** (that is what the tests assert on), with the low-level
            // detail hanging off InnerException (i.e. the same `cause`). Never splice err.Message into
            // the message: doing so breaks tests that assert on the text verbatim.
            switch (err)
            {
                case RemotingSendRequestException:
                    return (new MQClientException("send request failed", err), true);
                case RemotingTimeoutException:
                    return (new MQClientException("wait response timeout, cost="
                        + cost.ToString(CultureInfo.InvariantCulture), err), true);
                // The remaining RemotingExceptions are "unknown reason", but TooMuchRequest means "too many
                // of our own requests", which switching broker cannot fix (it is a non-retryable
                // criterion).
                case RemotingTooMuchRequestException:
                    return (new MQClientException("unknown reason", err), false);
                // Cannot connect (RemotingConnectException) as well as codec/command errors are
                // RemotingExceptions: switching to another broker has a chance.
                case RemotingConnectException:
                case RemotingCommandException:
                case RemotingException:
                    return (new MQClientException("unknown reason", err), true);
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
        // Oneway send also goes through sendKernelImpl: the address is resolved first (master only,
        // a failure throws immediately so the request never reaches a slave), then
        // CheckForbiddenHook runs.
        string brokerAddr = c.PublishAddrFor(selected.BrokerName, selected.Topic);

        RunCheckForbidden(outbound, selected, brokerAddr, null, CommunicationMode.Oneway);
        // W3C traceparent propagation (opt-in): oneway send injects it too
        if (_enableTraceContext)
        {
            TraceParentContext.Inject(outbound);
        }

        c.SendMessageOneway(_producerGroup, outbound, selected, _sendMsgTimeout, sysFlag, _unitMode,
            _createTopicKey, _defaultTopicQueueNums);
    }

    /// <summary>
    /// Pinned oneway send (<c>sendOneway(Message, MessageQueue)</c>).
    /// There is <b>no</b> topic consistency guard -- sendOneway deliberately does not check:
    /// the topic in the request header comes from <c>msg.getTopic()</c> and only the queueId
    /// comes from the mq, so a wrong topic lands in the same-numbered queue of another topic
    /// with no feedback at all. The address comes from the client's route cache (i.e.
    /// <c>findBrokerAddressInPublish</c>), so at least one successful send is required
    /// beforehand to warm that cache.
    /// </summary>
    public void SendOneway(Message msg, MessageQueue mq)
    {
        MQClientInstance c = GetClient();
        CheckMessage(msg);
        Message outbound = WithNamespace(msg);
        int sysFlag = PrepareForSend(outbound);
        // Same as SendOneway(msg): master-only address, throws when unavailable (slaves do not
        // accept SEND_MESSAGE).
        string brokerAddr = c.PublishAddrFor(mq.BrokerName, mq.Topic);

        RunCheckForbidden(outbound, mq, brokerAddr, null, CommunicationMode.Oneway);
        if (_enableTraceContext)
        {
            TraceParentContext.Inject(outbound);
        }

        c.SendMessageOneway(_producerGroup, outbound, mq, _sendMsgTimeout, sysFlag, _unitMode,
            _createTopicKey, _defaultTopicQueueNums);
    }

    // ---------------- Batch ----------------

    public SendResult SendBatch(List<Message> msgs, int timeoutMillis = -1)
    {
        MQClientInstance c = GetClient();
        int timeout = timeoutMillis >= 0 ? timeoutMillis : _sendMsgTimeout;
        Message outbound = BuildBatchOutbound(msgs);

        TopicPublishInfo publish = TryToFindTopicPublishInfo(c, outbound.Topic);
        MessageQueue selected = publish.SelectOneMessageQueue();
        // MessageBatch has isBatch = true, so prepareForSend returns 0 directly (no compression)
        int sysFlag = PrepareForSend(outbound);
        return SendWithHooks(c, outbound, selected, timeout, sysFlag);
    }

    /// <summary>Pinned batch send, corresponding to <c>send(Collection&lt;Message&gt;, MessageQueue,
    /// long)</c>: after batching it goes straight to sendKernelImpl -- no route fetch, no queue
    /// selection, no retry, the whole batch lands in the specified queue. Sub-message
    /// validation, batch homogeneity and namespace are the same set of steps as the
    /// round-robin batch path.</summary>
    public SendResult SendBatch(List<Message> msgs, MessageQueue mq, int timeoutMillis = -1)
    {
        MQClientInstance c = GetClient();
        int timeout = timeoutMillis >= 0 ? timeoutMillis : _sendMsgTimeout;
        Message outbound = BuildBatchOutbound(msgs);
        // `send(Collection, MessageQueue, timeout)` -> `impl.send(batch(msgs), mq, timeout)`,
        // shares the same synchronous guard with the single-message path (MessageBatch extends
        // Message).
        CheckPinnedTopic(outbound.Topic, mq, PinnedTopicMismatchSync);
        int sysFlag = PrepareForSend(outbound);
        return SendWithHooks(c, outbound, mq, timeout, sysFlag);
    }

    /// <summary>Common batch prologue: per-message <c>Validators.CheckMessage</c> ->
    /// <c>MessageBatch.GenerateFromList</c> (homogeneity check, per-message UNIQ_KEY fill-in,
    /// whole batch encoded into the body) -> one more check over the whole batch -> wrap the
    /// namespace. Both the round-robin and the pinned batch path must go through here:
    /// bypassing it bypasses every local check.</summary>
    private Message BuildBatchOutbound(List<Message> msgs)
    {
        if (msgs is null || msgs.Count == 0)
        {
            throw new MQClientException("message list is empty");
        }

        // Corresponds to batch(): **every sub-message** goes through Validators.checkMessage;
        // skipping this step means the batch path bypasses all local validation -- an
        // oversized/empty body or an illegal topic could be sent.
        foreach (Message m in msgs)
        {
            Validators.CheckMessage(m, _maxMessageSize);
        }

        MessageBatch batch = MessageBatch.GenerateFromList(msgs);
        CheckMessage(batch);
        Message outbound = CloneMessage(batch);
        // Corresponds to MessageBatch.generateFromList + withNamespace: a batch topic also needs
        // the namespace applied
        if (_namespace.Length != 0)
        {
            outbound.Topic = NamespaceUtil.WrapNamespace(_namespace, batch.Topic);
        }
        return outbound;
    }

    /// <summary>
    /// <summary>Batch asynchronous send: corresponds to <c>send(Collection&lt;Message&gt;, SendCallback,
    /// timeout)</c> -> <c>defaultMQProducerImpl.send(batch(msgs), sendCallback, timeout)</c>,
    /// i.e. the batch branch <c>send_async</c> enters when it receives a list.
    ///
    // In this port batch only has the synchronous kernel, so "async" = running the synchronous
    // batch kernel on an <c>AsyncSenderExecutor_N</c> thread, with the result still delivered
    // from <c>NettyClientPublicExecutor_N</c>. Semantically there is no difference for the
    // caller: the submitting thread does not block, the callback runs exactly once, and
    // failures still return the permit. The whole prologue of
    // <see cref="SendAsync(Message,ISendCallback,int,MessageQueue?)"/> (queueing, budget
    // recheck, both backpressure gates) is fully in effect here, and the byte permit is
    // accumulated over the body length of <b>every sub-message in the whole batch</b> (an
    // empty body counts as 1, an empty list counts as 1 -- the same formula as the list branch
    // of <c>_back_pressure_msg_len</c>); charging a batch as if it were a single message would
    // let the whole batch occupy one slot, making the byte gate useless for batches.
    ///
    /// <paramref name="mq"/> non-null means the whole batch is pinned to that queue
    /// (<c>send(Collection, MessageQueue, SendCallback, timeout)</c>).
    ///
    /// Just like <see cref="SendBatch"/>, <b>every sub-message</b> first goes through
    /// <c>Validators.CheckMessage</c>: handing the <see cref="MessageBatch"/> straight to
    /// <see cref="SendAsync(Message,ISendCallback,int,MessageQueue?)"/> skips this step, which
    /// means the batch path bypasses local validation (an oversized body or an illegal topic
    /// could be sent). Validation/batching failures (empty list, oversized, inhomogeneous)
    /// always go <b>into the callback</b>: they happen inside the batching step on the pool
    /// thread and are caught by the outer catch of the async chain.
    /// </summary>
    /// </summary>
    public void SendBatchAsync(List<Message> msgs, ISendCallback callback, int timeoutMillis = -1,
        MessageQueue? mq = null)
    {
        // The permit count is computed on the caller thread (before queueing); the batch formula:
        // accumulate per message, an empty body counts as 1, an empty list counts as 1
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

    /// <summary>The batching step of batch async (runs on the pool thread): exactly the same
    /// sequence of actions as the opening of <see cref="SendBatch"/> -- per-message
    /// <c>Validators.CheckMessage</c> -> <c>GenerateFromList</c> (homogeneity check,
    /// per-message UNIQ_KEY fill-in, whole batch encoded into the body) -> one more check over
    /// the whole batch. Exceptions here are handed to the callback by the async chain's outer
    /// catch.</summary>
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

    // ---------------- Request-Reply (5.x)----------------
    // The 6 request overloads on the facade,
    // implemented in DefaultMQProducerImpl: 3 synchronous shapes (default route /
    // selector / specified queue, blocking for the response after sending) and 3 async
    // shapes (the same 3 routes x RequestCallback).
    // Common requester flow (prepareSendRequest):
    //   1. Write CORRELATION_ID (a random UUID), REPLY_TO_CLIENT (this client's clientId)
    //      and TTL (= timeout) onto the request message; the latter two are what the broker
    //      uses to find this connection again and what the responder echoes back.
    //   2. Register the waiting slot in the process-wide RequestFutureHolder under
    //      correlationId.
    //   3. The synchronous shape blocks after sending; the async shape returns immediately.
    //      The response is pushed back by the broker via PUSH_REPLY_MESSAGE_TO_CLIENT(326)
    //      and delivered into the waiting slot by MQClientInstance.ProcessReplyMessage;
    //      TTL scanning in RequestFutureHolder is the backstop for timeouts.
    //
    // The synchronous shape throws RequestTimeoutException on timeout (the message was sent
    // but no response arrived); a failure of the send itself throws MQClientException
    // (carrying the low-level cause). Failures of the async shape go to the callback's
    // OnException -- the bridging callback only marks the send result, and the user callback
    // is guaranteed to fire exactly once.
    //
    // REPLY_TO_CLIENT is the clientId -- the broker looks the channel up by it, so this
    // producer must have sent a heartbeat first (start() already starts the heartbeat thread;
    // this also issues one, matching prepareSendRequest's
    // sendHeartbeatToAllBrokerWithLock).

    /// <summary>Synchronous: default route (request(msg, timeout)).</summary>
    public Message Request(Message msg, int timeoutMillis = -1)
    {
        int timeout = timeoutMillis >= 0 ? timeoutMillis : _requestTimeout;
        CheckMessage(msg);
        Message outbound = WithNamespace(msg);
        return RequestSyncCore(outbound, null, timeout);
    }

    /// <summary>Synchronous: selector picks the queue (request(msg, selector, arg, timeout)).
    /// The selector receives the <b>original message</b> (the same rule as ordered message
    /// sending), and the namespace wrapper is only applied to the outbound copy.</summary>
    public Message Request(Message msg, IMessageQueueSelector selector, string arg,
        int timeoutMillis = -1)
    {
        int timeout = timeoutMillis >= 0 ? timeoutMillis : _requestTimeout;
        CheckMessage(msg);
        Message outbound = WithNamespace(msg);
        MQClientInstance c = GetClient();
        TopicPublishInfo publish = TryToFindTopicPublishInfo(c, outbound.Topic);
        MessageQueue selected = selector.Select(publish.MsgQueueList, msg, arg);
        return RequestSyncCore(outbound, selected, timeout);
    }

    /// <summary>Synchronous: pinned send to the specified queue (request(msg, mq, timeout)).</summary>
    public Message Request(Message msg, MessageQueue mq, int timeoutMillis = -1)
    {
        int timeout = timeoutMillis >= 0 ? timeoutMillis : _requestTimeout;
        CheckMessage(msg);
        Message outbound = WithNamespace(msg);
        return RequestSyncCore(outbound, mq, timeout);
    }

    /// <summary>Asynchronous: default route + callback (request(msg, callback, timeout)).
    /// The callback fires exactly once: response arrival (PutResponse), send failure
    /// (RequestFail) or TTL timeout (RequestFutureHolder.ScanExpiredRequest) -- one of the
    /// three, never more than one.</summary>
    public void Request(Message msg, RequestCallback callback, int timeoutMillis = -1)
    {
        int timeout = timeoutMillis >= 0 ? timeoutMillis : _requestTimeout;
        CheckMessage(msg);
        Message outbound = WithNamespace(msg);
        RequestAsyncCore(outbound, null, timeout, callback);
    }

    /// <summary>Asynchronous: selector picks the queue + callback (request(msg, selector, arg, callback, timeout)).</summary>
    public void Request(Message msg, IMessageQueueSelector selector, string arg,
        RequestCallback callback, int timeoutMillis = -1)
    {
        int timeout = timeoutMillis >= 0 ? timeoutMillis : _requestTimeout;
        CheckMessage(msg);
        Message outbound = WithNamespace(msg);
        MQClientInstance c = GetClient();
        TopicPublishInfo publish = TryToFindTopicPublishInfo(c, outbound.Topic);
        MessageQueue selected = selector.Select(publish.MsgQueueList, msg, arg);
        RequestAsyncCore(outbound, selected, timeout, callback);
    }

    /// <summary>Asynchronous: pinned send to the specified queue + callback (request(msg, mq, callback, timeout)).</summary>
    public void Request(Message msg, MessageQueue mq, RequestCallback callback,
        int timeoutMillis = -1)
    {
        int timeout = timeoutMillis >= 0 ? timeoutMillis : _requestTimeout;
        CheckMessage(msg);
        Message outbound = WithNamespace(msg);
        RequestAsyncCore(outbound, mq, timeout, callback);
    }

    /// <summary>Synchronous core: register the waiting slot -> send (on failure mark
    /// !sendRequestOk and wake in place) -> wait for the response -> remove in finally.
    /// Corresponds to the try/finally section of the synchronous request.</summary>
    private Message RequestSyncCore(Message outbound, MessageQueue? mq, int timeout)
    {
        MQClientInstance c = GetClient();
        RequestResponseFuture future = PrepareRequest(c, outbound, timeout, callback: null);
        try
        {
            long elapsed = UtilAll.CurrentTimeMillis() - future.BeginTimestamp;
            int remaining = (int)(timeout > elapsed ? timeout - elapsed : 0);

            // On a send failure mark the waiting slot !sendRequestOk and wake it immediately (so
            // WaitRequestResponse takes the failure branch); there is no protocol difference -- the
            // response is pushed back by the broker over **another** 326 channel, unrelated to the mode
            // of this send.
            try
            {
                if (mq is null)
                {
                    Send(outbound, remaining);
                }
                else
                {
                    Send(outbound, mq, remaining);
                }
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
            RequestFutureHolder.Instance.RemoveRequest(future.CorrelationId);
        }
    }

    /// <summary>Asynchronous core: register the waiting slot -> hand off to the async send
    /// chain, return immediately. There is **no** removal in finally: in the async shape the
    /// waiting slot is removed by whichever of response arrival / send failure / TTL scan gets
    /// there first.</summary>
    private void RequestAsyncCore(Message outbound, MessageQueue? mq, int timeout,
        RequestCallback callback)
    {
        MQClientInstance c = GetClient();
        RequestResponseFuture future = PrepareRequest(c, outbound, timeout, callback);

        long elapsed = UtilAll.CurrentTimeMillis() - future.BeginTimestamp;
        int remaining = (int)(timeout > elapsed ? timeout - elapsed : 0);

        // Async send (the request family is always ASYNC on the send side): the bridge only marks
        // the send result;
        // the user callback is triggered by response arrival (PutResponse), the timeout scan
        // (ScanExpiredRequest) or a send failure (RequestFail) -- invoking the user callback inside
        // the bridge would fire onSuccess(null) early and duplicate the later callback (the
        // pitfall the request family comments call out).
        SendAsync(outbound, new RequestSendBridge(future), remaining, mq);
    }

    /// <summary>Corresponds to prepareSendRequest + waiting slot registration (the common
    /// prologue of the request overloads): writes CORRELATION_ID / REPLY_TO_CLIENT / TTL,
    /// makes sure the route is known and issues a heartbeat, then registers the future in the
    /// process-wide RequestFutureHolder under correlationId.</summary>
    private RequestResponseFuture PrepareRequest(MQClientInstance c, Message outbound, int timeout,
        RequestCallback? callback)
    {
        string correlationId = RequestReply.CreateCorrelationId();
        outbound.PutProperty(MessageConst.PropertyCorrelationId, correlationId);
        outbound.PutProperty(MessageConst.PropertyReplyToClient, c.ClientId);
        outbound.PutProperty(MessageConst.PropertyMessageTTL,
            timeout.ToString(CultureInfo.InvariantCulture));

        // Matches prepareSendRequest: make sure the route is known, then issue one heartbeat --
        // without registering as a producer on the broker, the broker cannot find the channel to
        // push the response back to.
        try
        {
            c.GetTopicPublishInfo(outbound.Topic);
            SendHeartbeatToAllBroker();
        }
        catch (Exception e)
        {
            ClientLog.Debug("request: prepare route/heartbeat failed: " + e.Message);
        }

        var future = new RequestResponseFuture(correlationId, timeout, callback);
        RequestFutureHolder.Instance.PutRequest(correlationId, future);
        return future;
    }

    /// <summary>Corresponds to requestFail(correlationId): on a send failure remove the
    /// waiting slot and fire the callback in the failed state (if it cannot be removed the
    /// response/timeout already got there first, so ignore it --
    /// remove-claims-ownership).</summary>
    private static void RequestFail(string correlationId)
    {
        RequestResponseFuture? future = RequestFutureHolder.Instance.RemoveRequest(correlationId);
        if (future is null)
        {
            return;
        }

        future.SendRequestOk = false;
        future.PutResponseMessage(null);
        try
        {
            future.ExecuteRequestCallback();
        }
        catch (Exception e)
        {
            ClientLog.Warn("execute requestCallback in requestFail, and callback throw: "
                           + e.Message);
        }
    }

    /// <summary>The send bridge of an async request (the SendCallback embedded in
    /// request(...)): on success it only marks sendRequestOk; on failure it records the cause
    /// and goes through <see cref="RequestFail"/>. The user callback is not fired here.</summary>
    private sealed class RequestSendBridge : ISendCallback
    {
        private readonly RequestResponseFuture _future;

        public RequestSendBridge(RequestResponseFuture future)
        {
            _future = future;
        }

        public void OnSuccess(SendResult sendResult)
        {
            _future.SendRequestOk = true;
        }

        public void OnException(Exception error)
        {
            _future.Cause = error;
            RequestFail(_future.CorrelationId);
        }
    }

    /// <summary>Corresponds to waitResponse: timeout and send failure throw different
    /// exceptions.</summary>
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

    // ---------------- Transactional messages ----------------
    // Mirrors the **two phases** of sendMessageInTransaction:
    //   1) Half message: set TRAN_MSG / PGROUP properties on msg, sysFlag gets
    //      TRANSACTION_PREPARED_TYPE;
    //   2) Local transaction: executed only on SendOk; Flush* / SlaveNotAvailable ->
    //      Rollback;
    //   3) EndTransaction: tells the broker commit / rollback / unknown via
    //      END_TRANSACTION(37, oneway);
    //   4) On Unknow the broker runs CHECK_TRANSACTION_STATE(39) and, after calling
    //      listener.CheckLocalTransaction, sends END_TRANSACTION(FromTransactionCheck=true).
    public TransactionSendResult SendMessageInTransaction(Message msg, ITransactionListener listener,
        string arg = "")
    {
        if (listener is null)
        {
            throw new MQClientException("tranExecutor is null");
        }

        // ensureNotDelayedForTransactional: transactional messages do not support delayed
        // delivery.
        // Note: C#'s DelayTimeLevel property has a default value of 0 (the getter returns 0 for a
        // missing key), so we must check whether the key is really present in properties -- the
        // property value alone is not enough.
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

        // Half-message marker: the broker writes the message to RMQ_SYS_TRANS_HALF_TOPIC on this
        // basis and waits for END_TRANSACTION
        outbound.PutProperty(MessageConst.PropertyTransactionPrepared, "true");
        outbound.PutProperty(MessageConst.PropertyProducerGroup, _producerGroup);
        _txListener = listener;

        // Compression is the same as for a normal send; the transaction type bit is then layered
        // on (sendKernelImpl sets PREPARED once it sees TRAN_MSG)
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
            // A failure to end the broker transaction is only warned about and does not affect the
            // returned result
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

    // ---------------- Heartbeat (broker transaction check depends on it) ----------------

    private void HeartbeatLoop()
    {
        ClientLog.SetThreadName("ProducerHeartbeatThread");
        int intervalMs = _heartbeatIntervalMillis;
        // Fixed rate: the first beat is immediate (MQClientInstance's heartbeat task has
        // initialDelay=1s, and this port has always sent one right away from the old behaviour, so
        // it is kept), then one beat every intervalMs anchored to the scheduled instant (see
        // Schedules) -- the old "work, then sleep 30s in 100ms slices" form has to sleep 30.6s on
        // macOS, stretching the heartbeat period by 2%.
        long next = Environment.TickCount64;
        while (_heartbeatRunning)
        {
            Schedules.WaitUntil(() => _heartbeatRunning, next);
            next += intervalMs;
            try
            {
                SendHeartbeatToAllBroker();
            }
            catch (Exception e)
            {
                ClientLog.Debug("producer heartbeat failed: " + e.Message);
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

        // Only ProducerData is carried: matches the registration content of the producerTable in
        // MQClientInstance. The broker registers that group in the ProducerManager (the transaction
        // check reaches this producer through that channel).
        var hb = new HeartbeatData(_clientId ?? string.Empty)
        {
            HeartbeatFingerprint = 0,  // take the V1 registration path, the safest
        };
        hb.AddProducerData(new ProducerData(_producerGroup));

        return c.SendHeartbeatToAllBrokers(hb, isRebalance: false, includeSlaves: false);
    }

    // Heartbeat thread (periodic heartbeat; interval defaults to 30s)
    private Thread? _heartbeatThread;
    private volatile bool _heartbeatRunning;
    private readonly int _heartbeatIntervalMillis = 30000;

    // The listener used by the most recent SendMessageInTransaction (the broker calls it back
    // during the transaction check). Reference semantics: the caller must guarantee that its
    // lifetime covers the transaction check (the same contract as TransactionListener).
    private ITransactionListener? _txListener;
    private readonly List<Thread> _txThreads = new();
    private readonly object _txThreadsLock = new();

    /// <summary>
    /// Tells the broker the final transaction state via END_TRANSACTION(37, oneway) (matching
    /// endTransaction / checkTransactionState). fromCheck=true means this is the closing step
    /// of a **check**, and fields such as the offsets come from the broker's check header
    /// (sendResult is not available then).
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
            // uniqueKey = the UNIQ_KEY property of msg, falling back to msgId only if absent
            string? uniqueKey = checkMsg?.GetProperty(MessageConst.PropertyUniqClientMessageIdKeyidx);
            header.MsgId = string.IsNullOrEmpty(uniqueKey) ? checkMsg?.MsgId : uniqueKey;
            addr = brokerAddr;
        }
        else
        {
            // id = decodeMessageId(offsetMsgId != null ? offsetMsgId : msgId)
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
            // This also uses findBrokerAddressInPublish (master only), and it oneways **without a null
            // check**; an early guard is kept here (cleaner than a null reference), but the address
            // source must likewise be master-only -- slaves do not accept END_TRANSACTION.
            addr = c.FindBrokerAddressInPublish(sendResult.MessageQueue?.BrokerName ?? string.Empty);
        }

        RemotingCommand request =
            RemotingCommand.CreateRequestCommand(RequestCode.EndTransaction, header);
        if (localExceptionText is not null)
        {
            request.Remark = "executeLocalTransactionBranch exception: " + localExceptionText;
        }

        if (addr.Length == 0)
        {
            throw new MQClientException("no broker address for end transaction");
        }

        c.RemotingClient.InvokeOneway(addr, request);

        // Transaction closing trace (corresponds to executeEndTransactionHook).
        // It must be called after END_TRANSACTION has really been sent, so that
        // transactionState in the trace is the final state. On the check path (fromCheck=true) msg
        // is the MessageExt the broker brought back, so prefer it.
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
    /// The entry point for a transaction check initiated by the broker
    /// (CHECK_TRANSACTION_STATE=39), called back by the transport layer. The check is sent by
    /// the broker with invokeOneway and expects no response, so null is returned.
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

        // The broker encodes the whole MessageExt into the body
        // (Broker2Client.checkProducerTransactionState)
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

        // Run the check callback on a separate thread so the reader thread is not blocked
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

    // ---------------- Query / Admin ----------------

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

    /// <summary>Recalls a scheduled/delayed message and returns the uniqKey of the recalled
    /// message.
    ///
    /// The validation order lines up with <c>recallMessage</c> step by step:
    /// state -> checkTopic -> forbid retry/DLQ -> decode the handle -> warm up the route ->
    /// locate the broker -> send the request. The first three steps must run <b>before</b> any
    /// network call, otherwise a mistyped handle costs a full RPC timeout.
    /// The handle comes from the scheduled message's <see cref="SendResult.RecallHandle"/>; a
    /// normal message has none.</summary>
    /// <remarks>
    /// Semantics: on the protocol side <c>findBrokerAddrByTopic</c> returns <b>all</b> broker
    /// addresses of that topic and then randomly picks one, whereas here the first usable
    /// address in the route is taken -- equivalent for a single broker, and in a multi-broker
    /// scenario both can only land on an address other than the one in the handle, so the broker
    /// ultimately rejects it with <c>ILLEGAL_OPERATION</c> (brokerName mismatch) and the
    /// semantics are unchanged.</summary>
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

        // This only calls tryToFindTopicPublishInfo to warm up the route; the return value is
        // unused, but **exceptions are still propagated**
        TryToFindTopicPublishInfo(c, realTopic);

        // findBrokerAddressInPublish(brokerName) (master only, no route refresh) degrades to
        // findBrokerAddrByTopic(topic) -- that path goes through SelectBrokerAddr() and **allows**
        // landing on a slave.
        string addr = c.FindBrokerAddressInPublish(handle.BrokerName);
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
            // The protocol reflection name of the inherited field is bname; writing brokerName makes
            // the broker silently drop it.
            Bname = handle.BrokerName,
        };
        return c.RecallMessage(addr, header, _sendMsgTimeout);
    }

    public void CreateTopic(string key, string newTopic, int queueNum = 4)
    {
        MQClientInstance c = GetClient();
        const int perm = 6; // PERM_READ | PERM_WRITE
        // Corresponds to createTopic: validate the raw topic name locally first, then look up the
        // system topic
        Validators.CheckTopic(newTopic);
        Validators.IsSystemTopic(newTopic);
        // createTopic ignores the key argument and always creates the route with the default topic
        // (MixAll.DefaultTopic)
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

    // ---------------- Internal utility: split on semicolon ----------------

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

// Transactional producer (the TransactionMQProducer contract): a TransactionListener can
// be preset
public class TransactionMQProducer : DefaultMQProducer
{
    public TransactionMQProducer(string producerGroup = MixAll.DefaultProducerGroup)
        : base(producerGroup)
    {
    }

    public ITransactionListener? TransactionListener { get; set; }

    // Two-phase transaction: run the local transaction with the preset listener (half message
    // -> local transaction -> END_TRANSACTION -> broker check).
    public TransactionSendResult SendMessageInTransaction(Message msg, string arg = "")
    {
        if (TransactionListener is null)
        {
            throw new MQClientException("transaction listener is not set");
        }

        return SendMessageInTransaction(msg, TransactionListener, arg);
    }
}
