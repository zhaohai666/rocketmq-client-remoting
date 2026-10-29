// Push consumer.
//
// Architecture (three layers):
//   1. Pull layer: one pull thread per queue (concurrent long polling --
//      the broker suspends a long-poll request per queue and returns
//      immediately when messages arrive). Pulled messages go into the
//      _pending buffer and the pull cursor advances to nextBeginOffset.
//   2. Dispatch layer: a single dispatch thread takes batches of
//      ConsumeMessageBatchMaxSize from the buffer and hands them to the
//      IMessageListener; RECONSUME_LATER / failed batches are sent back one
//      by one to the %RETRY% topic (delay level 3+reconsumeTimes; the broker
//      moves them to %DLQ% once maxReconsumeTimes is exceeded).
//   3. Offset layer: _consumeOffsetTable records the "consumed offset" and
//      commits it to the broker every 5s via UPDATE_CONSUMER_OFFSET
//      (periodic full flush); on startup QUERY_CONSUMER_OFFSET runs first.
//
// Key engineering points (learned from live verification, do not delete):
//   - Pulling must run per-queue in parallel: with a single sequential
//     long-poll thread, an idle queue's suspend blocks delivery for all
//     other queues (caused "first batch arrives, everything after is late").
//   - An idle queue's long poll outlasts our own request timeout, so idle
//     queues still time out periodically on the client side -- this is BENIGN;
//     log at debug, never ERROR.
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Runtime.ExceptionServices;
using System.Text;
using System.Threading;

using RocketMQ.Common;
using RocketMQ.Remoting;
using RocketMQ.Remoting.Protocol;

namespace RocketMQ.Client;

/// <summary>
/// Message selector (filters by tag set or SQL92 expression).
/// </summary>
public sealed class MessageSelector
{
    public string Type { get; set; } = ExpressionType.TAG;
    public string Expression { get; set; } = "*";

    public static MessageSelector ByTag(string tag) =>
        new() { Type = ExpressionType.TAG, Expression = tag };

    public static MessageSelector BySql(string sql) =>
        new() { Type = ExpressionType.Sql92, Expression = sql };
}

/// <summary>
/// Push consumer.
/// </summary>
/// <summary>
/// POP delay level table (unit: SECONDS).
/// This is not the same table as the send-side delay levels (first level 1s);
/// do not mix them up.
/// </summary>
public static class PopDelayLevelDefaults
{
    public static readonly int[] Table = { 10, 30, 60, 120, 180, 240, 300, 360,
                                           420, 480, 540, 600, 1200, 1800, 3600, 7200 };
}

/// <summary>
/// Queue state in POP mode: popped-but-not-acked count + whether rebalance
/// has already revoked it.
/// <para>
/// Unlike the pull-mode ProcessQueue, POP has NO "pulled but not consumed"
/// buffer: messages are handed to consume threads as soon as they are popped,
/// and confirmation relies on ack. Only two things are tracked here: how many
/// messages are popped-but-unacked (for flow control) and whether rebalance
/// has revoked the queue (after revocation this batch is neither consumed
/// nor acked; the broker revives and redelivers it when invisibleTime
/// expires).
/// </para>
/// </summary>
public sealed class PopProcessQueue
{
    private readonly object _lock = new();
    private int _waitAckCounter;
    private volatile bool _dropped;

    /// <summary>Timestamp (ms) of the most recent pop ATTEMPT; the field's
    /// initial value is likewise "now". The stall detector reads it on the
    /// POP path, so this must not be write-only.</summary>
    private long _lastPopTimestamp = UtilAll.CurrentTimeMillis();

    /// <summary>Read the most recent pop timestamp (ms).</summary>
    public long LastPopTimestamp => Interlocked.Read(ref _lastPopTimestamp);

    /// <summary>Overwrite the most recent pop timestamp (ms).</summary>
    public void Touch(long timestampMillis)
    {
        Interlocked.Exchange(ref _lastPopTimestamp, timestampMillis);
    }

    public void IncFoundMsg(int n)
    {
        lock (_lock) { _waitAckCounter += n; }
    }

    /// <summary>Accumulates the argument as given, so decrementing the debt means
    /// passing a negative count: <c>DecFoundMsg(-msgs.Count)</c>. The consume-result
    /// path relies on that, and a caller that handed over a positive count would
    /// raise the debt instead of writing it off.</summary>
    public void DecFoundMsg(int n)
    {
        lock (_lock) { _waitAckCounter += n; }
    }

    public int Ack()
    {
        lock (_lock) { return --_waitAckCounter; }
    }

    public int WaitAckCount()
    {
        lock (_lock) { return _waitAckCounter; }
    }

    public bool IsDropped() => _dropped;

    public void SetDropped(bool v) => _dropped = v;
}

/// <summary>Ack / extend-invisible-time target decoded from POP_CK.</summary>
public sealed class PopCkTarget
{
    /// <summary>Real topic restored by getRealTopic according to retryFlag.</summary>
    public string Topic { get; set; } = string.Empty;

    /// <summary>CK segment 6.</summary>
    public string BrokerName { get; set; } = string.Empty;

    /// <summary>CK segment 7.</summary>
    public int QueueId { get; set; }

    /// <summary>CK segment 8 = consumeQueue offset (NOT the commitlog offset).</summary>
    public long Offset { get; set; }

    /// <summary>The CK string echoed back verbatim.</summary>
    public string ExtraInfo { get; set; } = string.Empty;
}

public sealed class DefaultMQPushConsumer
{
    // ---------------- Config ----------------
    public string ConsumerGroup { get; private set; }

    private string _namespace = string.Empty;

    // Namespace: when non-empty, topics / groups are prefixed with "ns%"
    // before interacting with the broker (the same wrapping is applied to
    // consumerGroup at startup).
    public string Namespace
    {
        get => _namespace;
        set => _namespace = value ?? string.Empty;
    }

    /// <summary>New 5.x namespace (namespaceV2): when non-empty, the
    /// <see cref="NamespaceRpcHook"/> (first in the hook chain) adds the
    /// <c>nsd=true</c> / <c>ns</c> extension headers to every request (the hook
    /// reads the same-named property of this config). The hook re-reads this
    /// property per request, so setting it after Start() also takes effect
    /// from the next request onward.</summary>
    public string NamespaceV2
    {
        get => _namespaceV2;
        set => _namespaceV2 = value ?? string.Empty;
    }

    private string _namespaceV2 = string.Empty;

    // ---------------- unitName / unitMode / enableStreamRequestType ----------------
    // The three unitization/streaming switches. ⚠ Must be set before Start():
    // unitName / @STREAM shape the clientId, and stream shapes the request hook
    // chain (ReqT must be part of the ACL signature content).
    /// <summary>Unit name: becomes the <c>@&lt;unitName&gt;</c> segment of the
    /// clientId and is also appended to the dynamic addressing URL.</summary>
    public string UnitName
    {
        get => _unitName;
        set => _unitName = value ?? string.Empty;
    }

    /// <summary>
    /// Unitization switch. The push consumer applies it in three places:
    /// the message filtering context, the heartbeat's
    /// <c>ConsumerData.unitMode</c> (protocol field; the broker marks the
    /// %RETRY% topic with UNIT_SUB accordingly), and the send-back request
    /// header (see the notes at SendMessageBack).
    /// </summary>
    public bool UnitMode
    {
        get => _unitMode;
        set => _unitMode = value;
    }

    /// <summary>
    /// When true, every request carries <c>ReqT=0</c> and the clientId gains
    /// a trailing <c>@STREAM</c> segment. Default off for the push consumer
    /// (only the pull / lite consumers set it true in their constructors).
    /// </summary>
    public bool EnableStreamRequestType
    {
        get => _enableStreamRequestType;
        set => _enableStreamRequestType = value;
    }

    /// <summary>
    /// Whether the subscription expression is sent along with pull requests
    /// (default false). When enabled, every PULL_MESSAGE carries the
    /// subscription expression (SUBSCRIPTION flag set) so the broker does the
    /// filtering; disabled by default -- tag filtering falls back to the
    /// client-side <c>FilterMessagesForDelivery</c>.
    /// </summary>
    public bool PostSubscriptionWhenPull
    {
        get => _postSubscriptionWhenPull;
        set => _postSubscriptionWhenPull = value;
    }

    /// <summary>
    /// Default 30000ms: the route refresh period for topics in use, passed
    /// through to MQClientInstance at Start() (changing it afterwards does not
    /// reschedule already-started periodic tasks).
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

    /// <summary>
    /// Default 5000ms:
    /// the background offset flush period. The first flush happens at the 10s
    /// initialDelay; the final flush in Shutdown() is independent of this value
    /// (raising it only postpones timing, no offsets are lost).
    /// </summary>
    public int PersistConsumerOffsetIntervalMillis
    {
        get => _persistConsumerOffsetIntervalMillis;
        set => _persistConsumerOffsetIntervalMillis = value;
    }

    // ---------------- ACL authentication ----------------
    // Must be called before Start(): the hook is bound to MQClientInstance
    // inside Start().
    public void SetRpcHook(IRpcHook hook) => _rpcHook = hook;

    /// <summary>Convenience entry: build an AclClientRPCHook from
    /// accessKey/secretKey (optional securityToken).</summary>
    public void SetCredentials(string accessKey, string secretKey, string securityToken = "")
        => _rpcHook = new AclClientRPCHook(new SessionCredentials(accessKey, secretKey, securityToken));

    // ---------------- Message trace config (enableMsgTrace / customizedTraceTopic) ----------------
    // When enabled, Start() creates an AsyncTraceDispatcher (Type=Consume) and
    // registers a ConsumeMessageTraceHook.
    public bool EnableTrace
    {
        get => _enableTrace;
        set => _enableTrace = value;
    }

    /// <summary>Consume-side naming entry, equivalent to <see cref="EnableTrace"/>.</summary>
    public void SetEnableMsgTrace(bool enable) => _enableTrace = enable;

    /// <summary>Custom trace topic; empty falls back to the system default RMQ_SYS_TRACE_TOPIC.</summary>
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

    /// <summary>Consume timeout (minutes, default 15) -- decides whether the
    /// trace SubAfter contextCode is TIME_OUT.</summary>
    public int ConsumeTimeout { get; set; } = 15;

    /// <summary>Register a consume hook.</summary>
    public void RegisterConsumeMessageHook(IConsumeMessageHook hook)
    {
        if (hook is not null)
        {
            _consumeMessageHooks.Add(hook);
        }
    }

    public bool HasConsumeMessageHook() => _consumeMessageHooks.Count > 0;

    /// <summary>Register a pre-delivery filter hook.
    /// Messages dropped by the hook: silently skipped on the pull path (the
    /// offset still advances), immediately acked on the POP path.</summary>
    public void RegisterFilterMessageHook(IFilterMessageHook hook)
    {
        if (hook is not null)
        {
            _filterMessageHooks.Add(hook);
        }
    }

    public bool HasFilterMessageHook() => _filterMessageHooks.Count > 0;

    public int FilterMessageHookCount() => _filterMessageHooks.Count;

    /// <summary>Dropped count (pull + POP combined), for live scripts and unit tests.</summary>
    public long FilteredMessageCount() => Interlocked.Read(ref _filteredMessageCount);

    /// <summary>Run the filter hooks in order; <b>all exceptions are swallowed</b>
    /// and logged as errors. Opposite of CheckForbiddenHook: a broken filter
    /// hook must not affect consumption.</summary>
    public void ExecuteFilterMessageHook(FilterMessageContext context)
    {
        foreach (IFilterMessageHook hook in _filterMessageHooks)
        {
            try
            {
                hook.FilterMessage(context);
            }
            catch (Exception e)
            {
                ClientLog.Error("execute hook error. hookName=" + hook.HookName() + ": " + e.Message);
            }
        }
    }

    /// <summary>Pre-delivery filtering shared by the pull / POP paths:
    /// ① client-side second-pass tag filter (the broker's codeSet hash filter
    /// can collide and wrongly admit messages)
    /// ② IFilterMessageHook (may rewrite MsgList).
    /// <para>① both the pull and POP paths share the same gate: the broker
    /// filters by the tag's <b>hash (codeSet)</b>, which can collide and
    /// wrongly admit, so the client must re-check by string. The guard
    /// <c>!tagsSet.isEmpty()</c> means subscribing to "*" (SUB_ALL) skips
    /// filtering -- so FilterAPI.BuildSubscriptionData must leave the set
    /// empty for SUB_ALL.</para>
    /// <para>When sub is null, only ② runs.</para></summary>
    public List<MessageExt> FilterMessagesForDelivery(MessageQueue mq, SubscriptionData? sub,
        List<MessageExt> msgs)
    {
        List<MessageExt> result = msgs;
        if (result.Count == 0)
        {
            return result;
        }

        if (sub is not null && sub.TagsSet.Count > 0 && !sub.ClassFilterMode)
        {
            var kept = new List<MessageExt>(result.Count);
            foreach (MessageExt m in result)
            {
                string? tags = m.Tags;
                if (!string.IsNullOrEmpty(tags) && sub.TagsSet.Contains(tags!))
                {
                    kept.Add(m);
                }
            }

            result = kept;
        }

        if (_filterMessageHooks.Count > 0 && result.Count > 0)
        {
            var context = new FilterMessageContext(ConsumerGroup, result, mq)
            {
                // The unitization flag rides along in the context so hooks can
                // tell whether this is unitized traffic.
                UnitMode = _unitMode,
            };
            ExecuteFilterMessageHook(context);
            result = context.MsgList;
        }

        return result;
    }

    /// <summary>Set difference of original \ kept (the POP path must ack the
    /// removed messages). Reference-identity difference breaks after a List
    /// copy, so the difference is taken by MsgId instead (semantically
    /// equivalent).</summary>
    public static List<MessageExt> DroppedMessages(List<MessageExt> original, List<MessageExt> kept)
    {
        var dropped = new List<MessageExt>();
        if (kept.Count >= original.Count)
        {
            return dropped;
        }

        var keptIds = new HashSet<string>(StringComparer.Ordinal);
        foreach (MessageExt m in kept)
        {
            keptIds.Add(m.MsgId);
        }

        foreach (MessageExt m in original)
        {
            if (!keptIds.Contains(m.MsgId))
            {
                dropped.Add(m);
            }
        }

        return dropped;
    }

    /// <summary>POP-only third leg of the pre-delivery pipeline (tag re-check
    /// → IFilterMessageHook → this): drop every popped row whose reconsume
    /// count is <b>strictly greater</b> than the effective budget. The budget
    /// is the consumer's maxReconsumeTimes knob read through
    /// <see cref="MaxReconsumeTimesOrDefault"/> -- <c>-1</c> means the broker
    /// default 16, NOT unlimited (the unlimited reading belongs to the orderly
    /// side's helper and must not be used here). The caller diffs the result
    /// against the original popped list and acks every dropped row: the row is
    /// done, its retries are spent, and without the ack the broker would revive
    /// and redeliver it when invisibleTime expires.
    /// <para>This step deliberately lives OUTSIDE
    /// <see cref="FilterMessagesForDelivery"/>: that helper is shared with the
    /// pull path, which has no ack to give and whose offset advances anyway --
    /// the pull path must keep delivering an over-budget row (dropping it in
    /// the shared helper would silently skip it without any budget verdict
    /// being observable).</para></summary>
    public List<MessageExt> DropOverReconsumeBudget(List<MessageExt> msgs)
    {
        if (msgs.Count == 0)
        {
            return msgs;
        }

        int budget = MaxReconsumeTimesOrDefault();
        var kept = new List<MessageExt>(msgs.Count);
        foreach (MessageExt m in msgs)
        {
            if (m.ReconsumeTimes > budget)
            {
                ClientLog.Info("pop message over the reconsume budget, message is being acked"
                    + " and dropped before delivery: msgId=" + m.MsgId
                    + ", reconsumeTimes=" + m.ReconsumeTimes
                    + ", budget=" + budget);
                continue;
            }

            kept.Add(m);
        }

        return kept;
    }

    // ---------------- POP mode (5.x lightweight consumption) ----------------
    // When off, the original pull long-poll path runs exactly as before.
    public bool PopMode { get; set; }

    /// <summary>TLS (switch name tls.enable; default reads env ROCKETMQ_TLS_ENABLE).</summary>
    /// <remarks>
    /// The initial value must read env here: MQClientInstance is instantiated
    /// with a <c>bool</c> (not <c>bool?</c>), so a default <c>false</c> would
    /// override the env fallback inside RemotingClient.
    /// </remarks>
    public bool TlsEnable
    {
        get => _tlsEnable;
        set => _tlsEnable = value;
    }

    private bool _tlsEnable = MQClientInstance.TlsEnabledFromEnv();

    private TlsOptions? _tlsOptions;

    /// <summary>
    /// TLS details (CaCert=strict CA validation; ClientCert/ClientKey=mTLS;
    /// ServerName=hostname override).
    /// No CaCert = test-mode (trust self-signed).
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

    public long PopInvisibleTime { get; set; } = 60000;

    /// <summary>Max messages per POP (protocol field popBatchNums, default 32;
    /// the broker replies INVALID_PARAMETER for &gt;32).</summary>
    public int PopBatchNums { get; set; } = 32;

    /// <summary>Upper bound of this queue's "popped but unacked" counter; POP
    /// pauses once exceeded (default 96).</summary>
    public int PopThresholdForQueue { get; set; } = 96;

    /// <summary>
    /// POP long-poll suspend duration. 0 = short polling (the broker returns
    /// immediately or NO_NEW_MSG).
    /// ⚠ When non-zero, the request timeout must be greater than it,
    /// otherwise the client times out first.
    /// </summary>
    public int PopPollTimeMillis { get; set; } = 15000;

    public int PopTimeoutMillis { get; set; } = 25000;

    /// <summary>Delay gradient (seconds) used to extend the invisible time on
    /// consume failure.</summary>
    public List<int> PopDelayLevel { get; set; } = new(PopDelayLevelDefaults.Table);

    // POP invisibleTime valid range [5s, 300s]: out-of-range values always
    // fall back to 60000.
    // Protocol ConsumeInitMode: 0=MIN (pop from head), 1=MAX (pop from tail)
    public const int ConsumeInitModeMin = 0;
    public const int ConsumeInitModeMax = 1;

    public const long MinPopInvisibleTime = 5000;
    public const long MaxPopInvisibleTime = 300000;

    // ACL hook, bound to MQClientInstance's transport layer at Start()
    private IRpcHook? _rpcHook;

    // The three unitization/stream switches. The push consumer sets neither
    // unitMode nor enableStreamRequestType (only the pull / lite constructors
    // set true); all default to off.
    private string _unitName = string.Empty;
    private bool _unitMode;
    private bool _enableStreamRequestType;

    // Default false (the expression is not sent with the pull; tag filtering
    // goes through the client-side FilterMessagesForDelivery).
    private bool _postSubscriptionWhenPull;

    // Master/slave selection table (key = queue key string):
    // every pull response's suggestWhichBrokerId is written back here, and
    // the next pull picks master/slave accordingly.
    private readonly Dictionary<string, long> _pullFromWhichNode = new(StringComparer.Ordinal);

    // The two periods (default 30000ms / 5000ms, finalized at Start())
    private int _pollNameServerIntervalMillis = 30000;
    private bool _decodeReadBody = true;
    private int _mqClientApiTimeout = MQClientInstance.MqClientApiTimeoutMillis;
    private bool _enableConcurrentHeartbeat;
    private int _concurrentHeartbeatThreadPoolSize = Environment.ProcessorCount;
    private int _maxPageSizeInGetMetadata = MQClientInstance.DefaultMaxPageSizeInGetMetadata;
    private int _persistConsumerOffsetIntervalMillis = 5000;

    // ---------------- Message trace (consume side) ----------------
    private bool _enableTrace;
    private string _traceTopic = MixAll.TraceTopic;
    private int _traceMsgBatchNum = 10;
    private readonly List<IConsumeMessageHook> _consumeMessageHooks = new();
    private readonly List<IFilterMessageHook> _filterMessageHooks = new();
    private long _filteredMessageCount;
    private AsyncTraceDispatcher? _traceDispatcher;

    private string _instanceName = MixAll.DefaultInstanceName;
    private string _clientId = string.Empty;
    private string _messageModel = RocketMQ.Remoting.Protocol.MessageModel.Clustering;
    // Queue allocation strategy (constructor parameter defaults to
    // new AllocateMessageQueueAveragely()).
    private IAllocateMessageQueueStrategy? _allocateMessageQueueStrategy = new AllocateMessageQueueAveragely();
    private string _consumeFromWhere = RocketMQ.Remoting.Protocol.ConsumeFromWhere.ConsumeFromLastOffset;

    // Start time for CONSUME_FROM_TIMESTAMP, in "yyyyMMddHHmmss". The default is
    // boot time minus 30 minutes: an empty string would degrade the policy into
    // "consume from right now" and silently drop the backlog.
    private string _consumeTimestamp =
        UtilAll.TimeMillisToHumanString3(UtilAll.CurrentTimeMillis() - 30 * 60 * 1000L);

    // ---- Consume thread pool (consumeThreadMin/Max) ----
    // The defaults are min=20 AND max=20 -- both sides are 20
    // (the 4.x era was min=20/max=64, which this port used early on and is
    // now obsolete).
    // This implementation's PULL path is "one pull thread per queue + a
    // single dispatch thread"; only the POP path uses a real thread pool
    // (the POP concurrent consume service), so CorePoolSize directly
    // determines the POP consume concurrency (with an unbounded queue, max
    // is effectively unused); under the default config UpdateCorePoolSize
    // can only adjust DOWNWARD (guard n < max = 20).
    private int _consumeThreadMin = 20;
    private int _consumeThreadMax = 20;
    // adjustThreadPoolNumsThreshold defaults to 100000 (auto-elasticity
    // threshold; the elasticity adjustment is a no-op implementation)
    private long _adjustThreadPoolNumsThreshold = 100000;
    // Declarative core pool size (initial value for UpdateCorePoolSize),
    // default = consumeThreadMin
    private int _corePoolSize = 20;
    // awaitTerminationMillisWhenShutdown: the ceiling in milliseconds that Shutdown
    // spends draining the consume pool; <=0 keeps this port's default unbounded join
    // (worker threads capture `this`, so they must be joined before it is released).
    private long _awaitTerminationMillisWhenShutdown;
    // key -> ProcessQueue.msgAccCnt (backlog count computed by the last pull)
    private readonly Dictionary<string, long> _msgAccCntTable = new(StringComparer.Ordinal);
    // POP consume executor (created at Start when PopMode; released at Shutdown)
    private ConsumeExecutor? _popConsumeExecutor;

    private int _pullBatchSize = 32;
    private int _pullBatchSizeInBytes = 256 * 1024;
    private int _consumeMessageBatchMaxSize = 1;
    private int _pullTimeoutMillis = 30000;
    private int _pullSuspendTimeoutMillis = 15000;
    private int _suspendCurrentQueueTimeMillis = 1000;
    private int _maxReconsumeTimes = -1;
    private int _pullIntervalMillis;
    // The ONE generic error backoff, shared by the pull and the POP loop: the wait
    // before re-issuing a request after a round that failed for any reason other than
    // the broker's own flow control. The three other arms are separate numbers and do
    // not move with it: broker FLOW_CONTROL(215) retries in 20 ms, this client's own
    // cache-threshold gate parks for 50 ms, a paused consumer polls every 1000 ms.
    private int _pullExceptionBackoffMillis = 1000;
    private bool _heartbeatEnabled = true;
    private int _heartbeatIntervalMillis = 30000;

    private List<string> _nameServerAddrs = new();
    private readonly object _lock = new();
    private readonly Dictionary<string, SubscriptionData> _subscriptionData = new(StringComparer.Ordinal);
    private IMessageListener? _messageListener;
    private readonly Dictionary<string, long> _offsetTable = new(StringComparer.Ordinal);
    // ---- Consume progress / buffers / lock state ----
    // _offsetTable is the "pull cursor" (nextBeginOffset); _consumeOffsetTable
    // is the "consumed offset" (the object periodically persisted to the
    // broker); _pending is the pulled-but-not-consumed buffer.
    private readonly Dictionary<string, long> _consumeOffsetTable = new(StringComparer.Ordinal);
    private readonly Dictionary<string, MessageQueue> _mqMap = new(StringComparer.Ordinal);
    private readonly Dictionary<string, Queue<MessageExt>> _pending = new(StringComparer.Ordinal);
    // In-flight batch count (the part CURRENTLY being consumed by the
    // listener): +1 when DispatchLoop takes a batch, -1 when consumption
    // finishes (including the exception requeue path). _pending plus this
    // equals the total registered messages, and CorrectTagsOffsetLocked's
    // gate checks both.
    private readonly Dictionary<string, int> _inFlight = new(StringComparer.Ordinal);
    // The in-flight batches themselves: on the concurrent path messages stay
    // in the queue buffer while a consume task holds them, until removeMessage
    // runs when the result is processed -- cleanExpiredMsg's "head" sweep and
    // the containsMessage criterion both look at them. _inFlight only counts
    // (for the correctTagsOffset gate); this holds the bodies; both grow and
    // shrink together under _lock. Identity is by reference (batch and
    // registration share the same objects).
    private readonly Dictionary<string, List<MessageExt>> _inFlightMsgs = new(StringComparer.Ordinal);
    // Start() timestamp (the value of the 307 response PROP_CONSUMER_START_TIMESTAMP)
    private long _startTimestamp;
    // Orderly consumption: queue key set confirmed locked by broker LOCK_BATCH_MQ
    private readonly HashSet<string> _lockOk = new(StringComparer.Ordinal);
    // Grant timestamp (ms) per locked queue: a grant older than
    // LockMaxLiveTime counts as expired at the pull gate even before the next
    // sweep round replaces the set.
    private readonly Dictionary<string, long> _lockOkAt = new(StringComparer.Ordinal);
    /// <summary>How long a lock grant stays valid (30s), refreshed by the
    /// 20s sweep; an older stamp is treated as unlocked.</summary>
    private const long LockMaxLiveTime = 30000;
    private int _pullThresholdForQueue = 1000;
    // The remaining four flow-control thresholds: the byte gate counts whole
    // MiB (the held body total is divided by 1 MiB with truncating integer
    // arithmetic); the span is max-min of queueOffset over the held set. All
    // three per-queue gates trip on a STRICT <c>&gt;</c> over the threshold as
    // it stands: no floor and no off switch -- the legal range of each knob is
    // enforced once, at start-up (see CheckConfigRanges). The two topic-level
    // ones default to -1 (unset) and are NEVER compared at pull time: each
    // configured budget is divided over this consumer's owned queues at
    // rebalance, and the quotient becomes the matching per-queue knob
    // (see DivideTopicThresholds).
    private int _pullThresholdSizeForQueue = 100;
    private long _consumeConcurrentlyMaxSpan = 2000;
    private int _pullThresholdForTopic = -1;
    private int _pullThresholdSizeForTopic = -1;
    private long _flowControlTriggered;
    private Thread? _dispatchThread;
    private Thread? _persistThread;
    private Thread? _lockThread;
    private Thread? _rebalanceThread;
    // Sweep schedule for suspending listeners (runs cleanExpireMsg at a fixed
    // period); orderly consumption and POP have no such schedule
    private Thread? _cleanExpireThread;
    private readonly Dictionary<string, Thread> _pullThreads = new(StringComparer.Ordinal);
    // Queue key -> timestamp (ms) of the most recent pull/pop ATTEMPT. Pull
    // and POP share this one table: rebalance uses it to decide whether the
    // loop has stalled (PullMaxIdleTime), and 307 reports it as-is.
    private readonly Dictionary<string, long> _lastPullAt = new(StringComparer.Ordinal);
    /// <summary>Longest idle time for stall detection (PULL_MAX_IDLE_TIME,
    /// default 120000ms): exceeding it counts as stalled, using a strict
    /// <c>&gt;</c>.</summary>
    private const long PullMaxIdleTime = 120000;
    // Queue key -> PopProcessQueue (popped-unacked count + whether rebalance
    // has revoked it)
    private readonly Dictionary<string, PopProcessQueue> _popQueues = new(StringComparer.Ordinal);
    // The application's assignment-change callback, fired per topic whose queue set moved.
    private IMessageQueueListener? _messageQueueListener;
    // POP orderly request dedup set (equality =
    // (shardingKeyIndex, pq reference, mq); shardingKeyIndex is always 0 and
    // mq uses the key string)
    private readonly HashSet<(PopProcessQueue, string)> _popOrderlyRequests = new();

    // ---- Real rebalance ----
    // _assigned is the "queue set currently assigned to this instance",
    // computed by DoRebalance() per the allocation strategy; _rebalanceNow
    // triggers an immediate rebalance on NOTIFY_CONSUMER_IDS_CHANGED(40).
    private List<MessageQueue> _assigned = new();
    private readonly ManualResetEventSlim _rebalanceNow = new(false);
    // Revocation marker (isDropped semantics): after rebalance removes a
    // queue from this instance's assignment, _dropped is set and the pull
    // thread discards its batch and exits once the long poll returns (it no
    // longer serves that queue).
    private readonly HashSet<string> _dropped = new(StringComparer.Ordinal);
    // Queue "epoch" (the queue object's identity): +1 on every revocation.
    // DispatchLoop takes the epoch along with the batch and compares before
    // acking -- a mismatch means the queue was revoked/rebuilt during
    // consumption, so the whole batch is voided (isDropped short-circuit).
    private readonly Dictionary<string, long> _queueEpoch = new(StringComparer.Ordinal);
    // Queues whose offset was corrected by the broker and await rebuild (the
    // allowToUpdate=false semantics of updateAndFreezeOffset):
    // while frozen, neither ack nor correctTagsOffset may move the offset
    // until the queue is rebuilt.
    private readonly HashSet<string> _frozenOffsets = new(StringComparer.Ordinal);

    private MQClientInstance? _mqClient;
    private volatile bool _started;
    private volatile bool _stop;

    // Pause flag: the flag driven by Suspend()/Resume().
    // Once set, both pull loops stop sending requests and poll for it to
    // clear every 1000ms (this port's suspend poll interval).
    private volatile bool _paused;
    private readonly ManualResetEventSlim _stopEvent = new(false);
    private long _consumedCount;
    private long _heartbeatCount;
    private long _lastHeartbeatMs;

    public DefaultMQPushConsumer(string consumerGroup = MixAll.DefaultConsumerGroup)
    {
        if (UtilAll.IsBlank(consumerGroup))
        {
            throw new MQClientException("consumerGroup is empty");
        }

        ConsumerGroup = consumerGroup;
    }

    // ---------------- Config setters ----------------
    public void SetNamesrvAddr(string addr)
    {
        _nameServerAddrs = SplitSemicolon(addr);
    }

    public void SetNameServerAddresses(IReadOnlyList<string> addrs)
    {
        _nameServerAddrs = new List<string>(addrs);
    }

    public string InstanceName
    {
        get => _instanceName;
        set => _instanceName = value;
    }

    public string MessageModel
    {
        get => _messageModel;
        set => _messageModel = value;
    }

    /// <summary>
    /// Queue allocation strategy. The setter does not validate; setting null
    /// is rejected by Start()'s checkConfig (error text
    /// "allocateMessageQueueStrategy is null").
    /// </summary>
    public IAllocateMessageQueueStrategy? AllocateMessageQueueStrategy
    {
        get => _allocateMessageQueueStrategy;
        set => _allocateMessageQueueStrategy = value;
    }

    public string ConsumeFromWhere
    {
        get => _consumeFromWhere;
        set => _consumeFromWhere = value;
    }

    /// <summary>Start time used by CONSUME_FROM_TIMESTAMP, as "yyyyMMddHHmmss".
    /// Checked at start() and read when a queue has no stored offset.</summary>
    public string ConsumeTimestamp
    {
        get => _consumeTimestamp;
        set => _consumeTimestamp = value;
    }

    public void SetConsumeTimestamp(string ts) => _consumeTimestamp = ts;

    /// <summary>Convenience method: set Min and Max together.
    /// Now split into SetConsumeThreadMin/Max; this method is kept for
    /// compatibility with existing call sites.</summary>
    public void SetConsumeThreadNums(int n)
    {
        int v = Math.Max(1, n);
        _consumeThreadMin = v;
        _consumeThreadMax = v;
        _corePoolSize = v;
        _popConsumeExecutor?.SetCorePoolSize(v);
    }

    // ---- Consume thread elasticity ----

    public int ConsumeThreadMin => _consumeThreadMin;
    public int ConsumeThreadMax => _consumeThreadMax;
    public long AdjustThreadPoolNumsThreshold => _adjustThreadPoolNumsThreshold;

    public void SetConsumeThreadMin(int n)
    {
        _consumeThreadMin = Math.Max(1, n);
        _corePoolSize = _consumeThreadMin;
        _popConsumeExecutor?.SetCorePoolSize(_corePoolSize);
    }

    public void SetConsumeThreadMax(int n)
    {
        _consumeThreadMax = Math.Max(1, n);
    }

    public void SetAdjustThreadPoolNumsThreshold(long v) => _adjustThreadPoolNumsThreshold = v;

    public long AwaitTerminationMillisWhenShutdown => _awaitTerminationMillisWhenShutdown;

    public void SetAwaitTerminationMillisWhenShutdown(long v) => _awaitTerminationMillisWhenShutdown = v;

    /// <summary>Runtime adjustment of consume concurrency (updateCorePoolSize
    /// → setCorePoolSize).
    /// <para>The guards replicate the classic semantics one by one:
    /// ownsConsumeExecutor &amp;&amp; corePoolSize &gt; 0
    /// &amp;&amp; corePoolSize &lt;= Short.MAX_VALUE(32767)
    /// &amp;&amp; corePoolSize &lt; consumeThreadMax. If any fails it is
    /// SILENTLY ignored (silent return, no exception). The return value only
    /// lets unit tests assert whether it actually took effect.</para></summary>
    public bool UpdateCorePoolSize(int corePoolSize)
    {
        if (corePoolSize <= 0 || corePoolSize > 32767 || corePoolSize >= _consumeThreadMax)
        {
            return false;
        }
        _corePoolSize = corePoolSize;
        _popConsumeExecutor?.SetCorePoolSize(_corePoolSize);
        return true;
    }

    /// <summary>Read the core pool size (this implementation always owns its
    /// executor, so it never returns -1).</summary>
    public int GetCorePoolSize() => _popConsumeExecutor?.GetCorePoolSize() ?? _corePoolSize;

    /// <summary>Sum of all queues' msgAccCnt (cumulative backlog count).</summary>
    public long ComputeAccumulationTotal()
    {
        lock (_lock)
        {
            long total = 0;
            foreach (long v in _msgAccCntTable.Values) total += v;
            return total;
        }
    }

    /// <summary>A single queue's ProcessQueue.msgAccCnt (summed when no key is
    /// passed).</summary>
    public long MsgAccCnt(string? key = null)
    {
        lock (_lock)
        {
            if (string.IsNullOrEmpty(key))
            {
                long total = 0;
                foreach (long v in _msgAccCntTable.Values) total += v;
                return total;
            }
            return _msgAccCntTable.TryGetValue(key!, out long acc) ? acc : 0;
        }
    }

    /// <summary>Update msgAccCnt from the enqueued batch
    /// (= the last message's MAX_OFFSET property - its queueOffset, only when
    /// &gt; 0).</summary>
    public void UpdateMsgAccCnt(string key, IReadOnlyList<MessageExt> msgs)
    {
        lock (_lock)
        {
            UpdateMsgAccCntLocked(key, msgs);
        }
    }

    private void UpdateMsgAccCntLocked(string key, IReadOnlyList<MessageExt> msgs)
    {
        // accTotal = MAX_OFFSET property value - queueOffset, updated only
        // when > 0; take the LAST message of this batch; a missing/non-numeric
        // /non-positive property never updates (strict parsing would throw,
        // but the broker always sends it; this is fault tolerance so dirty
        // data cannot interrupt the pull thread).
        if (msgs.Count == 0) return;
        MessageExt last = msgs[msgs.Count - 1];
        string? maxOffset = last.GetProperty(MessageConst.PropertyMaxOffset);
        if (string.IsNullOrEmpty(maxOffset)) return;
        if (!long.TryParse(maxOffset, NumberStyles.Integer, CultureInfo.InvariantCulture, out long parsed))
        {
            return;
        }
        long accTotal = parsed - last.QueueOffset;
        if (accTotal > 0) _msgAccCntTable[key] = accTotal;
    }

    /// <summary>adjustThreadPool (scheduled once per minute).
    /// <para>⚠ In 5.5.1 classic semantics this is a NO-OP and this port
    /// copies the no-op: the consumeMessageService.incCorePoolSize()/
    /// decCorePoolSize() it calls have EMPTY bodies at
    /// AbstractConsumeMessageService:70-75. The threshold comparison is kept
    /// here only to make msgAccCnt / the threshold config observable; DO NOT
    /// "fix" it -- the effective path is the explicit
    /// UpdateCorePoolSize().</para></summary>
    public void AdjustThreadPool()
    {
        long accTotal = ComputeAccumulationTotal();
        long incThreshold = (long)(_adjustThreadPoolNumsThreshold * 1.0);
        long decThreshold = (long)(_adjustThreadPoolNumsThreshold * 0.8);
        if (accTotal >= incThreshold)
        {
            ClientLog.Debug($"adjustThreadPool: acc={accTotal} >= incThreshold={incThreshold} (inc is a no-op upstream)");
        }
        if (accTotal < decThreshold)
        {
            ClientLog.Debug($"adjustThreadPool: acc={accTotal} < decThreshold={decThreshold} (dec is a no-op upstream)");
        }
    }

    /// <summary>Observability: the current POP consume executor's live worker
    /// count / queued task count (0 when no executor exists).</summary>
    public int ConsumeExecutorWorkers() => _popConsumeExecutor?.WorkerCount() ?? 0;
    public int ConsumeExecutorQueued() => _popConsumeExecutor?.QueuedCount() ?? 0;
    // Unit tests only: inject an executor directly so testing thread
    // elasticity does not require a cluster (the test project has no
    // InternalsVisibleTo relationship with this assembly, hence public; not
    // for business code)
    public ConsumeExecutor? PopConsumeExecutorForTest
    {
        get => _popConsumeExecutor;
        set => _popConsumeExecutor = value;
    }

    public void SetMessageListener(IMessageListener listener)
    {
        _messageListener = listener;
    }

    /// <summary>Install the assignment-change callback. It fires once per topic whose queue SET
    /// moved at rebalance -- never for an unchanged round -- with that topic's full route set and
    /// this instance's share of it.</summary>
    public void SetMessageQueueListener(IMessageQueueListener? listener)
    {
        lock (_lock)
        {
            _messageQueueListener = listener;
        }
    }

    /// <summary>The currently installed queue listener (null when the application never asked).</summary>
    public IMessageQueueListener? MessageQueueListener
    {
        get
        {
            lock (_lock)
            {
                return _messageQueueListener;
            }
        }
    }

    public int PullBatchSize
    {
        get => _pullBatchSize;
        set => _pullBatchSize = value;
    }

    public int PullBatchSizeInBytes
    {
        get => _pullBatchSizeInBytes;
        set => _pullBatchSizeInBytes = value;
    }

    public int ConsumeMessageBatchMaxSize
    {
        get => _consumeMessageBatchMaxSize;
        set => _consumeMessageBatchMaxSize = Math.Max(1, value);
    }

    public int PullTimeoutMillis
    {
        get => _pullTimeoutMillis;
        set => _pullTimeoutMillis = value;
    }

    public int PullSuspendTimeoutMillis
    {
        get => _pullSuspendTimeoutMillis;
        set => _pullSuspendTimeoutMillis = value;
    }

    public int SuspendCurrentQueueTimeMillis
    {
        get => _suspendCurrentQueueTimeMillis;
        set => _suspendCurrentQueueTimeMillis = value;
    }

    public int MaxReconsumeTimes
    {
        get => _maxReconsumeTimes;
        set => _maxReconsumeTimes = value;
    }

    public int PullIntervalMillis
    {
        get => _pullIntervalMillis;
        set => _pullIntervalMillis = value;
    }

    /// <summary>The ONE generic error backoff for both consume loops: the wait before
    /// this consumer re-issues a pull (or pop) request after a round that failed for any
    /// reason other than the broker's own flow control -- a non-success answer, any
    /// exception that is not FLOW_CONTROL(215), or a POP status the re-schedule cannot
    /// act on (<c>POLLING_FULL</c> or an unknown one). Default 1000 ms; a negative value
    /// is clamped to 0 ("go again at once").
    /// <para>Each loop reads this once when it is set up and keeps that value for its
    /// life, so changing the knob after <see cref="Start"/> does not alter a loop that is
    /// already running -- the same way the POP loop's invisible time is taken.</para>
    /// <para>The three other waits are separate arms and do not move with it: a broker
    /// FLOW_CONTROL(215) retries after <see cref="BrokerFlowControlBackoffMillis"/>
    /// (20 ms), this client's own cache-threshold gate parks for
    /// <see cref="CacheFlowControlBackoffMillis"/> (50 ms) and a paused consumer keeps
    /// polling every 1000 ms.</para></summary>
    public int PullExceptionBackoffMillis
    {
        get => _pullExceptionBackoffMillis;
        set => _pullExceptionBackoffMillis = Math.Max(0, value);
    }

    public bool HeartbeatEnabled
    {
        get => _heartbeatEnabled;
        set => _heartbeatEnabled = value;
    }

    // Per-queue "pulled but not consumed" threshold; pull for that queue
    // pauses once exceeded (default 1000). The gate compares with a strict >,
    // so exactly this many cached messages still pull; the legal range is
    // checked once at Start().
    // ⚠ While a topic-level budget is enabled this value is NOT the one the
    // user set: rebalance overwrites it with the divided quotient (see
    // DivideTopicThresholds), so reading it back gives the divided number.
    public int PullThresholdForQueue
    {
        get => _pullThresholdForQueue;
        set => _pullThresholdForQueue = value;
    }

    /// <summary><c>pullThresholdSizeForQueue</c> (default 100): this queue's
    /// "pulled but not consumed" byte threshold in <b>MiB</b>, measured over the
    /// message BODIES and divided with truncating integer arithmetic -- a
    /// partially filled MiB is not charged, and the gate trips on a strict
    /// &gt;. While <see cref="PullThresholdSizeForTopic"/> is enabled
    /// rebalance overwrites this with the divided quotient.</summary>
    public int PullThresholdSizeForQueue
    {
        get => _pullThresholdSizeForQueue;
        set => _pullThresholdSizeForQueue = value;
    }

    /// <summary><c>consumeMessageMaxSpan</c> (default 2000): span threshold for
    /// the queueOffset of this queue's "pulled but not consumed" messages;
    /// trips only when STRICTLY greater (prevents one stuck head message from
    /// piling up behind it indefinitely). An orderly listener is never span-gated,
    /// so this comparison only runs on the concurrent path.</summary>
    public long ConsumeConcurrentlyMaxSpan
    {
        get => _consumeConcurrentlyMaxSpan;
        set => _consumeConcurrentlyMaxSpan = value;
    }

    /// <summary><c>pullThresholdForTopic</c> (default -1, i.e. no topic budget):
    /// the "pulled but not consumed" message-count budget for this instance's
    /// share of the topic. It is enforced by DIVISION, not by adding the queues
    /// up: every rebalance that changes the assignment divides it by the number of
    /// queues this consumer owns (all topics together) and writes the quotient
    /// into <see cref="PullThresholdForQueue"/> (see
    /// <see cref="DivideTopicThresholds"/>). The pull-time gate then only ever
    /// looks at one queue's own buffer against that per-queue number.</summary>
    public int PullThresholdForTopic
    {
        get => _pullThresholdForTopic;
        set => _pullThresholdForTopic = value;
    }

    /// <summary><c>pullThresholdSizeForTopic</c> (default -1, i.e. no topic
    /// budget): the byte budget for the same topic, in <b>MiB</b>. Enforced the
    /// same way as <see cref="PullThresholdForTopic"/> -- divided into
    /// <see cref="PullThresholdSizeForQueue"/> at rebalance, never summed over
    /// the queues at pull time.</summary>
    public int PullThresholdSizeForTopic
    {
        get => _pullThresholdSizeForTopic;
        set => _pullThresholdSizeForTopic = value;
    }

    // Flow-control trigger count (used to verify the flow-control capability)
    public long FlowControlTriggered => Interlocked.Read(ref _flowControlTriggered);

    public int HeartbeatIntervalMillis
    {
        get => _heartbeatIntervalMillis;
        set => _heartbeatIntervalMillis = value;
    }

    public string ClientId => _clientId;

    public bool IsStarted => _started;

    // Total messages consumed successfully (for tests/monitoring)
    public long ConsumedCount => _consumedCount;

    // Broker heartbeat success count (used to verify the heartbeat capability)
    public long HeartbeatCount => _heartbeatCount;

    // ---------------- Subscription ----------------
    /// <summary>
    /// Subscribe to a topic. It may STILL be called after start(): the
    /// subscription table is live (heartbeat and rebalance re-read it every
    /// round), and a heartbeat is pushed immediately after the put (see
    /// NotifySubscriptionChanged).
    /// </summary>
    public void Subscribe(string topic, string subExpression = "*")
    {
        string realTopic = NamespaceUtil.WrapNamespace(_namespace, topic);
        SubscriptionData sub = FilterAPI.BuildSubscriptionData(realTopic, subExpression);
        lock (_lock)
        {
            _subscriptionData[realTopic] = sub;
        }

        NotifySubscriptionChanged(realTopic);
    }

    /// <summary>Subscribe to a topic by selector.</summary>
    public void Subscribe(string topic, MessageSelector selector)
    {
        string realTopic = NamespaceUtil.WrapNamespace(_namespace, topic);
        var sub = new SubscriptionData(realTopic, selector.Expression)
        {
            ExpressionType = selector.Type,
        };
        if (selector.Type == ExpressionType.TAG)
        {
            SubscriptionData built = FilterAPI.BuildSubscriptionData(realTopic, selector.Expression);
            sub.TagsSet = built.TagsSet;
        }

        lock (_lock)
        {
            _subscriptionData[realTopic] = sub;
        }

        NotifySubscriptionChanged(realTopic);
    }

    /// <summary>
    /// Unsubscribe: only removes the table entry and sends no heartbeat (the
    /// next heartbeat round naturally carries the new subscription set), so
    /// NotifySubscriptionChanged is NOT called here.
    /// </summary>
    public void Unsubscribe(string topic)
    {
        lock (_lock)
        {
            _subscriptionData.Remove(topic);
        }
    }

    /// <summary>
    /// The immediate action after a subscription table insert/update: right
    /// after writing the subscription table, synchronously push one heartbeat
    /// round to all brokers.
    /// Why it must be immediate: the broker's consumer manager only records
    /// topic->group into its own registration table when it receives a heartbeat
    /// (the heartbeat handler's consumer-registration step), and it is exactly this
    /// table that QUERY_TOPIC_CONSUME_BY_WHO(300) reads; one round later means
    /// a default 30s gap. At the same time the new topic is registered as "in
    /// use" so the background route refresh task covers it (the refresh task
    /// collects the topics to refresh by walking the consumer's CURRENT
    /// subscription table). Before startup there is no client instance, so
    /// only the subscription table is written.
    /// </summary>
    private void NotifySubscriptionChanged(string topic)
    {
        if (!_started || _mqClient is null)
        {
            return;
        }

        try
        {
            _mqClient.RegisterTopicInUse(topic);
            SendHeartbeatToAllBroker();
        }
        catch (Exception e)
        {
            ClientLog.Debug("immediate heartbeat after subscribe(" + topic + ") failed: " + e.Message);
        }
    }

    public List<string> SubscribedTopics()
    {
        lock (_lock)
        {
            var outList = new List<string>(_subscriptionData.Count);
            foreach (var kv in _subscriptionData)
            {
                outList.Add(kv.Key);
            }

            return outList;
        }
    }

    /// <summary>The numeric-range section of checkConfig.
    /// <para>The checked ORDER, ranges, and messages are contractual (the
    /// classic implementation appends <c>FAQUrl.suggestTodo(CLIENT_PARAMETER_CHECK_URL)</c>
    /// to each; this repo follows the convention without the suffix).
    /// Comparisons are always <c>&lt; lo || &gt; hi</c>; <c>PopBatchNums</c>
    /// keeps the literal <c>&lt;= 0</c> form by convention. The error messages
    /// are asserted verbatim by test cases; confirm the ledger before
    /// changing them.</para>
    /// <para>Why gate at <see cref="Start"/>: these values directly determine
    /// buffer levels and thread-pool size, and the pull-time gates read the
    /// number as it stands -- no floor, no off switch. Left unchecked, a 0
    /// would make the FIRST cached message of a queue trip its count gate and
    /// that queue would stop being pulled PERMANENTLY (the consumer silently
    /// receives nothing), while an <c>int.MaxValue</c> would quietly disable the
    /// check for good. Waiting for a broker error is several beats too late, and
    /// the error only lands in the log.</para>
    /// <para>The -1 of <c>PullThresholdForTopic</c> /
    /// <c>PullThresholdSizeForTopic</c> is the "no topic budget configured"
    /// sentinel: it is out of range but not an error, and such a budget is
    /// simply not divided at rebalance, so its per-queue knob keeps the user's
    /// value (see <see cref="DivideTopicThresholds"/>). The other fields have no
    /// such exemption (writing -1 there is a startup error, not "unlimited"),
    /// and the per-queue gates never see the sentinel.</para>
    /// <para>public is for offline testability only: positive cases (boundary
    /// values must be allowed) cannot run after <see cref="Start"/>, and the
    /// test project has no InternalsVisibleTo relationship with this assembly.
    /// Business code should just call <c>Start()</c>.</para>
    /// <para>Note: the setters of <c>SetConsumeThreadMin/Max</c> and
    /// <c>ConsumeMessageBatchMaxSize</c> carry a <c>Math.Max(1, n)</c> floor
    /// (not a bare assignment), so the LOWER-bound violations of these three
    /// are unreachable via the public API; the upper bound and the relative
    /// check (min &gt; max) remain reachable, and the gate itself keeps the
    /// full set.</para>
    /// </summary>
    public void CheckConfigRanges()
    {
        // consumeThreadMin
        if (ConsumeThreadMin < 1 || ConsumeThreadMin > 1000)
        {
            throw new MQClientException("consumeThreadMin Out of range [1, 1000]");
        }
        // consumeThreadMax
        if (ConsumeThreadMax < 1 || ConsumeThreadMax > 1000)
        {
            throw new MQClientException("consumeThreadMax Out of range [1, 1000]");
        }
        // consumeThreadMin can't be larger than consumeThreadMax
        if (ConsumeThreadMin > ConsumeThreadMax)
        {
            throw new MQClientException("consumeThreadMin (" + ConsumeThreadMin
                                        + ") is larger than consumeThreadMax ("
                                        + ConsumeThreadMax + ")");
        }
        // consumeConcurrentlyMaxSpan
        if (ConsumeConcurrentlyMaxSpan < 1 || ConsumeConcurrentlyMaxSpan > 65535)
        {
            throw new MQClientException("consumeConcurrentlyMaxSpan Out of range [1, 65535]");
        }
        // pullThresholdForQueue
        if (PullThresholdForQueue < 1 || PullThresholdForQueue > 65535)
        {
            throw new MQClientException("pullThresholdForQueue Out of range [1, 65535]");
        }
        // pullThresholdForTopic
        if (PullThresholdForTopic != -1)
        {
            if (PullThresholdForTopic < 1 || PullThresholdForTopic > 6553500)
            {
                throw new MQClientException("pullThresholdForTopic Out of range [1, 6553500]");
            }
        }
        // pullThresholdSizeForQueue
        if (PullThresholdSizeForQueue < 1 || PullThresholdSizeForQueue > 1024)
        {
            throw new MQClientException("pullThresholdSizeForQueue Out of range [1, 1024]");
        }
        // pullThresholdSizeForTopic
        if (PullThresholdSizeForTopic != -1)
        {
            if (PullThresholdSizeForTopic < 1 || PullThresholdSizeForTopic > 102400)
            {
                throw new MQClientException("pullThresholdSizeForTopic Out of range [1, 102400]");
            }
        }
        // pullInterval's lower bound is 0 (no interval); do not copy the 1
        // from the other gates
        if (PullIntervalMillis < 0 || PullIntervalMillis > 65535)
        {
            throw new MQClientException("pullInterval Out of range [0, 65535]");
        }
        // consumeMessageBatchMaxSize
        if (ConsumeMessageBatchMaxSize < 1 || ConsumeMessageBatchMaxSize > 1024)
        {
            throw new MQClientException("consumeMessageBatchMaxSize Out of range [1, 1024]");
        }
        // pullBatchSize
        if (PullBatchSize < 1 || PullBatchSize > 1024)
        {
            throw new MQClientException("pullBatchSize Out of range [1, 1024]");
        }
        // popInvisibleTime: the range uses the same pair of constants as the
        // fallback in the POP loop
        if (PopInvisibleTime < MinPopInvisibleTime || PopInvisibleTime > MaxPopInvisibleTime)
        {
            throw new MQClientException("popInvisibleTime Out of range [" + MinPopInvisibleTime
                                        + ", " + MaxPopInvisibleTime + "]");
        }
        // popBatchNums (lower bound judged by <= 0, not < 1)
        if (PopBatchNums <= 0 || PopBatchNums > 32)
        {
            throw new MQClientException("popBatchNums Out of range [1, 32]");
        }
        // consumeTimestamp is parsed unconditionally, not only when
        // CONSUME_FROM_TIMESTAMP is selected: a malformed string must fail at
        // start() rather than surface as a pull error minutes later.
        DefaultLitePullConsumer.ParseConsumeTimestamp(ConsumeTimestamp);
    }

    // ---------------- Suspend / resume ----------------

    /// <summary>Suspend consumption: set the pause flag and log one info line.
    /// The assignment set, queue buffers, and committed offsets ALL stay in
    /// place; only the two pull loops stop sending requests and look back at
    /// the flag every 1000ms (<c>PULL_TIME_DELAY_MILLS_WHEN_SUSPEND</c>).
    /// Idempotent, never throws.</summary>
    public void Suspend()
    {
        _paused = true;
        ClientLog.Info("suspend this consumer, " + ConsumerGroup);
    }

    /// <summary>Resume consumption: clear the flag and rebalance immediately so
    /// assignments missed during the suspend window are caught up at once
    /// instead of waiting for the 20s period. No synchronous
    /// <c>doRebalance()</c> here; reuse the flag-only
    /// <see cref="WakeRebalanceLoop"/> (a synchronous DoRebalance would stall
    /// the calling thread's RPC on the reader thread waiting for itself; see
    /// its comments).</summary>
    public void Resume()
    {
        _paused = false;
        WakeRebalanceLoop();
        ClientLog.Info("resume this consumer, " + ConsumerGroup);
    }

    /// <summary>Whether the consumer is currently suspended.</summary>
    public bool IsPaused => _paused;

    // ---------------- Lifecycle ----------------
    public void Start()
    {
        {
            if (_started)
            {
                return;
            }

            // Start() wraps the consumer group with the namespace (ns%group);
            // every broker-facing group name afterwards (heartbeat / rebalance
            // / offsets / locks / send-back) uses the wrapped value.
            if (_namespace.Length != 0)
            {
                ConsumerGroup = NamespaceUtil.WrapNamespace(_namespace, ConsumerGroup);
            }

            // checkConfig runs first: Validators.checkGroup
            // (blank / 120 length / character table), then block
            // DEFAULT_CONSUMER -- sharing the default group makes the
            // broker-side subscription verdict mix the two groups together,
            // scrambling send-back and rebalance. checkConfig is placed
            // first, ahead of the address/subscription checks.
            Validators.CheckGroup(ConsumerGroup);
            if (ConsumerGroup == MixAll.DefaultConsumerGroup)
            {
                throw new MQClientException("consumerGroup can not equal " + MixAll.DefaultConsumerGroup
                                            + ", please specify another one.");
            }

            // Static addresses and dynamic addressing (ROCKETMQ_NAMESRV_DOMAIN):
            // exactly one of the two must be available
            if (_nameServerAddrs.Count == 0 && !DefaultTopAddressing.IsConfigured())
            {
                throw new MQClientException("name server address is not set");
            }

            // checkConfig: a null strategy refuses startup directly.
            if (_allocateMessageQueueStrategy is null)
            {
                throw new MQClientException("allocateMessageQueueStrategy is null");
            }

            if (_subscriptionData.Count == 0)
            {
                throw new MQClientException("subscription is not set, call subscribe() first");
            }

            if (_messageListener is null)
            {
                throw new MQClientException("message listener is not set");
            }

            // The numeric-range section of checkConfig.
            // Placed after all null checks and before building
            // MQClientInstance: a bad config must fail before instantiation,
            // otherwise background threads are started / clientId registered
            // and only then does the throw leave a half-started state.
            // CheckConfigRanges also parses ConsumeTimestamp, so a malformed
            // start time fails here rather than during the first offset lookup.
            CheckConfigRanges();

            // **Only CLUSTERING** swaps the default instanceName for
            // <pid>#<nanoTime>; BROADCASTING keeps "DEFAULT", so broadcasting
            // consumers of the same group on the same machine share one
            // clientId -- this is a deliberately retained sharing semantic
            // and must NOT be "fixed".
            if (_messageModel == RocketMQ.Remoting.Protocol.MessageModel.Clustering)
            {
                _instanceName = ClientIds.ChangeInstanceNameToPID(_instanceName);
            }
            if (_clientId.Length == 0)
            {
                _clientId = ClientIds.Build(_instanceName, _unitName, _enableStreamRequestType);
            }

            // Clustering mode auto-subscribes the retry topic
            // (copySubscription → getRetryTopic): the broker writes sent-back
            // messages to %RETRY%group, and the client cannot receive them
            // without subscribing
            if (_messageModel != RocketMQ.Remoting.Protocol.MessageModel.Broadcasting)
            {
                string retryTopic = MixAll.GetRetryTopic(ConsumerGroup);
                if (!_subscriptionData.ContainsKey(retryTopic))
                {
                    _subscriptionData[retryTopic] =
                        FilterAPI.BuildSubscriptionData(retryTopic, "*");
                }
            }

            // Request hooks (ACL signature / stream's ReqT): bound BEFORE
            // Start() -- rpcHook is passed in with the remoting instance
            // construction, so the instance's very first frame already
            // carries it. The order is restored by RequestHooks.Compose
            // (Namespace → stream → ACL ⇒ ns/nsd written first, ReqT lands in
            // the signature content).
            IRpcHook? requestHook = RequestHooks.Compose(_enableStreamRequestType, _rpcHook, () => _namespaceV2);
            _mqClient = MQClientManager.Instance.GetOrCreateMQClientInstance(_clientId,
                () => new MQClientInstance(_clientId, _nameServerAddrs,
                    /*connectTimeoutMillis=*/3000,
                    /*invokeTimeoutMillis=*/_pullTimeoutMillis,
                    tlsEnable: TlsEnable, tlsOptions: TlsOptions, socksProxy: _socksProxyConfig,
                    unitName: _unitName,
                    pollNameServerIntervalMillis: _pollNameServerIntervalMillis));
            if (requestHook is not null && !_mqClient.RegisterRpcHook(requestHook))
            {
                ClientLog.Warn("consumer rpc hook ignored: MQClientInstance already has one (clientId="
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
                // If it cannot start, hand the reference back: otherwise the
                // count leaks and the shared instance never reaches zero for
                // finalization
                MQClientManager.Instance.UnregisterMQClient(_clientId, _mqClient);
                _mqClient = null;
                throw;
            }
            // Dynamic name server: the instance may already have obtained
            // addresses from the address server at startup; backfill them into
            // this consumer (naturally synced via the shared client config)
            if (_nameServerAddrs.Count == 0 && _mqClient.NameServerAddrs.Count > 0)
            {
                _nameServerAddrs = new List<string>(_mqClient.NameServerAddrs);
            }

            // The POP consume executor must be built BEFORE rebalance (which
            // immediately starts per-queue POP loops), otherwise the loops
            // pop messages with nowhere to deliver them (the executor must
            // exist before the POP loops start).
            if (PopMode)
            {
                _popConsumeExecutor = new ConsumeExecutor(
                    Math.Max(1, _corePoolSize), Math.Max(1, _consumeThreadMax),
                    /*keepAliveSeconds=*/60.0, "rmq-popconsume-" + ConsumerGroup);
            }

            _stop = false;
            _started = true;
            _startTimestamp = UtilAll.CurrentTimeMillis();

            // Register subscribed topics as "in use" for MQClientInstance's
            // periodic route refresh (corresponds to task ⑥)
            foreach (string t in SubscribedTopics())
            {
                _mqClient.RegisterTopicInUse(t);
            }

            // The broker-initiated notification 40 (membership change →
            // immediate recompute) does NOT register a processor here: that
            // request code has only one processor, and each consumer
            // registering it would overwrite the others. Instead, register a
            // "wakeup" callback with the instance, which fans out on receipt.
            _mqClient.RegisterRebalanceWakeup(ConsumerGroup, WakeRebalanceLoop);

            // GET_CONSUMER_RUNNING_INFO(307) handling:
            // admin / broker queries this consumer's runtime info, replying
            // with a ConsumerRunningInfo JSON body. Reverse-request processors
            // always go through instance-level multicast registration (under a
            // shared instance each consumer registers its own and they do not
            // clobber each other).
            _mqClient.RegisterFacadeProcessor(RequestCode.GetConsumerRunningInfo,
                OnGetConsumerRunningInfo);

            // RESET_CONSUMER_CLIENT_OFFSET(220): sent by the broker with
            // invokeOneway, no reply needed. The reset triggers rebalance
            // (invokeSync), which cannot run synchronously on the reader
            // thread -- hand it to a background thread.
            _mqClient.RegisterFacadeProcessor(RequestCode.ResetConsumerClientOffset,
                OnResetConsumerOffset);

            // GET_CONSUMER_STATUS_FROM_CLIENT(221): admin queries the consumed
            // offset table.
            _mqClient.RegisterFacadeProcessor(RequestCode.GetConsumerStatusFromClient,
                OnGetConsumerStatus);

            // CONSUME_MESSAGE_DIRECTLY(309): the broker pushes one message
            // down for a real local consume.
            _mqClient.RegisterFacadeProcessor(RequestCode.ConsumeMessageDirectly,
                OnConsumeMessageDirectly);
        }

        // Startup order:
        // pull routes once → send a heartbeat (so the broker knows this
        // consumer first) → rebalance immediately → start consume threads.
        // The heartbeat must precede rebalance: rebalance queries the broker
        // for the consumer list.
        try
        {
            RefreshRoutes();
        }
        catch (Exception e)
        {
            ClientLog.Debug("initial refresh routes failed: " + e.Message);
        }

        // After routes arrive and before the heartbeat, send non-TAG
        // subscriptions to the broker for validation (CHECK_CLIENT_CONFIG 46).
        // A wrong SQL92 makes the broker's filter layer SILENTLY PASS ALL
        // messages, and only this request can turn it into a startup error;
        // on failure roll back in place before rethrowing.
        try
        {
            _mqClient.CheckSubscriptionsInBroker(ConsumerGroup, _subscriptionData.Values);
        }
        catch (Exception)
        {
            Shutdown();
            throw;
        }

        try
        {
            SendHeartbeatToAllBroker();
        }
        catch (Exception e)
        {
            ClientLog.Debug("initial heartbeat failed: " + e.Message);
        }

        // The first assignment round must complete synchronously: otherwise
        // pull threads spin on an empty assignment set until the first
        // rebalance round takes effect
        try
        {
            DoRebalance();
        }
        catch (Exception e)
        {
            ClientLog.Debug("initial rebalance failed: " + e.Message);
        }

        _dispatchThread = MakeThread("ConsumeMessageThread", DispatchLoop);
        _dispatchThread.Start();
        _persistThread = MakeThread("MQClientFactoryScheduledThread", OffsetPersistLoop);
        _persistThread.Start();
        _lockThread = MakeThread("ConsumeMessageOrderlyServiceThread", LockLoop);
        _lockThread.Start();
        _rebalanceThread = MakeThread("RebalanceThread", RebalanceLoop);
        _rebalanceThread.Start();

        // The sweep schedule for suspending listeners exists only on the
        // CLASSIC CONCURRENT path: it is built in the concurrent consume
        // service's constructor/start, orderly consumption's cleanExpiredMsg
        // returns directly anyway, and POP's
        // the concurrent POP listener has no corresponding schedule.
        if (!IsOrderly() && !PopMode)
        {
            _cleanExpireThread = MakeThread("CleanExpireMsgScheduledThread_" + ConsumerGroup,
                                            CleanExpiredMsgLoop);
            _cleanExpireThread.Start();
        }

        string topics = string.Empty;
        foreach (string t in SubscribedTopics())
        {
            if (topics.Length > 0) topics += ",";
            topics += t;
        }

        ClientLog.Info("DefaultMQPushConsumer[" + ConsumerGroup + "] started, clientId=" + _clientId
            + ", topics=" + topics + ", pullTimeout=" + _pullTimeoutMillis.ToString(CultureInfo.InvariantCulture)
            + "ms, pullSuspend=" + _pullSuspendTimeoutMillis.ToString(CultureInfo.InvariantCulture) + "ms");

        // The trace dispatcher starts last: it spins up its own internal
        // producer (each locking separately), which is safer placed after the
        // other threads.
        StartTraceDispatcher();
    }

    public void Shutdown()
    {
        bool wasStarted = _started;
        _started = false;
        if (!wasStarted)
        {
            return;
        }

        _stop = true;
        _stopEvent.Set();
        // POP: mark every queue dropped so in-flight batches are no longer
        // acked (the broker revives and redelivers them)
        if (PopMode)
        {
            foreach (PopProcessQueue pq in _popQueues.Values)
            {
                pq.SetDropped(true);
            }

            _popQueues.Clear();
        }

        // POP consume executor wraps up (shutdownGracefully semantics: no new
        // tasks accepted, finish the batches in hand). Dispose calls
        // Shutdown(wait=true): worker threads captured `this`, so they must be
        // joined before it can be released. awaitTerminationMillisWhenShutdown
        // bounds that join when > 0.
        _popConsumeExecutor?.Shutdown(true, (int)Math.Clamp(_awaitTerminationMillisWhenShutdown, 0, int.MaxValue));
        _popConsumeExecutor = null;

        // Persist consumed offsets once before exiting (shutdown →
        // persistAllConsumerOffset)
        try
        {
            PersistOffsetsOnce();
        }
        catch (Exception e)
        {
            ClientLog.Debug("persist offsets on shutdown failed: " + e.Message);
        }

        // Unlock queues when orderly consumption shuts down (shutdown →
        // unlockAll). The POP orderly service uses the same unlockAll, which
        // only reads processQueueTable -- an empty table in POP mode, so no
        // frame is sent.
        if (IsOrderly() && !PopMode
            && _messageModel != RocketMQ.Remoting.Protocol.MessageModel.Broadcasting)
        {
            try
            {
                List<MessageQueue> mqs = AssignedQueues();
                if (mqs.Count > 0 && _mqClient is not null)
                {
                    _mqClient.UnlockBatchMq(ConsumerGroup, _clientId, mqs);
                }
            }
            catch (Exception e)
            {
                ClientLog.Debug("unlock on shutdown failed: " + e.Message);
            }
        }

        foreach (Thread t in _pullThreads.Values)
        {
            if (t.IsAlive) t.Join();
        }

        _pullThreads.Clear();
        // Stale stamps left after shutdown would make the next Start()'s
        // self-healing check read the previous instance's timestamps
        lock (_lock)
        {
            _lastPullAt.Clear();
        }

        JoinIfAlive(_dispatchThread);
        JoinIfAlive(_persistThread);
        JoinIfAlive(_lockThread);
        JoinIfAlive(_rebalanceThread);

        // Graceful unregister (corresponds to task ④): remove this clientId
        // from all brokers before closing connections, without waiting for
        // heartbeat timeout (default ~120s) -- otherwise consumer-change
        // notifications may still be sent to an already-exited instance
        // during that window.
        if (_mqClient is not null)
        {
            // All consume threads are joined; remove the 40 "wakeup" callback
            // first (the callback captured this consumer's state)
            _mqClient.UnregisterRebalanceWakeup(ConsumerGroup);
            try
            {
                _mqClient.UnregisterClientAllBrokers(_clientId, "", ConsumerGroup);
            }
            catch (Exception e)
            {
                ClientLog.Debug("unregister client on shutdown failed: " + e.Message);
            }

            // These four processors are registered by this consumer on the
            // SHARED instance: remove them first so that if the instance is
            // reused by another facade, broker reverse requests no longer hit
            // an exited consumer.
            _mqClient.UnregisterFacadeProcessor(RequestCode.GetConsumerRunningInfo,
                OnGetConsumerRunningInfo);
            _mqClient.UnregisterFacadeProcessor(RequestCode.ResetConsumerClientOffset,
                OnResetConsumerOffset);
            _mqClient.UnregisterFacadeProcessor(RequestCode.GetConsumerStatusFromClient,
                OnGetConsumerStatus);
            _mqClient.UnregisterFacadeProcessor(RequestCode.ConsumeMessageDirectly,
                OnConsumeMessageDirectly);

            MQClientManager.Instance.UnregisterMQClient(_clientId, _mqClient);
            _mqClient = null;
        }

        // Shutdown order: stop this consumer first, then flush and stop the
        // trace dispatcher (the dispatcher uses its OWN internal producer,
        // independent of this instance, so it can still finish sending after
        // this consumer is closed)
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

    // ---------------- Consume hooks (executeHookBefore / executeHookAfter) ----------------
    // Hook exceptions are always swallowed and logged as warn: trace errors
    // must never affect normal consumption.

    private void ExecuteConsumeHookBefore(ConsumeMessageContext context)
    {
        foreach (IConsumeMessageHook hook in _consumeMessageHooks)
        {
            try
            {
                hook.ConsumeMessageBefore(context);
            }
            catch (Exception e)
            {
                ClientLog.Warn("consumeMessageHook executeHookBefore exception: " + e.Message);
            }
        }
    }

    private void ExecuteConsumeHookAfter(ConsumeMessageContext context)
    {
        foreach (IConsumeMessageHook hook in _consumeMessageHooks)
        {
            try
            {
                hook.ConsumeMessageAfter(context);
            }
            catch (Exception e)
            {
                ClientLog.Warn("consumeMessageHook executeHookAfter exception: " + e.Message);
            }
        }
    }

    /// <summary>Build the consume hook context. Initial values:
    /// success=false, empty Props, accessChannel=LOCAL.</summary>
    private ConsumeMessageContext BuildConsumeHookContext(List<MessageExt> msgs, MessageQueue mq)
        => new(ConsumerGroup, msgs, mq)
        {
            Success = false,
            Props = new PropertyMap(),
            AccessChannel = AccessChannel.Local,
        };

    /// <summary>returnType verdict (decides the trace SubAfter contextCode).
    /// Order: status == null → EXCEPTION/RETURNNULL; RT ≥ consumeTimeout
    /// (minutes) → TIME_OUT; failed → FAILED; succeeded → SUCCESS. Orderly
    /// consumption treats "suspend" as FAILED and success as SUCCESS, passed
    /// in by the caller via the two flags failed/succeeded.</summary>
    private string ConsumeReturnTypeName(bool hasStatus, bool hasException, double consumeRtMs,
        bool failed, bool succeeded)
    {
        if (!hasStatus)
        {
            return hasException ? "EXCEPTION" : "RETURNNULL";
        }

        if (consumeRtMs >= ConsumeTimeout * 60 * 1000.0)
        {
            return "TIME_OUT";
        }

        if (failed)
        {
            return "FAILED";
        }

        return succeeded ? "SUCCESS" : "SUCCESS";
    }

    /// <summary>Write returnType / status / success back into the context and
    /// fire the after hook.
    /// ⚠ The returnType verdict must happen BEFORE "status is normalized to
    /// RECONSUME_LATER", otherwise a null return from the listener would be
    /// misrecorded as FAILED instead of RETURNNULL.</summary>
    private void FinishConsumeHook(ConsumeMessageContext? hookCtx, bool hasStatus, bool hasException,
        long beginMs, string statusText, bool failed, bool succeeded)
    {
        if (hookCtx is null)
        {
            return;
        }

        double rt = UtilAll.CurrentTimeMillis() - beginMs;
        hookCtx.Props!["ConsumeContextType"] = ConsumeReturnTypeName(hasStatus, hasException, rt,
            failed, succeeded);
        hookCtx.Status = statusText;
        hookCtx.Success = succeeded;
        ExecuteConsumeHookAfter(hookCtx);
    }

    /// <summary>When enableTrace=true at Start(), build the dispatcher
    /// (Type=Consume), register a ConsumeMessageTraceHook, then start it.
    /// Any exception is only logged -- trace failures must not affect normal
    /// consumption.</summary>
    private void StartTraceDispatcher()
    {
        if (_enableTrace)
        {
            try
            {
                var dispatcher = new AsyncTraceDispatcher(ConsumerGroup,
                    TraceDispatcherType.Consume, _traceMsgBatchNum, _traceTopic, _rpcHook);
                dispatcher.SetHostConsumer(this);
                _traceDispatcher = dispatcher;
                RegisterConsumeMessageHook(new ConsumeMessageTraceHook(dispatcher));
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

    private static void JoinIfAlive(Thread? t)
    {
        if (t is not null && t.IsAlive)
        {
            t.Join();
        }
    }

    /// <summary>Background thread factory (dispatch / pull / persist / lock /
    /// rebalance all go through here).</summary>
    /// <remarks>⚠ The fallback try/catch here is DELIBERATE; do not delete it
    /// as "hiding bugs": when a background task throws Throwable, most
    /// runtimes' threading models lose only that one thread (with a log),
    /// while C#'s DEFAULT BEHAVIOR IS AN UNHANDLED EXCEPTION TERMINATES THE
    /// WHOLE PROCESS. Observed consequence: a pull thread calling Client()
    /// during a Shutdown race throws MQClientException → the entire live
    /// process `Abort trap: 6` (RC=134), and none of the later scenarios
    /// (S6/S7) ran a single line. With this fallback, a process-level crash
    /// degrades to "that background thread exits + one WARN", so the
    /// observable behavior converges to a single-thread loss.
    /// Note: this WARN should NOT appear on the normal path -- its presence
    /// means a real bug to investigate.</remarks>
    private static Thread MakeThread(string name, ThreadStart action)
    {
        var t = new Thread(() =>
        {
            try
            {
                action();
            }
            catch (Exception e)
            {
                ClientLog.Warn("background thread (" + name + ") exited unexpectedly: " + e);
            }
        })
        {
            IsBackground = true,
            Name = name,
        };
        return t;
    }

    /// <summary>At early startup, pull the real routes of subscribed topics
    /// into the local cache (the first pull of
    /// updateTopicRouteInfoFromNameServer). The consume side has no default
    /// topic fallback -- topics like %RETRY%group that the broker has not
    /// created yet stay empty when unpullable, so the rebalance view stays
    /// consistent.</summary>
    private void RefreshRoutes()
    {
        foreach (string t in SubscribedTopics())
        {
            try
            {
                Client().UpdateTopicRouteInfoFromNameServer(t);
            }
            catch (Exception e)
            {
                ClientLog.Debug("refresh route for " + t + " failed: " + e.Message);
            }
        }
    }

    public MQClientInstance Client()
    {
        if (!_started || _mqClient is null)
        {
            throw new MQClientException("consumer not started, call start() first");
        }

        return _mqClient;
    }

    // ---------------- Consume loops ----------------
    private bool IsOrderly() => _messageListener is not null && _messageListener.Orderly();

    /// <summary>Whether this thread is still the owner of the queue
    /// (ownership token). Must be called while holding <c>_lock</c>.</summary>
    /// <remarks>The common approach expresses this with the queue buffer
    /// object itself (one pull request bound to one queue object,
    /// <c>setDropped(true)</c> on revocation, and the request discards its
    /// batch on return). This implementation has no per-queue buffer object,
    /// so "is the thread registered in the table me" serves as the
    /// equivalent: even if the old thread is still hanging on a long poll
    /// during self-healing rebuild, it concedes on return and never serves
    /// the same queue as the new thread.</remarks>
    private bool OwnsQueueLocked(string key)
    {
        return _pullThreads.TryGetValue(key, out Thread? t) && ReferenceEquals(t, Thread.CurrentThread);
    }

    /// <summary>Remove the registration only while still the owner, to avoid
    /// deleting the new owner's entry.</summary>
    private void UnregisterOwnLoop(string key)
    {
        lock (_lock)
        {
            if (OwnsQueueLocked(key))
            {
                _pullThreads.Remove(key);
            }
        }
    }

    /// <summary>Stamp before initiating this pull/pop round: the stamp comes
    /// BEFORE flow control and suspend; the criterion is "this loop is still
    /// running", not "this round actually hit the network".</summary>
    private void StampPullAt(string key, bool pop)
    {
        long now = UtilAll.CurrentTimeMillis();
        PopProcessQueue? pq = null;
        lock (_lock)
        {
            _lastPullAt[key] = now;
            if (pop)
            {
                _popQueues.TryGetValue(key, out pq);
            }
        }

        pq?.Touch(now);
    }

    /// <summary>Pull-timeout verdict + thread liveness check: both count as
    /// stalled -- either the registered loop thread is gone (an exception
    /// punched through), or it is alive but has not initiated a pull for
    /// longer than <see cref="PullMaxIdleTime"/>. Never stamped (just
    /// assigned) does not count as stalled.</summary>
    private bool PullStalledLocked(string key, long now)
    {
        if (!_lastPullAt.TryGetValue(key, out long began))
        {
            return false;
        }

        if (now - began > PullMaxIdleTime)
        {
            return true;
        }

        return !(_pullThreads.TryGetValue(key, out Thread? t) && t is not null && t.IsAlive);
    }

    /// <summary>Stall self-healing: the queue is still assigned to this
    /// instance but its pull loop has stalled → revoke it and hand over the
    /// offset so the later <see cref="RebalancePullThreads"/> in this round
    /// rebuilds it in place.</summary>
    private void SweepStalledLoopsLocked(Dictionary<string, MessageQueue> current,
                                         List<RetiredQueue> retired)
    {
        if (!_started || _stop)
        {
            // During shutdown the loops are exiting anyway; do not turn
            // cleanup into a [BUG] log storm
            return;
        }

        long now = UtilAll.CurrentTimeMillis();
        List<string> stalled = new();
        foreach (string key in _pullThreads.Keys)
        {
            if (current.ContainsKey(key) && PullStalledLocked(key, now))
            {
                stalled.Add(key);
            }
        }

        foreach (string key in stalled)
        {
            ClientLog.Error("[BUG]doRebalance, " + ConsumerGroup
                            + ", try remove unnecessary mq, " + key
                            + ", because pull is pause, so try to fixed it");
            if (current.TryGetValue(key, out MessageQueue? mq) && mq is not null)
            {
                RetireQueueLocked(key, mq, retired);
            }
        }
    }

    /// <summary>The cleanup material for one revoked queue: the queue object
    /// plus the consumed offset to persist (which may not exist at all).</summary>
    private sealed record RetiredQueue(string Key, MessageQueue Mq, long ConsumeOffset, bool HadOffset);
    /// <summary>Remove every trace of one queue on this instance: the thread
    /// registration, stamp, buffer, both offset tables, lock state, and POP
    /// queue, and hand the consumed offset to persist to the caller's
    /// <paramref name="retired"/>.</summary>
    private void RetireQueueLocked(string key, MessageQueue mq, List<RetiredQueue> retired)
    {
        _pullThreads.Remove(key);   // the old loop finds the owner changed on
                                    // return → discards its batch and exits
        _lastPullAt.Remove(key);    // reusing the same queue name must not
                                    // inherit the old timestamp
        _pending.Remove(key);
        _offsetTable.Remove(key);
        _lockOk.Remove(key);
        _lockOkAt.Remove(key);
        _mqMap.Remove(key);
        // In-flight registrations are voided too: the queue has been removed
        // from the registered assignment table (which the sweep walks), so
        // old batches' references should not linger in the sweep view
        _inFlightMsgs.Remove(key);
        // Epoch +1 ⇒ in-flight batches' acks are all voided (isDropped
        // semantics); the frozen marker is RETAINED until the queue is
        // rebuilt (RebalancePullThreads unfreezes it) so a corrected offset
        // is not overwritten by an old ack.
        _queueEpoch.TryGetValue(key, out long epoch);
        _queueEpoch[key] = epoch + 1;
        if (_popQueues.TryGetValue(key, out PopProcessQueue? pq))
        {
            pq.SetDropped(true);
            _popQueues.Remove(key);
        }

        bool had = _consumeOffsetTable.TryGetValue(key, out long consumeOffset);
        _consumeOffsetTable.Remove(key);
        retired.Add(new RetiredQueue(key, mq, consumeOffset, had));
        _retiredForTest.Add(new RetiredForTest(key, mq, consumeOffset, had));
    }

    /// <summary>Unit tests only: write the consumed offset directly into the
    /// table (the real path is advanced by ConsumeBatch, which requires
    /// running the loop first). This offset must be handed over for
    /// persistence when the queue is revoked, otherwise the rebuild
    /// redelivers from the broker's old offset.</summary>
    public void SetConsumeOffsetForTest(string key, long offset)
    {
        lock (_lock)
        {
            _consumeOffsetTable[key] = offset;
        }
    }

    /// <summary>Unit tests only: occupy one queue's PopProcessQueue and return
    /// it, to assert that "self-healing setDropped's the in-flight batch and
    /// swaps in a clean queue buffer".</summary>
    public PopProcessQueue RegisterPopQueueForTest(string key)
    {
        var pq = new PopProcessQueue();
        lock (_lock)
        {
            _popQueues[key] = pq;
        }

        return pq;
    }

    /// <summary>Unit tests only: the PopProcessQueue currently attached to
    /// this queue (after self-healing rebuild it should be a fresh one).</summary>
    public PopProcessQueue? PopQueueForTest(string key)
    {
        lock (_lock)
        {
            return _popQueues.TryGetValue(key, out PopProcessQueue? pq) ? pq : null;
        }
    }

    // ---------------- Observable seams for pull-stall self-healing ----------------
    //
    // Exposed as verification seams (the test project has no
    // InternalsVisibleTo, hence public; not for business code): verification
    // scripts must be able to wind the clock back, trigger one sync, and then
    // confirm "the stalled queue is revoked and rebuilt, not one message
    // duplicated or lost". Without seams one could only sleep a full 120s and
    // guess.

    /// <summary>The effective value of PULL_MAX_IDLE_TIME.</summary>
    public static long PullMaxIdleTimeMillis => PullMaxIdleTime;

    /// <summary>Whether this queue would now be judged stalled by rebalance.</summary>
    public bool PullStalled(string key)
    {
        lock (_lock)
        {
            return PullStalledLocked(key, UtilAll.CurrentTimeMillis());
        }
    }

    /// <summary>Unit tests only: hand the criterion's clock to the caller.
    /// With a real clock the "exactly 120s" boundary cannot be asserted
    /// stably (a few milliseconds always advance between reading the table
    /// and comparing), and the criterion uses a strict <c>&gt;</c>; the
    /// one-character difference (<c>&gt;=</c>) can only be pinned by
    /// injecting the clock.</summary>
    public bool PullStalledForTest(string key, long now)
    {
        lock (_lock)
        {
            return PullStalledLocked(key, now);
        }
    }

    /// <summary>Timestamp (ms) of the most recent pull/pop attempt;
    /// <c>-1</c> = no record.</summary>
    public long LastPullAt(string key)
    {
        lock (_lock)
        {
            return _lastPullAt.TryGetValue(key, out long t) ? t : -1;
        }
    }

    /// <summary>Wind the stamp to a given moment (verification: equivalent to
    /// "this loop has been quiet for X milliseconds").</summary>
    public void SetLastPullAt(string key, long millis)
    {
        lock (_lock)
        {
            _lastPullAt[key] = millis;
        }
    }

    /// <summary>Whether this queue currently has a pull/pop loop registered
    /// (after self-healing it must be a different one).</summary>
    public bool HasPullLoop(string key)
    {
        lock (_lock)
        {
            return _pullThreads.ContainsKey(key);
        }
    }

    /// <summary>Unit tests only: the full cleanup material of one revocation.
    /// <see cref="SweepStalledLoopsForTest"/> only returns "how many were
    /// revoked" and cannot pin the CONTENT (whether the offset was taken
    /// along, whether the wrong queue was revoked).</summary>
    public sealed record RetiredForTest(string Key, MessageQueue Mq, long ConsumeOffset, bool HadOffset);

    private readonly List<RetiredForTest> _retiredForTest = new();

    public IReadOnlyList<RetiredForTest> RetiredQueuesForTest()
    {
        lock (_lock)
        {
            return _retiredForTest.ToList();
        }
    }

    public void ClearRetiredForTest()
    {
        lock (_lock)
        {
            _retiredForTest.Clear();
        }
    }

    /// <summary>Run one stall sweep and return the number of revoked queues
    /// (verification/unit tests).</summary>
    public int SweepStalledLoopsForTest(IEnumerable<MessageQueue> current)
    {
        var dict = new Dictionary<string, MessageQueue>(StringComparer.Ordinal);
        foreach (MessageQueue mq in current)
        {
            dict[OffsetKey(mq)] = mq;
        }

        var retired = new List<RetiredQueue>();
        lock (_lock)
        {
            SweepStalledLoopsLocked(dict, retired);
        }

        return retired.Count;
    }

    /// <summary>Set the "started" flag to a given value (unit tests only: the
    /// real Start does network registration).</summary>
    public void SetStartedForTest(bool started)
    {
        _started = started;
    }

    /// <summary>Unit-test entry: set the "queue set currently assigned to this
    /// instance" to the given value and run one pull-thread rebuild -- used to
    /// pin "rebuild unfreezes" (the queue relies on it to resume acking after
    /// OFFSET_ILLEGAL correction). No cluster: the new loop immediately exits
    /// on its own because this side has no MQClientInstance.</summary>
    public void RebuildPullThreadsForTest(List<MessageQueue> assigned)
    {
        lock (_lock)
        {
            _assigned = new List<MessageQueue>(assigned);
        }

        RebalancePullThreads();
    }

    /// <summary>Register a placeholder loop thread for a queue (unit tests):
    /// <paramref name="alive"/>=false is a "never started / already exited"
    /// thread, which triggers exactly that half of the <c>isAlive</c>
    /// criterion. Live placeholder threads are released uniformly by
    /// <see cref="ReleaseTestLoops"/>.</summary>
    public void RegisterLoopForTest(string key, bool alive)
    {
        Thread t = MakeThread("PullMessageService", () => _testLoopGate.Wait());
        lock (_lock)
        {
            _pullThreads[key] = t;
        }

        if (alive)
        {
            t.Start();
        }

        // !alive does not Start: the Thread object exists but IsAlive=false,
        // equivalent to the thread having exited
    }

    /// <summary>Release the placeholder threads started by
    /// <see cref="RegisterLoopForTest"/> so unit tests leave no live threads.</summary>
    public void ReleaseTestLoops()
    {
        _testLoopGate.Set();
    }

    private readonly ManualResetEventSlim _testLoopGate = new(false);

    private void RebalancePullThreads()
    {
        List<MessageQueue> queues = AssignedQueues();
        var current = new Dictionary<string, MessageQueue>(StringComparer.Ordinal);
        foreach (MessageQueue mq in queues)
        {
            current[OffsetKey(mq)] = mq;
        }

        List<KeyValuePair<string, MessageQueue>> toStart = new();
        long beganAt;
        lock (_lock)
        {
            beganAt = UtilAll.CurrentTimeMillis();
            foreach (KeyValuePair<string, MessageQueue> kv in current)
            {
                if (!_pullThreads.ContainsKey(kv.Key))
                {
                    toStart.Add(kv);
                    // Register the queue object and the start moment at the
                    // moment of assignment: a queue that has not pulled a
                    // single message must still be visible in 307/220 and
                    // have its offset persistable; without planting a moment
                    // the next rebalance round has nothing to judge by.
                    _mqMap[kv.Key] = kv.Value;
                    _lastPullAt[kv.Key] = beganAt;
                    // The new queue buffer is in place ⇒ unfreeze
                    // (removeProcessQueue also clears the frozen state): the
                    // rebuilt queue resumes advancing from the corrected
                    // offset. Staying in the frozen set would make this queue
                    // pull-but-never-ack from then on -- the offset stuck at
                    // the corrected value forever, never moving even on
                    // redelivery.
                    _frozenOffsets.Remove(kv.Key);
                }
            }
        }

        foreach (KeyValuePair<string, MessageQueue> kv in toStart)
        {
            // Do not start new pull threads during Shutdown (_started already
            // false / _stop already true): the thread would hit the
            // "not started" state right after start and exit immediately --
            // pure waste and it widens the race window.
            if (_stop || !_started) return;
            MessageQueue mq = kv.Value;
            if (PopMode && !_popQueues.ContainsKey(kv.Key))
            {
                _popQueues[kv.Key] = new PopProcessQueue();
            }

            Thread t = MakeThread(PopMode ? "PopMessageService" : "PullMessageService",
                                  () => { if (PopMode) QueuePopLoop(mq); else QueuePullLoop(mq); });
            lock (_lock)
            {
                // Race protection: rebalance may start the same key again
                if (_pullThreads.ContainsKey(kv.Key))
                {
                    continue;
                }

                _pullThreads[kv.Key] = t;
            }

            t.Start();
        }
    }

    /// <summary>Wake this consumer's rebalance thread after the instance
    /// receives the broker's 40 notification (the rebalanceImmediately →
    /// rebalance-loop wakeup semantics of this port). Runs on the remoting reader
    /// thread: only sets the flag, sends no RPC, does no heavy work.</summary>
    /// <remarks>⚠ You must ABSOLUTELY NEVER call DoRebalance() synchronously
    /// here: it internally does blocking invokeSync
    /// (GET_CONSUMER_LIST_BY_GROUP, heartbeats), and responses can only be
    /// delivered by the SAME reader thread -- a reader thread blocked on a
    /// synchronous call it initiated necessarily self-deadlocks until
    /// invokeTimeout (observed: 5s timeout, log shows
    /// "no consumer id list ..., keep current", and other requests'
    /// responses get stuck along with it). Just set the flag and let
    /// RebalanceThread do the math; no extra synchronous calls.</remarks>
    private void WakeRebalanceLoop()
    {
        _rebalanceNow.Set();
    }

    /// <summary>Live verification only: wake the rebalance thread, equivalent
    /// to the broker-pushed <c>NOTIFY_CONSUMER_IDS_CHANGED(40)</c> (also only
    /// sets the flag; the real rebalance is done by the rebalance thread, see
    /// <see cref="WakeRebalanceLoop"/>). After injecting a stall one has to
    /// wait for the 20s period to observe self-healing; without this event
    /// one can only sleep and gamble.</summary>
    public void WakeupRebalanceForTest()
    {
        WakeRebalanceLoop();
    }

    /// <summary>Unit-test entry: whether the rebalance request has been set
    /// (the queue must be rebuilt through it after OFFSET_ILLEGAL
    /// correction).</summary>
    public bool RebalancePendingForTest() => _rebalanceNow.IsSet;

    private RemotingCommand? OnGetConsumerRunningInfo(RemotingCommand cmd, string addr)
    {
        // GET_CONSUMER_RUNNING_INFO(307) handling.
        // Runs on the reader thread: only local snapshots (building
        // ConsumerRunningInfo), never any network calls.
        string? group = null;
        if (cmd.ExtFields is not null && cmd.ExtFields.TryGetValue("consumerGroup", out string? g))
        {
            group = g;
        }

        if (!string.Equals(group, ConsumerGroup, StringComparison.Ordinal))
        {
            // The running info is this consumer's own, so a request naming
            // another group is not this consumer's to answer. Replying here --
            // even with an error -- would shadow the sibling that owns that
            // group, because the instance hands back the first answer it gets.
            return null;
        }

        RemotingCommand resp = RemotingCommand.CreateResponseCommand(ResponseCode.Success, null);
        resp.Body = BuildConsumerRunningInfo().Encode();
        return resp;
    }

    /// <summary>Unit-test entry: 220 inbound handling (the real path is
    /// attached directly via <c>RegisterProcessor</c> to
    /// <see cref="OnResetConsumerOffset"/>; the test project has no
    /// InternalsVisibleTo relationship with this assembly, hence public; not
    /// for business code).</summary>
    public RemotingCommand? ProcessResetConsumerOffset(RemotingCommand cmd) =>
        OnResetConsumerOffset(cmd, string.Empty);

    private RemotingCommand? OnResetConsumerOffset(RemotingCommand cmd, string addr)
    {
        // RESET_CONSUMER_CLIENT_OFFSET(220) handling:
        // oneway request (the broker waits for no reply); the reset logic goes
        // to a background thread (must not block the reader thread).
        string? group = null;
        string? topic = null;
        if (cmd.ExtFields is not null)
        {
            if (cmd.ExtFields.TryGetValue("group", out string? g))
            {
                group = g;
            }

            if (cmd.ExtFields.TryGetValue("topic", out string? t))
            {
                topic = t;
            }
        }

        if (!string.Equals(group, ConsumerGroup, StringComparison.Ordinal))
        {
            return null;   // oneway, no reply
        }

        // 222's body has two shapes: a map (ResetOffsetBody) and an array
        // (ResetOffsetBodyForC, which the broker only pushes when the 222
        // initiator has language=CPP). The protocol's own processor only
        // recognizes the map -- an initiator that always sends JAVA never
        // encounters the array; this port falls back to the array to
        // interoperate with language=CPP initiators: the map parser only
        // throws/returns an empty table for an array, and not falling back
        // means the whole reset is silently discarded.
        var table = new Dictionary<MessageQueue, long>();
        if (cmd.Body is { Length: > 0 })
        {
            if (ResetOffsetBody.Decode(cmd.Body, out ResetOffsetBody body))
            {
                foreach (var kv in body.OffsetTable)
                {
                    table[kv.Key] = kv.Value;
                }
            }

            if (table.Count == 0 && ResetOffsetBodyForC.Decode(cmd.Body, out ResetOffsetBodyForC forC))
            {
                foreach (MessageQueueForC e in forC.OffsetTable)
                {
                    table[new MessageQueue(e.Topic, e.BrokerName, e.QueueId)] = e.Offset;
                }
            }
        }

        DefaultMQPushConsumer consumer = this;
        string topicArg = topic ?? string.Empty;
        MakeThread("ResetOffsetThread", () =>
        {
            try
            {
                consumer.ResetOffset(topicArg, table);
            }
            catch (Exception e)
            {
                ClientLog.Warn("reset offset failed (group=" + ConsumerGroup + "): " + e.Message);
            }
        }).Start();
        return null;
    }

    private RemotingCommand? OnGetConsumerStatus(RemotingCommand cmd, string addr)
    {
        // Runs on the reader thread: only local snapshots (the consumed
        // offset table), never any network calls.
        string? group = null;
        string? topic = null;
        if (cmd.ExtFields is not null)
        {
            if (cmd.ExtFields.TryGetValue("group", out string? g))
            {
                group = g;
            }

            if (cmd.ExtFields.TryGetValue("topic", out string? t))
            {
                topic = t;
            }
        }

        if (!string.Equals(group, ConsumerGroup, StringComparison.Ordinal))
        {
            // The offset table is looked up per group, so a request naming
            // another group is not this consumer's to answer. Reply nothing: a
            // sibling sharing this instance owns that group, and answering here
            // would shadow its table (the instance still answers an empty table
            // when nobody owns the group).
            return null;
        }

        var body = new GetConsumerStatusBody();
        foreach (var kv in GetConsumerStatus(topic ?? string.Empty))
        {
            body.MessageQueueTable[kv.Key] = kv.Value;
        }

        RemotingCommand resp = RemotingCommand.CreateResponseCommand(ResponseCode.Success, null);
        resp.Body = body.Encode();
        return resp;
    }

    private RemotingCommand? OnConsumeMessageDirectly(RemotingCommand cmd, string addr)
    {
        // The listener runs synchronously on the reader thread (an admin
        // one-shot probe; a synchronous request from inside the listener
        // would self-deadlock, but that is the user code's responsibility --
        // this constraint always holds on the reader thread).
        string? group = null;
        string? brokerName = null;
        if (cmd.ExtFields is not null)
        {
            if (cmd.ExtFields.TryGetValue("consumerGroup", out string? g))
            {
                group = g;
            }

            if (cmd.ExtFields.TryGetValue("brokerName", out string? b))
            {
                brokerName = b;
            }
        }

        if (!string.Equals(group, ConsumerGroup, StringComparison.Ordinal))
        {
            // Running the probe would consume the message with this consumer's
            // listener and report that as the named group's result. Stay
            // silent: the sibling that owns the group answers, and the instance
            // reports the group is not hosted here when nobody does.
            return null;
        }

        if (cmd.Body is null || cmd.Body.Length == 0)
        {
            return RemotingCommand.CreateResponseCommand(ResponseCode.SystemError, "empty message body");
        }

        if (!MessageDecoder.DecodeMessage(cmd.Body, out MessageExt msg, true, true, true, false))
        {
            return RemotingCommand.CreateResponseCommand(ResponseCode.SystemError, "decode message failed");
        }

        RemotingCommand ok = RemotingCommand.CreateResponseCommand(ResponseCode.Success, null);
        ok.Body = ConsumeMessageDirectly(msg, brokerName ?? string.Empty).Encode();
        return ok;
    }

    /// <summary>Build the 307 response body (consumerRunningInfo).</summary>
    public ConsumerRunningInfo BuildConsumerRunningInfo()
    {
        var info = new ConsumerRunningInfo
        {
            Properties =
            {
                [ConsumerRunningInfo.PropNameserverAddr] = string.Join(";", _nameServerAddrs) + ";",
                [ConsumerRunningInfo.PropConsumeType] = "CONSUME_PASSIVELY",
                [ConsumerRunningInfo.PropConsumeOrderly] = IsOrderly() ? "true" : "false",
                [ConsumerRunningInfo.PropThreadpoolCoreSize] =
                    GetCorePoolSize().ToString(CultureInfo.InvariantCulture),
                [ConsumerRunningInfo.PropConsumerStartTimestamp] =
                    _startTimestamp.ToString(CultureInfo.InvariantCulture),
                [ConsumerRunningInfo.PropClientVersion] = "V5_5_1",
            }
        };

        JsonValue subs = JsonValue.MakeArray();
        var statusTable = JsonValue.MakeObject();
        lock (_lock)
        {
            foreach (SubscriptionData sub in _subscriptionData.Values)
            {
                subs.PushArray(sub.ToJson());
            }

            foreach (var kv in _mqMap)
            {
                // In POP mode popped queues go to _popQueues
                // (popProcessQueueTable) while classic's processQueueTable is
                // empty -- the two tables are mutually exclusive, and the 307
                // response must keep them exclusive too: the same queue
                // appearing in both mqTable and mqPopTable makes the console
                // count one consumption path as two. _mqMap is the "assigned"
                // registry (self-healing and offset persistence rely on it),
                // written by both modes, so filter by mode.
                if (PopMode && _popQueues.ContainsKey(kv.Key))
                {
                    continue;
                }

                string mqKey = MessageQueueKeys.MessageQueueKey(kv.Value);
                long commitOffset = _consumeOffsetTable.TryGetValue(kv.Key, out long co) ? co : 0;
                int cachedMsgCount = _pending.TryGetValue(kv.Key, out Queue<MessageExt>? q)
                    ? q.Count
                    : 0;
                // This field in the runtime info -- ops reads it to tell
                // "is it still pulling", and rebalance's stall self-healing
                // uses the very same timestamp.
                long lastPull = _lastPullAt.TryGetValue(kv.Key, out long lpt) ? lpt : 0;
                info.MqTable.Set(mqKey, MakeProcessQueueInfo(commitOffset, cachedMsgCount,
                    droped: false, lastPullTimestamp: lastPull));
            }

            if (PopMode)
            {
                foreach (var kv in _popQueues)
                {
                    if (!_mqMap.TryGetValue(kv.Key, out MessageQueue? mq))
                    {
                        continue;
                    }

                    info.MqPopTable.Set(MessageQueueKeys.MessageQueueKey(mq),
                        MakeProcessQueueInfo(0, kv.Value.WaitAckCount(), kv.Value.IsDropped(),
                            // The pop view does not have this field
                            // (PopProcessQueue leaves it unset), but
                            // lastPopTimestamp is the stall criterion itself,
                            // so expose it truthfully.
                            lastPullTimestamp: kv.Value.LastPopTimestamp));
                }
            }
        }

        // statusTable (consumeStatus(group, topic), minute snapshot)
        foreach (string topic in _subscriptionData.Keys)
        {
            ConsumeStatus cs = _mqClient?.ConsumerStats.ConsumeStatus(ConsumerGroup, topic)
                ?? new ConsumeStatus();
            statusTable.Set(topic, cs.ToJson());
        }

        info.SubscriptionSet = subs;
        info.StatusTable = statusTable;
        return info;
    }

    /// <summary>
    /// resetOffset: the consumer-side logic of 220.
    /// <para>
    /// Four steps: <c>pq.setDropped(true); pq.clear()</c> (in-flight batches'
    /// acks are voided together with the buffer) → when not orderly, wait
    /// <c>RESET_OFFSET_MAX_WAIT</c> = 10 seconds for concurrent consumption to
    /// finish → <c>updateConsumeOffset(mq, offset)</c> +
    /// <c>removeUnnecessaryMessageQueue</c> (persist immediately, unlock
    /// orderly) → remove the queue from processQueueTable and let rebalance
    /// rebuild at the new offset.
    /// </para>
    /// <para>
    /// This port: the new offset lands in the table first, then the unified
    /// revocation path runs (<see cref="RetireQueueLocked"/> epoch +1 is
    /// setDropped, clearing buffers and detaching pull threads), the
    /// revocation epilogue persists the <b>new offset</b>, and finally
    /// rebalance rebuilds. Shortening the wait from 10s to 0.2s is a
    /// deliberate deviation: the epoch has already voided all in-flight acks,
    /// so "waiting" is not relied on to avoid races.
    /// </para>
    /// </summary>
    public void ResetOffset(string topic, IReadOnlyDictionary<MessageQueue, long> offsetTable)
    {
        if (string.IsNullOrEmpty(topic) || offsetTable.Count == 0)
        {
            return;
        }

        var retired = new List<RetiredQueue>();
        lock (_lock)
        {
            foreach (var kv in _mqMap)
            {
                MessageQueue mq = kv.Value;
                if (mq.Topic != topic)
                {
                    continue;
                }

                if (!offsetTable.TryGetValue(mq, out long off))
                {
                    continue;
                }

                // The new offset is written into the table first: revocation
                // hands IT to OnQueuesRevoked for persistence (the persist
                // right after updateConsumeOffset)
                _consumeOffsetTable[kv.Key] = off;
                RetireQueueLocked(kv.Key, mq, retired);
            }
        }

        if (retired.Count == 0)
        {
            return;
        }

        // Semantically wait for concurrent consumption to finish
        // (RESET_OFFSET_MAX_WAIT); shortened here to avoid blocking too long
        // (220 is oneway, the broker waits for no reply, but it should still
        // return as soon as possible).
        Thread.Sleep(200);
        OnQueuesRevoked(retired);

        try
        {
            DoRebalance();   // Reassign (the queue is still in the assignment
                            // set; re-pull from the new offset)
        }
        catch (Exception e)
        {
            ClientLog.Debug("rebalance after reset offset failed: " + e.Message);
        }

        ClientLog.Info("reset offset applied, group=" + ConsumerGroup + " topic=" + topic
            + " queues=" + retired.Count.ToString(CultureInfo.InvariantCulture));
    }

    /// <summary>
    /// The epilogue for revoked queues (removeUnnecessaryMessageQueue):
    /// clustering mode pushes the offset to the broker + unlocks orderly
    /// consumption; broadcasting mode stores the offset only locally, writing
    /// it to disk along with the queue info.
    /// </summary>
    private void OnQueuesRevoked(List<RetiredQueue> revoked)
    {
        if (_messageModel == RocketMQ.Remoting.Protocol.MessageModel.Broadcasting)
        {
            // Broadcasting mode stores the offset only locally. Revoked
            // offsets must be written back to the file along with the queue
            // info: the order is persist(mq) first, removeOffset(mq) after
            // (only the in-memory entry is removed; the file's value stays),
            // and without this step the rebuilt queue cannot read the old
            // offset and rescans from head/tail per consumeFromWhere.
            SaveLocalOffsets(revoked);
            return;
        }

        MQClientInstance? client = _mqClient;
        if (client is null)
        {
            return;
        }

        bool orderly = IsOrderly();
        var unlockList = new List<MessageQueue>();
        foreach (RetiredQueue r in revoked)
        {
            // When the offset is unknown, write nothing: sending a default 0
            // to UPDATE_CONSUMER_OFFSET would wind the broker's offset BACK,
            // hence "write only when present".
            if (r.HadOffset)
            {
                try
                {
                    client.UpdateConsumerOffset(ConsumerGroup, r.Mq, r.ConsumeOffset);
                }
                catch (Exception e)
                {
                    ClientLog.Debug("persist offset on revoke failed for " + r.Mq + ": " + e.Message);
                }
            }

            // The per-queue unlock on revocation belongs to classic orderly
            // only: removeUnnecessaryMessageQueue takes the queue buffer from
            // processQueueTable -- that table is empty in POP mode, so this
            // path never runs at all.
            if (orderly && !PopMode)
            {
                unlockList.Add(r.Mq);
            }
        }

        if (unlockList.Count > 0)
        {
            try
            {
                client.UnlockBatchMq(ConsumerGroup, _clientId, unlockList);
            }
            catch (Exception e)
            {
                ClientLog.Debug("unlock on revoke failed: " + e.Message);
            }
        }
    }

    /// <summary>
    /// getConsumerStatus: the response data source for 221:
    /// returns the CONSUMED OFFSET table (not the pull cursor); an empty
    /// topic returns everything.
    /// </summary>
    public SortedDictionary<MessageQueue, long> GetConsumerStatus(string topic)
    {
        var outTable = new SortedDictionary<MessageQueue, long>();
        lock (_lock)
        {
            foreach (var kv in _mqMap)
            {
                if (!string.IsNullOrEmpty(topic) && kv.Value.Topic != topic)
                {
                    continue;
                }

                if (_consumeOffsetTable.TryGetValue(kv.Key, out long off))
                {
                    outTable[kv.Value] = off;
                }
            }
        }

        return outTable;
    }

    /// <summary>
    /// consumeMessageDirectly (309):
    /// really consume one message locally (restore the retry topic then hand
    /// it to the listener), replying with the result to the admin.
    /// </summary>
    public ConsumeMessageDirectlyResult ConsumeMessageDirectly(MessageExt msg, string brokerName)
    {
        var result = new ConsumeMessageDirectlyResult { AutoCommit = true };
        var msgs = new List<MessageExt> { msg };
        var mq = new MessageQueue(msg.Topic, brokerName ?? string.Empty, msg.QueueId);
        result.Order = IsOrderly();
        ResetRetryTopicAndNamespace(msgs);
        long begin = UtilAll.CurrentTimeMillis();
        if (_messageListener is null)
        {
            result.ConsumeResult = "CR_RETURN_NULL";
        }
        else if (_messageListener is IMessageListenerOrderly orderlyListener)
        {
            var ctx = new ConsumeOrderlyContext(mq);
            try
            {
                // The orderly consumeMessageDirectly mapping has two more
                // members than the concurrent side (COMMIT → CR_COMMIT,
                // ROLLBACK → CR_ROLLBACK; the broker distinguishes binlog
                // semantics by them), and AutoCommit reads the context value
                // AFTER the listener finishes.
                ConsumeOrderlyStatus status = orderlyListener.ConsumeMessage(msgs, ctx);
                result.ConsumeResult = status switch
                {
                    ConsumeOrderlyStatus.Commit => "CR_COMMIT",
                    ConsumeOrderlyStatus.Rollback => "CR_ROLLBACK",
                    ConsumeOrderlyStatus.Success => "CR_SUCCESS",
                    _ => "CR_LATER",
                };
            }
            catch (Exception e)
            {
                result.ConsumeResult = "CR_THROW_EXCEPTION";
                result.Remark = e.GetType().Name + ": " + e.Message;
            }

            // The context value read is the one AFTER the listener finishes
            // (the exception path reads it too). The binlog usage of setting
            // false inside the listener relies on this echo-back to tell the
            // broker "this direct consume did not commit"; reading the
            // initial value early swallows it, observable on live machines
            // only as mqadmin always reporting AutoCommit=true.
            result.AutoCommit = ctx.AutoCommit;
        }
        else if (_messageListener is IMessageListenerConcurrently concurrentListener)
        {
            try
            {
                var ctx = new ConsumeConcurrentlyContext(mq);
                ConsumeConcurrentlyStatus status = concurrentListener.ConsumeMessage(msgs, ctx);
                result.ConsumeResult = status == ConsumeConcurrentlyStatus.ConsumeSuccess
                    ? "CR_SUCCESS" : "CR_LATER";
            }
            catch (Exception e)
            {
                result.ConsumeResult = "CR_THROW_EXCEPTION";
                result.Remark = e.GetType().Name + ": " + e.Message;
            }
        }
        else
        {
            result.ConsumeResult = "CR_RETURN_NULL";
        }

        result.SpentTimeMills = UtilAll.CurrentTimeMillis() - begin;
        return result;
    }

    private static JsonValue MakeProcessQueueInfo(long commitOffset, long cachedMsgCount, bool droped,
                                                  long lastPullTimestamp = 0)
    {
        // All ProcessQueueInfo fields (production field names; the "droped"
        // spelling is copied verbatim, it is the key consumers match on)
        var pqi = JsonValue.MakeObject();
        pqi.Set("commitOffset", JsonValue.MakeInt(commitOffset));
        pqi.Set("cachedMsgMinOffset", JsonValue.MakeInt(0));
        pqi.Set("cachedMsgMaxOffset", JsonValue.MakeInt(0));
        pqi.Set("cachedMsgCount", JsonValue.MakeInt(cachedMsgCount));
        pqi.Set("cachedMsgSizeInMiB", JsonValue.MakeInt(0));
        pqi.Set("transactionMsgMinOffset", JsonValue.MakeInt(0));
        pqi.Set("transactionMsgMaxOffset", JsonValue.MakeInt(0));
        pqi.Set("transactionMsgCount", JsonValue.MakeInt(0));
        pqi.Set("locked", JsonValue.MakeBool(false));
        pqi.Set("tryUnlockTimes", JsonValue.MakeInt(0));
        pqi.Set("lastLockTimestamp", JsonValue.MakeInt(0));
        pqi.Set("droped", JsonValue.MakeBool(droped));
        pqi.Set("lastPullTimestamp", JsonValue.MakeInt(lastPullTimestamp));
        pqi.Set("lastConsumeTimestamp", JsonValue.MakeInt(0));
        return pqi;
    }

    /// <summary>
    /// Consume-side RT/TPS counting (RT always recorded; OK/FAILED by result).
    /// <paramref name="ackCount"/> follows processConsumeResult's
    /// ok = ackIndex + 1: on partial ack the prefix counts OK and the tail
    /// counts FAILED. Omitted means the whole batch counts (the old semantics
    /// of the orderly/POP paths).
    /// </summary>
    private void RecordConsumeStats(string topic, int msgCount, long beginMs, bool failed,
        int? ackCount = null)
    {
        if (_mqClient is null)
        {
            return;
        }

        ConsumerStatsManager stats = _mqClient.ConsumerStats;
        long rt = UtilAll.CurrentTimeMillis() - beginMs;
        if (failed)
        {
            stats.IncConsumeFailedTPS(ConsumerGroup, topic, msgCount);
        }
        else
        {
            int ok = ackCount ?? msgCount;
            stats.IncConsumeOKTPS(ConsumerGroup, topic, ok);
            if (msgCount > ok)
            {
                stats.IncConsumeFailedTPS(ConsumerGroup, topic, msgCount - ok);
            }
        }

        stats.IncConsumeRT(ConsumerGroup, topic, rt);
    }

    private void RebalanceLoop()
    {
        // Recompute assignments periodically (default 20s), or immediately
        // when notified. During startup with no current assignment, shorten
        // to a 2s retry: the consumer may start before the topic is created
        // (with autoCreateTopicEnable the broker creates the topic on the
        // producer's first send), so the real route is not pullable yet --
        // the consume side has no default topic fallback (see
        // MQClientInstance.UpdateTopicRouteInfoFromNameServer), and waiting a
        // full 20s means a long time without consumption. This fast retry
        // only applies within 60s after startup.
        long startTime = UtilAll.CurrentTimeMillis();
        while (!_stop)
        {
            bool startingUp = (UtilAll.CurrentTimeMillis() - startTime) < 60000;
            int interval = (startingUp && AssignedQueues().Count == 0) ? 2000 : 20000;
            _rebalanceNow.Wait(interval);
            _rebalanceNow.Reset();
            if (_stop || !_started) return;
            try
            {
                MaybeSendHeartbeat();
                DoRebalance();
            }
            catch (Exception e)
            {
                ClientLog.Debug("rebalance error: " + e.Message);
            }
        }
    }

    /// <summary>The pre-pull flow-control verdict -- the three per-queue
    /// thresholds checked before enqueueing into the buffer, returning on the
    /// first hit in this order (an earlier hit masks the later ones). Every cache gate compares
    /// <b>strictly greater than</b> its threshold: a buffer that has reached it
    /// exactly is still allowed to pull, so a threshold reads as "how many may be
    /// cached", not "how many stop the pull".
    /// <list type="number">
    /// <item>held count &gt; <c>PullThresholdForQueue</c></item>
    /// <item>held body MiB &gt; <c>PullThresholdSizeForQueue</c></item>
    /// <item>held offset span &gt; <c>ConsumeConcurrentlyMaxSpan</c>, and this term
    /// is <b>not judged at all for an orderly listener</b> -- the knob is named after
    /// concurrent consumption, and an orderly queue has no run-ahead to guard, because
    /// its cursor only moves once the batch in the listener's hands has settled</item>
    /// </list>
    /// What all three terms measure is the queue's whole <b>held set</b>: the
    /// pulled-but-undispatched buffer union the batches already handed to the listener
    /// and not yet settled (<see cref="ProcessQueueEntriesLocked"/>, deduplicated by
    /// queue offset). A dispatched batch has only moved, it has not left the queue -- it
    /// leaves when it settles through an ack/complete or is requeued. Judging the buffer
    /// alone would under-count and keep pulling while the listener still sits on
    /// messages, which is exactly what these thresholds exist to stop.
    /// The size term is the <b>integer (truncating) whole MiB of message BODY bytes</b>:
    /// a 1 MiB budget tolerates a 1.9 MiB backlog, because 1.9 truncated is 1. The
    /// stored-record size a broker charges for the whole storage record is a telemetry
    /// number and never enters this comparison.
    /// No threshold is floored here and none has a "this value switches me off" escape: the
    /// gate reads each knob exactly as it stands, and keeping every knob inside its legal range
    /// is <see cref="CheckConfigRanges"/>'s job at start-up, not this gate's.
    /// The verdict never reads a sibling queue: a topic-level cache budget reaches it only
    /// through <see cref="DivideTopicThresholds"/>, which splits the budget into the
    /// per-queue knobs at rebalance. There is no pull-time topic aggregation.
    /// One hit only increments <see cref="FlowControlTriggered"/> by one.</summary>
    private bool FlowControlHit(MessageQueue mq, string key)
    {
        bool orderly = IsOrderly();
        long count;
        long sizeMiB;
        long span;
        lock (_lock)
        {
            List<MessageExt> held = ProcessQueueEntriesLocked(key);
            long bytes = 0;
            long minOffset = 0;
            long maxOffset = 0;
            bool first = true;
            foreach (MessageExt m in held)
            {
                bytes += m.Body.Length;
                if (first)
                {
                    minOffset = maxOffset = m.QueueOffset;
                    first = false;
                }
                else
                {
                    if (m.QueueOffset < minOffset) minOffset = m.QueueOffset;
                    if (m.QueueOffset > maxOffset) maxOffset = m.QueueOffset;
                }
            }

            count = held.Count;
            sizeMiB = bytes / CacheSizeGateMib;
            span = first ? 0 : maxOffset - minOffset;
        }

        string? reason = null;
        if (count > _pullThresholdForQueue)
        {
            reason = "count=" + count.ToString(CultureInfo.InvariantCulture);
        }
        else if (sizeMiB > _pullThresholdSizeForQueue)
        {
            reason = "size=" + sizeMiB.ToString(CultureInfo.InvariantCulture) + "MB";
        }
        else if (!orderly && span > _consumeConcurrentlyMaxSpan)
        {
            reason = "span=" + span.ToString(CultureInfo.InvariantCulture);
        }

        if (reason is null)
        {
            return false;
        }

        long times = Interlocked.Increment(ref _flowControlTriggered);
        ClientLog.Debug("flow control: queue " + mq + " " + reason + ", pause pull");
        if (IsFlowControlWarnTrip(times))
        {
            ClientLog.Warn("flow control: queue " + mq + " " + reason
                + " exceeds its cache threshold, pause pull for " + CacheFlowControlBackoffMillis
                + " ms (flowControlTimes=" + times.ToString(CultureInfo.InvariantCulture) + ")");
        }
        return true;
    }

    /// <summary>The POP path's cache gate: **this queue's own** popped-but-unacked debt, compared
    /// strictly greater than <see cref="PopThresholdForQueue"/>. POP keeps no "pulled but not
    /// consumed" buffer, so the count / size / span checks of <see cref="FlowControlHit"/> have
    /// nothing to read here — once a message is popped, consumption has begun and what matters is
    /// the ack debt. The debt is per queue and never summed, and this threshold is a plain client
    /// knob: the topic budgets divided at rebalance belong to the pull path and do not move it.
    /// A hit increments the SAME <see cref="FlowControlTriggered"/> total the pull path uses, so one
    /// client-wide flow-control count spans both paths and the throttled warn cadence also sees the
    /// POP trips.</summary>
    private bool PopFlowControlHit(MessageQueue mq, PopProcessQueue pq)
    {
        int waitAck = pq.WaitAckCount();
        if (waitAck <= PopThresholdForQueue)
        {
            return false;
        }

        long times = Interlocked.Increment(ref _flowControlTriggered);
        ClientLog.Debug("pop flow control: queue " + mq + " waiting-ack="
            + waitAck.ToString(CultureInfo.InvariantCulture)
            + " over popThresholdForQueue=" + PopThresholdForQueue.ToString(CultureInfo.InvariantCulture)
            + ", pause pop");
        if (IsFlowControlWarnTrip(times))
        {
            ClientLog.Warn("pop flow control: queue " + mq + " the messages waiting to ack ("
                + waitAck.ToString(CultureInfo.InvariantCulture) + ") exceeds the pop threshold "
                + PopThresholdForQueue.ToString(CultureInfo.InvariantCulture)
                + ", so do flow control, pause pop for " + CacheFlowControlBackoffMillis
                + " ms (flowControlTimes=" + times.ToString(CultureInfo.InvariantCulture) + ")");
        }
        return true;
    }

    /// <summary>The number of trips between two WARN lines. A throttled queue trips a cache gate on
    /// every pull round, so warning each round buries the signal in noise. Public for the same
    /// reason as <c>FlowControlHitForTest</c>: the test project has no InternalsVisibleTo.</summary>
    public const int FlowControlWarnEvery = 1000;

    /// <summary>Whether the n-th (1-based) flow-control trip logs a WARN: the first, then every
    /// <see cref="FlowControlWarnEvery"/>-th trip after it.</summary>
    public static bool IsFlowControlWarnTrip(long times)
        => times > 0 && (times - 1) % FlowControlWarnEvery == 0;

    /// <summary>Client-side cache threshold reached: pause this queue's pull
    /// for a full period before asking the broker again.</summary>
    public const int CacheFlowControlBackoffMillis = 50;

    /// <summary>One MiB. The byte cache gate divides the held body total by this with
    /// <b>integer</b> arithmetic, so a partially filled MiB is not charged.</summary>
    public const long CacheSizeGateMib = 1024 * 1024;

    /// <summary>Broker-side flow control (response code 215) is a "busy right
    /// now" answer, NOT a capacity verdict on this client: retry early instead
    /// of parking on the generic error backoff
    /// (<see cref="PullExceptionBackoffMillis"/>).</summary>
    public const int BrokerFlowControlBackoffMillis = 20;

    /// <summary>The backoff the pull loop applies after a pull request was
    /// answered with an error response code. Shared by the concurrent and the
    /// orderly path (both run in the same per-queue loop), and by the POP loop's
    /// broker-error arm: FLOW_CONTROL(215) retries in 20 ms, every other broker
    /// error waits the generic backoff. The generic value is the consumer's own
    /// <see cref="PullExceptionBackoffMillis"/>, passed in by the loop that
    /// captured it when it was set up, so this decision stays pure and one rule
    /// keeps one name. Public so the arms are pinnable offline without racing a
    /// timer.</summary>
    public static int PullBackoffMillisForBrokerResponse(int responseCode,
        int pullExceptionBackoffMillis) =>
        responseCode == ResponseCode.FlowControl
            ? BrokerFlowControlBackoffMillis
            : pullExceptionBackoffMillis;

    /// <summary>Pure POP re-schedule decision, keyed on what actually survives
    /// the pre-delivery pipeline rather than the raw popped count:
    /// <list type="bullet">
    /// <item>Found with kept rows: wait PullIntervalMillis when it is &gt; 0,
    /// otherwise go straight to the next round;</item>
    /// <item>Found whose rows were ALL filtered out: straight to the next
    /// round (the interval only paces real deliveries);</item>
    /// <item>NoNewMsg / PollingNotFound: straight to the next round;</item>
    /// <item>PollingFull and any other/unknown status: the generic error
    /// backoff -- the same <see cref="PullExceptionBackoffMillis"/> the pull
    /// path parks a failed round on, so one rule has one name.</item>
    /// </list>
    /// A zero return means "next round now"; the loop's own hot-round guard
    /// still naps a fast EMPTY round, and that guard must never replace the
    /// generic backoff. The backoff arrives as an argument, captured by the POP
    /// loop when it was set up, so the decision stays pure. Public so the timing
    /// legs are pinnable offline without racing a timer.</summary>
    public static int PopNextRoundDelayMillis(PopStatus status, int keptCount,
        int pullIntervalMillis, int pullExceptionBackoffMillis)
    {
        switch (status)
        {
            case PopStatus.Found:
                return keptCount > 0 && pullIntervalMillis > 0 ? pullIntervalMillis : 0;
            case PopStatus.NoNewMsg:
            case PopStatus.PollingNotFound:
                return 0;
            case PopStatus.PollingFull:
            default:
                return pullExceptionBackoffMillis;
        }
    }

    /// <summary>Pure PULL re-schedule decision, keyed on what actually survives
    /// the pre-delivery filter (the same list PULL_TPS counts) rather than the raw
    /// pulled count. It mirrors <see cref="PopNextRoundDelayMillis"/>: one rule, one
    /// name, pinnable offline without racing a timer.
    /// <list type="bullet">
    /// <item>Found with delivered rows: wait PullIntervalMillis when it is &gt; 0,
    /// otherwise go straight to the next round;</item>
    /// <item>Found whose rows were ALL filtered out, or which came back empty:
    /// straight to the next round (the interval only paces real deliveries);</item>
    /// <item>NoNewMsg / NoMatchedMsg: straight to the next round -- the cadence
    /// paces deliveries, so a local wait on an idle round would only delay the
    /// next fetch;</item>
    /// <item>OffsetIllegal: straight to the next round -- this path never reaches a
    /// cadence wait, it freezes, persists, revokes and recovers the queue.</item>
    /// </list>
    /// A failed round (a broker error or an exception) never consults this helper:
    /// it parks on <see cref="PullBackoffMillisForBrokerResponse"/> or the generic
    /// error backoff before reaching here, so those two waits are not replaced by
    /// the cadence. A zero return means "next round now". The interval arrives as an
    /// argument, read fresh by the loop each round, so the decision stays pure.</summary>
    public static int PullNextRoundDelayMillis(PullStatus status, int deliveredCount,
        int pullIntervalMillis)
    {
        // Only a Found round can deliver rows; every other status -- including the
        // OffsetIllegal path, which recovers outside this decision -- goes straight
        // to the next round, and even a Found round waits only when it actually
        // handed something on.
        return status == PullStatus.Found && deliveredCount > 0 && pullIntervalMillis > 0
            ? pullIntervalMillis
            : 0;
    }

    /// <summary>The one status whose round the offset sanity check applies to: the
    /// comparison needs both the cursor this round asked with and the row list the
    /// same round produced, and only a FOUND round has a row list. Warning for any
    /// other status would put a line in the log on every idle queue.</summary>
    public static bool PullOffsetSanityApplies(PullStatus status) =>
        status == PullStatus.Found;

    /// <summary>Pure offset sanity test for a FOUND pull round, over the three
    /// numbers that round alone knows: the cursor it asked with
    /// (<paramref name="prevRequestOffset"/>), the cursor it was answered with
    /// (<paramref name="nextBeginOffset"/>), and the queue offset of the first row
    /// it hands to the consume side (<paramref name="firstMsgOffset"/>). Either of
    /// the latter two sitting below the requested cursor means the round's own
    /// numbers contradict each other, which is worth one WARN line. Nothing else:
    /// the cursor and the cadence are decided by other code, and this decides
    /// neither. A round with no row to point at passes
    /// <see cref="long.MaxValue"/> (see <see cref="PullFirstMsgOffset"/>), so the
    /// row leg stays silent for it instead of firing on a missing value. Public so
    /// both legs are pinnable offline.</summary>
    public static bool PullOffsetsLookWrong(long nextBeginOffset, long firstMsgOffset,
        long prevRequestOffset) =>
        nextBeginOffset < prevRequestOffset || firstMsgOffset < prevRequestOffset;

    /// <summary>The queue offset of the first row of the list a round produces for
    /// consumption -- the list AFTER the pre-delivery filter, which is the one the
    /// warning is about. An empty list reports <see cref="long.MaxValue"/>: the
    /// sentinel of <see cref="PullOffsetsLookWrong"/>'s row leg, so "no rows" is
    /// never read as "a row that runs backwards".</summary>
    public static long PullFirstMsgOffset(IReadOnlyList<MessageExt> msgs) =>
        msgs.Count > 0 ? msgs[0].QueueOffset : long.MaxValue;

    private void QueuePullLoop(MessageQueue mq)
    {
        // ⚠ Client() must NOT be used here: Shutdown() sets _started=false
        //    first and only then joins the pull threads; RebalancePullThreads
        //    may start this thread exactly inside that window, so this
        //    thread's very first statement would hit the "not started" state
        //    and Client() throws MQClientException. That exception is thrown
        //    on a BACKGROUND thread, and C# therefore terminates the whole
        //    process (observed: Abort trap: 6 / RC=134), whereas other
        //    threading models would only let that one thread exit. Returning
        //    normally after the liveness check turns the graceful exit into a
        //    normal path.
        MQClientInstance? clientRef = _mqClient;
        if (clientRef is null || !_started) return;
        MQClientInstance c = clientRef;
        bool orderly = IsOrderly();
        // Taken once for the life of this loop, like the POP loop's invisible time:
        // changing the generic error backoff after start() must not move a loop that
        // is already running.
        int errorBackoffMillis = PullExceptionBackoffMillis;
        string key = OffsetKey(mq);
        while (!_stop && _started)
        {
            // The queue has been revoked by rebalance (isDropped) OR is no
            // longer served by this thread (self-healing rebuild swapped the
            // thread): exit and stop pulling for that queue.
            bool dropped;
            bool owns;
            lock (_lock)
            {
                dropped = _dropped.Contains(key);
                owns = OwnsQueueLocked(key);
            }

            if (dropped || !owns)
            {
                if (dropped)
                {
                    UnregisterOwnLoop(key);
                }

                return;
            }

            // Stamp on every pull ATTEMPT, BEFORE the
            // flow-control/lock verdict: the criterion is "this loop is still
            // running", not "this round actually hit the network".
            StampPullAt(key, pop: false);

            // The suspend (Suspend()) verdict comes AFTER the stamp but BEFORE
            // locks and flow control: the stamp must come first, otherwise
            // lastPullTimestamp does not update during the whole suspend
            // period and the 120s stall death verdict would tear this loop
            // down as a dead loop, when the consumer is merely paused and not
            // faulty.
            if (_paused)
            {
                if (_stopEvent.Wait(TimeSpan.FromMilliseconds(1000)) || _stop) return;
                continue;
            }

            SubscriptionData sub;
            lock (_lock)
            {
                if (!_subscriptionData.TryGetValue(mq.Topic, out SubscriptionData? s) || s is null)
                {
                    return;
                }

                sub = s!;
            }

            // Orderly consumption: do not pull queues the broker has not
            // confirmed locked (LOCK_BATCH_MQ). Broadcasting is implicitly
            // locked: no broker-side lock round runs for it at all. A grant
            // older than LockMaxLiveTime counts as expired even if the last
            // sweep still has it in the set.
            if (orderly && _messageModel != RocketMQ.Remoting.Protocol.MessageModel.Broadcasting)
            {
                bool locked;
                bool expired;
                lock (_lock)
                {
                    locked = _lockOk.Contains(key);
                    expired = locked
                        && UtilAll.CurrentTimeMillis() - (_lockOkAt.TryGetValue(key, out long at) ? at : 0)
                           > LockMaxLiveTime;
                }

                if (locked && expired)
                {
                    ClientLog.Warn("the lock of the message queue is expired, wait for the next lock round. group="
                        + ConsumerGroup + " mq=" + mq);
                }

                if (!locked || expired)
                {
                    _stopEvent.Wait(TimeSpan.FromMilliseconds(200));
                    continue;
                }
            }

            // Flow control (the three per-queue gates, see FlowControlHit):
            // hitting any one pauses this queue's pulling. The topic-level
            // budgets are not summed here; they were divided into these
            // per-queue knobs when the assignment last changed.
            if (FlowControlHit(mq, key))
            {
                _stopEvent.Wait(TimeSpan.FromMilliseconds(CacheFlowControlBackoffMillis));
                continue;
            }

            long offset;
            lock (_lock)
            {
                offset = _offsetTable.TryGetValue(key, out long v) ? v : -1;
            }

            if (offset < 0)
            {
                try
                {
                    offset = ResolveInitialOffset(mq);
                }
                catch (Exception e)
                {
                    ClientLog.Debug("resolve initial offset failed for " + mq + ": " + e.Message);
                    _stopEvent.Wait(TimeSpan.FromMilliseconds(1000));
                    continue;
                }

                lock (_lock)
                {
                    _offsetTable[key] = offset;
                }
            }

            PullResult result;
            try
            {
                // The subscription expression is carried only when
                // postSubscriptionWhenPull is on AND the mode is not
                // class-filter (SUBSCRIPTION bit = subExpression != null).
                string? expr = PullSubscriptionExpression(sub);
                int sysFlag = PullSysFlag.BuildSysFlag(
                    /*commitOffset=*/false, /*suspend=*/true,
                    /*subscription=*/expr is not null, /*classFilter=*/false);
                long brokerId;
                lock (_lock)
                {
                    // No record counts as master=0
                    brokerId = _pullFromWhichNode.TryGetValue(key, out long bid) ? bid : MixAll.MasterId;
                }

                long pullBegan = UtilAll.CurrentTimeMillis();
                result = c.PullMessage(ConsumerGroup, mq, offset, _pullBatchSize, sysFlag,
                    /*commitOffset=*/0, expr ?? string.Empty, sub.SubVersion, sub.ExpressionType,
                    _pullTimeoutMillis, _pullBatchSizeInBytes, _pullSuspendTimeoutMillis,
                    /*addrIn=*/null, /*requestSource=*/0, /*brokerId=*/brokerId);
                // Every response round writes back
                // pullFromWhichNodeTable (missing values count as master=0;
                // a missing value is not treated as "keep the old value")
                UpdatePullFromWhichNode(key, result.SuggestWhichBrokerId);

                // Consume statistics: only a FOUND round books anything. PULL_RT is
                // this round's own client-side trip time, measured around the
                // PullMessage call, and a FOUND round books it even when the answer
                // carries no rows; a non-FOUND answer books nothing (a failed round
                // never reaches this line, it unwinds into the catch below).
                // PULL_TPS is fed further down, from the list that survives the
                // pre-delivery filter.
                if (result.IsFound)
                {
                    _mqClient!.ConsumerStats.IncPullRT(ConsumerGroup, mq.Topic,
                        UtilAll.CurrentTimeMillis() - pullBegan);
                }
            }
            catch (MQBrokerException e)
            {
                // TOPIC_NOT_EXIST / PULL_NOT_FOUND etc. are mostly expected
                // paths (topic not created yet); debug + back off. A broker
                // FLOW_CONTROL(215) is the exception: the broker is merely
                // busy right now, so retry early rather than parking on the
                // general exception backoff.
                ClientLog.Debug("pull broker error for " + mq + ": " + e.Message);
                _stopEvent.Wait(TimeSpan.FromMilliseconds(
                    PullBackoffMillisForBrokerResponse(e.ResponseCode, errorBackoffMillis)));
                continue;
            }
            catch (RemotingTimeoutException e)
            {
                // A long poll with no new messages during suspend triggering a
                // client timeout is normal behavior: an idle queue answers on
                // its own schedule, so our own request timeout can expire
                // before the reply does and idle queues time out
                // periodically. Not an error; debug only, to avoid polluting
                // the runtime log.
                ClientLog.Debug("pull long-poll timeout for " + mq
                    + " (benign, will retry): " + e.Message);
                continue;
            }
            catch (Exception e)
            {
                ClientLog.Debug("pull error for " + mq + ": " + e.Message);
                _stopEvent.Wait(TimeSpan.FromMilliseconds(errorBackoffMillis));
                continue;
            }

            // Confirm again after the long poll returns: if the queue was
            // revoked or this thread was swapped out by self-healing in the
            // meantime, discard this batch and exit (the isDropped guard --
            // pulled messages no longer enter the buffer).
            lock (_lock)
            {
                if (_dropped.Contains(key))
                {
                    if (OwnsQueueLocked(key))
                    {
                        _pullThreads.Remove(key);
                    }

                    return;
                }

                if (!OwnsQueueLocked(key))
                {
                    return;
                }
            }

            lock (_lock)
            {
                if (!_pending.ContainsKey(key))
                {
                    _pending[key] = new Queue<MessageExt>();
                    _mqMap[key] = mq;
                }
            }

            // The list that survives the pre-delivery filter -- the same one
            // PULL_TPS counts -- is what decides the round's cadence below. An
            // idle round leaves this at 0, so the cadence wait never runs for it.
            int deliveredCount = 0;
            if (result.Status == PullStatus.Found && result.MsgFoundList.Count > 0)
            {
                // Pre-delivery client-side filtering (processPullResult):
                // second-pass tag filter first, then run FilterMessageHook.
                // MUST happen BEFORE taking _lock -- the hooks are user code
                // and may block, so they cannot sit inside the enqueue
                // critical section. Messages removed on the pull path are NOT
                // acked: the offset advances as usual = silently skipped.
                int before = result.MsgFoundList.Count;
                result.MsgFoundList = FilterMessagesForDelivery(mq, sub, result.MsgFoundList);
                if (result.MsgFoundList.Count < before)
                {
                    Interlocked.Add(ref _filteredMessageCount, before - result.MsgFoundList.Count);
                }

                // PULL_TPS counts what this round actually hands on: the filtered list. A row the
                // second tag pass or a filter hook removed was never delivered, so counting the raw
                // response would overstate the topic's throughput by exactly the rows dropped here.
                if (result.MsgFoundList.Count > 0)
                {
                    _mqClient!.ConsumerStats.IncPullTPS(ConsumerGroup, mq.Topic,
                        result.MsgFoundList.Count);
                }

                // Retry-message topic restoration (processPullResult): the
                // broker writes retry messages to %RETRY%group, but each
                // message carries a RETRY_TOPIC property pointing at the
                // original topic; restore it before dispatch, otherwise the
                // upper listener sees the topic as %RETRY% rather than the
                // business topic. Runs outside the enqueue lock: it only
                // rewrites the rows' own topic.
                PreparePullRowsForDelivery(result.MsgFoundList, orderly);

                lock (_lock)
                {
                    Queue<MessageExt> dq = _pending[key];
                    foreach (MessageExt m in result.MsgFoundList)
                    {
                        dq.Enqueue(m);
                    }

                    // msgAccCnt: computed from the FILTERED-then-enqueued batch
                    // (filter first, then count)
                    UpdateMsgAccCntLocked(key, result.MsgFoundList);
                }

                // Whatever survives the filter -- even an empty list -- is what
                // the round delivered: a Found answer the filter emptied counts
                // as nothing delivered, so its cadence is "next round now".
                deliveredCount = result.MsgFoundList.Count;
            }

            // The pull cursor advances to nextBeginOffset; the "consumed
            // offset" is tracked and persisted by _consumeOffsetTable
            if (result.NextBeginOffset >= 0)
            {
                bool illegal = false;
                lock (_lock)
                {
                    _offsetTable[key] = result.NextBeginOffset;
                    if (result.Status == PullStatus.OffsetIllegal)
                    {
                        // When the offset is corrected by the broker, the
                        // correction must be done together with the rebuild;
                        // see OffsetIllegalRecover. Freezing happens inside the
                        // lock first (equivalent to updateAndFreezeOffset run
                        // synchronously in the callback); the revoke-queue and
                        // persist RPCs go outside the lock.
                        FreezeOffsetForIllegalLocked(key, result.NextBeginOffset);
                        illegal = true;
                    }
                    else
                    {
                        // Correct the "consumed offset" on an empty response
                        // (NO_NEW_MSG / NO_MATCHED_MSG); see
                        // CorrectTagsOffsetLocked
                        CorrectTagsOffsetLocked(key, result.Status, result.NextBeginOffset);
                    }
                }

                if (illegal)
                {
                    OffsetIllegalRecover(key, mq);
                }
            }

            // Offset sanity, at the end of a FOUND round: the cursor has already
            // advanced above and the batch has already been handed on, and `offset`
            // is still this round's requested cursor because nothing between the
            // request and here writes it. A breach writes one WARN carrying all
            // three numbers and changes nothing else -- not the cursor just stored,
            // not the buffer, not the cadence below (see PullOffsetsLookWrong). The
            // check sits before the pace wait because that wait ends the round.
            long prevRequestOffset = offset;
            if (PullOffsetSanityApplies(result.Status))
            {
                long firstMsgOffset = PullFirstMsgOffset(result.MsgFoundList);
                if (PullOffsetsLookWrong(result.NextBeginOffset, firstMsgOffset,
                    prevRequestOffset))
                {
                    ClientLog.Warn("pull offset sanity warning for " + mq
                        + ": nextBeginOffset=" + result.NextBeginOffset
                        + " firstMsgOffset=" + firstMsgOffset
                        + " prevRequestOffset=" + prevRequestOffset);
                }
            }

            // Round cadence (see PullNextRoundDelayMillis for the legs): only a
            // round that delivered rows is paced by PullIntervalMillis. An idle
            // round -- empty, filtered out, NO_NEW_MSG or NO_MATCHED_MSG -- asks
            // again at once, because the cadence paces deliveries and a local wait
            // on an idle round would only delay the next fetch; and OffsetIllegal /
            // a failed round never reach this line (they exit or back off above), so
            // the cadence replaces neither the early retry nor the generic error
            // backoff. The
            // wait is stop-interruptible, like the loop's other waits, so a
            // shutdown is never held up by it.
            int nextRoundDelay = PullNextRoundDelayMillis(result.Status, deliveredCount,
                PullIntervalMillis);
            if (nextRoundDelay > 0)
            {
                _stopEvent.Wait(TimeSpan.FromMilliseconds(nextRoundDelay));
            }
        }
    }

    /// <summary>
    /// Pull statistics for the POP path (the PopCallback.onSuccess convention).
    /// <para>
    /// The pull callback records RT every time, the POP callback only on
    /// <c>FOUND</c>, and before the emptiness check; TPS is read from
    /// <c>result.MsgFoundList</c>, which the POP loop has already replaced with
    /// the batch that survived the pre-delivery pipeline before calling this.
    /// Copy that asymmetry: PULL_RT is one round's own client-side trip time,
    /// so it is only booked for a round that came back <c>FOUND</c> -- a
    /// <c>POLLING_NOT_FOUND</c> / <c>NO_NEW_MSG</c> answer books neither RT nor
    /// TPS, and a <c>FOUND</c> answer whose rows were all filtered out books RT
    /// only.
    /// </para>
    /// <para>
    /// A missed record is a SILENT failure: messages pop and ack as usual,
    /// consumption is entirely normal, only the 307 response's
    /// <c>statusTable</c> (the ops dashboard) is all zeros -- and on the
    /// dashboard "this consumer is not pulling" and "this consumer never came
    /// up" are two completely different treatments. The criterion itself must
    /// be pinnable offline, so it sits in public for the same reason as
    /// <c>FlowControlHit</c> (the test project has no InternalsVisibleTo).
    /// </para>
    /// </summary>
    public static void RecordPopPullStats(ConsumerStatsManager stats, string group, string topic,
                                          PopResult result, long beganMs)
    {
        if (result.Status != PopStatus.Found)
        {
            return;
        }

        stats.IncPullRT(group, topic, UtilAll.CurrentTimeMillis() - beganMs);
        if (result.MsgFoundList.Count > 0)
        {
            stats.IncPullTPS(group, topic, result.MsgFoundList.Count);
        }
    }

    // ---------------- POP consume loop (5.x lightweight consumption) ----------------

    /// <summary>
    /// Per-queue POP loop (the popMessage callback convention).
    /// <para>Key differences from the pull loop:
    /// <list type="bullet">
    /// <item>does NOT query or commit consume offsets: progress is tracked by
    /// the broker-side checkpoint, confirmation relies only on ack;</item>
    /// <item>popped messages are handed to consume threads immediately and the
    /// loop continues at once (without waiting for the consume result);</item>
    /// <item>PollingNotFound (the queue temporarily has no messages) is a
    /// NORMAL state: straight to the next round, not an error.</item>
    /// </list>
    /// </para>
    /// </summary>
    private void QueuePopLoop(MessageQueue mq)
    {
        string key = OffsetKey(mq);
        long invisible = PopInvisibleTime;
        if (invisible < MinPopInvisibleTime || invisible > MaxPopInvisibleTime)
        {
            // Clamp: out of [5s, 300s] always falls back to 60s
            invisible = 60000;
        }

        // Pop requests default to ConsumeInitMode.MAX; mapped here by
        // consumeFromWhere so the "consume from head" semantics hold in POP
        // mode too.
        int initMode = _consumeFromWhere == RocketMQ.Remoting.Protocol.ConsumeFromWhere.ConsumeFromFirstOffset
            ? ConsumeInitModeMin
            : ConsumeInitModeMax;

        // The generic error backoff is taken exactly once for the life of this loop,
        // like the invisible time above: the pull and the POP loop share one knob, and
        // a knob changed after start() must not move a loop already running.
        int errorBackoffMillis = PullExceptionBackoffMillis;

        while (!_stop && _started)
        {
            bool owns;
            lock (_lock)
            {
                owns = !_dropped.Contains(key) && OwnsQueueLocked(key);
            }

            if (!owns)
            {
                UnregisterOwnLoop(key);
                return;
            }

            // Stamp on initiating the pop (before flow control);
            // PopProcessQueue.lastPopTimestamp is written together with the
            // pull timestamp table, and the stall criterion reads exactly it.
            StampPullAt(key, pop: true);

            // The POP loop likewise honors the suspend flag, with the same
            // 1000ms backoff (PULL_TIME_DELAY_MILLS_WHEN_SUSPEND).
            if (_paused)
            {
                if (_stopEvent.Wait(TimeSpan.FromMilliseconds(1000)) || _stop) return;
                continue;
            }
            PopProcessQueue? pq;
            SubscriptionData? sub;
            lock (_lock)
            {
                _popQueues.TryGetValue(key, out pq);
                _subscriptionData.TryGetValue(mq.Topic, out sub);
            }

            if (pq is null || pq.IsDropped() || sub is null) return;
            // Flow control: this queue has too many popped-but-unacked messages, so hold back
            // (PopFlowControlHit, against popThresholdForQueue). The trip is counted on the pull
            // path's flow-control total, so one throttled warn cadence spans both paths.
            if (PopFlowControlHit(mq, pq))
            {
                _stopEvent.Wait(TimeSpan.FromMilliseconds(CacheFlowControlBackoffMillis));
                continue;
            }

            long began = UtilAll.CurrentTimeMillis();
            PopResult result;
            try
            {
                result = Client().PopMessage(ConsumerGroup, mq.Topic, mq.QueueId, PopBatchNums,
                                             invisible, PopPollTimeMillis, initMode,
                                             string.IsNullOrEmpty(sub.SubString) ? "*" : sub.SubString,
                                             sub.ExpressionType, false, PopTimeoutMillis, mq.BrokerName);
            }
            catch (RemotingTimeoutException e)
            {
                // No messages during long-poll suspend → the client times out
                // first; normal behavior, straight to the next round
                ClientLog.Debug("pop long-poll timeout for " + key + " (benign): " + e.Message);
                continue;
            }
            catch (MQBrokerException e)
            {
                // Same split as the pull path, same one knob: a broker
                // FLOW_CONTROL(215) is a "busy right now" answer, so retry in 20 ms
                // instead of parking on the generic error backoff.
                ClientLog.Debug("pop broker error for " + key + ": " + e.Message);
                _stopEvent.Wait(TimeSpan.FromMilliseconds(
                    PullBackoffMillisForBrokerResponse(e.ResponseCode, errorBackoffMillis)));
                continue;
            }
            catch (Exception e)
            {
                ClientLog.Debug("pop error for " + key + ": " + e.Message);
                _stopEvent.Wait(TimeSpan.FromMilliseconds(errorBackoffMillis));
                continue;
            }

            // The queue was revoked by rebalance after the pop (or this thread
            // was swapped out by self-healing): this batch is NEITHER consumed
            // NOR acked (the PopProcessQueue.isDropped() branch); the broker
            // automatically revives and redelivers it to the new owner when
            // invisibleTime expires.
            bool discarded;
            lock (_lock)
            {
                discarded = _dropped.Contains(key) || !OwnsQueueLocked(key);
            }

            if (discarded || pq.IsDropped())
            {
                if (discarded)
                {
                    UnregisterOwnLoop(key);
                }

                ClientLog.Debug("queue " + key + " revoked during pop, discard "
                                + result.MsgFoundList.Count + " messages un-acked");
                return;
            }

            // Pre-delivery pipeline (processPopResult), then the round's
            // bookkeeping keys on what SURVIVES it:
            //   ① tag re-check ② IFilterMessageHook ③ POP-only reconsume
            // budget drop. EVERY dropped row is acked against the ORIGINAL
            // popped list, otherwise the broker revives and redelivers it when
            // invisibleTime expires -- showing up as "filtering did not take
            // effect".
            int keptCount = 0;
            if (result.Status == PopStatus.Found && result.MsgFoundList.Count > 0)
            {
                pq.IncFoundMsg(result.MsgFoundList.Count);
                List<MessageExt> kept = FilterMessagesForDelivery(mq, sub, result.MsgFoundList);
                kept = DropOverReconsumeBudget(kept);
                if (kept.Count != result.MsgFoundList.Count)
                {
                    List<MessageExt> dropped = DroppedMessages(result.MsgFoundList, kept);
                    Interlocked.Add(ref _filteredMessageCount, dropped.Count);
                    foreach (MessageExt msg in dropped)
                    {
                        AckPopMsg(msg);
                        pq.Ack();
                    }

                    ClientLog.Info("pop filter dropped " + dropped.Count + " of "
                        + result.MsgFoundList.Count + " messages (acked)");
                }

                // The answer the re-schedule and the stats see is the KEPT
                // batch, not the raw popped one.
                result.MsgFoundList = kept;
                keptCount = kept.Count;
                if (keptCount > 0)
                {
                    SubmitPopConsumeRequest(kept, pq, mq);
                }
            }

            // Pull statistics (the PopCallback.onSuccess convention), recorded
            // AFTER the pipeline: a Found answer always books its round-trip
            // time -- even when nothing is left to deliver -- and pull TPS
            // counts only the kept rows, so a batch fully filtered out moves
            // RT but not TPS.
            RecordPopPullStats(Client().ConsumerStats, ConsumerGroup, mq.Topic, result, began);

            // Re-schedule by answer (see PopNextRoundDelayMillis for the legs): the
            // POLLING_FULL / unknown-status leg parks on the same generic error
            // backoff the pull path uses, captured when this loop was set up.
            int nextRoundDelay = PopNextRoundDelayMillis(result.Status, keptCount,
                PullIntervalMillis, errorBackoffMillis);
            if (nextRoundDelay > 0)
            {
                _stopEvent.Wait(TimeSpan.FromMilliseconds(nextRoundDelay));
            }
            else if (keptCount == 0 && UtilAll.CurrentTimeMillis() - began < 200)
            {
                // Empty result: if the broker did not suspend per pollTime
                // (returns immediately) this becomes a hot loop; back off here
                // as a "round took too short" fallback to avoid hammering the
                // broker. The generic error-backoff leg for POLLING_FULL/unknown
                // already waited above and is never reached here.
                Thread.Sleep(200);
            }
        }
    }

    /// <summary>The unified entry for POP popped batches: orderly listeners
    /// divert into the <see cref="SubmitPopOrderlyRequest"/> skeleton while
    /// concurrent listeners go through the concurrent POP listener.
    /// Public for the test project (no InternalsVisibleTo).
    /// <para>Service selection by listener type: orderly listener + POP goes
    /// through the orderly POP listener, which is today an unfinished
    /// skeleton (POPTODO): after the dedup enqueue, run() returns as soon as
    /// it takes the queue lock -- messages are NEITHER consumed NOR acked, and
    /// the broker revives and redelivers them when invisibleTime expires.
    /// Keep this behavior; do NOT "fix" it. msgs is ignored on the orderly
    /// path (orderly consume requests carry only pq + mq).</para>
    /// <para>Concurrent branch: split into batches by
    /// ConsumeMessageBatchMaxSize then submit to the **core/max two-tier
    /// thread pool** (core=ConsumeThreadMin / max=ConsumeThreadMax, with an
    /// unbounded `LinkedBlockingQueue()`).</para></summary>
    public void SubmitPopConsumeRequest(List<MessageExt> msgs, PopProcessQueue pq, MessageQueue mq)
    {
        if (IsOrderly())
        {
            SubmitPopOrderlyRequest(pq, mq, force: false);
            return;
        }
        int size = Math.Max(1, _consumeMessageBatchMaxSize);
        ConsumeExecutor? exec = _popConsumeExecutor;
        for (int i = 0; i < msgs.Count; i += size)
        {
            List<MessageExt> batch = msgs.GetRange(i, Math.Min(size, msgs.Count - i));
            if (batch.Count == 0) continue;
            if (exec != null)
            {
                // Capture the local variable so the closure does not share the
                // same batch variable
                List<MessageExt> captured = batch;
                exec.Submit(() => ConsumePopBatch(captured, pq, mq));
            }
            else
            {
                // No executor when not Start()ed (unit tests): execute
                // synchronously, keeping the testable behavior consistent with
                // before the change.
                ConsumePopBatch(batch, pq, mq);
            }
        }
    }

    /// <summary>Submit one POP orderly request (submitConsumeRequest): with
    /// force=false, dedup by (pq, mq), duplicate requests are dropped
    /// directly; only new requests go to the executor. The per-key
    /// MessageQueueLock is folded into _lock: the critical section has only
    /// one set operation. Public for the test project.</summary>
    public void SubmitPopOrderlyRequest(PopProcessQueue pq, MessageQueue mq, bool force)
    {
        lock (_lock)
        {
            bool isNew = _popOrderlyRequests.Add((pq, OffsetKey(mq)));
            if (!force && !isNew)
            {
                return;
            }
        }
        ConsumeExecutor? exec = _popConsumeExecutor;
        try
        {
            if (exec != null)
            {
                exec.Submit(() => RunPopOrderlyRequest(pq, mq));
            }
            else
            {
                // Not Start()ed (unit tests): execute synchronously
                RunPopOrderlyRequest(pq, mq);
            }
        }
        catch (Exception e)
        {
            // A submit failure only logs; the request stays in the set (it is
            // not automatically resubmitted).
            ClientLog.Error("error submit pop orderly request: " + e.Message + ", mq: " + mq);
        }
    }

    /// <summary>The request's execution body -- the 5.5.0 skeleton stops here:
    /// only a revoked pq removes the request (the removeConsumeRequest log
    /// with the same wording); otherwise it takes the queue lock and does
    /// NOTHING -- no listener call, no ack.</summary>
    public void RunPopOrderlyRequest(PopProcessQueue pq, MessageQueue mq)
    {
        if (pq.IsDropped())
        {
            ClientLog.Warn("run, message queue not be able to consume, because it's dropped. "
                + mq);
            lock (_lock)
            {
                _popOrderlyRequests.Remove((pq, OffsetKey(mq)));
            }
        }
    }

    /// <summary>Request-set count (consumeRequestSet.size(); the dedup
    /// behavior must be pinnable offline).</summary>
    public int PopOrderlyRequestCount()
    {
        lock (_lock)
        {
            return _popOrderlyRequests.Count;
        }
    }

    /// <summary>Consume one POP batch and ack / extend the invisible time per
    /// the result (ConsumeRequest.run).</summary>
    private void ConsumePopBatch(List<MessageExt> msgs, PopProcessQueue pq, MessageQueue mq)
    {
        if (pq.IsDropped() || msgs.Count == 0) return;

        long popTime = 0;
        long invisible = 0;
        try
        {
            string? ck = msgs[0].GetProperty(MessageConst.PropertyPopCk);
            if (!string.IsNullOrEmpty(ck))
            {
                string[] seg = ExtraInfoUtil.Split(ck);
                popTime = ExtraInfoUtil.GetPopTime(seg);
                invisible = ExtraInfoUtil.GetInvisibleTime(seg);
            }
        }
        catch (Exception e)
        {
            ClientLog.Debug("parse pop ck failed: " + e.Message);
        }

        if (IsPopTimeout(popTime, invisible))
        {
            // invisibleTime has already passed: an ack would not be honored
            // either; abandon this batch directly (wait for the broker to
            // revive and redeliver)
            pq.DecFoundMsg(-msgs.Count);
            return;
        }

        // POP retry-topic prefix strip at the delivery entry, immediately
        // BEFORE the property-based restore below (concurrent POP dispatch
        // only: the consume-directly path and every orderly path must NOT
        // strip). Runs on the popped rows as they enter the listener.
        StripPopRetryTopicPrefix(msgs);
        ResetRetryTopicAndNamespace(msgs);
        var ctx = new ConsumeConcurrentlyContext(mq);

        // The POP path uses ackIndex as
        // "acked up to which message", semantically consistent with the
        // classic send-back path; the default Integer.MAX_VALUE already
        // means "ack the whole batch"; clamping to size-1 here only aligns
        // the value on ctx with this batch's count and does not affect
        // CONSUME_SUCCESS behavior (not acking lets messages be revived and
        // redelivered after invisibleTime).
        ctx.AckIndex = msgs.Count - 1;
        ConsumeMessageContext? popHookCtx = null;
        if (_consumeMessageHooks.Count > 0)
        {
            popHookCtx = BuildConsumeHookContext(msgs, mq);
            ExecuteConsumeHookBefore(popHookCtx);
        }

        long popBegin = UtilAll.CurrentTimeMillis();
        // The POP path likewise stamps BEFORE handing to the listener (POP has
        // no sweep schedule; this is only to keep listener/trace visibility
        // consistent)
        string popStamp = popBegin.ToString(CultureInfo.InvariantCulture);
        foreach (MessageExt m in msgs)
        {
            m.PutProperty(MessageConst.PropertyConsumeStartTimestamp, popStamp);
        }

        bool popHasException = false;
        ConsumeConcurrentlyStatus status = ConsumeConcurrentlyStatus.ReconsumeLater;
        try
        {
            status = ((IMessageListenerConcurrently)_messageListener!).ConsumeMessage(msgs, ctx);
        }
        catch (Exception e)
        {
            // A consume exception is treated as RECONSUME_LATER
            ClientLog.Debug("pop listener error, treat as RECONSUME_LATER: " + e.Message);
            popHasException = true;
        }

        // The hook's returnType verdict happens BEFORE "status is normalized
        // to RECONSUME_LATER" (a null return records RETURNNULL, not FAILED)
        RecordConsumeStats(mq.Topic, msgs.Count, popBegin,
            failed: status == ConsumeConcurrentlyStatus.ReconsumeLater);
        FinishConsumeHook(popHookCtx, true, popHasException, popBegin,
            ConsumeConcurrentlyStatusNames.Name(status),
            failed: status == ConsumeConcurrentlyStatus.ReconsumeLater,
            succeeded: status == ConsumeConcurrentlyStatus.ConsumeSuccess);

        if (pq.IsDropped() || IsPopTimeout(popTime, invisible))
        {
            // The queue was revoked or timed out during consumption: the
            // result is no longer processed
            pq.DecFoundMsg(-msgs.Count);
            return;
        }

        ProcessPopConsumeResult(status, ctx, msgs, pq);
    }

    /// <summary>Concurrent pull-delivery topic preparation, run on the popped
    /// rows before they are enqueued: cut this group's POP retry wrapper first,
    /// then restore the classic retry queue's topic from its RETRY_TOPIC
    /// property.
    ///
    /// <paramref name="orderly"/> suppresses only the wrapper cut. An orderly
    /// consumer consumes the queue it locked under the name that queue has, and a
    /// retry queue's offset bookkeeping is keyed on it; the property-based restore
    /// still runs, because it is what makes the business topic visible for both
    /// consume modes. Public for the test project.</summary>
    public void PreparePullRowsForDelivery(List<MessageExt> msgs, bool orderly)
    {
        if (!orderly)
        {
            StripPopRetryTopicPrefix(msgs);
        }

        ResetRetryTopicAndNamespace(msgs);
    }

    /// <summary>POP retry-topic prefix strip at the delivery entry: a row popped
    /// from this group's POP retry queue carries the business topic inside its
    /// own topic name, as either <c>%RETRY%&lt;group&gt;_&lt;topic&gt;</c> or
    /// <c>%RETRY%&lt;group&gt;+&lt;topic&gt;</c>; cut whichever wrapper names
    /// THIS consumer's group so the listener sees the business topic.
    ///
    /// Cutting only one spelling would break the ack rather than merely the
    /// display name: the ack target is rebuilt from the message's topic plus the
    /// POP_CK retry flag, and that flag describes the V1 <em>or</em> the V2 form,
    /// so a topic left wrapped is confirmed against a name built twice over
    /// instead of the queue this client actually popped it from.
    ///
    /// Everything else is left exactly as it arrived, which makes this a no-op on
    /// ordinary pull rows: a bare <c>%RETRY%&lt;group&gt;</c> (the classic retry
    /// queue -- its real topic is restored by
    /// <see cref="ResetRetryTopicAndNamespace"/>), a wrapper naming a different
    /// group, a wrapper with nothing after the separator, and any ordinary topic.
    /// The plus form is tried first: it is unambiguous about where the group name
    /// ends, so it wins when a business topic itself contains an underscore.
    ///
    /// Whatever remains after the cut keeps its own shape; the namespace prefix is
    /// stripped one step later by <see cref="ResetRetryTopicAndNamespace"/>, which
    /// every concurrent delivery entry runs immediately after this. The
    /// consume-directly path and every orderly path must NOT call this.</summary>
    public void StripPopRetryTopicPrefix(List<MessageExt> msgs)
    {
        if (msgs.Count == 0)
        {
            return;
        }

        string[] names;
        if (_namespace.Length == 0)
        {
            names = new[] { ConsumerGroup };
        }
        else
        {
            // The group may be registered with the namespace already folded in,
            // in which case the wire name carries it too; both spellings of this
            // consumer's name are recognised.
            string plain = NamespaceUtil.WithoutNamespace(ConsumerGroup, _namespace);
            names = string.IsNullOrEmpty(plain) || plain == ConsumerGroup
                ? new[] { ConsumerGroup }
                : new[] { ConsumerGroup, plain };
        }

        string[] prefixes = new string[names.Length * 2];
        int n = 0;
        foreach (string name in names)
        {
            prefixes[n++] = MixAll.RetryGroupTopicPrefix + name + "+";
            prefixes[n++] = MixAll.RetryGroupTopicPrefix + name + "_";
        }

        // Longest wrapper first, so the more specific spelling of the group wins.
        Array.Sort(prefixes, (a, b) => b.Length.CompareTo(a.Length));

        foreach (MessageExt m in msgs)
        {
            foreach (string prefix in prefixes)
            {
                if (!m.Topic.StartsWith(prefix, StringComparison.Ordinal))
                {
                    continue;
                }

                string rest = m.Topic.Substring(prefix.Length);
                // An empty tail (the wire name is only the wrapper) must not
                // erase the topic.
                if (rest.Length > 0)
                {
                    m.Topic = rest;
                }

                break;
            }
        }
    }

    /// <summary>Before consumption, restore retry messages' topic to the
    /// original business topic (resetRetryAndNamespace): the row's own topic
    /// only names the classic retry queue, the business topic lives in the
    /// RETRY_TOPIC property. Shared by the pull and POP delivery entries.
    ///
    /// The namespace is taken off EVERY row, not only off a restored one: the
    /// fetch happens under the wrapped topic name, so without this step the
    /// listener would branch on <c>ns%Topic</c> instead of the
    /// <c>Topic</c> it subscribed to.</summary>
    private void ResetRetryTopicAndNamespace(List<MessageExt> msgs)
    {
        string retryTopic = MixAll.GetRetryTopic(ConsumerGroup);
        foreach (MessageExt m in msgs)
        {
            if (m.Topic == retryTopic)
            {
                string? orig = m.GetProperty(MessageConst.PropertyRetryTopic);
                if (!string.IsNullOrEmpty(orig))
                {
                    m.Topic = orig!;
                }
            }

            if (_namespace.Length != 0)
            {
                m.Topic = NamespaceUtil.WithoutNamespace(m.Topic, _namespace);
            }
        }
    }

    /// <summary>isPopTimeout: when popTime/invisibleTime cannot be parsed, treat
    /// it as timed out.</summary>
    public static bool IsPopTimeout(long popTime, long invisible)
    {
        if (popTime <= 0 || invisible <= 0) return true;
        return UtilAll.CurrentTimeMillis() - popTime >= invisible;
    }

    /// <summary>processConsumeResult: handle a POP batch's consume result.</summary>
    private void ProcessPopConsumeResult(ConsumeConcurrentlyStatus status,
                                         ConsumeConcurrentlyContext ctx,
                                         List<MessageExt> msgs, PopProcessQueue pq)
    {
        int ackIndex = ctx.AckIndex;
        if (status == ConsumeConcurrentlyStatus.ConsumeSuccess)
        {
            if (ackIndex >= msgs.Count)
            {
                ackIndex = msgs.Count - 1;
            }
        }
        else
        {
            ackIndex = -1; // RECONSUME_LATER: ack none
        }

        for (int i = 0; i <= ackIndex; i++)
        {
            AckPopMsg(msgs[i]);
            pq.Ack();
        }

        for (int i = ackIndex + 1; i < msgs.Count; i++)
        {
            pq.Ack();
            MessageExt msg = msgs[i];
            // Exceeding the max retry count: go through CheckNeedAckOrDelay
            // (too old → ack and drop directly; otherwise pick a delay level
            // by the message's elapsed lifetime)
            if (_maxReconsumeTimes >= 0 && msg.ReconsumeTimes >= _maxReconsumeTimes)
            {
                CheckNeedAckOrDelay(msg);
                continue;
            }

            ChangePopInvisibleTime(msg, ctx.DelayLevelWhenNextConsume);
        }
    }

    /// <summary>checkNeedAckOrDelay: the fallback after retries are exhausted.
    /// Message lifetime already exceeding 2x the max delay level → ack and
    /// drop directly (no more infinite retries); otherwise pick a level by
    /// lifetime and keep extending the invisible time.</summary>
    private void CheckNeedAckOrDelay(MessageExt msg)
    {
        List<int> table = PopDelayLevel;
        long msgDelayTime = UtilAll.CurrentTimeMillis() - msg.BornTimestamp;
        if (msgDelayTime > (long)table[table.Count - 1] * 1000 * 2)
        {
            ClientLog.Warn("pop consume too many times, ack and drop: " + msg.MsgId);
            AckPopMsg(msg);
            return;
        }

        int level = table.Count - 1;
        for (; level >= 0; level--)
        {
            if (msgDelayTime >= (long)table[level] * 1000)
            {
                level++;
                break;
            }
        }

        // ⚠ Deliberate correction: when lifetime is below the first level the
        // naive algorithm computes level=-1 and indexes out of bounds
        // delayLevelTable[-1]. It is clamped to the first level here inside
        // ChangePopInvisibleTime.
        ChangePopInvisibleTime(msg, level);
    }

    /// <summary>
    /// Decode from POP_CK the target needed for ack / extending the invisible
    /// time (extraInfo must be reconstructed on the client side).
    /// <para>⚠ Neither spot can be taken for granted:
    /// <list type="number">
    /// <item>the topic must be restored with ExtraInfoUtil.GetRealTopic by the
    /// CK's retryFlag -- the real topic of a revived message (retryFlag=1) is
    /// %RETRY%&lt;group&gt;_&lt;topic&gt;, NOT the topic on the message;</item>
    /// <item>the address must be looked up by the brokerName in the CK, not by
    /// topic routes -- retry topics usually have no independent route entry,
    /// so a topic lookup fails (must go through
    /// findBrokerAddressInSubscribe).</item>
    /// </list>
    /// </para>
    /// </summary>
    public PopCkTarget? PopCkTarget(MessageExt msg)
    {
        string? ck = msg.GetProperty(MessageConst.PropertyPopCk);
        if (string.IsNullOrEmpty(ck))
        {
            ClientLog.Debug("pop message without POP_CK, cannot ack: " + msg.MsgId);
            return null;
        }

        var target = new PopCkTarget { ExtraInfo = ck! };
        try
        {
            string[] seg = ExtraInfoUtil.Split(target.ExtraInfo);
            target.BrokerName = ExtraInfoUtil.GetBrokerName(seg);
            target.QueueId = ExtraInfoUtil.GetQueueId(seg);
            target.Offset = ExtraInfoUtil.GetQueueOffset(seg);
            string retry = ExtraInfoUtil.GetRetry(seg);
            target.Topic = ExtraInfoUtil.GetRealTopic(msg.Topic, ConsumerGroup, retry);
        }
        catch (Exception e)
        {
            ClientLog.Debug("bad POP_CK " + target.ExtraInfo + ": " + e.Message);
            return null;
        }

        return target;
    }

    /// <summary>Single-message ack (ackAsync).</summary>
    private void AckPopMsg(MessageExt msg)
    {
        PopCkTarget? target = PopCkTarget(msg);
        if (target is null) return;
        try
        {
            Client().AckMessage(ConsumerGroup, target.Topic, target.QueueId, target.ExtraInfo,
                                target.Offset, 3000, target.BrokerName);
        }
        catch (Exception e)
        {
            // An ack failure is not fatal: the broker revives and redelivers
            // the message when invisibleTime expires
            ClientLog.Debug("ack failed for " + msg.MsgId + ": " + e.Message);
        }
    }

    /// <summary>Extend the invisible time (changePopInvisibleTime).
    /// delayLevel == 0 uses the message's retry count as the level; the level
    /// table is in SECONDS, the interface takes milliseconds.</summary>
    private void ChangePopInvisibleTime(MessageExt msg, int delayLevel)
    {
        PopCkTarget? target = PopCkTarget(msg);
        if (target is null) return;
        if (delayLevel == 0)
        {
            delayLevel = msg.ReconsumeTimes;
        }

        List<int> table = PopDelayLevel;
        int delaySecond = delayLevel >= table.Count
            ? table[table.Count - 1]
            : table[Math.Max(0, delayLevel)];
        try
        {
            Client().ChangeInvisibleTime(ConsumerGroup, target.Topic, target.QueueId,
                                         target.ExtraInfo, target.Offset,
                                         (long)delaySecond * 1000, 3000, target.BrokerName);
        }
        catch (Exception e)
        {
            ClientLog.Debug("change invisible time failed for " + msg.MsgId + ": " + e.Message);
        }
    }

    private void DispatchLoop()
    {
        while (!_stop)
        {
            bool progressed = false;
            List<string> keys;
            lock (_lock)
            {
                keys = new List<string>(_pending.Keys);
            }

            foreach (string key in keys)
            {
                if (_stop || !_started) return;
                MessageQueue mq;
                lock (_lock)
                {
                    if (!_mqMap.TryGetValue(key, out MessageQueue? m)) continue;
                    mq = m!;
                    // Revoked queue: discard the buffer and skip (no
                    // consumption, no offset advance).
                    if (_dropped.Contains(key))
                    {
                        _pending.Remove(key);
                        continue;
                    }
                }

                // The epoch and batch are taken in the SAME lock (taking the
                // messages and the isDropped verdict share one critical
                // section): a queue revoked/rebuilt after the take has all
                // its acks voided
                List<MessageExt> batch = TakeBatchForConsume(key, out long epoch);
                if (batch.Count == 0) continue;
                try
                {
                    bool done = ConsumeBatch(key, mq, batch, epoch);
                    progressed = progressed || done;
                }
                catch (Exception e)
                {
                    // Unexpected exception on the dispatch path: push the batch
                    // back to the queue head and retry later (do not let it
                    // kill the dispatch thread)
                    ClientLog.Warn("dispatch batch error (will retry): " + e.Message);
                    lock (_lock)
                    {
                        if (_pending.TryGetValue(key, out Queue<MessageExt>? q))
                        {
                            for (int i = batch.Count - 1; i >= 0; --i)
                            {
                                PushFront(q, batch[i]);
                            }
                        }
                    }

                    _stopEvent.Wait(TimeSpan.FromMilliseconds(100));
                }
                finally
                {
                    FinishBatchConsume(key, batch);
                }
            }

            if (!progressed)
            {
                _stopEvent.Wait(TimeSpan.FromMilliseconds(50));
            }
        }
    }

    private static void PushFront(Queue<MessageExt> q, MessageExt m)
    {
        // Queue<T> has no PushFront: rebuild via a temporary queue (batches
        // are small, the overhead is negligible)
        var tmp = new Queue<MessageExt>(q.Count + 1);
        tmp.Enqueue(m);
        while (q.Count > 0)
        {
            tmp.Enqueue(q.Dequeue());
        }

        while (tmp.Count > 0)
        {
            q.Enqueue(tmp.Dequeue());
        }
    }

    // ---------------- Unit tests / live debugging only: drive classic consume
    // dispatch directly without going through the network ----------------
    // The classic path's ackIndex semantics fail very quietly (the tail is
    // silently lost, the offset skips unconsumed messages), so they must be
    // pinned offline before going live -- hence these openings.
    public bool ConsumeBatchForTest(string key, MessageQueue mq, List<MessageExt> batch,
                                    long? epoch = null)
        => ConsumeBatch(key, mq, batch, epoch);

    /// <summary>The orderly send-back newMsg constructor (shared by both
    /// chains), for unit tests to inspect properties directly.</summary>
    public Message BuildRetryMessageForTest(MessageExt msg, int maxReconsumeTimes)
        => BuildRetryMessage(msg, maxReconsumeTimes);

    public static string OffsetKeyForTest(MessageQueue mq) => OffsetKey(mq);

    /// <summary>Preset a queue's "pulled but not consumed" buffer;
    /// null = revoke that queue.</summary>
    public void SetPendingForTest(string key, List<MessageExt>? msgs)
    {
        lock (_lock)
        {
            if (msgs is null)
            {
                _pending.Remove(key);
            }
            else
            {
                _pending[key] = new Queue<MessageExt>(msgs);
            }
        }
    }

    public List<MessageExt> PendingForTest(string key)
    {
        lock (_lock)
        {
            return _pending.TryGetValue(key, out Queue<MessageExt>? q) ? q.ToList() : new List<MessageExt>();
        }
    }

    /// <summary>The "registered" view = in-flight (dispatched, not settled) ∪
    /// pulled-but-not-dispatched, i.e. the entire contents of the queue buffer
    /// (msgTreeMap); used by unit tests / live verification to assert the
    /// registered state before and after the sweep.</summary>
    public List<MessageExt> ProcessQueueEntriesForTest(string key)
    {
        lock (_lock)
        {
            return ProcessQueueEntriesLocked(key);
        }
    }

    /// <summary>Unit-test entry: preset the "handed to the listener, not settled yet"
    /// rows of one queue -- the other half of the held set the cache gate measures (see
    /// <see cref="ProcessQueueEntriesForTest"/>). The dispatch itself fills this table, so
    /// writing it directly is how a gate case stages a backlog without a thread pool.</summary>
    public void SetInFlightForTest(string key, List<MessageExt>? msgs)
    {
        lock (_lock)
        {
            if (msgs is null or { Count: 0 })
            {
                _inFlightMsgs.Remove(key);
            }
            else
            {
                _inFlightMsgs[key] = new List<MessageExt>(msgs);
            }
        }
    }

    /// <summary>Unit-test entry: clean one queue's expired head, returning the
    /// number of send-backs initiated this round (see CleanExpiredQueue).</summary>
    public int CleanExpiredQueueForTest(string key) => CleanExpiredQueue(key);

    /// <summary>Unit-test entry: sweep the currently held queues once (see
    /// CleanExpiredMsgOnce).</summary>
    public int CleanExpiredMsgOnceForTest() => CleanExpiredMsgOnce();

    /// <summary>Unit-test entry: the sweep period in milliseconds
    /// (initialDelay == period, see CleanExpirePeriodMillis).</summary>
    public long CleanExpirePeriodMillisForTest() => CleanExpirePeriodMillis();

    /// <summary>Unit-test entry: remove a message from the "registered" view
    /// (simulating cleanExpiredMsg having sent it back and removed it), used
    /// to assert that send-back skips already-swept entries.</summary>
    public void RemoveProcessQueueEntryForTest(string key, MessageExt msg)
    {
        lock (_lock)
        {
            RemoveProcessQueueEntryLocked(key, msg);
        }
    }

    /// <summary>Unit-test entry: the expiry criterion (see IsConsumeExpired).
    /// now is given by the caller so the "exactly at the threshold, do
    /// nothing" boundary can be pinned deterministically (reading the clock
    /// internally would differ by a few milliseconds).</summary>
    public static bool IsConsumeExpiredForTest(long? stamp, long nowMs, int consumeTimeoutMinutes)
        => IsConsumeExpired(stamp, nowMs, consumeTimeoutMinutes);

    /// <summary>Unit-test entry: CONSUME_START_TIME parsing (see
    /// ConsumeStartTimestamp).</summary>
    public static long? ConsumeStartTimestampForTest(MessageExt msg) => ConsumeStartTimestamp(msg);

    /// <summary>Unit-test entry: "remove only if still the head" after a
    /// successful send-back (see RemoveExpiredEntryIfStillHead).</summary>
    public bool RemoveExpiredEntryIfStillHeadForTest(string key, MessageExt msg)
        => RemoveExpiredEntryIfStillHead(key, msg);

    /// <summary>Preset a queue's "assigned queue" registration. This table is the
    /// divisor of the topic-level budgets (see
    /// <see cref="DivideTopicThresholds"/>); not registering a queue equals the
    /// queue having been revoked by rebalance.</summary>
    public void SetAssignedForTest(string key, MessageQueue mq)
    {
        lock (_lock)
        {
            _mqMap[key] = mq;
        }
    }

    /// <summary>Unit-test entry: whether this key is still in the "assigned
    /// queue" registry (revoking a queue must remove it too).</summary>
    public bool MqMapContainsForTest(string key)
    {
        lock (_lock)
        {
            return _mqMap.ContainsKey(key);
        }
    }

    /// <summary>Run one pre-pull flow-control verdict (without the network).
    /// A hit increments <see cref="FlowControlTriggered"/> by one.</summary>
    public bool FlowControlHitForTest(MessageQueue mq, string key) => FlowControlHit(mq, key);

    /// <summary>Run one POP cache-gate verdict (without the network): this
    /// queue's own wait-ack debt against <see cref="PopThresholdForQueue"/>.
    /// A hit increments <see cref="FlowControlTriggered"/> by one.</summary>
    public bool PopFlowControlHitForTest(MessageQueue mq, PopProcessQueue pq)
        => PopFlowControlHit(mq, pq);

    /// <summary>Unit-test entry: divide the enabled topic-level budgets over the
    /// queues this consumer owns right now (see
    /// <see cref="DivideTopicThresholds"/>).</summary>
    public void DivideTopicThresholdsForTest() => DivideTopicThresholds();

    /// <summary>Unit-test entry: run one full rebalance round on the calling
    /// thread. The real round runs on the rebalance thread, so a test that wants
    /// "this exact round, now" needs this; it is the only way to pin the
    /// division's two gating rules (only a changed assignment, only after the
    /// pull state was rebuilt) without racing the 20s timer.</summary>
    public void DoRebalanceForTest() => DoRebalance();

    /// <summary>Unit-test entry: the subscription version this instance carries for a topic,
    /// which is what the rebalance-change re-stamp moves. A topic that is not subscribed
    /// reads -1.</summary>
    public long SubVersionForTest(string topic)
    {
        lock (_lock)
        {
            return _subscriptionData.TryGetValue(topic, out SubscriptionData? sub) && sub is not null
                ? sub.SubVersion : -1L;
        }
    }

    /// <summary>Unit-test entry: the queues this instance owns right now -- the share a
    /// rebalance-change notification hands the application listener.</summary>
    public List<MessageQueue> AssignedQueuesForTest() => AssignedQueues();

    // ---------------- postSubscriptionWhenPull / pullFromWhichNode ----------------

    /// <summary>
    /// Subscription gating: the subscription expression is sent with the
    /// request only when <see cref="PostSubscriptionWhenPull"/> is on AND the
    /// mode is not class-filter; default off, with tag filtering falling back
    /// to the client-side <c>FilterMessagesForDelivery</c>. An empty
    /// expression normalizes to <c>"*"</c> (the same convention as FilterAPI).
    /// </summary>
    private string? PullSubscriptionExpression(SubscriptionData sub)
    {
        if (!PostSubscriptionWhenPull || sub.ClassFilterMode)
        {
            return null;
        }

        return UtilAll.IsBlank(sub.SubString) ? "*" : sub.SubString;
    }

    /// <summary>
    /// updatePullFromWhichNode: write the response header's
    /// <c>suggestWhichBrokerId</c> back into the table; a missing value (old
    /// brokers do not send the field) counts as master=0 -- the convention is
    /// "missing means master", so null must NOT be treated as "keep the old
    /// value".
    /// </summary>
    private void UpdatePullFromWhichNode(string key, long? suggest)
    {
        lock (_lock)
        {
            _pullFromWhichNode[key] = suggest ?? MixAll.MasterId;
        }
    }

    /// <summary>recalculatePullFromWhichNode: no record counts as master=0.</summary>
    private long PullFromWhichNode(string key)
    {
        lock (_lock)
        {
            return _pullFromWhichNode.TryGetValue(key, out long brokerId) ? brokerId : MixAll.MasterId;
        }
    }

    /// <summary>Unit-test entry: the two gating/accounting segments in the
    /// pull loop (semantics can be pinned without a cluster).</summary>
    public string? PullSubscriptionExpressionForTest(SubscriptionData sub) =>
        PullSubscriptionExpression(sub);

    /// <summary>Unit-test entry: see <see cref="PullFromWhichNode"/>.</summary>
    public long PullFromWhichNodeForTest(string key) => PullFromWhichNode(key);

    /// <summary>Unit-test entry: see <see cref="UpdatePullFromWhichNode"/>.</summary>
    public void UpdatePullFromWhichNodeForTest(string key, long? suggest) =>
        UpdatePullFromWhichNode(key, suggest);

    /// <summary>Consumed offset; null = this queue has no record yet.</summary>
    public long? ConsumeOffsetForTest(string key)
    {
        lock (_lock)
        {
            return _consumeOffsetTable.TryGetValue(key, out long off) ? off : null;
        }
    }

    /// <summary>Consume one batch and handle send-back/suspend. Returns whether
    /// the consume offset advanced.
    /// <para>
    /// <paramref name="epoch"/>: the ProcessQueue epoch this batch belongs to
    /// (see <see cref="_queueEpoch"/>). When the queue is dropped while this
    /// batch is queued (rebalance revocation / OFFSET_ILLEGAL correction /
    /// stall self-healing), this batch can NEITHER be consumed NOR acked --
    /// the <c>isDropped()</c> short-circuit of both the concurrent and
    /// orderly chains.
    /// </para>
    /// </summary>
    private bool ConsumeBatch(string key, MessageQueue mq, List<MessageExt> batch,
                              long? epoch = null)
    {
        if (epoch.HasValue)
        {
            long nowEpoch;
            lock (_lock)
            {
                nowEpoch = QueueEpochLocked(key);
            }

            if (epoch.Value != nowEpoch)
            {
                ClientLog.Warn("the message queue not be able to consume, because it's dropped. group="
                    + ConsumerGroup + " mq=" + mq + " msgs="
                    + batch.Count.ToString(CultureInfo.InvariantCulture) + " epoch="
                    + epoch.Value.ToString(CultureInfo.InvariantCulture) + "->"
                    + nowEpoch.ToString(CultureInfo.InvariantCulture));
                return false;
            }
        }

        bool broadcast = _messageModel == RocketMQ.Remoting.Protocol.MessageModel.Broadcasting;
        // ---- Orderly consumption ----
        if (IsOrderly())
        {
            var orderly = (IMessageListenerOrderly)_messageListener!;
            var ctx = new ConsumeOrderlyContext(mq);
            ConsumeMessageContext? ohookCtx = null;
            if (_consumeMessageHooks.Count > 0)
            {
                ohookCtx = BuildConsumeHookContext(batch, mq);
                ExecuteConsumeHookBefore(ohookCtx);
            }

            long obegin = UtilAll.CurrentTimeMillis();
            bool ohasException = false;
            ConsumeOrderlyStatus status;
            try
            {
                status = orderly.ConsumeMessage(batch, ctx);
            }
            catch (Exception e)
            {
                // Orderly consumption: exception → do not commit the offset,
                // retry in place
                ClientLog.Debug("orderly listener error (retry in place): " + e.Message);
                status = ConsumeOrderlyStatus.SuspendCurrentQueueAMoment;
                ohasException = true;
            }

            // The orderly consume hook likewise fires after status is
            // obtained: status is the normalized enum name, the success
            // criterion is SUCCESS||COMMIT, while returnType (CONSUME_CONTEXT_TYPE
            // in props) is computed from the pre-normalization status.
            if (status == ConsumeOrderlyStatus.Rollback ||
                status == ConsumeOrderlyStatus.SuspendCurrentQueueAMoment)
            {
                // Log one warn when returning "not OK" (including the
                // exception-normalized suspend). This log line is the only
                // clue to orderly consumption's head-of-line blocking;
                // without it one only sees "this group is not moving".
                ClientLog.Warn("consumeMessage Orderly return not OK, Group: " + ConsumerGroup +
                    " Msgs: " + batch.Count + " MQ: " + key);
            }

            FinishConsumeHook(ohookCtx, true, ohasException, obegin,
                ConsumeOrderlyStatusNames.Name(status),
                failed: status == ConsumeOrderlyStatus.SuspendCurrentQueueAMoment,
                succeeded: status == ConsumeOrderlyStatus.Success ||
                           status == ConsumeOrderlyStatus.Commit);

            if (ctx.AutoCommit)
            {
                // ---- processConsumeResult: the AutoCommit=true branch ----
                if (status == ConsumeOrderlyStatus.Commit || status == ConsumeOrderlyStatus.Rollback)
                {
                    // Commit/Rollback with AutoCommit=true is an ILLEGAL usage
                    // (for binlog consumption only): warn only, write NO
                    // break, falling through into the Success branch -- the
                    // messages are acked as usual and not treated as rollback.
                    ClientLog.Warn("the message queue consume result is illegal, we think you want " +
                        "to ack these message " + key);
                    status = ConsumeOrderlyStatus.Success;
                }

                if (status == ConsumeOrderlyStatus.SuspendCurrentQueueAMoment)
                {
                    RecordConsumeStats(mq.Topic, batch.Count, obegin, failed: true);
                    // Before suspending, checkReconsumeTimes must run first:
                    // only "still within retries / send-back failed" pushes
                    // this batch back to the queue head to retry in place;
                    // those already handed to the broker (send-back succeeded)
                    // must advance the offset, otherwise one poison message
                    // permanently occupies this queue -- which looks exactly
                    // like "the consumer died".
                    if (CheckOrderlyReconsumeTimes(batch))
                    {
                        RequeuePending(key, batch);
                        _stopEvent.Wait(TimeSpan.FromMilliseconds(OrderlySuspendMillis(ctx)));
                        return false;
                    }
                }
                else
                {
                    RecordConsumeStats(mq.Topic, batch.Count, obegin, failed: false);
                }

                AdvanceConsumeOffset(key, batch, null, epoch);
                Interlocked.Add(ref _consumedCount, batch.Count);
                return true;
            }

            // ---- AutoCommit=false (binlog consumption: commit authority is in
            // the listener's hands) ----
            if (status == ConsumeOrderlyStatus.Commit)
            {
                // Explicit commit: the offset advances, NO TPS recorded (RT is
                // recorded outside the branch as usual)
                RecordConsumeRt(mq.Topic, obegin);
                AdvanceConsumeOffset(key, batch, null, epoch);
                Interlocked.Add(ref _consumedCount, batch.Count);
                return true;
            }

            if (status == ConsumeOrderlyStatus.Rollback)
            {
                // rollback() returns the messages to the queue buffer and
                // delays the retry
                RecordConsumeRt(mq.Topic, obegin);
                RequeuePending(key, batch);
                _stopEvent.Wait(TimeSpan.FromMilliseconds(OrderlySuspendMillis(ctx)));
                return false;
            }

            if (status == ConsumeOrderlyStatus.SuspendCurrentQueueAMoment)
            {
                RecordConsumeStats(mq.Topic, batch.Count, obegin, failed: true);
                if (CheckOrderlyReconsumeTimes(batch))
                {
                    RequeuePending(key, batch);
                    _stopEvent.Wait(TimeSpan.FromMilliseconds(OrderlySuspendMillis(ctx)));
                }

                // The difference from the auto-commit branch: after the poison
                // message is handed to the broker there is NO commit; whether
                // the offset advances is the binlog consumer's own call.
                return false;
            }

            // Success + AutoCommit=false: record OK TPS only. The batch leaves
            // the queue buffer, no commit, no rollback and no re-delivery: the
            // commit authority is in the listener's hands, so whether the
            // offset advances is its own call.
            RecordConsumeStats(mq.Topic, batch.Count, obegin, failed: false);
            return true;
        }

        // ---- Concurrent consumption (ConsumeRequest.run) ----
        var conc = (IMessageListenerConcurrently)_messageListener!;
        var cctx = new ConsumeConcurrentlyContext(mq);
        // The before hook runs BEFORE the listener (generating the SubBefore
        // trace), after runs after status is obtained (generating SubAfter
        // with contextCode)
        ConsumeMessageContext? hookCtx = null;
        if (_consumeMessageHooks.Count > 0)
        {
            hookCtx = BuildConsumeHookContext(batch, mq);
            ExecuteConsumeHookBefore(hookCtx);
        }

        long beginMs = UtilAll.CurrentTimeMillis();
        // Stamp CONSUME_START_TIME on each message BEFORE handing to the
        // listener: the sweep's expiry criterion reads it (empty string
        // short-circuits, unstamped messages are not swept), and stamping too
        // late means the sweep never sees in-flight batches
        string stampText = beginMs.ToString(CultureInfo.InvariantCulture);
        foreach (MessageExt m in batch)
        {
            m.PutProperty(MessageConst.PropertyConsumeStartTimestamp, stampText);
        }

        bool hasException = false;
        ConsumeConcurrentlyStatus cstatus;
        try
        {
            cstatus = conc.ConsumeMessage(batch, cctx);
        }
        catch (Exception e)
        {
            // A consume exception is treated as RECONSUME_LATER
            ClientLog.Debug("listener error, treat as RECONSUME_LATER: " + e.Message);
            cstatus = ConsumeConcurrentlyStatus.ReconsumeLater;
            hasException = true;
        }

        // CONSUME_SUCCESS uses the ackIndex set by the listener to partition
        // the "acknowledged prefix / to-be-sent-back suffix" (default
        // Integer.MAX_VALUE, clamped to size-1 means the whole batch is
        // acknowledged); RECONSUME_LATER forces ackIndex=-1, sending the
        // whole batch back.
        int ackIndex = cctx.AckIndex;
        if (cstatus == ConsumeConcurrentlyStatus.ConsumeSuccess)
        {
            if (ackIndex >= batch.Count)
            {
                ackIndex = batch.Count - 1;
            }
        }
        else
        {
            ackIndex = -1;
        }

        int acked = ackIndex + 1;  // 0..batch.Count
        RecordConsumeStats(mq.Topic, batch.Count, beginMs,
            failed: cstatus == ConsumeConcurrentlyStatus.ReconsumeLater, ackCount: acked);
        FinishConsumeHook(hookCtx, true, hasException, beginMs,
            ConsumeConcurrentlyStatusNames.Name(cstatus),
            failed: cstatus == ConsumeConcurrentlyStatus.ReconsumeLater,
            succeeded: cstatus == ConsumeConcurrentlyStatus.ConsumeSuccess);

        if (broadcast)
        {
            // Broadcasting mode does not send back: the unacknowledged tail is
            // dropped with only one warn, and the whole batch's offset
            // advances as usual (no redelivery after restart)
            int dropped = batch.Count - acked;
            if (dropped > 0)
            {
                ClientLog.Warn("BROADCASTING, the message consume failed, drop it: "
                    + dropped.ToString(CultureInfo.InvariantCulture) + " msgs in " + mq);
            }

            AdvanceConsumeOffset(key, batch, null, epoch);
            Interlocked.Add(ref _consumedCount, batch.Count);
            return true;
        }

        if (acked >= batch.Count)
        {
            // Whole batch acknowledged (the default path): no send-back at
            // all, the offset advances directly
            AdvanceConsumeOffset(key, batch, null, epoch);
            Interlocked.Add(ref _consumedCount, batch.Count);
            return true;
        }

        // Clustering mode: the unacknowledged [acked, size) are sent back one
        // by one to the %RETRY% topic (delay level 3+reconsumeTimes; the
        // broker moves to %DLQ% once maxReconsumeTimes is exceeded)
        List<(int Index, MessageExt Msg)> msgBackFailed = SendBackBatch(batch, cctx, acked);
        var failedIdx = new HashSet<int>();
        long floor = 0;
        bool hasFloor = false;
        foreach ((int index, MessageExt _) in msgBackFailed)
        {
            failedIdx.Add(index);
            long off = batch[index].QueueOffset;
            if (!hasFloor || off < floor)
            {
                floor = off;
                hasFloor = true;
            }
        }

        // The failed send-back ones are pushed back to the queue head for a
        // later retry (they are not removed from the buffer)
        if (msgBackFailed.Count > 0)
        {
            lock (_lock)
            {
                if (_pending.TryGetValue(key, out Queue<MessageExt>? q))
                {
                    for (int i = msgBackFailed.Count - 1; i >= 0; --i)
                    {
                        PushFront(q, msgBackFailed[i].Msg);
                    }
                }
            }

            _stopEvent.Wait(TimeSpan.FromMilliseconds(200));
        }

        // What is committed is "the largest queueOffset + 1 among this
        // batch's handled entries", and it must not skip past the failed
        // send-backs still in the buffer, otherwise those would be silently
        // skipped by the offset (message loss)
        var handled = new List<MessageExt>(batch.Count - failedIdx.Count);
        for (int i = 0; i < batch.Count; ++i)
        {
            if (!failedIdx.Contains(i))
            {
                handled.Add(batch[i]);
            }
        }

        AdvanceConsumeOffset(key, handled, hasFloor ? floor : null, epoch);
        Interlocked.Add(ref _consumedCount, handled.Count);
        return msgBackFailed.Count == 0;
    }

    /// <summary>
    /// Send batch[base, size) back to the broker one by one starting from
    /// <paramref name="base"/> (processConsumeResult → sendMessageBack).
    /// <paramref name="base"/> is the tail's start index within the whole
    /// batch (on partial ack the prefix is already acknowledged and must not
    /// be sent back again). Returns the FAILED send-backs'
    /// (batch index, message); failed entries' ReconsumeTimes has already
    /// been +1 (the broker did not record that count, and without the client
    /// making it up they would never reach %DLQ%); the caller accordingly
    /// pushes the tail back to the queue head and clamps the offset.
    /// </summary>
    private List<(int Index, MessageExt Msg)> SendBackBatch(List<MessageExt> batch,
        ConsumeConcurrentlyContext ctx, int @base)
    {
        var failed = new List<(int, MessageExt)>();
        string key = OffsetKey(ctx.MessageQueue);
        for (int i = @base; i < batch.Count; ++i)
        {
            MessageExt msg = batch[i];
            // Entries already swept by cleanExpiredMsg (or whose queue was
            // revoked) SKIP the send-back: they are already on their way back
            // to the broker, and sending again would be a duplicate message
            lock (_lock)
            {
                if (!ProcessQueueContainsLocked(key, msg))
                {
                    ClientLog.Info("Message is not found in its process queue; skip send-back-procedure, topic="
                        + msg.Topic + ", brokerName=" + msg.BrokerName + ", queueId="
                        + msg.QueueId.ToString(CultureInfo.InvariantCulture) + ", queueOffset="
                        + msg.QueueOffset.ToString(CultureInfo.InvariantCulture));
                    continue;
                }
            }

            try
            {
                // delayLevelWhenNextConsume == 0 → 3 + reconsumeTimes
                // (reconsumeTimes is field 13 of the MessageExt wire format;
                // the broker increments it on redelivery)
                int delayLevel = ctx.DelayLevelWhenNextConsume;
                if (delayLevel == 0)
                {
                    delayLevel = 3 + msg.ReconsumeTimes;
                }

                SendMessageBack(msg, delayLevel);
            }
            catch (Exception e)
            {
                ClientLog.Debug("send message back failed for msg " + msg.MsgId + ": " + e.Message);
                // The count is added on the COPY TO BE RECONSUMED; the broker
                // did not record it successfully
                msg.ReconsumeTimes += 1;
                failed.Add((i, msg));
            }
        }

        return failed;
    }

    // ---------------- cleanExpiredMsg (the escape hatch for suspending
    // listeners) ----------------
    //
    // The sweep schedule and the cleanExpiredMsg loop body. Three semantics
    // that "fail very quietly" are pinned here:
    //   1. ONLY the head (the smallest offset) is looked at: if the head is
    //      not expired the whole queue is left alone (direct break);
    //   2. STRICTLY greater than consumeTimeout: exactly at the threshold is
    //      not touched; unstamped messages are not considered expired;
    //   3. A single round's cap is 16, and loop is computed once before the
    //      loop -- written loosely it would clear the whole queue in one go,
    //      scrambling the offset/retry rhythm entirely.
    // Only the "verdict" and "keep in place after send-back failure" branches
    // can be pinned offline (a not-started consumer's send-back always
    // fails); the live chain (suspend → sweep → %RETRY% redelivery arrives)
    // is in examples/RocketMQ.Examples/LiveCleanExpiredMsg.cs.

    /// <summary>The sweep's expiry criterion: STRICTLY greater than the
    /// threshold; no stamp (null) is not considered expired (the empty-string
    /// criterion short-circuits).</summary>
    private static bool IsConsumeExpired(long? stamp, long nowMs, int consumeTimeoutMinutes) =>
        stamp.HasValue && nowMs - stamp.Value > (long)consumeTimeoutMinutes * 60_000L;

    /// <summary>Consume-start stamp parsing: empty string / bad value always
    /// count as "never stamped".</summary>
    private static long? ConsumeStartTimestamp(MessageExt msg)
    {
        string? raw = msg.GetProperty(MessageConst.PropertyConsumeStartTimestamp);
        if (string.IsNullOrEmpty(raw))
        {
            return null;
        }

        return long.TryParse(raw, NumberStyles.Integer, CultureInfo.InvariantCulture, out long v) ? v : null;
    }

    /// <summary>scheduleAtFixedRate(cleanExpireMsg, consumeTimeout,
    /// consumeTimeout, MINUTES): initialDelay and period share the same value,
    /// both consumeTimeout minutes. 0/negative values clamp to 1 minute to
    /// avoid a busy wait.</summary>
    private long CleanExpirePeriodMillis() => Math.Max(1, ConsumeTimeout) * 60_000L;

    /// <summary>The "registered" view = in-flight (dispatched, not settled) ∪
    /// pulled-but-not-dispatched, i.e. the queue buffer msgTreeMap. Must be
    /// called while holding the lock.
    ///
    /// Dedup is by <b>queue offset</b>, not by object identity: a batch that was handed to the
    /// listener and a row still sitting in the buffer describe the same cached message when they
    /// carry the same offset, and the gate must charge it once. Because the dispatch window is
    /// ordered, the in-flight batch carries the LOWER offsets of the two parts.</summary>
    private List<MessageExt> ProcessQueueEntriesLocked(string key)
    {
        var outList = new List<MessageExt>();
        var seen = new HashSet<long>();
        if (_inFlightMsgs.TryGetValue(key, out List<MessageExt>? inflight))
        {
            foreach (MessageExt m in inflight)
            {
                if (seen.Add(m.QueueOffset))
                {
                    outList.Add(m);
                }
            }
        }

        if (_pending.TryGetValue(key, out Queue<MessageExt>? q))
        {
            foreach (MessageExt m in q)
            {
                if (seen.Add(m.QueueOffset))
                {
                    outList.Add(m);
                }
            }
        }

        return outList;
    }

    /// <summary>The head = the entry with the smallest queueOffset
    /// (firstEntry()). Must be called while holding the lock.</summary>
    private MessageExt? ProcessQueueFirstEntryLocked(string key)
    {
        MessageExt? head = null;
        foreach (MessageExt m in ProcessQueueEntriesLocked(key))
        {
            if (head is null || m.QueueOffset < head.QueueOffset)
            {
                head = m;
            }
        }

        return head;
    }

    /// <summary>containsMessage: is this message still attached to the queue?
    /// Entries swept by cleanExpiredMsg do not count (they are already on
    /// their way back to the broker); the send-back must skip them.</summary>
    private bool ProcessQueueContainsLocked(string key, MessageExt msg)
    {
        if (_inFlightMsgs.TryGetValue(key, out List<MessageExt>? inflight)
            && inflight.Exists(m => ReferenceEquals(m, msg)))
        {
            return true;
        }

        return _pending.TryGetValue(key, out Queue<MessageExt>? q)
            && q.Contains(msg);
    }

    /// <summary>Remove the just-swept entry (the caller has confirmed it is
    /// the head). Must be called while holding the lock.</summary>
    private void RemoveProcessQueueEntryLocked(string key, MessageExt msg)
    {
        if (_inFlightMsgs.TryGetValue(key, out List<MessageExt>? inflight))
        {
            inflight.RemoveAll(m => ReferenceEquals(m, msg));
            if (inflight.Count == 0)
            {
                _inFlightMsgs.Remove(key);
            }
        }

        // Queue<T> has no index-based removal: rebuild (batches are small, the
        // overhead is negligible)
        if (_pending.TryGetValue(key, out Queue<MessageExt>? q) && q.Count > 0)
        {
            var kept = new Queue<MessageExt>(q.Count);
            bool removed = false;
            foreach (MessageExt m in q)
            {
                if (!removed && ReferenceEquals(m, msg))
                {
                    removed = true;
                    continue;
                }

                kept.Enqueue(m);
            }

            if (removed)
            {
                _pending[key] = kept;
            }
        }
    }

    /// <summary>After a successful send-back, remove only when it is STILL the
    /// head (the smallest offset is still it): if the listener happens to
    /// finish normally during this window (or a smaller offset appears ahead
    /// of it), yield to the normal path; do not grab.</summary>
    private bool RemoveExpiredEntryIfStillHead(string key, MessageExt msg)
    {
        lock (_lock)
        {
            MessageExt? head = ProcessQueueFirstEntryLocked(key);
            if (head is null || head.QueueOffset != msg.QueueOffset)
            {
                return false;
            }

            RemoveProcessQueueEntryLocked(key, msg);
            return true;
        }
    }

    /// <summary>Clean one queue (the cleanExpiredMsg loop body), returning the
    /// number of send-backs actually initiated this round (unit-test evidence
    /// for "only the head moves, failures retry the same one, single-round cap
    /// 16").</summary>
    private int CleanExpiredQueue(string key)
    {
        // Orderly consumption has no such path (messages are retried in place
        // anyway; sending back would reorder them)
        if (IsOrderly())
        {
            return 0;
        }

        int loop;
        lock (_lock)
        {
            // loop is computed once BEFORE the loop (min(size, 16))
            loop = Math.Min(ProcessQueueEntriesLocked(key).Count, 16);
        }

        int attempts = 0;
        for (int i = 0; i < loop; ++i)
        {
            MessageExt? head;
            lock (_lock)
            {
                head = ProcessQueueFirstEntryLocked(key);
            }

            if (head is null)
            {
                break;   // Queue already empty: end this round directly
            }

            // Unstamped messages count as not expired; the expiry criterion is
            // STRICTLY greater
            if (!IsConsumeExpired(ConsumeStartTimestamp(head), UtilAll.CurrentTimeMillis(), ConsumeTimeout))
            {
                break;   // Head not expired, the rest are even less likely
            }

            attempts++;
            try
            {
                // sendMessageBack(head, 3): fixed delayLevel 3
                SendMessageBack(head, 3);
            }
            catch (Exception e)
            {
                // A send-back failure only logs: the message stays in place,
                // retried in the next round (or the next iteration of this
                // round); NEVER remove it (removing it really loses it)
                ClientLog.Error("send expired msg exception: " + e.Message);
                continue;
            }

            ClientLog.Info("send expire msg back. topic=" + head.Topic + ", msgId=" + head.MsgId
                + ", storeHost=" + head.StoreHost + ", queueId="
                + head.QueueId.ToString(CultureInfo.InvariantCulture) + ", queueOffset="
                + head.QueueOffset.ToString(CultureInfo.InvariantCulture));
            RemoveExpiredEntryIfStillHead(key, head);
        }

        return attempts;
    }

    /// <summary>cleanExpireMsg(): walk the currently held queues and sweep
    /// them one by one. An exception from any one queue aborts the whole
    /// round (the scheduler shell's catch contains it, the next round runs as
    /// usual).</summary>
    private int CleanExpiredMsgOnce()
    {
        List<string> keys;
        lock (_lock)
        {
            keys = new List<string>(_mqMap.Keys);
        }

        int attempts = 0;
        foreach (string key in keys)
        {
            attempts += CleanExpiredQueue(key);
        }

        return attempts;
    }

    /// <summary>The cleanExpiredMsg scheduling loop (scheduleAtFixedRate:
    /// initialDelay == period).</summary>
    private void CleanExpiredMsgLoop()
    {
        long period = CleanExpirePeriodMillis();
        long next = Environment.TickCount64 + period;
        while (!_stop)
        {
            if (!_started)
            {
                return;
            }

            Schedules.WaitUntil(_stopEvent, next);
            next += period;
            try
            {
                CleanExpiredMsgOnce();
            }
            catch (Exception e)
            {
                // The whole round aborts but scheduling continues (the next
                // round goes on)
                ClientLog.Error("cleanExpireMsg error: " + e.Message);
            }
        }
    }

    // ---------------- Orderly consumption's retry counting and send-back ----------------
    // Orderly consumption's getMaxReconsumeTimes / checkReconsumeTimes
    // / sendMessageBack are NOT the same chain as the concurrent set above;
    // both differences must be held:
    //   1. `-1` means "unlimited" (Integer.MAX_VALUE) on the orderly side,
    //      while 16 only on the concurrent side;
    //   2. the orderly side's send-back is a NORMAL MESSAGE SEND (to
    //      %RETRY%&lt;group&gt;), not ConsumerSendMsgBack(3).

    /// <summary>
    /// The orderly side's max reconsume times.
    ///
    /// Orderly-consumed messages always stop in the local queue and retry in
    /// place, and the broker side has no redelivery count at all, so the
    /// default should be to keep retrying until success -- <c>-1</c> is read
    /// as <c>int.MaxValue</c> here. The concurrent consumption's
    /// <c>-1 -> 16</c> (this client's concurrent-side default)
    /// is another set of semantics: every round of send-back there goes
    /// through the broker, and 16 is the broker's default retryMaxTimes.
    /// Merging the two into one constant would either conjure dead letters
    /// for orderly consumption out of thin air or let concurrent messages
    /// retry infinitely.
    /// </summary>
    public int OrderlyMaxReconsumeTimes() =>
        _maxReconsumeTimes == -1 ? int.MaxValue : _maxReconsumeTimes;

    /// <summary>The concurrent side's <c>-1</c> is read as the broker default
    /// 16 (not the same chain as the orderly side).</summary>
    public int MaxReconsumeTimesOrDefault() => _maxReconsumeTimes == -1 ? 16 : _maxReconsumeTimes;

    /// <summary>
    /// This port's to-be-consumed buffer is the <c>_pending</c> queue, and
    /// batches are popped from the queue head BEFORE dispatch to the
    /// listener; the pull cursor has already advanced to nextBeginOffset at
    /// the moment of pulling, so "the offset not advancing" does NOT make the
    /// broker send these few again -- to retry them in place they must be
    /// explicitly put back (classic chain messages always stay in the queue
    /// buffer, needing no such step). Equivalent to
    /// <c>makeMessageToConsumeAgain</c>/<c>rollback</c>.
    /// </summary>
    private void RequeuePending(string key, List<MessageExt> batch)
    {
        lock (_lock)
        {
            if (_pending.TryGetValue(key, out Queue<MessageExt>? q))
            {
                for (int i = batch.Count - 1; i >= 0; --i)
                {
                    PushFront(q, batch[i]);
                }
            }
        }
    }

    /// <summary>
    /// Record RT only (incConsumeRT is OUTSIDE processConsumeResult and
    /// always recorded): orderly consumption records no TPS at all on the
    /// three AutoCommit=false branches Success/Commit/Rollback, but RT is
    /// recorded as usual.
    /// </summary>
    private void RecordConsumeRt(string topic, long beginMs)
    {
        if (_mqClient is null)
        {
            return;
        }

        _mqClient.ConsumerStats.IncConsumeRT(ConsumerGroup, topic,
            UtilAll.CurrentTimeMillis() - beginMs);
    }

    /// <summary>
    /// Orderly consumption's suspend duration (submitConsumeRequestLater):
    /// the context's value takes priority, -1 falls back to the consumer
    /// config, then clamps to [10, 30000]. A listener passing 0 still waits
    /// 10ms (otherwise a suspend-returning listener turns the consume thread
    /// into a busy wait), and passing 1 hour only waits 30s.
    /// </summary>
    public int OrderlySuspendMillis(ConsumeOrderlyContext ctx)
    {
        int ms = ctx.SuspendCurrentQueueTimeMillis;
        if (ms == -1)
        {
            ms = _suspendCurrentQueueTimeMillis;
        }

        if (ms < 10)
        {
            return 10;
        }

        return ms > 30000 ? 30000 : ms;
    }

    /// <summary>
    /// checkReconsumeTimes.
    /// Returns "whether this batch should still suspend and retry in place".
    /// Two paths per message:
    ///   - Retries not exhausted: local <c>ReconsumeTimes + 1</c> (the broker
    ///     did not record this failure at all, and without the client making
    ///     it up the threshold would never be reached), keep suspending;
    ///   - Retries exhausted: hand to the broker for send-back. ON SUCCESSFUL
    ///     SEND-BACK, STOP SUSPENDING (commit the offset at this point, the
    ///     poison message yields and the queue moves on); only on send-back
    ///     failure +1 and suspend.
    /// </summary>
    public bool CheckOrderlyReconsumeTimes(List<MessageExt>? msgs)
    {
        bool suspend = false;
        int maxTimes = OrderlyMaxReconsumeTimes();
        if (msgs is null)
        {
            return false;
        }

        foreach (MessageExt msg in msgs)
        {
            if (msg.ReconsumeTimes >= maxTimes)
            {
                if (!OrderlySendMessageBack(msg))
                {
                    suspend = true;
                    msg.ReconsumeTimes += 1;
                }
            }
            else
            {
                suspend = true;
                msg.ReconsumeTimes += 1;
            }
        }

        return suspend;
    }

    /// <summary>
    /// sendMessageBack (the orderly chain).
    ///
    /// Take the instance's built-in internal producer and send this message
    /// AS A NORMAL MESSAGE to <c>%RETRY%&lt;group&gt;</c>: the broker's
    /// retry-and-DLQ interception sees the
    /// group still holds an unexpired queue lock (exactly the orderly
    /// consumption group's characteristic) and redirects it straight to
    /// <c>%DLQ%&lt;group&gt;</c>. Among them <c>RECONSUME_TIME</c> /
    /// <c>MAX_RECONSUME_TIMES</c> get lifted into the request header on the
    /// send side (<c>MQClientInstance.BuildSendRequest</c> lifts them into the
    /// request header on the send side, as the sync send path does).
    ///
    /// Failure only returns false, never throws: the whole segment is wrapped
    /// in try/catch; throwing to the consume thread means this message is
    /// neither acked nor sent back, and one can only wait for the lock to
    /// time out -- and the orderly consumption lock timeout is minutes-level,
    /// looking just like a hang.
    /// </summary>
    private bool OrderlySendMessageBack(MessageExt msg)
    {
        try
        {
            MQClientInstance c = Client();
            Message newMsg = BuildRetryMessage(msg, OrderlyMaxReconsumeTimes());
            TopicPublishInfo? publish = c.GetTopicPublishInfo(newMsg.Topic, isDefault: true);
            if (publish is null || publish.MsgQueueList.Count == 0)
            {
                ClientLog.Debug("orderly send back has no writable queue, topic=" + newMsg.Topic);
                return false;
            }

            // unitMode follows the consumer: building the internal producer
            // must copy the consumer config over; without it
            // the sent-back message would lose its unit mark.
            c.SendMessage(MixAll.ClientInnerProducerGroup, newMsg, publish.SelectOneMessageQueue(),
                3000, unitMode: _unitMode);
            return true;
        }
        catch (Exception e)
        {
            ClientLog.Debug("orderly send message back failed, group=" + ConsumerGroup
                            + " msg=" + msg.MsgId + ": " + e.Message);
            return false;
        }
    }

    /// <summary>
    /// Every retry copy this consumer builds (the concurrent send-back fallback and the
    /// orderly chain) comes from the one shared builder, so the two paths' property
    /// setting cannot drift apart.
    /// </summary>
    private Message BuildRetryMessage(MessageExt msg, int maxReconsumeTimes)
        => RetryMessageFactory.Build(ConsumerGroup, msg, maxReconsumeTimes);

    /// <summary>
    /// Advance the offset to the largest queueOffset+1 in the batch; when
    /// <paramref name="floor"/> is non-null do not skip past it (see
    /// removeMessage: when unconsumed messages remain in the tree the
    /// committed offset can only be firstKey, otherwise that one would be
    /// silently dropped). An empty batch returns directly.
    /// <para>
    /// <paramref name="epoch"/> is the queue buffer's epoch when this batch
    /// was taken: a mismatch with the current epoch means the queue has been
    /// revoked/rebuilt, and per isDropped-like gates the WHOLE BATCH'S acks
    /// are voided. What is in <see cref="_frozenOffsets"/> is the offset
    /// frozen by OFFSET_ILLEGAL correction, which likewise must not be
    /// changed.
    /// </para>
    /// </summary>
    private void AdvanceConsumeOffset(string key, List<MessageExt> batch, long? floor = null,
                                      long? epoch = null)
    {
        lock (_lock)
        {
            if (epoch.HasValue && epoch.Value != QueueEpochLocked(key))
            {
                ClientLog.Debug("drop ack for " + key + ": process queue was dropped (epoch "
                    + epoch.Value.ToString(CultureInfo.InvariantCulture) + " -> "
                    + QueueEpochLocked(key).ToString(CultureInfo.InvariantCulture) + ")");
                return;
            }

            if (_frozenOffsets.Contains(key))
            {
                return;
            }

            if (batch.Count == 0)
            {
                // When the whole batch's send-backs fail no entry is
                // acknowledged and the offset stays put
                return;
            }

            long nextOffset = 0;
            foreach (MessageExt m in batch)
            {
                if (m.QueueOffset + 1 > nextOffset)
                {
                    nextOffset = m.QueueOffset + 1;
                }
            }

            if (floor.HasValue && floor.Value < nextOffset)
            {
                nextOffset = floor.Value;
            }

            _consumeOffsetTable.TryGetValue(key, out long cur);
            if (cur < nextOffset)
            {
                _consumeOffsetTable[key] = nextOffset;
            }
        }
    }

    /// <summary>
    /// correctTagsOffset (the call site of the empty-response path).
    /// The caller must hold <c>_lock</c>.
    /// <para>
    /// When the pull response is <c>NoNewMsg</c> (the queue really has no
    /// messages) or <c>NoMatchedMsg</c> (the broker went a full round by the
    /// subscription expression and not one matched: the response carries
    /// <c>PULL_RETRY_IMMEDIATELY</c>, which this client maps to
    /// NoMatchedMsg), this queue's "consumed offset" must follow the pull cursor,
    /// otherwise it DEADLOCKS PERMANENTLY: messages nobody acks belong to
    /// nobody (those filtered out on the broker side are not in the response,
    /// and those removed by the client's second-pass tag filter are
    /// explicitly not acked), and with the offset stuck in place it stays
    /// there forever, rescanning this unwanted batch from the beginning after
    /// redelivery/restart.
    /// </para>
    /// <para>
    /// The update goes through the offset store, in its forward-only
    /// form (never backward), with one gate: the queue's own
    /// pending-message count must be 0. That count tracks
    /// messages STILL IN the process queue -- the concurrent consumption's
    /// per-batch removal only runs once the listener returns, so
    /// the removal doubles as the settle point: in-flight batches
    /// count too; raising the offset before they settle means this batch is
    /// silently skipped on process crash. This port combines <c>_pending</c>
    /// being empty and <c>_inFlight</c> being 0 into the same criterion, the
    /// latter's registration and "take the batch" happen in the same lock as
    /// <see cref="TakeBatchForConsume"/>.
    /// </para>
    /// <para>
    /// The only deliberate deviation is folding the status criterion into
    /// this function too (the classic approach does it in the callback's
    /// switch), so an offline unit test can pin both "which statuses to
    /// correct" and "when correction is allowed" in one go.
    /// </para>
    /// </summary>
    private void CorrectTagsOffsetLocked(string key, PullStatus status, long nextOffset)
    {
        if (status != PullStatus.NoNewMsg && status != PullStatus.NoMatchedMsg)
        {
            return;
        }

        if (_frozenOffsets.Contains(key))
        {
            // The offset is already frozen by OFFSET_ILLEGAL correction
            // (ControllableOffset.update discards the update outright when
            // allowToUpdate=false); this path likewise must not change it
            return;
        }

        if (_pending.TryGetValue(key, out Queue<MessageExt>? q) && q.Count > 0)
        {
            return;
        }

        if (_inFlight.TryGetValue(key, out int inFlight) && inFlight > 0)
        {
            return;
        }

        // When the table has no record for this,
        // putIfAbsent(mq, new ControllableOffset(offset)) -- create the entry
        // unconditionally, even when offset == 0. Using TryGetValue's default
        // 0 would conflate "no record" with "record is 0": an empty queue's
        // (nextBeginOffset == 0) first correction becomes a silent no-op and
        // the broker can never find the record.
        if (!_consumeOffsetTable.TryGetValue(key, out long cur) || cur < nextOffset)
        {
            _consumeOffsetTable[key] = nextOffset;
        }
    }

    /// <summary>
    /// The in-lock action for the pull response OFFSET_ILLEGAL
    /// (<c>updateAndFreezeOffset</c>): the offset is UNCONDITIONALLY written
    /// to this correction value and frozen
    /// (ControllableOffset.allowToUpdate=false). Writing it per
    /// <see cref="CorrectTagsOffsetLocked"/>'s increaseOnly would make the
    /// "broker winding the offset back" kind of correction never take
    /// effect. The caller must hold <c>_lock</c>.
    /// </summary>
    private void FreezeOffsetForIllegalLocked(string key, long nextOffset)
    {
        _frozenOffsets.Add(key);
        _consumeOffsetTable[key] = nextOffset;
    }

    /// <summary>Unit-test entry: see
    /// <see cref="FreezeOffsetForIllegalLocked"/> (lock wrapper).</summary>
    public void FreezeOffsetForIllegalForTest(string key, long nextOffset)
    {
        lock (_lock)
        {
            FreezeOffsetForIllegalLocked(key, nextOffset);
        }
    }

    /// <summary>
    /// The OFFSET_ILLEGAL branch.
    /// <para>
    /// The broker says "the offset you requested is illegal" (the queue was
    /// truncated, the commitlog expired, or it was reset server-side; see
    /// the broker replying <c>PULL_OFFSET_MOVED</c> uniformly
    /// for OFFSET_OVERFLOW_BADLY / OFFSET_TOO_SMALL / OFFSET_RESET, mapped by
    /// this client to this status) and puts the
    /// correction value in <c>nextBeginOffset</c>. The correction is not just
    /// moving the cursor over: all messages on this queue ALREADY FETCHED but
    /// not yet consumed/acked are VOIDED (they fall in the skipped range, and
    /// acking them would push the offset back to an illegal value), then the
    /// queue is rebuilt at the corrected offset.
    /// </para>
    /// <para>
    /// Four steps: <c>setNextOffset</c> → <c>setDropped(true)</c> →
    /// <c>{ updateAndFreezeOffset; persist; removeProcessQueue }</c> →
    /// <c>rebalanceImmediately</c>. This port: set the offset + freeze (in the
    /// callback's critical section, equivalent to updateAndFreezeOffset) →
    /// revoke the queue (<see cref="RetireQueueLocked"/> clears
    /// buffers/thread tables and EPOCH +1, equivalent to setDropped) → push
    /// the corrected offset to the broker outside the lock (equivalent to
    /// persist) → wake rebalance to rebuild.
    /// </para>
    /// <para>Must be called outside the lock: network RPC happens inside.</para>
    /// </summary>
    private void OffsetIllegalRecover(string key, MessageQueue mq)
    {
        var retired = new List<RetiredQueue>();
        lock (_lock)
        {
            RetireQueueLocked(key, mq, retired);
        }

        // persist: write the corrected offset back to the broker immediately
        // (an explicit persist, not waiting for periodic flushing); orderly
        // consumption must also unlock (removeProcessQueue →
        // removeUnnecessaryMessageQueue)
        bool broadcast = _messageModel == RocketMQ.Remoting.Protocol.MessageModel.Broadcasting;
        if (!broadcast && _mqClient is not null)
        {
            foreach (RetiredQueue r in retired)
            {
                if (r.HadOffset)
                {
                    try
                    {
                        _mqClient.UpdateConsumerOffset(ConsumerGroup, r.Mq, r.ConsumeOffset);
                    }
                    catch (Exception e)
                    {
                        ClientLog.Debug("persist offset on offset-illegal recover failed for "
                            + r.Mq + ": " + e.Message);
                    }
                }

                if (IsOrderly() && !PopMode)
                {
                    try
                    {
                        _mqClient.UnlockBatchMq(ConsumerGroup, _clientId, new List<MessageQueue> { r.Mq });
                    }
                    catch (Exception e)
                    {
                        ClientLog.Debug("unlock on offset-illegal recover failed for " + r.Mq + ": " + e.Message);
                    }
                }
            }
        }

        _rebalanceNow.Set();
        ClientLog.Warn("the pull request offset illegal, fix it, queue=" + key);
    }

    /// <summary>Unit-test entry: see <see cref="OffsetIllegalRecover"/> (no
    /// cluster, the network segments are skipped automatically).</summary>
    public void OffsetIllegalRecoverForTest(string key, MessageQueue mq) =>
        OffsetIllegalRecover(key, mq);

    /// <summary>
    /// "Take a batch + register in-flight": this is exactly what
    /// DispatchLoop's production path uses (taking and registering in the
    /// same lock). Missing the registration = <see cref="CorrectTagsOffsetLocked"/>'s
    /// gate cannot see in-flight batches, silently skipping messages on crash
    /// recovery; missing the <see cref="FinishBatchConsume"/> call = the gate
    /// stays closed forever and correctTagsOffset never takes effect. So
    /// these two are open to unit tests for the same reason as
    /// <c>ConsumeBatchForTest</c>.
    /// </summary>
    public List<MessageExt> TakeBatchForConsume(string key) => TakeBatchForConsume(key, out _);

    /// <summary>See <see cref="TakeBatchForConsume(string)"/>; the
    /// <paramref name="epoch"/> is this batch's queue buffer's epoch, taken
    /// together with the batch in the SAME lock (taking the messages and the
    /// isDropped verdict share the consume request's critical section).</summary>
    public List<MessageExt> TakeBatchForConsume(string key, out long epoch)
    {
        List<MessageExt> batch = new();
        lock (_lock)
        {
            epoch = QueueEpochLocked(key);
            if (_pending.TryGetValue(key, out Queue<MessageExt>? q) && q.Count > 0)
            {
                int n = Math.Min(q.Count, Math.Max(1, _consumeMessageBatchMaxSize));
                for (int i = 0; i < n; ++i)
                {
                    batch.Add(q.Dequeue());
                }
            }

            if (batch.Count > 0)
            {
                _inFlight.TryGetValue(key, out int cur);
                _inFlight[key] = cur + 1;
                // The in-flight bodies and the count are registered in the
                // same lock: cleanExpiredMsg's registered view (msgTreeMap)
                // can see it from this moment on
                if (!_inFlightMsgs.TryGetValue(key, out List<MessageExt>? bucket))
                {
                    bucket = new List<MessageExt>();
                    _inFlightMsgs[key] = bucket;
                }

                bucket.AddRange(batch);
            }
        }

        return batch;
    }

    /// <summary>This queue's current ProcessQueue epoch (no record counts as
    /// 0). Must be called while holding the lock.</summary>
    private long QueueEpochLocked(string key) =>
        _queueEpoch.TryGetValue(key, out long epoch) ? epoch : 0;

    /// <summary>"Consume epilogue + unregister in-flight": the exception
    /// requeue path must call it too, otherwise the gate stays closed
    /// forever. When <paramref name="batch"/> is given, the in-flight bodies
    /// (<see cref="_inFlightMsgs"/>) are removed along with it; entries
    /// already removed early by cleanExpiredMsg are idempotent here.</summary>
    public void FinishBatchConsume(string key, List<MessageExt>? batch = null)
    {
        lock (_lock)
        {
            if (_inFlight.TryGetValue(key, out int cur))
            {
                if (cur <= 1)
                {
                    _inFlight.Remove(key);
                }
                else
                {
                    _inFlight[key] = cur - 1;
                }
            }

            if (batch is not null && _inFlightMsgs.TryGetValue(key, out List<MessageExt>? bucket))
            {
                // Reference equality (MessageExt does not override Equals):
                // the batch and the registration are the same objects
                var gone = new HashSet<MessageExt>(batch);
                bucket.RemoveAll(m => gone.Contains(m));
                if (bucket.Count == 0)
                {
                    _inFlightMsgs.Remove(key);
                }
            }
        }
    }

    /// <summary>For unit tests to drive <see cref="CorrectTagsOffsetLocked"/>
    /// directly (lock wrapper).</summary>
    public void CorrectTagsOffsetForTest(string key, PullStatus status, long nextOffset)
    {
        lock (_lock)
        {
            CorrectTagsOffsetLocked(key, status, nextOffset);
        }
    }

    /// <summary>In-flight batch count (the "in the listener's hands" part of
    /// msgCount), for unit tests to observe registration/unregistration.</summary>
    public int InFlightForTest(string key)
    {
        lock (_lock)
        {
            return _inFlight.TryGetValue(key, out int cur) ? cur : 0;
        }
    }

    /// <summary>This queue's current ProcessQueue epoch (revocation/rebuild
    /// adds 1), see <see cref="_queueEpoch"/>.</summary>
    public long QueueEpochForTest(string key)
    {
        lock (_lock)
        {
            return QueueEpochLocked(key);
        }
    }

    /// <summary>Unit-test entry: set a queue's epoch to the given value
    /// directly (the real path is RetireQueueLocked +1).</summary>
    public void SetQueueEpochForTest(string key, long epoch)
    {
        lock (_lock)
        {
            _queueEpoch[key] = epoch;
        }
    }

    /// <summary>Unit-test entry: see <see cref="AdvanceConsumeOffset"/> (lock
    /// wrapper).</summary>
    public void AdvanceConsumeOffsetForTest(string key, List<MessageExt> batch,
                                            long? floor = null, long? epoch = null)
    {
        AdvanceConsumeOffset(key, batch, floor, epoch);
    }

    /// <summary>Whether this queue's offset is in the OFFSET_ILLEGAL
    /// correction freeze period, see <see cref="_frozenOffsets"/>.</summary>
    public bool OffsetFrozenForTest(string key)
    {
        lock (_lock)
        {
            return _frozenOffsets.Contains(key);
        }
    }

    // ---------------- Offset persistence ----------------
    private void OffsetPersistLoop()
    {
        // scheduleAtFixedRate(persistAllConsumerOffset, 1000 * 10,
        // persistConsumerOffsetInterval) -- the first task is delayed 10s,
        // after which the period takes persistConsumerOffsetInterval
        // (default 5s). The first flush happens AT the initialDelay MOMENT,
        // not one period after initialDelay + it; the period is a FIXED RATE:
        // the planned time is anchored at initialDelay + n*period (see
        // Schedules), so unlike "finish, then sleep one period in 100ms
        // slices" the period does not drag longer and longer.
        long next = Environment.TickCount64 + 10_000;
        while (!_stop)
        {
            if (!_started) return;
            Schedules.WaitUntil(_stopEvent, next);
            next += _persistConsumerOffsetIntervalMillis;
            try
            {
                PersistOffsetsOnce();
            }
            catch (Exception e)
            {
                ClientLog.Debug("persist offsets error: " + e.Message);
            }
        }
    }

    private void PersistOffsetsOnce()
    {
        if (_messageModel == RocketMQ.Remoting.Protocol.MessageModel.Broadcasting)
        {
            SaveLocalOffsets();
            return;
        }

        if (_mqClient is null)
        {
            return;
        }

        List<KeyValuePair<string, long>> items;
        lock (_lock)
        {
            items = new List<KeyValuePair<string, long>>(_consumeOffsetTable);
        }

        foreach (KeyValuePair<string, long> kv in items)
        {
            if (!_mqMap.TryGetValue(kv.Key, out MessageQueue? mq) || mq is null)
            {
                continue;
            }

            try
            {
                _mqClient.UpdateConsumerOffset(ConsumerGroup, mq, kv.Value);
            }
            catch (Exception e)
            {
                ClientLog.Debug("update consumer offset failed for " + mq + ": " + e.Message);
            }
        }
    }

    private string LocalOffsetPath() => BuildLocalOffsetPath(_clientId, ConsumerGroup);

    /// <summary>The one on-disk offset file every message-model-aware consumer mode
    /// shares: $HOME/.rocketmq_offsets/&lt;clientId&gt;/&lt;group&gt;/offsets.json.</summary>
    public static string BuildLocalOffsetPath(string clientId, string group)
    {
        string home = UtilAll.UserHome();
        if (home.Length == 0) home = ".";
        return home + "/.rocketmq_offsets/" + (clientId.Length == 0 ? "DEFAULT" : clientId)
            + "/" + group + "/offsets.json";
    }

    /// <param name="extra">
    /// Queue offsets just revoked and no longer in
    /// <c>_consumeOffsetTable</c>/<c>_mqMap</c>: <c>persist(mq)</c> writes the
    /// file first, <c>removeOffset(mq)</c> removes the in-memory entry after;
    /// the file's value must stay so the rebuilt queue can continue from it
    /// (see the broadcasting branch of <see cref="OnQueuesRevoked"/>).
    /// </param>
    private void SaveLocalOffsets(List<RetiredQueue>? extra = null)
    {
        Dictionary<string, long> items;
        Dictionary<string, MessageQueue> mqMap;
        lock (_lock)
        {
            items = new Dictionary<string, long>(_consumeOffsetTable, StringComparer.Ordinal);
            mqMap = new Dictionary<string, MessageQueue>(_mqMap, StringComparer.Ordinal);
        }

        foreach (RetiredQueue r in extra ?? new List<RetiredQueue>())
        {
            if (!r.HadOffset)
            {
                continue;
            }

            items[r.Key] = r.ConsumeOffset;
            if (!mqMap.ContainsKey(r.Key))
            {
                mqMap[r.Key] = r.Mq;
            }
        }

        SaveLocalOffsetsAt(LocalOffsetPath(), items, mqMap);
    }

    // The test project has no InternalsVisibleTo relationship with this
    // assembly, hence public; not for business code.
    public static void SaveLocalOffsetsAt(string path, Dictionary<string, long> items,
        Dictionary<string, MessageQueue> mqMap)
    {
        string? dir = Path.GetDirectoryName(path);
        if (!string.IsNullOrEmpty(dir))
        {
            Directory.CreateDirectory(dir);
        }

        // persistAll semantics: read the existing table, overlay only the
        // queues being flushed, write the union. A queue that is not part of
        // this flush (revoked, or owned by an earlier run) keeps its entry,
        // so a re-added queue resumes from where it stopped instead of from
        // the tail.
        var merged = new Dictionary<string, long>(items, StringComparer.Ordinal);
        var mergedMap = new Dictionary<string, MessageQueue>(mqMap, StringComparer.Ordinal);
        foreach (KeyValuePair<string, (MessageQueue Mq, long Offset)> kv
                 in ReadLocalOffsetTable(path))
        {
            if (!merged.ContainsKey(kv.Key))
            {
                merged[kv.Key] = kv.Value.Offset;
                mergedMap[kv.Key] = kv.Value.Mq;
            }
        }

        // string2File: the old content rolls to .bak before writing the new
        // file
        try
        {
            string prev = File.ReadAllText(path);
            if (prev.Length > 0)
            {
                File.WriteAllText(path + ".bak", prev);
            }
        }
        catch (IOException)
        {
            // The main file does not exist yet: first write, no previous
            // generation to roll
        }

        string tmp = path + ".tmp";
        File.WriteAllText(tmp, BuildLocalOffsetsJson(merged, mergedMap));
        File.Move(tmp, path, true);
    }

    // The on-disk format persistAll writes via fastjson (measured from real
    // on-disk files):
    // {"offsetTable":{{"brokerName":"broker-a","queueId":1,"topic":"Tt"}:9,...}}
    // -- the MessageQueue object is used as a JSON key directly. Entries with
    // no queue info are skipped (persistAll(mqs) skips them too).
    // The test project has no InternalsVisibleTo relationship with this
    // assembly, hence public; not for business code.
    public static string BuildLocalOffsetsJson(Dictionary<string, long> items,
        Dictionary<string, MessageQueue> mqMap)
    {
        var sb = new StringBuilder("{\"offsetTable\":{");
        bool first = true;
        foreach (KeyValuePair<string, long> kv in items)
        {
            if (!mqMap.TryGetValue(kv.Key, out MessageQueue? mq) || mq is null)
            {
                continue;
            }

            if (!first)
            {
                sb.Append(',');
            }

            first = false;
            sb.Append("{\"brokerName\":").Append(Json.Dump(JsonValue.MakeString(mq.BrokerName)))
                .Append(",\"queueId\":").Append(mq.QueueId.ToString(CultureInfo.InvariantCulture))
                .Append(",\"topic\":").Append(Json.Dump(JsonValue.MakeString(mq.Topic)))
                .Append("}:").Append(kv.Value.ToString(CultureInfo.InvariantCulture));
        }

        sb.Append("}}");
        return sb.ToString();
    }

    private Dictionary<string, long> LoadLocalOffsets()
    {
        return LoadLocalOffsetsAt(LocalOffsetPath());
    }

    // Fall back to .bak when the main file is missing/corrupted
    // (readLocalOffset → readLocalOffsetBak); return empty when both fail.
    // The test project has no InternalsVisibleTo relationship with this
    // assembly, hence public; not for business code.
    public static Dictionary<string, long> LoadLocalOffsetsAt(string path)
    {
        var outMap = new Dictionary<string, long>(StringComparer.Ordinal);
        foreach (string candidate in new[] { path, path + ".bak" })
        {
            string text;
            try
            {
                text = File.ReadAllText(candidate);
            }
            catch (IOException)
            {
                continue;
            }

            Dictionary<string, long>? parsed = ParseLocalOffsetsText(text);
            if (parsed is not null)
            {
                return parsed;
            }
        }

        return outMap;
    }

    // Main file first, .bak only when the main file is missing/corrupted.
    private static Dictionary<string, (MessageQueue Mq, long Offset)> ReadLocalOffsetTable(string path)
    {
        var outMap = new Dictionary<string, (MessageQueue Mq, long Offset)>(StringComparer.Ordinal);
        foreach (string candidate in new[] { path, path + ".bak" })
        {
            string text;
            try
            {
                text = File.ReadAllText(candidate);
            }
            catch (IOException)
            {
                continue;
            }

            List<(MessageQueue Mq, long Offset)>? parsed = ScanOffsetEntries(text.Trim());
            if (parsed is null)
            {
                continue;
            }

            foreach ((MessageQueue mq, long offset) in parsed)
            {
                outMap[QueueKey(mq)] = (mq, offset);
            }

            return outMap;
        }

        return outMap;
    }

    private static string QueueKey(MessageQueue mq) =>
        mq.Topic + mq.BrokerName + mq.QueueId.ToString(CultureInfo.InvariantCulture);

    // Strict JSON parsing first (the flat map an older version wrote);
    // the object-as-key format necessarily fails strict parsing (including
    // the pretty version), falling into ScanOffsetEntries.
    // The test project has no InternalsVisibleTo relationship with this
    // assembly, hence public; not for business code.
    public static Dictionary<string, long>? ParseLocalOffsetsText(string text)
    {
        string trimmed = text.Trim();
        if (trimmed.Length == 0)
        {
            return null;
        }

        if (Json.TryParse(trimmed, out JsonValue root, out _) && root is not null)
        {
            var flat = new Dictionary<string, long>(StringComparer.Ordinal);
            bool allNumbers = true;
            foreach (KeyValuePair<string, JsonValue> kv in root.ObjectItems())
            {
                if (!kv.Value.IsNumber)
                {
                    allNumbers = false;
                    break;
                }

                flat[kv.Key] = kv.Value.IntValue();
            }

            if (allNumbers)
            {
                return flat;
            }
        }

        List<(MessageQueue Mq, long Offset)>? entries = ScanOffsetEntries(trimmed);
        if (entries is null)
        {
            return null;
        }

        var outMap = new Dictionary<string, long>(StringComparer.Ordinal);
        foreach ((MessageQueue mq, long offset) in entries)
        {
            outMap[QueueKey(mq)] = offset;
        }

        return outMap;
    }

    private static List<(MessageQueue Mq, long Offset)>? ScanOffsetEntries(string text)
    {
        int pos = 0;
        if (!Eat(text, ref pos, '{'))
        {
            return null;
        }

        if (ScanString(text, ref pos) != "offsetTable")
        {
            return null;
        }

        if (!Eat(text, ref pos, ':') || !Eat(text, ref pos, '{'))
        {
            return null;
        }

        var entries = new List<(MessageQueue Mq, long Offset)>();
        while (true)
        {
            if (Eat(text, ref pos, '}'))
            {
                break;
            }

            if (!Eat(text, ref pos, '{'))
            {
                return null;
            }

            string? topic = null;
            string? broker = null;
            long? queueId = null;
            while (true)
            {
                if (Eat(text, ref pos, '}'))
                {
                    break;
                }

                string? fkey = ScanString(text, ref pos);
                if (fkey is null || !Eat(text, ref pos, ':'))
                {
                    return null;
                }

                if (Peek(text, pos) == '"')
                {
                    string? v = ScanString(text, ref pos);
                    if (v is null)
                    {
                        return null;
                    }

                    if (fkey == "topic")
                    {
                        topic = v;
                    }
                    else if (fkey == "brokerName")
                    {
                        broker = v;
                    }
                }
                else
                {
                    if (!ScanLong(text, ref pos, out long v))
                    {
                        return null;
                    }

                    if (fkey == "queueId")
                    {
                        queueId = v;
                    }
                }

                if (Eat(text, ref pos, ','))
                {
                    continue;
                }

                if (Eat(text, ref pos, '}'))
                {
                    break;
                }

                return null;
            }

            if (!Eat(text, ref pos, ':') || topic is null || broker is null || queueId is null
                || !ScanLong(text, ref pos, out long off))
            {
                return null;
            }

            entries.Add((new MessageQueue(topic, broker, (int)queueId.Value), off));
            if (Eat(text, ref pos, ','))
            {
                continue;
            }

            if (Eat(text, ref pos, '}'))
            {
                break;
            }

            return null;
        }

        // After the table closes there is still the wrapper's own }
        // ({"offsetTable":{...}} two layers of braces)
        if (!Eat(text, ref pos, '}'))
        {
            return null;
        }

        while (pos < text.Length && char.IsWhiteSpace(text[pos]))
        {
            pos++;
        }

        return pos == text.Length ? entries : null;
    }

    private static bool Eat(string s, ref int pos, char c)
    {
        SkipWs(s, ref pos);
        if (pos < s.Length && s[pos] == c)
        {
            pos++;
            return true;
        }

        return false;
    }

    private static char Peek(string s, int pos)
    {
        SkipWs(s, ref pos);
        return pos < s.Length ? s[pos] : '\0';
    }

    private static void SkipWs(string s, ref int pos)
    {
        while (pos < s.Length && char.IsWhiteSpace(s[pos]))
        {
            pos++;
        }
    }

    private static string? ScanString(string s, ref int pos)
    {
        SkipWs(s, ref pos);
        if (pos >= s.Length || s[pos] != '"')
        {
            return null;
        }

        pos++;
        var sb = new StringBuilder();
        while (pos < s.Length)
        {
            char c = s[pos++];
            if (c == '"')
            {
                return sb.ToString();
            }

            if (c != '\\')
            {
                sb.Append(c);
                continue;
            }

            if (pos >= s.Length)
            {
                return null;
            }

            char e = s[pos++];
            switch (e)
            {
                case '"': sb.Append('"'); break;
                case '\\': sb.Append('\\'); break;
                case '/': sb.Append('/'); break;
                case 'b': sb.Append('\b'); break;
                case 'f': sb.Append('\f'); break;
                case 'n': sb.Append('\n'); break;
                case 'r': sb.Append('\r'); break;
                case 't': sb.Append('\t'); break;
                case 'u':
                    if (pos + 4 > s.Length
                        || !int.TryParse(s.Substring(pos, 4), NumberStyles.HexNumber,
                            CultureInfo.InvariantCulture, out int cp))
                    {
                        return null;
                    }

                    sb.Append((char)cp);
                    pos += 4;
                    break;
                default:
                    return null;
            }
        }

        return null;
    }

    private static bool ScanLong(string s, ref int pos, out long value)
    {
        value = 0;
        SkipWs(s, ref pos);
        int start = pos;
        if (pos < s.Length && s[pos] == '-')
        {
            pos++;
        }

        int digits = pos;
        while (pos < s.Length && s[pos] >= '0' && s[pos] <= '9')
        {
            pos++;
        }

        if (pos == digits)
        {
            return false;
        }

        return long.TryParse(s[start..pos], NumberStyles.Integer, CultureInfo.InvariantCulture,
            out value);
    }

    // ---------------- Orderly consumption queue locks ----------------
    private void LockLoop()
    {
        if (!IsOrderly() || _messageModel == RocketMQ.Remoting.Protocol.MessageModel.Broadcasting)
        {
            return;
        }

        // Orderly consumption lockMQ: batch-lock the assigned queues every
        // 20s; try once immediately at startup to avoid idling the first 20s
        while (!_stop)
        {
            // POP orderly likewise runs this 20s timer, but lockAll() only
            // walks processQueueTable (the classic pull table) -- in POP mode
            // queues are registered in popProcessQueueTable, so this table is
            // empty ⇒ the timer turns but sends not a single frame.
            if (!PopMode)
            {
                try
                {
                    List<MessageQueue> mqs = AssignedQueues();
                    if (mqs.Count > 0 && _mqClient is not null)
                    {
                        List<MessageQueue> ok = _mqClient.LockBatchMq(ConsumerGroup, _clientId, mqs);
                        var okKeys = new HashSet<string>(StringComparer.Ordinal);
                        foreach (MessageQueue mq in ok)
                        {
                            okKeys.Add(OffsetKey(mq));
                        }

                        lock (_lock)
                        {
                            _lockOk.Clear();
                            _lockOkAt.Clear();
                            long now = UtilAll.CurrentTimeMillis();
                            foreach (string k in okKeys)
                            {
                                _lockOk.Add(k);
                                _lockOkAt[k] = now;
                            }
                        }

                        ClientLog.Debug("lock_batch_mq: " + okKeys.Count.ToString(CultureInfo.InvariantCulture)
                            + "/" + mqs.Count.ToString(CultureInfo.InvariantCulture) + " queues locked");
                    }
                }
                catch (Exception e)
                {
                    ClientLog.Debug("lock mq error: " + e.Message);
                }
            }

            // Wait 20s (responding to stop meanwhile). Whole-segment wait:
            // 100ms slices measured 131ms each on macOS, and 200 slices would
            // drag a 20s round into 26s; Wait itself is woken by Set and
            // stops just as immediately.
            _stopEvent.Wait(TimeSpan.FromSeconds(20));
        }
    }


    private static string OffsetKey(MessageQueue mq) => LocalOffsetKey(mq);

    /// <summary>The local offset table's per-queue key, shared by every consumer mode.</summary>
    public static string LocalOffsetKey(MessageQueue mq) =>
        mq.Topic + mq.BrokerName + mq.QueueId.ToString(CultureInfo.InvariantCulture);

    private List<MessageQueue> AssignedQueues()
    {
        lock (_lock)
        {
            return new List<MessageQueue>(_assigned);
        }
    }

    /// <summary>All queues of a topic (the topicSubscribeInfoTable convention).
    /// Taken from the SUBSCRIPTION INFO (topicRouteData2TopicSubscribeInfo:
    /// read bits + readQueueNums, master not filtered), not the publish info
    /// -- the two differ on the "perm=4 read-only topic" and "master down,
    /// only slaves left" routes, and the consume side must use the wider one
    /// (the rebalance queue set has always been subscription info; publish
    /// info only serves producer queue selection).</summary>
    private List<MessageQueue> AllQueuesOfTopic(string topic)
    {
        List<MessageQueue> outList = Client().GetTopicSubscribeInfo(topic);
        if (outList.Count == 0)
        {
            // The %RETRY% topic has no route before the first send-back; an
            // expected path, debug is enough
            ClientLog.Debug("rebalance: no subscribe info for topic " + topic);
        }

        outList.Sort(); // MessageQueue IComparable: topic → brokerName → queueId
        return outList;
    }

    /// <summary>Turn each configured topic-level cache budget into a per-queue
    /// knob for the queues this client owns right now: every budget (-1 means no
    /// topic budget, so it is not divided) is divided by that queue count
    /// -- ALL of this consumer's queues, across every
    /// topic -- and the quotient is written into the matching per-queue knob.
    /// This is the ONLY place a topic budget is enforced: the pull-time gate
    /// never aggregates sibling queues (see <see cref="FlowControlHit"/>).
    /// <list type="bullet">
    /// <item>The dividend is always the topic knob, never the current per-queue
    /// value, so repeating the division with an unchanged queue count writes the
    /// same number again (1000 over 8 queues is 125, not 250/8 = 31).</item>
    /// <item>The user's per-queue value is deliberately overwritten while a topic
    /// budget is enabled: only the queue count can make a topic-wide budget
    /// meaningful per queue, so <see cref="PullThresholdForQueue"/> reads back
    /// the divided number.</item>
    /// <item>Owning no queue means there is nothing to divide by: both knobs keep
    /// the user's values (no division by zero, and a 0 quotient would mean "no
    /// message may ever be cached").</item>
    /// <item>The quotient is floored at 1, so a budget smaller than the queue
    /// count still leaves a usable gate.</item>
    /// </list>
    /// The divisor is the live per-queue state table of the current consumption
    /// mode -- <c>_mqMap</c> on the pull path, <c>_popQueues</c> under POP --
    /// which is why the caller runs it AFTER the pull state of this round has
    /// been rebuilt (<see cref="DoRebalance"/>).</summary>
    /// <remarks>Locking: <c>_lock</c> guards the knobs and both state tables, but
    /// <see cref="ClientLog"/> calls must stay OUTSIDE it, so the new values and
    /// the log strings are decided in the critical section and only emitted
    /// after releasing it.</remarks>
    private void DivideTopicThresholds()
    {
        int owned;
        int countFrom = 0;
        int countTo = 0;
        int sizeFrom = 0;
        int sizeTo = 0;
        bool countChanged = false;
        bool sizeChanged = false;
        lock (_lock)
        {
            owned = PopMode ? _popQueues.Count : _mqMap.Count;
            if (owned > 0)
            {
                if (_pullThresholdForTopic != -1)
                {
                    int to = Math.Max(1, _pullThresholdForTopic / owned);
                    if (to != _pullThresholdForQueue)
                    {
                        countFrom = _pullThresholdForQueue;
                        countTo = to;
                        _pullThresholdForQueue = to;
                        countChanged = true;
                    }
                }

                if (_pullThresholdSizeForTopic != -1)
                {
                    int to = Math.Max(1, _pullThresholdSizeForTopic / owned);
                    if (to != _pullThresholdSizeForQueue)
                    {
                        sizeFrom = _pullThresholdSizeForQueue;
                        sizeTo = to;
                        _pullThresholdSizeForQueue = to;
                        sizeChanged = true;
                    }
                }
            }
        }

        // Off the lock: these two lines only report what the writes above decided.
        if (countChanged)
        {
            ClientLog.Info("The pullThresholdForQueue is changed from "
                + countFrom.ToString(CultureInfo.InvariantCulture) + " to "
                + countTo.ToString(CultureInfo.InvariantCulture));
        }

        if (sizeChanged)
        {
            ClientLog.Info("The pullThresholdSizeForQueue is changed from "
                + sizeFrom.ToString(CultureInfo.InvariantCulture) + " to "
                + sizeTo.ToString(CultureInfo.InvariantCulture));
        }
    }

    /// <summary>
    /// Compute the assignment by rebalanceByTopic, then sync the pull thread
    /// set. ⚠ POP's queues also come from this CLIENT REBALANCE (one POP loop
    /// + ack per queue). When clientRebalance=false the protocol also
    /// supports broker-side assignment (getRebalanceResultFromBroker →
    /// queryAssignment, QUERY_ASSIGNMENT=400, replying MessageQueueAssignment
    /// mode=POP). This port only does the client side and does not implement
    /// that path: semantically equivalent, differing only in "who decides the
    /// queue set". Read this paragraph before touching here.
    /// </summary>
    private void DoRebalance()
    {
        MQClientInstance c = Client();
        // The assignment is remembered PER TOPIC: what a rebalance round owes its application is
        // decided topic by topic, so a round that moved one topic's queues must not re-stamp or
        // re-report the topics that kept theirs. `prev` is the flat view of the same table, used
        // only to tell a newly owned queue from a continuing one when resolving initial offsets.
        var wasByTopic = new SortedDictionary<string, HashSet<string>>(StringComparer.Ordinal);
        var prev = new Dictionary<string, MessageQueue>(StringComparer.Ordinal);
        foreach (MessageQueue mq in AssignedQueues())
        {
            prev[OffsetKey(mq)] = mq;
            if (!wasByTopic.TryGetValue(mq.Topic, out HashSet<string>? keys) || keys is null)
            {
                keys = new HashSet<string>(StringComparer.Ordinal);
                wasByTopic[mq.Topic] = keys;
            }

            keys.Add(OffsetKey(mq));
        }

        var assigned = new List<MessageQueue>();
        List<string> topics;
        lock (_lock)
        {
            topics = new List<string>(_subscriptionData.Keys);
        }

        if (_messageModel == RocketMQ.Remoting.Protocol.MessageModel.Broadcasting)
        {
            foreach (string topic in topics)
            {
                assigned.AddRange(AllQueuesOfTopic(topic));
            }
        }
        else
        {
            foreach (string topic in topics)
            {
                List<MessageQueue> mqAll = AllQueuesOfTopic(topic);
                if (mqAll.Count == 0)
                {
                    continue;
                }

                List<string>? cidAll = c.GetConsumerIdListByGroup(topic, ConsumerGroup);
                if (cidAll is null || cidAll.Count == 0)
                {
                    // Consumer list not found: keep this topic's current
                    // assignment (warn only; NEVER fall back to "monopolize
                    // all queues", otherwise multiple instances of the same
                    // group would duplicate each other's consumption)
                    ClientLog.Debug("rebalance: no consumer id list for " + ConsumerGroup + "/" + topic + ", keep current");
                    assigned.AddRange(AssignedQueues().FindAll(m => m.Topic == topic));
                    continue;
                }

                cidAll.Sort(StringComparer.Ordinal);
                // When a custom strategy throws: KEEP THE CURRENT ASSIGNMENT
                // and end this rebalance round (catch Throwable → log error →
                // return false). The topic's queues must NEVER be revoked.
                IAllocateMessageQueueStrategy strategy = _allocateMessageQueueStrategy
                    ?? throw new MQClientException("allocateMessageQueueStrategy is null");
                List<MessageQueue> got;
                try
                {
                    got = strategy.Allocate(ConsumerGroup, _clientId, mqAll, cidAll);
                }
                catch (Exception e)
                {
                    ClientLog.Error("allocate message queue exception. strategy name: "
                        + strategy.GetName() + ", ex: " + e.Message);
                    return;
                }

                assigned.AddRange(got);
            }
        }

        lock (_lock)
        {
            _assigned = assigned;
        }

        // Revocation cleanup (updateProcessQueueTableInRebalance →
        // removeUnnecessaryMessageQueue + removeProcessQueue): queues no
        // longer assigned to this instance this round first have their
        // consumed offsets persisted, in-flight/buffered messages discarded,
        // orderly locks released (orderly+clustering), and are marked in
        // _dropped so the pull thread discards its batch and exits after the
        // long poll returns. The offsets must NEVER be discarded outright --
        // otherwise redelivery restarts from 0.
        var assignedKeys = new HashSet<string>(StringComparer.Ordinal);
        var current = new Dictionary<string, MessageQueue>(StringComparer.Ordinal);
        var nowByTopic = new SortedDictionary<string, HashSet<string>>(StringComparer.Ordinal);
        foreach (MessageQueue mq in assigned)
        {
            assignedKeys.Add(OffsetKey(mq));
            current[OffsetKey(mq)] = mq;
            if (!nowByTopic.TryGetValue(mq.Topic, out HashSet<string>? keys) || keys is null)
            {
                keys = new HashSet<string>(StringComparer.Ordinal);
                nowByTopic[mq.Topic] = keys;
            }

            keys.Add(OffsetKey(mq));
        }

        // Which topics' assignment SET moved this round? A SET comparison per topic, not a
        // count comparison: losing one queue while gaining another keeps the count but changes
        // which buffers exist, and every topic budget has to be re-split over the new queue
        // set. A topic that kept exactly its queues -- in any order -- is not a change and
        // owes nothing.
        List<string> moved = ChangedTopics(wasByTopic, nowByTopic);
        bool changed = moved.Count > 0;

        if (changed)
        {
            ClientLog.Info("rebalance result changed, group=" + ConsumerGroup
                + " clientId=" + _clientId
                + " assigned=" + assigned.Count.ToString(CultureInfo.InvariantCulture)
                + " topics=" + string.Join(",", moved));
            // The assignment moved, so every broker that speaks the fingerprint protocol has to
            // be handed the subscriptions again rather than the cheap subscription-free body.
            c.ResetBrokerAddrHeartbeatFingerprintTable();
        }

        // Stall self-healing (isPullExpired): in the same pass also revoke the
        // queues "still owned by this instance but whose pull loop has
        // stalled", handing them to the later RebalancePullThreads for an
        // in-place rebuild.
        List<RetiredQueue> healed = new();
        lock (_lock)
        {
            SweepStalledLoopsLocked(current, healed);
        }

        // Queues reassigned to this instance clear the revocation marker
        // (possibly revoked last round and assigned back this round),
        // otherwise the pull thread would misjudge isDropped and exit
        // directly. The key existence of _pullThreads prevents two threads
        // starting for the same queue. (OFFSET_ILLEGAL's offset freeze is not
        // lifted here; see RebalancePullThreads's rebuild branch.)
        lock (_lock)
        {
            foreach (string k in assignedKeys)
            {
                _dropped.Remove(k);
            }
        }

        var revoked = new List<MessageQueue>();
        foreach (KeyValuePair<string, MessageQueue> kv in prev)
        {
            if (!assignedKeys.Contains(kv.Key))
            {
                revoked.Add(kv.Value);
            }
        }

        if (revoked.Count > 0)
        {
            // The SAME revocation path as the 220 reset and self-healing
            // (RetireQueueLocked epoch +1 is equivalent to setDropped): the
            // offset is persisted before the rebuild, orderly consumption is
            // unlocked, and in-flight batches' acks are voided along with it.
            var retireList = new List<RetiredQueue>();
            lock (_lock)
            {
                foreach (MessageQueue mq in revoked)
                {
                    RetireQueueLocked(OffsetKey(mq), mq, retireList);
                }
            }

            OnQueuesRevoked(retireList);

            ClientLog.Info("rebalance: revoked " + revoked.Count.ToString(CultureInfo.InvariantCulture)
                + " queue(s), current assigned=" + assigned.Count.ToString(CultureInfo.InvariantCulture));
        }

        // Queues revoked by self-healing: the offset must be persisted BEFORE
        // the rebuild (the same convention as the real revocation above),
        // otherwise
        // the new loop re-pulls from an earlier cursor and redelivers already
        // consumed messages. Orderly consumption must also unlock, otherwise
        // the new owner cannot grab the lock.
        if (healed.Count > 0)
        {
            OnQueuesRevoked(healed);
        }

        // Newly assigned queues resolve the initial offset into the offset
        // table IMMEDIATELY (updateProcessQueueTableInRebalance: new queue →
        // computePullFromWhereWithException → offsetStore.updateOffset). It
        // cannot be left to lazy resolution at first pull -- CONSUME_FROM_LAST_OFFSET
        // semantics are "the latest offset at assignment time", and lazy
        // resolution skips messages produced after assignment (observable on
        // live machines as not receiving).
        foreach (MessageQueue mq in assigned)
        {
            string key = OffsetKey(mq);
            if (prev.ContainsKey(key))
            {
                continue;
            }

            SubscriptionData? sub;
            lock (_lock)
            {
                if (_offsetTable.ContainsKey(key))
                {
                    continue;
                }

                _subscriptionData.TryGetValue(mq.Topic, out sub);
            }

            if (sub is null)
            {
                continue;
            }

            long off;
            try
            {
                off = ResolveInitialOffset(mq);
            }
            catch (Exception e)
            {
                // The queue stays assigned but gets no cursor: its pull loop
                // resolves one again before the first pull and parks until the
                // store answers, so a guessed start offset never replays it.
                ClientLog.Warn("failed to resolve the initial offset for " + mq
                    + ", the pull loop will retry: " + e.Message);
                continue;
            }

            lock (_lock)
            {
                if (!_offsetTable.ContainsKey(key))
                {
                    _offsetTable[key] = off;
                }
            }
        }

        RebalancePullThreads();

        // The round's assignment moved the queue SET of at least one topic: pay what that change
        // owes, in the fixed order (re-stamp -> re-divide -> immediate heartbeat -> application
        // listener). The order matters: RebalancePullThreads() above is what rebuilt the
        // per-queue pull state (_mqMap / _popQueues), and that table is the divisor, so dividing
        // before it would use the previous round's queue count. An unchanged round leaves a
        // hand-set per-queue knob alone and sends nothing extra -- including a round that only
        // self-heals a stalled pull loop, which never re-stamps and never notifies.
        AfterAssignmentChange(moved);
    }

    /// <summary>The topics whose assigned queue SET differs between two rounds. Exposed for the
    /// offline cases: the round itself is only reachable through a live rebalance.</summary>
    public static List<string> ChangedTopics(
        IDictionary<string, HashSet<string>> before,
        IDictionary<string, HashSet<string>> after)
    {
        var moved = new List<string>();
        var topics = new SortedSet<string>(StringComparer.Ordinal);
        foreach (string t in before.Keys) topics.Add(t);
        foreach (string t in after.Keys) topics.Add(t);
        foreach (string topic in topics)
        {
            bool had = before.TryGetValue(topic, out HashSet<string>? wasSet);
            bool has = after.TryGetValue(topic, out HashSet<string>? nowSet);
            var a = had ? wasSet! : new HashSet<string>(StringComparer.Ordinal);
            var b = has ? nowSet! : new HashSet<string>(StringComparer.Ordinal);
            if (!a.SetEquals(b)) moved.Add(topic);
        }

        return moved;
    }

    /// <summary>Everything a rebalance round whose queue set moved pays, in one place and in one
    /// order that cannot drift: re-share the moved topics' subscription version, re-split the
    /// topic cache budgets over the queues alive this round, hand every broker the beat that
    /// carries the fresh versions, and finally tell the application's queue listener exactly
    /// those topics. An unchanged round pays nothing.</summary>
    private void AfterAssignmentChange(List<string> moved)
    {
        if (moved.Count == 0)
        {
            return;
        }

        RestampSubscriptionVersions(moved);
        DivideTopicThresholds();
        // The new version must reach the broker NOW rather than at the next periodic beat: the
        // broker keys the filter results it caches for a subscription by its version, so a topic
        // whose queues just moved would stay "unchanged" to the broker until that beat lands.
        try
        {
            SendHeartbeatToAllBroker();
        }
        catch (Exception e)
        {
            ClientLog.Debug("heartbeat after rebalance change failed: " + e.Message);
        }

        NotifyQueueChanged(moved);
    }

    /// <summary>Wall-clock milliseconds, one stamp shared by the round: the version is what makes
    /// a broker re-read a subscription instead of reusing the filtered result it cached for the
    /// old generation, so a topic whose queues moved has to look new to the broker even when its
    /// filter expression did not change. Topics that did not move keep their version --
    /// re-stamping everything would throw away the caching the broker does for the untouched
    /// subscriptions.</summary>
    private void RestampSubscriptionVersions(List<string> moved)
    {
        long newVersion = UtilAll.CurrentTimeMillis();
        var stamped = new List<(string Topic, long Old)>();
        lock (_lock)
        {
            foreach (string topic in moved)
            {
                if (!_subscriptionData.TryGetValue(topic, out SubscriptionData? sub))
                {
                    continue;
                }

                stamped.Add((topic, sub.SubVersion));
                sub.SubVersion = newVersion;
            }
        }

        // _lock is not reentrant-safe to hold while logging, so the lines are decided inside the
        // critical section and written after releasing it.
        foreach ((string topic, long old) in stamped)
        {
            ClientLog.Info(topic + " rebalance changed, also update version: "
                + old.ToString(CultureInfo.InvariantCulture) + ", "
                + newVersion.ToString(CultureInfo.InvariantCulture));
        }
    }

    /// <summary>Fire the application's queue listener for the moved topics only: the callback is
    /// a change notification, not a heartbeat, so an unchanged topic gets nothing. The signature
    /// is (topic, the FULL route set, this instance's share), so the application can tell a
    /// rebalance from a route change without asking again. A listener that throws is logged and
    /// never breaks the rebalance round.</summary>
    private void NotifyQueueChanged(List<string> moved)
    {
        IMessageQueueListener? listener;
        List<MessageQueue> assignedSnapshot;
        lock (_lock)
        {
            listener = _messageQueueListener;
            assignedSnapshot = new List<MessageQueue>(_assigned);
        }

        if (listener is null)
        {
            return;
        }

        var byTopic = new Dictionary<string, List<MessageQueue>>(StringComparer.Ordinal);
        foreach (MessageQueue mq in assignedSnapshot)
        {
            if (!byTopic.TryGetValue(mq.Topic, out List<MessageQueue>? list) || list is null)
            {
                list = new List<MessageQueue>();
                byTopic[mq.Topic] = list;
            }

            list.Add(mq);
        }

        foreach (string topic in moved)
        {
            try
            {
                listener.MessageQueueChanged(topic, AllQueuesOfTopic(topic),
                    byTopic.TryGetValue(topic, out List<MessageQueue>? mine)
                        ? mine : new List<MessageQueue>());
            }
            catch (Exception e)
            {
                ClientLog.Debug("message queue listener failed for " + topic + ": " + e.Message);
            }
        }
    }

    /// <summary>Where a newly assigned queue starts pulling.
    ///
    /// Two invariants hold this method together:
    /// <list><item>the start offset NEVER depends on the subscription type -- an
    /// earlier draft short-circuited SQL92 to the max offset, which silently skipped
    /// every message produced before the consumer started (send-first +
    /// CONSUME_FROM_FIRST_OFFSET received nothing). The broker applies the SQL92
    /// filter to whatever range the pull covers, so the policy alone decides;</item>
    /// <item>broadcasting and clustering differ only in WHERE the stored offset is
    /// read from (local file vs broker). Both fall through to the same policy
    /// switch when nothing is stored, so a timestamp or %RETRY% start cannot be
    /// mode-dependent.</item></list>
    /// A failure of the fallback RPCs propagates: the callers skip this queue and
    /// retry on the next rebalance, which is right -- guessing 0 here would replay
    /// the whole queue.</summary>
    private long ResolveInitialOffset(MessageQueue mq)
    {
        MQClientInstance c = Client();

        long stored = -1;
        if (_messageModel == RocketMQ.Remoting.Protocol.MessageModel.Broadcasting)
        {
            // Broadcasting mode: the offset is stored only locally
            // (LocalFileOffsetStore)
            if (LoadLocalOffsets().TryGetValue(OffsetKey(mq), out long local) && local >= 0)
            {
                stored = local;
            }
        }
        else
        {
            // Clustering mode: first query the offset committed on the broker.
            // "Not found" is the only answer that falls through to the policy;
            // any other failure propagates so the caller skips this queue for
            // the current round instead of guessing a start offset from it.
            if (c.QueryConsumerOffset(ConsumerGroup, mq, out long remote, 5000, null,
                    /*setZeroIfNotFound=*/false))
            {
                stored = remote;
            }
        }

        if (stored >= 0)
        {
            return stored;
        }

        if (_consumeFromWhere == RocketMQ.Remoting.Protocol.ConsumeFromWhere.ConsumeFromFirstOffset)
        {
            // FIRST_OFFSET with nothing stored gives 0 directly, for both modes
            // (the local file and broker reads share this branch); do NOT send a
            // minOffset query -- minOffset belongs to the admin port's convention
            // (master only), and during a master outage it would leave a newly
            // started consumer unable to pull a single message; an out-of-bounds
            // start is corrected by the broker's PULL_OFFSET_MOVED.
            return 0;
        }

        if (_consumeFromWhere == RocketMQ.Remoting.Protocol.ConsumeFromWhere.ConsumeFromTimestamp)
        {
            // A %RETRY% topic has no meaningful "time of the oldest retry", and
            // retried messages must all be retried, so it starts at the queue tail
            // rather than searching backwards through a timestamp.
            if (MixAll.IsRetryTopic(mq.Topic))
            {
                return c.GetMaxOffset(mq);
            }

            return c.SearchOffsetByTimestamp(mq,
                DefaultLitePullConsumer.ParseConsumeTimestamp(ConsumeTimestamp));
        }

        // Default CONSUME_FROM_LAST_OFFSET: a %RETRY% topic starts at 0 so every
        // retry already queued is delivered; a business topic starts at the tail.
        if (MixAll.IsRetryTopic(mq.Topic))
        {
            return 0;
        }

        return c.GetMaxOffset(mq);
    }

    // ---------------- Heartbeat ----------------
    public int SendHeartbeatToAllBroker()
    {
        if (_mqClient is null)
        {
            return 0;
        }

        List<string> addrs;
        try
        {
            // Send to every one (master + slave): this client's heartbeat round
            // walks every brokerId in the route-derived broker table, skipping only when
            // `consumerEmpty && id != MASTER_ID`; the consumer heartbeat
            // always carries ConsumerData, so slaves are not skipped. The
            // broker keeps its consumer registrations per machine, and a slave
            // not receiving the heartbeat replies SUBSCRIPTION_NOT_EXIST to
            // pulls pointing at itself; the
            // default PostSubscriptionWhenPull=false pull carries no
            // subscription flag and goes exactly that way. The producer
            // heartbeat is the opposite: it carries only ProducerData
            // (consumerEmpty); see the KnownBrokerAddrs used in Producer.cs.
            addrs = _mqClient.GetAllBrokerAddrs();
        }
        catch (Exception e)
        {
            ClientLog.Warn("heartbeat: gather brokers failed: " + e.Message);
            return 0;
        }

        if (addrs.Count == 0)
        {
            return 0;
        }

        var hb = new HeartbeatData(_clientId);
        var cd = new ConsumerData
        {
            GroupName = ConsumerGroup,
            ConsumeType = ConsumeType.ConsumePassively,
            MessageModel = _messageModel,
            ConsumeFromWhere = _consumeFromWhere,
            // The beat's ConsumerData carries the consumer's own unitMode.
            // The broker reads it and marks %RETRY%group with the
            // UNIT_SUB(0x2) system flag.
            UnitMode = _unitMode,
        };
        lock (_lock)
        {
            foreach (var kv in _subscriptionData)
            {
                cd.AddSubscriptionData(kv.Value);
            }
        }

        // Keep fingerprint = 0 so the broker takes the V1 registration path
        // (registering with the full subscriptionDataSet); withoutSub is only
        // read on the V2 path where fingerprint != 0, so keeping the default
        // false here is enough.
        hb.HeartbeatFingerprint = 0;
        hb.AddConsumerData(cd);

        int okCount = _mqClient.SendHeartbeatToAllBrokers(hb, isRebalance: false, includeSlaves: true);
        if (okCount > 0)
        {
            Interlocked.Add(ref _heartbeatCount, okCount);
        }

        return okCount;
    }

    private void MaybeSendHeartbeat()
    {
        if (!_heartbeatEnabled)
        {
            return;
        }

        long now = UtilAll.CurrentTimeMillis();
        long last = _lastHeartbeatMs;
        if (last != 0 && now - last < _heartbeatIntervalMillis)
        {
            return;
        }

        _lastHeartbeatMs = now;
        SendHeartbeatToAllBroker();
    }

    // ---------------- Admin ----------------
    public List<MessageQueue> FetchSubscribeMessageQueues(string topic)
    {
        // fetchSubscribeMessageQueues reads the rebalance subscription-info
        // chain (read bits, master not filtered), sharing the same convention
        // with
        // rebalance, not the publish info.
        return Client().GetTopicSubscribeInfo(topic);
    }

    /// <summary>The queue key list currently assigned to this instance (live
    /// verification that "two instances of the same group neither overlap nor
    /// miss"). The key format matches OffsetKey: topic + brokerName +
    /// queueId.</summary>
    public List<string> AssignedQueueKeys()
    {
        var outList = new List<string>();
        foreach (MessageQueue mq in AssignedQueues())
        {
            outList.Add(OffsetKey(mq));
        }

        return outList;
    }

    /// <summary>All clientIds of a consume group on a topic
    /// (findConsumerIdList), for verifying multi-instance registration. Not
    /// found / not started returns empty, never throws.</summary>
    public List<string> ConsumerIdListOfGroup(string topic)
    {
        if (_mqClient is null)
        {
            return new List<string>();
        }

        try
        {
            return _mqClient.GetConsumerIdListByGroup(topic, ConsumerGroup) ?? new List<string>();
        }
        catch (Exception)
        {
            return new List<string>();
        }
    }

    // Message send-back (sendMessageBack): success returns true. A refused bounce
    // (no master, transport failure, non-success answer) degrades to a normal send to
    // %RETRY%<group> so the redelivery is not lost; only when that send fails too does
    // the ORIGINAL error reach the caller.
    public bool SendMessageBack(MessageExt msg, int delayLevel, string? brokerNameIn = null)
    {
        MQClientInstance c = Client();
        int maxReconsume = MaxReconsumeTimesOrDefault();
        try
        {
            string brokerName = string.IsNullOrEmpty(brokerNameIn) ? msg.BrokerName : brokerNameIn!;
            // Use findBrokerAddressInPublish (master only, does not refresh
            // routes); when unavailable report "Broker[X] master node does not
            // exist"; slaves do not accept CONSUMER_SEND_MSG_BACK.
            string addr = c.FindBrokerAddressInPublish(brokerName);
            if (addr.Length == 0)
            {
                throw new MQClientException("Broker[" + brokerName + "] master node does not exist");
            }

            var header = new ConsumerSendMsgBackRequestHeader
            {
                Offset = msg.CommitLogOffset,
                Group = ConsumerGroup,
                DelayLevel = delayLevel,
                OriginMsgId = msg.MsgId,
                OriginTopic = msg.Topic,
                // ⚠ Deliberately beyond protocol requirements:
                // consumerSendMessageBack protocol-wise only fills
                // group/offset/delayLevel/originMsgId/originTopic/maxReconsumeTimes/brokerName,
                // never writes unitMode, the field is always false. The broker
                // side does read it (its sys-flag builder receives
                // (false, true)), so it is reported truthfully here
                // per the consumer config, and only then does the unitized retry
                // topic carry the UNIT_SUB flag.
                UnitMode = _unitMode,
                // maxReconsumeTimes == -1 is passed to the broker as 16 (the
                // broker moves to %DLQ% once exceeded)
                MaxReconsumeTimes = maxReconsume,
            };
            c.InvokeSync(c.VipAddr(addr), RequestCode.ConsumerSendMsgBack, header.ToExtFields(), null, false, 5000);
            return true;
        }
        catch (Exception e)
        {
            ClientLog.Error("Failed to send message back, consumerGroup=" + ConsumerGroup
                            + ", brokerName=" + msg.BrokerName + ", msg=" + msg.MsgId
                            + ": " + e.Message + "; fallback to normal send");
            try
            {
                SendRetryMessage(msg, maxReconsume, c);
                return true;
            }
            catch (Exception fallbackError)
            {
                ClientLog.Error("Fallback normal send failed, consumerGroup=" + ConsumerGroup
                                + ", msg=" + msg.MsgId + ": " + fallbackError.Message);
                ExceptionDispatchInfo.Capture(e).Throw();
                throw;  // ExceptionDispatchInfo.Throw never returns
            }
        }
    }

    // SendRetryMessage delivers a failed delivery as a normal message to
    // %RETRY%<group> through the internal producer.
    private void SendRetryMessage(MessageExt msg, int maxReconsumeTimes, MQClientInstance c)
    {
        Message newMsg = BuildRetryMessage(msg, maxReconsumeTimes);
        TopicPublishInfo publish = c.GetTopicPublishInfo(newMsg.Topic, isDefault: true);
        // unitMode follows the consumer: without it, sent-back messages lose their unitization mark.
        c.SendMessage(MixAll.ClientInnerProducerGroup, newMsg, publish.SelectOneMessageQueue(),
            3000, unitMode: _unitMode);
    }

    // ---------------- Internal utilities ----------------
    private static string Trim(string s)
    {
        int b = 0;
        while (b < s.Length && IsWs(s[b])) ++b;
        if (b == s.Length) return string.Empty;
        int e = s.Length - 1;
        while (e > b && IsWs(s[e])) --e;
        return s.Substring(b, e - b + 1);
    }

    private static bool IsWs(char c) => c == ' ' || c == '\t' || c == '\r' || c == '\n';

    private static List<string> SplitSemicolon(string addr)
    {
        var outList = new List<string>();
        if (addr is null) return outList;
        int start = 0;
        while (start <= addr.Length)
        {
            int pos = addr.IndexOf(';', start);
            string piece = pos < 0 ? addr.Substring(start) : addr.Substring(start, pos - start);
            string t = Trim(piece);
            if (t.Length > 0) outList.Add(t);
            if (pos < 0) break;
            start = pos + 1;
        }

        return outList;
    }
}
