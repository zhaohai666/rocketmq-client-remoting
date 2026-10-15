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
//   - The broker clamps the client-supplied suspend time to its own
//     brokerSuspendMaxTimeMillis, so idle queues still time out
//     periodically on the client side -- this is BENIGN; log at debug,
//     never ERROR.
using System;
using System.Collections.Generic;
using System.Globalization;
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

    /// <summary>The parameter is "how much to subtract" (a positive count),
    /// not a negative delta.</summary>
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
    private int _pullThresholdForQueue = 1000;
    // The remaining four flow-control thresholds: the byte gates are in MiB
    // (&lt;=0 disables); the span is max-min of queueOffset in pending and
    // counts only when STRICTLY greater; the two topic-level ones default to
    // -1 (disabled) and aggregate the buffered totals across ALL queues of
    // that topic on this instance.
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
    // clear every 1000ms (PULL_TIME_DELAY_MILLS_WHEN_SUSPEND, :113).
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

    public bool HeartbeatEnabled
    {
        get => _heartbeatEnabled;
        set => _heartbeatEnabled = value;
    }

    // Per-queue "pulled but not consumed" threshold; pull for that queue
    // pauses once exceeded (default 1000)
    public int PullThresholdForQueue
    {
        get => _pullThresholdForQueue;
        set => _pullThresholdForQueue = value;
    }

    /// <summary><c>pullThresholdSizeForQueue</c> (default 100): this queue's
    /// "pulled but not consumed" byte threshold in <b>MiB</b>; &lt;=0 disables
    /// this gate.</summary>
    public int PullThresholdSizeForQueue
    {
        get => _pullThresholdSizeForQueue;
        set => _pullThresholdSizeForQueue = value;
    }

    /// <summary><c>consumeMessageMaxSpan</c> (default 2000): span threshold for
    /// the queueOffset of this queue's "pulled but not consumed" messages;
    /// hits only when STRICTLY greater (prevents one stuck head message from
    /// piling up behind it indefinitely); &lt;=0 disables.</summary>
    public long ConsumeConcurrentlyMaxSpan
    {
        get => _consumeConcurrentlyMaxSpan;
        set => _consumeConcurrentlyMaxSpan = value;
    }

    /// <summary><c>pullThresholdForTopic</c> (default -1, disabled): the
    /// cumulative "pulled but not consumed" count across <b>all</b> queues of
    /// the same topic on this instance. The classic implementation divides the
    /// topic threshold by the queue count down to the queue level; this port
    /// compares the cumulative value directly.</summary>
    public int PullThresholdForTopic
    {
        get => _pullThresholdForTopic;
        set => _pullThresholdForTopic = value;
    }

    /// <summary><c>pullThresholdSizeForTopic</c> (default -1, disabled): the
    /// cumulative byte threshold for the same topic, in <b>MiB</b>. This gate
    /// reads only this property and does NOT reuse the queue-level switch.</summary>
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
    /// Why it must be immediate: the broker's ConsumerManager only records
    /// topic→group into its own topicGroupTable when it receives a heartbeat
    /// (ClientManageProcessor → registerConsumer), and it is exactly this
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
    /// buffer levels and thread-pool size. Configured as 0 or
    /// <c>int.MaxValue</c> without this gate, either every pull round is
    /// judged as backlog by "threshold &lt;= 0" and pulling stops PERMANENTLY
    /// (the consumer silently receives nothing), or integer overflow bypasses
    /// the flow-control check entirely. Waiting for a broker error is several
    /// beats too late, and the error only lands in the log.</para>
    /// <para>The -1 of <c>PullThresholdForTopic</c> /
    /// <c>PullThresholdSizeForTopic</c> is a "disable this gate" sentinel: it
    /// is out of range but not an error; the other fields have no such
    /// exemption (writing -1 there is a startup error, not "unlimited").</para>
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
            // The consumeTimestamp format check is a no-op here -- this port
            // has no configurable ConsumeTimestamp property; CONSUME_FROM_TIMESTAMP
            // always means "30 minutes ago" (see ResolveInitialOffset), so
            // there is no writable bad string to reject (one fewer config
            // item, not a missing check).
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
            // this consumer (naturally synced via the shared ClientConfig)
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
        // ConsumeMessagePopConcurrentlyService has no corresponding schedule.
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
        // joined before it can be released.
        _popConsumeExecutor?.Shutdown(true);
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
    /// RebalanceService#wakeup semantics). Runs on the remoting reader
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
            // Protocol convention: a group mismatch replies SYSTEM_ERROR (the
            // broker logs a warn)
            return RemotingCommand.CreateResponseCommand(
                ResponseCode.SystemError,
                "consumerGroup not matched, expect " + ConsumerGroup + ", got " + (group ?? "<null>"));
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
            // Protocol convention: a group mismatch replies SYSTEM_ERROR (the
            // broker logs a warn)
            return RemotingCommand.CreateResponseCommand(
                ResponseCode.SystemError,
                "consumerGroup not matched, expect " + ConsumerGroup + ", got " + (group ?? "<null>"));
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
            return RemotingCommand.CreateResponseCommand(
                ResponseCode.SystemError,
                "consumerGroup not matched, expect " + ConsumerGroup + ", got " + (group ?? "<null>"));
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

    /// <summary>The pre-pull flow-control verdict -- five thresholds checked
    /// before enqueueing into the buffer, returning on the first hit in this
    /// order (an earlier hit masks the later ones):
    /// <list type="number">
    /// <item>count &gt;= <c>PullThresholdForQueue</c> (<c>Math.max(1,n)</c>
    /// guard: 0 also counts as 1)</item>
    /// <item>bytes &gt;= <c>PullThresholdSizeForQueue</c>, in <b>MiB</b>
    /// (&lt;=0 disables)</item>
    /// <item>span <b>strictly greater than</b> <c>ConsumeConcurrentlyMaxSpan</c>
    /// (max-min of queueOffset within the buffer)</item>
    /// <item>topic cumulative count &gt;= <c>PullThresholdForTopic</c>
    /// (across <b>all</b> queues of that topic on this instance)</item>
    /// <item>topic cumulative bytes &gt;= <c>PullThresholdSizeForTopic</c>,
    /// in MiB (does not reuse the second item's switch)</item>
    /// </list>
    /// One hit only increments <see cref="FlowControlTriggered"/> by one. The
    /// whole table is aggregated only when the topic-level gate is truly on;
    /// otherwise every verdict would walk all queues' buffers one extra
    /// time.</summary>
    private bool FlowControlHit(MessageQueue mq, string key)
    {
        int count;
        double sizeMb;
        long span;
        int topicCount = 0;
        double topicSizeMb = 0.0;
        lock (_lock)
        {
            if (_pending.TryGetValue(key, out Queue<MessageExt>? q) && q.Count > 0)
            {
                long bytes = 0;
                long minOffset = long.MaxValue;
                long maxOffset = long.MinValue;
                foreach (MessageExt m in q)
                {
                    bytes += m.StoreSize;
                    if (m.QueueOffset < minOffset) minOffset = m.QueueOffset;
                    if (m.QueueOffset > maxOffset) maxOffset = m.QueueOffset;
                }

                count = q.Count;
                sizeMb = bytes / (1024.0 * 1024.0);
                span = maxOffset - minOffset;
            }
            else
            {
                count = 0;
                sizeMb = 0;
                span = 0;
            }

            if (_pullThresholdForTopic > 0 || _pullThresholdSizeForTopic > 0)
            {
                long topicBytes = 0;
                foreach (KeyValuePair<string, Queue<MessageExt>> kv in _pending)
                {
                    if (!_mqMap.TryGetValue(kv.Key, out MessageQueue? other)
                        || other.Topic != mq.Topic)
                    {
                        continue;
                    }

                    foreach (MessageExt m in kv.Value)
                    {
                        topicCount++;
                        topicBytes += m.StoreSize;
                    }
                }

                topicSizeMb = topicBytes / (1024.0 * 1024.0);
            }
        }

        string? reason = null;
        if (count >= Math.Max(1, _pullThresholdForQueue))
        {
            reason = "count=" + count.ToString(CultureInfo.InvariantCulture);
        }
        else if (_pullThresholdSizeForQueue > 0 && sizeMb >= _pullThresholdSizeForQueue)
        {
            reason = "size=" + sizeMb.ToString("F1", CultureInfo.InvariantCulture) + "MB";
        }
        else if (_consumeConcurrentlyMaxSpan > 0 && span > _consumeConcurrentlyMaxSpan)
        {
            reason = "span=" + span.ToString(CultureInfo.InvariantCulture);
        }
        else if (_pullThresholdForTopic > 0 && topicCount >= _pullThresholdForTopic)
        {
            reason = "topicCount=" + topicCount.ToString(CultureInfo.InvariantCulture);
        }
        else if (_pullThresholdSizeForTopic > 0 && topicSizeMb >= _pullThresholdSizeForTopic)
        {
            reason = "topicSize=" + topicSizeMb.ToString("F1", CultureInfo.InvariantCulture) + "MB";
        }

        if (reason is null)
        {
            return false;
        }

        Interlocked.Increment(ref _flowControlTriggered);
        ClientLog.Debug("flow control: queue " + mq + " " + reason + ", pause pull");
        return true;
    }

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
            // confirmed locked (LOCK_BATCH_MQ)
            if (orderly)
            {
                bool locked;
                lock (_lock)
                {
                    locked = _lockOk.Contains(key);
                }

                if (!locked)
                {
                    _stopEvent.Wait(TimeSpan.FromMilliseconds(200));
                    continue;
                }
            }

            // Flow control (five thresholds, see FlowControlHit): hitting any
            // one pauses this queue's pulling
            if (FlowControlHit(mq, key))
            {
                _stopEvent.Wait(TimeSpan.FromMilliseconds(100));
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
                    offset = ResolveInitialOffset(mq, sub);
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

                // Consume statistics (RT recorded every time, TPS only when
                // there are messages)
                _mqClient!.ConsumerStats.IncPullRT(ConsumerGroup, mq.Topic,
                    UtilAll.CurrentTimeMillis() - pullBegan);
                if (result.IsFound && result.MsgFoundList.Count > 0)
                {
                    _mqClient.ConsumerStats.IncPullTPS(ConsumerGroup, mq.Topic,
                        result.MsgFoundList.Count);
                }
            }
            catch (MQBrokerException e)
            {
                // TOPIC_NOT_EXIST / PULL_NOT_FOUND etc. are mostly expected
                // paths (topic not created yet); debug + back off
                ClientLog.Debug("pull broker error for " + mq + ": " + e.Message);
                _stopEvent.Wait(TimeSpan.FromMilliseconds(500));
                continue;
            }
            catch (RemotingTimeoutException e)
            {
                // A long poll with no new messages during suspend triggering a
                // client timeout is normal behavior: the broker clamps the
                // suspend time to its own brokerSuspendMaxTimeMillis,
                // ignoring the client-supplied value, so idle queues time out
                // periodically. Not an error; debug only, to avoid polluting
                // the runtime log.
                ClientLog.Debug("pull long-poll timeout for " + mq
                    + " (benign, will retry): " + e.Message);
                continue;
            }
            catch (Exception e)
            {
                ClientLog.Debug("pull error for " + mq + ": " + e.Message);
                _stopEvent.Wait(TimeSpan.FromMilliseconds(500));
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

                // Retry-message topic restoration (processPullResult): the
                // broker writes retry messages to %RETRY%group, but each
                // message carries a RETRY_TOPIC property pointing at the
                // original topic; restore it before dispatch, otherwise the
                // upper listener sees the topic as %RETRY% rather than the
                // business topic.
                string retryTopic = MixAll.GetRetryTopic(ConsumerGroup);
                lock (_lock)
                {
                    Queue<MessageExt> dq = _pending[key];
                    foreach (MessageExt m in result.MsgFoundList)
                    {
                        if (m.Topic == retryTopic)
                        {
                            string? orig = m.GetProperty(MessageConst.PropertyRetryTopic);
                            if (!string.IsNullOrEmpty(orig))
                            {
                                // The namespace must also be stripped from
                                // the restored topic before handing it to the
                                // listener
                                m.Topic = _namespace.Length == 0
                                    ? orig!
                                    : NamespaceUtil.WithoutNamespace(orig!, _namespace);
                            }
                        }

                        dq.Enqueue(m);
                    }

                    // msgAccCnt: computed from the FILTERED-then-enqueued batch
                    // (filter first, then count)
                    UpdateMsgAccCntLocked(key, result.MsgFoundList);
                }
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
        }
    }

    /// <summary>
    /// Pull statistics for the POP path (the PopCallback.onSuccess convention).
    /// <para>
    /// The pull callback records RT every time, the POP callback only on
    /// <c>FOUND</c>, and before the emptiness check; TPS is recorded only by
    /// the truly popped count. Copy this asymmetry: an empty-handed POP
    /// return is the norm for long polling (<c>POLLING_NOT_FOUND</c>), and
    /// counting it in RT would dilute the average pull time with suspend
    /// duration.
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
            // Flow control: too many popped-but-unacked, slow down first
            // (popThresholdForQueue)
            if (pq.WaitAckCount() > PopThresholdForQueue)
            {
                Thread.Sleep(50);
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
            catch (Exception e)
            {
                ClientLog.Debug("pop error for " + key + ": " + e.Message);
                Thread.Sleep(500);
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

            // Pull statistics (the PopCallback.onSuccess convention; the
            // verdict is in RecordPopPullStats)
            RecordPopPullStats(Client().ConsumerStats, ConsumerGroup, mq.Topic, result, began);

            if (result.Status == PopStatus.Found && result.MsgFoundList.Count > 0)
            {
                pq.IncFoundMsg(result.MsgFoundList.Count);
                // Pre-delivery filtering (processPopResult): the POP path MUST
                // ack the removed ones, otherwise the broker revives and
                // redelivers them when invisibleTime expires -- showing up as
                // "filtering did not take effect".
                List<MessageExt> kept = FilterMessagesForDelivery(mq, sub, result.MsgFoundList);
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

                if (kept.Count > 0)
                {
                    SubmitPopConsumeRequest(kept, pq, mq);
                }
            }
            else if (UtilAll.CurrentTimeMillis() - began < 200)
            {
                // Empty result: if the broker did not suspend per pollTime
                // (returns immediately) this becomes a hot loop; back off here
                // as a "round took too short" fallback to avoid hammering the
                // broker.
                Thread.Sleep(200);
            }
            // NoNewMsg / PollingNotFound / PollingFull all go straight to the
            // next round
        }
    }

    /// <summary>The unified entry for POP popped batches: orderly listeners
    /// divert into the <see cref="SubmitPopOrderlyRequest"/> skeleton while
    /// concurrent listeners go through ConsumeMessagePopConcurrentlyService.
    /// Public for the test project (no InternalsVisibleTo).
    /// <para>Service selection by listener type: orderly listener + POP goes
    /// through ConsumeMessagePopOrderlyService. In 5.5.0 it is an unfinished
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

    /// <summary>Before consumption, restore retry messages' topic to the
    /// original business topic (resetRetryAndNamespace). Equivalent to the
    /// inline segment on the pull path; extracted here for POP reuse.</summary>
    private void ResetRetryTopicAndNamespace(List<MessageExt> msgs)
    {
        string retryTopic = MixAll.GetRetryTopic(ConsumerGroup);
        foreach (MessageExt m in msgs)
        {
            if (m.Topic != retryTopic) continue;
            string? orig = m.GetProperty(MessageConst.PropertyRetryTopic);
            if (string.IsNullOrEmpty(orig)) continue;
            // The namespace must also be stripped from the restored topic
            // before handing it to the listener
            m.Topic = _namespace.Length == 0 ? orig! : NamespaceUtil.WithoutNamespace(orig!, _namespace);
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

    /// <summary>Preset a queue's "assigned queue" registration. The topic-level
    /// thresholds rely on this table to aggregate sibling queues of the same
    /// topic; not registering equals the queue having been revoked by
    /// rebalance.</summary>
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

            // Success + AutoCommit=false: record OK TPS only, no commit.
            // Deliberate deviation: the classic approach leaves messages in
            // consumingMsgOrderlyTreeMap waiting for an explicit commit(),
            // while the listener has no access to the queue buffer (no commit
            // opening), and copying "do nothing" would let this batch be
            // swallowed by the dispatch thread with the offset unmoved.
            // Here it is equivalently pushed back to the queue head and waits
            // one suspend period: the offset likewise does not advance, no
            // messages are lost, and the consume thread does not become a busy
            // wait (without the sleep the next round would immediately grab
            // the same batch again).
            RecordConsumeStats(mq.Topic, batch.Count, obegin, failed: false);
            RequeuePending(key, batch);
            _stopEvent.Wait(TimeSpan.FromMilliseconds(OrderlySuspendMillis(ctx)));
            return false;
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
    /// called while holding the lock; dedup by reference only guards against
    /// future new call sites writing overlaps.</summary>
    private List<MessageExt> ProcessQueueEntriesLocked(string key)
    {
        var outList = new List<MessageExt>();
        var seen = new HashSet<MessageExt>();
        if (_inFlightMsgs.TryGetValue(key, out List<MessageExt>? inflight))
        {
            foreach (MessageExt m in inflight)
            {
                if (seen.Add(m))
                {
                    outList.Add(m);
                }
            }
        }

        if (_pending.TryGetValue(key, out Queue<MessageExt>? q))
        {
            foreach (MessageExt m in q)
            {
                if (seen.Add(m))
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
    /// getMaxReconsumeTimes.
    ///
    /// Orderly-consumed messages always stop in the local queue and retry in
    /// place, and the broker side has no redelivery count at all, so the
    /// default should be to keep retrying until success -- <c>-1</c> is read
    /// as <c>int.MaxValue</c> here. The concurrent consumption's
    /// <c>-1 → 16</c> (<c>DefaultMQPushConsumerImpl#getMaxReconsumeTimes</c>)
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
    /// <c>handleRetryAndDLQ</c> (<c>SendMessageProcessor:199-234</c>) sees the
    /// group still holds an unexpired queue lock (exactly the orderly
    /// consumption group's characteristic) and redirects it straight to
    /// <c>%DLQ%&lt;group&gt;</c>. Among them <c>RECONSUME_TIME</c> /
    /// <c>MAX_RECONSUME_TIMES</c> get lifted into the request header on the
    /// send side (<c>MQClientInstance.BuildSendRequest</c> lifts them into the
    /// request header on the send side, equivalent to sendKernelImpl).
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
            // must copy the consumer config (resetClientConfig); without it
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
    /// The two newMsg constructors of <c>sendMessageBackAsNormalMessage</c>
    /// and <c>sendMessageBack</c> (the orderly chain) are LINE-BY-LINE
    /// IDENTICAL (only maxReconsumeTimes calls its own getter each), so they
    /// are extracted into one function to avoid the two places' property
    /// setting drifting apart.
    /// </summary>
    private Message BuildRetryMessage(MessageExt msg, int maxReconsumeTimes)
    {
        var newMsg = new Message(MixAll.GetRetryTopic(ConsumerGroup), msg.Body);
        newMsg.Properties = new PropertyMap(msg.Properties);
        newMsg.Flag = msg.Flag;
        string originMsgId = msg.GetProperty(MessageConst.PropertyOriginMessageId);
        if (string.IsNullOrEmpty(originMsgId))
        {
            originMsgId = msg.MsgId;
        }

        if (!string.IsNullOrEmpty(originMsgId))
        {
            newMsg.PutProperty(MessageConst.PropertyOriginMessageId, originMsgId);
        }

        newMsg.PutProperty(MessageConst.PropertyRetryTopic, msg.Topic);
        newMsg.PutProperty(MessageConst.PropertyReconsumeTime,
            (msg.ReconsumeTimes + 1).ToString(CultureInfo.InvariantCulture));
        newMsg.PutProperty(MessageConst.PropertyMaxReconsumeTimes,
            maxReconsumeTimes.ToString(CultureInfo.InvariantCulture));
        // Half-message send-back must not carry TRAN_MSG, otherwise the
        // broker treats it as a check-back message again
        newMsg.RemoveProperty(MessageConst.PropertyTransactionPrepared);
        newMsg.DelayTimeLevel = 3 + msg.ReconsumeTimes;
        if (string.IsNullOrEmpty(newMsg.GetProperty(MessageConst.PropertyUniqClientMessageIdKeyidx)))
        {
            MessageClientIDSetter.SetUniqId(newMsg);
        }

        return newMsg;
    }

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
    /// subscription expression and not one matched, see
    /// <c>MQClientAPIImpl:1095-1097</c> mapping <c>PULL_RETRY_IMMEDIATELY</c>
    /// to it), this queue's "consumed offset" must follow the pull cursor,
    /// otherwise it DEADLOCKS PERMANENTLY: messages nobody acks belong to
    /// nobody (those filtered out on the broker side are not in the response,
    /// and those removed by the client's second-pass tag filter are
    /// explicitly not acked), and with the offset stuck in place it stays
    /// there forever, rescanning this unwanted batch from the beginning after
    /// redelivery/restart.
    /// </para>
    /// <para>
    /// The update goes through
    /// <c>offsetStore.updateOffset(mq, nextOffset, increaseOnly=true)</c>
    /// (forward only, never backward), with one gate:
    /// <c>0L == processQueue.getMsgCount()</c>. <c>msgCount</c> counts
    /// messages STILL IN the ProcessQueue -- the concurrent consumption's
    /// <c>removeMessage</c> is only called after the listener returns
    /// (<c>ConsumeMessageConcurrentlyService:266</c>), so in-flight batches
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
    /// <c>PullMessageProcessor</c> replying <c>PULL_OFFSET_MOVED</c> uniformly
    /// for OFFSET_OVERFLOW_BADLY / OFFSET_TOO_SMALL / OFFSET_RESET, mapped by
    /// the client at <c>MQClientAPIImpl:1099</c> to this status) and puts the
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

    private string LocalOffsetPath()
    {
        // Local offset file path:
        // $HOME/.rocketmq_offsets/<clientId>/<group>/offsets.json
        string home = UtilAll.UserHome();
        if (home.Length == 0) home = ".";
        return home + "/.rocketmq_offsets/" + (_clientId.Length == 0 ? "DEFAULT" : _clientId)
            + "/" + ConsumerGroup + "/offsets.json";
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
        File.WriteAllText(tmp, BuildLocalOffsetsJson(items, mqMap));
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

    // Strict JSON parsing first (the flat map an older version wrote);
    // fastjson's "object as key" format necessarily fails strict parsing
    // (including the pretty version), falling into ScanOffsetEntries.
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

        return ScanOffsetEntries(trimmed);
    }

    private static Dictionary<string, long>? ScanOffsetEntries(string text)
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

        var outMap = new Dictionary<string, long>(StringComparer.Ordinal);
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

            outMap[topic + broker + queueId.Value.ToString(CultureInfo.InvariantCulture)] = off;
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

        return pos == text.Length ? outMap : null;
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
                            foreach (string k in okKeys)
                            {
                                _lockOk.Add(k);
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


    private static string OffsetKey(MessageQueue mq) => mq.Topic + mq.BrokerName + mq.QueueId.ToString(CultureInfo.InvariantCulture);

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
        var prev = new Dictionary<string, MessageQueue>(StringComparer.Ordinal);
        foreach (MessageQueue mq in AssignedQueues())
        {
            prev[OffsetKey(mq)] = mq;
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
        foreach (MessageQueue mq in assigned)
        {
            assignedKeys.Add(OffsetKey(mq));
            current[OffsetKey(mq)] = mq;
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
                off = ResolveInitialOffset(mq, sub);
            }
            catch (Exception e)
            {
                ClientLog.Debug("resolve initial offset for " + mq + " failed: " + e.Message);
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
    }

    private long ResolveInitialOffset(MessageQueue mq, SubscriptionData sub)
    {
        MQClientInstance c = Client();
        if (sub.ExpressionType == ExpressionType.Sql92)
        {
            // SQL filtering has no offset semantics, default latest
            return c.GetMaxOffset(mq);
        }

        if (_messageModel == RocketMQ.Remoting.Protocol.MessageModel.Broadcasting)
        {
            // Broadcasting mode: the offset is stored only locally
            // (LocalFileOffsetStore)
            Dictionary<string, long> stored = LoadLocalOffsets();
            if (stored.TryGetValue(OffsetKey(mq), out long v))
            {
                return v;
            }

            if (_consumeFromWhere == RocketMQ.Remoting.Protocol.ConsumeFromWhere.ConsumeFromFirstOffset)
            {
                // FIRST_OFFSET with no local offset gives 0 directly
                // (broadcasting mode reads the local offset table, the same
                // branch); do NOT send a minOffset query -- minOffset belongs
                // to the admin port's convention (master only), and during a
                // master
                // outage it would leave a newly started consumer unable to
                // pull a single message; an out-of-bounds start is corrected
                // by the broker's PULL_OFFSET_MOVED.
                return 0;
            }

            return c.GetMaxOffset(mq);
        }

        // Clustering mode: first query the offset committed on the broker
        // (RemoteBrokerOffsetStore.readOffset)
        try
        {
            if (c.QueryConsumerOffset(ConsumerGroup, mq, out long stored, 5000, null,
                    /*setZeroIfNotFound=*/false))
            {
                return stored;
            }
        }
        catch (Exception e)
        {
            ClientLog.Debug("query consumer offset for " + mq + " not found: " + e.Message);
        }

        try
        {
            if (_consumeFromWhere == RocketMQ.Remoting.Protocol.ConsumeFromWhere.ConsumeFromFirstOffset)
            {
                // Same as above (the local file and broker paths are the same
                // branch): the start is the literal 0.
                return 0;
            }

            if (_consumeFromWhere == RocketMQ.Remoting.Protocol.ConsumeFromWhere.ConsumeFromTimestamp)
            {
                long ts = UtilAll.CurrentTimeMillis() - 30 * 60 * 1000L;
                return c.SearchOffsetByTimestamp(mq, ts);
            }

            // Default CONSUME_FROM_LAST_OFFSET
            return c.GetMaxOffset(mq);
        }
        catch (Exception)
        {
            return 0;
        }
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
            // Send to every one (master + slave): sendHeartbeatToAllBroker
            // walks every brokerId of brokerAddrTable, skipping only when
            // `consumerEmpty && id != MASTER_ID`; the consumer heartbeat
            // always carries ConsumerData, so slaves are not skipped. The
            // broker's ConsumerManager has one copy per machine, and a slave
            // not receiving the heartbeat replies SUBSCRIPTION_NOT_EXIST to
            // pulls pointing at itself (PullMessageProcessor:420-427; the
            // default PostSubscriptionWhenPull=false pull carries no
            // subscription flag and goes exactly that way). The producer
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
            // consumerData.setUnitMode(impl.isUnitMode()).
            // The broker reads it and marks %RETRY%group with the
            // UNIT_SUB(0x2) system flag (ClientManageProcessor).
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

        int okCount = 0;
        foreach (string addr in addrs)
        {
            try
            {
                _mqClient.SendHeartbeat(addr, hb, 5000);
                ++okCount;
                Interlocked.Increment(ref _heartbeatCount);
            }
            catch (Exception e)
            {
                ClientLog.Warn("heartbeat to " + addr + " failed: " + e.Message);
            }
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

    // Message send-back (sendMessageBack): failure throws MQBrokerException,
    // success returns true
    public bool SendMessageBack(MessageExt msg, int delayLevel, string? brokerNameIn = null)
    {
        MQClientInstance c = Client();
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
            // side does read it (AbstractSendMessageProcessor:135-138 →
            // buildSysFlag(false, true)), so it is reported truthfully here
            // per the consumer config, and only then does the unitized retry
            // topic carry the UNIT_SUB flag.
            UnitMode = _unitMode,
            // maxReconsumeTimes == -1 is passed to the broker as 16 (the
            // broker moves to %DLQ% once exceeded)
            MaxReconsumeTimes = MaxReconsumeTimesOrDefault(),
        };
        c.InvokeSync(addr, RequestCode.ConsumerSendMsgBack, header.ToExtFields(), null, false, 5000);
        return true;
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
