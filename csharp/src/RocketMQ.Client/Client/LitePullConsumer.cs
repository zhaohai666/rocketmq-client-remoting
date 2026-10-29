using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Threading;

using RocketMQ.Common;
using RocketMQ.Remoting;
using RocketMQ.Remoting.Protocol;

namespace RocketMQ.Client;

public interface ILiteMessageQueueListener
{
    // mqAll=all queues currently subscribed; mqDivided=the queues newly assigned to this instance.
    void MessageQueueChanged(IReadOnlyList<MessageQueue> mqAll, IReadOnlyList<MessageQueue> mqDivided);
}

public interface ITopicMessageQueueChangeListener
{
    void OnChanged(string topic, IReadOnlyList<MessageQueue> messageQueues);
}

public sealed class DefaultLitePullConsumer
{
    private readonly object _lock = new();
    private string _consumerGroup;
    private string _namespace = string.Empty;
    private string _instanceName = MixAll.DefaultInstanceName;
    private string _clientId = string.Empty;
    private string _messageModel = MessageModel.Clustering;
    private IAllocateMessageQueueStrategy? _allocateMessageQueueStrategy = new AllocateMessageQueueAveragely();
    private string _consumeFromWhere = ConsumeFromWhere.ConsumeFromLastOffset;
    private string _consumeTimestamp =
        UtilAll.TimeMillisToHumanString3(UtilAll.CurrentTimeMillis() - 30 * 60 * 1000);
    private readonly List<string> _nameServerAddrs = new();
    private IRpcHook? _rpcHook;
    private string _unitName = string.Empty;
    private bool _unitMode;
    private bool _enableStreamRequestType = true;
    private int _pollNameServerIntervalMillis = 30000;
    private bool _decodeReadBody = true;
    private int _mqClientApiTimeout = MQClientInstance.MqClientApiTimeoutMillis;
    private bool _enableConcurrentHeartbeat;
    private int _concurrentHeartbeatThreadPoolSize = Environment.ProcessorCount;
    private int _maxPageSizeInGetMetadata = MQClientInstance.DefaultMaxPageSizeInGetMetadata;

    // subscribe-mode subscription table (topic -> sub_expression, namespace already applied)
    private readonly Dictionary<string, string> _subscription = new(StringComparer.Ordinal);
    // SubscriptionData for each subscription (with tagsSet), used in the heartbeat sent to
    // the broker for tag-filter registration
    private readonly Dictionary<string, SubscriptionData> _subscriptionData = new(StringComparer.Ordinal);
    // assign mode: topic -> tag filter expression (passed through to pull)
    private readonly Dictionary<string, string> _assignSubExpr = new(StringComparer.Ordinal);
    // Extra subscriptions the application announces in the heartbeat
    // (BuildSubscriptionsForHeartbeat). An assign-mode expression is not a topic-wide
    // subscription, so this is the only way a topic reaches the group's heartbeat.
    private readonly Dictionary<string, SubscriptionData> _heartbeatSubscriptions = new(StringComparer.Ordinal);
    // Expression type per topic for the selector-flavoured subscriptions (real topic ->
    // TAG/SQL92/CLASS_FILTER). A topic missing from this table is a plain TAG subscription,
    // which is exactly what every string-based subscription is today.
    private readonly Dictionary<string, string> _subscriptionType = new(StringComparer.Ordinal);
    // 0 = not chosen yet, 1 = subscribe, 2 = assign. The first facade call that needs a
    // delivery shape claims it; from then on the other shape is refused.
    private const int ModeNone = 0;
    private const int ModeSubscribe = 1;
    private const int ModeAssign = 2;
    private int _mode = ModeNone;
    private bool _assignMode => _mode == ModeAssign;
    private readonly HashSet<MessageQueue> _assigned = new();

    private int _brokerSuspendMaxTimeMillis = 20000;
    private int _consumerTimeoutMillisWhenSuspend = 30000;
    private int _pollTimeoutMillis = 5000;
    // The lite pull path asks for 10 per round (the push path's pullBatchSize stays 32):
    // since the aggregate gate budgets a whole batch per pending round, this number is a
    // multiplier of that budget and not just a request size.
    private int _pullBatchSize = 10;
    private bool _autoCommit = true;
    // Shortest auto-commit interval SetAutoCommitIntervalMillis accepts; below it a setting is ignored.
    public const int MinAutoCommitIntervalMillis = 1000;
    private int _autoCommitIntervalMillis = 5000;
    private int _pullIntervalMillis = 50;

    // ---- local-buffer flow control (the four budgets the pull round checks before it
    // builds a request). Plain budgets, deliberately unvalidated and unclamped: a
    // threshold of 0 means "trip on the first cached message", which is a legal
    // configuration, and a negative one simply always trips.
    private int _pullThresholdForAll = 10000;
    private int _pullThresholdForQueue = 1000;
    private int _pullThresholdSizeForQueue = 100;
    private long _consumeMaxSpan = 2000;

    private readonly object _bufferLock = new();
    private readonly Queue<MessageExt> _localBuffer = new();
    // One queue's slice of _localBuffer (cached count, cached body bytes, lowest and
    // highest cached queue offset). Only ever mutated together with the buffer, under
    // _bufferLock, so this view cannot drift from the messages it describes.
    private readonly Dictionary<MessageQueue, QueueCache> _queueCaches = new();
    // Queue -> the park armed by the last refusal (a cache gate or a broker
    // FLOW_CONTROL answer). Per queue on purpose: a parked queue is skipped for its own
    // round only and never stalls the rest of the round.
    private readonly Dictionary<MessageQueue, PullPause> _pullPause = new();
    // Monotonic clock for the park deadlines: a wall-clock jump must not park a queue for
    // hours nor let a deadline lapse early.
    private static readonly Stopwatch MonoClock = Stopwatch.StartNew();
    // Three refusal counters: the aggregate gate, the count+size gates (shared) and the
    // span gate. Read through the public observers below.
    private long _flowControlForAllTriggered;
    private long _flowControlForQueueTriggered;
    private long _flowControlForSpanTriggered;
    private readonly Dictionary<MessageQueue, long> _nextOffset = new();
    private readonly Dictionary<MessageQueue, long> _consumeOffset = new();
    private readonly Dictionary<MessageQueue, long> _offsetTable = new();
    private readonly Dictionary<MessageQueue, long> _seekOffset = new();
    // Offline test seam for the seek range check: when set, it answers the queue's
    // min/max without a broker. Production code leaves it null and reads the broker.
    private Func<MessageQueue, (long Min, long Max)>? _seekBoundsForTest;
    private long _nextAutoCommitDeadline = -1;
    private readonly HashSet<MessageQueue> _paused = new();
    private ILiteMessageQueueListener? _messageQueueListener;
    private int _topicMetadataCheckIntervalMillis = 30000;
    private readonly Dictionary<string, ITopicMessageQueueChangeListener> _topicChangeListeners =
        new(StringComparer.Ordinal);
    private readonly Dictionary<string, List<MessageQueue>> _messageQueuesForTopic =
        new(StringComparer.Ordinal);

    private const int ServiceStateCreateJust = 0;
    private const int ServiceStateRunning = 1;
    private const int ServiceStateStartFailed = 2;
    private const int ServiceStateShutdownAlready = 3;

    private MQClientInstance? _mqClient;
    private int _serviceState = ServiceStateCreateJust;
    private bool _started;
    private bool _running;
    private Thread? _pullThread;
    private Thread? _heartbeatThread;
    private Thread? _metadataThread;

    public DefaultLitePullConsumer(string consumerGroup = MixAll.DefaultConsumerGroup)
    {
        if (string.IsNullOrWhiteSpace(consumerGroup))
        {
            throw new MQClientException("consumerGroup is empty");
        }

        _consumerGroup = consumerGroup;
    }

    // ---------------- configuration ----------------
    public void SetNamesrvAddr(string addr) => SetNameServerAddresses(SplitSemicolon(addr));

    public void SetNameServerAddresses(IEnumerable<string> addrs)
    {
        lock (_lock)
        {
            _nameServerAddrs.Clear();
            _nameServerAddrs.AddRange(addrs);
        }
    }

    public void SetInstanceName(string name) => _instanceName = name;

    public void SetMessageModel(string model) => _messageModel = model;

    // Offline test seam: redirects the broadcast offset file away from the user home.
    public string? LocalOffsetPathForTest { get; set; }

    public void SetAllocateMessageQueueStrategy(IAllocateMessageQueueStrategy? strategy) =>
        _allocateMessageQueueStrategy = strategy;

    public IAllocateMessageQueueStrategy? AllocateMessageQueueStrategy => _allocateMessageQueueStrategy;

    public void SetNamespace(string ns) => _namespace = ns ?? string.Empty;

    public string UnitName
    {
        get => _unitName;
        set => _unitName = value ?? string.Empty;
    }

    public bool UnitMode
    {
        get => _unitMode;
        set => _unitMode = value;
    }

    public bool EnableStreamRequestType
    {
        get => _enableStreamRequestType;
        set => _enableStreamRequestType = value;
    }

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

    private SocksProxyConfig? _socksProxyConfig;

    public SocksProxyConfig? SocksProxyConfig
    {
        get => _socksProxyConfig;
        set => _socksProxyConfig = value;
    }

    public void SetRpcHook(IRpcHook hook) => _rpcHook = hook;

    public void SetCredentials(string accessKey, string secretKey, string securityToken = "") =>
        _rpcHook = new AclClientRPCHook(new SessionCredentials(accessKey, secretKey, securityToken));

    public string ConsumerGroup => _consumerGroup;

    public string ClientId => _clientId;

    public string Namespace
    {
        get => _namespace;
        set => _namespace = value ?? string.Empty;
    }

    public string NamespaceV2
    {
        get => _namespaceV2;
        set => _namespaceV2 = value ?? string.Empty;
    }

    private string _namespaceV2 = string.Empty;

    public bool IsStarted => _started;

    public bool IsRunning => _running;

    public void SetConsumeFromWhere(string where) => _consumeFromWhere = where;

    public void SetConsumeTimestamp(string ts) => _consumeTimestamp = ts;

    public string ConsumeTimestamp => _consumeTimestamp;

    public void SetPullBatchSize(int n) => _pullBatchSize = Math.Max(1, n);

    public int PullBatchSize => _pullBatchSize;

    public void SetPollTimeoutMillis(int ms) => _pollTimeoutMillis = Math.Max(0, ms);

    public void SetAutoCommit(bool b) => _autoCommit = b;

    // The floor is an acceptance rule, not a clamp: a value below it is ignored outright, so the
    // interval that was already running keeps running instead of being pulled down to the floor.
    public void SetAutoCommitIntervalMillis(int ms)
    {
        if (ms >= MinAutoCommitIntervalMillis)
        {
            _autoCommitIntervalMillis = ms;
        }
    }

    public int AutoCommitIntervalMillis => _autoCommitIntervalMillis;

    public void SetConsumerTimeoutMillisWhenSuspend(int ms) => _consumerTimeoutMillisWhenSuspend = ms;

    public void SetBrokerSuspendMaxTimeMillis(int ms) => _brokerSuspendMaxTimeMillis = ms;

    public void SetPullIntervalMillis(int ms) => _pullIntervalMillis = Math.Max(0, ms);

    /// <summary><c>pullThresholdForAll</c> (default 10000): the budget for the messages the
    /// <b>pending pull rounds</b> could still deliver -- a capacity projection, not the count
    /// sitting in the buffer. Measured as <c>rounds x pullBatchSize</c>, where a round counts
    /// while at least one of its messages is still cached, whether it came back full or with
    /// a single row. The gate fires only when that projection is STRICTLY greater, so the
    /// threshold reads as "how many may be in flight". A plain budget: 0 trips on the first
    /// pending round.</summary>
    public int PullThresholdForAll
    {
        get => _pullThresholdForAll;
        set => _pullThresholdForAll = value;
    }

    /// <summary><c>pullThresholdForQueue</c> (default 1000): the cached message count
    /// allowed for <b>one</b> queue. Judged against this queue's own share of the buffer
    /// and never summed with a sibling; strictly greater fires, so a queue sitting exactly
    /// on the threshold still pulls. 0 trips on the first cached message.</summary>
    public int PullThresholdForQueue
    {
        get => _pullThresholdForQueue;
        set => _pullThresholdForQueue = value;
    }

    /// <summary><c>pullThresholdSizeForQueue</c> (default 100): the cached bytes allowed
    /// for <b>one</b> queue, measured in <b>MiB</b>. The measure is this queue's sum of each
    /// cached message's <b>body</b> length, never the stored-record size a broker charges
    /// (<c>storeSize</c> is a telemetry field and must not reach a flow-control input).
    /// Divided by 1 MiB with <b>integer</b> division, so a threshold of 1 allows anything
    /// below 2 MiB. 0 trips as soon as a full MiB is cached.</summary>
    public int PullThresholdSizeForQueue
    {
        get => _pullThresholdSizeForQueue;
        set => _pullThresholdSizeForQueue = value;
    }

    /// <summary><c>consumeMaxSpan</c> (default 2000): the queueOffset span (highest cached
    /// offset minus lowest cached offset) allowed for <b>one</b> queue, computed over only
    /// this queue's cached messages. An empty or single-message cache has span 0. Strictly
    /// greater fires; 0 trips as soon as the cached offsets differ.</summary>
    public long ConsumeMaxSpan
    {
        get => _consumeMaxSpan;
        set => _consumeMaxSpan = value;
    }

    /// <summary>Aggregate-gate refusals (the whole buffer over
    /// <see cref="PullThresholdForAll"/>), exposed the same way the push path exposes its
    /// flow-control count.</summary>
    public long FlowControlForAllTriggered => Interlocked.Read(ref _flowControlForAllTriggered);

    /// <summary>Refusals of the per-queue count gate and of the per-queue size gate; the
    /// two share one counter, as they describe the same event (this queue's cached
    /// backlog is over budget).</summary>
    public long FlowControlForQueueTriggered => Interlocked.Read(ref _flowControlForQueueTriggered);

    /// <summary>Span-gate refusals (this queue's cached offset span over
    /// <see cref="ConsumeMaxSpan"/>).</summary>
    public long FlowControlForSpanTriggered => Interlocked.Read(ref _flowControlForSpanTriggered);

    /// <summary>Messages currently sitting in the local buffer, over every queue: the rows
    /// the per-queue count gate measures. The aggregate gate does NOT read this -- it
    /// budgets the pending pull rounds, one whole <c>pullBatchSize</c> each.</summary>
    public int CachedMessageCount
    {
        get
        {
            lock (_bufferLock)
            {
                return _localBuffer.Count;
            }
        }
    }

    /// <summary>This instance's pending pull rounds summed over every queue's accumulator:
    /// what the aggregate gate multiplies by <c>pullBatchSize</c>. A round counts while at
    /// least one of its messages is still cached.</summary>
    public long PendingPullRounds
    {
        get
        {
            lock (_bufferLock)
            {
                long rounds = 0;
                foreach (QueueCache cache in _queueCaches.Values) rounds += cache.PendingRounds;
                return rounds;
            }
        }
    }

    /// <summary>This queue's pending pull rounds (0 when the buffer holds nothing of
    /// it, and 0 once the queue was revoked, since its accumulator went with it).</summary>
    public int PendingPullRoundsOf(MessageQueue mq)
    {
        lock (_bufferLock)
        {
            return _queueCaches.TryGetValue(mq, out QueueCache? cache) ? cache.PendingRounds : 0;
        }
    }

    /// <summary>This queue's cached message count (0 when the buffer holds nothing of
    /// it).</summary>
    public int CachedMessageCountOf(MessageQueue mq)
    {
        lock (_bufferLock)
        {
            return _queueCaches.TryGetValue(mq, out QueueCache? cache) ? cache.Count : 0;
        }
    }

    /// <summary>This queue's cached bytes, measured as the sum of its messages' <b>body</b>
    /// lengths (0 when the buffer holds nothing of it).</summary>
    public long CachedBytesOf(MessageQueue mq)
    {
        lock (_bufferLock)
        {
            return _queueCaches.TryGetValue(mq, out QueueCache? cache) ? cache.SizeBytes : 0;
        }
    }

    /// <summary>This queue's cached queueOffset span, highest minus lowest over only this
    /// queue's cached messages (0 for an empty or single-message cache).</summary>
    public long CachedOffsetSpanOf(MessageQueue mq)
    {
        lock (_bufferLock)
        {
            return _queueCaches.TryGetValue(mq, out QueueCache? cache) ? cache.Span : 0;
        }
    }

    /// <summary>The park a cache-flow-control refusal arms for its queue: the same shared
    /// constant the push path pauses with, so one value governs every cache gate.</summary>
    public static int CacheFlowControlBackoffMillis =>
        DefaultMQPushConsumer.CacheFlowControlBackoffMillis;

    /// <summary>The park a broker FLOW_CONTROL(215) answer arms on this path (again the
    /// push path's shared constant: a busy broker is not a capacity verdict).</summary>
    public static int BrokerFlowControlBackoffMillis =>
        DefaultMQPushConsumer.BrokerFlowControlBackoffMillis;

    /// <summary>The warn cadence of all three paths: the first trip warns, then every
    /// thousandth after it (one shared rule, delegated to the push path's helper).</summary>
    public static bool IsFlowControlWarnTrip(long times) =>
        DefaultMQPushConsumer.IsFlowControlWarnTrip(times);

    /// <summary>How long a broker answer parks this queue's next round: FLOW_CONTROL(215)
    /// parks for <see cref="BrokerFlowControlBackoffMillis"/> (20 ms) instead of the
    /// generic failed-pull backoff; <c>-1</c> for every other code means "no per-queue
    /// park, keep what a failed lite pull already does". Public so the decision is
    /// pinnable offline without racing a timer.</summary>
    public static int LitePullPauseMillisForBrokerResponse(int responseCode) =>
        responseCode == ResponseCode.FlowControl
            ? DefaultMQPushConsumer.BrokerFlowControlBackoffMillis
            : NoPullPause;

    /// <summary>The milliseconds left on this queue's park (0 when it is not parked): the
    /// refusal re-schedules only this queue, so a parked queue does not delay its
    /// siblings.</summary>
    public long PullParkRemainingMillis(MessageQueue mq)
    {
        lock (_lock)
        {
            if (!_pullPause.TryGetValue(mq, out PullPause? pause)) return 0;
            long left = pause.DeadlineMillis - MonoClock.ElapsedMilliseconds;
            return left > 0 ? left : 0;
        }
    }

    /// <summary>The length (ms) of the last park armed for this queue, or
    /// <see cref="NoPullPause"/> when it was never parked. Sticky: the record survives the
    /// deadline, so what a refusal scheduled is observable without racing a timer.</summary>
    public int LastPullPauseMillis(MessageQueue mq)
    {
        lock (_lock)
        {
            return _pullPause.TryGetValue(mq, out PullPause? pause) ? pause.Millis : NoPullPause;
        }
    }

    /// <summary>Run one pre-pull cache-gate verdict for this queue (without the network),
    /// exactly the check the pull round makes: a hit increments its counter by one and
    /// parks this queue for <see cref="CacheFlowControlBackoffMillis"/>.</summary>
    public bool CacheFlowControlHitForTest(MessageQueue mq) => CacheFlowControlHit(mq);

    /// <summary>Hand messages to the local buffer through the append path a pull result
    /// uses (accumulators included), so a gate verdict can be pinned without a
    /// broker.</summary>
    public void AppendToLocalBufferForTest(IReadOnlyList<MessageExt> msgs) => Enqueue(msgs);

    /// <summary>Answer as though Start() had completed. The running flag is all the
    /// offset surface reads; the client stays null, so a call that would ask a broker
    /// still refuses.</summary>
    public void MarkRunningForTest() => _running = true;

    /// <summary>Supplies the broker boundary offsets a seek range check needs, so the
    /// guard can be pinned offline. When never called the seek reads the real broker.</summary>
    public void SetSeekBoundsForTest(long minOffset, long maxOffset) =>
        _seekBoundsForTest = _ => (minOffset, maxOffset);

    /// <summary>Whether this queue's paused flag is currently set.</summary>
    public bool IsPausedForTest(MessageQueue mq)
    {
        lock (_lock)
        {
            return _paused.Contains(mq);
        }
    }

    /// <summary>Nothing parked: the queue's next round runs on the loop's own cadence.</summary>
    public const int NoPullPause = -1;

    public void SetMessageQueueListener(ILiteMessageQueueListener listener) => _messageQueueListener = listener;

    public void SetTopicMetadataCheckIntervalMillis(int ms) =>
        _topicMetadataCheckIntervalMillis = Math.Max(1000, ms);

    public int TopicMetadataCheckIntervalMillis => _topicMetadataCheckIntervalMillis;

    // ---------------- subscription / allocation ----------------
    /// <summary>Claims one of the two mutually exclusive delivery shapes. The first
    /// caller decides; a later caller asking for the other shape is refused. Repeating
    /// the same shape (for example resubscribing a topic) stays allowed.</summary>
    private void ClaimMode(int mode)
    {
        lock (_lock)
        {
            if (_mode == ModeNone)
            {
                _mode = mode;
                return;
            }

            if (_mode != mode)
            {
                throw new MQClientException("Subscribe and assign are mutually exclusive.");
            }
        }
    }

    public void Subscribe(string topic, string subExpression = "*")
    {
        if (string.IsNullOrWhiteSpace(topic))
        {
            throw new ArgumentException("Topic can not be null or empty.");
        }

        ClaimMode(ModeSubscribe);
        string ns = WithNamespace(topic);
        lock (_lock)
        {
            _subscription[ns] = subExpression;
            _subscriptionType.Remove(ns);
            try
            {
                _subscriptionData[ns] = FilterAPI.BuildSubscriptionData(ns, subExpression);
            }
            catch
            {
                _subscriptionData.Remove(ns);
            }
        }

        NotifySubscriptionChanged(ns);
    }

    /// <summary>Selector-flavoured subscribe: the expression type is carried end-to-end
    /// (subscription table -> heartbeat -> the lite-pull request's expressionType). A TAG
    /// selector builds exactly the subscription the string overload builds today; a SQL92
    /// (or class-filter) expression is kept verbatim, never hashed as a tag list.</summary>
    public void Subscribe(string topic, MessageSelector selector)
    {
        if (string.IsNullOrWhiteSpace(topic))
        {
            throw new ArgumentException("Topic can not be null or empty.");
        }

        ArgumentNullException.ThrowIfNull(selector);
        ClaimMode(ModeSubscribe);
        string ns = WithNamespace(topic);
        lock (_lock)
        {
            try
            {
                SubscriptionData sub = SubscriptionSelectors.Build(ns, selector);
                _subscription[ns] = sub.SubString;
                if (sub.ExpressionType == ExpressionType.TAG)
                {
                    _subscriptionType.Remove(ns);
                }
                else
                {
                    _subscriptionType[ns] = sub.ExpressionType;
                }

                _subscriptionData[ns] = sub;
            }
            catch
            {
                _subscription.Remove(ns);
                _subscriptionType.Remove(ns);
                _subscriptionData.Remove(ns);
            }
        }

        NotifySubscriptionChanged(ns);
    }

    public void SetSubExpressionForAssign(string topic, string subExpression)
    {
        if (string.IsNullOrWhiteSpace(subExpression))
        {
            throw new ArgumentException("subExpression can not be null or empty.");
        }

        if (_running)
        {
            throw new MQClientException("setSubExpressionForAssign only can be called before start.");
        }

        if (string.IsNullOrWhiteSpace(topic))
        {
            throw new ArgumentException("Topic can not be null or empty.");
        }

        ClaimMode(ModeAssign);
        string ns = WithNamespace(topic);
        lock (_lock)
        {
            _assignSubExpr[ns] = subExpression;
            // The assign-mode string is a TAG filter again: drop any selector-typed entry
            // for this topic so pull and heartbeat agree on it.
            _subscriptionType.Remove(ns);
            try
            {
                _subscriptionData[ns] = FilterAPI.BuildSubscriptionData(ns, subExpression);
            }
            catch
            {
                _subscriptionData.Remove(ns);
            }
        }
    }

    /// <summary>Replaces the subscriptions this consumer announces in its heartbeat beyond
    /// its rebalance subscriptions. An assign-mode expression filters only the queues this
    /// instance was handed, so it is never announced on its own; this set is how the
    /// application opts a topic in.
    /// Every call replaces the whole set: a topic left out of the map stops being announced
    /// on the next beat. A topic the rebalance subscriptions already cover keeps their
    /// expression -- this set only fills the ones they do not. A null or empty map clears
    /// the set. A rejected selector leaves the previous set intact.</summary>
    public void BuildSubscriptionsForHeartbeat(IReadOnlyDictionary<string, MessageSelector>? selectors)
    {
        var built = new Dictionary<string, SubscriptionData>(StringComparer.Ordinal);
        if (selectors is not null)
        {
            foreach (KeyValuePair<string, MessageSelector> kv in selectors)
            {
                if (string.IsNullOrWhiteSpace(kv.Key))
                {
                    throw new ArgumentException("Topic can not be null or empty.");
                }

                ArgumentNullException.ThrowIfNull(kv.Value);
                string ns = WithNamespace(kv.Key);
                built[ns] = SubscriptionSelectors.Build(ns, kv.Value);
            }
        }

        lock (_lock)
        {
            _heartbeatSubscriptions.Clear();
            foreach (KeyValuePair<string, SubscriptionData> kv in built)
            {
                _heartbeatSubscriptions[kv.Key] = kv.Value;
            }
        }
    }

    /// <summary>The built set, sorted by topic. A snapshot: changing it does not change
    /// what the heartbeat announces.</summary>
    public IReadOnlyList<SubscriptionData> GetSubscriptionsForHeartbeat()
    {
        lock (_lock)
        {
            var subs = new List<SubscriptionData>(_heartbeatSubscriptions.Values);
            subs.Sort((a, b) => string.CompareOrdinal(a.Topic, b.Topic));
            return subs;
        }
    }

    public void Assign(IReadOnlyList<MessageQueue> messageQueues)
    {
        if (messageQueues is null || messageQueues.Count == 0)
        {
            throw new ArgumentException("Message queues can not be null or empty.");
        }

        ClaimMode(ModeAssign);
        var set = new HashSet<MessageQueue>(messageQueues);
        var dropped = new List<MessageQueue>();
        lock (_lock)
        {
            foreach (MessageQueue mq in _assigned)
            {
                if (set.Contains(mq)) continue;
                // A queue this instance lets go loses every piece of per-queue state:
                // cursors, pause flag, park deadline and seek pin. The buffered rows and
                // accumulators follow in ForgetQueues below.
                ForgetQueueStateLocked(mq);
                dropped.Add(mq);
            }

            _assigned.Clear();
            foreach (MessageQueue mq in set) _assigned.Add(mq);
            foreach (MessageQueue mq in _assigned)
            {
                if (!_nextOffset.ContainsKey(mq))
                {
                    try
                    {
                        _nextOffset[mq] = ResolveInitialOffset(mq);
                    }
                    catch
                    {
                        ClientLog.Debug("lite assign: resolve initial offset failed for " + mq);
                    }
                }
            }
        }

        // Outside _lock: the buffer has its own lock, and the state lock is never held while
        // it is taken.
        ForgetQueues(dropped);
    }

    // ---------------- lifecycle ----------------
    private static MQClientException ServiceStateError(string state) =>
        new("The PullConsumer service state not OK, maybe started once, " + state);

    /// <summary>Start-up configuration gate. A subscription is deliberately not required:
    /// subscribing while RUNNING is supported.</summary>
    public void CheckConfig()
    {
        lock (_lock)
        {
            Validators.CheckGroup(_consumerGroup);
            if (_consumerGroup == MixAll.DefaultConsumerGroup)
            {
                throw new MQClientException("consumerGroup can not equal " + MixAll.DefaultConsumerGroup
                                            + ", please specify another one.");
            }

            if (_messageModel != MessageModel.Clustering && _messageModel != MessageModel.Broadcasting)
            {
                throw new MQClientException("messageModel is invalid: " + _messageModel);
            }

            if (_allocateMessageQueueStrategy is null)
            {
                throw new MQClientException("allocateMessageQueueStrategy is null");
            }

            if (_consumerTimeoutMillisWhenSuspend < _brokerSuspendMaxTimeMillis)
            {
                throw new MQClientException("Long polling mode, the consumer consumerTimeoutMillisWhenSuspend"
                    + " must greater than brokerSuspendMaxTimeMillis");
            }
        }
    }

    public void Start()
    {
        lock (_lock)
        {
            switch (_serviceState)
            {
                case ServiceStateCreateJust:
                    break;
                case ServiceStateRunning:
                    throw ServiceStateError("RUNNING");
                case ServiceStateStartFailed:
                    throw ServiceStateError("START_FAILED");
                default:
                    throw ServiceStateError("SHUTDOWN_ALREADY");
            }

            _serviceState = ServiceStateStartFailed;
            if (_namespace.Length > 0)
            {
                _consumerGroup = NamespaceUtil.WrapNamespace(_namespace, _consumerGroup);
            }

            CheckConfig();

            if (_nameServerAddrs.Count == 0)
            {
                throw new MQClientException("name server address is not set");
            }

            ParseConsumeTimestamp(_consumeTimestamp);

            if (_messageModel == MessageModel.Clustering)
            {
                _instanceName = ClientIds.ChangeInstanceNameToPID(_instanceName);
            }
            if (_clientId.Length == 0)
            {
                _clientId = ClientIds.Build(_instanceName, _unitName, _enableStreamRequestType);
            }

            IRpcHook? requestHook = RequestHooks.Compose(_enableStreamRequestType, _rpcHook, () => _namespaceV2);
            _mqClient = MQClientManager.Instance.GetOrCreateMQClientInstance(_clientId,
                () => new MQClientInstance(_clientId, new List<string>(_nameServerAddrs),
                    /*connectTimeoutMillis=*/3000, /*invokeTimeoutMillis=*/10000,
                    socksProxy: _socksProxyConfig,
                    unitName: _unitName, pollNameServerIntervalMillis: _pollNameServerIntervalMillis));
            if (requestHook is not null && !_mqClient.RegisterRpcHook(requestHook))
            {
                ClientLog.Warn("lite pull consumer rpc hook ignored: MqClient already has one (clientId="
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
                // If startup fails, hand the reference back: otherwise the refcount
            // misses a decrement and the shared instance never reaches zero.
                MQClientManager.Instance.UnregisterMQClient(_clientId, _mqClient);
                _mqClient = null;
                throw;
            }

            if (_assignMode)
            {
                foreach (MessageQueue mq in _assigned)
                {
                    _mqClient.RegisterTopicInUse(mq.Topic);
                }
            }

            RefreshRouteForHeartbeat();
            SendHeartbeatToAllBroker();
            _running = true;
            _started = true;

            if (_assignMode)
            {
                foreach (MessageQueue mq in _assigned)
                {
                    if (_nextOffset.ContainsKey(mq)) continue;
                    try
                    {
                        _nextOffset[mq] = ResolveInitialOffset(mq);
                    }
                    catch
                    {
                        ClientLog.Debug("lite start: resolve initial offset failed for " + mq);
                    }
                }
            }

            _heartbeatThread = new Thread(HeartbeatLoop) { IsBackground = true, Name = "rmq-lite-hb-" + _clientId };
            _heartbeatThread.Start();
            _pullThread = new Thread(PullServiceLoop) { IsBackground = true, Name = "rmq-lite-pull-" + _consumerId() };
            _pullThread.Start();
            _metadataThread = new Thread(MetadataLoop)
                { IsBackground = true, Name = "rmq-lite-meta-" + _consumerId() };
            _metadataThread.Start();
            _serviceState = ServiceStateRunning;
        }
    }

    public void Shutdown()
    {
        bool autoCommit;
        List<MessageQueue> scope;
        lock (_lock)
        {
            _serviceState = ServiceStateShutdownAlready;
            if (!_started) return;
            _started = false;
            _running = false;
            autoCommit = _autoCommit;
            scope = new List<MessageQueue>(_assigned);
        }

        try
        {
            if (autoCommit) Commit();
            else PersistOffsetTable(scope);
        }
        catch
        {
            ClientLog.Debug("lite shutdown commit failed");
        }

        lock (_bufferLock)
        {
            Monitor.PulseAll(_bufferLock);
        }

        lock (_lock)
        {
            if (_pullThread is { IsAlive: true }) _pullThread.Join(2000);
            if (_heartbeatThread is { IsAlive: true }) _heartbeatThread.Join(2000);
            if (_metadataThread is { IsAlive: true }) _metadataThread.Join(2000);
            if (_mqClient is not null)
            {
                MQClientManager.Instance.UnregisterMQClient(_clientId, _mqClient);
                _mqClient = null;
            }
        }
    }

    // ---------------- pull service ----------------
    private void PullServiceLoop()
    {
        DateTime lastRebalance = DateTime.MinValue;
        while (_running)
        {
            bool gotAny = false;
            try
            {
                DateTime now = DateTime.UtcNow;
                if (!_assignMode &&
                    (lastRebalance == DateTime.MinValue || (now - lastRebalance).TotalMilliseconds > 1000))
                {
                    Rebalance();
                    lastRebalance = now;
                }

                List<MessageQueue> snapshot;
                lock (_lock)
                {
                    snapshot = new List<MessageQueue>(_assigned);
                }

                foreach (MessageQueue mq in snapshot)
                {
                    if (!_running) break;
                    bool paused;
                    lock (_lock)
                    {
                        paused = _paused.Contains(mq);
                    }

                    if (paused) continue;

                    // A refusal parks only this queue until its own deadline; skipping just
                    // this entry keeps the rest of the round running at full speed.
                    if (IsParked(mq)) continue;

                    // The four cache gates judge this queue before its pull offset is
                    // resolved and before any request is built: a refused round sends
                    // nothing.
                    if (CacheFlowControlHit(mq)) continue;

                    if (PullOne(mq)) gotAny = true;
                }
            }
            catch
            {
                ClientLog.Debug("lite pull service error");
            }

            int backoffMs = gotAny ? 5 : _pullIntervalMillis;
            SleepInterruptibly(backoffMs);
        }
    }

    /// <summary>The round's wait, sliced so shutdown cuts it short: the loop keeps the
    /// Thread.Sleep idiom but never sits in one uninterruptible sleep while
    /// <see cref="Shutdown"/> waits for the thread.</summary>
    private void SleepInterruptibly(int totalMillis)
    {
        int left = totalMillis;
        while (left > 0 && _running)
        {
            int slice = Math.Min(10, left);
            Thread.Sleep(slice);
            left -= slice;
        }
    }

    /// <summary>Whether this queue's park is still counting down.</summary>
    private bool IsParked(MessageQueue mq)
    {
        lock (_lock)
        {
            if (!_pullPause.TryGetValue(mq, out PullPause? pause)) return false;
            return MonoClock.ElapsedMilliseconds < pause.DeadlineMillis;
        }
    }

    /// <summary>Re-schedule only this queue's next round: the deadline is monotonic, so a
    /// parked queue neither stalls its siblings nor depends on the wall clock.</summary>
    private void ParkQueue(MessageQueue mq, int millis)
    {
        lock (_lock)
        {
            _pullPause[mq] = new PullPause(MonoClock.ElapsedMilliseconds + millis, millis);
        }
    }

    // ---------------- local-buffer flow control ----------------
    /// <summary>The four budgets the pull round checks before it builds a request, judged
    /// in this order (an earlier hit masks the later ones), every comparison
    /// <b>strictly greater than</b> so a queue sitting exactly on a threshold still
    /// pulls:</summary>
    /// <list type="number">
    /// <item>this instance's pending pull rounds, each reserved with a whole
    /// <c>pullBatchSize</c>, over <see cref="PullThresholdForAll"/></item>
    /// <item>this queue's cached count over <see cref="PullThresholdForQueue"/></item>
    /// <item>this queue's cached <b>body</b> MiB (integer division) over
    /// <see cref="PullThresholdSizeForQueue"/></item>
    /// <item>this queue's cached offset span over <see cref="ConsumeMaxSpan"/></item>
    /// </list>
    /// Gate 1 is a capacity projection rather than a sum of the buffer: a round that came
    /// back with two messages still reserves a whole batch, because the application has not
    /// drained it yet. Its warn states the threshold in the unit it is written in, so it
    /// prints how many rounds fit (ceiling division) and the round count.
    /// Gates 2 to 4 read <b>this queue's own accumulator only</b>, so a sibling over
    /// budget never stops this queue, and no gate ever scans the buffer. A queue this
    /// instance no longer owns cannot refuse a pull even if its parked accumulator still
    /// describes messages the application has not polled away. One refusal increments its
    /// counter by exactly one and parks this queue for
    /// <see cref="CacheFlowControlBackoffMillis"/>; count and size share one counter, the
    /// aggregate and the span each have their own.</summary>
    private bool CacheFlowControlHit(MessageQueue mq)
    {
        // Ownership first: a revoked queue's accumulator must never refuse a pull.
        lock (_lock)
        {
            if (!_assigned.Contains(mq)) return false;
        }

        long rounds;
        int count;
        long sizeBytes;
        long minOffset;
        long maxOffset;
        long batch = Math.Max(1, _pullBatchSize);
        lock (_bufferLock)
        {
            rounds = 0;
            foreach (QueueCache any in _queueCaches.Values)
            {
                rounds += any.PendingRounds;
            }

            if (_queueCaches.TryGetValue(mq, out QueueCache? cache))
            {
                count = cache.Count;
                sizeBytes = cache.SizeBytes;
                minOffset = cache.MinOffset;
                maxOffset = cache.MaxOffset;
            }
            else
            {
                count = 0;
                sizeBytes = 0;
                minOffset = 0;
                maxOffset = 0;
            }
        }

        // The byte budget is whole MiB, so the division is integer: 1 MiB - 1 byte of
        // backlog is 0 MiB and stays inside a threshold of 1.
        long sizeMiB = sizeBytes / MiB;
        long span = maxOffset - minOffset;
        long reserved = rounds * batch;

        if (reserved > _pullThresholdForAll)
        {
            // Ceiling division: the threshold is stated in messages, the gate counts rounds.
            long fits = (_pullThresholdForAll + batch - 1) / batch;
            long times = Interlocked.Increment(ref _flowControlForAllTriggered);
            Refuse(mq, times,
                "the consume request count exceeds threshold " + Num(fits)
                + ", so do flow control, consume request count=" + Num(rounds)
                + ", flowControlTimes=" + Num(times),
                "rounds=" + Num(rounds) + " x pullBatchSize=" + Num(batch)
                + " = " + Num(reserved) + " over pullThresholdForAll="
                + Num(_pullThresholdForAll));
            return true;
        }

        if (count > _pullThresholdForQueue)
        {
            string measure = "minOffset=" + Num(minOffset) + ", maxOffset=" + Num(maxOffset)
                             + ", count=" + Num(count) + ", size=" + Num(sizeMiB) + " MiB";
            long times = Interlocked.Increment(ref _flowControlForQueueTriggered);
            Refuse(mq, times,
                "The cached message count exceeds the threshold " + Num(_pullThresholdForQueue)
                + ", so do flow control, " + measure + ", flowControlTimes=" + Num(times),
                "count=" + Num(count) + " over pullThresholdForQueue=" + Num(_pullThresholdForQueue));
            return true;
        }

        if (sizeMiB > _pullThresholdSizeForQueue)
        {
            string measure = "minOffset=" + Num(minOffset) + ", maxOffset=" + Num(maxOffset)
                             + ", count=" + Num(count) + ", size=" + Num(sizeMiB) + " MiB";
            long times = Interlocked.Increment(ref _flowControlForQueueTriggered);
            Refuse(mq, times,
                "The cached message size exceeds the threshold " + Num(_pullThresholdSizeForQueue)
                + " MiB, so do flow control, " + measure + ", flowControlTimes=" + Num(times),
                "size=" + Num(sizeMiB) + " MiB over pullThresholdSizeForQueue="
                + Num(_pullThresholdSizeForQueue));
            return true;
        }

        if (span > _consumeMaxSpan)
        {
            long times = Interlocked.Increment(ref _flowControlForSpanTriggered);
            Refuse(mq, times,
                "The queue's messages, span too long, so do flow control, minOffset="
                + Num(minOffset) + ", maxOffset=" + Num(maxOffset) + ", maxSpan=" + Num(span)
                + ", flowControlTimes=" + Num(times),
                "span=" + Num(span) + " over consumeMaxSpan=" + Num(_consumeMaxSpan));
            return true;
        }

        return false;
    }

    /// <summary>Record one refusal: its counter is already advanced, so this only writes
    /// the log lines and parks the queue. Debug always carries the queue, the measured
    /// value, the threshold and the counter; warn follows the shared 1/1001 cadence.</summary>
    private void Refuse(MessageQueue mq, long times, string warn, string measured)
    {
        ClientLog.Debug("lite cache flow control: queue " + mq + " " + measured
            + " (flowControlTimes=" + Num(times) + "), pause pull for "
            + Num(CacheFlowControlBackoffMillis) + " ms");
        if (IsFlowControlWarnTrip(times))
        {
            ClientLog.Warn("lite cache flow control: queue " + mq + " " + warn);
        }

        ParkQueue(mq, CacheFlowControlBackoffMillis);
    }

    private static string Num(long value) => value.ToString(CultureInfo.InvariantCulture);

    /// <summary>The one place the buffer and its per-queue view change together: append
    /// after a pull round found messages. One round is booked <b>per queue this round
    /// touched</b>, next to the rows it brought back, so the aggregate gate can budget the
    /// round rather than the messages that happened to come with it.</summary>
    private void Enqueue(IReadOnlyList<MessageExt> msgs)
    {
        lock (_bufferLock)
        {
            var perQueue = new Dictionary<MessageQueue, int>();
            foreach (MessageExt m in msgs)
            {
                MessageQueue key = QueueKeyOf(m);
                perQueue.TryGetValue(key, out int seen);
                perQueue[key] = seen + 1;
            }

            foreach (MessageExt m in msgs)
            {
                MessageQueue key = QueueKeyOf(m);
                _localBuffer.Enqueue(m);
                QueueCache cache = CacheOf(key);
                cache.Append(m);
                // The round is booked once, on the first row of it that lands in this queue.
                if (perQueue[key] > 0)
                {
                    cache.BeginRound(perQueue[key]);
                    perQueue[key] = 0;
                }
            }

            Monitor.PulseAll(_bufferLock);
        }
    }

    /// <summary>The one place they change together on the way out: a poll drains the
    /// buffer head and its queue's accumulator with it.</summary>
    private MessageExt DequeueBuffered()
    {
        MessageExt m = _localBuffer.Dequeue();
        RemoveFromCache(QueueKeyOf(m), m);
        return m;
    }

    private static MessageQueue QueueKeyOf(MessageExt m) => new(m.Topic, m.BrokerName, m.QueueId);

    /// <summary>Caller holds <c>_bufferLock</c>.</summary>
    private QueueCache CacheOf(MessageQueue key)
    {
        if (!_queueCaches.TryGetValue(key, out QueueCache? cache))
        {
            cache = new QueueCache();
            _queueCaches[key] = cache;
        }

        return cache;
    }

    /// <summary>Caller holds <c>_bufferLock</c>. A queue's view exists exactly as long as
    /// the buffer holds its messages: once the last one is polled away the entry goes with
    /// it, so nothing a queue no longer owns is left behind to describe a pull.</summary>
    private void RemoveFromCache(MessageQueue key, MessageExt m)
    {
        if (!_queueCaches.TryGetValue(key, out QueueCache? cache)) return;
        cache.Remove(m);
        if (cache.IsEmpty) _queueCaches.Remove(key);
    }

    /// <summary>The one way a queue leaves this instance's assignment: its cached rows go
    /// with it, and its whole accumulator (count / body bytes / offsets / <b>pending
    /// rounds</b>) with them. Both ways out of an assignment -- the explicit
    /// <see cref="Assign"/> narrowing and a rebalance round -- come through here, which is
    /// what keeps those paths from drifting apart. A revoked queue must neither reach the
    /// caller through <see cref="Poll"/> nor keep reserving aggregate budget for rounds the
    /// application will never be handed. A sibling's accumulator is not touched, so its own
    /// pending rounds survive losing a neighbour's rows.</summary>
    private void ForgetQueues(IEnumerable<MessageQueue> dropped)
    {
        var gone = new HashSet<MessageQueue>(dropped);
        if (gone.Count == 0) return;
        lock (_bufferLock)
        {
            bool touched = false;
            foreach (MessageQueue mq in gone)
            {
                if (_queueCaches.Remove(mq)) touched = true;
            }

            var kept = new List<MessageExt>(_localBuffer.Count);
            foreach (MessageExt m in _localBuffer)
            {
                if (gone.Contains(QueueKeyOf(m)))
                {
                    touched = true;
                    continue;
                }

                kept.Add(m);
            }

            if (!touched) return;

            _localBuffer.Clear();
            foreach (MessageExt m in kept)
            {
                _localBuffer.Enqueue(m);
            }

            Monitor.PulseAll(_bufferLock);
        }
    }

    /// <summary>Caller holds <c>_lock</c>. Drops every per-queue decision this instance
    /// made: both cursors, the paused flag, the parked deadline and the seek pin. The
    /// buffered rows and their accumulator live under <c>_bufferLock</c> and go through
    /// <see cref="ForgetQueues"/>.</summary>
    private void ForgetQueueStateLocked(MessageQueue mq)
    {
        _nextOffset.Remove(mq);
        _consumeOffset.Remove(mq);
        _seekOffset.Remove(mq);
        _paused.Remove(mq);
        _pullPause.Remove(mq);
    }

    /// <summary>Removes a topic's subscription and every queue of that topic this
    /// instance holds, dropping all per-queue state immediately instead of waiting for
    /// the next rebalance round.</summary>
    public void Unsubscribe(string topic)
    {
        string ns = WithNamespace(topic);
        var dropped = new List<MessageQueue>();
        lock (_lock)
        {
            _subscription.Remove(ns);
            _subscriptionData.Remove(ns);
            _subscriptionType.Remove(ns);
            // The assign expression table survives: it is not a rebalance subscription
            // and an unsubscribe only revokes the topic's queues and their state.
            foreach (MessageQueue mq in _assigned)
            {
                if (mq.Topic != ns) continue;
                ForgetQueueStateLocked(mq);
                dropped.Add(mq);
            }

            foreach (MessageQueue mq in dropped) _assigned.Remove(mq);
        }

        ForgetQueues(dropped);
    }

    private string SubscriptionFor(string topic)
    {
        lock (_lock)
        {
            // Assign mode never reads the subscribe table: an assigned queue's filter is
            // whatever the assign expression says, defaulting to "all".
            if (_mode == ModeAssign)
            {
                return _assignSubExpr.TryGetValue(topic, out string? a) ? a : "*";
            }

            if (_subscription.TryGetValue(topic, out string? s)) return s;
        }

        return "*";
    }

    /// <summary>The expression type recorded for this topic; a topic without a selector
    /// entry is TAG, which is every string-based subscription's type today. Assign-mode
    /// expressions are always TAG filters.</summary>
    private string SubscriptionTypeFor(string topic)
    {
        lock (_lock)
        {
            if (_mode == ModeAssign) return ExpressionType.TAG;
            return _subscriptionType.TryGetValue(topic, out string? t) ? t : ExpressionType.TAG;
        }
    }

    private bool PullOne(MessageQueue mq)
    {
        long offset;
        lock (_lock)
        {
            offset = _nextOffset.TryGetValue(mq, out long o) ? o : ResolveInitialOffset(mq);
            _nextOffset[mq] = offset;
        }

        string sub = SubscriptionFor(mq.Topic);
        string exprType = SubscriptionTypeFor(mq.Topic);
        // A TAG subscription travels with subVersion 0 exactly as today; only an
        // expression with its own version (SQL92 / class filter) sends the real one.
        long subVersion = 0;
        if (exprType != ExpressionType.TAG)
        {
            lock (_lock)
            {
                if (_subscriptionData.TryGetValue(mq.Topic, out SubscriptionData? d)
                    && d is not null)
                {
                    subVersion = d.SubVersion;
                }
            }
        }

        int sysFlag = PullSysFlag.BuildSysFlag(commitOffset: false, suspend: false,
            subscription: true, classFilter: false, litePull: true);
        PullResult result;
        try
        {
            // Short poll, but the suspend field still carries the configured broker-suspend
            // budget: the request is short because this loop must answer its caller, not
            // because the budget is a constant.
            result = RequireClient().PullMessage(_consumerGroup, mq, offset, _pullBatchSize, sysFlag, 0,
                sub, subVersion, exprType, /*timeoutMillis=*/30000, /*maxMsgBytes=*/-1,
                /*suspendTimeoutMillis=*/_brokerSuspendMaxTimeMillis);
        }
        catch (MQBrokerException e)
        {
            // A broker FLOW_CONTROL(215) on this path is a "busy right now" answer, not a
            // capacity verdict on this client: park this one queue for the short broker
            // pause instead of letting the round's generic backoff decide. Any other code
            // keeps the failure handling this path already had.
            int pauseMillis = LitePullPauseMillisForBrokerResponse(e.ResponseCode);
            if (pauseMillis > 0)
            {
                ParkQueue(mq, pauseMillis);
            }

            ClientLog.Debug("lite pull_one failed for " + mq + ", code="
                + Num(e.ResponseCode) + ", next round in "
                + (pauseMillis > 0 ? Num(pauseMillis) + " ms" : "the regular pull interval"));
            return false;
        }
        catch
        {
            ClientLog.Debug("lite pull_one failed for " + mq);
            return false;
        }

        bool intact = false;
        lock (_lock)
        {
            if (_nextOffset.TryGetValue(mq, out long current) && current == offset)
            {
                _nextOffset[mq] = result.NextBeginOffset;
                intact = true;
            }
        }

        if (!intact || result.Status != PullStatus.Found || result.MsgFoundList.Count == 0)
        {
            return false;
        }

        var msgs = new List<MessageExt>(result.MsgFoundList);
        // Client-side tag re-checking only makes sense for a TAG subscription: an SQL92 /
        // class-filter expression is not a tag list and would filter everything out.
        if (exprType == ExpressionType.TAG)
        {
            FilterTags(mq.Topic, msgs, sub);
        }
        if (msgs.Count == 0)
        {
            // All filtered out by tags: the window is already read as far as the broker
            // is concerned (the cursor advanced past it), there is just nothing
            // deliverable — unlike the old implementation, do not re-pull the same window.
            return false;
        }

        Enqueue(msgs);
        return true;
    }

    private static void FilterTags(string topic, List<MessageExt> msgs, string sub)
    {
        if (string.IsNullOrEmpty(sub) || sub == "*") return;
        SubscriptionData subData;
        try
        {
            subData = FilterAPI.BuildSubscriptionData(topic, sub);
        }
        catch
        {
            return;
        }

        if (subData.TagsSet.Count == 0) return;
        msgs.RemoveAll(m => !subData.TagsSet.Contains(m.Tags));
    }

    private void MaybeAutoCommit()
    {
        long now = UtilAll.CurrentTimeMillis();
        lock (_lock)
        {
            if (now < _nextAutoCommitDeadline) return;
            _nextAutoCommitDeadline = now + _autoCommitIntervalMillis;
        }

        try
        {
            Commit();
        }
        catch
        {
            ClientLog.Debug("lite auto-commit failed");
        }
    }

    private void AdvanceConsumeOffset(IReadOnlyList<MessageExt> msgs)
    {
        if (msgs.Count == 0) return;
        var held = new Dictionary<(string, string, int), MessageQueue>();
        lock (_lock)
        {
            foreach (KeyValuePair<MessageQueue, long> kv in _nextOffset)
            {
                held[(kv.Key.Topic, kv.Key.BrokerName, kv.Key.QueueId)] = kv.Key;
            }
        }

        var advanced = new Dictionary<MessageQueue, long>();
        foreach (MessageExt m in msgs)
        {
            if (!held.TryGetValue((m.Topic, m.BrokerName, m.QueueId), out MessageQueue? mq)) continue;
            long next = m.QueueOffset + 1;
            if (!advanced.TryGetValue(mq, out long cur) || next > cur) advanced[mq] = next;
        }

        lock (_lock)
        {
            foreach (KeyValuePair<MessageQueue, long> kv in advanced)
            {
                if (kv.Value > (_consumeOffset.TryGetValue(kv.Key, out long cur) ? cur : -1))
                {
                    _consumeOffset[kv.Key] = kv.Value;
                }
            }
        }
    }

    /// <summary>The pull cursor (observation point: what real-machine parity checks want is
    /// exactly the gap between "how much was pulled" and "how much was consumed").</summary>
    public long PullCursorOf(MessageQueue mq)
    {
        lock (_lock)
        {
            return _nextOffset.TryGetValue(mq, out long o) ? o : -1;
        }
    }

    /// <summary>The consume cursor (what Poll() has handed out).</summary>
    public long ConsumeCursorOf(MessageQueue mq)
    {
        lock (_lock)
        {
            return _consumeOffset.TryGetValue(mq, out long o) ? o : -1;
        }
    }

    /// <summary>The in-memory offset table entry (where persist=false commits live until flushed).</summary>
    public long PendingCommitOf(MessageQueue mq)
    {
        lock (_lock)
        {
            return _offsetTable.TryGetValue(mq, out long o) ? o : -1;
        }
    }

    private long ResolveInitialOffset(MessageQueue mq)
    {
        lock (_lock)
        {
            if (_seekOffset.TryGetValue(mq, out long s)) return s;
        }

        MQClientInstance client = RequireClient();

        if (_messageModel == MessageModel.Broadcasting)
        {
            if (DefaultMQPushConsumer.LoadLocalOffsetsAt(LocalOffsetPath())
                .TryGetValue(DefaultMQPushConsumer.LocalOffsetKey(mq), out long local) && local >= 0)
            {
                return local;
            }
        }
        else
        {
            try
            {
                if (client.QueryConsumerOffset(_consumerGroup, mq, out long off))
                {
                    return off;
                }
            }
            catch
            {
            }
        }

        if (_consumeFromWhere == ConsumeFromWhere.ConsumeFromFirstOffset)
        {
            return 0;
        }

        if (_consumeFromWhere == ConsumeFromWhere.ConsumeFromTimestamp)
        {
            return client.SearchOffsetByTimestamp(mq, ParseConsumeTimestamp(_consumeTimestamp));
        }

        return client.GetMaxOffset(mq);
    }

    // ---------------- rebalance ----------------
    private void Rebalance()
    {
        var newSet = new HashSet<MessageQueue>();
        List<string> topics;
        lock (_lock)
        {
            topics = new List<string>(_subscription.Keys);
        }

        foreach (string topic in topics)
        {
            List<MessageQueue> mqAll = RequireClient().GetTopicSubscribeInfo(topic);

            List<string>? cidAll = RequireClient().GetConsumerIdListByGroup(topic, _consumerGroup);
            if (cidAll is null) cidAll = new List<string>();
            if (!cidAll.Contains(_clientId)) cidAll.Add(_clientId);
            mqAll.Sort();
            cidAll.Sort(StringComparer.Ordinal);
            List<MessageQueue> allocated;
            try
            {
                allocated = _allocateMessageQueueStrategy!.Allocate(_consumerGroup, _clientId, mqAll, cidAll);
            }
            catch (Exception e)
            {
                ClientLog.Error("allocate message queue exception. strategy name: "
                    + _allocateMessageQueueStrategy!.GetName() + ", ex: " + e.Message);
                allocated = new List<MessageQueue>();
                lock (_lock)
                {
                    foreach (MessageQueue mq in _assigned)
                    {
                        if (mq.Topic == topic)
                        {
                            allocated.Add(mq);
                        }
                    }
                }
            }

            foreach (MessageQueue mq in allocated) newSet.Add(mq);
        }

        HashSet<MessageQueue> old;
        bool changed;
        List<MessageQueue> revoked = new();
        List<MessageQueue> dropped = new();
        lock (_lock)
        {
            old = new HashSet<MessageQueue>(_assigned);
            changed = !newSet.SetEquals(old);
            if (changed)
            {
                _assigned.Clear();
                foreach (MessageQueue mq in newSet) _assigned.Add(mq);
                foreach (MessageQueue mq in newSet)
                {
                    if (!_nextOffset.ContainsKey(mq))
                    {
                        try
                        {
                            _nextOffset[mq] = ResolveInitialOffset(mq);
                        }
                        catch
                        {
                            ClientLog.Debug("lite rebalance: resolve offset failed for " + mq);
                        }
                    }
                }

                foreach (MessageQueue mq in old)
                {
                    if (newSet.Contains(mq)) continue;
                    if (_offsetTable.ContainsKey(mq)) revoked.Add(mq);
                    // Revoking a queue drops its **entire** per-queue state (pullOffset /
                    // consumeOffset / seekOffset), its paused flag and its park: keeping the
                    // seekOffset would silently jump back to a position the user pinned long
                    // ago whenever this queue returns to this instance.
                    ForgetQueueStateLocked(mq);
                    dropped.Add(mq);
                }
            }
        }

        // What the assignment narrowed away is also removed from the pollable buffer, with
        // its accumulator and its pending rounds (see ForgetQueues).
        ForgetQueues(dropped);

        foreach (MessageQueue mq in revoked)
        {
            PersistOffset(mq);
            lock (_lock)
            {
                _offsetTable.Remove(mq);
            }
        }

        if (changed && _messageQueueListener is not null)
        {
            try
            {
                List<MessageQueue> all = MqAllOfSubscription();
                _messageQueueListener.MessageQueueChanged(all, new List<MessageQueue>(newSet));
            }
            catch
            {
            }
        }
    }

    private List<MessageQueue> MqAllOfSubscription()
    {
        var all = new List<MessageQueue>();
        List<string> topics;
        lock (_lock)
        {
            topics = new List<string>(_subscription.Keys);
        }

        foreach (string topic in topics)
        {
            // Same basis as rebalance: subscription info.
            all.AddRange(RequireClient().GetTopicSubscribeInfo(topic));
        }

        return all;
    }

    // ---------------- Poll / offsets ----------------
    /// <summary>Hand back the buffered messages, waiting at most timeoutMillis.
    /// A consumer that never started refuses: an empty poll is exactly what a queue
    /// sitting at its tail looks like, which is how a missing Start() stays hidden.
    /// The state check comes before the argument check, so an unstarted consumer is
    /// reported as that rather than as a timeout problem.</summary>
    public List<MessageExt> Poll(int? timeoutMillis = null)
    {
        if (!_running)
        {
            throw new MQClientException("The consumer not running, please start it first.");
        }

        if (timeoutMillis < 0)
        {
            throw new ArgumentException("Timeout must not be negative");
        }

        if (_autoCommit) MaybeAutoCommit();
        int timeout = timeoutMillis > 0 ? timeoutMillis.Value : _pollTimeoutMillis;
        long deadline = UtilAll.CurrentTimeMillis() + timeout;
        List<MessageExt> outMsgs;
        lock (_bufferLock)
        {
            while (_localBuffer.Count == 0)
            {
                long remaining = deadline - UtilAll.CurrentTimeMillis();
                if (remaining <= 0) return new List<MessageExt>();
                Monitor.Wait(_bufferLock, (int)Math.Min(remaining, int.MaxValue - 1));
            }

            outMsgs = new List<MessageExt>();
            while (_localBuffer.Count > 0 && outMsgs.Count < 1024)
            {
                // Every message leaves the buffer and its queue's accumulator in the same
                // step, so the per-queue view keeps describing exactly what is cached.
                outMsgs.Add(DequeueBuffered());
            }
        }

        AdvanceConsumeOffset(outMsgs);
        return outMsgs;
    }

    public void Seek(MessageQueue mq, long offset)
    {
        bool assigned;
        bool assignMode;
        lock (_lock)
        {
            assigned = _assigned.Contains(mq);
            assignMode = _mode == ModeAssign;
        }

        // Membership is judged first: a queue this instance does not own is a rebalancing
        // situation in subscribe mode and a plain assignment mistake in assign mode. Both
        // are reported before the running state, so the caller learns the more useful fact.
        if (!assigned)
        {
            string message = assignMode
                ? "The message queue is not in assigned list, message queue: " + mq
                : "The message queue is not in assigned list, may be rebalancing, message queue: " + mq;
            throw new MQClientException(message);
        }

        if (!_running)
        {
            throw new MQClientException("The consumer not running, please start it first.");
        }

        (long minOffset, long maxOffset) = ReadSeekBounds(mq);
        if (offset < minOffset || offset > maxOffset)
        {
            throw new MQClientException("Seek offset illegal, seek offset = " + offset
                + ", min offset = " + minOffset + ", max offset = " + maxOffset);
        }

        lock (_lock)
        {
            _seekOffset[mq] = offset;
            _nextOffset[mq] = offset;
            _consumeOffset[mq] = offset;
        }

        lock (_bufferLock)
        {
            var kept = new List<MessageExt>(_localBuffer.Count);
            foreach (MessageExt m in _localBuffer)
            {
                bool drop = m.Topic == mq.Topic && m.BrokerName == mq.BrokerName &&
                            m.QueueId == mq.QueueId && m.QueueOffset < offset;
                if (drop)
                {
                    // Pruning is a buffer removal like any other, so it goes through the same
                    // accumulator step: what the gates read afterwards is what the buffer really
                    // holds after the seek, and the round the pruned rows came in with is
                    // released exactly as a poll would release it.
                    RemoveFromCache(QueueKeyOf(m), m);
                    continue;
                }

                kept.Add(m);
            }

            _localBuffer.Clear();
            foreach (MessageExt m in kept)
            {
                _localBuffer.Enqueue(m);
            }

            Monitor.PulseAll(_bufferLock);
        }
    }

    /// <summary>Reads the queue's current boundaries in the fixed order (min, then max).
    /// A failed read is not turned into a sentinel: the exception reaches the caller so a
    /// seek is never pinned to an offset the broker never confirmed.</summary>
    private (long Min, long Max) ReadSeekBounds(MessageQueue mq)
    {
        Func<MessageQueue, (long Min, long Max)>? boundsForTest = _seekBoundsForTest;
        if (boundsForTest is not null)
        {
            return boundsForTest(mq);
        }

        MQClientInstance client = RequireClient();
        long min = client.GetMinOffset(mq);
        long max = client.GetMaxOffset(mq);
        return (min, max);
    }

    public void SeekToBegin(MessageQueue mq)
    {
        long begin = RequireClient().GetMinOffset(mq);
        Seek(mq, begin);
    }

    public void SeekToEnd(MessageQueue mq)
    {
        long end = RequireClient().GetMaxOffset(mq);
        Seek(mq, end);
    }

    public long Committed(MessageQueue mq)
    {
        if (!_running)
        {
            throw new MQClientException("The consumer not running, please start it first.");
        }

        lock (_lock)
        {
            if (_offsetTable.TryGetValue(mq, out long cached)) return cached;
        }

        MQClientInstance? client = _mqClient;
        if (client is null) return -1;

        if (_messageModel == MessageModel.Broadcasting)
        {
            if (!DefaultMQPushConsumer.LoadLocalOffsetsAt(LocalOffsetPath())
                .TryGetValue(DefaultMQPushConsumer.LocalOffsetKey(mq), out long local))
            {
                return -1;
            }

            lock (_lock)
            {
                _offsetTable[mq] = local;
            }

            return local;
        }

        long off;
        try
        {
            if (!client.QueryConsumerOffset(_consumerGroup, mq, out off)) return -1;
        }
        catch
        {
            // A failed broker read is not "no offset": -1 would silently reset the
            // caller's view. Surface it as the offset-store failure it is.
            throw new MQClientException("Fetch consume offset from broker exception");
        }

        lock (_lock)
        {
            _offsetTable[mq] = off;
        }

        return off;
    }

    /// The commit source must never be the pull cursor: messages still sitting in the local
    /// buffer are not consumed, and committing early would make them never delivered again
    /// after a restart (silent message loss).
    /// </summary>
    public void Commit()
    {
        List<MessageQueue> scope;
        Dictionary<MessageQueue, long> targets = new();
        lock (_lock)
        {
            scope = new List<MessageQueue>(_assigned);
            foreach (MessageQueue mq in scope)
            {
                targets[mq] = _consumeOffset.TryGetValue(mq, out long o) ? o : -1;
            }
        }

        CommitTargets(targets, scope, persist: true);
    }

    public void Commit(IReadOnlyDictionary<MessageQueue, long> offsets, bool persist = true)
    {
        if (offsets.Count == 0)
        {
            ClientLog.Warn("MessageQueues is empty, Ignore this commit ");
            return;
        }

        var targets = new Dictionary<MessageQueue, long>(offsets);
        var scope = new List<MessageQueue>(targets.Keys);
        CommitTargets(targets, scope, persist);
    }

    public void Commit(IReadOnlyCollection<MessageQueue> messageQueues, bool persist = true)
    {
        if (messageQueues.Count == 0) return;
        var scope = new List<MessageQueue>(messageQueues);
        var targets = new Dictionary<MessageQueue, long>();
        lock (_lock)
        {
            foreach (MessageQueue mq in scope)
            {
                targets[mq] = _consumeOffset.TryGetValue(mq, out long o) ? o : -1;
            }
        }

        CommitTargets(targets, scope, persist);
    }

    /// <summary>
    /// The shared part of the three entry points: writes the in-memory offset table (two
    /// guards), and <c>persist</c> flushes this batch to the broker.
    private void CommitTargets(Dictionary<MessageQueue, long> targets, List<MessageQueue> scope,
        bool persist)
    {
        lock (_lock)
        {
            foreach (KeyValuePair<MessageQueue, long> kv in targets)
            {
                if (kv.Value == -1)
                {
                    ClientLog.Error("consumerOffset is -1 in messageQueue [" + kv.Key + "].");
                    continue;
                }

                if (!_assigned.Contains(kv.Key))
                {
                    continue;
                }

                _offsetTable[kv.Key] = kv.Value;
            }
        }

        if (persist) PersistOffsetTable(scope);
    }

    private void PersistOffset(MessageQueue mq)
    {
        long offset;
        lock (_lock)
        {
            if (!_offsetTable.TryGetValue(mq, out offset)) return;
        }

        MQClientInstance? client = _mqClient;
        if (client is null) return;
        PersistToStore(client, new List<KeyValuePair<MessageQueue, long>>
        {
            new(mq, offset),
        });
    }

    private void PersistOffsetTable(ICollection<MessageQueue> mqs)
    {
        if (mqs.Count == 0) return;
        var wanted = new HashSet<MessageQueue>(mqs);
        var toSend = new List<KeyValuePair<MessageQueue, long>>();
        lock (_lock)
        {
            foreach (MessageQueue mq in new List<MessageQueue>(_offsetTable.Keys))
            {
                if (!wanted.Contains(mq))
                {
                    _offsetTable.Remove(mq);
                    continue;
                }

                toSend.Add(new KeyValuePair<MessageQueue, long>(mq, _offsetTable[mq]));
            }
        }

        MQClientInstance? client = _mqClient;
        if (client is null) return;
        PersistToStore(client, toSend);
    }

    /// <summary>Broadcasting owns the local file; clustering commits to the broker.</summary>
    private void PersistToStore(MQClientInstance client,
        IReadOnlyList<KeyValuePair<MessageQueue, long>> items)
    {
        if (items.Count == 0) return;

        if (_messageModel == MessageModel.Broadcasting)
        {
            var offsets = new Dictionary<string, long>(StringComparer.Ordinal);
            var mqMap = new Dictionary<string, MessageQueue>(StringComparer.Ordinal);
            foreach (KeyValuePair<MessageQueue, long> kv in items)
            {
                string key = DefaultMQPushConsumer.LocalOffsetKey(kv.Key);
                offsets[key] = kv.Value;
                mqMap[key] = kv.Key;
            }

            DefaultMQPushConsumer.SaveLocalOffsetsAt(LocalOffsetPath(), offsets, mqMap);
            return;
        }

        foreach (KeyValuePair<MessageQueue, long> kv in items)
        {
            try
            {
                client.UpdateConsumerOffset(_consumerGroup, kv.Key, kv.Value);
            }
            catch
            {
                ClientLog.Debug("lite persist failed for " + kv.Key);
            }
        }
    }

    private string LocalOffsetPath() =>
        LocalOffsetPathForTest ?? DefaultMQPushConsumer.BuildLocalOffsetPath(_clientId, _consumerGroup);

    public long OffsetForTimestamp(MessageQueue mq, long timestamp) =>
        RequireClient().SearchOffsetByTimestamp(mq, timestamp);

    private List<MessageQueue> FetchMessageQueuesFull(string fullTopic)
    {
        MQClientInstance client = RequireClient();
        try
        {
            client.UpdateTopicRouteInfoFromNameServer(fullTopic);
        }
        catch (Exception e)
        {
            // On nameserver hiccups, fall back to the current cache and check once more:
            // if the cache still has queues, it should not be reported as "no queues".
            ClientLog.Debug("fetch queues: route refresh for " + fullTopic + " failed: " + e.Message);
        }

        List<MessageQueue> queues = client.GetTopicSubscribeInfo(fullTopic);
        if (queues.Count == 0)
        {
            throw new MQClientException("Can not find Message Queue for this topic, "
                                        + fullTopic + " Namesrv return empty");
        }

        return queues;
    }

    public List<MessageQueue> FetchMessageQueues(string topic) =>
        FetchMessageQueuesFull(WithNamespace(topic));

    public List<MessageQueue> Assignment()
    {
        lock (_lock)
        {
            return new List<MessageQueue>(_assigned);
        }
    }

    public void Pause(IReadOnlyList<MessageQueue> messageQueues)
    {
        lock (_lock)
        {
            foreach (MessageQueue mq in messageQueues) _paused.Add(mq);
        }
    }

    public void Resume(IReadOnlyList<MessageQueue> messageQueues)
    {
        lock (_lock)
        {
            foreach (MessageQueue mq in messageQueues) _paused.Remove(mq);
        }
    }

    public void RegisterTopicMessageQueueChangeListener(string topic,
        ITopicMessageQueueChangeListener? listener)
    {
        if (string.IsNullOrEmpty(topic) || listener is null)
        {
            throw new MQClientException("Topic or listener is null");
        }

        string key = WithNamespace(topic);
        bool started;
        lock (_lock)
        {
            if (_topicChangeListeners.ContainsKey(key))
            {
                ClientLog.Warn("Topic " + key + " had been registered, "
                               + "new listener will overwrite the old one");
            }

            _topicChangeListeners[key] = listener;
            started = _started;
        }

        if (!started)
        {
            return;
        }

        try
        {
            List<MessageQueue> snapshot = FetchMessageQueuesFull(key);
            lock (_lock)
            {
                _messageQueuesForTopic[key] = snapshot;
            }
        }
        catch (Exception e)
        {
            ClientLog.Debug("register listener: fetch queues for " + key + " failed: " + e.Message);
        }
    }

    /// <summary>For each topic, set-compare the current subscribed queue set against the last
    /// snapshot; only when it really changed fire OnChanged and advance the snapshot. A
    /// failing topic query is only logged and that topic skipped, **not affecting the rest
    /// of this round**: a topic whose route cannot be found for a long time must not
    /// starve listeners queued behind it.
    /// Returns how many listeners were fired this round.</summary>
    public int FetchTopicMessageQueuesAndCompare()
    {
        List<KeyValuePair<string, ITopicMessageQueueChangeListener>> entries;
        lock (_lock)
        {
            entries = new List<KeyValuePair<string, ITopicMessageQueueChangeListener>>(
                _topicChangeListeners);
        }

        int fired = 0;
        foreach (KeyValuePair<string, ITopicMessageQueueChangeListener> entry in entries)
        {
            string key = entry.Key;
            try
            {
                List<MessageQueue> newQueues = FetchMessageQueuesFull(key);
                bool changed;
                lock (_lock)
                {
                    changed = !messageQueuesForTopicTryEqual(key, newQueues);
                    if (changed)
                    {
                        _messageQueuesForTopic[key] = newQueues;
                    }
                }

                if (!changed)
                {
                    continue;
                }

                entry.Value.OnChanged(key, newQueues);
                fired++;
            }
            catch (Exception e)
            {
                ClientLog.Warn("ScheduledTask fetchMessageQueuesAndCompare exception: " + e.Message);
            }
        }

        return fired;
    }

    private bool messageQueuesForTopicTryEqual(string key, List<MessageQueue> newQueues)
    {
        if (!_messageQueuesForTopic.TryGetValue(key, out List<MessageQueue>? oldQueues))
        {
            return false;
        }

        if (oldQueues.Count != newQueues.Count)
        {
            return false;
        }

        var oldSet = new HashSet<MessageQueue>(oldQueues);
        foreach (MessageQueue mq in newQueues)
        {
            if (!oldSet.Contains(mq))
            {
                return false;
            }
        }

        return true;
    }

    public void MetadataLoop(int firstDelayMillis, int periodMillis)
    {
        DateTime next = DateTime.UtcNow.AddMilliseconds(firstDelayMillis);
        while (_running)
        {
            Thread.Sleep(200);
            if (!_running)
            {
                return;
            }

            DateTime now = DateTime.UtcNow;
            if (now < next)
            {
                continue;
            }

            next = now.AddMilliseconds(Math.Max(1000, periodMillis));
            try
            {
                FetchTopicMessageQueuesAndCompare();
            }
            catch (Exception e)
            {
                ClientLog.Debug("lite metadata loop error: " + e.Message);
            }
        }
    }

    private void MetadataLoop() =>
        MetadataLoop(10000, _topicMetadataCheckIntervalMillis);

    // ---------------- heartbeat (registers tag subscriptions with the broker) ----------------
    private HeartbeatData BuildHeartbeat()
    {
        var hb = new HeartbeatData(_clientId);
        var cd = new ConsumerData(_consumerGroup, ConsumeType.ConsumeActively, _messageModel, _consumeFromWhere)
        {
            UnitMode = _unitMode,
        };
        lock (_lock)
        {
            // Rebalance subscriptions plus the set the application built for the heartbeat.
            // An assign-mode expression is not a topic-wide subscription, so it is announced
            // only through that built set; a topic both cover keeps the rebalance expression.
            foreach (string topic in _subscription.Keys)
            {
                if (_subscriptionData.TryGetValue(topic, out SubscriptionData? sub) && sub is not null)
                {
                    cd.AddSubscriptionData(sub);
                }
            }

            foreach (KeyValuePair<string, SubscriptionData> kv in _heartbeatSubscriptions)
            {
                if (!_subscription.ContainsKey(kv.Key))
                {
                    cd.AddSubscriptionData(kv.Value);
                }
            }
        }

        hb.AddConsumerData(cd);
        return hb;
    }

    /// <summary>Subscribe() while RUNNING: pick up the topic's route, register it for the
    /// background refresh and push one heartbeat so brokers learn the subscription now.</summary>
    private void NotifySubscriptionChanged(string topic)
    {
        if (!_running || _mqClient is null)
        {
            return;
        }

        try
        {
            _mqClient.RegisterTopicInUse(topic);
            _mqClient.UpdateTopicRouteInfoFromNameServer(topic);
            SendHeartbeatToAllBroker();
        }
        catch (Exception e)
        {
            ClientLog.Debug("immediate heartbeat after subscribe(" + topic + ") failed: " + e.Message);
        }
    }

    /// <summary>
    /// Prepares broker addresses for the heartbeat: pull the route of every topic this
    /// instance cares about (subscribed + assigned) and register them as in use.
    /// The heartbeat is only sent to brokers known in the route table, so this step must
    /// run before the first heartbeat round.
    /// </summary>
    private void RefreshRouteForHeartbeat()
    {
        if (_mqClient is null)
        {
            return;
        }

        List<string> topics = new(_subscription.Keys);
        foreach (MessageQueue mq in _assigned)
        {
            if (!topics.Contains(mq.Topic, StringComparer.Ordinal))
            {
                topics.Add(mq.Topic);
            }
        }

        foreach (string topic in topics)
        {
            _mqClient.RegisterTopicInUse(topic);
            try
            {
                _mqClient.GetTopicPublishInfo(topic);
            }
            catch (Exception e)
            {
                ClientLog.Debug("lite start: refresh route for " + topic + " failed: " + e.Message);
            }
        }
    }

    private int SendHeartbeatToAllBroker()
    {
        if (_mqClient is null) return 0;
        HeartbeatData hb = BuildHeartbeat();
        return _mqClient.SendHeartbeatToAllBrokers(hb, isRebalance: false, includeSlaves: true);
    }

    private void HeartbeatLoop()
    {
        while (_running)
        {
            try
            {
                SendHeartbeatToAllBroker();
            }
            catch
            {
                ClientLog.Debug("lite heartbeat loop error");
            }

            // 5s heartbeat interval (same as push consumers)
            for (int i = 0; i < 50; ++i)
            {
                if (!_running) break;
                Thread.Sleep(100);
            }
        }
    }

    // ---------------- internal helpers ----------------
    private string WithNamespace(string topic)
    {
        return _namespace.Length == 0 ? topic : NamespaceUtil.WrapNamespace(_namespace, topic);
    }

    private MQClientInstance RequireClient()
    {
        MQClientInstance? c = _mqClient;
        if (!_started || c is null)
        {
            throw new MQClientException("The consumer not running, please start it first.");
        }

        return c;
    }

    private string _consumerId() => _clientId;

    private static List<string> SplitSemicolon(string addr)
    {
        var outAddrs = new List<string>();
        foreach (string piece in addr.Split(';'))
        {
            string t = piece.Trim();
            if (t.Length > 0) outAddrs.Add(t);
        }

        return outAddrs;
    }

    internal static long ParseConsumeTimestamp(string ts)
    {
        if (ts is { Length: 14 }
            && DateTimeOffset.TryParseExact(ts, "yyyyMMddHHmmss", CultureInfo.InvariantCulture,
                   DateTimeStyles.None, out DateTimeOffset dt))
        {
            return dt.ToUnixTimeMilliseconds();
        }

        throw new MQClientException(
            "consumeTimestamp is invalid, the valid format is yyyyMMddHHmmss,but received " + ts);
    }

    // ---------------- local-buffer bookkeeping ----------------
    /// <summary>One MiB, the unit <see cref="PullThresholdSizeForQueue"/> is written
    /// in.</summary>
    private const long MiB = 1024 * 1024;

    /// <summary>A queue's slice of the local buffer: how many of its messages are cached,
    /// how many <b>body</b> bytes they occupy, the lowest/highest cached queue offset and
    /// the pull rounds still waiting for the application. The only mutators are
    /// <see cref="AppendToLocalBufferForTest"/> / the pull append, the poll drain and the
    /// seek prune -- every one of them changes the buffer and this view in the same step
    /// under <c>_bufferLock</c>, which is what keeps the two from drifting apart.</summary>
    private sealed class QueueCache
    {
        private readonly OffsetSpan _span = new();
        // One entry per pull round whose messages are still (partly) cached, oldest round
        // first, holding how many of that round's messages are left. The aggregate gate
        // budgets a whole batch per round rather than the rows that round happened to
        // bring back, so it needs this list and not Count. This queue's own messages always
        // leave in round order -- poll() drains the shared buffer from its head and a seek
        // drops a prefix of this queue's offsets -- so only the front round's remainder
        // ever has to be tracked.
        private readonly Queue<int> _rounds = new();

        public int Count { get; private set; }
        public long SizeBytes { get; private set; }
        public long MinOffset => _span.Min;
        public long MaxOffset => _span.Max;
        public long Span => _span.Span;
        public bool IsEmpty => Count == 0;
        public int PendingRounds => _rounds.Count;

        public void Append(MessageExt m)
        {
            Count++;
            SizeBytes += m.Body.Length;
            _span.Add(m.QueueOffset);
        }

        /// <summary>Book the round that just filled the buffer, only when it brought
        /// messages: an empty round reserves nothing and must not hold budget forever.</summary>
        public void BeginRound(int count)
        {
            if (count > 0) _rounds.Enqueue(count);
        }

        public void Remove(MessageExt m)
        {
            Count--;
            SizeBytes -= m.Body.Length;
            _span.Remove();
            // Whatever left the buffer came out of the oldest round still represented here; a
            // round that runs out is dropped, a half-drained one stays pending exactly as the
            // gate budgets it.
            if (_rounds.Count > 0)
            {
                int front = _rounds.Dequeue() - 1;
                if (front > 0) _rounds.Enqueue(front);
            }
        }
    }

    /// <summary>The lowest and the highest cached queue offset of one queue, kept
    /// incrementally so neither gate ever scans the buffer. The buffer is a FIFO, so a
    /// queue's messages leave it in the order they entered it: two monotone deques (one
    /// rising for the minimum, one falling for the maximum) drop exactly the entries that
    /// leave the cache, which keeps min and max right for any arrival order -- including a
    /// seek that re-pulls an overlapping window -- at the cost of one compare per append
    /// and per removal.</summary>
    private sealed class OffsetSpan
    {
        private readonly LinkedList<(long Seq, long Offset)> _min = new();
        private readonly LinkedList<(long Seq, long Offset)> _max = new();
        private long _nextSeq;
        private long _leavingSeq;

        public void Add(long offset)
        {
            long seq = _nextSeq++;
            while (_min.Count > 0 && _min.Last!.Value.Offset >= offset) _min.RemoveLast();
            _min.AddLast((seq, offset));
            while (_max.Count > 0 && _max.Last!.Value.Offset <= offset) _max.RemoveLast();
            _max.AddLast((seq, offset));
        }

        /// <summary>The oldest cached offset leaves the window (it is always the one the
        /// FIFO hands out next); it is only still in a deque when it sits at that deque's
        /// front, because every entry behind it entered later.</summary>
        public void Remove()
        {
            long seq = _leavingSeq++;
            if (_min.Count > 0 && _min.First!.Value.Seq == seq) _min.RemoveFirst();
            if (_max.Count > 0 && _max.First!.Value.Seq == seq) _max.RemoveFirst();
        }

        public long Min => _min.Count > 0 ? _min.First!.Value.Offset : 0;
        public long Max => _max.Count > 0 ? _max.First!.Value.Offset : 0;

        /// <summary>0 for an empty or single-message cache, as the contract measures
        /// span.</summary>
        public long Span => _min.Count > 0 && _max.Count > 0 ? Max - Min : 0;
    }

    /// <summary>One queue's re-schedule: when its next round may run again, plus the
    /// length of the pause that armed it (kept after the deadline so what a refusal
    /// decided stays observable).</summary>
    private sealed class PullPause
    {
        public PullPause(long deadlineMillis, int millis)
        {
            DeadlineMillis = deadlineMillis;
            Millis = millis;
        }

        public long DeadlineMillis { get; }
        public int Millis { get; }
    }
}
