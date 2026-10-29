// The difference from a push consumer (DefaultMQPushConsumer): **the caller does the
// pulling and manages the offsets itself**. There is no background pulling thread, no
// rebalance and no consume listener; only:
//   FetchSubscribeMessageQueues / Pull / PullAsync / PullBlockIfNotFound /
//   FetchConsumeOffset / UpdateConsumeOffset / SearchOffset / MaxOffset / MinOffset /
//   EarliestMsgStoreTime / SendMessageBack / CreateTopic.
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Threading;

using RocketMQ.Common;
using RocketMQ.Remoting;
using RocketMQ.Remoting.Protocol;

namespace RocketMQ.Client;

public interface IMessageQueueListener
{
    void MessageQueueChanged(string topic, IReadOnlyList<MessageQueue> mqAll,
        IReadOnlyList<MessageQueue> mqDivided);
}

public sealed class DefaultMQPullConsumer
{
    private readonly object _lock = new();
    private string _consumerGroup;
    private string _namespace = string.Empty;
    private string _instanceName = MixAll.DefaultInstanceName;
    private string _clientId = string.Empty;
    private string _messageModel = MessageModel.Clustering;
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
    private bool _heartbeatEnabled = true;
    private int _heartbeatBrokerIntervalMillis = 30000;
    private long _heartbeatCount;
    private volatile bool _heartbeatStop;
    private Thread? _heartbeatThread;
    private readonly SortedSet<string> _registerTopics = new(StringComparer.Ordinal);
    private readonly Dictionary<string, IMessageQueueListener> _messageQueueListeners =
        new(StringComparer.Ordinal);
    private IMessageQueueListener? _messageQueueListener;
    /// <summary>Per topic, the queue SET last handed to the listeners: a round that re-publishes
    /// the same set is not a change. Keys are (topic, brokerName, queueId) identities.</summary>
    private readonly Dictionary<string, HashSet<string>> _lastBalancedQueues =
        new(StringComparer.Ordinal);
    private IAllocateMessageQueueStrategy? _allocateMessageQueueStrategy =
        new AllocateMessageQueueAveragely();

    private readonly int _brokerSuspendMaxTimeMillis = 20000;
    private readonly int _consumerPullTimeoutMillis = 10000;
    private readonly int _consumerTimeoutMillisWhenSuspend = 30000;

    private readonly Dictionary<MessageQueue, long> _pullFromWhichNode = new();

    /// <summary>Non-TAG subscriptions recorded by the selector-flavoured pull overloads
    /// (real topic -> SubscriptionData). The heartbeat then registers the real expression
    /// type with the broker; TAG subscriptions never enter this table because their
    /// heartbeat shape stays the legacy "*" one.</summary>
    private readonly Dictionary<string, SubscriptionData> _selectorSubscriptions =
        new(StringComparer.Ordinal);

    private MQClientInstance? _mqClient;
    private bool _started;

    public DefaultMQPullConsumer(string consumerGroup = MixAll.DefaultConsumerGroup)
    {
        if (string.IsNullOrWhiteSpace(consumerGroup))
        {
            throw new MQClientException("consumerGroup is empty");
        }

        _consumerGroup = consumerGroup;
    }

    // ---------------- Configuration ----------------
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

    public void SetMessageQueueListener(IMessageQueueListener listener) =>
        _messageQueueListener = listener;

    public void SetAllocateMessageQueueStrategy(IAllocateMessageQueueStrategy? strategy) =>
        _allocateMessageQueueStrategy = strategy;

    public IAllocateMessageQueueStrategy? AllocateMessageQueueStrategy =>
        _allocateMessageQueueStrategy;

    public void RegisterMessageQueueListener(string topic, IMessageQueueListener? listener)
    {
        if (string.IsNullOrEmpty(topic))
        {
            return;
        }

        _registerTopics.Add(topic);
        if (listener is not null)
        {
            _messageQueueListeners[topic] = listener;
        }
    }

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

    /// <summary>Whether to send heartbeats periodically (mirrors the push consumer's
    /// switch; the round at startup is not affected).</summary>
    public bool HeartbeatEnabled
    {
        get => _heartbeatEnabled;
        set => _heartbeatEnabled = value;
    }

    public int HeartbeatBrokerIntervalMillis
    {
        get => _heartbeatBrokerIntervalMillis;
        set => _heartbeatBrokerIntervalMillis = value;
    }

    /// <summary>Number of successful heartbeat rounds (for verification on a real
    /// machine).</summary>
    public long HeartbeatCount => Interlocked.Read(ref _heartbeatCount);

    // ---------------- ACL authentication ----------------
    /// <summary>Must be called before Start(): the hook is bound to the MqClient inside
    /// Start().</summary>
    public void SetRpcHook(IRpcHook hook) => _rpcHook = hook;

    public void SetCredentials(string accessKey, string secretKey, string securityToken = "") =>
        _rpcHook = new AclClientRPCHook(new SessionCredentials(accessKey, secretKey, securityToken));

    public string ConsumerGroup => _consumerGroup;

    public string ClientId => _clientId;

    public IReadOnlyCollection<string> RegisterTopics => _registerTopics;

    public bool IsStarted => _started;

    // ---------------- Lifecycle ----------------
    public void Start()
    {
        lock (_lock)
        {
            if (_started)
            {
                return;
            }

            if (_namespace.Length > 0)
            {
                _consumerGroup = NamespaceUtil.WrapNamespace(_namespace, _consumerGroup);
            }

            Validators.CheckGroup(_consumerGroup);
            if (_consumerGroup == MixAll.DefaultConsumerGroup)
            {
                throw new MQClientException("consumerGroup can not equal " + MixAll.DefaultConsumerGroup
                                            + ", please specify another one.");
            }

            if (_nameServerAddrs.Count == 0)
            {
                throw new MQClientException("name server address is not set");
            }

            if (_allocateMessageQueueStrategy is null)
            {
                throw new MQClientException("allocateMessageQueueStrategy is null");
            }

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
                    /*connectTimeoutMillis=*/3000, /*invokeTimeoutMillis=*/_consumerPullTimeoutMillis,
                    socksProxy: _socksProxyConfig,
                    unitName: _unitName, pollNameServerIntervalMillis: _pollNameServerIntervalMillis));
            if (requestHook is not null && !_mqClient.RegisterRpcHook(requestHook))
            {
                ClientLog.Warn("pull consumer rpc hook ignored: MqClient already has one (clientId="
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
                // Hand the reference back if startup fails: otherwise the count is never
                // decremented and a shared instance never reaches zero to finish up
                MQClientManager.Instance.UnregisterMQClient(_clientId, _mqClient);
                _mqClient = null;
                throw;
            }
            RefreshRouteForHeartbeat();
            _started = true;
            try
            {
                SendHeartbeatToAllBroker();
            }
            catch (Exception e)
            {
                ClientLog.Debug("initial heartbeat failed: " + e.Message);
            }
            StartHeartbeatLoop();
        }
    }

    public void Shutdown()
    {
        lock (_lock)
        {
            if (!_started)
            {
                return;
            }

            _started = false;
            _heartbeatStop = true;
            if (_heartbeatThread is { IsAlive: true })
            {
                _heartbeatThread.Join(2000);
            }
            if (_mqClient is not null)
            {
                try
                {
                    _mqClient.UnregisterClientAllBrokers(_clientId, string.Empty, _consumerGroup);
                }
                catch (Exception e)
                {
                    ClientLog.Debug("unregister on shutdown failed: " + e.Message);
                }
                MQClientManager.Instance.UnregisterMQClient(_clientId, _mqClient);
                _mqClient = null;
            }
        }
    }

    // ---------------- Heartbeat (registers the consumer group with the broker) ----------------
    /// <summary>
    /// Why pull mode must heartbeat too: the broker's
    /// registered-consumer table is per broker instance, and the table is only created
    /// by a heartbeat (or by a pull carrying the subscription flag). Without a heartbeat,
    /// neither 203 consumerConnection nor 38 GET_CONSUMER_LIST_BY_GROUP shows this group,
    /// and a broker with isRejectPullConsumerEnabled=true answers every pull of this
    /// group with "the pull consumer is rejected by server"
    /// at its pull check. And the broker's heartbeat handling **skips** the
    /// subscription registration of ACTIVELY-type heartbeats -- pull mode relies on the
    /// compensating branch taken by a pull that carries the subscription flag
    /// (the pull-side registration branch), so the failure caused by a missing heartbeat is silent: pulling still
    /// succeeds, the broker simply has no idea this group exists.
    /// </summary>
    private void RefreshRouteForHeartbeat()
    {
        if (_mqClient is null)
        {
            return;
        }

        foreach (string t in _registerTopics)
        {
            string real = NamespaceUtil.WrapNamespace(_namespace, t);
            _mqClient.RegisterTopicInUse(real);
            try
            {
                _mqClient.GetTopicPublishInfo(real);
            }
            catch (Exception e)
            {
                ClientLog.Debug("refresh route for " + real + " failed: " + e.Message);
            }
        }
    }

    private HeartbeatData BuildHeartbeat()
    {
        var hb = new HeartbeatData(_clientId);
        var cd = new ConsumerData(_consumerGroup, ConsumeType.ConsumeActively, _messageModel,
            ConsumeFromWhere.ConsumeFromLastOffset)
        {
            UnitMode = _unitMode,
        };
        var announced = new HashSet<string>(StringComparer.Ordinal);
        foreach (string t in _registerTopics)
        {
            string real = NamespaceUtil.WrapNamespace(_namespace, t);
            announced.Add(real);
            cd.AddSubscriptionData(HeartbeatSubscription(real));
        }

        // A selector pull of a topic nobody ever registered still has to reach the
        // heartbeat: without a subscription entry the broker cannot match its SQL92
        // expression against the filter factory.
        lock (_lock)
        {
            foreach (KeyValuePair<string, SubscriptionData> kv in _selectorSubscriptions)
            {
                if (announced.Contains(kv.Key))
                {
                    continue;
                }

                cd.AddSubscriptionData(kv.Value);
            }
        }

        hb.AddConsumerData(cd);
        return hb;
    }

    /// <summary>The subscription a registered topic contributes to the heartbeat: the
    /// selector-recorded one when its expression type is not TAG, otherwise the legacy
    /// "*" / TAG / subVersion-0 shape (unchanged wire bytes for every tag flow).</summary>
    private SubscriptionData HeartbeatSubscription(string realTopic)
    {
        lock (_lock)
        {
            if (_selectorSubscriptions.TryGetValue(realTopic, out SubscriptionData? recorded)
                && recorded is not null)
            {
                return recorded;
            }
        }

        SubscriptionData sub = FilterAPI.BuildSubscriptionData(realTopic, "*");
        sub.SubVersion = 0;
        return sub;
    }

    public int SendHeartbeatToAllBroker()
    {
        if (_mqClient is null)
        {
            return 0;
        }

        List<string> addrs;
        try
        {
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

        HeartbeatData hb = BuildHeartbeat();
        int ok = _mqClient.SendHeartbeatToAllBrokers(hb, isRebalance: false, includeSlaves: true);

        if (ok > 0)
        {
            Interlocked.Increment(ref _heartbeatCount);
        }

        return ok;
    }

    private void StartHeartbeatLoop()
    {
        _heartbeatThread = new Thread(HeartbeatLoop)
        {
            IsBackground = true,
            Name = "rmq-pull-hb-" + _consumerGroup,
        };
        _heartbeatThread.Start();
    }

    private void HeartbeatLoop()
    {
        while (!_heartbeatStop)
        {
            // Shutdown() sets it; at most one cycle is waited out (sleep in slices, never
            // one full 30s sleep)
            for (int slept = 0; slept < _heartbeatBrokerIntervalMillis && !_heartbeatStop; slept += 100)
            {
                Thread.Sleep(Math.Min(100, _heartbeatBrokerIntervalMillis - slept));
            }

            if (_heartbeatStop || !_heartbeatEnabled)
            {
                continue;
            }

            // The boundary of the resident loop: an exception escaping here would kill the
            // heartbeat thread (no more heartbeats at all), so it must be caught;
            // per-broker failures are already handled inside SendHeartbeatToAllBrokers.
            try
            {
                SendHeartbeatToAllBroker();
            }
            catch (Exception e)
            {
                ClientLog.Warn("heartbeat loop error: " + e.Message);
            }
        }
    }

    public List<MessageQueue> FetchSubscribeMessageQueues(string topic)
    {
        MQClientInstance c = RequireClient();
        string realTopic = NamespaceUtil.WrapNamespace(_namespace, topic);
        TopicRouteData? route = c.GetTopicRouteData(realTopic);
        if (route is null)
        {
            throw new MQClientException("the topic[" + topic + "] not exist");
        }

        return route.GetAllSubscribeMessageQueue(realTopic);
    }

    public List<MessageQueue> FetchMessageQueuesInBalance(string topic)
    {
        MQClientInstance c = RequireClient();
        if (topic is null)
        {
            throw new MQClientException("topic is null");
        }

        string realTopic = NamespaceUtil.WrapNamespace(_namespace, topic);
        List<MessageQueue> pulled = PulledQueues(realTopic);
        // The FULL route set of the topic, in the caller's (namespace-free) spelling -- this is
        // what a queue-change listener is handed next to this instance's share of it.
        var allOfTopic = new List<MessageQueue>();
        foreach (MessageQueue mq in SubscribeQueuesOfTopic(c, realTopic))
        {
            allOfTopic.Add(new MessageQueue(
                NamespaceUtil.WithoutNamespace(mq.Topic, _namespace), mq.BrokerName, mq.QueueId));
        }

        List<MessageQueue>? allocated = null;
        if (_messageModel == MessageModel.Broadcasting)
        {
            List<MessageQueue> all = SubscribeQueuesOfTopic(c, realTopic);
            allocated = all.Count > 0 ? all : pulled;
        }
        else
        {
            try
            {
                List<MessageQueue> mqAll = SubscribeQueuesOfTopic(c, realTopic);
                List<string>? cidAll = c.GetConsumerIdListByGroup(realTopic, _consumerGroup);
                if (mqAll.Count > 0 && cidAll is { Count: > 0 })
                {
                    cidAll.Sort(StringComparer.Ordinal);
                    IAllocateMessageQueueStrategy strategy = _allocateMessageQueueStrategy
                        ?? throw new MQClientException("allocateMessageQueueStrategy is null");
                    allocated = strategy.Allocate(_consumerGroup, _clientId, mqAll, cidAll)
                        ?? new List<MessageQueue>();
                }
            }
            catch (Exception e)
            {
                ClientLog.Debug("fetchMessageQueuesInBalance rebalance view unavailable: " + e.Message);
            }

            if (allocated is null)
            {
                ClientLog.Debug("fetchMessageQueuesInBalance: no route/consumer list for "
                    + _consumerGroup + "/" + realTopic + ", keep current assignment");
                allocated = pulled;
            }
        }

        var outList = new List<MessageQueue>();
        foreach (MessageQueue mq in allocated)
        {
            if (mq.Topic != realTopic)
            {
                continue;
            }

            string plain = NamespaceUtil.WithoutNamespace(mq.Topic, _namespace);
            outList.Add(new MessageQueue(plain, mq.BrokerName, mq.QueueId));
        }

        outList.Sort();
        outList = DedupeQueues(outList);
        // The classic pull consumer owes only this half of an assignment change: the application's
        // listener hears about the topics whose queue SET actually moved, and nothing else.
        NotifyAssignmentChanged(topic, allOfTopic, outList);
        return outList;
    }

    /// <summary>Fire the application's queue listeners for one topic, but only when its queue SET
    /// differs from the set last handed out. A re-ordering of the same queues is not a change; a
    /// same-count swap to another broker is. The very first view reports a change only when this
    /// instance actually owns queues -- an empty opening has nothing for the application to react
    /// to. A listener that throws is logged and never breaks the caller's lookup.</summary>
    public bool NotifyAssignmentChanged(string topic, IReadOnlyList<MessageQueue> mqAll,
        IReadOnlyList<MessageQueue> divided)
    {
        bool changed;
        IMessageQueueListener? perTopic;
        IMessageQueueListener? global;
        lock (_lock)
        {
            if (!_lastBalancedQueues.TryGetValue(topic, out HashSet<string>? was))
            {
                changed = divided.Count > 0;
            }
            else
            {
                var now = new HashSet<string>(StringComparer.Ordinal);
                foreach (MessageQueue mq in divided) now.Add(QueueIdentity(mq));
                changed = !was.SetEquals(now);
            }

            _lastBalancedQueues[topic] = ToKeySet(divided);
            _messageQueueListeners.TryGetValue(topic, out perTopic);
            global = _messageQueueListener;
        }

        if (!changed)
        {
            return false;
        }

        if (perTopic is not null)
        {
            Fire(perTopic, topic, mqAll, divided);
        }

        // The single listener is a different sink from the topic-keyed one; it is skipped only
        // when the application handed the same object to both.
        if (global is not null && !ReferenceEquals(global, perTopic))
        {
            Fire(global, topic, mqAll, divided);
        }

        return true;
    }

    private static void Fire(IMessageQueueListener listener, string topic,
        IReadOnlyList<MessageQueue> mqAll, IReadOnlyList<MessageQueue> divided)
    {
        try
        {
            listener.MessageQueueChanged(topic, mqAll, divided);
        }
        catch (Exception e)
        {
            ClientLog.Debug("message queue listener failed for " + topic + ": " + e.Message);
        }
    }

    private static HashSet<string> ToKeySet(IReadOnlyList<MessageQueue> queues)
    {
        var outSet = new HashSet<string>(StringComparer.Ordinal);
        foreach (MessageQueue mq in queues) outSet.Add(QueueIdentity(mq));
        return outSet;
    }

    private static string QueueIdentity(MessageQueue mq) =>
        mq.Topic + "@" + mq.BrokerName + "@" + mq.QueueId.ToString(CultureInfo.InvariantCulture);

    /// <summary>All queues as seen by the subscription info (offset reads, master not
    /// filtered), returned in the house ordering.</summary>
    private static List<MessageQueue> SubscribeQueuesOfTopic(MQClientInstance c, string realTopic)
    {
        TopicRouteData? route = c.GetTopicRouteData(realTopic);
        List<MessageQueue> outList = route is null
            ? new List<MessageQueue>()
            : route.GetAllSubscribeMessageQueue(realTopic);
        outList.Sort();
        return outList;
    }

    private List<MessageQueue> PulledQueues(string realTopic)
    {
        lock (_lock)
        {
            var outList = new List<MessageQueue>();
            foreach (MessageQueue mq in _pullFromWhichNode.Keys)
            {
                if (mq.Topic == realTopic)
                {
                    outList.Add(mq);
                }
            }

            outList.Sort();
            return outList;
        }
    }

    /// <summary>Deduplicates by (topic, brokerName, queueId); the input must already be
    /// sorted so duplicates are adjacent.</summary>
    private static List<MessageQueue> DedupeQueues(List<MessageQueue> sorted)
    {
        var outList = new List<MessageQueue>(sorted.Count);
        MessageQueue? prev = null;
        foreach (MessageQueue mq in sorted)
        {
            if (prev is not null && prev.Topic == mq.Topic && prev.BrokerName == mq.BrokerName
                && prev.QueueId == mq.QueueId)
            {
                continue;
            }

            outList.Add(mq);
            prev = mq;
        }

        return outList;
    }

    public PullResult Pull(MessageQueue mq, string subExpression = "*", long offset = 0,
        int maxNums = 32, int timeoutMillis = -1)
    {
        _ = RequireClient();
        int timeout = timeoutMillis > 0 ? timeoutMillis : _consumerPullTimeoutMillis;
        MessageQueue real = WrapMq(mq);
        SubscriptionData sub = FilterAPI.BuildSubscriptionData(real.Topic, subExpression);
        return PullCore(real, sub, offset, maxNums, suspend: false, timeout, 15000);
    }

    /// <summary>Selector flavour of <see cref="Pull(MessageQueue,string,long,int,int)"/>:
    /// the expression type travels subscription -> heartbeat -> the pull request's
    /// expressionType. A TAG selector produces exactly the request the string overload
    /// sends today; a SQL92 one carries its own type and real subscription version.</summary>
    public PullResult Pull(MessageQueue mq, MessageSelector selector, long offset = 0,
        int maxNums = 32, int timeoutMillis = -1)
    {
        ArgumentNullException.ThrowIfNull(selector);
        _ = RequireClient();
        int timeout = timeoutMillis > 0 ? timeoutMillis : _consumerPullTimeoutMillis;
        return PullSelectorCore(mq, selector, offset, maxNums, suspend: false, timeout, 15000);
    }

    public PullResult PullBlockIfNotFound(MessageQueue mq, string subExpression, long offset,
        int maxNums)
    {
        _ = RequireClient();
        MessageQueue real = WrapMq(mq);
        SubscriptionData sub = FilterAPI.BuildSubscriptionData(real.Topic, subExpression);
        return PullCore(real, sub, offset, maxNums, suspend: true,
            _consumerTimeoutMillisWhenSuspend, _brokerSuspendMaxTimeMillis);
    }

    /// <summary>Selector flavour of <see cref="PullBlockIfNotFound(MessageQueue,string,long,int)"/>
    /// (the long poll: suspend=true), carrying the expression type exactly like
    /// <see cref="Pull(MessageQueue,MessageSelector,long,int,int)"/>.</summary>
    public PullResult PullBlockIfNotFound(MessageQueue mq, MessageSelector selector, long offset,
        int maxNums)
    {
        ArgumentNullException.ThrowIfNull(selector);
        _ = RequireClient();
        return PullSelectorCore(mq, selector, offset, maxNums, suspend: true,
            _consumerTimeoutMillisWhenSuspend, _brokerSuspendMaxTimeMillis);
    }

    /// <summary>One pull request as the broker sees it: the expression type comes from the
    /// subscription itself. A TAG subscription keeps subVersion 0 on the wire (today's
    /// byte shape); only an expression with its own version (SQL92 / class filter) sends
    /// the real one.</summary>
    private PullResult PullCore(MessageQueue real, SubscriptionData sub, long offset,
        int maxNums, bool suspend, int timeoutMillis, int suspendTimeoutMillis)
    {
        MQClientInstance c = RequireClient();
        int sysFlag = PullSysFlag.BuildSysFlag(commitOffset: false, suspend: suspend,
            subscription: true, classFilter: false);
        long brokerId = TakePullFromWhichNode(real);
        long subVersion = sub.ExpressionType == ExpressionType.TAG ? 0 : sub.SubVersion;
        PullResult result = c.PullMessage(_consumerGroup, real, offset, maxNums, sysFlag, 0,
            string.IsNullOrEmpty(sub.SubString) ? "*" : sub.SubString,
            subVersion, sub.ExpressionType, timeoutMillis, -1, suspendTimeoutMillis,
            /*addrIn=*/null, /*requestSource=*/0, /*brokerId=*/brokerId);
        UpdatePullFromWhichNode(real, result);
        return result;
    }

    private PullResult PullSelectorCore(MessageQueue mq, MessageSelector selector, long offset,
        int maxNums, bool suspend, int timeoutMillis, int suspendTimeoutMillis)
    {
        MessageQueue real = WrapMq(mq);
        SubscriptionData sub = SubscriptionSelectors.Build(real.Topic, selector);
        RememberSelectorSubscription(sub);
        return PullCore(real, sub, offset, maxNums, suspend, timeoutMillis, suspendTimeoutMillis);
    }

    /// <summary>Only a non-TAG subscription joins the heartbeat table: the TAG heartbeat
    /// shape ("*", subVersion 0) is deliberate legacy behaviour and must not change.</summary>
    private void RememberSelectorSubscription(SubscriptionData sub)
    {
        if (sub.ExpressionType == ExpressionType.TAG)
        {
            return;
        }

        lock (_lock)
        {
            _selectorSubscriptions[sub.Topic] = sub;
        }
    }

    public void PullAsync(MessageQueue mq, string subExpression, long offset, int maxNums,
        IPullCallback callback, int timeoutMillis = -1)
    {
        ArgumentNullException.ThrowIfNull(callback);
        _ = RequireClient(); // not started: throws synchronously, matching the synchronous Pull (throwing from the thread pool would be far too late)
        _ = Task.Run(() =>
        {
            PullResult result;
            try
            {
                result = Pull(mq, subExpression, offset, maxNums, timeoutMillis);
            }
            catch (Exception e)
            {
                try
                {
                    callback.OnException(e);
                }
                catch (Exception inner)
                {
                    ClientLog.Warn("pullAsync onException callback threw: " + inner.Message);
                }

                return;
            }

            try
            {
                callback.OnSuccess(result);
            }
            catch (Exception e)
            {
                ClientLog.Warn("pullAsync onSuccess callback threw: " + e.Message);
            }
        });
    }

    /// <summary>The overload that locates the queue by (topic, brokerName, queueId) (no
    /// need to assemble a MessageQueue; the semantics are exactly those of
    /// <see cref="PullAsync(MessageQueue,string,long,int,IPullCallback,int)"/>).</summary>
    public void PullAsync(string topic, string subExpression, string brokerName, int queueId,
        long offset, int maxNums, IPullCallback callback, int timeoutMillis = -1) =>
        PullAsync(new MessageQueue(topic, brokerName, queueId), subExpression, offset, maxNums,
            callback, timeoutMillis);

    /// <summary>Selector flavour of <see cref="PullAsync(MessageQueue,string,long,int,IPullCallback,int)"/>:
    /// same asynchronous contract (a not-started client throws synchronously), same
    /// expression-type carrying rules as the synchronous selector pull.</summary>
    public void PullAsync(MessageQueue mq, MessageSelector selector, long offset, int maxNums,
        IPullCallback callback, int timeoutMillis = -1)
    {
        ArgumentNullException.ThrowIfNull(selector);
        ArgumentNullException.ThrowIfNull(callback);
        _ = RequireClient(); // not started: throws synchronously, matching the synchronous Pull
        _ = Task.Run(() =>
        {
            PullResult result;
            try
            {
                result = Pull(mq, selector, offset, maxNums, timeoutMillis);
            }
            catch (Exception e)
            {
                try
                {
                    callback.OnException(e);
                }
                catch (Exception inner)
                {
                    ClientLog.Warn("pullAsync onException callback threw: " + inner.Message);
                }

                return;
            }

            try
            {
                callback.OnSuccess(result);
            }
            catch (Exception e)
            {
                ClientLog.Warn("pullAsync onSuccess callback threw: " + e.Message);
            }
        });
    }

    private long TakePullFromWhichNode(MessageQueue mq)
    {
        lock (_lock)
        {
            return _pullFromWhichNode.TryGetValue(mq, out long brokerId) ? brokerId : MixAll.MasterId;
        }
    }

    private void UpdatePullFromWhichNode(MessageQueue mq, PullResult result)
    {
        lock (_lock)
        {
            _pullFromWhichNode[mq] = result.SuggestWhichBrokerId ?? MixAll.MasterId;
        }
    }

    // ---------------- Offset management ----------------
    private string LocalOffsetPath() =>
        LocalOffsetPathForTest ?? DefaultMQPushConsumer.BuildLocalOffsetPath(_clientId, _consumerGroup);

    /// <summary>false means this consumer group has no offset on that queue yet. The
    /// store follows the message model: broadcasting reads the local file, clustering
    /// asks the broker.</summary>
    public bool FetchConsumeOffset(MessageQueue mq, out long outOffset)
    {
        _ = RequireClient();
        if (_messageModel == MessageModel.Broadcasting)
        {
            if (DefaultMQPushConsumer.LoadLocalOffsetsAt(LocalOffsetPath())
                .TryGetValue(DefaultMQPushConsumer.LocalOffsetKey(mq), out long local))
            {
                outOffset = local;
                return true;
            }

            outOffset = -1;
            return false;
        }

        return RequireClient().QueryConsumerOffset(_consumerGroup, mq, out outOffset);
    }

    public void UpdateConsumeOffset(MessageQueue mq, long offset)
    {
        _ = RequireClient();
        if (_messageModel == MessageModel.Broadcasting)
        {
            string key = DefaultMQPushConsumer.LocalOffsetKey(mq);
            DefaultMQPushConsumer.SaveLocalOffsetsAt(LocalOffsetPath(),
                new Dictionary<string, long>(StringComparer.Ordinal) { [key] = offset },
                new Dictionary<string, MessageQueue>(StringComparer.Ordinal) { [key] = mq });
            return;
        }

        RequireClient().UpdateConsumerOffset(_consumerGroup, mq, offset);
    }

    public long SearchOffset(MessageQueue mq, long timestamp) =>
        RequireClient().SearchOffsetByTimestamp(mq, timestamp);

    public long MaxOffset(MessageQueue mq) => RequireClient().GetMaxOffset(mq);

    public long MinOffset(MessageQueue mq) => RequireClient().GetMinOffset(mq);

    public long EarliestMsgStoreTime(MessageQueue mq)
    {
        MQClientInstance c = RequireClient();
        string addr = c.PublishAddrFor(mq.BrokerName, mq.Topic);

        PropertyMap ext = new()
        {
            ["topic"] = mq.Topic,
            ["queueId"] = mq.QueueId.ToString(CultureInfo.InvariantCulture),
            ["brokerName"] = mq.BrokerName,
        };
        RemotingCommand response = c.InvokeSync(
            c.VipAddr(addr), RequestCode.GetEarliestMsgStoretime, ext, null, false, 5000);
        return ExtInt(response, "timestamp", 0);
    }

    public void SendMessageBack(MessageExt msg, int delayLevel)
    {
        MQClientInstance c = RequireClient();
        string addr = c.FindBrokerAddressInPublish(msg.BrokerName);
        if (addr.Length == 0)
        {
            throw new MQClientException("Broker[" + msg.BrokerName + "] master node does not exist");
        }

        var header = new ConsumerSendMsgBackRequestHeader
        {
            Offset = msg.CommitLogOffset,
            Group = _consumerGroup,
            DelayLevel = delayLevel,
            OriginMsgId = msg.MsgId,
            OriginTopic = msg.Topic,
            UnitMode = _unitMode,
            MaxReconsumeTimes = null,
        };
        c.InvokeSync(c.VipAddr(addr), RequestCode.ConsumerSendMsgBack, header.ToExtFields(), null, false, 5000);
    }

    public void CreateTopic(string key, string newTopic, int queueNum = 4)
    {
        MQClientInstance c = RequireClient();
        // C#'s CreateTopicInRoute ignores the key argument and always creates the route with the default topic (MixAll.DefaultTopic)
        _ = key;
        c.CreateTopicInRoute(NamespaceUtil.WrapNamespace(_namespace, newTopic), queueNum, queueNum, 6);
    }

    // ---------------- Internal utilities ----------------
    private MQClientInstance RequireClient()
    {
        MQClientInstance? c = _mqClient;
        if (!_started || c is null)
        {
            throw new MQClientException("consumer not started, call Start() first");
        }

        return c;
    }

    private MessageQueue WrapMq(MessageQueue mq)
    {
        string topic = NamespaceUtil.WrapNamespace(_namespace, mq.Topic);
        // Reuse the original object when no namespace is configured, saving one allocation (the zero-overhead path).
        return topic == mq.Topic ? mq : new MessageQueue(topic, mq.BrokerName, mq.QueueId);
    }

    internal static long ExtInt(RemotingCommand r, string key, long fallback)
    {
        if (!r.ExtFields.TryGetValue(key, out string? v) || string.IsNullOrEmpty(v))
        {
            return fallback;
        }

        return long.TryParse(v, NumberStyles.Integer, CultureInfo.InvariantCulture, out long parsed)
            ? parsed
            : fallback;
    }

    private static List<string> SplitSemicolon(string addr)
    {
        var outAddrs = new List<string>();
        foreach (string piece in addr.Split(';'))
        {
            string t = piece.Trim();
            if (t.Length > 0)
            {
                outAddrs.Add(t);
            }
        }

        return outAddrs;
    }
}
