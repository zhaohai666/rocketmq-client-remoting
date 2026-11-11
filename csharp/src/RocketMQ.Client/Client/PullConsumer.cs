// The difference from a push consumer (DefaultMQPushConsumer): **the caller does the
// pulling and manages the offsets itself**. There is no background pulling thread and no
// consume listener. There IS a background rebalance, exactly like Java's pull consumer
// (DefaultMQPullConsumer.java:97 `enableRebalance = true`): a per-facade thread runs one
// round every RebalanceWaitMillis (20s, `rocketmq.client.rebalance.waitInterval`) -- or
// RebalanceMinIntervalMillis (1s) after an unbalanced round -- allocating each registered
// topic's queues per the strategy and firing MessageQueueChanged only when a topic's queue
// SET moved. EnableRebalance=false returns it to the old "the caller owns the assignment"
// behaviour, where FetchMessageQueuesInBalance computes on the spot every call.
// Surface: FetchSubscribeMessageQueues / FetchMessageQueuesInBalance / Pull / PullAsync /
//   PullBlockIfNotFound / FetchConsumeOffset / UpdateConsumeOffset / SearchOffset /
//   MaxOffset / MinOffset / EarliestMsgStoreTime / SendMessageBack / CreateTopic.
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Runtime.ExceptionServices;
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
    /// the same set is not a change. Keys are (topic, brokerName, queueId) identities. Used by
    /// <see cref="NotifyAssignmentChanged"/>; the background rebalance compares its own table
    /// instead, so a lookup never feeds back into this history.</summary>
    private readonly Dictionary<string, HashSet<string>> _lastBalancedQueues =
        new(StringComparer.Ordinal);
    private IAllocateMessageQueueStrategy? _allocateMessageQueueStrategy =
        new AllocateMessageQueueAveragely();

    // ---- Pull-side rebalance (Java DefaultMQPullConsumer.java:97 / impl :388-406) ----
    // Java's pull consumer DOES rebalance: the shared RebalanceService thread calls
    // `mqClientFactory.doRebalance()` every `rocketmq.client.rebalance.waitInterval` (20s),
    // which walks every registered consumer and calls its `tryRebalance()`; that gates on
    // `enableRebalance` (default true) and then runs `rebalanceImpl.doRebalance(false)` ->
    // allocate -> `messageQueueChanged(topic, mqAll, mqDivided)`.
    // Deviation (documented): Java shares ONE RebalanceService per client instance, this
    // port gives each facade its own loop, the same shape as the heartbeat loop above; the
    // observable behaviour -- which queues this instance owns and when the listener fires --
    // is the same. Also, `balanced` here means "this round's allocation equals the previous
    // round's", so a changed-then-already-in-place topic costs at most one extra 1s round.
    private bool _enableRebalance = true;
    private int _rebalanceWaitMillis = 20000;
    private int _rebalanceMinIntervalMillis = 1000;
    /// <summary>The product of the background rebalance: topic -> the queues allocated to this
    /// instance (bare topic names). Java reads the keys of the rebalance's process-queue table;
    /// this is the equivalent, and FetchMessageQueuesInBalance prefers it when it is warm.</summary>
    private readonly Dictionary<string, List<MessageQueue>> _rebalanceTable =
        new(StringComparer.Ordinal);
    private volatile bool _rebalanceStop;
    private Thread? _rebalanceThread;

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

        // Locked: the background rebalance thread snapshots both tables while it walks the
        // registered topics, and a registration may well happen after Start().
        lock (_lock)
        {
            _registerTopics.Add(topic);
            if (listener is not null)
            {
                _messageQueueListeners[topic] = listener;
            }
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

    /// <summary>Re-registers this group with a broker as soon as a new connection to it is
    /// established (default on). Off, a reconnect leaves the group unknown to that broker
    /// until the next periodic heartbeat round.</summary>
    public bool EnableHeartbeatChannelEventListener
    {
        get => _enableHeartbeatChannelEventListener;
        set
        {
            _enableHeartbeatChannelEventListener = value;
            if (_mqClient is not null)
            {
                _mqClient.EnableHeartbeatChannelEventListener = value;
            }
        }
    }

    private bool _enableHeartbeatChannelEventListener = true;

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

    /// <summary>Java DefaultMQPullConsumer.java:97 (`enableRebalance = true`). Off means
    /// DoRebalance/RebalanceByTopic allocate nothing and FetchMessageQueuesInBalance goes back
    /// to computing the assignment on the spot every call.</summary>
    public bool EnableRebalance
    {
        get => _enableRebalance;
        set => _enableRebalance = value;
    }

    /// <summary>Round period: Java system property `rocketmq.client.rebalance.waitInterval`
    /// (20000ms). Clamped to 1 -- a zero period would spin the loop.</summary>
    public int RebalanceWaitMillis
    {
        get => _rebalanceWaitMillis;
        set => _rebalanceWaitMillis = value > 0 ? value : 1;
    }

    /// <summary>Lower bound between two rounds: Java `rocketmq.client.rebalance.minInterval`
    /// (1000ms). Used as the delay after an unbalanced round so a newly appeared queue is
    /// claimed quickly. Clamped to 1.</summary>
    public int RebalanceMinIntervalMillis
    {
        get => _rebalanceMinIntervalMillis;
        set => _rebalanceMinIntervalMillis = value > 0 ? value : 1;
    }

    /// <summary>Snapshot of the background rebalance's product (topic -> this instance's
    /// queues). The lists are copies: a caller cannot mutate what the next round compares
    /// against.</summary>
    public Dictionary<string, List<MessageQueue>> GetRebalanceTable()
    {
        lock (_lock)
        {
            var snapshot = new Dictionary<string, List<MessageQueue>>(StringComparer.Ordinal);
            foreach (KeyValuePair<string, List<MessageQueue>> kv in _rebalanceTable)
            {
                snapshot[kv.Key] = new List<MessageQueue>(kv.Value);
            }

            return snapshot;
        }
    }

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
            _mqClient.EnableHeartbeatChannelEventListener = _enableHeartbeatChannelEventListener;
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
            // A reconnect to a broker re-registers this group there right away: the broker
            // drops a group when its last channel closes, and pulls would otherwise answer
            // SUBSCRIPTION_NOT_EXIST until the next periodic heartbeat.
            _mqClient.RegisterConsumerHeartbeat(_consumerGroup, SendHeartbeatToBroker);
            StartHeartbeatLoop();
            // Java registers the pull consumer with the shared RebalanceService in start();
            // here the facade drives its own loop (see the field comment).
            StartRebalanceLoop();
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
            // Stop rebalancing before the client instance goes away: the loop calls back into
            // it (subscribe info / consumer id list). The wait is sliced, so the join below
            // returns promptly even between two rounds.
            _rebalanceStop = true;
            if (_rebalanceThread is { IsAlive: true })
            {
                _rebalanceThread.Join(2000);
            }
            _heartbeatStop = true;
            if (_heartbeatThread is { IsAlive: true })
            {
                _heartbeatThread.Join(2000);
            }
            if (_mqClient is not null)
            {
                _mqClient.UnregisterConsumerHeartbeat(_consumerGroup);
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

    /// <summary>Send this group's heartbeat to one broker and return 1. Used to restore the
    /// broker-side registration right after a reconnect (the broker drops a group with its
    /// last channel), instead of waiting for the next periodic round.</summary>
    public int SendHeartbeatToBroker(string addr)
    {
        MQClientInstance c = RequireClient();
        c.SendHeartbeat(addr, BuildHeartbeat(), _mqClientApiTimeout);
        return 1;
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

        // The background rebalance's product is authoritative when it has already produced a
        // view of this topic: Java reads the keys of its process-queue table here. The list is
        // the caller's spelling (bare topic) and a copy, so mutating it cannot corrupt the
        // round that produced it. Off (or not yet warmed) falls through to the on-the-spot
        // formula below.
        if (_enableRebalance && TryGetRebalanceView(topic, out List<MessageQueue> cached))
        {
            return cached;
        }

        string realTopic = NamespaceUtil.WrapNamespace(_namespace, topic);
        List<MessageQueue> pulled = PulledQueues(realTopic);
        // Pure query: Java's fetchMessageQueuesInBalance only reads the process-queue table
        // (DefaultMQPullConsumerImpl:120-135) and never calls messageQueueChanged -- the
        // listener belongs to the rebalance round, not to a lookup.

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
        return DedupeQueues(outList);
    }

    // ---------------- Pull-side rebalance (Java DefaultMQPullConsumerImpl :388-406) ----------------

    /// <summary>One rebalance round over every registered topic. Java
    /// `DefaultMQPullConsumerImpl#tryRebalance` (:398-406): with the switch off it returns
    /// **true** ("nothing left to retry") -- returning false would pin the loop at
    /// minInterval and burn a round-trip every second for nothing. The answer is the
    /// "balanced" verdict the loop uses to pick the next delay.</summary>
    public bool DoRebalance()
    {
        if (!_enableRebalance)
        {
            return true;
        }

        string[] topics;
        lock (_lock)
        {
            topics = new string[_registerTopics.Count];
            _registerTopics.CopyTo(topics);
        }

        bool balanced = true;
        foreach (string topic in topics)
        {
            try
            {
                if (!RebalanceByTopic(topic))
                {
                    balanced = false;
                }
            }
            catch (Exception e)
            {
                // The loop must survive one bad topic (a broker that stopped answering, a
                // strategy that threw): log it and report the round as unbalanced.
                ClientLog.Warn("rebalance topic " + topic + " failed: " + e.Message);
                balanced = false;
            }
        }

        return balanced;
    }

    /// <summary>Java `RebalanceImpl#rebalanceByTopic` (pull path) + `RebalancePullImpl#messageQueueChanged`
    /// (:44-53). Returns whether this topic is already balanced.
    ///
    /// BROADCASTING divides all queues to this instance; CLUSTERING asks the broker for the
    /// group's client ids and runs the allocation strategy, keeping only the queues of this
    /// topic. The listener fires **only when the allocation changed**, with
    /// `(topic, mqAll, mqDivided)`, exactly the three arguments Java passes -- and inside
    /// try/catch there too, so a throwing listener cannot kill the loop. A topic whose route
    /// vanished clears the table and (when it had queues) notifies with two empty sets while
    /// still reporting balanced (:276-294); a missing consumer list only warns, notifies
    /// nothing and also stays balanced (:300-302); only a throwing strategy is unbalanced.</summary>
    public bool RebalanceByTopic(string topic)
    {
        MQClientInstance c = RequireClient();
        string realTopic = NamespaceUtil.WrapNamespace(_namespace, topic);
        List<MessageQueue> mqAll = SubscribeQueuesOfTopic(c, realTopic);
        if (mqAll.Count == 0)
        {
            bool hadQueues;
            lock (_lock)
            {
                hadQueues = _rebalanceTable.TryGetValue(topic, out List<MessageQueue>? previous)
                            && previous.Count > 0;
                _rebalanceTable[topic] = new List<MessageQueue>();
            }

            if (hadQueues)
            {
                ClientLog.Warn("doRebalance, " + _consumerGroup + ", but the topic[" + topic
                               + "] not exist.");
                NotifyQueueChanged(topic, Array.Empty<MessageQueue>(), Array.Empty<MessageQueue>());
            }

            // Java's BROADCASTING (:276-284) and CLUSTERING (:286-294) branches both `break`
            // after that warn, leaving `balanced` at its initial true. Reporting false would
            // switch the loop to minInterval and spin once per second for a topic that is
            // merely temporarily absent -- the same reason the missing-cid path stays true.
            return true;
        }

        List<MessageQueue> divided;
        if (_messageModel == MessageModel.Broadcasting)
        {
            divided = DedupeQueues(BareQueues(mqAll));
        }
        else
        {
            List<string>? cidAll = c.GetConsumerIdListByGroup(realTopic, _consumerGroup);
            if (cidAll is null || cidAll.Count == 0)
            {
                // Java (:300-302) only warns: it does NOT notify and leaves balanced at true,
                // so the next round waits the full interval. Never fall back to "owning all
                // queues" -- several instances of the group would consume each other's messages.
                ClientLog.Warn("doRebalance, " + _consumerGroup + " " + topic
                               + ", get consumer id list failed");
                return true;
            }

            cidAll.Sort(StringComparer.Ordinal);
            List<MessageQueue>? allocated;
            try
            {
                IAllocateMessageQueueStrategy strategy = _allocateMessageQueueStrategy
                    ?? throw new MQClientException("allocateMessageQueueStrategy is null");
                allocated = strategy.Allocate(_consumerGroup, _clientId, mqAll, cidAll);
            }
            catch (Exception e)
            {
                // Java keeps the current assignment and ends the round (catch Throwable ->
                // log error -> return false); a topic's queues must never be revoked here.
                ClientLog.Error("allocate message queue exception. strategy name: "
                                + (_allocateMessageQueueStrategy?.GetName() ?? "null")
                                + ", ex: " + e.Message);
                return false;
            }

            var mine = new List<MessageQueue>();
            foreach (MessageQueue mq in allocated ?? new List<MessageQueue>())
            {
                if (mq.Topic == realTopic)
                {
                    mine.Add(new MessageQueue(
                        NamespaceUtil.WithoutNamespace(mq.Topic, _namespace), mq.BrokerName, mq.QueueId));
                }
            }

            mine.Sort();
            divided = DedupeQueues(mine);
        }

        bool balanced;
        lock (_lock)
        {
            // Deviation: balanced means "identical to the previous round" (Java compares the
            // allocate result with the freshly updated process-queue table). It costs at most
            // one extra minInterval round and never reports a change that did not happen.
            // First sight of the topic: Java's updateProcessQueueTableInRebalance counts only
            // table additions/removals as a change, so "nothing -> nothing" is no change -- an
            // empty opening share stays silent and the round counts as balanced.
            bool hasPrevious = _rebalanceTable.TryGetValue(topic, out List<MessageQueue>? previous)
                               && previous is not null;
            balanced = hasPrevious
                ? ToKeySet(_rebalanceTable[topic]).SetEquals(ToKeySet(divided))
                : divided.Count == 0;
            _rebalanceTable[topic] = new List<MessageQueue>(divided);
        }

        if (!balanced)
        {
            ClientLog.Info("client rebalanced result changed. Group: " + _consumerGroup
                           + ", topic: " + topic + ", mqAllSize: " + mqAll.Count
                           + ", rebalanceResultSize: " + divided.Count);
            NotifyQueueChanged(topic, BareQueues(mqAll), divided);
        }

        return balanced;
    }

    /// <summary>Java `RebalancePullImpl#messageQueueChanged` (:44-53): hand the change to the
    /// application's listeners. A listener that throws is logged and swallowed -- the loop must
    /// survive one bad callback. Nothing is filtered here: the caller has already established
    /// that the queue SET moved.</summary>
    public void NotifyQueueChanged(string topic, IReadOnlyList<MessageQueue> mqAll,
        IReadOnlyList<MessageQueue> divided)
    {
        IMessageQueueListener? perTopic;
        IMessageQueueListener? global;
        lock (_lock)
        {
            _messageQueueListeners.TryGetValue(topic, out perTopic);
            global = _messageQueueListener;
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
    }

    private void StartRebalanceLoop()
    {
        _rebalanceStop = false;
        _rebalanceThread = new Thread(RebalanceLoop)
        {
            IsBackground = true,
            Name = "rmq-pull-rebalance-" + _consumerGroup,
        };
        _rebalanceThread.Start();
    }

    /// <summary>Java `RebalanceService#run` shape: wait, rebalance, then pick the next delay --
    /// the full waitInterval when the round was balanced, minInterval otherwise so a freshly
    /// appeared queue is claimed quickly.</summary>
    private void RebalanceLoop()
    {
        int wait = _rebalanceWaitMillis;
        while (!_rebalanceStop)
        {
            // Sliced wait: Shutdown() interrupts within one slice instead of one full period.
            if (SleepUnlessStopped(wait))
            {
                break;
            }

            if (!_started)
            {
                continue;
            }

            try
            {
                bool balanced = DoRebalance();
                wait = balanced ? _rebalanceWaitMillis : _rebalanceMinIntervalMillis;
            }
            catch (Exception e)
            {
                // The boundary of the resident loop: anything escaping here would kill the
                // thread, so no allocation would ever be refreshed again.
                ClientLog.Warn("rebalance loop error: " + e.Message);
                wait = _rebalanceMinIntervalMillis;
            }
        }
    }

    /// <summary>Slices the wait so a stop lands within 100ms; returns true when stopped.</summary>
    private bool SleepUnlessStopped(int millis)
    {
        for (int slept = 0; slept < millis && !_rebalanceStop;)
        {
            int slice = Math.Min(100, millis - slept);
            Thread.Sleep(slice);
            slept += slice;
        }

        return _rebalanceStop;
    }

    /// <summary>The rebalance table's view of one topic as a sorted copy, or false when the
    /// round has not produced one yet.</summary>
    private bool TryGetRebalanceView(string topic, out List<MessageQueue> view)
    {
        lock (_lock)
        {
            if (_rebalanceTable.TryGetValue(topic, out List<MessageQueue>? cached))
            {
                view = new List<MessageQueue>(cached);
                view.Sort();
                return true;
            }
        }

        view = new List<MessageQueue>();
        return false;
    }

    /// <summary>Namespace-free copies in the house ordering: what a listener is handed is the
    /// caller's spelling of the topic, never this facade's wrapped resource name.</summary>
    private List<MessageQueue> BareQueues(List<MessageQueue> queues)
    {
        var outList = new List<MessageQueue>(queues.Count);
        foreach (MessageQueue mq in queues)
        {
            outList.Add(new MessageQueue(
                NamespaceUtil.WithoutNamespace(mq.Topic, _namespace), mq.BrokerName, mq.QueueId));
        }

        outList.Sort();
        return outList;
    }

    /// <summary>Fire the application's queue listeners for one topic, but only when its queue SET
    /// differs from the set last handed out. A re-ordering of the same queues is not a change; a
    /// same-count swap to another broker is. The very first view reports a change only when this
    /// instance actually owns queues -- an empty opening has nothing for the application to react
    /// to. A listener that throws is logged and never breaks the caller's lookup.
    ///
    /// Helper for callers that compute their own assignment and want the standard change
    /// detection; <see cref="FetchMessageQueuesInBalance"/> is a pure query and does NOT call it
    /// (Java's is a pure query too), the background rebalance notifies through
    /// <see cref="NotifyQueueChanged"/>.</summary>
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

    /// <summary>The retry ceiling sent on a send-back and written onto the retry copy;
    /// 16 when untouched. <c>-1</c> is reported as 16, the point at which the broker moves
    /// the message to %DLQ%.</summary>
    public int MaxReconsumeTimes { get; set; } = 16;

    // Send-back: a refused bounce (no master, transport failure, non-success answer)
    // degrades to a normal send to %RETRY%<group> so the redelivery is not lost; only when
    // that send fails too does the ORIGINAL error reach the caller.
    public void SendMessageBack(MessageExt msg, int delayLevel)
    {
        MQClientInstance c = RequireClient();
        int maxReconsume = MaxReconsumeTimes == -1 ? 16 : MaxReconsumeTimes;
        try
        {
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
                OriginMsgId = msg.ClientMsgId,
                OriginTopic = msg.Topic,
                UnitMode = _unitMode,
                MaxReconsumeTimes = maxReconsume,
            };
            c.InvokeSync(c.VipAddr(addr), RequestCode.ConsumerSendMsgBack, header.ToExtFields(), null, false, 3000);
        }
        catch (Exception e)
        {
            ClientLog.Error("Failed to send message back, consumerGroup=" + _consumerGroup
                            + ", brokerName=" + msg.BrokerName + ", msg=" + msg.MsgId
                            + ": " + e.Message + "; fallback to normal send");
            try
            {
                Message newMsg = RetryMessageFactory.Build(_consumerGroup, msg, maxReconsume);
                TopicPublishInfo publish = c.GetTopicPublishInfo(newMsg.Topic, isDefault: true);
                // unitMode follows the consumer: without it, sent-back messages lose their unitization mark.
                c.SendMessage(MixAll.ClientInnerProducerGroup, newMsg, publish.SelectOneMessageQueue(),
                    3000, unitMode: _unitMode);
            }
            catch (Exception fallbackError)
            {
                ClientLog.Error("Fallback normal send failed, consumerGroup=" + _consumerGroup
                                + ", msg=" + msg.MsgId + ": " + fallbackError.Message);
                ExceptionDispatchInfo.Capture(e).Throw();
                throw;  // ExceptionDispatchInfo.Throw never returns
            }
        }
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

    /// <summary>
    /// Java DefaultMQPullConsumerImpl#viewMessage parity
    /// (DefaultMQPullConsumerImpl.java:838): the MQAdminImpl single-leg path —
    /// offset msgId only, no uniq-key fallback at this layer.
    /// </summary>
    public MessageExt ViewMessage(string topic, string msgId) =>
        RequireClient().ViewMessage(topic, msgId);

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
