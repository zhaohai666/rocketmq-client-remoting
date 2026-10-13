using System;
using System.Collections.Generic;
using System.Globalization;
using System.Threading;

using RocketMQ.Common;
using RocketMQ.Remoting;
using RocketMQ.Remoting.Protocol;

namespace RocketMQ.Client;

public interface ILiteMessageQueueListener
{
    // mqAll=当前订阅的全部队列；mqDivided=本次新分配给本实例的队列。
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

    // subscribe 模式的订阅表（topic -> sub_expression，已套命名空间）
    private readonly Dictionary<string, string> _subscription = new(StringComparer.Ordinal);
    // 订阅对应的 SubscriptionData（带 tagsSet），用于发给 broker 的心跳做 tag 过滤注册
    private readonly Dictionary<string, SubscriptionData> _subscriptionData = new(StringComparer.Ordinal);
    // assign 模式：topic -> tag 过滤表达式（透传给 pull）
    private readonly Dictionary<string, string> _assignSubExpr = new(StringComparer.Ordinal);
    private bool _assignMode;
    private readonly HashSet<MessageQueue> _assigned = new();

    private int _brokerSuspendMaxTimeMillis = 20000;
    private int _consumerTimeoutMillisWhenSuspend = 30000;
    private int _pollTimeoutMillis = 5000;
    private int _pullBatchSize = 32;
    private bool _autoCommit = true;
    private int _autoCommitIntervalMillis = 5000;
    private int _pullIntervalMillis = 50;

    private readonly object _bufferLock = new();
    private readonly Queue<MessageExt> _localBuffer = new();
    private readonly Dictionary<MessageQueue, long> _nextOffset = new();
    private readonly Dictionary<MessageQueue, long> _consumeOffset = new();
    private readonly Dictionary<MessageQueue, long> _offsetTable = new();
    private readonly Dictionary<MessageQueue, long> _seekOffset = new();
    private long _nextAutoCommitDeadline = -1;
    private readonly HashSet<MessageQueue> _paused = new();
    private ILiteMessageQueueListener? _messageQueueListener;
    private int _topicMetadataCheckIntervalMillis = 30000;
    private readonly Dictionary<string, ITopicMessageQueueChangeListener> _topicChangeListeners =
        new(StringComparer.Ordinal);
    private readonly Dictionary<string, List<MessageQueue>> _messageQueuesForTopic =
        new(StringComparer.Ordinal);

    private MQClientInstance? _mqClient;
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

    // ---------------- 配置 ----------------
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

    public void SetPollTimeoutMillis(int ms) => _pollTimeoutMillis = Math.Max(0, ms);

    public void SetAutoCommit(bool b) => _autoCommit = b;

    public void SetAutoCommitIntervalMillis(int ms) => _autoCommitIntervalMillis = Math.Max(0, ms);

    public void SetConsumerTimeoutMillisWhenSuspend(int ms) => _consumerTimeoutMillisWhenSuspend = ms;

    public void SetBrokerSuspendMaxTimeMillis(int ms) => _brokerSuspendMaxTimeMillis = ms;

    public void SetPullIntervalMillis(int ms) => _pullIntervalMillis = Math.Max(0, ms);

    public void SetMessageQueueListener(ILiteMessageQueueListener listener) => _messageQueueListener = listener;

    public void SetTopicMetadataCheckIntervalMillis(int ms) =>
        _topicMetadataCheckIntervalMillis = Math.Max(1000, ms);

    public int TopicMetadataCheckIntervalMillis => _topicMetadataCheckIntervalMillis;

    // ---------------- 订阅 / 分配 ----------------
    public void Subscribe(string topic, string subExpression = "*")
    {
        _assignMode = false;
        string ns = WithNamespace(topic);
        lock (_lock)
        {
            _subscription[ns] = subExpression;
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

    public void SetSubExpressionForAssign(string topic, string subExpression)
    {
        string ns = WithNamespace(topic);
        lock (_lock)
        {
            _assignSubExpr[ns] = subExpression;
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

    public void Assign(IReadOnlyList<MessageQueue> messageQueues)
    {
        _assignMode = true;
        var set = new HashSet<MessageQueue>(messageQueues);
        lock (_lock)
        {
            foreach (MessageQueue mq in _assigned)
            {
                if (set.Contains(mq)) continue;
                _nextOffset.Remove(mq);
                _consumeOffset.Remove(mq);
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
    }

    // ---------------- 生命周期 ----------------
    public void Start()
    {
        lock (_lock)
        {
            if (_started) return;
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

            bool hasSub;
            lock (_lock)
            {
                hasSub = _subscription.Count > 0;
            }

            if (_allocateMessageQueueStrategy is null)
            {
                throw new MQClientException("allocateMessageQueueStrategy is null");
            }

            if (!hasSub && !_assignMode)
            {
                throw new MQClientException("subscription is not set, call Subscribe() or Assign() first");
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
            try
            {
                _mqClient.Start();
            }
            catch
            {
                // 起不来就把引用还回去：否则计数漏减，共享实例永远等不到归零收尾
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
        }
    }

    public void Shutdown()
    {
        bool autoCommit;
        List<MessageQueue> scope;
        lock (_lock)
        {
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

    // ---------------- 拉取服务 ----------------
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
                    if (PullOne(mq)) gotAny = true;
                }
            }
            catch
            {
                ClientLog.Debug("lite pull service error");
            }

            int backoffMs = gotAny ? 5 : _pullIntervalMillis;
            Thread.Sleep(backoffMs);
        }
    }

    private string SubscriptionFor(string topic)
    {
        lock (_lock)
        {
            if (_subscription.TryGetValue(topic, out string? s)) return s;
            if (_assignSubExpr.TryGetValue(topic, out string? a)) return a;
        }

        return "*";
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
        int sysFlag = PullSysFlag.BuildSysFlag(commitOffset: false, suspend: false,
            subscription: true, classFilter: false, litePull: true);
        PullResult result;
        try
        {
            result = RequireClient().PullMessage(_consumerGroup, mq, offset, _pullBatchSize, sysFlag, 0,
                sub, /*subVersion=*/0, ExpressionType.TAG, /*timeoutMillis=*/30000, /*maxMsgBytes=*/-1,
                /*suspendTimeoutMillis=*/15000);
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
        FilterTags(mq.Topic, msgs, sub);
        if (msgs.Count == 0)
        {
            // 全被 tag 过滤掉：窗口在 broker 眼里已经读过（游标上面推过了），
            // 只是没有可交付的——不能像旧实现那样原地重拉同一窗口。
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

    private void Enqueue(IReadOnlyList<MessageExt> msgs)
    {
        lock (_bufferLock)
        {
            foreach (MessageExt m in msgs) _localBuffer.Enqueue(m);
            Monitor.PulseAll(_bufferLock);
        }
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

    /// <summary>拉取游标（观测点：真机对拍要看的正是"拉了多少"与"交了多少"这两格的差）。</summary>
    public long PullCursorOf(MessageQueue mq)
    {
        lock (_lock)
        {
            return _nextOffset.TryGetValue(mq, out long o) ? o : -1;
        }
    }

    /// <summary>已消费游标（Poll() 交出去的那一格）。</summary>
    public long ConsumeCursorOf(MessageQueue mq)
    {
        lock (_lock)
        {
            return _consumeOffset.TryGetValue(mq, out long o) ? o : -1;
        }
    }

    /// <summary>内存位点表里那一格（persist=false 提交但还没落盘的值就在这）。</summary>
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
                    _nextOffset.Remove(mq);
                    // 撤队列丢掉的是**整份 MessageQueueState**（pullOffset / consumeOffset /
                    // seekOffset）：留着 seekOffset 就是让这条队列哪天回到本实例时静默跳回
                    // 用户很久以前手动钉过的位置。
                    _consumeOffset.Remove(mq);
                    _seekOffset.Remove(mq);
                }
            }
        }

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
            // 与 rebalance 同口径：订阅信息。
            all.AddRange(RequireClient().GetTopicSubscribeInfo(topic));
        }

        return all;
    }

    // ---------------- Poll / 位点 ----------------
    public List<MessageExt> Poll(int timeoutMillis = -1)
    {
        if (_autoCommit) MaybeAutoCommit();
        int timeout = timeoutMillis > 0 ? timeoutMillis : _pollTimeoutMillis;
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
                outMsgs.Add(_localBuffer.Dequeue());
            }
        }

        AdvanceConsumeOffset(outMsgs);
        return outMsgs;
    }

    public void Seek(MessageQueue mq, long offset)
    {
        lock (_lock)
        {
            _seekOffset[mq] = offset;
            _nextOffset[mq] = offset;
            _consumeOffset[mq] = offset;
        }

        lock (_bufferLock)
        {
            var kept = new Queue<MessageExt>();
            while (_localBuffer.Count > 0)
            {
                MessageExt m = _localBuffer.Dequeue();
                bool drop = m.Topic == mq.Topic && m.BrokerName == mq.BrokerName &&
                            m.QueueId == mq.QueueId && m.QueueOffset < offset;
                if (!drop) kept.Enqueue(m);
            }

            _localBuffer.Clear();
            foreach (MessageExt m in kept) _localBuffer.Enqueue(m);
        }
    }

    public void SeekToBegin(MessageQueue mq) => Seek(mq, RequireClient().GetMinOffset(mq));

    public void SeekToEnd(MessageQueue mq) => Seek(mq, RequireClient().GetMaxOffset(mq));

    public long Committed(MessageQueue mq)
    {
        lock (_lock)
        {
            if (_offsetTable.TryGetValue(mq, out long cached)) return cached;
        }

        MQClientInstance? client = _mqClient;
        if (client is null) return -1;
        long off;
        try
        {
            if (!client.QueryConsumerOffset(_consumerGroup, mq, out off)) return -1;
        }
        catch
        {
            return -1;
        }

        lock (_lock)
        {
            _offsetTable[mq] = off;
        }

        return off;
    }

    /// 提交源绝不能是拉取游标：本地缓冲里压着没交出去的消息不算已消费，提前提交会让那段
    /// 消息在重启后永远不再投递（静默丢消息）。
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
    /// 三个入口的共同部分：写内存位点表（两道守卫），<c>persist</c> 再把这一批刷给 broker。
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
        try
        {
            client.UpdateConsumerOffset(_consumerGroup, mq, offset);
        }
        catch
        {
            ClientLog.Debug("lite persist failed for " + mq);
        }
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
        foreach (KeyValuePair<MessageQueue, long> kv in toSend)
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
            // nameserver 抖动时退回当下缓存再判一次：缓存里还有队列就不该报成"没队列"。
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

    /// <summary>逐个 topic 把当前订阅队列集合与上次快照做集合比对，只有真的变了才回调
    /// OnChanged 并推进快照。某个 topic 查询失败只 log、跳过它自己，**不影响本轮其余
    /// topic**：一个长期查不到路由的 topic 不该把排在它后面的监听器永久饿死。
    /// 返回本轮回调了几个监听器。</summary>
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

    // ---------------- 心跳（把 tag 订阅注册给 broker）----------------
    private HeartbeatData BuildHeartbeat()
    {
        var hb = new HeartbeatData(_clientId);
        var cd = new ConsumerData(_consumerGroup, ConsumeType.ConsumeActively, _messageModel, _consumeFromWhere)
        {
            UnitMode = _unitMode,
        };
        lock (_lock)
        {
            foreach (KeyValuePair<string, SubscriptionData> kv in _subscriptionData)
            {
                cd.AddSubscriptionData(kv.Value);
            }
        }

        hb.AddConsumerData(cd);
        return hb;
    }

    /// <summary>
    /// 为心跳准备 broker 地址：把本实例关注的 topic（订阅的 + assign 到的）路由拉一遍
    /// 并登记为在用。
    /// 心跳只发给路由表里已知的 broker，所以这一步必须在首轮心跳之前。
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
        int ok = 0;
        foreach (string addr in _mqClient.GetAllBrokerAddrs())
        {
            try
            {
                _mqClient.SendHeartbeat(addr, hb, 5000);
                ++ok;
            }
            catch
            {
                ClientLog.Debug("lite heartbeat to " + addr + " failed");
            }
        }

        return ok;
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

            // 5s 心跳间隔（与 push 消费者一致）
            for (int i = 0; i < 50; ++i)
            {
                if (!_running) break;
                Thread.Sleep(100);
            }
        }
    }

    // ---------------- 内部工具 ----------------
    private string WithNamespace(string topic)
    {
        return _namespace.Length == 0 ? topic : NamespaceUtil.WrapNamespace(_namespace, topic);
    }

    private MQClientInstance RequireClient()
    {
        MQClientInstance? c = _mqClient;
        if (!_started || c is null)
        {
            throw new MQClientException("consumer not started, call Start() first");
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

    private static long ParseConsumeTimestamp(string ts)
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
}
