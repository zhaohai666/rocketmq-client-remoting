// 与 push 消费者（DefaultMQPushConsumer）的区别：**由调用方自己拉、自己管位点**。
// 没有后台拉取线程、没有 rebalance、没有消费监听器；只有：
//   FetchSubscribeMessageQueues / Pull / PullAsync / PullBlockIfNotFound /
//   FetchConsumeOffset / UpdateConsumeOffset / SearchOffset / MaxOffset / MinOffset /
//   EarliestMsgStoreTime / SendMessageBack / CreateTopic。
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
    private bool _heartbeatEnabled = true;
    private int _heartbeatBrokerIntervalMillis = 30000;
    private long _heartbeatCount;
    private volatile bool _heartbeatStop;
    private Thread? _heartbeatThread;
    private readonly SortedSet<string> _registerTopics = new(StringComparer.Ordinal);
    private readonly Dictionary<string, IMessageQueueListener> _messageQueueListeners =
        new(StringComparer.Ordinal);
    private IMessageQueueListener? _messageQueueListener;
    private IAllocateMessageQueueStrategy? _allocateMessageQueueStrategy =
        new AllocateMessageQueueAveragely();

    private readonly int _brokerSuspendMaxTimeMillis = 20000;
    private readonly int _consumerPullTimeoutMillis = 10000;
    private readonly int _consumerTimeoutMillisWhenSuspend = 30000;

    private readonly Dictionary<MessageQueue, long> _pullFromWhichNode = new();

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

    private SocksProxyConfig? _socksProxyConfig;

    public SocksProxyConfig? SocksProxyConfig
    {
        get => _socksProxyConfig;
        set => _socksProxyConfig = value;
    }

    /// <summary>是否周期性发心跳（对齐 push 消费者的开关；启动时那一轮不受影响）。</summary>
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

    /// <summary>心跳成功轮数（真机验证用）。</summary>
    public long HeartbeatCount => Interlocked.Read(ref _heartbeatCount);

    // ---------------- ACL 鉴权 ----------------
    /// <summary>必须在 Start() 之前调用：钩子在 Start() 里绑定到 MqClient。</summary>
    public void SetRpcHook(IRpcHook hook) => _rpcHook = hook;

    public void SetCredentials(string accessKey, string secretKey, string securityToken = "") =>
        _rpcHook = new AclClientRPCHook(new SessionCredentials(accessKey, secretKey, securityToken));

    public string ConsumerGroup => _consumerGroup;

    public string ClientId => _clientId;

    public IReadOnlyCollection<string> RegisterTopics => _registerTopics;

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

    // ---------------- 心跳（把消费组注册给 broker） ----------------
    /// <summary>
    /// 为什么拉模式也必须心跳：broker 的 <c>ConsumerManager.consumerTable</c> 是按台的，
    /// 只有心跳（或带订阅标志的拉取）才会建表。没有心跳时 203 consumerConnection /
    /// 38 GET_CONSUMER_LIST_BY_GROUP 都看不到本组，isRejectPullConsumerEnabled=true 的
    /// broker 会给本组每次拉取都回 "the pull consumer is rejected by server"
    /// （PullMessageProcessor:493-505）。而 ClientManageProcessor:87-92 **跳过** ACTIVELY
    /// 类型心跳的订阅注册 —— 拉模式靠自带订阅标志的拉取走补偿分支（:397-412），
    /// 所以缺心跳的失效是静默的：拉取照样成功，只是 broker 侧完全不知道本组存在。
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
        foreach (string t in _registerTopics)
        {
            SubscriptionData sub =
                FilterAPI.BuildSubscriptionData(NamespaceUtil.WrapNamespace(_namespace, t), "*");
            sub.SubVersion = 0;
            cd.AddSubscriptionData(sub);
        }

        hb.AddConsumerData(cd);
        return hb;
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
        int ok = 0;
        foreach (string addr in addrs)
        {
            try
            {
                _mqClient.SendHeartbeat(addr, hb, 5000);
                ++ok;
            }
            catch (Exception e)
            {
                ClientLog.Warn("heartbeat to " + addr + " failed: " + e.Message);
            }
        }

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
            // 停机时由 Shutdown() 置位，最多等一个周期（分片睡，别一次睡满 30s）
            for (int slept = 0; slept < _heartbeatBrokerIntervalMillis && !_heartbeatStop; slept += 100)
            {
                Thread.Sleep(Math.Min(100, _heartbeatBrokerIntervalMillis - slept));
            }

            if (_heartbeatStop || !_heartbeatEnabled)
            {
                continue;
            }

            // 常驻循环的边界：这里逃出的异常会杀掉心跳线程（后续心跳全没了），必须接住；
            // 逐台失败已在 SendHeartbeatToAllBroker 内部处理。
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

    /// <summary>订阅信息口径的全部队列（读位、不筛 master），按 house 排序口径返回。</summary>
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

    /// <summary>按 (topic, brokerName, queueId) 去重；入参必须已排序，重复项必相邻。</summary>
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
        MQClientInstance c = RequireClient();
        int timeout = timeoutMillis > 0 ? timeoutMillis : _consumerPullTimeoutMillis;
        int sysFlag = PullSysFlag.BuildSysFlag(commitOffset: false, suspend: false,
            subscription: true, classFilter: false);
        MessageQueue real = WrapMq(mq);
        SubscriptionData sub = FilterAPI.BuildSubscriptionData(real.Topic, subExpression);
        long brokerId = TakePullFromWhichNode(real);
        PullResult result = c.PullMessage(_consumerGroup, real, offset, maxNums, sysFlag, 0,
            string.IsNullOrEmpty(sub.SubString) ? "*" : sub.SubString,
            0, ExpressionType.TAG, timeout, -1, 15000,
            /*addrIn=*/null, /*requestSource=*/0, /*brokerId=*/brokerId);
        UpdatePullFromWhichNode(real, result);
        return result;
    }

    public PullResult PullBlockIfNotFound(MessageQueue mq, string subExpression, long offset,
        int maxNums)
    {
        MQClientInstance c = RequireClient();
        int sysFlag = PullSysFlag.BuildSysFlag(commitOffset: false, suspend: true,
            subscription: true, classFilter: false);
        MessageQueue real = WrapMq(mq);
        SubscriptionData sub = FilterAPI.BuildSubscriptionData(real.Topic, subExpression);
        long brokerId = TakePullFromWhichNode(real);
        PullResult result = c.PullMessage(_consumerGroup, real, offset, maxNums, sysFlag, 0,
            string.IsNullOrEmpty(sub.SubString) ? "*" : sub.SubString,
            0, ExpressionType.TAG, _consumerTimeoutMillisWhenSuspend, -1,
            _brokerSuspendMaxTimeMillis,
            /*addrIn=*/null, /*requestSource=*/0, /*brokerId=*/brokerId);
        UpdatePullFromWhichNode(real, result);
        return result;
    }

    public void PullAsync(MessageQueue mq, string subExpression, long offset, int maxNums,
        IPullCallback callback, int timeoutMillis = -1)
    {
        ArgumentNullException.ThrowIfNull(callback);
        _ = RequireClient(); // 未启动：同步抛，语义与同步 Pull 对齐（线程池里再抛就太晚了）
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

    /// <summary>按 (topic, brokerName, queueId) 定位队列的重载（免拼 MessageQueue；语义与
    /// <see cref="PullAsync(MessageQueue,string,long,int,IPullCallback,int)"/> 完全一致）。</summary>
    public void PullAsync(string topic, string subExpression, string brokerName, int queueId,
        long offset, int maxNums, IPullCallback callback, int timeoutMillis = -1) =>
        PullAsync(new MessageQueue(topic, brokerName, queueId), subExpression, offset, maxNums,
            callback, timeoutMillis);

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

    // ---------------- 位点管理 ----------------
    /// <summary>返回 false 表示该消费组在该队列上尚无位点（broker 回 QUERY_NOT_FOUND）。</summary>
    public bool FetchConsumeOffset(MessageQueue mq, out long outOffset) =>
        RequireClient().QueryConsumerOffset(_consumerGroup, mq, out outOffset);

    public void UpdateConsumeOffset(MessageQueue mq, long offset) =>
        RequireClient().UpdateConsumerOffset(_consumerGroup, mq, offset);

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
            addr, RequestCode.GetEarliestMsgStoretime, ext, null, false, 5000);
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
        c.InvokeSync(addr, RequestCode.ConsumerSendMsgBack, header.ToExtFields(), null, false, 5000);
    }

    public void CreateTopic(string key, string newTopic, int queueNum = 4)
    {
        MQClientInstance c = RequireClient();
        // C# 的 CreateTopicInRoute 忽略 key 形参，统一走默认 topic（MixAll.DefaultTopic）建路由
        _ = key;
        c.CreateTopicInRoute(NamespaceUtil.WrapNamespace(_namespace, newTopic), queueNum, queueNum, 6);
    }

    // ---------------- 内部工具 ----------------
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
        // 未配置命名空间时直接复用原对象，省一次分配（零开销路径）。
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
