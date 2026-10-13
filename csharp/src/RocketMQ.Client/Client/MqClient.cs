// 职责：NameServer 地址管理、Topic 路由获取与缓存、Broker 地址解析、
// 消息发送（SEND_MESSAGE_V2）、拉取（PULL_MESSAGE）、offset 查询/更新、心跳、
// 按 Key 查询消息、创建 Topic。
//
// 按协议口径逐项实现。
using System.Diagnostics;
using System.Globalization;
using System.Text;
using RocketMQ.Common;
using RocketMQ.Remoting;
using RocketMQ.Remoting.Protocol;

namespace RocketMQ.Client;

public static class ClientIds
{
    private static readonly Stopwatch NanoClock = Stopwatch.StartNew();

    private static long NanoTime() => NanoClock.Elapsed.Ticks * 100L;

    public static string ChangeInstanceNameToPID(string instanceName)
    {
        if (instanceName != MixAll.DefaultInstanceName) return instanceName;
        return UtilAll.Pid().ToString(CultureInfo.InvariantCulture) + "#"
            + NanoTime().ToString(CultureInfo.InvariantCulture);
    }

    public static string BuildMqClientId(string clientIp, string instanceName,
        string? unitName = null, bool enableStreamRequestType = false)
    {
        var sb = new StringBuilder();
        sb.Append(clientIp).Append('@').Append(instanceName);
        if (!string.IsNullOrWhiteSpace(unitName)) sb.Append('@').Append(unitName);
        if (enableStreamRequestType) sb.Append('@').Append("STREAM");
        return sb.ToString();
    }

    public static string Build(string instanceName, string? unitName = null,
        bool enableStreamRequestType = false)
        => BuildMqClientId(MixAll.CachedIpStr(), instanceName, unitName, enableStreamRequestType);
}

internal static class Schedules
{
    /// <summary>睡到 <paramref name="deadlineTick"/>（Environment.TickCount64 口径），
    /// <paramref name="stop"/> 被 Set 时立刻返回（提前返回由调用方的循环条件收尾）。</summary>
    public static void WaitUntil(ManualResetEventSlim stop, long deadlineTick)
    {
        long remain = deadlineTick - Environment.TickCount64;
        if (remain > 0)
        {
            stop.Wait(TimeSpan.FromMilliseconds(remain));
        }
    }

    /// <summary>没有可等待事件的任务（生产者心跳线程）用 100ms 分段睡到计划时刻：
    /// 每段都重新对着**绝对时刻**算，误差不累积，关停响应也在 100ms 内。</summary>
    public static void WaitUntil(Func<bool> running, long deadlineTick)
    {
        long remain;
        while ((remain = deadlineTick - Environment.TickCount64) > 0 && running())
        {
            Thread.Sleep((int)Math.Min(100, remain));
        }
    }
}

public sealed class TopicPublishInfo
{
    private long _index;

    public bool OrderTopic { get; set; }
    public List<MessageQueue> MsgQueueList { get; set; } = new();
    public TopicRouteData TopicRouteData { get; set; } = new();

    public bool Ok() => MsgQueueList.Count > 0;

    public MessageQueue SelectOneMessageQueue()
    {
        if (MsgQueueList.Count == 0)
        {
            throw new MQClientException("no message queue for publish info");
        }

        // 游标用原子自增：生产端可能被多线程并发调用，且游标是跨调用共享状态
        long idx = Interlocked.Increment(ref _index) - 1;
        return MsgQueueList[(int)(idx % MsgQueueList.Count)];
    }

    public MessageQueue SelectOneMessageQueue(string lastBrokerName)
    {
        if (MsgQueueList.Count == 0)
        {
            throw new MQClientException("no message queue for publish info");
        }

        for (int i = 0; i < MsgQueueList.Count; ++i)
        {
            long idx = Interlocked.Increment(ref _index) - 1;
            MessageQueue mq = MsgQueueList[(int)(idx % MsgQueueList.Count)];
            if (mq.BrokerName != lastBrokerName)
            {
                return mq;
            }
        }

        long last = Interlocked.Increment(ref _index) - 1;
        return MsgQueueList[(int)(last % MsgQueueList.Count)];
    }

    /// <summary>
    /// 带过滤器的轮询：游标照常推进，
    /// 一轮内全部不匹配返回 null，由调用方退化选择。filter 与 brokerFilter 都通过才选中。
    /// </summary>
    public MessageQueue? SelectOneMessageQueue(Func<MessageQueue, bool> filter,
                                               Func<MessageQueue, bool> brokerFilter)
    {
        if (MsgQueueList.Count == 0)
        {
            throw new MQClientException("no message queue for publish info");
        }

        for (int i = 0; i < MsgQueueList.Count; ++i)
        {
            long idx = Interlocked.Increment(ref _index) - 1;
            MessageQueue mq = MsgQueueList[(int)(idx % MsgQueueList.Count)];
            if (filter(mq) && brokerFilter(mq))
            {
                return mq;
            }
        }

        return null;
    }

    /// <summary>重置轮询游标（故障规避 resetIndex 用）。</summary>
    public void ResetIndex()
    {
        Interlocked.Exchange(ref _index, 0);
    }
}

/// <summary>客户端核心编排实例。</summary>
public sealed class MQClientInstance : IDisposable
{
    private readonly string _clientId;
    private List<string> _nameServerAddrs;
    private readonly RemotingClient _remotingClient;

    private readonly object _routeLock = new();
    private readonly Dictionary<string, TopicRouteData> _topicRouteTable = new(StringComparer.Ordinal);

    private readonly Dictionary<string, SortedDictionary<long, string>> _brokerAddrTable = new(StringComparer.Ordinal);
    private readonly Dictionary<string, Dictionary<MessageQueue, string>> _topicEndPointsTable =
        new(StringComparer.Ordinal);

    private readonly Dictionary<string, TopicPublishInfo> _topicPublishInfoTable = new(StringComparer.Ordinal);
    private volatile bool _started;

    private readonly HashSet<string> _topicsInUse = new(StringComparer.Ordinal);
    private Thread? _routeRefreshThread;
    private readonly ManualResetEventSlim _routeRefreshStop = new(false);

    private readonly int _pollNameServerIntervalMillis;

    public bool VipChannelEnabled
    {
        get => _vipChannelEnabled;
        set => _vipChannelEnabled = value;
    }

    private bool _vipChannelEnabled;

    internal string VipAddr(string addr) => MixAll.BrokerVipChannel(_vipChannelEnabled, addr);

    public DefaultTopAddressing TopAddressing { get; }
    private Thread? _namesrvRefreshThread;
    private readonly ManualResetEventSlim _namesrvRefreshStop = new(false);

    public ConsumerStatsManager ConsumerStats { get; } = new();

    // ---- broker 主动通知 40（实例级处理器 + 每个消费者的「叫醒」回调）----
    private readonly object _wakeupLock = new();
    private readonly Dictionary<string, Action> _rebalanceWakeups = new(StringComparer.Ordinal);
    private long _consumerIdsChangedCount;

    public void FetchNameServerAddr()
    {
        string? changed = TopAddressing.FetchAndApply();
        if (string.IsNullOrEmpty(changed)) return;
        var addrs = new List<string>();
        foreach (string part in changed.Split(';'))
        {
            string t = part.Trim();
            if (t.Length > 0) addrs.Add(t);
        }
        UpdateNameServerAddressList(addrs);
    }

    /// <summary>是否已 Start（诊断用）。</summary>
    public bool Started => _started;

    /// <summary>实例持有的路由刷新周期（离线用例断言门面透传结果用）。</summary>
    public int PollNameServerIntervalMillis => _pollNameServerIntervalMillis;

    internal static bool TlsEnabledFromEnv() => RemotingClient.EnvTlsEnabled();

    public MQClientInstance(string clientId, IReadOnlyList<string> nameServerAddrs,
        int connectTimeoutMillis = 3000, int invokeTimeoutMillis = 15000, bool? tlsEnable = null,
        TlsOptions? tlsOptions = null, SocksProxyConfig? socksProxy = null, string? unitName = null,
        int pollNameServerIntervalMillis = 30000, bool vipChannelEnabled = false)
    {
        _clientId = clientId;
        _nameServerAddrs = new List<string>(nameServerAddrs);
        _pollNameServerIntervalMillis = pollNameServerIntervalMillis > 0
            ? pollNameServerIntervalMillis
            : 30000;
        _vipChannelEnabled = vipChannelEnabled;
        _remotingClient = new RemotingClient(connectTimeoutMillis, invokeTimeoutMillis, tlsEnable, tlsOptions,
            socksProxy);
        TopAddressing = new DefaultTopAddressing(unitName: unitName);

        _remotingClient.RegisterProcessor(RequestCode.PushReplyMessageToClient, ProcessReplyMessage);

        _remotingClient.RegisterProcessor(RequestCode.NotifyConsumerIdsChanged,
            ProcessNotifyConsumerIdsChanged);
    }


    /// <summary>收到过多少次 broker 的 40 通知。反向请求只有 broker 发得出来，用例
    /// 注入不了，计数是真机断言的唯一落点。</summary>
    public long ConsumerIdsChangedCount => Interlocked.Read(ref _consumerIdsChangedCount);

    public void RegisterRebalanceWakeup(string group, Action wakeup)
    {
        lock (_wakeupLock)
        {
            _rebalanceWakeups[group] = wakeup;
        }
    }

    public void UnregisterRebalanceWakeup(string group)
    {
        lock (_wakeupLock)
        {
            _rebalanceWakeups.Remove(group);
        }
    }

    public void RebalanceImmediately()
    {
        Action[] snapshot;
        lock (_wakeupLock)
        {
            snapshot = _rebalanceWakeups.Values.ToArray();
        }
        foreach (Action wake in snapshot)
        {
            try
            {
                wake();
            }
            catch (Exception e)
            {
                ClientLog.Warn("rebalanceImmediately failed: " + e.Message);
            }
        }
    }

    public RemotingCommand? ProcessNotifyConsumerIdsChanged(RemotingCommand cmd, string addr)
    {
        var header = new NotifyConsumerIdsChangedRequestHeader();
        header.FromExtFields(cmd.ExtFields ?? new PropertyMap());
        Interlocked.Increment(ref _consumerIdsChangedCount);
        ClientLog.Info("receive broker's notification[" + addr + "], the consumer group: "
                       + (header.ConsumerGroup ?? string.Empty) + " changed, rebalance immediately");
        RebalanceImmediately();
        return null;
    }

    private readonly object _facadeProcessorLock = new();
    private readonly Dictionary<int, List<RemotingClient.RequestProcessor>> _facadeProcessors = new();

    /// <summary>门面把反向请求处理器登记到实例（共享实例下按登记人数扇出，不互相覆盖）。</summary>
    public void RegisterFacadeProcessor(int requestCode, RemotingClient.RequestProcessor handler)
    {
        lock (_facadeProcessorLock)
        {
            if (!_facadeProcessors.TryGetValue(requestCode, out List<RemotingClient.RequestProcessor>? list))
            {
                list = new List<RemotingClient.RequestProcessor>();
                _facadeProcessors[requestCode] = list;
                _remotingClient.RegisterProcessor(requestCode, DispatchFacadeProcessor);
            }

            list.Add(handler);
        }
    }

    /// <summary>门面退出时摘除自己的处理器；最后一个摘除者负责卸掉传输层的槽。</summary>
    public void UnregisterFacadeProcessor(int requestCode, RemotingClient.RequestProcessor handler)
    {
        lock (_facadeProcessorLock)
        {
            if (!_facadeProcessors.TryGetValue(requestCode, out List<RemotingClient.RequestProcessor>? list))
            {
                return;
            }

            list.Remove(handler);
            if (list.Count == 0)
            {
                _facadeProcessors.Remove(requestCode);
                _remotingClient.UnregisterProcessor(requestCode);
            }
        }
    }

    /// <summary>
    /// 驱动一次门面处理器的多播分派。公开只为让离线用例能驱动这条反向路径
    /// （真连接上 broker 推不进来，与 <see cref="ProcessNotifyConsumerIdsChanged"/> 同一理由），
    /// 勿用于业务代码 —— 传输层自己会按注册表调它。
    /// 返回第一个非 null 的应答；该 code 没有任何登记者时返回 null。
    /// </summary>
    public RemotingCommand? DispatchFacadeProcessor(RemotingCommand cmd, string addr)
    {
        RemotingClient.RequestProcessor[] snapshot;
        lock (_facadeProcessorLock)
        {
            if (!_facadeProcessors.TryGetValue(cmd.Code, out List<RemotingClient.RequestProcessor>? list))
            {
                return null;
            }

            snapshot = list.ToArray();
        }

        foreach (RemotingClient.RequestProcessor handler in snapshot)
        {
            try
            {
                RemotingCommand? resp = handler(cmd, addr);
                if (resp is not null)
                {
                    return resp;
                }
            }
            catch (Exception e)
            {
                ClientLog.Warn("facade processor for code " + cmd.Code + " failed: " + e.Message);
            }
        }

        return null;
    }

    /// <summary>某 code 当前登记了几个门面处理器（测试观测面：共享实例下互不覆盖的落点）。</summary>
    public int FacadeProcessorCount(int requestCode)
    {
        lock (_facadeProcessorLock)
        {
            return _facadeProcessors.TryGetValue(requestCode,
                out List<RemotingClient.RequestProcessor>? list) ? list.Count : 0;
        }
    }

    public bool RegisterRpcHook(IRpcHook hook) => _remotingClient.RegisterRpcHook(hook);

    // ---------------- 生命周期 ----------------

    public void Start()
    {
        _started = true;
        ConsumerStats.Start();
        if (_nameServerAddrs.Count == 0 && !string.IsNullOrEmpty(TopAddressing.WsAddr))
        {
            FetchNameServerAddr();
            if (_nameServerAddrs.Count == 0)
            {
                throw new MQClientException("name server address is not set and address server ("
                    + TopAddressing.WsAddr + ") returned none",
                    ClientErrorCode.NoNameServerException);
            }
            if (_namesrvRefreshThread is null)
            {
                _namesrvRefreshStop.Reset();
                _namesrvRefreshThread = new Thread(NamesrvRefreshLoop)
                {
                    IsBackground = true,
                    Name = "rmq-namesrv-refresh-" + _clientId,
                };
                _namesrvRefreshThread.Start();
            }
        }
        string ns = string.Join(";", _nameServerAddrs);
        ClientLog.Info("MQClientInstance[" + _clientId + "] started, namesrv=" + ns);
        if (_routeRefreshThread is null)
        {
            _routeRefreshStop.Reset();
            _routeRefreshThread = new Thread(RouteRefreshLoop)
            {
                IsBackground = true,
                Name = "rmq-route-refresh-" + _clientId,
            };
            _routeRefreshThread.Start();
        }
    }

    public void Shutdown()
    {
        _started = false;
        _routeRefreshStop.Set();
        _namesrvRefreshStop.Set();
        if (_namesrvRefreshThread is { IsAlive: true })
        {
            _namesrvRefreshThread.Join(2000);
        }
        if (_routeRefreshThread is { IsAlive: true })
        {
            _routeRefreshThread.Join(2000);
        }

        ConsumerStats.Shutdown();
        // 40 的回调是消费者登记的闭包（捕获了消费者状态）：关连接之前先摘掉处理器和
        // 回调表，避免收尾期间 broker 的 40 还打进一个正在退出的消费者。
        _remotingClient.UnregisterProcessor(RequestCode.NotifyConsumerIdsChanged);
        lock (_wakeupLock)
        {
            _rebalanceWakeups.Clear();
        }
        lock (_facadeProcessorLock)
        {
            _facadeProcessors.Clear();
        }
        _remotingClient.Shutdown();
    }

    private void NamesrvRefreshLoop()
    {
        long next = Environment.TickCount64 + 10_000;
        while (!_namesrvRefreshStop.IsSet)
        {
            if (!_started) return;
            Schedules.WaitUntil(_namesrvRefreshStop, next);
            next += 120_000;
            try
            {
                FetchNameServerAddr();
            }
            catch (Exception e)
            {
                ClientLog.Debug("fetchNameServerAddr exception: " + e.Message);
            }
        }
    }

    public void RegisterTopicInUse(string topic)
    {
        if (topic.Length > 0)
        {
            lock (_routeLock)
            {
                _topicsInUse.Add(topic);
            }
        }
    }

    private void RouteRefreshLoop()
    {
        long next = Environment.TickCount64 + 10;
        while (!_routeRefreshStop.IsSet)
        {
            if (!_started) return;

            Schedules.WaitUntil(_routeRefreshStop, next);
            next += _pollNameServerIntervalMillis;

            List<string> topics;
            lock (_routeLock)
            {
                topics = new List<string>(_topicsInUse);
            }

            foreach (string topic in topics)
            {
                try
                {
                    UpdateTopicRouteInfoFromNameServer(topic);
                }
                catch (Exception e)
                {
                    ClientLog.Debug("route refresh failed for " + topic + ": " + e.Message);
                }
            }
        }
    }

    public void Dispose() => Shutdown();

    public string ClientId => _clientId;

    public IReadOnlyList<string> NameServerAddrs => _nameServerAddrs;

    public RemotingClient RemotingClient => _remotingClient;

    public void UpdateNameServerAddressList(IReadOnlyList<string> addrs)
    {
        if (addrs.Count > 0)
        {
            _nameServerAddrs = new List<string>(addrs);
        }
    }

    private RemotingCommand InvokeSyncOnAddr(string addr, RemotingCommand request, int timeoutMillis) =>
        _remotingClient.InvokeSync(addr, request, timeoutMillis);

    public void InvokeAsyncOnAddr(string addr, RemotingCommand request, int timeoutMillis,
        RemotingClient.InvokeCallback callback) =>
        _remotingClient.InvokeAsync(addr, request, callback, timeoutMillis);

    /// <summary>把响应码非 SUCCESS 转成 MQBrokerException。</summary>
    public static void CheckResponseCode(RemotingCommand response)
    {
        if (response.Code != ResponseCode.Success)
        {
            throw new MQBrokerException(response.Code, response.Remark);
        }
    }

    // ---------------- 路由管理 ----------------

    public bool UpdateTopicRouteInfoFromNameServer(string topic, bool isDefault = false, int timeoutMillis = 5000)
    {
        if (_nameServerAddrs.Count == 0)
        {
            throw new MQClientException("name server address list is empty",
                ClientErrorCode.NoNameServerException);
        }

        bool Fetch(string t, out TopicRouteData @out)
        {
            @out = new TopicRouteData();
            var request = RemotingCommand.CreateRequestCommand(RequestCode.GetRouteinfoByTopic, null);
            request.ExtFields["topic"] = t;
            foreach (string nsAddr in _nameServerAddrs)
            {
                try
                {
                    RemotingCommand response = InvokeSyncOnAddr(nsAddr, request, timeoutMillis);
                    if (response.Code == ResponseCode.Success && response.Body.Length > 0)
                    {
                        return TopicRouteData.Decode(response.Body, out @out);
                    }

                    // 第一个可达的 NS 明确返回非 SUCCESS（如 TOPIC_NOT_EXIST）就停止轮询
                    break;
                }
                catch (RemotingException)
                {
                    continue;
                }
            }

            return false;
        }

        bool ok = Fetch(topic, out TopicRouteData route);
        if (!ok && isDefault && topic != MixAll.DefaultTopic)
        {
            if (Fetch(MixAll.DefaultTopic, out TopicRouteData defaultRoute))
            {
                foreach (QueueData qd in defaultRoute.QueueDatas)
                {
                    if (qd.WriteQueueNums > MixAll.DefaultTopicQueueNums)
                    {
                        qd.WriteQueueNums = MixAll.DefaultTopicQueueNums;
                    }

                    if (qd.ReadQueueNums > MixAll.DefaultTopicQueueNums)
                    {
                        qd.ReadQueueNums = MixAll.DefaultTopicQueueNums;
                    }
                }

                route = defaultRoute;
                ok = true;
            }
        }

        if (!ok)
        {
            return false;
        }

        lock (_routeLock)
        {
            _topicRouteTable[topic] = route;

            foreach (BrokerData bd in route.BrokerDatas)
            {
                _brokerAddrTable[bd.BrokerName] = bd.BrokerAddrs;
            }

            var endpoints = new Dictionary<MessageQueue, string>();
            if (StaticTopic.TopicRouteData2EndpointsForStaticTopic(topic, route, endpoints))
            {
                _topicEndPointsTable[topic] = endpoints;
            }
            else
            {
                _topicEndPointsTable.Remove(topic);
            }

            if (!_topicPublishInfoTable.TryGetValue(topic, out TopicPublishInfo? publish) || publish is null)
            {
                publish = new TopicPublishInfo();
                _topicPublishInfoTable[topic] = publish;
            }

            publish.OrderTopic = route.OrderTopicConf.Length > 0;
            publish.TopicRouteData = route;
            publish.MsgQueueList = route.GetAllMessageQueue(topic);
            if (topic.Contains("ORDER", StringComparison.Ordinal))
            {
                publish.OrderTopic = true;
            }
        }

        return true;
    }

    /// <summary>
    /// 取发布信息（**缓存实例共享**，轮询游标在实例内推进）；
    /// 缓存未命中会触发一次路由刷新，仍拿不到则抛 MQClientException。
    /// <paramref name="isDefault"/> 透传给路由刷新：仅生产者发真实路由拉不到时传 true。
    /// </summary>
    public TopicPublishInfo GetTopicPublishInfo(string topic, bool isDefault = false)
    {
        lock (_routeLock)
        {
            if (_topicPublishInfoTable.TryGetValue(topic, out TopicPublishInfo? hit) && hit is not null && hit.Ok())
            {
                return hit;
            }
        }

        UpdateTopicRouteInfoFromNameServer(topic, isDefault);
        lock (_routeLock)
        {
            if (_topicPublishInfoTable.TryGetValue(topic, out TopicPublishInfo? p) && p is not null && p.Ok())
            {
                return p;
            }
        }

        throw new MQClientException("Can not find Message Queue for topic: " + topic);
    }

    public List<MessageQueue> GetTopicSubscribeInfo(string topic)
    {
        TopicRouteData? route = GetTopicRouteData(topic);
        if (route is null)
        {
            return new List<MessageQueue>();
        }

        return route.GetAllSubscribeMessageQueue(topic);
    }

    /// <summary>取缓存路由；未命中会尝试刷新一次，仍没有返回 null。</summary>
    public TopicRouteData? GetTopicRouteData(string topic)
    {
        lock (_routeLock)
        {
            if (_topicRouteTable.TryGetValue(topic, out TopicRouteData? hit))
            {
                return hit;
            }
        }

        try
        {
            UpdateTopicRouteInfoFromNameServer(topic);
        }
        catch (Exception)
        {
            // 路由刷新失败不抛，交给下面的查表返回空
        }

        lock (_routeLock)
        {
            return _topicRouteTable.TryGetValue(topic, out TopicRouteData? route) ? route : null;
        }
    }

    public static string FindBrokerAddrInRoute(TopicRouteData route, string brokerName)
    {
        foreach (BrokerData bd in route.BrokerDatas)
        {
            if (bd.BrokerName == brokerName)
            {
                return bd.SelectBrokerAddr();
            }
        }

        return string.Empty;
    }

    public static (string Addr, bool IsSlave) FindBrokerAddressInSubscribe(
        IReadOnlyDictionary<long, string> brokerAddrs, long brokerId, bool onlyThisBroker = false)
    {
        if (brokerAddrs.Count == 0)
        {
            return (string.Empty, false);
        }

        if (brokerAddrs.TryGetValue(brokerId, out string? hit))
        {
            return (hit, brokerId != MixAll.MasterId);
        }

        if (brokerId != MixAll.MasterId
            && brokerAddrs.TryGetValue(brokerId + 1, out string? next))
        {
            return (next, true);
        }

        if (!onlyThisBroker)
        {
            long minId = brokerAddrs.Keys.Min();
            return (brokerAddrs[minId], minId != MixAll.MasterId);
        }

        return (string.Empty, false);
    }

    private string BrokerAddr(MessageQueue mq)
    {
        TopicRouteData? route = GetTopicRouteData(mq.Topic);
        if (route is null)
        {
            throw new MQClientNoRouteException(mq.Topic);
        }

        string addr = FindBrokerAddrInRoute(route, mq.BrokerName);
        if (addr.Length == 0)
        {
            throw new MQClientException("Broker " + mq.BrokerName + " not found in route of topic "
                + mq.Topic);
        }

        return addr;
    }

    public string BrokerAddrOf(string brokerName)
    {
        lock (_routeLock)
        {
            foreach (var kv in _topicRouteTable)
            {
                string addr = FindBrokerAddrInRoute(kv.Value, brokerName);
                if (addr.Length > 0)
                {
                    return addr;
                }
            }
        }

        return string.Empty;
    }

    public List<string> GetRouteOfAllBrokers()
    {
        var addrs = new List<string>();
        lock (_routeLock)
        {
            foreach (var kv in _topicRouteTable)
            {
                foreach (BrokerData bd in kv.Value.BrokerDatas)
                {
                    string a = bd.SelectBrokerAddr();
                    if (a.Length > 0 && !addrs.Contains(a))
                    {
                        addrs.Add(a);
                    }
                }
            }
        }

        return addrs;
    }

    /// 返回空串是正常结果（master 掉线就是这个形状），报什么错由调用方决定
    /// （<c>sendKernelImpl</c> 报 MQClientException、<c>endTransaction</c> 什么都不报）。
    /// </summary>
    public string FindBrokerAddressInPublish(string brokerName)
    {
        lock (_routeLock)
        {
            if (_brokerAddrTable.TryGetValue(brokerName, out SortedDictionary<long, string>? addrs)
                && addrs is not null
                && addrs.TryGetValue(MixAll.MasterId, out string? master))
            {
                return master ?? string.Empty;
            }

            return string.Empty;
        }
    }

    /// 对应 <c>DefaultMQProducerImpl.sendKernelImpl:919-924</c>（发送）、
    /// <c>DefaultMQPushConsumerImpl.changePopInvisibleTimeAsync:869-876</c> / <c>ackAsync</c>
    /// （POP 的 ack 与延长不可见时间走 <c>findBrokerAddressInSubscribe(brokerName, MASTER_ID, true)</c>，
    /// 只要主），以及 <c>MQAdminImpl</c> 的 offset 查询（见 <see cref="PublishAddrInAdmin"/>）。
    /// 形状一致：查发布地址（只认 brokerId=0）→ 查不到按 topic 刷一次路由 → 重查 →
    /// 仍查不到照 <c>sendKernelImpl:1100</c> 抛
    /// <c>MQClientException("The broker[X] not exist")</c>（本端口码为 -1，表示
    /// 「不是 broker 回的码」）。
    ///
    /// 定点发送不会在 <c>sendDefaultImpl</c> 里取发布信息，这里是它唯一的路由来源；
    /// 主从切换期间这也是「本端立刻报错」与「把请求打到从节点上白挨一轮 SYSTEM_BUSY(2)」的
    /// 分水岭 —— 从节点不接 SEND_MESSAGE / CONSUMER_SEND_MSG_BACK / CHANGE_INVISIBLE_TIME
    /// 这些写请求。
    /// </summary>
    public string PublishAddrFor(string brokerName, string topic)
    {
        string addr = FindBrokerAddressInPublish(brokerName);
        if (addr.Length == 0)
        {
            UpdateTopicRouteInfoFromNameServer(topic);
            addr = FindBrokerAddressInPublish(brokerName);
        }

        if (addr.Length == 0)
        {
            throw new MQClientException("The broker[" + brokerName + "] not exist", -1);
        }

        return addr;
    }

    public string PublishAddrInAdmin(MessageQueue mq) => PublishAddrFor(mq.BrokerName, mq.Topic);

    public string GetBrokerNameFromMessageQueue(MessageQueue mq)
    {
        lock (_routeLock)
        {
            if (_topicEndPointsTable.TryGetValue(mq.Topic,
                    out Dictionary<MessageQueue, string>? endpoints)
                && endpoints is { Count: > 0 }
                && endpoints.TryGetValue(mq, out string? real) && real is not null)
            {
                return real;
            }
        }

        return mq.BrokerName;
    }

    public string PopAckAddrFor(string topic, string brokerName, int queueId)
    {
        string ResolveDes()
        {
            return StaticTopic.IsLogicalQueueMockBrokerName(brokerName)
                ? GetBrokerNameFromMessageQueue(new MessageQueue(topic, brokerName, queueId))
                : brokerName;
        }

        string des = ResolveDes();
        string addr = FindBrokerAddressInPublish(des);
        if (addr.Length == 0)
        {
            UpdateTopicRouteInfoFromNameServer(topic);
            des = ResolveDes();
            addr = FindBrokerAddressInPublish(des);
        }

        if (addr.Length == 0)
        {
            throw new MQClientException("Broker[" + des + "] master node does not exist", -1);
        }

        return addr;
    }

    /// <summary>测试工程与本程序集没有 InternalsVisibleTo 关系，故用 public，勿用于业务代码：
    /// 离线用例没有名字服务，直接播种端点表（等价于一次静态主题路由落库）。</summary>
    public void SetTopicEndPointsForTest(string topic, Dictionary<MessageQueue, string> endpoints)
    {
        lock (_routeLock)
        {
            _topicEndPointsTable[topic] = new Dictionary<MessageQueue, string>(endpoints);
        }
    }

    public List<string> KnownBrokerAddrs() => GetRouteOfAllBrokers();

    public List<string> GetAllBrokerAddrs()
    {
        var addrs = new List<string>();
        lock (_routeLock)
        {
            foreach (var kv in _topicRouteTable)
            {
                foreach (BrokerData bd in kv.Value.BrokerDatas)
                {
                    foreach (string a in bd.BrokerAddrs.Values)
                    {
                        if (a.Length > 0 && !addrs.Contains(a))
                        {
                            addrs.Add(a);
                        }
                    }
                }
            }
        }

        return addrs;
    }

    // ---------------- 消息发送 ----------------

    private static int ParseIntOrZero(string? value) =>
        TryParseInt(value) ?? 0;

    private static int? TryParseInt(string? value) =>
        int.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out int parsed)
            ? parsed
            : null;

    public RemotingCommand BuildSendRequest(string producerGroup, Message msg, MessageQueue mq,
        int sysFlag = 0, bool unitMode = false, string? createTopicKey = null,
        int? defaultTopicQueueNums = null)
    {
        int reconsumeTimes = 0;
        int? maxReconsumeTimes = null;
        if (MixAll.IsRetryTopic(msg.Topic))
        {
            reconsumeTimes = ParseIntOrZero(msg.GetProperty(MessageConst.PropertyReconsumeTime));
            maxReconsumeTimes = TryParseInt(msg.GetProperty(MessageConst.PropertyMaxReconsumeTimes));
        }

        var header = new SendMessageRequestHeaderV2
        {
            ProducerGroup = producerGroup,
            Topic = msg.Topic,
            DefaultTopic = string.IsNullOrEmpty(createTopicKey) ? MixAll.DefaultTopic : createTopicKey,
            DefaultTopicQueueNums = defaultTopicQueueNums ?? MixAll.DefaultTopicQueueNums,
            QueueId = mq.QueueId,
            SysFlag = sysFlag,
            BornTimestamp = UtilAll.CurrentTimeMillis(),
            Flag = msg.Flag,
            Properties = MessageDecoder.MessagePropertiesToString(msg.Properties),
            ReconsumeTimes = reconsumeTimes,
            UnitMode = unitMode,
            MaxReconsumeTimes = maxReconsumeTimes,
            Batch = msg.IsBatch,
            BrokerName = string.IsNullOrEmpty(mq.BrokerName) ? null : mq.BrokerName,
        };

        int code;
        if (RequestReply.IsReplyMessage(msg))
        {
            code = RequestCode.SendReplyMessageV2;
        }
        else
        {
            code = msg.IsBatch ? RequestCode.SendBatchMessage : RequestCode.SendMessageV2;
        }

        RemotingCommand request = RemotingCommand.CreateRequestCommand(code, header);
        request.Body = msg.Body;
        request.HasBody = true;
        return request;
    }

    public SendResult SendMessage(string producerGroup, Message msg, MessageQueue mq,
        int timeoutMillis = 3000, int sysFlag = 0, bool unitMode = false,
        string? createTopicKey = null, int? defaultTopicQueueNums = null)
    {
        string addr = VipAddr(PublishAddrFor(mq.BrokerName, mq.Topic));
        if (!msg.IsBatch)
        {
            MessageClientIDSetter.SetUniqId(msg);
        }

        RemotingCommand request = BuildSendRequest(producerGroup, msg, mq, sysFlag, unitMode,
            createTopicKey, defaultTopicQueueNums);
        RemotingCommand response = InvokeSyncOnAddr(addr, request, timeoutMillis);
        return ProcessSendResponse(response, msg, mq);
    }

    public SendResult ProcessSendResponse(RemotingCommand response, Message msg, MessageQueue mq)
    {
        SendStatus status;
        switch (response.Code)
        {
            case ResponseCode.Success:
                status = SendStatus.SendOk;
                break;
            case ResponseCode.FlushDiskTimeout:
                status = SendStatus.FlushDiskTimeout;
                break;
            case ResponseCode.FlushSlaveTimeout:
                status = SendStatus.FlushSlaveTimeout;
                break;
            case ResponseCode.SlaveNotAvailable:
                status = SendStatus.SlaveNotAvailable;
                break;
            default:
                throw new MQBrokerException(response.Code, response.Remark);
        }

        var respHeader = new SendMessageResponseHeader();
        respHeader.FromExtFields(response.ExtFields);
        string clientUniqId = MessageClientIDSetter.GetUniqId(msg);
        string msgId = !string.IsNullOrEmpty(clientUniqId)
            ? clientUniqId
            : respHeader.BatchUniqId ?? respHeader.MsgId ?? string.Empty;
        string regionId = MixAll.DefaultTraceRegionId;
        if (response.ExtFields.TryGetValue(MessageConst.PropertyMsgRegion, out string? rid)
            && !string.IsNullOrEmpty(rid))
        {
            regionId = rid;
        }

        bool traceOn = true;
        if (response.ExtFields.TryGetValue(MessageConst.PropertyTraceSwitch, out string? ts)
            && ts == "false")
        {
            traceOn = false;
        }

        return new SendResult
        {
            SendStatus = status,
            MsgId = msgId,
            OffsetMsgId = respHeader.MsgId ?? string.Empty,
            MessageQueue = new MessageQueue(mq.Topic, mq.BrokerName, respHeader.QueueId ?? mq.QueueId),
            QueueOffset = respHeader.QueueOffset ?? 0,
            TransactionId = respHeader.TransactionId ?? string.Empty,
            RegionId = regionId,
            TraceOn = traceOn,
            RecallHandle = respHeader.RecallHandle,
        };
    }

    public void SendMessageOneway(string producerGroup, Message msg, MessageQueue mq,
        int timeoutMillis = 3000, int sysFlag = 0, bool unitMode = false,
        string? createTopicKey = null, int? defaultTopicQueueNums = null)
    {
        // 单向发送同样是写请求，地址口径与同步发送一致（只认 master + 同一处 VIP 改写）。
        string addr = VipAddr(PublishAddrFor(mq.BrokerName, mq.Topic));
        // 单向发送同样补 UNIQ_KEY（与同步发送语义一致）
        if (!msg.IsBatch)
        {
            MessageClientIDSetter.SetUniqId(msg);
        }

        RemotingCommand request = BuildSendRequest(producerGroup, msg, mq, sysFlag, unitMode,
            createTopicKey, defaultTopicQueueNums);
        request.MarkOnewayRpc();
        _remotingClient.InvokeOneway(addr, request);
    }

    // ---------------- 定时消息撤回 ----------------

    public string RecallMessage(string addr, RecallMessageRequestHeader header,
        int timeoutMillis = 3000)
    {
        RemotingCommand request = RemotingCommand.CreateRequestCommand(RequestCode.RecallMessage, header);
        RemotingCommand response = InvokeSyncOnAddr(addr, request, timeoutMillis);
        CheckResponseCode(response);
        var respHeader = new RecallMessageResponseHeader();
        respHeader.FromExtFields(response.ExtFields);
        if (string.IsNullOrEmpty(respHeader.MsgId))
        {
            throw new MQBrokerException(response.Code, "recall message response has no msgId");
        }
        return respHeader.MsgId!;
    }

    // ---------------- Request-Reply：接收 broker 推回的应答（326）----------------

    /// <summary>
    /// 处理 PUSH_REPLY_MESSAGE_TO_CLIENT(326)：把应答交给等待中的 Request()。
    public RemotingCommand? ProcessReplyMessage(RemotingCommand cmd, string addr)
    {
        var header = new ReplyMessageRequestHeader();
        try
        {
            header.FromExtFields(cmd.ExtFields ?? new PropertyMap());
        }
        catch (Exception e)
        {
            ClientLog.Warn("processReplyMessage: decode header failed from " + addr + ": " + e.Message);
            return RemotingCommand.CreateResponseCommand(
                ResponseCode.SystemError, "process reply message fail: " + e.Message);
        }

        try
        {
            byte[] body = cmd.Body ?? Array.Empty<byte>();
            int sysFlag = header.SysFlag ?? 0;
            if (MessageSysFlag.IsCompressed(sysFlag))
            {
                body = CompressorFactory.Decompress(body, MessageSysFlag.GetCompressionType(sysFlag));
            }

            var msg = new MessageExt
            {
                Topic = header.Topic ?? string.Empty,
                Body = body,
                QueueId = header.QueueId ?? 0,
                StoreTimestamp = header.StoreTimestamp ?? 0,
                Flag = header.Flag ?? 0,
                BornTimestamp = header.BornTimestamp ?? 0,
                ReconsumeTimes = header.ReconsumeTimes ?? 0,
            };
            if (!string.IsNullOrEmpty(header.BornHost))
            {
                msg.BornHost = header.BornHost;
            }

            if (!string.IsNullOrEmpty(header.StoreHost))
            {
                msg.StoreHost = header.StoreHost;
            }

            PropertyMap props = MessageDecoder.StringToMessageProperties(header.Properties ?? string.Empty);
            foreach (var kv in props)
            {
                msg.Properties[kv.Key] = kv.Value;
            }

            msg.PutProperty(MessageConst.PropertyReplyMessageArriveTime,
                UtilAll.CurrentTimeMillis().ToString(CultureInfo.InvariantCulture));

            string correlationId = msg.GetProperty(MessageConst.PropertyCorrelationId);
            if (RequestFutureHolder.Instance.PutResponse(correlationId, msg) is null)
            {
                ClientLog.Warn("receive reply message, but not matched any request, CorrelationId: "
                    + correlationId + ", reply from host: " + (header.BornHost ?? addr));
            }

            return RemotingCommand.CreateResponseCommand(ResponseCode.Success, null);
        }
        catch (Exception e)
        {
            // 解析失败绝不能让读线程崩：回 SYSTEM_ERROR（broker 侧记 warn 但不丢连接）。
            ClientLog.Warn("unknown err when receiveReplyMsg: " + e.Message);
            return RemotingCommand.CreateResponseCommand(
                ResponseCode.SystemError, "process reply message fail: " + e.Message);
        }
    }

    // ---------------- 消息拉取 ----------------

    public PullResult PullMessage(string consumerGroup, MessageQueue mq,
        long queueOffset, int maxMsgNums, int sysFlag,
        long commitOffset, string subscription,
        long subVersion, string expressionType,
        int timeoutMillis = 30000, int maxMsgBytes = -1,
        int suspendTimeoutMillis = 15000,
        string? addrIn = null,
        int requestSource = 0,
        long? brokerId = null)
    {
        string addr = addrIn ?? string.Empty;
        if (addr.Length == 0)
        {
            if (brokerId is null)
            {
                addr = BrokerAddr(mq);
            }
            else
            {
                TopicRouteData? route = GetTopicRouteData(mq.Topic)
                    ?? throw new MQClientNoRouteException(mq.Topic);
                BrokerData? brokerData = null;
                foreach (BrokerData bd in route.BrokerDatas)
                {
                    if (bd.BrokerName == mq.BrokerName)
                    {
                        brokerData = bd;
                        break;
                    }
                }

                if (brokerData is null)
                {
                    throw new MQClientException("Broker " + mq.BrokerName + " not exist");
                }

                (addr, bool isSlave) = FindBrokerAddressInSubscribe(
                    brokerData.BrokerAddrs, brokerId.Value);
                if (addr.Length == 0)
                {
                    throw new MQClientException("Broker " + mq.BrokerName + " not exist");
                }

                if (isSlave)
                {
                    sysFlag = PullSysFlag.ClearCommitOffsetFlag(sysFlag);
                }
            }
        }

        var header = new PullMessageRequestHeader
        {
            ConsumerGroup = consumerGroup,
            Topic = mq.Topic,
            QueueId = mq.QueueId,
            QueueOffset = queueOffset,
            MaxMsgNums = maxMsgNums,
            SysFlag = sysFlag,
            CommitOffset = commitOffset,
            SuspendTimeoutMillis = suspendTimeoutMillis,
            Subscription = PullSysFlag.HasSubscriptionFlag(sysFlag) ? subscription : null,
            SubVersion = subVersion,
            ExpressionType = expressionType,
            MaxMsgBytes = maxMsgBytes,
            RequestSource = requestSource,
        };

        int requestCode = PullSysFlag.HasLitePullFlag(sysFlag)
            ? RequestCode.LitePullMessage
            : RequestCode.PullMessage;
        RemotingCommand request = RemotingCommand.CreateRequestCommand(requestCode, header);
        RemotingCommand response = InvokeSyncOnAddr(addr, request, timeoutMillis);

        PullStatus status;
        switch (response.Code)
        {
            case ResponseCode.Success:
                status = PullStatus.Found;
                break;
            case ResponseCode.PullNotFound:
                status = PullStatus.NoNewMsg;
                break;
            case ResponseCode.PullOffsetMoved:
                status = PullStatus.OffsetIllegal;
                break;
            case ResponseCode.PullRetryImmediately:
                status = PullStatus.NoMatchedMsg;
                break;
            default:
                throw new MQBrokerException(response.Code, response.Remark);
        }

        var respHeader = new PullMessageResponseHeader();
        respHeader.FromExtFields(response.ExtFields);

        var result = new PullResult
        {
            Status = status,
            NextBeginOffset = respHeader.NextBeginOffset ?? 0,
            MinOffset = respHeader.MinOffset ?? 0,
            MaxOffset = respHeader.MaxOffset ?? 0,
            // 透传给调用方，由它回写 pullFromWhichNodeTable（processPullResult:77）
            SuggestWhichBrokerId = respHeader.SuggestWhichBrokerId,
        };
        if (response.Body.Length > 0)
        {
            result.MsgFoundList = MessageDecoder.DecodeMessages(response.Body);
            foreach (MessageExt m in result.MsgFoundList)
            {
                m.BrokerName = mq.BrokerName;
                m.QueueId = mq.QueueId;
            }
        }

        return result;
    }

    // ---------------- POP（5.x 轻量消费） ----------------

    /// ⚠ 这是 POP 最容易踩的坑：**普通 topic 直连 POP 时 broker 不在消息上写 POP_CK**
    /// （只有 retry topic 的重编码路径才写），而 ACK 必须要这个串，所以只能由客户端用
    /// 响应头的 startOffsetInfo / msgOffsetInfo 反构出来。
    ///
    /// 公开为 static 是为了单测能直接覆盖这段纯逻辑（不联网）。
    /// </summary>
    public static void StampPopCk(List<MessageExt> msgs, string brokerName,
        PopMessageResponseHeader respHeader)
    {
        long popTime = respHeader.PopTime ?? 0;
        long invisibleTime = respHeader.InvisibleTime ?? 0;
        int reviveQid = respHeader.ReviveQid ?? 0;
        string startOffsetInfo = respHeader.StartOffsetInfo ?? string.Empty;
        string msgOffsetInfo = respHeader.MsgOffsetInfo ?? string.Empty;

        if (startOffsetInfo.Length == 0)
        {
            var perQueue = new Dictionary<string, string>();
            foreach (MessageExt m in msgs)
            {
                string key = m.Topic + m.QueueId.ToString(CultureInfo.InvariantCulture);
                if (!perQueue.TryGetValue(key, out string? built))
                {
                    built = ExtraInfoUtil.BuildExtraInfo(m.QueueOffset, popTime, invisibleTime,
                        reviveQid, m.Topic, brokerName, m.QueueId);
                    perQueue[key] = built;
                }

                m.Properties[MessageConst.PropertyPopCk] = built + ExtraInfoUtil.KeySeparator
                    + m.QueueOffset.ToString(CultureInfo.InvariantCulture);
            }
        }
        else
        {
            Dictionary<string, long>? startMap = ExtraInfoUtil.ParseStartOffsetInfo(startOffsetInfo);
            Dictionary<string, List<long>>? msgMap = ExtraInfoUtil.ParseMsgOffsetInfo(msgOffsetInfo);

            var sortedOffsets = new Dictionary<string, List<long>>();
            foreach (MessageExt m in msgs)
            {
                string sortKey = ExtraInfoUtil.GetStartOffsetInfoMapKey(
                    m.Topic, m.Properties.TryGetValue(MessageConst.PropertyPopCk, out string? ck) ? ck : null,
                    m.QueueId);
                if (!sortedOffsets.TryGetValue(sortKey, out List<long>? list))
                {
                    list = new List<long>();
                    sortedOffsets[sortKey] = list;
                }

                list.Add(m.QueueOffset);
            }

            foreach (List<long> list in sortedOffsets.Values)
            {
                list.Sort();
            }

            foreach (MessageExt m in msgs)
            {
                // retry topic 弹回来的消息 broker 已经写好 POP_CK，不能覆盖。
                if (m.Properties.ContainsKey(MessageConst.PropertyPopCk))
                {
                    continue;
                }

                if (startMap is null || msgMap is null)
                {
                    continue;
                }

                string key = ExtraInfoUtil.GetStartOffsetInfoMapKey(m.Topic, m.QueueId);
                if (!startMap.TryGetValue(key, out long startOffset)
                    || !msgMap.TryGetValue(key, out List<long>? offsets)
                    || !sortedOffsets.TryGetValue(key, out List<long>? ordered))
                {
                    continue;
                }

                int index = ordered.IndexOf(m.QueueOffset);
                if (index < 0 || index >= offsets.Count)
                {
                    continue;
                }

                m.Properties[MessageConst.PropertyPopCk] = ExtraInfoUtil.BuildExtraInfo(
                    startOffset, popTime, invisibleTime, reviveQid, m.Topic, brokerName,
                    m.QueueId, offsets[index]);
            }
        }

        foreach (MessageExt m in msgs)
        {
            if (!m.Properties.ContainsKey(MessageConst.PropertyFirstPopTime))
            {
                m.Properties[MessageConst.PropertyFirstPopTime] =
                    popTime.ToString(CultureInfo.InvariantCulture);
            }
        }
    }

    /// <summary>
    /// POP_MESSAGE（200050）：从 broker 直接弹出消息，**不提交位点** —— 消费成功后必须
    /// 显式 ACK，否则 invisibleTime 到期后 broker 会把消息复活重投到
    /// %RETRY%&lt;group&gt;_&lt;topic&gt;（至少一次语义）。
    ///
    /// queueId = -1 表示弹该 topic 的所有队列。
    /// initMode：0=MIN（从最小位点开始，消费历史），1=MAX（只取新消息）。
    /// </summary>
    public PopResult PopMessage(string consumerGroup, string topic, int queueId,
        int maxMsgNums, long invisibleTime, long pollTime, int initMode,
        string expression = "*", string expressionType = "TAG", bool order = false,
        int timeoutMillis = 10000, string? brokerNameIn = null, string? addrIn = null)
    {
        string brokerName = brokerNameIn ?? string.Empty;
        string addr = addrIn ?? string.Empty;
        if (addr.Length == 0)
        {
            if (brokerName.Length == 0)
            {
                TopicRouteData? route = GetTopicRouteData(topic);
                if (route is null || route.BrokerDatas.Count == 0)
                {
                    throw new MQClientNoRouteException(topic);
                }

                brokerName = route.BrokerDatas[0].BrokerName;
            }

            addr = PublishAddrFor(brokerName, topic);
        }

        var header = new PopMessageRequestHeader
        {
            ConsumerGroup = consumerGroup,
            Topic = topic,
            QueueId = queueId,
            MaxMsgNums = maxMsgNums,
            InvisibleTime = invisibleTime,
            PollTime = pollTime,

            // ⚠ 必须填当前毫秒时间戳：broker 校验 now - bornTime - pollTime > 500 会直接回
            // POLLING_TIMEOUT(210)（PopMessageRequestHeader.isTimeoutTooMuch），
            // 填 0 等于必定超时。
            BornTime = UtilAll.CurrentTimeMillis(),
            InitMode = initMode,
            Exp = expression,
            ExpType = expressionType,
            Order = order,
        };

        RemotingCommand request = RemotingCommand.CreateRequestCommand(RequestCode.PopMessage, header);
        RemotingCommand response = InvokeSyncOnAddr(addr, request, timeoutMillis);

        PopStatus status;
        switch (response.Code)
        {
            case ResponseCode.Success:
                status = PopStatus.Found;
                break;
            case ResponseCode.PollingFull:
                status = PopStatus.PollingFull;
                break;
            case ResponseCode.PollingTimeout:
                status = PopStatus.PollingNotFound;
                break;
            case ResponseCode.PullNotFound:
                status = PopStatus.PollingNotFound;
                break;
            default:
                throw new MQBrokerException(response.Code, response.Remark);
        }

        var respHeader = new PopMessageResponseHeader();
        respHeader.FromExtFields(response.ExtFields);

        var result = new PopResult
        {
            Status = status,
            RestNum = respHeader.RestNum ?? 0,
            PopTime = respHeader.PopTime ?? 0,
            InvisibleTime = respHeader.InvisibleTime ?? 0,
            ReviveQid = respHeader.ReviveQid ?? 0,
            StartOffsetInfo = respHeader.StartOffsetInfo ?? string.Empty,
            MsgOffsetInfo = respHeader.MsgOffsetInfo ?? string.Empty,
            OrderCountInfo = respHeader.OrderCountInfo ?? string.Empty,
        };

        if (result.Status == PopStatus.Found && response.Body.Length > 0)
        {
            result.MsgFoundList = MessageDecoder.DecodeMessages(response.Body);
            StampPopCk(result.MsgFoundList, brokerName, respHeader);
        }

        foreach (MessageExt m in result.MsgFoundList)
        {
            m.BrokerName = brokerName;
            m.Topic = topic;
        }

        return result;
    }

    /// <summary>
    /// ACK_MESSAGE（200051）：确认一条 POP 消息已消费完。
    ///
    /// ⚠ offset 是 **consumeQueue offset**（即 CK 串第 8 段 / msgQueueOffset），
    /// 不是 commitlog offset。返回 broker 的响应码，SUCCESS 即成功。
    /// </summary>
    public int AckMessage(string consumerGroup, string topic, int queueId,
        string extraInfo, long offset, int timeoutMillis = 3000,
        string? brokerNameIn = null, string? addrIn = null)
    {
        string brokerName = brokerNameIn ?? string.Empty;
        if (brokerName.Length == 0 && extraInfo.Length > 0)
        {
            brokerName = ExtraInfoUtil.GetBrokerName(ExtraInfoUtil.Split(extraInfo));
        }

        string addr = addrIn ?? string.Empty;
        if (addr.Length == 0)
        {
            addr = PopAckAddrFor(topic, brokerName, queueId);
        }

        var header = new AckMessageRequestHeader
        {
            ConsumerGroup = consumerGroup,
            Topic = topic,
            QueueId = queueId,
            ExtraInfo = extraInfo,
            Offset = offset,
        };

        RemotingCommand request = RemotingCommand.CreateRequestCommand(RequestCode.AckMessage, header);
        RemotingCommand response = InvokeSyncOnAddr(addr, request, timeoutMillis);
        return response.Code;
    }

    /// <summary>
    /// CHANGE_MESSAGE_INVISIBLETIME（200053，注意不是 200052 —— 那是 PEEK）：
    /// 延长一条 POP 消息的不可见时间。
    ///
    /// 响应给的是**新的** popTime/invisibleTime/reviveQid；成功时用它们 + 请求里的 offset
    /// 重建一个 8 段 extraInfo（结果里的 ExtraInfo），**后续 ACK 必须用新串**。
    /// </summary>
    public ChangeInvisibleTimeResult ChangeInvisibleTime(string consumerGroup, string topic,
        int queueId, string extraInfo, long offset, long invisibleTime,
        int timeoutMillis = 3000, string? brokerNameIn = null, string? addrIn = null)
    {
        string brokerName = brokerNameIn ?? string.Empty;
        if (brokerName.Length == 0 && extraInfo.Length > 0)
        {
            brokerName = ExtraInfoUtil.GetBrokerName(ExtraInfoUtil.Split(extraInfo));
        }

        string addr = addrIn ?? string.Empty;
        if (addr.Length == 0)
        {
            addr = PopAckAddrFor(topic, brokerName, queueId);
        }

        var header = new ChangeInvisibleTimeRequestHeader
        {
            ConsumerGroup = consumerGroup,
            Topic = topic,
            QueueId = queueId,
            ExtraInfo = extraInfo,
            Offset = offset,
            InvisibleTime = invisibleTime,
        };

        RemotingCommand request =
            RemotingCommand.CreateRequestCommand(RequestCode.ChangeMessageInvisibletime, header);
        RemotingCommand response = InvokeSyncOnAddr(addr, request, timeoutMillis);

        var respHeader = new ChangeInvisibleTimeResponseHeader();
        respHeader.FromExtFields(response.ExtFields);

        var result = new ChangeInvisibleTimeResult
        {
            Code = response.Code,
            PopTime = respHeader.PopTime ?? 0,
            InvisibleTime = respHeader.InvisibleTime ?? 0,
            ReviveQid = respHeader.ReviveQid ?? 0,
        };

        if (response.Code == ResponseCode.Success)
        {
            result.ExtraInfo = ExtraInfoUtil.BuildExtraInfo(offset, result.PopTime,
                result.InvisibleTime, result.ReviveQid, topic, brokerName, queueId, offset);
        }

        return result;
    }

    // ---------------- 消费位点 ----------------

    /// 与管理侧 offset 查询（<see cref="PublishAddrInAdmin"/>）的差别只在最后那一步：
    /// 管理 API 一律打主、主没了就报错；位点读取允许退到从节点。
    /// </summary>
    private string ConsumerOffsetAddrFor(MessageQueue mq)
    {
        string addr = FindBrokerAddressInPublish(mq.BrokerName);
        if (addr.Length == 0)
        {
            UpdateTopicRouteInfoFromNameServer(mq.Topic);
            addr = BrokerAddrOf(mq.BrokerName);
        }

        if (addr.Length == 0)
        {
            throw new MQClientException("The broker[" + mq.BrokerName + "] not exist", -1);
        }

        return addr;
    }

    public bool QueryConsumerOffset(string consumerGroup, MessageQueue mq,
        out long outOffset, int timeoutMillis = 5000,
        string? addrIn = null,
        bool setZeroIfNotFound = false)
    {
        outOffset = 0;
        string addr = addrIn is { Length: > 0 } ? addrIn : ConsumerOffsetAddrFor(mq);
        var header = new QueryConsumerOffsetRequestHeader
        {
            ConsumerGroup = consumerGroup,
            Topic = mq.Topic,
            QueueId = mq.QueueId,
        };
        RemotingCommand request = RemotingCommand.CreateRequestCommand(RequestCode.QueryConsumerOffset, header);
        if (setZeroIfNotFound)
        {
            request.ExtFields["setZeroIfNotFound"] = "true";
        }

        RemotingCommand response = InvokeSyncOnAddr(addr, request, timeoutMillis);
        if (response.Code == ResponseCode.QueryNotFound)
        {
            return false;
        }

        CheckResponseCode(response);
        var respHeader = new QueryConsumerOffsetResponseHeader();
        respHeader.FromExtFields(response.ExtFields);
        outOffset = respHeader.Offset ?? 0;
        return true;
    }

    public void UpdateConsumerOffset(string consumerGroup, MessageQueue mq,
        long commitOffset, int timeoutMillis = 5000,
        string? addrIn = null)
    {
        string addr = addrIn is { Length: > 0 } ? addrIn : BrokerAddr(mq);
        var header = new UpdateConsumerOffsetRequestHeader
        {
            ConsumerGroup = consumerGroup,
            Topic = mq.Topic,
            QueueId = mq.QueueId,
            CommitOffset = commitOffset,
        };
        RemotingCommand request = RemotingCommand.CreateRequestCommand(RequestCode.UpdateConsumerOffset, header);
        RemotingCommand response = InvokeSyncOnAddr(addr, request, timeoutMillis);
        CheckResponseCode(response);
    }


    private static JsonValue BuildMqSetJson(IEnumerable<MessageQueue> mqs)
    {
        var arr = JsonValue.MakeArray();
        foreach (MessageQueue mq in mqs)
        {
            var o = JsonValue.MakeObject();
            o.Set("topic", JsonValue.MakeString(mq.Topic));
            o.Set("brokerName", JsonValue.MakeString(mq.BrokerName));
            o.Set("queueId", JsonValue.MakeInt(mq.QueueId));
            arr.PushArray(o);
        }

        return arr;
    }

    /// <summary>批量锁队列；返回 broker 确认锁定成功的队列集（lockOKMQSet）。</summary>
    public List<MessageQueue> LockBatchMq(string consumerGroup, string clientId,
        IReadOnlyList<MessageQueue> mqs, int timeoutMillis = 5000)
    {
        var lockOk = new List<MessageQueue>();
        var byBroker = new Dictionary<string, List<MessageQueue>>(StringComparer.Ordinal);
        foreach (MessageQueue mq in mqs)
        {
            if (!byBroker.TryGetValue(mq.BrokerName, out List<MessageQueue>? list))
            {
                list = new List<MessageQueue>();
                byBroker[mq.BrokerName] = list;
            }

            list.Add(mq);
        }

        foreach (KeyValuePair<string, List<MessageQueue>> kv in byBroker)
        {
            string addr = FindBrokerAddressInPublish(kv.Key);
            if (addr.Length == 0)
            {
                continue;
            }

            var body = JsonValue.MakeObject();
            body.Set("consumerGroup", JsonValue.MakeString(consumerGroup));
            body.Set("clientId", JsonValue.MakeString(clientId));
            body.Set("mqSet", BuildMqSetJson(kv.Value));
            byte[] payload = Encoding.UTF8.GetBytes(body.Dump());
            RemotingCommand response = InvokeSyncRaw(addr, RequestCode.LockBatchMq,
                null, payload, true, timeoutMillis);
            CheckResponseCode(response);
            // LockBatchResponseBody：{"lockOKMQSet":[{topic,brokerName,queueId}]}
            string text = Encoding.UTF8.GetString(response.Body ?? Array.Empty<byte>());
            if (!Json.TryParse(text, out JsonValue root, out string? error) || root is null)
            {
                ClientLog.Warn("lockBatchMq: parse response failed: " + (error ?? "unknown"));
                continue;
            }

            JsonValue okSet = root.Get("lockOKMQSet");
            if (!okSet.IsArray)
            {
                continue;
            }

            for (int i = 0; i < okSet.Size(); ++i)
            {
                JsonValue o = okSet.At(i);
                lockOk.Add(new MessageQueue(
                    o.Get("topic").StringValue(),
                    o.Get("brokerName").StringValue(),
                    (int)o.Get("queueId").IntValue()));
            }
        }

        return lockOk;
    }

    /// <summary>批量解锁队列（顺序消费清退时调用）。</summary>
    public void UnlockBatchMq(string consumerGroup, string clientId,
        IReadOnlyList<MessageQueue> mqs, int timeoutMillis = 5000)
    {
        var byBroker = new Dictionary<string, List<MessageQueue>>(StringComparer.Ordinal);
        foreach (MessageQueue mq in mqs)
        {
            if (!byBroker.TryGetValue(mq.BrokerName, out List<MessageQueue>? list))
            {
                list = new List<MessageQueue>();
                byBroker[mq.BrokerName] = list;
            }

            list.Add(mq);
        }

        foreach (KeyValuePair<string, List<MessageQueue>> kv in byBroker)
        {
            string addr = FindBrokerAddressInPublish(kv.Key);
            if (addr.Length == 0)
            {
                continue;
            }

            var body = JsonValue.MakeObject();
            body.Set("consumerGroup", JsonValue.MakeString(consumerGroup));
            body.Set("clientId", JsonValue.MakeString(clientId));
            body.Set("mqSet", BuildMqSetJson(kv.Value));
            byte[] payload = Encoding.UTF8.GetBytes(body.Dump());
            RemotingCommand response = InvokeSyncRaw(addr, RequestCode.UnlockBatchMq,
                null, payload, true, timeoutMillis);
            CheckResponseCode(response);
        }
    }

    public long GetMaxOffset(MessageQueue mq, int timeoutMillis = 5000, string? addrIn = null)
    {
        string addr = addrIn is { Length: > 0 } ? addrIn : PublishAddrInAdmin(mq);
        var header = new GetMaxOffsetRequestHeader
        {
            Topic = mq.Topic,
            QueueId = mq.QueueId,
        };
        RemotingCommand request = RemotingCommand.CreateRequestCommand(RequestCode.GetMaxOffset, header);
        RemotingCommand response = InvokeSyncOnAddr(addr, request, timeoutMillis);
        CheckResponseCode(response);
        var respHeader = new GetMaxOffsetResponseHeader();
        respHeader.FromExtFields(response.ExtFields);
        return respHeader.Offset ?? 0;
    }

    public long GetMinOffset(MessageQueue mq, int timeoutMillis = 5000, string? addrIn = null)
    {
        string addr = addrIn is { Length: > 0 } ? addrIn : PublishAddrInAdmin(mq);
        var header = new GetMinOffsetRequestHeader
        {
            Topic = mq.Topic,
            QueueId = mq.QueueId,
        };
        RemotingCommand request = RemotingCommand.CreateRequestCommand(RequestCode.GetMinOffset, header);
        RemotingCommand response = InvokeSyncOnAddr(addr, request, timeoutMillis);
        CheckResponseCode(response);
        var respHeader = new GetMinOffsetResponseHeader();
        respHeader.FromExtFields(response.ExtFields);
        return respHeader.Offset ?? 0;
    }

    public long SearchOffsetByTimestamp(MessageQueue mq, long timestamp,
        int timeoutMillis = 5000, string? addrIn = null) =>
        SearchOffsetByBoundary(mq, timestamp, BoundaryType.Lower, timeoutMillis, addrIn);

    public long SearchOffsetByBoundary(MessageQueue mq, long timestamp, BoundaryType? boundaryType,
        int timeoutMillis = 5000, string? addrIn = null)
    {
        string addr = addrIn is { Length: > 0 } ? addrIn : PublishAddrInAdmin(mq);
        var header = new SearchOffsetRequestHeader
        {
            Topic = mq.Topic,
            QueueId = mq.QueueId,
            Timestamp = timestamp,
            BoundaryType = boundaryType,
        };
        RemotingCommand request = RemotingCommand.CreateRequestCommand(RequestCode.SearchOffsetByTimestamp, header);
        RemotingCommand response = InvokeSyncOnAddr(addr, request, timeoutMillis);
        CheckResponseCode(response);
        var respHeader = new SearchOffsetResponseHeader();
        respHeader.FromExtFields(response.ExtFields);
        return respHeader.Offset ?? 0;
    }

    // ---------------- 心跳 / 注销 ----------------

    public void SendHeartbeat(string addr, HeartbeatData heartbeatData, int timeoutMillis = 5000)
    {
        RemotingCommand request = RemotingCommand.CreateRequestCommand(RequestCode.HeartBeat, null);
        request.Body = heartbeatData.Encode();
        request.HasBody = true;
        RemotingCommand response = InvokeSyncOnAddr(addr, request, timeoutMillis);
        CheckResponseCode(response);
    }

    public void UnregisterClient(string addr, string clientId,
        string producerGroup, string consumerGroup,
        int timeoutMillis = MqClientApiTimeoutMillis)
    {
        var header = new UnregisterClientRequestHeader
        {
            ClientId = clientId,
            ProducerGroup = string.IsNullOrWhiteSpace(producerGroup) ? null : producerGroup,
            ConsumerGroup = string.IsNullOrWhiteSpace(consumerGroup) ? null : consumerGroup,
        };
        RemotingCommand request = RemotingCommand.CreateRequestCommand(RequestCode.UnregisterClient, header);
        RemotingCommand response = InvokeSyncOnAddr(addr, request, timeoutMillis);
        CheckResponseCode(response);
    }

    // ---------------- CHECK_CLIENT_CONFIG(46)：订阅表达式向 broker 求证 ----------------

    public const int MqClientApiTimeoutMillis = 3000;

    public string? FindBrokerAddrByTopic(string topic)
    {
        TopicRouteData? route;
        lock (_routeLock)
        {
            _topicRouteTable.TryGetValue(topic, out route);
        }

        if (route == null)
        {
            return null;
        }

        List<BrokerData> brokers = route.BrokerDatas;
        if (brokers.Count == 0)
        {
            return null;
        }

        return brokers[ThreadLocalRandom.Next(brokers.Count)].SelectBrokerAddr();
    }

    public void CheckClientConfig(string brokerAddr, string consumerGroup, string clientId,
        SubscriptionData subscriptionData, int timeoutMillis = MqClientApiTimeoutMillis)
    {
        RemotingCommand request = RemotingCommand.CreateRequestCommand(RequestCode.CheckClientConfig, null);
        var body = new CheckClientRequestBody
        {
            ClientId = clientId,
            Group = consumerGroup,
            SubscriptionData = subscriptionData,
        };
        request.Body = body.Encode();
        request.HasBody = true;
        RemotingCommand response = InvokeSyncOnAddr(brokerAddr, request, timeoutMillis);
        if (response.Code != ResponseCode.Success)
        {
            throw new MQClientException(response.Remark, response.Code);
        }
    }

    /// 为什么必须发：SQL92 表达式写错时 broker 的 ExpressionMessageFilter 在 ConsumeQueue
    /// 阶段拿不到编译好的过滤数据会<b>直接放行全部消息</b>（返回 true），静默变成「订阅全部
    /// 消息」、启动也不报错；这一调用把「写错的表达式」变成启动期一次显式失败。
    public void CheckSubscriptionsInBroker(string group, IEnumerable<SubscriptionData> subs)
    {
        foreach (SubscriptionData? sub in subs)
        {
            if (sub == null || string.IsNullOrEmpty(sub.ExpressionType)
                || sub.ExpressionType == RocketMQ.Common.ExpressionType.TAG)
            {
                continue;
            }

            string? addr = FindBrokerAddrByTopic(sub.Topic);
            if (string.IsNullOrEmpty(addr))
            {
                continue;
            }

            try
            {
                CheckClientConfig(addr, group, ClientId, sub);
            }
            catch (MQClientException)
            {
                throw;  // 已带 broker 响应码，原样上抛
            }
            catch (Exception e)
            {
                throw new MQClientException(
                    "Check client in broker error, maybe because you use " + sub.ExpressionType
                    + " to filter message, but server has not been upgraded to support!This error"
                    + " would not affect the launch of consumer, but may has impact on message "
                    + "receiving if you have use the new features which are not supported by "
                    + "server, please check the log!", e);
            }
        }
    }

    // ---------------- 通用同步调用（管理端复用）----------------

    /// <summary>
    /// 下发任意 requestCode + extFields + body。languageOverride &gt;= 0 时覆盖请求的
    /// language 字段：个别 RPC 会按它改变行为（INVOKE_BROKER_TO_RESET_OFFSET 对
    /// CPP/PYTHON 才返回可解析的 offsetTable 响应体）。
    ///
    /// 不做响应码校验，留给调用方自己判断（管理端很多接口的"未找到"
    /// 是正常分支，例如 QUERY_NOT_FOUND）。
    /// </summary>
    public RemotingCommand InvokeSyncRaw(string addr, int code,
        PropertyMap? extFields = null, byte[]? body = null, bool hasBody = false,
        int timeoutMillis = 3000, int languageOverride = -1)
    {
        RemotingCommand request = RemotingCommand.CreateRequestCommand(code, null);
        if (extFields is not null)
        {
            foreach (var kv in extFields)
            {
                request.ExtFields[kv.Key] = kv.Value;
            }
        }

        if (hasBody && body is not null)
        {
            request.Body = body;
            request.HasBody = true;
        }

        if (languageOverride >= 0)
        {
            request.Language = unchecked((byte)languageOverride);
        }

        return _remotingClient.InvokeSync(addr, request, timeoutMillis);
    }

    /// <summary>invokeSyncRaw + 响应码校验（非 SUCCESS 抛 MQBrokerException）。</summary>
    public RemotingCommand InvokeSync(string addr, int code,
        PropertyMap? extFields = null, byte[]? body = null, bool hasBody = false,
        int timeoutMillis = 3000, int languageOverride = -1)
    {
        RemotingCommand response = InvokeSyncRaw(addr, code, extFields, body, hasBody, timeoutMillis, languageOverride);
        CheckResponseCode(response);
        return response;
    }

    // ---------------- 管理类 ----------------

    public void CreateTopicInBroker(string brokerAddr, string defaultTopic,
        string topic, int readQueueNums = 4,
        int writeQueueNums = 4, int perm = 6,
        int topicSysFlag = 0,
        string? topicFilterType = null,
        string? attributes = null,
        bool force = false, int timeoutMillis = 5000,
        int retryTimes = 5)
    {
        var header = new CreateTopicRequestHeader
        {
            Topic = topic,
            DefaultTopic = defaultTopic,
            ReadQueueNums = readQueueNums,
            WriteQueueNums = writeQueueNums,
            Perm = perm,
            TopicFilterType = topicFilterType ?? TopicFilterType.SingleTag,
            TopicSysFlag = topicSysFlag,
            Order = false,
            Attributes = attributes ?? string.Empty,
            Force = force,
        };
        RemotingCommand request = RemotingCommand.CreateRequestCommand(RequestCode.UpdateAndCreateTopic, header);

        string lastError = string.Empty;
        int attempts = retryTimes > 0 ? retryTimes : 1;
        for (int i = 0; i < attempts; ++i)
        {
            try
            {
                RemotingCommand response = InvokeSyncOnAddr(brokerAddr, request, timeoutMillis);
                CheckResponseCode(response);
                return;
            }
            catch (MQBrokerException)
            {
                // broker 明确拒绝（如 TOPIC_EXIST_ALREADY）不重试，直接上抛
                throw;
            }
            catch (Exception e)
            {
                lastError = e.Message;
            }
        }

        throw new MQClientException("create topic [" + topic + "] in broker " + brokerAddr
            + " failed after " + attempts.ToString(CultureInfo.InvariantCulture)
            + " attempts: " + lastError);
    }

    public void CreateTopicInRoute(string topic, int readQueueNums = 4,
        int writeQueueNums = 4, int perm = 6,
        int timeoutMillis = 5000)
    {
        TopicRouteData? route = GetTopicRouteData(MixAll.DefaultTopic);
        if (route is null)
        {
            throw new MQClientException("No route info of default topic " + MixAll.DefaultTopic);
        }

        bool createdAtLeastOnce = false;
        string lastError = string.Empty;
        foreach (BrokerData bd in route.BrokerDatas)
        {
            string addr = bd.SelectBrokerAddr();
            if (addr.Length == 0)
            {
                continue;
            }

            try
            {
                CreateTopicInBroker(addr, MixAll.DefaultTopic, topic, readQueueNums, writeQueueNums,
                    perm, 0, TopicFilterType.SingleTag, string.Empty, false, timeoutMillis);
                createdAtLeastOnce = true;
            }
            catch (Exception e)
            {
                lastError = e.Message;
            }
        }

        if (!createdAtLeastOnce)
        {
            throw new MQClientException("create new topic failed: " + lastError);
        }
    }

    public void DeleteTopicInBroker(string brokerAddr, string topic, int timeoutMillis = 5000)
    {
        var ext = new PropertyMap { ["topic"] = topic };
        InvokeSync(brokerAddr, RequestCode.DeleteTopicInBroker, ext, null, false, timeoutMillis);
    }

    public void DeleteTopicInNamesrv(string topic, int timeoutMillis = 5000)
    {
        var ext = new PropertyMap { ["topic"] = topic };
        string lastError = string.Empty;
        foreach (string nsAddr in _nameServerAddrs)
        {
            try
            {
                InvokeSync(nsAddr, RequestCode.DeleteTopicInNamesrv, ext, null, false, timeoutMillis);
                return;
            }
            catch (Exception e)
            {
                lastError = e.Message;
            }
        }

        throw new MQClientException("Failed to delete topic " + topic + " in name server: " + lastError);
    }

    // ---------------- 集群 / Topic / 消费者列表 ----------------

    public ClusterInfo GetBrokerClusterInfo(int timeoutMillis = 10000)
    {
        foreach (string nsAddr in _nameServerAddrs)
        {
            try
            {
                RemotingCommand response = InvokeSyncRaw(
                    nsAddr, RequestCode.GetBrokerClusterInfo, null, null, false, timeoutMillis);
                if (response.Code == ResponseCode.Success && response.Body.Length > 0)
                {
                    ClusterInfo.Decode(response.Body, out ClusterInfo ci);
                    return ci;
                }
            }
            catch (Exception)
            {
                continue;
            }
        }

        throw new MQClientException("Failed to get broker cluster info from name server");
    }

    public TopicList GetAllTopicListFromNameServer(int timeoutMillis = 10000)
    {
        foreach (string nsAddr in _nameServerAddrs)
        {
            try
            {
                RemotingCommand response = InvokeSyncRaw(
                    nsAddr, RequestCode.GetAllTopicListFromNameserver, null, null, false, timeoutMillis);
                if (response.Code == ResponseCode.Success && response.Body.Length > 0)
                {
                    TopicList.Decode(response.Body, out TopicList tl);
                    return tl;
                }
            }
            catch (Exception)
            {
                continue;
            }
        }

        throw new MQClientException("Failed to get all topic list from name server");
    }

    public GetConsumerListByGroupResponseBody GetConsumerListByGroup(
        string consumerGroup, string addr, int timeoutMillis = 5000)
    {
        var header = new GetConsumerListByGroupRequestHeader
        {
            ConsumerGroup = consumerGroup,
        };
        RemotingCommand request = RemotingCommand.CreateRequestCommand(RequestCode.GetConsumerListByGroup, header);
        RemotingCommand response = InvokeSyncOnAddr(addr, request, timeoutMillis);
        CheckResponseCode(response);
        GetConsumerListByGroupResponseBody.Decode(response.Body, out GetConsumerListByGroupResponseBody @out);
        return @out;
    }

    public string BrokerAddrForTopic(string topic)
    {
        TopicRouteData? route = GetTopicRouteData(topic);
        if (route is null)
        {
            return string.Empty;
        }

        foreach (BrokerData bd in route.BrokerDatas)
        {
            string addr = bd.SelectBrokerAddr();
            if (addr.Length > 0)
            {
                return addr;
            }
        }

        return string.Empty;
    }

    public List<string>? GetConsumerIdListByGroup(string topic, string consumerGroup, int timeoutMillis = 5000)
    {
        string addr = BrokerAddrForTopic(topic);
        if (addr.Length == 0)
        {
            return null;
        }

        try
        {
            string brokerName = string.Empty;
            TopicRouteData? route = GetTopicRouteData(topic);
            if (route is not null && route.BrokerDatas.Count > 0)
            {
                brokerName = route.BrokerDatas[0].BrokerName;
            }

            var header = new GetConsumerListByGroupRequestHeader
            {
                ConsumerGroup = consumerGroup,
                Bname = brokerName.Length > 0 ? brokerName : null,
            };
            RemotingCommand request = RemotingCommand.CreateRequestCommand(RequestCode.GetConsumerListByGroup, header);
            RemotingCommand response = InvokeSyncOnAddr(addr, request, timeoutMillis);
            if (response.Code != ResponseCode.Success || response.Body.Length == 0)
            {
                return null;
            }

            GetConsumerListByGroupResponseBody.Decode(response.Body, out GetConsumerListByGroupResponseBody body);
            return body.ConsumerIdList;
        }
        catch (Exception e)
        {
            ClientLog.Debug("get consumer id list failed, " + addr + " " + consumerGroup + ": " + e.Message);
            return null;
        }
    }

    public void UnregisterClientAllBrokers(string clientId, string producerGroup, string consumerGroup,
        int timeoutMillis = MqClientApiTimeoutMillis)
    {
        foreach (string addr in GetAllBrokerAddrs())
        {
            try
            {
                UnregisterClient(addr, clientId, producerGroup, consumerGroup, timeoutMillis);
            }
            catch (Exception e)
            {
                ClientLog.Debug("unregister_client failed, addr=" + addr + ": " + e.Message);
            }
        }
    }

    // ---------------- 按 Key / uniqKey 查消息 ----------------

    /// <summary>
    /// indexType 见 MessageConst.Index*Type；uniqKey 为 true 时额外下发
    /// MixAll.UniqueMsgQueryFlag，命中后按 msgId == key 二次校验。
    /// </summary>
    public bool QueryMessage(string topic, string key, int maxNum,
        long beginTimestamp, long endTimestamp, out byte[] outBody,
        int timeoutMillis = 15000, string? addrIn = null,
        string? indexType = null, bool uniqKey = false)
    {
        outBody = Array.Empty<byte>();
        string addr = addrIn ?? string.Empty;
        if (addr.Length == 0)
        {
            TopicRouteData? route = GetTopicRouteData(topic);
            if (route is null)
            {
                throw new MQClientNoRouteException(topic);
            }

            if (route.BrokerDatas.Count == 0)
            {
                throw new MQClientException("no broker in route of topic " + topic);
            }

            addr = FindBrokerAddrInRoute(route, route.BrokerDatas[0].BrokerName);
        }

        var header = new QueryMessageRequestHeader
        {
            Topic = topic,
            Key = key,
            MaxNum = maxNum,
            BeginTimestamp = beginTimestamp,
            EndTimestamp = endTimestamp,
        };
        if (indexType is { Length: > 0 })
        {
            header.IndexType = indexType;
        }

        RemotingCommand request = RemotingCommand.CreateRequestCommand(RequestCode.QueryMessage, header);
        if (uniqKey)
        {
            request.ExtFields[MixAll.UniqueMsgQueryFlag] = "true";
        }

        RemotingCommand response = InvokeSyncOnAddr(addr, request, timeoutMillis);
        if (response.Code == ResponseCode.QueryNotFound)
        {
            return false;
        }

        CheckResponseCode(response);
        outBody = response.Body;
        return true;
    }

    public List<MessageExt> QueryMessageAllBrokers(string topic,
        string key, int maxNum,
        long beginTimestamp, long endTimestamp,
        string? indexType = null,
        bool uniqKey = false,
        int timeoutMillis = 15000)
    {
        var messages = new List<MessageExt>();
        TopicRouteData? route = GetTopicRouteData(topic);
        if (route is null)
        {
            return messages;
        }

        foreach (BrokerData bd in route.BrokerDatas)
        {
            string addr = bd.SelectBrokerAddr();
            if (addr.Length == 0)
            {
                continue;
            }

            byte[] body;
            try
            {
                if (!QueryMessage(topic, key, maxNum, beginTimestamp, endTimestamp, out body,
                        timeoutMillis, addr, indexType, uniqKey))
                {
                    continue;
                }
            }
            catch (Exception)
            {
                continue;
            }

            if (body.Length == 0)
            {
                continue;
            }

            List<MessageExt> decoded = MessageDecoder.DecodeMessages(body, true);
            foreach (MessageExt m in decoded)
            {
                m.BrokerName = bd.BrokerName;
                if (uniqKey)
                {
                    if (m.MsgId == key)
                    {
                        messages.Add(m);
                    }
                }
                else
                {
                    string keys = m.Keys;
                    if (keys.Length > 0)
                    {
                        // KEYS 以空格（MessageConst.KEY_SEPARATOR）分隔
                        if (keys.Split(' ', StringSplitOptions.None).Any(piece => piece == key && m.Topic == topic))
                        {
                            messages.Add(m);
                        }
                    }
                }
            }
        }

        messages.Sort((a, b) => a.QueueOffset.CompareTo(b.QueueOffset));
        if (maxNum > 0 && messages.Count > maxNum)
        {
            messages.RemoveRange(maxNum, messages.Count - maxNum);
        }

        return messages;
    }

    // ---------------- 工具 ----------------
}
