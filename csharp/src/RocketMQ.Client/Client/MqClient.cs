// Responsibilities: NameServer address management, topic route fetching and caching,
// broker address resolution, message sending (SEND_MESSAGE_V2), pulling
// (PULL_MESSAGE), offset query/update, heartbeat, message lookup by key, and topic
// creation.
//
// Implemented item by item against the protocol rules.
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
    // <summary>Sleeps until <paramref name="deadlineTick"/> (on the
    /// Environment.TickCount64 scale) and returns immediately once
    /// <paramref name="stop"/> is set (the early return is finalised by the caller's
    /// loop condition).</summary>
    public static void WaitUntil(ManualResetEventSlim stop, long deadlineTick)
    {
        long remain = deadlineTick - Environment.TickCount64;
        if (remain > 0)
        {
            stop.Wait(TimeSpan.FromMilliseconds(remain));
        }
    }

    /// <summary>Tasks with no event to wait on (the producer heartbeat thread) sleep in
    /// 100ms slices until the scheduled instant: each slice recomputes against the
    /// **absolute** instant, so the error does not accumulate and shutdown stays
    /// responsive within 100ms.</summary>
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

        // The cursor uses atomic increment: the producer side may be called concurrently
        // from several threads and the cursor is state shared across calls
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
    /// Round-robin with a filter: the cursor still advances, a round where nothing
    /// matches returns null and the caller degrades to its own selection. A queue is
    /// chosen only when both filter and brokerFilter pass.
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

    /// <summary>Resets the round-robin cursor (used by fault-avoidance resetIndex).</summary>
    public void ResetIndex()
    {
        Interlocked.Exchange(ref _index, 0);
    }
}

/// <summary>The core orchestration instance of the client.</summary>
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

    // ---- broker-initiated notification 40 (instance-level handler plus a per-consumer "wake up" callback) ----
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

    /// <summary>Whether Start has been called (for diagnostics).</summary>
    public bool Started => _started;

    /// <summary>The route refresh interval held by the instance (offline cases assert the
    /// facade passes the value through).</summary>
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

        // NOTIFY_UNSUBSCRIBE_LITE(200073): registered for exactly one reason -- to keep it off
        // the transport's "no processor for broker request code" path. The protocol defines no
        // answer for it: the broker only ever sends it oneway, so nobody waits for a reply.
        // Following the protocol here means doing nothing -- this port's classic-remoting client
        // keeps no lite-subscription registry to drop (see the comment on
        // ProcessNotifyUnsubscribeLite).
        _remotingClient.RegisterProcessor(RequestCode.NotifyUnsubscribeLite,
            ProcessNotifyUnsubscribeLite);
    }

    /// <summary>
    /// The broker's "one of your lite subscriptions is gone" push. Swallowed without a reply,
    /// the way this client's processor handles it: on the classic-remoting path the
    /// lite-subscription bookkeeping lives on the broker side, and the subscription sets this
    /// client actually registers (the ConsumerData of a push / pull / lite-pull consumer) are
    /// re-reported by every heartbeat anyway, so a dropped lite subscription cannot leave this
    /// client pulling something it should have forgotten. A sync sender therefore times out
    /// rather than getting a bogus SUCCESS -- the only answer a oneway-only notification allows.
    /// </summary>
    public RemotingCommand? ProcessNotifyUnsubscribeLite(RemotingCommand cmd, string addr)
    {
        var header = new NotifyUnsubscribeLiteRequestHeader();
        header.FromExtFields(cmd.ExtFields ?? new PropertyMap());
        ClientLog.Debug("receive NOTIFY_UNSUBSCRIBE_LITE(200073) from " + addr
            + ": group=" + (header.ConsumerGroup ?? string.Empty)
            + ", clientId=" + (header.ClientId ?? string.Empty)
            + ", liteTopic=" + (header.LiteTopic ?? string.Empty)
            + "; nothing to do on the classic remoting path");
        return null;
    }


    /// <summary>How many broker notification-40 messages have been received. Only the
    /// broker can send a reverse request, so a test cannot inject one and this counter is
    /// the only assertion point on a real connection.</summary>
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

    /// <summary>Registers the facade's reverse request handler on the instance (fan-out
    /// per registration under a shared instance, never overwriting each other).</summary>
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

    /// <summary>The facade removes its own handler on exit; the last one out also removes
    /// the transport layer's slot.</summary>
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
    /// Drives one multicast dispatch of the facade handlers. Public only so that offline
    /// cases can drive this reverse path (on a real connection the broker cannot push
    /// in, for the same reason as <see cref="ProcessNotifyConsumerIdsChanged"/>); do not
    /// use it from business code -- the transport layer calls it from the registry.
    /// Returns the first non-null response, or null when nobody registered for that code.
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

    /// <summary>How many facade handlers are currently registered for a code (a test
    /// observation point: under a shared instance the registrations do not overwrite each
    /// other).</summary>
    public int FacadeProcessorCount(int requestCode)
    {
        lock (_facadeProcessorLock)
        {
            return _facadeProcessors.TryGetValue(requestCode,
                out List<RemotingClient.RequestProcessor>? list) ? list.Count : 0;
        }
    }

    public bool RegisterRpcHook(IRpcHook hook) => _remotingClient.RegisterRpcHook(hook);

    // ---------------- Lifecycle ----------------

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
        // The 40 callback is a closure registered by the consumer (it captures consumer
        // state): remove the handler and the callback table before closing the connection,
        // so a broker 40 during shutdown does not reach a consumer that is already exiting.
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

    /// <summary>Turns a response code other than SUCCESS into an MQBrokerException.</summary>
    public static void CheckResponseCode(RemotingCommand response)
    {
        if (response.Code != ResponseCode.Success)
        {
            throw new MQBrokerException(response.Code, response.Remark);
        }
    }

    // ---------------- Route management ----------------

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

                    // Stop polling as soon as the first reachable NS explicitly returns non-SUCCESS (e.g. TOPIC_NOT_EXIST)
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
    /// Fetches the publish info (**shared per cache instance**, the round-robin cursor
    /// advances inside the instance); a cache miss triggers one route refresh and an
    /// MQClientException is thrown when it still cannot be obtained.
    /// <paramref name="isDefault"/> is passed through to the route refresh: only pass
    /// true when a producer cannot fetch the real route.
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

    /// <summary>Gets the cached route; on a miss it tries one refresh and returns null when
    /// there is still none.</summary>
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
            // A failed route refresh does not throw; the table lookup below returns empty
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

    /// Returning an empty string is a normal outcome (a down master looks exactly like
    /// this); what error to report is up to the caller
    /// (the send path reports MQClientException, the transaction-finish path reports
    /// nothing).
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

    /// Used by the sync send path (its address step resolves here), by POP's ack and
    /// invisible-time extension (both go through
    /// <c>FindBrokerAddressInSubscribe(brokerName, MASTER_ID, true)</c>, master only),
    /// and by the admin side's offset query
    /// (see <see cref="PublishAddrInAdmin"/>).
    /// Same shape: look up the publish address (brokerId=0 only) -> on a miss refresh the
    /// route once for the topic -> look again -> if still missing throw
    /// <c>MQClientException("The broker[X] not exist")</c> with the same wording the
    /// send path reports to its caller (the code in this port is -1, meaning "not a code
    /// returned by the broker").
    ///
    /// A pinned send does not fetch publish info on its send path, so this is
    /// its only source of routing; during a master/slave switch this is also the dividing
    /// line between "fail immediately on our side" and "send the request to a slave and
    /// eat a round of SYSTEM_BUSY(2) for nothing" -- slaves do not accept the write
    /// requests SEND_MESSAGE / CONSUMER_SEND_MSG_BACK / CHANGE_INVISIBLE_TIME.
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

    /// <summary>The test project has no InternalsVisibleTo relationship with this
    /// assembly, hence public; do not use it from business code: offline cases have no
    /// name server, so they seed the endpoint table directly (equivalent to persisting a
    /// static topic route).</summary>
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

    // ---------------- Message sending ----------------

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
        // A oneway send is a write request too; the address rule matches the synchronous send
        // (master only plus the same VIP rewrite).
        string addr = VipAddr(PublishAddrFor(mq.BrokerName, mq.Topic));
        // A oneway send also fills in UNIQ_KEY (same semantics as the synchronous send)
        if (!msg.IsBatch)
        {
            MessageClientIDSetter.SetUniqId(msg);
        }

        RemotingCommand request = BuildSendRequest(producerGroup, msg, mq, sysFlag, unitMode,
            createTopicKey, defaultTopicQueueNums);
        request.MarkOnewayRpc();
        _remotingClient.InvokeOneway(addr, request);
    }

    // ---------------- Scheduled message recall ----------------

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

    // ---------------- Request-Reply: receiving broker-pushed responses (326)----------------

    /// <summary>
    /// Handles PUSH_REPLY_MESSAGE_TO_CLIENT(326): hands the response to the waiting Request().
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
            // A parse failure must never take down the reader thread: reply SYSTEM_ERROR (the
            // broker side logs a warn but does not drop the connection).
            ClientLog.Warn("unknown err when receiveReplyMsg: " + e.Message);
            return RemotingCommand.CreateResponseCommand(
                ResponseCode.SystemError, "process reply message fail: " + e.Message);
        }
    }

    // ---------------- Message pulling ----------------

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
            // Passed through to the caller, which writes it back into pullFromWhichNodeTable
            // (processPullResult:77)
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

    // ---------------- POP (5.x lightweight consumption) ----------------

    /// Note: this is the pitfall POP is most often bitten by: when a plain topic is popped
    /// directly, the broker does **not** write POP_CK on the message (only the retry
    /// topic's re-encoding path does), yet the ACK requires that string, so the client has
    /// to reconstruct it from the response header's startOffsetInfo / msgOffsetInfo.
    ///
    /// It is public static so unit tests can cover this pure logic directly (no network).
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
                // Messages popped back from a retry topic already carry a POP_CK written by the
                // broker; it must not be overwritten.
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
    /// POP_MESSAGE (200050): pops messages straight from the broker **without committing
    /// an offset** -- after consuming successfully you must ACK explicitly, otherwise once
    /// invisibleTime expires the broker revives the message and redelivers it to
    /// %RETRY%&lt;group&gt;_&lt;topic&gt; (at-least-once semantics).
    ///
    /// queueId = -1 means pop from all queues of that topic.
    /// initMode: 0=MIN (start at the smallest offset, consuming history), 1=MAX (new
    /// messages only).
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

            // Note: the current millisecond timestamp is mandatory: the broker checks
            // now - bornTime - pollTime > 500 and replies POLLING_TIMEOUT(210) right away
            // (PopMessageRequestHeader.isTimeoutTooMuch); leaving it 0 guarantees a timeout.
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
    /// ACK_MESSAGE (200051): confirms that a POP message has been fully consumed.
    ///
    /// Note: offset is the **consumeQueue offset** (i.e. the 8th segment of the CK string /
    /// msgQueueOffset), not the commitlog offset. Returns the broker response code;
    /// SUCCESS means it worked.
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
    /// CHANGE_MESSAGE_INVISIBLETIME (200053, note it is not 200052 -- that one is PEEK):
    /// extends the invisible time of a POP message.
    ///
    /// The response carries the **new** popTime/invisibleTime/reviveQid; on success use
    /// them plus the offset from the request to rebuild an 8-segment extraInfo (the
    /// ExtraInfo in the result), and **later ACKs must use the new string**.
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

    // ---------------- Consumer offsets ----------------

    /// The only difference from the admin-side offset query
    /// (<see cref="PublishAddrInAdmin"/>) is that last step: the admin API always targets
    /// the master and reports an error when there is none, while an offset read may fall
    /// back to a slave.
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

        // The broker also listens on (route port - 2) for offset reads; the rewrite happens
        // per request so a shifted address is never kept in the route tables.
        RemotingCommand response = InvokeSyncOnAddr(VipAddr(addr), request, timeoutMillis);
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
        RemotingCommand response = InvokeSyncOnAddr(VipAddr(addr), request, timeoutMillis);
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

    /// <summary>Locks queues in bulk; returns the set of queues the broker confirmed as
    /// locked (lockOKMQSet).</summary>
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
            RemotingCommand response = InvokeSyncRaw(VipAddr(addr), RequestCode.LockBatchMq,
                null, payload, true, timeoutMillis);
            CheckResponseCode(response);
            // LockBatchResponseBody: {"lockOKMQSet":[{topic,brokerName,queueId}]}
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

    /// <summary>Unlocks queues in bulk (called when an orderly consumer retires).</summary>
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
            RemotingCommand response = InvokeSyncRaw(VipAddr(addr), RequestCode.UnlockBatchMq,
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
        RemotingCommand response = InvokeSyncOnAddr(VipAddr(addr), request, timeoutMillis);
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
        RemotingCommand response = InvokeSyncOnAddr(VipAddr(addr), request, timeoutMillis);
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
        RemotingCommand response = InvokeSyncOnAddr(VipAddr(addr), request, timeoutMillis);
        CheckResponseCode(response);
        var respHeader = new SearchOffsetResponseHeader();
        respHeader.FromExtFields(response.ExtFields);
        return respHeader.Offset ?? 0;
    }

    // ---------------- Heartbeat / unregister ----------------

    /// <summary>
    /// One broker instance as the heartbeat fan-out sees it: which brokerName it belongs
    /// to, which brokerId it has inside that name (0 = the master, see
    /// <see cref="MixAll.MasterId"/>) and the address to talk to.
    /// </summary>
    public sealed record BrokerAddrEntry(string BrokerName, long BrokerId, string Addr);

    /// <summary>
    /// Heartbeat V2 switch for this instance. Off (the default) every beat
    /// keeps its full subscription set and fingerprint 0, i.e. exactly what a V1 client sends.
    ///
    /// Turning it either way restarts the conversation with every broker: the caches below say
    /// "this address already holds this client's subscriptions", and that statement means
    /// something different as soon as the mode changes (in V1 the client never reported a
    /// fingerprint, so a broker that was told nothing must not be trusted to skip).
    /// </summary>
    public bool UseHeartbeatV2
    {
        get => _useHeartbeatV2;
        set
        {
            if (_useHeartbeatV2 == value)
            {
                return;
            }

            _useHeartbeatV2 = value;
            ClearHeartbeatV2State();
        }
    }

    private volatile bool _useHeartbeatV2;

    private readonly object _heartbeatV2Lock = new();

    /// <summary>Addresses that answered IS_SUPPORT_HEART_BEAT_V2=true.</summary>
    private readonly HashSet<string> _brokerSupportV2HeartbeatSet = new(StringComparer.Ordinal);

    /// <summary>Last fingerprint accepted per address.</summary>
    private readonly Dictionary<string, int> _brokerAddrHeartbeatFingerprintTable =
        new(StringComparer.Ordinal);

    /// <summary>Broker protocol version per brokerName/addr, taken off every heartbeat reply
    /// from the response header.</summary>
    private readonly Dictionary<string, Dictionary<string, int>> _brokerVersionTable =
        new(StringComparer.Ordinal);

    /// <summary>Whether this address has ever declared heartbeat V2 support.</summary>
    public bool IsBrokerSupportV2Heartbeat(string addr)
    {
        lock (_heartbeatV2Lock)
        {
            return _brokerSupportV2HeartbeatSet.Contains(addr);
        }
    }

    /// <summary>The fingerprint this address last accepted, or null when none is cached.</summary>
    public int? BrokerHeartbeatFingerprint(string addr)
    {
        lock (_heartbeatV2Lock)
        {
            return _brokerAddrHeartbeatFingerprintTable.TryGetValue(addr, out int fp) ? fp : null;
        }
    }

    /// <summary>The broker version recorded from the last heartbeat reply of this
    /// brokerName/addr pair (null when this instance never beat it successfully).</summary>
    public int? BrokerVersionOf(string brokerName, string addr)
    {
        lock (_heartbeatV2Lock)
        {
            return _brokerVersionTable.TryGetValue(brokerName, out Dictionary<string, int>? one)
                && one.TryGetValue(addr, out int version)
                ? version
                : null;
        }
    }

    /// <summary>
    /// Clears the per-address fingerprint table. This client also clears it when a
    /// beat is triggered by a rebalance: the assignment moved, so
    /// the client must re-report its subscriptions rather than ask every broker to skip them.
    /// </summary>
    public void ResetBrokerAddrHeartbeatFingerprintTable()
    {
        lock (_heartbeatV2Lock)
        {
            _brokerAddrHeartbeatFingerprintTable.Clear();
        }
    }

    /// <summary>Clears both V2 caches: the conversation with every broker starts over.</summary>
    public void ClearHeartbeatV2State()
    {
        lock (_heartbeatV2Lock)
        {
            _brokerSupportV2HeartbeatSet.Clear();
            _brokerAddrHeartbeatFingerprintTable.Clear();
        }
    }

    /// <summary>
    /// Sends one heartbeat to one broker address (HEART_BEAT(34); there is no separate request
    /// code for V2) and returns what the reply said.
    ///
    /// V1 (the default): the body goes out exactly as built -- subscriptions included,
    /// fingerprint 0 -- and the V2 flags are not even read, which is what keeps a V1 beat
    /// indistinguishable from a beat sent to a broker that never heard of V2.
    ///
    /// V2: the fingerprint of this snapshot is stamped on the body and on its withoutSub twin,
    /// then the short body goes out only when THIS address is known V2-capable AND the
    /// fingerprint it last accepted is the current one; otherwise the subscriptions travel
    /// again. A reply reporting subChange drops the cached fingerprint so the next beat
    /// re-reports.
    /// </summary>
    public HeartbeatV2Result SendHeartbeat(string addr, HeartbeatData heartbeatData,
        int timeoutMillis = 5000)
    {
        if (!UseHeartbeatV2)
        {
            return HeartbeatV2Result.Of(InvokeHeartbeat(addr, heartbeatData, timeoutMillis));
        }

        int fingerprint = heartbeatData.ComputeHeartbeatFingerprint();
        heartbeatData.HeartbeatFingerprint = fingerprint;
        HeartbeatData withoutSub = heartbeatData.CloneWithoutSubscriptions();

        bool skipSub;
        lock (_heartbeatV2Lock)
        {
            skipSub = _brokerSupportV2HeartbeatSet.Contains(addr)
                && _brokerAddrHeartbeatFingerprintTable.TryGetValue(addr, out int cached)
                && cached == fingerprint;
        }

        HeartbeatV2Result result =
            HeartbeatV2Result.Of(InvokeHeartbeat(addr, skipSub ? withoutSub : heartbeatData, timeoutMillis));

        lock (_heartbeatV2Lock)
        {
            if (skipSub)
            {
                // Only subChange is acted on here: support is already known.
                if (result.SubChange)
                {
                    _brokerAddrHeartbeatFingerprintTable.Remove(addr);
                }
            }
            else if (result.SupportV2)
            {
                _brokerSupportV2HeartbeatSet.Add(addr);
                if (result.SubChange)
                {
                    _brokerAddrHeartbeatFingerprintTable.Remove(addr);
                }
                else
                {
                    _brokerAddrHeartbeatFingerprintTable[addr] = fingerprint;
                }
            }
        }

        return result;
    }

    private RemotingCommand InvokeHeartbeat(string addr, HeartbeatData heartbeatData,
        int timeoutMillis)
    {
        RemotingCommand request = RemotingCommand.CreateRequestCommand(RequestCode.HeartBeat, null);
        request.Body = heartbeatData.Encode();
        request.HasBody = true;
        RemotingCommand response = InvokeSyncOnAddr(addr, request, timeoutMillis);
        CheckResponseCode(response);
        return response;
    }

    /// <summary>
    /// The broker instances the heartbeat fan-out walks. Built from the cached topic routes,
    /// which is where this client learns broker addresses in the first place. With
    /// <paramref name="includeSlaves" /> false, one entry per brokerName (the master, falling
    /// back to whichever replica the route advertises) -- the address set the producer side
    /// uses everywhere else on this port.
    /// </summary>
    public List<BrokerAddrEntry> GetBrokerAddrEntries(bool includeSlaves = true)
    {
        var entries = new List<BrokerAddrEntry>();
        var seen = new HashSet<string>(StringComparer.Ordinal);
        lock (_routeLock)
        {
            foreach (KeyValuePair<string, TopicRouteData> kv in _topicRouteTable)
            {
                foreach (BrokerData bd in kv.Value.BrokerDatas)
                {
                    if (includeSlaves)
                    {
                        foreach (KeyValuePair<long, string> one in bd.BrokerAddrs)
                        {
                            AddEntry(entries, seen, bd.BrokerName, one.Key, one.Value);
                        }

                        continue;
                    }

                    string masterFirst = bd.SelectBrokerAddr();
                    if (masterFirst.Length > 0)
                    {
                        long id = MasterIdOf(bd);
                        AddEntry(entries, seen, bd.BrokerName, id, masterFirst);
                    }
                }
            }
        }

        return entries;
    }

    private static void AddEntry(List<BrokerAddrEntry> entries, HashSet<string> seen,
        string brokerName, long brokerId, string addr)
    {
        if (string.IsNullOrEmpty(addr))
        {
            return;
        }

        string key = brokerName + "#" + brokerId.ToString(CultureInfo.InvariantCulture) + "@" + addr;
        if (seen.Add(key))
        {
            entries.Add(new BrokerAddrEntry(brokerName, brokerId, addr));
        }
    }

    private static long MasterIdOf(BrokerData bd)
    {
        foreach (KeyValuePair<long, string> one in bd.BrokerAddrs)
        {
            if (one.Value == bd.SelectBrokerAddr())
            {
                return one.Key;
            }
        }

        return MixAll.MasterId;
    }

    /// <summary>
    /// One heartbeat round over the broker table (V1 and V2 share this entry point,
    /// under the same two guards):
    /// <list type="number">
    /// <item>a client that registers nothing has nothing to report -- no beat at all;</item>
    /// <item>a producer-only client beats the masters only: a slave holding a producer
    /// registration can never use it, so this client skips every entry whose brokerId is not
    /// <c>MixAll.MASTER_ID</c> while the ConsumerData set is empty.</item>
    /// </list>
    /// <paramref name="isRebalance" /> is the push consumer's "the allocation moved" signal: it
    /// drops every cached fingerprint first, so this round re-reports subscriptions to every
    /// broker instead of sending the short body.
    /// Returns how many brokers answered.
    /// </summary>
    public int SendHeartbeatToAllBrokers(HeartbeatData heartbeatData, bool isRebalance = false,
        int timeoutMillis = 5000, bool includeSlaves = true)
    {
        bool producerEmpty = heartbeatData.ProducerDataSet.Count == 0;
        bool consumerEmpty = heartbeatData.ConsumerDataSet.Count == 0;
        if (producerEmpty && consumerEmpty)
        {
            ClientLog.Warn("sending heartbeat, but no consumer and no producer. [" + ClientId + "]");
            return 0;
        }

        List<BrokerAddrEntry> entries = GetBrokerAddrEntries(includeSlaves);
        if (entries.Count == 0)
        {
            return 0;
        }

        if (isRebalance)
        {
            ResetBrokerAddrHeartbeatFingerprintTable();
        }

        int ok = 0;
        foreach (BrokerAddrEntry entry in entries)
        {
            if (consumerEmpty && entry.BrokerId != MixAll.MasterId)
            {
                continue;
            }

            try
            {
                HeartbeatV2Result result = SendHeartbeat(entry.Addr, heartbeatData, timeoutMillis);
                RecordBrokerVersion(entry.BrokerName, entry.Addr, result.Version);
                ok++;
            }
            catch (Exception e)
            {
                ClientLog.Warn("heartbeat to " + entry.Addr + " failed: " + e.Message);
            }
        }

        return ok;
    }

    private void RecordBrokerVersion(string brokerName, string addr, int version)
    {
        lock (_heartbeatV2Lock)
        {
            if (!_brokerVersionTable.TryGetValue(brokerName, out Dictionary<string, int>? one))
            {
                one = new Dictionary<string, int>(StringComparer.Ordinal);
                _brokerVersionTable[brokerName] = one;
            }

            one[addr] = version;
        }
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

    // ---------------- CHECK_CLIENT_CONFIG(46): have the broker validate the subscription expression ----------------

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
        RemotingCommand response = InvokeSyncOnAddr(VipAddr(brokerAddr), request, timeoutMillis);
        if (response.Code != ResponseCode.Success)
        {
            throw new MQClientException(response.Remark, response.Code);
        }
    }

    /// Why it must be sent: when a SQL92 expression is wrong, the broker's
    /// ExpressionMessageFilter cannot obtain the compiled filter data at the ConsumeQueue
    /// stage and **lets every message through** (returns true), silently turning into
    /// "subscribe to all messages" with no error at startup; this call turns "a wrong
    /// expression" into one explicit failure at startup.
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
                throw;  // already carries the broker response code, rethrown as is
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

    // ---------------- Generic synchronous invocation (reused by the admin side)----------------

    /// <summary>
    /// Issues any requestCode + extFields + body. When languageOverride &gt;= 0 it overrides
    /// the request's language field: a few RPCs change behaviour based on it
    /// (INVOKE_BROKER_TO_RESET_OFFSET only returns a parseable offsetTable body for
    /// CPP/PYTHON).
    ///
    /// The response code is not validated; that is left to the caller (for many admin
    /// endpoints "not found" is a normal branch, e.g. QUERY_NOT_FOUND).
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

    /// <summary>invokeSyncRaw plus response code validation (non-SUCCESS throws
    /// MQBrokerException).</summary>
    public RemotingCommand InvokeSync(string addr, int code,
        PropertyMap? extFields = null, byte[]? body = null, bool hasBody = false,
        int timeoutMillis = 3000, int languageOverride = -1)
    {
        RemotingCommand response = InvokeSyncRaw(addr, code, extFields, body, hasBody, timeoutMillis, languageOverride);
        CheckResponseCode(response);
        return response;
    }

    // ---------------- Admin ----------------

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
                RemotingCommand response = InvokeSyncOnAddr(VipAddr(brokerAddr), request, timeoutMillis);
                CheckResponseCode(response);
                return;
            }
            catch (MQBrokerException)
            {
                // An explicit broker rejection (e.g. TOPIC_EXIST_ALREADY) is not retried, it is rethrown
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
        InvokeSync(VipAddr(brokerAddr), RequestCode.DeleteTopicInBroker, ext, null, false, timeoutMillis);
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

    // ---------------- Cluster / topic / consumer lists ----------------

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
        RemotingCommand response = InvokeSyncOnAddr(VipAddr(addr), request, timeoutMillis);
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
            RemotingCommand response = InvokeSyncOnAddr(VipAddr(addr), request, timeoutMillis);
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

    // ---------------- Query messages by key / uniqKey ----------------

    /// <summary>
    /// indexType see MessageConst.Index*Type; when uniqKey is true the request additionally
    /// carries MixAll.UniqueMsgQueryFlag, and on a hit it is verified a second time with
    /// msgId == key.
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

        RemotingCommand response = InvokeSyncOnAddr(VipAddr(addr), request, timeoutMillis);
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
                        // KEYS are separated by a space (MessageConst.KEY_SEPARATOR)
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

    // ---------------- Utilities ----------------
}
