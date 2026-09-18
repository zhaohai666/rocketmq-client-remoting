// MQClientInstance：客户端核心编排（对应 org.apache.rocketmq.client.impl.factory.MQClientInstance
// 与 MQClientAPIImpl 的核心调用面）。
//
// 职责：NameServer 地址管理、Topic 路由获取与缓存、Broker 地址解析、
// 消息发送（SEND_MESSAGE_V2）、拉取（PULL_MESSAGE）、offset 查询/更新、心跳、
// 按 Key 查询消息、创建 Topic。
//
// 与 Python 参考实现（python/client/mq_client.py）逐项对齐。
using System.Globalization;
using System.Text;
using RocketMQ.Common;
using RocketMQ.Remoting;
using RocketMQ.Remoting.Protocol;

namespace RocketMQ.Client;

/// <summary>
/// 生成唯一 clientId：instanceName@yyyymmddhhmmss@pid@seq。
/// Java 用 ip@instanceName@unitName(pid 派生)，Python 用 instanceName@时间戳；
/// 这里额外带 pid 与进程内序号，保证同一秒内多次启动的客户端（如测试里连续起
/// 多个 consumer）不会撞 clientId —— broker 的消费组 channel 表以 clientId 为键。
/// </summary>
public static class ClientIds
{
    private static int _seq;

    public static string Build(string instanceName)
    {
        string ts = UtilAll.TimeToHumanString(UtilAll.CurrentTimeMillis(), "%Y%m%d%H%M%S");
        int seq = Interlocked.Increment(ref _seq) - 1;
        return instanceName + "@" + ts + "@" + UtilAll.Pid().ToString(CultureInfo.InvariantCulture) + "@"
            + seq.ToString(CultureInfo.InvariantCulture);
    }
}

/// <summary>
/// 对应 org.apache.rocketmq.client.impl.producer.TopicPublishInfo。
///
/// 注意：本类型的**轮询游标是共享状态**（Java 用 ThreadLocal，Python 用缓存的单例），
/// 调用方一律通过 GetTopicPublishInfo() 返回的缓存实例使用它（引用共享），
/// 这样多次发送才能在队列间真正轮转，而不是每次都从 0 号队列开始。
/// </summary>
public sealed class TopicPublishInfo
{
    private long _index;

    public bool OrderTopic { get; set; }
    public List<MessageQueue> MsgQueueList { get; set; } = new();
    public TopicRouteData TopicRouteData { get; set; } = new();

    public bool Ok() => MsgQueueList.Count > 0;

    /// <summary>轮询选择（对应 Java selectOneMessageQueue）。</summary>
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

    /// <summary>避开上一次失败的 broker（对应 Java selectOneMessageQueue(lastBrokerName)）。</summary>
    public MessageQueue SelectOneMessageQueue(string lastBrokerName)
    {
        if (MsgQueueList.Count == 0)
        {
            throw new MQClientException("no message queue for publish info");
        }

        // 对应 Java：尽量避开上次失败的 broker；若全是同一 broker 则退化为轮询
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
    /// 带过滤器的轮询（对应 Python select_one_message_queue(*filters)）：游标照常推进，
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

    /// <summary>重置轮询游标（对应 Python reset_index，故障规避 resetIndex 用）。</summary>
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
    private readonly Dictionary<string, TopicPublishInfo> _topicPublishInfoTable = new(StringComparer.Ordinal);
    private volatile bool _started;

    /// <summary>本客户端「在用」的 topic（消费者订阅 + 生产者发过的），对应 Java 的
    /// MQConsumerInner.subscriptions() / MQProducerInner.getPublishTopicList()，由周期任务
    /// updateTopicRouteInfoFromNameServer() 逐个刷新路由。</summary>
    private readonly HashSet<string> _topicsInUse = new(StringComparer.Ordinal);
    private Thread? _routeRefreshThread;
    private readonly ManualResetEventSlim _routeRefreshStop = new(false);

    // ---- 动态 name server（对应 Java MQClientAPIImpl.topAddressing + fetchNameServerAddr）----
    // 未配置 ROCKETMQ_NAMESRV_DOMAIN 时 WsAddr 为空 → fetch 是 no-op，行为不变。
    public DefaultTopAddressing TopAddressing { get; } = new();
    private Thread? _namesrvRefreshThread;
    private readonly ManualResetEventSlim _namesrvRefreshStop = new(false);

    // ---- 消费统计（Java MQClientFactory.getConsumerStatsManager，实例级共享）----
    public ConsumerStatsManager ConsumerStats { get; } = new();

    /// <summary>取一次地址；变化才应用到 _nameServerAddrs（Java 地址变化才 update）。</summary>
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

    /// <summary>Java 的 tls.enable 是 JVM 全局系统属性；这里等价为 env ROCKETMQ_TLS_ENABLE。</summary>
    internal static bool TlsEnabledFromEnv() => RemotingClient.EnvTlsEnabled();

    public MQClientInstance(string clientId, IReadOnlyList<string> nameServerAddrs,
        int connectTimeoutMillis = 3000, int invokeTimeoutMillis = 15000, bool? tlsEnable = null)
    {
        _clientId = clientId;
        _nameServerAddrs = new List<string>(nameServerAddrs);
        _remotingClient = new RemotingClient(connectTimeoutMillis, invokeTimeoutMillis, tlsEnable);

        // Request-Reply：broker 用 PUSH_REPLY_MESSAGE_TO_CLIENT(326) 把应答推回来。
        // 对应 Java MQClientAPIImpl 构造里
        // registerProcessor(PUSH_REPLY_MESSAGE_TO_CLIENT, clientRemotingProcessor, null)——
        // 它是**客户端实例级**的（与具体 producer 无关，应答按 clientId 推回），所以在这里注册。
        _remotingClient.RegisterProcessor(RequestCode.PushReplyMessageToClient, ProcessReplyMessage);
    }

    /// <summary>
    /// 安装 RPC 钩子（ACL 鉴权）。对应 Java 在构造 MQClientInstance 时绑定 rpcHook。
    /// **first-wins**：同一 clientId 的实例被复用，第二个注册者不会覆盖（与 Java 一致），
    /// 此时返回 false。故钩子必须在 Start() 之前设置。
    /// </summary>
    public bool RegisterRpcHook(IRpcHook hook) => _remotingClient.RegisterRpcHook(hook);

    // ---------------- 生命周期 ----------------

    public void Start()
    {
        _started = true;
        // 消费统计采样线程（Java 挂在每个 StatsItem 的调度器上，这里收敛为实例级一个）
        ConsumerStats.Start();
        // 动态 name server（Java MQClientInstance.start:344-348）：**当且仅当**没配置
        // 静态地址时先 fetch 一次；取不到直接报错（比 Java 更严格——Java 会让运行期
        // 各处各自失败，这里在 Start 时给一个明确错误）。
        if (_nameServerAddrs.Count == 0 && !string.IsNullOrEmpty(TopAddressing.WsAddr))
        {
            FetchNameServerAddr();
            if (_nameServerAddrs.Count == 0)
            {
                throw new MQClientException("name server address is not set and address server ("
                    + TopAddressing.WsAddr + ") returned none");
            }
            // 周期刷新（Java scheduleAtFixedRate(fetchNameServerAddr, 10s, 2min)）
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
        _remotingClient.Shutdown();
    }

    /// <summary>动态 name server 周期刷新：Java 首次延迟 10s、周期 2 分钟。</summary>
    private void NamesrvRefreshLoop()
    {
        if (_namesrvRefreshStop.Wait(TimeSpan.FromSeconds(10)))
        {
            return;
        }
        while (!_namesrvRefreshStop.IsSet)
        {
            if (!_started) return;
            try
            {
                FetchNameServerAddr();
            }
            catch (Exception e)
            {
                ClientLog.Debug("fetchNameServerAddr exception: " + e.Message);
            }
            if (_namesrvRefreshStop.Wait(TimeSpan.FromMinutes(2)))
            {
                return;
            }
        }
    }

    /// <summary>登记需要在后台周期刷新路由的 topic（对应 Java 的订阅/发布 topic 列表）。</summary>
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
        // 对齐 Java MQClientInstance.startScheduledTask 的
        // scheduleAtFixedRate(updateTopicRouteInfoFromNameServer, 10, pollNameServerInterval)（默认 30s）。
        if (_routeRefreshStop.Wait(30))
        {
            return; // 启动后立刻收到 stop，直接退出
        }

        while (!_routeRefreshStop.IsSet)
        {
            // 等待 30s（期间响应 stop），再刷新在用 topic 的路由
            for (int i = 0; i < 300 && !_routeRefreshStop.IsSet; ++i)
            {
                _routeRefreshStop.Wait(TimeSpan.FromMilliseconds(100));
            }

            if (_routeRefreshStop.IsSet)
            {
                break;
            }

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

    /// <summary>把响应码非 SUCCESS 转成 MQBrokerException。</summary>
    public static void CheckResponseCode(RemotingCommand response)
    {
        if (response.Code != ResponseCode.Success)
        {
            throw new MQBrokerException(response.Code, response.Remark);
        }
    }

    // ---------------- 路由管理 ----------------

    /// <summary>
    /// 从 NameServer 拉取 topic 路由。未知 topic 会回退到 MixAll.DefaultTopic
    /// （5.x nameserver 不为未知 topic 合成路由，返回 TOPIC_NOT_EXIST）。
    /// </summary>
    /// <summary>
    /// 从 NameServer 拉取 topic 路由。未知 topic 的**默认 topic 兜底（TBW102 合成）只允许生产者
    /// 在真实路由拉不到时走**（<paramref name="isDefault"/>=true，对齐 Java
    /// DefaultMQProducerImpl.tryToFindTopicPublishInfo:898-905 先真实路由、失败才 isDefault=true）。
    /// 消费者**绝不能**兜底：否则 %RETRY%group 这类尚未由 broker 创建的主题会被合成出一组假队列，
    /// 两个实例在不同时间拉取会得到不同队列数，rebalance 视图不一致（真机重复消费根因之一）。
    /// </summary>
    public bool UpdateTopicRouteInfoFromNameServer(string topic, bool isDefault = false, int timeoutMillis = 5000)
    {
        if (_nameServerAddrs.Count == 0)
        {
            throw new MQClientException("name server address list is empty");
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
            // 5.x nameServer 不为未知 topic 合成默认路由（返回 TOPIC_NOT_EXIST），
            // 需像 Java 客户端那样回退到默认 topic（TBW102）来构造发布信息。
            // 新 topic 由 broker 用 defaultTopicQueueNums 创建队列，而默认 topic 自身
            // 可能配置了更多队列，这里按 broker 实际创建数裁剪，避免选中非法 queueId。
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
            // 与 Python 一致：路由刷新失败不抛，交给下面的查表返回空
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

    public List<string> KnownBrokerAddrs() => GetRouteOfAllBrokers();

    // ---------------- 消息发送 ----------------

    /// <summary>
    /// 组装发送请求（对应 Java MQClientAPIImpl.sendMessage 的头部拼装 + V2 选择）。
    /// Request-Reply 的应答消息（MSG_TYPE == "reply"）会用 SEND_REPLY_MESSAGE_V2(325)
    /// 而不是普通 SEND_MESSAGE_V2(314)——broker 只在 324/325 上注册了 ReplyMessageProcessor。
    /// sysFlag 由调用方（Producer）算好：压缩标志与压缩类型位都在这里下发，
    /// 且 msg.body 应已经是压缩后的字节（见 DefaultMQProducer.PrepareForSend）。
    /// </summary>
    public RemotingCommand BuildSendRequest(string producerGroup, Message msg, MessageQueue mq,
        int sysFlag = 0)
    {
        var header = new SendMessageRequestHeaderV2
        {
            ProducerGroup = producerGroup,
            Topic = msg.Topic,
            DefaultTopic = MixAll.DefaultTopic,
            DefaultTopicQueueNums = MixAll.DefaultTopicQueueNums,
            QueueId = mq.QueueId,
            SysFlag = sysFlag,
            BornTimestamp = UtilAll.CurrentTimeMillis(),
            Flag = msg.Flag,
            Properties = MessageDecoder.MessagePropertiesToString(msg.Properties),
            ReconsumeTimes = 0,
            UnitMode = false,
            MaxReconsumeTimes = 0,
            Batch = msg.IsBatch,
        };

        // 对应 Java MQClientAPIImpl.sendMessage:550-558（sendSmartMsg 默认 true → V2）。
        int code = RequestReply.IsReplyMessage(msg)
            ? RequestCode.SendReplyMessageV2
            : RequestCode.SendMessageV2;

        RemotingCommand request = RemotingCommand.CreateRequestCommand(code, header);
        request.Body = msg.Body;
        request.HasBody = true;
        return request;
    }

    /// <summary>
    /// sysFlag 由调用方（Producer）算好：压缩标志与压缩类型位都在这里下发，
    /// 且 msg.body 应已经是压缩后的字节（见 DefaultMQProducer.PrepareForSend）。
    /// </summary>
    public SendResult SendMessage(string producerGroup, Message msg, MessageQueue mq,
        int timeoutMillis = 3000, int sysFlag = 0)
    {
        string addr = BrokerAddr(mq);
        // 对应 Java DefaultMQProducerImpl.sendKernelImpl：非批量消息在**发请求之前**
        // 补一个客户端唯一 ID（UNIQ_KEY）。它决定 SendResult.MsgId，也是消息轨迹
        // 里 msgId 的来源（控制台按它把发送轨迹与消费轨迹串起来）。
        if (!msg.IsBatch)
        {
            MessageClientIDSetter.SetUniqId(msg);
        }

        RemotingCommand request = BuildSendRequest(producerGroup, msg, mq, sysFlag);
        RemotingCommand response = InvokeSyncOnAddr(addr, request, timeoutMillis);

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
        // 对应 Java MQClientAPIImpl.processSendResponse：
        //   msgId         = 客户端唯一 ID（UNIQ_KEY；批量消息为逐条逗号拼接）
        //   offsetMsgId   = 响应头里的 msgId（broker 生成的 offset 消息 ID）
        //   regionId      = 响应头 MSG_REGION，缺省回落 DefaultRegion
        //   traceOn       = 响应头 TRACE_ON != "false"（broker 默认 true）
        string msgId = msg.IsBatch
            ? JoinBatchUniqId(msg) ?? (respHeader.MsgId ?? string.Empty)
            : (MessageClientIDSetter.GetUniqId(msg) ?? respHeader.MsgId ?? string.Empty);
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
        };
    }

    private static string? JoinBatchUniqId(Message msg)
    {
        if (msg is not MessageBatch batch || batch.Messages.Count == 0)
        {
            return null;
        }

        var parts = new List<string>(batch.Messages.Count);
        foreach (Message m in batch.Messages)
        {
            string? id = MessageClientIDSetter.GetUniqId(m);
            if (!string.IsNullOrEmpty(id))
            {
                parts.Add(id);
            }
        }

        return parts.Count == 0 ? null : string.Join(",", parts);
    }

    public void SendMessageOneway(string producerGroup, Message msg, MessageQueue mq,
        int timeoutMillis = 3000, int sysFlag = 0)
    {
        string addr = BrokerAddr(mq);
        // 单向发送同样补 UNIQ_KEY（与同步发送语义一致）
        if (!msg.IsBatch)
        {
            MessageClientIDSetter.SetUniqId(msg);
        }

        RemotingCommand request = BuildSendRequest(producerGroup, msg, mq, sysFlag);
        request.MarkOnewayRpc();
        _remotingClient.InvokeOneway(addr, request);
    }

    // ---------------- Request-Reply：接收 broker 推回的应答（326）----------------

    /// <summary>
    /// 处理 PUSH_REPLY_MESSAGE_TO_CLIENT(326)：把应答交给等待中的 Request()。
    ///
    /// 对应 Java ClientRemotingProcessor#receiveReplyMessage(:222-271)。
    /// 与 Java 一样**必须回一个响应**：broker 侧 Broker2Client.callClient 是 invokeSync
    /// （10s 超时），不回响应它那边就会超时并记 "push reply message to &lt;id&gt; fail"，
    /// 应答虽然已经投递成功，broker 日志里却是失败。
    ///
    /// 该回调运行在**读线程**上：绝不能在这里做 invokeSync（会死锁读线程）；异常必须兜住
    /// （解析失败回 SYSTEM_ERROR，绝不让读线程崩），对应 Java 的同处 try/catch。
    /// </summary>
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
            // sysFlag 里带压缩标志时要先解压：326 推的是**裸包**，不走消息解码路径
            // （对齐 Java 同处的 Compressor 分支）。
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

            // 应答到达时间（Java 同处写入 REPLY_MESSAGE_ARRIVE_TIME）。
            msg.PutProperty(MessageConst.PropertyReplyMessageArriveTime,
                UtilAll.CurrentTimeMillis().ToString(CultureInfo.InvariantCulture));

            string correlationId = msg.GetProperty(MessageConst.PropertyCorrelationId);
            if (RequestFutureHolder.Instance.PutResponse(correlationId, msg) is null)
            {
                // 查不到是正常情况（请求已超时 / 应答重复），Java 此处也是 warn
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
        int requestSource = 0)
    {
        string addr = addrIn ?? string.Empty;
        if (addr.Length == 0)
        {
            addr = BrokerAddr(mq);
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
            Subscription = subscription,
            SubVersion = subVersion,
            ExpressionType = expressionType,
            MaxMsgBytes = maxMsgBytes,
            RequestSource = requestSource,
        };

        RemotingCommand request = RemotingCommand.CreateRequestCommand(RequestCode.PullMessage, header);
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

    /// <summary>
    /// 给 POP 出来的消息反构 POP_CK 与 1ST_POP_TIME（逐条对齐 Java
    /// MQClientAPIImpl.processPopResponse:1150-1230）。
    ///
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
            // Java 的 startOffsetInfo == null 分支：用消息自身 queueOffset 当 ckQueueOffset
            // 建 7 段，再手工补一段凑成 8 段。按 topic+queueId 缓存，同队列共用同一基准。
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

            // Java 先按队列收集 queueOffset 并**排序**，再用 indexOf 求下标，
            // 用这个下标去 msgOffsetInfo 里取该条消息真正对应的 msgQueueOffset。
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

                // 注意：查 startOffsetInfo/msgOffsetInfo 用的是**只看 topic** 的 key
                // （Java :1200 的两参重载），与上面 sortMap 用的 POP_CK 感知 key 不同；
                // 能走到这里说明 POP_CK 为空，两者恰好等价。
                string key = ExtraInfoUtil.GetStartOffsetInfoMapKey(m.Topic, m.QueueId);
                if (!startMap.TryGetValue(key, out long startOffset)
                    || !msgMap.TryGetValue(key, out List<long>? offsets)
                    || !sortedOffsets.TryGetValue(key, out List<long>? ordered))
                {
                    continue;
                }

                // ⚠ 下标是在**本批该队列的 queueOffset 排序表**里找，不是直接在
                // msgOffsetInfo 列表里找 —— 后者是 broker 侧写入的 offset，可能与本条消息
                // 自身的 queueOffset 不等（Java 正是用 sortMap.indexOf 再取值）。
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

        // Java 用 computeIfAbsent：只在缺失时补。
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
        if (brokerName.Length == 0 || addr.Length == 0)
        {
            TopicRouteData? route = GetTopicRouteData(topic);
            if (route is null)
            {
                throw new MQClientNoRouteException(topic);
            }

            ResolveBrokerFromRoute(route, topic, ref brokerName, ref addr);
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

        // Java processPopResponse 收尾：统一盖 brokerName，并把 topic 还原成请求的 topic
        // （broker 可能把 retry topic 改写回原 topic）。
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
            // 与 Java 一致：从 CK 串第 6 段取 brokerName（ACK 靠它定位 broker）
            brokerName = ExtraInfoUtil.GetBrokerName(ExtraInfoUtil.Split(extraInfo));
        }

        string addr = addrIn ?? string.Empty;
        if (addr.Length == 0)
        {
            TopicRouteData? route = GetTopicRouteData(topic);
            if (route is null)
            {
                throw new MQClientNoRouteException(topic);
            }

            ResolveBrokerFromRoute(route, topic, ref brokerName, ref addr);
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
            TopicRouteData? route = GetTopicRouteData(topic);
            if (route is null)
            {
                throw new MQClientNoRouteException(topic);
            }

            ResolveBrokerFromRoute(route, topic, ref brokerName, ref addr);
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
            // 与 Java MQClientAPIImpl.changeInvisibleTimeAsync 一致：用**响应里的**新值重建。
            result.ExtraInfo = ExtraInfoUtil.BuildExtraInfo(offset, result.PopTime,
                result.InvisibleTime, result.ReviveQid, topic, brokerName, queueId, offset);
        }

        return result;
    }

    /// <summary>
    /// 由路由补齐 brokerName / addr（缺哪个补哪个）。Java 侧对应
    /// MQClientAPIImpl.getBrokerName/getBrokerAddr 的组合语义。
    /// </summary>
    private static void ResolveBrokerFromRoute(TopicRouteData route, string topic,
        ref string brokerName, ref string addr)
    {
        if (brokerName.Length == 0)
        {
            if (route.BrokerDatas.Count == 0)
            {
                throw new MQClientException("No broker in route of topic: " + topic);
            }

            brokerName = route.BrokerDatas[0].BrokerName;
        }

        if (addr.Length == 0)
        {
            addr = FindBrokerAddrInRoute(route, brokerName);
            if (addr.Length == 0 && route.BrokerDatas.Count > 0)
            {
                addr = route.BrokerDatas[0].SelectBrokerAddr();
            }

            if (addr.Length == 0)
            {
                throw new MQClientException("No available broker addr for topic: " + topic);
            }
        }
    }

    // ---------------- 消费位点 ----------------

    /// <summary>返回 false 表示 broker 回 QUERY_NOT_FOUND（消费组尚无位点）。</summary>
    public bool QueryConsumerOffset(string consumerGroup, MessageQueue mq,
        out long outOffset, int timeoutMillis = 5000,
        string? addrIn = null,
        bool setZeroIfNotFound = true)
    {
        outOffset = 0;
        string addr = addrIn is { Length: > 0 } ? addrIn : BrokerAddr(mq);
        var header = new QueryConsumerOffsetRequestHeader
        {
            ConsumerGroup = consumerGroup,
            Topic = mq.Topic,
            QueueId = mq.QueueId,
        };
        RemotingCommand request = RemotingCommand.CreateRequestCommand(RequestCode.QueryConsumerOffset, header);
        // setZeroIfNotFound 在 Java 里是 header 字段；
        // QueryConsumerOffsetRequestHeader 无该字段时，Java 会把未找到当错误；这里按需附加
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

    // ---------------- 队列锁（顺序消费，Java MQClientAPIImpl.lockBatchMQ / unlockBatchMQ）----------------

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
        // 按 broker 分组（Java 按 brokerName 逐个发请求）
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
            string addr = BrokerAddrOf(kv.Key);
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
            string addr = BrokerAddrOf(kv.Key);
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
        string addr = addrIn is { Length: > 0 } ? addrIn : BrokerAddr(mq);
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
        string addr = addrIn is { Length: > 0 } ? addrIn : BrokerAddr(mq);
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
        int timeoutMillis = 5000, string? addrIn = null)
    {
        string addr = addrIn is { Length: > 0 } ? addrIn : BrokerAddr(mq);
        var header = new SearchOffsetRequestHeader
        {
            Topic = mq.Topic,
            QueueId = mq.QueueId,
            Timestamp = timestamp,
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
        string producerGroup, string consumerGroup, int timeoutMillis = 5000)
    {
        var header = new UnregisterClientRequestHeader
        {
            ClientId = clientId,
            ProducerGroup = producerGroup,
            ConsumerGroup = consumerGroup,
        };
        RemotingCommand request = RemotingCommand.CreateRequestCommand(RequestCode.UnregisterClient, header);
        RemotingCommand response = InvokeSyncOnAddr(addr, request, timeoutMillis);
        CheckResponseCode(response);
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
            // ⚠ 必须下发 topicFilterType：broker 的 CreateTopicRequestHeader.checkFields()
            // 会把它转成枚举，为空直接报 "topicFilterType = [null] value invalid"。
            // attributes 必须是 ""（Java AttributeParser.parseToString(空 map) 的结果）而非 null。
            // 这两条都是先在 Python 侧被真实 broker 打回、再回填到 C++ 的。
            TopicFilterType = topicFilterType ?? TopicFilterType.SingleTag,
            TopicSysFlag = topicSysFlag,
            Order = false,
            Attributes = attributes ?? string.Empty,
            Force = force,
        };
        RemotingCommand request = RemotingCommand.CreateRequestCommand(RequestCode.UpdateAndCreateTopic, header);

        // Java MQAdminImpl.createTopic 对每个 broker 重试 5 次（连接抖动容忍）
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

    /// <summary>取 topic 路由里第一个 broker 地址（对应 Java MQClientInstance.findBrokerAddrByTopic）。
    /// 所有 broker 都持有完整消费者列表，任取一台即可查 GET_CONSUMER_LIST_BY_GROUP。</summary>
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

    /// <summary>
    /// 查询消费组内所有 clientId（对应 Java MQClientInstance.findConsumerIdList）。
    /// 取该 topic 路由里的 broker 发 GET_CONSUMER_LIST_BY_GROUP(38)。查不到（无路由 / 非
    /// SUCCESS / 异常）返回 null；调用方按 Java 语义「保留当前分配」，不要回退成
    /// "自己独占全部队列"（那会让多实例互相重复消费）。
    /// </summary>
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

    /// <summary>
    /// 向所有已知 broker 注销本 clientId（对应 Java MQClientInstance.unregisterClient）。
    /// 对齐 Java 生产者/消费者 shutdown：逐台 broker 发 UNREGISTER_CLIENT(35)。
    /// 不发的话 broker 端 ConsumerManager 只能等心跳超时（默认 ~120s）清理。
    /// 单台失败只记 debug —— shutdown 路径不应因网络抖动抛异常。
    /// </summary>
    public void UnregisterClientAllBrokers(string clientId, string producerGroup, string consumerGroup,
        int timeoutMillis = 5000)
    {
        foreach (string addr in GetRouteOfAllBrokers())
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
            // Java MixAll.UNIQUE_MSG_QUERY_FLAG："_UNIQUE_KEY_QUERY"="true"。
            // 注意它是 extFields 的**键名**，不是标志位（早期实现写成数字键是错的）。
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

    /// <summary>
    /// 对应 Java MQAdminImpl.queryMessage：查该 topic 全部 broker 并做客户端侧二次校验。
    /// </summary>
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

    /// <summary>解析 mq 对应 broker 地址（找不到抛 MQClientException）。</summary>
    public string BrokerAddrForMq(MessageQueue mq) => BrokerAddr(mq);
}
