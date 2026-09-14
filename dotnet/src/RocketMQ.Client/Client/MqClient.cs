// MQClientInstance：客户端核心编排（对应 org.apache.rocketmq.client.impl.factory.MQClientInstance
// 与 MQClientAPIImpl 的核心调用面）。
//
// 职责：NameServer 地址管理、Topic 路由获取与缓存、Broker 地址解析、
// 消息发送（SEND_MESSAGE_V2）、拉取（PULL_MESSAGE）、offset 查询/更新、心跳、
// 按 Key 查询消息、创建 Topic。
//
// 与 Python 参考实现（python/client/mq_client.py）逐项对齐。
using System.Globalization;
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

    /// <summary>是否已 Start（诊断用）。</summary>
    public bool Started => _started;

    public MQClientInstance(string clientId, IReadOnlyList<string> nameServerAddrs,
        int connectTimeoutMillis = 3000, int invokeTimeoutMillis = 15000)
    {
        _clientId = clientId;
        _nameServerAddrs = new List<string>(nameServerAddrs);
        _remotingClient = new RemotingClient(connectTimeoutMillis, invokeTimeoutMillis);
    }

    // ---------------- 生命周期 ----------------

    public void Start()
    {
        _started = true;
        string ns = string.Join(";", _nameServerAddrs);
        ClientLog.Info("MQClientInstance[" + _clientId + "] started, namesrv=" + ns);
    }

    public void Shutdown()
    {
        _started = false;
        _remotingClient.Shutdown();
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
    public bool UpdateTopicRouteInfoFromNameServer(string topic, int timeoutMillis = 5000)
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
        if (!ok && topic != MixAll.DefaultTopic)
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
    /// </summary>
    public TopicPublishInfo GetTopicPublishInfo(string topic)
    {
        lock (_routeLock)
        {
            if (_topicPublishInfoTable.TryGetValue(topic, out TopicPublishInfo? hit) && hit is not null && hit.Ok())
            {
                return hit;
            }
        }

        UpdateTopicRouteInfoFromNameServer(topic);
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
    /// sysFlag 由调用方（Producer）算好：压缩标志与压缩类型位都在这里下发，
    /// 且 msg.body 应已经是压缩后的字节（见 DefaultMQProducer.PrepareForSend）。
    /// </summary>
    public SendResult SendMessage(string producerGroup, Message msg, MessageQueue mq,
        int timeoutMillis = 3000, int sysFlag = 0)
    {
        string addr = BrokerAddr(mq);

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

        RemotingCommand request = RemotingCommand.CreateRequestCommand(RequestCode.SendMessageV2, header);
        request.Body = msg.Body;
        request.HasBody = true;

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
        return new SendResult
        {
            SendStatus = status,
            MsgId = respHeader.MsgId ?? string.Empty,
            OffsetMsgId = respHeader.MsgId ?? string.Empty,
            MessageQueue = new MessageQueue(mq.Topic, mq.BrokerName, respHeader.QueueId ?? mq.QueueId),
            QueueOffset = respHeader.QueueOffset ?? 0,
            TransactionId = respHeader.TransactionId ?? string.Empty,
        };
    }

    public void SendMessageOneway(string producerGroup, Message msg, MessageQueue mq,
        int timeoutMillis = 3000, int sysFlag = 0)
    {
        string addr = BrokerAddr(mq);

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

        RemotingCommand request = RemotingCommand.CreateRequestCommand(RequestCode.SendMessageV2, header);
        request.Body = msg.Body;
        request.HasBody = true;
        request.MarkOnewayRpc();
        _remotingClient.InvokeOneway(addr, request);
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
