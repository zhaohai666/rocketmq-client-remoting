// 拉模式消费者（对齐 org.apache.rocketmq.client.consumer.DefaultMQPullConsumer 与
// Python client/consumer.py 的 DefaultMQPullConsumer）。
//
// 与 push 消费者（DefaultMQPushConsumer）的区别：**由调用方自己拉、自己管位点**。
// 没有后台拉取线程、没有 rebalance、没有消费监听器；只有：
//   FetchSubscribeMessageQueues / Pull / PullBlockIfNotFound /
//   FetchConsumeOffset / UpdateConsumeOffset / SearchOffset / MaxOffset / MinOffset /
//   EarliestMsgStoreTime / SendMessageBack / CreateTopic。
//
// 这正是 pull 模式的语义（Java 亦如此）：把队列分配与位点推进交给使用方，便于做批量
// 离线消费、按时间回溯、精确控制提交时机等 push 模式做不到的事。
//
// ⚠ 与 Java 的一处有意差异：Java DefaultMQPullConsumer 内嵌 MQPullConsumerImpl，起了
// 一个定时 rebalance 并在队列变更时回调 MessageQueueListener；本实现（同 Python 参考
// 实现）**不做后台 rebalance**——队列由 FetchSubscribeMessageQueues（全部可消费队列）
// 或 FetchMessageQueuesInBalance（本实例平衡后那一份：按 rebalanceByTopic 同一条公式
// 当场算，不做后台线程）显式取，监听器只作为 API 形状保留。需要自动分配队列请用 push 消费者。
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Threading;

using RocketMQ.Common;
using RocketMQ.Remoting;
using RocketMQ.Remoting.Protocol;

namespace RocketMQ.Client;

/// <summary>队列变更监听器（对应 Java MessageQueueListener / Python MessageQueueListener）。</summary>
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
    // ClientConfig 的三个单元化/stream 开关。⚠ Java 的 DefaultMQPullConsumer 在两个
    // 构造函数里就把 enableStreamRequestType 置真（:113/126），所以这里默认 **true**。
    private string _unitName = string.Empty;
    private bool _unitMode;
    private bool _enableStreamRequestType = true;
    // Java ClientConfig#pollNameServerInterval 的默认值（:58）
    private int _pollNameServerIntervalMillis = 30000;
    // 心跳（对齐 Java ClientConfig:59 的 heartbeatBrokerInterval，默认 30000ms）。
    // Java 侧心跳由实例级周期任务发出（DefaultMQPullConsumerImpl:746 把本组注册进实例的
    // consumerTable），本端口没有实例级心跳，改由消费者自带线程发（与 push / lite 同款）。
    private bool _heartbeatEnabled = true;
    private int _heartbeatBrokerIntervalMillis = 30000;
    private long _heartbeatCount;
    private volatile bool _heartbeatStop;
    private Thread? _heartbeatThread;
    private readonly SortedSet<string> _registerTopics = new(StringComparer.Ordinal);
    private readonly Dictionary<string, IMessageQueueListener> _messageQueueListeners =
        new(StringComparer.Ordinal);
    private IMessageQueueListener? _messageQueueListener;
    // 队列分配策略，对应 Java DefaultMQPullConsumer.allocateMessageQueueStrategy
    // （字段初值 new AllocateMessageQueueAveragely():89，getter/setter:196-202）。
    // 与 Java 同款：setter 不校验，置 null 由 Start() 的 checkConfig(:803) 拒绝。
    // 本端口拉模式不做 rebalance，所以它只是配置面 + 启动校验。
    private IAllocateMessageQueueStrategy? _allocateMessageQueueStrategy =
        new AllocateMessageQueueAveragely();

    private readonly int _brokerSuspendMaxTimeMillis = 20000;
    private readonly int _consumerPullTimeoutMillis = 10000;
    private readonly int _consumerTimeoutMillisWhenSuspend = 30000;

    // Java DefaultMQPullConsumerImpl.pullAPIWrapper.pullFromWhichNodeTable：每次拉取应答头里的
    // suggestWhichBrokerId 回写进来，下次拉取按它选主/从（缺省 master=0）。
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

    /// <summary>对应 Java DefaultMQPullConsumer.getAllocateMessageQueueStrategy(:196)。</summary>
    public IAllocateMessageQueueStrategy? AllocateMessageQueueStrategy =>
        _allocateMessageQueueStrategy;

    /// <summary>
    /// 对应 Java registerMessageQueueListener(topic, listener)（DefaultMQPullConsumer:328-335）：
    /// topic **无条件**进 RegisterTopics，listener 为 null 也登记 —— Java 的
    /// MQPullConsumerScheduleService:100 就是传 null 只登记不监听。RegisterTopics 同时是
    /// 拉模式心跳订阅集的唯一来源（DefaultMQPullConsumerImpl.subscriptions():357-385），
    /// 漏登记 = 心跳不带订阅 = broker 侧 GET_CONSUMER_LIST_BY_GROUP(38) 数不出本组。
    /// ⚠ 存储口径与 Java 不同：Java 入表时就 withNamespace(topic)（:330），本端口集合存
    /// 裸名、用到时再 WrapNamespace（与 FetchSubscribeMessageQueues 的入参口径一致）。
    /// </summary>
    public void RegisterMessageQueueListener(string topic, IMessageQueueListener? listener)
    {
        if (string.IsNullOrEmpty(topic))
        {
            return;
        }

        _registerTopics.Add(topic);
        // Java :331-333：listener 为 null 时只登记，不清空既有监听器。
        if (listener is not null)
        {
            _messageQueueListeners[topic] = listener;
        }
    }

    /// <summary>命名空间（对应 Java DefaultMQPullConsumer.setNamespace）。</summary>
    public string Namespace
    {
        get => _namespace;
        set => _namespace = value ?? string.Empty;
    }

    /// <summary>对应 Java <c>ClientConfig#namespaceV2</c>（5.x 新命名空间）：非空时由
    /// <see cref="NamespaceRpcHook"/>（钩子链首）给每笔请求加 <c>nsd=true</c> / <c>ns</c>
    /// 扩展头。钩子每笔请求现读本属性（Java 同款），Start() 之后设置也从下一笔请求起生效。</summary>
    public string NamespaceV2
    {
        get => _namespaceV2;
        set => _namespaceV2 = value ?? string.Empty;
    }

    private string _namespaceV2 = string.Empty;

    // ---------------- unitName / unitMode / enableStreamRequestType ----------------
    // 对应 Java ClientConfig 的三个同名开关。⚠ 必须在 Start() 之前设置。
    /// <summary>单元名：进 clientId 的 <c>@&lt;unitName&gt;</c> 段，也拼进动态取址 URL。</summary>
    public string UnitName
    {
        get => _unitName;
        set => _unitName = value ?? string.Empty;
    }

    /// <summary>
    /// 对应 Java <c>ClientConfig#isUnitMode()</c>：进回投请求头与（有过滤钩子时的）
    /// 消息过滤上下文，broker 据此给 %RETRY% topic 打 UNIT_SUB(0x2)。
    /// </summary>
    public bool UnitMode
    {
        get => _unitMode;
        set => _unitMode = value;
    }

    /// <summary>
    /// 每笔请求带 <c>ReqT=0</c>、clientId 末尾多一段 <c>@STREAM</c>。
    /// Java 的 DefaultMQPullConsumer 在构造里就置真（:113/126），本端口默认值与之对齐。
    /// </summary>
    public bool EnableStreamRequestType
    {
        get => _enableStreamRequestType;
        set => _enableStreamRequestType = value;
    }

    /// <summary>
    /// Java <c>ClientConfig#pollNameServerInterval</c>（:58，默认 30000ms）：在用 topic 的
    /// 路由刷新周期，Start() 时透传给 MqClient（之后改不重排已启动的周期任务）。
    /// </summary>
    public int PollNameServerIntervalMillis
    {
        get => _pollNameServerIntervalMillis;
        set => _pollNameServerIntervalMillis = value;
    }

    /// <summary>是否周期性发心跳（对齐 push 消费者的开关；启动时那一轮不受影响）。</summary>
    public bool HeartbeatEnabled
    {
        get => _heartbeatEnabled;
        set => _heartbeatEnabled = value;
    }

    /// <summary>心跳周期，对应 Java <c>ClientConfig#heartbeatBrokerInterval</c>（:59，默认 30000ms）。</summary>
    public int HeartbeatBrokerIntervalMillis
    {
        get => _heartbeatBrokerIntervalMillis;
        set => _heartbeatBrokerIntervalMillis = value;
    }

    /// <summary>心跳成功轮数（真机验证用；对应 Python <c>heartbeat_count()</c>）。</summary>
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

            // 对齐 Java DefaultMQPullConsumer.start()：把消费组套上命名空间（ns%group），
            // 之后所有面向 broker 的组名（心跳 / 位点 / 回投）都用包装后的值。
            if (_namespace.Length > 0)
            {
                _consumerGroup = NamespaceUtil.WrapNamespace(_namespace, _consumerGroup);
            }

            // 对应 Java DefaultMQPullConsumerImpl.checkConfig（:772）：组名合法性 + 挡掉
            // DEFAULT_CONSUMER（订阅关系/位点会串组）。纯本地校验，排在连地址之前。
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

            // 对应 Java DefaultMQPullConsumerImpl.checkConfig(:803)：策略为 null 直接拒绝启动。
            if (_allocateMessageQueueStrategy is null)
            {
                throw new MQClientException("allocateMessageQueueStrategy is null");
            }

            // 对应 Java DefaultMQPullConsumerImpl.start()（:713）：只有 CLUSTERING 才改写
            // 默认 instanceName，clientId 统一走 buildMQClientId 的 <ip>@<instanceName>。
            if (_messageModel == MessageModel.Clustering)
            {
                _instanceName = ClientIds.ChangeInstanceNameToPID(_instanceName);
            }
            if (_clientId.Length == 0)
            {
                _clientId = ClientIds.Build(_instanceName, _unitName, _enableStreamRequestType);
            }

            // 请求钩子（ACL 签名 / stream 的 ReqT）：绑定在 Start() **之前** ——
            // Java 的 rpcHook 随 MQClientAPIImpl 构造传入，实例第一笔报文（路由拉取、
            // 位点查询）就该带着它。拉模式默认开 stream，所以这里必须走 Compose，
            // 直接注册 _rpcHook 会让 ReqT 漏发、且 ACL 签的内容与上线字段不一致。
            IRpcHook? requestHook = RequestHooks.Compose(_enableStreamRequestType, _rpcHook, () => _namespaceV2);
            _mqClient = new MQClientInstance(_clientId, new List<string>(_nameServerAddrs),
                /*connectTimeoutMillis=*/3000, /*invokeTimeoutMillis=*/_consumerPullTimeoutMillis,
                unitName: _unitName, pollNameServerIntervalMillis: _pollNameServerIntervalMillis);
            if (requestHook is not null && !_mqClient.RegisterRpcHook(requestHook))
            {
                ClientLog.Warn("pull consumer rpc hook ignored: MqClient already has one (clientId="
                    + _clientId + ")");
            }
            _mqClient.Start();
            // 拉模式也要登记 topic，路由才会被周期刷新（对齐 Java registerTopicInUse）。
            // 心跳前的刷路由走同一个入口：brokerAddrTable 空着时心跳没有收件人。
            RefreshRouteForHeartbeat();
            _started = true;
            // 同步发一轮让 broker 立刻认识本组，然后交给常驻循环。顺序对齐 lite 拉取消费者
            // Start（同一份理由），也贴合 Java 的 registerConsumer:746 → mQClientFactory.start():755
            // （实例级心跳任务首个周期在 1s 内发出）。
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
            // 优雅注销（对齐 Java DefaultMQPullConsumerImpl.shutdown:689-692：
            // unregisterConsumer → mQClientFactory.shutdown）：立刻从各 broker 的
            // ConsumerManager 摘除本组，不必等心跳超时（默认 ~120s）。本端口没有 Java 的
            // 本地位点表，所以 persistConsumerOffset() 那一步无对应物（位点由调用方
            // UpdateConsumeOffset 直接写给 broker）。
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
                _mqClient.Shutdown();
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

        // Java 侧这条链路是间接的：DefaultMQPullConsumerImpl.subscriptions():357-385
        // 返回 registerTopics 构出的订阅集，实例的 updateTopicRouteInfoFromNameServer
        // 周期任务据此刷路由 → brokerAddrTable 有地址 → 心跳发得出去。本端口没有实例级
        // 路由任务，所以显式刷一遍并按 registerTopics 登记「在用」。
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

    /// <summary>
    /// Java <c>MQClientInstance#prepareHeartbeatData:1031-1045</c> 为拉模式消费者组出来的
    /// 那一份：consumeType() 恒为 CONSUME_ACTIVELY（DefaultMQPullConsumerImpl:348）、
    /// consumeFromWhere() 恒为 CONSUME_FROM_LAST_OFFSET（:353）。订阅集取
    /// subscriptions():357-385：逐条 BuildSubscriptionData(topic, SUB_ALL) 并显式把
    /// SubVersion 置 0（Java setSubVersion(0L)）—— 带默认的时间戳会让 broker 每轮心跳
    /// 都认为订阅变了。⚠ 本端口没有 Java 的 registerSubscriptions 入口，订阅集只从
    /// RegisterTopics 来。
    /// </summary>
    private HeartbeatData BuildHeartbeat()
    {
        var hb = new HeartbeatData(_clientId);
        var cd = new ConsumerData(_consumerGroup, ConsumeType.ConsumeActively, _messageModel,
            ConsumeFromWhere.ConsumeFromLastOffset)
        {
            // Java MQClientInstance:1039：consumerData.setUnitMode(tc.isUnitMode())，
            // broker 据此给 %RETRY%group 打 UNIT_SUB(0x2)（ClientManageProcessor:113-118）。
            UnitMode = _unitMode,
        };
        foreach (string t in _registerTopics)
        {
            // Java 这里拼的是入表时已带命名空间的 topic（DefaultMQPullConsumer:330）；
            // 本端口集合存裸名，所以订阅必须在这里 WrapNamespace —— 与上面
            // RefreshRouteForHeartbeat 的收件人口径一致，否则刷的是 "ns%topic" 的路由、
            // 报的却是 "topic" 的订阅，broker 建的订阅表和实际拉取的 topic 对不上。
            SubscriptionData sub =
                FilterAPI.BuildSubscriptionData(NamespaceUtil.WrapNamespace(_namespace, t), "*");
            sub.SubVersion = 0;
            cd.AddSubscriptionData(sub);
        }

        hb.AddConsumerData(cd);
        return hb;
    }

    /// <summary>
    /// 向所有已知 broker（含从节点）发一次本组心跳，返回成功台数。收件人取
    /// <c>GetAllBrokerAddrs</c>：Java sendHeartbeatToAllBroker:732-750 只对 consumerEmpty
    /// 跳从节点，消费者心跳必带 ConsumerData，故从节点不跳（理由同 push 消费者同名方法）。
    /// </summary>
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

    // ---------------- 队列 ----------------
    /// <summary>该 topic 的全部可消费队列（Java fetchSubscribeMessageQueues）。</summary>
    public List<MessageQueue> FetchSubscribeMessageQueues(string topic)
    {
        MQClientInstance c = RequireClient();
        string realTopic = NamespaceUtil.WrapNamespace(_namespace, topic);
        // Java DefaultMQPullConsumerImpl.fetchSubscribeMessageQueues:142 与 push 同源：读的
        // 是 rebalanceImpl 的订阅信息（topicRouteData2TopicSubscribeInfo：读位 +
        // readQueueNums、不筛 master），不是发布信息。
        TopicRouteData? route = c.GetTopicRouteData(realTopic);
        if (route is null)
        {
            // 对齐 Java：topic 不存在（拿不到路由）时直接抛，而不是返回空列表。
            throw new MQClientException("the topic[" + topic + "] not exist");
        }

        return route.GetAllSubscribeMessageQueue(realTopic);
    }

    /// <summary>本实例「平衡后」应负责的队列（对应 Java MQPullConsumer:187；官方
    /// example/simple/PullConsumer.java:62 就靠它决定去拉哪些队列）。</summary>
    /// <remarks>
    /// Java（DefaultMQPullConsumerImpl:120-135）读的是后台 rebalance 填出来的
    /// <c>processQueueTable</c>；本端口拉模式没有那条后台线程（见文件头说明），所以按
    /// <c>RebalanceImpl.rebalanceByTopic</c> 的**同一条公式**当场算一遍，结果与 Java 表里
    /// 那份一致：BROADCASTING 全量；CLUSTERING 用订阅信息（读位口径，与
    /// <see cref="FetchSubscribeMessageQueues"/> 同源）作 mqAll、
    /// GET_CONSUMER_LIST_BY_GROUP 作 cidAll，再交给本实例配置的分配策略取自己那一份。
    /// 拿不到路由或消费者列表时**保留现有分配**——退回本实例实际拉过的队列
    /// （Java 同名表 pullFromWhichNodeTable 的键集），绝不回退成「独占全部队列」，
    /// 否则同组多实例会互相重复消费（与 push 的 DoRebalance 同一条铁律）。
    /// 返回值已剥掉命名空间前缀（Java parseSubscribeMessageQueues），并按
    /// topic → brokerName → queueId 排序，保证多次调用顺序稳定。
    /// </remarks>
    public List<MessageQueue> FetchMessageQueuesInBalance(string topic)
    {
        MQClientInstance c = RequireClient(); // Java isRunning()：未启动直接抛
        if (topic is null)
        {
            // Java :122-124 的 throw new IllegalArgumentException("topic is null")。
            throw new MQClientException("topic is null");
        }

        string realTopic = NamespaceUtil.WrapNamespace(_namespace, topic);
        List<MessageQueue> pulled = PulledQueues(realTopic);
        List<MessageQueue>? allocated = null;
        if (_messageModel == MessageModel.Broadcasting)
        {
            // Java rebalanceByTopic 对 BROADCASTING 不查消费者列表、全量分配。
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
                // 拿不到路由或消费者列表：**保留现有分配**（本实例实际拉过的队列，
                // Java 同名表 pullFromWhichNodeTable 的键集）。注意「算出来确实是空」
                // （队列都分给了同组别的实例）不走这条分支——那时返回空才是 Java 的表内容。
                ClientLog.Debug("fetchMessageQueuesInBalance: no route/consumer list for "
                    + _consumerGroup + "/" + realTopic + ", keep current assignment");
                allocated = pulled;
            }
        }

        // Java parseSubscribeMessageQueues：把带命名空间的 topic 还原成用户侧的裸名。
        var outList = new List<MessageQueue>();
        foreach (MessageQueue mq in allocated)
        {
            if (mq.Topic != realTopic)
            {
                // Java :128-131 逐个比对表键的 topic；别让策略的意外返回值把别的 topic
                // 混进调用方的拉取循环。
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

    /// <summary>本实例实际拉过的队列（Java pullFromWhichNodeTable 的键集），按 topic 收口。</summary>
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

    // ---------------- 拉取 ----------------
    /// <summary>一次短轮询拉取（Java pull）。timeoutMillis &lt;= 0 表示用默认 consumerPullTimeoutMillis。</summary>
    public PullResult Pull(MessageQueue mq, string subExpression = "*", long offset = 0,
        int maxNums = 32, int timeoutMillis = -1)
    {
        MQClientInstance c = RequireClient();
        int timeout = timeoutMillis > 0 ? timeoutMillis : _consumerPullTimeoutMillis;
        // 对齐 Java DefaultMQPullConsumerImpl.pullSyncImpl（:248）：
        //   sysFlag = PullSysFlag.buildSysFlag(false, block, true, false)
        // 即 commitOffset=false、suspend=block。**Pull() 的 block=false** ——
        // 位点由调用方自己 UpdateConsumeOffset 提交，且这是**短轮询**不挂起。
        // ⚠ 曾在这里写成 suspend=true：broker 在队尾会挂起到 brokerSuspendMaxTimeMillis
        // （默认 20s），而客户端 5s 就超时 → RemotingTimeoutException（真机必现）。
        int sysFlag = PullSysFlag.BuildSysFlag(commitOffset: false, suspend: false,
            subscription: true, classFilter: false);
        MessageQueue real = WrapMq(mq);
        SubscriptionData sub = FilterAPI.BuildSubscriptionData(real.Topic, subExpression);
        // Java PullAPIWrapper#pullKernelImpl：按 pullFromWhichNodeTable 选主/从
        long brokerId = TakePullFromWhichNode(real);
        PullResult result = c.PullMessage(_consumerGroup, real, offset, maxNums, sysFlag, 0,
            string.IsNullOrEmpty(sub.SubString) ? "*" : sub.SubString,
            // Java：TAG 类型时 subVersion 传 0（isTagType ? 0L : subVersion）
            0, ExpressionType.TAG, timeout, -1, 15000,
            /*addrIn=*/null, /*requestSource=*/0, /*brokerId=*/brokerId);
        UpdatePullFromWhichNode(real, result);
        return result;
    }

    /// <summary>长轮询拉取（Java pullBlockIfNotFound，block=true → 挂起等消息）。</summary>
    public PullResult PullBlockIfNotFound(MessageQueue mq, string subExpression, long offset,
        int maxNums)
    {
        MQClientInstance c = RequireClient();
        // block=true：suspend=true 让 broker 挂起到有消息；超时用
        // _consumerTimeoutMillisWhenSuspend（Java :250 的 `block ? ... : timeout`）。
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

    /// <summary>Java <c>PullAPIWrapper#recalculatePullFromWhichNode</c>：无记录按 master=0。</summary>
    private long TakePullFromWhichNode(MessageQueue mq)
    {
        lock (_lock)
        {
            return _pullFromWhichNode.TryGetValue(mq, out long brokerId) ? brokerId : MixAll.MasterId;
        }
    }

    /// <summary>
    /// Java <c>PullAPIWrapper#updatePullFromWhichNode:157-164</c>：把应答头里的
    /// <c>suggestWhichBrokerId</c> 写回表；缺省（老 broker 不带该字段）按 master=0 记账 ——
    /// 与 Java 的 long 原语口径一致，所以**不能**把 null 当成「保留旧值」。
    /// </summary>
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
        // Java MQAdminImpl:250（earliestMsgStoreTime）与 max/min/search 同一个形状：
        // 只认 master，刷一次路由重查，仍拿不到照 :264 抛「The broker[X] not exist」。
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

    // ---------------- 回投 / 建 topic ----------------
    /// <summary>
    /// 消息回投（对应 Java DefaultMQPullConsumer.sendMessageBack）。
    /// 注意两点（真机踩过）：
    /// 1. 地址靠 FindBrokerAddressInPublish(msg.BrokerName) 反查**发布地址表**（只认 master），
    ///    所以调用方必须先用本 consumer 访问过该 topic（Java DefaultMQPullConsumerImpl:654
    ///    走的也是 findBrokerAddressInPublish）。从节点不接 CONSUMER_SEND_MSG_BACK。
    /// 2. 与 Java 的**有意差异**：Java 在失败时会吞掉异常、改用内部默认生产者把消息直接发到
    ///    %RETRY%group（DefaultMQPullConsumerImpl:666 的 catch 分支）。本实现不做这个兜底 ——
    ///    回投失败就抛，让调用方看见，而不是换一条路径静默重发。
    /// </summary>
    public void SendMessageBack(MessageExt msg, int delayLevel)
    {
        MQClientInstance c = RequireClient();
        // Java DefaultMQPullConsumerImpl:657-658：拿不到发布地址就报
        // 「Broker[X] master node does not exist」（只认 brokerId=0）。
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
            // ⚠ 有意超出 Java：MQClientAPIImpl#consumerSendMessageBack(:1684-1693) 从不写
            // unitMode（恒 false），但 broker 侧读它（AbstractSendMessageProcessor:135-138
            // → buildSysFlag(false, true)）。按消费者配置如实上报，单元化重试 topic
            // 才会带 UNIT_SUB 标记。
            UnitMode = _unitMode,
            // ⚠ 不照抄 Java 弃用的 DefaultMQPullConsumerImpl#sendMessageBack（它直接传
            // getMaxReconsumeTimes()，默认 -1）。客户端版本 ≥ V3_4_9 后 broker 无条件采信
            // 该字段（AbstractSendMessageProcessor:172-179），-1 会让 reconsumeTimes(0) >= -1
            // 成立、消息直接进 %DLQ%。留空交给订阅组的 retryMaxTimes 判定。
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
