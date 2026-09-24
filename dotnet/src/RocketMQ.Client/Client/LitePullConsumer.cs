// 轻量拉取消费者（对齐 org.apache.rocketmq.client.consumer.DefaultLitePullConsumer 与
// Python client/consumer.py 的 DefaultLitePullConsumer、C++ lite_pull_consumer）。
//
// 与 DefaultMQPullConsumer（C# PullConsumer.cs）的本质区别：
// - DefaultMQPullConsumer：调用方自己 Pull(mq,offset) 逐队列拉、自己管位点；没有本地缓冲、
//   没有后台拉取线程、不做 rebalance。
// - DefaultLitePullConsumer：支持两种模式——
//   * subscribe 模式：登记订阅后自动 rebalance 分配队列（与 push 一致），后台拉取线程把
//     消息灌进**本地缓冲**；Poll() 只从本地缓冲取消息，不用调用方管位点；
//   * assign 模式：调用方 Assign([mq...]) 显式指定队列，不走 rebalance，同样后台灌本地缓冲。
// 两种模式都用 Poll(timeout) 取批量消息；位点默认 AutoCommit，但**提交的是"已消费游标"**
// （Poll 交给调用方的那一格），不是后台的拉取游标 —— 与 Java
// `AssignedMessageQueue.MessageQueueState{pullOffset, consumeOffset}` + `offsetStore` 的
// 三张表同口径，详见 Commit() 的注释。
//
// 设计取舍（与 C++ / Python 参考实现一致）：
// - 后台**单个**拉取线程顺序遍历所有已分配队列做短轮询（suspend=false），把消息塞进
//   一个线程安全的本地缓冲 _localBuffer；Poll() 用 Monitor 等待并 drain 该缓冲。
// - subscribe 模式的 rebalance 复用既有 GetConsumerIdListByGroup + 队列分配策略
//   （AllocateStrategy.cs，默认 AllocateMessageQueueAveragely），与 push 消费者同一套分配算法；
//   查询不到消费组列表时按 Java 语义「保留当前分配」，不回退独占。
// - 不做 POP / 推模式；不做 broker 主动请求（309/313）处理（那是 push 消费者的职责）。
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Threading;

using RocketMQ.Common;
using RocketMQ.Remoting;
using RocketMQ.Remoting.Protocol;

namespace RocketMQ.Client;

/// <summary>队列分配变更监听器（对应 Java MessageQueueListener / Python MessageQueueListener）。</summary>
public interface ILiteMessageQueueListener
{
    // mqAll=当前订阅的全部队列；mqDivided=本次新分配给本实例的队列。
    void MessageQueueChanged(IReadOnlyList<MessageQueue> mqAll, IReadOnlyList<MessageQueue> mqDivided);
}

public sealed class DefaultLitePullConsumer
{
    private readonly object _lock = new();
    private string _consumerGroup;
    private string _namespace = string.Empty;
    private string _instanceName = MixAll.DefaultInstanceName;
    private string _clientId = string.Empty;
    private string _messageModel = MessageModel.Clustering;
    // 队列分配策略，对应 Java DefaultLitePullConsumer.allocateMessageQueueStrategy
    // （字段初值 new AllocateMessageQueueAveragely()）。
    private IAllocateMessageQueueStrategy? _allocateMessageQueueStrategy = new AllocateMessageQueueAveragely();
    private string _consumeFromWhere = ConsumeFromWhere.ConsumeFromLastOffset;
    // Java DefaultLitePullConsumer.consumeTimestamp 的字段初值：now - 30 分钟。
    // 留空会让 CONSUME_FROM_TIMESTAMP 退化成「从当前时刻起消费」。
    private string _consumeTimestamp =
        UtilAll.TimeMillisToHumanString3(UtilAll.CurrentTimeMillis() - 30 * 60 * 1000);
    private readonly List<string> _nameServerAddrs = new();
    private IRpcHook? _rpcHook;
    // ClientConfig 的三个单元化/stream 开关。⚠ Java 的 DefaultLitePullConsumer 在构造
    // 函数里就把 enableStreamRequestType 置真（:213/228），所以这里默认 **true**。
    private string _unitName = string.Empty;
    private bool _unitMode;
    private bool _enableStreamRequestType = true;
    // Java ClientConfig#pollNameServerInterval 的默认值（:58）
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
    /// <summary>
    /// **拉取游标**（Java <c>MessageQueueState.pullOffset</c>）：只回答"下一次从哪拉"，
    /// 任何情况下都不是提交内容。
    /// </summary>
    private readonly Dictionary<MessageQueue, long> _nextOffset = new();
    /// <summary>
    /// **已消费游标**（Java <c>MessageQueueState.consumeOffset</c>）：只有 Poll() 把消息交到
    /// 调用方手上才前进，本地缓冲里压着的部分不算已消费。
    /// </summary>
    private readonly Dictionary<MessageQueue, long> _consumeOffset = new();
    /// <summary>
    /// 内存位点表，对位 Java <c>RemoteBrokerOffsetStore.offsetTable</c>：提交的落点，
    /// <c>persist=false</c> 的值就攒在这里。
    /// </summary>
    private readonly Dictionary<MessageQueue, long> _offsetTable = new();
    private readonly Dictionary<MessageQueue, long> _seekOffset = new();
    // Java DefaultLitePullConsumerImpl.nextAutoCommitDeadline 的初值：**-1** 而不是 0
    // （0 是 epoch 起点，等价于"永远不到点"）⇒ 第一次检查就会提交一次。
    private long _nextAutoCommitDeadline = -1;
    private readonly HashSet<MessageQueue> _paused = new();
    private ILiteMessageQueueListener? _messageQueueListener;

    private MQClientInstance? _mqClient;
    private bool _started;
    private bool _running;
    private Thread? _pullThread;
    private Thread? _heartbeatThread;

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

    /// <summary>
    /// 队列分配策略（对应 Java DefaultLitePullConsumer.setAllocateMessageQueueStrategy）。
    /// 与 Java 同款：setter 不校验，置 null 由 Start() 的 checkConfig 拒绝
    /// （Java DefaultLitePullConsumerImpl.checkConfig:435 "allocateMessageQueueStrategy is null"）。
    /// </summary>
    public void SetAllocateMessageQueueStrategy(IAllocateMessageQueueStrategy? strategy) =>
        _allocateMessageQueueStrategy = strategy;

    public IAllocateMessageQueueStrategy? AllocateMessageQueueStrategy => _allocateMessageQueueStrategy;

    public void SetNamespace(string ns) => _namespace = ns ?? string.Empty;

    // ---------------- unitName / unitMode / enableStreamRequestType ----------------
    // 对应 Java ClientConfig 的三个同名开关。⚠ 必须在 Start() 之前设置。
    /// <summary>单元名：进 clientId 的 <c>@&lt;unitName&gt;</c> 段，也拼进动态取址 URL。</summary>
    public string UnitName
    {
        get => _unitName;
        set => _unitName = value ?? string.Empty;
    }

    /// <summary>
    /// 对应 Java <c>ClientConfig#isUnitMode()</c>：lite 路径经 MQClientInstance:1039 进
    /// 心跳的 ConsumerData.unitMode，决定 broker 侧 %RETRY% topic 的 UNIT_SUB(0x2) 标记。
    /// （Java 还把它传进 PullAPIWrapper 用于过滤上下文，DefaultLitePullConsumerImpl:356-359；
    /// 本端 lite 消费者没有 filter message hook，所以只有心跳这一处落地。）
    /// </summary>
    public bool UnitMode
    {
        get => _unitMode;
        set => _unitMode = value;
    }

    /// <summary>
    /// 每笔请求带 <c>ReqT=0</c>、clientId 末尾多一段 <c>@STREAM</c>。
    /// Java 的 DefaultLitePullConsumer 在构造里就置真，本端口默认值与之对齐。
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

    public bool IsStarted => _started;

    public bool IsRunning => _running;

    public void SetConsumeFromWhere(string where) => _consumeFromWhere = where;

    public void SetConsumeTimestamp(string ts) => _consumeTimestamp = ts;

    /// <summary>对应 Java DefaultLitePullConsumer.getConsumeTimestamp：默认「now - 30 分钟」的 yyyyMMddHHmmss。</summary>
    public string ConsumeTimestamp => _consumeTimestamp;

    public void SetPullBatchSize(int n) => _pullBatchSize = Math.Max(1, n);

    public void SetPollTimeoutMillis(int ms) => _pollTimeoutMillis = Math.Max(0, ms);

    public void SetAutoCommit(bool b) => _autoCommit = b;

    public void SetAutoCommitIntervalMillis(int ms) => _autoCommitIntervalMillis = Math.Max(0, ms);

    public void SetConsumerTimeoutMillisWhenSuspend(int ms) => _consumerTimeoutMillisWhenSuspend = ms;

    public void SetBrokerSuspendMaxTimeMillis(int ms) => _brokerSuspendMaxTimeMillis = ms;

    public void SetPullIntervalMillis(int ms) => _pullIntervalMillis = Math.Max(0, ms);

    public void SetMessageQueueListener(ILiteMessageQueueListener listener) => _messageQueueListener = listener;

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
            // Java assignedMessageQueue.updateAssignedMessageQueue：撤掉的队列连着整份
            // MessageQueueState 丢掉（拉取游标与已消费游标一起消失）。**不**动内存位点表、
            // 也不 persist —— 那份清理挂在 subscribe 模式的 rebalance 上，assign 不走 rebalance。
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

            // 对应 Java DefaultLitePullConsumerImpl.checkConfig（:415）：组名合法性 + 挡掉
            // DEFAULT_CONSUMER。纯本地校验，排在地址/订阅检查之前。
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

            // 对应 Java DefaultLitePullConsumerImpl.checkConfig（:435）：策略为 null 直接拒绝启动。
            if (_allocateMessageQueueStrategy is null)
            {
                throw new MQClientException("allocateMessageQueueStrategy is null");
            }

            if (!hasSub && !_assignMode)
            {
                throw new MQClientException("subscription is not set, call Subscribe() or Assign() first");
            }

            // 对应 Java DefaultMQPushConsumerImpl.checkConfig（:1058）：启动即无条件校验，
            // 而不是等到算起点时抛出、被下层 catch 吞掉后静默退化成从 max offset 消费。
            ParseConsumeTimestamp(_consumeTimestamp);

            // 对应 Java DefaultLitePullConsumerImpl.start()（:289）：只有 CLUSTERING 才改写
            // 默认 instanceName，clientId 统一走 buildMQClientId 的 <ip>@<instanceName>。
            if (_messageModel == MessageModel.Clustering)
            {
                _instanceName = ClientIds.ChangeInstanceNameToPID(_instanceName);
            }
            if (_clientId.Length == 0)
            {
                _clientId = ClientIds.Build(_instanceName, _unitName, _enableStreamRequestType);
            }

            // 请求钩子（ACL 签名 / stream 的 ReqT）：lite 消费者默认开 stream，必须走
            // RequestHooks.Compose 把 StreamTypeRPCHook 排在用户钩子之前 —— 直接注册
            // _rpcHook 会让 ReqT 漏发，开 ACL 时签的内容也与上线字段不一致。
            // 绑定位置同样在 Start() **之前**（Java 的 rpcHook 随 MQClientAPIImpl 构造传入）。
            IRpcHook? requestHook = RequestHooks.Compose(_enableStreamRequestType, _rpcHook);
            _mqClient = new MQClientInstance(_clientId, new List<string>(_nameServerAddrs),
                /*connectTimeoutMillis=*/3000, /*invokeTimeoutMillis=*/10000,
                unitName: _unitName, pollNameServerIntervalMillis: _pollNameServerIntervalMillis);
            if (requestHook is not null && !_mqClient.RegisterRpcHook(requestHook))
            {
                ClientLog.Warn("lite pull consumer rpc hook ignored: MqClient already has one (clientId="
                    + _clientId + ")");
            }
            _mqClient.Start();

            if (_assignMode)
            {
                foreach (MessageQueue mq in _assigned)
                {
                    _mqClient.RegisterTopicInUse(mq.Topic);
                }
            }

            // 先把 tag 订阅注册给 broker（心跳），再启动后台拉取，避免首轮拉取因 broker 不认订阅而丢消息。
            // 心跳只发给「实例路由表里已知的 broker」，而自建实例此刻路由表还是空的，
            // 所以先同步把订阅/分配的 topic 路由拉进来（Python `_refresh_route_for_heartbeat`、
            // Java 在注册心跳前先 updateTopicRouteInfoFromNameServer）。
            // 少了这一步，订阅要等到 5s 心跳循环第一轮才注册上 broker，
            // 同组多实例时首轮 rebalance 会各自独占全部队列。
            RefreshRouteForHeartbeat();
            SendHeartbeatToAllBroker();
            _running = true;
            _started = true;

            // assign 模式的起点必须排在「实例已启动 + 路由已刷新」之后：查已提交位点既要过
            // RequireClient() 的状态检查，又要按 brokerName 找 broker 地址，而本端口的
            // MQClientInstance 不像 Java 的 admin 路径那样按需拉路由 —— 早一步算就是查不到，
            // 异常被吞掉之后 Start() 返回时拉取游标还停在 -1。
            // Java 的对应位置在 start() 把 serviceState 置成 RUNNING 之后的
            // operateAfterRunning → updateAssignPullTask → computePullOffset。
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

        // Java 的 shutdown 走 persistConsumerOffset()：把内存位点表按当下持有的队列刷一遍，
        // 与 auto_commit 无关（手动模式 persist=false 攒下的值同样要落盘）。自动提交模式再多
        // 走一步 Commit()：本端口没有 Java 那份 5s 定时器，"poll 交出去但还没到截止时刻"的
        // 位点得在这里补上，否则重启后从上一格重投。
        // ⚠ 必须在 _lock 之外发：提交是 RPC，抱着锁做网络会把整条 poll/拉取路径卡住。
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
            _mqClient?.Shutdown();
            _mqClient = null;
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
        // 短轮询（suspend=false），位点由 auto-commit 单独提交（与 Java LitePull 一致）。
        int sysFlag = PullSysFlag.BuildSysFlag(commitOffset: false, suspend: false,
            subscription: true, classFilter: false);
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

        if (result.Status == PullStatus.Found && result.MsgFoundList.Count > 0)
        {
            var msgs = new List<MessageExt>(result.MsgFoundList);
            FilterTags(mq.Topic, msgs, sub);
            if (msgs.Count > 0)
            {
                Enqueue(msgs);
                lock (_lock)
                {
                    // 只推进**拉取游标**。「已消费游标」是 Poll() 交付时才写的，两条线不是一条：
                    // 缓冲里压着没交出去的消息不能算已消费（Java processQueue.removeMessage 同口径）。
                    _nextOffset[mq] = msgs[msgs.Count - 1].QueueOffset + 1;
                }

                return true;
            }
        }

        return false;
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

    /// <summary>
    /// 对位 Java <c>DefaultLitePullConsumerImpl#maybeAutoCommit</c>：只有一道全局截止时刻，
    /// 到点走一次 <see cref="Commit()"/> 并把截止时刻推到 <c>now + autoCommitIntervalMillis</c>。
    ///
    /// 调用点与 Java 一致，只有 <see cref="Poll"/> 开头一处（外加 Shutdown 的兜底提交）。
    /// Java 里空闲消费者靠 MQClientInstance 每 5s 的 persistConsumerOffset 定时器刷的是
    /// **内存位点表**，而那张表也只有 commit 路径会写，所以停掉 poll 之后 Java 同样不会往前
    /// 推进 broker 位点；这里若把它塞进拉取循环，就成了"没人 poll 也提交"，比 Java 激进。
    /// </summary>
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

    /// <summary>
    /// Java <c>poll()</c> 出口处的 <c>updateConsumeOffset(mq, processQueue.removeMessage(msgs))</c>：
    /// 消息交到调用方手上才算已消费。只更新**本实例持有**（有拉取记录）的队列，
    /// 取每种队列里最大的那一格 +1。
    /// </summary>
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

        try
        {
            if (RequireClient().QueryConsumerOffset(_consumerGroup, mq, out long off))
            {
                return off;
            }
        }
        catch
        {
        }

        if (_consumeFromWhere == ConsumeFromWhere.ConsumeFromFirstOffset)
        {
            return RequireClient().GetMinOffset(mq);
        }

        if (_consumeFromWhere == ConsumeFromWhere.ConsumeFromTimestamp)
        {
            return RequireClient().SearchOffsetByTimestamp(mq, ParseConsumeTimestamp(_consumeTimestamp));
        }

        return RequireClient().GetMaxOffset(mq);
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
            List<MessageQueue> mqAll;
            try
            {
                TopicPublishInfo info = RequireClient().GetTopicPublishInfo(topic);
                mqAll = info.MsgQueueList;
            }
            catch
            {
                mqAll = new List<MessageQueue>();
            }

            List<string>? cidAll = RequireClient().GetConsumerIdListByGroup(topic, _consumerGroup);
            if (cidAll is null) cidAll = new List<string>();
            if (!cidAll.Contains(_clientId)) cidAll.Add(_clientId);
            // Java RebalanceImpl.rebalanceByTopic 在分配前 Collections.sort(mqAll) + sort(cidAll)：
            // 顺序不一致会让同组不同实例算出冲突的分配（同一队列被两个实例同时消费）。
            mqAll.Sort();
            cidAll.Sort(StringComparer.Ordinal);
            // Java RebalanceImpl#rebalanceByTopic 的 catch (Throwable) 直接 return，位置在
            // updateProcessQueueTableInRebalance **之前** → 一次分配异常不该把队列撤走。
            // 所以这里回退成「沿用本 topic 当前的分配」（Python / C++ / Rust 同口径）。
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
                    // Java RebalanceLitePullImpl#removeUnnecessaryMessageQueue：先 persist(mq)
                    // 再 removeOffset(mq) —— 撤手之前把最后一次提交的位点补发出去。
                    // persist 是 RPC，抱着锁做网络会把整条 poll/拉取路径卡住，所以这里只把
                    // **该补发的队列**记下来，锁外再发；表里那一格也留到发完再清。
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

        // 锁外补发末次提交，发完再清掉内存位点表那一格（Java persist → removeOffset 的次序）。
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
            try
            {
                TopicPublishInfo info = RequireClient().GetTopicPublishInfo(topic);
                all.AddRange(info.MsgQueueList);
            }
            catch
            {
            }
        }

        return all;
    }

    // ---------------- Poll / 位点 ----------------
    public List<MessageExt> Poll(int timeoutMillis = -1)
    {
        // Java poll() 进来先按截止时刻试一次自动提交（拿锁之前做：提交要发 RPC，
        // 抱着缓冲锁等网络会把 Enqueue 一起卡住）。
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

        // 消息交到调用方手上才推进已消费游标（Java 同一处 updateConsumeOffset）。
        AdvanceConsumeOffset(outMsgs);
        return outMsgs;
    }

    public void Seek(MessageQueue mq, long offset)
    {
        lock (_lock)
        {
            _seekOffset[mq] = offset;
            _nextOffset[mq] = offset;
            // Java 的 seek 只置 seekOffset，下一次拉取时 nextPullOffset() 才把它同时写进
            // consumeOffset（"跳回去"意味着"那里之前都还没消费"，否则重放的消息会被旧位点跳过）。
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

    /// <summary>
    /// Java <c>committed()</c> 走 <c>offsetStore.readOffset(MEMORY_FIRST_THEN_STORE)</c>：
    /// 先看内存位点表（<c>persist=false</c> 刚提交、还没发给 broker 的值也算数），
    /// 再问 broker，并把 broker 的值回填进表里（Java 同一处也回填）。-1 表示 broker 无记录。
    /// </summary>
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

    /// <summary>
    /// 对位 Java <c>commitAll()</c>：按**已消费游标**提交所有当下持有的队列。
    ///
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

    /// <summary>
    /// 对位 Java <c>commit(Map, persist)</c>：调用方指定位点，**只改提交落点，两条游标都不动**。
    /// 空 map 与 Java 一样记一条 warn 就 return，连表都不碰（上一轮 persist=false 攒下的
    /// 内存值原样保留）。
    /// </summary>
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

    /// <summary>
    /// 对位 Java <c>commit(Set, persist)</c>：只提交点名这几条队列，取的是它们当下的
    /// **已消费游标**。空集合静默 return（Java 同）。
    /// </summary>
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
    ///
    /// 两处已知的偏离，与 Python 逐字对应：
    /// ① Java 的 commitAll() 只写内存表，真正发给 broker 靠 MQClientInstance 每
    ///    persistConsumerOffsetInterval（5s）一次的定时器；本端口的 lite 消费者没挂那个定时器，
    ///    所以 persist=true（默认）就地发出去。
    /// ② Java 的 persistAll 用 oneway、异常只记日志；这里发同步带应答，坏位点当场可见。
    /// </summary>
    private void CommitTargets(Dictionary<MessageQueue, long> targets, List<MessageQueue> scope,
        bool persist)
    {
        lock (_lock)
        {
            foreach (KeyValuePair<MessageQueue, long> kv in targets)
            {
                if (kv.Value == -1)
                {
                    // Java 原文：这条队列还没消费过，记 error 并跳过。绝不能把 -1 写给 broker
                    // —— 位点 -1 会让下次消费从队首重投全量。
                    ClientLog.Error("consumerOffset is -1 in messageQueue [" + kv.Key + "].");
                    continue;
                }

                if (!_assigned.Contains(kv.Key))
                {
                    // Java 的 processQueue != null && !isDropped() 守卫：不是本实例持有的队列
                    // 一律不替它提交，静默跳过（Java 原文这里连日志都没有）。
                    continue;
                }

                _offsetTable[kv.Key] = kv.Value;
            }
        }

        if (persist) PersistOffsetTable(scope);
    }

    /// <summary>
    /// Java <c>OffsetStore#persist(mq)</c>：只把这一条队列的内存位点发给 broker，不做清理。
    /// </summary>
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

    /// <summary>
    /// Java <c>RemoteBrokerOffsetStore#persistAll(Set)</c>：内存位点表里落在 <c>mqs</c> 上的
    /// 那部分写给 broker，**不在**其中的条目顺手从表里删掉（Java 日志里那句
    /// <c>remove unused mq</c>）。
    ///
    /// 后半句是 Java 的真实行为：这张表只服务于当下持有的队列。代价是
    /// <c>Commit(部分队列, persist: true)</c> 会把其余队列**尚未落盘**的内存值一起丢掉 ——
    /// 要提交谁就一次给全。
    /// </summary>
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

        // 未启动时表照样写得进去、只是发不出去（Python/C++/Rust 同）：Java 在这里会先
        // checkServiceState 抛错，本端口放宽这一步，好让表逻辑能离线单测。
        // 直接读连接字段而不是 RequireClient()：Shutdown 已经翻掉 started 标记，
        // 但末次提交仍要用这条还没关掉的连接。
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

    // ---------------- 队列查询 / 控制 ----------------
    public List<MessageQueue> FetchMessageQueues(string topic) =>
        RequireClient().GetTopicPublishInfo(WithNamespace(topic)).MsgQueueList;

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

    // ---------------- 心跳（把 tag 订阅注册给 broker）----------------
    private HeartbeatData BuildHeartbeat()
    {
        var hb = new HeartbeatData(_clientId);
        var cd = new ConsumerData(_consumerGroup, ConsumeType.ConsumePassively, _messageModel, _consumeFromWhere)
        {
            // Java MQClientInstance:1039：consumerData.setUnitMode(impl.isUnitMode())，
            // broker 据此给 %RETRY%group 打 UNIT_SUB(0x2)（ClientManageProcessor:113-118）。
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
    /// 并登记为在用（对应 Python `_refresh_route_for_heartbeat`）。
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
        foreach (string addr in _mqClient.GetRouteOfAllBrokers())
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

    // 对应 Java UtilAll.parseDate(ts, UtilAll.YYYYMMDDHHMMSS)：该字段**只**是 14 位本地墙钟日期。
    // 旧实现完全不解析日期，把任何纯数字串当 epoch 秒/毫秒，"20230101000000" 会被解释成
    // 公元 2611 年，起点彻底错位。
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
