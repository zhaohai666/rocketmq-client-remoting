// 推模式消费者（对齐 org.apache.rocketmq.client.consumer.DefaultMQPushConsumer）。
//
// 架构（对齐 Java PushConsumer 的三层模型）：
//   1. 拉取层：每个队列一个拉取线程（对应 Java PullMessageService 的并发长轮询——
//      broker 为每个队列挂起长轮询请求、消息到达立即返回），拉到的消息进
//      _pending 缓冲（对应 ProcessQueue），拉取游标推进到 nextBeginOffset；
//   2. 分发层：单分发线程从缓冲按 ConsumeMessageBatchMaxSize 取批次交给
//      IMessageListener；RECONSUME_LATER/异常批次逐条回投 %RETRY%topic
//      （延迟梯度 3+reconsumeTimes，超 maxReconsumeTimes 由 broker 转 %DLQ%）；
//   3. 位点层：_consumeOffsetTable 记录"已消费位点"，每 5s 用 UPDATE_CONSUMER_OFFSET
//      提交 broker（Java persistAllConsumerOffset），启动先 QUERY_CONSUMER_OFFSET。
//
// 关键工程点（真机验证得出，勿删注释）：
//   - 拉取必须按队列并行：单线程顺序长轮询下，空闲队列的 suspend 会阻塞
//     其余队列投递（曾导致"第一批消息能收到、后续全迟到"）。
//   - broker 会把客户端下发的 suspend 时间钳制到自身 brokerSuspendMaxTimeMillis，
//     故空闲队列仍会周期性客户端超时 —— 这是**良性**的，按 debug 处理不记 ERROR。
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Threading;

using RocketMQ.Common;
using RocketMQ.Remoting;
using RocketMQ.Remoting.Protocol;

namespace RocketMQ.Client;

/// <summary>
/// 选择器（对应 Java MessageSelector / Python MessageSelector）。
/// </summary>
public sealed class MessageSelector
{
    public string Type { get; set; } = ExpressionType.TAG;
    public string Expression { get; set; } = "*";

    public static MessageSelector ByTag(string tag) =>
        new() { Type = ExpressionType.TAG, Expression = tag };

    public static MessageSelector BySql(string sql) =>
        new() { Type = ExpressionType.Sql92, Expression = sql };
}

/// <summary>
/// 推模式消费者（对应 org.apache.rocketmq.client.consumer.DefaultMQPushConsumer）。
/// </summary>
/// <summary>
/// Java DefaultMQPushConsumerImpl.popDelayLevel（单位**秒**）。
/// 与 send 的延迟档位（首档 1s）不是同一张表，别混。
/// </summary>
public static class PopDelayLevelDefaults
{
    public static readonly int[] Table = { 10, 30, 60, 120, 180, 240, 300, 360,
                                           420, 480, 540, 600, 1200, 1800, 3600, 7200 };
}

/// <summary>
/// POP 模式的队列状态（对应 org.apache.rocketmq.client.impl.consumer.PopProcessQueue）。
/// <para>
/// 与 pull 模式的 ProcessQueue 不同，POP **没有"已拉未消费"缓冲**：消息一弹出就交给
/// 消费线程，确认靠 ack。这里只跟踪两件事：已弹未 ack 的条数（流控用）和队列是否已被
/// rebalance 撤走（撤走后本批消息不再消费、也不 ack，交给 invisibleTime 到期后
/// broker 自动复活重投）。
/// </para>
/// </summary>
public sealed class PopProcessQueue
{
    private readonly object _lock = new();
    private int _waitAckCounter;
    private volatile bool _dropped;

    /// <summary>Java <c>PopProcessQueue.lastPopTimestamp</c>：最近一次**发起**弹出的时刻（毫秒），
    /// 字段初始值同样是"现在"。Java 的 <c>isPullExpired</c> 在 pop 路径上读的就是它，
    /// 所以这里不能只写不读。</summary>
    private long _lastPopTimestamp = UtilAll.CurrentTimeMillis();

    /// <summary>Java <c>getLastPopTimestamp()</c>。</summary>
    public long LastPopTimestamp => Interlocked.Read(ref _lastPopTimestamp);

    /// <summary>Java <c>setLastPopTimestamp(long)</c>。</summary>
    public void Touch(long timestampMillis)
    {
        Interlocked.Exchange(ref _lastPopTimestamp, timestampMillis);
    }

    public void IncFoundMsg(int n)
    {
        lock (_lock) { _waitAckCounter += n; }
    }

    /// <summary>Java 传的是负数（decFoundMsg(-msgs.size())），这里按"减多少"理解。</summary>
    public void DecFoundMsg(int n)
    {
        lock (_lock) { _waitAckCounter += n; }
    }

    public int Ack()
    {
        lock (_lock) { return --_waitAckCounter; }
    }

    public int WaitAckCount()
    {
        lock (_lock) { return _waitAckCounter; }
    }

    public bool IsDropped() => _dropped;

    public void SetDropped(bool v) => _dropped = v;
}

/// <summary>从 POP_CK 解出的 ack / 延长不可见时间目标。</summary>
public sealed class PopCkTarget
{
    /// <summary>getRealTopic 按 retryFlag 还原后的真实 topic。</summary>
    public string Topic { get; set; } = string.Empty;

    /// <summary>CK 第 6 段。</summary>
    public string BrokerName { get; set; } = string.Empty;

    /// <summary>CK 第 7 段。</summary>
    public int QueueId { get; set; }

    /// <summary>CK 第 8 段 = consumeQueue offset（不是 commitlog offset）。</summary>
    public long Offset { get; set; }

    /// <summary>原样回传的 CK 串。</summary>
    public string ExtraInfo { get; set; } = string.Empty;
}

public sealed class DefaultMQPushConsumer
{
    // ---------------- 配置 ----------------
    public string ConsumerGroup { get; private set; }

    private string _namespace = string.Empty;

    // 命名空间（对应 Java DefaultMQPushConsumer.setNamespace）：非空时把 topic / group
    // 套上 "ns%" 前缀再与 broker 交互（对齐 Java start() 里对 consumerGroup 的包装）。
    public string Namespace
    {
        get => _namespace;
        set => _namespace = value ?? string.Empty;
    }

    // ---------------- unitName / unitMode / enableStreamRequestType ----------------
    // 对应 Java ClientConfig 的三个同名开关。⚠ 必须在 Start() 之前设置：unitName / @STREAM
    // 决定 clientId 形状，stream 决定请求钩子链（ReqT 要进 ACL 签名内容）。
    /// <summary>单元名：进 clientId 的 <c>@&lt;unitName&gt;</c> 段，也拼进动态取址 URL。</summary>
    public string UnitName
    {
        get => _unitName;
        set => _unitName = value ?? string.Empty;
    }

    /// <summary>
    /// 对应 Java <c>ClientConfig#isUnitMode()</c>。推送消费者有三处落点：
    /// 消息过滤上下文（DefaultMQPushConsumerImpl:640）、心跳里的
    /// <c>ConsumerData.unitMode</c>（MQClientInstance:1039，broker 据此给 %RETRY% topic
    /// 打 UNIT_SUB 标记）、回投请求头（见 SendMessageBack 处的说明）。
    /// </summary>
    public bool UnitMode
    {
        get => _unitMode;
        set => _unitMode = value;
    }

    /// <summary>
    /// true 时每笔请求带 <c>ReqT=0</c>、clientId 末尾多一段 <c>@STREAM</c>。
    /// Java 的推送消费者默认关（只有 pull / lite 消费者在构造里置真）。
    /// </summary>
    public bool EnableStreamRequestType
    {
        get => _enableStreamRequestType;
        set => _enableStreamRequestType = value;
    }

    /// <summary>
    /// Java <c>ClientConfig#pollNameServerInterval</c>（:58，默认 30000ms）：在用 topic 的
    /// 路由刷新周期，Start() 时透传给 MQClientInstance（之后改不重排已启动的周期任务）。
    /// </summary>
    public int PollNameServerIntervalMillis
    {
        get => _pollNameServerIntervalMillis;
        set => _pollNameServerIntervalMillis = value;
    }

    /// <summary>
    /// Java <c>ClientConfig#persistConsumerOffsetInterval</c>（:66，默认 5000ms）：
    /// 后台位点落盘周期。首个落盘在 10s 的 initialDelay 处（Java startScheduledTask:417-423），
    /// Shutdown() 的收尾落盘与该值无关（调大只推迟时机，不丢位点）。
    /// </summary>
    public int PersistConsumerOffsetIntervalMillis
    {
        get => _persistConsumerOffsetIntervalMillis;
        set => _persistConsumerOffsetIntervalMillis = value;
    }

    // ---------------- ACL 鉴权（对应 Java DefaultMQPushConsumer(group, rpcHook)）----------------
    // 必须在 Start() 之前调用：钩子在 Start() 里绑定到 MQClientInstance。
    public void SetRpcHook(IRpcHook hook) => _rpcHook = hook;

    /// <summary>便捷入口：用 accessKey/secretKey（可选 securityToken）构造 AclClientRPCHook。</summary>
    public void SetCredentials(string accessKey, string secretKey, string securityToken = "")
        => _rpcHook = new AclClientRPCHook(new SessionCredentials(accessKey, secretKey, securityToken));

    // ---------------- 消息轨迹配置（对应 Java DefaultMQPushConsumer 的 enableMsgTrace / customizedTraceTopic）----------------
    // 开启后 Start() 会建 AsyncTraceDispatcher（Type=Consume）并注册 ConsumeMessageTraceHook。
    public bool EnableTrace
    {
        get => _enableTrace;
        set => _enableTrace = value;
    }

    /// <summary>消费侧的命名入口（对应 Java setEnableMsgTrace），与 <see cref="EnableTrace"/> 等价。</summary>
    public void SetEnableMsgTrace(bool enable) => _enableTrace = enable;

    /// <summary>自定义轨迹 topic（Java customizedTraceTopic）；空则回落系统默认 RMQ_SYS_TRACE_TOPIC。</summary>
    public string TraceTopic
    {
        get => _traceTopic;
        set => _traceTopic = string.IsNullOrEmpty(value) ? MixAll.TraceTopic : value;
    }

    public int TraceMsgBatchNum
    {
        get => _traceMsgBatchNum;
        set => _traceMsgBatchNum = value;
    }

    /// <summary>消费超时（分钟，Java consumeTimeout 默认 15）——决定轨迹 SubAfter 的 contextCode 是否为 TIME_OUT。</summary>
    public int ConsumeTimeout { get; set; } = 15;

    /// <summary>注册消费钩子（对应 Java registerConsumeMessageHook）。</summary>
    public void RegisterConsumeMessageHook(IConsumeMessageHook hook)
    {
        if (hook is not null)
        {
            _consumeMessageHooks.Add(hook);
        }
    }

    public bool HasConsumeMessageHook() => _consumeMessageHooks.Count > 0;

    /// <summary>注册投递前过滤钩子（对应 Java registerFilterMessageHook / hasFilterMessageHook）。
    /// 钩子摘掉的消息：拉取路径静默跳过（位点照常推进），POP 路径立刻 ack。</summary>
    public void RegisterFilterMessageHook(IFilterMessageHook hook)
    {
        if (hook is not null)
        {
            _filterMessageHooks.Add(hook);
        }
    }

    public bool HasFilterMessageHook() => _filterMessageHooks.Count > 0;

    public int FilterMessageHookCount() => _filterMessageHooks.Count;

    /// <summary>已丢弃的条数（拉取 + POP 合计），供联调脚本与单测观测。</summary>
    public long FilteredMessageCount() => Interlocked.Read(ref _filteredMessageCount);

    /// <summary>依次执行过滤钩子，<b>异常一律吞掉</b>并记 error
    /// （Java PullAPIWrapper.executeHook:171-178）。
    /// 与 CheckForbiddenHook 相反：过滤钩子挂了不能影响消费。</summary>
    public void ExecuteFilterMessageHook(FilterMessageContext context)
    {
        foreach (IFilterMessageHook hook in _filterMessageHooks)
        {
            try
            {
                hook.FilterMessage(context);
            }
            catch (Exception e)
            {
                ClientLog.Error("execute hook error. hookName=" + hook.HookName() + ": " + e.Message);
            }
        }
    }

    /// <summary>拉取 / POP 两条路径共用的投递前过滤：
    /// ① 客户端二次 tag 过滤（broker 侧按 codeSet 哈希过滤有碰撞误放）
    /// ② IFilterMessageHook（可改写 MsgList）。
    /// <para>① 对齐 Java PullAPIWrapper.processPullResult:113-122 与 processPopResult:625-635：
    /// broker 侧是按 tag 的<b>哈希（codeSet）</b>过滤的，存在哈希碰撞误放，客户端要再按
    /// 字符串核一遍。守卫 <c>!tagsSet.isEmpty()</c> 意味着订阅 "*"（SUB_ALL）时不过滤 ——
    /// 所以 FilterAPI.BuildSubscriptionData 对 SUB_ALL 必须留空。</para>
    /// <para>sub 为 null 时只跑 ②。</para></summary>
    public List<MessageExt> FilterMessagesForDelivery(MessageQueue mq, SubscriptionData? sub,
        List<MessageExt> msgs)
    {
        List<MessageExt> result = msgs;
        if (result.Count == 0)
        {
            return result;
        }

        if (sub is not null && sub.TagsSet.Count > 0 && !sub.ClassFilterMode)
        {
            var kept = new List<MessageExt>(result.Count);
            foreach (MessageExt m in result)
            {
                string? tags = m.Tags;
                if (!string.IsNullOrEmpty(tags) && sub.TagsSet.Contains(tags!))
                {
                    kept.Add(m);
                }
            }

            result = kept;
        }

        if (_filterMessageHooks.Count > 0 && result.Count > 0)
        {
            var context = new FilterMessageContext(ConsumerGroup, result, mq)
            {
                // Java DefaultMQPushConsumerImpl:640：filterMessageContext
                //   .setUnitMode(this.defaultMQPushConsumer.isUnitMode())
                // —— 钩子据此判断是否单元化流量。
                UnitMode = _unitMode,
            };
            ExecuteFilterMessageHook(context);
            result = context.MsgList;
        }

        return result;
    }

    /// <summary>求 original \ kept 的差集（POP 路径要给被摘掉的消息补 ack）。
    /// Java 用 List.contains 的引用同一性；C# 里 List 拷贝后无同一性，改按 MsgId 求差（语义等价）。</summary>
    public static List<MessageExt> DroppedMessages(List<MessageExt> original, List<MessageExt> kept)
    {
        var dropped = new List<MessageExt>();
        if (kept.Count >= original.Count)
        {
            return dropped;
        }

        var keptIds = new HashSet<string>(StringComparer.Ordinal);
        foreach (MessageExt m in kept)
        {
            keptIds.Add(m.MsgId);
        }

        foreach (MessageExt m in original)
        {
            if (!keptIds.Contains(m.MsgId))
            {
                dropped.Add(m);
            }
        }

        return dropped;
    }

    // ---------------- POP 模式（5.x 轻量消费）----------------
    // 关掉时完全走原来的 pull 长轮询路径，行为与改动前一致。
    public bool PopMode { get; set; }

    /// <summary>TLS（对应 Java tls.enable；缺省读 env ROCKETMQ_TLS_ENABLE）。</summary>
    /// <remarks>
    /// 初值必须在这里取 env：实例化 <c>MQClientInstance</c> 时传的是 <c>bool</c>（不是
    /// <c>bool?</c>），默认的 <c>false</c> 会盖掉 RemotingClient 内部的 env 兜底。
    /// </remarks>
    public bool TlsEnable
    {
        get => _tlsEnable;
        set => _tlsEnable = value;
    }

    private bool _tlsEnable = MQClientInstance.TlsEnabledFromEnv();

    /// <summary>弹出后对其它实例不可见的时长（Java popInvisibleTime 默认 60000）。</summary>
    public long PopInvisibleTime { get; set; } = 60000;

    /// <summary>单次 POP 的最大条数（Java popBatchNums 默认 32；broker 侧 >32 会回 INVALID_PARAMETER）。</summary>
    public int PopBatchNums { get; set; } = 32;

    /// <summary>本队列"已弹未 ack"计数器上限，超过就暂停 POP（Java popThresholdForQueue 默认 96）。</summary>
    public int PopThresholdForQueue { get; set; } = 96;

    /// <summary>
    /// POP 长轮询挂起时长。0 = 短轮询（broker 立即返回或 NO_NEW_MSG）。
    /// ⚠ 非 0 时请求超时必须 &gt; 它，否则客户端先超时。
    /// </summary>
    public int PopPollTimeMillis { get; set; } = 15000;

    public int PopTimeoutMillis { get; set; } = 25000;

    /// <summary>消费失败时延长不可见时间的梯度（秒）。</summary>
    public List<int> PopDelayLevel { get; set; } = new(PopDelayLevelDefaults.Table);

    // Java DefaultMQPushConsumerImpl.MIN/MAX_POP_INVISIBLE_TIME：超出范围一律回落到 60000
    // Java ConsumeInitMode
    public const int ConsumeInitModeMin = 0;
    public const int ConsumeInitModeMax = 1;

    public const long MinPopInvisibleTime = 5000;
    public const long MaxPopInvisibleTime = 300000;

    // ACL 钩子，Start() 时绑定到 MQClientInstance 的传输层
    private IRpcHook? _rpcHook;

    // ClientConfig 的三个单元化/stream 开关。Java 的 DefaultMQPushConsumer 既不置
    // unitMode 也不置 enableStreamRequestType（只有 pull / lite 构造里置 true），默认全关。
    private string _unitName = string.Empty;
    private bool _unitMode;
    private bool _enableStreamRequestType;

    // ClientConfig 的两个周期（Java :58 / :66 的默认值，Start() 时定型）
    private int _pollNameServerIntervalMillis = 30000;
    private int _persistConsumerOffsetIntervalMillis = 5000;

    // ---------------- 消息轨迹（消费侧）----------------
    private bool _enableTrace;
    private string _traceTopic = MixAll.TraceTopic;
    private int _traceMsgBatchNum = 10;
    private readonly List<IConsumeMessageHook> _consumeMessageHooks = new();
    private readonly List<IFilterMessageHook> _filterMessageHooks = new();
    private long _filteredMessageCount;
    private AsyncTraceDispatcher? _traceDispatcher;

    private string _instanceName = MixAll.DefaultInstanceName;
    private string _clientId = string.Empty;
    private string _messageModel = RocketMQ.Remoting.Protocol.MessageModel.Clustering;
    // 队列分配策略，对应 Java DefaultMQPushConsumer.allocateMessageQueueStrategy
    // （构造参数默认 new AllocateMessageQueueAveragely()）。
    private IAllocateMessageQueueStrategy? _allocateMessageQueueStrategy = new AllocateMessageQueueAveragely();
    private string _consumeFromWhere = RocketMQ.Remoting.Protocol.ConsumeFromWhere.ConsumeFromLastOffset;

    // ---- 消费线程池（对齐 Java DefaultMQPushConsumer 的 consumeThreadMin/Max）----
    // Java 5.x 的默认值是 min=20（:162）**且** max=20（:169）—— 两侧同为 20
    // （4.x 时代才是 min=20/max=64，本端口早年照抄的是 4.x 那一组）。
    // 本实现的**拉取**路径是"每队列一个拉取线程 + 单分发线程"，
    // 只有 **POP** 路径用真正的线程池（对应 Java ConsumeMessagePopConcurrentlyService），
    // 因此 CorePoolSize 直接决定 POP 的消费并发度（Java 无界队列下 max 实际用不到）；
    // 默认配置下 UpdateCorePoolSize 只能往**下**调（守卫 n < max = 20）。
    private int _consumeThreadMin = 20;
    private int _consumeThreadMax = 20;
    // Java adjustThreadPoolNumsThreshold 默认 100000（自动弹性阈值；上游 inc/dec 是空实现）
    private long _adjustThreadPoolNumsThreshold = 100000;
    // 声明式 core pool size（Java setCorePoolSize 的等价物），默认 = consumeThreadMin
    private int _corePoolSize = 20;
    // key -> ProcessQueue.msgAccCnt（最近一次拉取算出的积压条数）
    private readonly Dictionary<string, long> _msgAccCntTable = new(StringComparer.Ordinal);
    // POP 消费执行器（Start 且 PopMode 时创建；Shutdown 时释放）
    private ConsumeExecutor? _popConsumeExecutor;

    private int _pullBatchSize = 32;
    private int _pullBatchSizeInBytes = 256 * 1024;
    private int _consumeMessageBatchMaxSize = 1;
    private int _pullTimeoutMillis = 30000;
    private int _pullSuspendTimeoutMillis = 15000;
    private int _suspendCurrentQueueTimeMillis = 1000;
    private int _maxReconsumeTimes = -1;
    private int _pullIntervalMillis;
    private bool _heartbeatEnabled = true;
    private int _heartbeatIntervalMillis = 30000;

    private List<string> _nameServerAddrs = new();
    private readonly object _lock = new();
    private readonly Dictionary<string, SubscriptionData> _subscriptionData = new(StringComparer.Ordinal);
    private IMessageListener? _messageListener;
    private readonly Dictionary<string, long> _offsetTable = new(StringComparer.Ordinal);
    // ---- 对齐 Java 的消费进度 / 缓冲 / 锁状态 ----
    // _offsetTable 是"拉取游标"（nextBeginOffset）；_consumeOffsetTable 是"已消费位点"
    // （周期持久化到 broker 的对象）；_pending 是已拉未消费缓冲（Java ProcessQueue）。
    private readonly Dictionary<string, long> _consumeOffsetTable = new(StringComparer.Ordinal);
    private readonly Dictionary<string, MessageQueue> _mqMap = new(StringComparer.Ordinal);
    private readonly Dictionary<string, Queue<MessageExt>> _pending = new(StringComparer.Ordinal);
    // Start() 时刻（307 应答 PROP_CONSUMER_START_TIMESTAMP，对应 Java consumerStartTimestamp）
    private long _startTimestamp;
    // 顺序消费：broker LOCK_BATCH_MQ 确认锁定成功的队列 key 集
    private readonly HashSet<string> _lockOk = new(StringComparer.Ordinal);
    private int _pullThresholdForQueue = 1000;
    // Java ProcessQueue 余下四个阈值：字节闸门单位是 **MiB**（<=0 关闭）；跨度是 pending 里
    // queueOffset 的 max-min，**严格大于**才算命中；topic 级两条默认 -1（关闭），统计的是
    // 本实例该 topic **所有**队列的累计缓冲。
    private int _pullThresholdSizeForQueue = 100;
    private long _consumeConcurrentlyMaxSpan = 2000;
    private int _pullThresholdForTopic = -1;
    private int _pullThresholdSizeForTopic = -1;
    private long _flowControlTriggered;
    private Thread? _dispatchThread;
    private Thread? _persistThread;
    private Thread? _lockThread;
    private Thread? _rebalanceThread;
    private readonly Dictionary<string, Thread> _pullThreads = new(StringComparer.Ordinal);
    // 队列 key -> 最近一次**发起**拉取/弹出的时刻（毫秒）。对齐 Java
    // ProcessQueue.lastPullTimestamp / PopProcessQueue.lastPopTimestamp：rebalance 用它判
    // 这条循环是不是停摆了（PullMaxIdleTime），307 也把它如实报出去。
    private readonly Dictionary<string, long> _lastPullAt = new(StringComparer.Ordinal);
    /// <summary>Java <c>ProcessQueue.PULL_MAX_IDLE_TIME</c>（系统属性
    /// <c>rocketmq.client.pull.pullMaxIdleTime</c>，默认 120000ms）：超过它就判停摆，
    /// 用严格 <c>&gt;</c>。</summary>
    private const long PullMaxIdleTime = 120000;
    // 队列 key -> PopProcessQueue（已弹未 ack 计数 + 是否已被 rebalance 撤销）
    private readonly Dictionary<string, PopProcessQueue> _popQueues = new(StringComparer.Ordinal);

    // ---- 真实 rebalance（对齐 Java RebalanceImpl）----
    // _assigned 是"当前分给本实例的队列集"（Java ProcessQueueTable 的键集），
    // 由 DoRebalance() 按分配策略计算；_rebalanceNow 用于
    // NOTIFY_CONSUMER_IDS_CHANGED(40) 触发的即时 rebalance（Java rebalanceImmediately）。
    private List<MessageQueue> _assigned = new();
    private readonly ManualResetEventSlim _rebalanceNow = new(false);
    // 撤销标记（对齐 Java ProcessQueue.isDropped）：rebalance 把某队列从本实例分配中去掉后，
    // 置 _dropped，pull 线程长轮询返回后看到该标记即丢弃批次并退出（不再为该队列服务）。
    private readonly HashSet<string> _dropped = new(StringComparer.Ordinal);

    private MQClientInstance? _mqClient;
    private volatile bool _started;
    private volatile bool _stop;
    private readonly ManualResetEventSlim _stopEvent = new(false);
    private long _consumedCount;
    private long _heartbeatCount;
    private long _lastHeartbeatMs;

    public DefaultMQPushConsumer(string consumerGroup = MixAll.DefaultConsumerGroup)
    {
        if (UtilAll.IsBlank(consumerGroup))
        {
            throw new MQClientException("consumerGroup is empty");
        }

        ConsumerGroup = consumerGroup;
    }

    // ---------------- 配置 setter ----------------
    public void SetNamesrvAddr(string addr)
    {
        _nameServerAddrs = SplitSemicolon(addr);
    }

    public void SetNameServerAddresses(IReadOnlyList<string> addrs)
    {
        _nameServerAddrs = new List<string>(addrs);
    }

    public string InstanceName
    {
        get => _instanceName;
        set => _instanceName = value;
    }

    public string MessageModel
    {
        get => _messageModel;
        set => _messageModel = value;
    }

    /// <summary>
    /// 队列分配策略（对应 Java DefaultMQPushConsumer.get/setAllocateMessageQueueStrategy）。
    /// 与 Java 同款：setter 不校验，置 null 由 Start() 的 checkConfig 拒绝
    /// （Java DefaultMQPushConsumerImpl.checkConfig:1067 "allocateMessageQueueStrategy is null"）。
    /// </summary>
    public IAllocateMessageQueueStrategy? AllocateMessageQueueStrategy
    {
        get => _allocateMessageQueueStrategy;
        set => _allocateMessageQueueStrategy = value;
    }

    public string ConsumeFromWhere
    {
        get => _consumeFromWhere;
        set => _consumeFromWhere = value;
    }

    /// <summary>便捷方法：Min 与 Max 一起设（Java 4.x setConsumeThreadNums 的语义）。
    /// Java 5.x 已拆成 SetConsumeThreadMin/Max，本方法保留是为了兼容既有调用点。</summary>
    public void SetConsumeThreadNums(int n)
    {
        int v = Math.Max(1, n);
        _consumeThreadMin = v;
        _consumeThreadMax = v;
        _corePoolSize = v;
        _popConsumeExecutor?.SetCorePoolSize(v);
    }

    // ---- 消费线程弹性（对应 Java AbstractConsumeMessageService）----

    public int ConsumeThreadMin => _consumeThreadMin;
    public int ConsumeThreadMax => _consumeThreadMax;
    public long AdjustThreadPoolNumsThreshold => _adjustThreadPoolNumsThreshold;

    public void SetConsumeThreadMin(int n)
    {
        _consumeThreadMin = Math.Max(1, n);
        _corePoolSize = _consumeThreadMin;
        _popConsumeExecutor?.SetCorePoolSize(_corePoolSize);
    }

    public void SetConsumeThreadMax(int n)
    {
        _consumeThreadMax = Math.Max(1, n);
    }

    public void SetAdjustThreadPoolNumsThreshold(long v) => _adjustThreadPoolNumsThreshold = v;

    /// <summary>运行时调整消费并发度（对应 Java updateCorePoolSize → setCorePoolSize）。
    /// <para>Java 的守卫逐条照抄（AbstractConsumeMessageService:63-71）：
    /// ownsConsumeExecutor &amp;&amp; corePoolSize &gt; 0
    /// &amp;&amp; corePoolSize &lt;= Short.MAX_VALUE(32767)
    /// &amp;&amp; corePoolSize &lt; consumeThreadMax。任一条不满足就**静默忽略**
    /// （Java 也是静默 return，不抛异常）。返回值只用于单测断言"是否真的生效"。</para></summary>
    public bool UpdateCorePoolSize(int corePoolSize)
    {
        if (corePoolSize <= 0 || corePoolSize > 32767 || corePoolSize >= _consumeThreadMax)
        {
            return false;
        }
        _corePoolSize = corePoolSize;
        _popConsumeExecutor?.SetCorePoolSize(_corePoolSize);
        return true;
    }

    /// <summary>Java getCorePoolSize（本实现恒为自建执行器，故不会返回 -1）。</summary>
    public int GetCorePoolSize() => _popConsumeExecutor?.GetCorePoolSize() ?? _corePoolSize;

    /// <summary>Java DefaultMQPushConsumerImpl.computeAccumulationTotal：所有队列 msgAccCnt 之和。</summary>
    public long ComputeAccumulationTotal()
    {
        lock (_lock)
        {
            long total = 0;
            foreach (long v in _msgAccCntTable.Values) total += v;
            return total;
        }
    }

    /// <summary>单队列（不传 key 则求和）的 ProcessQueue.msgAccCnt。</summary>
    public long MsgAccCnt(string? key = null)
    {
        lock (_lock)
        {
            if (string.IsNullOrEmpty(key))
            {
                long total = 0;
                foreach (long v in _msgAccCntTable.Values) total += v;
                return total;
            }
            return _msgAccCntTable.TryGetValue(key!, out long acc) ? acc : 0;
        }
    }

    /// <summary>按 Java ProcessQueue.putMessage 的规则更新 msgAccCnt
    /// （= 最后一条消息的 MAX_OFFSET 属性 - 它的 queueOffset，&gt; 0 才更新）。</summary>
    public void UpdateMsgAccCnt(string key, IReadOnlyList<MessageExt> msgs)
    {
        lock (_lock)
        {
            UpdateMsgAccCntLocked(key, msgs);
        }
    }

    private void UpdateMsgAccCntLocked(string key, IReadOnlyList<MessageExt> msgs)
    {
        // Java ProcessQueue.java:148-158：
        //   long accTotal = Long.parseLong(msg.getProperty(MAX_OFFSET)) - msg.getQueueOffset();
        //   if (accTotal > 0) this.msgAccCnt = accTotal;
        // 取**本批最后一条**；属性缺失/非数字/非正一律不更新（Java 里 parse 失败会抛，
        // 但 broker 恒会带上该属性，这里做容错以免脏数据打断拉取线程）。
        if (msgs.Count == 0) return;
        MessageExt last = msgs[msgs.Count - 1];
        string? maxOffset = last.GetProperty(MessageConst.PropertyMaxOffset);
        if (string.IsNullOrEmpty(maxOffset)) return;
        if (!long.TryParse(maxOffset, NumberStyles.Integer, CultureInfo.InvariantCulture, out long parsed))
        {
            return;
        }
        long accTotal = parsed - last.QueueOffset;
        if (accTotal > 0) _msgAccCntTable[key] = accTotal;
    }

    /// <summary>Java DefaultMQPushConsumerImpl.adjustThreadPool（每分钟调度一次）。
    /// <para>⚠ **在 Java 5.5.1 这是 no-op，我们照抄 no-op**：它调用的
    /// consumeMessageService.incCorePoolSize()/decCorePoolSize() 在
    /// AbstractConsumeMessageService:70-75 是**空方法体**。这里保留阈值比较仅为让
    /// msgAccCnt / 阈值配置可观测；**不要"修好"它** —— 真正生效的是显式的
    /// UpdateCorePoolSize()。</para></summary>
    public void AdjustThreadPool()
    {
        long accTotal = ComputeAccumulationTotal();
        long incThreshold = (long)(_adjustThreadPoolNumsThreshold * 1.0);
        long decThreshold = (long)(_adjustThreadPoolNumsThreshold * 0.8);
        if (accTotal >= incThreshold)
        {
            ClientLog.Debug($"adjustThreadPool: acc={accTotal} >= incThreshold={incThreshold} (inc is a no-op upstream)");
        }
        if (accTotal < decThreshold)
        {
            ClientLog.Debug($"adjustThreadPool: acc={accTotal} < decThreshold={decThreshold} (dec is a no-op upstream)");
        }
    }

    /// <summary>观测：当前 POP 消费执行器的存活线程数 / 排队任务数（未建执行器时为 0）。</summary>
    public int ConsumeExecutorWorkers() => _popConsumeExecutor?.WorkerCount() ?? 0;
    public int ConsumeExecutorQueued() => _popConsumeExecutor?.QueuedCount() ?? 0;
    // 仅供单测：直接注入一个执行器，避免为了测线程弹性去起集群
    // （测试工程与本程序集没有 InternalsVisibleTo 关系，故用 public，勿用于业务代码）
    public ConsumeExecutor? PopConsumeExecutorForTest
    {
        get => _popConsumeExecutor;
        set => _popConsumeExecutor = value;
    }

    public void SetMessageListener(IMessageListener listener)
    {
        _messageListener = listener;
    }

    public int PullBatchSize
    {
        get => _pullBatchSize;
        set => _pullBatchSize = value;
    }

    public int PullBatchSizeInBytes
    {
        get => _pullBatchSizeInBytes;
        set => _pullBatchSizeInBytes = value;
    }

    public int ConsumeMessageBatchMaxSize
    {
        get => _consumeMessageBatchMaxSize;
        set => _consumeMessageBatchMaxSize = Math.Max(1, value);
    }

    public int PullTimeoutMillis
    {
        get => _pullTimeoutMillis;
        set => _pullTimeoutMillis = value;
    }

    public int PullSuspendTimeoutMillis
    {
        get => _pullSuspendTimeoutMillis;
        set => _pullSuspendTimeoutMillis = value;
    }

    public int SuspendCurrentQueueTimeMillis
    {
        get => _suspendCurrentQueueTimeMillis;
        set => _suspendCurrentQueueTimeMillis = value;
    }

    public int MaxReconsumeTimes
    {
        get => _maxReconsumeTimes;
        set => _maxReconsumeTimes = value;
    }

    public int PullIntervalMillis
    {
        get => _pullIntervalMillis;
        set => _pullIntervalMillis = value;
    }

    public bool HeartbeatEnabled
    {
        get => _heartbeatEnabled;
        set => _heartbeatEnabled = value;
    }

    // 每队列"已拉未消费"阈值，超过则暂停该队列拉取（Java pullThresholdForQueue，默认 1000）
    public int PullThresholdForQueue
    {
        get => _pullThresholdForQueue;
        set => _pullThresholdForQueue = value;
    }

    /// <summary>Java <c>pullThresholdSizeForQueue</c>（默认 100）：本队列「已拉未消费」
    /// 字节阈值，单位 <b>MiB</b>；&lt;=0 表示关闭这条闸门。</summary>
    public int PullThresholdSizeForQueue
    {
        get => _pullThresholdSizeForQueue;
        set => _pullThresholdSizeForQueue = value;
    }

    /// <summary>Java <c>consumeMessageMaxSpan</c>（默认 2000）：本队列「已拉未消费」消息
    /// queueOffset 的跨度阈值，严格大于才命中（防止队首一条卡住、后面无限堆）；&lt;=0 关闭。</summary>
    public long ConsumeConcurrentlyMaxSpan
    {
        get => _consumeConcurrentlyMaxSpan;
        set => _consumeConcurrentlyMaxSpan = value;
    }

    /// <summary>Java <c>pullThresholdForTopic</c>（默认 -1 关闭）：本实例同 topic <b>所有</b>队列
    /// 累计「已拉未消费」条数阈值。注意 Java 在 RebalancePushImpl 里会把 topic 阈值除以队列数
    /// 折算到队列级，本移植直接拿累计值比对（与 Python/Rust/C++ 同形）。</summary>
    public int PullThresholdForTopic
    {
        get => _pullThresholdForTopic;
        set => _pullThresholdForTopic = value;
    }

    /// <summary>Java <c>pullThresholdSizeForTopic</c>（默认 -1 关闭）：同 topic 累计字节阈值，
    /// 单位 <b>MiB</b>。这条闸门只看本属性，<b>不复用</b>队列级那道开关。</summary>
    public int PullThresholdSizeForTopic
    {
        get => _pullThresholdSizeForTopic;
        set => _pullThresholdSizeForTopic = value;
    }

    // 流控触发次数（用于验证流控能力）
    public long FlowControlTriggered => Interlocked.Read(ref _flowControlTriggered);

    public int HeartbeatIntervalMillis
    {
        get => _heartbeatIntervalMillis;
        set => _heartbeatIntervalMillis = value;
    }

    public string ClientId => _clientId;

    public bool IsStarted => _started;

    // 已成功消费的消息总数（用于测试/监控）
    public long ConsumedCount => _consumedCount;

    // broker 心跳成功次数（用于验证心跳能力）
    public long HeartbeatCount => _heartbeatCount;

    // ---------------- 订阅 ----------------
    public void Subscribe(string topic, string subExpression = "*")
    {
        if (_started)
        {
            throw new MQClientException("consumer already started, cannot change configuration");
        }

        string realTopic = NamespaceUtil.WrapNamespace(_namespace, topic);
        SubscriptionData sub = FilterAPI.BuildSubscriptionData(realTopic, subExpression);
        lock (_lock)
        {
            _subscriptionData[realTopic] = sub;
        }
    }

    public void Subscribe(string topic, MessageSelector selector)
    {
        if (_started)
        {
            throw new MQClientException("consumer already started, cannot change configuration");
        }

        string realTopic = NamespaceUtil.WrapNamespace(_namespace, topic);
        var sub = new SubscriptionData(realTopic, selector.Expression)
        {
            ExpressionType = selector.Type,
        };
        if (selector.Type == ExpressionType.TAG)
        {
            SubscriptionData built = FilterAPI.BuildSubscriptionData(realTopic, selector.Expression);
            sub.TagsSet = built.TagsSet;
        }

        lock (_lock)
        {
            _subscriptionData[realTopic] = sub;
        }
    }

    public void Unsubscribe(string topic)
    {
        lock (_lock)
        {
            _subscriptionData.Remove(topic);
        }
    }

    public List<string> SubscribedTopics()
    {
        lock (_lock)
        {
            var outList = new List<string>(_subscriptionData.Count);
            foreach (var kv in _subscriptionData)
            {
                outList.Add(kv.Key);
            }

            return outList;
        }
    }

    /// <summary>Java <c>DefaultMQPushConsumerImpl#checkConfig</c> 的数值段（:1099-1209）。
    /// <para>逐条照抄 Java 的<b>顺序、区间和文案</b>（Java 每条都拼
    /// <c>FAQUrl.suggestTodo(CLIENT_PARAMETER_CHECK_URL)</c>，本仓库按约定不带后缀）。
    /// 比较一律 <c>&lt; lo || &gt; hi</c>；<c>PopBatchNums</c> 跟随 Java 的字面写法
    /// <c>&lt;= 0</c>。四语言（Python 参考实现 / C++ / Rust / 本移植）文案逐字一致，
    /// 改一处要同时改四处，否则跨语言用例的断言会分叉。</para>
    /// <para>为什么要在 <see cref="Start"/> 拦：这些值直接决定缓冲水位与线程池规模。
    /// 配成 0 或 <c>int.MaxValue</c> 而没有这道闸门时，要么每轮拉取都被"阈值 &lt;= 0"
    /// 判成积压而<b>永久停拉</b>（消费端静默收不到消息），要么整数溢出把流控判断整个
    /// 绕开。等 broker 报错已经晚了几拍，且错误只落在日志里。</para>
    /// <para><c>PullThresholdForTopic</c> / <c>PullThresholdSizeForTopic</c> 的 -1 是
    /// Java 的"关闭这条闸门"哨兵，不在区间内也不报错；其余字段没有这个豁免（写成 -1
    /// 在 Java 里是启动错误，不是"不限制"）。</para>
    /// <para>public 只为离线可测：正向用例（边界值必须放行）走不到 <see cref="Start"/>
    /// 之后，测试工程与本程序集没有 InternalsVisibleTo 关系。业务代码请直接 <c>Start()</c>。</para>
    /// <para>已知与 Java 的差异：<c>SetConsumeThreadMin/Max</c> 与
    /// <c>ConsumeMessageBatchMaxSize</c> 的 setter 带 <c>Math.Max(1, n)</c> 兜底
    /// （Java 的 setter 是裸赋值），于是这三条的**下界**越界从公开 API 走不到；
    /// 上界与相对检查（min &gt; max）仍然可达，闸门本身照抄全量。</para>
    /// </summary>
    public void CheckConfigRanges()
    {
        // consumeThreadMin
        if (ConsumeThreadMin < 1 || ConsumeThreadMin > 1000)
        {
            throw new MQClientException("consumeThreadMin Out of range [1, 1000]");
        }
        // consumeThreadMax
        if (ConsumeThreadMax < 1 || ConsumeThreadMax > 1000)
        {
            throw new MQClientException("consumeThreadMax Out of range [1, 1000]");
        }
        // consumeThreadMin can't be larger than consumeThreadMax
        if (ConsumeThreadMin > ConsumeThreadMax)
        {
            throw new MQClientException("consumeThreadMin (" + ConsumeThreadMin
                                        + ") is larger than consumeThreadMax ("
                                        + ConsumeThreadMax + ")");
        }
        // consumeConcurrentlyMaxSpan
        if (ConsumeConcurrentlyMaxSpan < 1 || ConsumeConcurrentlyMaxSpan > 65535)
        {
            throw new MQClientException("consumeConcurrentlyMaxSpan Out of range [1, 65535]");
        }
        // pullThresholdForQueue
        if (PullThresholdForQueue < 1 || PullThresholdForQueue > 65535)
        {
            throw new MQClientException("pullThresholdForQueue Out of range [1, 65535]");
        }
        // pullThresholdForTopic
        if (PullThresholdForTopic != -1)
        {
            if (PullThresholdForTopic < 1 || PullThresholdForTopic > 6553500)
            {
                throw new MQClientException("pullThresholdForTopic Out of range [1, 6553500]");
            }
        }
        // pullThresholdSizeForQueue
        if (PullThresholdSizeForQueue < 1 || PullThresholdSizeForQueue > 1024)
        {
            throw new MQClientException("pullThresholdSizeForQueue Out of range [1, 1024]");
        }
        // pullThresholdSizeForTopic
        if (PullThresholdSizeForTopic != -1)
        {
            if (PullThresholdSizeForTopic < 1 || PullThresholdSizeForTopic > 102400)
            {
                throw new MQClientException("pullThresholdSizeForTopic Out of range [1, 102400]");
            }
        }
        // pullInterval：Java 的下界就是 0（不间隔），别照抄别条闸门的 1
        if (PullIntervalMillis < 0 || PullIntervalMillis > 65535)
        {
            throw new MQClientException("pullInterval Out of range [0, 65535]");
        }
        // consumeMessageBatchMaxSize
        if (ConsumeMessageBatchMaxSize < 1 || ConsumeMessageBatchMaxSize > 1024)
        {
            throw new MQClientException("consumeMessageBatchMaxSize Out of range [1, 1024]");
        }
        // pullBatchSize
        if (PullBatchSize < 1 || PullBatchSize > 1024)
        {
            throw new MQClientException("pullBatchSize Out of range [1, 1024]");
        }
        // popInvisibleTime：区间与 POP 循环里那条兜底用的是同一对常量
        if (PopInvisibleTime < MinPopInvisibleTime || PopInvisibleTime > MaxPopInvisibleTime)
        {
            throw new MQClientException("popInvisibleTime Out of range [" + MinPopInvisibleTime
                                        + ", " + MaxPopInvisibleTime + "]");
        }
        // popBatchNums（Java 写的就是 <= 0，不是 < 1）
        if (PopBatchNums <= 0 || PopBatchNums > 32)
        {
            throw new MQClientException("popBatchNums Out of range [1, 32]");
        }
    }

    // ---------------- 生命周期 ----------------
    public void Start()
    {
        {
            if (_started)
            {
                return;
            }

            // 对齐 Java DefaultMQPushConsumer.start()：把消费组套上命名空间（ns%group），
            // 之后所有面向 broker 的组名（心跳 / rebalance / 位点 / 锁 / 回投）都用包装后的值。
            if (_namespace.Length != 0)
            {
                ConsumerGroup = NamespaceUtil.WrapNamespace(_namespace, ConsumerGroup);
            }

            // 对应 Java DefaultMQPushConsumerImpl.checkConfig（:1026）：先 Validators.checkGroup
            // （blank / 120 长度 / 字符表），再挡 DEFAULT_CONSUMER —— 共用默认组会让
            // broker 侧的订阅关系判定把两组混在一起，回投与重平衡都错乱。
            // Java 把 checkConfig 排在最前，所以这里也领先于地址/订阅检查。
            Validators.CheckGroup(ConsumerGroup);
            if (ConsumerGroup == MixAll.DefaultConsumerGroup)
            {
                throw new MQClientException("consumerGroup can not equal " + MixAll.DefaultConsumerGroup
                                            + ", please specify another one.");
            }

            // 静态地址与动态取址（ROCKETMQ_NAMESRV_DOMAIN）二选一必须可用
            if (_nameServerAddrs.Count == 0 && !DefaultTopAddressing.IsConfigured())
            {
                throw new MQClientException("name server address is not set");
            }

            // 对应 Java DefaultMQPushConsumerImpl.checkConfig（:1067）：策略为 null 直接拒绝启动。
            if (_allocateMessageQueueStrategy is null)
            {
                throw new MQClientException("allocateMessageQueueStrategy is null");
            }

            if (_subscriptionData.Count == 0)
            {
                throw new MQClientException("subscription is not set, call subscribe() first");
            }

            if (_messageListener is null)
            {
                throw new MQClientException("message listener is not set");
            }

            // 对应 Java DefaultMQPushConsumerImpl.checkConfig 的数值段（:1099-1209）。
            // 排在所有 null 检查之后、建 MQClientInstance 之前：坏配置必须在实例化之前
            // 失败，否则起了后台线程/注册了 clientId 再抛错就留下半启动状态。
            // Java :1058 那条 consumeTimestamp 格式校验在这里是 no-op —— 本移植没有
            // 可配的 ConsumeTimestamp 属性，CONSUME_FROM_TIMESTAMP 一律取"30 分钟前"
            // （见 ComputePullOffsetFromWhere），没有可写坏的字符串可拒。这是一处已知
            // 与 Java 的差距（少一个可配项），不是这里漏了校验。
            CheckConfigRanges();

            // 对应 Java DefaultMQPushConsumerImpl.start()（:935）：**只有 CLUSTERING** 才把默认
            // instanceName 换成 <pid>#<nanoTime>；BROADCASTING 保留 "DEFAULT"，于是同机同组的
            // 广播消费者共用一个 clientId —— 那是 Java 故意留的共享语义，不能"修"掉。
            if (_messageModel == RocketMQ.Remoting.Protocol.MessageModel.Clustering)
            {
                _instanceName = ClientIds.ChangeInstanceNameToPID(_instanceName);
            }
            if (_clientId.Length == 0)
            {
                _clientId = ClientIds.Build(_instanceName, _unitName, _enableStreamRequestType);
            }

            // 集群模式自动订阅重试 topic（对齐 Java copySubscription → getRetryTopic）：
            // broker 回投的消息写到 %RETRY%group，客户端不订阅就收不到
            if (_messageModel != RocketMQ.Remoting.Protocol.MessageModel.Broadcasting)
            {
                string retryTopic = MixAll.GetRetryTopic(ConsumerGroup);
                if (!_subscriptionData.ContainsKey(retryTopic))
                {
                    _subscriptionData[retryTopic] =
                        FilterAPI.BuildSubscriptionData(retryTopic, "*");
                }
            }

            // 请求钩子（ACL 签名 / stream 的 ReqT）：绑定在 Start() **之前** ——
            // Java 的 rpcHook 随 MQClientAPIImpl 构造传入，实例第一笔报文就带着它。
            // 顺序由 RequestHooks.Compose 还原（stream 在 ACL 前 ⇒ ReqT 落在签名内容里）。
            IRpcHook? requestHook = RequestHooks.Compose(_enableStreamRequestType, _rpcHook);
            _mqClient = new MQClientInstance(_clientId, _nameServerAddrs,
                /*connectTimeoutMillis=*/3000,
                /*invokeTimeoutMillis=*/_pullTimeoutMillis,
                tlsEnable: TlsEnable, unitName: _unitName,
                pollNameServerIntervalMillis: _pollNameServerIntervalMillis);
            if (requestHook is not null && !_mqClient.RegisterRpcHook(requestHook))
            {
                ClientLog.Warn("consumer rpc hook ignored: MQClientInstance already has one (clientId="
                    + _clientId + ")");
            }
            _mqClient.Start();
            // 动态 name server：实例启动时可能已从地址服务器拿到地址，回填到本消费者
            // （Java 由共享的 ClientConfig 天然同步）
            if (_nameServerAddrs.Count == 0 && _mqClient.NameServerAddrs.Count > 0)
            {
                _nameServerAddrs = new List<string>(_mqClient.NameServerAddrs);
            }

            // POP 消费执行器必须在 rebalance（会立刻起每队列 POP 循环）之前建好，
            // 否则循环弹出消息后无处投递（对齐 Java 在 service 构造时就建 consumeExecutor）。
            if (PopMode)
            {
                _popConsumeExecutor = new ConsumeExecutor(
                    Math.Max(1, _corePoolSize), Math.Max(1, _consumeThreadMax),
                    /*keepAliveSeconds=*/60.0, "rmq-popconsume-" + ConsumerGroup);
            }

            _stop = false;
            _started = true;
            _startTimestamp = UtilAll.CurrentTimeMillis();

            // 登记订阅 topic 为"在用"，交给 MQClientInstance 周期刷新路由（对应 task ⑥）
            foreach (string t in SubscribedTopics())
            {
                _mqClient.RegisterTopicInUse(t);
            }

            // broker 主动通知 40（成员变化 → 立即重算）**不**在这里注册处理器：
            // Java 把它注册在 MQClientAPIImpl（实例级），而一个 code 只能有一个处理器，
            // 各自注册会互相覆盖。改成向实例登记「叫醒」回调，由实例收到后扇出。
            _mqClient.RegisterRebalanceWakeup(ConsumerGroup, WakeRebalanceLoop);

            // 对应 Java ClientRemotingProcessor GET_CONSUMER_RUNNING_INFO(307)：
            // admin / broker 查询本消费者运行信息，回 ConsumerRunningInfo JSON body。
            _mqClient.RemotingClient.RegisterProcessor(RequestCode.GetConsumerRunningInfo,
                OnGetConsumerRunningInfo);

            // RESET_CONSUMER_CLIENT_OFFSET(220)：broker 用 invokeOneway 发，无需应答。
            // 重置会触发 rebalance（invokeSync），不能在读线程上同步跑——丢后台线程。
            _mqClient.RemotingClient.RegisterProcessor(RequestCode.ResetConsumerClientOffset,
                OnResetConsumerOffset);

            // GET_CONSUMER_STATUS_FROM_CLIENT(221)：admin 查询已消费位点表。
            _mqClient.RemotingClient.RegisterProcessor(RequestCode.GetConsumerStatusFromClient,
                OnGetConsumerStatus);

            // CONSUME_MESSAGE_DIRECTLY(309)：broker 推一条消息下来，本地真实消费一次。
            _mqClient.RemotingClient.RegisterProcessor(RequestCode.ConsumeMessageDirectly,
                OnConsumeMessageDirectly);
        }

        // 对齐 Java DefaultMQPushConsumerImpl.start 的顺序：
        // 拉一次路由 → 发心跳（broker 先认识本消费者）→ 立即 rebalance → 起消费线程。
        // 心跳必须在 rebalance 之前：rebalance 要向 broker 查消费者列表。
        try
        {
            RefreshRoutes();
        }
        catch (Exception e)
        {
            ClientLog.Debug("initial refresh routes failed: " + e.Message);
        }

        // 对齐 Java DefaultMQPushConsumerImpl.start:1013-1020：路由到手之后、心跳之前，把
        // 非 TAG 订阅发给 broker 校验（CHECK_CLIENT_CONFIG 46）。SQL92 写错时 broker 的过滤层
        // 会**静默放行全部消息**，只有这一笔请求能把它变成启动错误；失败就地回滚后再上抛。
        try
        {
            _mqClient.CheckSubscriptionsInBroker(ConsumerGroup, _subscriptionData.Values);
        }
        catch (Exception)
        {
            Shutdown();
            throw;
        }

        try
        {
            SendHeartbeatToAllBroker();
        }
        catch (Exception e)
        {
            ClientLog.Debug("initial heartbeat failed: " + e.Message);
        }

        // 首轮分配必须同步完成：否则拉取线程会在空分配集上白转，直到首轮 rebalance 才生效
        try
        {
            DoRebalance();
        }
        catch (Exception e)
        {
            ClientLog.Debug("initial rebalance failed: " + e.Message);
        }

        _dispatchThread = MakeThread("ConsumeMessageThread", DispatchLoop);
        _dispatchThread.Start();
        _persistThread = MakeThread("MQClientFactoryScheduledThread", OffsetPersistLoop);
        _persistThread.Start();
        _lockThread = MakeThread("ConsumeMessageOrderlyServiceThread", LockLoop);
        _lockThread.Start();
        _rebalanceThread = MakeThread("RebalanceThread", RebalanceLoop);
        _rebalanceThread.Start();

        string topics = string.Empty;
        foreach (string t in SubscribedTopics())
        {
            if (topics.Length > 0) topics += ",";
            topics += t;
        }

        ClientLog.Info("DefaultMQPushConsumer[" + ConsumerGroup + "] started, clientId=" + _clientId
            + ", topics=" + topics + ", pullTimeout=" + _pullTimeoutMillis.ToString(CultureInfo.InvariantCulture)
            + "ms, pullSuspend=" + _pullSuspendTimeoutMillis.ToString(CultureInfo.InvariantCulture) + "ms");

        // 轨迹分发器最后启动：它会拉起自己的内部生产者（各自加锁），放在其它线程之后更安全。
        StartTraceDispatcher();
    }

    public void Shutdown()
    {
        bool wasStarted = _started;
        _started = false;
        if (!wasStarted)
        {
            return;
        }

        _stop = true;
        _stopEvent.Set();
        // POP：把所有队列标成 dropped，在途批次不再 ack（交给 broker 复活重投）
        if (PopMode)
        {
            foreach (PopProcessQueue pq in _popQueues.Values)
            {
                pq.SetDropped(true);
            }

            _popQueues.Clear();
        }

        // POP 消费执行器收工（对齐 Java shutdownGracefully：不再收新任务，把手上的批次跑完）。
        // Dispose 里 Shutdown(wait=true)：工作线程捕获了 this，必须 join 完才能放。
        _popConsumeExecutor?.Shutdown(true);
        _popConsumeExecutor = null;

        // 退出前把已消费位点持久化一次（对齐 Java shutdown → persistAllConsumerOffset）
        try
        {
            PersistOffsetsOnce();
        }
        catch (Exception e)
        {
            ClientLog.Debug("persist offsets on shutdown failed: " + e.Message);
        }

        // 顺序消费清退时解锁队列（对齐 Java ConsumeMessageOrderlyService.shutdown → unlockAll）
        if (IsOrderly() && _messageModel != RocketMQ.Remoting.Protocol.MessageModel.Broadcasting)
        {
            try
            {
                List<MessageQueue> mqs = AssignedQueues();
                if (mqs.Count > 0 && _mqClient is not null)
                {
                    _mqClient.UnlockBatchMq(ConsumerGroup, _clientId, mqs);
                }
            }
            catch (Exception e)
            {
                ClientLog.Debug("unlock on shutdown failed: " + e.Message);
            }
        }

        foreach (Thread t in _pullThreads.Values)
        {
            if (t.IsAlive) t.Join();
        }

        _pullThreads.Clear();
        // 停机后残留的盖章会让下次 Start() 的自愈判定读到旧实例的时刻
        lock (_lock)
        {
            _lastPullAt.Clear();
        }

        JoinIfAlive(_dispatchThread);
        JoinIfAlive(_persistThread);
        JoinIfAlive(_lockThread);
        JoinIfAlive(_rebalanceThread);

        // 优雅注销（对应 task ④）：关闭连接前从所有 broker 摘除本 clientId，不必等心跳超时
        // （默认 ~120s）——否则这段时间内消费者变更通知仍可能发往已退出的实例。
        if (_mqClient is not null)
        {
            // 消费线程都已 join，先把 40 的「叫醒」回调摘掉（回调捕获了本消费者状态）
            _mqClient.UnregisterRebalanceWakeup(ConsumerGroup);
            try
            {
                _mqClient.UnregisterClientAllBrokers(_clientId, "", ConsumerGroup);
            }
            catch (Exception e)
            {
                ClientLog.Debug("unregister client on shutdown failed: " + e.Message);
            }
        }

        _mqClient?.Shutdown();

        // 顺序对齐 Java DefaultMQPushConsumer.shutdown()：先关本消费者，再 flush 并关轨迹分发器
        //（分发器用的是**自己的**内部生产者，与本实例无关，所以关掉了照样能发完）
        if (_traceDispatcher is not null)
        {
            try
            {
                _traceDispatcher.Shutdown();
            }
            catch (Exception e)
            {
                ClientLog.Warn("trace dispatcher shutdown failed: " + e.Message);
            }
        }
    }

    // ---------------- 消费钩子（对应 Java executeHookBefore / executeHookAfter）----------------
    // 钩子异常一律吞掉并记 warn：轨迹出错绝不能影响正常消费。

    private void ExecuteConsumeHookBefore(ConsumeMessageContext context)
    {
        foreach (IConsumeMessageHook hook in _consumeMessageHooks)
        {
            try
            {
                hook.ConsumeMessageBefore(context);
            }
            catch (Exception e)
            {
                ClientLog.Warn("consumeMessageHook executeHookBefore exception: " + e.Message);
            }
        }
    }

    private void ExecuteConsumeHookAfter(ConsumeMessageContext context)
    {
        foreach (IConsumeMessageHook hook in _consumeMessageHooks)
        {
            try
            {
                hook.ConsumeMessageAfter(context);
            }
            catch (Exception e)
            {
                ClientLog.Warn("consumeMessageHook executeHookAfter exception: " + e.Message);
            }
        }
    }

    /// <summary>构造消费钩子上下文。初始值与 Java 一致：success=false、Props 空、accessChannel=LOCAL。</summary>
    private ConsumeMessageContext BuildConsumeHookContext(List<MessageExt> msgs, MessageQueue mq)
        => new(ConsumerGroup, msgs, mq)
        {
            Success = false,
            Props = new PropertyMap(),
            AccessChannel = AccessChannel.Local,
        };

    /// <summary>对应 Java 的 returnType 判定（决定轨迹 SubAfter 的 contextCode）。
    /// 顺序：status == null → EXCEPTION/RETURNNULL；RT ≥ consumeTimeout（分钟）→ TIME_OUT；
    /// 失败 → FAILED；成功 → SUCCESS。顺序消费把「挂起」当 FAILED、成功当 SUCCESS，
    /// 由调用方用 failed/succeeded 两个标志传入。</summary>
    private string ConsumeReturnTypeName(bool hasStatus, bool hasException, double consumeRtMs,
        bool failed, bool succeeded)
    {
        if (!hasStatus)
        {
            return hasException ? "EXCEPTION" : "RETURNNULL";
        }

        if (consumeRtMs >= ConsumeTimeout * 60 * 1000.0)
        {
            return "TIME_OUT";
        }

        if (failed)
        {
            return "FAILED";
        }

        return succeeded ? "SUCCESS" : "SUCCESS";
    }

    /// <summary>把 returnType / status / success 写回上下文并触发 after 钩子（对齐 Java）。
    /// ⚠ returnType 判定必须在「status 归一化为 RECONSUME_LATER」**之前**做，
    /// 否则 listener 返回 null 会被误记成 FAILED 而不是 RETURNNULL。</summary>
    private void FinishConsumeHook(ConsumeMessageContext? hookCtx, bool hasStatus, bool hasException,
        long beginMs, string statusText, bool failed, bool succeeded)
    {
        if (hookCtx is null)
        {
            return;
        }

        double rt = UtilAll.CurrentTimeMillis() - beginMs;
        hookCtx.Props!["ConsumeContextType"] = ConsumeReturnTypeName(hasStatus, hasException, rt,
            failed, succeeded);
        hookCtx.Status = statusText;
        hookCtx.Success = succeeded;
        ExecuteConsumeHookAfter(hookCtx);
    }

    /// <summary>对应 Java DefaultMQPushConsumer.start()：enableTrace=true 时建分发器
    /// （Type=Consume）并注册 ConsumeMessageTraceHook，随后 start 它。
    /// 任何异常都只记日志 —— 轨迹挂了不能影响正常消费。</summary>
    private void StartTraceDispatcher()
    {
        if (_enableTrace)
        {
            try
            {
                var dispatcher = new AsyncTraceDispatcher(ConsumerGroup,
                    TraceDispatcherType.Consume, _traceMsgBatchNum, _traceTopic, _rpcHook);
                dispatcher.SetHostConsumer(this);
                _traceDispatcher = dispatcher;
                RegisterConsumeMessageHook(new ConsumeMessageTraceHook(dispatcher));
            }
            catch (Exception e)
            {
                ClientLog.Error("system mqtrace hook init failed, maybe can't send msg trace data: "
                    + e.Message);
            }
        }

        if (_traceDispatcher is not null)
        {
            try
            {
                _traceDispatcher.Start(_nameServerAddrs, AccessChannel.Local);
            }
            catch (Exception e)
            {
                ClientLog.Warn("trace dispatcher start failed: " + e.Message);
            }
        }
    }

    private static void JoinIfAlive(Thread? t)
    {
        if (t is not null && t.IsAlive)
        {
            t.Join();
        }
    }

    /// <summary>后台线程工厂（dispatch / pull / persist / lock / rebalance 全走这里）。</summary>
    /// <remarks>⚠ 这里的兜底 try/catch 是**刻意**的，别当成"掩盖 bug"删掉：
    /// Java 的 ServiceThread / 线程池会把后台任务抛出的 Throwable 吞成日志（只损失那一个线程），
    /// 而 **.NET 的默认行为是未处理异常直接终止整个进程**。实测后果：拉取线程在 Shutdown 竞态里
    /// 调 Client() 抛 MQClientException → 整个联调进程 `Abort trap: 6`（RC=134），
    /// 其后的所有场景（S6/S7）一行都跑不到。有了这层兜底，进程级崩溃降级为
    /// "该后台线程退出 + 一条 WARN"，与 Java 的可观测行为一致。
    /// 注意：正常路径**不应**出现这条 WARN —— 出现即代表有真 bug 要查。</remarks>
    private static Thread MakeThread(string name, ThreadStart action)
    {
        var t = new Thread(() =>
        {
            try
            {
                action();
            }
            catch (Exception e)
            {
                ClientLog.Warn("background thread (" + name + ") exited unexpectedly: " + e);
            }
        })
        {
            IsBackground = true,
            Name = name,
        };
        return t;
    }

    /// <summary>启动初期把订阅 topic 的真实路由拉到本地缓存（对应 Java
    /// MQClientInstance.updateTopicRouteInfoFromNameServer 的首轮拉取）。消费侧不做默认 topic
    /// 兜底——%RETRY%group 等尚未由 broker 创建的主题拉不到就保持空，rebalance 视图才一致。</summary>
    private void RefreshRoutes()
    {
        foreach (string t in SubscribedTopics())
        {
            try
            {
                Client().UpdateTopicRouteInfoFromNameServer(t);
            }
            catch (Exception e)
            {
                ClientLog.Debug("refresh route for " + t + " failed: " + e.Message);
            }
        }
    }

    public MQClientInstance Client()
    {
        if (!_started || _mqClient is null)
        {
            throw new MQClientException("consumer not started, call start() first");
        }

        return _mqClient;
    }

    // ---------------- 消费循环 ----------------
    private bool IsOrderly() => _messageListener is not null && _messageListener.Orderly();

    /// <summary>本线程是否仍是该队列的属主（Python <c>_owns_queue</c> / C++ 的归属令牌）。
    /// 必须持 <c>_lock</c> 调用。</summary>
    /// <remarks>Java 用 <c>ProcessQueue</c> 对象本身表达这件事（一次拉取请求绑一把队列对象，
    /// 撤走时 <c>setDropped(true)</c>，请求回来见到就丢弃批次）。.NET 没有 per-queue 的
    /// classic ProcessQueue，所以拿"登记在表里的线程是不是我自己"当等价物：
    /// 自愈重建时旧线程即便还挂在长轮询上，回来后也会认输，不会和新线程同时服务一把队列。</remarks>
    private bool OwnsQueueLocked(string key)
    {
        return _pullThreads.TryGetValue(key, out Thread? t) && ReferenceEquals(t, Thread.CurrentThread);
    }

    /// <summary>只在仍属主时摘除登记，避免把新属主的条目删掉。</summary>
    private void UnregisterOwnLoop(string key)
    {
        lock (_lock)
        {
            if (OwnsQueueLocked(key))
            {
                _pullThreads.Remove(key);
            }
        }
    }

    /// <summary>发起本轮拉取/弹出前盖章（Java <c>pullMessage:253</c> / <c>popMessage:508</c>：
    /// 盖章在流控与挂起**之前**，判据是"这条循环还在跑"，不是"这轮真的打了网络"）。</summary>
    private void StampPullAt(string key, bool pop)
    {
        long now = UtilAll.CurrentTimeMillis();
        PopProcessQueue? pq = null;
        lock (_lock)
        {
            _lastPullAt[key] = now;
            if (pop)
            {
                _popQueues.TryGetValue(key, out pq);
            }
        }

        pq?.Touch(now);
    }

    /// <summary>Java <c>ProcessQueue.isPullExpired</c> + Python 的线程存活检查：两种都算停摆
    /// —— 登记过的循环线程已经不在了（异常穿透），或还在但超过 <see cref="PullMaxIdleTime"/>
    /// 没发起过拉取。从没盖过章（刚分配）不算停摆。</summary>
    private bool PullStalledLocked(string key, long now)
    {
        if (!_lastPullAt.TryGetValue(key, out long began))
        {
            return false;
        }

        if (now - began > PullMaxIdleTime)
        {
            return true;
        }

        return !(_pullThreads.TryGetValue(key, out Thread? t) && t is not null && t.IsAlive);
    }

    /// <summary>停摆自愈（Java <c>updateProcessQueueTableInRebalance:438-461</c>）：队列仍分给
    /// 本实例、但拉取循环已经停摆 → 撤销它并交位点，让本轮后面的
    /// <see cref="RebalancePullThreads"/> 原地重建。</summary>
    private void SweepStalledLoopsLocked(Dictionary<string, MessageQueue> current,
                                         List<RetiredQueue> retired)
    {
        if (!_started || _stop)
        {
            // 停机过程中循环本来就在退出，别把清退变成 [BUG] 日志风暴
            return;
        }

        long now = UtilAll.CurrentTimeMillis();
        List<string> stalled = new();
        foreach (string key in _pullThreads.Keys)
        {
            if (current.ContainsKey(key) && PullStalledLocked(key, now))
            {
                stalled.Add(key);
            }
        }

        foreach (string key in stalled)
        {
            ClientLog.Error("[BUG]doRebalance, " + ConsumerGroup
                            + ", try remove unnecessary mq, " + key
                            + ", because pull is pause, so try to fixed it");
            if (current.TryGetValue(key, out MessageQueue? mq) && mq is not null)
            {
                RetireQueueLocked(key, mq, retired);
            }
        }
    }

    /// <summary>一把被撤队列的收尾材料：队列对象 + 需要持久化的已消费位点（可能压根没有）。</summary>
    private sealed record RetiredQueue(string Key, MessageQueue Mq, long ConsumeOffset, bool HadOffset);
    /// <summary>撤掉一把队列在本实例里的所有痕迹（Python <c>_retire_queue_locked</c>）：
    /// 线程登记、盖章、缓冲、两张位点表、锁状态、POP 队列一起摘掉，并把要持久化的
    /// 已消费位点交给调用方的 <paramref name="retired"/>。</summary>
    private void RetireQueueLocked(string key, MessageQueue mq, List<RetiredQueue> retired)
    {
        _pullThreads.Remove(key);   // 旧循环回来会发现属主已换 → 丢弃批次并退出
        _lastPullAt.Remove(key);    // 同名队列复用不能继承旧时刻
        _pending.Remove(key);
        _offsetTable.Remove(key);
        _lockOk.Remove(key);
        _mqMap.Remove(key);
        if (_popQueues.TryGetValue(key, out PopProcessQueue? pq))
        {
            pq.SetDropped(true);
            _popQueues.Remove(key);
        }

        bool had = _consumeOffsetTable.TryGetValue(key, out long consumeOffset);
        _consumeOffsetTable.Remove(key);
        retired.Add(new RetiredQueue(key, mq, consumeOffset, had));
        _retiredForTest.Add(new RetiredForTest(key, mq, consumeOffset, had));
    }

    /// <summary>单测专用：把已消费位点直接写进表（真实路径由 ConsumeBatch 推进，那需要先跑循环）。
    /// 撤走队列时必须把这个位点交出去持久化，否则重建后从 broker 的旧位点重投。</summary>
    public void SetConsumeOffsetForTest(string key, long offset)
    {
        lock (_lock)
        {
            _consumeOffsetTable[key] = offset;
        }
    }

    /// <summary>单测专用：占住一把队列的 PopProcessQueue 并返回它，用于断言
    /// 「自愈会把在途批次 setDropped，并换一具干净的队列缓冲」。</summary>
    public PopProcessQueue RegisterPopQueueForTest(string key)
    {
        var pq = new PopProcessQueue();
        lock (_lock)
        {
            _popQueues[key] = pq;
        }

        return pq;
    }

    /// <summary>单测专用：该队列当前挂着的 PopProcessQueue（自愈重建后应当是新的一具）。</summary>
    public PopProcessQueue? PopQueueForTest(string key)
    {
        lock (_lock)
        {
            return _popQueues.TryGetValue(key, out PopProcessQueue? pq) ? pq : null;
        }
    }

    // ---------------- 拉取停摆自愈的可观测接缝 ----------------
    //
    // 与 C++/Python/Rust 三版一致地公开（测试工程没有 InternalsVisibleTo，故用 public，
    // 勿用于业务代码）：验证脚本要能把时钟倒拨、触发一次同步，再确认「停摆的队列被撤掉
    // 重建、消息一条不重不丢」。没有接缝就只能 sleep 满 120s 去猜。

    /// <summary>Java <c>ProcessQueue.PULL_MAX_IDLE_TIME</c> 的生效值。</summary>
    public static long PullMaxIdleTimeMillis => PullMaxIdleTime;

    /// <summary>该队列现在是否会被 rebalance 判成停摆。</summary>
    public bool PullStalled(string key)
    {
        lock (_lock)
        {
            return PullStalledLocked(key, UtilAll.CurrentTimeMillis());
        }
    }

    /// <summary>单测专用：把判据的时钟交给调用方。真实时钟下"正好等于 120s"这条边界
    /// 无法稳定断言（读表与比较之间总会推进几毫秒），而 Java 用的是严格 <c>&gt;</c>，
    /// 差一个字符的写法（<c>&gt;=</c>）只能靠注入时钟锁死。</summary>
    public bool PullStalledForTest(string key, long now)
    {
        lock (_lock)
        {
            return PullStalledLocked(key, now);
        }
    }

    /// <summary>最近一次发起拉取/弹出的时刻（毫秒）；<c>-1</c> = 没有记录。</summary>
    public long LastPullAt(string key)
    {
        lock (_lock)
        {
            return _lastPullAt.TryGetValue(key, out long t) ? t : -1;
        }
    }

    /// <summary>把盖章拨到指定时刻（验证用：等价于"这条循环已经 X 毫秒没动静了"）。</summary>
    public void SetLastPullAt(string key, long millis)
    {
        lock (_lock)
        {
            _lastPullAt[key] = millis;
        }
    }

    /// <summary>该队列当前有没有拉取/弹出循环在登记（自愈后必须换一个）。</summary>
    public bool HasPullLoop(string key)
    {
        lock (_lock)
        {
            return _pullThreads.ContainsKey(key);
        }
    }

    /// <summary>单测专用：一次撤走的完整收尾材料。<see cref="SweepStalledLoopsForTest"/>
    /// 只返回"撤了几把"，锁不住撤走的**内容**（位点有没有带走、是不是撤错了队列）。</summary>
    public sealed record RetiredForTest(string Key, MessageQueue Mq, long ConsumeOffset, bool HadOffset);

    private readonly List<RetiredForTest> _retiredForTest = new();

    public IReadOnlyList<RetiredForTest> RetiredQueuesForTest()
    {
        lock (_lock)
        {
            return _retiredForTest.ToList();
        }
    }

    public void ClearRetiredForTest()
    {
        lock (_lock)
        {
            _retiredForTest.Clear();
        }
    }

    /// <summary>跑一次停摆清扫，返回撤掉的队列数（验证/单测用）。</summary>
    public int SweepStalledLoopsForTest(IEnumerable<MessageQueue> current)
    {
        var dict = new Dictionary<string, MessageQueue>(StringComparer.Ordinal);
        foreach (MessageQueue mq in current)
        {
            dict[OffsetKey(mq)] = mq;
        }

        var retired = new List<RetiredQueue>();
        lock (_lock)
        {
            SweepStalledLoopsLocked(dict, retired);
        }

        return retired.Count;
    }

    /// <summary>把"已启动"标志拨成指定值（只喂给单测：真实 Start 会做网络注册）。</summary>
    public void SetStartedForTest(bool started)
    {
        _started = started;
    }

    /// <summary>给某把队列登记一条占位循环线程（单测用）：<paramref name="alive"/>=false 时
    /// 是一条"从未启动/已退出"的线程，正好触发 Java 侧 <c>isAlive</c> 那一半判据。
    /// 活着的占位线程由 <see cref="ReleaseTestLoops"/> 统一放行。</summary>
    public void RegisterLoopForTest(string key, bool alive)
    {
        Thread t = MakeThread("PullMessageService", () => _testLoopGate.Wait());
        lock (_lock)
        {
            _pullThreads[key] = t;
        }

        if (alive)
        {
            t.Start();
        }

        // !alive 时不 Start：Thread 对象存在但 IsAlive=false，等价于线程已退出
    }

    /// <summary>释放 <see cref="RegisterLoopForTest"/> 起的占位线程，让单测不遗留活线程。</summary>
    public void ReleaseTestLoops()
    {
        _testLoopGate.Set();
    }

    private readonly ManualResetEventSlim _testLoopGate = new(false);

    private void RebalancePullThreads()
    {
        List<MessageQueue> queues = AssignedQueues();
        var current = new Dictionary<string, MessageQueue>(StringComparer.Ordinal);
        foreach (MessageQueue mq in queues)
        {
            current[OffsetKey(mq)] = mq;
        }

        List<KeyValuePair<string, MessageQueue>> toStart = new();
        long beganAt;
        lock (_lock)
        {
            beganAt = UtilAll.CurrentTimeMillis();
            foreach (KeyValuePair<string, MessageQueue> kv in current)
            {
                if (!_pullThreads.ContainsKey(kv.Key))
                {
                    toStart.Add(kv);
                    // 分配那一刻就登记队列对象与起点时刻：一条消息都没拉到的队列也要能在
                    // 307/220 里看到、位点也能持久化；不种时刻的话下一轮 rebalance 无从判断。
                    _mqMap[kv.Key] = kv.Value;
                    _lastPullAt[kv.Key] = beganAt;
                }
            }
        }

        foreach (KeyValuePair<string, MessageQueue> kv in toStart)
        {
            // Shutdown 期间（_started 已置 false / _stop 已置 true）不要再起新拉取线程：
            // 线程刚 start 就会撞上"未启动"状态而立刻退出，纯属浪费且放大竞态窗口。
            if (_stop || !_started) return;
            MessageQueue mq = kv.Value;
            if (PopMode && !_popQueues.ContainsKey(kv.Key))
            {
                _popQueues[kv.Key] = new PopProcessQueue();
            }

            Thread t = MakeThread(PopMode ? "PopMessageService" : "PullMessageService",
                                  () => { if (PopMode) QueuePopLoop(mq); else QueuePullLoop(mq); });
            lock (_lock)
            {
                // 竞态保护：rebalance 可能把同 key 再起一次
                if (_pullThreads.ContainsKey(kv.Key))
                {
                    continue;
                }

                _pullThreads[kv.Key] = t;
            }

            t.Start();
        }
    }

    /// <summary>实例收到 broker 的 40 通知后叫醒本消费者的重平衡线程
    /// （对应 Java MQClientInstance#rebalanceImmediately → RebalanceService#wakeup）。
    /// 跑在 remoting 读线程上：只置位，不发 RPC、不做重活。</summary>
    /// <remarks>⚠ 这里**绝不能**同步调用 DoRebalance()：它内部会做阻塞式 invokeSync
    /// （GET_CONSUMER_LIST_BY_GROUP、心跳），而响应只能由**同一个读线程**投递 ——
    /// 读线程阻塞在自己发起的同步调用上必然自死锁，直到 invokeTimeout
    /// （实测 5s 超时、日志出现 "no consumer id list ..., keep current"，
    /// 并连带把其它请求的响应一起卡住）。只置标志、交给 RebalanceThread 去算即可，
    /// 与 C++ 侧只置 rebalanceNow_ 标志的语义一致。</remarks>
    private void WakeRebalanceLoop()
    {
        _rebalanceNow.Set();
    }

    /// <summary>真机验证专用：叫醒重平衡线程，等价于 broker 推来的
    /// <c>NOTIFY_CONSUMER_IDS_CHANGED(40)</c>（同样只置位，真正的 rebalance 由
    /// 重平衡线程去做，见 <see cref="WakeRebalanceLoop"/>）。注入停摆之后要等
    /// 20s 周期才能观察到自愈，没这颗事件就只能 sleep 赌。</summary>
    public void WakeupRebalanceForTest()
    {
        WakeRebalanceLoop();
    }

    private RemotingCommand? OnGetConsumerRunningInfo(RemotingCommand cmd, string addr)
    {
        // 对应 Java ClientRemotingProcessor.processRequest GET_CONSUMER_RUNNING_INFO 分支。
        // 运行在读线程上：只做本地快照（构造 ConsumerRunningInfo），绝不做网络调用。
        string? group = null;
        if (cmd.ExtFields is not null && cmd.ExtFields.TryGetValue("consumerGroup", out string? g))
        {
            group = g;
        }

        if (!string.Equals(group, ConsumerGroup, StringComparison.Ordinal))
        {
            // 与 Java 一致：组不匹配回 SYSTEM_ERROR（broker 端会打 warn）
            return RemotingCommand.CreateResponseCommand(
                ResponseCode.SystemError,
                "consumerGroup not matched, expect " + ConsumerGroup + ", got " + (group ?? "<null>"));
        }

        RemotingCommand resp = RemotingCommand.CreateResponseCommand(ResponseCode.Success, null);
        resp.Body = BuildConsumerRunningInfo().Encode();
        return resp;
    }

    private RemotingCommand? OnResetConsumerOffset(RemotingCommand cmd, string addr)
    {
        // 对应 Java ClientRemotingProcessor RESET_CONSUMER_CLIENT_OFFSET 分支：
        // oneway 请求（broker 不等响应），重置逻辑丢到后台线程（不能阻塞读线程）。
        string? group = null;
        string? topic = null;
        if (cmd.ExtFields is not null)
        {
            if (cmd.ExtFields.TryGetValue("group", out string? g))
            {
                group = g;
            }

            if (cmd.ExtFields.TryGetValue("topic", out string? t))
            {
                topic = t;
            }
        }

        if (!string.Equals(group, ConsumerGroup, StringComparison.Ordinal))
        {
            return null;   // oneway，不回响应
        }

        var table = new Dictionary<MessageQueue, long>();
        if (cmd.Body is { Length: > 0 } && ResetOffsetBody.Decode(cmd.Body, out ResetOffsetBody body))
        {
            foreach (var kv in body.OffsetTable)
            {
                table[kv.Key] = kv.Value;
            }
        }

        DefaultMQPushConsumer consumer = this;
        string topicArg = topic ?? string.Empty;
        MakeThread("ResetOffsetThread", () =>
        {
            try
            {
                consumer.ResetOffset(topicArg, table);
            }
            catch (Exception e)
            {
                ClientLog.Warn("reset offset failed (group=" + ConsumerGroup + "): " + e.Message);
            }
        }).Start();
        return null;
    }

    private RemotingCommand? OnGetConsumerStatus(RemotingCommand cmd, string addr)
    {
        // 运行在读线程上：只做本地快照（已消费位点表），绝不做网络调用。
        string? group = null;
        string? topic = null;
        if (cmd.ExtFields is not null)
        {
            if (cmd.ExtFields.TryGetValue("group", out string? g))
            {
                group = g;
            }

            if (cmd.ExtFields.TryGetValue("topic", out string? t))
            {
                topic = t;
            }
        }

        if (!string.Equals(group, ConsumerGroup, StringComparison.Ordinal))
        {
            // 与 Java 一致：组不匹配回 SYSTEM_ERROR（broker 端会打 warn）
            return RemotingCommand.CreateResponseCommand(
                ResponseCode.SystemError,
                "consumerGroup not matched, expect " + ConsumerGroup + ", got " + (group ?? "<null>"));
        }

        var body = new GetConsumerStatusBody();
        foreach (var kv in GetConsumerStatus(topic ?? string.Empty))
        {
            body.MessageQueueTable[kv.Key] = kv.Value;
        }

        RemotingCommand resp = RemotingCommand.CreateResponseCommand(ResponseCode.Success, null);
        resp.Body = body.Encode();
        return resp;
    }

    private RemotingCommand? OnConsumeMessageDirectly(RemotingCommand cmd, string addr)
    {
        // 与 Python 一致：监听器在读线程上同步跑（admin 一次性探针；监听器里如果再发
        // 同步请求会自死锁，但那是用户代码职责，Java 读线程同样有此约束）。
        string? group = null;
        string? brokerName = null;
        if (cmd.ExtFields is not null)
        {
            if (cmd.ExtFields.TryGetValue("consumerGroup", out string? g))
            {
                group = g;
            }

            if (cmd.ExtFields.TryGetValue("brokerName", out string? b))
            {
                brokerName = b;
            }
        }

        if (!string.Equals(group, ConsumerGroup, StringComparison.Ordinal))
        {
            return RemotingCommand.CreateResponseCommand(
                ResponseCode.SystemError,
                "consumerGroup not matched, expect " + ConsumerGroup + ", got " + (group ?? "<null>"));
        }

        if (cmd.Body is null || cmd.Body.Length == 0)
        {
            return RemotingCommand.CreateResponseCommand(ResponseCode.SystemError, "empty message body");
        }

        if (!MessageDecoder.DecodeMessage(cmd.Body, out MessageExt msg, true, true, true, false))
        {
            return RemotingCommand.CreateResponseCommand(ResponseCode.SystemError, "decode message failed");
        }

        RemotingCommand ok = RemotingCommand.CreateResponseCommand(ResponseCode.Success, null);
        ok.Body = ConsumeMessageDirectly(msg, brokerName ?? string.Empty).Encode();
        return ok;
    }

    /// <summary>对应 Java DefaultMQPushConsumerImpl.consumerRunningInfo（307 的应答体）。</summary>
    public ConsumerRunningInfo BuildConsumerRunningInfo()
    {
        var info = new ConsumerRunningInfo
        {
            Properties =
            {
                [ConsumerRunningInfo.PropNameserverAddr] = string.Join(";", _nameServerAddrs) + ";",
                [ConsumerRunningInfo.PropConsumeType] = "CONSUME_PASSIVELY",
                [ConsumerRunningInfo.PropConsumeOrderly] = IsOrderly() ? "true" : "false",
                [ConsumerRunningInfo.PropThreadpoolCoreSize] =
                    GetCorePoolSize().ToString(CultureInfo.InvariantCulture),
                [ConsumerRunningInfo.PropConsumerStartTimestamp] =
                    _startTimestamp.ToString(CultureInfo.InvariantCulture),
                [ConsumerRunningInfo.PropClientVersion] = "V5_5_1",
            }
        };

        JsonValue subs = JsonValue.MakeArray();
        var statusTable = JsonValue.MakeObject();
        lock (_lock)
        {
            foreach (SubscriptionData sub in _subscriptionData.Values)
            {
                subs.PushArray(sub.ToJson());
            }

            foreach (var kv in _mqMap)
            {
                // POP 模式下弹出去 _popQueues（Java 的 popProcessQueueTable），classic 的
                // processQueueTable 是空的 —— 两把表在 Java 里互斥，307 里也必须互斥：同一把
                // 队列既进 mqTable 又进 mqPopTable 会让控制台把一路消费数成两路。
                // _mqMap 是"已分配"注册表（自愈、位点持久化都靠它），两种模式都写，所以按模式过滤。
                if (PopMode && _popQueues.ContainsKey(kv.Key))
                {
                    continue;
                }

                string mqKey = MessageQueueKeys.MessageQueueKey(kv.Value);
                long commitOffset = _consumeOffsetTable.TryGetValue(kv.Key, out long co) ? co : 0;
                int cachedMsgCount = _pending.TryGetValue(kv.Key, out Queue<MessageExt>? q)
                    ? q.Count
                    : 0;
                // Java ProcessQueue.fillOutRunningInfo:456 —— 运维看这个字段判断"还在不在拉"，
                // rebalance 的停摆自愈用的就是同一个时刻。
                long lastPull = _lastPullAt.TryGetValue(kv.Key, out long lpt) ? lpt : 0;
                info.MqTable.Set(mqKey, MakeProcessQueueInfo(commitOffset, cachedMsgCount,
                    droped: false, lastPullTimestamp: lastPull));
            }

            if (PopMode)
            {
                foreach (var kv in _popQueues)
                {
                    if (!_mqMap.TryGetValue(kv.Key, out MessageQueue? mq))
                    {
                        continue;
                    }

                    info.MqPopTable.Set(MessageQueueKeys.MessageQueueKey(mq),
                        MakeProcessQueueInfo(0, kv.Value.WaitAckCount(), kv.Value.IsDropped(),
                            // Java 的 pop 视图本没有这个字段（PopProcessQueue 不填），但
                            // lastPopTimestamp 正是停摆判据本身，如实暴露（与 Python 一致）。
                            lastPullTimestamp: kv.Value.LastPopTimestamp));
                }
            }
        }

        // statusTable（Java consumerRunningInfo：consumeStatus(group, topic)，minute 快照）
        foreach (string topic in _subscriptionData.Keys)
        {
            ConsumeStatus cs = _mqClient?.ConsumerStats.ConsumeStatus(ConsumerGroup, topic)
                ?? new ConsumeStatus();
            statusTable.Set(topic, cs.ToJson());
        }

        info.SubscriptionSet = subs;
        info.StatusTable = statusTable;
        return info;
    }

    /// <summary>
    /// 对应 Java MQClientInstance.resetOffset（220 的消费者侧逻辑）：
    /// 命中本 topic 分配队列的 → 清在途缓冲与拉取游标 → 写新已消费位点 →
    /// 撤销该队列（持久化新位点 + 顺序解锁）→ 立即 rebalance 从新位点重拉。
    /// </summary>
    public void ResetOffset(string topic, IReadOnlyDictionary<MessageQueue, long> offsetTable)
    {
        if (string.IsNullOrEmpty(topic) || offsetTable.Count == 0)
        {
            return;
        }

        var hit = new List<MessageQueue>();
        lock (_lock)
        {
            foreach (var kv in _mqMap)
            {
                MessageQueue mq = kv.Value;
                if (mq.Topic != topic)
                {
                    continue;
                }

                if (!offsetTable.TryGetValue(mq, out long off))
                {
                    continue;
                }

                _pending.Remove(kv.Key);        // 等价 ProcessQueue.clear()
                _offsetTable.Remove(kv.Key);    // 拉取游标一并清掉
                _consumeOffsetTable[kv.Key] = off;
                hit.Add(mq);
            }
        }

        if (hit.Count == 0)
        {
            return;
        }

        // Java 用 RESET_OFFSET_MAX_WAIT（5 秒）等并发消费跑完；这里缩短以免阻塞太久
        // （220 是 oneway，broker 不等响应，但仍应尽快返回）。
        Thread.Sleep(200);
        bool orderly = IsOrderly();
        bool broadcast = _messageModel == RocketMQ.Remoting.Protocol.MessageModel.Broadcasting;
        var unlockList = new List<MessageQueue>();
        foreach (MessageQueue mq in hit)
        {
            string key = OffsetKey(mq);
            lock (_lock)
            {
                _dropped.Add(key);   // 拉取线程见到 dropped 自行退出并从 _pullThreads 摘除
                _pending.Remove(key);
                _mqMap.Remove(key);
                _offsetTable.Remove(key);
            }

            // 新位点已在 _consumeOffsetTable：撤销收尾时持久化（对齐 Java resetOffset）
            if (!broadcast && _mqClient is not null)
            {
                try
                {
                    _mqClient.UpdateConsumerOffset(ConsumerGroup, mq, offsetTable[mq]);
                }
                catch (Exception e)
                {
                    ClientLog.Debug("persist offset on reset failed for " + mq + ": " + e.Message);
                }

                if (orderly)
                {
                    unlockList.Add(mq);
                }
            }
        }

        if (unlockList.Count > 0 && _mqClient is not null)
        {
            try
            {
                _mqClient.UnlockBatchMq(ConsumerGroup, _clientId, unlockList);
            }
            catch (Exception e)
            {
                ClientLog.Debug("unlock on reset failed: " + e.Message);
            }
        }

        try
        {
            DoRebalance();   // 重新分配（队列仍在分配集里，从新位点重拉）
        }
        catch (Exception e)
        {
            ClientLog.Debug("rebalance after reset offset failed: " + e.Message);
        }

        ClientLog.Info("reset offset applied, group=" + ConsumerGroup + " topic=" + topic
            + " queues=" + hit.Count.ToString(CultureInfo.InvariantCulture));
    }

    /// <summary>
    /// 对应 Java MQClientInstance.getConsumerStatus（221 的应答数据源）：
    /// 返回**已消费位点**表（不是拉取游标），topic 为空则返回全部。
    /// </summary>
    public SortedDictionary<MessageQueue, long> GetConsumerStatus(string topic)
    {
        var outTable = new SortedDictionary<MessageQueue, long>();
        lock (_lock)
        {
            foreach (var kv in _mqMap)
            {
                if (!string.IsNullOrEmpty(topic) && kv.Value.Topic != topic)
                {
                    continue;
                }

                if (_consumeOffsetTable.TryGetValue(kv.Key, out long off))
                {
                    outTable[kv.Value] = off;
                }
            }
        }

        return outTable;
    }

    /// <summary>
    /// 对应 Java ConsumeMessageConcurrentlyService.consumeMessageDirectly（309）：
    /// 本地真实消费一条消息（还原重投 topic 后交给监听器），把结果回给 admin。
    /// </summary>
    public ConsumeMessageDirectlyResult ConsumeMessageDirectly(MessageExt msg, string brokerName)
    {
        var result = new ConsumeMessageDirectlyResult { AutoCommit = true };
        var msgs = new List<MessageExt> { msg };
        var mq = new MessageQueue(msg.Topic, brokerName ?? string.Empty, msg.QueueId);
        result.Order = IsOrderly();
        ResetRetryTopicAndNamespace(msgs);
        long begin = UtilAll.CurrentTimeMillis();
        if (_messageListener is null)
        {
            result.ConsumeResult = "CR_RETURN_NULL";
        }
        else if (_messageListener is IMessageListenerOrderly orderlyListener)
        {
            try
            {
                var ctx = new ConsumeOrderlyContext(mq);
                ConsumeOrderlyStatus status = orderlyListener.ConsumeMessage(msgs, ctx);
                result.ConsumeResult = status == ConsumeOrderlyStatus.Success
                    ? "CR_SUCCESS" : "CR_LATER";
            }
            catch (Exception e)
            {
                result.ConsumeResult = "CR_THROW_EXCEPTION";
                result.Remark = e.GetType().Name + ": " + e.Message;
            }
        }
        else if (_messageListener is IMessageListenerConcurrently concurrentListener)
        {
            try
            {
                var ctx = new ConsumeConcurrentlyContext(mq);
                ConsumeConcurrentlyStatus status = concurrentListener.ConsumeMessage(msgs, ctx);
                result.ConsumeResult = status == ConsumeConcurrentlyStatus.ConsumeSuccess
                    ? "CR_SUCCESS" : "CR_LATER";
            }
            catch (Exception e)
            {
                result.ConsumeResult = "CR_THROW_EXCEPTION";
                result.Remark = e.GetType().Name + ": " + e.Message;
            }
        }
        else
        {
            result.ConsumeResult = "CR_RETURN_NULL";
        }

        result.SpentTimeMills = UtilAll.CurrentTimeMillis() - begin;
        return result;
    }

    private static JsonValue MakeProcessQueueInfo(long commitOffset, long cachedMsgCount, bool droped,
                                                  long lastPullTimestamp = 0)
    {
        // ProcessQueueInfo 全字段（Java body.ProcessQueueInfo；"droped" 拼写照抄）
        var pqi = JsonValue.MakeObject();
        pqi.Set("commitOffset", JsonValue.MakeInt(commitOffset));
        pqi.Set("cachedMsgMinOffset", JsonValue.MakeInt(0));
        pqi.Set("cachedMsgMaxOffset", JsonValue.MakeInt(0));
        pqi.Set("cachedMsgCount", JsonValue.MakeInt(cachedMsgCount));
        pqi.Set("cachedMsgSizeInMiB", JsonValue.MakeInt(0));
        pqi.Set("transactionMsgMinOffset", JsonValue.MakeInt(0));
        pqi.Set("transactionMsgMaxOffset", JsonValue.MakeInt(0));
        pqi.Set("transactionMsgCount", JsonValue.MakeInt(0));
        pqi.Set("locked", JsonValue.MakeBool(false));
        pqi.Set("tryUnlockTimes", JsonValue.MakeInt(0));
        pqi.Set("lastLockTimestamp", JsonValue.MakeInt(0));
        pqi.Set("droped", JsonValue.MakeBool(droped));
        pqi.Set("lastPullTimestamp", JsonValue.MakeInt(lastPullTimestamp));
        pqi.Set("lastConsumeTimestamp", JsonValue.MakeInt(0));
        return pqi;
    }

    /// <summary>
    /// 消费侧 RT/TPS 记数（Java ConsumeRequest.run：RT 恒记，OK/FAILED 按结果）。
    /// <paramref name="ackCount"/> 对应 Java processConsumeResult:217-220 的 ok = ackIndex + 1：
    /// 部分 ack 时前缀算 OK、尾巴算 FAILED。不传则按整批算（顺序/POP 路径的旧口径）。
    /// </summary>
    private void RecordConsumeStats(string topic, int msgCount, long beginMs, bool failed,
        int? ackCount = null)
    {
        if (_mqClient is null)
        {
            return;
        }

        ConsumerStatsManager stats = _mqClient.ConsumerStats;
        long rt = UtilAll.CurrentTimeMillis() - beginMs;
        if (failed)
        {
            stats.IncConsumeFailedTPS(ConsumerGroup, topic, msgCount);
        }
        else
        {
            int ok = ackCount ?? msgCount;
            stats.IncConsumeOKTPS(ConsumerGroup, topic, ok);
            if (msgCount > ok)
            {
                stats.IncConsumeFailedTPS(ConsumerGroup, topic, msgCount - ok);
            }
        }

        stats.IncConsumeRT(ConsumerGroup, topic, rt);
    }

    private void RebalanceLoop()
    {
        // 周期重算分配（对齐 Java RebalanceService 默认 20s），或被通知时立即重算。
        // 启动阶段且当前没有任何分配时缩短为 2s 重试：消费者可能先于 topic 被创建启动
        // （autoCreateTopicEnable 下 broker 由生产者的首次发送建 topic），此时真实路由还
        // 拉不到——消费端不做默认 topic 兜底（见 MQClientInstance.UpdateTopicRouteInfoFromNameServer），
        // 死等 20s 会长时间不消费。该快速重试只在启动后 60s 内生效。
        long startTime = UtilAll.CurrentTimeMillis();
        while (!_stop)
        {
            bool startingUp = (UtilAll.CurrentTimeMillis() - startTime) < 60000;
            int interval = (startingUp && AssignedQueues().Count == 0) ? 2000 : 20000;
            _rebalanceNow.Wait(interval);
            _rebalanceNow.Reset();
            if (_stop || !_started) return;
            try
            {
                MaybeSendHeartbeat();
                DoRebalance();
            }
            catch (Exception e)
            {
                ClientLog.Debug("rebalance error: " + e.Message);
            }
        }
    }

    /// <summary>拉取前的流控判定 —— Python <c>_flow_control_hit</c> / Java
    /// <c>ProcessQueue.putMessage</c> 的五个阈值，按此顺序命中即返回（先命中的那条会掩盖后面的）：
    /// <list type="number">
    /// <item>条数 &gt;= <c>PullThresholdForQueue</c>（Java 的 <c>Math.max(1,n)</c> 守卫：配 0 也按 1 条算）</item>
    /// <item>字节 &gt;= <c>PullThresholdSizeForQueue</c>，单位 <b>MiB</b>（&lt;=0 关闭）</item>
    /// <item>跨度 <b>严格大于</b> <c>ConsumeConcurrentlyMaxSpan</c>（缓冲内 queueOffset 的 max-min）</item>
    /// <item>topic 累计条数 &gt;= <c>PullThresholdForTopic</c>（本实例该 topic <b>所有</b>队列合起来）</item>
    /// <item>topic 累计字节 &gt;= <c>PullThresholdSizeForTopic</c>，单位 MiB（不复用第 2 条的开关）</item>
    /// </list>
    /// 命中一次只把 <see cref="FlowControlTriggered"/> 加一格。只有真开着 topic 级闸门时才遍历
    /// 全表聚合，否则每次判定都多走一遍所有队列的缓冲。</summary>
    private bool FlowControlHit(MessageQueue mq, string key)
    {
        int count;
        double sizeMb;
        long span;
        int topicCount = 0;
        double topicSizeMb = 0.0;
        lock (_lock)
        {
            if (_pending.TryGetValue(key, out Queue<MessageExt>? q) && q.Count > 0)
            {
                long bytes = 0;
                long minOffset = long.MaxValue;
                long maxOffset = long.MinValue;
                foreach (MessageExt m in q)
                {
                    bytes += m.StoreSize;
                    if (m.QueueOffset < minOffset) minOffset = m.QueueOffset;
                    if (m.QueueOffset > maxOffset) maxOffset = m.QueueOffset;
                }

                count = q.Count;
                sizeMb = bytes / (1024.0 * 1024.0);
                span = maxOffset - minOffset;
            }
            else
            {
                count = 0;
                sizeMb = 0;
                span = 0;
            }

            if (_pullThresholdForTopic > 0 || _pullThresholdSizeForTopic > 0)
            {
                long topicBytes = 0;
                foreach (KeyValuePair<string, Queue<MessageExt>> kv in _pending)
                {
                    if (!_mqMap.TryGetValue(kv.Key, out MessageQueue? other)
                        || other.Topic != mq.Topic)
                    {
                        continue;
                    }

                    foreach (MessageExt m in kv.Value)
                    {
                        topicCount++;
                        topicBytes += m.StoreSize;
                    }
                }

                topicSizeMb = topicBytes / (1024.0 * 1024.0);
            }
        }

        string? reason = null;
        if (count >= Math.Max(1, _pullThresholdForQueue))
        {
            reason = "count=" + count.ToString(CultureInfo.InvariantCulture);
        }
        else if (_pullThresholdSizeForQueue > 0 && sizeMb >= _pullThresholdSizeForQueue)
        {
            reason = "size=" + sizeMb.ToString("F1", CultureInfo.InvariantCulture) + "MB";
        }
        else if (_consumeConcurrentlyMaxSpan > 0 && span > _consumeConcurrentlyMaxSpan)
        {
            reason = "span=" + span.ToString(CultureInfo.InvariantCulture);
        }
        else if (_pullThresholdForTopic > 0 && topicCount >= _pullThresholdForTopic)
        {
            reason = "topicCount=" + topicCount.ToString(CultureInfo.InvariantCulture);
        }
        else if (_pullThresholdSizeForTopic > 0 && topicSizeMb >= _pullThresholdSizeForTopic)
        {
            reason = "topicSize=" + topicSizeMb.ToString("F1", CultureInfo.InvariantCulture) + "MB";
        }

        if (reason is null)
        {
            return false;
        }

        Interlocked.Increment(ref _flowControlTriggered);
        ClientLog.Debug("flow control: queue " + mq + " " + reason + ", pause pull");
        return true;
    }

    private void QueuePullLoop(MessageQueue mq)
    {
        // ⚠ 这里**不能**用 Client()：Shutdown() 会先置 _started=false，之后才 join 拉取线程；
        //    RebalancePullThreads 可能正好在那个窗口里把本线程 start 起来，于是本线程的
        //    第一条语句就撞上"未启动"状态，Client() 抛 MQClientException。该异常抛在**后台
        //    线程**上，.NET 会因此终止整个进程（实测 Abort trap: 6 / RC=134），
        //    而 Java/C++/Python 只是让这一个线程退出。判活后正常返回，把优雅退出变成正常路径。
        MQClientInstance? clientRef = _mqClient;
        if (clientRef is null || !_started) return;
        MQClientInstance c = clientRef;
        bool orderly = IsOrderly();
        string key = OffsetKey(mq);
        while (!_stop && _started)
        {
            // 队列已被 rebalance 撤销（isDropped）**或**已不再由本线程服务（自愈重建换了
            // 线程）：退出，不再为该队列拉取。
            bool dropped;
            bool owns;
            lock (_lock)
            {
                dropped = _dropped.Contains(key);
                owns = OwnsQueueLocked(key);
            }

            if (dropped || !owns)
            {
                if (dropped)
                {
                    UnregisterOwnLoop(key);
                }

                return;
            }

            // Java DefaultMQPushConsumerImpl.pullMessage:253 —— 每次**发起**拉取就盖章，在
            // 流控/锁判定之前：判据是"这条循环还在跑"，不是"这轮真的打了网络"。
            StampPullAt(key, pop: false);

            SubscriptionData sub;
            lock (_lock)
            {
                if (!_subscriptionData.TryGetValue(mq.Topic, out SubscriptionData? s) || s is null)
                {
                    return;
                }

                sub = s!;
            }

            // 顺序消费：broker 未确认锁定（LOCK_BATCH_MQ）的队列不拉取
            if (orderly)
            {
                bool locked;
                lock (_lock)
                {
                    locked = _lockOk.Contains(key);
                }

                if (!locked)
                {
                    _stopEvent.Wait(TimeSpan.FromMilliseconds(200));
                    continue;
                }
            }

            // 流控（Java ProcessQueue 的五个阈值，见 FlowControlHit）：命中任一条就暂停本队列拉取
            if (FlowControlHit(mq, key))
            {
                _stopEvent.Wait(TimeSpan.FromMilliseconds(100));
                continue;
            }

            long offset;
            lock (_lock)
            {
                offset = _offsetTable.TryGetValue(key, out long v) ? v : -1;
            }

            if (offset < 0)
            {
                try
                {
                    offset = ResolveInitialOffset(mq, sub);
                }
                catch (Exception e)
                {
                    ClientLog.Debug("resolve initial offset failed for " + mq + ": " + e.Message);
                    _stopEvent.Wait(TimeSpan.FromMilliseconds(1000));
                    continue;
                }

                lock (_lock)
                {
                    _offsetTable[key] = offset;
                }
            }

            PullResult result;
            try
            {
                int sysFlag = PullSysFlag.BuildSysFlag(
                    /*commitOffset=*/false, /*suspend=*/true, /*subscription=*/true, /*classFilter=*/false);
                string expr = UtilAll.IsBlank(sub.SubString) ? "*" : sub.SubString;
                long pullBegan = UtilAll.CurrentTimeMillis();
                result = c.PullMessage(ConsumerGroup, mq, offset, _pullBatchSize, sysFlag,
                    /*commitOffset=*/0, expr, sub.SubVersion, sub.ExpressionType,
                    _pullTimeoutMillis, _pullBatchSizeInBytes, _pullSuspendTimeoutMillis);
                // 消费统计（Java PullCallback.onSuccess：RT 每次都记，TPS 只在有消息时记）
                _mqClient!.ConsumerStats.IncPullRT(ConsumerGroup, mq.Topic,
                    UtilAll.CurrentTimeMillis() - pullBegan);
                if (result.IsFound && result.MsgFoundList.Count > 0)
                {
                    _mqClient.ConsumerStats.IncPullTPS(ConsumerGroup, mq.Topic,
                        result.MsgFoundList.Count);
                }
            }
            catch (MQBrokerException e)
            {
                // TOPIC_NOT_EXIST / PULL_NOT_FOUND 等多为预期路径（topic 未创建等），debug + 退避
                ClientLog.Debug("pull broker error for " + mq + ": " + e.Message);
                _stopEvent.Wait(TimeSpan.FromMilliseconds(500));
                continue;
            }
            catch (RemotingTimeoutException e)
            {
                // 长轮询在 suspend 期间无新消息触发客户端超时属正常行为：broker 会把
                // suspend 时间钳制到自身 brokerSuspendMaxTimeMillis，忽略客户端下发值，
                // 故空闲队列会周期性超时。非错误，仅 debug，避免污染运行日志。
                ClientLog.Debug("pull long-poll timeout for " + mq
                    + " (benign, will retry): " + e.Message);
                continue;
            }
            catch (Exception e)
            {
                ClientLog.Debug("pull error for " + mq + ": " + e.Message);
                _stopEvent.Wait(TimeSpan.FromMilliseconds(500));
                continue;
            }

            // 长轮询返回后再次确认：若期间被撤销或本线程已被自愈换下，丢弃本批消息并退出
            // （对齐 Java ProcessQueue.isDropped 守卫——拉到的消息不再进入缓冲）。
            lock (_lock)
            {
                if (_dropped.Contains(key))
                {
                    if (OwnsQueueLocked(key))
                    {
                        _pullThreads.Remove(key);
                    }

                    return;
                }

                if (!OwnsQueueLocked(key))
                {
                    return;
                }
            }

            lock (_lock)
            {
                if (!_pending.ContainsKey(key))
                {
                    _pending[key] = new Queue<MessageExt>();
                    _mqMap[key] = mq;
                }
            }

            if (result.Status == PullStatus.Found && result.MsgFoundList.Count > 0)
            {
                // 投递前的客户端侧过滤（对齐 Java PullAPIWrapper.processPullResult:113-128）：
                // 先二次 tag 过滤，再跑 FilterMessageHook。**必须在拿 _lock 之前做** ——
                // 钩子是用户代码，可能阻塞，不能压在入队的临界区里。
                // 拉取路径被摘掉的消息**不 ack**（Java 亦然）：位点照常推进 = 静默跳过。
                int before = result.MsgFoundList.Count;
                result.MsgFoundList = FilterMessagesForDelivery(mq, sub, result.MsgFoundList);
                if (result.MsgFoundList.Count < before)
                {
                    Interlocked.Add(ref _filteredMessageCount, before - result.MsgFoundList.Count);
                }

                // 重投消息 topic 还原（对齐 Java PullAPIWrapper.processPullResult）：broker 把重试
                // 消息写到 %RETRY%group，但消息自带 RETRY_TOPIC 属性指向原始 topic，分发前还原，
                // 否则上层 listener 看到的 topic 是 %RETRY% 而非业务 topic。
                string retryTopic = MixAll.GetRetryTopic(ConsumerGroup);
                lock (_lock)
                {
                    Queue<MessageExt> dq = _pending[key];
                    foreach (MessageExt m in result.MsgFoundList)
                    {
                        if (m.Topic == retryTopic)
                        {
                            string? orig = m.GetProperty(MessageConst.PropertyRetryTopic);
                            if (!string.IsNullOrEmpty(orig))
                            {
                                // Java 还会把命名空间从还原后的 topic 上剥掉再交给 listener
                                m.Topic = _namespace.Length == 0
                                    ? orig!
                                    : NamespaceUtil.WithoutNamespace(orig!, _namespace);
                            }
                        }

                        dq.Enqueue(m);
                    }

                    // ProcessQueue.msgAccCnt：用**过滤后入队**的那批算（Java 是先过滤再 putMessage）
                    UpdateMsgAccCntLocked(key, result.MsgFoundList);
                }
            }

            // 拉取游标推进到 nextBeginOffset；"已消费位点"由 _consumeOffsetTable 跟踪并持久化
            if (result.NextBeginOffset >= 0)
            {
                lock (_lock)
                {
                    _offsetTable[key] = result.NextBeginOffset;
                }
            }
        }
    }

    /// <summary>
    /// POP 路径的拉取统计（Java <c>popMessage</c> 的 <c>PopCallback.onSuccess:556-563</c>）。
    /// <para>
    /// Java 的 pull 回调每次都记 RT，POP 回调只在 <c>FOUND</c> 记，而且是在判空**之前**记；
    /// TPS 只按真正弹到的条数记。照抄这个不对称：POP 的空手而归是长轮询常态
    /// （<c>POLLING_NOT_FOUND</c>），把它算进 RT 等于用挂起时长稀释平均拉取耗时。
    /// </para>
    /// <para>
    /// 漏记是**静默**故障：消息照弹照 ack、消费完全正常，只有 307 应答的
    /// <c>statusTable</c>（运维看板）上一片 0 —— 而看板上"这个消费者没在拉取"和
    /// "这个消费者压根没起来"是两种完全不同的处置。判据本身要能离线锁死，
    /// 所以与 <c>FlowControlHit</c> 同一理由放在 public（测试工程无 InternalsVisibleTo）。
    /// </para>
    /// </summary>
    public static void RecordPopPullStats(ConsumerStatsManager stats, string group, string topic,
                                          PopResult result, long beganMs)
    {
        if (result.Status != PopStatus.Found)
        {
            return;
        }

        stats.IncPullRT(group, topic, UtilAll.CurrentTimeMillis() - beganMs);
        if (result.MsgFoundList.Count > 0)
        {
            stats.IncPullTPS(group, topic, result.MsgFoundList.Count);
        }
    }

    // ---------------- POP 消费循环（5.x 轻量消费）----------------

    /// <summary>
    /// 单队列 POP 循环（对应 Java DefaultMQPushConsumerImpl.popMessage 的回调部分）。
    /// <para>与 pull 循环的关键差别：
    /// <list type="bullet">
    /// <item>**不查、不提交消费位点**：进度由 broker 侧的 checkpoint 跟踪，确认只靠 ack；</item>
    /// <item>弹出即投递给消费线程，本轮循环立刻继续（不等消费结果）；</item>
    /// <item>PollingNotFound（队列暂时没消息）是**正常态**，直接下一轮，不算错误。</item>
    /// </list>
    /// </para>
    /// </summary>
    private void QueuePopLoop(MessageQueue mq)
    {
        string key = OffsetKey(mq);
        long invisible = PopInvisibleTime;
        if (invisible < MinPopInvisibleTime || invisible > MaxPopInvisibleTime)
        {
            // Java 的钳制：超出 [5s, 300s] 一律回落到 60s
            invisible = 60000;
        }

        // Java PopRequest 默认 ConsumeInitMode.MAX；这里按 consumeFromWhere 映射，
        // 让"从头消费"的语义在 POP 模式下也成立。
        int initMode = _consumeFromWhere == RocketMQ.Remoting.Protocol.ConsumeFromWhere.ConsumeFromFirstOffset
            ? ConsumeInitModeMin
            : ConsumeInitModeMax;

        while (!_stop && _started)
        {
            bool owns;
            lock (_lock)
            {
                owns = !_dropped.Contains(key) && OwnsQueueLocked(key);
            }

            if (!owns)
            {
                UnregisterOwnLoop(key);
                return;
            }

            // Java DefaultMQPushConsumerImpl.popMessage:508 —— 发起弹出即盖章（在流控之前），
            // PopProcessQueue.lastPopTimestamp 与拉取时刻表一起写，Java 的停摆判据读的就是它。
            StampPullAt(key, pop: true);
            PopProcessQueue? pq;
            SubscriptionData? sub;
            lock (_lock)
            {
                _popQueues.TryGetValue(key, out pq);
                _subscriptionData.TryGetValue(mq.Topic, out sub);
            }

            if (pq is null || pq.IsDropped() || sub is null) return;
            // 流控：已弹未 ack 太多就先缓一缓（Java popThresholdForQueue）
            if (pq.WaitAckCount() > PopThresholdForQueue)
            {
                Thread.Sleep(50);
                continue;
            }

            long began = UtilAll.CurrentTimeMillis();
            PopResult result;
            try
            {
                result = Client().PopMessage(ConsumerGroup, mq.Topic, mq.QueueId, PopBatchNums,
                                             invisible, PopPollTimeMillis, initMode,
                                             string.IsNullOrEmpty(sub.SubString) ? "*" : sub.SubString,
                                             sub.ExpressionType, false, PopTimeoutMillis, mq.BrokerName);
            }
            catch (RemotingTimeoutException e)
            {
                // 长轮询挂起期间没有消息 → 客户端先超时，属正常行为，直接下一轮
                ClientLog.Debug("pop long-poll timeout for " + key + " (benign): " + e.Message);
                continue;
            }
            catch (Exception e)
            {
                ClientLog.Debug("pop error for " + key + ": " + e.Message);
                Thread.Sleep(500);
                continue;
            }

            // 弹出后队列被 rebalance 撤走（或本线程已被自愈换下）：这一批**既不消费也不 ack**
            // （Java 对应 PopProcessQueue.isDropped() 分支），交给 invisibleTime 到期后
            // broker 自动复活重投给新属主。
            bool discarded;
            lock (_lock)
            {
                discarded = _dropped.Contains(key) || !OwnsQueueLocked(key);
            }

            if (discarded || pq.IsDropped())
            {
                if (discarded)
                {
                    UnregisterOwnLoop(key);
                }

                ClientLog.Debug("queue " + key + " revoked during pop, discard "
                                + result.MsgFoundList.Count + " messages un-acked");
                return;
            }

            // 拉取统计（Java PopCallback.onSuccess:556-563，判定见 RecordPopPullStats）
            RecordPopPullStats(Client().ConsumerStats, ConsumerGroup, mq.Topic, result, began);

            if (result.Status == PopStatus.Found && result.MsgFoundList.Count > 0)
            {
                pq.IncFoundMsg(result.MsgFoundList.Count);
                // 投递前过滤（对齐 Java processPopResult:621-661）：POP 路径**必须 ack 被摘掉的**，
                // 否则 invisibleTime 到期后 broker 会复活重投 —— 表现为"过滤没生效"。
                List<MessageExt> kept = FilterMessagesForDelivery(mq, sub, result.MsgFoundList);
                if (kept.Count != result.MsgFoundList.Count)
                {
                    List<MessageExt> dropped = DroppedMessages(result.MsgFoundList, kept);
                    Interlocked.Add(ref _filteredMessageCount, dropped.Count);
                    foreach (MessageExt msg in dropped)
                    {
                        AckPopMsg(msg);
                        pq.Ack();
                    }

                    ClientLog.Info("pop filter dropped " + dropped.Count + " of "
                        + result.MsgFoundList.Count + " messages (acked)");
                }

                if (kept.Count > 0)
                {
                    SubmitPopConsumeRequest(kept, pq, mq);
                }
            }
            else if (UtilAll.CurrentTimeMillis() - began < 200)
            {
                // 空结果：若 broker 没按 pollTime 挂起（立即返回）就会变成热循环，
                // 这里按"本轮耗时过短"兜底退避，避免打爆 broker。
                Thread.Sleep(200);
            }
            // NoNewMsg / PollingNotFound / PollingFull 都直接进下一轮
        }
    }

    /// <summary>按 ConsumeMessageBatchMaxSize 切批后投给消费线程（对应 Java
    /// ConsumeMessagePopConcurrentlyService.submitPopConsumeRequest）。
    /// <para>投给 **core/max 两档线程池**（core=ConsumeThreadMin / max=ConsumeThreadMax，
    /// 队列无界，同 Java 的 `LinkedBlockingQueue()`）。
    /// 此前这里每批起一个裸线程（无上限），慢监听器一上来就线程爆炸，而
    /// SetConsumeThreadNums() 设的值完全没作用。Java 用线程池 + 无界队列，
    /// 因此真实并发度 == CorePoolSize，UpdateCorePoolSize() 在运行时能改它。</para></summary>
    private void SubmitPopConsumeRequest(List<MessageExt> msgs, PopProcessQueue pq, MessageQueue mq)
    {
        int size = Math.Max(1, _consumeMessageBatchMaxSize);
        ConsumeExecutor? exec = _popConsumeExecutor;
        for (int i = 0; i < msgs.Count; i += size)
        {
            List<MessageExt> batch = msgs.GetRange(i, Math.Min(size, msgs.Count - i));
            if (batch.Count == 0) continue;
            if (exec != null)
            {
                // 捕获局部变量，避免闭包共享同一个 batch 变量
                List<MessageExt> captured = batch;
                exec.Submit(() => ConsumePopBatch(captured, pq, mq));
            }
            else
            {
                // 未 Start（单测）时没有执行器：同步执行，保持与改动前一致的可测行为。
                ConsumePopBatch(batch, pq, mq);
            }
        }
    }

    /// <summary>消费一个 POP 批次并按结果 ack / 延长不可见时间（对应 Java
    /// ConsumeMessagePopConcurrentlyService$ConsumeRequest.run）。</summary>
    private void ConsumePopBatch(List<MessageExt> msgs, PopProcessQueue pq, MessageQueue mq)
    {
        if (pq.IsDropped() || msgs.Count == 0) return;

        long popTime = 0;
        long invisible = 0;
        try
        {
            string? ck = msgs[0].GetProperty(MessageConst.PropertyPopCk);
            if (!string.IsNullOrEmpty(ck))
            {
                string[] seg = ExtraInfoUtil.Split(ck);
                popTime = ExtraInfoUtil.GetPopTime(seg);
                invisible = ExtraInfoUtil.GetInvisibleTime(seg);
            }
        }
        catch (Exception e)
        {
            ClientLog.Debug("parse pop ck failed: " + e.Message);
        }

        if (IsPopTimeout(popTime, invisible))
        {
            // 已经超过 invisibleTime：ack 也不会被承认，直接放弃本批（等 broker 复活重投）
            pq.DecFoundMsg(-msgs.Count);
            return;
        }

        ResetRetryTopicAndNamespace(msgs);
        var ctx = new ConsumeConcurrentlyContext(mq);

        // 对应 Java ConsumeMessagePopConcurrentlyService：POP 路径把 ackIndex 当
        // 「已 ack 到第几条」用，语义与 classic 回投路径一致，默认值 Integer.MAX_VALUE
        // 已经表示「整批 ack」；这里钳到 size-1 只是让 ctx 上的值与本批条数对齐，
        // 不影响 CONSUME_SUCCESS 的行为（不 ack 会让消息在 invisibleTime 后被复活重投）。
        ctx.AckIndex = msgs.Count - 1;
        ConsumeMessageContext? popHookCtx = null;
        if (_consumeMessageHooks.Count > 0)
        {
            popHookCtx = BuildConsumeHookContext(msgs, mq);
            ExecuteConsumeHookBefore(popHookCtx);
        }

        long popBegin = UtilAll.CurrentTimeMillis();
        bool popHasException = false;
        ConsumeConcurrentlyStatus status = ConsumeConcurrentlyStatus.ReconsumeLater;
        try
        {
            status = ((IMessageListenerConcurrently)_messageListener!).ConsumeMessage(msgs, ctx);
        }
        catch (Exception e)
        {
            // Java：消费抛异常按 RECONSUME_LATER 处理
            ClientLog.Debug("pop listener error, treat as RECONSUME_LATER: " + e.Message);
            popHasException = true;
        }

        // 钩子的 returnType 判定在「status 归一化为 RECONSUME_LATER」**之前**做，
        // 与 Java 一致（返回 null 记 RETURNNULL，而不是 FAILED）
        RecordConsumeStats(mq.Topic, msgs.Count, popBegin,
            failed: status == ConsumeConcurrentlyStatus.ReconsumeLater);
        FinishConsumeHook(popHookCtx, true, popHasException, popBegin, status.ToString(),
            failed: status == ConsumeConcurrentlyStatus.ReconsumeLater,
            succeeded: status == ConsumeConcurrentlyStatus.ConsumeSuccess);

        if (pq.IsDropped() || IsPopTimeout(popTime, invisible))
        {
            // 消费期间队列被撤走或已超时：结果不再处理
            pq.DecFoundMsg(-msgs.Count);
            return;
        }

        ProcessPopConsumeResult(status, ctx, msgs, pq);
    }

    /// <summary>消费前把重投消息的 topic 还原成业务原始 topic（对应 Java resetRetryAndNamespace）。
    /// 与 pull 路径内联的那段同义，这里抽出来供 POP 复用。</summary>
    private void ResetRetryTopicAndNamespace(List<MessageExt> msgs)
    {
        string retryTopic = MixAll.GetRetryTopic(ConsumerGroup);
        foreach (MessageExt m in msgs)
        {
            if (m.Topic != retryTopic) continue;
            string? orig = m.GetProperty(MessageConst.PropertyRetryTopic);
            if (string.IsNullOrEmpty(orig)) continue;
            // Java 还会把命名空间从还原后的 topic 上剥掉再交给 listener
            m.Topic = _namespace.Length == 0 ? orig! : NamespaceUtil.WithoutNamespace(orig!, _namespace);
        }
    }

    /// <summary>Java ConsumeRequest.isPopTimeout：解析不出 popTime/invisibleTime 时按超时处理。</summary>
    public static bool IsPopTimeout(long popTime, long invisible)
    {
        if (popTime <= 0 || invisible <= 0) return true;
        return UtilAll.CurrentTimeMillis() - popTime >= invisible;
    }

    /// <summary>对应 Java ConsumeMessagePopConcurrentlyService.processConsumeResult。</summary>
    private void ProcessPopConsumeResult(ConsumeConcurrentlyStatus status,
                                         ConsumeConcurrentlyContext ctx,
                                         List<MessageExt> msgs, PopProcessQueue pq)
    {
        int ackIndex = ctx.AckIndex;
        if (status == ConsumeConcurrentlyStatus.ConsumeSuccess)
        {
            if (ackIndex >= msgs.Count)
            {
                ackIndex = msgs.Count - 1;
            }
        }
        else
        {
            ackIndex = -1; // RECONSUME_LATER：一条都不 ack
        }

        for (int i = 0; i <= ackIndex; i++)
        {
            AckPopMsg(msgs[i]);
            pq.Ack();
        }

        for (int i = ackIndex + 1; i < msgs.Count; i++)
        {
            pq.Ack();
            MessageExt msg = msgs[i];
            // 超过最大重试次数：Java 走 CheckNeedAckOrDelay（太老就直接 ack 丢弃，
            // 否则按消息已存活时间选一个延迟档位）
            if (_maxReconsumeTimes >= 0 && msg.ReconsumeTimes >= _maxReconsumeTimes)
            {
                CheckNeedAckOrDelay(msg);
                continue;
            }

            ChangePopInvisibleTime(msg, ctx.DelayLevelWhenNextConsume);
        }
    }

    /// <summary>Java checkNeedAckOrDelay：重试次数用尽后的兜底。
    /// 消息存活时间已超过最大延迟档位的 2 倍 → 直接 ack 丢弃（不再无限重试）；
    /// 否则按存活时间选一个档位继续延长不可见时间。</summary>
    private void CheckNeedAckOrDelay(MessageExt msg)
    {
        List<int> table = PopDelayLevel;
        long msgDelayTime = UtilAll.CurrentTimeMillis() - msg.BornTimestamp;
        if (msgDelayTime > (long)table[table.Count - 1] * 1000 * 2)
        {
            ClientLog.Warn("pop consume too many times, ack and drop: " + msg.MsgId);
            AckPopMsg(msg);
            return;
        }

        int level = table.Count - 1;
        for (; level >= 0; level--)
        {
            if (msgDelayTime >= (long)table[level] * 1000)
            {
                level++;
                break;
            }
        }

        // ⚠ 有意偏离 Java：存活时间小于首档时 Java 会算出 level=-1 并索引
        // delayLevelTable[-1] 抛 IndexOutOfRangeException。这里在 ChangePopInvisibleTime
        // 里钳到首档。
        ChangePopInvisibleTime(msg, level);
    }

    /// <summary>
    /// 从 POP_CK 解出 ack/延长不可见时间需要的目标（对应 Java 客户端自行反构 extraInfo）。
    /// <para>⚠ 两处都不能想当然：
    /// <list type="number">
    /// <item>topic 要用 ExtraInfoUtil.GetRealTopic 按 CK 的 retryFlag 还原 —— 复活消息
    /// （retryFlag=1）的真实 topic 是 %RETRY%&lt;group&gt;_&lt;topic&gt;，**不是**消息上的 topic；</item>
    /// <item>地址要按 CK 里的 brokerName 反查，不能按 topic 查路由 —— retry topic 通常没有
    /// 独立路由表项，按 topic 查会失败（Java 同理走 findBrokerAddressInSubscribe）。</item>
    /// </list>
    /// </para>
    /// </summary>
    public PopCkTarget? PopCkTarget(MessageExt msg)
    {
        string? ck = msg.GetProperty(MessageConst.PropertyPopCk);
        if (string.IsNullOrEmpty(ck))
        {
            ClientLog.Debug("pop message without POP_CK, cannot ack: " + msg.MsgId);
            return null;
        }

        var target = new PopCkTarget { ExtraInfo = ck! };
        try
        {
            string[] seg = ExtraInfoUtil.Split(target.ExtraInfo);
            target.BrokerName = ExtraInfoUtil.GetBrokerName(seg);
            target.QueueId = ExtraInfoUtil.GetQueueId(seg);
            target.Offset = ExtraInfoUtil.GetQueueOffset(seg);
            string retry = ExtraInfoUtil.GetRetry(seg);
            target.Topic = ExtraInfoUtil.GetRealTopic(msg.Topic, ConsumerGroup, retry);
        }
        catch (Exception e)
        {
            ClientLog.Debug("bad POP_CK " + target.ExtraInfo + ": " + e.Message);
            return null;
        }

        return target;
    }

    /// <summary>单条 ack（对应 Java DefaultMQPushConsumerImpl.ackAsync）。</summary>
    private void AckPopMsg(MessageExt msg)
    {
        PopCkTarget? target = PopCkTarget(msg);
        if (target is null) return;
        try
        {
            Client().AckMessage(ConsumerGroup, target.Topic, target.QueueId, target.ExtraInfo,
                                target.Offset, 3000, target.BrokerName);
        }
        catch (Exception e)
        {
            // ack 失败不致命：消息会在 invisibleTime 到期后被 broker 复活重投
            ClientLog.Debug("ack failed for " + msg.MsgId + ": " + e.Message);
        }
    }

    /// <summary>延长不可见时间（对应 Java changePopInvisibleTime）。
    /// delayLevel == 0 时 Java 用消息已重试次数当档位；档位表是**秒**，接口要毫秒。</summary>
    private void ChangePopInvisibleTime(MessageExt msg, int delayLevel)
    {
        PopCkTarget? target = PopCkTarget(msg);
        if (target is null) return;
        if (delayLevel == 0)
        {
            delayLevel = msg.ReconsumeTimes;
        }

        List<int> table = PopDelayLevel;
        int delaySecond = delayLevel >= table.Count
            ? table[table.Count - 1]
            : table[Math.Max(0, delayLevel)];
        try
        {
            Client().ChangeInvisibleTime(ConsumerGroup, target.Topic, target.QueueId,
                                         target.ExtraInfo, target.Offset,
                                         (long)delaySecond * 1000, 3000, target.BrokerName);
        }
        catch (Exception e)
        {
            ClientLog.Debug("change invisible time failed for " + msg.MsgId + ": " + e.Message);
        }
    }

    private void DispatchLoop()
    {
        while (!_stop)
        {
            bool progressed = false;
            List<string> keys;
            lock (_lock)
            {
                keys = new List<string>(_pending.Keys);
            }

            foreach (string key in keys)
            {
                if (_stop || !_started) return;
                MessageQueue mq;
                lock (_lock)
                {
                    if (!_mqMap.TryGetValue(key, out MessageQueue? m)) continue;
                    mq = m!;
                    // 被撤销的队列：丢弃缓冲、跳过（不消费、不推进位点），对齐 Java 丢弃
                    // 已从 ProcessQueueTable 移除的队列消息。
                    if (_dropped.Contains(key))
                    {
                        _pending.Remove(key);
                        continue;
                    }
                }

                List<MessageExt> batch = new();
                lock (_lock)
                {
                    if (_pending.TryGetValue(key, out Queue<MessageExt>? q) && q.Count > 0)
                    {
                        int n = Math.Min(q.Count, Math.Max(1, _consumeMessageBatchMaxSize));
                        for (int i = 0; i < n; ++i)
                        {
                            batch.Add(q.Dequeue());
                        }
                    }
                }

                if (batch.Count == 0) continue;
                try
                {
                    bool done = ConsumeBatch(key, mq, batch);
                    progressed = progressed || done;
                }
                catch (Exception e)
                {
                    // 分发路径意外异常：批次塞回队首，稍后重试（不要让它杀死分发线程）
                    ClientLog.Warn("dispatch batch error (will retry): " + e.Message);
                    lock (_lock)
                    {
                        if (_pending.TryGetValue(key, out Queue<MessageExt>? q))
                        {
                            for (int i = batch.Count - 1; i >= 0; --i)
                            {
                                PushFront(q, batch[i]);
                            }
                        }
                    }

                    _stopEvent.Wait(TimeSpan.FromMilliseconds(100));
                }
            }

            if (!progressed)
            {
                _stopEvent.Wait(TimeSpan.FromMilliseconds(50));
            }
        }
    }

    private static void PushFront(Queue<MessageExt> q, MessageExt m)
    {
        // Queue<T> 无 PushFront：借助临时队列重组（批次很小，开销可忽略）
        var tmp = new Queue<MessageExt>(q.Count + 1);
        tmp.Enqueue(m);
        while (q.Count > 0)
        {
            tmp.Enqueue(q.Dequeue());
        }

        while (tmp.Count > 0)
        {
            q.Enqueue(tmp.Dequeue());
        }
    }

    // ---------------- 仅供单测/联调：不经过网络直接驱动 classic 消费分发 ----------------
    // classic 路径的 ackIndex 语义错得很安静（尾巴静默丢失、位点越过未消费完的消息），
    // 必须能离线锁死再上真机，所以开这几个口子。
    public bool ConsumeBatchForTest(string key, MessageQueue mq, List<MessageExt> batch)
        => ConsumeBatch(key, mq, batch);

    /// <summary>顺序回投的 newMsg 构造体（Java 两条链路共用），供单测直接查属性。</summary>
    public Message BuildRetryMessageForTest(MessageExt msg, int maxReconsumeTimes)
        => BuildRetryMessage(msg, maxReconsumeTimes);

    public static string OffsetKeyForTest(MessageQueue mq) => OffsetKey(mq);

    /// <summary>预置某队列的「已拉未消费」缓冲（Java ProcessQueue）；null = 撤走该队列。</summary>
    public void SetPendingForTest(string key, List<MessageExt>? msgs)
    {
        lock (_lock)
        {
            if (msgs is null)
            {
                _pending.Remove(key);
            }
            else
            {
                _pending[key] = new Queue<MessageExt>(msgs);
            }
        }
    }

    public List<MessageExt> PendingForTest(string key)
    {
        lock (_lock)
        {
            return _pending.TryGetValue(key, out Queue<MessageExt>? q) ? q.ToList() : new List<MessageExt>();
        }
    }

    /// <summary>预置某队列的「已分配队列」登记（Java ProcessQueueTable 的键值）。topic 级
    /// 阈值要靠这张表把同 topic 的兄弟队列聚合起来，不登记就等于队列已被 rebalance 撤走。</summary>
    public void SetAssignedForTest(string key, MessageQueue mq)
    {
        lock (_lock)
        {
            _mqMap[key] = mq;
        }
    }

    /// <summary>跑一次拉取前流控判定（不经过网络）。命中会把 <see cref="FlowControlTriggered"/> 加一格。</summary>
    public bool FlowControlHitForTest(MessageQueue mq, string key) => FlowControlHit(mq, key);

    /// <summary>已消费位点；null = 该队列还没有记录。</summary>
    public long? ConsumeOffsetForTest(string key)
    {
        lock (_lock)
        {
            return _consumeOffsetTable.TryGetValue(key, out long off) ? off : null;
        }
    }

    /// <summary>消费一个批次并处理回投/挂起。返回消费位点是否前进。</summary>
    private bool ConsumeBatch(string key, MessageQueue mq, List<MessageExt> batch)
    {
        bool broadcast = _messageModel == RocketMQ.Remoting.Protocol.MessageModel.Broadcasting;
        // ---- 顺序消费（Java ConsumeMessageOrderlyService）----
        if (IsOrderly())
        {
            var orderly = (IMessageListenerOrderly)_messageListener!;
            var ctx = new ConsumeOrderlyContext(mq);
            ConsumeMessageContext? ohookCtx = null;
            if (_consumeMessageHooks.Count > 0)
            {
                ohookCtx = BuildConsumeHookContext(batch, mq);
                ExecuteConsumeHookBefore(ohookCtx);
            }

            long obegin = UtilAll.CurrentTimeMillis();
            bool ohasException = false;
            ConsumeOrderlyStatus status;
            try
            {
                status = orderly.ConsumeMessage(batch, ctx);
            }
            catch (Exception e)
            {
                // Java 顺序消费：异常 → 不提交 offset，原地重试
                ClientLog.Debug("orderly listener error (retry in place): " + e.Message);
                status = ConsumeOrderlyStatus.SuspendCurrentQueueAMoment;
                ohasException = true;
            }

            // 顺序消费的钩子同样在拿到 status 后触发（Java ConsumeMessageOrderlyService:511）
            RecordConsumeStats(mq.Topic, batch.Count, obegin,
                failed: status != ConsumeOrderlyStatus.Success);
            FinishConsumeHook(ohookCtx, true, ohasException, obegin, status.ToString(),
                failed: status != ConsumeOrderlyStatus.Success,
                succeeded: status == ConsumeOrderlyStatus.Success);

            if (status == ConsumeOrderlyStatus.SuspendCurrentQueueAMoment)
            {
                // Java processConsumeResult:254-266：挂起之前要先过 checkReconsumeTimes。
                // 只有「还在重试次数内 / 回投失败」才把这一批塞回队首原地重试；已经交给
                // broker 的（回投成功）要前进位点，否则一条毒消息永久占住这条队列 ——
                // 而那看起来跟「消费者死了」一模一样。
                if (CheckOrderlyReconsumeTimes(batch))
                {
                    lock (_lock)
                    {
                        if (_pending.TryGetValue(key, out Queue<MessageExt>? q))
                        {
                            for (int i = batch.Count - 1; i >= 0; --i)
                            {
                                PushFront(q, batch[i]);
                            }
                        }
                    }

                    _stopEvent.Wait(TimeSpan.FromMilliseconds(_suspendCurrentQueueTimeMillis));
                    return false;
                }

                AdvanceConsumeOffset(key, batch);
                Interlocked.Add(ref _consumedCount, batch.Count);
                return true;
            }

            AdvanceConsumeOffset(key, batch);
            Interlocked.Add(ref _consumedCount, batch.Count);
            return true;
        }

        // ---- 并发消费（Java ConsumeMessageConcurrentlyService$ConsumeRequest.run）----
        var conc = (IMessageListenerConcurrently)_messageListener!;
        var cctx = new ConsumeConcurrentlyContext(mq);
        // 顺序与 Java 一致：before 钩子在 listener **之前**（生成 SubBefore 轨迹），
        // after 在拿到 status 之后（生成 SubAfter，带 contextCode）
        ConsumeMessageContext? hookCtx = null;
        if (_consumeMessageHooks.Count > 0)
        {
            hookCtx = BuildConsumeHookContext(batch, mq);
            ExecuteConsumeHookBefore(hookCtx);
        }

        long beginMs = UtilAll.CurrentTimeMillis();
        bool hasException = false;
        ConsumeConcurrentlyStatus cstatus;
        try
        {
            cstatus = conc.ConsumeMessage(batch, cctx);
        }
        catch (Exception e)
        {
            // Java：消费抛异常按 RECONSUME_LATER 处理
            ClientLog.Debug("listener error, treat as RECONSUME_LATER: " + e.Message);
            cstatus = ConsumeConcurrentlyStatus.ReconsumeLater;
            hasException = true;
        }

        // Java processConsumeResult:207-229 —— CONSUME_SUCCESS 用 listener 设的 ackIndex
        // 划分「已认可前缀 / 待回投后缀」（默认 Integer.MAX_VALUE，钳到 size-1 即整批认可）；
        // RECONSUME_LATER 强制 ackIndex=-1，整批回投。
        int ackIndex = cctx.AckIndex;
        if (cstatus == ConsumeConcurrentlyStatus.ConsumeSuccess)
        {
            if (ackIndex >= batch.Count)
            {
                ackIndex = batch.Count - 1;
            }
        }
        else
        {
            ackIndex = -1;
        }

        int acked = ackIndex + 1;  // 0..batch.Count
        RecordConsumeStats(mq.Topic, batch.Count, beginMs,
            failed: cstatus == ConsumeConcurrentlyStatus.ReconsumeLater, ackCount: acked);
        FinishConsumeHook(hookCtx, true, hasException, beginMs, cstatus.ToString(),
            failed: cstatus == ConsumeConcurrentlyStatus.ReconsumeLater,
            succeeded: cstatus == ConsumeConcurrentlyStatus.ConsumeSuccess);

        if (broadcast)
        {
            // Java:232-237 —— 广播模式不回投：未认可的尾巴只打一条 warn 就丢掉，
            // 整批位点照样前进（重启后不重投）
            int dropped = batch.Count - acked;
            if (dropped > 0)
            {
                ClientLog.Warn("BROADCASTING, the message consume failed, drop it: "
                    + dropped.ToString(CultureInfo.InvariantCulture) + " msgs in " + mq);
            }

            AdvanceConsumeOffset(key, batch);
            Interlocked.Add(ref _consumedCount, batch.Count);
            return true;
        }

        if (acked >= batch.Count)
        {
            // 整批认可（默认路径）：一条都不用回投，位点直接前进
            AdvanceConsumeOffset(key, batch);
            Interlocked.Add(ref _consumedCount, batch.Count);
            return true;
        }

        // 集群模式：未认可的 [acked, size) 逐条回投 %RETRY%topic
        //（延迟梯度 3+reconsumeTimes，超 maxReconsumeTimes 由 broker 转 %DLQ%）
        List<(int Index, MessageExt Msg)> msgBackFailed = SendBackBatch(batch, cctx, acked);
        var failedIdx = new HashSet<int>();
        long floor = 0;
        bool hasFloor = false;
        foreach ((int index, MessageExt _) in msgBackFailed)
        {
            failedIdx.Add(index);
            long off = batch[index].QueueOffset;
            if (!hasFloor || off < floor)
            {
                floor = off;
                hasFloor = true;
            }
        }

        // Java:256-260 —— 回投失败的那几条塞回队首稍后重试（ProcessQueue 里不摘掉它们）
        if (msgBackFailed.Count > 0)
        {
            lock (_lock)
            {
                if (_pending.TryGetValue(key, out Queue<MessageExt>? q))
                {
                    for (int i = msgBackFailed.Count - 1; i >= 0; --i)
                    {
                        PushFront(q, msgBackFailed[i].Msg);
                    }
                }
            }

            _stopEvent.Wait(TimeSpan.FromMilliseconds(200));
        }

        // Java:266-269 —— 提交的是「本批已处理条目里最大的 queueOffset + 1」，且不能越过
        // 回投失败、仍留在缓冲里的那几条，否则那几条会被位点静默跳过（丢消息）
        var handled = new List<MessageExt>(batch.Count - failedIdx.Count);
        for (int i = 0; i < batch.Count; ++i)
        {
            if (!failedIdx.Contains(i))
            {
                handled.Add(batch[i]);
            }
        }

        AdvanceConsumeOffset(key, handled, hasFloor ? floor : null);
        Interlocked.Add(ref _consumedCount, handled.Count);
        return msgBackFailed.Count == 0;
    }

    /// <summary>
    /// 从 <paramref name="base"/> 起把 batch[base, size) 逐条回投 broker
    /// （对齐 Java processConsumeResult → sendMessageBack）。<paramref name="base"/> 是尾巴在
    /// 整批里的起始下标（部分 ack 时前缀已认可，不能再回投）。返回回投**失败**的
    /// (整批下标, 消息)，失败条目的 ReconsumeTimes 已 +1（Java :251 —— broker 那边没记上
    /// 这次数，客户端不补就永远进不了 %DLQ%）；调用方据此把尾巴塞回队首并钳住位点。
    /// </summary>
    private List<(int Index, MessageExt Msg)> SendBackBatch(List<MessageExt> batch,
        ConsumeConcurrentlyContext ctx, int @base)
    {
        var failed = new List<(int, MessageExt)>();
        for (int i = @base; i < batch.Count; ++i)
        {
            MessageExt msg = batch[i];
            try
            {
                // Java：delayLevelWhenNextConsume == 0 → 3 + reconsumeTimes
                //（reconsumeTimes 在 MessageExt 线上格式第 13 字段，broker 重投时 +1）
                int delayLevel = ctx.DelayLevelWhenNextConsume;
                if (delayLevel == 0)
                {
                    delayLevel = 3 + msg.ReconsumeTimes;
                }

                SendMessageBack(msg, delayLevel);
            }
            catch (Exception e)
            {
                ClientLog.Debug("send message back failed for msg " + msg.MsgId + ": " + e.Message);
                // 与 Java 一样：次数加在**要被重新消费的副本**上，broker 没记成功
                msg.ReconsumeTimes += 1;
                failed.Add((i, msg));
            }
        }

        return failed;
    }

    // ---------------- 顺序消费的重试计数与回投 ----------------
    // 对位 Java ConsumeMessageOrderlyService 的 getMaxReconsumeTimes / checkReconsumeTimes
    // / sendMessageBack（:313-362），与上面并发那一套**不是同一条链路**，两处差异都要守住：
    //   1. `-1` 在顺序侧是「不设限」（Integer.MAX_VALUE），在并发侧才是 16；
    //   2. 顺序侧的回投是**普通消息发送**（发到 %RETRY%<group>），不是 ConsumerSendMsgBack(3)。

    /// <summary>
    /// Java <c>ConsumeMessageOrderlyService#getMaxReconsumeTimes:313-320</c>。
    ///
    /// 顺序消费的消息一直停在本地队列里原地重试，broker 侧压根没有重投计数，所以默认就该
    /// 一直重试到成功为止 —— <c>-1</c> 在这里读成 <c>int.MaxValue</c>。并发消费的
    /// <c>-1 → 16</c>（<c>DefaultMQPushConsumerImpl#getMaxReconsumeTimes</c>）是另一套语义：
    /// 那边每轮回投都要过一遍 broker，16 是 broker 的默认 retryMaxTimes。把两者并成一个常量，
    /// 等于要么给顺序消费凭空造出死信，要么让并发消息无限重投。
    /// </summary>
    public int OrderlyMaxReconsumeTimes() =>
        _maxReconsumeTimes == -1 ? int.MaxValue : _maxReconsumeTimes;

    /// <summary>并发侧的 <c>-1</c> 读成 broker 默认的 16（与顺序侧那条不是一条链路）。</summary>
    public int MaxReconsumeTimesOrDefault() => _maxReconsumeTimes == -1 ? 16 : _maxReconsumeTimes;

    /// <summary>
    /// Java <c>ConsumeMessageOrderlyService#checkReconsumeTimes:322-339</c>。
    /// 返回「这一批是否还要原地挂起重试」。逐条两种走法：
    ///   - 次数没用尽：本地 <c>ReconsumeTimes + 1</c>（broker 那边压根没记这次失败，客户端
    ///     不补就永远到不了阈值），继续挂起；
    ///   - 次数已用尽：交给 broker 回投。<b>回投成功就不再挂起</b>（Java 此时 commit 位点，
    ///     毒消息让路、队列继续往前），回投失败才 +1 并挂起。
    /// </summary>
    public bool CheckOrderlyReconsumeTimes(List<MessageExt>? msgs)
    {
        bool suspend = false;
        int maxTimes = OrderlyMaxReconsumeTimes();
        if (msgs is null)
        {
            return false;
        }

        foreach (MessageExt msg in msgs)
        {
            if (msg.ReconsumeTimes >= maxTimes)
            {
                if (!OrderlySendMessageBack(msg))
                {
                    suspend = true;
                    msg.ReconsumeTimes += 1;
                }
            }
            else
            {
                suspend = true;
                msg.ReconsumeTimes += 1;
            }
        }

        return suspend;
    }

    /// <summary>
    /// Java <c>ConsumeMessageOrderlyService#sendMessageBack:341-362</c>。
    ///
    /// 拿实例自带的内部生产者，把这条消息<b>当普通消息</b>发到 <c>%RETRY%&lt;group&gt;</c>：
    /// broker 的 <c>handleRetryAndDLQ</c>（<c>SendMessageProcessor:199-234</c>）见该组还持有
    /// 未过期的队列锁（正是顺序消费组的特征），直接把它改投 <c>%DLQ%&lt;group&gt;</c>。
    /// 其中 <c>RECONSUME_TIME</c> / <c>MAX_RECONSUME_TIMES</c> 会被发送侧抬进请求头
    ///（<c>MQClientInstance.BuildSendRequest</c>，Java <c>sendKernelImpl:1004-1018</c>）。
    ///
    /// 失败只返回 false、绝不抛：Java 整段包在 try/catch 里，抛给消费线程等于这条既没
    /// ack 也没回投，只能等锁超时 —— 而顺序消费的锁超时是分钟级，看起来就像卡死。
    /// </summary>
    private bool OrderlySendMessageBack(MessageExt msg)
    {
        try
        {
            MQClientInstance c = Client();
            Message newMsg = BuildRetryMessage(msg, OrderlyMaxReconsumeTimes());
            TopicPublishInfo? publish = c.GetTopicPublishInfo(newMsg.Topic, isDefault: true);
            if (publish is null || publish.MsgQueueList.Count == 0)
            {
                ClientLog.Debug("orderly send back has no writable queue, topic=" + newMsg.Topic);
                return false;
            }

            // unitMode 跟着消费者：Java 构造内部生产者时调过 resetClientConfig(clientConfig)，
            // 不带的话重投出去的消息会丢单元标记。
            c.SendMessage(MixAll.ClientInnerProducerGroup, newMsg, publish.SelectOneMessageQueue(),
                3000, unitMode: _unitMode);
            return true;
        }
        catch (Exception e)
        {
            ClientLog.Debug("orderly send message back failed, group=" + ConsumerGroup
                            + " msg=" + msg.MsgId + ": " + e.Message);
            return false;
        }
    }

    /// <summary>
    /// Java <c>DefaultMQPushConsumerImpl#sendMessageBackAsNormalMessage:1148-1160</c> 与
    /// <c>ConsumeMessageOrderlyService#sendMessageBack:341-362</c> 的两个 newMsg 构造体
    /// <b>逐行相同</b>（只有 maxReconsumeTimes 各调各的 getter），所以抽成一个函数，
    /// 避免两处属性置法漂移。
    /// </summary>
    private Message BuildRetryMessage(MessageExt msg, int maxReconsumeTimes)
    {
        var newMsg = new Message(MixAll.GetRetryTopic(ConsumerGroup), msg.Body);
        newMsg.Properties = new PropertyMap(msg.Properties);
        newMsg.Flag = msg.Flag;
        string originMsgId = msg.GetProperty(MessageConst.PropertyOriginMessageId);
        if (string.IsNullOrEmpty(originMsgId))
        {
            originMsgId = msg.MsgId;
        }

        if (!string.IsNullOrEmpty(originMsgId))
        {
            newMsg.PutProperty(MessageConst.PropertyOriginMessageId, originMsgId);
        }

        newMsg.PutProperty(MessageConst.PropertyRetryTopic, msg.Topic);
        newMsg.PutProperty(MessageConst.PropertyReconsumeTime,
            (msg.ReconsumeTimes + 1).ToString(CultureInfo.InvariantCulture));
        newMsg.PutProperty(MessageConst.PropertyMaxReconsumeTimes,
            maxReconsumeTimes.ToString(CultureInfo.InvariantCulture));
        // 半消息重投时不能带上 TRAN_MSG，否则 broker 会把它再当回查消息处理
        newMsg.RemoveProperty(MessageConst.PropertyTransactionPrepared);
        newMsg.DelayTimeLevel = 3 + msg.ReconsumeTimes;
        if (string.IsNullOrEmpty(newMsg.GetProperty(MessageConst.PropertyUniqClientMessageIdKeyidx)))
        {
            MessageClientIDSetter.SetUniqId(newMsg);
        }

        return newMsg;
    }

    /// <summary>
    /// 推进位点到 batch 中最大 queueOffset+1；<paramref name="floor"/> 非空时不越过它
    /// （对应 Java ProcessQueue.removeMessage：树里还留着未消费完的消息时提交位点只能是
    /// firstKey，否则会静默丢掉那条）。空批次直接返回。
    /// </summary>
    private void AdvanceConsumeOffset(string key, List<MessageExt> batch, long? floor = null)
    {
        if (batch.Count == 0)
        {
            // 整批回投都失败时没有任何条目被认可，位点原地不动
            return;
        }

        long nextOffset = 0;
        foreach (MessageExt m in batch)
        {
            if (m.QueueOffset + 1 > nextOffset)
            {
                nextOffset = m.QueueOffset + 1;
            }
        }

        if (floor.HasValue && floor.Value < nextOffset)
        {
            nextOffset = floor.Value;
        }

        lock (_lock)
        {
            _consumeOffsetTable.TryGetValue(key, out long cur);
            if (cur < nextOffset)
            {
                _consumeOffsetTable[key] = nextOffset;
            }
        }
    }

    // ---------------- 位点持久化 ----------------
    private void OffsetPersistLoop()
    {
        // Java MQClientInstance.startScheduledTask:417-423：
        //   scheduleAtFixedRate(persistAllConsumerOffset, 1000 * 10, persistConsumerOffsetInterval)
        // ——首个任务延迟 10s，之后周期取 ClientConfig#persistConsumerOffsetInterval（:66，
        // 默认 5s）。首笔落盘发生在 initialDelay **这一刻**，不是 initialDelay + 一个周期后；
        // 周期是**固定速率**：计划时刻锚定在 initialDelay + n*周期（见 Schedules），
        // 所以不会像"干完再按 100ms 切片睡一个周期"那样把周期越拖越长。
        long next = Environment.TickCount64 + 10_000;
        while (!_stop)
        {
            if (!_started) return;
            Schedules.WaitUntil(_stopEvent, next);
            next += _persistConsumerOffsetIntervalMillis;
            try
            {
                PersistOffsetsOnce();
            }
            catch (Exception e)
            {
                ClientLog.Debug("persist offsets error: " + e.Message);
            }
        }
    }

    private void PersistOffsetsOnce()
    {
        if (_messageModel == RocketMQ.Remoting.Protocol.MessageModel.Broadcasting)
        {
            SaveLocalOffsets();
            return;
        }

        if (_mqClient is null)
        {
            return;
        }

        List<KeyValuePair<string, long>> items;
        lock (_lock)
        {
            items = new List<KeyValuePair<string, long>>(_consumeOffsetTable);
        }

        foreach (KeyValuePair<string, long> kv in items)
        {
            if (!_mqMap.TryGetValue(kv.Key, out MessageQueue? mq) || mq is null)
            {
                continue;
            }

            try
            {
                _mqClient.UpdateConsumerOffset(ConsumerGroup, mq, kv.Value);
            }
            catch (Exception e)
            {
                ClientLog.Debug("update consumer offset failed for " + mq + ": " + e.Message);
            }
        }
    }

    private string LocalOffsetPath()
    {
        // Java LocalFileOffsetStore：$HOME/.rocketmq_offsets/<clientId>/<group>/offsets.json
        string home = UtilAll.UserHome();
        if (home.Length == 0) home = ".";
        return home + "/.rocketmq_offsets/" + (_clientId.Length == 0 ? "DEFAULT" : _clientId)
            + "/" + ConsumerGroup + "/offsets.json";
    }

    private void SaveLocalOffsets()
    {
        Dictionary<string, long> items;
        lock (_lock)
        {
            items = new Dictionary<string, long>(_consumeOffsetTable, StringComparer.Ordinal);
        }

        string path = LocalOffsetPath();
        string dir = path[..path.LastIndexOf('/')];
        Directory.CreateDirectory(dir);
        var root = JsonValue.MakeObject();
        foreach (KeyValuePair<string, long> kv in items)
        {
            root.Set(kv.Key, JsonValue.MakeInt(kv.Value));
        }

        File.WriteAllText(path, root.Dump());
    }

    private Dictionary<string, long> LoadLocalOffsets()
    {
        var outMap = new Dictionary<string, long>(StringComparer.Ordinal);
        try
        {
            if (!File.Exists(LocalOffsetPath()))
            {
                return outMap;
            }

            string text = File.ReadAllText(LocalOffsetPath());
            if (!Json.TryParse(text, out JsonValue root, out _) || root is null)
            {
                return outMap;
            }

            foreach (KeyValuePair<string, JsonValue> kv in root.ObjectItems())
            {
                outMap[kv.Key] = kv.Value.IntValue();
            }
        }
        catch (Exception)
        {
            // 本地位点文件缺失/损坏按首次启动处理
        }

        return outMap;
    }

    // ---------------- 顺序消费队列锁 ----------------
    private void LockLoop()
    {
        if (!IsOrderly() || _messageModel == RocketMQ.Remoting.Protocol.MessageModel.Broadcasting)
        {
            return;
        }

        // Java ConsumeMessageOrderlyService.lockMQ：每 20s 批量锁分到的队列；
        // 启动时立刻尝试一次，避免首个 20s 空转
        while (!_stop)
        {
            try
            {
                List<MessageQueue> mqs = AssignedQueues();
                if (mqs.Count > 0 && _mqClient is not null)
                {
                    List<MessageQueue> ok = _mqClient.LockBatchMq(ConsumerGroup, _clientId, mqs);
                    var okKeys = new HashSet<string>(StringComparer.Ordinal);
                    foreach (MessageQueue mq in ok)
                    {
                        okKeys.Add(OffsetKey(mq));
                    }

                    lock (_lock)
                    {
                        _lockOk.Clear();
                        foreach (string k in okKeys)
                        {
                            _lockOk.Add(k);
                        }
                    }

                    ClientLog.Debug("lock_batch_mq: " + okKeys.Count.ToString(CultureInfo.InvariantCulture)
                        + "/" + mqs.Count.ToString(CultureInfo.InvariantCulture) + " queues locked");
                }
            }
            catch (Exception e)
            {
                ClientLog.Debug("lock mq error: " + e.Message);
            }

            // 等待 20s（期间响应 stop）。整段 wait：100ms 切片在 macOS 上每段实测 131ms，
            // 200 段会把 Java 的 20s 轮次拖成 26s；Wait 本身被 Set 唤醒，照样立刻收手。
            _stopEvent.Wait(TimeSpan.FromSeconds(20));
        }
    }


    private static string OffsetKey(MessageQueue mq) => mq.Topic + mq.BrokerName + mq.QueueId.ToString(CultureInfo.InvariantCulture);

    private List<MessageQueue> AssignedQueues()
    {
        lock (_lock)
        {
            return new List<MessageQueue>(_assigned);
        }
    }

    /// <summary>topic 的全部队列（对应 Java RebalanceImpl.topicSubscribeInfoTable）。消费侧
    /// 用 isDefault=false，绝不兜底默认 topic。</summary>
    private List<MessageQueue> AllQueuesOfTopic(string topic)
    {
        var outList = new List<MessageQueue>();
        try
        {
            TopicPublishInfo? publish = Client().GetTopicPublishInfo(topic);
            if (publish is null || !publish.Ok())
            {
                return outList;
            }

            foreach (MessageQueue q in publish.MsgQueueList)
            {
                outList.Add(new MessageQueue(topic, q.BrokerName, q.QueueId));
            }
        }
        catch (Exception e)
        {
            // %RETRY%topic 在首次回投前无路由，属预期路径，debug 即可
            ClientLog.Debug("rebalance: no route for topic " + topic + ": " + e.Message);
        }

        outList.Sort(); // MessageQueue IComparable：topic → brokerName → queueId
        return outList;
    }

    /// <summary>按 Java RebalanceImpl.rebalanceByTopic 计算分配，再同步拉取线程集。</summary>
    private void DoRebalance()
    {
        MQClientInstance c = Client();
        var prev = new Dictionary<string, MessageQueue>(StringComparer.Ordinal);
        foreach (MessageQueue mq in AssignedQueues())
        {
            prev[OffsetKey(mq)] = mq;
        }

        var assigned = new List<MessageQueue>();
        List<string> topics;
        lock (_lock)
        {
            topics = new List<string>(_subscriptionData.Keys);
        }

        if (_messageModel == RocketMQ.Remoting.Protocol.MessageModel.Broadcasting)
        {
            foreach (string topic in topics)
            {
                assigned.AddRange(AllQueuesOfTopic(topic));
            }
        }
        else
        {
            foreach (string topic in topics)
            {
                List<MessageQueue> mqAll = AllQueuesOfTopic(topic);
                if (mqAll.Count == 0)
                {
                    continue;
                }

                List<string>? cidAll = c.GetConsumerIdListByGroup(topic, ConsumerGroup);
                if (cidAll is null || cidAll.Count == 0)
                {
                    // 查不到消费者列表：保留本 topic 现有分配（Java 仅告警；绝不回退成
                    // "独占全部队列"，否则同组多实例会互相重复消费）
                    ClientLog.Debug("rebalance: no consumer id list for " + ConsumerGroup + "/" + topic + ", keep current");
                    assigned.AddRange(AssignedQueues().FindAll(m => m.Topic == topic));
                    continue;
                }

                cidAll.Sort(StringComparer.Ordinal);
                // 自定义策略抛异常时：**保持现有分配**并结束本轮 rebalance（Java catch Throwable
                // → log error → return false；Python 同款 try/except → return）。
                // 绝不能把该 topic 的队列撤走。
                IAllocateMessageQueueStrategy strategy = _allocateMessageQueueStrategy
                    ?? throw new MQClientException("allocateMessageQueueStrategy is null");
                List<MessageQueue> got;
                try
                {
                    got = strategy.Allocate(ConsumerGroup, _clientId, mqAll, cidAll);
                }
                catch (Exception e)
                {
                    ClientLog.Error("allocate message queue exception. strategy name: "
                        + strategy.GetName() + ", ex: " + e.Message);
                    return;
                }

                assigned.AddRange(got);
            }
        }

        lock (_lock)
        {
            _assigned = assigned;
        }

        // 撤销清理（对齐 Java updateProcessQueueTableInRebalance → removeUnnecessaryMessageQueue
        // + removeProcessQueue）：本轮不再分配给本实例的队列，先把已消费位点持久化、丢弃
        // 在途/缓冲消息、解除顺序锁（orderly+clustering），并在 _dropped 打标记让 pull 线程
        // 长轮询返回后丢弃批次并退出。绝不能直接丢弃位点——否则重分配后从 0 重投。
        var assignedKeys = new HashSet<string>(StringComparer.Ordinal);
        var current = new Dictionary<string, MessageQueue>(StringComparer.Ordinal);
        foreach (MessageQueue mq in assigned)
        {
            assignedKeys.Add(OffsetKey(mq));
            current[OffsetKey(mq)] = mq;
        }

        // 停摆自愈（Java isPullExpired / PopProcessQueue.isPullExpired）：同一趟里还要撤掉
        // "仍归本实例、但拉取循环已经停摆"的队列，交给后面的 RebalancePullThreads 原地重建。
        List<RetiredQueue> healed = new();
        lock (_lock)
        {
            SweepStalledLoopsLocked(current, healed);
        }

        // 重新分配给本实例的队列清除撤销标记（可能上轮被撤销、本轮又分回），否则 pull 线程
        // 会误判 isDropped 直接退出。_pullThreads 的键存在性会阻止同一队列起两个线程。
        lock (_lock)
        {
            foreach (string k in assignedKeys)
            {
                _dropped.Remove(k);
            }
        }

        var revoked = new List<MessageQueue>();
        foreach (KeyValuePair<string, MessageQueue> kv in prev)
        {
            if (!assignedKeys.Contains(kv.Key))
            {
                revoked.Add(kv.Value);
            }
        }

        if (revoked.Count > 0)
        {
            bool orderly = IsOrderly();
            bool broadcast = _messageModel == RocketMQ.Remoting.Protocol.MessageModel.Broadcasting;
            var unlockList = new List<MessageQueue>();
            foreach (MessageQueue mq in revoked)
            {
                string key = OffsetKey(mq);
                long consumeOffset;
                lock (_lock)
                {
                    _dropped.Add(key);
                    _pending.Remove(key);
                    _mqMap.Remove(key);
                    _offsetTable.Remove(key);
                    _consumeOffsetTable.TryGetValue(key, out consumeOffset);
                    _consumeOffsetTable.Remove(key);
                    // POP：标记 dropped，在途批次不再消费也不 ack（交给 broker 复活重投）
                    if (_popQueues.TryGetValue(key, out PopProcessQueue? pq))
                    {
                        pq.SetDropped(true);
                        _popQueues.Remove(key);
                    }
                }

                // 1) 先持久化已消费位点（UPDATE_CONSUMER_OFFSET=15），再清缓冲/解锁
                if (!broadcast && _mqClient is not null)
                {
                    try
                    {
                        _mqClient.UpdateConsumerOffset(ConsumerGroup, mq, consumeOffset);
                    }
                    catch (Exception e)
                    {
                        ClientLog.Debug("update consumer offset on revoke failed for " + mq + ": " + e.Message);
                    }

                    // 2) 顺序消费（orderly + clustering）撤销队列需主动解锁（UNLOCK_BATCH_MQ=42），
                    //    否则 broker 侧锁长期不释放，新 owner 抢不到锁会在原地空转。
                    if (orderly)
                    {
                        unlockList.Add(mq);
                    }
                }
            }

            if (unlockList.Count > 0 && _mqClient is not null)
            {
                try
                {
                    _mqClient.UnlockBatchMq(ConsumerGroup, _clientId, unlockList);
                }
                catch (Exception e)
                {
                    ClientLog.Debug("unlock on revoke failed: " + e.Message);
                }
            }

            ClientLog.Info("rebalance: revoked " + revoked.Count.ToString(CultureInfo.InvariantCulture)
                + " queue(s), current assigned=" + assigned.Count.ToString(CultureInfo.InvariantCulture));
        }

        // 自愈撤下的队列：位点必须在**重建之前**落盘（和上面的真撤销同一口径），否则新循环
        // 会从更早的游标重拉，把已消费的消息再投一遍。顺序消费还要解锁，不然新属主抢不到锁。
        if (healed.Count > 0)
        {
            bool healOrderly = IsOrderly();
            bool healBroadcast = _messageModel == RocketMQ.Remoting.Protocol.MessageModel.Broadcasting;
            MQClientInstance? healClient = _mqClient;
            var healUnlockList = new List<MessageQueue>();
            foreach (RetiredQueue r in healed)
            {
                if (healBroadcast || healClient is null)
                {
                    continue;
                }

                if (r.HadOffset)
                {
                    try
                    {
                        healClient.UpdateConsumerOffset(ConsumerGroup, r.Mq, r.ConsumeOffset);
                    }
                    catch (Exception e)
                    {
                        ClientLog.Debug("persist offset on self-heal failed for " + r.Mq + ": " + e.Message);
                    }
                }

                if (healOrderly)
                {
                    healUnlockList.Add(r.Mq);
                }
            }

            if (healUnlockList.Count > 0 && healClient is not null)
            {
                try
                {
                    healClient.UnlockBatchMq(ConsumerGroup, _clientId, healUnlockList);
                }
                catch (Exception e)
                {
                    ClientLog.Debug("unlock on self-heal failed: " + e.Message);
                }
            }
        }

        // 新分配的队列**立刻**解析初始位点写入 offset 表（对齐 Java
        // updateProcessQueueTableInRebalance：新队列 → computePullFromWhereWithException →
        // offsetStore.updateOffset）。不能留到首次拉取时惰性解析——CONSUME_FROM_LAST_OFFSET
        // 语义是"分配时刻的最新位点"，惰性解析会跳过分配后新产生的消息（真机表现为收不到）。
        foreach (MessageQueue mq in assigned)
        {
            string key = OffsetKey(mq);
            if (prev.ContainsKey(key))
            {
                continue;
            }

            SubscriptionData? sub;
            lock (_lock)
            {
                if (_offsetTable.ContainsKey(key))
                {
                    continue;
                }

                _subscriptionData.TryGetValue(mq.Topic, out sub);
            }

            if (sub is null)
            {
                continue;
            }

            long off;
            try
            {
                off = ResolveInitialOffset(mq, sub);
            }
            catch (Exception e)
            {
                ClientLog.Debug("resolve initial offset for " + mq + " failed: " + e.Message);
                continue;
            }

            lock (_lock)
            {
                if (!_offsetTable.ContainsKey(key))
                {
                    _offsetTable[key] = off;
                }
            }
        }

        RebalancePullThreads();
    }

    private long ResolveInitialOffset(MessageQueue mq, SubscriptionData sub)
    {
        MQClientInstance c = Client();
        if (sub.ExpressionType == ExpressionType.Sql92)
        {
            // SQL 过滤无 offset 语义，默认最新
            return c.GetMaxOffset(mq);
        }

        if (_messageModel == RocketMQ.Remoting.Protocol.MessageModel.Broadcasting)
        {
            // 广播模式：offset 只存本地（对齐 Java LocalFileOffsetStore）
            Dictionary<string, long> stored = LoadLocalOffsets();
            if (stored.TryGetValue(OffsetKey(mq), out long v))
            {
                return v;
            }

            if (_consumeFromWhere == RocketMQ.Remoting.Protocol.ConsumeFromWhere.ConsumeFromFirstOffset)
            {
                return c.GetMinOffset(mq);
            }

            return c.GetMaxOffset(mq);
        }

        // 集群模式：先查 broker 上已提交的位点（对齐 Java RemoteBrokerOffsetStore.readOffset）
        try
        {
            if (c.QueryConsumerOffset(ConsumerGroup, mq, out long stored, 5000, null,
                    /*setZeroIfNotFound=*/false))
            {
                return stored;
            }
        }
        catch (Exception e)
        {
            ClientLog.Debug("query consumer offset for " + mq + " not found: " + e.Message);
        }

        try
        {
            if (_consumeFromWhere == RocketMQ.Remoting.Protocol.ConsumeFromWhere.ConsumeFromFirstOffset)
            {
                return c.GetMinOffset(mq);
            }

            if (_consumeFromWhere == RocketMQ.Remoting.Protocol.ConsumeFromWhere.ConsumeFromTimestamp)
            {
                long ts = UtilAll.CurrentTimeMillis() - 30 * 60 * 1000L;
                return c.SearchOffsetByTimestamp(mq, ts);
            }

            // 默认 CONSUME_FROM_LAST_OFFSET
            return c.GetMaxOffset(mq);
        }
        catch (Exception)
        {
            return 0;
        }
    }

    // ---------------- 心跳 ----------------
    public int SendHeartbeatToAllBroker()
    {
        if (_mqClient is null)
        {
            return 0;
        }

        List<string> addrs;
        try
        {
            addrs = _mqClient.KnownBrokerAddrs();
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

        var hb = new HeartbeatData(_clientId);
        var cd = new ConsumerData
        {
            GroupName = ConsumerGroup,
            ConsumeType = ConsumeType.ConsumePassively,
            MessageModel = _messageModel,
            ConsumeFromWhere = _consumeFromWhere,
            // Java MQClientInstance:1039：consumerData.setUnitMode(impl.isUnitMode())。
            // broker 读到它会给 %RETRY%group 打上 UNIT_SUB(0x2) 系统标记
            // （ClientManageProcessor:113-118）。
            UnitMode = _unitMode,
        };
        lock (_lock)
        {
            foreach (var kv in _subscriptionData)
            {
                cd.AddSubscriptionData(kv.Value);
            }
        }

        // 保持 fingerprint = 0，让 broker 走 V1 注册路径（用完整 subscriptionDataSet 注册）
        // withoutSub 仅在 fingerprint != 0 的 V2 路径下被读取，这里保持默认 false 即可。
        hb.HeartbeatFingerprint = 0;
        hb.AddConsumerData(cd);

        int okCount = 0;
        foreach (string addr in addrs)
        {
            try
            {
                _mqClient.SendHeartbeat(addr, hb, 5000);
                ++okCount;
                Interlocked.Increment(ref _heartbeatCount);
            }
            catch (Exception e)
            {
                ClientLog.Warn("heartbeat to " + addr + " failed: " + e.Message);
            }
        }

        return okCount;
    }

    private void MaybeSendHeartbeat()
    {
        if (!_heartbeatEnabled)
        {
            return;
        }

        long now = UtilAll.CurrentTimeMillis();
        long last = _lastHeartbeatMs;
        if (last != 0 && now - last < _heartbeatIntervalMillis)
        {
            return;
        }

        _lastHeartbeatMs = now;
        SendHeartbeatToAllBroker();
    }

    // ---------------- 管理 ----------------
    public List<MessageQueue> FetchSubscribeMessageQueues(string topic)
    {
        MQClientInstance c = Client();
        TopicPublishInfo publish = c.GetTopicPublishInfo(topic);
        var outList = new List<MessageQueue>(publish.MsgQueueList.Count);
        foreach (MessageQueue q in publish.MsgQueueList)
        {
            outList.Add(new MessageQueue(q.Topic, q.BrokerName, q.QueueId));
        }

        return outList;
    }

    /// <summary>当前分给本实例的队列 key 列表（真机验证"同组两实例不重不漏"用）。
    /// key 格式与 OffsetKey 一致：topic + brokerName + queueId。</summary>
    public List<string> AssignedQueueKeys()
    {
        var outList = new List<string>();
        foreach (MessageQueue mq in AssignedQueues())
        {
            outList.Add(OffsetKey(mq));
        }

        return outList;
    }

    /// <summary>查消费组在某 topic 上的全部 clientId（对应 Java findConsumerIdList），
    /// 用于验证多实例注册。查不到/未启动返回空，不抛。</summary>
    public List<string> ConsumerIdListOfGroup(string topic)
    {
        if (_mqClient is null)
        {
            return new List<string>();
        }

        try
        {
            return _mqClient.GetConsumerIdListByGroup(topic, ConsumerGroup) ?? new List<string>();
        }
        catch (Exception)
        {
            return new List<string>();
        }
    }

    // 消息重投（对应 Java sendMessageBack）：失败抛 MQBrokerException，成功返回 true
    public bool SendMessageBack(MessageExt msg, int delayLevel, string? brokerNameIn = null)
    {
        MQClientInstance c = Client();
        string brokerName = string.IsNullOrEmpty(brokerNameIn) ? msg.BrokerName : brokerNameIn!;
        string addr = c.BrokerAddrOf(brokerName);
        if (addr.Length == 0)
        {
            throw new MQClientException("broker " + brokerName + " not found");
        }

        var header = new ConsumerSendMsgBackRequestHeader
        {
            Offset = msg.CommitLogOffset,
            Group = ConsumerGroup,
            DelayLevel = delayLevel,
            OriginMsgId = msg.MsgId,
            OriginTopic = msg.Topic,
            // ⚠ 有意超出 Java：MQClientAPIImpl#consumerSendMessageBack(:1684-1693) 只填
            // group/offset/delayLevel/originMsgId/originTopic/maxReconsumeTimes/brokerName，
            // 从不写 unitMode，字段恒为 false。broker 侧确实读它
            // （AbstractSendMessageProcessor:135-138 → buildSysFlag(false, true)），所以这里
            // 按消费者配置如实上报，单元化重试 topic 才会带上 UNIT_SUB 标记。
            UnitMode = _unitMode,
            // Java：maxReconsumeTimes == -1 时按 16 传给 broker（超限由 broker 转 %DLQ%）
            MaxReconsumeTimes = MaxReconsumeTimesOrDefault(),
        };
        c.InvokeSync(addr, RequestCode.ConsumerSendMsgBack, header.ToExtFields(), null, false, 5000);
        return true;
    }

    // ---------------- 内部工具 ----------------
    private static string Trim(string s)
    {
        int b = 0;
        while (b < s.Length && IsWs(s[b])) ++b;
        if (b == s.Length) return string.Empty;
        int e = s.Length - 1;
        while (e > b && IsWs(s[e])) --e;
        return s.Substring(b, e - b + 1);
    }

    private static bool IsWs(char c) => c == ' ' || c == '\t' || c == '\r' || c == '\n';

    private static List<string> SplitSemicolon(string addr)
    {
        var outList = new List<string>();
        if (addr is null) return outList;
        int start = 0;
        while (start <= addr.Length)
        {
            int pos = addr.IndexOf(';', start);
            string piece = pos < 0 ? addr.Substring(start) : addr.Substring(start, pos - start);
            string t = Trim(piece);
            if (t.Length > 0) outList.Add(t);
            if (pos < 0) break;
            start = pos + 1;
        }

        return outList;
    }
}
