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
                UnitMode = false, // 本项目无 unit mode
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

    // ---------------- 消息轨迹（消费侧）----------------
    private bool _enableTrace;
    private string _traceTopic = MixAll.TraceTopic;
    private int _traceMsgBatchNum = 10;
    private readonly List<IConsumeMessageHook> _consumeMessageHooks = new();
    private readonly List<IFilterMessageHook> _filterMessageHooks = new();
    private long _filteredMessageCount;
    private AsyncTraceDispatcher? _traceDispatcher;

    private string _instanceName = "DEFAULT";
    private string _clientId = string.Empty;
    private string _messageModel = RocketMQ.Remoting.Protocol.MessageModel.Clustering;
    private string _consumeFromWhere = RocketMQ.Remoting.Protocol.ConsumeFromWhere.ConsumeFromLastOffset;

    // ---- 消费线程池（对齐 Java DefaultMQPushConsumer 的 consumeThreadMin/Max）----
    // Java 默认 min=20 / max=64。本实现的**拉取**路径是"每队列一个拉取线程 + 单分发线程"，
    // 只有 **POP** 路径用真正的线程池（对应 Java ConsumeMessagePopConcurrentlyService），
    // 因此 CorePoolSize 直接决定 POP 的消费并发度（Java 无界队列下 max 实际用不到）。
    private int _consumeThreadMin = 20;
    private int _consumeThreadMax = 64;
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
    // 顺序消费：broker LOCK_BATCH_MQ 确认锁定成功的队列 key 集
    private readonly HashSet<string> _lockOk = new(StringComparer.Ordinal);
    private int _pullThresholdForQueue = 1000;
    private long _flowControlTriggered;
    private Thread? _dispatchThread;
    private Thread? _persistThread;
    private Thread? _lockThread;
    private Thread? _rebalanceThread;
    private readonly Dictionary<string, Thread> _pullThreads = new(StringComparer.Ordinal);
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

    // ---------------- 生命周期 ----------------
    public void Start()
    {
        {
            if (_started)
            {
                return;
            }

            // 静态地址与动态取址（ROCKETMQ_NAMESRV_DOMAIN）二选一必须可用
            if (_nameServerAddrs.Count == 0 && !DefaultTopAddressing.IsConfigured())
            {
                throw new MQClientException("name server address is not set");
            }

            if (_subscriptionData.Count == 0)
            {
                throw new MQClientException("subscription is not set, call subscribe() first");
            }

            if (_messageListener is null)
            {
                throw new MQClientException("message listener is not set");
            }

            if (_clientId.Length == 0)
            {
                _clientId = ClientIds.Build(_instanceName);
            }

            // 对齐 Java DefaultMQPushConsumer.start()：把消费组套上命名空间（ns%group），
            // 之后所有面向 broker 的组名（心跳 / rebalance / 位点 / 锁 / 回投）都用包装后的值。
            if (_namespace.Length != 0)
            {
                ConsumerGroup = NamespaceUtil.WrapNamespace(_namespace, ConsumerGroup);
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

            _mqClient = new MQClientInstance(_clientId, _nameServerAddrs,
                /*connectTimeoutMillis=*/3000,
                /*invokeTimeoutMillis=*/_pullTimeoutMillis);
            _mqClient.Start();
            // 动态 name server：实例启动时可能已从地址服务器拿到地址，回填到本消费者
            // （Java 由共享的 ClientConfig 天然同步）
            if (_nameServerAddrs.Count == 0 && _mqClient.NameServerAddrs.Count > 0)
            {
                _nameServerAddrs = new List<string>(_mqClient.NameServerAddrs);
            }
            // ACL 鉴权钩子：必须在首包（路由拉取 / 心跳 / rebalance）发出之前绑定。
            if (_rpcHook is not null && !_mqClient.RegisterRpcHook(_rpcHook))
            {
                ClientLog.Warn("consumer rpc hook ignored: MQClientInstance already has one (clientId="
                    + _clientId + ")");
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

            // 登记订阅 topic 为"在用"，交给 MQClientInstance 周期刷新路由（对应 task ⑥）
            foreach (string t in SubscribedTopics())
            {
                _mqClient.RegisterTopicInUse(t);
            }

            // 注册 broker 主动通知：消费者实例上下线 → 立即重算分配（对应 Java
            // ClientRemotingProcessor → NOTIFY_CONSUMER_IDS_CHANGED → rebalanceImmediately）
            _mqClient.RemotingClient.RegisterProcessor(RequestCode.NotifyConsumerIdsChanged,
                OnConsumerIdsChanged);
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
        JoinIfAlive(_dispatchThread);
        JoinIfAlive(_persistThread);
        JoinIfAlive(_lockThread);
        JoinIfAlive(_rebalanceThread);

        // 优雅注销（对应 task ④）：关闭连接前从所有 broker 摘除本 clientId，不必等心跳超时
        // （默认 ~120s）——否则这段时间内消费者变更通知仍可能发往已退出的实例。
        if (_mqClient is not null)
        {
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

    private void RebalancePullThreads()
    {
        List<MessageQueue> queues = AssignedQueues();
        var current = new Dictionary<string, MessageQueue>(StringComparer.Ordinal);
        foreach (MessageQueue mq in queues)
        {
            current[OffsetKey(mq)] = mq;
        }

        List<KeyValuePair<string, MessageQueue>> toStart = new();
        lock (_lock)
        {
            foreach (KeyValuePair<string, MessageQueue> kv in current)
            {
                if (!_pullThreads.ContainsKey(kv.Key))
                {
                    toStart.Add(kv);
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

    /// <summary>broker 通知本消费组实例上下线（NOTIFY_CONSUMER_IDS_CHANGED=40）。对应 Java
    /// ClientRemotingProcessor → rebalanceImmediately：置标记唤醒自己的 rebalance 线程，
    /// 避免等下个 20s 周期。</summary>
    /// <remarks>⚠ 本回调在 **remoting 读线程**上执行（RemotingClient 的分发路径）。这里**绝不能**
    /// 同步调用 DoRebalance()：它内部会做阻塞式 invokeSync（GET_CONSUMER_LIST_BY_GROUP、心跳），
    /// 而响应只能由**同一个读线程**投递 —— 读线程阻塞在自己发起的同步调用上必然自死锁，直到
    /// invokeTimeout（实测 5s 超时、日志出现 "no consumer id list ..., keep current"，
    /// 并连带把其它请求的响应一起卡住）。只置标志、交给 RebalanceThread 去算即可，
    /// 与 C++ 侧只置 rebalanceNow_ 标志、Java 侧 rebalanceImmediately() 的语义一致。</remarks>
    private RemotingCommand? OnConsumerIdsChanged(RemotingCommand request, string addr)
    {
        ClientLog.Debug("received NOTIFY_CONSUMER_IDS_CHANGED, rebalance now (on reader thread, defer to RebalanceThread)");
        _rebalanceNow.Set();
        return null; // 回查类通知是 invokeOneway，不期待响应
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
            // 队列已被 rebalance 撤销（isDropped）：退出线程并摘除自身（对齐 Java
            // ProcessQueue.isDropped → pull 线程停止服务该队列）。
            bool dropped;
            lock (_lock)
            {
                dropped = _dropped.Contains(key);
            }
            if (dropped)
            {
                lock (_lock)
                {
                    _pullThreads.Remove(key);
                }
                return;
            }

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

            // 流控（对齐 Java ProcessQueue 的 pullThresholdForQueue 检查）：
            // 已拉未消费的条数超过阈值就暂停本队列拉取
            {
                int pendingN;
                lock (_lock)
                {
                    pendingN = _pending.TryGetValue(key, out Queue<MessageExt>? q) ? q.Count : 0;
                }

                if (pendingN >= Math.Max(1, _pullThresholdForQueue))
                {
                    Interlocked.Increment(ref _flowControlTriggered);
                    ClientLog.Debug("flow control: queue " + mq + " pause pull");
                    _stopEvent.Wait(TimeSpan.FromMilliseconds(100));
                    continue;
                }
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
                result = c.PullMessage(ConsumerGroup, mq, offset, _pullBatchSize, sysFlag,
                    /*commitOffset=*/0, expr, sub.SubVersion, sub.ExpressionType,
                    _pullTimeoutMillis, _pullBatchSizeInBytes, _pullSuspendTimeoutMillis);
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

            // 长轮询返回后再次确认：若期间被撤销，丢弃本批消息并退出（对齐 Java
            // ProcessQueue.isDropped 守卫——拉到的消息不再进入缓冲）。
            lock (_lock)
            {
                if (_dropped.Contains(key))
                {
                    _pullThreads.Remove(key);
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
            if (_dropped.Contains(key)) return;
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

            // 弹出后队列被 rebalance 撤走：这一批**既不消费也不 ack**
            // （Java 对应 PopProcessQueue.isDropped() 分支），交给 invisibleTime 到期后
            // broker 自动复活重投给新属主。
            if (_dropped.Contains(key) || pq.IsDropped())
            {
                ClientLog.Debug("queue " + key + " revoked during pop, discard "
                                + result.MsgFoundList.Count + " messages un-acked");
                return;
            }

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
    /// <para>投给**有界线程池**（core=ConsumeThreadMin / max=ConsumeThreadMax）。
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

        // ⚠ 对齐 Java ConsumeConcurrentlyContext.ackIndex = Integer.MAX_VALUE：
        // 默认就是"全部 ack"。本项目的默认值是 -1（push 回投路径的语义），若不在 POP 这里
        // 改成 size-1，CONSUME_SUCCESS 会**一条都不 ack**，消息在 invisibleTime 到期后被
        // broker 复活重投 —— 短观测窗口下会伪装成通过。
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
            FinishConsumeHook(ohookCtx, true, ohasException, obegin, status.ToString(),
                failed: status != ConsumeOrderlyStatus.Success,
                succeeded: status == ConsumeOrderlyStatus.Success);

            if (status == ConsumeOrderlyStatus.SuspendCurrentQueueAMoment)
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

        FinishConsumeHook(hookCtx, true, hasException, beginMs, cstatus.ToString(),
            failed: cstatus == ConsumeConcurrentlyStatus.ReconsumeLater,
            succeeded: cstatus == ConsumeConcurrentlyStatus.ConsumeSuccess);

        if (cstatus == ConsumeConcurrentlyStatus.ConsumeSuccess)
        {
            AdvanceConsumeOffset(key, batch);
            Interlocked.Add(ref _consumedCount, batch.Count);
            return true;
        }

        // RECONSUME_LATER：广播模式不回投（仅告警，位点前进）；集群模式回投 %RETRY%topic
        if (broadcast)
        {
            ClientLog.Warn("BROADCASTING: message consume failed, no redelivery: "
                + batch.Count.ToString(CultureInfo.InvariantCulture) + " msgs in " + mq);
            AdvanceConsumeOffset(key, batch);
            Interlocked.Add(ref _consumedCount, batch.Count);
            return true;
        }

        if (SendBackBatch(batch, cctx))
        {
            AdvanceConsumeOffset(key, batch);
            Interlocked.Add(ref _consumedCount, batch.Count);
            return true;
        }

        // 回投失败：批次塞回队首稍后重试（Java 中这些消息不从 ProcessQueue 移除）
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

        _stopEvent.Wait(TimeSpan.FromMilliseconds(200));
        return false;
    }

    /// <summary>失败批次逐条回投 broker（对齐 Java processConsumeResult → sendMessageBack）。</summary>
    private bool SendBackBatch(List<MessageExt> batch, ConsumeConcurrentlyContext ctx)
    {
        bool ok = true;
        foreach (MessageExt msg in batch)
        {
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
                ok = false;
            }
        }

        return ok;
    }

    private void AdvanceConsumeOffset(string key, List<MessageExt> batch)
    {
        long nextOffset = 0;
        foreach (MessageExt m in batch)
        {
            if (m.QueueOffset + 1 > nextOffset)
            {
                nextOffset = m.QueueOffset + 1;
            }
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
        // Java MQClientInstance.startScheduledTask：persistAllConsumerOffset 每 5s
        while (!_stop)
        {
            _stopEvent.Wait(TimeSpan.FromMilliseconds(5000));
            if (_stop || !_started) return;
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
        string home = Environment.GetEnvironmentVariable("HOME") is { Length: > 0 } h ? h : ".";
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

            // 等待 20s（期间响应 stop）
            for (int i = 0; i < 200 && !_stop; ++i)
            {
                _stopEvent.Wait(TimeSpan.FromMilliseconds(100));
            }
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

    /// <summary>平均分配（对应 Java AllocateMessageQueueAveragely）。</summary>
    private static List<MessageQueue> AllocateMessageQueueAveragely(string consumerGroup, string currentCid,
        List<MessageQueue> mqAll, List<string> cidAll)
    {
        if (mqAll.Count == 0)
        {
            return new List<MessageQueue>();
        }

        if (cidAll.Count == 0 || !cidAll.Contains(currentCid))
        {
            return new List<MessageQueue>();
        }

        int index = cidAll.IndexOf(currentCid);
        int mod = mqAll.Count % cidAll.Count;
        int avg = mqAll.Count <= cidAll.Count
            ? 1
            : (mod > 0 && index < mod ? mqAll.Count / cidAll.Count + 1 : mqAll.Count / cidAll.Count);
        int startIndex = (mod > 0 && index < mod) ? index * avg : index * avg + mod;
        int range = Math.Min(avg, mqAll.Count - startIndex);
        if (range <= 0)
        {
            return new List<MessageQueue>();
        }

        return mqAll.GetRange(startIndex, range);
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
                assigned.AddRange(AllocateMessageQueueAveragely(ConsumerGroup, _clientId, mqAll, cidAll));
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
        foreach (MessageQueue mq in assigned)
        {
            assignedKeys.Add(OffsetKey(mq));
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
            UnitMode = false,
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
            UnitMode = false,
            // Java：maxReconsumeTimes == -1 时按 16 传给 broker（超限由 broker 转 %DLQ%）
            MaxReconsumeTimes = _maxReconsumeTimes == -1 ? 16 : _maxReconsumeTimes,
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
