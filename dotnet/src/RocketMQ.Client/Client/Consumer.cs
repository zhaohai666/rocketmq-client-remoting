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
public sealed class DefaultMQPushConsumer
{
    // ---------------- 配置 ----------------
    public string ConsumerGroup { get; }

    private string _instanceName = "DEFAULT";
    private string _clientId = string.Empty;
    private string _messageModel = RocketMQ.Remoting.Protocol.MessageModel.Clustering;
    private string _consumeFromWhere = RocketMQ.Remoting.Protocol.ConsumeFromWhere.ConsumeFromLastOffset;

    private int _consumeThreadNums = 1;
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

    public void SetConsumeThreadNums(int n)
    {
        _consumeThreadNums = Math.Max(1, n);
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

        SubscriptionData sub = FilterAPI.BuildSubscriptionData(topic, subExpression);
        lock (_lock)
        {
            _subscriptionData[topic] = sub;
        }
    }

    public void Subscribe(string topic, MessageSelector selector)
    {
        if (_started)
        {
            throw new MQClientException("consumer already started, cannot change configuration");
        }

        var sub = new SubscriptionData(topic, selector.Expression)
        {
            ExpressionType = selector.Type,
        };
        if (selector.Type == ExpressionType.TAG)
        {
            SubscriptionData built = FilterAPI.BuildSubscriptionData(topic, selector.Expression);
            sub.TagsSet = built.TagsSet;
        }

        lock (_lock)
        {
            _subscriptionData[topic] = sub;
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

            if (_nameServerAddrs.Count == 0)
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
            _stop = false;
            _started = true;
        }

        // 拉取：每队列一个线程（并发长轮询，避免空队列 suspend 阻塞其他队列投递）
        RebalancePullThreads();
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
        _mqClient?.Shutdown();
    }

    private static void JoinIfAlive(Thread? t)
    {
        if (t is not null && t.IsAlive)
        {
            t.Join();
        }
    }

    private static Thread MakeThread(string name, ThreadStart action)
    {
        var t = new Thread(action)
        {
            IsBackground = true,
        };
        return t;
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
            MessageQueue mq = kv.Value;
            Thread t = MakeThread("PullMessageService", () => QueuePullLoop(mq));
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

    private void RebalanceLoop()
    {
        // 简化 rebalance：周期刷新分配集，为新增队列（如 %RETRY%topic 建立路由后）补拉取线程
        while (!_stop)
        {
            _stopEvent.Wait(TimeSpan.FromMilliseconds(2000));
            if (_stop || !_started) return;
            try
            {
                MaybeSendHeartbeat();
                RebalancePullThreads();
            }
            catch (Exception e)
            {
                ClientLog.Debug("rebalance pull threads error: " + e.Message);
            }
        }
    }

    private void QueuePullLoop(MessageQueue mq)
    {
        MQClientInstance c = Client();
        bool orderly = IsOrderly();
        string key = OffsetKey(mq);
        while (!_stop && _started)
        {
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
                lock (_lock)
                {
                    Queue<MessageExt> dq = _pending[key];
                    foreach (MessageExt m in result.MsgFoundList)
                    {
                        dq.Enqueue(m);
                    }
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
            }

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
        }

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
        MQClientInstance c = Client();
        var result = new List<MessageQueue>();
        foreach (string topic in SubscribedTopics())
        {
            try
            {
                TopicPublishInfo? publish = c.GetTopicPublishInfo(topic);
                foreach (MessageQueue q in publish.MsgQueueList)
                {
                    var mq = new MessageQueue(topic, q.BrokerName, q.QueueId);
                    if (!result.Contains(mq))
                    {
                        result.Add(mq);
                    }
                }
            }
            catch (Exception e)
            {
                // %RETRY%topic 在首次回投前无路由，属预期路径，debug 即可
                ClientLog.Debug("assigned_queues: skip topic " + topic + ": " + e.Message);
            }
        }

        return result;
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
