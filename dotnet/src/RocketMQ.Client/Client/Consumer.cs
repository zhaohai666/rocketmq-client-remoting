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

    /// <summary>broker 通知本消费组实例上下线（NOTIFY_CONSUMER_IDS_CHANGED=40）。对应 Java
    /// ClientRemotingProcessor → rebalanceImmediately：置标记唤醒自己的 rebalance 线程，
    /// 避免等下个 20s 周期。</summary>
    /// <remarks>⚠ 本回调在 **remoting 读线程**上执行（RemotingClient 的分发路径）。这里**绝不能**
    /// 同步调用 DoRebalance()：它内部会做阻塞式 invokeSync（GET_CONSUMER_LIST_BY_GROUP、心跳），
    /// 而响应只能由**同一个读线程**投递 —— 读线程阻塞在自己发起的同步调用上必然自死锁，直到
    /// invokeTimeout（实测 5s 超时、日志出现 "no consumer id list ..., keep current"，
    /// 并连带把其它请求的响应一起卡住）。只置标志、交给 RebalanceThread 去算即可，
    /// 与 C++ 侧只置 rebalanceNow_ 标志、Java 侧 rebalanceImmediately() 的语义一致。</remarks>
    private void OnConsumerIdsChanged(RemotingCommand request, string addr)
    {
        ClientLog.Debug("received NOTIFY_CONSUMER_IDS_CHANGED, rebalance now (on reader thread, defer to RebalanceThread)");
        _rebalanceNow.Set();
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
                                m.Topic = orig!;
                            }
                        }

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
