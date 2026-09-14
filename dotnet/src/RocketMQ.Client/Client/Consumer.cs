// 推模式消费者（对应 org.apache.rocketmq.client.consumer.DefaultMQPushConsumer
// 与 Python client/consumer.py 的 DefaultMQPushConsumer）。
//
// 实现方式与 Python 参考实现一致：**单线程拉取循环 + 本地消费**
//   1. Start() 启动一个消费线程；
//   2. 每轮对「已分配队列」逐个调用 PULL_MESSAGE（带订阅信息的长轮询）；
//   3. 按 PullStatus 推进 offset：FOUND -> offset + 成功消费条数；
//      NO_NEW_MSG / OFFSET_ILLEGAL -> nextBeginOffset；
//   4. 把消息交给 MessageListener（并发/顺序两种）。
//
// 关键工程点（真机验证得出，勿删注释）：
//   - 单线程顺序长轮询下，排在满载队列前面的**空闲队列**会用 suspend 长轮询
//     阻塞整轮，把满载队列饿死（顺序消息尤其明显，因为同 key 全落一个队列）。
//     因此 pull_suspend_timeout_millis 与 pull_timeout_millis 必须可配且设短。
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

    private MQClientInstance? _mqClient;
    private volatile bool _started;
    private volatile bool _stop;
    private readonly List<Thread> _consumeThreads = new();
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

            _mqClient = new MQClientInstance(_clientId, _nameServerAddrs,
                /*connectTimeoutMillis=*/3000,
                /*invokeTimeoutMillis=*/_pullTimeoutMillis);
            _mqClient.Start();
            _stop = false;
            _started = true;
        }

        int n = Math.Max(1, _consumeThreadNums);
        _consumeThreads.Clear();
        for (int i = 0; i < n; ++i)
        {
            int idx = i;
            // 线程名对齐 Java 的 ThreadFactoryImpl("ConsumeMessageThread_")：日志里能区分是哪个消费线程。
            var t = new Thread(() =>
            {
                ClientLog.SetThreadName("ConsumeMessageThread_" + idx.ToString(CultureInfo.InvariantCulture));
                ConsumeLoop();
            });
            t.IsBackground = true;
            t.Start();
            _consumeThreads.Add(t);
        }

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
        foreach (Thread t in _consumeThreads)
        {
            if (t.IsAlive) t.Join();
        }

        _consumeThreads.Clear();
        _mqClient?.Shutdown();
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
    private void ConsumeLoop()
    {
        while (!_stop)
        {
            try
            {
                if (!_started) break;
                PullAndConsumeOnce();
            }
            catch (Exception e)
            {
                ClientLog.Warn("consume loop error: " + e.Message);
            }

            int interval = _pullIntervalMillis > 0 ? _pullIntervalMillis : 10;
            _stopEvent.Wait(TimeSpan.FromMilliseconds(interval));
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
                ClientLog.Warn("assigned_queues: skip topic " + topic + ": " + e.Message);
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

    private void PullAndConsumeOnce()
    {
        MQClientInstance c = Client();
        List<MessageQueue> queues = AssignedQueues();
        // 路由已加载（或尝试过），此时发心跳才能拿到 broker 地址
        MaybeSendHeartbeat();

        foreach (MessageQueue mq in queues)
        {
            if (_stop || !_started) return;

            SubscriptionData sub;
            {
                lock (_lock)
                {
                    if (!_subscriptionData.TryGetValue(mq.Topic, out SubscriptionData? s) || s is null)
                    {
                        continue;
                    }

                    sub = s!;
                }
            }

            string key = OffsetKey(mq);
            long offset;
            {
                lock (_lock)
                {
                    offset = _offsetTable.TryGetValue(key, out long v) ? v : -1;
                }
            }

            if (offset < 0)
            {
                offset = ResolveInitialOffset(mq, sub);
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
                // PULL_OFFSET_MOVED 等已映射到 PullStatus；其余 broker 错误跳过本轮
                ClientLog.Debug("pull broker error for " + mq + ": " + e.Message);
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
                ClientLog.Warn("pull error for " + mq + ": " + e.Message);
                continue;
            }

            if (result.Status == PullStatus.Found && result.MsgFoundList.Count > 0)
            {
                int dispatched = DispatchMessages(mq, result.MsgFoundList);
                lock (_lock)
                {
                    _offsetTable[key] = offset + dispatched;
                }
            }
            else if (result.Status == PullStatus.NoNewMsg || result.Status == PullStatus.OffsetIllegal)
            {
                lock (_lock)
                {
                    _offsetTable[key] = result.NextBeginOffset;
                }
            }
        }
    }

    // 返回可推进 offset 的消息条数
    private int DispatchMessages(MessageQueue mq, List<MessageExt> msgs)
    {
        IMessageListener? listener = _messageListener;
        if (listener is null)
        {
            return 0;
        }

        int batchSize = Math.Max(1, _consumeMessageBatchMaxSize);
        int consumed = 0;
        int i = 0;
        while (i < msgs.Count)
        {
            int end = Math.Min(i + batchSize, msgs.Count);
            List<MessageExt> batch = msgs.GetRange(i, end - i);
            try
            {
                if (listener.Orderly())
                {
                    var orderly = (IMessageListenerOrderly)listener;
                    var ctx = new ConsumeOrderlyContext(mq);
                    ConsumeOrderlyStatus status = orderly.ConsumeMessage(batch, ctx);
                    if (status == ConsumeOrderlyStatus.SuspendCurrentQueueAMoment)
                    {
                        _stopEvent.Wait(TimeSpan.FromMilliseconds(_suspendCurrentQueueTimeMillis));
                        break;
                    }
                }
                else
                {
                    var conc = (IMessageListenerConcurrently)listener;
                    var ctx = new ConsumeConcurrentlyContext(mq);
                    ConsumeConcurrentlyStatus status = conc.ConsumeMessage(batch, ctx);
                    if (status == ConsumeConcurrentlyStatus.ReconsumeLater)
                    {
                        // 简化：本批不推进 offset，留给后续重投
                        break;
                    }
                }

                consumed += batch.Count;
                Interlocked.Add(ref _consumedCount, batch.Count);
                i = end;
            }
            catch (Exception e)
            {
                ClientLog.Warn("listener error: " + e.Message);
                break;
            }
        }

        return consumed;
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
            MaxReconsumeTimes = _maxReconsumeTimes,
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
