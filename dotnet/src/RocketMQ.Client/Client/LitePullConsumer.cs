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
// 两种模式都用 Poll(timeout) 取批量消息；位点默认 AutoCommit（拉完即向 broker 提交）。
//
// 设计取舍（与 C++ / Python 参考实现一致）：
// - 后台**单个**拉取线程顺序遍历所有已分配队列做短轮询（suspend=false），把消息塞进
//   一个线程安全的本地缓冲 _localBuffer；Poll() 用 Monitor 等待并 drain 该缓冲。
// - subscribe 模式的 rebalance 复用既有 GetConsumerIdListByGroup + AllocateMessageQueueAveragely，
//   与 push 消费者同一套分配算法；查询不到消费组列表时按 Java 语义「保留当前分配」，不回退独占。
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
    private string _instanceName = "DEFAULT";
    private string _clientId = string.Empty;
    private string _messageModel = MessageModel.Clustering;
    private string _consumeFromWhere = ConsumeFromWhere.ConsumeFromLastOffset;
    // Java DefaultLitePullConsumer.consumeTimestamp 的字段初值：now - 30 分钟。
    // 留空会让 CONSUME_FROM_TIMESTAMP 退化成「从当前时刻起消费」。
    private string _consumeTimestamp =
        UtilAll.TimeMillisToHumanString3(UtilAll.CurrentTimeMillis() - 30 * 60 * 1000);
    private readonly List<string> _nameServerAddrs = new();
    private IRpcHook? _rpcHook;

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
    private readonly Dictionary<MessageQueue, long> _nextOffset = new();
    private readonly Dictionary<MessageQueue, long> _seekOffset = new();
    private readonly Dictionary<MessageQueue, long> _lastCommit = new();
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

    public void SetNamespace(string ns) => _namespace = ns ?? string.Empty;

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

            if (!hasSub && !_assignMode)
            {
                throw new MQClientException("subscription is not set, call Subscribe() or Assign() first");
            }

            // 对应 Java DefaultMQPushConsumerImpl.checkConfig（:1058）：启动即无条件校验，
            // 而不是等到算起点时抛出、被下层 catch 吞掉后静默退化成从 max offset 消费。
            ParseConsumeTimestamp(_consumeTimestamp);

            if (_clientId.Length == 0)
            {
                _clientId = ClientIds.Build(_instanceName);
            }

            _mqClient = new MQClientInstance(_clientId, new List<string>(_nameServerAddrs),
                /*connectTimeoutMillis=*/3000, /*invokeTimeoutMillis=*/10000);
            _mqClient.Start();

            if (_rpcHook is not null && !_mqClient.RegisterRpcHook(_rpcHook))
            {
                ClientLog.Warn("lite pull consumer rpc hook ignored: MqClient already has one (clientId="
                    + _clientId + ")");
            }

            if (_assignMode)
            {
                foreach (MessageQueue mq in _assigned)
                {
                    _mqClient.RegisterTopicInUse(mq.Topic);
                    if (!_nextOffset.ContainsKey(mq))
                    {
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
            }

            foreach (string t in _subscription.Keys)
            {
                _mqClient.RegisterTopicInUse(t);
            }

            // 先把 tag 订阅注册给 broker（心跳），再启动后台拉取，避免首轮拉取因 broker 不认订阅而丢消息。
            SendHeartbeatToAllBroker();
            _running = true;
            _started = true;
            _heartbeatThread = new Thread(HeartbeatLoop) { IsBackground = true, Name = "rmq-lite-hb-" + _clientId };
            _heartbeatThread.Start();
            _pullThread = new Thread(PullServiceLoop) { IsBackground = true, Name = "rmq-lite-pull-" + _consumerId() };
            _pullThread.Start();
        }
    }

    public void Shutdown()
    {
        lock (_lock)
        {
            if (!_started) return;
            _started = false;
            _running = false;
            if (_autoCommit)
            {
                try
                {
                    Commit();
                }
                catch
                {
                    ClientLog.Debug("lite shutdown commit failed");
                }
            }

            lock (_bufferLock)
            {
                Monitor.PulseAll(_bufferLock);
            }

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
                    _nextOffset[mq] = msgs[msgs.Count - 1].QueueOffset + 1;
                }

                if (_autoCommit) MaybeCommit(mq);
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

    private void MaybeCommit(MessageQueue mq)
    {
        long now = UtilAll.CurrentTimeMillis();
        bool due;
        lock (_lock)
        {
            due = !_lastCommit.TryGetValue(mq, out long last) ||
                  now - last >= _autoCommitIntervalMillis;
        }

        if (!due) return;
        try
        {
            RequireClient().UpdateConsumerOffset(_consumerGroup, mq, NextOffsetOf(mq));
            lock (_lock)
            {
                _lastCommit[mq] = now;
            }
        }
        catch
        {
            ClientLog.Debug("lite auto-commit failed for " + mq);
        }
    }

    private long NextOffsetOf(MessageQueue mq)
    {
        lock (_lock)
        {
            return _nextOffset.TryGetValue(mq, out long o) ? o : 0;
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
            List<MessageQueue> allocated = DefaultMQPushConsumer.AllocateMessageQueueAveragely(_consumerGroup, _clientId, mqAll, cidAll);
            foreach (MessageQueue mq in allocated) newSet.Add(mq);
        }

        HashSet<MessageQueue> old;
        bool changed;
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
                    if (!newSet.Contains(mq))
                    {
                        _nextOffset.Remove(mq);
                        _lastCommit.Remove(mq);
                    }
                }
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
        int timeout = timeoutMillis > 0 ? timeoutMillis : _pollTimeoutMillis;
        long deadline = UtilAll.CurrentTimeMillis() + timeout;
        lock (_bufferLock)
        {
            while (_localBuffer.Count == 0)
            {
                long remaining = deadline - UtilAll.CurrentTimeMillis();
                if (remaining <= 0) return new List<MessageExt>();
                Monitor.Wait(_bufferLock, (int)Math.Min(remaining, int.MaxValue - 1));
            }

            var outMsgs = new List<MessageExt>();
            while (_localBuffer.Count > 0 && outMsgs.Count < 1024)
            {
                outMsgs.Add(_localBuffer.Dequeue());
            }

            return outMsgs;
        }
    }

    public void Seek(MessageQueue mq, long offset)
    {
        lock (_lock)
        {
            _seekOffset[mq] = offset;
            _nextOffset[mq] = offset;
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

    public long Committed(MessageQueue mq)
    {
        if (_mqClient is null) return -1;
        try
        {
            if (RequireClient().QueryConsumerOffset(_consumerGroup, mq, out long off)) return off;
        }
        catch
        {
        }

        return -1;
    }

    public void Commit()
    {
        long now = UtilAll.CurrentTimeMillis();
        Dictionary<MessageQueue, long> snapshot;
        lock (_lock)
        {
            snapshot = new Dictionary<MessageQueue, long>(_nextOffset);
        }

        foreach (KeyValuePair<MessageQueue, long> kv in snapshot)
        {
            try
            {
                RequireClient().UpdateConsumerOffset(_consumerGroup, kv.Key, kv.Value);
                lock (_lock)
                {
                    _lastCommit[kv.Key] = now;
                }
            }
            catch
            {
                ClientLog.Debug("lite commit failed for " + kv.Key);
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
        var cd = new ConsumerData(_consumerGroup, ConsumeType.ConsumePassively, _messageModel, _consumeFromWhere);
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
