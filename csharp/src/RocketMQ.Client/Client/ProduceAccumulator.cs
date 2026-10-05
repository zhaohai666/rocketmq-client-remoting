// 对应 org.apache.rocketmq.client.producer.ProduceAccumulator（Java 5.5.0）。
//
// 生产者打开 AutoBatch 之后，Send(Message) 不再一条一条直发，而是先按
// AggregateKey(topic, mq, waitStoreMsgOK, tag) 归并进 MessageAccumulation，攒够
// holdMs/holdSize（或被守卫线程唤醒）再合成**一个** MessageBatch 发出去，最后把 broker
// 回的**批量** SendResult 拆回每条消息各自的 SendResult —— 调用方看到的东西与直发一致。
//
// 契约（逐条对齐 Java，别"顺手优化"）：
//   1. AggregateKey 是 topic + mq + waitStoreMsgOK + tag 四元组：tag 不同不合并
//      （一个 MessageBatch 只有一个 TAGS 属性）；指定 mq 与不指定 mq 也不合并。
//   2. TryAddMessage 是全局字节闸门：currentlyHoldSize < totalHoldSize 才放行，放行时把
//      本条 body 长度记进去；**批次真的发完**才扣回（同步版在 finally、异步版在回调里）。
//      ⚠ 上游真实口径是「先记账，再判延时/重试」—— CanBatch 里因延时消息退回直发的那条
//      消息，其字节数已被记进 currentlyHoldSize 且**永不归还**（Java 遗漏，照抄）。
//   3. 批量应答拆条：broker 对批量消息回的 MsgId/OffsetMsgId 是**逗号分隔**的逐条 ID；
//      含逗号才拆，条数对不上抛 IllegalArgumentException 对等的异常；不含逗号（老 broker /
//      单条）时**所有**下标指向同一个 SendResult 实例（就地共享，不复制）。
//   4. **同步 Add 收集 keys，异步 Add 不收集**（Java 的不对称行为，照抄）。
//   5. Message.Keys 是 string.Join(" ", keys)：分隔符是**空格**（MessageConst.KEY_SEPARATOR），
//      且**无条件**写属性 —— 空集合写出 KEYS=""。跨进程/跨语言 KEYS 顺序不保证。
//   6. 守卫线程每轮 max(1, holdMs/2) ms：sync 版对每个批次 Wakeup()（叫醒正在 Add 里等阈值
//      的调用方去自查 ReadyToSend），再把 MessagesSize == 0 的空批次置 closed 并摘表；
//      async 版先 ReadyToSend 就 Send，再做同样的摘表。
//      ⚠ 发完的批次 MessagesSize 仍 > 0（Send 只置 closed，不重置 MessagesSize），所以它
//      会**留在表里**，直到下一次同键 Send 拿到它、Add 返回 -1 才被摘掉重取。
//   7. IsBatch 必须置 true：本端发送侧用 msg.IsBatch 判断批量（对应 Java 的
//      msg instanceof MessageBatch），不置就会走单条 SEND_MESSAGE。

using System.Collections.Concurrent;
using RocketMQ.Common;

namespace RocketMQ.Client;

/// <summary>归并键：topic + mq + waitStoreMsgOK + tag（Java ProduceAccumulator.AggregateKey）。</summary>
public sealed class AggregateKey
{
    public string Topic { get; }
    public MessageQueue? Mq { get; }
    public bool WaitStoreMsgOk { get; }
    public string? Tag { get; }

    public AggregateKey(string topic, MessageQueue? mq, bool waitStoreMsgOk, string? tag)
    {
        Topic = topic;
        Mq = mq;
        WaitStoreMsgOk = waitStoreMsgOk;
        Tag = tag;
    }

    public static AggregateKey OfMessage(Message msg) =>
        new(msg.Topic, null, msg.WaitStoreMsgOk, TagOf(msg));

    public static AggregateKey OfMessageWithMq(Message msg, MessageQueue mq) =>
        new(msg.Topic, mq, msg.WaitStoreMsgOk, TagOf(msg));

    /// <summary>Java <c>Message.getTags()</c>：属性缺失返回 null（不是空串）。</summary>
    private static string? TagOf(Message msg) =>
        msg.Properties.TryGetValue(MessageConst.PropertyTags, out var tag) ? tag : null;

    public override bool Equals(object? obj) =>
        obj is AggregateKey other &&
        WaitStoreMsgOk == other.WaitStoreMsgOk &&
        Topic == other.Topic &&
        Equals(Mq, other.Mq) &&
        Tag == other.Tag;

    public override int GetHashCode() => HashCode.Combine(Topic, Mq, WaitStoreMsgOk, Tag);

    public override string ToString() =>
        "AggregateKey(topic=" + Topic + ", mq=" + (Mq?.ToString() ?? "null")
        + ", waitStoreMsgOK=" + WaitStoreMsgOk + ", tag=" + (Tag ?? "null") + ")";
}

/// <summary>一批待归并的消息（Java ProduceAccumulator.MessageAccumulation）。</summary>
public sealed class MessageAccumulation
{
    private readonly DefaultMQProducer _producer;
    private readonly ProduceAccumulator _owner;
    private readonly object _closedLock = new();
    private bool _closed;

    public AggregateKey AggregateKey { get; }
    public List<Message> Messages { get; } = new();
    public List<ISendCallback> SendCallbacks { get; } = new();
    /// <summary>同步 Add 收集、异步 Add 不收集（见文件头第 4 条）。</summary>
    public HashSet<string> Keys { get; } = new();
    public SendResult[] SendResults { get; private set; } = Array.Empty<SendResult>();
    public int Count { get; private set; }
    public int MessagesSize { get; private set; }
    public long CreateTime { get; }

    internal MessageAccumulation(AggregateKey aggregateKey, DefaultMQProducer producer,
        ProduceAccumulator owner)
    {
        AggregateKey = aggregateKey;
        _producer = producer;
        _owner = owner;
        CreateTime = UtilAll.CurrentTimeMillis();
    }

    /// <summary>closed 既是状态位又是锁对象（Java 用 ``synchronized (this.closed)``）。</summary>
    internal object ClosedLock => _closedLock;

    /// <summary>本批是否已被关掉（关掉即"不再接受新消息"，且已经发过/正在发）。
    /// public 只为测试断言用。</summary>
    public bool IsClosed
    {
        get
        {
            lock (_closedLock)
            {
                return _closed;
            }
        }
    }

    internal void MarkClosed()
    {
        lock (_closedLock)
        {
            _closed = true;
        }
    }

    /// <summary>Java <c>readyToSend()</c>：按**本批**字节数或本批存活时间（不是全局限额）。</summary>
    internal bool ReadyToSend() =>
        MessagesSize > _owner.HoldSize ||
        UtilAll.CurrentTimeMillis() >= CreateTime + _owner.HoldMs;

    /// <summary>Java <c>wakeup()</c>：叫醒一个正在 Add 里等阈值的调用方。</summary>
    internal void Wakeup()
    {
        lock (this)
        {
            if (IsClosed)
            {
                return;
            }

            Monitor.PulseAll(this);
        }
    }

    /// <summary>
    /// 同步加入；返回本条消息在本批里的下标，-1 表示本批已关闭（调用方需重取）。
    /// 返回前保证本批**已经发出去**，因此 SendResults[index] 一定可用。
    /// </summary>
    public int Add(Message msg)
    {
        int ret;
        lock (_closedLock)
        {
            if (_closed)
            {
                return -1;
            }

            ret = Count++;
            Messages.Add(msg);
            int bodySize = msg.Body?.Length ?? 0;
            if (bodySize > 0)
            {
                MessagesSize += bodySize;
            }

            string? keys = KeysOf(msg);
            if (keys != null)
            {
                SplitKeysInto(Keys, keys);
            }
        }

        lock (this)
        {
            while (!IsClosed)
            {
                if (ReadyToSend())
                {
                    Send();
                    break;
                }

                Monitor.Wait(this);
            }
        }

        return ret;
    }

    /// <summary>异步加入；false 表示本批已关闭（调用方需重取）。</summary>
    public bool Add(Message msg, ISendCallback sendCallback)
    {
        lock (_closedLock)
        {
            if (_closed)
            {
                return false;
            }

            Count++;
            Messages.Add(msg);
            SendCallbacks.Add(sendCallback);
            int bodySize = msg.Body?.Length ?? 0;
            if (bodySize > 0)
            {
                MessagesSize += bodySize;
            }
        }

        if (ReadyToSend())
        {
            Send(sendCallback);
        }

        return true;
    }

    /// <summary>Java <c>Message.getKeys()</c>：缺属性返回 null；空串返回 ""。</summary>
    private static string? KeysOf(Message msg) =>
        msg.Properties.TryGetValue(MessageConst.PropertyKeys, out var keys) ? keys : null;

    /// <summary>
    /// <c>msg.getKeys().split(MessageConst.KEY_SEPARATOR)</c> 的 Java 等价物：Java 的
    /// <c>String.split</c>（limit=0）**丢弃所有尾部空串**，C# 的 <c>Split</c> 会保留。
    /// </summary>
    private static void SplitKeysInto(HashSet<string> target, string keys)
    {
        string[] parts = keys.Split(MessageConst.KeySeparator);
        int end = parts.Length;
        while (end > 0 && parts[end - 1].Length == 0)
        {
            end--;
        }

        for (int i = 0; i < end; i++)
        {
            target.Add(parts[i]);
        }
    }

    /// <summary>Java <c>batch()</c>：把本批组装成一个 MessageBatch。</summary>
    private MessageBatch Batch()
    {
        var batch = new MessageBatch(new List<Message>(Messages))
        {
            Topic = AggregateKey.Topic,
            WaitStoreMsgOk = AggregateKey.WaitStoreMsgOk,
            // 无条件写（空集合即 KEYS=""，见文件头第 5 条）
            Keys = string.Join(MessageConst.KeySeparator, Keys),
        };
        if (AggregateKey.Tag != null)
        {
            batch.Tags = AggregateKey.Tag;
        }

        MessageClientIDSetter.SetUniqId(batch);
        batch.Body = batch.Encode();
        batch.IsBatch = true; // 见文件头第 7 条
        return batch;
    }

    /// <summary>Java <c>splitSendResults</c>：批量应答拆成逐条 SendResult。</summary>
    private void SplitSendResults(SendResult sendResult)
    {
        string msgId = sendResult.MsgId ?? string.Empty;
        SendResults = new SendResult[Count];
        if (msgId.Contains(','))
        {
            string[] msgIds = msgId.Split(',');
            string[] offsetMsgIds = (sendResult.OffsetMsgId ?? string.Empty).Split(',');
            if (offsetMsgIds.Length != Count || msgIds.Length != Count)
            {
                throw new InvalidOperationException("sendResult is illegal");
            }

            for (int i = 0; i < Count; i++)
            {
                SendResults[i] = new SendResult
                {
                    SendStatus = sendResult.SendStatus,
                    MsgId = msgIds[i],
                    MessageQueue = sendResult.MessageQueue,
                    QueueOffset = sendResult.QueueOffset + i,
                    TransactionId = sendResult.TransactionId,
                    OffsetMsgId = offsetMsgIds[i],
                    RegionId = sendResult.RegionId,
                };
            }
        }
        else
        {
            // 不含逗号：老 broker / 单条应答，所有下标共享同一个实例（Java 同）
            for (int i = 0; i < Count; i++)
            {
                SendResults[i] = sendResult;
            }
        }
    }

    /// <summary>
    /// Java <c>MessageAccumulation.send()</c>（同步）。⚠ 只能在**持有 <c>lock (this)</c>** 时
    /// 调用：Java 的 <c>notifyAll()</c> 靠调用方（Add 里的 <c>synchronized (this)</c>）提供的
    /// 监视器，C# 的 Monitor 同样要求持有锁。测试也直接调它（Java 单测同款：手工触发一次发送，
    /// 不等守卫线程的周期）。
    /// </summary>
    public void Send()
    {
        lock (_closedLock)
        {
            if (_closed)
            {
                return;
            }

            _closed = true;
        }

        MessageBatch batch = Batch();
        try
        {
            // sendCallback == null 的分支在 Java 里返回非 null；C# 侧签名是 SendResult?
            // （异步分支返回 null），这里同步链路断言一次。
            SendResult result = _producer.SendDirect(batch, AggregateKey.Mq, null)
                               ?? throw new InvalidOperationException("sync send returned null");
            SplitSendResults(result);
        }
        finally
        {
            // 无论成败都归还全局字节额度（Java：finally 里 currentlyHoldSize -= messagesSize）
            _owner.ReleaseHold(MessagesSize);
            Monitor.PulseAll(this);
        }
    }

    /// <summary>
    /// Java <c>MessageAccumulation.send(SendCallback)</c>（异步）。参数与 Java 一样**不参与**
    /// 逻辑（回调来自 <see cref="SendCallbacks"/>）；守卫线程传的就是 <c>null</c>。
    /// </summary>
    internal void Send(ISendCallback? sendCallback)
    {
        _ = sendCallback;
        lock (_closedLock)
        {
            if (_closed)
            {
                return;
            }

            _closed = true;
        }

        MessageBatch batch = Batch();
        int size = MessagesSize;

        void OnSuccess(SendResult sendResult)
        {
            try
            {
                SplitSendResults(sendResult);
                int i = 0;
                foreach (ISendCallback cb in SendCallbacks)
                {
                    cb.OnSuccess(SendResults[i++]);
                }

                if (i != Count)
                {
                    throw new InvalidOperationException("sendResult is illegal");
                }

                _owner.ReleaseHold(size);
            }
            catch (Exception e)
            {
                OnException(e);
            }
        }

        void OnException(Exception e)
        {
            foreach (ISendCallback cb in SendCallbacks)
            {
                cb.OnException(e);
            }

            _owner.ReleaseHold(size);
        }

        try
        {
            _producer.SendDirect(batch, AggregateKey.Mq, new InlineSendCallback(OnSuccess, OnException));
        }
        catch (Exception e)
        {
            // ⚠ Java 在这里**没有**归还 currentlyHoldSize（只有回调路径会还）—— 即"异步发送在
            // 发起阶段就抛异常"会漏掉一份字节额度。照抄，别修。
            foreach (ISendCallback cb in SendCallbacks)
            {
                cb.OnException(e);
            }
        }
    }

    /// <summary>把两个回调函数包成 ISendCallback（对应 Java 的匿名内部类）。</summary>
    private sealed class InlineSendCallback : ISendCallback
    {
        private readonly Action<SendResult> _onSuccess;
        private readonly Action<Exception> _onException;

        public InlineSendCallback(Action<SendResult> onSuccess, Action<Exception> onException)
        {
            _onSuccess = onSuccess;
            _onException = onException;
        }

        public void OnSuccess(SendResult sendResult) => _onSuccess(sendResult);

        public void OnException(Exception e) => _onException(e);
    }
}

/// <summary>对应 Java ProduceAccumulator：按 clientId 复用的自动攒批器。</summary>
public sealed class ProduceAccumulator
{
    public const long DefaultTotalHoldSize = 32L * 1024 * 1024;
    public const long DefaultHoldSize = 32L * 1024;
    public const int DefaultHoldMs = 10;

    private readonly object _holdLock = new();
    private readonly ConcurrentDictionary<AggregateKey, MessageAccumulation> _syncSendBatchs = new();
    private readonly ConcurrentDictionary<AggregateKey, MessageAccumulation> _asyncSendBatchs = new();
    private readonly GuardService _guardThreadForSyncSend;
    private readonly GuardService _guardThreadForAsyncSend;
    private long _currentlyHoldSize;
    private int _holdMs = DefaultHoldMs;
    private long _holdSize = DefaultHoldSize;
    private long _totalHoldSize = DefaultTotalHoldSize;

    public string InstanceName { get; }

    public ProduceAccumulator(string instanceName)
    {
        InstanceName = instanceName;
        _guardThreadForSyncSend = new GuardService(this, "GuardForSyncSend", sync: true);
        _guardThreadForAsyncSend = new GuardService(this, "GuardForAsyncSend", sync: false);
    }

    public int HoldMs => Volatile.Read(ref _holdMs);

    public long HoldSize => Volatile.Read(ref _holdSize);

    public long TotalHoldSize => Volatile.Read(ref _totalHoldSize);

    public long CurrentlyHoldSize
    {
        get
        {
            lock (_holdLock)
            {
                return _currentlyHoldSize;
            }
        }
    }

    // ---------------- 取证 / 测试接缝 ----------------
    // 测试工程与本程序集没有 InternalsVisibleTo 关系，这几个用 public，勿用于业务代码。

    /// <summary>同步表里当前的批次数。</summary>
    public int SyncBatchCount => _syncSendBatchs.Count;

    /// <summary>异步表里当前的批次数。</summary>
    public int AsyncBatchCount => _asyncSendBatchs.Count;

    /// <summary>同步表快照（顺序不定）。</summary>
    public MessageAccumulation[] SyncBatchesSnapshot() =>
        _syncSendBatchs.Values.ToArray();

    /// <summary>异步表快照（顺序不定）。</summary>
    public MessageAccumulation[] AsyncBatchesSnapshot() =>
        _asyncSendBatchs.Values.ToArray();

    /// <summary>测试用：往同步表里放一个（尚无人 add 的）批次 —— 用来验证守卫线程对
    /// <c>MessagesSize == 0</c> 的清理口径，正常调用链里这个窗口只有一两行代码那么宽。</summary>
    public MessageAccumulation PutEmptySyncBatch(AggregateKey key, DefaultMQProducer producer) =>
        GetOrCreateSyncSendBatch(key, producer);

    /// <summary>测试用：手工跑一轮守卫（等价于守卫线程某个 <c>DoWork</c> 周期）。</summary>
    public void RunGuardOnce(bool sync) =>
        (sync ? _guardThreadForSyncSend : _guardThreadForAsyncSend).DoOneRound();

    // ---------------- 生命周期 ----------------
    public void Start()
    {
        _guardThreadForSyncSend.Start();
        _guardThreadForAsyncSend.Start();
    }

    public void Shutdown()
    {
        _guardThreadForSyncSend.Shutdown();
        _guardThreadForAsyncSend.Shutdown();
    }

    // ---------------- 参数（Java 的校验口径与文案逐字照抄）----------------
    public int GetBatchMaxDelayMs() => HoldMs;

    public void BatchMaxDelayMs(int holdMs)
    {
        if (holdMs <= 0 || holdMs > 30 * 1000)
        {
            throw new ArgumentException(
                "batchMaxDelayMs expect between 1ms and 30s, but get " + holdMs + "!");
        }

        Volatile.Write(ref _holdMs, holdMs);
    }

    public long GetBatchMaxBytes() => HoldSize;

    public void BatchMaxBytes(long holdSize)
    {
        if (holdSize <= 0 || holdSize > 2 * 1024 * 1024)
        {
            throw new ArgumentException(
                "batchMaxBytes expect between 1B and 2MB, but get " + holdSize + "!");
        }

        Volatile.Write(ref _holdSize, holdSize);
    }

    /// <summary>Java 这里也返回 holdSize（不是 totalHoldSize）—— 上游笔误，照抄。</summary>
    public long GetTotalBatchMaxBytes() => HoldSize;

    public void TotalBatchMaxBytes(long totalHoldSize)
    {
        if (totalHoldSize <= 0)
        {
            throw new ArgumentException(
                "totalBatchMaxBytes must bigger then 0, but get " + totalHoldSize + "!");
        }

        Volatile.Write(ref _totalHoldSize, totalHoldSize);
    }

    // ---------------- 全局字节闸门 ----------------
    /// <summary>Java <c>tryAddMessage</c>：还有额度就记账放行，否则拒绝（调用方退回直发）。</summary>
    public bool TryAddMessage(Message message)
    {
        lock (_holdLock)
        {
            if (_currentlyHoldSize < _totalHoldSize)
            {
                int bodySize = message.Body?.Length ?? 0;
                if (bodySize > 0)
                {
                    _currentlyHoldSize += bodySize;
                }

                return true;
            }

            return false;
        }
    }

    /// <summary>批次发送完成后的归还（Java 直接 <c>currentlyHoldSize.addAndGet(-size)</c>）。</summary>
    public void ReleaseHold(int size)
    {
        lock (_holdLock)
        {
            _currentlyHoldSize -= size;
        }
    }

    // ---------------- 表操作 ----------------
    private MessageAccumulation GetOrCreateSyncSendBatch(AggregateKey key,
        DefaultMQProducer producer) =>
        _syncSendBatchs.GetOrAdd(key, k => new MessageAccumulation(k, producer, this));

    private MessageAccumulation GetOrCreateAsyncSendBatch(AggregateKey key,
        DefaultMQProducer producer) =>
        _asyncSendBatchs.GetOrAdd(key, k => new MessageAccumulation(k, producer, this));

    /// <summary>Java <c>syncSendBatchs.remove(key, batch)</c>：只在值仍是它时才摘。</summary>
    internal void RemoveSyncBatch(AggregateKey key, MessageAccumulation batch) =>
        _syncSendBatchs.TryRemove(new KeyValuePair<AggregateKey, MessageAccumulation>(key, batch));

    internal void RemoveAsyncBatch(AggregateKey key, MessageAccumulation batch) =>
        _asyncSendBatchs.TryRemove(new KeyValuePair<AggregateKey, MessageAccumulation>(key, batch));

    // ---------------- 对外发送入口 ----------------
    /// <summary>Java <c>send(Message, DefaultMQProducer)</c>：只返回本条消息自己的 SendResult。</summary>
    public SendResult Send(Message msg, DefaultMQProducer producer)
    {
        var key = AggregateKey.OfMessage(msg);
        while (true)
        {
            MessageAccumulation batch = GetOrCreateSyncSendBatch(key, producer);
            int index = batch.Add(msg);
            if (index == -1)
            {
                // 本批在本次 Add 之前就被别的线程关掉了：摘掉它，重取/新建一个再试
                RemoveSyncBatch(key, batch);
                continue;
            }

            return batch.SendResults[index];
        }
    }

    public SendResult Send(Message msg, MessageQueue mq, DefaultMQProducer producer)
    {
        var key = AggregateKey.OfMessageWithMq(msg, mq);
        while (true)
        {
            MessageAccumulation batch = GetOrCreateSyncSendBatch(key, producer);
            int index = batch.Add(msg);
            if (index == -1)
            {
                RemoveSyncBatch(key, batch);
                continue;
            }

            return batch.SendResults[index];
        }
    }

    public void SendAsync(Message msg, ISendCallback sendCallback, DefaultMQProducer producer)
    {
        var key = AggregateKey.OfMessage(msg);
        while (true)
        {
            MessageAccumulation batch = GetOrCreateAsyncSendBatch(key, producer);
            if (!batch.Add(msg, sendCallback))
            {
                RemoveAsyncBatch(key, batch);
                continue;
            }

            return;
        }
    }

    public void SendAsync(Message msg, MessageQueue mq, ISendCallback sendCallback,
        DefaultMQProducer producer)
    {
        var key = AggregateKey.OfMessageWithMq(msg, mq);
        while (true)
        {
            MessageAccumulation batch = GetOrCreateAsyncSendBatch(key, producer);
            if (!batch.Add(msg, sendCallback))
            {
                RemoveAsyncBatch(key, batch);
                continue;
            }

            return;
        }
    }

    // ---------------- 守卫线程 ----------------
    /// <summary>对应 Java 的 <c>ServiceThread</c> 子类（两个：同步 / 异步）。</summary>
    private sealed class GuardService
    {
        private readonly ProduceAccumulator _owner;
        private readonly string _serviceName;
        private readonly bool _sync;
        private Thread? _thread;
        private volatile bool _stopped;

        public GuardService(ProduceAccumulator owner, string suffix, bool sync)
        {
            _owner = owner;
            _serviceName = "Client_" + owner.InstanceName + "_" + suffix;
            _sync = sync;
        }

        public void Start()
        {
            if (_thread != null)
            {
                return;
            }

            _stopped = false;
            _thread = new Thread(Run) { IsBackground = true, Name = _serviceName };
            _thread.Start();
        }

        public void Shutdown()
        {
            _stopped = true;
            _thread?.Join(5000);
            // 摘掉句柄，让后续 Start() 能再拉一根新线程：累加器是**按 clientId 复用**的，
            // 同一个实例会经历 start→shutdown→start（Java 的 ServiceThread 同样可重复 start）。
            _thread = null;
        }

        private void Run()
        {
            while (!_stopped)
            {
                DoOneRound();
            }
        }

        /// <summary>一个守卫周期。<see cref="Run"/> 循环调它，测试也手工调它（不必等真实周期）。</summary>
        public void DoOneRound()
        {
            try
            {
                DoWork();
            }
            catch (Exception)
            {
                // Java：日志告警后继续跑（这里静默续跑，与其它守卫线程同口径）
            }
        }

        private void DoWork()
        {
            int sleepTime = Math.Max(1, _owner.HoldMs / 2);
            MessageAccumulation[] values = _sync
                ? _owner.SyncBatchesSnapshot()
                : _owner.AsyncBatchesSnapshot();
            foreach (MessageAccumulation v in values)
            {
                if (_sync)
                {
                    v.Wakeup();
                    lock (v)
                    {
                        lock (v.ClosedLock)
                        {
                            if (v.MessagesSize == 0)
                            {
                                v.MarkClosed();
                                _owner.RemoveSyncBatch(v.AggregateKey, v);
                            }
                            else
                            {
                                Monitor.PulseAll(v);
                            }
                        }
                    }
                }
                else
                {
                    if (v.ReadyToSend())
                    {
                        // Java `v.send(null)`：异步版（参数不参与逻辑，回调来自本批自己收集的
                        // 那一串）。写 `v.Send()` 会落到**同步**重载上 —— 那是另一个语义。
                        v.Send(null);
                    }

                    lock (v.ClosedLock)
                    {
                        if (v.MessagesSize == 0)
                        {
                            v.MarkClosed();
                            _owner.RemoveAsyncBatch(v.AggregateKey, v);
                        }
                    }
                }
            }

            Thread.Sleep(sleepTime);
        }
    }
}

/// <summary>
/// 进程级复用表：对应 Java <c>MQClientManager.getOrCreateProduceAccumulator</c> —— 按 clientId
/// 缓存，所以同进程里两个 clientId 相同的 producer 共享同一个累加器与同一对守卫线程
/// （这也是「阈值先记在 producer 上、Start() 时再同步下去」的原因）。
/// </summary>
public static class ProduceAccumulatorRegistry
{
    private static readonly ConcurrentDictionary<string, ProduceAccumulator> Table = new();

    public static ProduceAccumulator GetOrCreate(string clientId) =>
        Table.GetOrAdd(clientId, id => new ProduceAccumulator(id));
}
