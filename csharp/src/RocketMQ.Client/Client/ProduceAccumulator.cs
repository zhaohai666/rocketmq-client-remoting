// Once the producer turns AutoBatch on, Send(Message) no longer sends one by one
// directly: messages are first merged into a MessageAccumulation keyed by
// AggregateKey(topic, mq, waitStoreMsgOK, tag), and once holdMs/holdSize is reached
// (or the guard thread wakes it up) they are combined into **one** MessageBatch that is
// sent out; finally the **batch** SendResult returned by the broker is split back into
// a per-message SendResult -- what the caller sees is the same as with a direct send.

using System.Collections.Concurrent;
using RocketMQ.Common;

namespace RocketMQ.Client;

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

public sealed class MessageAccumulation
{
    private readonly DefaultMQProducer _producer;
    private readonly ProduceAccumulator _owner;
    private readonly object _closedLock = new();
    private bool _closed;

    public AggregateKey AggregateKey { get; }
    public List<Message> Messages { get; } = new();
    public List<ISendCallback> SendCallbacks { get; } = new();
    /// <summary>A synchronous Add accumulates, an asynchronous Add does not (see item 4 in
    /// the file header).</summary>
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

    internal object ClosedLock => _closedLock;

    /// <summary>Whether this batch has already been closed (closed means "no longer
    /// accepting new messages", and it has already been sent or is being sent).
    /// public only so tests can assert on it.</summary>
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

    internal bool ReadyToSend() =>
        MessagesSize > _owner.HoldSize ||
        UtilAll.CurrentTimeMillis() >= CreateTime + _owner.HoldMs;

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
    /// Synchronous add; returns the index of this message inside the batch, -1 meaning the
    /// batch was closed (the caller has to fetch a new one). Before returning, the batch is
    /// guaranteed to have **already been sent**, so SendResults[index] is always usable.
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

    /// <summary>Asynchronous add; false means the batch was closed (the caller has to fetch
    /// a new one).</summary>
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

    private static string? KeysOf(Message msg) =>
        msg.Properties.TryGetValue(MessageConst.PropertyKeys, out var keys) ? keys : null;

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

    private MessageBatch Batch()
    {
        var batch = new MessageBatch(new List<Message>(Messages))
        {
            Topic = AggregateKey.Topic,
            WaitStoreMsgOk = AggregateKey.WaitStoreMsgOk,
            // Unconditional write (an empty collection means KEYS="", see item 5 in the file header)
            Keys = string.Join(MessageConst.KeySeparator, Keys),
        };
        if (AggregateKey.Tag != null)
        {
            batch.Tags = AggregateKey.Tag;
        }

        MessageClientIDSetter.SetUniqId(batch);
        batch.Body = batch.Encode();
        batch.IsBatch = true; // see item 7 in the file header
        return batch;
    }

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
            for (int i = 0; i < Count; i++)
            {
                SendResults[i] = sendResult;
            }
        }
    }

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
            SendResult result = _producer.SendDirect(batch, AggregateKey.Mq, null)
                               ?? throw new InvalidOperationException("sync send returned null");
            SplitSendResults(result);
        }
        finally
        {
            _owner.ReleaseHold(MessagesSize);
            Monitor.PulseAll(this);
        }
    }

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
            foreach (ISendCallback cb in SendCallbacks)
            {
                cb.OnException(e);
            }
        }
    }

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

    // ---------------- Evidence gathering / test seams ----------------
    // The test project has no InternalsVisibleTo relationship with this assembly, so these
    // are public; do not use them from business code.

    /// <summary>The current number of batches in the synchronous table.</summary>
    public int SyncBatchCount => _syncSendBatchs.Count;

    /// <summary>The current number of batches in the asynchronous table.</summary>
    public int AsyncBatchCount => _asyncSendBatchs.Count;

    /// <summary>A snapshot of the synchronous table (order unspecified).</summary>
    public MessageAccumulation[] SyncBatchesSnapshot() =>
        _syncSendBatchs.Values.ToArray();

    /// <summary>A snapshot of the asynchronous table (order unspecified).</summary>
    public MessageAccumulation[] AsyncBatchesSnapshot() =>
        _asyncSendBatchs.Values.ToArray();

    /// <summary>For tests: puts a (not yet added to) batch into the synchronous table --
    /// used to verify the guard thread's cleanup rule for <c>MessagesSize == 0</c>, a window
    /// that is only a line or two wide in the normal call chain.</summary>
    public MessageAccumulation PutEmptySyncBatch(AggregateKey key, DefaultMQProducer producer) =>
        GetOrCreateSyncSendBatch(key, producer);

    /// <summary>For tests: manually runs one guard round (equivalent to one
    /// <c>DoWork</c> cycle of the guard thread).</summary>
    public void RunGuardOnce(bool sync) =>
        (sync ? _guardThreadForSyncSend : _guardThreadForAsyncSend).DoOneRound();

    // ---------------- Lifecycle ----------------
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

    public void ReleaseHold(int size)
    {
        lock (_holdLock)
        {
            _currentlyHoldSize -= size;
        }
    }

    // ---------------- Table operations ----------------
    private MessageAccumulation GetOrCreateSyncSendBatch(AggregateKey key,
        DefaultMQProducer producer) =>
        _syncSendBatchs.GetOrAdd(key, k => new MessageAccumulation(k, producer, this));

    private MessageAccumulation GetOrCreateAsyncSendBatch(AggregateKey key,
        DefaultMQProducer producer) =>
        _asyncSendBatchs.GetOrAdd(key, k => new MessageAccumulation(k, producer, this));

    internal void RemoveSyncBatch(AggregateKey key, MessageAccumulation batch) =>
        _syncSendBatchs.TryRemove(new KeyValuePair<AggregateKey, MessageAccumulation>(key, batch));

    internal void RemoveAsyncBatch(AggregateKey key, MessageAccumulation batch) =>
        _asyncSendBatchs.TryRemove(new KeyValuePair<AggregateKey, MessageAccumulation>(key, batch));

    public SendResult Send(Message msg, DefaultMQProducer producer)
    {
        var key = AggregateKey.OfMessage(msg);
        while (true)
        {
            MessageAccumulation batch = GetOrCreateSyncSendBatch(key, producer);
            int index = batch.Add(msg);
            if (index == -1)
            {
                // This batch was closed by another thread before this Add: remove it,
                // fetch/create a new one and try again
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
            _thread = null;
        }

        private void Run()
        {
            while (!_stopped)
            {
                DoOneRound();
            }
        }

        /// <summary>One guard cycle. <see cref="Run"/> calls it in a loop, and tests
        /// call it manually (no need to wait for a real cycle).</summary>
        public void DoOneRound()
        {
            try
            {
                DoWork();
            }
            catch (Exception)
            {
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

public static class ProduceAccumulatorRegistry
{
    private static readonly ConcurrentDictionary<string, ProduceAccumulator> Table = new();

    public static ProduceAccumulator GetOrCreate(string clientId) =>
        Table.GetOrAdd(clientId, id => new ProduceAccumulator(id));
}
