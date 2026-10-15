using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

using RocketMQ.Common;
using RocketMQ.Remoting;
using RocketMQ.Remoting.Protocol;

namespace RocketMQ.Client;

public interface IPullTaskCallback
{
    void DoPullTask(MessageQueue mq, PullTaskContext context);
}

public sealed class PullTaskContext
{
    public const int DefaultNextDelayMillis = 200;

    public const int FailureNextDelayMillis = 1000;

    public DefaultMQPullConsumer PullConsumer { get; }

    public int PullNextDelayTimeMillis { get; set; } = DefaultNextDelayMillis;

    internal PullTaskContext(DefaultMQPullConsumer pullConsumer) => PullConsumer = pullConsumer;
}

public sealed class PullConsumerScheduleService : IDisposable
{
    private readonly object _lock = new();

    private readonly ConcurrentDictionary<MessageQueue, PullTask> _taskTable = new();
    private readonly ConcurrentDictionary<string, IPullTaskCallback> _callbackTable =
        new(StringComparer.Ordinal);

    private DefaultMQPullConsumer _pullConsumer;
    private int _pullThreadNums = 20;
    private int _rebalanceScanIntervalMillis = 20000;

    private ConsumeExecutor? _executor;
    private Thread? _scanThread;
    private volatile bool _scanStop;
    private volatile bool _stopped = true;
    private bool _started;

    public PullConsumerScheduleService(string consumerGroup = MixAll.DefaultConsumerGroup)
    {
        _pullConsumer = new DefaultMQPullConsumer(consumerGroup);
        _pullConsumer.SetMessageModel(MessageModel.Clustering);
    }

    public PullConsumerScheduleService(string consumerGroup, IRpcHook rpcHook)
        : this(consumerGroup)
    {
        _pullConsumer.SetRpcHook(rpcHook);
    }

    // ---------------- Configuration ----------------

    public DefaultMQPullConsumer PullConsumer
    {
        get
        {
            lock (_lock)
            {
                return _pullConsumer;
            }
        }
    }

    public void SetPullConsumer(DefaultMQPullConsumer consumer)
    {
        lock (_lock)
        {
            if (_started)
            {
                throw new MQClientException("pull consumer can not be replaced after start");
            }

            _pullConsumer = consumer ?? throw new MQClientException("pull consumer is null");
        }
    }

    public int PullThreadNums
    {
        get => _pullThreadNums;
        set => _pullThreadNums = Math.Max(1, value);
    }

    /// <summary>Scan interval in milliseconds (default 20000, clamped to a minimum of
    /// 100). A change takes effect on the next round.</summary>
    public int RebalanceScanIntervalMillis
    {
        get => _rebalanceScanIntervalMillis;
        set => _rebalanceScanIntervalMillis = value;
    }

    public IReadOnlyDictionary<string, IPullTaskCallback> CallbackTable => _callbackTable;

    public int TaskCount => _taskTable.Count;

    public bool IsStarted => _started;

    public string ConsumerGroup => _pullConsumer.ConsumerGroup;

    // ---------------- Registration ----------------

    public void RegisterPullTaskCallback(string topic, IPullTaskCallback callback)
    {
        ArgumentNullException.ThrowIfNull(callback);
        if (string.IsNullOrEmpty(topic))
        {
            return;
        }

        _callbackTable[topic] = callback;
        _pullConsumer.RegisterMessageQueueListener(topic, null);
    }

    /// <summary>The same registration in delegate form (this port's functional
    /// convenience; the semantics are verbatim identical to the interface version).</summary>
    public void RegisterPullTaskCallback(string topic, Action<MessageQueue, PullTaskContext> callback) =>
        RegisterPullTaskCallback(topic, new DelegatePullTaskCallback(callback));

    // ---------------- Lifecycle ----------------

    public void Start()
    {
        lock (_lock)
        {
            if (_started)
            {
                return;
            }

            DefaultMQPullConsumer consumer = _pullConsumer;
            _stopped = false;
            _scanStop = false;
            _executor = new ConsumeExecutor(_pullThreadNums, _pullThreadNums,
                threadNamePrefix: "rmq-pull-sched-" + consumer.ConsumerGroup);
            try
            {
                consumer.Start();
            }
            catch
            {
                _executor.Shutdown();
                _executor = null;
                _stopped = true;
                throw;
            }

            _started = true;
        }

        ScanOnce();
        _scanThread = new Thread(ScanLoop)
        {
            IsBackground = true,
            Name = "rmq-pull-sched-scan-" + ConsumerGroup,
        };
        _scanThread.Start();
        ClientLog.Info("PullConsumerScheduleService start OK, " + ConsumerGroup
                       + ", callbacks=" + _callbackTable.Count);
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
        }

        _stopped = true;
        _scanStop = true;
        if (_scanThread is { IsAlive: true })
        {
            _scanThread.Join(2000);
        }

        foreach (PullTask task in _taskTable.Values.ToArray())
        {
            task.Cancelled = true;
            task.Disarm();
        }

        _taskTable.Clear();

        _executor?.Shutdown();
        _executor = null;
        _pullConsumer.Shutdown();
        ClientLog.Info("PullConsumerScheduleService shutdown OK, " + ConsumerGroup);
    }

    public void Dispose() => Shutdown();

    // ---------------- Task expansion ----------------

    /// <summary>The scan loop (a background thread hung off the tail of Start): reconciles
    /// once every <see cref="RebalanceScanIntervalMillis"/>. It sleeps in slices (rechecking
    /// the shutdown flag every 100ms, the same rule as the facade's HeartbeatLoop), so
    /// Shutdown exits the current sleep segment within 100ms; exceptions are always caught
    /// per topic inside ScanOnce, and this outer layer adds one more guard so the loop
    /// thread cannot be broken through (being broken through means queue additions and
    /// removals are never handled again).</summary>
    private void ScanLoop()
    {
        while (!_scanStop && !_stopped)
        {
            int interval = Math.Max(100, _rebalanceScanIntervalMillis);
            for (int slept = 0; slept < interval && !_scanStop; slept += 100)
            {
                Thread.Sleep(Math.Min(100, interval - slept));
            }

            if (_scanStop || _stopped)
            {
                break;
            }

            try
            {
                ScanOnce();
            }
            catch (Exception e)
            {
                ClientLog.Warn("pull schedule: scan loop error: " + e.Message);
            }
        }
    }

    public void ScanOnce()
    {
        foreach (string topic in _callbackTable.Keys.ToArray())
        {
            try
            {
                PutTask(topic, _pullConsumer.FetchMessageQueuesInBalance(topic));
            }
            catch (Exception e)
            {
                ClientLog.Warn("pull schedule: rebalance scan for " + topic + " failed: "
                               + e.Message);
            }
        }
    }

    public void PutTask(string topic, IReadOnlyCollection<MessageQueue> mqNewSet)
    {
        var mqSet = new HashSet<MessageQueue>(mqNewSet);
        foreach (KeyValuePair<MessageQueue, PullTask> pair in _taskTable.ToArray())
        {
            if (pair.Key.Topic != topic)
            {
                continue;
            }

            if (!mqSet.Contains(pair.Key))
            {
                pair.Value.Cancelled = true;
                pair.Value.Disarm();
                _taskTable.TryRemove(pair.Key, out _);
            }
        }

        foreach (MessageQueue mq in mqSet)
        {
            var task = new PullTask(mq);
            if (_taskTable.TryAdd(mq, task))
            {
                ScheduleTask(task, 0);
            }
        }
    }

    private void ScheduleTask(PullTask task, int delayMillis)
    {
        if (task.Cancelled || _stopped)
        {
            return;
        }

        ConsumeExecutor? executor = _executor;
        if (executor is null)
        {
            return;
        }

        task.Arm(() =>
        {
            // The pool is already closed (a Shutdown race): only this round is lost, the exception does not blow up the delay carrier
            try
            {
                executor.Submit(() => RunTask(task));
            }
            catch (Exception e)
            {
                ClientLog.Debug("pull schedule: task submit rejected: " + e.Message);
            }
        }, Math.Max(0, delayMillis));
    }

    private void RunTask(PullTask task)
    {
        if (task.Cancelled)
        {
            ClientLog.Warn("The Pull Task is cancelled, " + task.Mq);
            return;
        }

        if (!_callbackTable.TryGetValue(task.Mq.Topic, out IPullTaskCallback? callback))
        {
            ClientLog.Warn("Pull Task Callback not exist , " + task.Mq.Topic);
            return;
        }

        var context = new PullTaskContext(_pullConsumer);
        try
        {
            callback.DoPullTask(task.Mq, context);
        }
        catch (Exception e)
        {
            context.PullNextDelayTimeMillis = PullTaskContext.FailureNextDelayMillis;
            ClientLog.Error("doPullTask Exception, mq=" + task.Mq + ": " + e.Message);
        }

        if (task.Cancelled)
        {
            ClientLog.Warn("The Pull Task is cancelled after doPullTask, " + task.Mq);
            return;
        }

        ScheduleTask(task, context.PullNextDelayTimeMillis);
    }

    /// <summary>A thin delegate -> IPullTaskCallback wrapper (for the functional
    /// registration overload).</summary>
    private sealed class DelegatePullTaskCallback(Action<MessageQueue, PullTaskContext> inner)
        : IPullTaskCallback
    {
        public void DoPullTask(MessageQueue mq, PullTaskContext context) => inner(mq, context);
    }

    private sealed class PullTask
    {
        private readonly object _gate = new();
        private CancellationTokenSource? _pending;

        public PullTask(MessageQueue mq) => Mq = mq;

        internal MessageQueue Mq { get; }

        internal volatile bool Cancelled;

        /// <summary>Runs fire after delayMillis (fire itself usually only submits the work
        /// to the executor pool). A task that was already cancelled is not scheduled.</summary>
        internal void Arm(Action fire, int delayMillis)
        {
            CancellationTokenSource? previous;
            var cts = new CancellationTokenSource();
            lock (_gate)
            {
                if (Cancelled)
                {
                    cts.Dispose();
                    return;
                }

                previous = _pending;
                _pending = cts;
            }

            // The wait position of the previous round is withdrawn (on the normal path it has already fired; this is a defensive cleanup)
            CancelQuietly(previous);

            _ = Task.Delay(Math.Max(0, delayMillis), cts.Token).ContinueWith(armed =>
            {
                // A cancelled/already-released round is simply discarded; only a successful due-time delivery counts
                if (!armed.IsCompletedSuccessfully)
                {
                    return;
                }

                try
                {
                    fire();
                }
                catch (Exception e)
                {
                    ClientLog.Debug("pull schedule: delayed arm failed: " + e.Message);
                }
            }, TaskScheduler.Default);
        }

        /// <summary>Withdraws a round that is waiting at its due time (cancel + release the
        /// handle). A round that has already fired or is running is unaffected and is
        /// finalised by the Cancelled check in RunTask.</summary>
        internal void Disarm()
        {
            CancellationTokenSource? pending;
            lock (_gate)
            {
                pending = _pending;
                _pending = null;
            }

            CancelQuietly(pending);
        }

        private static void CancelQuietly(CancellationTokenSource? cts)
        {
            if (cts is null)
            {
                return;
            }

            try
            {
                cts.Cancel();
            }
            catch (ObjectDisposedException)
            {
                // Already released elsewhere: equivalent to already withdrawn, nothing to do
            }

            cts.Dispose();
        }
    }
}
