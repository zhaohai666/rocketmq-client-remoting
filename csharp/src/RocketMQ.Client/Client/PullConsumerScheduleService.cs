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

    // ---------------- 配置 ----------------

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

    /// <summary>扫描周期毫秒（默认 20000，下限钳 100）。改在下一圈生效。</summary>
    public int RebalanceScanIntervalMillis
    {
        get => _rebalanceScanIntervalMillis;
        set => _rebalanceScanIntervalMillis = value;
    }

    public IReadOnlyDictionary<string, IPullTaskCallback> CallbackTable => _callbackTable;

    public int TaskCount => _taskTable.Count;

    public bool IsStarted => _started;

    public string ConsumerGroup => _pullConsumer.ConsumerGroup;

    // ---------------- 注册 ----------------

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

    /// <summary>delegate 形状的同款注册（本端口的函数式便车，语义与接口版逐字一致）。</summary>
    public void RegisterPullTaskCallback(string topic, Action<MessageQueue, PullTaskContext> callback) =>
        RegisterPullTaskCallback(topic, new DelegatePullTaskCallback(callback));

    // ---------------- 生命周期 ----------------

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

    // ---------------- 任务展开 ----------------

    /// <summary>扫描循环（Start 尾挂起的后台线程）：每 <see cref="RebalanceScanIntervalMillis"/>
    /// 对账一次。分片睡（每 100ms 复查停机位，口径同门面 HeartbeatLoop），Shutdown 最多
    /// 100ms 就能退出当前睡眠段；异常一律在 ScanOnce 内部被逐 topic 接住，这里再兜一层
    /// 防循环线程被打穿（打穿 = 队列增减从此不再响应）。</summary>
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
            // 池已关（Shutdown 竞态）只丢这一轮，不让异常炸掉延迟载体
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

    /// <summary>delegate → IPullTaskCallback 的薄包装（给函数式注册重载用）。</summary>
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

        /// <summary>delayMillis 后跑 fire（fire 本身通常只把活投进执行池）。已被取消的任务
        /// 不再排。</summary>
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

            // 撤旧一轮的等点位（正常路径旧的已触发，这里是防御性收口）
            CancelQuietly(previous);

            _ = Task.Delay(Math.Max(0, delayMillis), cts.Token).ContinueWith(armed =>
            {
                // 被取消/已释放的一轮直接作废；到点成功才投递
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

        /// <summary>撤掉在等点位的一轮（取消 + 释放句柄）。已触发或在跑的轮次不受影响，
        /// 由 RunTask 的 Cancelled 检查收口。</summary>
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
                // 已被别处释放：等价于已撤防，无需处理
            }

            cts.Dispose();
        }
    }
}
