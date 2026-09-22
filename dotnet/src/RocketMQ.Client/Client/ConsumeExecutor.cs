// 消费执行器：Java ThreadPoolExecutor 的最小等价物（core/max 两档）。
//
// 为什么不直接用 System.Threading.ThreadPool / Task：
//   * ThreadPool 的并发度由 .NET 运行时统一管理（SetMinThreads 是**进程级**全局设置，
//     改它会波及宿主应用的所有线程池使用者），无法按消费者实例设置 core；
//   * Java 在 LinkedBlockingQueue（无界）下 **真实并发度 == corePoolSize**
//     （poolSize < corePoolSize 才新建线程，否则入队），而线程弹性
//     （AbstractConsumeMessageService.updateCorePoolSize → setCorePoolSize）改的正是它。
//
// 对齐点（与 Python consume_executor.py / C++ consume_executor.h 逐条同源）：
//   1. 投递时只有 workers < core 才新建线程；否则入队。
//   2. 入队后若 workers == 0 就补一个线程（Java execute 的兜底分支，core=0 时必须）。
//   3. > core 的线程空闲超过 keepAlive 退出；<= core 的线程永不退出
//      （Java allowCoreThreadTimeOut 默认 false）。
//   4. SetCorePoolSize(n)：core 变大时按 min(delta, 队列长度) 补线程（Java 的启发式算法）。
//   5. 任务抛异常不杀 worker（Java 会补一个新 worker，效果等价）。
//
// 队列也可以是有界的（maxQueueSize），对应 Java 的 LinkedBlockingQueue(50000)：生产者的
// 异步发送池（AsyncSenderExecutor）就是这一档 —— 队列满了 Submit 抛
// RejectedExecutionException（等价 Java 的 RejectedExecutionException），由调用方决定是
// 报错还是就地跑完（Java executeAsyncMessageSend:670-681 两种都有）。
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Threading;

using RocketMQ.Common;

namespace RocketMQ.Client;

/// <summary>队列已满或线程池已关闭，任务被拒（对应 Java
/// <code>java.util.concurrent.RejectedExecutionException</code>）。
/// 派生自 <see cref="InvalidOperationException"/>：本端口原先就是用它报「池已关闭」，
/// 老调用方的 catch 不会因为换了类型而漏接。</summary>
public class RejectedExecutionException : InvalidOperationException
{
    public RejectedExecutionException(string message) : base(message)
    {
    }
}

public sealed class ConsumeExecutor : IDisposable
{
    private readonly object _gate = new();
    private readonly Queue<Action> _queue = new();
    // 所有**曾经**创建过的线程都留在这里（只增不减）：已退出的线程 join 立即返回，
    // 而留着句柄才能在 Dispose 时确保没有线程还在跑（否则会 use-after-free / 后台线程泄漏）。
    private readonly List<Thread> _threads = new();

    private int _core;
    private int _max;
    private readonly double _keepAliveSeconds;
    private readonly string _prefix;
    // 线程名的分隔符与起始序号（Java ThreadFactoryImpl 的序号从 **1** 开始，且各执行器分隔符
    // 不同：消费池是 `ConsumeMessageThread_`、异步发送池是 `AsyncSenderExecutor_`）。
    private readonly string _nameSep;
    private readonly int _maxQueueSize;
    private int _workers;
    private int _seq;
    private bool _shutdown;
    private long _handlerExceptions;

    /// <param name="maxQueueSize">0 = 无界（Java `LinkedBlockingQueue()`，消费池用这一档）；
    /// &gt; 0 即 Java 的 `LinkedBlockingQueue(N)`，投满时 <see cref="Submit"/> 抛
    /// <see cref="RejectedExecutionException"/>（生产者的异步发送池用这一档）。</param>
    public ConsumeExecutor(int corePoolSize, int maximumPoolSize,
                           double keepAliveSeconds = 60.0, string threadNamePrefix = "rmq-consume",
                           int maxQueueSize = 0, string threadNameSep = "-",
                           int threadIndexFrom = 0)
    {
        _core = Math.Max(0, corePoolSize);
        _max = Math.Max(_core, maximumPoolSize);
        _keepAliveSeconds = keepAliveSeconds;
        _prefix = threadNamePrefix;
        _maxQueueSize = Math.Max(0, maxQueueSize);
        _nameSep = threadNameSep;
        _seq = threadIndexFrom;
    }

    /// <summary>投递任务（Java execute）。已关闭、或有界队列已满且线程数已到 max 时抛
    /// <see cref="RejectedExecutionException"/>（Java 的 RejectedExecutionException）。</summary>
    public void Submit(Action task)
    {
        if (task == null) throw new ArgumentNullException(nameof(task));
        lock (_gate)
        {
            if (_shutdown) throw new RejectedExecutionException("ConsumeExecutor has been shut down");
            // Java：入队失败（队列满）才考虑开一个非 core 线程，再不行就 reject
            bool queueFull = _maxQueueSize > 0 && _queue.Count >= _maxQueueSize;
            bool canGrow = _workers < _max;
            if (queueFull && !canGrow)
            {
                throw new RejectedExecutionException("ConsumeExecutor queue is full ("
                                                    + _maxQueueSize.ToString(CultureInfo.InvariantCulture)
                                                    + ")");
            }

            _queue.Enqueue(task);
            // Java 无界队列语义：只有 poolSize < corePoolSize 才新建线程；
            // 第二个条件是 execute() 里"入队后 workerCount == 0 再补一个线程"的兜底分支。
            if (_workers < _core || (queueFull && canGrow) || _workers == 0)
            {
                SpawnLocked();
            }
            Monitor.Pulse(_gate);
        }
    }

    /// <summary>对应 Java ThreadPoolExecutor.setCorePoolSize。</summary>
    public void SetCorePoolSize(int n)
    {
        if (n < 0) throw new ArgumentOutOfRangeException(nameof(n), "core pool size must be >= 0");
        lock (_gate)
        {
            int delta = n - _core;
            _core = n;
            if (n > _max) _max = n;   // Java 允许 core > max（等价于把 max 抬到 core）
            if (delta > 0 && !_shutdown)
            {
                // Java setCorePoolSize 的启发式：k = min(delta, 队列长度)，队列一空就停。
                int k = Math.Min(delta, _queue.Count);
                while (k > 0 && _workers < _max)
                {
                    SpawnLocked();
                    k--;
                    if (_queue.Count == 0) break;
                }
            }
        }
    }

    public int GetCorePoolSize() { lock (_gate) return _core; }
    public int GetMaximumPoolSize() { lock (_gate) return _max; }
    /// <summary>当前存活 worker 数（Java getPoolSize）。</summary>
    public int WorkerCount() { lock (_gate) return _workers; }
    /// <summary>队列中待执行任务数（Java getQueue().size()）。</summary>
    public int QueuedCount() { lock (_gate) return _queue.Count; }
    public long HandlerExceptionCount() => Interlocked.Read(ref _handlerExceptions);

    /// <summary>对应 Java shutdown：不再接收新任务，把队列跑完（**不是** shutdownNow）。</summary>
    public void Shutdown(bool wait = false)
    {
        List<Thread> pending;
        lock (_gate)
        {
            _shutdown = true;
            Monitor.PulseAll(_gate);
            pending = new List<Thread>(_threads);
        }
        if (!wait) return;
        foreach (Thread t in pending)
        {
            // 不能 Join 自己（工作线程不会走到这里，只有外部调用者会）
            if (t.IsAlive && t != Thread.CurrentThread) t.Join();
        }
    }

    public void Dispose() => Shutdown(true);

    // ---------------------------------------------------------------- 内部

    private void SpawnLocked()
    {
        _workers++;
        string name = _prefix + _nameSep + _seq++.ToString(CultureInfo.InvariantCulture);
        var t = new Thread(Run)
        {
            IsBackground = true,
            Name = name,
        };
        _threads.Add(t);
        t.Start();
    }

    private void Run()
    {
        for (; ; )
        {
            Action task;
            lock (_gate)
            {
                while (_queue.Count == 0 && !_shutdown)
                {
                    // Java allowCoreThreadTimeOut 默认 false：只有超编线程会超时退出，
                    // 所以等待时长的"到期"只对 workers > core 有意义。
                    Monitor.Wait(_gate, TimeSpan.FromSeconds(_keepAliveSeconds));
                    if (_queue.Count > 0 || _shutdown) break;
                    if (_workers > _core)
                    {
                        _workers--;
                        return;
                    }
                }
                if (_shutdown && _queue.Count == 0)
                {
                    _workers--;
                    return;
                }
                task = _queue.Dequeue();
            }
            try
            {
                task();
            }
            catch (Exception e)
            {
                // 任务异常不能杀 worker（Java 会补一个新 worker，效果等价）。
                Interlocked.Increment(ref _handlerExceptions);
                ClientLog.Warn("consume executor task raised: " + e);
            }
        }
    }
}
