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
using System;
using System.Collections.Generic;
using System.Threading;

using RocketMQ.Common;

namespace RocketMQ.Client;

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
    private int _workers;
    private int _seq;
    private bool _shutdown;
    private long _handlerExceptions;

    public ConsumeExecutor(int corePoolSize, int maximumPoolSize,
                           double keepAliveSeconds = 60.0, string threadNamePrefix = "rmq-consume")
    {
        _core = Math.Max(0, corePoolSize);
        _max = Math.Max(_core, maximumPoolSize);
        _keepAliveSeconds = keepAliveSeconds;
        _prefix = threadNamePrefix;
    }

    /// <summary>投递任务（Java execute）。已关闭时抛 InvalidOperationException。</summary>
    public void Submit(Action task)
    {
        if (task == null) throw new ArgumentNullException(nameof(task));
        lock (_gate)
        {
            if (_shutdown) throw new InvalidOperationException("ConsumeExecutor has been shut down");
            _queue.Enqueue(task);
            // Java 无界队列语义：只有 poolSize < corePoolSize 才新建线程；
            // 第二个条件是 execute() 里"入队后 workerCount == 0 再补一个线程"的兜底分支。
            if (_workers < _core || _workers == 0)
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
        string name = _prefix + "-" + _seq++;
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
