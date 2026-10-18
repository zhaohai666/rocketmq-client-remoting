using System;
using System.Collections.Generic;
using System.Globalization;
using System.Threading;

using RocketMQ.Common;

namespace RocketMQ.Client;

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
    // All threads that were **ever** created stay here (append-only): a thread that
    // already exited returns from Join immediately, but keeping the handles lets
    // Dispose make sure no thread is still running (otherwise use-after-free /
    // background thread leaks).
    private readonly List<Thread> _threads = new();

    private int _core;
    private int _max;
    private readonly double _keepAliveSeconds;
    private readonly string _prefix;
    private readonly string _nameSep;
    private readonly int _maxQueueSize;
    private int _workers;
    private int _seq;
    private bool _shutdown;
    private long _handlerExceptions;

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

    public void Submit(Action task)
    {
        if (task == null) throw new ArgumentNullException(nameof(task));
        lock (_gate)
        {
            if (_shutdown) throw new RejectedExecutionException("ConsumeExecutor has been shut down");
            bool queueFull = _maxQueueSize > 0 && _queue.Count >= _maxQueueSize;
            bool canGrow = _workers < _max;
            if (queueFull && !canGrow)
            {
                throw new RejectedExecutionException("ConsumeExecutor queue is full ("
                                                    + _maxQueueSize.ToString(CultureInfo.InvariantCulture)
                                                    + ")");
            }

            _queue.Enqueue(task);
            if (_workers < _core || (queueFull && canGrow) || _workers == 0)
            {
                SpawnLocked();
            }
            Monitor.Pulse(_gate);
        }
    }

    public void SetCorePoolSize(int n)
    {
        if (n < 0) throw new ArgumentOutOfRangeException(nameof(n), "core pool size must be >= 0");
        lock (_gate)
        {
            int delta = n - _core;
            _core = n;
            if (n > _max) _max = n;
            if (delta > 0 && !_shutdown)
            {
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
    public int WorkerCount() { lock (_gate) return _workers; }
    public int QueuedCount() { lock (_gate) return _queue.Count; }
    public long HandlerExceptionCount() => Interlocked.Read(ref _handlerExceptions);

    public void Shutdown(bool wait = false) => Shutdown(wait, 0);

    // awaitTerminationMillis semantics: <=0 waits forever (this port's default - worker
    // threads capture `this`, so they must be joined before it can be released);
    // >0 bounds the wait and returns without the join once the budget expires.
    public void Shutdown(bool wait, int timeoutMillis)
    {
        List<Thread> pending;
        lock (_gate)
        {
            _shutdown = true;
            Monitor.PulseAll(_gate);
            pending = new List<Thread>(_threads);
        }
        if (!wait) return;
        long deadline = timeoutMillis > 0
            ? Environment.TickCount64 + timeoutMillis
            : 0;
        foreach (Thread t in pending)
        {
            // Never Join the current thread (worker threads never reach this
            // point; only external callers do)
            if (!t.IsAlive || t == Thread.CurrentThread) continue;
            if (deadline == 0)
            {
                t.Join();
                continue;
            }

            long remaining = deadline - Environment.TickCount64;
            if (remaining <= 0) break;
            t.Join((int)Math.Min(remaining, int.MaxValue));
        }
    }

    public void Dispose() => Shutdown(true);

    // ---------------------------------------------------------------- internals

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
                Interlocked.Increment(ref _handlerExceptions);
                ClientLog.Warn("consume executor task raised: " + e);
            }
        }
    }
}
