// 公平性只保证「队列头部才可能拿到」，不保证拿到顺序严格等于到达顺序：超时的等待者会
// 从队列里摘掉自己，摘除瞬间顺序由剩余等待者的到达顺序决定 —— 与 Semaphore(true) 一致。
namespace RocketMQ.Client;

public sealed class FairSemaphore
{
    public const long MinAsyncSendNum = 10;

    public const long MinAsyncSendSize = 1024 * 1024;

    // 一个还在排队的许可申请（只为让队首能被稳定识别，不代表已拿到许可）。
    private sealed class Waiter
    {
        public Waiter(long permits) { Permits = permits; }

        public long Permits { get; }
    }

    private readonly object _sync = new();
    private readonly LinkedList<Waiter> _queue = new();
    private long _total;
    private long _free;

    public FairSemaphore(long permits)
    {
        _total = permits;
        _free = permits;
    }

    public bool TryAcquire(long permits, int timeoutMillis)
    {
        var waiter = new Waiter(permits);
        long deadline = Environment.TickCount64 + Math.Max(timeoutMillis, 0);
        lock (_sync)
        {
            LinkedListNode<Waiter> node = _queue.AddLast(waiter);
            while (true)
            {
                if (ReferenceEquals(_queue.First, node) && _free >= permits)
                {
                    _queue.Remove(node);
                    _free -= permits;
                    Monitor.PulseAll(_sync);  // 队首换人，下一个人可能就够了
                    return true;
                }

                long remaining = deadline - Environment.TickCount64;
                if (remaining <= 0)
                {
                    // 超时：把自己从队列里摘掉，别挡后面的人
                    _queue.Remove(node);
                    Monitor.PulseAll(_sync);  // 同上：挡路的人走了
                    return false;
                }

                // Wait 的唤醒可能是假的（PulseAll 叫醒所有人，被叫醒的人可能仍排不到队首），
                // 所以醒来后重新核对，不依赖返回值。
                Monitor.Wait(_sync, (int)Math.Min(remaining, int.MaxValue));
            }
        }
    }

    public void Release(long permits)
    {
        if (permits <= 0)
        {
            return;
        }

        lock (_sync)
        {
            _free += permits;
            Monitor.PulseAll(_sync);
        }
    }

    /// <summary>当前空闲许可。可能为负（见 <see cref="SetTotalPermits" />）。</summary>
    public long AvailablePermits()
    {
        lock (_sync)
        {
            return _free;
        }
    }

    public long TotalPermits()
    {
        lock (_sync)
        {
            return _total;
        }
    }

    public int WaitingCount()
    {
        lock (_sync)
        {
            return _queue.Count;
        }
    }

    public void SetTotalPermits(long total)
    {
        lock (_sync)
        {
            // 差额全记在空闲许可上 ⇒ 在途份数 (total - free) 保持不变
            _free += total - _total;
            _total = total;
            Monitor.PulseAll(_sync);
        }
    }
}
