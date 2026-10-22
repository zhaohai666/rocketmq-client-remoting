// Fairness only guarantees that "only the head of the queue may acquire"; it does not guarantee the acquisition order
// strictly equals the arrival order: a timed-out waiter removes itself from the queue, and at that moment the order is
// determined by the arrival order of the remaining waiters — consistent with Semaphore(true).
namespace RocketMQ.Client;

public sealed class FairSemaphore
{
    public const long MinAsyncSendNum = 10;

    public const long MinAsyncSendSize = 1024 * 1024;

    // A permit request still in the queue (only so the head can be stably identified; does not mean the permit is acquired).
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
            bool woken = false;
            while (true)
            {
                // Only the first pass may grant on an already-spent budget: that is the "poll once without waiting"
                // contract of a non-positive timeout. A later pass runs after a Wait returned, and on a busy host the
                // thread can be off-core long after both its wake-up and its deadline. Granting there would put a
                // caller that is already gone back at the head of the queue it had left, and the permit it hands back
                // a few statements later is one the next waiter could still have used in time.
                if (woken && deadline - Environment.TickCount64 <= 0)
                {
                    LeaveQueue(node);
                    return false;
                }

                if (ReferenceEquals(_queue.First, node) && _free >= permits)
                {
                    _queue.Remove(node);
                    _free -= permits;
                    Monitor.PulseAll(_sync);  // head changed, the next person may be enough
                    return true;
                }

                long remaining = deadline - Environment.TickCount64;
                if (remaining <= 0)
                {
                    // Timed out: remove self from the queue, don't block those behind
                    LeaveQueue(node);
                    return false;
                }

                // A Wait wakeup may be spurious (PulseAll wakes everyone, a woken one may still not reach the head),
                // so re-check after waking and do not rely on the return value.
                Monitor.Wait(_sync, (int)Math.Min(remaining, int.MaxValue));
                woken = true;
            }
        }
    }

    /// <summary>Give up the ticket, with the lock held: timeout must not block those behind, and the blocker leaving is itself a head change.</summary>
    private void LeaveQueue(LinkedListNode<Waiter> node)
    {
        _queue.Remove(node);
        Monitor.PulseAll(_sync);  // same as above: the blocker left
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

    /// <summary>Current free permits. May be negative (see <see cref="SetTotalPermits" />).</summary>
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
            // The whole delta is recorded on free permits => in-flight count (total - free) stays unchanged
            _free += total - _total;
            _total = total;
            Monitor.PulseAll(_sync);
        }
    }
}
