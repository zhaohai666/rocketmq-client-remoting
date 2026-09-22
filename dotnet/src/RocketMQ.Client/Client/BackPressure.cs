// 异步发送背压：Java DefaultMQProducerImpl 的那两个公平计数信号量
// （与 python/rocketmq/client/backpressure.py、cpp/.../backpressure.h 同题）。
//
// Java 在把任务投给 AsyncSenderExecutor **之前**、也就是在**调用方线程**上过一道闸
// （DefaultMQProducerImpl.executeAsyncMessageSend:635-682）：按「在途条数」和「在途字节数」
// 两个维度各拿一份许可，拿不到就回调 RemotingTooMuchRequestException。为什么要有这道闸：
// 异步发送本身不阻塞调用方，一个不节制的生产者可以把任意多的消息压进发送池 —— 池子有界
// （队满 reject），但**在途请求**没有上界，慢 broker 会把整条链（含消息 body）留在内存里。
//
// 与 Java 的两处实现差别（语义等价）：
//   1. Java 用 java.util.concurrent.Semaphore(permits, true)。它的公平模式靠 AQS 的
//      等待队列保证 FIFO，但**改容量只能换新对象**（DefaultMQProducer:1383-1391 就是
//      new Semaphore(num - acquired)），换对象的瞬间已在等待的线程与新对象无关。
//      这里用 Monitor 自己实现，容量可以**原地**改：在途份数（total - free）保持不变，
//      等待队列也不清空，扩容时把卡住的人叫醒。
//   2. Java 的 setBackPressureForAsyncSendNum 外面套了一层 ReadWriteCASLock（自旋、写优先，
//      而且**跨** tryAcquire 持有）。这里不需要：resize 只是一个在锁内的加减，读空闲许可
//      也是同一把锁。
//
// 公平性只保证「队列头部才可能拿到」，不保证拿到顺序严格等于到达顺序：超时的等待者会
// 从队列里摘掉自己，摘除瞬间顺序由剩余等待者的到达顺序决定 —— 与 Semaphore(true) 一致。
namespace RocketMQ.Client;

/// <summary>
/// 对应 Java <c>new Semaphore(permits, true)</c>：**公平**的计数信号量。
/// </summary>
public sealed class FairSemaphore
{
    /// <summary>Java DefaultMQProducer:1386 的条数地板值。</summary>
    public const long MinAsyncSendNum = 10;

    /// <summary>Java DefaultMQProducer:1402 的字节地板值（1M）。</summary>
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

    /// <summary>
    /// 对应 Java <c>tryAcquire(permits, timeout, MILLIS)</c>：拿不到就返回 false，不抛异常
    /// （Java 也只有被 interrupt 才抛）。
    /// </summary>
    /// <remarks>
    /// ⚠ 拿到许可和**放弃排队**这两个出口都必须再叫醒一次：公平模式下只有队首能拿，队首一
    /// 换人，后面的申请就可能从「轮不到我」变成「该我了」，而它的 permits 数量未必被前一个人
    /// 的动作影响（队首要 5 个、空闲 6 个时，队首拿走 5 个后剩下 1 个，正好够排在第二的那 1
    /// 个 —— 但 Release 早就跑完了，没人为它叫醒）。少叫醒这一次，那个人就会一直睡到自己的
    /// 超时：异步发送里是白等满 sendMsgTimeout 再回调 TooMuchRequest，实测真会撞上。
    /// </remarks>
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

    /// <summary>
    /// 对应 Java <c>release(permits)</c>：**可以超过总量**（Java 同样不做校验），
    /// 所以一次改小容量的窗口里多还几次不会丢计数。
    /// </summary>
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

    /// <summary>
    /// 正在等许可的线程数（Java <c>Semaphore#getQueueLength</c>）。观测/测试用 —— 公平性只有
    /// 靠「谁先排上队」才说得清，没有这个口径就只能靠 sleep 猜顺序。
    /// </summary>
    public int WaitingCount()
    {
        lock (_sync)
        {
            return _queue.Count;
        }
    }

    /// <summary>
    /// 把总量平移到 <paramref name="total" />，在途份数原样保留（可能算出负的空闲许可 ——
    /// Java <c>new Semaphore(负数)</c> 同样接受，归还许可会把它拉回正数）。
    /// </summary>
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
