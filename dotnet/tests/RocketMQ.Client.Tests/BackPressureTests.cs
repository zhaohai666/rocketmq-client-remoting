// ``FairSemaphore`` 的单测 —— 只测那个公平计数信号量本身。
//
// 对端是 Java ``new Semaphore(permits, true)``：异步发送背压整套语义都压在它身上，
// 所以这里盯的是**公平**（只有队首能拿）与**运行时改容量**（在途份数原样保留）两件事，
// 而不是「能不能拿到许可」这种换谁都一样的部分。
//
// 与 python/tests/test_backpressure.py、cpp/tests/test_backpressure.cpp 同题。
// 生产者那一侧怎么用它（拿不到就回调、还几次）由真机工具 `dotnet backpressure` 钉
// （见 dotnet/README.md 的联调表）。
using System;
using System.Collections.Generic;
using System.Threading;

using Xunit;

namespace RocketMQ.Client.Tests;

public class BackPressureTests
{
    private static void WaitUntil(Func<bool> predicate, int timeoutMs = 3000)
    {
        var deadline = Environment.TickCount64 + timeoutMs;
        while (!predicate())
        {
            Assert.True(Environment.TickCount64 < deadline, "等待条件超时");
            Thread.Sleep(5);
        }
    }

    private static void JoinAll(IEnumerable<Thread> threads, int timeoutMs = 5000)
    {
        long deadline = Environment.TickCount64 + timeoutMs;
        foreach (Thread t in threads)
        {
            t.Join((int)Math.Max(0, deadline - Environment.TickCount64));
        }
    }

    [Fact]
    public void TryAcquireTimesOutWithoutThrowing()
    {
        // Java tryAcquire(permits, timeout, MILLIS) 超时返回 false，只有 interrupt 才抛
        var sem = new FairSemaphore(1);
        Assert.True(sem.TryAcquire(1, 0));
        long began = Environment.TickCount64;
        Assert.False(sem.TryAcquire(1, 120));
        long elapsed = Environment.TickCount64 - began;
        Assert.InRange(elapsed, 100, 1000); // 不提前返回，也不无限等
        // 超时的人已经出队，不会把后面的人永久挡在一个已经消失的请求上
        sem.Release(1);
        Assert.True(sem.TryAcquire(1, 0));
    }

    [Fact]
    public void NonPositiveTimeoutNeverWaits()
    {
        var sem = new FairSemaphore(0);
        Assert.False(sem.TryAcquire(1, 0));
        Assert.False(sem.TryAcquire(1, -5000));
    }

    [Fact]
    public void OnlyTheQueueHeadIsGranted()
    {
        // 公平模式的全部意义：排在别人后面的请求**不许**插队，哪怕许可现在够它
        var sem = new FairSemaphore(2);
        Assert.True(sem.TryAcquire(2, 0)); // 掏空
        var acquired = new List<string>();
        var big = new Thread(() =>
        {
            if (sem.TryAcquire(2, 5000)) acquired.Add("big");
        });
        big.Start();
        WaitUntil(() => sem.WaitingCount() == 1);
        // 现在空闲 0、队首是一个要 2 个的大请求。还 1 个只够小请求 —— 它必须等。
        var small = new Thread(() =>
        {
            if (sem.TryAcquire(1, 300)) acquired.Add("small");
        });
        small.Start();
        WaitUntil(() => sem.WaitingCount() == 2);
        sem.Release(1);
        small.Join(5000);
        Assert.DoesNotContain("small", acquired); // 队首没满足时后来者插队了
        sem.Release(1); // 补齐队首要的 2 个
        big.Join(5000);
        Assert.Contains("big", acquired);
    }

    [Fact]
    public void ReleaseWakesTheHeadInOrder()
    {
        var sem = new FairSemaphore(1);
        Assert.True(sem.TryAcquire(1, 0));
        var order = new List<string>();
        var gate = new object();
        void Waiter(string tag)
        {
            string outcome = sem.TryAcquire(1, 5000) ? tag : tag + "-lost";
            lock (gate) order.Add(outcome);
        }

        var a = new Thread(() => Waiter("a"));
        var b = new Thread(() => Waiter("b"));
        a.Start();
        WaitUntil(() => sem.WaitingCount() == 1);
        b.Start();
        WaitUntil(() => sem.WaitingCount() == 2);
        sem.Release(1);
        sem.Release(1);
        JoinAll(new[] { a, b });
        // 队列顺序就是醒来顺序（公平信号量的可观察承诺）
        Assert.Equal(new[] { "a", "b" }, order);
    }

    [Fact]
    public void GrantedHeadStillFeedsTheWaiterBehindIt()
    {
        // 队首要 5 个、空闲 6 个：队首拿走 5 个后剩 1 个，正好够排在第二的那 1 个 ——
        // 但 Release 早就跑完了，没人为它叫醒。少那一嗓子它就睡到自己的超时。
        // （这里的竞态窗口比下面那条窄，靠 sleep 排不出确定顺序，所以只作补充。）
        var sem = new FairSemaphore(6);
        Assert.True(sem.TryAcquire(5, 0)); // 空闲 1
        var got = new List<string>();
        var head = new Thread(() =>
        {
            if (sem.TryAcquire(5, 3000)) lock (got) got.Add("head");
        });
        head.Start();
        WaitUntil(() => sem.WaitingCount() == 1);
        var second = new Thread(() =>
        {
            if (sem.TryAcquire(1, 3000)) lock (got) got.Add("second");
        });
        second.Start();
        WaitUntil(() => sem.WaitingCount() == 2);
        sem.Release(5); // 只够队首（它要 5 个，第二个要 1 个，都不够它一个 5+1）
        JoinAll(new[] { head, second });
        Assert.Equal(2, got.Count);
    }

    [Fact]
    public void TimedOutHeadWakesTheWaiterBehindIt()
    {
        // 回归守卫：队首要 4 个但总量只有 3 个 ⇒ 它必然超时；它超时出队后，排在第二、
        // 只要 1 个的人应当**立刻**被叫醒。少了「超时也 PulseAll」这一步，第二个人要
        // 一直睡到自己的超时（实测 3002ms）。
        var sem = new FairSemaphore(3);
        var headResult = false;
        long secondWaitedMs = -1;
        var head = new Thread(() => headResult = sem.TryAcquire(4, 200));
        var second = new Thread(() =>
        {
            long began = Environment.TickCount64;
            sem.TryAcquire(1, 3000);
            secondWaitedMs = Environment.TickCount64 - began;
        });
        head.Start();
        WaitUntil(() => sem.WaitingCount() == 1);
        second.Start();
        WaitUntil(() => sem.WaitingCount() == 2);
        JoinAll(new[] { head, second }, 8000);
        Assert.False(headResult);
        Assert.True(secondWaitedMs < 2000, "第二个人睡到了自己的超时：" + secondWaitedMs + "ms");
    }

    [Fact]
    public void SetTotalPermitsKeepsOutstandingWork()
    {
        // 改总量 = 「空闲 = 新总量 - 在途」，与 Java new Semaphore(num - acquired) 同解
        var sem = new FairSemaphore(10);
        Assert.True(sem.TryAcquire(4, 0)); // 在途 4
        sem.SetTotalPermits(15);
        Assert.Equal(11, sem.AvailablePermits());
        Assert.Equal(15, sem.TotalPermits());
        sem.Release(4);
        Assert.Equal(15, sem.AvailablePermits());
    }

    [Fact]
    public void ShrinkingBelowOutstandingGivesNegativeFree()
    {
        // Java 的 new Semaphore(负数) 是合法的，归还许可会把它拉回正数 —— 这里同样接受
        var sem = new FairSemaphore(10);
        Assert.True(sem.TryAcquire(6, 0));
        sem.SetTotalPermits(2);
        Assert.Equal(-4, sem.AvailablePermits());
        sem.Release(6);
        Assert.Equal(2, sem.AvailablePermits());
        Assert.True(sem.TryAcquire(2, 0));
    }

    [Fact]
    public void ResizeWakesSomeoneBlockedOnTheOldCapacity()
    {
        // 这是本实现换掉 Java「整体换一个 Semaphore 对象」写法的理由：调大容量时，正堵在
        // 等待队列里的人会被叫醒；Java 那些等待者挂在旧对象上，只能等到自己的超时。
        var sem = new FairSemaphore(1);
        Assert.True(sem.TryAcquire(1, 0));
        bool got = false;
        var waiter = new Thread(() => got = sem.TryAcquire(1, 5000));
        waiter.Start();
        WaitUntil(() => sem.WaitingCount() == 1);
        sem.SetTotalPermits(5); // 扩容
        sem.Release(1); // 在途的那份还回去
        waiter.Join(5000);
        Assert.True(got);
    }

    [Fact]
    public void ReleaseBeyondTotalIsAllowed()
    {
        // Java release() 不校验是否超过总量（信号量可以被"无中生有"地放大）
        var sem = new FairSemaphore(1);
        sem.Release(3);
        Assert.Equal(4, sem.AvailablePermits());
    }

    [Fact]
    public void ZeroPermitsAcquireIsSatisfiedEvenWhenEmpty()
    {
        // Java tryAcquire(0, …) 恒真；批量消息为空时我们按 1 算，所以这里只锁住基元语义
        var sem = new FairSemaphore(0);
        Assert.True(sem.TryAcquire(0, 0));
    }

    [Fact]
    public void FloorsMatchJava()
    {
        Assert.Equal(10, FairSemaphore.MinAsyncSendNum);
        Assert.Equal(1024 * 1024, FairSemaphore.MinAsyncSendSize);
    }
}
