// Unit tests for ``FairSemaphore`` -- testing only that fair counting semaphore itself.
//
// How the producer side uses it (call back when it cannot get one, return it a few times)
// is pinned by the real-machine tool `dotnet backpressure` (see the integration table in
// csharp/README.md).
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
            Assert.True(Environment.TickCount64 < deadline, "the wait condition timed out");
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
        var sem = new FairSemaphore(1);
        Assert.True(sem.TryAcquire(1, 0));
        long began = Environment.TickCount64;
        Assert.False(sem.TryAcquire(1, 120));
        long elapsed = Environment.TickCount64 - began;
        Assert.InRange(elapsed, 100, 1000); // it neither returns early nor waits forever
        // Someone who timed out has already left the queue, so they cannot permanently block
        // everyone behind them on a request that no longer exists
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
        // The entire point of fair mode: a request queued behind others is **not** allowed to jump the queue, even when a permit is available for it right now
        var sem = new FairSemaphore(2);
        Assert.True(sem.TryAcquire(2, 0)); // drain it
        var acquired = new List<string>();
        var big = new Thread(() =>
        {
            if (sem.TryAcquire(2, 5000)) acquired.Add("big");
        });
        big.Start();
        WaitUntil(() => sem.WaitingCount() == 1);
        // Now 0 free and the head is a big request needing 2. Handing back 1 is only enough for the small request -- it must wait.
        var small = new Thread(() =>
        {
            if (sem.TryAcquire(1, 300)) acquired.Add("small");
        });
        small.Start();
        WaitUntil(() => sem.WaitingCount() == 2);
        sem.Release(1);
        small.Join(5000);
        Assert.DoesNotContain("small", acquired); // a later request jumped the queue while the head was unsatisfied
        sem.Release(1); // top up the head's 2
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
        // The queue order is the wake-up order (the observable promise of a fair semaphore)
        Assert.Equal(new[] { "a", "b" }, order);
    }

    [Fact]
    public void GrantedHeadStillFeedsTheWaiterBehindIt()
    {
        // The head needs 5 and 6 are free: after the head takes 5, 1 is left, exactly
        // enough for the 1 that is queued second -- but Release already finished and nobody
        // is going to wake it. Without that one shout it sleeps until its own timeout.
        // (The race window here is narrower than the one below and a sleep cannot order it
        // deterministically, so this is only a supplement.)
        var sem = new FairSemaphore(6);
        Assert.True(sem.TryAcquire(5, 0)); // 1 free
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
        sem.Release(5); // only enough for the head (it needs 5, the second needs 1, neither is satisfied by 5+1)
        JoinAll(new[] { head, second });
        Assert.Equal(2, got.Count);
    }

    [Fact]
    public void TimedOutHeadWakesTheWaiterBehindIt()
    {
        // Regression guard: the head needs 4 but only 3 exist in total => it must time out;
        // once it times out and leaves the queue, the person queued second who needs only 1
        // must be woken **immediately**. Without the "PulseAll on timeout too" step the
        // second person sleeps until its own timeout (measured at 3002ms).
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
        Assert.True(secondWaitedMs < 2000, "the second person slept until its own timeout: " + secondWaitedMs + "ms");
    }

    [Fact]
    public void SetTotalPermitsKeepsOutstandingWork()
    {
        var sem = new FairSemaphore(10);
        Assert.True(sem.TryAcquire(4, 0)); // 4 in flight
        sem.SetTotalPermits(15);
        Assert.Equal(11, sem.AvailablePermits());
        Assert.Equal(15, sem.TotalPermits());
        sem.Release(4);
        Assert.Equal(15, sem.AvailablePermits());
    }

    [Fact]
    public void ShrinkingBelowOutstandingGivesNegativeFree()
    {
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
        var sem = new FairSemaphore(1);
        Assert.True(sem.TryAcquire(1, 0));
        bool got = false;
        var waiter = new Thread(() => got = sem.TryAcquire(1, 5000));
        waiter.Start();
        WaitUntil(() => sem.WaitingCount() == 1);
        sem.SetTotalPermits(5); // grow
        sem.Release(1); // give the in-flight share back
        waiter.Join(5000);
        Assert.True(got);
    }

    [Fact]
    public void ReleaseBeyondTotalIsAllowed()
    {
        var sem = new FairSemaphore(1);
        sem.Release(3);
        Assert.Equal(4, sem.AvailablePermits());
    }

    [Fact]
    public void ZeroPermitsAcquireIsSatisfiedEvenWhenEmpty()
    {
        var sem = new FairSemaphore(0);
        Assert.True(sem.TryAcquire(0, 0));
    }

    [Fact]
    public void FloorsMatchLegacy()
    {
        Assert.Equal(10, FairSemaphore.MinAsyncSendNum);
        Assert.Equal(1024 * 1024, FairSemaphore.MinAsyncSendSize);
    }
}
