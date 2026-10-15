using System;
using System.Collections.Generic;
using System.Threading;

using RocketMQ.Common;
using Xunit;

namespace RocketMQ.Client.Tests;

public class ConsumeExecutorTests
{
    private static void SleepMs(int ms) => Thread.Sleep(ms);

    private static MessageExt MakeMsg(long queueOffset, string? maxOffset)
    {
        var m = new MessageExt { Topic = "T" };
        m.Body = "x"u8.ToArray();
        m.QueueOffset = queueOffset;
        if (maxOffset != null) m.PutProperty(MessageConst.PropertyMaxOffset, maxOffset);
        return m;
    }

    // ------------------------------------------------ ConsumeExecutor

    [Fact]
    public void SpawnsUpToCoreThenQueues()
    {
        using var ex = new ConsumeExecutor(2, 8, keepAliveSeconds: 30);
        int running = 0;
        var release = new ManualResetEventSlim(false);
        void Block()
        {
            Interlocked.Increment(ref running);
            release.Wait(TimeSpan.FromSeconds(5));
            Interlocked.Decrement(ref running);
        }

        ex.Submit(Block);
        ex.Submit(Block);
        ex.Submit(() => { });
        ex.Submit(() => { });
        SleepMs(150);
        Assert.Equal(2, ex.WorkerCount());
        Assert.Equal(2, ex.QueuedCount());
        release.Set();
        ex.Shutdown(true);
        Assert.Equal(0, running);
    }

    [Fact]
    public void RaisingCoreSpawnsForQueuedTasks()
    {
        var ex = new ConsumeExecutor(1, 8, keepAliveSeconds: 30);
        var release = new ManualResetEventSlim(false);
        ex.Submit(() => release.Wait(TimeSpan.FromSeconds(5)));
        SleepMs(80);
        for (int i = 0; i < 3; i++) ex.Submit(() => { });
        Assert.Equal(1, ex.WorkerCount());
        Assert.Equal(3, ex.QueuedCount());
        ex.SetCorePoolSize(4);   // delta=3, queue=3 -> 3 more should be added
        SleepMs(200);
        Assert.Equal(4, ex.WorkerCount());
        Assert.Equal(0, ex.QueuedCount());
        release.Set();
        ex.Shutdown(true);
    }

    [Fact]
    public void ExtraThreadRetiresAfterKeepAliveCoreThreadDoesNot()
    {
        // Threads above core exit after being idle for keepAlive; threads at or below core never exit
        var ex = new ConsumeExecutor(1, 4, keepAliveSeconds: 0.2);
        ex.Submit(() => { });
        ex.SetCorePoolSize(2);   // queue empty -> no extra thread, core becomes 2
        SleepMs(80);
        Assert.Equal(2, ex.GetCorePoolSize());
        ex.Submit(() => { });    // raise the worker to 2 (still <= core)
        SleepMs(80);
        Assert.Equal(2, ex.WorkerCount());
        ex.SetCorePoolSize(1);   // core drops to 1 -> the extra one becomes "over core"
        SleepMs(700);            // longer than keepAlive
        Assert.Equal(1, ex.WorkerCount());
        SleepMs(400);
        Assert.Equal(1, ex.WorkerCount());   // a thread within core does not exit
        ex.Shutdown(true);
    }

    [Fact]
    public void TaskExceptionDoesNotKillWorker()
    {
        var ex = new ConsumeExecutor(1, 2, keepAliveSeconds: 30);
        ex.Submit(() => throw new InvalidOperationException("boom"));
        SleepMs(200);
        Assert.Equal(1, ex.HandlerExceptionCount());
        Assert.Equal(1, ex.WorkerCount());
        var ran = new ManualResetEventSlim(false);
        ex.Submit(ran.Set);
        Assert.True(ran.Wait(TimeSpan.FromSeconds(3)));   // the same worker can still work
        ex.Shutdown(true);
    }

    [Fact]
    public void SubmitAfterShutdownThrows()
    {
        var ex = new ConsumeExecutor(1, 2);
        ex.Shutdown();
        Assert.Throws<RejectedExecutionException>(() => ex.Submit(() => { }));
        ex.Shutdown(true);
    }

    [Fact]
    public void BoundedQueueRejectsWhenFullAndWorkersSaturated()
    {
        var ex = new ConsumeExecutor(1, 1, keepAliveSeconds: 30, maxQueueSize: 2);
        var busy = new ManualResetEventSlim(false);
        var release = new ManualResetEventSlim(false);
        ex.Submit(() =>
        {
            busy.Set();
            release.Wait();
        });
        Assert.True(busy.Wait(5000), "the worker did not start");
        ex.Submit(() => release.Wait()); // queue slot 1
        ex.Submit(() => release.Wait()); // queue slot 2 = full
        Assert.Equal(2, ex.QueuedCount());
        var e = Assert.Throws<RejectedExecutionException>(() => ex.Submit(() => { }));
        Assert.Contains("queue is full", e.Message);
        release.Set();
        ex.Shutdown(true);
    }

    [Fact]
    public void ThreadNamesFollowLegacyThreadFactoryImpl()
    {
        var ex = new ConsumeExecutor(2, 2, keepAliveSeconds: 30, "AsyncSenderExecutor",
            threadNameSep: "_", threadIndexFrom: 1);
        var names = new List<string>();
        var gate = new ManualResetEventSlim(false);
        Action collect = () =>
        {
            lock (names)
            {
                names.Add(Thread.CurrentThread.Name ?? string.Empty);
            }

            gate.Wait();
        };
        ex.Submit(collect);
        ex.Submit(collect);
        AssertSpin(() => ex.WorkerCount() == 2);
        gate.Set();
        ex.Shutdown(true);
        lock (names)
        {
            names.Sort(StringComparer.Ordinal);
            Assert.Equal(new[] { "AsyncSenderExecutor_1", "AsyncSenderExecutor_2" }, names);
        }
    }

    private static void AssertSpin(Func<bool> cond)
    {
        var deadline = DateTime.UtcNow.AddSeconds(5);
        while (DateTime.UtcNow < deadline)
        {
            if (cond())
            {
                return;
            }

            Thread.Sleep(10);
        }

        Assert.False(cond(), "the condition was not met within 5s");
    }

    [Fact]
    public void ShutdownWaitDrainsQueuedTasks()
    {
        var ex = new ConsumeExecutor(1, 2, keepAliveSeconds: 30);
        int done = 0;
        for (int i = 0; i < 5; i++)
        {
            ex.Submit(() =>
            {
                Thread.Sleep(20);
                Interlocked.Increment(ref done);
            });
        }
        ex.Shutdown(true);
        Assert.Equal(5, done);
    }

    [Fact]
    public void ZeroCoreStillRunsTasks()
    {
        var ex = new ConsumeExecutor(0, 2, keepAliveSeconds: 0.1);
        var ran = new ManualResetEventSlim(false);
        ex.Submit(ran.Set);
        Assert.True(ran.Wait(TimeSpan.FromSeconds(3)));
        ex.Shutdown(true);
    }

    [Fact]
    public void CorePoolSizeMutation()
    {
        var ex = new ConsumeExecutor(3, 9, keepAliveSeconds: 30);
        Assert.Equal(3, ex.GetCorePoolSize());
        Assert.Equal(9, ex.GetMaximumPoolSize());
        ex.SetCorePoolSize(5);
        Assert.Equal(5, ex.GetCorePoolSize());
        ex.SetCorePoolSize(2);
        Assert.Equal(2, ex.GetCorePoolSize());
        ex.SetCorePoolSize(12);
        Assert.Equal(12, ex.GetMaximumPoolSize());
        ex.Shutdown(true);
    }

    // ------------------------------------------------ Consumer side (no networking)

    [Fact]
    public void LegacyDefaults()
    {
        var c = new DefaultMQPushConsumer("GID_ThreadPoolUnit");
        Assert.Equal(20, c.ConsumeThreadMin);
        Assert.Equal(20, c.ConsumeThreadMax);
        Assert.Equal(100000L, c.AdjustThreadPoolNumsThreshold);
        Assert.Equal(20, c.GetCorePoolSize());      // = consumeThreadMin
    }

    [Fact]
    public void UpdateCorePoolSizeGuards()
    {
        var c = new DefaultMQPushConsumer("GID_ThreadPoolUnit");
        Assert.True(c.UpdateCorePoolSize(15));
        Assert.Equal(15, c.GetCorePoolSize());
        Assert.False(c.UpdateCorePoolSize(0));
        Assert.False(c.UpdateCorePoolSize(-1));
        Assert.False(c.UpdateCorePoolSize(20));     // == consumeThreadMax
        Assert.True(c.UpdateCorePoolSize(19));      // just below max
        Assert.Equal(19, c.GetCorePoolSize());
        // The Short.MAX_VALUE upper bound: consumeThreadMax has to be raised for this guard to be reached
        c.SetConsumeThreadMax(40000);
        Assert.False(c.UpdateCorePoolSize(32768));  // above Short.MAX_VALUE
        Assert.True(c.UpdateCorePoolSize(32767));   // the upper bound itself is legal
        Assert.Equal(32767, c.GetCorePoolSize());
    }

    [Fact]
    public void DefaultMaxIsTwentyNotSixtyFour()
    {
        var c = new DefaultMQPushConsumer("GID_ThreadPoolUnit");
        Assert.Equal(20, c.ConsumeThreadMax);
        Assert.False(c.UpdateCorePoolSize(30));
        Assert.False(c.UpdateCorePoolSize(21));
        Assert.Equal(20, c.GetCorePoolSize());      // not one got through
        c.SetConsumeThreadMax(30);
        Assert.True(c.UpdateCorePoolSize(25));
        Assert.Equal(25, c.GetCorePoolSize());
    }

    [Fact]
    public void SetConsumeThreadNumsSetsMinMaxAndCore()
    {
        var c = new DefaultMQPushConsumer("GID_ThreadPoolUnit");
        c.SetConsumeThreadNums(4);
        Assert.Equal(4, c.ConsumeThreadMin);
        Assert.Equal(4, c.ConsumeThreadMax);
        Assert.Equal(4, c.GetCorePoolSize());
        Assert.False(c.UpdateCorePoolSize(4));      // max is 4 now
        Assert.True(c.UpdateCorePoolSize(3));
        Assert.Equal(3, c.GetCorePoolSize());
    }

    [Fact]
    public void ConsumeThreadMinSetterMovesCoreMaxDoesNot()
    {
        var c = new DefaultMQPushConsumer("GID_ThreadPoolUnit");
        c.SetConsumeThreadMin(8);
        Assert.Equal(8, c.GetCorePoolSize());
        c.SetConsumeThreadMax(16);
        Assert.Equal(16, c.ConsumeThreadMax);
        Assert.Equal(8, c.GetCorePoolSize());
        c.SetConsumeThreadMin(0);
        c.SetConsumeThreadMax(-5);
        Assert.Equal(1, c.ConsumeThreadMin);        // clamped to 1
        Assert.Equal(1, c.ConsumeThreadMax);
    }

    [Fact]
    public void ExecutorObservabilityWithoutStart()
    {
        var c = new DefaultMQPushConsumer("GID_ThreadPoolUnit");
        // While not Started (no executor built yet) the observed value is always 0, it must not crash
        Assert.Equal(0, c.ConsumeExecutorWorkers());
        Assert.Equal(0, c.ConsumeExecutorQueued());
    }

    // ------------------------------------------------ msgAccCnt / thresholds

    [Fact]
    public void MsgAccCntIsMaxOffsetMinusLastQueueOffset()
    {
        var c = new DefaultMQPushConsumer("GID_ThreadPoolUnit");
        c.UpdateMsgAccCnt("k1", new List<MessageExt> { MakeMsg(10, "100"), MakeMsg(12, "100") });
        Assert.Equal(88, c.MsgAccCnt("k1"));        // 100 - 12
    }

    [Fact]
    public void MsgAccCntIgnoresNonPositive()
    {
        var c = new DefaultMQPushConsumer("GID_ThreadPoolUnit");
        c.UpdateMsgAccCnt("k1", new List<MessageExt> { MakeMsg(100, "100") });   // == 0
        Assert.Equal(0, c.MsgAccCnt("k1"));
        c.UpdateMsgAccCnt("k1", new List<MessageExt> { MakeMsg(200, "100") });   // < 0
        Assert.Equal(0, c.MsgAccCnt("k1"));
    }

    [Fact]
    public void MsgAccCntIgnoresMissingOrGarbageProperty()
    {
        var c = new DefaultMQPushConsumer("GID_ThreadPoolUnit");
        c.UpdateMsgAccCnt("k1", new List<MessageExt> { MakeMsg(10, null) });      // property missing
        Assert.Equal(0, c.MsgAccCnt("k1"));
        c.UpdateMsgAccCnt("k1", new List<MessageExt> { MakeMsg(10, "not-a-number") });
        Assert.Equal(0, c.MsgAccCnt("k1"));
    }

    [Fact]
    public void MsgAccCntLatestBatchOverwrites()
    {
        var c = new DefaultMQPushConsumer("GID_ThreadPoolUnit");
        c.UpdateMsgAccCnt("k1", new List<MessageExt> { MakeMsg(10, "500") });
        c.UpdateMsgAccCnt("k1", new List<MessageExt> { MakeMsg(400, "450") });   // a new, smaller backlog
        Assert.Equal(50, c.MsgAccCnt("k1"));
    }

    [Fact]
    public void ComputeAccumulationTotalSumsQueues()
    {
        var c = new DefaultMQPushConsumer("GID_ThreadPoolUnit");
        c.UpdateMsgAccCnt("a", new List<MessageExt> { MakeMsg(0, "30") });
        c.UpdateMsgAccCnt("b", new List<MessageExt> { MakeMsg(0, "70") });
        Assert.Equal(100, c.MsgAccCnt());
        Assert.Equal(100, c.ComputeAccumulationTotal());
    }

    [Fact]
    public void AdjustThreadPoolIncAndDecAreNoOps()
    {
        var c = new DefaultMQPushConsumer("GID_ThreadPoolUnit");
        c.SetAdjustThreadPoolNumsThreshold(100);
        c.UpdateMsgAccCnt("a", new List<MessageExt> { MakeMsg(0, "500") });      // 500 >= 100 -> inc branch
        int before = c.GetCorePoolSize();
        c.AdjustThreadPool();
        Assert.Equal(before, c.GetCorePoolSize());  // inc is an empty implementation
        c.SetAdjustThreadPoolNumsThreshold(1000);
        c.UpdateMsgAccCnt("a", new List<MessageExt> { MakeMsg(0, "10") });       // 10 < 800 -> dec branch
        c.AdjustThreadPool();
        Assert.Equal(before, c.GetCorePoolSize());  // dec is an empty implementation too
        // an automatic no-op != a broken API
        Assert.True(c.UpdateCorePoolSize(11));
        Assert.Equal(11, c.GetCorePoolSize());
    }

    [Fact]
    public void UpdateCorePoolSizePropagatesToInjectedExecutor()
    {
        // Verifying "the explicit API really does reach the executor" via internal executor injection (no cluster needed)
        var c = new DefaultMQPushConsumer("GID_ThreadPoolUnit");
        using var ex = new ConsumeExecutor(20, 64, keepAliveSeconds: 30);
        c.PopConsumeExecutorForTest = ex;
        Assert.Equal(20, c.GetCorePoolSize());
        Assert.True(c.UpdateCorePoolSize(13));      // < the default max 20, it gets through
        Assert.Equal(13, ex.GetCorePoolSize());
        Assert.Equal(13, c.GetCorePoolSize());
        Assert.False(c.UpdateCorePoolSize(20));     // the guard still judges by consumeThreadMax
        Assert.Equal(13, ex.GetCorePoolSize());
        c.SetConsumeThreadMax(40);
        Assert.True(c.UpdateCorePoolSize(33));
        Assert.Equal(33, ex.GetCorePoolSize());
    }
}
