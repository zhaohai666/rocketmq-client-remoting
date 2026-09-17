// 消费线程弹性单测（对应 Java DefaultMQPushConsumer 的线程池配置与 updateCorePoolSize）。
//
// 为什么单独一个文件：Java 的 ThreadPoolExecutor 在 LinkedBlockingQueue（无界）下，
// **真实并发度 == corePoolSize**（max 永远用不到），所以 updateCorePoolSize 是"运行时能改
// 并发度"的 API。.NET 侧此前 POP 路径是"每批一个裸线程"（无上限）且
// SetConsumeThreadNums() 完全没作用 —— 换成有界 core/max 执行器后，本文件把语义钉死。
//
// 另一个必须钉住的反直觉事实：**Java 5.5.1 的自动弹性（inc/decCorePoolSize）是空实现**，
// adjustThreadPool() 整套是 no-op。我们照抄 no-op，不允许"顺手修好"。
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
        // 两个 worker 被占住；第 3、4 个任务只能排队（未到 core 时不会建线程）
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
        // Java setCorePoolSize 的启发式：k = min(delta, 队列长度)，逐个补线程，队列空则停。
        var ex = new ConsumeExecutor(1, 8, keepAliveSeconds: 30);
        var release = new ManualResetEventSlim(false);
        ex.Submit(() => release.Wait(TimeSpan.FromSeconds(5)));
        SleepMs(80);
        for (int i = 0; i < 3; i++) ex.Submit(() => { });
        Assert.Equal(1, ex.WorkerCount());
        Assert.Equal(3, ex.QueuedCount());
        ex.SetCorePoolSize(4);   // delta=3, queue=3 → 应补 3 个
        SleepMs(200);
        Assert.Equal(4, ex.WorkerCount());
        Assert.Equal(0, ex.QueuedCount());
        release.Set();
        ex.Shutdown(true);
    }

    [Fact]
    public void ExtraThreadRetiresAfterKeepAliveCoreThreadDoesNot()
    {
        // > core 的线程空闲到 keepAlive 退出；<= core 的线程永不退出
        var ex = new ConsumeExecutor(1, 4, keepAliveSeconds: 0.2);
        ex.Submit(() => { });
        ex.SetCorePoolSize(2);   // 队列空 → 不补线程，core 变 2
        SleepMs(80);
        Assert.Equal(2, ex.GetCorePoolSize());
        ex.Submit(() => { });    // 把 worker 抬到 2（仍 <= core）
        SleepMs(80);
        Assert.Equal(2, ex.WorkerCount());
        ex.SetCorePoolSize(1);   // core 降到 1 → 多出来的那个成为"超编"
        SleepMs(700);            // 超过 keepAlive
        Assert.Equal(1, ex.WorkerCount());
        SleepMs(400);
        Assert.Equal(1, ex.WorkerCount());   // core 内线程不会退出
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
        Assert.True(ran.Wait(TimeSpan.FromSeconds(3)));   // 同一个 worker 还能继续干活
        ex.Shutdown(true);
    }

    [Fact]
    public void SubmitAfterShutdownThrows()
    {
        var ex = new ConsumeExecutor(1, 2);
        ex.Shutdown();
        Assert.Throws<InvalidOperationException>(() => ex.Submit(() => { }));
        ex.Shutdown(true);
    }

    [Fact]
    public void ShutdownWaitDrainsQueuedTasks()
    {
        // Java shutdown() 不丢已提交任务：wait=true 要等队列跑完
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
        // Java execute 的兜底分支：入队后若 workerCount == 0 仍要补一个线程
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
        ex.SetCorePoolSize(12);   // Java 允许 core > max（等价于把 max 抬到 core）
        Assert.Equal(12, ex.GetMaximumPoolSize());
        ex.Shutdown(true);
    }

    // ------------------------------------------------ 消费者侧（不联网）

    [Fact]
    public void JavaDefaults()
    {
        var c = new DefaultMQPushConsumer("GID_ThreadPoolUnit");
        Assert.Equal(20, c.ConsumeThreadMin);       // Java consumeThreadMin 默认 20
        Assert.Equal(64, c.ConsumeThreadMax);       // Java consumeThreadMax 默认 64
        Assert.Equal(100000L, c.AdjustThreadPoolNumsThreshold);
        Assert.Equal(20, c.GetCorePoolSize());      // = consumeThreadMin
    }

    [Fact]
    public void UpdateCorePoolSizeGuards()
    {
        var c = new DefaultMQPushConsumer("GID_ThreadPoolUnit");
        Assert.True(c.UpdateCorePoolSize(30));
        Assert.Equal(30, c.GetCorePoolSize());
        Assert.False(c.UpdateCorePoolSize(0));
        Assert.False(c.UpdateCorePoolSize(-1));
        Assert.False(c.UpdateCorePoolSize(64));     // == consumeThreadMax
        Assert.True(c.UpdateCorePoolSize(63));      // 刚好低于 max
        Assert.Equal(63, c.GetCorePoolSize());
        // Short.MAX_VALUE 上界：要把 consumeThreadMax 抬上去才轮得到这条守卫
        c.SetConsumeThreadMax(40000);
        Assert.False(c.UpdateCorePoolSize(32768));  // above Short.MAX_VALUE
        Assert.True(c.UpdateCorePoolSize(32767));   // 上界本身合法
        Assert.Equal(32767, c.GetCorePoolSize());
    }

    [Fact]
    public void SetConsumeThreadNumsSetsMinMaxAndCore()
    {
        var c = new DefaultMQPushConsumer("GID_ThreadPoolUnit");
        c.SetConsumeThreadNums(4);
        Assert.Equal(4, c.ConsumeThreadMin);
        Assert.Equal(4, c.ConsumeThreadMax);
        Assert.Equal(4, c.GetCorePoolSize());
        Assert.False(c.UpdateCorePoolSize(4));      // max 现在是 4
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
        Assert.Equal(1, c.ConsumeThreadMin);        // 钳到 1
        Assert.Equal(1, c.ConsumeThreadMax);
    }

    [Fact]
    public void ExecutorObservabilityWithoutStart()
    {
        var c = new DefaultMQPushConsumer("GID_ThreadPoolUnit");
        // 未 Start（未建执行器）时观测值恒为 0，不应崩
        Assert.Equal(0, c.ConsumeExecutorWorkers());
        Assert.Equal(0, c.ConsumeExecutorQueued());
    }

    // ------------------------------------------------ msgAccCnt / 阈值

    [Fact]
    public void MsgAccCntIsMaxOffsetMinusLastQueueOffset()
    {
        // Java ProcessQueue:148-158 —— accTotal = MAX_OFFSET - queueOffset，取最后一条
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
        c.UpdateMsgAccCnt("k1", new List<MessageExt> { MakeMsg(10, null) });      // 缺属性
        Assert.Equal(0, c.MsgAccCnt("k1"));
        c.UpdateMsgAccCnt("k1", new List<MessageExt> { MakeMsg(10, "not-a-number") });
        Assert.Equal(0, c.MsgAccCnt("k1"));
    }

    [Fact]
    public void MsgAccCntLatestBatchOverwrites()
    {
        var c = new DefaultMQPushConsumer("GID_ThreadPoolUnit");
        c.UpdateMsgAccCnt("k1", new List<MessageExt> { MakeMsg(10, "500") });
        c.UpdateMsgAccCnt("k1", new List<MessageExt> { MakeMsg(400, "450") });   // 新一批积压变小
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
        c.UpdateMsgAccCnt("a", new List<MessageExt> { MakeMsg(0, "500") });      // 500 >= 100 → inc 分支
        int before = c.GetCorePoolSize();
        c.AdjustThreadPool();
        Assert.Equal(before, c.GetCorePoolSize());  // inc 是空实现
        c.SetAdjustThreadPoolNumsThreshold(1000);
        c.UpdateMsgAccCnt("a", new List<MessageExt> { MakeMsg(0, "10") });       // 10 < 800 → dec 分支
        c.AdjustThreadPool();
        Assert.Equal(before, c.GetCorePoolSize());  // dec 也是空实现
        // 自动 no-op ≠ API 失效
        Assert.True(c.UpdateCorePoolSize(11));
        Assert.Equal(11, c.GetCorePoolSize());
    }

    [Fact]
    public void UpdateCorePoolSizePropagatesToInjectedExecutor()
    {
        // 通过内部注入执行器验证"显式 API 真的会落到执行器上"（不用起集群）
        var c = new DefaultMQPushConsumer("GID_ThreadPoolUnit");
        using var ex = new ConsumeExecutor(20, 64, keepAliveSeconds: 30);
        c.PopConsumeExecutorForTest = ex;
        Assert.Equal(20, c.GetCorePoolSize());
        Assert.True(c.UpdateCorePoolSize(33));
        Assert.Equal(33, ex.GetCorePoolSize());
        Assert.Equal(33, c.GetCorePoolSize());
        Assert.False(c.UpdateCorePoolSize(64));     // 守卫仍按 consumeThreadMax 判
        Assert.Equal(33, ex.GetCorePoolSize());
    }
}
