// 拉取过期（expired）处理的离线单测。真机恢复场景见 examples 的 LiveRedelivery（H 段）。
//
// C# 侧的实现差异（不是漏掉，是刻意为之，详见 Consumer.cs 注释）：没有 per-queue 的
// classic ProcessQueue 对象，所以"这条循环还是不是这把队列的属主"用登记线程的引用相等来判，
// 停摆 = 盖章超阈值 ∥ 登记的线程已经不在了。
using RocketMQ.Client;
using RocketMQ.Common;
using Xunit;

namespace RocketMQ.Client.Tests;

public class PullExpiredTests
{
    private const string Group = "GID_pull_expired";
    private const string Topic = "PullExpiredTopic";

    /// <summary>一个"已经跑起来"的消费者的最小替身：只置 _started，不碰网络。</summary>
    private static DefaultMQPushConsumer NewConsumer()
    {
        var consumer = new DefaultMQPushConsumer(Group);
        consumer.SetStartedForTest(true);
        return consumer;
    }

    private static MessageQueue Mq(int queueId) => new(Topic, "broker-a", queueId);

    [Fact]
    public void PullMaxIdleTime_MatchesLegacyDefault()
    {
        // 阈值写错方向很安静：调小了会误撤健康队列（白白重投），调大了等于没有自愈。
        Assert.Equal(120000L, DefaultMQPushConsumer.PullMaxIdleTimeMillis);
    }

    [Fact]
    public void NeverStampedQueue_IsNotStalled()
    {
        // 线程刚建、还没跑到盖章处（表里没时刻）：不能判停摆，否则队列一分配到就被撤，永远起不来。
        var consumer = NewConsumer();
        string key = DefaultMQPushConsumer.OffsetKeyForTest(Mq(0));
        consumer.RegisterLoopForTest(key, alive: true);

        Assert.False(consumer.PullStalledForTest(key, UtilAll.CurrentTimeMillis()));
        Assert.Equal(-1L, consumer.LastPullAt(key));
        consumer.ReleaseTestLoops();
    }

    [Fact]
    public void StallBoundary_IsStrictlyGreaterThanThreshold()
    {
        var consumer = NewConsumer();
        MessageQueue mq = Mq(0);
        string key = DefaultMQPushConsumer.OffsetKeyForTest(mq);
        consumer.RegisterLoopForTest(key, alive: true);
        long now = UtilAll.CurrentTimeMillis();

        consumer.SetLastPullAt(key, now - DefaultMQPushConsumer.PullMaxIdleTimeMillis);
        Assert.False(consumer.PullStalledForTest(key, now));

        consumer.SetLastPullAt(key, now - DefaultMQPushConsumer.PullMaxIdleTimeMillis - 1);
        Assert.True(consumer.PullStalledForTest(key, now));

        consumer.ReleaseTestLoops();
    }

    [Fact]
    public void DeadLoop_IsStalledEvenWithAFreshStamp()
    {
        var consumer = NewConsumer();
        string key = DefaultMQPushConsumer.OffsetKeyForTest(Mq(0));
        consumer.RegisterLoopForTest(key, alive: false);
        consumer.SetLastPullAt(key, UtilAll.CurrentTimeMillis());

        Assert.True(consumer.PullStalled(key));
    }

    [Fact]
    public void AliveLoopWithFreshStamp_IsHealthy()
    {
        var consumer = NewConsumer();
        string key = DefaultMQPushConsumer.OffsetKeyForTest(Mq(0));
        consumer.RegisterLoopForTest(key, alive: true);
        consumer.SetLastPullAt(key, UtilAll.CurrentTimeMillis());

        Assert.False(consumer.PullStalled(key));
        consumer.ReleaseTestLoops();
    }

    [Fact]
    public void StallJudgementIsPerQueue()
    {
        // 判据必须逐队列：一把停摆不能把同实例其它队列一起带走。
        var consumer = NewConsumer();
        string stale = DefaultMQPushConsumer.OffsetKeyForTest(Mq(0));
        string healthy = DefaultMQPushConsumer.OffsetKeyForTest(Mq(1));
        consumer.RegisterLoopForTest(stale, alive: true);
        consumer.RegisterLoopForTest(healthy, alive: true);
        long now = UtilAll.CurrentTimeMillis();
        consumer.SetLastPullAt(stale, now - 300000);
        consumer.SetLastPullAt(healthy, now);

        Assert.True(consumer.PullStalledForTest(stale, now));
        Assert.False(consumer.PullStalledForTest(healthy, now));

        int retired = consumer.SweepStalledLoopsForTest(new[] { Mq(0), Mq(1) });
        Assert.Equal(1, retired);
        var record = Assert.Single(consumer.RetiredQueuesForTest());
        Assert.Equal(stale, record.Key);
        Assert.Equal(0, record.Mq.QueueId);
        // 健康队列原样留着：换线程等于把在途消息丢弃重投。
        Assert.True(consumer.HasPullLoop(healthy));
        Assert.True(consumer.LastPullAt(healthy) > 0);

        consumer.ReleaseTestLoops();
    }

    [Fact]
    public void SweepRetiresStalledQueueAndClearsEveryTrace()
    {
        // 撤走的收尾：盖章、线程登记、缓冲、两张位点表全摘掉，
        // 而已消费位点必须**带着走**（交给调用方持久化）——重建的新循环要从 broker 的位点续拉，
        // 本地留着旧值会让下一轮把它写回去，位点回退 = 整把队列重投。
        var consumer = NewConsumer();
        MessageQueue mq = Mq(0);
        string key = DefaultMQPushConsumer.OffsetKeyForTest(mq);
        consumer.RegisterLoopForTest(key, alive: true);
        consumer.SetPendingForTest(key, new List<MessageExt> { new(), new() });
        consumer.SetConsumeOffsetForTest(key, 42);
        consumer.SetLastPullAt(key, UtilAll.CurrentTimeMillis() - 300000);

        Assert.Equal(1, consumer.SweepStalledLoopsForTest(new[] { mq }));

        var record = Assert.Single(consumer.RetiredQueuesForTest());
        Assert.True(record.HadOffset);
        Assert.Equal(42L, record.ConsumeOffset);
        Assert.Equal(mq, record.Mq);
        Assert.Equal(-1L, consumer.LastPullAt(key));
        Assert.False(consumer.HasPullLoop(key));
        Assert.Empty(consumer.PendingForTest(key));
        Assert.Null(consumer.ConsumeOffsetForTest(key));

        consumer.ReleaseTestLoops();
    }

    [Fact]
    public void RetiringAQueueWithoutAConsumedOffsetDoesNotInventOne()
    {
        // 从没消费过（表里没位点）就不能持久化 0：写 0 会把 broker 上别的实例推进的位点抹掉。
        var consumer = NewConsumer();
        MessageQueue mq = Mq(3);
        string key = DefaultMQPushConsumer.OffsetKeyForTest(mq);
        consumer.RegisterLoopForTest(key, alive: false);
        consumer.SetLastPullAt(key, UtilAll.CurrentTimeMillis());

        Assert.Equal(1, consumer.SweepStalledLoopsForTest(new[] { mq }));
        var record = Assert.Single(consumer.RetiredQueuesForTest());
        Assert.False(record.HadOffset);
    }

    [Fact]
    public void SweepIgnoresQueuesNoLongerAssigned()
    {
        var consumer = NewConsumer();
        string key = DefaultMQPushConsumer.OffsetKeyForTest(Mq(0));
        consumer.RegisterLoopForTest(key, alive: true);
        consumer.SetLastPullAt(key, UtilAll.CurrentTimeMillis() - 300000);

        Assert.Equal(0, consumer.SweepStalledLoopsForTest(Array.Empty<MessageQueue>()));
        Assert.True(consumer.HasPullLoop(key));
        Assert.True(consumer.LastPullAt(key) > 0);

        consumer.ReleaseTestLoops();
    }

    [Fact]
    public void SweepIsNoopBeforeStartAndDuringShutdown()
    {
        // 停机过程中循环本来就陆续退出：这时判停摆会撤掉正常队列、刷一堆假的 [BUG] 日志，
        // 还会在 Shutdown 已经清表之后再持久化一次位点。
        var consumer = NewConsumer();
        consumer.SetStartedForTest(false);
        MessageQueue mq = Mq(0);
        string key = DefaultMQPushConsumer.OffsetKeyForTest(mq);
        consumer.RegisterLoopForTest(key, alive: false);
        consumer.SetConsumeOffsetForTest(key, 7);
        consumer.SetLastPullAt(key, UtilAll.CurrentTimeMillis() - 300000);

        Assert.Equal(0, consumer.SweepStalledLoopsForTest(new[] { mq }));
        Assert.Empty(consumer.RetiredQueuesForTest());
        Assert.Equal(7L, consumer.ConsumeOffsetForTest(key).GetValueOrDefault());
        Assert.True(consumer.HasPullLoop(key));
    }

    [Fact]
    public void PopSweepDropsTheProcessQueueAndLeavesNothingBehind()
    {
        var consumer = NewConsumer();
        consumer.PopMode = true;
        MessageQueue mq = Mq(0);
        string key = DefaultMQPushConsumer.OffsetKeyForTest(mq);
        consumer.RegisterLoopForTest(key, alive: true);
        PopProcessQueue old = consumer.RegisterPopQueueForTest(key);
        old.IncFoundMsg(3);
        consumer.SetLastPullAt(key, UtilAll.CurrentTimeMillis() - 300000);

        Assert.Equal(1, consumer.SweepStalledLoopsForTest(new[] { mq }));
        Assert.True(old.IsDropped());
        Assert.Null(consumer.PopQueueForTest(key));
        Assert.Equal(-1L, consumer.LastPullAt(key));

        consumer.ReleaseTestLoops();
    }

    [Fact]
    public void ClearRetiredForTestResetsTheRecorder()
    {
        // 记录表是实例级的；用例之间靠它互不干扰，得能清零。
        var consumer = NewConsumer();
        MessageQueue mq = Mq(0);
        string key = DefaultMQPushConsumer.OffsetKeyForTest(mq);
        consumer.RegisterLoopForTest(key, alive: false);
        consumer.SetLastPullAt(key, UtilAll.CurrentTimeMillis() - 300000);
        Assert.Equal(1, consumer.SweepStalledLoopsForTest(new[] { mq }));
        Assert.NotEmpty(consumer.RetiredQueuesForTest());

        consumer.ClearRetiredForTest();
        Assert.Empty(consumer.RetiredQueuesForTest());
    }
}
