// Offline unit tests for pull expiry (expired) handling. The real-machine recovery
// scenario is in the examples' LiveRedelivery (section H).
//
// The implementation differences on the C# side (not an omission but deliberate, see the
// comments in Consumer.cs): there is no per-queue classic ProcessQueue object, so "is this
// loop still the owner of this queue" is decided by reference equality of the registered
// thread, and stalled = stamped past the threshold OR the registered thread is gone.
using RocketMQ.Client;
using RocketMQ.Common;
using Xunit;

namespace RocketMQ.Client.Tests;

public class PullExpiredTests
{
    private const string Group = "GID_pull_expired";
    private const string Topic = "PullExpiredTopic";

    /// <summary>The minimal stand-in for an "already running" consumer: only sets
    /// _started and does not touch the network.</summary>
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
        // A threshold in the wrong direction is very quiet: too small withdraws healthy queues (resending for nothing), too large means there is no self-healing at all.
        Assert.Equal(120000L, DefaultMQPushConsumer.PullMaxIdleTimeMillis);
    }

    [Fact]
    public void NeverStampedQueue_IsNotStalled()
    {
        // The thread was just created and has not reached the stamping point yet (no instant in the table): it must not be judged stalled, otherwise the queue is withdrawn as soon as it is assigned and never gets going.
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
        // The criterion must be per queue: one stalled queue must not take the instance's other queues with it.
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
        // A healthy queue stays as is: swapping the thread means discarding in-flight messages and resending them.
        Assert.True(consumer.HasPullLoop(healthy));
        Assert.True(consumer.LastPullAt(healthy) > 0);

        consumer.ReleaseTestLoops();
    }

    [Fact]
    public void SweepRetiresStalledQueueAndClearsEveryTrace()
    {
        // The cleanup of a withdrawn queue: the stamp, the thread registration, the buffer and both offset tables are all removed,
        // while the consumed offset must **be carried along** (handed to the caller for persistence) -- the rebuilt new loop
        // resumes from the broker's offset, and keeping the old value locally would make
        // the next round write it back, and an offset rollback means the whole queue is resent.
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
        // Never consumed (no offset in the table) must not persist 0: writing 0 would wipe out the offset another instance advanced on the broker.
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
        // During shutdown the loops exit one by one anyway: judging "stalled" then would withdraw healthy queues, flood the log with false [BUG] entries,
        // and persist the offsets once more after Shutdown has already cleared the tables.
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
        // The registry is instance-level; the cases must not disturb each other, so it has to be resettable.
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
