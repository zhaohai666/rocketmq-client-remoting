// Why it must be pinned offline: getting this path wrong is **silent** in two extremes --
//
//   - only moving the pull cursor to nextBeginOffset without dropping the queue / freezing
//     it: the broker has just corrected the offset into the legal range, but the old batch
//     on this queue that was **already fetched and not yet acked** pushes the offset back to
//     the illegal value as soon as it is acked, and the next pull is rejected by the broker
//     once more -- client and broker bounce back and forth forever;
//   - the corrected offset not being flushed immediately: if the process crashes before the
//     next periodic flush (5s by default), what is left on the broker is still the illegal
//     offset and the restart pulls from the illegal offset -- the correction was as good as
//     not done.
//
// In a short real-machine window neither direction shows a difference (messages are still
// consumed, it just keeps bouncing / it only surfaces after one crash), so all the
// assertions are placed offline; the real machine has a separate line of proof (the
// examples' live-offset-illegal subcommand).
//
// The implementation differences on the C# side (not an omission but deliberate, see the
// comments in Consumer.cs): there is no per-queue classic ProcessQueue object, so "is this
// queue still mine" is judged by reference equality of _pullThreads; the network leg
// (persist sends to the broker) is not observable offline, so what is asserted here is
// "the corrected value really was handed to the persistence path" (the
// ConsumeOffset/HadOffset of that record in RetiredQueuesForTest is exactly the material
// that goes to UpdateConsumerOffset), and the on-wire step is proven by a real-machine
// script reading the offset back from the broker side.
using RocketMQ.Client;
using RocketMQ.Common;
using Xunit;

namespace RocketMQ.Client.Tests;

public class OffsetIllegalRecoverTests
{
    private const string Group = "GID_OffsetIllegalNetUnit";
    private const string Topic = "OffsetIllegalNetUnitTopic";
    private const string Broker = "broker-a";

    private static MessageQueue Queue0() => new(Topic, Broker, 0);

    private static MessageExt Ext(long queueOffset) => new()
    {
        Topic = Topic,
        BrokerName = Broker,
        QueueId = 0,
        QueueOffset = queueOffset,
    };

    private static List<MessageExt> OffsetBatch(params int[] offsets)
        => offsets.Select(o => Ext(o)).ToList();

    /// <summary>Only counts the invocations: used for "a batch dropped while queued must
    /// not even reach the listener".</summary>
    private sealed class CountingListener : IMessageListenerConcurrently
    {
        public List<List<MessageExt>> Calls { get; } = new();

        public bool Orderly() => false;

        public ConsumeConcurrentlyStatus ConsumeMessage(List<MessageExt> msgs,
                                                       ConsumeConcurrentlyContext context)
        {
            Calls.Add(new List<MessageExt>(msgs));
            return ConsumeConcurrentlyStatus.ConsumeSuccess;
        }
    }

    /// <summary>A push consumer that touches no network: buffer / offsets / generations
    /// are all set up by hand.</summary>
    private sealed class Harness
    {
        public DefaultMQPushConsumer C { get; }
        public MessageQueue Mq { get; } = Queue0();
        public string Key { get; }

        public Harness(bool assigned = true, long epoch = 0)
        {
            C = new DefaultMQPushConsumer(Group) { ConsumeMessageBatchMaxSize = 2 };
            Key = DefaultMQPushConsumer.OffsetKeyForTest(Mq);
            if (assigned)
            {
                C.SetAssignedForTest(Key, Mq);
                C.RegisterLoopForTest(Key, alive: false);
                C.SetConsumeOffsetForTest(Key, 3);
                C.SetQueueEpochForTest(Key, epoch);
            }
        }

        /// <summary>Simulates the in-lock actions of OFFSET_ILLEGAL in the pull callback
        /// (cursor + freeze + corrected value).</summary>
        public void Freeze(long off) => C.FreezeOffsetForIllegalForTest(Key, off);

        public void Recover() => C.OffsetIllegalRecoverForTest(Key, Mq);

        public void Ack(int[] offsets, long? epoch = null)
            => C.AdvanceConsumeOffsetForTest(Key, OffsetBatch(offsets), null, epoch);

        public long? Offset() => C.ConsumeOffsetForTest(Key);
    }

    // ------------------------------------------------- A correction = freeze + drop the queue + flush immediately

    /// <summary>All of the queue's local state is invalidated: buffer, offsets, thread
    /// registration, assignment registration.</summary>
    [Fact]
    public void Recover_DropsEverythingForTheQueue()
    {
        var h = new Harness();
        h.C.SetPendingForTest(h.Key, OffsetBatch(1, 2));
        Assert.Equal(2, h.C.TakeBatchForConsume(h.Key, out _).Count);
        Assert.Equal(1, h.C.InFlightForTest(h.Key));
        h.Freeze(0);

        h.Recover();

        Assert.Empty(h.C.PendingForTest(h.Key));
        Assert.Null(h.Offset());
        Assert.False(h.C.HasPullLoop(h.Key));
        Assert.Equal(-1L, h.C.LastPullAt(h.Key));
        Assert.False(h.C.MqMapContainsForTest(h.Key));
    }

    [Fact]
    public void Recover_HandsTheCorrectedOffsetToPersist()
    {
        var h = new Harness();
        h.Freeze(0);

        h.Recover();

        DefaultMQPushConsumer.RetiredForTest record = Assert.Single(h.C.RetiredQueuesForTest());
        Assert.True(record.HadOffset);
        Assert.Equal(0L, record.ConsumeOffset);
        Assert.Equal(h.Mq, record.Mq);
    }

    /// <summary>After removeProcessQueue, a rebalanceImmediately rebuilds this
    /// queue.</summary>
    [Fact]
    public void Recover_WakesTheRebalanceLoop()
    {
        var h = new Harness();
        h.Freeze(0);
        Assert.False(h.C.RebalancePendingForTest());

        h.Recover();

        Assert.True(h.C.RebalancePendingForTest());
    }

    [Fact]
    public void Recover_BumpsTheQueueEpoch()
    {
        var h = new Harness(epoch: 0);
        h.Freeze(0);

        h.Recover();

        Assert.Equal(1L, h.C.QueueEpochForTest(h.Key));
    }

    [Fact]
    public void Recover_WithoutAConsumedOffset_DoesNotInventOne()
    {
        var h = new Harness(assigned: false);

        h.Recover();

        DefaultMQPushConsumer.RetiredForTest record = Assert.Single(h.C.RetiredQueuesForTest());
        Assert.False(record.HadOffset);
        Assert.True(h.C.RebalancePendingForTest());
    }

    // ------------------------------------------------- Freezing: the corrected value cannot be overturned by an in-flight ack

    [Fact]
    public void FrozenOffset_IgnoresAck()
    {
        var h = new Harness();
        h.Freeze(0);

        h.Ack(new[] { 0, 1, 2 }, epoch: 0);

        Assert.Equal(0L, h.Offset());
    }

    [Fact]
    public void FrozenOffset_IgnoresCorrectTagsOffset()
    {
        var h = new Harness();
        h.Freeze(0);

        h.C.CorrectTagsOffsetForTest(h.Key, PullStatus.NoNewMsg, 110);

        Assert.Equal(0L, h.Offset());
    }

    [Fact]
    public void FreezeSurvivesTheDrop_UntilTheQueueIsRebuilt()
    {
        var h = new Harness();
        h.Freeze(0);

        h.Recover();

        Assert.True(h.C.OffsetFrozenForTest(h.Key));
    }

    /// <summary>A rebuild (a new rebalance round assigns the queue to itself again and the
    /// new loop is in place) must unfreeze it, otherwise this queue only ever pulls and
    /// never acks -- the offset stays frozen at the corrected value forever.</summary>
    [Fact]
    public void Rebuild_ClearsTheFreeze()
    {
        var h = new Harness();
        h.Freeze(0);
        h.Recover();
        h.C.ClearRetiredForTest();

        h.C.SetStartedForTest(true);
        h.C.RebuildPullThreadsForTest(new List<MessageQueue> { h.Mq });

        Assert.False(h.C.OffsetFrozenForTest(h.Key));
        Assert.Equal(1L, h.C.QueueEpochForTest(h.Key));
        // after the rebuild acking works normally with the new generation; an ack arriving with the old one (0) still must not move the offset
        h.Ack(new[] { 0, 1 }, epoch: 0);
        Assert.Null(h.Offset());
        h.Ack(new[] { 0, 1 }, epoch: 1);
        Assert.Equal(2L, h.Offset());
    }


    /// <summary>After the queue is withdrawn/rebuilt (generation 0 -> 1), a late ack from
    /// the old batch must no longer move the offset.</summary>
    [Fact]
    public void StaleEpochAck_IsDropped()
    {
        var h = new Harness(epoch: 1);
        h.C.SetConsumeOffsetForTest(h.Key, 0);

        h.Ack(new[] { 0, 1, 2 }, epoch: 0);

        Assert.Equal(0L, h.Offset());
    }

    [Fact]
    public void CurrentEpochAck_Advances()
    {
        var h = new Harness(epoch: 1);
        h.C.SetConsumeOffsetForTest(h.Key, 0);

        h.Ack(new[] { 0, 1, 2 }, epoch: 1);

        Assert.Equal(3L, h.Offset());
    }

    [Fact]
    public void StaleEpochBatch_IsNotConsumed()
    {
        var h = new Harness(epoch: 1);
        var listener = new CountingListener();
        h.C.SetMessageListener(listener);

        bool done = h.C.ConsumeBatchForTest(h.Key, h.Mq, OffsetBatch(0, 1), epoch: 0);

        Assert.False(done);
        Assert.Empty(listener.Calls);
    }

    [Fact]
    public void CurrentEpochBatch_IsConsumed()
    {
        var h = new Harness(epoch: 1);
        var listener = new CountingListener();
        h.C.SetMessageListener(listener);
        h.C.SetConsumeOffsetForTest(h.Key, 0);

        bool done = h.C.ConsumeBatchForTest(h.Key, h.Mq, OffsetBatch(0, 1), epoch: 1);

        Assert.True(done);
        Assert.Single(listener.Calls);
        Assert.Equal(2L, h.Offset());
    }
}
