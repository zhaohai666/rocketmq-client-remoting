
using RocketMQ.Client;
using RocketMQ.Common;

using Xunit;

namespace RocketMQ.Client.Tests;

public class CorrectTagsOffsetTests
{
    private const string Group = "GID_CorrectTagsOffsetNetUnit";
    private const string Topic = "CorrectTagsOffsetNetUnitTopic";
    private const string Broker = "broker-a";

    private static MessageQueue Queue0() => new(Topic, Broker, 0);

    private static MessageExt Ext(long queueOffset) => new()
    {
        Topic = Topic,
        BrokerName = Broker,
        QueueId = 0,
        QueueOffset = queueOffset,
    };

    private static List<MessageExt> OffsetBatch(int n)
        => Enumerable.Range(0, n).Select(i => Ext(i)).ToList();

    /// <summary>A consumer that is not Started is enough: CorrectTagsOffset / Take / Finish
    /// all work on local state.</summary>
    private sealed class Harness
    {
        private readonly DefaultMQPushConsumer _consumer;
        private readonly string _key;

        public Harness()
        {
            _consumer = new DefaultMQPushConsumer(Group) { ConsumeMessageBatchMaxSize = 2 };
            _key = DefaultMQPushConsumer.OffsetKeyForTest(Queue0());
        }

        public void SeedPending(List<MessageExt> msgs) => _consumer.SetPendingForTest(_key, msgs);

        public void Correct(PullStatus status, long nextOffset)
            => _consumer.CorrectTagsOffsetForTest(_key, status, nextOffset);

        public long? Offset() => _consumer.ConsumeOffsetForTest(_key);

        public List<MessageExt> Take() => _consumer.TakeBatchForConsume(_key);

        public void Finish() => _consumer.FinishBatchConsume(_key);

        public int InFlight() => _consumer.InFlightForTest(_key);

        public int PendingCount() => _consumer.PendingForTest(_key).Count;
    }

    /// <summary>Both states of an empty response (NO_NEW_MSG / NO_MATCHED_MSG) advance;
    /// every other state leaves the offset alone.</summary>
    [Fact]
    public void AdvancesOnlyOnEmptyPullStatuses()
    {
        foreach (PullStatus s in new[] { PullStatus.NoNewMsg, PullStatus.NoMatchedMsg })
        {
            var h = new Harness();
            h.SeedPending(new List<MessageExt>());
            h.Correct(s, 42);
            Assert.Equal(42, h.Offset());
            h.Correct(s, 43);
            Assert.Equal(43, h.Offset());
        }

        foreach (PullStatus s in new[] { PullStatus.Found, PullStatus.OffsetIllegal })
        {
            var h = new Harness();
            h.SeedPending(new List<MessageExt>());
            h.Correct(s, 42);
            Assert.Null(h.Offset());
        }
    }

    [Fact]
    public void NeverRegresses()
    {
        var h = new Harness();
        h.Correct(PullStatus.NoNewMsg, 100);
        Assert.Equal(100, h.Offset());
        h.Correct(PullStatus.NoNewMsg, 50);
        Assert.Equal(100, h.Offset());
        h.Correct(PullStatus.NoNewMsg, 100);
        Assert.Equal(100, h.Offset());
        h.Correct(PullStatus.NoNewMsg, 101);
        Assert.Equal(101, h.Offset());
    }

    [Fact]
    public void CreatesTheRecordEvenForAZeroOffset()
    {
        var h = new Harness();
        h.SeedPending(new List<MessageExt>());
        h.Correct(PullStatus.NoNewMsg, 0);
        Assert.Equal(0, h.Offset());
        h.Correct(PullStatus.NoNewMsg, 0);
        Assert.Equal(0, h.Offset());
    }

    /// <summary>While the ProcessQueue still holds messages (the ones in the buffer plus
    /// the ones in the listener's hand) the offset is never moved.</summary>
    [Fact]
    public void RespectsTheProcessQueueGuard()
    {
        // there are still unconsumed messages in the buffer
        var h = new Harness();
        h.SeedPending(new List<MessageExt> { Ext(0) });
        h.Correct(PullStatus.NoNewMsg, 42);
        Assert.Null(h.Offset());

        // the buffer is empty but one message is in the listener's hand (removeMessage has not run yet)
        h.Take();
        Assert.Equal(1, h.InFlight());
        h.Correct(PullStatus.NoMatchedMsg, 42);
        Assert.Null(h.Offset());

        // after consumption finishes (including the exception put-back path) the count drops to zero and only then may the correction take effect
        h.Finish();
        Assert.Equal(0, h.InFlight());
        h.Correct(PullStatus.NoMatchedMsg, 42);
        Assert.Equal(42, h.Offset());
    }

    /// <summary>Not even a pending table entry (that queue was never pulled) is treated as
    /// empty; the correction must not be skipped for a missing entry.</summary>
    [Fact]
    public void MissingPendingEntryAllowsCorrection()
    {
        var h = new Harness();
        h.Correct(PullStatus.NoNewMsg, 7);
        Assert.Equal(7, h.Offset());
    }

    /// <summary>Taking a batch must be in the same critical section as registering it in
    /// flight: once taken, the gate closes immediately.</summary>
    [Fact]
    public void TakeRegistersInFlightInTheSameCriticalSection()
    {
        var h = new Harness();
        h.SeedPending(OffsetBatch(3));

        List<MessageExt> batch = h.Take();
        Assert.Equal(2, batch.Count);
        Assert.Equal(1, h.InFlight());
        Assert.Equal(1, h.PendingCount());
        h.Correct(PullStatus.NoNewMsg, 42);
        Assert.Null(h.Offset());

        // there is still an untaken allowance left: the buffer is non-empty, which blocks it too
        h.Finish();
        h.Correct(PullStatus.NoNewMsg, 42);
        Assert.Null(h.Offset());

        // the second batch finished, the gate is fully open
        Assert.Single(h.Take());
        h.Finish();
        h.Correct(PullStatus.NoNewMsg, 42);
        Assert.Equal(42, h.Offset());
    }

    /// <summary>Two batches in flight at once (two consume threads) must accumulate, and a
    /// key that was erased after being zeroed must be able to register again.</summary>
    [Fact]
    public void InFlightCountsAccumulateAndCanBeReRegistered()
    {
        var h = new Harness();
        h.SeedPending(OffsetBatch(4));
        h.Take();
        h.Take();
        Assert.Equal(2, h.InFlight());
        h.Finish();
        h.Correct(PullStatus.NoNewMsg, 42);
        Assert.Null(h.Offset());
        h.Finish();
        h.Correct(PullStatus.NoNewMsg, 42);
        Assert.Equal(42, h.Offset());

        var h2 = new Harness();
        h2.SeedPending(OffsetBatch(3));
        h2.Take();
        h2.Finish();
        Assert.Equal(0, h2.InFlight());
        h2.Take();
        Assert.Equal(1, h2.InFlight());
        h2.Correct(PullStatus.NoNewMsg, 42);
        Assert.Null(h2.Offset());
        h2.Finish();
        h2.Correct(PullStatus.NoNewMsg, 42);
        Assert.Equal(42, h2.Offset());
    }

    /// <summary>The gate must not be killed by its own bookkeeping: a take on an empty
    /// queue does not register, and a repeated finish does not revive the count.</summary>
    [Fact]
    public void BookkeepingIsIdempotentAndLeakFree()
    {
        var h = new Harness();
        h.SeedPending(new List<MessageExt>());
        Assert.Empty(h.Take());
        Assert.Equal(0, h.InFlight());
        h.Correct(PullStatus.NoNewMsg, 42);
        Assert.Equal(42, h.Offset());

        var h2 = new Harness();
        h2.SeedPending(OffsetBatch(1));
        h2.Take();
        h2.Finish();
        h2.Finish();
        Assert.Equal(0, h2.InFlight());
        h2.Correct(PullStatus.NoNewMsg, 42);
        Assert.Equal(42, h2.Offset());
    }
}
