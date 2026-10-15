// Why it must be pinned offline: this is the only gate that can rescue a **hung listener**,
// and when it breaks everything is silent:
//   - judged too loosely (not strictly greater, counting unstamped as expired, touching
//     anything that is not the head): a message that is being consumed gets taken away and
//     resent, so it is delivered twice;
//   - judged too tightly (the head is not expired but it keeps scanning further): the head
//     is stuck forever and the expired messages behind it can never get out;
//   - the upper bound dropped (unbounded per round): one round empties the whole queue and
//     the offset / redelivery cadence is completely scrambled;
//   - removed after a failed resend: the message is really lost.
// A consumer that is not Started always throws from SendMessageBack, so offline can pin the
// "decision" and the "a failed resend keeps it in place" branches; the real-machine path
// (suspend -> sweep -> arrival via %RETRY%) is covered by
// examples/RocketMQ.Examples/LiveCleanExpiredMsg.cs.

using RocketMQ.Client;
using RocketMQ.Common;
using RocketMQ.Remoting.Protocol;

using Xunit;

namespace RocketMQ.Client.Tests;

public class CleanExpiredMsgTests
{
    private const string Group = "GID_CleanExpiredNetUnit";
    private const string Topic = "CleanExpiredNetUnitTopic";
    private const string Broker = "broker-a";

    private static MessageQueue Queue0() => new(Topic, Broker, 0);

    private static MessageExt Ext(long queueOffset, long? stampMs = null)
    {
        var m = new MessageExt
        {
            Topic = Topic,
            BrokerName = Broker,
            QueueId = 0,
            QueueOffset = queueOffset,
        };
        if (stampMs.HasValue)
        {
            m.PutProperty(MessageConst.PropertyConsumeStartTimestamp,
                stampMs.Value.ToString(System.Globalization.CultureInfo.InvariantCulture));
        }

        return m;
    }

    private static long Now() => UtilAll.CurrentTimeMillis();

    /// <summary>A consumer that is not Started: SendMessageBack always throws (exactly the
    /// "a failed resend keeps it in place" fixture).</summary>
    private sealed class Harness
    {
        private readonly DefaultMQPushConsumer _consumer;

        public Harness(int consumeTimeout = 1)
        {
            _consumer = new DefaultMQPushConsumer(Group) { ConsumeTimeout = consumeTimeout };
            Key = DefaultMQPushConsumer.OffsetKeyForTest(Queue0());
        }

        public string Key { get; }

        public DefaultMQPushConsumer Consumer => _consumer;

        public void Seed(params MessageExt[] msgs)
            => _consumer.SetPendingForTest(Key, msgs.ToList());

        public int Clean() => _consumer.CleanExpiredQueueForTest(Key);

        public List<MessageExt> Entries() => _consumer.ProcessQueueEntriesForTest(Key);

        public long? Offset() => _consumer.ConsumeOffsetForTest(Key);
    }

    private sealed class ReconsumeListener : IMessageListenerConcurrently
    {
        public bool Orderly() => false;

        public ConsumeConcurrentlyStatus ConsumeMessage(List<MessageExt> msgs,
                                                       ConsumeConcurrentlyContext context)
            => ConsumeConcurrentlyStatus.ReconsumeLater;
    }

    private sealed class NoopOrderlyListener : IMessageListenerOrderly
    {
        public bool Orderly() => true;

        public ConsumeOrderlyStatus ConsumeMessage(List<MessageExt> msgs, ConsumeOrderlyContext context)
            => ConsumeOrderlyStatus.Success;
    }

    [Fact]
    public void PeriodEqualsConsumeTimeoutMinutes()
    {
        Assert.Equal(900_000L, new Harness(15).Consumer.CleanExpirePeriodMillisForTest());
        Assert.Equal(60_000L, new Harness(1).Consumer.CleanExpirePeriodMillisForTest());
        Assert.Equal(60_000L, new Harness(0).Consumer.CleanExpirePeriodMillisForTest());
        Assert.Equal(60_000L, new Harness(-3).Consumer.CleanExpirePeriodMillisForTest());
    }

    [Fact]
    public void ConsumeStartTimestampReadsOnlyWellFormedStamps()
    {
        MessageExt m = Ext(0);
        Assert.Null(DefaultMQPushConsumer.ConsumeStartTimestampForTest(m));
        m.PutProperty(MessageConst.PropertyConsumeStartTimestamp, string.Empty);
        Assert.Null(DefaultMQPushConsumer.ConsumeStartTimestampForTest(m));
        m.PutProperty(MessageConst.PropertyConsumeStartTimestamp, "not-a-number");
        Assert.Null(DefaultMQPushConsumer.ConsumeStartTimestampForTest(m));
        m.PutProperty(MessageConst.PropertyConsumeStartTimestamp, "1700000000000");
        Assert.Equal(1_700_000_000_000L, DefaultMQPushConsumer.ConsumeStartTimestampForTest(m));
    }

    /// <summary>The expiry criterion is **strictly** greater (exactly at the threshold
    /// nothing happens), and an unstamped entry is not expired.</summary>
    [Fact]
    public void IsConsumeExpiredIsStrictAndIgnoresMissingStamps()
    {
        const long now = 10_000_000L;
        Assert.True(DefaultMQPushConsumer.IsConsumeExpiredForTest(now - 60_001, now, 1));
        Assert.False(DefaultMQPushConsumer.IsConsumeExpiredForTest(now - 60_000, now, 1));
        Assert.False(DefaultMQPushConsumer.IsConsumeExpiredForTest(now, now, 1));
        Assert.False(DefaultMQPushConsumer.IsConsumeExpiredForTest(null, now, 1));
        // The default is 15 minutes: 14 minutes 59.999 seconds does nothing, 15 minutes and 1 millisecond does
        Assert.False(DefaultMQPushConsumer.IsConsumeExpiredForTest(now - 899_999, now, 15));
        Assert.True(DefaultMQPushConsumer.IsConsumeExpiredForTest(now - 900_001, now, 15));
    }

    [Fact]
    public void StopsAtAFreshHead()
    {
        var h = new Harness();
        h.Seed(Ext(0), Ext(1, Now() - 120_000));  // the head is unstamped, the tail is expired
        Assert.Equal(0, h.Clean());
        Assert.Equal(2, h.Entries().Count);
    }

    [Fact]
    public void LeavesUnsignedMessagesAlone()
    {
        var h = new Harness();
        h.Seed(Ext(0));
        Assert.Equal(0, h.Clean());
        Assert.Single(h.Entries());
    }

    /// <summary>A bad stamp and an absent stamp fare alike: a bad value that fails to
    /// parse is treated as unstamped, not as "equivalent to 0".</summary>
    [Fact]
    public void BadStampIsNotTreatedAsAncient()
    {
        var h = new Harness();
        MessageExt m = Ext(0);
        m.PutProperty(MessageConst.PropertyConsumeStartTimestamp, "1700000000000x");
        h.Seed(m);
        Assert.Equal(0, h.Clean());
        Assert.Single(h.Entries());
    }

    [Fact]
    public void KeepsTheEntryWhenSendBackFails()
    {
        var h = new Harness();
        h.Seed(Ext(0, Now() - 120_000), Ext(1, Now() - 120_000));
        Assert.Equal(2, h.Clean());  // loop=min(2,16)=2: the same head is retried twice
        Assert.Equal(2, h.Entries().Count);
        Assert.Null(h.Offset());
    }

    [Fact]
    public void CapsOneRoundAtSixteen()
    {
        var h = new Harness();
        long stamp = Now() - 120_000;
        h.Seed(Enumerable.Range(0, 20).Select(i => Ext(i, stamp)).ToArray());
        Assert.Equal(16, h.Clean());
        Assert.Equal(20, h.Entries().Count);
    }

    [Fact]
    public void SkipsOrderlyConsumers()
    {
        var h = new Harness();
        h.Consumer.SetMessageListener(new NoopOrderlyListener());
        h.Seed(Ext(0, Now() - 120_000));
        Assert.Equal(0, h.Clean());
        Assert.Single(h.Entries());
    }

    /// <summary>The two ways of yielding "still the head": a normal finish (the entry is
    /// already removed / has advanced) and a smaller offset surfacing in front.</summary>
    [Fact]
    public void RemoveExpiredEntryOnlyWhenStillHead()
    {
        // Still the head: removed from the view
        var h = new Harness();
        h.Consumer.SetPendingForTest(h.Key, new List<MessageExt> { Ext(0), Ext(1) });
        Assert.True(h.Consumer.RemoveExpiredEntryIfStillHeadForTest(h.Key, h.Entries().Single(m => m.QueueOffset == 0)));
        Assert.Single(h.Entries());

        // A smaller offset surfaced in front: yield, it must not be grabbed away
        var h2 = new Harness();
        h2.Consumer.SetPendingForTest(h2.Key, new List<MessageExt> { Ext(3), Ext(5) });
        MessageExt five = h2.Entries().Single(m => m.QueueOffset == 5);
        Assert.False(h2.Consumer.RemoveExpiredEntryIfStillHeadForTest(h2.Key, five));
        Assert.Equal(2, h2.Entries().Count);

        // Already gone from the view (the listener removed it on its normal finish): likewise false, and it does not throw
        var h3 = new Harness();
        Assert.False(h3.Consumer.RemoveExpiredEntryIfStillHeadForTest(h3.Key, Ext(0)));
    }

    [Fact]
    public void ProcessQueueViewSeesInFlightAndPending()
    {
        var h = new Harness();
        h.Consumer.ConsumeMessageBatchMaxSize = 2;
        h.Seed(Ext(0), Ext(1), Ext(2));
        List<MessageExt> batch = h.Consumer.TakeBatchForConsume(h.Key);
        Assert.Equal(2, batch.Count);
        Assert.Equal(3, h.Entries().Count);
        h.Consumer.FinishBatchConsume(h.Key, batch);
        Assert.Single(h.Entries());
    }

    [Fact]
    public void SendBackSkipsEntriesSweptAway()
    {
        var h = new Harness();
        h.Consumer.ConsumeMessageBatchMaxSize = 2;
        h.Consumer.SetMessageListener(new ReconsumeListener());
        h.Seed(Ext(0), Ext(1));
        List<MessageExt> batch = h.Consumer.TakeBatchForConsume(h.Key);
        Assert.Equal(2, batch.Count);

        // Simulates the sweep resending and removing the first message of this batch (it is in _inFlightMsgs, both are cleared together)
        h.Consumer.RemoveProcessQueueEntryForTest(h.Key, batch[0]);

        h.Consumer.ConsumeBatchForTest(h.Key, Queue0(), batch);

        Assert.Equal(0, batch[0].ReconsumeTimes);
        Assert.Equal(1, batch[1].ReconsumeTimes);
        List<MessageExt> pending = h.Consumer.PendingForTest(h.Key);
        Assert.Single(pending);
        Assert.Same(batch[1], pending[0]);
    }
}
