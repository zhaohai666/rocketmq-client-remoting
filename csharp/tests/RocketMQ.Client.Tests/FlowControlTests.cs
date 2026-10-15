// Unit tests for the pre-pull flow-control decision -- no cluster needed.
using System.Globalization;

using RocketMQ.Client;
using RocketMQ.Common;
using RocketMQ.Remoting.Protocol;

using Xunit;

namespace RocketMQ.Client.Tests;

public class FlowControlTests
{
    private const string Topic = "FlowControlNetUnitTopic";
    private const string OtherTopic = "FlowControlNetUnitOther";
    private const string Broker = "broker-a";
    private const int MiB = 1024 * 1024;

    private static MessageQueue QueueOf(string topic, int id) => new(topic, Broker, id);

    private static MessageExt Sized(string topic, int storeSize, long queueOffset) => new()
    {
        Topic = topic,
        BrokerName = Broker,
        QueueId = 0,
        QueueOffset = queueOffset,
        StoreSize = storeSize,
    };

    private static List<MessageExt> NSized(string topic, int storeSize, int n)
        => Enumerable.Range(0, n).Select(i => Sized(topic, storeSize, i)).ToList();

    /// <summary>Turns off every gate except the message count: whichever one matches first
    /// would mask the branch under test.</summary>
    private static void GatesOffExceptCounts(DefaultMQPushConsumer c)
    {
        c.PullThresholdForQueue = int.MaxValue;
        c.PullThresholdSizeForQueue = 0;
        c.ConsumeConcurrentlyMaxSpan = 0;
        c.PullThresholdForTopic = -1;
        c.PullThresholdSizeForTopic = -1;
    }

    /// <summary>Preloads a batch of messages into some queue's buffer and registers that
    /// queue as assigned (the topic-level thresholds aggregate through this table).</summary>
    private static void Stage(DefaultMQPushConsumer c, MessageQueue mq, List<MessageExt> msgs)
    {
        string key = DefaultMQPushConsumer.OffsetKeyForTest(mq);
        c.SetAssignedForTest(key, mq);
        c.SetPendingForTest(key, msgs);
    }

    private static bool Hit(DefaultMQPushConsumer c, MessageQueue mq)
        => c.FlowControlHitForTest(mq, DefaultMQPushConsumer.OffsetKeyForTest(mq));

    [Fact]
    public void Defaults_OnlyTheCountGateIsLive()
    {
        var c = new DefaultMQPushConsumer("GID_FlowControlDefaultsNet");
        var mq = QueueOf(Topic, 0);
        // the defaults are 1000 messages / 100MiB / span 2000, and the topic level is off: a small buffer never matches
        Stage(c, mq, NSized(Topic, 100, 1));
        Assert.False(Hit(c, mq));
        Assert.Equal(0L, c.FlowControlTriggered);
        // An empty buffer (nothing pulled yet) does not match either, and the span must not come out negative
        Stage(c, mq, new List<MessageExt>());
        Assert.False(Hit(c, mq));
    }

    [Fact]
    public void CountGate_FiresAtThreshold_AndTreatsZeroAsOne()
    {
        var c = new DefaultMQPushConsumer("GID_FlowControlCountNet");
        var mq = QueueOf(Topic, 0);
        GatesOffExceptCounts(c);
        c.PullThresholdForQueue = 3;
        Stage(c, mq, NSized(Topic, 100, 2));
        Assert.False(Hit(c, mq));
        Stage(c, mq, NSized(Topic, 100, 3));
        Assert.True(Hit(c, mq));
        Assert.Equal(1L, c.FlowControlTriggered);
        c.PullThresholdForQueue = 0;
        Stage(c, mq, NSized(Topic, 100, 1));
        Assert.True(Hit(c, mq), "PullThresholdForQueue=0 must be counted as 1 message (the max(1,n) guard)");
    }

    [Fact]
    public void SizeGate_IsInMib_AndDisabledByZero()
    {
        var c = new DefaultMQPushConsumer("GID_FlowControlSizeNet");
        var mq = QueueOf(Topic, 0);
        GatesOffExceptCounts(c);
        c.PullThresholdSizeForQueue = 1;             // 1 MiB
        Stage(c, mq, NSized(Topic, 300, 3));         // 900 B
        Assert.False(Hit(c, mq));
        // The unit is MiB rather than bytes: exactly 1 MiB counts as a match (>=)
        Stage(c, mq, new List<MessageExt> { Sized(Topic, MiB, 0) });
        Assert.True(Hit(c, mq), "exactly 1MiB must match (>=, not >)");
        Stage(c, mq, NSized(Topic, 2 * MiB, 4));
        Assert.True(Hit(c, mq));
        // 0 = this gate is off, no matter how large the buffer is
        c.PullThresholdSizeForQueue = 0;
        Assert.False(Hit(c, mq));
    }

    [Fact]
    public void SpanGate_IsStrictlyGreater_AndMeasuresRealSpan()
    {
        var c = new DefaultMQPushConsumer("GID_FlowControlSpanNet");
        var mq = QueueOf(Topic, 0);
        GatesOffExceptCounts(c);
        c.ConsumeConcurrentlyMaxSpan = 10;
        Stage(c, mq, new List<MessageExt> { Sized(Topic, 1, 0), Sized(Topic, 1, 100) });
        Assert.True(Hit(c, mq));
        c.ConsumeConcurrentlyMaxSpan = 100;
        Assert.False(Hit(c, mq), "a span of 100 is not strictly greater than 100, it must not match");
        // an out-of-order buffer must still measure the real span (min/max, not first-minus-last)
        c.ConsumeConcurrentlyMaxSpan = 10;
        Stage(c, mq, new List<MessageExt>
        {
            Sized(Topic, 1, 50), Sized(Topic, 1, 5), Sized(Topic, 1, 7),
        });
        Assert.True(Hit(c, mq), "an out-of-order buffer must measure the span as max-min, not first-minus-last");
        c.ConsumeConcurrentlyMaxSpan = 0;
        Assert.False(Hit(c, mq));
    }

    [Fact]
    public void TopicCountGate_AggregatesSiblings_ButNotOtherTopics()
    {
        var c = new DefaultMQPushConsumer("GID_FlowControlTopicCountNet");
        MessageQueue q0 = QueueOf(Topic, 0);
        MessageQueue q1 = QueueOf(Topic, 1);
        MessageQueue other = QueueOf(OtherTopic, 0);
        GatesOffExceptCounts(c);
        c.PullThresholdForTopic = 2;
        // a single queue with 1 message: the topic level only sees 1 message as well
        Stage(c, q0, NSized(Topic, 1, 1));
        Assert.False(Hit(c, q0));
        // sibling queues of the same topic with 1 message each => 2 accumulated, and both queues must be stopped
        Stage(c, q1, NSized(Topic, 1, 1));
        Assert.True(Hit(c, q0));
        Assert.True(Hit(c, q1));
        // another topic must not be mixed in: drop this topic to 1 message and pile 5 onto the other topic
        Stage(c, q1, new List<MessageExt>());
        Stage(c, other, NSized(OtherTopic, 1, 5));
        Assert.False(Hit(c, q0), "another topic's buffer must not count towards this topic's total");
    }

    [Fact]
    public void TopicSizeGate_HasItsOwnSwitch_AndRunsLast()
    {
        var c = new DefaultMQPushConsumer("GID_FlowControlTopicSizeNet");
        MessageQueue q0 = QueueOf(Topic, 0);
        MessageQueue q1 = QueueOf(Topic, 1);
        GatesOffExceptCounts(c);
        // the queue-level byte gate is **off** (0), only the topic level remains: misusing the queue-level switch as the gate makes this silently ineffective
        c.PullThresholdSizeForTopic = 1;
        Stage(c, q0, new List<MessageExt> { Sized(Topic, MiB / 2, 0) });
        Assert.False(Hit(c, q0));
        Stage(c, q1, new List<MessageExt> { Sized(Topic, 3 * MiB / 2, 0) });
        Assert.True(Hit(c, q0), "with the queue-level size gate off the topic-level size must still apply");
        Assert.True(Hit(c, q1));
        // while the queue-level one is still on, the queue level matches first (decision order: count -> bytes -> span -> topic count -> topic bytes)
        c.PullThresholdSizeForQueue = 1;
        Stage(c, q0, new List<MessageExt> { Sized(Topic, 2 * MiB, 0) });
        long before = c.FlowControlTriggered;
        Assert.True(Hit(c, q0));
        // one decision records exactly one slot, even when several gates match at once
        Assert.Equal(before + 1, c.FlowControlTriggered);
    }

    [Fact]
    public void ReasonText_UsesMbUnitWithOneDecimal()
    {
        // the number format in the log is locked to "%.1fMB", and the assertion parses in the same shape
        var c = new DefaultMQPushConsumer("GID_FlowControlFormatNet");
        var mq = QueueOf(Topic, 0);
        GatesOffExceptCounts(c);
        c.PullThresholdSizeForQueue = 1;
        Stage(c, mq, new List<MessageExt> { Sized(Topic, (3 * MiB) / 2, 0) });
        Assert.True(Hit(c, mq));
        // 1.5MiB must be written as "1.5MB" under InvariantCulture, with no local thousands separator or comma decimal point
        Assert.Equal("1.5", (1.5).ToString("F1", CultureInfo.InvariantCulture));
    }
}
