// One to one with the src side's range checks: the same gate table, the same check order, the same message text.
using RocketMQ.Client;
using RocketMQ.Common;

using Xunit;

namespace RocketMQ.Client.Tests;

public class ConsumerCheckConfigTests
{
    private const string Group = "CID_net_check_config";

    private sealed record Gate(
        string Field,
        long Lo,
        long Hi,
        string Message,
        Action<DefaultMQPushConsumer, long> Set,
        bool MinusOneOff,
        bool LowerBoundClampedBySetter);

    private static readonly Gate[] Gates =
    {
        new("consumeThreadMin", 1, 1000,
            "consumeThreadMin Out of range [1, 1000]",
            (c, v) => c.SetConsumeThreadMin((int)v), false, true),
        new("consumeThreadMax", 1, 1000,
            "consumeThreadMax Out of range [1, 1000]",
            (c, v) => c.SetConsumeThreadMax((int)v), false, true),
        new("consumeConcurrentlyMaxSpan", 1, 65535,
            "consumeConcurrentlyMaxSpan Out of range [1, 65535]",
            (c, v) => c.ConsumeConcurrentlyMaxSpan = v, false, false),
        new("pullThresholdForQueue", 1, 65535,
            "pullThresholdForQueue Out of range [1, 65535]",
            (c, v) => c.PullThresholdForQueue = (int)v, false, false),
        new("pullThresholdForTopic", 1, 6553500,
            "pullThresholdForTopic Out of range [1, 6553500]",
            (c, v) => c.PullThresholdForTopic = (int)v, true, false),
        new("pullThresholdSizeForQueue", 1, 1024,
            "pullThresholdSizeForQueue Out of range [1, 1024]",
            (c, v) => c.PullThresholdSizeForQueue = (int)v, false, false),
        new("pullThresholdSizeForTopic", 1, 102400,
            "pullThresholdSizeForTopic Out of range [1, 102400]",
            (c, v) => c.PullThresholdSizeForTopic = (int)v, true, false),
        new("pullInterval", 0, 65535,
            "pullInterval Out of range [0, 65535]",
            (c, v) => c.PullIntervalMillis = (int)v, false, false),
        new("consumeMessageBatchMaxSize", 1, 1024,
            "consumeMessageBatchMaxSize Out of range [1, 1024]",
            (c, v) => c.ConsumeMessageBatchMaxSize = (int)v, false, true),
        new("pullBatchSize", 1, 1024,
            "pullBatchSize Out of range [1, 1024]",
            (c, v) => c.PullBatchSize = (int)v, false, false),
        new("popInvisibleTime",
            DefaultMQPushConsumer.MinPopInvisibleTime, DefaultMQPushConsumer.MaxPopInvisibleTime,
            "popInvisibleTime Out of range [5000, 300000]",
            (c, v) => c.PopInvisibleTime = v, false, false),
        new("popBatchNums", 1, 32,
            "popBatchNums Out of range [1, 32]",
            (c, v) => c.PopBatchNums = (int)v, false, false),
    };

    /// <summary>An instance with only the numeric config: CheckConfigRanges is pure logic
    /// and needs no listener/route.</summary>
    private static DefaultMQPushConsumer New() => new(Group);

    /// <summary>An instance with one gate value configured. The two thread gates are
    /// mutually exclusive (min must not exceed max), and the default min=20/max=64 would
    /// turn "change max to a single digit" into a min&gt;max error, so that pair is first
    /// moved to legal ends of the range (1 / 1000) to guarantee that what gets reported is
    /// the gate under test.</summary>
    private static DefaultMQPushConsumer Broke(Gate gate, long value)
    {
        DefaultMQPushConsumer c = New();
        c.SetConsumeThreadMin(1);
        c.SetConsumeThreadMax(1000);
        gate.Set(c, value);
        return c;
    }

    private static string FirstMessageOf(DefaultMQPushConsumer c)
    {
        try
        {
            c.CheckConfigRanges();
        }
        catch (MQClientException e)
        {
            return e.Message;
        }

        return string.Empty;
    }

    private static void ExpectRejected(Action<DefaultMQPushConsumer> breakIt, string want)
    {
        DefaultMQPushConsumer c = New();
        breakIt(c);
        Assert.Equal(want, FirstMessageOf(c));
    }

    private static void ExpectAccepted(Action<DefaultMQPushConsumer>? set = null)
    {
        DefaultMQPushConsumer c = New();
        if (set is not null)
        {
            set(c);
        }

        // not throwing means passing; if it throws, xUnit will surface the exception verbatim
        c.CheckConfigRanges();
    }

    [Fact]
    public void DefaultConfigurationPassesTheGate()
    {
        // The default must be legal, otherwise this change would lock out every production user who does not touch the config.
        ExpectAccepted();
    }

    [Fact]
    public void EveryGateRejectsAboveItsUpperBound()
    {
        foreach (Gate gate in Gates)
        {
            Assert.Equal(gate.Message, FirstMessageOf(Broke(gate, gate.Hi + 1)));
        }
    }

    [Fact]
    public void EveryGateRejectsBelowItsLowerBound()
    {
        foreach (Gate gate in Gates)
        {
            if (gate.LowerBoundClampedBySetter)
            {
                // The lower bounds of these three are lifted by the setter's Math.Max(1, n), so they are unreachable through
                // the public API; SetterClampKeepsThreadAndBatchLowerBoundsReachable pins that clamp itself separately.
                continue;
            }

            Assert.Equal(gate.Message, FirstMessageOf(Broke(gate, gate.Lo - 1)));
        }
    }

    [Fact]
    public void EveryGateAcceptsBothEndsOfItsInterval()
    {
        foreach (Gate gate in Gates)
        {
            Assert.Equal(string.Empty, FirstMessageOf(Broke(gate, gate.Lo)));
            Assert.Equal(string.Empty, FirstMessageOf(Broke(gate, gate.Hi)));
        }
    }

    [Fact]
    public void OnlyTopicThresholdsTreatMinusOneAsSwitchOff()
    {
        foreach (Gate gate in Gates.Where(g => g.MinusOneOff))
        {
            Assert.Equal(string.Empty, FirstMessageOf(Broke(gate, -1)));
        }

        ExpectRejected(c => c.PullThresholdSizeForQueue = -1,
            "pullThresholdSizeForQueue Out of range [1, 1024]");
        ExpectRejected(c => c.PullThresholdForTopic = 0,
            "pullThresholdForTopic Out of range [1, 6553500]");
    }

    [Fact]
    public void PullIntervalAcceptsZeroButRejectsNegative()
    {
        ExpectAccepted(c => c.PullIntervalMillis = 0);
        ExpectRejected(c => c.PullIntervalMillis = -1, "pullInterval Out of range [0, 65535]");
    }

    [Fact]
    public void ThreadMinLargerThanThreadMaxIsRejectedWithBothNumbers()
    {
        ExpectAccepted(c =>
        {
            c.SetConsumeThreadMin(8);
            c.SetConsumeThreadMax(8);
        });
        ExpectRejected(c =>
        {
            c.SetConsumeThreadMin(8);
            c.SetConsumeThreadMax(4);
        }, "consumeThreadMin (8) is larger than consumeThreadMax (4)");
    }

    [Fact]
    public void SetterClampKeepsThreadAndBatchLowerBoundsReachable()
    {
        DefaultMQPushConsumer c = New();
        c.SetConsumeThreadMin(0);
        c.SetConsumeThreadMax(0);
        Assert.Equal(1, c.ConsumeThreadMin);
        Assert.Equal(1, c.ConsumeThreadMax);
        c.ConsumeMessageBatchMaxSize = 0;
        Assert.Equal(1, c.ConsumeMessageBatchMaxSize);
        c.CheckConfigRanges();
    }

    [Fact]
    public void GatesAreCheckedInLegacyOrder()
    {
        ExpectRejected(c =>
        {
            c.ConsumeConcurrentlyMaxSpan = 0;
            c.PullBatchSize = 0;
        }, "consumeConcurrentlyMaxSpan Out of range [1, 65535]");

        ExpectRejected(c =>
        {
            c.PullThresholdForQueue = 0;
            c.PullBatchSize = 0;
        }, "pullThresholdForQueue Out of range [1, 65535]");

        ExpectRejected(c =>
        {
            c.PullThresholdSizeForQueue = 0;
            c.PullThresholdSizeForTopic = 0;
        }, "pullThresholdSizeForQueue Out of range [1, 1024]");

        ExpectRejected(c =>
        {
            c.PullBatchSize = 0;
            c.PopBatchNums = 0;
        }, "pullBatchSize Out of range [1, 1024]");

        ExpectRejected(c =>
        {
            c.PopInvisibleTime = 4999;
            c.PopBatchNums = 33;
        }, "popInvisibleTime Out of range [5000, 300000]");
    }

    [Fact]
    public void PopBatchNumsFollowsLegacyLiteralLessOrEqualZero()
    {
        ExpectRejected(c => c.PopBatchNums = 0, "popBatchNums Out of range [1, 32]");
        ExpectRejected(c => c.PopBatchNums = -7, "popBatchNums Out of range [1, 32]");
    }

    [Fact]
    public void StartRejectsBadConfigBeforeTouchingTheNetwork()
    {
        // The validation point must precede any connecting/registering: a bad config failing
        // to start is normal, and what is feared most is "it started but left a
        // half-started instance behind". Here the gate is made bad and it points at a
        // nameServer nobody listens on, and what is required is a **config** error rather
        // than a connection error, with IsStarted still false.
        DefaultMQPushConsumer c = New();
        c.SetNamesrvAddr("127.0.0.1:1");
        c.InstanceName = "net_check_config_bad";
        c.Subscribe("NetCheckConfigTopic", "*");
        c.SetMessageListener(new NoOpListener());
        c.PullBatchSize = 1025;

        MQClientException e = Assert.Throws<MQClientException>(() => c.Start());
        Assert.Equal("pullBatchSize Out of range [1, 1024]", e.Message);
        Assert.False(c.IsStarted);
    }

    private sealed class NoOpListener : IMessageListenerConcurrently
    {
        public bool Orderly() => false;

        public ConsumeConcurrentlyStatus ConsumeMessage(List<MessageExt> msgs,
            ConsumeConcurrentlyContext context)
        {
            return ConsumeConcurrentlyStatus.ConsumeSuccess;
        }
    }
}
