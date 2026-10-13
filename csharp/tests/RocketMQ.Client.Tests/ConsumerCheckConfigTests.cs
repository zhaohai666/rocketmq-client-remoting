// 与 src 侧的取值范围检查一一对应：同一张闸门表、同一段检查顺序、同一条文案。
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

    /// <summary>只带数值配置的实例：CheckConfigRanges 是纯逻辑，不需要 listener/路由。</summary>
    private static DefaultMQPushConsumer New() => new(Group);

    /// <summary>配好某一条闸门取值的实例。线程两条闸门互斥（min 不能大于 max），默认
    /// min=20/max=64 会把"改 max 到个位数"顶成 min&gt;max 错，所以先把这一对挪到合法
    /// 区间两端（1 / 1000），保证报出来的一定是被测那条闸门。</summary>
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

        // 不抛即通过；抛了让 xUnit 把异常原文带出来
        c.CheckConfigRanges();
    }

    [Fact]
    public void DefaultConfigurationPassesTheGate()
    {
        // 默认值必须合法，否则这条改动会把所有不改配置的现网用户挡在门外。
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
                // 这三条的下界被 setter 的 Math.Max(1, n) 抬掉了，从公开 API 走不到，
                // 由 SetterClampKeepsThreadAndBatchLowerBoundsReachable 单独锁兜底本身。
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
        // 校验点必须领先于任何建连/注册：坏配置起不来是常态，最怕的是"起来了但留了
        // 半启动实例"。这里把闸门配坏 + 指向一个没人监听的 nameServer，要求报的是
        // **配置**错而不是连接错，且 IsStarted 仍为 false。
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
