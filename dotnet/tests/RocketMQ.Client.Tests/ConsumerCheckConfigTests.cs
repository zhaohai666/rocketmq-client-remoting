// 推送消费者启动期数值闸门（Java DefaultMQPushConsumerImpl.checkConfig :1099-1209）的单测。
//
// 为什么必须离线锁死：这条闸门是"用户配错就别起"的前置检查，真机上只有把它**写严**才
// 看得出来 —— 比 Java 多拒一个合法值，用户直接起不来；比 Java 少拒一个越界值，坏配置
// 会带着僵尸 clientId 打到 broker 上，把 rebalance 用的 cidAll 撑歪。两种后果在真机上
// 都表现为"消息偶尔不均"，排查成本极高，所以区间端点与文案逐字对 Java 锁死。
//
// 与 Python(tests/test_consumer_check_config.py)、C++(tests/test_consumer_check_config.cpp)、
// Rust(consumer.rs 的 RANGE_GATES)一一对应：同一张闸门表、同一段检查顺序、同一条文案。
//
// ⚠ 本移植有三处与 Java 的**已知差异**，都在这里显式锁住而不是假装一致：
//   1. SetConsumeThreadMin/Max 与 ConsumeMessageBatchMaxSize 的 setter 带 Math.Max(1, n)
//      兜底，所以这三条闸门的**下界从公开 API 走不到**（写 0 会被抬成 1）。上界照常可测。
//   2. Java :1058 的 consumeTimestamp 格式校验在这里是 no-op：本移植没有可配的
//      ConsumeTimestamp 属性（CONSUME_FROM_TIMESTAMP 一律取"30 分钟前"），没有可写坏的
//      字符串可拒。这是少一个可配项，不是漏了校验。
//   3. 校验点在 Start() 里所有 null 检查之后、实例化 MQClientInstance 之前 —— 坏配置必须
//      在建立任何网络连接之前失败（真机侧由 LiveFlowControl S5 反证 broker 查不到该组）。
using RocketMQ.Client;
using RocketMQ.Common;

using Xunit;

namespace RocketMQ.Client.Tests;

public class ConsumerCheckConfigTests
{
    private const string Group = "CID_net_check_config";

    /// <summary>一条数值闸门：Java 里的字段名、闭区间、逐字文案、怎么写这个字段、
    /// -1 是否是"关闭"哨兵、下界是否被 setter 抬掉（从公开 API 走不到）。</summary>
    private sealed record Gate(
        string Field,
        long Lo,
        long Hi,
        string Message,
        Action<DefaultMQPushConsumer, long> Set,
        bool MinusOneOff,
        bool LowerBoundClampedBySetter);

    /// <summary>Java checkConfig 数值段的全部闸门，**数组顺序即检查顺序**。</summary>
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
        // pullInterval 的下界是 **0**（Java 原文如此，0 = 不间隔），别照抄邻居闸门的 1。
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
        // Java 用严格 `lo <= x <= hi`（< lo || > hi 才拒），两端都是合法值。
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
        // 边界值被误拒是这条闸门最常见的写坏方式（比 Java 还严），真机上表现为"贴着
        // 上限配就是不启动"，离线必须逐条锁住两端。
        foreach (Gate gate in Gates)
        {
            Assert.Equal(string.Empty, FirstMessageOf(Broke(gate, gate.Lo)));
            Assert.Equal(string.Empty, FirstMessageOf(Broke(gate, gate.Hi)));
        }
    }

    [Fact]
    public void OnlyTopicThresholdsTreatMinusOneAsSwitchOff()
    {
        // Java 对 pullThresholdForTopic / pullThresholdSizeForTopic 写的是
        // `if (x != -1) { 区间检查 }`，-1 是"关闭"哨兵；其余闸门没有这层豁免。
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
        // 严格 >：min == max 合法（Java 允许单线程消费者）。
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
        // 与 Java 的差异（属性文档注释已声明）：setter 的 Math.Max(1, n) 把 0 抬成 1。
        // 锁住这个兜底本身 —— 哪天删掉兜底，CheckConfigRanges 就是唯一防线，
        // 而这张表的下界用例已经证明那条防线自己写对了。
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
    public void GatesAreCheckedInJavaOrder()
    {
        // 多条同时越界时报哪一条取决于顺序 —— 与 Java 逐条同序才能保证用户看到的
        // 第一句错一样。每段用"被测闸门 + 后面某条也越界"证明前者先命中。
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
    public void PopBatchNumsFollowsJavaLiteralLessOrEqualZero()
    {
        // Java 原文是 `popBatchNums <= 0`，不是 `< 1`；0 与负数都拒，文案仍写 [1, 32]。
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
