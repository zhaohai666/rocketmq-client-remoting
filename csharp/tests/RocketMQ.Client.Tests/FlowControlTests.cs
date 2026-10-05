// 拉取前流控判定的单测 —— 不需要集群。
//
// 为什么必须离线锁死：Java ProcessQueue 的五个阈值里只有**条数**那一条会在"消息小、拉得快"
// 的场景里先命中，其余四条要等真出问题才看得见（队列级字节阈值失效 ⇒ 大消息把堆撑爆；
// 位点跨度失效 ⇒ 队首一条卡住、后面无限堆；topic 级阈值失效 ⇒ 同 topic 多队列各自为政）。
// 判据与 Python `_flow_control_hit` / Rust `flow_control_hit` / C++ `flowControlHit` 逐条同构：
//   1) 条数   >= PullThresholdForQueue（Java 的 Math.Max(1, n) 守卫：配 0 也按 1 条算）
//   2) 字节   >= PullThresholdSizeForQueue，单位 **MiB**（<=0 关闭）
//   3) 跨度   **严格大于** ConsumeConcurrentlyMaxSpan（pending 里 queueOffset 的 max-min；<=0 关闭）
//   4) topic 累计条数 >= PullThresholdForTopic（本实例该 topic **所有**队列合起来；-1 关闭）
//   5) topic 累计字节 >= PullThresholdSizeForTopic，单位 MiB（-1 关闭，且不复用第 2 条的开关）
// 命中一次只记一格 FlowControlTriggered。
//
// 注意 topic 级两条的语义偏差：Java RebalancePushImpl:67-81 会把 topic 阈值**除以队列数**
// 折算进队列级闸门，本移植（四语言一致）直接拿累计值比对，见各属性文档注释。
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

    /// <summary>关掉除条数以外的所有闸门：先命中的那条会掩盖被测分支。</summary>
    private static void GatesOffExceptCounts(DefaultMQPushConsumer c)
    {
        c.PullThresholdForQueue = int.MaxValue;
        c.PullThresholdSizeForQueue = 0;
        c.ConsumeConcurrentlyMaxSpan = 0;
        c.PullThresholdForTopic = -1;
        c.PullThresholdSizeForTopic = -1;
    }

    /// <summary>把一批消息预置进某队列缓冲，并登记该队列已分配（topic 级阈值靠这张表聚合）。</summary>
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
        // 默认 1000 条 / 100MiB / 跨度 2000，topic 级关闭：小缓冲一律不命中
        Stage(c, mq, NSized(Topic, 100, 1));
        Assert.False(Hit(c, mq));
        Assert.Equal(0L, c.FlowControlTriggered);
        // 缓冲为空（还没拉过任何消息）同样不命中，且跨度不能被算成负数
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
        // Java 的 Math.Max(1, n) 守卫：配 0 不是"全放行"，而是"1 条就停"
        c.PullThresholdForQueue = 0;
        Stage(c, mq, NSized(Topic, 100, 1));
        Assert.True(Hit(c, mq), "PullThresholdForQueue=0 要按 1 条算（Java 的 max(1,n) 守卫）");
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
        // 单位是 MiB 而不是字节：正好 1 MiB 就算命中（>=）
        Stage(c, mq, new List<MessageExt> { Sized(Topic, MiB, 0) });
        Assert.True(Hit(c, mq), "正好 1MiB 要命中（>=，不是 >）");
        Stage(c, mq, NSized(Topic, 2 * MiB, 4));
        Assert.True(Hit(c, mq));
        // 0 = 关闭这条闸门，再大的缓冲也不管
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
        // **严格大于**：跨度正好等于阈值不算（Java 同）
        c.ConsumeConcurrentlyMaxSpan = 100;
        Assert.False(Hit(c, mq), "跨度 100 不严格大于 100，不该命中");
        // 乱序缓冲也要量出真实跨度（min/max 而不是首尾差）
        c.ConsumeConcurrentlyMaxSpan = 10;
        Stage(c, mq, new List<MessageExt>
        {
            Sized(Topic, 1, 50), Sized(Topic, 1, 5), Sized(Topic, 1, 7),
        });
        Assert.True(Hit(c, mq), "乱序缓冲要按 max-min 量跨度，不是首尾差");
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
        // 单队列 1 条：topic 级也只看到 1 条
        Stage(c, q0, NSized(Topic, 1, 1));
        Assert.False(Hit(c, q0));
        // 同 topic 的兄弟队列各 1 条 ⇒ 累计 2 条，两条队列都必须停
        Stage(c, q1, NSized(Topic, 1, 1));
        Assert.True(Hit(c, q0));
        Assert.True(Hit(c, q1));
        // 别的 topic 不许掺进来：把本 topic 降到 1 条，另一 topic 堆 5 条
        Stage(c, q1, new List<MessageExt>());
        Stage(c, other, NSized(OtherTopic, 1, 5));
        Assert.False(Hit(c, q0), "别的 topic 的缓冲不能算进本 topic 的累计");
    }

    [Fact]
    public void TopicSizeGate_HasItsOwnSwitch_AndRunsLast()
    {
        var c = new DefaultMQPushConsumer("GID_FlowControlTopicSizeNet");
        MessageQueue q0 = QueueOf(Topic, 0);
        MessageQueue q1 = QueueOf(Topic, 1);
        GatesOffExceptCounts(c);
        // 队列级字节闸门**关掉**（0），只留 topic 级：误用队列级开关当闸门会让这条静默失效
        c.PullThresholdSizeForTopic = 1;
        Stage(c, q0, new List<MessageExt> { Sized(Topic, MiB / 2, 0) });
        Assert.False(Hit(c, q0));
        Stage(c, q1, new List<MessageExt> { Sized(Topic, 3 * MiB / 2, 0) });
        Assert.True(Hit(c, q0), "队列级 size 闸门关掉时主题级 size 仍要生效");
        Assert.True(Hit(c, q1));
        // 队列级那道还开着时先命中队列级（判定顺序：条数 → 字节 → 跨度 → topic 条数 → topic 字节）
        c.PullThresholdSizeForQueue = 1;
        Stage(c, q0, new List<MessageExt> { Sized(Topic, 2 * MiB, 0) });
        long before = c.FlowControlTriggered;
        Assert.True(Hit(c, q0));
        // 一次判定只记一格，即使多条闸门同时命中
        Assert.Equal(before + 1, c.FlowControlTriggered);
    }

    [Fact]
    public void ReasonText_UsesMbUnitWithOneDecimal()
    {
        // 日志里的数字格式是跨语言对拍的一部分（Python/Rust/C++ 同为 "%.1fMB"）
        var c = new DefaultMQPushConsumer("GID_FlowControlFormatNet");
        var mq = QueueOf(Topic, 0);
        GatesOffExceptCounts(c);
        c.PullThresholdSizeForQueue = 1;
        Stage(c, mq, new List<MessageExt> { Sized(Topic, (3 * MiB) / 2, 0) });
        Assert.True(Hit(c, mq));
        // 1.5MiB 在 InvariantCulture 下必须写成 "1.5MB"，不能带本地千分位或逗号小数点
        Assert.Equal("1.5", (1.5).ToString("F1", CultureInfo.InvariantCulture));
    }
}
