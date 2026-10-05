// 定时任务的 initialDelay / 周期语义单测（Java MQClientInstance#startScheduledTask:389-432）。
//
// 为什么必须离线锁死：Java 一律用
//   scheduleAtFixedRate(work, initialDelay, period)
// ——**首跳落在 initialDelay 这一刻**，不是 initialDelay + period；周期在调度时定型，
// 之后改配置不重排。旧实现是「先睡 initialDelay，再在循环头睡一个整周期」，于是路由刷新
// 的首跳晚了整整一个周期（30.03s 才刷第一次），周期还写死 30s，
// ClientConfig#pollNameServerInterval（:58）形同虚设。
//
// 这两个偏差在真机上都只表现为「慢」：没有异常、没有缺字段，只有「新 topic 的路由要等
// 半分钟才刷新」这种没人会去计时的现象。所以这里用 MockCluster 记录
// GET_ROUTEINFO_BY_TOPIC 的**到达时刻**（WireRecord.ArrivalMs），把节奏钉死。
//
// 与 Python(tests/test_scheduled_intervals.py)、C++(tests/test_scheduled_intervals.cpp)、
// Rust(mq_client.rs / producer.rs / consumer.rs 内的同题用例) 一一对应。
using RocketMQ.Client;
using RocketMQ.Common;
using RocketMQ.Remoting.Protocol;

namespace RocketMQ.Client.Tests;

public class ScheduledIntervalsTests
{
    /// <summary>
    /// Java ClientConfig:58 / :66 的默认值落在**门面**上（不只是实例上）：
    /// pollNameServerInterval=30000、persistConsumerOffsetInterval=5000。
    /// </summary>
    [Fact]
    public void FacadeDefaultsMatchJavaClientConfig()
    {
        var producer = new DefaultMQProducer("PG_sched_default");
        Assert.Equal(30000, producer.PollNameServerIntervalMillis);

        var consumer = new DefaultMQPushConsumer("CID_sched_default");
        Assert.Equal(30000, consumer.PollNameServerIntervalMillis);
        Assert.Equal(5000, consumer.PersistConsumerOffsetIntervalMillis);

        var pull = new DefaultMQPullConsumer("PG_sched_pull_default");
        Assert.Equal(30000, pull.PollNameServerIntervalMillis);

        var lite = new DefaultLitePullConsumer("PG_sched_lite_default");
        Assert.Equal(30000, lite.PollNameServerIntervalMillis);

        var admin = new DefaultMQAdminExt("sched_admin_default");
        Assert.Equal(30000, admin.PollNameServerIntervalMillis);

        // setter 生效（不是只读的装饰）
        producer.PollNameServerIntervalMillis = 700;
        consumer.PollNameServerIntervalMillis = 1500;
        consumer.PersistConsumerOffsetIntervalMillis = 250;
        pull.PollNameServerIntervalMillis = 800;
        lite.PollNameServerIntervalMillis = 900;
        admin.PollNameServerIntervalMillis = 1100;
        Assert.Equal(700, producer.PollNameServerIntervalMillis);
        Assert.Equal(1500, consumer.PollNameServerIntervalMillis);
        Assert.Equal(250, consumer.PersistConsumerOffsetIntervalMillis);
        Assert.Equal(800, pull.PollNameServerIntervalMillis);
        Assert.Equal(900, lite.PollNameServerIntervalMillis);
        Assert.Equal(1100, admin.PollNameServerIntervalMillis);
    }

    /// <summary>
    /// 门面透传：Start() 时把门面上的值交给 MQClientInstance（Java 的实例级副本），
    /// 非正数回落到 Java 默认 30000（而不是退化成忙转）。
    /// </summary>
    [Fact]
    public void FacadeIntervalReachesTheInstance()
    {
        using MockCluster cluster = MockCluster.Start(1);

        var producer = new DefaultMQProducer("PG_sched_fwd");
        producer.InstanceName = "sched_fwd_producer";
        producer.NamesrvAddr = cluster.NamesrvAddr;
        producer.PollNameServerIntervalMillis = 700;
        producer.Start();
        try
        {
            Assert.Equal(700, producer.Client().PollNameServerIntervalMillis);
        }
        finally
        {
            producer.Shutdown();
        }

        var consumer = new DefaultMQPushConsumer("CID_sched_fwd");
        consumer.InstanceName = "sched_fwd_consumer";
        consumer.SetNamesrvAddr(cluster.NamesrvAddr);
        consumer.Subscribe("SchedIntervalFwd", "*");
        consumer.SetMessageListener(new NullListener());
        consumer.PollNameServerIntervalMillis = 1500;
        consumer.PersistConsumerOffsetIntervalMillis = 250;
        consumer.Start();
        try
        {
            Assert.Equal(1500, consumer.Client().PollNameServerIntervalMillis);
        }
        finally
        {
            consumer.Shutdown();
        }

        // 非正数回落（Java ClientConfig:58 的默认值），不是 0 也不是负周期
        var bad = new MQClientInstance("sched_bad_period", new List<string> { "127.0.0.1:1" },
            pollNameServerIntervalMillis: 0);
        Assert.Equal(30000, bad.PollNameServerIntervalMillis);
        bad.Dispose();
    }

    /// <summary>
    /// 路由刷新的节奏：首跳落在 10ms 的 initialDelay（旧写法是 initialDelay + 一个周期），
    /// 之后按构造时传入的 pollNameServerInterval 重复。
    ///
    /// 阈值取 period/2：理想首跳是 10ms 加一次本机建连，只有顺序错了才会翻倍到一个周期。
    /// </summary>
    [Fact]
    public void RouteRefreshFirstTickLandsAtInitialDelayThenFollowsThePeriod()
    {
        using MockCluster cluster = MockCluster.Start(1);
        const string topic = "SchedIntervalTick";
        const int periodMillis = 1200;

        var instance = new MQClientInstance("sched_tick_instance",
            new List<string> { cluster.NamesrvAddr },
            pollNameServerIntervalMillis: periodMillis);
        Assert.Equal(periodMillis, instance.PollNameServerIntervalMillis);
        instance.RegisterTopicInUse(topic);

        long mark = cluster.ClockOrigin;
        instance.Start();

        List<long> arrivals = WaitForRouteTicks(cluster, 3, TimeSpan.FromSeconds(6), mark);
        instance.Dispose();

        Assert.True(arrivals.Count >= 3,
            $"route refresh must tick 3 times within 6s, got {arrivals.Count}");
        Assert.True(arrivals[0] < periodMillis / 2,
            $"first tick must land at the initialDelay, not one period later: {arrivals[0]}ms");
        long gap = arrivals[1] - arrivals[0];
        Assert.True(gap >= 1000 && gap <= 2400,
            $"second tick must follow the configured period {periodMillis}ms, gap={gap}ms");
        long gap2 = arrivals[2] - arrivals[1];
        Assert.True(gap2 >= 1000 && gap2 <= 2400,
            $"third tick must follow the configured period {periodMillis}ms, gap={gap2}ms");
    }

    /// <summary>
    /// 消费位点落盘：首笔在 Java 的 10s initialDelay 处（不是立刻、也不是 10s+周期），
    /// 之后按 persistConsumerOffsetInterval 重复。
    ///
    /// 这条只能在真集群上验「位点真的写到了 broker 且节奏正确」（examples/LiveScheduledIntervals），
    /// 离线这一层只能锁「周期字段被读到」——循环体本身要先把位点表喂上数据才会发请求。
    /// </summary>
    [Fact]
    public void PersistIntervalIsReadableAndConfigurable()
    {
        var consumer = new DefaultMQPushConsumer("CID_sched_persist");
        Assert.Equal(5000, consumer.PersistConsumerOffsetIntervalMillis);
        consumer.PersistConsumerOffsetIntervalMillis = 700;
        Assert.Equal(700, consumer.PersistConsumerOffsetIntervalMillis);
    }

    private static List<long> WaitForRouteTicks(MockCluster cluster, int want,
        TimeSpan timeout, long mark)
    {
        DateTime deadline = DateTime.UtcNow + timeout;
        List<long> arrivals = new();
        while (DateTime.UtcNow < deadline)
        {
            arrivals = cluster.Records()
                .Where(r => r.Code == RequestCode.GetRouteinfoByTopic)
                .Where(r => r.Ext.TryGetValue("topic", out string? t) && t == "SchedIntervalTick")
                .Select(r => r.ArrivalMs - mark)
                .ToList();
            if (arrivals.Count >= want) break;
            Thread.Sleep(20);
        }

        return arrivals;
    }

    private sealed class NullListener : IMessageListenerConcurrently
    {
        public bool Orderly() => false;

        public ConsumeConcurrentlyStatus ConsumeMessage(List<MessageExt> msgs,
            ConsumeConcurrentlyContext context) => ConsumeConcurrentlyStatus.ConsumeSuccess;
    }
}
