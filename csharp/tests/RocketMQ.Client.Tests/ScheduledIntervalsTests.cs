// Both of these deviations only show up as "slowness" on a real machine: no exception, no
// missing field, only phenomena like "the route of a new topic takes half a minute to
// refresh" that nobody is going to time. So MockCluster is used here to record the
// **arrival instant** of GET_ROUTEINFO_BY_TOPIC (WireRecord.ArrivalMs) and pin the cadence.
using RocketMQ.Client;
using RocketMQ.Common;
using RocketMQ.Remoting.Protocol;

namespace RocketMQ.Client.Tests;

public class ScheduledIntervalsTests
{
    [Fact]
    public void FacadeDefaultsMatchLegacyClientConfig()
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

        // the setter takes effect (it is not a read-only decoration)
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

        var bad = new MQClientInstance("sched_bad_period", new List<string> { "127.0.0.1:1" },
            pollNameServerIntervalMillis: 0);
        Assert.Equal(30000, bad.PollNameServerIntervalMillis);
        bad.Dispose();
    }

    /// <summary>
    /// The cadence of route refresh: the first hop lands on a 10ms initialDelay (the old
    /// form was initialDelay plus one period), after which it repeats at the
    /// pollNameServerInterval passed to the constructor.
    ///
    /// The threshold takes period/2: the ideal first hop is 10ms plus one local connect,
    /// and only a wrong order doubles it to a whole period.
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

    /// This one can only verify on a real cluster that "the offset really reached the broker
    /// with the right cadence" (examples/LiveScheduledIntervals); offline this layer can
    /// only lock "the period field was read" -- the loop body itself has to be fed offset
    /// table data before it sends a request.
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
