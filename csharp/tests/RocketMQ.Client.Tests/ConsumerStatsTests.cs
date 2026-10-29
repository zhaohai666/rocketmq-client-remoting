// Unit tests for ConsumerStatsManager plus ConsumeStatus/ConsumerRunningInfo(307) encoding.
// The sampling chain does not depend on a real time window (sum/tps use the difference of
// the window endpoints, and the test injects sample points directly).
using System;
using System.Collections.Generic;
using RocketMQ.Common;
using RocketMQ.Remoting.Protocol;
using Xunit;

namespace RocketMQ.Client.Tests;

public class ConsumerStatsTests
{
    private const string Group = "GID_StatsUnit";
    private const string Topic = "StatsTopic";

    private static StatsItemSet FeedTwoPoints(ConsumerStatsManager m,
        Func<ConsumerStatsManager, StatsItemSet> pick, long v, long t)
    {
        StatsItemSet set = pick(m);
        long now = UtilAll.CurrentTimeMillis();
        // two sample points 1s apart (no dependence on the real clock, the sample instants are written straight into the samples)
        set.GetAndCreate(Topic + "@" + Group).AppendSampleForTest(now - 1000, 0, 0);
        set.GetAndCreate(Topic + "@" + Group).AppendSampleForTest(now, v, t);
        return set;
    }

    [Fact]
    public void ComputeStatsData_EmptyChain_AllZero()
    {
        StatsSnapshot ss = StatsItems.Compute(new List<(long, long, long)>());
        Assert.Equal(0, ss.Sum);
        Assert.Equal(0, ss.Times);
        Assert.Equal(0.0, ss.Tps);
        Assert.Equal(0.0, ss.Avgpt);
    }

    [Fact]
    public void ComputeStatsData_DifferentialWindow()
    {
        // sum = last - first; tps = sum*1000/span(ms); avgpt = sum/timesDiff
        var chain = new List<(long, long, long)>
        {
            (1000, 0, 0),
            (2000, 300, 10),
            (3000, 900, 20)
        };
        StatsSnapshot ss = StatsItems.Compute(chain);
        Assert.Equal(900, ss.Sum);
        Assert.Equal(450.0, ss.Tps, 6);
        Assert.Equal(20, ss.Times);
        Assert.Equal(45.0, ss.Avgpt, 6);
    }

    [Fact]
    public void StatsItem_MinuteAndHourChainsIndependent()
    {
        StatsItem item = new StatsItemSet("PULL_RT").GetAndCreate("T@G");
        item.AddValue(100, 2);
        item.Sample();
        item.AddValue(200, 2);
        item.Sample();
        StatsSnapshot minute = item.GetStatsDataInMinute();
        Assert.Equal(300, minute.Sum);       // cumulative delta from the seeded zero to the last point
        Assert.Equal(4, minute.Times);       // the seeded zero baseline contributes no times

        item.SampleHour();
        StatsSnapshot hour = item.GetStatsDataInHour();
        Assert.Equal(300, hour.Sum);         // one cumulative point on top of the seeded zero
        Assert.Equal(4, hour.Times);
    }

    [Fact]
    public void StatsItem_MinuteAndHour_KeepSevenSamples()
    {
        StatsItem item = new StatsItemSet("PULL_RT").GetAndCreate("T@G");
        long now = UtilAll.CurrentTimeMillis();
        for (int i = 0; i < 30; i++)
        {
            item.AppendSampleForTest(now + i * 1000, i, i);
        }

        // The last seven points are 23..29; older samples and the zero baseline are evicted.
        Assert.Equal(6, item.GetStatsDataInMinute().Sum);
        Assert.Equal(6, item.GetStatsDataInMinute().Times);
        Assert.Equal(6, item.GetStatsDataInHour().Sum);
        Assert.Equal(6, item.GetStatsDataInHour().Times);
    }

    [Fact]
    public void StatsItem_DayChain_SeedsZeroOnePeriodBack()
    {
        StatsItem item = new StatsItemSet("PULL_RT").GetAndCreate("T@G");
        item.AddValue(7, 3);
        item.SampleDay();

        StatsSnapshot day = item.GetStatsDataInDay();
        Assert.Equal(7, day.Sum);
        Assert.Equal(3, day.Times);
    }

    [Fact]
    public void StatsItem_DayChain_Keeps25Samples()
    {
        StatsItem item = new StatsItemSet("PULL_RT").GetAndCreate("T@G");
        long now = UtilAll.CurrentTimeMillis();
        // The first append also seeds the zero baseline one hour back, hence 31 entries
        // before trimming: points 5..29 survive.
        for (int i = 0; i < 30; i++)
        {
            item.AppendDaySampleForTest(now - 3_600_000 + i * 1000, i, i);
        }

        StatsSnapshot day = item.GetStatsDataInDay();
        Assert.Equal(24, day.Sum);
        Assert.Equal(24, day.Times);
    }

    [Fact]
    public void ConsumeStatus_Maps_RT_ToAvgpt_TPS_ToTps()
    {
        var m = new ConsumerStatsManager();
        FeedTwoPoints(m, x => x.TopicAndGroupPullRT, 50, 1);
        FeedTwoPoints(m, x => x.TopicAndGroupPullTPS, 5, 1);
        FeedTwoPoints(m, x => x.TopicAndGroupConsumeRT, 10, 1);
        FeedTwoPoints(m, x => x.TopicAndGroupConsumeOKTPS, 8, 1);
        FeedTwoPoints(m, x => x.TopicAndGroupConsumeFailedTPS, 3, 1);

        // The minute chain is seeded one sampling period before the first injected point
        // (now - 1000), so TPS divides by the 11 s span from that baseline.
        const double minuteSpanMs = StatsItems.MinuteSampleIntervalMs + 1000;
        ConsumeStatus cs = m.ConsumeStatus(Group, Topic);
        Assert.Equal(50.0, cs.PullRT, 6);
        Assert.Equal(5.0 * 1000 / minuteSpanMs, cs.PullTPS, 6);
        Assert.Equal(10.0, cs.ConsumeRT, 6);
        Assert.Equal(8.0 * 1000 / minuteSpanMs, cs.ConsumeOKTPS, 6);
        Assert.Equal(3.0 * 1000 / minuteSpanMs, cs.ConsumeFailedTPS, 6);

        // an unrecorded key returns all zeros
        ConsumeStatus zero = m.ConsumeStatus(Group, "NoTopic");
        Assert.Equal(0.0, zero.PullRT, 9);
    }

    [Fact]
    public void StartShutdown_Lifecycle()
    {
        var m = new ConsumerStatsManager();
        m.Start();
        m.IncPullRT(Group, Topic, 1);
        m.Shutdown();
        m.Shutdown();    // idempotent
    }

    [Fact]
    public void ConsumeStatus_JsonRoundtrip()
    {
        var cs = new ConsumeStatus
        {
            PullRT = 1.5,
            PullTPS = 2.0,
            ConsumeRT = 3.0,
            ConsumeOKTPS = 4.0,
            ConsumeFailedTPS = 5.0,
            ConsumeFailedMsgs = 6
        };
        ConsumeStatus back = ConsumeStatus.FromJson(cs.ToJson());
        Assert.Equal(1.5, back.PullRT, 9);
        Assert.Equal(6, back.ConsumeFailedMsgs);
    }

    [Fact]
    public void ConsumerRunningInfo_StatusTableAndDefaults()
    {
        var ri = new ConsumerRunningInfo();
        ri.Properties[ConsumerRunningInfo.PropConsumeType] = "CONSUME_PASSIVELY";
        ri.SubscriptionSet = JsonValue.MakeArray();

        string json = System.Text.Encoding.UTF8.GetString(ri.Encode());
        Assert.Contains("\"statusTable\":{}", json);
        Assert.Contains("\"mqPopTable\":{}", json);
        Assert.Contains("\"userConsumerInfo\":{}", json);

        Assert.True(ConsumerRunningInfo.Decode(ri.Encode(), out ConsumerRunningInfo back));
        Assert.True(back.StatusTable.IsObject);
    }
}
