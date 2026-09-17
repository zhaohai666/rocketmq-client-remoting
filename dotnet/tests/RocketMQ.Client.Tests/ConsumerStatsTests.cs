// ConsumerStatsManager + ConsumeStatus/ConsumerRunningInfo(307) 编码单测。
// 采样链不依赖真实时间窗（sum/tps 用窗口端点差分，测试直接注入采样点）。
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
        // 相隔 1s 的两个采样点（与 Python 测试同款做法，不依赖真实时钟）
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
        // sum = 末点-首点；tps = sum*1000/span(ms)；avgpt = sum/timesDiff
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
        Assert.Equal(200, minute.Sum);       // 差分窗口：只算两点之间
        Assert.Equal(2, minute.Times);

        item.SampleHour();
        StatsSnapshot hour = item.GetStatsDataInHour();
        Assert.Equal(0, hour.Sum);           // 小时链单点 → 0
        Assert.Equal(0, hour.Times);
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

        ConsumeStatus cs = m.ConsumeStatus(Group, Topic);
        Assert.Equal(50.0, cs.PullRT, 6);
        Assert.Equal(5.0, cs.PullTPS, 6);
        Assert.Equal(10.0, cs.ConsumeRT, 6);
        Assert.Equal(8.0, cs.ConsumeOKTPS, 6);
        Assert.Equal(3.0, cs.ConsumeFailedTPS, 6);

        // 未记录的 key 返回全 0
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
        m.Shutdown();    // 幂等
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
        // Python/Java 侧 307 应答总是带这三个字段（空 map 序列化为 {}）
        Assert.Contains("\"statusTable\":{}", json);
        Assert.Contains("\"mqPopTable\":{}", json);
        Assert.Contains("\"userConsumerInfo\":{}", json);

        Assert.True(ConsumerRunningInfo.Decode(ri.Encode(), out ConsumerRunningInfo back));
        Assert.True(back.StatusTable.IsObject);
    }
}
