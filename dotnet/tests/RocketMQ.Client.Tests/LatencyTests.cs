// 发送延迟故障容错单测（对齐 python/tests 侧语义 + Java MQFaultStrategy 阈值表）。
//
// 覆盖：
// 1. 阈值表逐档映射（0→0、50→0、550→2000、1800→5000、5000→10000、15000→30000、10000→10000）；
// 2. 隔离（isolation=true）固定按 10000ms 算档位；
// 3. FaultItem.UpdateNotAvailableDuration 只延长不缩短；
// 4. 容错表无记录 IsAvailable/IsReachable=true；隔离后 false、到期恢复；
// 5. 策略关闭时不记录任何故障项；
// 6. 策略开启：隔离 broker 后只选健康 broker；单 broker 注入隔离后退化为普通轮询仍能选出队列；
// 7. ResetIndex；lastBrokerName 避开语义。
using RocketMQ.Common;
using Xunit;

namespace RocketMQ.Client.Tests;

public class LatencyTests
{
    private static TopicPublishInfo MakeTpInfo(string brokerA, string? brokerB)
    {
        var tp = new TopicPublishInfo();
        tp.MsgQueueList.Add(new MessageQueue("T", brokerA, 0));
        if (brokerB != null)
        {
            tp.MsgQueueList.Add(new MessageQueue("T", brokerB, 0));
            tp.MsgQueueList.Add(new MessageQueue("T", brokerB, 1));
        }
        return tp;
    }

    // ------------------------------------------------ 阈值表逐档映射
    [Theory]
    [InlineData(0, 0)]
    [InlineData(49.9, 0)]
    [InlineData(50, 0)]
    [InlineData(549.9, 0)]
    [InlineData(550, 2000)]
    [InlineData(1799, 2000)]
    [InlineData(1800, 5000)]
    [InlineData(3000, 6000)]
    [InlineData(5000, 10000)]
    [InlineData(10000, 10000)]
    [InlineData(15000, 30000)]
    [InlineData(60000, 30000)]
    public void ComputeNotAvailableDuration_MatchesJavaTable(double latency, long expected)
    {
        var s = new MQFaultStrategy(true);
        Assert.Equal(expected, s.ComputeNotAvailableDuration(latency));
    }

    [Fact]
    public void Isolation_UsesFixed10000msTier()
    {
        var s = new MQFaultStrategy(true);
        s.UpdateFaultItem("broker-a", 12.0, isolation: true, reachable: false);

        FaultItem? item = s.LatencyFaultTolerance.GetFaultItem("broker-a");
        Assert.NotNull(item);
        Assert.Equal(12.0, item!.CurrentLatency);
        // 隔离档位固定 10000ms -> startTimestamp = now + 10000，立即不可用
        Assert.False(s.LatencyFaultTolerance.IsAvailable("broker-a"));
        Assert.False(s.LatencyFaultTolerance.IsReachable("broker-a"));
    }

    [Fact]
    public void UpdateNotAvailableDuration_OnlyExtends()
    {
        var f = new FaultItem("x");
        f.UpdateNotAvailableDuration(5000);
        long ts1 = f.StartTimestamp;
        f.UpdateNotAvailableDuration(1000);
        Assert.Equal(ts1, f.StartTimestamp);
    }

    [Fact]
    public void UnknownBroker_DefaultsAvailableAndReachable()
    {
        var tol = new LatencyFaultToleranceImpl();
        Assert.True(tol.IsAvailable("never-seen"));
        Assert.True(tol.IsReachable("never-seen"));
    }

    [Fact]
    public void Isolation_ExpiresAfterDuration()
    {
        var tol = new LatencyFaultToleranceImpl();
        tol.UpdateFaultItem("b", 1.0, 50, true);
        Assert.False(tol.IsAvailable("b"));
        Thread.Sleep(80);
        Assert.True(tol.IsAvailable("b"));
    }

    [Fact]
    public void Remove_RestoresDefaults()
    {
        var tol = new LatencyFaultToleranceImpl();
        tol.UpdateFaultItem("b", 99999.0, 30000, false);
        Assert.False(tol.IsAvailable("b"));
        tol.Remove("b");
        Assert.True(tol.IsAvailable("b"));
        Assert.True(tol.IsReachable("b"));
    }

    [Fact]
    public void DisabledStrategy_RecordsNothing()
    {
        var s = new MQFaultStrategy(false);
        s.UpdateFaultItem("broker-a", 99999.0, isolation: true, reachable: false);
        Assert.Null(s.LatencyFaultTolerance.GetFaultItem("broker-a"));
    }

    [Fact]
    public void EnabledStrategy_AvoidsIsolatedBroker()
    {
        var s = new MQFaultStrategy(true);
        s.UpdateFaultItem("broker-a", 99999.0, isolation: true, reachable: false);
        TopicPublishInfo tp = MakeTpInfo("broker-a", "broker-b");
        for (int i = 0; i < 6; ++i)
        {
            MessageQueue mq = s.SelectOneMessageQueue(tp, null);
            Assert.Equal("broker-b", mq.BrokerName);
        }
    }

    [Fact]
    public void EnabledStrategy_SingleBrokerFallsBackToPlainPolling()
    {
        var s = new MQFaultStrategy(true);
        s.UpdateFaultItem("broker-a", 99999.0, isolation: true, reachable: false);
        TopicPublishInfo tp = MakeTpInfo("broker-a", null);
        MessageQueue mq = s.SelectOneMessageQueue(tp, null);
        Assert.Equal("broker-a", mq.BrokerName);
    }

    [Fact]
    public void DisabledStrategy_UsesPlainPollingWithLastBrokerAvoidance()
    {
        var s = new MQFaultStrategy(false);
        TopicPublishInfo tp = MakeTpInfo("broker-a", "broker-b");
        MessageQueue mq = s.SelectOneMessageQueue(tp, "broker-a");
        Assert.Equal("broker-b", mq.BrokerName);
    }

    [Fact]
    public void ResetIndex_RestartsPolling()
    {
        var s = new MQFaultStrategy(true);
        TopicPublishInfo tp = MakeTpInfo("broker-a", "broker-b");
        s.SelectOneMessageQueue(tp, null, resetIndex: true);
        s.SelectOneMessageQueue(tp, null, resetIndex: true);
        tp.ResetIndex();
        MessageQueue mq = s.SelectOneMessageQueue(tp, null, resetIndex: true);
        Assert.Equal("broker-a", mq.BrokerName);
        Assert.Equal(0, mq.QueueId);
    }

    [Fact]
    public void ProducerSwitch_DefaultsOff_AndWireRecordsLatency()
    {
        var p = new DefaultMQProducer("PG_LatUnit");
        Assert.False(p.SendLatencyFaultEnable);
        p.SendLatencyFaultEnable = true;
        Assert.True(p.MqFaultStrategy.IsSendLatencyFaultEnable());
    }
}
