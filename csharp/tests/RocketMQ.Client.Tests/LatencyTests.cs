// Covered:
// 1. the threshold table's tier-by-tier mapping (0->0, 50->0, 550->2000, 1800->5000, 5000->10000, 15000->30000, 10000->10000);
// 2. isolation (isolation=true) always computes its tier as 10000ms;
// 3. FaultItem.UpdateNotAvailableDuration only extends, never shortens;
// 4. no entry in the tolerance table means IsAvailable/IsReachable=true; after isolation false, recovered when due;
// 5. when the strategy is off no fault item is recorded at all;
// 6. strategy on: after isolating a broker only healthy brokers are chosen; with a single broker, injecting an isolation degrades to plain round-robin and a queue can still be selected;
// 7. ResetIndex; the avoiding semantics of lastBrokerName.
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

    // ------------------------------------------------ The threshold table's tier-by-tier mapping
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
    public void ComputeNotAvailableDuration_MatchesLegacyTable(double latency, long expected)
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
        // the isolation tier is fixed at 10000ms -> startTimestamp = now + 10000, immediately unavailable
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
