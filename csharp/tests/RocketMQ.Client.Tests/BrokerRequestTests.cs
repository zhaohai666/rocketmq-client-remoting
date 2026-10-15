// Offline unit tests for the broker reverse-pushing NOTIFY_CONSUMER_IDS_CHANGED(40) (no cluster needed).
//
// Why a separate file: these requests are pushed by the broker into the client in the
// **reverse direction over an already established long connection**, and a real-machine
// case cannot inject them (the live tool can only send requests to the broker), so
// whatever can be pinned offline must be pinned first. Otherwise a mistake like "the
// handler was never registered" only shows up in production as "the membership changed
// but it waits 20s for the periodic rebalance", which is almost invisible.
using System;
using System.Collections.Generic;
using RocketMQ.Remoting.Protocol;
using Xunit;

namespace RocketMQ.Client.Tests;

public class BrokerRequestTests
{
    private static RemotingCommand NotifyCmd(string? group)
    {
        var cmd = new RemotingCommand { Code = RequestCode.NotifyConsumerIdsChanged };
        if (group is not null) cmd.ExtFields["consumerGroup"] = group;
        return cmd;
    }

    [Fact]
    public void NotifyConsumerIdsChanged_WakesEveryRegisteredConsumer()
    {
        var instance = new MQClientInstance("dotnet-unit-40@1", new List<string> { "127.0.0.1:9876" });
        int wakeA = 0;
        int wakeB = 0;
        instance.RegisterRebalanceWakeup("GID_A", () => wakeA++);
        instance.RegisterRebalanceWakeup("GID_B", () => wakeB++);

        Assert.Null(instance.ProcessNotifyConsumerIdsChanged(NotifyCmd("GID_A"), "127.0.0.1:10911"));
        Assert.Equal(1, instance.ConsumerIdsChangedCount);
        Assert.Equal(1, wakeA);
        Assert.Equal(1, wakeB);

        // A missing consumerGroup is only used for logging, the notification is still fully processed
        Assert.Null(instance.ProcessNotifyConsumerIdsChanged(NotifyCmd(null), "127.0.0.1:10911"));
        Assert.Equal(2, instance.ConsumerIdsChangedCount);
        Assert.Equal(2, wakeA);

        // A group that has unregistered is no longer woken, but the counter keeps running
        instance.UnregisterRebalanceWakeup("GID_B");
        instance.ProcessNotifyConsumerIdsChanged(NotifyCmd("GID_A"), "127.0.0.1:10911");
        Assert.Equal(3, instance.ConsumerIdsChangedCount);
        Assert.Equal(3, wakeA);
        Assert.Equal(2, wakeB);

        // An empty table must not blow up: the notification must still be consumed when not a single consumer registered
        instance.UnregisterRebalanceWakeup("GID_A");
        instance.RebalanceImmediately();
        Assert.Equal(3, instance.ConsumerIdsChangedCount);
        instance.Shutdown();
    }

    [Fact]
    public void Shutdown_ClearsWakeups()
    {
        var instance = new MQClientInstance("dotnet-unit-40@2", new List<string> { "127.0.0.1:9876" });
        int woken = 0;
        instance.RegisterRebalanceWakeup("GID_A", () => woken++);
        instance.Shutdown();
        instance.RebalanceImmediately();
        Assert.Equal(0, woken);
        // Getting a notification after shutdown (the reader thread may still be winding up) must not throw
        instance.ProcessNotifyConsumerIdsChanged(NotifyCmd("GID_A"), "127.0.0.1:10911");
        Assert.Equal(1, instance.ConsumerIdsChangedCount);
    }

    [Fact]
    public void RebalanceImmediately_SwallowsConsumerExceptions()
    {
        var instance = new MQClientInstance("dotnet-unit-40@3", new List<string> { "127.0.0.1:9876" });
        int second = 0;
        instance.RegisterRebalanceWakeup("GID_A", () => throw new InvalidOperationException("boom"));
        instance.RegisterRebalanceWakeup("GID_B", () => second++);
        instance.RebalanceImmediately();
        Assert.Equal(1, second);
        instance.Shutdown();
    }
}
