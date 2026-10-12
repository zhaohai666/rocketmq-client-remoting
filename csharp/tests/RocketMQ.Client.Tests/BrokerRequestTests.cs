// broker 反向推送 NOTIFY_CONSUMER_IDS_CHANGED(40) 的离线单测（不需要集群）。
//
// 为什么单独一个文件：这组请求是 broker 沿**已建立的长连接反向**打进客户端的，
// 真机用例注入不了（live 工具只能正向往 broker 发请求），所以能离线锁死的部分必须
// 先锁死。否则「处理器根本没注册」这种错只会在生产上表现为「成员变了却要等 20s
// 定时重平衡」，安静得几乎不可见。
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

        // 缺 consumerGroup 也只用于日志，通知本身照样处理完
        Assert.Null(instance.ProcessNotifyConsumerIdsChanged(NotifyCmd(null), "127.0.0.1:10911"));
        Assert.Equal(2, instance.ConsumerIdsChangedCount);
        Assert.Equal(2, wakeA);

        // 注销的那一组不再被叫醒，但计数继续走
        instance.UnregisterRebalanceWakeup("GID_B");
        instance.ProcessNotifyConsumerIdsChanged(NotifyCmd("GID_A"), "127.0.0.1:10911");
        Assert.Equal(3, instance.ConsumerIdsChangedCount);
        Assert.Equal(3, wakeA);
        Assert.Equal(2, wakeB);

        // 空表不炸：一个消费者都没注册时通知仍要吃完
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
        // shutdown 之后再收到通知（读线程可能还在收尾）也不能抛
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
