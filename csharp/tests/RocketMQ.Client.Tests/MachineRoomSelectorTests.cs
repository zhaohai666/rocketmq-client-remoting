// 离线单测：SelectMessageQueueByMachineRoom（Java selector/SelectMessageQueueByMachineRoom
// 的空壳在本仓库统一落地为「brokerName 前缀匹配 + 无匹配退回第一个」，口径同
// python producer.py / nodeJs producer.ts）、ConsumeExecutor 的有界 Shutdown
// （Java DefaultMQPushConsumer#awaitTerminationMillisWhenShutdown 的执行侧）与
// DefaultMQPushConsumer 的配置项往返。
using RocketMQ.Client;
using RocketMQ.Common;
using System.Diagnostics;

using Xunit;

namespace RocketMQ.Client.Tests;

public class MachineRoomSelectorTests
{
    private static List<MessageQueue> Queues(params string[] brokerNames)
    {
        var list = new List<MessageQueue>();
        for (int i = 0; i < brokerNames.Length; i++)
        {
            list.Add(new MessageQueue("T", brokerNames[i], i));
        }
        return list;
    }

    [Fact]
    public void MachineRoom_PicksFirstQueueWhoseBrokerStartsWithRoom()
    {
        var mqs = Queues("idcA-broker-0", "idcB-broker-0", "idcA-broker-1");
        var sel = new SelectMessageQueueByMachineRoom();

        MessageQueue picked = sel.Select(mqs, new Message("T", new byte[] { 1 }), "idcB");
        Assert.Equal("idcB-broker-0", picked.BrokerName);

        // 前缀匹配取第一个命中，不是最后一个
        Assert.Equal("idcA-broker-0", sel.Select(mqs, new Message("T", new byte[] { 1 }), "idcA").BrokerName);
    }

    [Fact]
    public void MachineRoom_FallsBackToFirstWhenNoMatch()
    {
        var mqs = Queues("idcA-broker-0", "idcB-broker-0");
        var sel = new SelectMessageQueueByMachineRoom();

        Assert.Equal("idcA-broker-0", sel.Select(mqs, new Message("T", new byte[] { 1 }), "idcZ").BrokerName);
        // arg 为 null 视同空机房号：任何 brokerName 都以 "" 开头，等价于取第一个
        Assert.Equal("idcA-broker-0", sel.Select(mqs, new Message("T", new byte[] { 1 }), null!).BrokerName);
    }

    [Fact]
    public void MachineRoom_EmptyListThrows()
    {
        var sel = new SelectMessageQueueByMachineRoom();
        Assert.Throws<MQClientException>(() => sel.Select(new List<MessageQueue>(), new Message("T", new byte[] { 1 }), "idcA"));
    }

    [Fact]
    public void Random_AlwaysReturnsAMember()
    {
        var mqs = Queues("b0", "b1", "b2");
        var sel = new SelectMessageQueueByRandom();
        for (int i = 0; i < 50; i++)
        {
            Assert.Contains(sel.Select(mqs, new Message("T", new byte[] { 1 }), null!), mqs);
        }
        Assert.Throws<MQClientException>(() => sel.Select(new List<MessageQueue>(), new Message("T", new byte[] { 1 }), null!));
    }

    [Fact]
    public void ConsumeExecutor_BoundedShutdownReturnsAtDeadline()
    {
        var exec = new ConsumeExecutor(1, 1, 60.0, "rmq-await-test");
        var release = new ManualResetEventSlim(false);
        exec.Submit(() => release.Wait(TimeSpan.FromSeconds(30)));
        // 让 worker 真正跑起来并占住任务
        SpinWait.SpinUntil(() => exec.WorkerCount() == 1, 5000);

        var sw = Stopwatch.StartNew();
        exec.Shutdown(true, 200);
        sw.Stop();

        // 有界等待：~200ms 就返回，不被 30s 的任务拖住；留足 Windows 计时余量
        Assert.InRange(sw.ElapsedMilliseconds, 150, 3000);
        release.Set(); // 放行 worker（后台线程，测试结束进程回收）
        exec.Dispose();
    }

    [Fact]
    public void ConsumeExecutor_UnboundedShutdownJoinsRunningTask()
    {
        var exec = new ConsumeExecutor(1, 1, 60.0, "rmq-join-test");
        int done = 0;
        exec.Submit(() =>
        {
            Thread.Sleep(300);
            Interlocked.Increment(ref done);
        });
        SpinWait.SpinUntil(() => Volatile.Read(ref done) == 1 || exec.QueuedCount() > 0, 5000);

        exec.Shutdown(true, 0); // <=0：无限 join（默认口径）
        Assert.Equal(1, Volatile.Read(ref done));
    }

    [Fact]
    public void Consumer_AwaitTerminationPropertyRoundTrips()
    {
        var consumer = new DefaultMQPushConsumer("GID_AwaitTest");
        Assert.Equal(0, consumer.AwaitTerminationMillisWhenShutdown); // Java 默认 0

        consumer.SetAwaitTerminationMillisWhenShutdown(1500);
        Assert.Equal(1500, consumer.AwaitTerminationMillisWhenShutdown);
    }
}
