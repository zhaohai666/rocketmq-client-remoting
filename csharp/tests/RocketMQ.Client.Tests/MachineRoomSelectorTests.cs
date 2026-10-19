// Offline unit tests: SelectMessageQueueByMachineRoom (this client's contract:
// prefix-match brokerName against the machine room and take the first hit, falling
// back to the first queue when nothing matches; an empty queue list throws), the bounded
// ConsumeExecutor.Shutdown behind awaitTerminationMillisWhenShutdown, and the
// push-consumer config roundtrip.
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

        // prefix match returns the FIRST hit, not the last
        Assert.Equal("idcA-broker-0", sel.Select(mqs, new Message("T", new byte[] { 1 }), "idcA").BrokerName);
    }

    [Fact]
    public void MachineRoom_FallsBackToFirstWhenNoMatch()
    {
        var mqs = Queues("idcA-broker-0", "idcB-broker-0");
        var sel = new SelectMessageQueueByMachineRoom();

        Assert.Equal("idcA-broker-0", sel.Select(mqs, new Message("T", new byte[] { 1 }), "idcZ").BrokerName);
        // a null arg is an empty room id: every brokerName starts with "", so it takes the first
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
        // let the worker actually start and hold the task
        SpinWait.SpinUntil(() => exec.WorkerCount() == 1, 5000);

        var sw = Stopwatch.StartNew();
        exec.Shutdown(true, 200);
        sw.Stop();

        // bounded wait: returns in ~200ms instead of being held by the 30s task; generous Windows timing margin
        Assert.InRange(sw.ElapsedMilliseconds, 150, 3000);
        release.Set(); // release the worker (background thread, reclaimed when the process exits)
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

        exec.Shutdown(true, 0); // <=0: unbounded join (this port's default)
        Assert.Equal(1, Volatile.Read(ref done));
    }

    [Fact]
    public void Consumer_AwaitTerminationPropertyRoundTrips()
    {
        var consumer = new DefaultMQPushConsumer("GID_AwaitTest");
        Assert.Equal(0, consumer.AwaitTerminationMillisWhenShutdown); // default 0

        consumer.SetAwaitTerminationMillisWhenShutdown(1500);
        Assert.Equal(1500, consumer.AwaitTerminationMillisWhenShutdown);
    }
}
