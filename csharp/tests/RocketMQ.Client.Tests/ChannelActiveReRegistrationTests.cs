// What the client instance does when the transport reports "a connection to this address has
// just been established": re-send every registered consumer group's heartbeat to that broker
// and wake rebalance. The broker drops a consumer group when the last channel of that group
// closes, so a reconnect without this reaction leaves pulls answering SUBSCRIPTION_NOT_EXIST
// until the next periodic heartbeat round.
//
// The transport side is covered in TransportContractTests; here the event is delivered
// directly so the instance's reaction can be measured without a broker.
using System.Reflection;
using RocketMQ.Client;
using Xunit;

namespace RocketMQ.Client.Tests;

public class ChannelActiveReRegistrationTests
{
    private const string BrokerAddr = "127.0.0.1:10911";
    private const string BrokerSlaveAddr = "127.0.0.1:10912";
    private const string NameServerAddr = "127.0.0.1:9876";

    private static MQClientInstance InstanceWithBrokers()
    {
        var instance = new MQClientInstance("channel-active-test", new List<string> { NameServerAddr });
        FieldInfo field = typeof(MQClientInstance).GetField("_brokerAddrTable",
            BindingFlags.NonPublic | BindingFlags.Instance)!;
        var table = (Dictionary<string, SortedDictionary<long, string>>)field.GetValue(instance)!;
        lock (table)
        {
            table["broker-a"] = new SortedDictionary<long, string>
            {
                { 0, BrokerAddr },
                { 1, BrokerSlaveAddr },
            };
        }

        return instance;
    }

    private static void Fire(MQClientInstance instance, string addr)
    {
        MethodInfo method = typeof(MQClientInstance).GetMethod("OnChannelActive",
            BindingFlags.NonPublic | BindingFlags.Instance)!;
        method.Invoke(instance, new object[] { addr });
    }

    [Fact]
    public void ANewConnectionToAKnownBrokerReRegistersAndWakesRebalance()
    {
        using MQClientInstance instance = InstanceWithBrokers();
        var beats = new List<string>();
        using var beatArrived = new SemaphoreSlim(0);
        using var rebalanced = new ManualResetEventSlim(false);
        instance.RegisterConsumerHeartbeat("GID_A", addr =>
        {
            lock (beats)
            {
                beats.Add(addr);
            }

            beatArrived.Release();
            return 1;
        });
        instance.RegisterRebalanceWakeup("GID_A", rebalanced.Set);

        Fire(instance, BrokerAddr);

        Assert.True(beatArrived.Wait(TimeSpan.FromSeconds(5)),
            "a reconnect has to re-register the group with that broker");
        Assert.True(rebalanced.Wait(TimeSpan.FromSeconds(5)), "rebalance has to wake on a reconnect");
        lock (beats)
        {
            Assert.Equal(new[] { BrokerAddr }, beats.ToArray());
        }

        // The slave address is part of the same broker table, so it counts as known too.
        Fire(instance, BrokerSlaveAddr);
        Assert.True(beatArrived.Wait(TimeSpan.FromSeconds(5)));
        lock (beats)
        {
            Assert.Equal(new[] { BrokerAddr, BrokerSlaveAddr }, beats.ToArray());
        }
    }

    [Fact]
    public void AnAddressThatIsNotAKnownBrokerReRegistersNothing()
    {
        using MQClientInstance instance = InstanceWithBrokers();
        int heartbeats = 0;
        int rebalances = 0;
        instance.RegisterConsumerHeartbeat("GID_A", _ =>
        {
            Interlocked.Increment(ref heartbeats);
            return 1;
        });
        instance.RegisterRebalanceWakeup("GID_A", () => Interlocked.Increment(ref rebalances));

        Fire(instance, NameServerAddr);
        Thread.Sleep(300);

        Assert.Equal(0, heartbeats);
        Assert.Equal(0, rebalances);
    }

    [Fact]
    public void TheSwitchOffSuppressesTheReRegistration()
    {
        using MQClientInstance instance = InstanceWithBrokers();
        instance.EnableHeartbeatChannelEventListener = false;
        int heartbeats = 0;
        int rebalances = 0;
        instance.RegisterConsumerHeartbeat("GID_A", _ =>
        {
            Interlocked.Increment(ref heartbeats);
            return 1;
        });
        instance.RegisterRebalanceWakeup("GID_A", () => Interlocked.Increment(ref rebalances));

        Fire(instance, BrokerAddr);
        Thread.Sleep(300);

        Assert.Equal(0, heartbeats);
        Assert.Equal(0, rebalances);
    }

    [Fact]
    public void AFailingGroupDoesNotStopTheOthersOrTheRebalance()
    {
        using MQClientInstance instance = InstanceWithBrokers();
        int second = 0;
        using var rebalanced = new ManualResetEventSlim(false);
        instance.RegisterConsumerHeartbeat("GID_BROKEN", _ =>
            throw new InvalidOperationException("no route to that broker"));
        instance.RegisterConsumerHeartbeat("GID_OK", _ =>
        {
            Interlocked.Increment(ref second);
            return 1;
        });
        instance.RegisterRebalanceWakeup("GID_OK", rebalanced.Set);

        Fire(instance, BrokerAddr);

        Assert.True(rebalanced.Wait(TimeSpan.FromSeconds(5)),
            "one failing group must not skip the rebalance for the rest");
        Assert.Equal(1, Volatile.Read(ref second));
    }
}
