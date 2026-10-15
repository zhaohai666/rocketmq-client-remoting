// Offline unit tests for the clientId generation and reuse rules.
using System.Text.RegularExpressions;
using RocketMQ.Client;
using RocketMQ.Common;
using RocketMQ.Remoting.Protocol;
using Xunit;

namespace RocketMQ.Client.Tests;

public class ClientIdTests
{
    // <IP>@<pid>#<nanoseconds>
    private static readonly Regex LegacyStyle =
        new Regex(@"^[^@\s]+@" + Environment.ProcessId + @"#\d+$");

    private const string StreamSuffix = "@STREAM";

    private static string Ip => MixAll.CachedIpStr();

    /// <summary>Strips the trailing <c>@STREAM</c>: pull / lite consumers enable stream by
    /// default, and the structural assertions only care about the `<ip>@<instance>`
    /// part.</summary>
    private static string WithoutStream(string clientId)
    {
        return clientId.EndsWith(StreamSuffix, StringComparison.Ordinal)
            ? clientId[..^StreamSuffix.Length]
            : clientId;
    }

    private static (string Ip, string Instance) Split(string clientId)
    {
        int at = clientId.IndexOf('@');
        Assert.True(at > 0 && at + 1 < clientId.Length, "clientId is missing the IP@instanceName separator: " + clientId);
        return (clientId[..at], clientId[(at + 1)..]);
    }

    private static T Stamped<T>(T client, Action start, Action shutdown)
    {
        try
        {
            start();
        }
        catch
        {
            // the assertion only looks at clientId, a connection failure is expected
        }
        try
        {
            shutdown();
        }
        catch
        {
            // shutdown in a half-started state must not fail the test
        }
        return client;
    }

    // ---------------------------------------------------------------- Pure string part

    [Fact]
    public void BuildMqClientIdIsIpFirst()
    {
        Assert.Equal("10.0.0.1@inst", ClientIds.BuildMqClientId("10.0.0.1", "inst"));
    }

    [Fact]
    public void UnitNameIsAnOptionalSuffix()
    {
        Assert.Equal("10.0.0.1@inst@unit-a", ClientIds.BuildMqClientId("10.0.0.1", "inst", "unit-a"));
        Assert.Equal("10.0.0.1@inst", ClientIds.BuildMqClientId("10.0.0.1", "inst", "   "));
        Assert.Equal("10.0.0.1@inst", ClientIds.BuildMqClientId("10.0.0.1", "inst", null));
    }

    [Fact]
    public void ChangeInstanceNameToPidOnlyTouchesTheDefault()
    {
        Assert.Equal("inst", ClientIds.ChangeInstanceNameToPID("inst"));
        string rewritten = ClientIds.ChangeInstanceNameToPID(MixAll.DefaultInstanceName);
        Assert.StartsWith(Environment.ProcessId + "#", rewritten);
        Assert.Equal(rewritten, ClientIds.ChangeInstanceNameToPID(rewritten));
    }

    [Fact]
    public void CachedIpStrIsStable()
    {
        Assert.Equal(Ip, MixAll.CachedIpStr());
        Assert.False(string.IsNullOrEmpty(Ip));
    }

    [Fact]
    public void BuildPrefixesTheLocalIp()
    {
        (string ip, string instance) = Split(ClientIds.Build("inst"));
        Assert.Equal(Ip, ip);
        Assert.Equal("inst", instance);
    }

    // ---------------------------------------------------------------- Producers

    [Fact]
    public void ProducerStartStampsALegacyStyleClientId()
    {
        var p = new DefaultMQProducer("PID_clientid_shape") { NamesrvAddr = "127.0.0.1:1" };
        Stamped(p, p.Start, p.Shutdown);
        Assert.Matches(LegacyStyle, p.ClientId);
        (string ip, string instance) = Split(p.ClientId);
        Assert.Equal(Ip, ip);
        // instanceName is written back in place: a restart must not switch to another clientId
        Assert.Equal(instance, p.InstanceName);

        string restarted = p.ClientId;
        Stamped(p, p.Start, p.Shutdown);
        Assert.Equal(restarted, p.ClientId);
    }

    [Fact]
    public void TwoProducersInOneProcessDoNotShareAClientId()
    {
        // Uniqueness comes from the rewritten instanceName (pid + nanoseconds), no longer relying on the sequence number at the tail of the clientId.
        var a = new DefaultMQProducer("PID_clientid_a") { NamesrvAddr = "127.0.0.1:1" };
        var b = new DefaultMQProducer("PID_clientid_b") { NamesrvAddr = "127.0.0.1:1" };
        Stamped(a, a.Start, a.Shutdown);
        Stamped(b, b.Start, b.Shutdown);
        Assert.NotEqual(a.ClientId, b.ClientId);
    }

    [Fact]
    public void ExplicitInstanceNameIsKeptVerbatim()
    {
        var p = new DefaultMQProducer("PID_clientid_named")
        {
            InstanceName = "clientid-parity-fixed",
            NamesrvAddr = "127.0.0.1:1",
        };
        Stamped(p, p.Start, p.Shutdown);
        Assert.Equal(Ip + "@clientid-parity-fixed", p.ClientId);
        Assert.Equal("clientid-parity-fixed", p.InstanceName);
    }

    // ---------------------------------------------------------------- Consumers

    [Fact]
    public void PushClusteringRewritesAndBroadcastDoesNot()
    {
        var clustering = new DefaultMQPushConsumer("CID_clientid_clustering");
        clustering.SetNamesrvAddr("127.0.0.1:1");
        clustering.Subscribe("T", "TagA");
        clustering.SetMessageListener(new EmptyListener());
        Stamped(clustering, clustering.Start, clustering.Shutdown);
        Assert.Matches(LegacyStyle, clustering.ClientId);

        var broadcast = new DefaultMQPushConsumer("CID_clientid_broadcast")
        {
            MessageModel = MessageModel.Broadcasting,
        };
        broadcast.SetNamesrvAddr("127.0.0.1:1");
        broadcast.Subscribe("T", "TagA");
        broadcast.SetMessageListener(new EmptyListener());
        Stamped(broadcast, broadcast.Start, broadcast.Shutdown);
        Assert.Equal(MixAll.DefaultInstanceName, broadcast.InstanceName);
        Assert.Equal(Ip + "@DEFAULT", broadcast.ClientId);
    }

    [Fact]
    public void PullAndLiteFollowTheSameRule()
    {
        var pull = new DefaultMQPullConsumer("CID_clientid_pull");
        pull.SetNamesrvAddr("127.0.0.1:1");
        Stamped(pull, pull.Start, pull.Shutdown);
        Assert.Matches(LegacyStyle, WithoutStream(pull.ClientId));
        Assert.EndsWith(StreamSuffix, pull.ClientId);

        var lite = new DefaultLitePullConsumer("CID_clientid_lite");
        lite.SetNamesrvAddr("127.0.0.1:1");
        lite.Subscribe("T", "*");
        Stamped(lite, lite.Start, lite.Shutdown);
        Assert.Matches(LegacyStyle, WithoutStream(lite.ClientId));
        Assert.EndsWith(StreamSuffix, lite.ClientId);

        var liteBroadcast = new DefaultLitePullConsumer("CID_clientid_lite_broadcast");
        liteBroadcast.SetNamesrvAddr("127.0.0.1:1");
        liteBroadcast.SetMessageModel(MessageModel.Broadcasting);
        liteBroadcast.Subscribe("T", "*");
        Stamped(liteBroadcast, liteBroadcast.Start, liteBroadcast.Shutdown);
        Assert.Equal(Ip + "@DEFAULT" + StreamSuffix, liteBroadcast.ClientId);
    }

    [Fact]
    public void StreamSuffixIsOptOutable()
    {
        var lite = new DefaultLitePullConsumer("CID_clientid_lite_nostream")
        {
            EnableStreamRequestType = false,
        };
        lite.SetNamesrvAddr("127.0.0.1:1");
        lite.Subscribe("T", "*");
        Stamped(lite, lite.Start, lite.Shutdown);
        Assert.DoesNotContain(StreamSuffix, lite.ClientId);
        Assert.Matches(LegacyStyle, lite.ClientId);
    }

    // ---------------------------------------------------------------- unitName / stream suffix

    [Fact]
    public void StreamSuffixComesAfterUnitName()
    {
        Assert.Equal("10.0.0.1@inst@unit-a@STREAM",
            ClientIds.BuildMqClientId("10.0.0.1", "inst", "unit-a", true));
        Assert.Equal("10.0.0.1@inst@STREAM",
            ClientIds.BuildMqClientId("10.0.0.1", "inst", null, true));
        Assert.Equal("10.0.0.1@inst",
            ClientIds.BuildMqClientId("10.0.0.1", "inst", "  ", false));
    }

    /// The defaults per facade: producers / push consumers / the admin side do not enable
    /// stream, pull/lite do.
    [Fact]
    public void FacadeStreamDefaultsMatchLegacy()
    {
        Assert.False(new DefaultMQProducer("PID_stream_default").EnableStreamRequestType);
        Assert.False(new DefaultMQPushConsumer("CID_stream_default").EnableStreamRequestType);
        Assert.False(new DefaultMQAdminExt().EnableStreamRequestType);
        Assert.True(new DefaultMQPullConsumer("CID_stream_default").EnableStreamRequestType);
        Assert.True(new DefaultLitePullConsumer("CID_stream_default").EnableStreamRequestType);
    }

    [Fact]
    public void UnitNameReachesProducerClientId()
    {
        var p = new DefaultMQProducer("PID_unitname_clientid")
        {
            UnitName = "unit-east",
            InstanceName = "unitname-fixed",
            NamesrvAddr = "127.0.0.1:1",
        };
        Stamped(p, p.Start, p.Shutdown);
        Assert.Equal(Ip + "@unitname-fixed@unit-east", p.ClientId);
    }

    [Fact]
    public void UnitModeDefaultsToFalseAndIsSettable()
    {
        var producer = new DefaultMQProducer("PID_unitmode");
        Assert.False(producer.UnitMode);
        producer.UnitMode = true;
        Assert.True(producer.UnitMode);

        var push = new DefaultMQPushConsumer("CID_unitmode");
        Assert.False(push.UnitMode);
        var lite = new DefaultLitePullConsumer("CID_unitmode");
        Assert.False(lite.UnitMode);
    }

    // ---------------------------------------------------------------- Admin side

    [Fact]
    public void AdminKeepsItsOwnInstanceName()
    {
        var admin = new DefaultMQAdminExt();
        admin.SetNamesrvAddr("127.0.0.1:1");
        Stamped(admin, admin.Start, admin.Shutdown);
        (string ip, string instance) = Split(admin.ClientId);
        Assert.Equal(Ip, ip);
        Assert.Equal("ADMIN", instance);
    }

    [Fact]
    public void AdminRewritesADefaultInstanceName()
    {
        var admin = new DefaultMQAdminExt();
        admin.SetInstanceName(MixAll.DefaultInstanceName);
        admin.SetNamesrvAddr("127.0.0.1:1");
        Stamped(admin, admin.Start, admin.Shutdown);
        Assert.Matches(LegacyStyle, admin.ClientId);
    }

    // ---------------------------------------------------------------- Failure paths

    [Fact]
    public void FailedStartDoesNotStamp()
    {
        // Both the renaming and the clientId assembly come after the address validation: with no namesrv configured it must not leave half a clientId behind.
        var p = new DefaultMQProducer("PID_clientid_dead_group");
        Assert.Throws<MQClientException>(p.Start);
        Assert.Equal(string.Empty, p.ClientId);
        Assert.Equal(MixAll.DefaultInstanceName, p.InstanceName);
    }

    private sealed class EmptyListener : IMessageListenerConcurrently
    {
        public bool Orderly() => false;

        public ConsumeConcurrentlyStatus ConsumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext ctx)
            => ConsumeConcurrentlyStatus.ConsumeSuccess;
    }
}
