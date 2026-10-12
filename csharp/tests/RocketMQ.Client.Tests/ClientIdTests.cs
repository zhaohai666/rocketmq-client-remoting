// 与 python/tests/test_client_id.py、cpp/tests/test_client_id.cpp、
// rust/src/client/{producer,consumer}.rs 的同名测试同题。
using System.Text.RegularExpressions;
using RocketMQ.Client;
using RocketMQ.Common;
using RocketMQ.Remoting.Protocol;
using Xunit;

namespace RocketMQ.Client.Tests;

public class ClientIdTests
{
    // <IP>@<pid>#<纳秒>
    private static readonly Regex LegacyStyle =
        new Regex(@"^[^@\s]+@" + Environment.ProcessId + @"#\d+$");

    private const string StreamSuffix = "@STREAM";

    private static string Ip => MixAll.CachedIpStr();

    /// <summary>剥掉尾部的 <c>@STREAM</c>：pull / lite 消费者默认开 stream，
    /// 结构断言只关心 `<ip>@<instance>` 那部分。</summary>
    private static string WithoutStream(string clientId)
    {
        return clientId.EndsWith(StreamSuffix, StringComparison.Ordinal)
            ? clientId[..^StreamSuffix.Length]
            : clientId;
    }

    private static (string Ip, string Instance) Split(string clientId)
    {
        int at = clientId.IndexOf('@');
        Assert.True(at > 0 && at + 1 < clientId.Length, "clientId 少了 IP@instanceName 的分隔符: " + clientId);
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
            // 断言只看 clientId，连接失败是预期的
        }
        try
        {
            shutdown();
        }
        catch
        {
            // 半启动状态下 shutdown 不该让测试失败
        }
        return client;
    }

    // ---------------------------------------------------------------- 纯字符串部分

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

    // ---------------------------------------------------------------- 生产者

    [Fact]
    public void ProducerStartStampsALegacyStyleClientId()
    {
        var p = new DefaultMQProducer("PID_clientid_shape") { NamesrvAddr = "127.0.0.1:1" };
        Stamped(p, p.Start, p.Shutdown);
        Assert.Matches(LegacyStyle, p.ClientId);
        (string ip, string instance) = Split(p.ClientId);
        Assert.Equal(Ip, ip);
        // instanceName 就地写回：重启不能再换一个 clientId
        Assert.Equal(instance, p.InstanceName);

        string restarted = p.ClientId;
        Stamped(p, p.Start, p.Shutdown);
        Assert.Equal(restarted, p.ClientId);
    }

    [Fact]
    public void TwoProducersInOneProcessDoNotShareAClientId()
    {
        // 唯一性来自改写后的 instanceName（pid + 纳秒），不再依赖 clientId 尾巴上的序号。
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

    // ---------------------------------------------------------------- 消费者

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

    // ---------------------------------------------------------------- unitName / stream 后缀

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

    /// 各 facade 的默认值：生产者/推送消费者/管理端不开 stream，pull/lite 开。
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

    // ---------------------------------------------------------------- 管理端

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

    // ---------------------------------------------------------------- 失败路径

    [Fact]
    public void FailedStartDoesNotStamp()
    {
        // 改名与拼 clientId 都在地址校验之后：没配 namesrv 就不该留下半个 clientId。
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
