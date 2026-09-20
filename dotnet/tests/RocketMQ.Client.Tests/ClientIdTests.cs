// clientId 口径与 Java `ClientConfig#buildMQClientId` 对齐的回归测试。
//
// Java 的默认 clientId 是 `<本机 IP>@<instanceName>`，并且 instanceName 还是默认值
// "DEFAULT" 时会在 start() 里被就地改写成 `<pid>#<nanoTime>`。本端口旧口径
// `<instanceName>@<秒级时间戳>@<pid>@<seq>` 靠后缀也不撞号，但少了本机 IP：broker 的消费组
// channel 表以 clientId 为键，运维工具（examineConsumerRunningInfo 等）按 Java 的
// `<ip>@<instanceName>` 形态查不到本客户端。
//
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
    private static readonly Regex JavaStyle =
        new Regex(@"^[^@\s]+@" + Environment.ProcessId + @"#\d+$");

    private static string Ip => MixAll.CachedIpStr();

    private static (string Ip, string Instance) Split(string clientId)
    {
        int at = clientId.IndexOf('@');
        Assert.True(at > 0 && at + 1 < clientId.Length, "clientId 少了 IP@instanceName 的分隔符: " + clientId);
        return (clientId[..at], clientId[(at + 1)..]);
    }

    /// <summary>
    /// 盖 clientId 发生在任何 I/O 之前，但 `127.0.0.1:1` 上没有 broker，start() 可能抛；
    /// 这里把异常吞掉，只关心 clientId 有没有按 Java 的口径盖上。
    /// </summary>
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
        // Java `UtilAll.isBlank(unitName)`：空白等同于没有
        Assert.Equal("10.0.0.1@inst", ClientIds.BuildMqClientId("10.0.0.1", "inst", "   "));
        Assert.Equal("10.0.0.1@inst", ClientIds.BuildMqClientId("10.0.0.1", "inst", null));
    }

    [Fact]
    public void ChangeInstanceNameToPidOnlyTouchesTheDefault()
    {
        Assert.Equal("inst", ClientIds.ChangeInstanceNameToPID("inst"));
        string rewritten = ClientIds.ChangeInstanceNameToPID(MixAll.DefaultInstanceName);
        Assert.StartsWith(Environment.ProcessId + "#", rewritten);
        // Java 是就地覆盖字段，第二次调用不许再换一个名字（否则重启换 clientId）
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
    public void ProducerStartStampsAJavaStyleClientId()
    {
        var p = new DefaultMQProducer("PID_clientid_shape") { NamesrvAddr = "127.0.0.1:1" };
        Stamped(p, p.Start, p.Shutdown);
        Assert.Matches(JavaStyle, p.ClientId);
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

    /// 只有 CLUSTERING 才改写 instanceName（Java 的三个 impl 都是这个条件）。
    [Fact]
    public void PushClusteringRewritesAndBroadcastDoesNot()
    {
        var clustering = new DefaultMQPushConsumer("CID_clientid_clustering");
        clustering.SetNamesrvAddr("127.0.0.1:1");
        clustering.Subscribe("T", "TagA");
        clustering.SetMessageListener(new EmptyListener());
        Stamped(clustering, clustering.Start, clustering.Shutdown);
        Assert.Matches(JavaStyle, clustering.ClientId);

        var broadcast = new DefaultMQPushConsumer("CID_clientid_broadcast")
        {
            MessageModel = MessageModel.Broadcasting,
        };
        broadcast.SetNamesrvAddr("127.0.0.1:1");
        broadcast.Subscribe("T", "TagA");
        broadcast.SetMessageListener(new EmptyListener());
        Stamped(broadcast, broadcast.Start, broadcast.Shutdown);
        Assert.Equal(MixAll.DefaultInstanceName, broadcast.InstanceName);
        // Java 只对 CLUSTERING 改写 instanceName，广播消费者保留 "DEFAULT" —— 本端口每个
        // facade 各建私有 MQClientInstance（不共用），但 clientId 形态要跟 Java 一致。
        Assert.Equal(Ip + "@DEFAULT", broadcast.ClientId);
    }

    [Fact]
    public void PullAndLiteFollowTheSameRule()
    {
        var pull = new DefaultMQPullConsumer("CID_clientid_pull");
        pull.SetNamesrvAddr("127.0.0.1:1");
        Stamped(pull, pull.Start, pull.Shutdown);
        Assert.Matches(JavaStyle, pull.ClientId);

        var lite = new DefaultLitePullConsumer("CID_clientid_lite");
        lite.SetNamesrvAddr("127.0.0.1:1");
        lite.Subscribe("T", "*");
        Stamped(lite, lite.Start, lite.Shutdown);
        Assert.Matches(JavaStyle, lite.ClientId);

        var liteBroadcast = new DefaultLitePullConsumer("CID_clientid_lite_broadcast");
        liteBroadcast.SetNamesrvAddr("127.0.0.1:1");
        liteBroadcast.SetMessageModel(MessageModel.Broadcasting);
        liteBroadcast.Subscribe("T", "*");
        Stamped(liteBroadcast, liteBroadcast.Start, liteBroadcast.Shutdown);
        Assert.Equal(Ip + "@DEFAULT", liteBroadcast.ClientId);
    }

    // ---------------------------------------------------------------- 管理端

    [Fact]
    public void AdminKeepsItsOwnInstanceName()
    {
        // Java 的 admin 也调用 changeInstanceNameToPID，但本端口默认名是 ADMIN 不是 DEFAULT。
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
        Assert.Matches(JavaStyle, admin.ClientId);
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
