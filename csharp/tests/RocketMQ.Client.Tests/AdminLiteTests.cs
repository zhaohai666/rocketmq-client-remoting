// Wire-level proof of the lite-admin single-broker calls (200074..200079): they all share one
// invocation path that pins the address to the VIP port, decodes the body only when the broker
// sent one (null otherwise), and turns a non-success reply into a broker exception. The header
// fields travel under their exact names, and the primitive ones (maxCount, topK) are always on
// the wire even when the caller never mentioned them.
using System.Text;
using RocketMQ.Client;
using RocketMQ.Remoting.Protocol;
using Xunit;

namespace RocketMQ.Client.Tests;

public class AdminLiteTests
{
    private static DefaultMQAdminExt Started(MockCluster cluster, string tag, bool vipOn = false)
    {
        var admin = new DefaultMQAdminExt("ADMIN_lite_" + tag);
        admin.SetNamesrvAddr(cluster.NamesrvAddr);
        admin.VipChannelEnabled = vipOn;
        admin.Start();
        return admin;
    }

    private static WireRecord Single(MockCluster cluster, int code) =>
        Assert.Single(cluster.Records().FindAll(r => r.Code == code));

    private static List<string> KeysOf(WireRecord record) => new(record.Ext.Keys);

    private static byte[] Body(string json) => Encoding.UTF8.GetBytes(json);

    // ---------------------------------------------------------------- 200074 broker overview

    [Fact]
    public void GetBrokerLiteInfo_SendsABareFrame_AndDecodesTheStoreOverview()
    {
        using var cluster = MockCluster.Start(1);
        DefaultMQAdminExt admin = Started(cluster, "broker_info");
        try
        {
            cluster.SetReplyBody(RequestCode.GetBrokerLiteInfo, req => Body(
                "{\"storeType\":\"rocksdb\",\"maxLmqNum\":4096,\"currentLmqNum\":12,\"cqTableSize\":7}"));

            GetBrokerLiteInfoResponseBody? body = admin.GetBrokerLiteInfo(cluster.BrokerAddrs[0]);

            Assert.NotNull(body);
            Assert.Equal("rocksdb", body!.StoreType);
            Assert.Equal(4096, body.MaxLmqNum);
            Assert.Equal(12, body.CurrentLmqNum);
            Assert.Equal(7, body.CqTableSize);

            WireRecord rec = Single(cluster, RequestCode.GetBrokerLiteInfo);
            Assert.Empty(rec.Ext);
        }
        finally
        {
            admin.Shutdown();
        }
    }

    /// <summary>The shared invocation moves the frame to the fast channel when the switch is
    /// on: the probe bound two ports below the plain address answers it there, and nothing
    /// may land on the plain mock.</summary>
    [Fact]
    public void GetBrokerLiteInfo_TakesTheVipPortWhenTheSwitchIsOn()
    {
        using var cluster = MockCluster.Start(1);
        DefaultMQAdminExt admin = Started(cluster, "broker_info_vip", vipOn: true);
        using FastChannelReply? fast = FastChannelReply.Start(1, "store busy");
        Assert.NotNull(fast);
        try
        {
            MQBrokerException e = Assert.Throws<MQBrokerException>(() =>
                admin.GetBrokerLiteInfo(fast!.PlainAddr));
            Assert.Equal(1, e.ResponseCode);
            Assert.True(fast!.FrameArrived);
            Assert.Equal(0, cluster.CountRequests(RequestCode.GetBrokerLiteInfo));
        }
        finally
        {
            admin.Shutdown();
        }
    }

    [Fact]
    public void GetBrokerLiteInfo_AnAbsentBodyComesBackAsNull_NotAsAnEmptyOverview()
    {
        using var cluster = MockCluster.Start(1);
        DefaultMQAdminExt admin = Started(cluster, "broker_info_empty");
        try
        {
            Assert.Null(admin.GetBrokerLiteInfo(cluster.BrokerAddrs[0]));
        }
        finally
        {
            admin.Shutdown();
        }
    }

    [Fact]
    public void GetBrokerLiteInfo_ABrokerRefusalIsABrokerException()
    {
        using var cluster = MockCluster.Start(1);
        DefaultMQAdminExt admin = Started(cluster, "broker_info_err");
        cluster.SetReplyCode(RequestCode.GetBrokerLiteInfo, 1, "store busy");
        try
        {
            MQBrokerException e = Assert.Throws<MQBrokerException>(() =>
                admin.GetBrokerLiteInfo(cluster.BrokerAddrs[0]));
            Assert.Equal(1, e.ResponseCode);
        }
        finally
        {
            admin.Shutdown();
        }
    }

    // ---------------------------------------------------------------- 200075/200076 topic views

    [Fact]
    public void GetParentTopicInfo_CarriesTheTopicUnderItsOwnName()
    {
        using var cluster = MockCluster.Start(1);
        DefaultMQAdminExt admin = Started(cluster, "parent_info");
        try
        {
            cluster.SetReplyBody(RequestCode.GetParentTopicInfo, req => Body(
                "{\"topic\":\"T_lite\",\"ttl\":72,\"groups\":[\"GID_lite\"],\"lmqNum\":3,\"liteTopicCount\":9}"));

            GetParentTopicInfoResponseBody? body =
                admin.GetParentTopicInfo(cluster.BrokerAddrs[0], "T_lite");

            Assert.NotNull(body);
            Assert.Equal("T_lite", body!.Topic);
            Assert.Equal(72, body.Ttl);
            Assert.Equal(9, body.LiteTopicCount);
            Assert.Equal("GID_lite", Assert.Single(body.Groups));

            WireRecord rec = Single(cluster, RequestCode.GetParentTopicInfo);
            Assert.Equal(new[] { "topic" }, KeysOf(rec));
        }
        finally
        {
            admin.Shutdown();
        }
    }

    [Fact]
    public void GetLiteTopicInfo_OmitsTheLiteTopicKeyWhenTheCallerPassesNull()
    {
        using var cluster = MockCluster.Start(1);
        DefaultMQAdminExt admin = Started(cluster, "lite_topic_info");
        try
        {
            admin.GetLiteTopicInfo(cluster.BrokerAddrs[0], "T_lite", null);

            WireRecord bare = Single(cluster, RequestCode.GetLiteTopicInfo);
            Assert.Equal(new[] { "parentTopic" }, KeysOf(bare));

            admin.GetLiteTopicInfo(cluster.BrokerAddrs[0], "T_lite", "lmq://gA");

            List<WireRecord> frames = cluster.Records().FindAll(r => r.Code == RequestCode.GetLiteTopicInfo);
            Assert.Equal(2, frames.Count);
            Assert.Equal(new[] { "liteTopic", "parentTopic" }, KeysOf(frames[1]));
            Assert.Equal("lmq://gA", frames[1].Ext["liteTopic"]);
        }
        finally
        {
            admin.Shutdown();
        }
    }

    // ---------------------------------------------------------------- 200077 client view

    [Fact]
    public void GetLiteClientInfo_AlwaysCarriesTheDefaultMaxCount_AndOnlySetFields()
    {
        using var cluster = MockCluster.Start(1);
        DefaultMQAdminExt admin = Started(cluster, "lite_client_info");
        try
        {
            cluster.SetReplyBody(RequestCode.GetLiteClientInfo, req => Body(
                "{\"parentTopic\":\"T_lite\",\"group\":\"GID_lite\",\"clientId\":\"c1\","
                + "\"lastAccessTime\":17,\"lastConsumeTime\":19,\"liteTopicCount\":2,"
                + "\"liteTopicSet\":[\"lmq://gA\"]}"));

            GetLiteClientInfoResponseBody? body =
                admin.GetLiteClientInfo(cluster.BrokerAddrs[0], "T_lite", "GID_lite", "c1");

            Assert.NotNull(body);
            Assert.Equal("c1", body!.ClientId);
            Assert.Equal(2, body.LiteTopicCount);
            Assert.Equal(17, body.LastAccessTime);
            Assert.Equal("lmq://gA", Assert.Single(body.LiteTopicSet));

            WireRecord rec = Single(cluster, RequestCode.GetLiteClientInfo);
            Assert.Equal(new[] { "clientId", "group", "maxCount", "parentTopic" }, KeysOf(rec));
            Assert.Equal("1000", rec.Ext["maxCount"]);
        }
        finally
        {
            admin.Shutdown();
        }
    }

    [Fact]
    public void GetLiteClientInfo_WithNoClientIdTheKeyStaysOffTheHeader()
    {
        using var cluster = MockCluster.Start(1);
        DefaultMQAdminExt admin = Started(cluster, "lite_client_no_id");
        try
        {
            admin.GetLiteClientInfo(cluster.BrokerAddrs[0], "T_lite", "GID_lite", null);

            WireRecord rec = Single(cluster, RequestCode.GetLiteClientInfo);
            Assert.Equal(new[] { "group", "maxCount", "parentTopic" }, KeysOf(rec));
        }
        finally
        {
            admin.Shutdown();
        }
    }

    // ---------------------------------------------------------------- 200078/200079 group + dispatch

    [Fact]
    public void GetLiteGroupInfo_AlwaysCarriesTopKEvenAtZero()
    {
        using var cluster = MockCluster.Start(1);
        DefaultMQAdminExt admin = Started(cluster, "lite_group_info");
        try
        {
            cluster.SetReplyBody(RequestCode.GetLiteGroupInfo, req => Body(
                "{\"group\":\"GID_lite\",\"totalLagCount\":5,"
                + "\"lagCountTopK\":[{\"liteTopic\":\"lmq://gA\",\"lagCount\":5,"
                + "\"earliestUnconsumedTimestamp\":-1}]}"));

            GetLiteGroupInfoResponseBody? body =
                admin.GetLiteGroupInfo(cluster.BrokerAddrs[0], "GID_lite", null, 0);

            Assert.NotNull(body);
            Assert.Equal(5, body!.TotalLagCount);
            LiteLagInfo top = Assert.Single(body.LagCountTopK);
            Assert.Equal("lmq://gA", top.LiteTopic);
            Assert.Equal(5, top.LagCount);

            WireRecord rec = Single(cluster, RequestCode.GetLiteGroupInfo);
            Assert.Equal(new[] { "group", "topK" }, KeysOf(rec));
            Assert.Equal("0", rec.Ext["topK"]);
        }
        finally
        {
            admin.Shutdown();
        }
    }

    [Fact]
    public void TriggerLiteDispatch_CarriesGroupAndClientId_WithoutAnyBody()
    {
        using var cluster = MockCluster.Start(1);
        DefaultMQAdminExt admin = Started(cluster, "trigger_dispatch");
        try
        {
            admin.TriggerLiteDispatch(cluster.BrokerAddrs[0], "GID_lite", "c1");

            WireRecord rec = Single(cluster, RequestCode.TriggerLiteDispatch);
            Assert.Equal(new[] { "clientId", "group" }, KeysOf(rec));
            Assert.False(rec.HasBody);
        }
        finally
        {
            admin.Shutdown();
        }
    }

    [Fact]
    public void TriggerLiteDispatch_ABrokerRefusalIsABrokerException()
    {
        using var cluster = MockCluster.Start(1);
        DefaultMQAdminExt admin = Started(cluster, "trigger_dispatch_err");
        cluster.SetReplyCode(RequestCode.TriggerLiteDispatch, 14, "dispatch refused");
        try
        {
            MQBrokerException e = Assert.Throws<MQBrokerException>(() =>
                admin.TriggerLiteDispatch(cluster.BrokerAddrs[0], "GID_lite", "c1"));
            Assert.Equal(14, e.ResponseCode);
        }
        finally
        {
            admin.Shutdown();
        }
    }
}
