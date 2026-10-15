// The consume side (DefaultMQPushConsumer's 309 handler, Consumer.cs) is already covered
// on one front by ConsumerStatsTests; here the originating side
// (DefaultMQAdminExt.ConsumeMessageDirectly) is pinned.
using System;
using System.Globalization;
using RocketMQ.Client;
using RocketMQ.Common;
using RocketMQ.Remoting.Protocol;
using Xunit;

namespace RocketMQ.Client.Tests;

public class ConsumeMessageDirectlyTests
{
    private const string Group = "GID_Directly";
    private const string Client = "127.0.0.1@CID_Directly";
    private const string Topic = "T_Directly";
    private const long CommitLogOffset = 4096;

    private static DefaultMQAdminExt StartedAdmin(MockCluster cluster)
    {
        var admin = new DefaultMQAdminExt("ADMIN_directly");
        admin.SetNamesrvAddr(cluster.NamesrvAddr);
        admin.Start();
        return admin;
    }

    /// <summary>ip:port + commitLogOffset -> an uppercase hex offset msgId.</summary>
    private static string BuildOffsetMsgId(string addr, long offset)
    {
        int colon = addr.LastIndexOf(':');
        string ip = addr[..colon];
        int port = int.Parse(addr[(colon + 1)..], CultureInfo.InvariantCulture);
        return MessageDecoder.CreateMessageId(MessageDecoder.IpAndPortToBytes(ip, port), offset);
    }

    private static byte[] EncodedViewBody(string brokerAddr, string? uniqKey)
    {
        int colon = brokerAddr.LastIndexOf(':');
        var msg = new MessageExt
        {
            Topic = Topic,
            Body = new byte[] { 0x61, 0x62, 0x63 },
            HasBody = true,
            QueueId = 0,
            QueueOffset = 7,
            SysFlag = 0,
            BornTimestamp = 1700000000000L,
            StoreTimestamp = 1700000000001L,
            BornHost = "127.0.0.1",
            BornHostPort = 9876,
            StoreHost = brokerAddr[..colon],
            StoreHostPort = int.Parse(brokerAddr[(colon + 1)..], CultureInfo.InvariantCulture),
            CommitLogOffset = CommitLogOffset,
        };
        if (uniqKey is not null)
        {
            msg.SetUserProperty(MessageConst.PropertyUniqClientMessageIdKeyidx, uniqKey);
        }

        return MessageDecoder.EncodeMessageExt(msg, false);
    }

    private static byte[] ResultBody() => new ConsumeMessageDirectlyResult
    {
        Order = true,
        AutoCommit = false,
        ConsumeResult = "CR_SUCCESS",
        Remark = "consumed directly",
        SpentTimeMills = 42,
    }.Encode();

    // ---------------------------------------------------------------- Wire shape + result decoding

    [Fact]
    public void SendsTheExpectedHeaderFieldsToTheStoreHostAndDecodesTheResult()
    {
        using var cluster = MockCluster.Start(1);
        string brokerAddr = cluster.BrokerAddrs[0];
        cluster.SetReplyBody(RequestCode.ViewMessageById, _ => EncodedViewBody(brokerAddr, null));
        cluster.SetReplyBody(RequestCode.ConsumeMessageDirectly, _ => ResultBody());
        DefaultMQAdminExt admin = StartedAdmin(cluster);
        try
        {
            string msgId = BuildOffsetMsgId(brokerAddr, CommitLogOffset);
            ConsumeMessageDirectlyResult result =
                admin.ConsumeMessageDirectly(Group, Client, Topic, msgId);

            Assert.Equal("CR_SUCCESS", result.ConsumeResult);
            Assert.True(result.Order);
            Assert.False(result.AutoCommit);
            Assert.Equal("consumed directly", result.Remark);
            Assert.Equal(42, result.SpentTimeMills);

            WireRecord hit = Assert.Single(cluster.Records().FindAll(
                r => r.Code == RequestCode.ConsumeMessageDirectly));
            Assert.Equal(Group, hit.Ext["consumerGroup"]);
            Assert.Equal(Client, hit.Ext["clientId"]);
            Assert.Equal(msgId, hit.Ext["msgId"]);
            Assert.Equal(Topic, hit.Ext["topic"]);
        }
        finally
        {
            admin.Shutdown();
        }
    }

    [Fact]
    public void SubstitutesTheOffsetMsgIdWhenTheMessageCarriesTheUniqKey()
    {
        using var cluster = MockCluster.Start(1);
        string brokerAddr = cluster.BrokerAddrs[0];
        cluster.SetReplyBody(RequestCode.ViewMessageById,
            _ => EncodedViewBody(brokerAddr, "UNIQ-TEST-KEY-0001"));
        cluster.SetReplyBody(RequestCode.ConsumeMessageDirectly, _ => ResultBody());
        DefaultMQAdminExt admin = StartedAdmin(cluster);
        try
        {
            // the offset 8192 only serves to make the incoming msgId differ from the message's real OffsetMsgId(4096)
            string inputMsgId = BuildOffsetMsgId(brokerAddr, 8192);
            admin.ConsumeMessageDirectly(Group, Client, Topic, inputMsgId);

            WireRecord hit = Assert.Single(cluster.Records().FindAll(
                r => r.Code == RequestCode.ConsumeMessageDirectly));
            Assert.Equal(BuildOffsetMsgId(brokerAddr, CommitLogOffset), hit.Ext["msgId"]);
            Assert.NotEqual(inputMsgId, hit.Ext["msgId"]);
        }
        finally
        {
            admin.Shutdown();
        }
    }

    [Fact]
    public void KeepsTheCallerMsgIdWhenTheMessageHasNoUniqKey()
    {
        using var cluster = MockCluster.Start(1);
        string brokerAddr = cluster.BrokerAddrs[0];
        cluster.SetReplyBody(RequestCode.ViewMessageById, _ => EncodedViewBody(brokerAddr, null));
        cluster.SetReplyBody(RequestCode.ConsumeMessageDirectly, _ => ResultBody());
        DefaultMQAdminExt admin = StartedAdmin(cluster);
        try
        {
            string inputMsgId = BuildOffsetMsgId(brokerAddr, 8192);
            admin.ConsumeMessageDirectly(Group, Client, Topic, inputMsgId);

            WireRecord hit = Assert.Single(cluster.Records().FindAll(
                r => r.Code == RequestCode.ConsumeMessageDirectly));
            Assert.Equal(inputMsgId, hit.Ext["msgId"]);
        }
        finally
        {
            admin.Shutdown();
        }
    }

    // ---------------------------------------------------------------- Failure branches

    /// <summary>309 replies SUCCESS but the body is empty => MQClientException (an empty body is treated as failure).</summary>
    [Fact]
    public void EmptyResultBodyThrows()
    {
        using var cluster = MockCluster.Start(1);
        string brokerAddr = cluster.BrokerAddrs[0];
        cluster.SetReplyBody(RequestCode.ViewMessageById, _ => EncodedViewBody(brokerAddr, null));
        // 309 carries no response body: the mock replies SUCCESS with no body
        DefaultMQAdminExt admin = StartedAdmin(cluster);
        try
        {
            string msgId = BuildOffsetMsgId(brokerAddr, CommitLogOffset);
            Exception e = Assert.Throws<MQClientException>(() =>
                admin.ConsumeMessageDirectly(Group, Client, Topic, msgId));
            Assert.Contains(Client, e.Message);
            Assert.Equal(1, cluster.CountRequests(RequestCode.ConsumeMessageDirectly));
        }
        finally
        {
            admin.Shutdown();
        }
    }

    /// <summary>Calling it without Start => MQClientException (the network is not touched).</summary>
    [Fact]
    public void RequiresStartBeforeUse()
    {
        var admin = new DefaultMQAdminExt("ADMIN_directly_offline");
        Assert.Throws<MQClientException>(() =>
            admin.ConsumeMessageDirectly(Group, Client, Topic, "deadbeef"));
    }
}
