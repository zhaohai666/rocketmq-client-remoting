// Wire-level proof of the remaining single-target admin RPCs: broker container lifecycle
// (902/903), cold data flow control (2001/2002/2003), commit-log read-ahead (2004), pop record
// export (200056), RocksDB introspection (354/355), the name-server order-conf read-modify-write
// (100/101), and the list forms of the topic/subscription-group config writes (18/225).
//
// The claim of every case here is read back off the frame the loopback broker recorded: which
// ext keys ride, whether a body exists and what it says, and which exception shape a non-success
// reply turns into. Where the rule is "this call must not go through the VIP port", the control
// flips the admin's VIP switch on -- the mock only listens on the normal port, so a call that
// still succeeds cannot have moved to the fast channel.
using System.Text;
using RocketMQ.Client;
using RocketMQ.Common;
using RocketMQ.Remoting;
using RocketMQ.Remoting.Protocol;
using Xunit;
using PropertyMap = System.Collections.Generic.SortedDictionary<string, string>;

namespace RocketMQ.Client.Tests;

public class AdminLongTailTests
{
    private static DefaultMQAdminExt Started(MockCluster cluster, string tag, bool vipOn = false)
    {
        var admin = new DefaultMQAdminExt("ADMIN_tail_" + tag);
        admin.SetNamesrvAddr(cluster.NamesrvAddr);
        admin.VipChannelEnabled = vipOn;
        admin.Start();
        return admin;
    }

    private static WireRecord Single(MockCluster cluster, int code) =>
        Assert.Single(cluster.Records().FindAll(r => r.Code == code));

    private static string BodyOf(WireRecord record) => Encoding.UTF8.GetString(record.Body);

    private static List<string> KeysOf(WireRecord record) => new(record.Ext.Keys);

    // ---------------------------------------------------------------- container 902/903

    [Fact]
    public void AddBrokerToContainer_SendsOnlyTheConfigPathHeader_AndNeverTakesTheVipPort()
    {
        using var cluster = MockCluster.Start(1);
        // The fast channel has no listener in the mock: a success with the switch on proves
        // the request stayed on the normal port.
        DefaultMQAdminExt admin = Started(cluster, "add_broker", vipOn: true);
        try
        {
            admin.AddBrokerToContainer(cluster.BrokerAddrs[0], "10.0.0.7:10911");

            WireRecord rec = Single(cluster, RequestCode.AddBroker);
            Assert.Equal(new[] { "configPath" }, KeysOf(rec));
            Assert.Equal("10.0.0.7:10911", rec.Ext["configPath"]);
            Assert.False(rec.HasBody);
        }
        finally
        {
            admin.Shutdown();
        }
    }

    [Fact]
    public void AddBrokerToContainer_ABrokerRefusalComesBackAsABrokerException()
    {
        using var cluster = MockCluster.Start(1);
        DefaultMQAdminExt admin = Started(cluster, "add_broker_err");
        cluster.SetReplyCode(RequestCode.AddBroker, 1, "container full");
        try
        {
            MQBrokerException e = Assert.Throws<MQBrokerException>(() =>
                admin.AddBrokerToContainer(cluster.BrokerAddrs[0], "10.0.0.7:10911"));
            Assert.Equal(1, e.ResponseCode);
            Assert.Equal("container full", e.ResponseMessage);
        }
        finally
        {
            admin.Shutdown();
        }
    }

    [Fact]
    public void RemoveBrokerFromContainer_CarriesClusterNameBrokerNameAndBrokerId()
    {
        using var cluster = MockCluster.Start(1);
        DefaultMQAdminExt admin = Started(cluster, "remove_broker");
        try
        {
            admin.RemoveBrokerFromContainer(cluster.BrokerAddrs[0], "MockCluster", "broker-a", 3);

            WireRecord rec = Single(cluster, RequestCode.RemoveBroker);
            Assert.Equal(new[] { "brokerClusterName", "brokerId", "brokerName" }, KeysOf(rec));
            Assert.Equal("MockCluster", rec.Ext["brokerClusterName"]);
            Assert.Equal("broker-a", rec.Ext["brokerName"]);
            Assert.Equal("3", rec.Ext["brokerId"]);
            Assert.False(rec.HasBody);
        }
        finally
        {
            admin.Shutdown();
        }
    }

    // ---------------------------------------------------------------- cold data 2001/2002/2003

    [Fact]
    public void UpdateColdDataFlowCtrGroupConfig_SendsThePropertiesTextThroughTheVipPort()
    {
        using var cluster = MockCluster.Start(1);
        DefaultMQAdminExt admin = Started(cluster, "cold_update");
        try
        {
            var props = new PropertyMap { ["consumerGroup"] = "GID_cold", ["threshold"] = "1024" };
            admin.UpdateColdDataFlowCtrGroupConfig(cluster.BrokerAddrs[0], props);

            WireRecord rec = Single(cluster, RequestCode.UpdateColdDataFlowCtrConfig);
            Assert.True(rec.HasBody);
            Assert.Equal("consumerGroup=GID_cold\nthreshold=1024\n", BodyOf(rec));
            Assert.Empty(rec.Ext);
        }
        finally
        {
            admin.Shutdown();
        }
    }

    /// <summary>The fast-channel proof is positive: the test itself listens two ports below
    /// the plain address and the refusal it sends back is the proof the update really used
    /// the fast channel (nothing may land on the plain mock).</summary>
    [Fact]
    public void UpdateColdDataFlowCtrGroupConfig_MovesToTheFastChannelWhenTheSwitchIsOn()
    {
        using var cluster = MockCluster.Start(1);
        DefaultMQAdminExt admin = Started(cluster, "cold_update_vip", vipOn: true);
        using FastChannelReply? fast = FastChannelReply.Start(1, "cold data busy");
        Assert.NotNull(fast);
        try
        {
            var props = new PropertyMap { ["consumerGroup"] = "GID_cold" };
            MQBrokerException e = Assert.Throws<MQBrokerException>(() =>
                admin.UpdateColdDataFlowCtrGroupConfig(fast!.PlainAddr, props));
            Assert.Equal(1, e.ResponseCode);
            Assert.True(fast!.FrameArrived);
            Assert.Equal(0, cluster.CountRequests(RequestCode.UpdateColdDataFlowCtrConfig));
        }
        finally
        {
            admin.Shutdown();
        }
    }

    [Fact]
    public void UpdateColdDataFlowCtrGroupConfig_WithNothingToWriteSendsNothingAtAll()
    {
        using var cluster = MockCluster.Start(1);
        DefaultMQAdminExt admin = Started(cluster, "cold_update_empty");
        try
        {
            admin.UpdateColdDataFlowCtrGroupConfig(cluster.BrokerAddrs[0], new PropertyMap());
            Assert.Equal(0, cluster.CountRequests(RequestCode.UpdateColdDataFlowCtrConfig));
        }
        finally
        {
            admin.Shutdown();
        }
    }

    [Fact]
    public void UpdateColdDataFlowCtrGroupConfig_ABrokerRefusalIsABrokerException()
    {
        using var cluster = MockCluster.Start(1);
        DefaultMQAdminExt admin = Started(cluster, "cold_update_err");
        cluster.SetReplyCode(RequestCode.UpdateColdDataFlowCtrConfig, 14, "flow ctr busy");
        try
        {
            var props = new PropertyMap { ["consumerGroup"] = "GID_cold" };
            MQBrokerException e = Assert.Throws<MQBrokerException>(() =>
                admin.UpdateColdDataFlowCtrGroupConfig(cluster.BrokerAddrs[0], props));
            Assert.Equal(14, e.ResponseCode);
        }
        finally
        {
            admin.Shutdown();
        }
    }

    [Fact]
    public void RemoveColdDataFlowCtrGroupConfig_SendsTheGroupAsAPlainStringBody()
    {
        using var cluster = MockCluster.Start(1);
        DefaultMQAdminExt admin = Started(cluster, "cold_remove");
        try
        {
            admin.RemoveColdDataFlowCtrGroupConfig(cluster.BrokerAddrs[0], "GID_cold");

            WireRecord rec = Single(cluster, RequestCode.RemoveColdDataFlowCtrConfig);
            Assert.Equal("GID_cold", BodyOf(rec));
            Assert.Empty(rec.Ext);
        }
        finally
        {
            admin.Shutdown();
        }
    }

    [Fact]
    public void RemoveColdDataFlowCtrGroupConfig_WithNoGroupSendsNothing()
    {
        using var cluster = MockCluster.Start(1);
        DefaultMQAdminExt admin = Started(cluster, "cold_remove_empty");
        try
        {
            admin.RemoveColdDataFlowCtrGroupConfig(cluster.BrokerAddrs[0], "");
            admin.RemoveColdDataFlowCtrGroupConfig(cluster.BrokerAddrs[0], null!);
            Assert.Equal(0, cluster.CountRequests(RequestCode.RemoveColdDataFlowCtrConfig));
        }
        finally
        {
            admin.Shutdown();
        }
    }

    [Fact]
    public void GetColdDataFlowCtrInfo_ReadsTheRawBody_AndStaysOnTheNormalPort()
    {
        using var cluster = MockCluster.Start(1);
        // The switch is on: a success proves the read did not move to the fast channel.
        DefaultMQAdminExt admin = Started(cluster, "cold_get", vipOn: true);
        try
        {
            cluster.SetReplyBody(RequestCode.GetColdDataFlowCtrInfo,
                req => Encoding.UTF8.GetBytes("{\"GID_cold\":1024}"));

            Assert.Equal("{\"GID_cold\":1024}", admin.GetColdDataFlowCtrInfo(cluster.BrokerAddrs[0]));

            WireRecord rec = Single(cluster, RequestCode.GetColdDataFlowCtrInfo);
            Assert.Empty(rec.Ext);
            Assert.False(rec.HasBody);
        }
        finally
        {
            admin.Shutdown();
        }
    }

    [Fact]
    public void GetColdDataFlowCtrInfo_AnEmptyBodyMeansNoRules()
    {
        using var cluster = MockCluster.Start(1);
        DefaultMQAdminExt admin = Started(cluster, "cold_get_empty");
        try
        {
            Assert.Null(admin.GetColdDataFlowCtrInfo(cluster.BrokerAddrs[0]));
        }
        finally
        {
            admin.Shutdown();
        }
    }

    // ---------------------------------------------------------------- read-ahead 2004

    [Fact]
    public void SetCommitLogReadAheadMode_PutsTheModeUnderItsExactKey_AndReportsTheRemark()
    {
        using var cluster = MockCluster.Start(1);
        DefaultMQAdminExt admin = Started(cluster, "read_ahead");
        try
        {
            cluster.SetReplyCode(RequestCode.SetCommitlogReadMode, 0, "disk mode kept");

            string? remark = admin.SetCommitLogReadAheadMode(cluster.BrokerAddrs[0], "force_on");

            Assert.Equal("disk mode kept", remark);
            WireRecord rec = Single(cluster, RequestCode.SetCommitlogReadMode);
            Assert.Equal(new[] { "readAheadMode" }, KeysOf(rec));
            Assert.Equal("force_on", rec.Ext["readAheadMode"]);
            Assert.False(rec.HasBody);
        }
        finally
        {
            admin.Shutdown();
        }
    }

    [Fact]
    public void SetCommitLogReadAheadMode_ASilentBrokerAcceptsAndNoRemarkComesBack()
    {
        using var cluster = MockCluster.Start(1);
        DefaultMQAdminExt admin = Started(cluster, "read_ahead_quiet");
        try
        {
            Assert.Null(admin.SetCommitLogReadAheadMode(cluster.BrokerAddrs[0], "disk"));
        }
        finally
        {
            admin.Shutdown();
        }
    }

    [Fact]
    public void SetCommitLogReadAheadMode_ABrokerRefusalIsABrokerException()
    {
        using var cluster = MockCluster.Start(1);
        DefaultMQAdminExt admin = Started(cluster, "read_ahead_err");
        cluster.SetReplyCode(RequestCode.SetCommitlogReadMode, 1, "rocksdb only");
        try
        {
            MQBrokerException e = Assert.Throws<MQBrokerException>(() =>
                admin.SetCommitLogReadAheadMode(cluster.BrokerAddrs[0], "force_off"));
            Assert.Equal(1, e.ResponseCode);
        }
        finally
        {
            admin.Shutdown();
        }
    }

    // ---------------------------------------------------------------- pop rollback 200056

    [Fact]
    public void ExportPopRecords_SendsABareFrameWithNoHeaderAndNoBody()
    {
        using var cluster = MockCluster.Start(1);
        DefaultMQAdminExt admin = Started(cluster, "pop_export");
        try
        {
            admin.ExportPopRecords(cluster.BrokerAddrs[0], 12345);

            WireRecord rec = Single(cluster, RequestCode.PopRollback);
            Assert.Empty(rec.Ext);
            Assert.False(rec.HasBody);
        }
        finally
        {
            admin.Shutdown();
        }
    }

    [Fact]
    public void ExportPopRecords_ABrokerRefusalIsABrokerException()
    {
        using var cluster = MockCluster.Start(1);
        DefaultMQAdminExt admin = Started(cluster, "pop_export_err");
        cluster.SetReplyCode(RequestCode.PopRollback, 1, "export busy");
        try
        {
            MQBrokerException e = Assert.Throws<MQBrokerException>(() =>
                admin.ExportPopRecords(cluster.BrokerAddrs[0], 12345));
            Assert.Equal(1, e.ResponseCode);
        }
        finally
        {
            admin.Shutdown();
        }
    }

    // ---------------------------------------------------------------- RocksDB 354/355

    [Fact]
    public void CheckRocksdbCqWriteProgress_AlwaysCarriesTopicAndCheckStoreTime()
    {
        using var cluster = MockCluster.Start(1);
        DefaultMQAdminExt admin = Started(cluster, "rocksdb_check");
        try
        {
            cluster.SetReplyBody(RequestCode.CheckRocksdbCqWriteProgress, req =>
                new CheckRocksdbCqWriteResult { CheckResult = "ok: 3 pending", CheckStatus = 0 }.Encode());

            CheckRocksdbCqWriteResult? result =
                admin.CheckRocksdbCqWriteProgress(cluster.BrokerAddrs[0], "T_rock", 1700000000000);

            Assert.NotNull(result);
            Assert.Equal("ok: 3 pending", result!.CheckResult);
            Assert.Equal(CheckRocksdbCheckStatus.CheckOk, result.CheckStatus);

            WireRecord rec = Single(cluster, RequestCode.CheckRocksdbCqWriteProgress);
            Assert.Equal(new[] { "checkStoreTime", "topic" }, KeysOf(rec));
            Assert.Equal("T_rock", rec.Ext["topic"]);
            Assert.Equal("1700000000000", rec.Ext["checkStoreTime"]);
        }
        finally
        {
            admin.Shutdown();
        }
    }

    [Fact]
    public void CheckRocksdbCqWriteProgress_AnEmptyBodyDecodesToNull()
    {
        using var cluster = MockCluster.Start(1);
        DefaultMQAdminExt admin = Started(cluster, "rocksdb_check_empty");
        try
        {
            Assert.Null(admin.CheckRocksdbCqWriteProgress(cluster.BrokerAddrs[0], "T_rock", 1));
        }
        finally
        {
            admin.Shutdown();
        }
    }

    [Fact]
    public void CheckRocksdbCqWriteProgress_ABrokerRefusalIsAClientException()
    {
        using var cluster = MockCluster.Start(1);
        DefaultMQAdminExt admin = Started(cluster, "rocksdb_check_err");
        cluster.SetReplyCode(RequestCode.CheckRocksdbCqWriteProgress, 16, "not rocksdb storage");
        try
        {
            MQClientException e = Assert.Throws<MQClientException>(() =>
                admin.CheckRocksdbCqWriteProgress(cluster.BrokerAddrs[0], "T_rock", 1));
            Assert.Equal(16, e.ResponseCode);
        }
        finally
        {
            admin.Shutdown();
        }
    }

    [Fact]
    public void ExportRocksdbConfigToJson_JoinsTheTypesWithATrailingSeparator()
    {
        using var cluster = MockCluster.Start(1);
        DefaultMQAdminExt admin = Started(cluster, "rocksdb_export");
        try
        {
            admin.ExportRocksdbConfigToJson(cluster.BrokerAddrs[0],
                new[] { "topics", "subscriptionGroups" });

            WireRecord rec = Single(cluster, RequestCode.ExportRocksdbConfigToJson);
            Assert.Equal(new[] { "configType" }, KeysOf(rec));
            Assert.Equal("topics;subscriptionGroups;", rec.Ext["configType"]);
            Assert.False(rec.HasBody);
        }
        finally
        {
            admin.Shutdown();
        }
    }

    [Fact]
    public void ExportRocksdbConfigToJson_ABrokerRefusalIsAClientException()
    {
        using var cluster = MockCluster.Start(1);
        DefaultMQAdminExt admin = Started(cluster, "rocksdb_export_err");
        cluster.SetReplyCode(RequestCode.ExportRocksdbConfigToJson, 1, "export failed");
        try
        {
            MQClientException e = Assert.Throws<MQClientException>(() =>
                admin.ExportRocksdbConfigToJson(cluster.BrokerAddrs[0], new[] { "consumerOffsets" }));
            Assert.Equal(1, e.ResponseCode);
        }
        finally
        {
            admin.Shutdown();
        }
    }

    // ---------------------------------------------------------------- order conf 100/101

    [Fact]
    public void CreateOrUpdateOrderConf_ReadsTheCurrentValueThenPutsTheMergedOne()
    {
        using var cluster = MockCluster.Start(1);
        DefaultMQAdminExt admin = Started(cluster, "order_conf");
        try
        {
            admin.CreateOrUpdateOrderConf("T_order", "broker-a:8");

            WireRecord get = Single(cluster, RequestCode.GetKvConfig);
            Assert.Equal(MixAll.NamespaceOrderTopicConfig, get.Ext["namespace"]);
            Assert.Equal("T_order", get.Ext["key"]);

            WireRecord put = Single(cluster, RequestCode.PutKvConfig);
            Assert.Equal(MixAll.NamespaceOrderTopicConfig, put.Ext["namespace"]);
            Assert.Equal("T_order", put.Ext["key"]);
            Assert.Equal("broker-a:8", put.Ext["value"]);
        }
        finally
        {
            admin.Shutdown();
        }
    }

    [Fact]
    public void CreateOrUpdateOrderConf_MergesIntoTheExistingEntriesByFirstToken()
    {
        using var cluster = MockCluster.Start(1);
        DefaultMQAdminExt admin = Started(cluster, "order_conf_merge");
        try
        {
            cluster.SetReplyExt(RequestCode.GetKvConfig,
                req => new Dictionary<string, string> { ["value"] = "broker-a:8;broker-b:3" });

            admin.CreateOrUpdateOrderConf("T_order", "broker-b:7");

            WireRecord put = Single(cluster, RequestCode.PutKvConfig);
            Assert.Equal("broker-a:8;broker-b:7", put.Ext["value"]);
        }
        finally
        {
            admin.Shutdown();
        }
    }

    // ---------------------------------------------------------------- list forms 18/225

    [Fact]
    public void UpdateAndCreateTopicList_WrapsTheConfigsInTheListBody_WithAnEmptyHeader()
    {
        using var cluster = MockCluster.Start(1);
        DefaultMQAdminExt admin = Started(cluster, "topic_list");
        try
        {
            var configs = new List<TopicConfig>
            {
                new() { TopicName = "T_one", ReadQueueNums = 4, WriteQueueNums = 4 },
                new() { TopicName = "T_two", ReadQueueNums = 2, WriteQueueNums = 2 },
            };
            admin.CreateAndUpdateTopicConfigList(cluster.BrokerAddrs[0], configs);

            WireRecord rec = Single(cluster, RequestCode.UpdateAndCreateTopicList);
            Assert.Empty(rec.Ext);
            string body = BodyOf(rec);
            Assert.Contains("\"topicConfigList\":[", body);
            Assert.Contains("\"topicName\":\"T_one\"", body);
            Assert.Contains("\"topicName\":\"T_two\"", body);
        }
        finally
        {
            admin.Shutdown();
        }
    }

    [Fact]
    public void UpdateAndCreateTopicList_ABrokerRefusalIsAClientException()
    {
        using var cluster = MockCluster.Start(1);
        DefaultMQAdminExt admin = Started(cluster, "topic_list_err");
        cluster.SetReplyCode(RequestCode.UpdateAndCreateTopicList, 1, "list refused");
        try
        {
            MQClientException e = Assert.Throws<MQClientException>(() =>
                admin.CreateAndUpdateTopicConfigList(cluster.BrokerAddrs[0],
                    new List<TopicConfig> { new() { TopicName = "T_one" } }));
            Assert.Equal(1, e.ResponseCode);
            Assert.Equal("list refused", e.Message);
        }
        finally
        {
            admin.Shutdown();
        }
    }

    [Fact]
    public void UpdateAndCreateSubscriptionGroupList_WrapsTheGroupsInTheListBody()
    {
        using var cluster = MockCluster.Start(1);
        DefaultMQAdminExt admin = Started(cluster, "group_list");
        try
        {
            var configs = new List<SubscriptionGroupConfig>
            {
                new() { GroupName = "GID_one" },
                new() { GroupName = "GID_two" },
            };
            admin.CreateAndUpdateSubscriptionGroupConfigList(cluster.BrokerAddrs[0], configs);

            WireRecord rec = Single(cluster, RequestCode.UpdateAndCreateSubscriptiongroupList);
            Assert.Empty(rec.Ext);
            string body = BodyOf(rec);
            Assert.Contains("\"groupConfigList\":[", body);
            Assert.Contains("\"groupName\":\"GID_one\"", body);
            Assert.Contains("\"groupName\":\"GID_two\"", body);
        }
        finally
        {
            admin.Shutdown();
        }
    }

    [Fact]
    public void UpdateAndCreateSubscriptionGroupList_ABrokerRefusalIsAClientException()
    {
        using var cluster = MockCluster.Start(1);
        DefaultMQAdminExt admin = Started(cluster, "group_list_err");
        cluster.SetReplyCode(RequestCode.UpdateAndCreateSubscriptiongroupList, 14, "group refused");
        try
        {
            MQClientException e = Assert.Throws<MQClientException>(() =>
                admin.CreateAndUpdateSubscriptionGroupConfigList(cluster.BrokerAddrs[0],
                    new List<SubscriptionGroupConfig> { new() { GroupName = "GID_one" } }));
            Assert.Equal(14, e.ResponseCode);
        }
        finally
        {
            admin.Shutdown();
        }
    }

    // ---------------------------------------------------------------- non-success controls

    /// <summary>The broker answering 17 for the cold-dump read must surface as a broker
    /// exception with the code intact -- never as a null "no rules" result.</summary>
    [Fact]
    public void GetColdDataFlowCtrInfo_ANonSuccessReplyIsNeverANullDump()
    {
        using var cluster = MockCluster.Start(1);
        DefaultMQAdminExt admin = Started(cluster, "cold_get_err");
        cluster.SetReplyCode(RequestCode.GetColdDataFlowCtrInfo, 17, "no such store");
        try
        {
            MQBrokerException e = Assert.Throws<MQBrokerException>(() =>
                admin.GetColdDataFlowCtrInfo(cluster.BrokerAddrs[0]));
            Assert.Equal(17, e.ResponseCode);
        }
        finally
        {
            admin.Shutdown();
        }
    }

    /// <summary>Success must decode the body, not the remark: a remark that happens to carry
    /// text on a success is ignored.</summary>
    [Fact]
    public void ExportPopRecords_KeepsSuccessSilentEvenWhenARemarkRidesAlong()
    {
        using var cluster = MockCluster.Start(1);
        DefaultMQAdminExt admin = Started(cluster, "pop_export_remark");
        cluster.SetReplyCode(RequestCode.PopRollback, 0, "exported 42 records");
        try
        {
            admin.ExportPopRecords(cluster.BrokerAddrs[0], 12345);
            Assert.Equal(1, cluster.CountRequests(RequestCode.PopRollback));
        }
        finally
        {
            admin.Shutdown();
        }
    }
}
