// Wire-level proof of the #47 ops RPC group (Java MQClientAPIImpl:2773-3816):
// queryCorrectionOffset(308), the three unit-topic reads (311/312/313), getAllProducerInfo(328),
// getBrokerHAStatus(907), resetMasterFlushOffset(908), electMaster(1002), getBrokerEpochCache(1007)
// and switchTimerEngine(5001).
//
// Everything runs against the in-process mock cluster; no real cluster is involved. The routing
// split is pinned by evidence, not by trusting the caller: 308/328 move to the fast channel when
// the switch is on (the probe bound two ports below the plain address records the landing),
// 907 keeps the plain address even with the switch on, and the unit-topic family is answered by
// the name server side of the mock.
using System.Text;
using RocketMQ.Common;
using RocketMQ.Remoting.Protocol;
using Xunit;
using PropertyMap = System.Collections.Generic.SortedDictionary<string, string>;

namespace RocketMQ.Client.Tests;

public class AdminOpsRpcTests
{
    private const string Topic = "OpsTopic";
    private const string Group = "GID_OpsTopic";

    private static DefaultMQAdminExt Started(MockCluster cluster, string tag, bool vipOn = false)
    {
        var admin = new DefaultMQAdminExt("ADMIN_ops_" + tag);
        admin.SetNamesrvAddr(cluster.NamesrvAddr);
        admin.VipChannelEnabled = vipOn;
        admin.Start();
        return admin;
    }

    private static WireRecord Single(MockCluster cluster, int code) =>
        Assert.Single(cluster.Records().FindAll(r => r.Code == code));

    private static byte[] Body(string json) => Encoding.UTF8.GetBytes(json);

    // ---------------------------------------------------------------- 308

    [Fact]
    public void QueryCorrectionOffset_SendsTheThreeHeaderFields_AndDecodesTheMap()
    {
        using var cluster = MockCluster.Start(1);
        DefaultMQAdminExt admin = Started(cluster, "correction");
        try
        {
            cluster.SetReplyBody(RequestCode.QueryCorrectionOffset, req => Body(
                "{\"correctionOffsets\":{\"0\":110,\"3\":407}}"));

            Dictionary<int, long> offsets = admin.QueryCorrectionOffset(cluster.BrokerAddrs[0],
                Topic, Group, new[] { "GID_x", "GID_other" });

            WireRecord rec = Single(cluster, RequestCode.QueryCorrectionOffset);
            // Java joins the Set itself, comma-joined; compareGroup carries the group
            Assert.Equal(3, rec.Ext.Count);
            Assert.Equal(Topic, rec.Ext["topic"]);
            Assert.Equal(Group, rec.Ext["compareGroup"]);
            Assert.Equal(new HashSet<string> { "GID_other", "GID_x" },
                new HashSet<string>(rec.Ext["filterGroups"].Split(',')));
            Assert.Equal(2, offsets.Count);
            Assert.Equal(110, offsets[0]);
            Assert.Equal(407, offsets[3]);
        }
        finally
        {
            admin.Shutdown();
        }
    }

    [Fact]
    public void QueryCorrectionOffset_AnEmptyFilterSetOmitsTheField()
    {
        using var cluster = MockCluster.Start(1);
        DefaultMQAdminExt admin = Started(cluster, "correction_nofilter");
        try
        {
            cluster.SetReplyBody(RequestCode.QueryCorrectionOffset,
                req => Body("{\"correctionOffsets\":{}}"));

            Dictionary<int, long> offsets =
                admin.QueryCorrectionOffset(cluster.BrokerAddrs[0], Topic, Group);

            WireRecord rec = Single(cluster, RequestCode.QueryCorrectionOffset);
            Assert.False(rec.Ext.ContainsKey("filterGroups"));
            Assert.Equal(2, rec.Ext.Count);
            Assert.Empty(offsets);
        }
        finally
        {
            admin.Shutdown();
        }
    }

    [Fact]
    public void QueryCorrectionOffset_TakesTheVipPortWhenTheSwitchIsOn()
    {
        using var cluster = MockCluster.Start(1);
        DefaultMQAdminExt admin = Started(cluster, "correction_vip", vipOn: true);
        using FastChannelReply? fast = FastChannelReply.Start(1, "store busy");
        Assert.NotNull(fast);
        try
        {
            MQBrokerException e = Assert.Throws<MQBrokerException>(() =>
                admin.QueryCorrectionOffset(fast!.PlainAddr, Topic, Group));
            Assert.Equal(1, e.ResponseCode);
            Assert.True(fast!.FrameArrived);
            Assert.Equal(0, cluster.CountRequests(RequestCode.QueryCorrectionOffset));
        }
        finally
        {
            admin.Shutdown();
        }
    }

    // ---------------------------------------------------------------- 311/312/313

    [Fact]
    public void UnitTopicListFamily_HitsTheNameServer_WithANullHeader()
    {
        const string body = "{\"topicList\":[\"TopicA\",\"%RETRY%G\"]}";
        (int Code, Func<DefaultMQAdminExt, bool, TopicList> Call)[] family =
        {
            (RequestCode.GetUnitTopicList, (a, retry) => a.GetUnitTopicList(retry)),
            (RequestCode.GetHasUnitSubTopicList, (a, retry) => a.GetHasUnitSubTopicList(retry)),
            (RequestCode.GetHasUnitSubUnunitTopicList,
                (a, retry) => a.GetHasUnitSubUnUnitTopicList(retry)),
        };
        foreach ((int code, Func<DefaultMQAdminExt, bool, TopicList> call) in family)
        {
            using var cluster = MockCluster.Start(1);
            DefaultMQAdminExt admin = Started(cluster, "unit_" + code);
            try
            {
                cluster.SetReplyBody(code, req => Body(body));

                TopicList list = call(admin, true);

                WireRecord rec = Single(cluster, code);
                // Java invokeSync(null, ...): the request goes to a name server with no header at all
                Assert.Empty(rec.Ext);
                Assert.Equal(2, list.Topics.Count);
                Assert.Equal("TopicA", list.Topics[0]);
                Assert.Equal("%RETRY%G", list.Topics[1]);
                // only the name server saw it: no broker copy
                Assert.Equal(1, cluster.CountRequests(code));
            }
            finally
            {
                admin.Shutdown();
            }
        }
    }

    [Fact]
    public void UnitTopicList_FiltersRetryTopicsLocallyWhenNotContained()
    {
        using var cluster = MockCluster.Start(1);
        DefaultMQAdminExt admin = Started(cluster, "unit_filter");
        try
        {
            cluster.SetReplyBody(RequestCode.GetUnitTopicList, req => Body(
                "{\"topicList\":[\"TopicA\",\"%RETRY%G\",\"%RETRY%G_lite\"]}"));

            TopicList list = admin.GetUnitTopicList(containRetry: false);

            // the raw answer is untouched on the wire: the filter happens after decoding
            WireRecord rec = Single(cluster, RequestCode.GetUnitTopicList);
            Assert.Empty(rec.Ext);
            string remaining = Assert.Single(list.Topics);
            Assert.Equal("TopicA", remaining);
        }
        finally
        {
            admin.Shutdown();
        }
    }

    [Fact]
    public void UnitTopicList_ABodylessNameServerAnswerRaisesAClientException()
    {
        using var cluster = MockCluster.Start(1);
        DefaultMQAdminExt admin = Started(cluster, "unit_failure");
        try
        {
            // The mock name server answers SUCCESS without a body for the unscripted code:
            // the empty body is a failure for this family, not an empty list.
            MQClientException e = Assert.Throws<MQClientException>(() =>
                admin.GetHasUnitSubTopicList());
            Assert.Equal("the name server answered without a body", e.Message);
        }
        finally
        {
            admin.Shutdown();
        }
    }

    // ---------------------------------------------------------------- 328

    [Fact]
    public void GetAllProducerInfo_DecodesTheProducerTable_WithAnEmptyHeader()
    {
        using var cluster = MockCluster.Start(1);
        DefaultMQAdminExt admin = Started(cluster, "producer_table");
        try
        {
            cluster.SetReplyBody(RequestCode.GetAllProducerInfo, req => Body(
                "{\"data\":{\"G\":[{\"clientId\":\"cid-1\",\"remoteIP\":\"10.0.0.1\","
                + "\"language\":\"JAVA\",\"version\":500,\"lastUpdateTimestamp\":1700000000000}]}}"));

            ProducerTableInfo table = admin.GetAllProducerInfo(cluster.BrokerAddrs[0]);

            WireRecord rec = Single(cluster, RequestCode.GetAllProducerInfo);
            // GetAllProducerInfoRequestHeader is empty
            Assert.Empty(rec.Ext);
            ProducerInfo producer = Assert.Single(table.Data["G"]);
            Assert.Equal("cid-1", producer.ClientId);
            Assert.Equal("10.0.0.1", producer.RemoteIP);
            Assert.Equal("JAVA", producer.Language);
            Assert.Equal(500, producer.Version);
            Assert.Equal(1700000000000, producer.LastUpdateTimestamp);
        }
        finally
        {
            admin.Shutdown();
        }
    }

    [Fact]
    public void GetAllProducerInfo_TakesTheVipPortWhenTheSwitchIsOn()
    {
        using var cluster = MockCluster.Start(1);
        DefaultMQAdminExt admin = Started(cluster, "producer_vip", vipOn: true);
        using FastChannelReply? fast = FastChannelReply.Start(1, "store busy");
        Assert.NotNull(fast);
        try
        {
            MQBrokerException e = Assert.Throws<MQBrokerException>(() =>
                admin.GetAllProducerInfo(fast!.PlainAddr));
            Assert.Equal(1, e.ResponseCode);
            Assert.True(fast!.FrameArrived);
            Assert.Equal(0, cluster.CountRequests(RequestCode.GetAllProducerInfo));
        }
        finally
        {
            admin.Shutdown();
        }
    }

    [Fact]
    public void GetAllProducerInfo_AnEmptyBodyFails()
    {
        using var cluster = MockCluster.Start(1);
        DefaultMQAdminExt admin = Started(cluster, "producer_empty");
        try
        {
            Assert.Throws<MQBrokerException>(() => admin.GetAllProducerInfo(cluster.BrokerAddrs[0]));
        }
        finally
        {
            admin.Shutdown();
        }
    }

    // ---------------------------------------------------------------- 907

    [Fact]
    public void GetBrokerHaStatus_DecodesTheNestedRuntimeInfo_FromThePlainAddress()
    {
        using var cluster = MockCluster.Start(1);
        DefaultMQAdminExt admin = Started(cluster, "ha_status");
        try
        {
            cluster.SetReplyBody(RequestCode.GetBrokerHaStatus, req => Body(
                "{\"master\":true,\"masterCommitLogMaxOffset\":9001,\"inSyncSlaveNums\":2,"
                + "\"haConnectionInfo\":[{\"addr\":\"10.0.0.2:10912\",\"slaveAckOffset\":8900,"
                + "\"diff\":101,\"inSync\":true,\"transferredByteInSecond\":2048,"
                + "\"transferFromWhere\":8700}],"
                + "\"haClientRuntimeInfo\":{\"masterAddr\":\"10.0.0.1:10912\","
                + "\"transferredByteInSecond\":12,\"maxOffset\":9001,\"lastReadTimestamp\":111,"
                + "\"lastWriteTimestamp\":222,\"masterFlushOffset\":8800,\"activated\":true}}"));

            HARuntimeInfo info = admin.GetBrokerHaStatus(cluster.BrokerAddrs[0]);

            WireRecord rec = Single(cluster, RequestCode.GetBrokerHaStatus);
            Assert.Empty(rec.Ext); // null header
            Assert.True(info.Master);
            Assert.Equal(9001, info.MasterCommitLogMaxOffset);
            Assert.Equal(2, info.InSyncSlaveNums);
            HAConnectionRuntimeInfo conn = Assert.Single(info.HaConnectionInfo);
            Assert.Equal("10.0.0.2:10912", conn.Addr);
            Assert.Equal(8900, conn.SlaveAckOffset);
            Assert.Equal(101, conn.Diff);
            Assert.True(conn.InSync);
            Assert.Equal(2048, conn.TransferredByteInSecond);
            Assert.Equal(8700, conn.TransferFromWhere);
            Assert.Equal("10.0.0.1:10912", info.HaClientRuntimeInfo.MasterAddr);
            Assert.Equal(9001, info.HaClientRuntimeInfo.MaxOffset);
            Assert.Equal(8800, info.HaClientRuntimeInfo.MasterFlushOffset);
            Assert.True(info.HaClientRuntimeInfo.Activated);
        }
        finally
        {
            admin.Shutdown();
        }
    }

    [Fact]
    public void GetBrokerHaStatus_StaysOnThePlainAddressEvenWithTheSwitchOn()
    {
        using var cluster = MockCluster.Start(1);
        DefaultMQAdminExt admin = Started(cluster, "ha_status_vip", vipOn: true);
        try
        {
            cluster.SetReplyBody(RequestCode.GetBrokerHaStatus,
                req => Body("{\"master\":false}"));

            // 907 is outside the rewritten family: the plain mock must answer (the VIP probe
            // two ports below is not even bound, so a mis-rewrite would fail the call)
            HARuntimeInfo info = admin.GetBrokerHaStatus(cluster.BrokerAddrs[0]);

            Assert.False(info.Master);
            Assert.Equal(1, cluster.CountRequests(RequestCode.GetBrokerHaStatus));
        }
        finally
        {
            admin.Shutdown();
        }
    }

    [Fact]
    public void GetBrokerHaStatus_ANonSuccessAnswerRaisesABrokerException()
    {
        using var cluster = MockCluster.Start(1);
        DefaultMQAdminExt admin = Started(cluster, "ha_failure");
        try
        {
            cluster.SetReplyCode(RequestCode.GetBrokerHaStatus, ResponseCode.SystemError, "boom");

            MQBrokerException e = Assert.Throws<MQBrokerException>(() =>
                admin.GetBrokerHaStatus(cluster.BrokerAddrs[0]));
            Assert.Equal(ResponseCode.SystemError, e.ResponseCode);
        }
        finally
        {
            admin.Shutdown();
        }
    }

    // ---------------------------------------------------------------- 908

    [Fact]
    public void ResetMasterFlushOffset_SendsThePrimitiveLong_EvenAsZero()
    {
        using var cluster = MockCluster.Start(1);
        DefaultMQAdminExt admin = Started(cluster, "flush_offset");
        try
        {
            admin.ResetMasterFlushOffset(cluster.BrokerAddrs[0], 12345);

            WireRecord rec = Single(cluster, RequestCode.ResetMasterFlushOffset);
            Assert.Equal("12345", rec.Ext["masterFlushOffset"]);
            Assert.Single(rec.Ext);

            // 0 is a real value and must be sent, never dropped as "unset"
            admin.ResetMasterFlushOffset(cluster.BrokerAddrs[0], 0);
            List<WireRecord> both = cluster.Records()
                .FindAll(r => r.Code == RequestCode.ResetMasterFlushOffset);
            Assert.Equal(2, both.Count);
            Assert.Equal("0", both[1].Ext["masterFlushOffset"]);
        }
        finally
        {
            admin.Shutdown();
        }
    }

    [Fact]
    public void ResetMasterFlushOffset_ANonSuccessAnswerRaisesABrokerException()
    {
        using var cluster = MockCluster.Start(1);
        DefaultMQAdminExt admin = Started(cluster, "flush_failure");
        try
        {
            cluster.SetReplyCode(RequestCode.ResetMasterFlushOffset, ResponseCode.SystemError,
                "still syncing");

            MQBrokerException e = Assert.Throws<MQBrokerException>(() =>
                admin.ResetMasterFlushOffset(cluster.BrokerAddrs[0], 99));
            Assert.Equal(ResponseCode.SystemError, e.ResponseCode);
            Assert.Equal("still syncing", e.ResponseMessage);
        }
        finally
        {
            admin.Shutdown();
        }
    }

    // ---------------------------------------------------------------- 1002

    private static void ScriptLeader(MockCluster cluster, string leaderAddr)
    {
        cluster.SetReplyExt(RequestCode.ControllerGetMetadataInfo, req => new Dictionary<string, string>
        {
            ["group"] = "default",
            ["controllerLeaderId"] = "1",
            ["controllerLeaderAddress"] = leaderAddr,
            ["isLeader"] = "true",
            ["peers"] = "1: " + leaderAddr,
        });
    }

    [Fact]
    public void ElectMaster_ResolvesTheLeaderThenSendsTheAdminTriggerHeader()
    {
        using var cluster = MockCluster.Start(1);
        DefaultMQAdminExt admin = Started(cluster, "elect");
        try
        {
            string leader = cluster.BrokerAddrs[0];
            ScriptLeader(cluster, leader);
            cluster.SetReplyExt(RequestCode.ControllerElectMaster, req => new Dictionary<string, string>
            {
                ["masterBrokerId"] = "1",
                ["masterAddress"] = "10.0.0.2:10911",
                ["masterEpoch"] = "3",
                ["syncStateSetEpoch"] = "7",
            });
            cluster.SetReplyBody(RequestCode.ControllerElectMaster, req => Body(
                "{\"cluster\":\"DefaultCluster\",\"brokerName\":\"broker-a\","
                + "\"brokerAddrs\":{\"0\":\"10.0.0.1:10911\",\"1\":\"10.0.0.2:10911\"}}"));

            (ElectMasterResponseHeader Header, BrokerMemberGroup MemberGroup) result =
                admin.ElectMaster(leader, "DefaultCluster", "broker-a", 1);

            // step 1: the metadata read names the leader; step 2 lands on the leader
            WireRecord meta = Single(cluster, RequestCode.ControllerGetMetadataInfo);
            WireRecord rec = Single(cluster, RequestCode.ControllerElectMaster);
            List<WireRecord> all = cluster.Records();
            Assert.True(all.FindIndex(r => ReferenceEquals(r, meta))
                        < all.FindIndex(r => ReferenceEquals(r, rec)),
                "1005 metadata must precede the 1002 elect");
            // ofAdminTrigger shape: five fields, designateElect forced true, invokeTime = now
            Assert.Equal(5, rec.Ext.Count);
            Assert.Equal("DefaultCluster", rec.Ext["clusterName"]);
            Assert.Equal("broker-a", rec.Ext["brokerName"]);
            Assert.Equal("1", rec.Ext["brokerId"]);
            Assert.Equal("true", rec.Ext["designateElect"]);
            Assert.True(long.Parse(rec.Ext["invokeTime"]) > 0);
            // response header comes back from the extFields
            Assert.Equal(1, result.Header.MasterBrokerId);
            Assert.Equal("10.0.0.2:10911", result.Header.MasterAddress);
            Assert.Equal(3, result.Header.MasterEpoch);
            Assert.Equal(7, result.Header.SyncStateSetEpoch);
            // response body BrokerMemberGroup with numeric-keyed brokerAddrs
            Assert.Equal("DefaultCluster", result.MemberGroup.Cluster);
            Assert.Equal("broker-a", result.MemberGroup.BrokerName);
            Assert.Equal(2, result.MemberGroup.BrokerAddrs.Count);
            Assert.Equal("10.0.0.1:10911", result.MemberGroup.BrokerAddrs[0]);
            Assert.Equal("10.0.0.2:10911", result.MemberGroup.BrokerAddrs[1]);
        }
        finally
        {
            admin.Shutdown();
        }
    }

    [Fact]
    public void ElectMaster_WithoutADesignatedIdSendsMinusOne()
    {
        using var cluster = MockCluster.Start(1);
        DefaultMQAdminExt admin = Started(cluster, "elect_noid");
        try
        {
            string leader = cluster.BrokerAddrs[0];
            ScriptLeader(cluster, leader);
            cluster.SetReplyBody(RequestCode.ControllerElectMaster, req => Body("{}"));

            admin.ElectMaster(leader, "DefaultCluster", "broker-a");

            WireRecord rec = Single(cluster, RequestCode.ControllerElectMaster);
            Assert.Equal(5, rec.Ext.Count);
            Assert.Equal("-1", rec.Ext["brokerId"]);
            Assert.Equal("true", rec.Ext["designateElect"]);
        }
        finally
        {
            admin.Shutdown();
        }
    }

    [Fact]
    public void ElectMaster_ANonSuccessAnswerRaisesABrokerException()
    {
        using var cluster = MockCluster.Start(1);
        DefaultMQAdminExt admin = Started(cluster, "elect_failure");
        try
        {
            ScriptLeader(cluster, cluster.BrokerAddrs[0]);
            cluster.SetReplyCode(RequestCode.ControllerElectMaster, ResponseCode.SystemError,
                "elect failed");

            MQBrokerException e = Assert.Throws<MQBrokerException>(() =>
                admin.ElectMaster(cluster.BrokerAddrs[0], "DefaultCluster", "broker-a"));
            Assert.Equal(ResponseCode.SystemError, e.ResponseCode);
        }
        finally
        {
            admin.Shutdown();
        }
    }

    // ---------------------------------------------------------------- 1007

    [Fact]
    public void GetBrokerEpochCache_DecodesTheEpochList_WithTheLongMaxDefaultEndOffset()
    {
        using var cluster = MockCluster.Start(1);
        DefaultMQAdminExt admin = Started(cluster, "epoch");
        try
        {
            cluster.SetReplyBody(RequestCode.GetBrokerEpochCache, req => Body(
                "{\"clusterName\":\"DefaultCluster\",\"brokerName\":\"broker-a\",\"brokerId\":1,"
                + "\"epochList\":[{\"epoch\":1,\"startOffset\":0,\"endOffset\":500},"
                + "{\"epoch\":2,\"startOffset\":500}],\"maxOffset\":900}"));

            EpochEntryCache cache = admin.GetBrokerEpochCache(cluster.BrokerAddrs[0]);

            WireRecord rec = Single(cluster, RequestCode.GetBrokerEpochCache);
            Assert.Empty(rec.Ext);
            Assert.Equal("DefaultCluster", cache.ClusterName);
            Assert.Equal("broker-a", cache.BrokerName);
            Assert.Equal(1, cache.BrokerId);
            Assert.Equal(900, cache.MaxOffset);
            Assert.Equal(2, cache.EpochList.Count);
            Assert.Equal((1, 0, 500), (cache.EpochList[0].Epoch, cache.EpochList[0].StartOffset,
                cache.EpochList[0].EndOffset));
            // a missing endOffset decodes to Java's LAST_EPOCH_END_OFFSET (Long.MAX_VALUE)
            Assert.Equal((2, 500), (cache.EpochList[1].Epoch, cache.EpochList[1].StartOffset));
            Assert.Equal(long.MaxValue, cache.EpochList[1].EndOffset);
        }
        finally
        {
            admin.Shutdown();
        }
    }

    [Fact]
    public void GetBrokerEpochCache_AnEmptyBodyFails()
    {
        using var cluster = MockCluster.Start(1);
        DefaultMQAdminExt admin = Started(cluster, "epoch_empty");
        try
        {
            Assert.Throws<MQBrokerException>(() => admin.GetBrokerEpochCache(cluster.BrokerAddrs[0]));
        }
        finally
        {
            admin.Shutdown();
        }
    }

    // ---------------------------------------------------------------- 5001

    [Fact]
    public void SwitchTimerEngine_SendsTheSingleETField()
    {
        using var cluster = MockCluster.Start(1);
        DefaultMQAdminExt admin = Started(cluster, "timer_engine");
        try
        {
            admin.SwitchTimerEngine(cluster.BrokerAddrs[0], "rocksdb", 4500);

            WireRecord rec = Single(cluster, RequestCode.SwitchTimerEngine);
            // the engine type is the single ext field E_T (MessageConst.TIMER_ENGINE_TYPE)
            Assert.Single(rec.Ext);
            Assert.Equal("rocksdb", rec.Ext["E_T"]);
        }
        finally
        {
            admin.Shutdown();
        }
    }

    [Fact]
    public void SwitchTimerEngine_ANonSuccessAnswerRaisesABrokerException()
    {
        using var cluster = MockCluster.Start(1);
        DefaultMQAdminExt admin = Started(cluster, "timer_failure");
        try
        {
            cluster.SetReplyCode(RequestCode.SwitchTimerEngine, ResponseCode.SystemError,
                "engine busy");

            Assert.Throws<MQBrokerException>(() =>
                admin.SwitchTimerEngine(cluster.BrokerAddrs[0], "rocksdb"));
        }
        finally
        {
            admin.Shutdown();
        }
    }
}
